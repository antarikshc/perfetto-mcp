"""Slice finder tool for discovering slices via flexible patterns.

Provides aggregated statistics and example slices without requiring manual SQL.
"""

import logging
from typing import Any, Dict, List, Optional, Tuple

from .base import BaseTool, ToolError

logger = logging.getLogger(__name__)


class SliceFinderTool(BaseTool):
    """Tool for discovering slices matching a pattern with optional filters."""

    def find_slices(
        self,
        trace_path: str,
        pattern: str,
        process_name: Optional[str] = None,
        match_mode: str = "contains",
        limit: int = 50,
        main_thread_only: bool = False,
        time_range: Optional[Dict[str, float | int]] = None,
    ) -> str:
        """Find slices by name using flexible matching and return aggregates + examples.

        Args:
            trace_path: Path to the trace file.
            pattern: Slice name pattern to match. Required and non-empty.
            process_name: Optional process name filter. Supports wildcards ('*' or '%').
            match_mode: One of {'contains', 'exact', 'glob'}. Defaults to 'contains'.
            limit: Max number of example slices to return (1..50). Defaults to 50.
            main_thread_only: If true, only include slices from process main threads.
            time_range: Optional dict with {'start_ms': X, 'end_ms': Y} bounds.

        Returns:
            JSON envelope string with result payload:
            {
              "matchMode": str,
              "filters": {...},
              "timeRangeMs": {...} | null,
              "aggregates": [
                {"name", "count", "minMs", "avgMs", "maxMs", "p50Ms", "p90Ms", "p99Ms", "linkable"}
              ],
              "examples": [
                {"sliceId", "tsMs", "endTsMs", "durMs", "thread_name", "tid", "is_main_thread", "process_name", "pid", "trackName", "category", "depth"}
              ],
              "notes": [str]
            }
        """

        # Validate inputs early and build operation for connection execution
        def _validate_and_normalize() -> Tuple[str, str, int, Optional[Tuple[int, int]], List[str]]:
            notes: List[str] = []

            if not isinstance(pattern, str) or not pattern.strip():
                raise ToolError("INVALID_PARAMETERS", "'pattern' must be a non-empty string")

            safe_pattern = pattern.strip().replace("'", "''")

            supported_modes = {"contains", "exact", "glob"}
            if match_mode not in supported_modes:
                raise ToolError(
                    "INVALID_PARAMETERS",
                    f"Unsupported match_mode '{match_mode}'. Supported: contains|exact|glob",
                )

            # Clamp limit to a safe range
            try:
                limit_int = int(limit)
            except Exception:
                raise ToolError("INVALID_PARAMETERS", "'limit' must be an integer")
            if limit_int < 1:
                limit_int = 1
            if limit_int > 500:
                limit_int = 500

            time_bounds_ns: Optional[Tuple[int, int]] = None
            if time_range is not None:
                if not isinstance(time_range, dict):
                    raise ToolError("INVALID_PARAMETERS", "'time_range' must be a dict with start_ms/end_ms")
                start_ms = time_range.get("start_ms")
                end_ms = time_range.get("end_ms")
                if start_ms is None or end_ms is None:
                    raise ToolError("INVALID_PARAMETERS", "time_range requires both start_ms and end_ms")
                try:
                    start_ns = int(float(start_ms) * 1e6)
                    end_ns = int(float(end_ms) * 1e6)
                except Exception:
                    raise ToolError("INVALID_PARAMETERS", "time_range values must be numeric")
                if end_ns < start_ns:
                    raise ToolError("INVALID_PARAMETERS", "time_range end_ms must be >= start_ms")
                time_bounds_ns = (start_ns, end_ns)

            if match_mode == "glob":
                notes.append("GLOB match is case-sensitive per SQLite semantics")

            return safe_pattern, match_mode, limit_int, time_bounds_ns, notes

        safe_pattern, normalized_mode, limit_int, time_bounds_ns, initial_notes = _validate_and_normalize()

        def _build_where_clauses() -> List[str]:
            clauses: List[str] = []

            if normalized_mode == "contains":
                clauses.append(f"UPPER(s.name) LIKE UPPER('%{safe_pattern}%')")
            elif normalized_mode == "exact":
                clauses.append(f"UPPER(s.name) = UPPER('{safe_pattern}')")
            elif normalized_mode == "glob":
                clauses.append(f"s.name GLOB '{safe_pattern}'")

            if process_name:
                proc = str(process_name).strip().replace("'", "''")
                # LIKE with wildcard support: treat '*' as '%'
                if "*" in proc:
                    proc_like = proc.replace("*", "%")
                else:
                    # If no wildcard provided, do contains match for ergonomics
                    proc_like = f"%{proc}%"
                clauses.append(f"UPPER(p.name) LIKE UPPER('{proc_like}')")

            if main_thread_only:
                clauses.append("th.is_main_thread = 1")

            if time_bounds_ns is not None:
                start_ns, end_ns = time_bounds_ns
                clauses.append(f"s.ts BETWEEN {start_ns} AND {end_ns}")

            return clauses

        def _to_ms(value_ns: Optional[int | float]) -> Optional[float]:
            if value_ns is None:
                return None
            try:
                return float(value_ns) / 1e6
            except Exception:
                return None

        def _operation(tp) -> Dict[str, Any]:
            where_clauses = _build_where_clauses()
            where_sql = ("WHERE " + " AND ".join(where_clauses)) if where_clauses else ""

            # Common slice rows with joins to resolve thread/process context
            base_cte = (
                "WITH slice_rows AS (\n"
                "  SELECT s.id, s.ts, s.dur, s.depth, s.category, s.track_id, s.name AS slice_name,\n"
                "         tr.name AS track_name,\n"
                "         th.name AS thread_name, th.tid, th.is_main_thread,\n"
                "         p.name AS process_name, p.pid\n"
                "  FROM slice s\n"
                "  JOIN track tr ON s.track_id = tr.id\n"
                "  LEFT JOIN thread_track ttr ON s.track_id = ttr.id\n"
                "  LEFT JOIN thread th ON ttr.utid = th.utid\n"
                "  LEFT JOIN process_track pt ON s.track_id = pt.id\n"
                "  LEFT JOIN process p ON COALESCE(th.upid, pt.upid) = p.upid\n"
                f"  {where_sql}\n"
                ")\n"
            )

            aggregates: List[Dict[str, Any]] = []
            examples: List[Dict[str, Any]] = []
            notes: List[str] = list(initial_notes)

            # Attempt to compute percentiles if available, using the CTE to avoid alias issues
            agg_with_percentiles = (
                base_cte
                + "SELECT\n"
                + "  slice_name AS name,\n"
                + "  COUNT(*) AS total_count,\n"
                + "  MIN(dur) AS min_dur_ns,\n"
                + "  AVG(dur) AS avg_dur_ns,\n"
                + "  MAX(dur) AS max_dur_ns,\n"
                + "  quantile(dur, 0.5) AS p50_ns,\n"
                + "  quantile(dur, 0.9) AS p90_ns,\n"
                + "  quantile(dur, 0.99) AS p99_ns\n"
                + "FROM slice_rows\n"
                + "GROUP BY slice_name\n"
                + "ORDER BY total_count DESC\n"
            )

            agg_fallback = (
                base_cte
                + "SELECT\n"
                + "  slice_name AS name,\n"
                + "  COUNT(*) AS total_count,\n"
                + "  MIN(dur) AS min_dur_ns,\n"
                + "  AVG(dur) AS avg_dur_ns,\n"
                + "  MAX(dur) AS max_dur_ns\n"
                + "FROM slice_rows\n"
                + "GROUP BY slice_name\n"
                + "ORDER BY total_count DESC\n"
            )

            def _collect_aggs(row) -> Dict[str, Any]:
                name_val = getattr(row, "name", None)
                count_val = int(getattr(row, "total_count", 0) or 0)
                min_ms = _to_ms(getattr(row, "min_dur_ns", None))
                avg_ms = _to_ms(getattr(row, "avg_dur_ns", None))
                max_ms = _to_ms(getattr(row, "max_dur_ns", None))
                p50_ms = _to_ms(getattr(row, "p50_ns", None)) if hasattr(row, "p50_ns") else None
                p90_ms = _to_ms(getattr(row, "p90_ns", None)) if hasattr(row, "p90_ns") else None
                p99_ms = _to_ms(getattr(row, "p99_ns", None)) if hasattr(row, "p99_ns") else None
                return {
                    "name": name_val,
                    "count": count_val,
                    "minMs": min_ms,
                    "avgMs": avg_ms,
                    "maxMs": max_ms,
                    "p50Ms": p50_ms,
                    "p90Ms": p90_ms,
                    "p99Ms": p99_ms,
                    "linkable": True,
                }

            # Try percentiles, fall back gracefully if unavailable
            tried_percentiles = False
            try:
                tried_percentiles = True
                for row in tp.query(agg_with_percentiles):
                    aggregates.append(_collect_aggs(row))
            except Exception as e:
                # Detect missing quantile function
                msg = str(e).lower()
                if "no such function" in msg and "quantile" in msg:
                    notes.append("Percentile functions unavailable; p50/p90/p99 set to null")
                else:
                    notes.append(f"Percentiles not computed: {e}")
                # Fallback without percentiles
                try:
                    for row in tp.query(agg_fallback):
                        aggregates.append(_collect_aggs(row))
                except Exception as e2:
                    raise ToolError("QUERY_FAILED", f"Aggregate query failed: {e2}")

            # Examples: top-N by duration
            examples_query = (
                base_cte
                + "SELECT\n"
                + "  id AS slice_id,\n"
                + "  CAST(ts / 1e6 AS INT) AS ts_ms,\n"
                + "  CAST((ts + dur) / 1e6 AS INT) AS end_ts_ms,\n"
                + "  CAST(dur / 1e6 AS REAL) AS dur_ms,\n"
                + "  depth, category, track_id, track_name,\n"
                + "  thread_name, tid, is_main_thread, process_name, pid\n"
                + "FROM slice_rows\n"
                + "ORDER BY dur DESC\n"
                + f"LIMIT {limit_int};\n"
            )

            try:
                for row in tp.query(examples_query):
                    examples.append(
                        {
                            "sliceId": getattr(row, "slice_id", None),
                            "trackId": getattr(row, "track_id", None),
                            "tsMs": getattr(row, "ts_ms", None),
                            "endTsMs": getattr(row, "end_ts_ms", None),
                            "durMs": float(getattr(row, "dur_ms", 0.0) or 0.0),
                            "depth": getattr(row, "depth", None),
                            "category": getattr(row, "category", None),
                            "trackName": getattr(row, "track_name", None),
                            "thread_name": getattr(row, "thread_name", None),
                            "tid": getattr(row, "tid", None),
                            "is_main_thread": getattr(row, "is_main_thread", None),
                            "process_name": getattr(row, "process_name", None),
                            "pid": getattr(row, "pid", None),
                        }
                    )
            except Exception as e:
                raise ToolError("QUERY_FAILED", f"Example slice query failed: {e}")

            result: Dict[str, Any] = {
                "matchMode": normalized_mode,
                "filters": {
                    "processName": process_name,
                    "mainThreadOnly": main_thread_only,
                    "limit": limit_int,
                    "pattern": pattern,
                },
                "timeRangeMs": (
                    None
                    if time_bounds_ns is None
                    else {
                        "startMs": int((time_bounds_ns[0]) / 1e6),
                        "endMs": int((time_bounds_ns[1]) / 1e6),
                    }
                ),
                "aggregates": aggregates,
                "examples": examples,
                "notes": notes,
            }
            return result

        return self.run_formatted(trace_path, process_name, _operation)


