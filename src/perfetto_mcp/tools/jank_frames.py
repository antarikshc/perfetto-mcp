"""Jank frames detection tool using Perfetto frame timeline data."""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

from .base import BaseTool, ToolError
from ..utils.query_helpers import format_query_result_row

logger = logging.getLogger(__name__)


class JankFramesTool(BaseTool):
    """Tool for detecting janky frames with classification and context.

    Joins Android frame timeline and per-frame metrics to provide severity and
    source classification (Application vs SurfaceFlinger), along with CPU/UI time.
    """

    def detect_jank_frames(
        self,
        trace_path: str,
        process_name: str,
        jank_threshold_ms: float = 16.67,
        severity_filter: Optional[List[str]] = None,
    ) -> str:
        """Detect janky frames for a given process.

        Parameters
        ----------
        trace_path : str
            Path to the Perfetto trace file.
        process_name : str
            Target process name (exact match as per query spec).
        jank_threshold_ms : float, optional
            Frame duration threshold in ms to consider a frame janky. Defaults to 16.67ms.
        severity_filter : list[str] | None, optional
            Optional list of jank severity types to include (e.g. ["severe", "moderate"]).

        Returns
        -------
        str
            JSON envelope with fields: processName, tracePath, success, error, result.
            Result shape:
              {
                totalCount: number,
                frames: [
                  {
                    frame_id, timestamp_ms, duration_ms, overrun_ms,
                    jank_type, jank_severity_type, jank_source,
                    cpu_time_ms, ui_time_ms, layer_name, jank_classification
                  }
                ],
                filters: { process_name, jank_threshold_ms, severity_filter }
              }
        """

        def _op(tp):
            if not process_name or not isinstance(process_name, str):
                raise ToolError("INVALID_PARAMETERS", "process_name must be a non-empty string")

            # Basic input hardening: escape single quotes
            safe_proc = process_name.replace("'", "''")

            # Build severity filter clause if provided
            severity_clause = ""
            if severity_filter:
                try:
                    # Escape values and build an IN (...) list
                    safe_vals = ["'%s'" % str(v).replace("'", "''") for v in severity_filter]
                    severity_clause = f" AND atl.jank_severity_type IN ({', '.join(safe_vals)})"
                except Exception:
                    raise ToolError("INVALID_PARAMETERS", "severity_filter must be a list of strings")

            sql_query = f"""
            INCLUDE PERFETTO MODULE android.frames.timeline;
            INCLUDE PERFETTO MODULE android.frames.per_frame_metrics;

            WITH frame_analysis AS (
              SELECT 
                af.frame_id,
                af.ts,
                af.dur,
                af.process_name,
                afo.overrun,
                atl.jank_type,
                atl.jank_severity_type,
                atl.present_type,
                atl.layer_name,
                afs.cpu_time,
                afs.ui_time,
                afs.was_jank,
                afs.was_big_jank,
                afs.was_huge_jank,
                CASE 
                  WHEN android_is_sf_jank_type(atl.jank_type) THEN 'SurfaceFlinger'
                  WHEN android_is_app_jank_type(atl.jank_type) THEN 'Application'
                  ELSE 'Unknown'
                END as jank_source
              FROM android_frames af
              LEFT JOIN android_frames_overrun afo USING(frame_id)
              LEFT JOIN actual_frame_timeline_slice atl 
                ON af.ts = atl.ts AND af.process_name = atl.process_name
              LEFT JOIN android_frame_stats afs USING(frame_id)
              WHERE af.process_name = '{safe_proc}'
                AND af.dur > ({float(jank_threshold_ms)} * 1e6)
                {severity_clause}
            )
            SELECT 
              frame_id,
              CAST(ts / 1e6 AS INT) as timestamp_ms,
              CAST(dur / 1e6 AS REAL) as duration_ms,
              CAST(overrun / 1e6 AS REAL) as overrun_ms,
              jank_type,
              jank_severity_type,
              jank_source,
              CAST(cpu_time / 1e6 AS REAL) as cpu_time_ms,
              CAST(ui_time / 1e6 AS REAL) as ui_time_ms,
              layer_name,
              CASE
                WHEN was_huge_jank THEN 'HUGE_JANK'
                WHEN was_big_jank THEN 'BIG_JANK'
                WHEN was_jank THEN 'JANK'
                ELSE 'SMOOTH'
              END as jank_classification
            FROM frame_analysis
            ORDER BY dur DESC;
            """

            def _collect(qr_it):
                frames_local: List[Dict[str, Any]] = []
                cols = None
                for row in qr_it:
                    if cols is None:
                        cols = list(row.__dict__.keys())
                    frames_local.append(format_query_result_row(row, cols))
                return frames_local

            frames: List[Dict[str, Any]] = []

            try:
                qr_it = tp.query(sql_query)
                frames = _collect(qr_it)
            except Exception as primary_err:
                # Attempt a fallback using raw frame timeline tables (without per_frame_metrics)
                logger.info(
                    "Primary jank query failed; attempting fallback using raw frame timeline tables: %s",
                    primary_err,
                )
                fallback_severity_clause = ""
                if severity_filter:
                    safe_vals = ["'%s'" % str(v).replace("'", "''") for v in severity_filter]
                    fallback_severity_clause = (
                        f" AND a.jank_severity_type IN ({', '.join(safe_vals)})"
                    )

                fallback_sql = f"""
                -- Fallback: rely only on actual/expected_frame_timeline_slice and process
                WITH frames AS (
                  SELECT 
                    a.ts,
                    a.dur,
                    a.upid,
                    a.layer_name,
                    a.present_type,
                    a.jank_type,
                    a.jank_severity_type,
                    a.display_frame_token,
                    a.surface_frame_token
                  FROM actual_frame_timeline_slice a
                  JOIN process p ON a.upid = p.upid
                  WHERE p.name = '{safe_proc}'
                    AND a.dur > ({float(jank_threshold_ms)} * 1e6)
                    {fallback_severity_clause}
                ),
                overrun_calc AS (
                  SELECT
                    f.*,
                    (f.ts + f.dur) AS actual_end,
                    (
                      SELECT e.ts + e.dur
                      FROM expected_frame_timeline_slice e
                      WHERE (
                        (f.surface_frame_token IS NOT NULL AND e.surface_frame_token = f.surface_frame_token)
                        OR (f.surface_frame_token IS NULL AND e.display_frame_token = f.display_frame_token)
                      )
                      AND e.upid = f.upid
                      LIMIT 1
                    ) AS expected_end
                  FROM frames f
                )
                SELECT 
                  COALESCE(surface_frame_token, display_frame_token) AS frame_id,
                  CAST(ts / 1e6 AS INT) AS timestamp_ms,
                  CAST(dur / 1e6 AS REAL) AS duration_ms,
                  CAST((actual_end - expected_end) / 1e6 AS REAL) AS overrun_ms,
                  jank_type,
                  jank_severity_type,
                  CASE
                    WHEN jank_type LIKE 'SurfaceFlinger%' THEN 'SurfaceFlinger'
                    WHEN jank_type LIKE 'App%' THEN 'Application'
                    ELSE 'Unknown'
                  END AS jank_source,
                  NULL AS cpu_time_ms,
                  NULL AS ui_time_ms,
                  layer_name,
                  CASE
                    WHEN dur > 200e6 THEN 'HUGE_JANK'
                    WHEN dur > 50e6 THEN 'BIG_JANK'
                    WHEN dur > ({float(jank_threshold_ms)} * 1e6) THEN 'JANK'
                    ELSE 'SMOOTH'
                  END AS jank_classification
                FROM overrun_calc
                ORDER BY dur DESC;
                """

                try:
                    qr_it_fb = tp.query(fallback_sql)
                    frames = _collect(qr_it_fb)
                except Exception as fb_err:
                    # If fallback also fails, return a structured error with hints
                    msg1 = str(primary_err)
                    msg2 = str(fb_err)
                    raise ToolError(
                        "FRAME_DATA_UNAVAILABLE",
                        "This trace does not contain frame timeline/metrics data required for jank analysis.",
                        details=f"primary_error={msg1}; fallback_error={msg2}",
                    )

            return {
                "totalCount": len(frames),
                "frames": frames,
                "filters": {
                    "process_name": process_name,
                    "jank_threshold_ms": jank_threshold_ms,
                    "severity_filter": severity_filter,
                },
            }

        return self.run_formatted(trace_path, process_name, _op)
