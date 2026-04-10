"""ANR Root Cause Analyzer tool.

Correlates multiple signals around an ANR time window to surface likely causes:
- Main-thread blocking (thread_state)
- Slow Binder transactions (android.binder)
- Memory pressure (MemAvailable at start/end of window)
- Java monitor contention (android.monitor_contention)

Returns a unified JSON envelope via BaseTool.run_formatted().
"""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional, Tuple

from .base import BaseTool, ToolError

logger = logging.getLogger(__name__)


class AnrRootCauseTool(BaseTool):
    """Tool for analyzing likely ANR root causes within a window."""

    def anr_root_cause_analyzer(
        self,
        trace_path: str,
        process_name: Optional[str] = None,
        anr_timestamp_ms: Optional[int] = None,
        analysis_window_ms: int = 10_000,
        time_range: Optional[Dict[str, int]] = None,
        deep_analysis: bool = False,
    ) -> str:
        """Analyze likely ANR root causes for a process and time window.

        Parameters
        ----------
        trace_path : str
            Path to Perfetto trace file.
        process_name : str | None
            Process filter (supports GLOB). If None, analyzes without process filter where possible.
        anr_timestamp_ms : int | None
            Anchor timestamp in milliseconds from trace start.
        analysis_window_ms : int
            Half-window size around anchor (default 10,000 ms). Used if `time_range` not provided.
        time_range : dict | None
            Explicit time range: { 'start_ms': int, 'end_ms': int }.
        deep_analysis : bool
            If True, strengthens heuristics and may compute additional notes.

        Returns
        -------
        str
            JSON envelope with fields: processName, tracePath, success, error, result
            result shape:
              {
                window: { startMs, endMs },
                filters: { process_name },
                mainThreadBlocks: [...],
                binderDelays: [...],
                memoryPressure: { start: {tsMs, availableMemoryMb}, end: {...}, deltaMb },
                lockContention: [...],
                insights: { likelyCauses: [...], rationale: [...], signalsUsed: [...] },
                notes: [...]
              }
        """

        def _op(tp):
            # Resolve time window
            start_ms, end_ms = self._resolve_window(anr_timestamp_ms, analysis_window_ms, time_range)

            # If both provided, enforce containment rule
            if time_range and anr_timestamp_ms is not None:
                if not (start_ms <= int(anr_timestamp_ms) <= end_ms):
                    raise ToolError(
                        "INVALID_PARAMETERS",
                        "anr_timestamp_ms is outside the provided time_range. Please adjust or remove one.",
                    )

            start_ns, end_ns = int(start_ms * 1e6), int(end_ms * 1e6)

            notes: List[str] = []

            # Queries
            main_thread_blocks = self._query_main_thread_blocks(tp, process_name, start_ns, end_ns, notes)
            binder_delays = self._query_binder_delays(tp, process_name, start_ns, end_ns, notes)
            mem_start = self._query_mem_available(tp, ts_ns_bound=start_ns, notes=notes)
            mem_end = self._query_mem_available(tp, ts_ns_bound=end_ns, notes=notes)
            lock_contention = self._query_monitor_contention(tp, process_name, start_ns, end_ns, notes)

            memory_pressure = self._format_memory_pressure(mem_start, mem_end)

            insights = self._build_insights(
                main_thread_blocks, binder_delays, lock_contention, memory_pressure, deep_analysis
            )

            return {
                "window": {"startMs": start_ms, "endMs": end_ms},
                "filters": {"process_name": process_name},
                "mainThreadBlocks": main_thread_blocks,
                "binderDelays": binder_delays,
                "memoryPressure": memory_pressure,
                "lockContention": lock_contention,
                "insights": insights,
                "notes": notes,
            }

        return self.run_formatted(trace_path, process_name, _op)

    # -------------------------------
    # Window resolution and utilities
    # -------------------------------
    def _resolve_window(
        self,
        anr_timestamp_ms: Optional[int],
        analysis_window_ms: int,
        time_range: Optional[Dict[str, int]],
    ) -> Tuple[int, int]:
        if time_range is not None:
            try:
                start_ms = int(time_range.get("start_ms"))
                end_ms = int(time_range.get("end_ms"))
            except Exception:
                raise ToolError(
                    "INVALID_PARAMETERS",
                    "time_range must include integer start_ms and end_ms",
                )
            if start_ms < 0:
                start_ms = 0
            if end_ms < 0:
                raise ToolError("INVALID_PARAMETERS", "end_ms cannot be negative")
            if start_ms > end_ms:
                raise ToolError("INVALID_PARAMETERS", "start_ms must be <= end_ms")
            return start_ms, end_ms

        # Fallback: use anchor ± interval
        if anr_timestamp_ms is None:
            raise ToolError(
                "INVALID_PARAMETERS",
                "Provide either time_range or anr_timestamp_ms (with optional analysis_window_ms)",
            )
        try:
            anchor = int(anr_timestamp_ms)
            half = int(analysis_window_ms) if analysis_window_ms is not None else 10_000
        except Exception:
            raise ToolError("INVALID_PARAMETERS", "Invalid anr_timestamp_ms or analysis_window_ms")

        start_ms = max(0, anchor - half)
        end_ms = anchor + half
        return start_ms, end_ms

    @staticmethod
    def _ns_to_ms(value_ns: Optional[int | float]) -> Optional[float]:
        if value_ns is None:
            return None
        try:
            return float(value_ns) / 1e6
        except Exception:
            return None

    # -------------------------------
    # Section queries
    # -------------------------------
    def _query_main_thread_blocks(
        self,
        tp,
        process_name: Optional[str],
        start_ns: int,
        end_ns: int,
        notes: List[str],
    ) -> List[Dict[str, Any]]:
        safe_proc = process_name.replace("'", "''") if process_name else ""
        proc_filter = f"AND p.name GLOB '{safe_proc}'" if process_name else ""
        sql = f"""
        SELECT
          ts.ts AS ts,
          ts.dur AS dur,
          ts.state AS state,
          ts.io_wait AS io_wait,
          ts.waker_utid AS waker_utid,
          wt.name AS waker_thread_name,
          wp.name AS waker_process_name
        FROM thread_state ts
        JOIN thread t USING(utid)
        JOIN process p USING(upid)
        LEFT JOIN thread wt ON ts.waker_utid = wt.utid
        LEFT JOIN process wp ON wt.upid = wp.upid
        WHERE t.is_main_thread = 1
          {proc_filter}
          AND ts.ts BETWEEN {start_ns} AND {end_ns}
          AND ts.state != 'Running'
        ORDER BY ts.dur DESC
        LIMIT 20;
        """
        try:
            rows = list(tp.query(sql))
        except Exception as e:
            logger.warning(f"main_thread_blocks query failed: {e}")
            notes.append(f"mainThreadBlocks unavailable: {e}")
            return []

        results: List[Dict[str, Any]] = []
        for r in rows:
            results.append(
                {
                    "tsMs": int(self._ns_to_ms(getattr(r, "ts", None)) or 0),
                    "durMs": float(self._ns_to_ms(getattr(r, "dur", None)) or 0.0),
                    "state": getattr(r, "state", None),
                    "ioWait": getattr(r, "io_wait", None),
                    "wakerUtid": getattr(r, "waker_utid", None),
                    "wakerThreadName": getattr(r, "waker_thread_name", None),
                    "wakerProcessName": getattr(r, "waker_process_name", None),
                }
            )
        return results

    def _query_binder_delays(
        self,
        tp,
        process_name: Optional[str],
        start_ns: int,
        end_ns: int,
        notes: List[str],
    ) -> List[Dict[str, Any]]:
        safe_proc2 = process_name.replace("'", "''") if process_name else ""
        proc_filter = (
            f"AND client_process GLOB '{safe_proc2}'" if process_name else ""
        )
        sql = f"""
        INCLUDE PERFETTO MODULE android.binder;
        SELECT 
          binder_txn_id,
          client_ts AS ts,
          client_dur AS dur,
          server_process,
          aidl_name,
          method_name,
          is_main_thread
        FROM android_binder_txns
        WHERE 1=1
          {proc_filter}
          AND client_ts BETWEEN {start_ns} AND {end_ns}
          AND client_dur > 100e6
        ORDER BY client_dur DESC
        LIMIT 30;
        """
        try:
            rows = list(tp.query(sql))
        except Exception as e:
            msg = str(e)
            if "android_binder_txns" in msg or "android.binder" in msg or "no such" in msg.lower():
                notes.append("binderDelays unavailable (module not present in trace)")
                return []
            logger.warning(f"binder_delays query failed: {e}")
            notes.append(f"binderDelays error: {e}")
            return []

        results: List[Dict[str, Any]] = []
        for r in rows:
            results.append(
                {
                    "binderTxnId": getattr(r, "binder_txn_id", None),
                    "clientTsMs": int(self._ns_to_ms(getattr(r, "ts", None)) or 0),
                    "clientDurMs": float(self._ns_to_ms(getattr(r, "dur", None)) or 0.0),
                    "serverProcess": getattr(r, "server_process", None),
                    "aidlName": getattr(r, "aidl_name", None),
                    "methodName": getattr(r, "method_name", None),
                    "clientMainThread": getattr(r, "is_main_thread", None),
                }
            )
        return results

    def _query_mem_available(
        self,
        tp,
        ts_ns_bound: int,
        notes: List[str],
    ) -> Optional[Dict[str, Any]]:
        sql = f"""
        SELECT c.ts AS ts,
               c.value / 1024.0 / 1024.0 AS available_memory_mb
        FROM counter c
        JOIN counter_track ct ON c.track_id = ct.id
        WHERE (ct.name LIKE '%MemAvailable%' OR ct.name LIKE 'Mem.available%')
          AND c.ts <= {ts_ns_bound}
        ORDER BY c.ts DESC
        LIMIT 1;
        """
        try:
            rows = list(tp.query(sql))
        except Exception as e:
            logger.warning(f"memory query failed: {e}")
            notes.append(f"memoryPressure unavailable: {e}")
            return None

        if not rows:
            notes.append("memoryPressure: no MemAvailable samples found")
            return None

        r = rows[0]
        return {
            "tsMs": int(self._ns_to_ms(getattr(r, "ts", None)) or 0),
            "availableMemoryMb": float(getattr(r, "available_memory_mb", None) or 0.0),
        }

    def _query_monitor_contention(
        self,
        tp,
        process_name: Optional[str],
        start_ns: int,
        end_ns: int,
        notes: List[str],
    ) -> List[Dict[str, Any]]:
        if not process_name:
            # android_monitor_contention requires a target upid; without process filter, skip
            notes.append("lockContention skipped (no process_name provided)")
            return []

        safe_proc = process_name.replace("'", "''")
        sql = f"""
        INCLUDE PERFETTO MODULE android.monitor_contention;
        SELECT blocked_thread_name,
               blocking_thread_name,
               blocked_method_name,
               short_blocking_method_name,
               blocking_src,
               waiter_count,
               blocked_thread_waiter_count,
               dur,
               ts
        FROM android_monitor_contention
        WHERE upid = (SELECT upid FROM process WHERE name GLOB '{safe_proc}' LIMIT 1)
          AND ts BETWEEN {start_ns} AND {end_ns}
          AND is_blocked_thread_main = 1
        ORDER BY dur DESC
        LIMIT 20;
        """
        try:
            rows = list(tp.query(sql))
        except Exception as e:
            msg = str(e)
            if "android_monitor_contention" in msg or "android.monitor_contention" in msg or "no such" in msg.lower():
                notes.append("lockContention unavailable (module not present in trace)")
                return []
            logger.warning(f"monitor_contention query failed: {e}")
            notes.append(f"lockContention error: {e}")
            return []

        results: List[Dict[str, Any]] = []
        for r in rows:
            results.append(
                {
                    "blockedThreadName": getattr(r, "blocked_thread_name", None),
                    "blockingThreadName": getattr(r, "blocking_thread_name", None),
                    "blockedMethod": getattr(r, "blocked_method_name", None),
                    "shortBlockingMethod": getattr(r, "short_blocking_method_name", None),
                    "blockingSrc": getattr(r, "blocking_src", None),
                    "waiterCount": getattr(r, "waiter_count", None),
                    "blockedThreadWaiterCount": getattr(r, "blocked_thread_waiter_count", None),
                    "durMs": float(self._ns_to_ms(getattr(r, "dur", None)) or 0.0),
                    "tsMs": int(self._ns_to_ms(getattr(r, "ts", None)) or 0),
                }
            )
        return results

    @staticmethod
    def _format_memory_pressure(
        mem_start: Optional[Dict[str, Any]], mem_end: Optional[Dict[str, Any]]
    ) -> Dict[str, Any]:
        start_val = mem_start.get("availableMemoryMb") if mem_start else None
        end_val = mem_end.get("availableMemoryMb") if mem_end else None
        delta = None
        if start_val is not None and end_val is not None:
            try:
                delta = float(end_val) - float(start_val)
            except Exception:
                delta = None
        return {
            "start": mem_start,
            "end": mem_end,
            "deltaMb": delta,
        }

    # -------------------------------
    # Insights heuristic
    # -------------------------------
    def _build_insights(
        self,
        main_thread_blocks: List[Dict[str, Any]],
        binder_delays: List[Dict[str, Any]],
        lock_contention: List[Dict[str, Any]],
        memory_pressure: Dict[str, Any],
        deep_analysis: bool,
    ) -> Dict[str, Any]:
        likely: List[str] = []
        rationale: List[str] = []
        signals_used = ["mainThreadBlocks", "binderDelays", "lockContention", "memoryPressure"]

        # Binder delay heuristic
        if binder_delays:
            long_binders = [b for b in binder_delays if (b.get("clientDurMs") or 0) >= 500]
            avg_top = None
            try:
                top = binder_delays[:5]
                if top:
                    avg_top = sum((b.get("clientDurMs") or 0.0) for b in top) / len(top)
            except Exception:
                avg_top = None
            if long_binders or (avg_top is not None and avg_top >= 250):
                likely.append("binder_delay")
                if long_binders:
                    rationale.append(
                        f"{len(long_binders)} slow binder txn(s) >= 500ms; top avg ~{avg_top:.0f}ms"
                        if avg_top is not None
                        else f"{len(long_binders)} slow binder txn(s) >= 500ms"
                    )

        # IO wait / sleep on main thread
        if main_thread_blocks:
            io_blocks = [m for m in main_thread_blocks if (m.get("ioWait") == 1) or (m.get("state") in ("D", "Uninterruptible"))]
            long_blocks = [m for m in main_thread_blocks if (m.get("durMs") or 0) >= 500]
            if io_blocks and long_blocks:
                likely.append("io_wait")
                rationale.append(
                    f"Main thread IO/sleep state with long block (>=500ms), count={len(long_blocks)}"
                )

        # Lock contention
        if lock_contention:
            long_locks = [l for l in lock_contention if (l.get("durMs") or 0) >= 300]
            if long_locks:
                likely.append("lock_contention")
                rationale.append(f"Java monitor contention on main thread (>=300ms), count={len(long_locks)}")

        # Memory pressure
        end_mem = memory_pressure.get("end") or {}
        delta = memory_pressure.get("deltaMb")
        end_mb = end_mem.get("availableMemoryMb") if end_mem else None
        try:
            low_mem = end_mb is not None and float(end_mb) < 200
        except Exception:
            low_mem = False
        try:
            drop_mem = delta is not None and float(delta) <= -200.0
        except Exception:
            drop_mem = False
        if low_mem or drop_mem:
            likely.append("memory_pressure")
            if low_mem:
                rationale.append(f"Low MemAvailable at window end (~{end_mb:.0f} MB)")
            if drop_mem:
                rationale.append(f"MemAvailable dropped by {delta:.0f} MB over window")

        # CPU starvation (fallback): running state dominates and no other signals
        if not likely and main_thread_blocks:
            # We filtered out 'Running' states, so if there's nothing else, skip.
            pass

        # Strengthen heuristics if requested
        if deep_analysis:
            # Example: prioritize binder if both binder and lock present but binder delays are huge
            max_binder = max((b.get("clientDurMs") or 0) for b in binder_delays) if binder_delays else 0
            max_lock = max((l.get("durMs") or 0) for l in lock_contention) if lock_contention else 0
            if "binder_delay" in likely and "lock_contention" in likely and max_binder >= 2 * max_lock:
                rationale.append("Binder delays significantly exceed lock contention durations")

        return {
            "likelyCauses": likely,
            "rationale": rationale,
            "signalsUsed": signals_used,
        }
