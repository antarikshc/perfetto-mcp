[![MseeP.ai Security Assessment Badge](https://mseep.net/pr/antarikshc-perfetto-mcp-badge.png)](https://mseep.ai/app/antarikshc-perfetto-mcp)

# Perfetto MCP

This is a Model Context Protocol (MCP) server that gets answers from your Perfetto Traces. It turns natural‑language prompts into focused Perfetto analyses so you can quickly explain jank, diagnose ANRs, spot CPU hot threads, uncover lock contention, and find memory leaks — without writing SQL. Point it at a trace and process, ask a question, and receive concise, actionable summaries with structured results you can drill into.

## Prerequisites

- Python 3.13+ installed on your system (python.org, Homebrew, or your distro package manager)

## Getting Started

Installation instructions coming soon.

## How to use?

Most tools require:
- **trace_path**: Absolute path to your Perfetto trace file (e.g., `.pftrace`, `.perfetto-trace`).
- **process_name**: Target process/app name (exact or wildcard where supported, e.g., `com.example.app`).

In your prompt, mention the trace and process explicitly so the assistant can route calls correctly. For example:
- “Use trace `/absolute/path/to/trace.pftrace` for process `com.example.app`.”

Optional filters supported by many tools:
- **time_range**: Limit analysis to a window, e.g., `{start_ms: 10000, end_ms: 25000}`.
- Tool‑specific thresholds (e.g., `min_block_ms`, `jank_threshold_ms`, `limit`).

## Tools

Below are the available tools, how they help, and what to expect in the result. Each tool returns a concise JSON envelope with a tool‑specific `result` payload.

### find_slices
- **What it does**: Scans slice names using contains/exact/glob matching, filters by process, main thread, and time window, then surfaces per‑name duration stats and the heaviest example slices with track/thread context. Great for quickly mapping an unfamiliar trace and pinpointing hot paths.
- **Useful for**: Exploring unfamiliar traces, locating hot paths, scoping by process/main thread/time window.
- **Examples**: "On `/abs/trace.pftrace` for `com.example.app`, survey slice names containing 'Choreographer' and show top examples." "Using `/abs/trace.pftrace` for `com.example.app`, list main‑thread hotspots matching glob `input*` within 10s–25s."
- **Output**: Aggregates with count/duration stats and example slices (ids, timestamps, durations, track context), plus notes on any fallbacks.

### execute_sql_query
- **What it does**: Executes multi‑statement PerfettoSQL scripts verbatim against the trace database, including optional standard‑library modules. Ideal when you need bespoke metrics, custom joins, or correlations beyond prebuilt tools.
- **Useful for**: Custom analyses that go beyond the prebuilt tools.
- **Examples**: "Run a custom PerfettoSQL analysis on `/abs/trace.pftrace` for `com.example.app` to correlate threads and frames in the first 30s." "On `/abs/trace.pftrace` for `com.example.app`, execute a custom query to compare runtime across worker threads."
- **Output**: Columns, rows, rowCount, and basic metadata about the script execution.

### detect_anrs
- **What it does**: Detects Application Not Responding (ANR) events and augments each with timing, process context, last‑known main‑thread state, and GC pressure near the event. Applies a simple severity heuristic so you can triage quickly.
- **Useful for**: Investigating app freezes and unresponsive periods.
- **Examples**: "On `/abs/trace.pftrace`, detect ANRs for `com.example.app` in the first 60s and summarize severity." "Check if there's an ANR around 20s for `com.example.app` on `/abs/trace.pftrace` and show main‑thread state."
- **Output**: ANR list with timestamps, process info, main‑thread state, GC pressure indicators, and severity classification.

### anr_root_cause_analyzer
- **What it does**: Correlates main‑thread blocking states, slow Binder transactions, memory pressure, and Java monitor contention within a focused window around an ANR or timestamp. Ranks likely causes with clear rationale and highlights missing data when applicable.
- **Useful for**: Pinpointing likely ANR causes and prioritizing fixes.
- **Examples**: "For `/abs/trace.pftrace` and `com.example.app`, analyze root cause around 20,000 ms and rank likely causes." "On `/abs/trace.pftrace`, deep‑analyze the ANR at 35s for `com.example.app` and correlate locks, GC, and Binder."
- **Output**: Ranked likely causes with rationale, plus detailed per‑signal findings and notes on data availability.

### cpu_utilization_profiler
- **What it does**: Aggregates per‑thread runtime from scheduler data to compute CPU% of the selected window, scheduling counts, and typical vs worst run‑slice lengths. Flags main‑thread overload and summarizes CPU frequency/boost behavior when available.
- **Useful for**: Identifying CPU‑bound hotspots, thread thrashing, and main‑thread overload.
- **Examples**: "Profile CPU usage by thread for `com.example.app` on `/abs/trace.pftrace` and flag the hottest threads." "On `/abs/trace.pftrace`, show per‑thread CPU% for `com.example.app` during 10s–40s."
- **Output**: Per‑thread CPU %, runtime totals, scheduling counts, average/max slice times, and DVFS insights when available.

### detect_jank_frames
- **What it does**: Uses frame‑timeline data to identify frames that miss their deadline, classifies severity, and attributes the source (application vs SurfaceFlinger). Includes per‑frame CPU/UI time when present and supports threshold/severity filtering.
- **Useful for**: Diagnosing UI stutters and prioritizing the worst frames.
- **Examples**: "Find janky frames for `com.example.app` in `/abs/trace.pftrace` above 16.67 ms and list the worst 20." "Focus on severe jank frames near 22s for `com.example.app` on `/abs/trace.pftrace` and classify the source."
- **Output**: Frame rows with timestamps, durations, deadline overrun, jank type/severity/source, CPU/UI time (when available), and classification.

### frame_performance_summary
- **What it does**: Summarizes overall frame health with jank rate and category counts, plus CPU‑time distribution (avg/max/P95/P99). Delivers a simple rating so you can baseline and compare builds or scenarios.
- **Useful for**: Establishing baselines and comparing before/after optimizations.
- **Examples**: "Summarize frame performance for `com.example.app` in `/abs/trace.pftrace` and report jank rate and P99 CPU time."
- **Output**: Totals, jank rate, frame category counts (slow/jank/big/huge), CPU time distribution (avg/max/P95/P99), and a performance rating.

### memory_leak_detector
- **What it does**: Analyzes process RSS over time for sustained growth patterns and, when heap data exists, aggregates classes dominating retained memory. Correlates growth with suspects to prioritize likely leaks quickly.
- **Useful for**: Investigating OOMs and gradual performance degradation over long sessions.
- **Examples**: "Detect memory‑leak signals for `com.example.app` on `/abs/trace.pftrace` over the last 60s."
- **Output**: Growth metrics (avg/max, leak indicators) and suspicious classes with sizes/instance counts; notes when data is partial.

### heap_dominator_tree_analyzer
- **What it does**: Builds a dominator‑style view of the latest heap snapshot to surface classes retaining the most memory, breaking down Java vs native where available and estimating reachability characteristics.
- **Useful for**: Finding memory‑hogging types, native allocations, and GC root proximity.
- **Examples**: "From `/abs/trace.pftrace`, analyze heap dominator classes for `com.example.app` and list the top offenders."
- **Output**: Top classes with instance counts and memory breakdown (self/native/total), reachability indicators when available, and impact tiers.

### thread_contention_analyzer
- **What it does**: Detects synchronization bottlenecks by analyzing Java monitor contention (primary) or inferring blocking from scheduler states with waker linkage (fallback). Groups by blocked/holding threads, quantifies duration/occurrence, and can include per‑thread blocked‑state breakdown and concrete examples.
- **Useful for**: Explaining freezes with low CPU usage, diagnosing ANRs due to locks.
- **Examples**: "On `/abs/trace.pftrace`, find lock contention affecting `com.example.app` between 15s–30s and show the worst waits." "Explain if the main thread of `com.example.app` is blocked by another thread around 20s on `/abs/trace.pftrace`."
- **Output**: Contentions grouped by blocked/holding threads with duration stats, severity, optional per‑thread blocked‑state breakdown, and example waits.

### binder_transaction_profiler
- **What it does**: Profiles Binder IPC performance by decomposing client‑side latency, server processing time, and overhead, identifying main‑thread synchronous calls and slow system interactions. Supports grouping by AIDL or server process.
- **Useful for**: Uncovering slow system service interactions that contribute to ANRs or jank.
- **Examples**: "Profile slow Binder transactions involving `com.example.app` on `/abs/trace.pftrace` and group by server process." "On `/abs/trace.pftrace`, list main‑thread synchronous Binder calls >100 ms for `com.example.app`."
- **Output**: Transaction rows or aggregates with client/server latencies, overhead, main‑thread impact, and top thread states.

### main_thread_hotspot_slices
- **What it does**: Enumerates the longest‑running main‑thread slices for the target process using the explicit main‑thread flag when available or a safe heuristic otherwise. Highlights heavy callbacks and phases for rapid ANR/jank triage.
- **Useful for**: Fast ANR/jank triage to reveal heavy callbacks and phases.
- **Examples**: "List the longest‑running main‑thread slices for `com.example.app` using `/abs/trace.pftrace` and include timestamps." "On `/abs/trace.pftrace`, show main‑thread hotspots >50 ms for `com.example.app` during 10s–25s."
- **Output**: Hotspots with ids, timestamps, durations, thread/track context, plus summary and notes on any heuristics used.

## Resource

- Perfetto Trace Processor: [Trace Processor Python API](https://perfetto.dev/docs/analysis/trace-processor-python)
- Perfetto SQL: [Perfetto SQL syntax](https://perfetto.dev/docs/analysis/perfetto-sql-syntax)

## License

This project is licensed under the Apache 2.0 License. See the [LICENSE](https://github.com/antarikshc/perfetto-mcp/blob/main/LICENSE) file for details.


