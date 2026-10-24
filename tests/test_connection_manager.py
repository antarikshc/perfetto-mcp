"""Concurrency tests use only an in-memory fake TraceProcessor."""

from __future__ import annotations

import threading
import unittest
from typing import Any
from unittest.mock import patch

from perfetto_mcp.connection_manager import ConnectionManager
from perfetto_mcp.tools.base import BaseTool


class FakeTraceProcessor:
    """Minimal processor double; it never opens a trace or starts a service."""

    def __init__(self, trace: str, **_kwargs: Any) -> None:
        self.trace = trace
        self.closed = False

    def close(self) -> None:
        self.closed = True

    def read(self) -> str:
        if self.closed:
            raise ValueError("stale processor handle")
        return self.trace


class ConnectionManagerConcurrencyTests(unittest.TestCase):
    """A shared processor must not be replaced while an operation uses it."""

    def test_different_trace_requests_do_not_close_in_flight_processor(self) -> None:
        manager = ConnectionManager()
        tool = BaseTool(manager)
        first_operation_entered = threading.Event()
        release_first_operation = threading.Event()
        second_call_started = threading.Event()
        second_operation_entered = threading.Event()
        results: dict[str, str] = {}
        errors: list[BaseException] = []

        def create_processor(trace: str, **kwargs: Any) -> FakeTraceProcessor:
            return FakeTraceProcessor(trace, **kwargs)

        def first_operation(processor: FakeTraceProcessor) -> str:
            first_operation_entered.set()
            if not release_first_operation.wait(timeout=3):
                raise TimeoutError("synthetic test gate timed out")
            return processor.read()

        def second_operation(processor: FakeTraceProcessor) -> str:
            second_operation_entered.set()
            return processor.read()

        def run_operation(key: str, trace: str, operation: Any) -> None:
            if key == "second":
                second_call_started.set()
            try:
                results[key] = tool.execute_with_connection(trace, operation)
            except (ConnectionError, TimeoutError, ValueError) as error:
                errors.append(error)

        first = threading.Thread(
            target=run_operation,
            args=("first", "synthetic-trace-a", first_operation),
        )
        second = threading.Thread(
            target=run_operation,
            args=("second", "synthetic-trace-b", second_operation),
        )

        with patch(
            "perfetto_mcp.connection_manager.TraceProcessor",
            side_effect=create_processor,
        ):
            first.start()
            try:
                self.assertTrue(first_operation_entered.wait(timeout=3))
                second.start()
                self.assertTrue(second_call_started.wait(timeout=3))
                second_entered_early = second_operation_entered.wait(timeout=0.2)
            finally:
                release_first_operation.set()
                first.join(timeout=3)
                if second.ident is not None:
                    second.join(timeout=3)
                manager.cleanup()

        self.assertFalse(first.is_alive(), "first operation did not finish")
        self.assertFalse(second.is_alive(), "second operation did not finish")
        self.assertFalse(second_entered_early, "second trace replaced an in-flight processor")
        self.assertEqual(errors, [])
        self.assertEqual(
            results,
            {"first": "synthetic-trace-a", "second": "synthetic-trace-b"},
        )


if __name__ == "__main__":
    unittest.main()
