"""Base tool class for all Perfetto MCP tools."""

import json
import logging
from typing import Callable, Any, Dict, Optional
from ..connection_manager import ConnectionManager

logger = logging.getLogger(__name__)


class ToolError(Exception):
    """Custom exception carrying a structured error code and message."""

    def __init__(self, code: str, message: str, details: Optional[str] = None):
        super().__init__(message)
        self.code = code
        self.message = message
        self.details = details


class BaseTool:
    """Base class for all Perfetto tools with connection management and formatting."""

    def __init__(self, connection_manager: ConnectionManager):
        """Initialize the tool with a connection manager.

        Args:
            connection_manager: Shared connection manager instance
        """
        self.connection_manager = connection_manager

    def execute_with_connection(self, trace_path: str, operation: Callable) -> Any:
        """Execute operation with managed connection and auto-reconnection.

        Args:
            trace_path: Path to the trace file
            operation: Function that takes a TraceProcessor and returns a result

        Returns:
            Any: Result from the operation

        Raises:
            FileNotFoundError: If trace file doesn't exist
            ConnectionError: If connection fails
            Exception: Any other errors from the operation
        """
        try:
            return self.connection_manager.run_with_connection(trace_path, operation)
        except (ConnectionError, Exception) as e:
            # Check if this is a connection-related error that might benefit from reconnection
            if self._should_retry_on_error(e):
                logger.info(f"Attempting reconnection due to error: {e}")
                try:
                    return self.connection_manager.run_with_connection(
                        trace_path,
                        operation,
                        reconnect=True,
                    )
                except Exception as reconnect_error:
                    logger.error(f"Reconnection attempt failed: {reconnect_error}")
                    # Raise the original error if reconnection fails
                    raise e
            else:
                # Don't retry for errors like FileNotFoundError
                raise e

    def _should_retry_on_error(self, error: Exception) -> bool:
        """Determine if an error should trigger a reconnection attempt.

        Args:
            error: The exception that occurred

        Returns:
            bool: True if reconnection should be attempted
        """
        # Don't retry for file not found errors
        if isinstance(error, FileNotFoundError):
            return False

        # Retry for connection errors or other exceptions that might be connection-related
        if isinstance(error, ConnectionError):
            return True

        # Check if error message suggests connection issues
        error_str = str(error).lower()
        connection_indicators = [
            'connection', 'broken pipe', 'socket', 'network', 'timeout',
            'disconnected', 'closed', 'reset', 'refused'
        ]

        for indicator in connection_indicators:
            if indicator in error_str:
                return True

        return False

    # -------------------------
    # Unified response helpers
    # -------------------------
    def _make_envelope(
        self,
        *,
        trace_path: Optional[str],
        process_name: Optional[str],
        success: bool,
        result: Optional[Dict[str, Any]] = None,
        error: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """Create the standard response envelope."""
        return {
            "processName": process_name or "not-specified",
            "tracePath": trace_path,
            "success": success,
            "error": error,
            "result": result or {},
        }

    def _error(self, code: str, message: str, details: Optional[str] = None) -> Dict[str, Any]:
        """Create a standardized error object."""
        err: Dict[str, Any] = {"code": code, "message": message}
        if details:
            err["details"] = details
        return err

    def run_formatted(
        self,
        trace_path: str,
        process_name: Optional[str],
        op: Callable[[Any], Dict[str, Any]],  # (tp) -> Dict[str, Any] (result payload)
    ) -> str:
        """Run an operation with connection management and return a JSON envelope string."""
        try:
            def wrapped(tp):
                result = op(tp)
                return self._make_envelope(
                    trace_path=trace_path,
                    process_name=process_name,
                    success=True,
                    result=result,
                )

            envelope = self.execute_with_connection(trace_path, wrapped)
        except ToolError as te:
            envelope = self._make_envelope(
                trace_path=trace_path,
                process_name=process_name,
                success=False,
                error=self._error(te.code, te.message, te.details),
            )
        except FileNotFoundError as fnf:
            envelope = self._make_envelope(
                trace_path=trace_path,
                process_name=process_name,
                success=False,
                error=self._error("FILE_NOT_FOUND", "Trace file not found", str(fnf)),
            )
        except ConnectionError as ce:
            envelope = self._make_envelope(
                trace_path=trace_path,
                process_name=process_name,
                success=False,
                error=self._error("CONNECTION_FAILED", "Could not connect to trace processor", str(ce)),
            )
        except Exception as e:
            envelope = self._make_envelope(
                trace_path=trace_path,
                process_name=process_name,
                success=False,
                error=self._error("INTERNAL_ERROR", str(e)),
            )

        return json.dumps(envelope, indent=2)
