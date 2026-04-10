"""Connection manager for persistent TraceProcessor connections."""

import threading
import logging
from typing import Optional
from perfetto.trace_processor import TraceProcessor, TraceProcessorConfig

logger = logging.getLogger(__name__)


class ConnectionManager:
    """Manages persistent TraceProcessor connections with reconnection support."""
    
    def __init__(self):
        self._current_trace_path: Optional[str] = None
        self._current_connection: Optional[TraceProcessor] = None
        self._lock = threading.Lock()  # Thread safety
        
    def get_connection(self, trace_path: str) -> TraceProcessor:
        """Get or create connection for trace_path with automatic reconnection.
        
        Args:
            trace_path: Path to the Perfetto trace file
            
        Returns:
            TraceProcessor: Active connection to the trace
            
        Raises:
            FileNotFoundError: If trace file doesn't exist
            ConnectionError: If connection fails
        """
        with self._lock:
            # If different path, close existing and open new
            if self._current_trace_path != trace_path:
                logger.info(f"Switching trace connection from {self._current_trace_path} to {trace_path}")
                self._close_current_unsafe()
                self._current_trace_path = trace_path
                self._current_connection = self._create_connection(trace_path)
                
            # If same path but no connection, create new one
            elif self._current_connection is None:
                logger.info(f"Creating new connection to {trace_path}")
                self._current_connection = self._create_connection(trace_path)

            return self._current_connection
    
    def _create_connection(self, trace_path: str) -> TraceProcessor:
        """Create a new TraceProcessor connection.
        
        Args:
            trace_path: Path to the trace file
            
        Returns:
            TraceProcessor: New connection
            
        Raises:
            FileNotFoundError: If trace file doesn't exist
            ConnectionError: If connection fails
        """
        try:
            config = TraceProcessorConfig(load_timeout=30)
            tp = TraceProcessor(trace=trace_path, config=config)
            logger.info(f"Successfully connected to trace: {trace_path}")
            return tp
        except FileNotFoundError as e:
            logger.error(f"Trace file not found: {trace_path}")
            raise FileNotFoundError(
                f"Failed to open the trace file. Please double-check the trace_path "
                f"you supplied. Underlying error: {e}"
            )
        except Exception as e:
            logger.error(f"Failed to connect to trace: {trace_path}, error: {e}")
            raise ConnectionError(f"Could not connect to trace processor: {e}")
    
    def _reconnect(self, trace_path: str) -> TraceProcessor:
        """Reconnect to trace file after connection failure.
        
        Args:
            trace_path: Path to the trace file
            
        Returns:
            TraceProcessor: New connection
            
        Raises:
            ConnectionError: If reconnection fails
        """
        with self._lock:
            return self._reconnect_unsafe(trace_path)
    
    def _reconnect_unsafe(self, trace_path: str) -> TraceProcessor:
        """Reconnect without acquiring lock (internal use only).
        
        Args:
            trace_path: Path to the trace file
            
        Returns:
            TraceProcessor: New connection
        """
        logger.info(f"Attempting to reconnect to {trace_path}")
        
        # Close existing connection
        self._close_current_unsafe()
        
        # Create new connection
        try:
            self._current_connection = self._create_connection(trace_path)
            self._current_trace_path = trace_path
            logger.info(f"Successfully reconnected to {trace_path}")
            return self._current_connection
        except Exception as e:
            logger.error(f"Reconnection failed for {trace_path}: {e}")
            raise ConnectionError(f"Reconnection failed: {e}")
    
    def close_current(self):
        """Close the current connection if it exists."""
        with self._lock:
            self._close_current_unsafe()
    
    def _close_current_unsafe(self):
        """Close current connection without acquiring lock (internal use only)."""
        if self._current_connection is not None:
            try:
                logger.info(f"Closing connection to {self._current_trace_path}")
                self._current_connection.close()
            except Exception as e:
                logger.warning(f"Error closing connection: {e}")
            finally:
                self._current_connection = None
                self._current_trace_path = None
    
    def cleanup(self):
        """Cleanup method called by MCP server shutdown lifecycle."""
        logger.info("Cleaning up connection manager")
        self.close_current()
    
    def get_current_trace_path(self) -> Optional[str]:
        """Get the currently connected trace path.
        
        Returns:
            Optional[str]: Current trace path or None if no connection
        """
        with self._lock:
            return self._current_trace_path
    
    def is_connected(self) -> bool:
        """Check if there's an active connection.
        
        Returns:
            bool: True if connected, False otherwise
        """
        with self._lock:
            return self._current_connection is not None
