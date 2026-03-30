"""Thread-safe connection pool for database connectors.

Provides a generic ``ConnectionPool`` that manages a fixed number of
database connections. Workers check out connections via the ``acquire()``
context manager.  On normal exit the connection is returned to the pool;
on exception the pool calls a caller-supplied ``reset`` function
(typically ``conn.rollback()``) before returning it.
"""

from __future__ import annotations

import logging
import queue
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from typing import Generic, TypeVar

logger = logging.getLogger(__name__)

C = TypeVar("C")


class ConnectionPool(Generic[C]):
    """Fixed-size, thread-safe connection pool.

    Args:
        factory: Zero-arg callable that creates a new connection.
        reset: Called on a connection when it is returned after an error
            (e.g. ``conn.rollback()``).
        close: Called on a connection during ``close_all()``.
        size: Number of connections to create.
    """

    def __init__(
        self,
        factory: Callable[[], C],
        reset: Callable[[C], None],
        close: Callable[[C], None],
        size: int,
    ) -> None:
        if size < 1:
            raise ValueError("Pool size must be at least 1")

        self._factory = factory
        self._reset = reset
        self._close = close
        self._size = size
        self._queue: queue.Queue[C] = queue.Queue(maxsize=size)
        self._all: list[C] = []

        for _ in range(size):
            conn = factory()
            self._all.append(conn)
            self._queue.put(conn)

    @property
    def primary(self) -> C:
        """The first connection, usable for single-threaded schema operations.

        This is a reference to ``connections[0]`` — it may currently be
        checked out by a worker, so only use it when no parallel work is
        running (e.g. during schema introspection before data transfer).
        """
        return self._all[0]

    @contextmanager
    def acquire(self) -> Iterator[C]:
        """Check out a connection, yielding it for use.

        On normal exit the connection is returned to the pool.  If an
        exception propagates, ``reset(conn)`` is called first (to clear
        aborted-transaction state) before the connection is returned.

        Blocks if no connection is available.
        """
        conn = self._queue.get()
        failed = False
        try:
            yield conn
        except BaseException:
            failed = True
            raise
        finally:
            if failed:
                try:
                    self._reset(conn)
                except Exception:
                    logger.debug(
                        "Connection reset failed during pool checkin",
                        exc_info=True,
                    )
            self._queue.put(conn)

    def close_all(self) -> None:
        """Close every connection in the pool.

        Drains the queue first so that checked-out connections are not
        double-closed.  Best-effort: individual close errors are logged
        but do not prevent other connections from being closed.
        """
        # Drain any connections currently in the queue
        while True:
            try:
                self._queue.get_nowait()
            except queue.Empty:
                break

        for conn in self._all:
            try:
                self._close(conn)
            except Exception:
                logger.debug("Error closing pooled connection", exc_info=True)

        self._all.clear()
