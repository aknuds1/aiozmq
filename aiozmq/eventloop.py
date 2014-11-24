import sys
import weakref
import errno
import threading
from collections import Iterable
import asyncio.events
import zmq

from .basetransport import BaseTransport
from .selector import ZmqSelector


if sys.platform == 'win32':
    from asyncio.windows_events import SelectorEventLoop
else:
    from asyncio.unix_events import SelectorEventLoop, SafeChildWatcher


class _ZmqTransportImpl(BaseTransport):

    def __init__(self, loop, zmq_type, zmq_sock, protocol, waiter=None):
        super().__init__(loop, zmq_type, zmq_sock, protocol)

        self._loop.add_reader(self._zmq_sock, self._read_ready)
        self._loop.call_soon(self._protocol.connection_made, self)
        if waiter is not None:
            self._loop.call_soon(waiter.set_result, None)

    def _read_ready(self):
        try:
            try:
                data = self._zmq_sock.recv_multipart(zmq.NOBLOCK)
            except zmq.ZMQError as exc:
                if exc.errno in (errno.EAGAIN, errno.EINTR):
                    return
                else:
                    raise OSError(exc.errno, exc.strerror) from exc
        except Exception as exc:
            self._fatal_error(exc, 'Fatal read error on zmq socket transport')
        else:
            self._protocol.msg_received(data)

    def _do_send(self, data):
        try:
            self._zmq_sock.send_multipart(data, zmq.DONTWAIT)
            return True
        except zmq.ZMQError as exc:
            if exc.errno in (errno.EAGAIN, errno.EINTR):
                self._loop.add_writer(self._zmq_sock,
                                      self._write_ready)
                return False
            else:
                raise OSError(exc.errno, exc.strerror) from exc

    def _write_ready(self):
        assert self._buffer, 'Data should not be empty'

        try:
            try:
                self._zmq_sock.send_multipart(self._buffer[0][1], zmq.DONTWAIT)
            except zmq.ZMQError as exc:
                if exc.errno in (errno.EAGAIN, errno.EINTR):
                    return
                else:
                    raise OSError(exc.errno, exc.strerror) from exc
        except Exception as exc:
            self._fatal_error(exc,
                              'Fatal write error on zmq socket transport')
        else:
            sent_len, sent_data = self._buffer.popleft()
            self._buffer_size -= sent_len

            self._maybe_resume_protocol()

            if not self._buffer:
                self._loop.remove_writer(self._zmq_sock)
                if self._closing:
                    self._call_connection_lost(None)

    def close(self):
        if self._closing:
            return
        self._closing = True
        if not self._paused:
            self._loop.remove_reader(self._zmq_sock)
        if not self._buffer:
            self._conn_lost += 1
            self._loop.call_soon(self._call_connection_lost, None)

    def _force_close(self, exc):
        if self._conn_lost:
            return
        if self._buffer:
            self._buffer.clear()
            self._buffer_size = 0
            self._loop.remove_writer(self._zmq_sock)
        if not self._closing:
            self._closing = True
            if not self._paused:
                self._loop.remove_reader(self._zmq_sock)
        self._conn_lost += 1
        self._loop.call_soon(self._call_connection_lost, exc)

    def _do_pause_reading(self):
        self._loop.remove_reader(self._zmq_sock)

    def _do_resume_reading(self):
        self._loop.add_reader(self._zmq_sock, self._read_ready)


class ZmqEventLoop(SelectorEventLoop):
    """ZeroMQ event loop.

    Follows asyncio.AbstractEventLoop specification, in addition implements
    create_zmq_connection method for working with ZeroMQ sockets.
    """

    def __init__(self, *, zmq_context=None):
        super().__init__(selector=ZmqSelector())
        if zmq_context is None:
            self._zmq_context = zmq.Context.instance()
        else:
            self._zmq_context = zmq_context
        self._zmq_sockets = weakref.WeakSet()

    def close(self):
        for zmq_sock in self._zmq_sockets:
            if not zmq_sock.closed:
                zmq_sock.close()
        super().close()

    @asyncio.coroutine
    def create_zmq_connection(self, protocol_factory, zmq_type, *,
                              bind=None, connect=None, zmq_sock=None):
        """A coroutine which creates a ZeroMQ connection endpoint.

        See aiozmq.create_zmq_connection() coroutine for details.
        """

        try:
            if zmq_sock is None:
                zmq_sock = self._zmq_context.socket(zmq_type)
            elif zmq_sock.getsockopt(zmq.TYPE) != zmq_type:
                raise ValueError('Invalid zmq_sock type')
        except zmq.ZMQError as exc:
            raise OSError(exc.errno, exc.strerror) from exc

        protocol = protocol_factory()
        waiter = asyncio.Future(loop=self)
        transport = _ZmqTransportImpl(self, zmq_type,
                                      zmq_sock, protocol, waiter)
        yield from waiter

        try:
            if bind is not None:
                if isinstance(bind, str):
                    bind = [bind]
                else:
                    if not isinstance(bind, Iterable):
                        raise ValueError('bind should be str or iterable')
                for endpoint in bind:
                    yield from transport.bind(endpoint)
            if connect is not None:
                if isinstance(connect, str):
                    connect = [connect]
                else:
                    if not isinstance(connect, Iterable):
                        raise ValueError('connect should be '
                                         'str or iterable')
                for endpoint in connect:
                    yield from transport.connect(endpoint)
            self._zmq_sockets.add(zmq_sock)
            return transport, protocol
        except OSError:
            # don't care if zmq_sock.close can raise exception
            # that should never happen
            zmq_sock.close()
            raise


class ZmqEventLoopPolicy(asyncio.AbstractEventLoopPolicy):
    """ZeroMQ policy implementation for accessing the event loop.

    In this policy, each thread has its own event loop.  However, we
    only automatically create an event loop by default for the main
    thread; other threads by default have no event loop.
    """

    class _Local(threading.local):
        _loop = None
        _set_called = False

    def __init__(self):
        self._local = self._Local()
        self._watcher = None

    def get_event_loop(self):
        """Get the event loop.

        If current thread is the main thread and there are no
        registered event loop for current thread then the call creates
        new event loop and registers it.

        Return an instance of ZmqEventLoop.
        Raise RuntimeError if there is no registered event loop
        for current thread.
        """
        if (self._local._loop is None and
                not self._local._set_called and
                isinstance(threading.current_thread(), threading._MainThread)):
            self.set_event_loop(self.new_event_loop())
        assert self._local._loop is not None, \
            ('There is no current event loop in thread %r.' %
             threading.current_thread().name)
        return self._local._loop

    def new_event_loop(self):
        """Create a new event loop.

        You must call set_event_loop() to make this the current event
        loop.
        """
        return ZmqEventLoop()

    def set_event_loop(self, loop):
        """Set the event loop.

        As a side effect, if a child watcher was set before, then calling
        .set_event_loop() from the main thread will call .attach_loop(loop) on
        the child watcher.
        """

        self._local._set_called = True
        assert loop is None or isinstance(loop, asyncio.AbstractEventLoop), \
            "loop should be None or AbstractEventLoop instance"
        self._local._loop = loop

        if (self._watcher is not None and
                isinstance(threading.current_thread(), threading._MainThread)):
            self._watcher.attach_loop(loop)

    if sys.platform != 'win32':
        def _init_watcher(self):
            with asyncio.events._lock:
                if self._watcher is None:  # pragma: no branch
                    self._watcher = SafeChildWatcher()
                    if isinstance(threading.current_thread(),
                                  threading._MainThread):
                        self._watcher.attach_loop(self._local._loop)

        def get_child_watcher(self):
            """Get the child watcher.

            If not yet set, a SafeChildWatcher object is automatically created.
            """
            if self._watcher is None:
                self._init_watcher()

            return self._watcher

        def set_child_watcher(self, watcher):
            """Set the child watcher."""

            assert watcher is None or \
                isinstance(watcher, asyncio.AbstractChildWatcher), \
                "watcher should be None or AbstractChildWatcher instance"

            if self._watcher is not None:
                self._watcher.close()

            self._watcher = watcher
