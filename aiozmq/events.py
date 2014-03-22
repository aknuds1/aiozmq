import asyncio
import errno
import re
import zmq

from asyncio.unix_events import SelectorEventLoop, DefaultEventLoopPolicy
from asyncio.transports import _FlowControlMixin
from collections import deque, Iterable, Set
from ipaddress import ip_address

from .selector import ZmqSelector
from .interface import ZmqTransport


__all__ = ['ZmqEventLoop', 'ZmqEventLoopPolicy']


class ZmqEventLoop(SelectorEventLoop):
    """ZeroMQ event loop.

    Follows asyncio.AbstractEventLoop specification, in addition implements
    create_zmq_connection method for working with ZeroMQ sockets.
    """

    def __init__(self, *, io_threads=1):
        super().__init__(selector=ZmqSelector())
        self._zmq_context = zmq.Context(io_threads)

    def close(self):
        super().close()
        self._zmq_context.destroy()

    @asyncio.coroutine
    def create_zmq_connection(self, protocol_factory, zmq_type, *,
                               bind=None, connect=None, zmq_sock=None):
        """A coroutine which creates a ZeroMQ connection endpoint.

        The return value is a pair of (transport, protocol),
        where transport support ZmqTransport interface.

        protocol_factory should instantiate object with ZmqProtocol interface.

        zmq_type is type of ZeroMQ socket (zmq.REQ, zmq.REP, zmq.PUB, zmq.SUB,
        zmq.PAIR, zmq.DEALER, zmq.ROUTER, zmq.PULL, zmq.PUSH, etc.)

        bind is string or iterable of strings that specifies enpoints.
        Every endpoint creates ending for acceptin connections
        and binds it to the transport.
        Other side should use connect parameter to connect to this transport.
        See http://api.zeromq.org/master:zmq-bind for details.

        connect is string or iterable of strings that specifies enpoints.
        Every endpoint connects transport to specified transport.
        Other side should use bind parameter to wait for incoming connections.
        See http://api.zeromq.org/master:zmq-connect for details.

        endpoint is a string consisting of two parts as follows:
        transport://address.

        The transport part specifies the underlying transport protocol to use.
        The meaning of the address part is specific to the underlying
        transport protocol selected.

        The following transports are defined:

        inproc - local in-process (inter-thread) communication transport,
        see http://api.zeromq.org/master:zmq-inproc.

        ipc - local inter-process communication transport,
        see http://api.zeromq.org/master:zmq-ipc

        tcp - unicast transport using TCP,
        see http://api.zeromq.org/master:zmq_tcp

        pgm, epgm - reliable multicast transport using PGM,
        see http://api.zeromq.org/master:zmq_pgm

        zmq_sock is a zmq.Socket instance to use preexisting object
        with created transport.

        The only one of bind, connect or zmq_sock should be specified.
        """
        if 1 != sum(
                1 if i is not None else 0 for i in [zmq_sock, bind, connect]):
            raise ValueError(
                "the only bind, connect or zmq_sock should be specified "
                "at the same time", bind, connect, zmq_sock)

        try:
            if zmq_sock is None:
                own_zmq_sock = True
                zmq_sock = self._zmq_context.socket(zmq_type)
            else:
                own_zmq_sock = False
                if zmq_sock.getsockopt(zmq.TYPE) != zmq_type:
                    raise ValueError('Invalid zmq_sock type')
        except zmq.ZMQError as exc:
            raise OSError(exc.errno, exc.strerror) from exc

        protocol = protocol_factory()
        transport = _ZmqTransportImpl(self, zmq_sock, protocol)

        try:
            if bind is not None:
                if isinstance(bind, str):
                    bind = [bind]
                else:
                    if not isinstance(bind, Iterable):
                        raise ValueError('bind should be str or iterable')
                for endpoint in bind:
                    transport.bind(endpoint)
            elif connect is not None:
                if isinstance(connect, str):
                    connect = [connect]
                else:
                    if not isinstance(connect, Iterable):
                        raise ValueError('connect should be '
                                         'str or iterable')
                for endpoint in connect:
                    transport.connect(endpoint)

            waiter = asyncio.Future(loop=self)
            transport._init_done(waiter)
            yield from waiter
            return transport, protocol
        except OSError:
            if own_zmq_sock:
                # don't care if zmq_sock.close can raise exception
                zmq_sock.close()
            raise


class _EndpointsSet(Set):

    __slots__ = ('_collection',)

    def __init__(self, collection):
        self._collection = collection

    def __len__(self):
        return len(self._collection)

    def __contains__(self, endpoint):
        return endpoint in self._collection

    def __iter__(self):
        return iter(self._collection)

    def __repr__(self):
        return '{' + ', '.join(self._collection) + '}'

    __str__ = __repr__


class _ZmqTransportImpl(ZmqTransport, _FlowControlMixin):

    _TCP_RE = re.compile('^tcp://(.+):(\d+)|\*$')

    def __init__(self, loop, zmq_sock, protocol):
        super().__init__(None)
        self._extra['zmq_socket'] = zmq_sock
        self._loop = loop
        self._zmq_sock = zmq_sock
        self._protocol = protocol
        self._closing = False
        self._buffer = deque()
        self._buffer_size = 0
        self._listeners = set()
        self._connections = set()

    def _init_done(self, waiter):
        self._loop.add_reader(self._zmq_sock, self._read_ready)
        self._loop.call_soon(self._protocol.connection_made, self)
        if waiter is not None:
            self._loop.call_soon(waiter.set_result, None)

    def _read_ready(self):
        try:
            data = self._zmq_sock.recv_multipart(zmq.NOBLOCK)
        except zmq.ZMQError as exc:
            if exc.errno in (errno.EAGAIN, errno.EINTR):
                pass
            else:
                raise OSError(exc.errno, exc.strerror) from exc
        except Exception as exc:
            self._fatal_error(exc, 'Fatal read error on zmq socket transport')
        else:
            self._protocol.msg_received(tuple(data))

    def write(self, data):
        if not data:
            return
        for part in data:
            if not isinstance(part, (bytes, bytearray, memoryview)):
                raise TypeError('data argument must be iterable of '
                                'byte-ish (%r)' % data)
        data_len = sum(len(part) for part in data)
        if not data_len:
            return

        if not self._buffer:
            try:
                try:
                    self._zmq_sock.send_multipart(data, zmq.DONTWAIT)
                    return
                except zmq.ZMQError as exc:
                    if exc.errno in (errno.EAGAIN, errno.EINTR):
                        self._loop.add_writer(self._zmq_sock, self._write_ready)
                    else:
                        raise OSError(exc.errno, exc.strerror) from exc
            except Exception as exc:
                self._fatal_error(exc,
                                  'Fatal write error on zmq socket transport')
                return

        self._buffer.append(data)
        self._buffer_size += data_len
        self._maybe_pause_protocol()

    def _write_ready(self):
        assert self._buffer, 'Data should not be empty'

        try:
            try:
                self._zmq_sock.send_multipart(self._buffer[0], zmq.DONTWAIT)
            except zmq.ZMQError as exc:
                if exc.errno in (errno.EAGAIN, errno.EINTR):
                    pass
                else:
                    raise OSError(exc.errno, exc.strerror) from exc
        except Exception as exc:
            self._loop.remove_writer(self._zmq_sock)
            self._buffer.clear()
            self._buffer_size = 0
            self._fatal_error(exc,
                              'Fatal write error on zmq socket transport')
        else:
            sent_data = self._buffer.popleft()
            self._buffer_size -= sum(len(part) for part in sent_data)

            self._maybe_resume_protocol()

            if not self._buffer:
                self._loop.remove_writer(self._zmq_sock)
                if self._closing:
                    self._call_connection_lost(None)

    def can_write_eof(self):
        return False

    def abort(self):
        self._force_close(None)

    def close(self):
        if self._closing:
            return
        self._closing = True
        self._loop.remove_reader(self._zmq_sock)
        if not self._buffer:
            self._loop.call_soon(self._call_connection_lost, None)

    def _fatal_error(self, exc, message='Fatal error on transport'):
        # Should be called from exception handler only.
        self._loop.call_exception_handler({
            'message': message,
            'exception': exc,
            'transport': self,
            'protocol': self._protocol,
            })
        self._force_close(exc)

    def _force_close(self, exc):
        if self._buffer:
            self._buffer.clear()
            self._loop.remove_writer(self._zmq_sock)
        if not self._closing:
            self._closing = True
            self._loop.remove_reader(self._zmq_sock)
        self._loop.call_soon(self._call_connection_lost, exc)

    def _call_connection_lost(self, exc):
        try:
            self._protocol.connection_lost(exc)
        finally:
            self._zmq_sock.close()
            self._zmq_sock = None
            self._protocol = None
            self._loop = None

    def getsockopt(self, option):
        try:
            return self._zmq_sock.getsockopt(option)
        except zmq.ZMQError as exc:
            raise OSError(exc.errno, exc.strerror) from exc

    def setsockopt(self, option, value):
        try:
            self._zmq_sock.setsockopt(option, value)
        except zmq.ZMQError as exc:
            raise OSError(exc.errno, exc.strerror) from exc

    def get_write_buffer_size(self):
        return self._buffer_size

    def bind(self, endpoint):
        try:
            self._zmq_sock.bind(endpoint)
            breal_endpoint = self._zmq_sock.getsockopt(zmq.LAST_ENDPOINT)
        except zmq.ZMQError as exc:
            raise OSError(exc.errno, exc.strerror) from exc
        else:
            real_endpoint = breal_endpoint.decode('utf-8').rstrip('\x00')
            self._listeners.add(real_endpoint)
            return real_endpoint

    def unbind(self, endpoint):
        try:
            self._zmq_sock.unbind(endpoint)
        except zmq.ZMQError as exc:
            raise OSError(exc.errno, exc.strerror) from exc
        else:
            self._listeners.discard(endpoint)

    def listeners(self):
        return _EndpointsSet(self._listeners)

    def connect(self, endpoint):
        match = self._TCP_RE.match(endpoint)
        if match:
            ip_address(match.group(1))  # check for correct IPv4 or IPv6
        try:
            self._zmq_sock.connect(endpoint)
        except zmq.ZMQError as exc:
            raise OSError(exc.errno, exc.strerror) from exc
        else:
            self._connections.add(endpoint)
            return endpoint

    def disconnect(self, endpoint):
        try:
            self._zmq_sock.disconnect(endpoint)
        except zmq.ZMQError as exc:
            raise OSError(exc.errno, exc.strerror) from exc
        else:
            self._connections.discard(endpoint)

    def connections(self):
        return _EndpointsSet(self._connections)


class ZmqEventLoopPolicy(DefaultEventLoopPolicy):

    _loop_factory = ZmqEventLoop()
