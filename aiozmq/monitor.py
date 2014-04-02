import abc
import asyncio
import errno
import re
import socket
import struct
import zmq

from ipaddress import ip_address

from .util import _EndpointsSet


EVENT_NAMES = {getattr(zmq, k): k for k in dir(zmq) if k.startswith('EVENT_')}


class AbstractMonitor(metaclass=abc.ABCMeta):
    """XXX"""

    _TCP_RE = re.compile('^tcp://(.+):(\d+)|\*$')

    def __init__(self, loop, transport):
        self._loop = loop
        self._transport = transport
        self._zmq_sock = transport.get_extra_info('zmq_socket')
        self._bindings = set()
        self._connections = set()

    @abc.abstractmethod
    def close(self):
        pass

    @asyncio.coroutine
    @abc.abstractmethod
    def bind(self, endpoint):
        pass

    @asyncio.coroutine
    @abc.abstractmethod
    def unbind(self, endpoint):
        pass

    def bindings(self):
        return _EndpointsSet(self._bindings)

    @asyncio.coroutine
    @abc.abstractmethod
    def connect(self, endpoint):
        pass

    @asyncio.coroutine
    @abc.abstractmethod
    def disconnect(self, endpoint):
        pass

    def connections(self):
        return _EndpointsSet(self._connections)

    def _validate_tcp_addr(self, endpoint):
        match = self._TCP_RE.match(endpoint)
        if match:
            # check for correct IPv4 or IPv6
            # raise ValueError if addr has DNS name
            ip_address(match.group(1))

    def _validate_endpoint_type(self, endpoint):
        if not isinstance(endpoint, str):
            raise TypeError('endpoint should be str, got {!r}'
                            .format(endpoint))


class _FallbackMonitor(AbstractMonitor):

    def close(self):
        super().close()

    def bind(self, endpoint):
        fut = asyncio.Future(loop=self._loop)
        try:
            self._validate_endpoint_type(endpoint)
            try:
                self._zmq_sock.bind(endpoint)
                real_endpoint = self._transport.getsockopt(zmq.LAST_ENDPOINT)
            except zmq.ZMQError as exc:
                raise OSError(exc.errno, exc.strerror) from exc
        except Exception as exc:
            fut.set_exception(exc)
        else:
            self._bindings.add(real_endpoint)
            fut.set_result(real_endpoint)
        return fut

    def unbind(self, endpoint):
        fut = asyncio.Future(loop=self._loop)
        try:
            self._validate_endpoint_type(endpoint)
            try:
                self._zmq_sock.unbind(endpoint)
            except zmq.ZMQError as exc:
                raise OSError(exc.errno, exc.strerror) from exc
            else:
                self._bindings.discard(endpoint)
        except Exception as exc:
            fut.set_exception(exc)
        else:
            fut.set_result(None)
        return fut

    def connect(self, endpoint):
        fut = asyncio.Future(loop=self._loop)
        try:
            self._validate_endpoint_type(endpoint)
            self._validate_tcp_addr(endpoint)
            try:
                self._zmq_sock.connect(endpoint)
            except zmq.ZMQError as exc:
                raise OSError(exc.errno, exc.strerror) from exc
        except Exception as exc:
            fut.set_exception(exc)
        else:
            self._connections.add(endpoint)
            fut.set_result(endpoint)
        return fut

    def disconnect(self, endpoint):
        fut = asyncio.Future(loop=self._loop)
        try:
            self._validate_endpoint_type(endpoint)
            try:
                self._zmq_sock.disconnect(endpoint)
            except zmq.ZMQError as exc:
                raise OSError(exc.errno, exc.strerror) from exc
        except Exception as exc:
            fut.set_exception(exc)
        else:
            self._connections.discard(endpoint)
            fut.set_result(None)
        return fut


class _TrueMonitor(_FallbackMonitor):

    EVENTS = zmq.EVENT_CONNECTED #| zmq.EVENT_DISCONNECTED
    EVENT_STRUCT = struct.Struct('=HL')

    def __init__(self, loop, transport):
        super().__init__(loop, transport)

        self._monitor_sock = self._zmq_sock.get_monitor_socket()
        self._loop.add_reader(self._monitor_sock, self._read_ready)
        self._waiters = {}

    def _read_ready(self):
        try:
            try:
                ready = self._monitor_sock.getsockopt(zmq.EVENTS)
                if ready != zmq.POLLIN:
                    print('!', end='')
                    return
                bdata, bendpoint = self._monitor_sock.recv_multipart(
                    zmq.NOBLOCK)
            except zmq.ZMQError as exc:
                if exc.errno in (errno.EAGAIN, errno.EINTR):
                    return
                else:
                    raise OSError(exc.errno, exc.strerror) from exc
            else:
                event, fd = self.EVENT_STRUCT.unpack(bdata)
                # value = self.EVENT_STRUCT.unpack(bvalue)
                # FIXME: actually should be fsdecode
                endpoint = bendpoint.decode('utf-8')
                print(EVENT_NAMES[event], fd, endpoint)
                if event != zmq.EVENT_CONNECTED:
                    return
                #name = self._find_sockname(fd)
                #print(name)
                fut = self._waiters.pop(endpoint, None)
                if fut is None:
                    self._loop.call_exception_handler({
                        'message': 'Unknown endpoint {} from monitor'.format(
                            endpoint),
                        'transport': self._transport,
                        })
                    return
                if fut.cancelled():
                    self._loop.call_exception_handler({
                        'message': ('Future for endpoint {} from monitor '
                                    'has been cancelled'.format(
                                    endpoint)),
                        'transport': self._transport,
                        })
                    return
                #if event == zmq.EVENT_CONNECTED:
                self._connections.add(endpoint)
                fut.set_result(endpoint)
                #elif event == zmq.EVENT_DISCONNECTED:
                #    self._connections.discard(name)
        except Exception as exc:
            self._loop.call_exception_handler({
                'message': 'Unknown exception from monitor',
                'exception': exc,
                'transport': self._transport,
                })

    def _find_sockname(self, fd):
        sock = socket.fromfd(fd, socket.AF_INET, socket.SOCK_STREAM)
        try:
            return sock.getsockname()
        finally:
            sock.detach()

    def close(self):
        self._loop.remove_reader(self._monitor_sock)
        self._monitor_sock.close()
        super().close()

    def connect(self, endpoint):
        if not endpoint.startswith(('ipc://', 'tcp://')):
            return super().connect(endpoint)
        fut = asyncio.Future(loop=self._loop)
        try:
            self._validate_endpoint_type(endpoint)
            self._validate_tcp_addr(endpoint)
            assert endpoint not in self._waiters
            self._waiters[endpoint] = fut
            try:
                self._zmq_sock.connect(endpoint)
            except zmq.ZMQError as exc:
                raise OSError(exc.errno, exc.strerror) from exc
        except Exception as exc:
            fut.set_exception(exc)
        return fut
