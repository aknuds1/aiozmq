import abc
import asyncio
import re
import zmq

from ipaddress import ip_address

from .utils import _EndpointsSet


class AbstractMonitor(metaclass=abc.ABCMeta):
    """XXX"""

    def __init__(self, loop, transport):
        self._loop = loop
        self._transport = transport
        self._zmq_sock = transport.get_extra_info('zmq_socket')
        self._bindings = set()
        self._connections = set()

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


class _FallbackMonitor(AbstractMonitor):

    _TCP_RE = re.compile('^tcp://(.+):(\d+)|\*$')

    def bind(self, endpoint):
        fut = asyncio.Future(loop=self._loop)
        try:
            if not isinstance(endpoint, str):
                raise TypeError('endpoint should be str, got {!r}'
                                .format(endpoint))
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
            if not isinstance(endpoint, str):
                raise TypeError('endpoint should be str, got {!r}'
                                .format(endpoint))
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
            if not isinstance(endpoint, str):
                raise TypeError('endpoint should be str, got {!r}'
                                .format(endpoint))
            match = self._TCP_RE.match(endpoint)
            if match:
                ip_address(match.group(1))  # check for correct IPv4 or IPv6
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
            if not isinstance(endpoint, str):
                raise TypeError('endpoint should be str, got {!r}'
                                .format(endpoint))
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
