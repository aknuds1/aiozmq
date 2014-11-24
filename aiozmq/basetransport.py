import re
import errno
import asyncio
from collections import deque
from ipaddress import ip_address
import zmq

from .interface import ZmqTransport
from .util import _EndpointsSet
from .log import logger


class BaseTransport(ZmqTransport):

    _TCP_RE = re.compile('^tcp://(.+):(\d+)|\*$')
    LOG_THRESHOLD_FOR_CONNLOST_WRITES = 5
    ZMQ_TYPES = {zmq.PUB: 'PUB',
                 zmq.SUB: 'SUB',
                 zmq.REP: 'REP',
                 zmq.REQ: 'REQ',
                 zmq.PUSH: 'PUSH',
                 zmq.PULL: 'PULL',
                 zmq.DEALER: 'DEALER',
                 zmq.ROUTER: 'ROUTER',
                 zmq.XPUB: 'XPUB',
                 zmq.XSUB: 'XSUB',
                 zmq.PAIR: 'PAIR',
                 zmq.STREAM: 'STREAM'}

    def __init__(self, loop, zmq_type, zmq_sock, protocol):
        super().__init__(None)
        self._protocol_paused = False
        self._set_write_buffer_limits()
        self._extra['zmq_socket'] = zmq_sock
        self._extra['zmq_type'] = zmq_type
        self._loop = loop
        self._zmq_sock = zmq_sock
        self._zmq_type = zmq_type
        self._protocol = protocol
        self._closing = False
        self._buffer = deque()
        self._buffer_size = 0
        self._bindings = set()
        self._connections = set()
        self._subscriptions = set()
        self._paused = False
        self._conn_lost = 0

    def __repr__(self):
        info = ['ZmqTransport',
                'sock={}'.format(self._zmq_sock),
                'type={}'.format(self.ZMQ_TYPES[self._zmq_type])]
        try:
            events = self._zmq_sock.getsockopt(zmq.EVENTS)
            if events & zmq.POLLIN:
                info.append('read=polling')
            else:
                info.append('read=idle')
            if events & zmq.POLLOUT:
                state = 'polling'
            else:
                state = 'idle'
            bufsize = self.get_write_buffer_size()
            info.append('write=<{}, bufsize={}>'.format(state, bufsize))
        except zmq.ZMQError:
            pass
        return '<{}>'.format(' '.join(info))

    def write(self, data):
        if not data:
            return
        for part in data:
            if not isinstance(part, (bytes, bytearray, memoryview)):
                raise TypeError('data argument must be iterable of '
                                'byte-ish (%r)' % data)
        data_len = sum(len(part) for part in data)

        logger.debug('Transport: Writing data, {} byte(s)'.format(
            data_len
        ))

        if self._conn_lost:
            if self._conn_lost >= self.LOG_THRESHOLD_FOR_CONNLOST_WRITES:
                logger.warning('write to closed ZMQ socket.')
            self._conn_lost += 1
            return

        if not self._buffer:
            logger.debug('Transport: Have no buffer, sending data immediately')
            try:
                if self._do_send(data):
                    return
            except Exception as exc:
                self._fatal_error(exc,
                                  'Fatal write error on zmq socket transport')
                return

        self._buffer.append((data_len, data))
        self._buffer_size += data_len
        self._maybe_pause_protocol()

    def can_write_eof(self):
        return False

    def abort(self):
        logger.debug('Transport: Aborting')
        self._force_close(None)

    def _fatal_error(self, exc, message='Fatal error on transport'):
        # Should be called from exception handler only.
        logger.warning('Transport: Fatal error: {}, {}'.format(
            exc, message
        ))
        self._loop.call_exception_handler({
            'message': message,
            'exception': exc,
            'transport': self,
            'protocol': self._protocol,
            })
        self._force_close(exc)

    def _call_connection_lost(self, exc):
        logger.debug('Transport: Connection lost: {}'.format(exc))
        try:
            self._protocol.connection_lost(exc)
        finally:
            if not self._zmq_sock.closed:
                self._zmq_sock.close()
            self._zmq_sock = None
            self._protocol = None
            self._loop = None

    def _maybe_pause_protocol(self):
        size = self.get_write_buffer_size()
        if size <= self._high_water:
            return
        if not self._protocol_paused:
            logger.debug('Transport: Pausing protocol')
            self._protocol_paused = True
            try:
                self._protocol.pause_writing()
            except Exception as exc:
                logger.warning('Transport: Pausing protocol failed: {}'.format(
                    exc
                ))
                self._loop.call_exception_handler({
                    'message': 'protocol.pause_writing() failed',
                    'exception': exc,
                    'transport': self,
                    'protocol': self._protocol,
                })

    def _maybe_resume_protocol(self):
        if (self._protocol_paused and
                self.get_write_buffer_size() <= self._low_water):
            logger.debug('Transport: Resuming protocol')
            self._protocol_paused = False
            try:
                self._protocol.resume_writing()
            except Exception as exc:
                logger.warning('Transport: Resuming protocol failed: {}'.format(
                    exc
                ))
                self._loop.call_exception_handler({
                    'message': 'protocol.resume_writing() failed',
                    'exception': exc,
                    'transport': self,
                    'protocol': self._protocol,
                })

    def _set_write_buffer_limits(self, high=None, low=None):
        if high is None:
            if low is None:
                high = 64*1024
            else:
                high = 4*low
        if low is None:
            low = high // 4
        if not high >= low >= 0:
            raise ValueError('high (%r) must be >= low (%r) must be >= 0' %
                             (high, low))
        self._high_water = high
        self._low_water = low

    def get_write_buffer_limits(self):
        return (self._low_water, self._high_water)

    def set_write_buffer_limits(self, high=None, low=None):
        self._set_write_buffer_limits(high=high, low=low)
        self._maybe_pause_protocol()

    def pause_reading(self):
        if self._closing:
            raise RuntimeError('Cannot pause_reading() when closing')
        if self._paused:
            raise RuntimeError('Already paused')
        logger.debug('Transport: Pausing reading')
        self._paused = True
        self._do_pause_reading()

    def resume_reading(self):
        if not self._paused:
            raise RuntimeError('Not paused')
        logger.debug('Transport: Resuming reading')
        self._paused = False
        if self._closing:
            return
        self._do_resume_reading()

    def getsockopt(self, option):
        while True:
            try:
                ret = self._zmq_sock.getsockopt(option)
                if option == zmq.LAST_ENDPOINT:
                    ret = ret.decode('utf-8').rstrip('\x00')
                return ret
            except zmq.ZMQError as exc:
                if exc.errno == errno.EINTR:
                    continue
                raise OSError(exc.errno, exc.strerror) from exc

    def setsockopt(self, option, value):
        while True:
            try:
                self._zmq_sock.setsockopt(option, value)
                if option == zmq.SUBSCRIBE:
                    self._subscriptions.add(value)
                elif option == zmq.UNSUBSCRIBE:
                    self._subscriptions.discard(value)
                return
            except zmq.ZMQError as exc:
                if exc.errno == errno.EINTR:
                    continue
                raise OSError(exc.errno, exc.strerror) from exc

    def get_write_buffer_size(self):
        return self._buffer_size

    def bind(self, endpoint):
        fut = asyncio.Future(loop=self._loop)
        try:
            if not isinstance(endpoint, str):
                raise TypeError('endpoint should be str, got {!r}'
                                .format(endpoint))
            try:
                logger.debug(
                    'Transport: Binding socket to endpoint {}'.format(
                        endpoint
                    ))
                self._zmq_sock.bind(endpoint)
                real_endpoint = self.getsockopt(zmq.LAST_ENDPOINT)
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
            logger.debug('Transport: Unbinding endpoint {}'.format(
                endpoint
            ))
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

    def bindings(self):
        return _EndpointsSet(self._bindings)

    def connect(self, endpoint):
        fut = asyncio.Future(loop=self._loop)
        try:
            if not isinstance(endpoint, str):
                raise TypeError('endpoint should be str, got {!r}'
                                .format(endpoint))
            match = self._TCP_RE.match(endpoint)
            if match:
                ip_address(match.group(1))  # check for correct IPv4 or IPv6
            logger.debug('Transport: Connecting to endpoint {}'.format(
                endpoint
            ))
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
            logger.debug('Transport: Disconnecting from endpoint {}'.format(
                endpoint
            ))
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

    def connections(self):
        return _EndpointsSet(self._connections)

    def subscribe(self, value):
        if self._zmq_type != zmq.SUB:
            raise NotImplementedError("Not supported ZMQ socket type")
        if not isinstance(value, bytes):
            raise TypeError("value argument should be bytes")
        if value in self._subscriptions:
            return
        self.setsockopt(zmq.SUBSCRIBE, value)

    def unsubscribe(self, value):
        if self._zmq_type != zmq.SUB:
            raise NotImplementedError("Not supported ZMQ socket type")
        if not isinstance(value, bytes):
            raise TypeError("value argument should be bytes")
        self.setsockopt(zmq.UNSUBSCRIBE, value)

    def subscriptions(self):
        if self._zmq_type != zmq.SUB:
            raise NotImplementedError("Not supported ZMQ socket type")
        return _EndpointsSet(self._subscriptions)
