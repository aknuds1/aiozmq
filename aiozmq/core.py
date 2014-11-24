import asyncio
import errno
import zmq

from collections import Iterable

from .log import logger
from .eventloop import ZmqEventLoop, ZmqEventLoopPolicy
from .basetransport import BaseTransport


__all__ = ['ZmqEventLoop', 'ZmqEventLoopPolicy', 'create_zmq_connection']


@asyncio.coroutine
def create_zmq_connection(protocol_factory, zmq_type, *,
                          bind=None, connect=None, zmq_sock=None, loop=None):
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
    """
    if loop is None:
        loop = asyncio.get_event_loop()
    if isinstance(loop, ZmqEventLoop):
        ret = yield from loop.create_zmq_connection(protocol_factory,
                                                    zmq_type,
                                                    bind=bind,
                                                    connect=connect,
                                                    zmq_sock=zmq_sock)
        return ret

    try:
        if zmq_sock is None:
            logger.debug('Creating socket of type {}'.format(zmq_type))
            zmq_sock = zmq.Context().instance().socket(zmq_type)
        elif zmq_sock.getsockopt(zmq.TYPE) != zmq_type:
            raise ValueError('Invalid zmq_sock type')
    except zmq.ZMQError as exc:
        raise OSError(exc.errno, exc.strerror) from exc

    protocol = protocol_factory()
    waiter = asyncio.Future(loop=loop)
    transport = _ZmqLooplessTransportImpl(loop, zmq_type,
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
        return transport, protocol
    except OSError:
        # don't care if zmq_sock.close can raise exception
        # that should never happen
        zmq_sock.close()
        raise


class _ZmqLooplessTransportImpl(BaseTransport):

    def __init__(self, loop, zmq_type, zmq_sock, protocol, waiter):
        super().__init__(loop, zmq_type, zmq_sock, protocol)

        fd = zmq_sock.getsockopt(zmq.FD)
        self._fd = fd
        self._loop.add_reader(fd, self._read_ready)

        logger.debug(
            'Arranging for call to protocol.connection_made (protocol: {!r})'
            .format(self._protocol))
        self._loop.call_soon(self._protocol.connection_made, self)
        self._loop.call_soon(waiter.set_result, None)
        self._soon_call = None

    def _read_ready(self):
        logger.debug('Transport ready to read from ZMQ socket')
        self._soon_call = None
        if self._zmq_sock is None:
            return
        events = self._zmq_sock.getsockopt(zmq.EVENTS)
        try_again = False
        if not self._paused and events & zmq.POLLIN:
            logger.debug(
                'Transport: Input detected on socket')
            self._do_read()
            try_again = True
        if self._buffer and events & zmq.POLLOUT:
            logger.debug(
                'Transport: Socket can be written to')
            self._do_write()
            if not try_again:
                try_again = bool(self._buffer)
        if try_again:
            postevents = self._zmq_sock.getsockopt(zmq.EVENTS)
            if postevents & zmq.POLLIN:
                schedule = True
            elif self._buffer and postevents & zmq.POLLOUT:
                schedule = True
            else:
                schedule = False
            if schedule:
                logger.debug('Transport: Scheduling I/O retry')
                self._soon_call = self._loop.call_soon(self._read_ready)

    def _do_read(self):
        logger.debug('Transport: Reading from ZMQ socket')
        try:
            try:
                data = self._zmq_sock.recv_multipart(zmq.NOBLOCK)
            except zmq.ZMQError as exc:
                if exc.errno in (errno.EAGAIN, errno.EINTR):
                    return
                else:
                    logger.debug(
                        '_ZmqLooplessTransportImpl: Got unexpected exception '
                        'reading from socket: {}'.format(exc)
                    )
                    raise OSError(exc.errno, exc.strerror) from exc
        except Exception as exc:
            self._fatal_error(exc, 'Fatal read error on zmq socket transport')
        else:
            logger.debug(
                'Transport: Received {} byte(s) from ZMQ socket'.format(
                    len(data)
                ))
            self._protocol.msg_received(data)

    def _do_write(self):
        if not self._buffer:
            return
        logger.debug('Transport: Writing to ZMQ socket')
        try:
            try:
                self._zmq_sock.send_multipart(self._buffer[0][1], zmq.DONTWAIT)
            except zmq.ZMQError as exc:
                if exc.errno in (errno.EAGAIN, errno.EINTR):
                    if self._soon_call is None:
                        self._soon_call = self._loop.call_soon(
                            self._read_ready)
                    return
                else:
                    raise OSError(exc.errno, exc.strerror) from exc
        except Exception as exc:
            self._fatal_error(exc,
                              'Fatal write error on zmq socket transport')
        else:
            sent_len, sent_data = self._buffer.popleft()
            logger.debug('Transport: Sent {} byte(s) over socket'.format(
                sent_len
            ))
            self._buffer_size -= sent_len

            self._maybe_resume_protocol()

            if not self._buffer and self._closing:
                logger.debug(
                    'Transport: About to close and buffer emtpy, removing '
                    'reader')
                self._loop.remove_reader(self._fd)
                self._call_connection_lost(None)
            else:
                if self._soon_call is None:
                    self._soon_call = self._loop.call_soon(self._read_ready)

    def _do_send(self, data):
        logger.debug('Transport: Sending {} data part(s): {}'.format(
            len(data), data
        ))
        try:
            self._zmq_sock.send_multipart(data)
            if self._soon_call is None:
                self._soon_call = self._loop.call_soon(self._read_ready)
            logger.debug('_ZmqLooplessTransportImpl: Successfully sent data')
            return True
        except zmq.ZMQError as exc:
            logger.debug('_ZmqLooplessTransportImpl: Socket send failed')
            if exc.errno not in (errno.EAGAIN, errno.EINTR):
                logger.warning(
                    '_ZmqLooplessTransportImpl: Unexpected exception: {}'
                    .format(exc)
                )
                raise OSError(exc.errno, exc.strerror) from exc
            else:
                logger.debug(
                    '_ZmqLooplessTransportImpl: Will have to retry send')
                if self._soon_call is None:
                    self._soon_call = self._loop.call_soon(self._read_ready)
                return False

    def close(self):
        if self._closing:
            return
        logger.debug('_ZmqLooplessTransportImpl: Closing')
        self._closing = True
        if not self._buffer:
            self._conn_lost += 1
            if not self._paused:
                self._loop.remove_reader(self._fd)
            self._loop.call_soon(self._call_connection_lost, None)

    def _force_close(self, exc):
        logger.debug('_ZmqLooplessTransportImpl: Forcing close')
        if self._conn_lost:
            return
        if self._buffer:
            self._buffer.clear()
            self._buffer_size = 0
        self._closing = True
        self._loop.remove_reader(self._fd)
        self._conn_lost += 1
        self._loop.call_soon(self._call_connection_lost, exc)

    def _do_pause_reading(self):
        pass

    def _do_resume_reading(self):
        logger.debug('_ZmqLooplessTransportImpl: Resuming reading')
        self._read_ready()

    def _call_connection_lost(self, exc):
        try:
            super()._call_connection_lost(exc)
        finally:
            self._soon_call = None
