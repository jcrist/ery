import asyncio
import collections
import itertools
import time

from .protocol import Protocol, build_message


class ChannelProtocol(asyncio.BufferedProtocol):
    """
    start -> CM [-> GB [-> BU?]]* [-> ER?] -> CL -> end
    """

    def __init__(self, handler=None, loop=None, **kwargs):
        super().__init__()
        self._handler = handler
        self.transport = None
        self.channel = None
        self._loop = loop or asyncio.get_running_loop()
        self._connection_lost = None
        self._paused = False
        self._drain_waiter = None
        self.protocol = Protocol(self.message_received, **kwargs)

    def connection_made(self, transport):
        self.transport = transport
        self.channel = Channel(self, self.transport, loop=self._loop)

        if self._handler is not None:
            self._loop.create_task(self._handler(self.channel))

    def get_buffer(self, sizehint):
        return self.protocol.get_buffer()

    def message_received(self, kind, args):
        self.channel._append_msg(build_message(kind, args))

    def buffer_updated(self, nbytes):
        self.protocol.buffer_updated(nbytes)

    def eof_received(self):
        self.channel._set_exception(ConnectionResetError())

    def connection_lost(self, exc=None):
        if exc is None:
            exc = ConnectionResetError("Connection closed")
        self.channel._set_exception(exc)
        self._connection_lost = exc

        if self._paused:
            waiter = self._drain_waiter
            if waiter is not None:
                self._drain_waiter = None
                if not waiter.done():
                    waiter.set_exception(exc)

    def pause_writing(self):
        self._paused = True

    def resume_writing(self):
        self._paused = False

        waiter = self._drain_waiter
        if waiter is not None:
            self._drain_waiter = None
            if not waiter.done():
                waiter.set_result(None)

    async def write(self, msg):
        parts = msg.serialize()
        if len(parts) > 1:
            self.transport.writelines(parts)
        else:
            self.transport.write(parts[0])
        if self.transport.is_closing():
            await asyncio.sleep(0, loop=self._loop)
        elif self._paused and not self._connection_lost:
            self._drain_waiter = self._loop.create_future()
            await self._drain_waiter


class Channel(object):
    """A communication channel between two endpoints.

    Use ``new_channel`` to create a channel.
    """

    def __init__(self, protocol, transport, loop):
        self._protocol = protocol
        self._transport = transport
        self._loop = loop

        self._queue = collections.deque()
        self._yield_cycler = itertools.cycle(range(50))
        self._waiter = None
        self._exception = None

    async def __aenter__(self):
        return self

    async def __aexit__(self, typ, value, traceback):
        await self.close()
        if isinstance(value, ConnectionResetError):
            return True

    async def _maybe_yield(self):
        if not next(self._yield_cycler):
            await asyncio.sleep(0, loop=self._loop)

    async def send(self, msg):
        if self._exception is not None:
            raise self._exception
        await self._protocol.write(msg)
        await self._maybe_yield()

    async def __aiter__(self):
        try:
            while True:
                yield await self.recv()
        except ConnectionResetError:
            await self.close()

    async def recv(self):
        """Wait for the next request"""
        if self._exception is not None:
            raise self._exception

        if not self._queue:
            if self._waiter is not None:
                raise RuntimeError(
                    "Channel.recv may only be called by one coroutine at a time"
                )
            self._waiter = self._loop.create_future()
            try:
                await self._waiter
            finally:
                self._waiter = None

        return self._queue.popleft()

    def _close(self):
        if self._transport is not None:
            transport = self._transport
            self._transport = None
            return transport.close()

    async def close(self):
        """Close the channel and release all resources.

        It is invalid to use this channel after closing.

        This method is idempotent.
        """
        self._close()

    def _append_msg(self, msg):
        self._queue.append(msg)

        waiter = self._waiter
        if waiter is not None:
            self._waiter = None
            waiter.set_result(False)

    def _set_exception(self, exc):
        if self._exception:
            return

        self._exception = exc

        waiter = self._waiter
        if waiter is not None:
            self._waiter = None
            if not waiter.cancelled():
                waiter.set_exception(exc)

        self._close()


class RemoteException(Exception):
    """A remote exception that occurs on a different machine"""

    pass


async def new_channel(addr, *, loop=None, timeout=0, **kwargs):
    """Create a new channel.

    Parameters
    ----------
    addr : tuple or str
        The address to connect to.
    loop : AbstractEventLoop, optional
        The event loop to use.
    timeout : float, optional
        Timeout for initial connection to the server.
    **kwargs
        All remaining arguments are forwarded to ``loop.create_connection``.

    Returns
    -------
    channel : Channel
    """
    if loop is None:
        loop = asyncio.get_event_loop()

    if timeout is None:
        timeout = float("inf")

    def factory():
        return ChannelProtocol(loop=loop)

    if isinstance(addr, tuple):
        connect = loop.create_connection
        args = (factory,) + addr
        connect_errors = (ConnectionRefusedError, OSError)
    elif isinstance(addr, str):
        connect = loop.create_unix_connection
        args = (factory, addr)
        connect_errors = FileNotFoundError
    else:
        raise ValueError("Unknown address type: %s" % addr)

    retry_interval = 0.5
    start_time = time.monotonic()
    while True:
        try:
            _, p = await connect(*args, **kwargs)
            break
        except connect_errors:
            if (time.monotonic() - start_time) > timeout:
                raise
            await asyncio.sleep(retry_interval)
            retry_interval = min(30, 1.5 * retry_interval)

    return p.channel


async def start_server(addr, handler, *, loop=None, **kwargs):
    """Start a new server.

    Parameters
    ----------
    addr : tuple or str
        The address to listen at.
    handler : callable
        An async callable. When a new client connects, the handler will be
        called with its respective channel to handle all requests.
    loop : AbstractEventLoop, optional
        The event loop to use.
    **kwargs
        All remaining parameters will be forwarded to ``loop.create_server``.

    Returns
    -------
    server : Server
    """
    if loop is None:
        loop = asyncio.get_event_loop()

    def factory():
        return ChannelProtocol(handler)

    if isinstance(addr, tuple):
        return await loop.create_server(factory, *addr, **kwargs)
    elif isinstance(addr, str):
        return await loop.create_unix_server(factory, addr, **kwargs)
    else:
        raise ValueError("Unknown address type: %s" % addr)
