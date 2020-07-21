import asyncio
import collections
import itertools
import time

from .protocol import Protocol, build_message, Request, Payload


class ConnectionProtocol(asyncio.BufferedProtocol):
    """
    start -> CM [-> GB [-> BU?]]* [-> ER?] -> CL -> end
    """

    def __init__(self, on_connection=None, loop=None, **kwargs):
        super().__init__()
        self._on_connection = on_connection
        self.transport = None
        self.connection = None
        self._loop = loop or asyncio.get_running_loop()
        self._connection_lost = None
        self._paused = False
        self._drain_waiter = None
        self.protocol = Protocol(self.message_received, **kwargs)

    def connection_made(self, transport):
        self.transport = transport
        self.connection = Connection(self, self.transport, loop=self._loop)

        if self._on_connection is not None:
            self._on_connection(self.connection)

    def get_buffer(self, sizehint):
        return self.protocol.get_buffer()

    def message_received(self, kind, args):
        self.connection._append_msg(build_message(kind, args))

    def buffer_updated(self, nbytes):
        self.protocol.buffer_updated(nbytes)

    def eof_received(self):
        self.connection._set_exception(ConnectionResetError())

    def connection_lost(self, exc=None):
        if exc is None:
            exc = ConnectionResetError("Connection closed")
        self.connection._set_exception(exc)
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


class Connection(object):
    """A connection between two endpoints.

    Use ``connect`` to create a connection.
    """

    def __init__(self, protocol, transport, loop):
        self._protocol = protocol
        self._transport = transport
        self._loop = loop

        self._id_iter = itertools.count()
        self._active_reqs = {}
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

    async def request(self, route, metadata=None, body=None):
        """Send a request message and wait for a response"""
        if self._exception is not None:
            raise self._exception

        msg_id = next(self._id_iter)
        msg = Request(msg_id, route, metadata=metadata, body=body)
        reply = self._active_reqs[msg_id] = self._loop.create_future()
        await self._protocol.write(msg)
        return await reply

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
        """Wait for the next message"""
        if self._exception is not None:
            raise self._exception

        if not self._queue:
            if self._waiter is not None:
                raise RuntimeError(
                    "Connection.recv may only be called by one coroutine at a time"
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
        """Close the connection and release all resources.

        It is invalid to use this connection after closing.

        This method is idempotent.
        """
        self._close()
        try:
            futs = self._active_reqs.values()
            await asyncio.gather(*futs, return_exceptions=True)
        except asyncio.CancelledError:
            pass

    def _append_msg(self, msg):
        if isinstance(msg, Request):
            self._queue.append(msg)

            waiter = self._waiter
            if waiter is not None:
                self._waiter = None
                waiter.set_result(False)
        elif isinstance(msg, Payload):
            message = self._active_reqs.pop(msg.id, None)
            if message is not None and not message.done():
                message.set_result(msg)
        else:
            self._set_exception(RuntimeError("Invalid message %s" % msg))

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


async def connect(addr, *, loop=None, timeout=0, **kwargs):
    """Create a new connection.

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
    connection : Connection
    """
    if loop is None:
        loop = asyncio.get_event_loop()

    if timeout is None:
        timeout = float("inf")

    def factory():
        return ConnectionProtocol(loop=loop)

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

    return p.connection
