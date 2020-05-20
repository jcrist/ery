import asyncio
import collections
import enum
import itertools
import struct
import time


READ_BUFFER_SIZE = 256 * 1024
ERY_PREFIX = b"ery"


class Step(enum.IntEnum):
    PREFIX = 0
    NFRAMES = 1
    LENGTHS = 2
    FRAMES = 3


long_struct = struct.Struct("!L")
unpack_long = long_struct.unpack_from
pack_long = long_struct.pack_into


def dumps(*frames):
    lens = [len(f) for f in frames]
    nframes = len(lens)
    buf = bytearray(b"ery")
    buf.extend(long_struct.pack(nframes))
    for l in lens:
        buf.extend(long_struct.pack(l))
    for f in frames:
        buf.extend(f)
    return buf


class ProtocolError(Exception):
    pass


class ChannelProtocol(asyncio.BufferedProtocol):
    """
    start -> CM [-> GB [-> BU?]]* [-> ER?] -> CL -> end
    """
    def __init__(self, handler=None, buffer_size=READ_BUFFER_SIZE, loop=None):
        super().__init__()
        self._handler = handler
        self.transport = None
        self.channel = None
        self._loop = loop or asyncio.get_running_loop()
        self._connection_lost = None
        self._paused = False
        self._drain_waiter = None
        # Data pulled off header
        self.nframes = None
        self.frame_lengths = None
        # Default IO buffer
        self.default_buffer = bytearray(buffer_size)
        self.default_buffer_start = 0
        self.default_buffer_end = 0
        # Current output buffer, if any
        self.output_buffer = None
        self.output_buffer_end = 0
        # Current state
        self.current_frame_index = None
        self.using_output_buffer = False
        self.outputs = []
        self.step = Step.PREFIX

    def connection_made(self, transport):
        self.transport = transport
        self.channel = Channel(self, self.transport, loop=self._loop)

        if self._handler is not None:
            self._loop.create_task(self._handler(self.channel))

    def get_buffer(self, sizehint):
        if self.step < Step.FRAMES:
            # Still waiting on full header read
            self.using_output_buffer = False
            return memoryview(self.default_buffer)[self.default_buffer_end:]
        else:
            if self.output_buffer is None:
                self.setup_output_buffer()
            to_read = len(self.output_buffer) - self.output_buffer_end
            if to_read >= len(self.default_buffer):
                # More than the max read size is needed for the next output frame
                # Read directly into the output frame
                self.using_output_buffer = True
                return memoryview(self.output_buffer)[self.output_buffer_end:]
            else:
                self.using_output_buffer = False
                return memoryview(self.default_buffer)[self.default_buffer_end:]

    def setup_output_buffer(self):
        assert self.step == Step.FRAMES
        assert self.current_frame_index < len(self.frame_lengths)
        to_read = self.frame_lengths[self.current_frame_index]
        self.output_buffer = bytearray(to_read)
        self.output_buffer_end = 0

    def reset_default_buffer(self):
        start = self.default_buffer_start
        end = self.default_buffer_end
        if start < end:
            self.default_buffer[:(end - start)] = self.default_buffer[start:end]
            self.default_buffer_start = 0
            self.default_buffer_end = end - start
        else:
            self.default_buffer_start = 0
            self.default_buffer_end = 0

    def send_outputs(self):
        self.channel._append_msg(tuple(self.outputs))
        self.outputs = []
        self.current_frame_index = None
        self.step = Step.PREFIX
        self.output_buffer = None
        self.output_buffer_end = 0

    def parse_prefix(self):
        start = self.default_buffer_start
        end = self.default_buffer_end
        if end - start >= 3:
            prefix = self.default_buffer[start:start + 3]
            if prefix != ERY_PREFIX:
                raise ProtocolError("invalid prefix")
            self.default_buffer_start += 3
            self.step = Step.NFRAMES
            return True
        return False

    def parse_nframes(self):
        start = self.default_buffer_start
        end = self.default_buffer_end
        if end - start >= 4:
            self.nframes = unpack_long(self.default_buffer, start)[0]
            self.default_buffer_start += 4
            if self.nframes > 0:
                self.step = Step.LENGTHS
            else:
                self.step = Step.PREFIX
                self.send_outputs()
            return True
        return False

    def parse_frame_lengths(self):
        start = self.default_buffer_start
        end = self.default_buffer_end
        if end - start >= self.nframes * 4:
            self.frame_lengths = [
                unpack_long(self.default_buffer, start + i * 4)[0]
                for i in range(self.nframes)
            ]
            # nframes check should prevent us from getting here
            assert self.frame_lengths
            self.default_buffer_start += self.nframes * 4
            self.step = Step.FRAMES
            self.current_frame_index = 0
            return True
        return False

    def parse_frames(self):
        if not self.using_output_buffer:
            start = self.default_buffer_start
            end = self.default_buffer_end

            if self.output_buffer is None:
                self.setup_output_buffer()

            offset = self.output_buffer_end
            available = end - start
            needed = len(self.output_buffer) - offset
            ncopy = min(available, needed)

            self.output_buffer[offset:offset + ncopy] = self.default_buffer[start:start + ncopy]
            self.output_buffer_end += ncopy
            self.default_buffer_start += ncopy

        if self.output_buffer_end == len(self.output_buffer):
            # We've filled this output buffer
            self.outputs.append(self.output_buffer)
            self.output_buffer = None
            self.current_frame_index += 1
            # If a msg ends with some empty frames, we should eagerly detect
            # these to avoid waiting for the next message to finish them.
            while (
                (self.current_frame_index < len(self.frame_lengths))
                and self.frame_lengths[self.current_frame_index] == 0
            ):
                self.outputs.append(bytearray(0))
                self.current_frame_index += 1
            if self.current_frame_index == len(self.frame_lengths):
                self.send_outputs()

        if self.using_output_buffer:
            return False
        elif self.default_buffer_start == self.default_buffer_end:
            return False
        else:
            return True

    def advance(self):
        if self.step == Step.PREFIX:
            return self.parse_prefix()
        elif self.step == Step.NFRAMES:
            return self.parse_nframes()
        elif self.step == Step.LENGTHS:
            return self.parse_frame_lengths()
        elif self.step == Step.FRAMES:
            return self.parse_frames()

    def buffer_updated(self, nbytes):
        if nbytes == 0:
            return

        if self.using_output_buffer:
            self.output_buffer_end += nbytes
        else:
            self.default_buffer_end += nbytes

        while self.advance():
            pass

        self.reset_default_buffer()

    def eof_received(self):
        self.channel._set_exception(ConnectionResetError())

    def connection_lost(self, exc=None):
        if exc is None:
            exc = ConnectionResetError('Connection closed')
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

    def _encode(self, frames):
        lens = [len(f) for f in frames]
        nframes = len(lens)
        buf = bytearray(b"ery")
        buf.extend(long_struct.pack(nframes))
        for l in lens:
            buf.extend(long_struct.pack(l))
        return buf

    async def write(self, *frames):
        header = self._encode(frames)
        self.transport.write(header)
        self.transport.writelines(frames)
        await self.drain()

    async def drain(self):
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

    async def send(self, *frames):
        if self._exception is not None:
            raise self._exception
        await self._protocol.write(*frames)
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
                    'Channel.recv may only be called by one coroutine at a time'
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

    def _append_msg(self, frames):
        self._queue.append(frames)

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
        timeout = float('inf')

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
        raise ValueError('Unknown address type: %s' % addr)

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
        raise ValueError('Unknown address type: %s' % addr)
