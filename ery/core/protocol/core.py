import asyncio
import enum
import struct

from . import _lib


class ErrorCode(enum.IntEnum):
    SETUP_INVALID = 0x0001
    SETUP_UNSUPPORTED = 0x0002
    SETUP_REJECTED = 0x0003
    CONNECTION_ERROR = 0x0004
    CONNECTION_CLOSE = 0x0005
    REQUEST_ERROR = 0x0006
    REQUEST_REJECTED = 0x0007
    REQUEST_CANCELLED = 0x0008


FLAG_METADATA = 1 << 7
FLAG_BODY = 1 << 6
FLAG_FRAMES = 1 << 5
FLAG_NEXT = 1 << 4
FLAG_COMPLETE = 1 << 3


u32_struct = struct.Struct("<L")
pack_u32 = u32_struct.pack_into

u16_struct = struct.Struct("<H")
pack_u16 = u16_struct.pack_into


def serialize_msg(
    kind,
    id=None,
    uint32=None,
    route=None,
    metadata=None,
    data=None,
    is_next=False,
    is_complete=False,
):
    nframes = route_length = None
    length = 2
    if id is not None:
        length += 4
    if uint32 is not None:
        length += 4
    flags = 0
    if route is not None:
        route_length = len(route)
        length += 2 + route_length
    if metadata is not None:
        flags |= FLAG_METADATA
        length += 4
    if data is not None:
        flags |= FLAG_BODY
        if isinstance(data, (list, tuple)):
            flags |= FLAG_FRAMES
            nframes = len(data)
            length += 2 + 4 * nframes
        else:
            length += 4
    if is_next:
        flags |= FLAG_NEXT
    if is_complete:
        flags |= FLAG_COMPLETE

    header = bytearray(length)
    header[0] = kind
    header[1] = flags
    chunks = [header]
    offset = 2
    if id is not None:
        pack_u32(header, offset, id)
        offset += 4
    if route is not None:
        pack_u16(header, offset, route_length)
        offset += 2
    if uint32 is not None:
        pack_u32(header, offset, uint32)
        offset += 4
    if metadata is not None:
        pack_u32(header, offset, len(metadata))
        offset += 4
        chunks.append(metadata)
    if data is not None:
        if isinstance(data, (list, tuple)):
            pack_u16(header, offset, nframes)
            offset += 2
            for frame in data:
                frame_length = len(frame)
                pack_u32(header, offset, frame_length)
                offset += 4
                if frame_length > 0:
                    chunks.append(frame)
        else:
            data_length = len(data)
            pack_u32(header, offset, data_length)
            offset += 4
            if data_length > 0:
                chunks.append(data)
    if route is not None:
        header[offset:] = route
    return chunks


class ConnectionProtocol(asyncio.BufferedProtocol):
    def __init__(self, callbacks, **kwargs):
        super().__init__()

        self._cb = callbacks(self)
        self._msg_handlers = {
            _lib.KIND_SETUP: self._cb.on_msg_setup,
            _lib.KIND_SETUP_RESPONSE: self._cb.on_msg_setup_response,
            _lib.KIND_HEARTBEAT: self._cb.on_msg_heartbeat,
            _lib.KIND_ERROR: self._cb.on_msg_error,
            _lib.KIND_CANCEL: self._cb.on_msg_cancel,
            _lib.KIND_INCREASE_QUOTA: self._cb.on_msg_increase_quota,
            _lib.KIND_REQUEST: self._cb.on_msg_request,
            _lib.KIND_STREAM: self._cb.on_msg_stream,
            _lib.KIND_CHANNEL: self._cb.on_msg_channel,
            _lib.KIND_PAYLOAD: self._cb.on_msg_payload,
        }
        self._protocol = _lib.Protocol(self.message_received, **kwargs)

        self._transport = None

        self._loop = asyncio.get_running_loop()
        self._connection_exc = None
        self._paused = False
        self._drain_waiter = None

    def connection_made(self, transport):
        self._transport = transport
        self._cb.on_connection_made()

    def get_buffer(self, sizehint):
        return self._protocol.get_buffer()

    def message_received(self, kind, args):
        self._msg_handlers[kind](*args)

    def buffer_updated(self, nbytes):
        self._protocol.buffer_updated(nbytes)

    def connection_lost(self, exc=None):
        if exc is None:
            exc = ConnectionResetError("Connection closed")
        self._cb.on_connection_lost(exc)
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

    async def wait_closed(self):
        await self._cb.wait_closed()

    async def write(self, parts):
        if self._connection_exc:
            raise self._connection_exc
        if len(parts) > 1:
            self._transport.writelines(parts)
        else:
            self._transport.write(parts[0])
        if self._transport.is_closing():
            await asyncio.sleep(0, loop=self._loop)
        elif self._paused:
            self._drain_waiter = self._loop.create_future()
            await self._drain_waiter

    async def send_setup(self, heartbeat, metadata):
        parts = serialize_msg(_lib.KIND_SETUP, uint32=heartbeat, metadata=metadata)
        await self.write(parts)

    async def send_setup_response(self, heartbeat, metadata):
        parts = serialize_msg(_lib.KIND_SETUP_RESPONSE, uint32=heartbeat, metadata=metadata)
        await self.write(parts)

    async def send_heartbeat(self):
        parts = [_lib.KIND_HEARTBEAT.to_bytes(1, "big", signed=True)]
        await self.write(parts)

    async def send_error(self, id, code, data):
        parts = serialize_msg(_lib.KIND_ERROR, id=id, uint32=code, data=data)
        await self.write(parts)

    async def send_cancel(self, id):
        parts = serialize_msg(_lib.KIND_CANCEL, id=id)
        await self.write(parts)

    async def send_increase_quota(self, id, quota):
        parts = serialize_msg(_lib.KIND_INCREASE_QUOTA, id=id, uint32=quota)
        await self.write(parts)

    async def send_request(self, id, route, data=None):
        parts = serialize_msg(_lib.KIND_REQUEST, id=id, route=route, data=data)
        await self.write(parts)

    async def send_stream(self, id, route, quota, data=None):
        parts = serialize_msg(
            _lib.KIND_REQUEST_STREAM, id=id, route=route, uint32=quota, data=data,
        )
        await self.write(parts)

    async def send_channel(self, id, route, quota, data=None):
        parts = serialize_msg(
            _lib.KIND_REQUEST_CHANNEL, id=id, route=route, uint32=quota, data=data,
        )
        await self.write(parts)

    async def send_payload(self, id, data=None, is_next=False, is_complete=False):
        parts = serialize_msg(
            _lib.KIND_PAYLOAD,
            id=id,
            data=data,
            is_next=is_next,
            is_complete=is_complete,
        )
        await self.write(parts)
