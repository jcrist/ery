import array
import enum
import struct


READ_BUFFER_SIZE = 256 * 1024

MAX_KIND = 11

FLAG_METADATA = 1 << 7
FLAG_BODY = 1 << 6
FLAG_FRAMES = 1 << 5
FLAG_NEXT = 1 << 4
FLAG_COMPLETE = 1 << 3


class Kind(enum.IntEnum):
    SETUP = 1
    SETUP_RESPONSE = 2
    HEARTBEAT = 3
    ERROR = 4
    CANCEL = 5
    INCREMENT_WINDOW = 6
    REQUEST = 7
    NOTICE = 8
    REQUEST_STREAM = 9
    REQUEST_CHANNEL = 10
    PAYLOAD = 11


class Op(enum.IntEnum):
    KIND = 0
    FLAGS = 1
    HEARTBEAT = 2
    ID = 3
    CODE = 4
    WINDOW = 5
    ROUTE_LENGTH = 6
    METADATA_LENGTH = 7
    BODY_LENGTH = 8
    NFRAMES = 9
    FRAME_LENGTHS = 10
    ROUTE = 11
    METADATA = 12
    BODY = 13
    FRAMES = 14


u32_struct = struct.Struct("<L")
unpack_u32 = u32_struct.unpack_from
pack_u32 = u32_struct.pack_into

u16_struct = struct.Struct("<H")
unpack_u16 = u16_struct.unpack_from
pack_u16 = u16_struct.pack_into


def pack_nbytes(buf, offset, bytes):
    buf[offset : len(bytes) + offset] = bytes


def _write(
    kind,
    id=None,
    uint32=None,
    route=None,
    metadata=None,
    body=None,
    frames=None,
    is_next=False,
    is_complete=False,
):
    length = 2
    if id is not None:
        length += 4
    if uint32 is not None:
        length += 4
    flags = 0
    if route is not None:
        length += 2 + len(route)
    if metadata is not None:
        flags |= FLAG_METADATA
        length += 4
    if body is not None:
        flags |= FLAG_BODY
        length += 4
    elif frames is not None:
        flags |= FLAG_BODY
        if len(frames) == 1:
            length += 4
        else:
            flags |= FLAG_FRAMES
            length += 2 + 4 * len(frames)
    if is_next:
        flags &= FLAG_NEXT
    if is_complete:
        flags &= FLAG_COMPLETE

    header = bytearray(length)
    header[0] = kind
    header[1] = flags
    chunks = [header]
    offset = 2
    if id is not None:
        pack_u32(header, offset, id)
        offset += 4
    if route is not None:
        pack_u16(header, offset, len(route))
        offset += 2
    if uint32 is not None:
        pack_u32(header, offset, uint32)
        offset += 4
    if metadata is not None:
        pack_u32(header, offset, len(metadata))
        offset += 4
        chunks.append(metadata)
    if body is not None:
        pack_u32(header, offset, len(body))
        if len(body) > 0:
            chunks.append(body)
    elif frames is not None:
        if len(frames) > 1:
            pack_u16(header, offset, len(frames))
            offset += 2
        for f in frames:
            pack_u32(header, offset, len(f))
            offset += 4
            if len(f) > 0:
                chunks.append(f)
    if route is not None:
        pack_nbytes(header, offset, route)
    return chunks


class Setup(object):
    __slots__ = ("heartbeat", "metadata", "body")

    def __init__(self, heartbeat, metadata=None, body=None):
        self.heartbeat = heartbeat
        self.metadata = metadata
        self.body = body

    def serialize(self):
        return _write(
            Kind.SETUP, uint32=self.heartbeat, metadata=self.metadata, body=self.body
        )


class SetupResponse(object):
    __slots__ = ("heartbeat", "metadata", "body")

    def __init__(self, heartbeat, metadata=None, body=None):
        self.heartbeat = heartbeat
        self.metadata = metadata
        self.body = body

    def serialize(self):
        return _write(
            Kind.SETUP_RESPONSE,
            uint32=self.heartbeat,
            metadata=self.metadata,
            body=self.body,
        )


class Heartbeat(object):
    __slots__ = ()

    def serialize(self):
        return [Kind.HEARTBEAT.to_bytes(1, "big", signed=True)]


class Error(object):
    __slots__ = ("id", "code", "metadata", "body")

    def __init__(self, id, code, metadata=None, body=None):
        self.id = id
        self.code = code
        self.metadata = metadata
        self.body = body

    def serialize(self):
        return _write(
            Kind.ERROR,
            id=self.id,
            uint32=self.code,
            metadata=self.metadata,
            body=self.body,
        )


class Cancel(object):
    __slots__ = ("id",)

    def __init__(self, id):
        self.id = id

    def serialize(self):
        return _write(Kind.CANCEL, id=self.id)


class IncrementWindow(object):
    __slots__ = ("id", "window")

    def __init__(self, id, window):
        self.id = id
        self.window = window

    def serialize(self):
        return _write(Kind.INCREMENT_WINDOW, id=self.id, uint32=self.window)


class Request(object):
    __slots__ = ("id", "route", "metadata", "frames")

    def __init__(self, id, route, metadata=None, frames=None):
        self.id = id
        self.route = route
        self.metadata = metadata
        self.frames = frames

    def serialize(self):
        return _write(
            Kind.REQUEST,
            id=self.id,
            route=self.route,
            metadata=self.metadata,
            frames=self.frames,
        )


class Notice(object):
    __slots__ = ("route", "metadata", "frames")

    def __init__(self, route, metadata=None, frames=None):
        self.route = route
        self.metadata = metadata
        self.frames = frames

    def serialize(self):
        return _write(
            Kind.NOTICE, route=self.route, metadata=self.metadata, frames=self.frames
        )


class RequestStream(object):
    __slots__ = ("id", "route", "window", "metadata", "frames")

    def __init__(self, id, window, route, metadata=None, frames=None):
        self.id = id
        self.window = window
        self.route = route
        self.metadata = metadata
        self.frames = frames

    def serialize(self):
        return _write(
            Kind.REQUEST_STREAM,
            id=self.id,
            uint32=self.window,
            route=self.route,
            metadata=self.metadata,
            frames=self.frames,
        )


class RequestChannel(object):
    __slots__ = ("id", "route", "window", "metadata", "frames")

    def __init__(self, id, window, route, metadata=None, frames=None):
        self.id = id
        self.window = window
        self.route = route
        self.metadata = metadata
        self.frames = frames

    def serialize(self):
        return _write(
            Kind.REQUEST_CHANNEL,
            id=self.id,
            uint32=self.window,
            route=self.route,
            metadata=self.metadata,
            frames=self.frames,
        )


class Payload(object):
    __slots__ = ("id", "metadata", "frames", "is_next", "is_complete")

    def __init__(
        self, id, metadata=None, frames=None, is_next=False, is_complete=False
    ):
        self.id = id
        self.metadata = metadata
        self.frames = frames
        self.is_next = is_next
        self.is_complete = is_complete

    def serialize(self):
        return _write(
            Kind.PAYLOAD,
            id=self.id,
            metadata=self.metadata,
            frames=self.frames,
            is_next=self.is_next,
            is_complete=self.is_complete,
        )


class ProtocolError(Exception):
    pass


HANDLERS = {}


def handler(op):
    def f(func):
        HANDLERS[op] = func
        return func

    return f


class Protocol(object):
    """A sans-io protocol for ery"""

    def __init__(self, buffer_size=READ_BUFFER_SIZE, frame_lengths_size=512):
        # Default IO buffer
        self.default_buffer = bytearray(buffer_size)
        self.default_buffer_start = 0
        self.default_buffer_end = 0
        # Current state
        self.default_frame_lengths_buffer = array.array("L", [0] * frame_lengths_size)
        self.messages = []
        self.reset_message_state()

    def get_buffer(self):
        """Get a buffer to write bytes into"""
        if self.op < Op.FRAMES:
            # Still waiting on full header read
            self.using_frame_buffer = False
            return memoryview(self.default_buffer)[self.default_buffer_end :]
        else:
            if self.frame_buffer is None:
                self.setup_frame_buffer()
            to_read = len(self.frame_buffer) - self.frame_buffer_index
            if to_read >= len(self.default_buffer):
                # More than the max read size is needed for the next output frame
                # Read directly into the output frame
                self.using_frame_buffer = True
                return memoryview(self.frame_buffer)[self.frame_buffer_index :]
            else:
                self.using_frame_buffer = False
                return memoryview(self.default_buffer)[self.default_buffer_end :]

    def buffer_updated(self, nbytes):
        """Notify that `nbytes` of the buffer have been written to."""
        if nbytes == 0:
            return

        if self.using_frame_buffer:
            self.frame_buffer_index += nbytes
        else:
            self.default_buffer_end += nbytes

        while self.advance():
            pass

        self.reset_default_buffer()
        if self.messages:
            out = self.messages
            self.messages = []
        else:
            out = []
        return out

    def setup_frame_buffer(self):
        assert self.frame_index < self.nframes
        to_read = self.frame_lengths[self.frame_index]
        self.frame_buffer = bytearray(to_read)
        self.frame_buffer_index = 0

    def reset_default_buffer(self):
        start = self.default_buffer_start
        end = self.default_buffer_end
        if start < end:
            self.default_buffer[: (end - start)] = self.default_buffer[start:end]
            self.default_buffer_start = 0
            self.default_buffer_end = end - start
        else:
            self.default_buffer_start = 0
            self.default_buffer_end = 0

    def reset_message_state(self):
        self.kind = None
        self.flags = 0
        self.id = 0
        self.route_length = 0
        self.route = None
        self.route_index = 0
        self.metadata_length = 0
        self.metadata = None
        self.metadata_index = 0
        self.nframes = 0
        self.frame_lengths = None
        self.frame_lengths_index = 0
        self.frames = []
        self.frame_index = 0
        self.frame_buffer = None
        self.frame_buffer_index = 0
        self.op = Op.KIND

    def message_completed(self, msg):
        self.messages.append(msg)
        self.reset_message_state()

    def parse_uint8(self):
        start = self.default_buffer_start
        end = self.default_buffer_end
        if end - start >= 1:
            out = self.default_buffer[start]
            self.default_buffer_start += 1
            return True, out
        return False, None

    def parse_uint16(self):
        start = self.default_buffer_start
        end = self.default_buffer_end
        if end - start >= 2:
            out = unpack_u16(self.default_buffer, start)[0]
            self.default_buffer_start += 2
            return True, out
        return False, None

    def parse_uint32(self):
        start = self.default_buffer_start
        end = self.default_buffer_end
        if end - start >= 4:
            out = unpack_u32(self.default_buffer, start)[0]
            self.default_buffer_start += 4
            return True, out
        return False, None

    def parse_nbytes(self, buf, index, length):
        start = self.default_buffer_start
        end = self.default_buffer_end

        available = end - start
        needed = length - index
        ncopy = min(available, needed)
        if available:
            buf[index : index + ncopy] = self.default_buffer[start : start + ncopy]
            self.default_buffer_start += ncopy
        ok = ncopy == needed
        return ok, ncopy

    def parse_kind(self):
        ok, kind = self.parse_uint8()
        if not ok:
            return False

        if kind > MAX_KIND:
            raise ProtocolError("Invalid kind %d" % self.kind)
        self.kind = Kind(kind)
        if self.kind == Op.HEARTBEAT:
            self.message_completed(Heartbeat())
        else:
            self.op = Op.FLAGS
        return True

    @handler(Op.FLAGS)
    def parse_flags(self):
        ok, flags = self.parse_uint8()
        if not ok:
            return False
        self.flags = flags
        return True

    @handler(Op.ID)
    def parse_id(self):
        ok, id = self.parse_uint32()
        if not ok:
            return False
        self.id = id
        return True

    @handler(Op.ROUTE_LENGTH)
    def parse_route_length(self):
        ok, length = self.parse_uint16()
        if not ok:
            return False
        self.route_length = length
        return True

    @handler(Op.METADATA_LENGTH)
    def parse_metadata_length(self):
        if self.flags & FLAG_METADATA:
            ok, length = self.parse_uint32()
            if not ok:
                return False
            self.metadata_length = length
        else:
            self.metadata_length = 0
        return True

    @handler(Op.NFRAMES)
    def parse_nframes(self):
        if self.flags & FLAG_BODY:
            if self.flags & FLAG_FRAMES:
                ok, nframes = self.parse_uint16()
                if not ok:
                    return False
                self.nframes = nframes
            else:
                self.nframes = 1
        else:
            self.nframes = 0
        return True

    @handler(Op.FRAME_LENGTHS)
    def parse_frame_lengths(self):
        if self.nframes > 0:
            if self.frame_lengths is None:
                if self.nframes > len(self.default_frame_lengths_buffer):
                    self.frame_lengths = array.array("L", [0] * self.nframes)
                else:
                    self.frame_lengths = self.default_frame_lengths_buffer
            while self.frame_lengths_index < self.nframes:
                ok, val = self.parse_uint32()
                if not ok:
                    return False
                self.frame_lengths[self.frame_lengths_index] = val
                self.frame_lengths_index += 1
        return True

    @handler(Op.ROUTE)
    def parse_route(self):
        if self.route is None:
            self.route = bytearray(self.route_length)
            self.route_index = 0
        ok, ncopy = self.parse_nbytes(self.route, self.route_index, self.route_length)
        self.route_index += ncopy
        if not ok:
            return False
        return True

    @handler(Op.METADATA)
    def parse_metadata(self):
        if self.flags & FLAG_METADATA:
            if self.metadata is None:
                self.metadata = bytearray(self.metadata_length)
                self.metadata_index = 0
            ok, ncopy = self.parse_nbytes(
                self.metadata, self.metadata_index, self.metadata_length
            )
            self.metadata_index += ncopy
            if not ok:
                return False
        else:
            self.metadata = None
        return True

    def parse_frame(self):
        if self.frame_buffer is None:
            self.setup_frame_buffer()
        frame_length = self.frame_lengths[self.frame_index]
        if frame_length > 0:
            ok, ncopy = self.parse_nbytes(
                self.frame_buffer, self.frame_buffer_index, frame_length
            )
            self.frame_buffer_index += ncopy
        else:
            ok = True
        return ok

    @handler(Op.FRAMES)
    def parse_frames(self):
        while self.frame_index < self.nframes:
            if self.using_frame_buffer:
                ok = self.frame_buffer_index == len(self.frame_buffer)
                self.using_frame_buffer = False
            else:
                ok = self.parse_frame()
            if not ok:
                return False

            # We've filled this output buffer
            self.frames.append(self.frame_buffer)
            self.frame_buffer = None
            self.frame_index += 1
        return True

    def parse_msg(self, ops):
        for op in ops:
            if op < self.op:
                continue
            ok = HANDLERS[op](self)
            if not ok:
                self.op = op
                return False
        return True

    def parse_request(self):
        ok = self.parse_msg(
            [
                Op.FLAGS,
                Op.ID,
                Op.ROUTE_LENGTH,
                Op.METADATA_LENGTH,
                Op.NFRAMES,
                Op.FRAME_LENGTHS,
                Op.ROUTE,
                Op.METADATA,
                Op.FRAMES,
            ]
        )
        if ok:
            self.message_completed(
                Request(
                    id=self.id,
                    route=self.route,
                    metadata=self.metadata,
                    frames=self.frames,
                )
            )
        return ok

    def parse_payload(self):
        ok = self.parse_msg(
            [
                Op.FLAGS,
                Op.ID,
                Op.METADATA_LENGTH,
                Op.NFRAMES,
                Op.FRAME_LENGTHS,
                Op.METADATA,
                Op.FRAMES,
            ]
        )
        if ok:
            self.message_completed(
                Payload(
                    id=self.id,
                    metadata=self.metadata,
                    frames=self.frames,
                    is_next=self.flags & FLAG_NEXT,
                    is_complete=self.flags & FLAG_COMPLETE,
                )
            )
        return ok

    def advance(self):
        if self.op == Op.KIND:
            return self.parse_kind()
        else:
            if self.kind == Kind.REQUEST:
                return self.parse_request()
            elif self.kind == Kind.PAYLOAD:
                return self.parse_payload()
