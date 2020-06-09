import enum
import struct

from ._lib import Protocol  # noqa


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
pack_u32 = u32_struct.pack_into

u16_struct = struct.Struct("<H")
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


dispatch_table = {
    Kind.SETUP: Setup,
    Kind.SETUP_RESPONSE: SetupResponse,
    Kind.HEARTBEAT: Heartbeat,
    Kind.ERROR: Error,
    Kind.CANCEL: Cancel,
    Kind.INCREMENT_WINDOW: IncrementWindow,
    Kind.REQUEST: Request,
    Kind.NOTICE: Notice,
    Kind.REQUEST_STREAM: RequestStream,
    Kind.REQUEST_CHANNEL: RequestChannel,
    Kind.PAYLOAD: Payload,
}


def build_message(kind, args):
    return dispatch_table[kind](*args)
