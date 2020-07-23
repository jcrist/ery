import struct

from . import _lib


Protocol = _lib.Protocol


FLAG_METADATA = 1 << 7
FLAG_BODY = 1 << 6
FLAG_FRAMES = 1 << 5
FLAG_NEXT = 1 << 4
FLAG_COMPLETE = 1 << 3


u32_struct = struct.Struct("<L")
pack_u32 = u32_struct.pack_into

u16_struct = struct.Struct("<H")
pack_u16 = u16_struct.pack_into


def _write(
    kind,
    id=None,
    uint32=None,
    route=None,
    metadata=None,
    body=None,
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
    if body is not None:
        flags |= FLAG_BODY
        if isinstance(body, (list, tuple)):
            flags |= FLAG_FRAMES
            nframes = len(body)
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
    if body is not None:
        if isinstance(body, (list, tuple)):
            pack_u16(header, offset, nframes)
            offset += 2
            for frame in body:
                frame_length = len(frame)
                pack_u32(header, offset, frame_length)
                offset += 4
                if frame_length > 0:
                    chunks.append(frame)
        else:
            body_length = len(body)
            pack_u32(header, offset, body_length)
            offset += 4
            if body_length > 0:
                chunks.append(body)
    if route is not None:
        header[offset:] = route
    return chunks


class Setup(object):
    __slots__ = ("heartbeat", "metadata")

    def __init__(self, heartbeat, metadata=None):
        self.heartbeat = heartbeat
        self.metadata = metadata

    def serialize(self):
        return _write(_lib.KIND_SETUP, uint32=self.heartbeat, metadata=self.metadata,)


class SetupResponse(object):
    __slots__ = ("heartbeat", "metadata")

    def __init__(self, heartbeat, metadata=None):
        self.heartbeat = heartbeat
        self.metadata = metadata

    def serialize(self):
        return _write(
            _lib.KIND_SETUP_RESPONSE, uint32=self.heartbeat, metadata=self.metadata,
        )


class Heartbeat(object):
    __slots__ = ()

    def serialize(self):
        return [_lib.KIND_HEARTBEAT.to_bytes(1, "big", signed=True)]


class Error(object):
    __slots__ = ("id", "code", "metadata", "body")

    def __init__(self, id, code, metadata=None, body=None):
        self.id = id
        self.code = code
        self.metadata = metadata
        self.body = body

    def serialize(self):
        return _write(
            _lib.KIND_ERROR,
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
        return _write(_lib.KIND_CANCEL, id=self.id)


class IncrementWindow(object):
    __slots__ = ("id", "window")

    def __init__(self, id, window):
        self.id = id
        self.window = window

    def serialize(self):
        return _write(_lib.KIND_INCREMENT_WINDOW, id=self.id, uint32=self.window)


class Request(object):
    __slots__ = ("id", "route", "metadata", "body")

    def __init__(self, id, route, metadata=None, body=None):
        self.id = id
        self.route = route
        self.metadata = metadata
        self.body = body

    def serialize(self):
        return _write(
            _lib.KIND_REQUEST,
            id=self.id,
            route=self.route,
            metadata=self.metadata,
            body=self.body,
        )


class Stream(object):
    __slots__ = ("id", "route", "window", "metadata", "body")

    def __init__(self, id, window, route, metadata=None, body=None):
        self.id = id
        self.window = window
        self.route = route
        self.metadata = metadata
        self.body = body

    def serialize(self):
        return _write(
            _lib.KIND_REQUEST_STREAM,
            id=self.id,
            uint32=self.window,
            route=self.route,
            metadata=self.metadata,
            body=self.body,
        )


class Channel(object):
    __slots__ = ("id", "route", "window", "metadata", "body")

    def __init__(self, id, window, route, metadata=None, body=None):
        self.id = id
        self.window = window
        self.route = route
        self.metadata = metadata
        self.body = body

    def serialize(self):
        return _write(
            _lib.KIND_REQUEST_CHANNEL,
            id=self.id,
            uint32=self.window,
            route=self.route,
            metadata=self.metadata,
            body=self.body,
        )


class Payload(object):
    __slots__ = ("id", "metadata", "body", "is_next", "is_complete")

    def __init__(self, id, metadata=None, body=None, is_next=False, is_complete=False):
        self.id = id
        self.metadata = metadata
        self.body = body
        self.is_next = is_next
        self.is_complete = is_complete

    def serialize(self):
        return _write(
            _lib.KIND_PAYLOAD,
            id=self.id,
            metadata=self.metadata,
            body=self.body,
            is_next=self.is_next,
            is_complete=self.is_complete,
        )


dispatch_table = {
    _lib.KIND_SETUP: Setup,
    _lib.KIND_SETUP_RESPONSE: SetupResponse,
    _lib.KIND_HEARTBEAT: Heartbeat,
    _lib.KIND_ERROR: Error,
    _lib.KIND_CANCEL: Cancel,
    _lib.KIND_INCREMENT_WINDOW: IncrementWindow,
    _lib.KIND_REQUEST: Request,
    _lib.KIND_STREAM: Stream,
    _lib.KIND_CHANNEL: Channel,
    _lib.KIND_PAYLOAD: Payload,
}


def build_message(kind, args):
    return dispatch_table[kind](*args)
