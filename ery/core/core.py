import enum
import struct


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
        if isinstance(route, str):
            route = route.encode()
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
