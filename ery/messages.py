from collections import namedtuple


Setup = namedtuple("Setup", ["heartbeat", "metadata", "body"])
SetupResponse = namedtuple("SetupResponse", ["heartbeat", "metadata", "body"])

Heartbeat = namedtuple("Heartbeat", [])

Error = namedtuple("Error", ["id", "code", "metadata", "body"])

class Request(object):
    __slots__ = ("id", "route", "metadata", "frames")

    def __init__(self, id, route, metadata=None, frames=None):
        self.id = id
        self.route = route
        self.metadata = metadata
        self.frames = frames

    def to_chunks(self):
        flags = 0
        header_length = 1 + 1 + 4 + 2  # kind | flags | id | route_length

        route_length = len(self.route)
        header_length += route_length

        if self.metadata is None:
            metadata_length = 0
        else:
            flags |= FLAG_METADATA
            metadata_length = len(self.metadata)
            header_length += 4

        if self.frames is None:
            nframes = 0
        else:
            flags |= FLAG_BODY
            nframes = len(self.frames)
            if nframes == 1:
                header_length += 4
            elif nframes > 1:
                flags |= FLAG_FRAMES
                header_length += 2 + 4 * len(self.frames)

        head = bytearray(header_length)
        chunks = [head]
        head[0] = Kind.REQUEST
        head[1] = flags
        offset = 2
        pack_u32(head, offset, self.id)
        offset += 4
        pack_u16(head, offset, route_length)
        offset += 2
        if self.metadata is not None:
            pack_u32(head, offset, metadata_length)
            offset += 4
        if self.frames is not None:
            if nframes > 1:
                pack_u16(head, offset, nframes)
                offset += 2
            for f in self.frames:
                pack_u32(head, offset, len(f))
                offset += 4
        pack_nbytes(head, offset, self.route)
        offset += route_length
        assert offset == header_length
        if self.metadata is not None:
            chunks.append(self.metadata)
        if self.frames is not None:
            chunks.extend(self.frames)
        return chunks


Payload = namedtuple("Payload", ["id", "is_next", "is_complete", "metadata", "frames"])


from .protocol import pack_u32, pack_u16, pack_nbytes, Kind, FLAG_METADATA, FLAG_BODY, FLAG_FRAMES
