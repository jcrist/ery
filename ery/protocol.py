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
    METADATA_LENGTH = 6
    METADATA = 7
    BODY_LENGTH = 8
    BODY = 9
    ROUTE_LENGTH = 10
    ROUTE = 11
    NFRAMES = 12
    FRAME_LENGTHS = 13
    FRAMES = 14


u32_struct = struct.Struct("!L")
unpack_u32 = u32_struct.unpack_from
pack_u32 = u32_struct.pack_into

u16_struct = struct.Struct("!H")
unpack_u16 = u16_struct.unpack_from
pack_u16 = u16_struct.pack_into

def pack_nbytes(buf, offset, bytes):
    buf[offset:len(bytes) + offset] = bytes


class ProtocolError(Exception):
    pass


class Protocol(object):
    """A sans-io protocol for ery"""

    def __init__(self, buffer_size=READ_BUFFER_SIZE, frame_lengths_size=512):
        # Default IO buffer
        self.default_buffer = bytearray(buffer_size)
        self.default_buffer_start = 0
        self.default_buffer_end = 0
        # Current state
        self.default_frame_lengths_buffer = array.array("L", [0]*frame_lengths_size)
        self.messages = []
        self.reset_message_state()

    def get_buffer(self):
        """Get a buffer to write bytes into"""
        if self.op < Op.FRAMES:
            # Still waiting on full header read
            self.using_output_buffer = False
            return memoryview(self.default_buffer)[self.default_buffer_end :]
        else:
            if self.output_buffer is None:
                self.setup_output_buffer()
            to_read = len(self.output_buffer) - self.output_buffer_end
            if to_read >= len(self.default_buffer):
                # More than the max read size is needed for the next output frame
                # Read directly into the output frame
                self.using_output_buffer = True
                return memoryview(self.output_buffer)[self.output_buffer_end :]
            else:
                self.using_output_buffer = False
                return memoryview(self.default_buffer)[self.default_buffer_end :]

    def buffer_updated(self, nbytes):
        """Notify that `nbytes` of the buffer have been written to."""
        if nbytes == 0:
            return

        if self.using_output_buffer:
            self.output_buffer_end += nbytes
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

    def setup_output_buffer(self):
        assert self.op == Op.FRAMES
        assert self.frame_index < self.nframes
        to_read = self.frame_lengths[self.frame_index]
        self.output_buffer = bytearray(to_read)
        self.output_buffer_end = 0

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
        self.output_buffer = None
        self.output_buffer_end = 0
        self.op = Op.KIND

    def message_completed(self):
        # Construct message
        if self.kind == Kind.REQUEST:
            msg = messages.Request(
                id=self.id,
                route=self.route,
                metadata=self.metadata,
                frames=self.frames
            )
        elif self.kind == Kind.PAYLOAD:
            msg = messages.Request(
                id=self.id,
                is_next=bool(self.flags & FLAG_NEXT),
                is_complete=bool(self.flags & FLAG_COMPLETE),
                metadata=self.metadata,
                frames=self.frames
            )
        elif self.kind == Kind.HEARTBEAT:
            msg = messages.Heartbeat()
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
            buf[index : index + ncopy] = self.default_buffer[
                start : start + ncopy
            ]
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
        if kind == Kind.HEARTBEAT:
            self.message_completed()
            return False
        else:
            self.op = Op.FLAGS
            return True

    def parse_flags(self):
        ok, flags = self.parse_uint8()
        if not ok:
            return False
        self.flags = flags
        self.op = Op.ID
        return True

    def parse_id(self):
        ok, id = self.parse_uint32()
        if not ok:
            return False
        self.id = id
        if self.kind == Kind.REQUEST:
            self.op = Op.ROUTE_LENGTH
        elif self.kind == Kind.PAYLOAD:
            self.op = Op.METADATA_LENGTH
        return True

    def parse_route_length(self):
        ok, length = self.parse_uint16()
        if not ok:
            return False
        self.route_length = length
        self.op = Op.METADATA_LENGTH
        return True

    def parse_metadata_length(self):
        if self.flags & FLAG_METADATA:
            ok, length = self.parse_uint32()
            if not ok:
                return False
            self.metadata_length = length
        else:
            self.metadata_length = 0
        if self.kind == Kind.REQUEST:
            self.op = Op.NFRAMES
        elif self.kind == Kind.PAYLOAD:
            self.op = Op.NFRAMES
        return True

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
        self.op = Op.FRAME_LENGTHS
        return True

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
        if self.kind == Kind.REQUEST:
            self.op = Op.ROUTE
        elif self.kind == Kind.PAYLOAD:
            self.op = Op.METADATA
        return True

    def parse_route(self):
        if self.route is None:
            self.route = bytearray(self.route_length)
            self.route_index = 0
        ok, ncopy = self.parse_nbytes(self.route, self.route_index, self.route_length)
        if not ok:
            return False
        self.op = Op.METADATA
        return True

    def parse_metadata(self):
        if self.flags & FLAG_METADATA:
            if self.metadata is None:
                self.metadata = bytearray(self.metadata_length)
                self.metadata_index = 0
            ok, ncopy = self.parse_nbytes(self.metadata, self.metadata_index, self.metadata_length)
            self.metadata_index += ncopy
            if not ok:
                return False
        else:
            self.metadata = None
        self.op = Op.FRAMES
        return True

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

            self.output_buffer[offset : offset + ncopy] = self.default_buffer[
                start : start + ncopy
            ]
            self.output_buffer_end += ncopy
            self.default_buffer_start += ncopy

        if self.output_buffer_end == len(self.output_buffer):
            # We've filled this output buffer
            self.frames.append(self.output_buffer)
            self.output_buffer = None
            self.frame_index += 1
            # If a msg ends with some empty frames, we should eagerly detect
            # these to avoid waiting for the next message to finish them.
            while self.frame_index < self.nframes and self.frame_lengths[self.frame_index] == 0:
                self.frames.append(bytearray(0))
                self.frame_index += 1
            if self.frame_index == self.nframes:
                self.message_completed()

        if self.using_output_buffer:
            return False
        elif self.default_buffer_start == self.default_buffer_end:
            return False
        else:
            return True

    def advance(self):
        if self.op == Op.KIND:
            return self.parse_kind()
        elif self.op == Op.FLAGS:
            return self.parse_flags()
        elif self.op == Op.HEARTBEAT:
            return self.parse_heartbeat()
        elif self.op == Op.ID:
            return self.parse_id()
        elif self.op == Op.CODE:
            return self.parse_code()
        elif self.op == Op.WINDOW:
            return self.parse_window()
        elif self.op == Op.METADATA_LENGTH:
            return self.parse_metadata_length()
        elif self.op == Op.METADATA:
            return self.parse_metadata()
        elif self.op == Op.BODY_LENGTH:
            return self.parse_body_length()
        elif self.op == Op.BODY:
            return self.parse_body()
        elif self.op == Op.ROUTE_LENGTH:
            return self.parse_route_length()
        elif self.op == Op.ROUTE:
            return self.parse_route()
        elif self.op == Op.NFRAMES:
            return self.parse_nframes()
        elif self.op == Op.FRAME_LENGTHS:
            return self.parse_frame_lengths()
        elif self.op == Op.FRAMES:
            return self.parse_frames()


from . import messages
