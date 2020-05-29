import enum
import struct


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


def dumps_header(frames):
    lens = [len(f) for f in frames]
    nframes = len(lens)
    buf = bytearray(b"ery")
    buf.extend(long_struct.pack(nframes))
    for l in lens:
        buf.extend(long_struct.pack(l))
    return buf


def dumps(*frames):
    buf = dumps_header(frames)
    for f in frames:
        buf.extend(f)
    return buf


class ProtocolError(Exception):
    pass


class Protocol(object):
    """A sans-io protocol for ery"""

    def __init__(self, buffer_size=READ_BUFFER_SIZE):
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
        self.frames = []
        self.step = Step.PREFIX
        self.messages = []

    def get_buffer(self):
        """Get a buffer to write bytes into"""
        if self.step < Step.FRAMES:
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
        assert self.step == Step.FRAMES
        assert self.current_frame_index < len(self.frame_lengths)
        to_read = self.frame_lengths[self.current_frame_index]
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

    def message_completed(self):
        self.messages.append(tuple(self.frames))
        self.frames = []
        self.current_frame_index = None
        self.step = Step.PREFIX
        self.output_buffer = None
        self.output_buffer_end = 0

    def parse_prefix(self):
        start = self.default_buffer_start
        end = self.default_buffer_end
        if end - start >= 3:
            prefix = self.default_buffer[start : start + 3]
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
                self.message_completed()
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

            self.output_buffer[offset : offset + ncopy] = self.default_buffer[
                start : start + ncopy
            ]
            self.output_buffer_end += ncopy
            self.default_buffer_start += ncopy

        if self.output_buffer_end == len(self.output_buffer):
            # We've filled this output buffer
            self.frames.append(self.output_buffer)
            self.output_buffer = None
            self.current_frame_index += 1
            # If a msg ends with some empty frames, we should eagerly detect
            # these to avoid waiting for the next message to finish them.
            while (
                self.current_frame_index < len(self.frame_lengths)
            ) and self.frame_lengths[self.current_frame_index] == 0:
                self.frames.append(bytearray(0))
                self.current_frame_index += 1
            if self.current_frame_index == len(self.frame_lengths):
                self.message_completed()

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
