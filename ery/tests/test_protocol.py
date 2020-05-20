import string
from ery.protocol import ChannelProtocol, dumps

import pytest


@pytest.mark.parametrize("n", [2, 4, 6, 8, 10, 12])
@pytest.mark.parametrize("buffer_size", [10, 20])
def test_small_chunks(n, buffer_size):
    b1 = string.ascii_lowercase[:20].encode()
    b2 = string.ascii_uppercase[:17].encode()
    msg = dumps(b1, b2)

    protocol = ChannelProtocol(buffer_size=buffer_size)

    i = 0
    while i < len(msg):
        buf = protocol.get_buffer(-1)
        nbytes = min(n, len(buf), len(msg) - i)
        buf[:nbytes] = msg[i:i + nbytes]
        protocol.buffer_updated(nbytes)
        i += nbytes
    o = protocol.output_queue[0]
    assert o == [b1, b2]


@pytest.mark.parametrize("n", [30, 45, 60, 80])
def test_small_messages(n):
    parts = []
    for i in range(12):
        parts.append(string.ascii_lowercase[i * 2:(i + 1)*2].encode())
    msgs = [[parts[i], parts[i + 1]] for i in range(0, 12, 2)]
    msg = b"".join(dumps(*m) for m in msgs)

    protocol = ChannelProtocol(buffer_size=200)

    i = 0
    while i < len(msg):
        buf = protocol.get_buffer(-1)
        nbytes = min(n, len(buf), len(msg) - i)
        buf[:nbytes] = msg[i:i + nbytes]
        protocol.buffer_updated(nbytes)
        i += nbytes
    o = protocol.output_queue
    assert o == msgs


@pytest.mark.parametrize("lengths", [[20, 17, 0, 0, 0], [0], [5, 0, 2], [0, 0, 5]])
def test_zero_length_frames(lengths):
    n = 5
    parts = [string.ascii_lowercase[:l].encode() for l in lengths]
    msg = dumps(*parts)

    protocol = ChannelProtocol(buffer_size=30)

    i = 0
    while i < len(msg):
        buf = protocol.get_buffer(-1)
        nbytes = min(n, len(buf), len(msg) - i)
        buf[:nbytes] = msg[i:i + nbytes]
        protocol.buffer_updated(nbytes)
        i += nbytes
    o = protocol.output_queue[0]
    assert o == parts


def test_no_frames():
    n = 5
    msg = dumps()

    protocol = ChannelProtocol(buffer_size=30)

    i = 0
    while i < len(msg):
        buf = protocol.get_buffer(-1)
        nbytes = min(n, len(buf), len(msg) - i)
        buf[:nbytes] = msg[i:i + nbytes]
        protocol.buffer_updated(nbytes)
        i += nbytes
    o = protocol.output_queue[0]
    assert o == []
