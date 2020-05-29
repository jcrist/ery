import string
from ery.protocol import Protocol, dumps

import pytest


@pytest.mark.parametrize("n", [2, 4, 6, 8, 10, 12])
@pytest.mark.parametrize("buffer_size", [10, 20])
def test_small_chunks(n, buffer_size):
    b1 = string.ascii_lowercase[:20].encode()
    b2 = string.ascii_uppercase[:17].encode()
    msg = dumps(b1, b2)

    protocol = Protocol(buffer_size=buffer_size)

    i = 0
    outputs = []
    while i < len(msg):
        buf = protocol.get_buffer()
        nbytes = min(n, len(buf), len(msg) - i)
        buf[:nbytes] = msg[i : i + nbytes]
        for m in protocol.buffer_updated(nbytes):
            outputs.append(m)
        i += nbytes
    assert outputs == [(b1, b2)]


@pytest.mark.parametrize("n", [30, 45, 60, 80])
def test_small_messages(n):
    parts = []
    for i in range(12):
        parts.append(string.ascii_lowercase[i * 2 : (i + 1) * 2].encode())
    msgs = [(parts[i], parts[i + 1]) for i in range(0, 12, 2)]
    msg = b"".join(dumps(*m) for m in msgs)

    protocol = Protocol(buffer_size=200)

    i = 0
    outputs = []
    while i < len(msg):
        buf = protocol.get_buffer()
        nbytes = min(n, len(buf), len(msg) - i)
        buf[:nbytes] = msg[i : i + nbytes]
        for m in protocol.buffer_updated(nbytes):
            outputs.append(m)
        i += nbytes
    assert outputs == msgs


@pytest.mark.parametrize("lengths", [[20, 17, 0, 0, 0], [0], [5, 0, 2], [0, 0, 5]])
def test_zero_length_frames(lengths):
    n = 5
    parts = tuple(string.ascii_lowercase[:l].encode() for l in lengths)
    msg = dumps(*parts)

    protocol = Protocol(buffer_size=30)

    i = 0
    outputs = []
    while i < len(msg):
        buf = protocol.get_buffer()
        nbytes = min(n, len(buf), len(msg) - i)
        buf[:nbytes] = msg[i : i + nbytes]
        for m in protocol.buffer_updated(nbytes):
            outputs.append(m)
        i += nbytes
    assert outputs == [parts]


def test_no_frames():
    n = 5
    msg = dumps()

    protocol = Protocol(buffer_size=30)

    i = 0
    outputs = []
    while i < len(msg):
        buf = protocol.get_buffer()
        nbytes = min(n, len(buf), len(msg) - i)
        buf[:nbytes] = msg[i : i + nbytes]
        for m in protocol.buffer_updated(nbytes):
            outputs.append(m)
        i += nbytes
    assert outputs == [()]
