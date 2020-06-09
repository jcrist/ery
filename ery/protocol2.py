from ._lib import Protocol as _Protocol

from .protocol import Request, Payload

class Protocol(object):
    def __init__(self):
        self._prot = _Protocol(self.on_message)
        self.messages = []

    def get_buffer(self):
        return self._prot.get_buffer()

    def buffer_updated(self, nbytes):
        self._prot.buffer_updated(nbytes)
        if self.messages:
            out = self.messages
            self.messages = []
            return out
        else:
            return self.messages

    def on_message(self, kind, msg):
        if kind == 7:
            msg = Request(*msg)
        elif kind == 11:
            msg = Payload(*msg)
        self.messages.append(msg)
