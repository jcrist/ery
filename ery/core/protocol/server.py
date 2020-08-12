import asyncio
import collections

from .core import ErrorCode


REQUEST = 1
STREAM = 2
CHANNEL = 3


class Comm:
    def __init__(self, connection, id, kind):
        self._connection = connection
        self._id = id
        self._kind = kind
        self._closed = False
        self._queue = collections.deque() if kind == "channel" else None
        self._waiter = None

    async def __aenter__(self):
        return self

    async def __aexit__(self, typ, value, traceback):
        await self.close()

    def _on_close(self):
        self._closed = True
        if self._waiter is not None:
            self._waiter.set_result(None)
        self._queue = None

    async def close(self):
        if not self._closed:
            self._on_close()
            await self._connection.send_payload(self._id, is_complete=True)

    async def error(self, code, message=None):
        if self._closed:
            raise ValueError("Cannot error on closed comm")
        self._on_close()
        await self._connection.send_error(self._id, code, message)

    async def send(self, data, close=None):
        if self._closed:
            raise ValueError("Cannot send on closed comm")
        if close is None:
            close = self._kind == "request"
        if close:
            self._on_close()
        await self._connection.send_payload(
            self._id, data, is_next=True, is_complete=close
        )

    async def recv(self):
        if self._kind != "channel":
            raise ValueError(f"Cannot recv on {self._kind} comm")

        if not self._queue and not self._closed:
            if self._waiter is not None:
                raise RuntimeError("recv may only be called by one coroutine at a time")
            self._waiter = self._loop.create_future()
            try:
                await self._waiter
            finally:
                self._waiter = None

        if self._closed:
            raise ValueError("Cannot recv on closed comm")

        return self._queue.popleft()

    async def increase_quota(self, quota):
        if self._kind != "channel":
            raise ValueError(f"Cannot increase_quota on {self._kind} comm")
        if self._closed:
            raise ValueError("Cannot increase_quota on closed comm")
        await self._connection.send_increase_quota(self._id, quota)


class ServerCallbacks:
    def __init__(self, app, connection):
        self.app = app
        self.connection = connection
        self.active = {}

    async def handle_request(self, handler, data, id):
        comm = Comm(self.connection, id, "request")
        try:
            await handler(data, comm)
        finally:
            self.active.pop(id, None)

    def on_connection_made(self):
        pass

    def on_connection_lost(self, exc):
        for _, handler in self.active.values():
            handler.cancel()

    async def wait_closed(self):
        await asyncio.gather(*(handler for _, handler in self.active.values()))

    def on_msg_setup(self, heartbeat, metadata):
        pass

    def on_msg_setup_response(self, heartbeat, metadata):
        pass

    def on_msg_heartbeat(self):
        pass

    def on_msg_error(self, id, code, data):
        pass

    def on_msg_cancel(self, id):
        pass

    def on_msg_increase_quota(self, id, quota):
        pass

    def on_msg_request(self, id, route, data):
        if id in self.active:
            return
        handler = self.app.get_request_handler(route)
        if handler is None:
            self.send_error(ErrorCode.REQUEST_REJECTED, f"Unknown route {route}")
        handler = asyncio.ensure_future(self.handle_request(handler, data, id))
        self.active[id] = (REQUEST, handler)

    def on_msg_stream(self, id, route, quota, data=None):
        pass

    def on_msg_channel(self, id, route, quota, data=None):
        pass

    def on_msg_payload(self, id, data=None, is_next=False, is_complete=False):
        pass
