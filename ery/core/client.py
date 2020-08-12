import asyncio
import itertools
from urllib.parse import urlparse

from .core import serialize_msg, ErrorCode
from . import _lib


class RequestError(Exception):
    pass


class Client:
    def __init__(self, address, **kwargs):
        self.address = address
        self._connect_kwargs = kwargs

        self._id_iter = itertools.count()
        self._exception = None
        self._requests = {}

    def __await__(self):
        return self.__aenter__().__await__()

    async def __aenter__(self):
        await self._connect()
        return self

    async def __aexit__(self, typ, value, traceback):
        await self.close()

    async def _connect(self):
        def factory():
            return ClientProtocol(self)

        loop = asyncio.get_event_loop()
        parsed = urlparse(self.address)

        if parsed.scheme in ("tcp", "tls"):
            host = parsed.hostname
            port = parsed.port

            transport, protocol = await loop.create_connection(
                factory, host, port, **self._connect_kwargs
            )
        elif parsed.scheme in ("unix", "unix+tls"):
            path = parsed.path
            transport, protocol = await loop.create_unix_connection(factory, path)
        else:
            raise ValueError(f"Unknown address type: {self.address!r}")

        self._protocol = protocol
        self._transport = transport
        self._loop = loop

    async def request(self, route, data=None):
        if self._exception is not None:
            raise self._exception

        id = next(self._id_iter)
        fut = self._loop.create_future()
        self._requests[id] = fut
        await self._protocol.send_request(id, route, data)
        return await fut

    async def close(self):
        self._close()
        try:
            futs = (f for f in self._requests.values())
            await asyncio.gather(*futs, return_exceptions=True)
        except asyncio.CancelledError:
            pass

    def _set_exception(self, exc):
        if self._exception:
            return

        self._exception = exc

        for h in self._requests.values():
            if not h.done():
                h.set_exception(exc)

        self._close()

    def _close(self):
        if self._transport is not None:
            transport = self._transport
            self._transport = None
            transport.close()

    def _on_msg_payload(self, id, data=None, is_next=False, is_complete=False):
        fut = self._requests.pop(id, None)
        if fut is not None and not fut.done():
            fut.set_result(data)


class ClientProtocol(asyncio.BufferedProtocol):
    def __init__(self, client, **kwargs):
        super().__init__()

        self.client = client

        self._msg_handlers = {
            _lib.KIND_SETUP_RESPONSE: self.on_msg_setup_response,
            _lib.KIND_HEARTBEAT: self.on_msg_heartbeat,
            _lib.KIND_ERROR: self.on_msg_error,
            _lib.KIND_INCREASE_QUOTA: self.on_msg_increase_quota,
            _lib.KIND_PAYLOAD: self.on_msg_payload,
        }
        self._protocol = _lib.Protocol(self.message_received, **kwargs)

        self._transport = None

        self._loop = asyncio.get_running_loop()
        self._connection_exc = None
        self._paused = False
        self._drain_waiter = None

    def connection_made(self, transport):
        self._transport = transport

    def get_buffer(self, sizehint):
        return self._protocol.get_buffer()

    def message_received(self, kind, args):
        self._msg_handlers[kind](*args)

    def buffer_updated(self, nbytes):
        self._protocol.buffer_updated(nbytes)

    def connection_lost(self, exc=None):
        if exc is None:
            exc = ConnectionResetError("Connection closed")
        self.client._set_exception(exc)
        self._connection_lost = exc

        if self._paused:
            waiter = self._drain_waiter
            if waiter is not None:
                self._drain_waiter = None
                if not waiter.done():
                    waiter.set_exception(exc)

    def pause_writing(self):
        self._paused = True

    def resume_writing(self):
        self._paused = False

        waiter = self._drain_waiter
        if waiter is not None:
            self._drain_waiter = None
            if not waiter.done():
                waiter.set_result(None)

    async def wait_closed(self):
        pass

    async def write(self, parts):
        if self._connection_exc:
            raise self._connection_exc
        if len(parts) > 1:
            self._transport.writelines(parts)
        else:
            self._transport.write(parts[0])
        if self._transport.is_closing():
            await asyncio.sleep(0, loop=self._loop)
        elif self._paused:
            self._drain_waiter = self._loop.create_future()
            await self._drain_waiter

    async def send_setup(self, heartbeat, metadata):
        parts = serialize_msg(_lib.KIND_SETUP, uint32=heartbeat, metadata=metadata)
        await self.write(parts)

    async def send_heartbeat(self):
        parts = [_lib.KIND_HEARTBEAT.to_bytes(1, "big", signed=True)]
        await self.write(parts)

    async def send_error(self, id, code, data):
        parts = serialize_msg(_lib.KIND_ERROR, id=id, uint32=code, data=data)
        await self.write(parts)

    async def send_cancel(self, id):
        parts = serialize_msg(_lib.KIND_CANCEL, id=id)
        await self.write(parts)

    async def send_increase_quota(self, id, quota):
        parts = serialize_msg(_lib.KIND_INCREASE_QUOTA, id=id, uint32=quota)
        await self.write(parts)

    async def send_request(self, id, route, data=None):
        parts = serialize_msg(_lib.KIND_REQUEST, id=id, route=route, data=data)
        await self.write(parts)

    async def send_stream(self, id, route, quota, data=None):
        parts = serialize_msg(
            _lib.KIND_REQUEST_STREAM, id=id, route=route, uint32=quota, data=data,
        )
        await self.write(parts)

    async def send_channel(self, id, route, quota, data=None):
        parts = serialize_msg(
            _lib.KIND_REQUEST_CHANNEL, id=id, route=route, uint32=quota, data=data,
        )
        await self.write(parts)

    async def send_payload(self, id, data=None, is_next=False, is_complete=False):
        parts = serialize_msg(
            _lib.KIND_PAYLOAD,
            id=id,
            data=data,
            is_next=is_next,
            is_complete=is_complete,
        )
        await self.write(parts)

    def on_msg_setup_response(self, heartbeat, metadata):
        pass

    def on_msg_heartbeat(self):
        pass

    def on_msg_error(self, id, code, data):
        fut = self.client._requests.pop(id, None)
        if fut is not None and not fut.done():
            if code == ErrorCode.REQUEST_ERROR:
                fut.set_exception(RequestError(data.decode()))
            else:
                fut.set_exception(Exception(data.decode()))

    def on_msg_increase_quota(self, id, quota):
        pass

    def on_msg_payload(self, id, data=None, is_next=False, is_complete=False):
        self.client._on_msg_payload(id, data, is_next=is_next, is_complete=is_complete)
