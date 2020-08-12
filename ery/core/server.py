import asyncio
import collections
import os
import socket
import weakref

from .core import ErrorCode, serialize_msg
from . import _lib


REQUEST = 1
STREAM = 2
CHANNEL = 3


class Comm:
    def __init__(self, protocol, id, kind):
        self._protocol = protocol
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
            await self._protocol.send_payload(self._id, is_complete=True)

    async def error(self, code, message=None):
        if self._closed:
            raise ValueError("Cannot error on closed comm")
        self._on_close()
        await self._protocol.send_error(self._id, code, message)

    async def send(self, data, close=None):
        if self._closed:
            raise ValueError("Cannot send on closed comm")
        if close is None:
            close = self._kind == "request"
        if close:
            self._on_close()
        await self._protocol.send_payload(
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
        await self._protocol.send_increase_quota(self._id, quota)


class ServerProtocol(asyncio.BufferedProtocol):
    def __init__(self, app, **kwargs):
        super().__init__()

        self.app = app

        self._msg_handlers = {
            _lib.KIND_SETUP: self.on_msg_setup,
            _lib.KIND_SETUP_RESPONSE: self.on_msg_setup_response,
            _lib.KIND_HEARTBEAT: self.on_msg_heartbeat,
            _lib.KIND_ERROR: self.on_msg_error,
            _lib.KIND_CANCEL: self.on_msg_cancel,
            _lib.KIND_INCREASE_QUOTA: self.on_msg_increase_quota,
            _lib.KIND_REQUEST: self.on_msg_request,
            _lib.KIND_STREAM: self.on_msg_stream,
            _lib.KIND_CHANNEL: self.on_msg_channel,
            _lib.KIND_PAYLOAD: self.on_msg_payload,
        }
        self._protocol = _lib.Protocol(self.message_received, **kwargs)

        self._transport = None

        self._loop = asyncio.get_running_loop()
        self._connection_exc = None
        self._paused = False
        self._drain_waiter = None

        self.active = {}

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
        for _, handler in self.active.values():
            handler.cancel()
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

    async def send_setup_response(self, heartbeat, metadata):
        parts = serialize_msg(
            _lib.KIND_SETUP_RESPONSE, uint32=heartbeat, metadata=metadata
        )
        await self.write(parts)

    async def send_heartbeat(self):
        parts = [_lib.KIND_HEARTBEAT.to_bytes(1, "big", signed=True)]
        await self.write(parts)

    async def send_error(self, id, code, data):
        parts = serialize_msg(_lib.KIND_ERROR, id=id, uint32=code, data=data)
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

    async def handle_request(self, id, route, data):
        try:
            comm = Comm(self, id, "request")
            handler = self.app.get_request_handler(route)
            if handler is None:
                await comm.error(
                    ErrorCode.REQUEST_REJECTED, b"Unknown route %s" % route
                )
                return
            try:
                await handler(self.app, data, comm)
            except Exception:
                if not comm._closed:
                    await comm.error(
                        ErrorCode.REQUEST_ERROR, b"Unexpected internal error"
                    )
                else:
                    raise
            else:
                if not comm._closed:
                    await comm.close()
        finally:
            self.active.pop(id, None)

    def on_msg_request(self, id, route, data):
        if id in self.active:
            return
        handler = asyncio.ensure_future(self.handle_request(id, route, data))
        self.active[id] = (REQUEST, handler)

    def on_msg_stream(self, id, route, quota, data=None):
        pass

    def on_msg_channel(self, id, route, quota, data=None):
        pass

    def on_msg_payload(self, id, data=None, is_next=False, is_complete=False):
        pass


class Listener(object):
    """Base class for all listeners"""

    def __init__(self, server, ssl=None, **kwargs):
        self.server = server
        self._ssl = ssl
        self._extra_kwargs = kwargs
        self._handle = None

    async def start(self):
        pass

    async def stop(self):
        if self._handle is None:
            return
        self._handle.close()
        await self._handle.wait_closed()


class TCPListener(Listener):
    def __init__(self, server, port, host=None, **kwargs):
        super().__init__(server, **kwargs)
        if host is None:
            host = "0.0.0.0"

        scheme = "tls" if self._ssl else "tcp"
        self.name = f"{scheme}://{host}:{port}"
        self._host = host
        self._port = port

    async def start(self) -> None:
        await super().start()
        loop = asyncio.get_event_loop()
        self._handle = await loop.create_server(
            self.server._protocol_factory,
            host=self._host,
            port=self._port,
            ssl=self._ssl,
            **self._extra_kwargs,
        )


class UnixListener(Listener):
    def __init__(self, server, path, **kwargs):
        super().__init__(server, **kwargs)
        path = os.path.abspath(path)
        scheme = "unix+tls" if self._ssl else "unix"
        self.name = f"{scheme}:{path}"
        self._path = path

    async def start(self) -> None:
        await super().start()
        loop = asyncio.get_event_loop()
        self._handle = await loop.create_unix_server(
            self.server._protocol_factory,
            path=self._path,
            ssl=self._ssl,
            **self._extra_kwargs,
        )


class SocketListener(Listener):
    def __init__(self, server, sock, **kwargs):
        super().__init__(server, **kwargs)

        if hasattr(socket, "AF_UNIX") and sock.family == socket.AF_UNIX:
            path = os.path.abspath(sock.getsockname())
            scheme = "unix+tls" if self._ssl else "unix"
            self.name = f"{scheme}:{path}"
        else:
            host, port = sock.getsockname()[:2]
            scheme = "tls" if self._ssl else "tcp"
            self.name = f"{scheme}://{host}:{port}"
        self._sock = sock

    async def start(self) -> None:
        await super().start()
        loop = asyncio.get_event_loop()
        self._handle = await loop.create_server(
            self.server._protocol_factory,
            sock=self._sock,
            ssl=self._ssl,
            **self._extra_kwargs,
        )


class Server(object):
    def __init__(self, app):
        self.app = app
        self.listeners = []
        self._connections = weakref.WeakSet()

    def _protocol_factory(self):
        conn = ServerProtocol(self.app)
        self._connections.add(conn)
        return conn

    def add_tcp_listener(self, port, *, host=None, ssl=None, **kwargs):
        """Add a new TCP listener.

        Parameters
        ----------
        port : int
            The port to listen on.
        host : str or list[str], optional
            The host (or hosts) to listen on. Default is all interfaces.
        ssl : SSLContext, optional
            If provided, TLS will be used over accepted connections.
        **kwargs : optional
            Additional parameters to forward to ``asyncio.EventLoop.create_server``.
        """
        self.listeners.append(TCPListener(self, port, host=host, ssl=ssl, **kwargs))

    def add_unix_listener(self, path, *, ssl=None, **kwargs):
        """Add a new Unix listener.

        Parameters
        ----------
        path : str
            The path of the unix domain socket to listen on.
        ssl : SSLContext, optional
            If provided, TLS will be used over accepted connections.
        **kwargs : optional
            Additional parameters to forward to ``asyncio.EventLoop.create_unix_server``.
        """
        self.listeners.append(UnixListener(self, path, ssl=ssl, **kwargs))

    def add_socket_listener(self, sock, *, ssl=None, **kwargs):
        """Add a new listener on an already created socket.

        Parameters
        ----------
        sock : socket.socket
            An already created socket object.
        ssl : SSLContext, optional
            If provided, TLS will be used over accepted connections.
        **kwargs : optional
            Additional parameters to forward to ``asyncio.EventLoop.create_server``.
        """
        self.listeners.append(SocketListener(self, sock, ssl=ssl, **kwargs))

    async def start(self):
        await asyncio.gather(*(listener.start() for listener in self.listeners))

    async def stop(self):
        await asyncio.gather(*(listener.stop() for listener in self.listeners))
        await asyncio.gather(*(c.wait_closed() for c in self._connections))
