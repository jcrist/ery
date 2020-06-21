import asyncio
import os
import socket
import weakref

from .aioprotocol import ConnectionProtocol


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
    def __init__(self, handler):
        self.handler = handler
        self.listeners = []
        self._handlers = weakref.WeakSet()

    def _protocol_factory(self):
        return ConnectionProtocol(self._on_connection)

    def _on_connection(self, connection):
        task = asyncio.ensure_future(self.handler(connection))
        self._handlers.add(task)

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
        await asyncio.gather(*self._handlers, return_exceptions=True)
