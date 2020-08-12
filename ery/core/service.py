class ServiceMetaclass(type):
    def __new__(cls, name, bases, dct):
        request_handlers = {}
        stream_handlers = {}
        channel_handlers = {}

        for key, value in dct.items():
            kind = getattr(value, "_ery_handler_type", None)
            if kind is None:
                continue
            route = value._ery_handler_route
            if isinstance(route, str):
                route = route.encode("utf-8")
            if kind == "request":
                request_handlers[route] = value
            elif kind == "stream":
                stream_handlers[route] = value
            elif kind == "channel":
                channel_handlers[route] = value
            else:
                raise ValueError(f"Unknown handler type {kind}")

        dct = dct.copy()
        dct["_ery_request_handlers"] = request_handlers
        dct["_ery_stream_handlers"] = stream_handlers
        dct["_ery_channel_handlers"] = channel_handlers
        return type.__new__(cls, name, bases, dct)


class Service(metaclass=ServiceMetaclass):
    def get_request_handler(self, route):
        return self._ery_request_handlers.get(route)


def _register_handler(func, type, route):
    if route is None:
        route = func.__name__
    func._ery_handler_route = route
    func._ery_handler_type = type
    return func


def request(func=None, *, route=None):
    if func is None:
        return lambda func: request(func, route=route)
    return _register_handler(func, "request", route)


def stream(func=None, *, route=None):
    if func is None:
        return lambda func: stream(func, route=route)
    return _register_handler(func, "stream", route)


def channel(func=None, *, route=None):
    if func is None:
        return lambda func: channel(func, route=route)
    return _register_handler(func, "channel", route)
