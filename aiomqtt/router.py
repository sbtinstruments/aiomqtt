class Router:
    def __init__(self) -> None:
        self._handlers = {}

    def match(self, *args: str):
        """Add a new handler with one or multiple wildcards to the router."""
        def decorator(func):
            for wildcard in args:
                self._handlers[wildcard] = func
            return func
        return decorator
