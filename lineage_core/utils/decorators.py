import threading
import time
from functools import wraps
from typing import (
    Any,
    Callable,
)


def rate_limit(calls_per_second: float = 10) -> Callable:
    """Decorator that limits the number of function calls per second."""
    min_interval = 1.0 / calls_per_second
    last_called = [0.0]
    lock = threading.Lock()

    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            with lock:
                elapsed = time.time() - last_called[0]
                left_to_wait = min_interval - elapsed
                if left_to_wait > 0:
                    time.sleep(left_to_wait)
                last_called[0] = time.time()
            return func(*args, **kwargs)

        return wrapper

    return decorator