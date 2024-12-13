"""Utilities for handling background tasks."""

import asyncio
from functools import wraps
from typing import Any, Callable, Coroutine


def background_task(func: Callable[..., Coroutine[Any, Any, None]]) -> Callable:
    """Decorator to run a function as a background task."""
    @wraps(func)
    def wrapper(self, *args: Any, **kwargs: Any) -> asyncio.Task:
        task = asyncio.create_task(func(self, *args, **kwargs))
        self._background_tasks.add(task)
        task.add_done_callback(self._background_tasks.discard)
        return task
    return wrapper 