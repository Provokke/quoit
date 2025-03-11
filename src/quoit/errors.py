class QuoitError(Exception):
    pass


class QueueClosed(QuoitError):
    pass


class QueueFull(QuoitError):
    pass


class TaskCancelled(QuoitError):
    pass


class TaskFailed(QuoitError):
    def __init__(self, name: str, attempts: int, cause: BaseException) -> None:
        super().__init__(f"task {name!r} failed after {attempts} attempt(s): {cause!r}")
        self.name = name
        self.attempts = attempts
        self.cause = cause
