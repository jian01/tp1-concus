from functools import partial
from logging import Logger, getLogger
from multiprocessing import Queue
from threading import Thread


class MultiprocessingLogger:
    def __init__(self, logger: Logger):
        self.logger = logger
        self.queue = Queue(-1)
        t = Thread(target=self._log_messages)
        t.daemon = True
        t.start()

    @classmethod
    def getLogger(cls, name: str) -> 'MultiprocessingLogger':
        return MultiprocessingLogger(getLogger(name))

    def __getattr__(self, attr):
        if attr != '_log_messages':
            function = partial(self._global_method, self.queue, attr)
            return function
        else:
            return self._log_messages

    @staticmethod
    def _global_method(queue, name, *args, **kwargs):
        queue.put_nowait((name, args, kwargs))

    def _log_messages(self):
        while True:
            try:
                method_name, args, kwargs = self.queue.get()
                getattr(self.logger, method_name)(*args, **kwargs)
            except (KeyboardInterrupt, SystemExit):
                raise
            except EOFError:
                break
