from inspect import signature

from .sig import Signature
from .task import Task


class Interface(Signature):
    def __init__(self, name):
        super().__init__()
        self.name = name
        self.tasks = {}
        self.server = None

    def task(self, fn):
        task = Task(fn.__name__, signature(fn), self)

        sig = task.signature
        assert sig not in self.tasks, "Task with same signature already exists in this cion_interface."

        self.tasks[sig] = task
        setattr(self, task.name, task)

    def signature_generator(self):
        # The cion_interface name is included in each cion_interface signature, so to need to yield it here
        for task in self.tasks.values():
            yield from task.signature_generator()

    def is_implemented_guard(self):
        for task in self.tasks.values():
            assert task.implementation, f"Task {task.pretty_name} is not implemented"

        return True

    def enable(self, server):
        assert self.server is None, "Interface already enabled"
        self.server = server
