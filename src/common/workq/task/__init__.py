import hashlib


def md5(obj):
    m = hashlib.md5()

    for sig in obj.signature():
        m.update(sig)

    return m.hexdigest()


class Interface:
    def __init__(self, name):
        self.name = name
        self.tasks = {}
        self.server = None

    def task(self, fn):
        task = Task(fn.__name__, self)

        self.tasks[md5(task)] = task
        setattr(self, task.name, task)

    def signature(self):
        yield self.name.encode()

        for task in self.tasks.values():
            yield from task.signature()

    def is_implemented_guard(self):
        for task in self.tasks.values():
            assert task.implementation, f"Task {task.name} is not implemented"

        return True  # TODO

    def enable(self, server):
        assert self.server is None, "Interface already enabled"
        self.server = server


class Task:
    def __init__(self, name, interface):
        self.name = name
        self.member_of = interface
        self.implementation = None

    def signature(self):
        yield self.name.encode()

    def implement(self, fn):
        # assert self.valid_args(args, kwargs), "Invalid arguments"

        self.implementation = fn

    def __call__(self, *args, **kwargs):
        # assert self.valid_args(args, kwargs), "Invalid arguments"

        assert self.member_of.server is not None, "Interface not enabled"
        return self.member_of.server.start_task(self, args, kwargs)