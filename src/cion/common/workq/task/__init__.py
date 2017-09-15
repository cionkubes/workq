import hashlib
from inspect import signature


class Signature:
    @property
    def signature(self):
        m = hashlib.md5()

        for sig in self.signature_generator():
            m.update(sig)

        return m.hexdigest()

    def signature_generator(self):
        raise NotImplementedError()


class Interface(Signature):
    def __init__(self, name):
        self.name = name
        self.tasks = {}
        self.server = None

    def task(self, fn):
        task = Task(fn.__name__, signature(fn), self)

        sig = task.signature
        assert sig not in self.tasks, "Task with same signature already exists in this interface."

        self.tasks[sig] = task
        setattr(self, task.name, task)

    def signature_generator(self):
        # The interface name is included in each task signature, so to need to yield it here
        for task in self.tasks.values():
            yield from task.signature_generator()

    def is_implemented_guard(self):
        for task in self.tasks.values():
            assert task.implementation, f"Task {task.name} is not implemented"

        return True  # TODO

    def enable(self, server):
        assert self.server is None, "Interface already enabled"
        self.server = server


class Task(Signature):
    def __init__(self, name, signature, interface):
        self.name = name
        self.member_of = interface
        self.implementation = None

        self.args = signature.parameters

    @property
    def pretty_name(self):
        return f"{self.member_of.name}.{self.name}({', '.join(self.args)})"

    def signature_generator(self):
        yield self.member_of.name.encode()
        yield self.name.encode()

        yield from map(lambda param: bytes(param.kind), self.args.values())

    def valid_implementation_guard(self, fn):
        other_args = signature(fn).parameters

        assert len(self.args) == len(other_args), f"Expected {len(self.args)} parameters, but got {len(other_args)}"

        for self_param, other_param in zip(self.args.values(), other_args.values()):
            assert self_param.kind == other_param.kind, f"Expected parameter {self_param}, but got {other_param}"

    def implement(self, fn):
        self.valid_implementation_guard(fn)

        self.implementation = fn

    def __call__(self, *args, **kwargs):
        # assert self.valid_args(args, kwargs), "Invalid arguments"

        assert self.member_of.server is not None, "Interface not enabled"
        return self.member_of.server.start_task(self, args, kwargs)
