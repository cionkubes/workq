import random
import socket
import string
import io

import pytest

from workq.net.stream import Stream


class AsyncBytesIOChannel:
    def __init__(self):
        self.read_head = 0
        self.write_head = 0
        self.buffer = io.BytesIO()

    async def write(self, data, *args, **kwargs):
        self.buffer.seek(self.write_head)
        self.buffer.write(data, *args, **kwargs)
        self.write_head += len(data)

    async def read(self, n):
        self.buffer.seek(self.read_head)
        result = self.buffer.read(n)
        self.read_head += n
        return result

    async def readline(self):
        line = self.buffer.readline()
        self.read_head += len(line)
        return line

    def seek(self, at):
        self.read_head = at


@pytest.fixture
def channel():
    channel = AsyncBytesIOChannel()
    
    return channel


@pytest.fixture
def channel_generator():
    def gen():
        while True:
            yield channel()

    return gen()


@pytest.fixture
def streampair(event_loop):
    s1, s2 = socket.socketpair()
    s1.setblocking(False)
    s2.setblocking(False)

    return Stream(s1, loop=event_loop), Stream(s2, loop=event_loop)


@pytest.fixture
def streampair_generator(event_loop):
    def gen():
        while True:
            yield streampair(event_loop)

    return gen()


def random_int(maxdepth=1):
    return random.randint(-2**32, 2**32)


def random_float(maxdepth=1):
    return random.uniform(-2**32, 2**32)


def random_complex(maxdepth=1):
    return complex(random_float(), random_float())


def random_bool(maxdepth=1):
    return random.choice([True, False])


def random_none(maxdepth=1):
    return None


def random_immutable(maxdepth=1):
    return random.choice(random_immutable_fns)()


def random_string(maxdepth=1):
    size = random.randint(0, 50)

    return ''.join(random.choices(string.printable, k=size))


def random_bytes(maxdepth=1):
    size = random.randint(0, 50)
    bx = [bytes([byte]) for byte in range(0, 255)]

    return b''.join(random.choices(bx, k=size))


def random_tuple(maxdepth=1):
    size = int(random.triangular(0, 10, 1))

    if maxdepth <= 1:
        return tuple(random_shallow() for _ in range(size))
    else:
        return tuple(random_immutable(maxdepth-1) for _ in range(size))


def random_dict(maxdepth=1):
    size = int(random.triangular(0, 10, 1))

    if maxdepth <= 1:
        return {random_shallow(): random_shallow() for _ in range(size)}
    else:
        return {random_immutable(maxdepth-1): object(maxdepth - 1) for _ in range(size)}


def random_list(maxdepth=1):
    size = int(random.triangular(0, 10, 1))

    if maxdepth <= 1:
        return [random_shallow() for _ in range(size)]
    else:
        return [object(maxdepth - 1) for _ in range(size)]


def random_shallow():
    return random.choice(random_shallow_fns)()


random_shallow_fns = [random_string, random_int, random_float, random_bytes, random_bool, random_none, random_complex]
random_immutable_fns = [random_tuple, *random_shallow_fns]
random_fns = [random_dict, random_list, random_immutable]


@pytest.fixture
def object(maxdepth=None):
    if maxdepth is None:
        maxdepth = int(random.triangular(0, 10, 1))

    return random.choice(random_fns)(maxdepth)


@pytest.fixture(scope='session')
def objects():
    numiter = 100
    return [object() for _ in range(numiter)]