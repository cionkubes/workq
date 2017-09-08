import asyncio
import random
import socket
import string
from itertools import islice

import pytest

from cion.common.workq.net import Stream


def random_int():
    return random.randint(-2**32, 2**32)


def random_float():
    return random.uniform(-2**32, 2**32)


def random_complex():
    return complex(random_float(), random_float())


def random_bool():
    return random.choice([True, False])


def random_none():
    return None


def random_immutable():
    return random.choice(random_immutable_fns)()


def random_string():
    size = random.randint(0, 50)

    return ''.join(random.choices(string.printable, k=size))


def random_bytes():
    size = random.randint(0, 50)
    bx = [bytes([byte]) for byte in range(0, 255)]

    return b''.join(random.choices(bx, k=size))


def random_tuple():
    size = int(random.triangular(0, 5, 1))

    return tuple(random_immutable() for _ in range(size))


def random_dict():
    size = int(random.triangular(0, 5, 1))

    return {random_immutable(): random_object() for _ in range(size)}


def random_list():
    size = int(random.triangular(0, 5, 1))

    return [random_object() for _ in range(size)]


def random_object():
    return random.choice(random_fns)()


random_immutable_fns = [random_string, random_int, random_float, random_bytes, random_bool, random_none, random_complex, random_tuple]
random_fns = [random_dict, random_list, random_immutable]


def object_gen():
    while True:
        yield random_object()

@pytest.fixture
def object():
    return random_object()


@pytest.fixture
def object_generator():
    return object_gen()


@pytest.fixture
def streampair():
    s1, s2 = socket.socketpair()
    s1.setblocking(False)
    s2.setblocking(False)

    return Stream(s1), Stream(s2)


@pytest.mark.asyncio
async def test_pickling(streampair):
    r, w = streampair

    obj = {u"Test": 32, u'res': {0x3: [4, 5, 7]}}

    read_task = r.decode()
    await w.send(obj)
    assert obj == await read_task