import asyncio
import io
from functools import partial

from .. import apickle


class Types:
    SUPPORTS = 0
    RESPONSE = 1
    DO_WORK = 2
    WORK_COMPLETE = 3


class Keys:
    WORK_ID = 'w'
    WORK_RESULT = 'r'
    WORK_EXC = 'x'
    KWARGS = 'd'
    ARGS = 'a'
    TASK = 'T'
    TYPE = "t"
    INTERFACE = 'i'
    ERROR = 'e'
    MSG = 'm'


def ok(**data):
    return {Keys.TYPE: Types.RESPONSE, Keys.ERROR: False, **data}


def error(msg):
    return {Keys.TYPE: Types.RESPONSE, Keys.ERROR: True, Keys.MSG: msg}


def supports_interface(interface):
    return {Keys.TYPE: Types.SUPPORTS, Keys.INTERFACE: interface.signature()}


def start_work(work_id, task, args, kwargs):
    return {
        Keys.TYPE: Types.DO_WORK,
        Keys.TASK: task.signature(),
        Keys.WORK_ID: work_id,
        Keys.ARGS: args,
        Keys.KWARGS: kwargs
    }


def work_result(work_id, result):
    return {
        Keys.TYPE: Types.WORK_COMPLETE,
        Keys.WORK_ID: work_id,
        Keys.WORK_RESULT: result
    }


def work_failed(work_id, exception):
    return {
        Keys.TYPE: Types.WORK_COMPLETE,
        Keys.WORK_ID: work_id,
        Keys.WORK_EXC: exception
    }


def error_guard(response):
    assert response[Keys.TYPE] == Types.RESPONSE, "Expected response type"
    assert not response[Keys.ERROR], response[Keys.MSG]


NEWLINE = b'\n'


class Stream:
    def __init__(self, sock):
        self.loop = asyncio.get_event_loop()

        self.sock = sock
        self._read = partial(self.loop.sock_recv, self.sock)
        self.write = partial(self.loop.sock_sendall, self.sock)
        self.buf = io.BytesIO()
        self.read_lock = asyncio.Lock()
        self.buf_len = 0

    def send(self, data):
        return apickle.dump(data, self)

    def decode(self):
        return apickle.load(self)

    async def read(self, n):
        with await self.read_lock:
            if self.buf_len > 0:
                if self.buf_len > n:
                    self.buf_len -= n
                    return self.buf.read(n)
                else:
                    self.buf_len = 0
                    return self.buf.read(self.buf_len)
            else:
                return await self._read(n)

    async def readline(self):
        buf = self.buf
        read = self._read

        with await self.read_lock:
            while True:
                data = await read(4096)
                i = data.find(NEWLINE)

                if i < 0:
                    buf.write(data)
                    self.buf_len += len(data)
                else:
                    buf.write(data[:i])
                    data_len = len(data[:i])
                    buf.seek(-self.buf_len - data_len, io.SEEK_CUR)
                    line = buf.read(self.buf_len + data_len)
                    buf.seek(-self.buf_len - data_len, io.SEEK_CUR)
                    extra = len(data[i:])
                    buf.write(data[i:])
                    self.buf_len += extra

                    return line
