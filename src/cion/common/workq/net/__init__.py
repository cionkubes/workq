import asyncio
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
    return {Keys.TYPE: Types.SUPPORTS, Keys.INTERFACE: interface.signature}


def start_work(work_id, task, args, kwargs):
    return {
        Keys.TYPE: Types.DO_WORK,
        Keys.TASK: task.signature,
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


class Buffer:
    def __init__(self, limit, loop=asyncio.get_event_loop()):
        self.backing = bytearray(limit)
        self.read_available = 0
        self.write_available = limit
        self.limit = limit
        self.read_head = 0
        self.write_head = 0
        self.read_signal = asyncio.Event(loop=loop)
        self.eof = False

    async def write(self, data):
        length = len(data)

        assert length <= self.limit

        while self.write_available < length:
            await self.read_signal.wait()

        tail = self.limit - self.write_head
        if tail < length:
            self.backing[self.write_head:] = data[:tail]
            self.backing[:length - tail] = data[tail:]
            self.write_head = length - tail
        else:
            self.backing[self.write_head:self.write_head + length] = data
            self.write_head += length

        self.read_available += length
        self.write_available -= length

    async def read(self, n=-1, chunksize=None):
        if chunksize is None:
            chunksize = self.limit // 8

        if n < 0:
            buffer = bytearray(chunksize)
            result = bytearray()
            read = 1
            while read != 0:
                to_read = min(chunksize, self.read_available)
                read = await self.read_into(buffer, to_read)
                result.extend(buffer[:read])

            return result
        else:
            to_read = min(n, self.read_available)
            read = 0
            result = bytearray(to_read)

            while to_read - read > chunksize:
                read += await self.read_into(result, chunksize, offset=read)

            if to_read - read > 0:
                read += await self.read_into(result, to_read - read, offset=read)

            assert read == to_read
            return bytes(result)

    async def read_into(self, buffer, n, offset=0):
        if n == 0:
            return n

        tail = self.read_head + n
        if tail > self.limit:
            remaining = self.limit - self.read_head
            buffer[offset:offset + remaining] = self.backing[self.read_head:self.limit]
            buffer[offset + remaining:offset + n] = self.backing[:n - remaining]
            self.read_head = n - remaining
        else:
            buffer[offset:offset + n] = self.backing[self.read_head:tail]
            self.read_head = tail

        self.read_available -= n
        self.write_available += n

        self.read_signal.set()
        self.read_signal.clear()
        return n

    def feed_eof(self):
        """Feed the buffer with `end of file`"""
        self.eof = True
        self.write_available = 0

    def empty(self):
        """
        :return: True iff there is no more data to read.
        """
        return self.read_available == 0

    def at_eof(self):
        """
        :return: True iff there is no more data to read AND we have been fed `end of file`.
        """
        return self.eof and self.read_available == 0


NEWLINE = b'\n'


class Stream:
    def __init__(self, sock, bufsize=4096, loop=asyncio.get_event_loop()):
        self.sock = sock
        self._sock_recv = partial(loop.sock_recv, self.sock)
        self.read_lock = asyncio.Lock()
        self.write = partial(loop.sock_sendall, self.sock)

        self.buffer = Buffer(bufsize, loop)
        self.buffer_size = bufsize

    def send(self, data):
        return apickle.dump(data, self)

    def decode(self):
        return apickle.load(self)

    async def _read_nolock(self, n):
        if self.buffer.read_available > 0:
            return await self.buffer.read(n)
        else:
            return await self._sock_recv(n)

    async def read_exactly(self, n):
        result = bytearray(n)
        with await self.read_lock:
            read = 0
            while read < n:
                data = await self._read_nolock(n - read)
                result[read:] = data
                read += len(data)

            assert read == n
            return bytes(result)

    # Just for cPickle, because it expects f.read to always return exactly n bytes
    read = read_exactly

    async def readline(self):
        buf_write = self.buffer.write
        read = self._read_nolock
        limit = self.buffer_size
        result = bytearray()

        with await self.read_lock:
            while True:
                data = await read(limit)
                i = data.find(NEWLINE)

                if i < 0:
                    result.extend(data)
                else:
                    result.extend(data[:i+1])
                    await buf_write(data[i+1:])
                    break

            return bytes(result)