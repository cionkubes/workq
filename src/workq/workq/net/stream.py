import asyncio
from collections import deque
from functools import partial

from workq import apickle
from .buffer import Buffer

NEWLINE = b'\n'


class Stream:
    def __init__(self, sock, bufsize=4096, loop=asyncio.get_event_loop()):
        self.sock = sock
        self._sock_recv = partial(loop.sock_recv, self.sock)
        self.read_lock = asyncio.Lock()
        self.write_lock = asyncio.Lock()
        self.write = partial(loop.sock_sendall, self.sock)

        self.buffer = Buffer(bufsize, loop)
        self.buffer_size = bufsize

    async def send(self, data):
        with await self.write_lock:
            return await apickle.dump(data, self)

    async def decode(self):
        with await self.read_lock:
            return await apickle.load(self)

    async def _read(self, n):
        if self.buffer.read_available > 0:
            return await self.buffer.read(n)
        else:
            return await self._sock_recv(n)

    async def read_exactly(self, n):
        result = bytearray(n)

        read = 0
        while read < n:
            data = await self._read(n - read)
            length = len(data)
            if length == 0:
                raise EOFError

            result[read:] = data
            read += length

        assert read == n
        return bytes(result)

    # Just for cPickle, because it expects f.read to always return exactly n bytes
    read = read_exactly

    async def readline(self):
        buf_write = self.buffer.write
        read = self._read
        limit = self.buffer_size
        result = bytearray()

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