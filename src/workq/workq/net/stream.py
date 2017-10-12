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
        self.write = partial(loop.sock_sendall, self.sock)

        self.buffer = Buffer(bufsize, loop)
        self.buffer_size = bufsize
        self.pushback_que = asyncio.Queue(maxsize=10)

    def send(self, data):
        return apickle.dump(data, self)

    def decode(self):
        if self.pushback_que.empty():
            return apickle.load(self)
        else:
            return self.pushback_que.get()

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

    def wait_for(self, predicate, timeout=10):
        async def task():
            while True:
                msg = await self.decode()
                if predicate(msg):
                    return msg
                else:
                    await self.pushback_que.put(msg)
                    await asyncio.sleep(0.001)

        return asyncio.wait_for(task(), timeout=timeout)