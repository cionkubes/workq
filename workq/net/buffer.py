import asyncio


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