from contextlib import contextmanager

import asyncio

from logzero import logger


class T:
    def __init__(self, aiter):
        self._aiter = aiter
        self.iterables = []
        self.some_listen = Listeners()

    @contextmanager
    def tee(self):
        iterable = TIterable(self.some_listen)

        self.iterables.append(iterable)
        yield iterable

        self.iterables.remove(iterable)
        iterable.dispose()

    async def drive(self):
        await self.some_listen.wait()

        async for elem in self._aiter:
            next = elem
            for listener in self.iterables:
                cont, next = await listener.push(next)
                if not cont:
                    break
            else:
                logger.warning(f"No iterables consumed element {elem}")

            await self.some_listen.wait()


class TIterable:
    def __init__(self, when_listen):
        self.when_listen = when_listen
        self.next = None
        self.update = asyncio.Event()
        self.receive_event = asyncio.Event()
        self.received = True

    def __aiter__(self):
        return self

    async def push(self, elem):
        self.next = elem
        self.update.set()
        self.update.clear()

        await self.receive_event.wait()
        return self.received, self.next

    async def __anext__(self):
        if not self.received:
            self.receive_event.set()
            self.receive_event.clear()

        with self.when_listen.listen():
            await self.update.wait()
            elem = self.next

            self.received = False
            return elem

    def send(self, value):
        self.next = value
        self.received = True
        self.receive_event.set()
        self.receive_event.clear()

    def dispose(self):
        self.receive_event.set()
        self.receive_event.clear()


class Listeners:
    def __init__(self):
        self.ref_count = 0
        self.update = asyncio.Event()

    async def wait(self):
        while self.ref_count == 0:
            await self.update.wait()

    @contextmanager
    def listen(self):
        self.ref_count += 1
        self.update.set()
        self.update.clear()
        yield
        self.ref_count -= 1