import asyncio
import socket
from logzero import logger

from ..net import supports_interface, Stream, error_guard, Types, Keys, work_result, work_failed


class StreamWrapper:
    def __init__(self, retry_timeout, loop=asyncio.get_event_loop()):
        self.retry_timeout = retry_timeout
        self.loop = loop
        self.available = asyncio.Event(loop=loop)
        self.backing = None
        self.socket = None
        self.address = None
        self.on_connect_callback = None

        @self.on_connect
        async def callback():
            pass

    def on_connect(self, fn):
        self.on_connect_callback = fn

    async def send(self, msg):
        while True:
            await self.available.wait()
            try:
                await self.backing.send(msg)
                return
            except ConnectionResetError:
                logger.debug(f"Server {self.address[0]}:{self.address[1]} forcefully disconnected.")
                self.socket.close()
                self.available.clear()
                await self._connect()

    async def decode(self):
        while True:
            await self.available.wait()
            try:
                return await self.backing.decode()
            except ConnectionResetError:
                logger.debug(f"Server {self.address[0]}:{self.address[1]} forcefully disconnected.")
                self.socket.close()
                self.available.clear()
                await self._connect()

    def close(self):
        self.socket.close()

    async def connect(self, addr, port):
        self.address = addr, port
        await self._connect()

    async def _connect(self):
        self.socket = await self.connect_retry()
        self.backing = Stream(self.socket)
        self.available.set()

        await self.on_connect_callback()

    async def connect_retry(self):
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.setblocking(False)
        while True:
            try:
                await self.loop.sock_connect(sock, self.address)
                logger.info(f"Connected to {self.address[0]}:{self.address[1]}.")
                sock.setblocking(True)
                return sock
            except ConnectionRefusedError:
                logger.debug(f"Connect call to {self.address[0]}:{self.address[1]} failed, retying in {self.retry_timeout} "
                             "second(s).")
                await asyncio.sleep(self.retry_timeout)


class Orchestrator:
    def __init__(self, addr, port, retry_timeout=1):
        self.addr = addr
        self.port = port
        self.loop = asyncio.get_event_loop()
        self.tasks = {}
        self.retry_timeout = retry_timeout

    async def join(self, *interfaces):
        for interface in interfaces:
            interface.is_implemented_guard()

            for task in interface.tasks.values():
                self.tasks[task.signature] = task

        stream = StreamWrapper(self.retry_timeout, loop=self.loop)

        @stream.on_connect
        async def on_connect():
            for interface in interfaces:
                await stream.send(supports_interface(interface))
                error_guard(await stream.decode())

        await stream.connect(self.addr, self.port)

        try:
            while True:
                msg = await stream.decode()

                try:
                    handler = dispatch_table[msg[Keys.TYPE]]
                except KeyError:
                    logger.warning("Unknown message type.")
                    continue

                await handler(self, stream, msg)
        finally:
            stream.close()

    async def work(self, stream, msg):
        work_id = msg[Keys.WORK_ID]

        task = self.tasks[msg[Keys.TASK]]
        args, kwargs = msg[Keys.ARGS], msg[Keys.KWARGS]

        try:
            task = task.implementation(*args, **kwargs)
            result = await task
        except Exception as e:
            if hasattr(task, 'exception') and task.exception():
                await stream.send(work_failed(work_id, task.exception()))
                logger.warning("Exception during work.\n%s", task.exception())
            else:
                await stream.send(work_failed(work_id, e))
                logger.exception("Exception in work scheduling.")

            return

        await stream.send(work_result(work_id, result))


dispatch_table = {
    Types.DO_WORK: Orchestrator.work
}
