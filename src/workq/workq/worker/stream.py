import asyncio
import socket

from logzero import logger

from workq.net.stream import Stream


class StreamWrapper:
    def __init__(self, retry_timeout, loop=asyncio.get_event_loop()):
        self.retry_timeout = retry_timeout
        self.loop = loop
        self.available = asyncio.Event(loop=loop)
        self.backing = None
        self.socket = None
        self.address = None
        self.on_connect_callback = None
        self.connect_task = None

        @self.on_connect
        async def callback():
            pass

    def on_connect(self, fn):
        self.on_connect_callback = fn

    async def reconnect(self):
        self.available.clear()
        self.socket.close()
        await self._connect()

    async def send(self, msg):
        while True:
            await self.available.wait()
            try:
                await self.backing.send(msg)
                return
            except ConnectionResetError:
                logger.waring(f"Server {self.address[0]}:{self.address[1]} forcefully disconnected.")
                await self.reconnect()

    async def decode(self):
        while True:
            await self.available.wait()
            try:
                return await self.backing.decode()
            except ConnectionResetError:
                logger.warning(f"Server {self.address[0]}:{self.address[1]} forcefully disconnected.")
                await self.reconnect()

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
                return sock
            except (ConnectionRefusedError, ConnectionAbortedError, socket.gaierror):
                logger.critical(f"Connect call to {self.address[0]}:{self.address[1]} failed, retying in {self.retry_timeout} "
                             "second(s).")
                await asyncio.sleep(self.retry_timeout)