import asyncio
import logging
import socket
import ssl
from collections import defaultdict
from enum import IntEnum, auto
from uuid import uuid4

from ..net import Types, Keys, Stream, ok, error, start_work

logger = logging.getLogger(__name__)


class Client:
    def __init__(self, addr, port, stream):
        self.addr = addr
        self.port = port
        self.stream = stream
        self.state = ClientState.IDLE
        self.futures = {}

    async def start(self, task, args, kwargs):
        self.state = ClientState.WORKING

        work_id = str(uuid4())
        self.futures[work_id] = asyncio.get_event_loop().create_future()

        await self.stream.send(start_work(work_id, task, args, kwargs))
        return self.futures[work_id]

    async def work_done(self, msg):
        future = self.futures[msg[Keys.WORK_ID]]
        del self.futures[msg[Keys.WORK_ID]]

        if len(self.futures) <= 0:
            self.state = ClientState.IDLE

        if future.cancelled():
            logger.debug("Future cancelled, discarding result.")
            return

        if Keys.WORK_EXC in msg:
            future.set_exception(msg[Keys.WORK_EXC])
        else:
            future.set_result(msg[Keys.WORK_RESULT])


class ClientState(IntEnum):
    IDLE = auto()
    WORKING = auto()


class Server:
    def __init__(self):
        self.interfaces = {}
        self.interfaces_hash = []
        self.clients_supporting = defaultdict(lambda: [])
        self.loop = asyncio.get_event_loop()

    def enable(self, interface):
        sig = interface.signature()
        self.interfaces[sig] = interface
        self.interfaces_hash.append(sig)
        interface.enable(self)

    async def find_worker(self, task):
        workers = self.clients_supporting[task.signature()]
        try:
            return next(filter(lambda c: c.state == ClientState.IDLE, workers))
        except StopIteration:
            while len(workers) <= 0:
                logger.info(f"No client implementing task {task.name}, waiting.")
                await asyncio.sleep(1)

            worker = workers.pop(0)
            workers.append(worker)
            return worker

    def start_task(self, task, args, kwargs):
        async def start():
            worker = await self.find_worker(task)
            future = await worker.start(task, args, kwargs)
            return await future

        return start()

    def run(self, addr, port, certs=None):
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.setblocking(False)

        if certs:
            crt, key = certs
            context = ssl.create_default_context(ssl.Purpose.SERVER_AUTH)
            context.load_cert_chain(certfile=crt, keyfile=key)
            sock = context.wrap_socket(sock)

        sock.bind((addr, port))

        async def runner():
            with sock:
                sock.listen(1)

                while True:
                    try:
                        conn, peer = await self.loop.sock_accept(sock)
                        with conn:
                            await self.socket_connect(Stream(conn), peer)
                    except:
                        logger.exception("Exception in server")

        return sock, runner()

    async def socket_connect(self, stream, peer):
        addr, port = peer
        logger.info(f"Connection from {addr}:{port}")

        client = Client(addr, port, stream)

        try:
            while True:
                msg = await stream.decode()
                try:
                    handler = dispatch_table[msg[Keys.TYPE]]
                    await handler(self, client, msg)
                except KeyError:
                    logger.warning("Unknown message type.")
        finally:
            self.disconnect_client(client)

    def disconnect_client(self, client):
        pass

    async def supports(self, client, msg):
        interface_hash = msg[Keys.INTERFACE]
        if interface_hash in self.interfaces_hash:
            interface = self.interfaces[interface_hash]

            for task in interface.tasks.values():
                self.clients_supporting[task.signature()].append(client)

            await client.stream.send(ok())
        else:
            await client.stream.send(error("Server does not use this interface"))

    async def work_done(self, client, msg):
        await client.work_done(msg)


dispatch_table = {
    Types.SUPPORTS: Server.supports,
    Types.WORK_COMPLETE: Server.work_done
}
