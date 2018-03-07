import asyncio
import socket
import ssl
from collections import defaultdict

from logzero import logger

from .client import Client, ClientState
from ..net.messages import Types, Keys, ok, error, ping
from ..net.stream import Stream


class Server:
    def __init__(self, hc_sleep=5):
        self.interfaces = {}
        self.interfaces_hash = []
        self.clients_supporting = defaultdict(lambda: [])
        self.clients = []
        self.loop = asyncio.get_event_loop()
        self.waiting_tasks = defaultdict(lambda: [])
        self.alive = True
        self.health_check_timeout = hc_sleep

        asyncio.ensure_future(self.health_check(), loop=self.loop)

    async def health_check(self):
        while self.alive:
            await asyncio.sleep(self.health_check_timeout)

            for tasks in self.waiting_tasks.values():
                num_waiting_tasks = len(tasks)
                first_task, _ = tasks[0]
                if num_waiting_tasks > 0:
                    logger.warning(f"{num_waiting_tasks} {first_task.pretty_name} task(s) without any clients "
                                   "implementing their cion_interface.")

    def enable(self, interface):
        self.interfaces[interface.signature] = interface
        self.interfaces_hash.append(interface.signature)
        interface.enable(self)

    async def find_worker(self, task):
        workers = self.clients_supporting[task.signature]
        try:
            return next(filter(lambda c: c.state == ClientState.IDLE, workers))
        except StopIteration:
            if len(workers) > 0:
                worker = workers.pop(0)
                workers.append(worker)
                return worker
            else:
                fut = self.loop.create_future()
                self.waiting_tasks[task.signature].append((task, fut))
                return await fut

    async def start_task(self, task, args, kwargs):
        worker = await self.find_worker(task)

        logger.debug(f"Starting task {task.signature} on client {worker.name}")
        future = await worker.start(task, args, kwargs)
        return await future

    def run(self, addr, port, certs=None):
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.setblocking(False)

        if certs:
            crt, key = certs
            context = ssl.create_default_context(ssl.Purpose.SERVER_AUTH)
            context.load_cert_chain(certfile=crt, keyfile=key)
            sock = context.wrap_socket(sock)

        sock.bind((addr, port))

        async def server():
            try:
                with sock:
                    sock.listen(1)

                    while self.alive:
                        conn, peer = await self.loop.sock_accept(sock)
                        asyncio.ensure_future(self.spawn_connection(conn, peer), loop=self.loop)
            except asyncio.CancelledError:
                logger.info("Server stopped.")
            finally:
                self.shutdown()
                self.alive = False

        return sock, server()

    async def spawn_connection(self, conn, peer):
        try:
            with conn:
                await self.socket_connect(Stream(conn), peer)
        except:
            logger.exception("Uncaught exception :")

    def shutdown(self):
        for client in self.clients:
            self.disconnect_client(client)

    async def socket_connect(self, stream, peer):
        addr, port = peer
        logger.info(f"Connection from {addr}:{port}")

        client = Client(addr, port, stream)
        self.clients.append(client)

        try:
            while True:
                msg = await stream.decode()
                try:
                    handler = dispatch_table[msg[Keys.TYPE]]
                except KeyError:
                    logger.warning("Unknown message type.")
                    continue

                await handler(self, client, msg)
        except ConnectionResetError:
            logger.info(f"Client {client.addr}:{client.port} forcefully disconnected.")
        except EOFError:
            logger.info(f"Client {client.addr}:{client.port} disconnected gracefully.")
        finally:
            self.disconnect_client(client)

    def disconnect_client(self, client):
        self.clients.remove(client)
        for interface in client.supported_interfaces:
            for task in interface.tasks.values():
                self.clients_supporting[task.signature].remove(client)

        client.disconnected()

    async def supports(self, client, msg):
        interface_hash = msg[Keys.INTERFACE]
        if interface_hash in self.interfaces_hash:
            interface = self.interfaces[interface_hash]

            client.supports(interface)

            for task in interface.tasks.values():
                self.clients_supporting[task.signature].append(client)

                if task.signature in self.waiting_tasks:
                    for _, future in self.waiting_tasks.pop(task.signature):
                        future.set_result(client)

            await client.stream.send(ok())
        else:
            await client.stream.send(error("Server does not use this cion_interface"))

    async def work_done(self, client, msg):
        await client.work_done(msg)

    async def recv_ping(self, client, msg):
        logger.debug(f"Received keep-alive ping from client {client.name}")
        await client.stream.send(ping)


dispatch_table = {
    Types.SUPPORTS: Server.supports,
    Types.WORK_COMPLETE: Server.work_done,
    Types.PING: Server.recv_ping
}
