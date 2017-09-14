import asyncio
import socket
from logzero import logger

from ..net import supports_interface, Stream, error_guard, Types, Keys, work_result, work_failed


class Orchestrator:
    def __init__(self, addr, port):
        self.addr = addr
        self.port = port
        self.loop = asyncio.get_event_loop()
        self.tasks = {}

    async def join(self, *interfaces):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.setblocking(False)
            await self.loop.sock_connect(sock, (self.addr, self.port))
            sock.setblocking(True)

            stream = Stream(sock)

            for interface in interfaces:
                interface.is_implemented_guard()

                for task in interface.tasks.values():
                    self.tasks[task.signature()] = task

                await stream.send(supports_interface(interface))
                error_guard(await stream.decode())

            while True:
                msg = await stream.decode()
                try:
                    handler = dispatch_table[msg[Keys.TYPE]]
                    await handler(self, stream, msg)
                except KeyError:
                    logger.warning("Unknown message type.")

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
