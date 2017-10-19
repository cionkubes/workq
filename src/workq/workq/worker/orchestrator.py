import asyncio
import concurrent.futures

from logzero import logger

from workq.net.messages import supports_interface, error_guard, Types, Keys, work_result, work_failed
from .stream import StreamWrapper


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

        keepalive = await stream.connect(self.addr, self.port)

        async def loop():
            try:
                while True:
                    msg = await stream.decode()

                    try:
                        handler = dispatch_table[msg[Keys.TYPE]]
                    except KeyError:
                        logger.warning("Unknown message type.")
                        continue

                    await handler(self, stream, msg)
            except:
                logger.exception("Unhandled exception:")
                raise
            finally:
                stream.close()

        done, pending = await asyncio.wait([loop(), keepalive], return_when=concurrent.futures.FIRST_COMPLETED)

        for task in pending:
            task.cancel()

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
