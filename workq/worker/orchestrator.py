import asyncio
import traceback
import concurrent.futures

from logzero import logger

from ..net.messages import supports_interface, ping, error_guard, Types, Keys, work_result, work_failed
from .stream import StreamWrapper


class PingObserver:
    def __init__(self):
        self.subscription = None
        self.ping_event = asyncio.Event()

    def start(self, observable):
        self.subscription = observable.subscribe(self)

    async def on_no_ping(self, corutine_producer, timeout=10):
        try:
            await asyncio.wait_for(self.ping_event.wait(), timeout=timeout)
        except asyncio.futures.TimeoutError:
            await corutine_producer()

    async def got_response_in(self, seconds=10):
        try:
            await asyncio.wait_for(self.ping_event.wait(), timeout=seconds)
            return True
        except asyncio.futures.TimeoutError:
            return False

    def on_next(self, message):
        if message[Keys.TYPE] == Types.PING:
            self.ping_event.set()
            self.subscription.dispose()

    def on_error(self, error):
        pass

    def on_complete(self):
        pass


class Handle:
    def __init__(self, shutdown_corutine, stream):
        self.shutdown = shutdown_corutine
        self.stream = stream

    def run_until_complete(self):
        return self.shutdown

    def own_ip(self):
        return self.stream.own_ip()

    async def ping(self):
        observer = PingObserver()
        observer.start(self.stream.observable)

        await self.stream.send(ping)
        return await observer.got_response_in(seconds=3)


class Orchestrator:
    def __init__(self, addr, port, retry_timeout=1, keepalive_every=4):
        self.addr = addr
        self.port = port
        self.loop = asyncio.get_event_loop()
        self.tasks = {}
        self.retry_timeout = retry_timeout

        self.keepalive_task = None
        self.keepalive_every = keepalive_every
        self.keepalive_pause = False

    async def _keepalive(self, stream):
        while True:
            try:
                await asyncio.sleep(self.keepalive_every)

                if self.keepalive_pause:
                    logger.debug("Keepalive paused, skipping check")

                if not stream.available.is_set():
                    logger.debug(
                        "Keepalive failed, no connection. Waiting for connection...")
                    await stream.available.wait()
                    logger.debug("Keepalive resumed")

                observer = PingObserver()
                observer.start(stream.observable)

                await stream.send(ping)

                async def no_ping():
                    if not stream.available.is_set():
                        logger.debug(
                            f"Server {self.addr}:{self.port} timed out.")
                        await stream.reconnect()

                await observer.on_no_ping(no_ping, timeout=4)
            except BrokenPipeError:
                if not stream.available.is_set():
                    logger.warning("Connection is closed. Reconnecting")
                    await stream.reconnect()

            except:
                logger.exception("Unknown exception in keepalive.")

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

        shutdown_event = asyncio.Event()

        def receive(message):
            try:
                type = message[Keys.TYPE]

                if type not in Types.all:
                    logger.critical(f"Unknown message type {type}")
                    return

                if type not in dispatch_table:
                    logger.debug(
                        f"Message type {type} does not have an handler.")
                    return

                handler = dispatch_table[type]
                asyncio.ensure_future(handler(self, stream, message))
            except:
                logger.exeption("Unexpected exception in message handling")

        def teardown():
            stream.close()
            shutdown_event.set()

        stream.observable.subscribe(receive, logger.critical, teardown)

        async def shutdown():
            done, pending = await asyncio.wait([shutdown_event.wait(), self._keepalive(stream)], return_when=concurrent.futures.FIRST_COMPLETED)

            for task in pending:
                task.cancel()

            return done

        return Handle(shutdown(), stream)

    async def work(self, stream, msg):
        work_id = msg[Keys.WORK_ID]

        task = self.tasks[msg[Keys.TASK]]
        args, kwargs = msg[Keys.ARGS], msg[Keys.KWARGS]

        task = task.implementation(*args, **kwargs)
        try:
            result = await task
        except Exception as e:
            logger.exception("Exception in work scheduling.")
            await stream.send(work_failed(work_id, traceback.format_exc()))
            return

        await stream.send(work_result(work_id, result))

    async def recv_ping(self, stream, msg):
        logger.log(0, "Recived ping from server.")


dispatch_table = {
    Types.DO_WORK: Orchestrator.work,
    Types.PING: Orchestrator.recv_ping
}
