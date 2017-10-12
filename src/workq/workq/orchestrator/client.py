import asyncio
from enum import IntEnum, auto
from uuid import uuid4

from logzero import logger

from ..net.messages import start_work, Keys


class Client:
    def __init__(self, addr, port, stream):
        self.addr = addr
        self.port = port
        self.stream = stream
        self.state = ClientState.IDLE
        self.futures = {}
        self.supported_interfaces = []

    async def start(self, task, args, kwargs):
        self.state = ClientState.WORKING

        work_id = str(uuid4())
        self.futures[work_id] = asyncio.get_event_loop().create_future()

        await self.stream.send(start_work(work_id, task, args, kwargs))
        return self.futures[work_id]

    def supports(self, interface):
        self.supported_interfaces.append(interface)

    async def work_done(self, msg):
        try:
            future = self.futures.pop(msg[Keys.WORK_ID])
        except KeyError:
            logger.error(f"Worker finished working on a cion_interface, but no such work was started by this orchestrator instance.")
            logger.info(", ".join(map(lambda item: f"{item[0]}: {item[1]}", msg.items())))
            return

        if len(self.futures) <= 0:
            self.state = ClientState.IDLE

        if future.cancelled():
            logger.debug("Future cancelled, discarding result.")
            return

        if Keys.WORK_EXC in msg:
            future.set_exception(msg[Keys.WORK_EXC])
        else:
            future.set_result(msg[Keys.WORK_RESULT])

    def disconnected(self):
        pass

    @property
    def name(self):
        return f"{self.addr}:{self.port}"


class ClientState(IntEnum):
    IDLE = auto()
    WORKING = auto()