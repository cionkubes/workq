import asyncio

from logzero import logger

from cion.common.webhook import webhook
from cion.common.workq.orchestrator import Server


async def test():
    while True:
        asyncio.ensure_future(start_task())
        await asyncio.sleep(10)


async def start_task():
    result = await webhook.hello("Alan")
    logger.info(result)


def main():
    loop = asyncio.get_event_loop()
    orchestrator = Server()
    orchestrator.enable(webhook)

    socket, server = orchestrator.run(addr='localhost', port=8890)
    logger.info(f'Serving on {socket.getsockname()}')
    loop.create_task(test())
    loop.run_until_complete(server)
    loop.close()


if __name__ == '__main__':
    main()
