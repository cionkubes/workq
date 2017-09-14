import asyncio

from cion.common.workq.server import Server
from cion.common.webhook import interface as webhook
from logzero import logger


async def test():
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
