import asyncio

from common.webhook import interface as webhook
from common.workq.server import Server


async def test():
    result = await webhook.hello("Alan")
    print(result)


def main():
    loop = asyncio.get_event_loop()
    orchestrator = Server()
    orchestrator.enable(webhook)

    socket, server = orchestrator.run(addr='localhost', port=8890)
    print(f'Serving on {socket.getsockname()}')
    loop.create_task(test())
    loop.run_until_complete(server)
    loop.close()


if __name__ == '__main__':
    main()