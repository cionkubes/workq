import asyncio

from common.webhook import interface as webhook
from common.workq.client import Orchestrator


@webhook.hello.implement
async def hello(name):
    print(f"Hello {name}")
    # raise NotImplementedError()
    return "HW!"


def main():
    loop = asyncio.get_event_loop()

    orchestrator = Orchestrator('localhost', 8890)

    loop.run_until_complete(orchestrator.join(webhook))
    loop.close()

if __name__ == '__main__':
    main()