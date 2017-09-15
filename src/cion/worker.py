import asyncio

from cion.common.webhook import webhook
from cion.common.workq.worker import Orchestrator


@webhook.hello.implement
async def hello(name):
    print(f"Hello {name}")
    await asyncio.sleep(5)
    return "HW!"


def main():
    loop = asyncio.get_event_loop()

    orchestrator = Orchestrator('localhost', 8890)

    loop.run_until_complete(orchestrator.join(webhook))
    loop.close()


if __name__ == '__main__':
    main()
