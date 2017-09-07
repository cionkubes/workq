from common.workq.apickle.unpickler import load
from common.workq.apickle.pickler import dump


class LWrapper:
    def __init__(self, file):
        self.file = file

    async def read(self, n):
        return self.file.read(n)

    async def readline(self):
        return self.file.readline()


class DWrapper:
    def __init__(self, file):
        self.file = file

    async def write(self, *args, **kwargs):
        return self.file.write(*args, **kwargs)


async def test():
    import io

    stream = io.BytesIO()

    await dump({"Test": 32, u'res': b'hello'}, DWrapper(stream))
    stream.seek(0)

    obj = await load(LWrapper(stream))
    print(obj)


if __name__ == '__main__':
    import asyncio
    loop = asyncio.get_event_loop()
    loop.run_until_complete(test())