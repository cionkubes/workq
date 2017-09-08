from .pickler import dump
from .unpickler import load




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
