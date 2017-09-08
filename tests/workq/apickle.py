import pytest

from cion.common.workq.apickle import load, dump


class Wrapper:
    def __init__(self, file):
        self.file = file

    async def read(self, n):
        return self.file.read(n)

    async def readline(self):
        return self.file.readline()

    async def write(self, *args, **kwargs):
        return self.file.write(*args, **kwargs)

    def seek(self, at):
        self.file.seek(at)


@pytest.fixture
def stream():
    import io
    return Wrapper(io.BytesIO())


@pytest.mark.asyncio
async def test_pickling(stream):
    obj = {"Test": 32, u'res': b'hello'}
    await dump(obj, stream)
    stream.seek(0)

    assert obj == await load(stream)