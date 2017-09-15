import asyncio

import pytest

from cion.common.workq.apickle import dump, load


@pytest.mark.asyncio
async def test_readline(streampair, event_loop):
    r, w = streampair

    lines = [b'nHuJONnMFE\n', b'W66tOBAfrw943gh\n', b'SyDMv143gaGE#%@274577&$TTHU\n']

    asyncio.ensure_future(w.write(b''.join(lines)), loop=event_loop)

    for line in lines:
        assert line == await r.readline()


@pytest.mark.asyncio
async def test_read_exactly(streampair, event_loop):
    r, w = streampair

    strings = [b'nHuJONnMFE', b'W66tOBAfrw943gh', b'SyDMv143gaGE#%@274577&$TTHU']

    asyncio.ensure_future(w.write(b''.join(strings)), loop=event_loop)

    for line in strings:
        assert line == await r.read_exactly(len(line))


@pytest.mark.asyncio
async def test_many_separate_streams(streampair_generator, objects):
    for obj, streampair in zip(objects, streampair_generator):
        r, w = streampair
        await dump(obj, w)
        assert obj == await load(r)


@pytest.mark.asyncio
async def test_many_same_stream(streampair, objects):
    r, w = streampair

    for obj in objects:
        await dump(obj, w)
        assert obj == await load(r)
