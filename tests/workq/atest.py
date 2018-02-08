import pytest
import asyncio

from workq.apickle import dump, load


@pytest.mark.asyncio
async def test_many_separate_streams(streampair_generator, objects):
    r, w = next(streampair_generator)
    o = objects[0]
    for streampair, obj in zip(streampair_generator, objects):
        r, w = streampair

        await dump(o, w)
        assert o == await load(r)
