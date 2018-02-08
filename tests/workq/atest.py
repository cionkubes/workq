import pytest
import asyncio

from workq.apickle import dump, load


@pytest.mark.asyncio
async def test_many_separate_streams(streampair_generator, objects):
    r, w = next(streampair_generator)
    obj = objects[0]
    await dump(obj, w)
    assert obj == await load(r)
