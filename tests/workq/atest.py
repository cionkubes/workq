import pytest
import asyncio

from workq.apickle import dump, load


@pytest.mark.asyncio
async def test_many_separate_streams(streampair_generator, objects):
    for obj, streampair in zip(objects, streampair_generator):
        r, w = streampair
        assert True


@pytest.mark.asyncio
async def test_many_separate_streams(streampair_generator, objects):
    r, w = next(streampair_generator)

    await dump("Hello", w)
    assert "Hello" == await load(r)
