import pytest
import asyncio

from workq.apickle import dump, load


@pytest.mark.asyncio
async def test_many_separate_streams(streampair_generator, objects):
    for obj, streampair in zip(objects, streampair_generator):
        r, w = streampair
        await dump(obj, w)
        assert obj == await load(r)
