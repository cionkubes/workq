import asyncio

import pytest

from workq.apickle import load, dump


@pytest.mark.asyncio
async def test_many_separate_files(event_loop, channel_generator, objects):
    for obj, channel in zip(objects, channel_generator):
        await dump(obj, channel)
        assert obj == await load(channel)


@pytest.mark.asyncio
async def test_many_same_files(event_loop, channel, objects):
    for obj in objects:
        await dump(obj, channel) 
        assert obj == await load(channel)
        