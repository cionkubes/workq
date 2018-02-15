import asyncio

import pytest

from workq.apickle import load, dump


@pytest.mark.asyncio
async def test_many_separate_files(event_loop, filepair_generator, objects):
    for obj, filepair in zip(objects, filepair_generator):
        r, w = filepair

        # We need to wait for it to start writing because fileobjects
        # will return 0 bytes on reads instead of blocking until bytes are available
        await dump(obj, w)
        assert obj == await load(r)


@pytest.mark.asyncio
async def test_many_same_files(event_loop, filepair, objects):
    r, w = filepair

    for obj in objects:
        # We need to wait for it to start writing because fileobjects
        # will return 0 bytes on reads instead of blocking until bytes are available
        await dump(obj, w) 
        assert obj == await load(r)
        