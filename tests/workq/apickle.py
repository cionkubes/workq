import pytest

from workq.apickle import load, dump


@pytest.mark.asyncio
async def test_many_separate_files(event_loop, filepair_generator, objects):
    for obj, filepair in zip(objects, filepair_generator):
        r, w = filepair
        write_task = event_loop.create_task(dump(obj, w))
        assert obj == await load(r)
        await write_task


@pytest.mark.asyncio
async def test_many_same_files(event_loop, filepair, objects):
    r, w = filepair

    for obj in objects:
        write_task = event_loop.create_task(dump(obj, w))
        assert obj == await load(r)
        await write_task
