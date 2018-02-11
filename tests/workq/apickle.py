import asyncio

import pytest

from workq.apickle import load, dump


@pytest.mark.asyncio
async def test_many_separate_files(event_loop, filepair_generator, objects):
    print("1")
    for obj, filepair in zip(objects, filepair_generator):
        print("2")
        r, w = filepair
        write_task = asyncio.ensure_future(dump(obj, w), loop=event_loop)
        read_task = asyncio.ensure_future(load(r), loop=event_loop)
        await write_task
        assert obj == await read_task


@pytest.mark.asyncio
async def test_many_same_files(event_loop, filepair, objects):
    r, w = filepair

    print("3")
    for obj in objects:
        print("4")
        write_task = asyncio.ensure_future(dump(obj, w), loop=event_loop)
        read_task = asyncio.ensure_future(load(r), loop=event_loop)
        await write_task
        assert obj == await read_task
