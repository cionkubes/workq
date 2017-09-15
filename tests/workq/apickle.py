import pytest

from cion.common.workq.apickle import load, dump


@pytest.mark.asyncio
async def test_many_separate_files(filepair_generator, objects):
    for obj, filepair in zip(objects, filepair_generator):
        r, w = filepair
        await dump(obj, w)
        assert obj == await load(r)


@pytest.mark.asyncio
async def test_many_same_files(filepair, objects):
    r, w = filepair

    for obj in objects:
        await dump(obj, w)
        assert obj == await load(r)


