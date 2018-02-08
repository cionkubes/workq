import pytest
import asyncio


@pytest.mark.asyncio
async def test_many_separate_files(streampair_generator, objects):
    await asyncio.sleep(3)

    for object in objects:
        pass
