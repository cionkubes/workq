import pytest
import asyncio


@pytest.mark.asyncio
async def test_many_separate_files():
    await asyncio.sleep(3)
