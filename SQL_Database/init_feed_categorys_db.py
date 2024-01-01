import asyncio
import pandas as pd
from typing import List
from prisma import Prisma
from datetime import datetime
#from prisma import PrismaClient


async def main() -> None:
    prisma = Prisma()
    await prisma.connect()

    # Test querie
    new_category = await prisma.category.create(
        data={
            'category': 'None',
        },
    )
    print(new_category)

    new_category = await prisma.category.create(
        data={
            'category': 'Income',
        },
    )
    print(new_category)

    new_category = await prisma.category.create(
        data={
            'category': 'Food',
        },
    )
    print(new_category)

    await prisma.disconnect()

async def add_category(prisma: Prisma, category: str) -> dict:
    new_category = await prisma.category.create(
        data={
            'category': category
        }
    )
    return new_category

if __name__ == '__main__':
    asyncio.run(main())