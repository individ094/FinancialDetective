import asyncio
import pandas as pd
from typing import List
from prisma import Prisma
from datetime import datetime
#from prisma import PrismaClient


"""
Test database connection to the purpose table.
"""

async def main() -> None:
    prisma = Prisma()
    await prisma.connect()

    # Test querie
    new_purpose = await prisma.purpose.create(
        data={
            'category': {
                'connect': {'category': 'Food'}
            },
            'purposeString': 'None',
            'payeeString': 'aldi',
        }
    )


    print(new_purpose)

    await prisma.disconnect()


if __name__ == '__main__':
    asyncio.run(main())




async def add_purpose(prisma: Prisma, category: str, purpose_string: str) -> dict:
    category = await prisma.category.find_unique(where={'category': category})
    new_purpose = await prisma.purpose.create(
        data={
            'category': {
                'connect': {'id': category['id']}
            },
            'purposeString': purpose_string,
        }
    )
    return new_purpose