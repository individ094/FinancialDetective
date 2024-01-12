import asyncio
import pandas as pd
from typing import List
from prisma import Prisma
from datetime import datetime
#from prisma import PrismaClient


"""
File to test the connection to the database and to test the queries.

"""

async def main() -> None:
    prisma = Prisma()
    await prisma.connect()

    # Test querie
    new_category = await prisma.transactions.create(
        data={
            'date': datetime.now(),
            'bookingType': 'income',
            'payee': 'John Doe',
            'purpose': 'Salary payment',
            'accountNumber': '12345678',
            'amount': 2500.0,
            'isBalanceRelevant': True,
            'category': {
                'connect': {'category': 'Salary'}
            },
            'other': 'Extra bonus',
            'account': 'Savings',
        },
    )

    print(new_category)

    await prisma.disconnect()

async def add_transaction(prisma: Prisma, date: datetime, booking_type: str, payee: str, purpose: str,
                           account_number: str, amount: float, is_balance_relevant: bool, category: str,
                           other: str, account: str) -> dict:
    new_transaction = await prisma.transactions.create(
        data={
            'date': date,
            'bookingType': booking_type,
            'payee': payee,
            'purpose': purpose,
            'accountNumber': account_number,
            'amount': amount,
            'isBalanceRelevant': is_balance_relevant,
            'category': {
                'connect': {'category': category}
            },
            'other': other,
            'account': account,
        },
    )
    return new_transaction

if __name__ == '__main__':
    asyncio.run(main())