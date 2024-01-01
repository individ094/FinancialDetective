import asyncio
import pandas as pd
from collections import Counter
from typing import List
from prisma import Prisma
from datetime import datetime

import database_analyser as da
#from prisma import PrismaClient


class TransactionsAnalyser():

    def __init__(self):
        self.db = Prisma()

    # function to connect to the "Transactions" table in the database and search for the string 
    # that appears most often in the "payee" collumn and has also the "Category"="None" and "isBalanceRelevant" = true
    async def get_most_common_payee_without_category(self) -> str:
            
            #db = Prisma()
            await self.db.connect()

            # return a list of all the string that appear in the "payee" collumn of the "Transaction" table
            # and have the "categoryID" is 1, and this is id linked in the Category table "id"=1 is "category"="None". Also "isBalanceRelevant" is equal to true in the Transactions table
            result = await self.db.transactions.find_many(
                where={
                    'categoryId': 1,
                    'isBalanceRelevant': True
                }
            )

            if result is None:
                # If there are no rows in the database, return a empty string
                return ""
            else:
                # transform the result into a list with all the strings that appear in the "payee" collumn (somehow this is not possible with the find_many function)
                payee = []
                for transaction in result:
                    payee.append(transaction.payee)

                # count how many times each string appears in the list
                payee_count = Counter(payee)
                # sort the list by the number of times each string appears and return the first element of the list (most common string)
                payee_count_sorted = payee_count.most_common(1)
            
                return payee_count_sorted[0][0]
    
async def main():
    try:
        analyser = TransactionsAnalyser()
        get_most_common_payee_without_category = await analyser.get_most_common_payee_without_category()
    except Exception as e:
        print(e)
        get_most_common_payee_without_category = "Error"

    print(get_most_common_payee_without_category)
    # close connection to the database
    #await analyser.db.disconnect()

asyncio.run(main())


