
import asyncio
import pandas as pd
import math
from typing import List, Dict
from prisma import Prisma, Client
from datetime import datetime, timedelta

#from prisma import PrismaClient


class TransactionsUpdater:
    def __init__(self, prisma: Prisma(), table_name: str):
        self.prisma = prisma
        self.table_name = table_name
        self.db = Prisma()

    """Updates the database with the data from the given dataframe.
    
    Parameters:
    ----------
    df: pd.DataFrame
        The dataframe containing the data to be inserted into the database.
    
    Returns:
    ----------
    None
    """
    async def update_database(self, df: pd.DataFrame):
        
        # connecting to database
        await self.db.connect()

        # Sort dataframe by date column
        df = df.sort_values('Validated_Date')

        # Get the latest date in the database as a datetime object
        latest_date = await self.get_latest_date_in_database()

        # Filter the dataframe to only include rows that are after the latest date in the database
        df = df[df['Validated_Date'] >= latest_date]

        # Convert dataframe to list of dictionaries
        rows_to_insert = df.to_dict('records')

        # Update each row with the correct categoryID from the Category table
        rows_to_insert = await self.update_categories(rows_to_insert, latest_date)

        # Insert rows into the database
        await self.insert_rows(rows_to_insert)

    """Gets the latest/newest date in the database.
    
    Parameters:
    ----------
    None
    
    Returns:
    ----------
    latest_date: datetime
        The latest date in the database as a datetime object.
    """
    async def get_latest_date_in_database(self) -> datetime:

        db = Prisma()
        await db.connect()

        if self.table_name == "Transactions":
            result = await db.transactions.find_first(order=[{"date": "desc"}])

        else:
            raise ValueError(f"Unknown table name: {self.table_name}")
        
        if result is None:
            # If there are no rows in the database, return a datetime object with a year of 1900
            return datetime(1900, 1, 1)
        else:
            # Extract the datetime object from the row and return it
            latest_date = datetime.strptime(result.date.strftime("%Y-%m-%d"), "%Y-%m-%d")
            return latest_date

    """Inserts a list of rows into the database.
    
    Parameters:
    ----------
    rows: List[dict]
        A list of dictionaries, where each dictionary represents a row to be inserted into the database.
    
    Returns:
    ----------
    None
    """
    async def insert_rows(self, rows: List[dict]):
        for row in rows:
            # Convert the date column to a datetime object
            #row["Validated_Date"] = datetime.utcfromtimestamp(row['Validated_Date'].timestamp())
            # Replace any nan values in the dictionary with None
            # row = {k: (v if not isinstance(v, float) or not math.isnan(v) else 'None') for k, v in row.items()}


            # Check if the row already exists in the database
            if self.table_name == "Transactions":
                existing_record = await self.db.transactions.find_first(
                    where={
                    "date": row["Validated_Date"], 
                    "amount": row["Amount"], 
                    "payee": row["Payment_Requester"], 
                    "purpose": row["Purpose"],
                    "other": row["Additional_Information"],
                    }
                )

            if not existing_record:
                # write to database
                await TransactionsUpdater.write_to_db(data=row, client=self.db)


    # Check for each row if the payee and purpose combination already exists in the database
    # If it does, update the row with the categoryID from the database
    async def update_categories(self, rows: List[dict], latest_date: datetime) -> List[dict]:
        #connect to the database
        da = Prisma()
        await da.connect()

        # Get the list with all the purpose combinations
        purposes = await da.purpose.find_many()
        
        # Write the categoryID from the database into the dictionary with the categoryID as key and the category as value 
        categories = await da.category.find_many()
        # for all categories in the database create a key-value pair in a category_dict dictionary
        category_dict = {}
        for category in categories:
            # add a key-value pair with category.id as key and category.category as value
            category_dict[category.id] = category.category

        for row in rows:
            # Check if the purpose and payee substring combinations of the database are in the row
            # If the substring combination is in the row, update the row with the categoryID from the database
            for purpose in purposes:
                # if purpose.purpose is "None", then dont check for the purpose substring
                if purpose.purposeString == "None":
                    if purpose.payeeString.lower() in row["Payment_Requester"].lower():
                        # insert the category value from the categories dictionary with purpose.categoryID as the key
                        row["Category"] = category_dict[purpose.categoryID]
                        break
                elif purpose.purposeString != "None":
                    if purpose.payeeString.lower() in row["Payment_Requester"].lower() and purpose.purposeString.lower() in row["Purpose"].lower():
                        # insert the category value from the categories dictionary with purpose.categoryID as the key
                        row["Category"] = category_dict[purpose.categoryID]
                        break
                


        return rows

    """Writes a row with predefined collumns to an SQL Database using the prisma client.
    
    Parameters:
    ----------
    data: Dict[str, any]
        The dictionary containing the data to be inserted into the database.
    client: Client
        The prisma client to be used to connect to the database.
    
    Returns:
    ----------
    None
    """
    async def write_to_db(data: Dict[str, any], client: Client):        
        # create entry in the SQL database
        await client.transactions.create({
            'date': data["Validated_Date"],
            'bookingType': data['Transaction_Type'],
            'payee': data['Payment_Requester'],
            'purpose': data['Purpose'],
            'accountNumber': data['Account_Number'],
            'amount': data['Amount'],
            'isBalanceRelevant': data['BalanceRelevant'],
            'category':{
                'connect': {'category': data['Category']}# if data['Kategorie'] != 'None' else 'Other']}
            },
            'other': data['Additional_Information'],
            'account': data['Account']
        })

        print(f"Inserted row into database: ")
    