import asyncio
import pandas as pd
import math
from collections import Counter
from typing import List, Dict
from prisma import Prisma, Client
from datetime import datetime, timedelta
#from prisma import PrismaClient


class TransactionsAnalyser():

    def __init__(self):
        self.db = Prisma()
    

    """Initialise the database connection"""
    async def initialize_database(self):
        await self.db.connect()
        

    # function to connect to the "Transactions" table in the database and search for the string 
    # that appears most often in the "payee" collumn and has also the "Category"="None" and "isBalanceRelevant" = true
    """Returns the most common string from the "payee" collumn in the "Transactions" table that has the "Category"="None" and "isBalanceRelevant" = true

    Parameters
    ----------
    None

    Returns
    -------
    payee_substring: str
        A payee substring tha best categorises multiple versioins of the most common payee string in the "payee" collumn 
        in the "Transactions" table that has the "Category"="None" and "isBalanceRelevant" = true

    """
    async def get_most_common_payee_and_purpose_substring_without_category(self) -> str:
            
            #db = Prisma()
            # await self.db.connect()

            # return a list of all the string that appear in the "payee" collumn of the "Transaction" table
            # and have the "categoryID" is 1, and this is id linked in the Category table "id"=1 is "category"="None". Also "isBalanceRelevant" is equal to true in the Transactions table
            result = await self.db.transactions.find_many(
                where={
                    'categoryID': 1,
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

                # get all entrys that contain the "payee_count_sorted[0][0]" string in the "payee" collumn
                sub_result = await self.db.transactions.find_many(
                    where={
                        'categoryID': 1,
                        'isBalanceRelevant': True,
                        'payee': {
                            'contains': payee_count_sorted[0][0],
                        },
                    }  
                )

                # Show the user the most common payee string
                print("The most common payee string is: " + payee_count_sorted[0][0])


                # Print, if possible, the first 3 entrys from the sub_result list
                # print each of this valuse "sub_result[i].date, sub_result[i].payee, sub_result[i].amount, sub_result[i].isBalanceRelevant, sub_result[i].purpose" 
                # in a f-string with a short description of the value
                entrys = len(sub_result)
                i = 0
                print(f"The first entrys from the sub_result list are:")
                while i >= 0:
                    # print each of this valuse "sub_result[i].date, sub_result[i].payee, sub_result[i].amount, sub_result[i].isBalanceRelevant, sub_result[i].purpose" 
                    # in a f-string with a short description of the value
                    print(f"Payee: {sub_result[i].payee}, Purpose: {sub_result[i].purpose}, Amount: {sub_result[i].amount}, isBalanceRelevant: {sub_result[i].isBalanceRelevant}, Date: {sub_result[i].date}")
                    i += 1
                    if i == 3:
                        break
                    if i == entrys:
                        break
                print("---------------------------------")             

                # Ask the user for a payeeSubstring that best categorises multiple different versions of the payee string
                payee_substring = input("Please enter a substring that best categorises multiple different versions of the *payee* string: ")

                # if nothin is entered, the most common payee string is returned
                if payee_substring == "":
                    payee_substring = sub_result[0].payee

                # check if the "payee_substring" string is contained in the most common payee string ("sub_result[0].payee")
                while not payee_substring.lower() in sub_result[0].payee.lower():
                    print("The entered payeeSubstring is not contained in the most common payee string.")
                    payee_substring = input("Please enter a substring that best categorises multiple different versions of the payee string: ")
                print("---------------------------------")
                
                # Ask the user for a purposeSubstring that best categorises multiple different versions of the payee string
                purpose_substring = input("Please enter a substring that best categorises multiple different versions of the *purpose* string: ")

                # if nothin is entered, the most common payee string is returned
                if purpose_substring == "":
                    purpose_substring = sub_result[0].purpose

                # check if the "purpose_substring" string is contained in the most common payee string ("sub_result[0].purpose")
                # if not ask the user if he wants to continue anyway, if yes continue with the entered purposeSubstring
                # if not ask again for a purposeSubstring
                force_update = False
                while not purpose_substring.lower() in sub_result[0].purpose.lower():
                    print("The entered purposeSubstring is not contained in the most common purpose string.")
                    continue_anyway = input("Do you want to continue with your specific purposeSting? (Y/n): ")
                    while continue_anyway != "Y" and continue_anyway != "n":
                        print("Please only enter 'Y' or 'n'")
                        continue_anyway = input("Do you want to continue anyway? (Y/n): ")
                    if continue_anyway == "Y":
                        force_update = True
                        break
                    elif continue_anyway == "n":
                        purpose_substring = input("Please enter a substring that best categorises multiple different versions of the purpose string: ")

                return payee_substring, purpose_substring, force_update
            
    """Assigns all strings form the "payees" list the same categoryID in the "Transactions" table

    Parameters
    ----------
    payees: set[str]
        List of all "payee" strings that should be assigned the same categoryID
    categoryID: int
        The categoryID that should be assigned to each row that contains the "payee" string

    Returns
    -------
    success: bool
        True if the categoryID was successfully changed, False if not
   
    """
    async def assign_category_to_payee(self, payees: set[str], purpose: str, categoryID: int, force_update: bool) -> bool:
        #await self.db.connect()

        # assign all rows in the "Transactions" table that contain the "payee" string the "categoryID"
        # if purpose is not "None", also filter for the purpose string
        for payee in payees:
            if purpose == "None" or force_update == True:
                await self.db.transactions.update_many(
                    where={
                        'payee': payee
                    },
                    data={
                        'categoryID': categoryID
                    }
                )
            else:
                await self.db.transactions.update_many(
                    where={
                        'payee': payee,
                        'purpose':{"contains": purpose}
                    },
                    data={
                        'categoryID': categoryID
                    }
                )

        return True
    
    """ Get the categoryID of a specific "category" string from the "Category" table

    Parameters
    ----------
    category: str
        The category string for which the categoryID should be returned

    Returns
    -------
    categoryID: int
        Associated categoryID of the "category" string from the "Category" table 
    """
    async def get_category_id_form_category_string(self, category: str) -> int:
        #await self.db.connect()

        # return all category strings form the "Category" table
        categorys = await self.db.category.find_many(
            where={
                'category': category
            }
        )
        # Test if the "categorys" list has only one element
        if len(categorys) == 1:
            # return the categoryID of the "category" string
            return categorys[0].id
        else:
            # If there are more or less than one element in the "categorys" list, raise an error
            raise ValueError("There are more or less than one category with the same name in the database")
        

        """Find all "payee" strings that contain the "payee_substring" string
        
        Parameters
        ----------
        payee_substring: str
            The substring that should be contained in the "payee" string

        Returns
        -------
        payees: List[str]
            List of all "payee" strings that contain the "payee_substring" string
        
        """
    
    """
    Get all payees that contain the "payee_substring" string from the "Transactions" table

    Parameters
    ----------
    payee_substring: str
        The substring that should be contained in the "payee" string

    Returns
    -------
    payees: List[str]
        List of all "payee" strings that contain the "payee_substring" string

    """
    async def get_similar_payees_from_payee_substring(self, payee_substring: str) -> set[str]:
        #await self.db.connect()

        # return all payee strings form the "Transactions" table that contain the "payee_substring" string
        # compare all strings in lower case writing
        payees = await self.db.transactions.find_many(
            where={
                'payee': {
                    'contains': payee_substring,
                }
            }  
        )
        
        # transform the result into a set with all the strings that appear in the "payee" collumn
        similar_payees = set()
        for transaction in payees:
            similar_payees.add(transaction.payee)
        
        return similar_payees
    
    """Return all available Categorys from the "Category" table
    
    Parameters
    ----------
    None
    
    Returns
    -------
    categorys: List[str]
        List of all available Categorys from the "Category" table
    
    """
    async def get_all_categorys(self) -> List[str]:
        #await self.db.connect()

        # return all category strings form the "Category" table
        categorys = await self.db.category.find_many()

        # transform the result into a list with all the strings that appear in the "category" collumn
        categorys_list = []
        for category in categorys:
            categorys_list.append(category.category)

        return categorys_list
    

    """
    Test if the given category string is valid

    Parameters
    ----------
    category: str
        The category string that should be tested

    Returns
    -------
    True if the category string is valid, False if not
    """
    async def test_if_category_is_valid(self, category: str) -> bool:

        categorys = await self.get_all_categorys()

        if category in categorys:
            return True
        else:
            return False
        

    """Ask user what category should be assigned to a specific "payee" string
    
    Parameters
    ----------
    payee_substring: str
        The payee_substring string for which the user should assign a category
    
    Returns
    -------
    category: str
        The category string that the user assigned to the payee_substring string
        
    """
    async def ask_user_for_category(self, payee_substring: str) -> str:
        #await self.db.connect()

        # get all categorys form the "Category" table
        categorys = await self.get_all_categorys()

        # ask the user for a category
        print(f"Please assign a category to the payee: {payee_substring}")
        print("0: Skipp this payee")
        for i in range(len(categorys)):
            print(f"{i+1}: {categorys[i]}")
        category = input("Please enter the number of the category: ")

        # if category is 0, return an empty string
        if category == "0":
            return ""
        
        # check if the user entered a valid number
        while not category.isdigit() or int(category) > len(categorys) or int(category) < 0:
            print("Please enter a valid number")
            category = input("Please enter the number of the category: ")

        # If the user entered 1 for no category, ask the user if he wants to add a new category
        if int(category) == 1:
            # ask the user if he wants to add a new category, otherwise use 0 as category
            add_new_category = input("Do you want to add a new category? (Y/n): ")
            while add_new_category != "Y" and add_new_category != "n":
                print("Please only enter 'Y' or 'n'")
                add_new_category = input("Do you want to add a new category? (Y/n): ")

            if add_new_category == "n":
                return categorys[int(category)-1]
            elif add_new_category == "Y":
                # ask the user for the name of the new category
                new_category = input("Please enter the name of the new category: ")
                # check if the user entered a valid category name
                while new_category == "" or new_category in categorys:
                    print("Please enter a valid/new category name")
                    new_category = input("Please enter the name of the new category: ")

                # add the new category to the "Category" table
                await self.db.category.create(
                    data={
                        'category': new_category
                    }
                )

                # return the category string that the user assigned to the payee_substring string
                return new_category
            
        
        # return the category string that the user assigned to the payee_substring string
        return categorys[int(category)-1]
    
    """Get Transactions from a specific month and year
    
    Parameters
    ----------
    month: int
        The month for which the transactions should be returned
    year: int
        The year for which the transactions should be returned
        
    Returns
    -------
    transactions: List[Transaction]
        List of all transactions that happened in the month and year specified by the "month" and "year" variables
    """
    async def get_transactions_for_specific_month(self, month: int, year: int, category="All") -> List:
        #await self.db.connect()

        # get the first and last day of the month, depending on what month and year is specified
        first_day_of_month = datetime(year, month, 1)
        # create a dictionary how many days every month of the year has
        days_in_month = {
            1: 31,
            2: 28,
            3: 31,
            4: 30,
            5: 31,
            6: 30,
            7: 31,
            8: 31,
            9: 30,
            10: 31,
            11: 30,
            12: 31,
        }
        last_day_of_month = datetime(year, month, days_in_month[month])


        # return all transactions that happened between the first and last day of the month and have
        # the category specified by the "category" variable
        if category != "All":

            # test if the category is valid, if it is true continue,if false throw an error with an explanation
            if await self.test_if_category_is_valid(category) == False:
                raise ValueError("The specified category for transactions is not valid")

            transactions = await self.db.transactions.find_many(
                where={
                    'isBalanceRelevant': True,
                    'date': {
                        'gte': first_day_of_month,
                        'lte': last_day_of_month
                    },
                    'category': {'category': category}
                } 
            )
        elif category == "All":
            transactions = await self.db.transactions.find_many(
                where={
                    'isBalanceRelevant': True,
                    'date': {
                        'gte': first_day_of_month,
                        'lte': last_day_of_month
                    }
                }
            )

        return transactions
    
    """Create a new entry in the "Purpose" table with a payeeString and a category
    
    Parameters
    ----------
    payee_substring: str
        The payee_substring that will be assigned to a category
    category: str
        The category that will be assigned to the payee_substring
    
    Returns
    -------
    success: bool
        True if the category was successfully assigned to the payee_substring, False if not
    """
    async def create_purpose_for_substring(self, payee_substring: str, purpose: str, category: str) -> bool:
        #await self.db.connect()

        # get the categoryID of the category string
        category_id = await self.get_category_id_form_category_string(category)

        # create a new entry in the "Purpose" table with the payee_substring and the categoryID
        try:
            await self.db.purpose.create(
                data={
                    'payeeString': payee_substring,
                    'categoryID': category_id,
                    'purposeString': purpose
                }
            )
        except Exception as e:
            print(e)
            return False

        return True




    
async def main():
    try:
        analyser = TransactionsAnalyser()
        await analyser.initialize_database()
        # search for the most common payee without a category
        payee_substring, purpose_substring, force_update = await analyser.get_most_common_payee_and_purpose_substring_without_category()
        # print(f"Most common payee substring ist: {payee_substring}")

        # get a set() of all payees that contain the payee_substring 
        similar_payees = await analyser.get_similar_payees_from_payee_substring(payee_substring)

        # ask the user what category_ID should be assigned to the payee_substring
        category = await analyser.ask_user_for_category(payee_substring)

        category_ID = await analyser.get_category_id_form_category_string(category)

        # update the Purpose database with the new category
        await analyser.create_purpose_for_substring(payee_substring=payee_substring, purpose=purpose_substring ,category=category)

        await analyser.assign_category_to_payee(payees=similar_payees, purpose=purpose_substring , categoryID=category_ID, force_update=force_update)
        await analyser.db.disconnect()

    except Exception as e:
        print(e)
        await analyser.db.disconnect()


if __name__ == "__main__":
    asyncio.run(main())