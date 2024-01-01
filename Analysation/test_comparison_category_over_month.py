### Imports
from database_analyser import TransactionsAnalyser as ta
import asyncio

# --------------------------
### Variables
category = 'Food'
month = 11
year = 2023
cmb = 10 # CompaerMonthBackwards (how many months back should be compared)


# --------------------------
### Description
# This script will compare the expences of multiple month of a specific category and print the results 
#TODO: later the results will be displayed in a graph

# --------------------------
### Functions
"""This function will compare the expences of multiple month of a specific category and print the results
    
    Parameters
    ----------
    category: str
        the category that should be compared
    month: int
        the month of the first transaction that should be compared
    year: int
        the year of the first transaction that should be compared
    cmb: int
        how many months back should be compared, excluding the month specified by the "month" and "year" variables

    Returns
    -------
    None
        This function does not return anything
    
    Raises
    ------
    None
        This function does not raise anything
    """
async def compare_category_over_month(category: str, month: int, year: int) -> None:
    
    database = ta()
    await database.initialize_database()

    # create all the month an year combinations that need to be compared
    # starting with the month and year specified by the "month" and "year" variables
    # and going back in time by the amount of months specified by the "cmb" variable
    # example: cmb = 4, month = 12, year = 2022
    # result: 12/2022, 11/2022, 10/2022, 09/2022, 08/2022
    # the result will be stored in the "month_year_combinations" list
    month_year_combinations = []
    month_year_combinations.append((month, year))
    for i in range(cmb):
        if month == 1:
            month = 12
            year -= 1
        else:
            month -= 1
        month_year_combinations.append((month, year))



    # Create a list to store all transactions in
    transactions = []

    # Get all transactions from the "Transactions" table that have the category 
    # specified by the "category" variable, and happend in the month and year 
    # specified by the month_year_combinations list
    for month, year in month_year_combinations:
        monthly_transactions = await database.get_transactions_for_specific_month(month, year, category)
        transactions.append(monthly_transactions)
    #print(transactions)

    # add up all the transactions in each of the lists in the transactions list
    # and store the result in the "total" dictionary
    total = {}
    for transaction_list in transactions:
        # the key of the total dictionary is the month and year of the transactions
        # example: 12/2022
        key = f'{transaction_list[0].date.month}/{transaction_list[0].date.year}'
        total[key] = 0
        for transaction in transaction_list:
            total[key] += transaction.amount
        total[key] = abs(total[key])
        total[key] = round(total[key], 2)

    """
    calculate the difference to the previous month in percentage and return the result

    Parameters
    ----------
    type: str
        "to_each_previous_month" will calculate the difference to the previous month in percentage
        "to_first_month(static)" will calculate the difference to the first month in percentage

    Returns
    -------
    difference: dict
        a dictionary with the difference to the previous month in percentage
        example: {"12/2022": 0, "11/2022": 0.5, "10/2022": 0.2, "09/2022": 0.3, "08/2022": 0.1}   

    """
    def calculate_difference (type:str):
        difference = {}

        if type == "to_each_previous_month":
            for i in range(len(total)):
                if i == 0:
                    difference[list(total.keys())[i]] = 0
                else:
                    difference[list(total.keys())[i]] = ((list(total.values())[i] - list(total.values())[i-1]) / list(total.values())[i-1]) * 100
        elif type == "to_first_month(static)":
            for i in range(len(total)):
                if i == 0:
                    difference[list(total.keys())[i]] = 0
                else:
                    difference[list(total.keys())[i]] = ((list(total.values())[i] - list(total.values())[0]) / list(total.values())[0]) * 100

        return difference
    
    # calculate for each month the difference to the first month in percentage
    #difference = calculate_difference("to_first_month(static)")
    difference = calculate_difference("to_each_previous_month")

    # print the total dictionary with each new key value pair in a new line
    # add after each key value pair the difference to the previous month in percentage
    # with two digits after the decimal point, and seperated by a vertical line
    for key, value in total.items():
        print(f"{key}, {value}     | {difference[key]:.2f}%")
    
    await database.db.disconnect()



if __name__ == '__main__':
    asyncio.run(compare_category_over_month(category=category, month=month, year=year))