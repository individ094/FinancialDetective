# Importing modules
import database_analyser
import asyncio

#--------------------------
# Variables
month = 6
year = 2023
categorys = []

#--------------------------


async def main():
    try:
        # Initialise the database
        da = database_analyser.TransactionsAnalyser()
        await da.initialize_database()
        categorys = await da.db.category.find_many()
        #print(categorys)
        
        # Get all trasactions from the "Transactions" table that happened 
        # in the month and year specified by the "month" and "year" variables 
        transactions = await da.get_transactions_for_specific_month(month, year)
        #print(transactions)
        
        # Create a dictionary with all the categorys as keys and the value of each key is 0
        # This will be used to add up the transactions for each category
        categorys_dict = {}
        for category in categorys:
            categorys_dict[category.category] = 0
        #print(categorys_dict)
        
        # sort all transactions into seperate lists based on their category
        for category in categorys:
            transactions_sorted_by_category = []
            for transaction in transactions:
                if transaction.categoryID == category.id:
                    transactions_sorted_by_category.append(transaction)
            
            # Adding up all ammount values of all transactions in the transactions_sorted_by_category list
            total = 0
            for transaction in transactions_sorted_by_category:
                total += transaction.amount
            
            # Adding the total value of all transactions in the transactions_sorted_by_category list to the categorys_dict dictionary
            categorys_dict[category.category] = total
        # print the dictionary with each new key value pair in a new line
        for key, value in categorys_dict.items():
            print(key, value)
        
        # print empty line for seperation
        print()

        # Add up all the positive and negative values in the transaction list
        positive_total = 0
        negative_total = 0
        for transaction in transactions:
            if transaction.amount > 0:
                positive_total += transaction.amount
            else:
                negative_total += transaction.amount
        
        # Print the total positive and negative values
        print("Positive total:", positive_total)
        print("Negative total:", negative_total)

        # Print Summ of all transactions
        print("Sum of all transactions:", positive_total + negative_total)
        # Print empty line for seperation
        print()
        
        await da.db.disconnect()


        # Return the categorys and the amount of money spent in each category

    except Exception as e:
        print(e)


if __name__ == "__main__":
    asyncio.run(main())
