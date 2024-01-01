# ------------------------ imports ------------------------
# packages
import pandas as pd
import numpy as np
import os
from fuzzywuzzy import fuzz

# ------------------------ global varibels ------------------------



# ------------------------ code ------------------------

class Analysation():

    def __init__(self) -> None:
        pass
    
   
    def get_bilanz_relevent_df(df_clean, verbose:int = 1):
        """
        This function filters the input DataFrame based on the "Bilanzrelevant" column. 
        It creates a new DataFrame that only includes rows where "Bilanzrelevant" is True.

        Parameters:
        df_clean (pandas.DataFrame): The input DataFrame to be filtered. 
        It is expected to have a column named "Bilanzrelevant".

        Returns:
        df_bilanz (pandas.DataFrame): The filtered DataFrame that only includes rows 
        where "Bilanzrelevant" is True.
        """ 
        # create a new df where Bilanzrelevant == True
        df_bilanz = df_clean.loc[df_clean["BalanceRelevant"] == True]
        if verbose >= 2:
            print(df_bilanz.head(10))

        return df_bilanz

    
    def get_month(df_bilanz):
        """
        This function groups the input DataFrame by month based on the 'Validated_Date' column. 
        It creates a list of DataFrames, each representing a different month.

        Parameters:
        df_bilanz (pandas.DataFrame): The input DataFrame to be grouped. 
        It is expected to have a column named 'Validated_Date' of datetime type.

        Returns:
        dfs (list of pandas.DataFrame): The list of DataFrames. Each DataFrame in the list 
        represents a different month from the 'Validated_Date' column in the input DataFrame.
        """
        #month = list(set(pd.DatetimeIndex(df_dates).month))

        # groupby your key and freq
        g = df_bilanz.groupby(pd.Grouper(key='Validated_Date', freq='M'))
        # groups to a list of dataframes with list comprehension
        dfs = [group for _,group in g]

        return dfs

    def income_spend_analysis(df_bilanz, verbose:int = 1):
        """
        This function performs an analysis on the income and spending of a given DataFrame. 
        It calculates the total income, total spending, and the balance (income - spending).

        Parameters:
        df (pandas.DataFrame): The input DataFrame to be analyzed. 
        It is expected to have columns named 'Income' and 'Spending' with numerical values.

        Prints into console:
        total_income (float): The total income calculated by summing up the 'Income' column.
        total_spending (float): The total spending calculated by summing up the 'Spending' column.
        balance (float): The balance calculated by subtracting total spending from total income.
        
        Returns:
        None
        """
        df_monthly_split = Analysation.get_month(df_bilanz)

        # iterate over the month and calculate kpi's (income, expenditure, monthly_reserves)
        for idx, df_monthly in enumerate(df_monthly_split):

            # Analyse if income transactions are falsy labled as income and are instead paybacks from earlyer transactions
            
            # Separate income and expenditure transactions
            df_income = df_monthly[df_monthly["Amount"] > 0].copy()
            df_expenditure = df_monthly[df_monthly["Amount"] < 0].copy()

            if verbose == 2:
                print(df_income)

            # Analyse if income transactions are falsy labled as income and are instead paybacks from earlyer transactions
            for income_idx, income_transaction in df_income.iterrows():
                similar_expenditures = []
                for expenditure_idx, expenditure_transaction in df_expenditure.iterrows():
                    # Calculate similarity ratio
                    ratio = fuzz.ratio(income_transaction["Payment_Requester"], expenditure_transaction["Payment_Requester"])

                    # If the ratio is above a certain threshold, they are considered similar
                    if ratio > 65:  # You can adjust this threshold as needed
                        similar_expenditures.append(expenditure_idx)

                if similar_expenditures:
                    # Sum up the amounts of similar expenditures
                    total_expenditure = df_expenditure.loc[similar_expenditures, "Amount"].sum()

                    # Subtract the income amount from the total expenditure
                    total_expenditure += income_transaction["Amount"]

                    # Remove the income transaction
                    df_income.drop(income_idx, inplace=True)

                    # Remove the individual expenditures
                    df_expenditure.drop(similar_expenditures, inplace=True)

                    # Add the new expenditure entry
                    new_entry = expenditure_transaction.copy()
                    new_entry["Amount"] = total_expenditure
                    df_expenditure = pd.concat([df_expenditure, pd.DataFrame(new_entry).T], ignore_index=True)

                    if verbose == 2:
                        print(f"Removed income transaction {income_idx} and expenditure transactions {similar_expenditures}")
                        print(f"Added new expenditure transaction with amount {total_expenditure}")

    
            # Combine and sort the dataframes
            df_monthly_repayment_corrected = pd.concat([df_income, df_expenditure]).sort_values(by="Validated_Date")

            #df_month = df_bilanz[pd.to_datetime(df_bilanz['Buchungsdatum']).dt.month == i]
            if verbose >= 2:
                print(df_monthly[["Validated_Date", "Payment_Requester", "Amount"]])

            beträge = list(df_monthly_repayment_corrected["Amount"])
            income = format(sum(i for i in beträge if i > 0),".2f")
            expenditure = format(sum(i for i in beträge if i < 0),".2f")
            monthly_reserves = format(sum(i for i in beträge),".2f")


            month = df_monthly["Validated_Date"].min()
            y_m = month.strftime("%Y-%m")
            if verbose >= 0:
                print(f"Month: {y_m}")
                print(f"net_income: {income}    |   net_expenditure: {expenditure}    |    monthly_reserves: {monthly_reserves} ")


