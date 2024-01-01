# Imports

import pandas as pd
import numpy as np
import os
import tqdm
import json


# Functions

def get_balance_irelevant_transactions_by_index(df: pd.DataFrame, verbose: int = 0):
    """
    This function:
        Compares every transaction in the dataframe with each other to find transactions between banks.
    
    Parameters
    ----------
    df : pd.DataFrame
        The DataFrame is a subset of all transactions df and contains only transactions that all have the same transaction amount.

    Returns
    -------
    index_list_is_not_BalanceRelevant : list
        A list with the index of the transactions that are not BalanceRelevant.
    """
    # split the dataframe into two dataframes, one containing all transaction with positive amounts and one containing all transaction with negative amounts in column "Amount"
    df_positive = df[df["Amount"] > 0]
    df_negative = df[df["Amount"] < 0]

    # compare the first row of df_positive with every row of df_negative
    index_list_is_not_BalanceRelevant = []

    if len(df_positive) != 0 and len(df_negative) != 0:
        for i, row_positive in enumerate(df_positive.itertuples()):
            for j, row_negative in enumerate(df_negative.itertuples()):
                # test order:
                # 1. the account must be a different string
                # 2. the date must be within 5 days of each other
                if row_positive.Account != row_negative.Account:
                    if abs((row_positive.Validated_Date - row_negative.Validated_Date).days) <= 5:
                        # check if the transaction is already in the index_list_is_not_BalanceRelevant
                        if row_positive.df_index not in index_list_is_not_BalanceRelevant:
                            index_list_is_not_BalanceRelevant.append(row_positive.df_index)
                        if row_negative.df_index not in index_list_is_not_BalanceRelevant:
                            index_list_is_not_BalanceRelevant.append(row_negative.df_index)
    
    return index_list_is_not_BalanceRelevant

def test_for_standing_orders(same_values_df):
    is_account_identical = True
    for j, account_row in enumerate(same_values_df.itertuples()):
        if j == 0:
            account_value = account_row.Account
        else:
            if account_value == account_row.Account:
                continue
            elif account_value != account_row.Account:
                return False
    return is_account_identical



def find_money_streams_between_banks(df: pd.DataFrame, verbose: int = 0):
    """
    This function:
        1. Finds transactions in the df that are between banks.
        2. Sets the column "isBalanceRelevant" to False for those transactions.
    
    Parameters
    ----------
    df : pd.DataFrame
        The DataFrame containing the transactions.
    
    Returns
    -------
    df : pd.DataFrame
        The DataFrame with the transactions between banks set to False.
    """

    # sort by date and reset index, baecause every index exists multiple times if multiple dataframes are combined
    df.sort_values(by=["Validated_Date"], ascending=False, inplace=True)
    df.reset_index(drop=True, inplace=True)

    # work with a copy of the dataframe, but keep the original index
    # df_copy = df.copy()
    df_copy = df[["Validated_Date", "Amount", "Account", "Purpose"]]
    df_copy["Abs_Amount"] = df_copy["Amount"].abs()
    df_copy_sorted = df_copy.sort_values(by=["Abs_Amount"], ascending=False)
    if verbose == 2:
        print(df_copy_sorted.head(50))

    seen_amounts = []
    same_values_df = None
    balance_irelevant_transactions_by_id = []

    # Initialize the progress bar
    progress_bar = tqdm.tqdm(total=len(df_copy_sorted))

    # Go through every row of the dataframen to find transactions between banks
    for i, row in enumerate(df_copy_sorted.itertuples()):
        progress_bar.update()

        # Check if the value of Abs_Amount is the same as the previous row
        if i == 0:
            Amount2Test = row.Abs_Amount
            seen_amounts.append(Amount2Test)
        else:
            if row.Abs_Amount == Amount2Test:
                # Add the row to the same_values_df if the Abs_Amount has already been seen before
                if Amount2Test in seen_amounts:
                    
                    # if same_values_df dosent exist, set it to same_values_row, otherwise concatenate same_values_df and same_values_row
                    if same_values_df is None:
                        # set same_values_df to same_values_row
                        same_values_df = same_values_old_row

                        # Create a dictionary with the row values
                        same_values_row = {
                            "df_index": row.Index,
                            "Validated_Date": row.Validated_Date,
                            "Amount": row.Amount,
                            "Account": row.Account,
                            "Purpose": row.Purpose,
                            "Abs_Amount": row.Abs_Amount,
                            
                        }

                        # Convert the dictionary to a DataFrame and append it to same_values_df
                        same_values_row = pd.DataFrame(same_values_row, index=[0])

                        # Concatenate same_values_df and same_values_row
                        same_values_df = pd.concat([same_values_df, same_values_row], sort=False)
                    else:
                        # Create a dictionary with the row values
                        same_values_row = {
                            "df_index": row.Index,
                            "Validated_Date": row.Validated_Date,
                            "Amount": row.Amount,
                            "Account": row.Account,
                            "Purpose": row.Purpose,
                            "Abs_Amount": row.Abs_Amount,
                            
                        }

                        # Convert the dictionary to a DataFrame and append it to same_values_df
                        same_values_row = pd.DataFrame(same_values_row, index=[0])

                        # Concatenate same_values_df and same_values_row
                        same_values_df = pd.concat([same_values_df, same_values_row], sort=False)
            else:
                # The Abs_Amount value has changed, so update the seen_amounts list and reset the flag
                Amount2Test = row.Abs_Amount
                seen_amounts.append(Amount2Test)

                ### Further analyse the same_values_df to find the transactions between banks
                

                if same_values_df is not None:

                    is_account_identical = test_for_standing_orders(same_values_df) # Test für daueraufträge

                    if is_account_identical == False:
                        # analyse if the transactions are BalanceRelevant and retrun a list with the df_index of the transactions that are not BalanceRelevant
                        balance_irelevant_transactions = get_balance_irelevant_transactions_by_index(same_values_df)

                        # add the returned values in the list to the balance_irelevant_transactions_by_id list
                        balance_irelevant_transactions_by_id.extend(balance_irelevant_transactions)

                # Create a dictionary with the row values for comparing to the next row
                same_values_row = {
                    "df_index": row.Index,
                    "Validated_Date": row.Validated_Date,
                    "Amount": row.Amount,
                    "Account": row.Account,
                    "Purpose": row.Purpose,
                    "Abs_Amount": row.Abs_Amount,
                    
                }

                # Convert the dictionary to a DataFrame and append it to same_values_df
                same_values_old_row = pd.DataFrame(same_values_row, index=[0])

                # Reset same_values_df
                same_values_df = None

        # TBD, For Debugging only
        # if i > 20:
        #     break

    progress_bar.update()
    # Set the variable BalanceRelevant in the df to false if the name of the row is in the balace_irelevant_transactions_by_id list
    for index in balance_irelevant_transactions_by_id:
        df.loc[index, 'BalanceRelevant'] = False

    return df


def set_bilance_relevant_by_account_number(df, verbose):
    """Set the 'BalanceRelevant' column to False if the account number is in the greenlist.json file."""

    # Read the IBAN values from the JSON file
    with open(os.path.join('Processing', 'greenlist_bankaccounts.json')) as f:
        greenlist_bankaccounts_info = json.load(f)
    greenlist_bankaccounts_IBAN = []
    for bankaccount in greenlist_bankaccounts_info['bankaccounts']:
        greenlist_bankaccounts_IBAN.append(bankaccount['IBAN'])

    # Set all BalanceRelevant to False if the account number is in the greenlist
    df.loc[df['Account_Number'].isin(greenlist_bankaccounts_IBAN), 'BalanceRelevant'] = False

    if verbose >= 2:
        print("Set BalanceRelevant to False for transactions with account numbers in the greenlist.json file")
