### imports

import pandas as pd
import numpy as np
import os
import tkinter as Tk
from tkinter.filedialog import askdirectory, askopenfile
import config
from Banks.data_preparation import BankCSV

### variables

#data_path = r"Data"
# What month sould be analysed?
#Month_in_question = 5
#Year_in_question = 2022

### ------------------------------------------------------------ ### 
# model example: https://python-course.eu/numerical-programming/expenses-and-income-example-with-pandas-and-python.php


class American_Express(BankCSV):

    def __init__(self) -> None:
        super().__init__()

    def get_file_path(title):
        return BankCSV.get_file_path(title)
   
    def load_American_Express_csv(file_path:str):
        # import file with pandas
        collumn_names = ["Datum","Beschreibung","Betrag","Adresse", "PLZ", "Land"]
        df_amex = pd.read_csv(file_path, sep=",", header=0, usecols=collumn_names, encoding="cp1250") #index_col=7,   usecols=collumn_names,
        
        #print("   ")
        #print(df_amex.head())
        return df_amex
    
    def amex_preprocessing(df_amex):

        # Bilanzrelevanz validieren (setting all values of "Bilanzrelevant" to False if the IBAN is in config.greenlist_bankaccounts_IBAN)
        # internal transactioins between personal bankaccounts will not be used for bilanz analysis.
        # Since these are not expenses or revenues but allocations of existing income.
        try:
            df_amex.loc[df_amex['Auftraggeber']== "ZAHLUNG/ĂśBERWEISUNG ERHALTEN BESTEN DANK", "Bilanzrelevant"] = False
        except:
            True

        return df_amex

    def clean_American_Express_csv(df_amex):
        # add column for later Monetary balance
        df_amex['Bilanzrelevant'] = pd.Series([True for x in range(len(df_amex.index))])
        df_amex['Kategorie'] = pd.Series(["None" for x in range(len(df_amex.index))])
        df_amex['Buchungsart'] = pd.Series(["None" for x in range(len(df_amex.index))])
        df_amex['Kontonummer'] = pd.Series(["None" for x in range(len(df_amex.index))])
        df_amex['Sonstiges'] = pd.Series(["None" for x in range(len(df_amex.index))])
        df_amex['Buchungsart'] = pd.Series(["None" for x in range(len(df_amex.index))])# Warum zwei mal???
        df_amex['Verwendungszweck'] = pd.Series(["None" for x in range(len(df_amex.index))])
        df_amex['Konto'] = pd.Series(["AmEx" for x in range(len(df_amex.index))])
        
        
        # print(df_amex)

        # transfer collumn names to template names
        df_amex = df_amex.rename(columns={"Datum" : "Date",
                               "Beschreibung" : "Auftraggeber"})
        # print(df_amex)

        # combine the Adress, PLZ and Land to the Sonstiges Collumn
        df_amex["Sonstiges"] = df_amex["Adresse"]+"-"+df_amex["PLZ"]+"-"+df_amex["Land"]   #-{df_amex["PLZ"]}-{df_amex["Land"]}"
        # print("   ")
        # print(df_amex)

        # delete columns "Adresse", "PLZ" und "Land"
        df_amex = df_amex.drop(["Adresse", "PLZ", "Land"], axis=1)
        # print("   ")
        # print(df_amex)

        # change positive to negative number and reversed
        df_amex['Betrag'] = df_amex['Betrag'].str.replace(',','.')
        df_amex["Betrag"] = df_amex["Betrag"].astype(float)
        df_amex["Betrag"] = df_amex["Betrag"]*-1

        # transfere Date format to template form
        df_amex['Date'] = df_amex['Date'].str.replace('/','.')
        # convert "date" column to datetime format
        df_amex['Date'] = pd.to_datetime(df_amex['Date'], format='%d.%m.%Y')
        # convert datetime format to "%Y-%m-%d" datetime object
        # df_amex['Date'] = df_amex['Date'].dt.strftime('%Y-%m-%d')

        # change the sequence of the df to match the template format
        col_sequ = ["Date","Buchungsart","Auftraggeber","Verwendungszweck", "Kontonummer", "Betrag", "Bilanzrelevant", "Kategorie", "Sonstiges", "Konto"]
        df_amex = df_amex[col_sequ]
        # print("   ")
        # print(df_amex)

        # replace any nan values with "None"
        df_amex = df_amex.replace(np.nan, 'None', regex=True)

        

        df_amex = American_Express.amex_preprocessing(df_amex)

        # Create a dictionary to store the new column names
        new_column_names = {
            'Date': 'Validated_Date',  # Change 'Date' to 'Validated_Date'
            'Buchungsart': 'Transaction_Type',
            'Auftraggeber': 'Payment_Requester',
            'Verwendungszweck': 'Purpose',
            'Kontonummer': 'Account_Number',
            'Betrag': 'Amount',
            'Bilanzrelevant': 'BalanceRelevant',
            'Kategorie': 'Category',
            'Sonstiges': 'Additional_Information',
            'Konto': 'Account'
        }

        # Rename the columns using the new column names
        df_amex.rename(columns=new_column_names, inplace=True)

        return df_amex