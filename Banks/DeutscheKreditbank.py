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


class DKB(BankCSV):

    def __init__(self) -> None:
        super().__init__()

    def get_file_path(title):
        return BankCSV.get_file_path(title)    
    
    def load_dkb_csv_V2(file_path:str):
        # allfiles = [f for f in os.listdir(file_path) if os.path.isfile(os.path.join(file_path, f))]
        # print(allfiles)
        # # find the dkb file through substring
        # file = [f for f in allfiles if "DKB" in f]
        # print(file)
        # file_path = os.path.join(file_path,file[0])

        # import file with pandas
        collumn_names = ["Buchungsdatum","Zahlungspflichtige*r","Zahlungsempfänger*in","Verwendungszweck", "Kontonummer", "Betrag"]
        df_dkb = pd.read_csv(file_path, sep=";", skiprows=6, header=0, usecols=collumn_names, encoding="cp1250") #index_col=7,

        return df_dkb    
    
    
    def load_dkb_csv(file_path:str):
        # allfiles = [f for f in os.listdir(file_path) if os.path.isfile(os.path.join(file_path, f))]
        # print(allfiles)
        # # find the dkb file through substring
        # file = [f for f in allfiles if "DKB" in f]
        # print(file)
        # file_path = os.path.join(file_path,file[0])

        # import file with pandas
        collumn_names = ["Buchungstag","Buchungstext","Auftraggeber / Begünstigter","Verwendungszweck", "Kontonummer", "Betrag (EUR)"]
        df_dkb = pd.read_csv(file_path, sep=";", skiprows=6, header=0, usecols=collumn_names, encoding="cp1250") #index_col=7,

        return df_dkb
    
    def dkb_preprocessing(dkb_df):
        # replace any nan values with "None"
        dkb_df = dkb_df.replace(np.nan, 'None', regex=True)
        
        return dkb_df

    def dkb_csv2standard_format(dkb_df):
        # add column for later Monetary balance#
        dkb_df['Bilanzrelevant'] = pd.Series([True for x in range(len(dkb_df.index))])
        dkb_df['Kategorie'] = pd.Series(["None" for x in range(len(dkb_df.index))])
        dkb_df['Sonstiges'] = pd.Series(["None" for x in range(len(dkb_df.index))])
        dkb_df['Konto'] = pd.Series(["DKB" for x in range(len(dkb_df.index))])
        #print(dkb_df)

        # transfer collumn names to template names
        dkb_df = dkb_df.rename(columns={"Buchungstag" : "Date",
                               "Buchungstext" : "Buchungsart",
                               "Auftraggeber / Begünstigter" : "Auftraggeber",
                               "Betrag (EUR)" : "Betrag"})
        #print(dkb_df)

        # set dot for beträge as seperator
        dkb_df['Betrag'] = dkb_df['Betrag'].str.replace('.','')
        dkb_df['Betrag'] = dkb_df['Betrag'].str.replace(',','.')
        dkb_df['Betrag'] = dkb_df['Betrag'].replace(np.NaN,'0.00')
        dkb_df['Betrag'] = [float(i) for i in dkb_df['Betrag']]


        # convert "date" column to datetime object
        dkb_df['Date'] = pd.to_datetime(dkb_df['Date'], format='%d.%m.%Y')
        # convert datetime format to "%Y-%m-%d" dattime object
        #dkb_df['Date'] = dkb_df['Date'].dt.strftime('%Y-%m-%d')

        dkb_df = DKB.dkb_preprocessing(dkb_df)

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
        dkb_df.rename(columns=new_column_names, inplace=True)

        return dkb_df