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


class BanknameHere(BankCSV):

    def __init__(self) -> None:
        super().__init__()


    def get_file_path(title):
        return BankCSV.get_file_path(title)
   
    def load_BanknameHere_csv(file_path:str):
        # throw exception that this is not implemented yet
        raise NotImplementedError("This is not implemented yet")
    
        return df_loaded_BanknameHere

    def clean_BanknameHere_csv(BanknameHere_df): 
        # throw exception that this is not implemented yet
        raise NotImplementedError("This is not implemented yet")
          
        return df_cleanded_BanknameHere
  