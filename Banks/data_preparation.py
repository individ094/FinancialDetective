### imports

import pandas as pd
import numpy as np
import os
import tkinter as Tk
from tkinter.filedialog import askdirectory, askopenfile, askopenfilename
import config


### variables

data_path = r"Data"
# What month sould be analysed?
Month_in_question = 5
Year_in_question = 2022



### ------------------------------------------------------------ ### 
# model example: https://python-course.eu/numerical-programming/expenses-and-income-example-with-pandas-and-python.php

# loading files
class BankCSV():
    def __init__(self) -> None:
        True

    @staticmethod
    def get_file_path(title):
        window = Tk.Tk()
        window.wm_attributes('-topmost', 1)
        window.withdraw() 
        path = askopenfilename(title=title)
        return path

