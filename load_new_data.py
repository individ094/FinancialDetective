# ------------------------ imports ------------------------
# packages
import asyncio
import pandas as pd
import numpy as np
from dotenv import load_dotenv
import os
from tkinter.filedialog import askdirectory
from prisma import Prisma
 
# local code
from Banks import data_preparation as dp
from Banks.DeutscheKreditbank import DKB as dkb
from Banks.AmericanExpress import American_Express as amex

from Processing import prepare_for_DB_load as pfl

from Analysation.analyse_transactions import Analysation

from SQL_Database import database_feeder as dbf
from logging_utils import get_logger 


# ------------------------ global varibels ------------------------
verbose = 1


# ------------------------ code ------------------------

'''
step by step dummy guide:
1) locating filepath for different bank csv documents
2) transformation of every csv file transaction to a class object
    - for each bank csv file
        1) manipulate csv file so that only the relevant rows and collumns (informations) are present.
        2) translate csv file transactions to class objects 
           and add necessary additional informations zu
        

'''

# Laden der Umgebungsvariablen aus der .env-Datei
load_dotenv()
# Zugriff auf die Umgebungsvariable, die das Passwort enthält
db_password = os.getenv("DB_PASSWORD")


if __name__ == "__main__":
    logger = get_logger()

    # get the path to the dkb file and bring it to the standard format
    dkb_path = dkb.get_file_path(title="Select the DKB file you want to analyse.")
    dkb_df = dkb.load_dkb_csv(dkb_path)
    dkb_standard_df = dkb.dkb_csv2standard_format(dkb_df)

    amex_path = amex.get_file_path(title="Select the American Express file you want to analyse.")
    amex_df = amex.load_American_Express_csv(amex_path)
    amex_clean_df = amex.clean_American_Express_csv(amex_df)

    # TODO: Test here if the collumns of the dataframes that schould be concatinated have the correct column names and dtypes

    df_combined_clean = pd.concat([dkb_standard_df, amex_clean_df])
    if verbose >= 2:
        print(df_combined_clean.head(20))
    if verbose >= 1:
        # file loading has been seucessfull
        print("File loading has been sucessfull.")
    


    # data processing
    df2DB = pfl.find_money_streams_between_banks(df_combined_clean, verbose=verbose)
    df2DB = pfl.set_bilance_relevant_by_account_number(df2DB, verbose=verbose)



    # updating the database with new data
    database_updater = dbf.TransactionsUpdater(prisma=Prisma(), table_name="Transactions")
    try:
        new_loop = asyncio.new_event_loop()
        asyncio.set_event_loop(new_loop)
        new_loop.run_until_complete(database_updater.update_database(df_combined_clean))
        new_loop.close()
    except KeyboardInterrupt:
        pass

    df_bilanz = Analysation.get_bilanz_relevent_df(df_combined_clean, verbose=verbose)
    logger.info(df_bilanz.head(5))

    Analysation.income_spend_analysis(df_bilanz, verbose=verbose)

    logger.debug("    ")
    logger.debug("main programm END!")