import pandas as pd
import os
from convert_date import *
from prepare_settings import ReadSettings

"""
NOTE: Not used in the final approach.

In this approach, monthly output files (named yyyymm_variable.parquet) are created by 
extracting Freddie-Mac quarterly loan data for a subset of loans originated at or 
after a specified date. The process starts by processing the most recent quarterly 
files, with the monthly data being progressively added to each individual monthly 
extract file. The earlier the quarterly data file, the longer it would take to process, 
because it would contain more and more of different yearly-monthly records. Thus, unless 
we can satisfy ourselves with an incomplete set of active loans existing at each monthly 
extract date, we would need to continue the extraction process all the way back to the 
earliest dataset, e.i. 1999 Q1. That would take several days to complete.

Looping logic: for each quarterly file (containing loans originated at that quarter)
extract all sets of monthly data and add them to existing monthly extract files.

Output file name: yyyymm_variable.parquet
"""

conf = ReadSettings()

dir = conf.freddie_mac_dir
loancol = conf.loancol
datecol = conf.datecol
quarterly_file_prefix = conf.quarterly_file_prefix

varcol = 'Total Expenses'  # make a function parameter
varcol_fname = varcol.replace(' ', '_').replace(')', '').replace('(', '')
var_dir = os.path.join(dir, varcol_fname)
if not os.path.exists(var_dir):
    os.makedirs(var_dir)
quarterly_file_prefix = 'historical_data_time_'  # in yaml

col_type = 'float64'
datecol = 'Monthly Reporting Period'    # in yaml
loancol = 'Loan Sequence Number'        # in yaml
cols_to_extract = {
    loancol: str,
    datecol : 'Int64',
    varcol: col_type
}
selected_cols = list(cols_to_extract.keys())
nfiles = 55  # The arliest data will be from 2010Q1

# Create a reverse list of parquet data files to start with the most recent ones
parquet_list = [f for f in os.listdir(dir) if f.startswith(quarterly_file_prefix) & f.endswith('.parquet')]
parquet_list.reverse()
parquet_list = parquet_list[:nfiles]  # Change this

for pname in parquet_list:

    print(f'Reading in {pname}')
    ppath = os.path.join(dir, pname)
    fr = pd.read_parquet(ppath)
    fr = fr[selected_cols]
    dates = list(fr[datecol].unique())
    dates.sort()
    dates.reverse()
    
    for di in dates:
        
        d = str(di)
        print('\033[K', d, '\r', end='')  # Clear the line and print the date

        dname = d + '_' + varcol_fname + '.parquet'
        fr_date = fr.loc[fr[datecol]==di, :]  # Subset of input data for each date

        if dname in os.listdir(var_dir):
            df_date = pd.read_parquet(os.path.join(var_dir, dname))
            df_date = pd.concat([df_date, fr_date])
        else:
            df_date = fr_date.copy()
        
        df_date = df_date.drop_duplicates()
        df_date = df_date.reset_index(drop=True)
        df_date = df_date.sort_values(by=[datecol, loancol])
        df_date.to_parquet(os.path.join(var_dir, dname))

