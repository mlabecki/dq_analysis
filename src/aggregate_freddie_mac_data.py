import pandas as pd
import os
from convert_date import *
from prepare_settings import ReadYaml


"""
NOTE: Not used in the final approach.

An initial attempt to aggregate all time series data for several variables 
- and eventually for a single variable due to hitting numpy memory limits - 
into a single file. This would take a very long time and create impractically 
large parquet output. For example, aggregating all loans originated 2011Q1 or later
would have to be done in batches to eventually, after a couple of days, produce
a 15 GB output for Current UPB. This approach was abandoned for the other variables
(Estimated LTV and Total Expenses).
"""
 
conf = ReadYaml()

dir = conf.freddie_mac_dir
loancol = conf.loancol
datecol = conf.datecol
quarterly_file_prefix = conf.quarterly_file_prefix

# col_name = 'Current Actual UPB'  # UnPaid Balance or outstanding
varcol = 'Total Expenses'
col_type = 'float64'
varcol_fname = varcol.replace(' ', '_').replace(')', '').replace('(', '')
input_fname = f'input_{varcol_fname}.parquet'
allpath = os.path.join(dir, input_fname)

cols_to_extract = {
    loancol: str,
    datecol : 'Int64',
    varcol: col_type
}
selected_cols = list(cols_to_extract.keys())
nfiles = 55  # add as a function parameter

# Create a reverse list of parquet data files to start with the most recent ones
parquet_list = [f for f in os.listdir(dir) if f.startswith(quarterly_file_prefix) & f.endswith('.parquet')]
parquet_list.reverse()
parquet_list = parquet_list[nfiles:]  # Change this

# df = pd.DataFrame(columns=selected_cols).astype(cols_to_extract)

for pname in parquet_list:

    print(f'Reading in {pname}')
    ppath = os.path.join(dir, pname)
    fr = pd.read_parquet(ppath)
    fr = fr[selected_cols]

    df = pd.read_parquet(allpath)
    df = pd.concat([df, fr])

    # print('   Sorting by date and loan number')
    # df = df.sort_values(by=['Monthly Reporting Period', 'Loan Sequence Number'])
    print('   Saving to parquet')
    df.to_parquet(allpath)
    
