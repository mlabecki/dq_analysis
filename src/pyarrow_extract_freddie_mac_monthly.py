import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import os
import time
from mapping_freddie_mac import *
from convert_date import *
from prepare_settings import ReadYaml


"""
Extract all loans at a given date (yyyymm), starting from the latest, i.e. 202312.
This differs from group_freddie_mac_monthly(), where each monthly extract contains only
loans originated at or past a selected starting year-quarter file, in this case 2011Q1
The total number of loans originated at or after that quarter is 24,346,304. 

Looping logic: For each year-month (yyyymm), extract that data from all quarterly files
('historical_data_time_YYYYQQ.parquet') and add them to each individual monthly file. 
This has the advantage of each incremental batch output not modifying the previously 
generated monthly extracts. Also any monthly batch run can be restarted if the code 
execution gets interrupted in any way. Memory problems should not occur because each 
monthly extract saved to disk is relatively small (< 200 MB). 
The output file name starts with the monthly file prefix of 'all_loans_'.

TO-DO: Re-organize code to handle hard-coded variables and parameters, e.g. varcol, ym_list.
"""

conf = ReadYaml()

dir = conf.freddie_mac_dir
loancol = conf.loancol
datecol = conf.datecol
quarterly_file_prefix = conf.quarterly_file_prefix
monthly_file_prefix = conf.monthly_file_prefix
min_extract_date = 202301
max_extract_date = 202312

# min_extract_date = conf.min_extract_date
# max_extract_date = conf.max_extract_date

varcol = 'Expenses'  # make a function parameter
varcol_fname = varcol.replace(' ', '_').replace(')', '').replace('(', '') + '_pyarrow'
var_dir = os.path.join(dir, varcol_fname)
if not os.path.exists(var_dir):
    os.makedirs(var_dir)

col_type = 'float64'
cols_to_extract = {
    loancol: str,
    datecol : 'Int64',
    varcol: col_type
}
pyarrow_schema = pa.schema([
    pa.field(loancol, pa.string()),
    pa.field(datecol, pa.int64()),
    pa.field(varcol, pa.float64())]
)
selected_cols = list(cols_to_extract.keys())

def prepare_date_list(
    min_extract_date: int,
    max_extract_date: int
):
    
    min_date_year = str(min_extract_date)[:4]
    min_date_month = str(min_extract_date)[4:]
    max_date_year = str(max_extract_date)[:4]
    max_date_month = str(max_extract_date)[4:]

    ym_list = []
    year = min_date_year
    while year <= max_date_year:
        first_month = min_date_month if year == min_date_year else '01'
        last_month = max_date_month if year == max_date_year else '12'
        month = first_month
        while month <= last_month:
            yyyymm = int(year + month)
            ym_list.append(yyyymm)
            next_month_int = int(month) + 1
            month = ('0' + str(next_month_int))[-2:]
        year = str(int(year) + 1)

    ym_list.reverse()

    return ym_list


ym_list = prepare_date_list(min_extract_date, max_extract_date)

# Create a reverse list of parquet data files to start with the most recent ones
parquet_list = [f for f in os.listdir(dir) if f.startswith(quarterly_file_prefix) & f.endswith('.parquet')]
parquet_list.reverse()

start_time_global = time.perf_counter()

for ym in ym_list:

    start_time = time.perf_counter()
    
    df0 = pd.DataFrame(columns=selected_cols)
    df = pa.Table.from_pandas(df0, schema=pyarrow_schema)  # pyarrow table from pandas dataframe

    print(f'Extracting {ym} data from all quarterly files...')

    for pname in parquet_list:

        ppath = os.path.join(dir, pname)
        fr = pq.read_table(ppath, columns=selected_cols, filters=[(datecol, '=', ym)])
        df = pa.concat_tables([df, fr])

    end_time = time.perf_counter()
    print(f'    Finished extraction for {ym}, duration: {end_time - start_time:0.2f}')
    # Typically 30-33s

    start_time = time.perf_counter()

    dname = monthly_file_prefix + str(ym) + '_' + varcol_fname + '_pandas.parquet'
    print(f'    Saving {dname}')
    pq.write_table(df, os.path.join(var_dir, dname))
    
    end_time = time.perf_counter()
    print(f'    Saved {dname}, duration: {end_time - start_time:0.2f}')

end_time_global = time.perf_counter()
print(f'    Finished extraction, total duration: {end_time_global - start_time_global:0.2f}')

