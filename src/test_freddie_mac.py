import pandas as pd
import os
from convert_date import *
from prepare_settings import ReadYaml

conf = ReadYaml()

dir = conf.dir
layout_file = conf.layout_file
quarterly_file_prefix = conf.quarterly_file_prefix

lpath = os.path.join(dir, layout_file)

# Process the data layout file, include only selected variables marked with the INCLUDE flag equal to 1
ldf = pd.read_csv(lpath)
allcols = [col for col in ldf['ATTRIBUTE NAME']]
cols = [col for col in ldf.loc[ldf['INCLUDE'] == 1, 'ATTRIBUTE NAME']]
dtype_dict = pd.Series(ldf['DTYPE'].values, index=ldf['ATTRIBUTE NAME']).to_dict()

parquet_list = [f for f in os.listdir(dir) if f.startswith(quarterly_file_prefix) & f.endswith('.parquet')]
# Check which output parquet files already exist, skip them from processing
files_to_process = [f for f in os.listdir(dir) if f.startswith(quarterly_file_prefix) & f.endswith('.txt') & ~(f[:-3] + 'parquet' in parquet_list)]

for fname in files_to_process:

    print(f'Processing {fname} ...')
    pname = fname[:-3] + 'parquet'
    fpath = os.path.join(dir, fname)
    ppath = os.path.join(dir, pname)

    # Read in pipe-separated quarterly Freddie-Mac file containing no column header
    fr = pd.read_csv(fpath, names=allcols, header=None, sep='|', dtype=dtype_dict)

    # Add column header based on selected variables from time_data_layout.csv
    fr = fr[cols]

    # Write re-formatted quarterly file to parquet, e.g. historical_data_time_2023Q3.parquet
    fr.to_parquet(ppath)

