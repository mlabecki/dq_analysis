import pandas as pd
import os
from prepare_settings import ReadSettings

"""
Because of a syntactic error in extract_freddie_mac_monthly, all but one 
extract for a given year got contaminated with other months for that date. 
This script fixes that.
"""

conf = ReadSettings()

datecol = conf['datecol']
dir = conf['freddie_mac_dir']

varname = 'Current Actual UPB'  # Unpaid balance or outstanding
varname_fname = varname.replace(' ', '_')
var_dir = os.path.join(dir, varname_fname)

years = [2018, 2019, 2020, 2021, 2022, 2023]
         
end_fname = f'_{varname_fname}.parquet'

for year in years:
    
    start_fname = f'all_loans_{year}'
    file_list = [f for f in os.listdir(var_dir) if f.startswith(start_fname) & f.endswith(end_fname)]
    
    for fname in file_list:
    
        ym = int(fname.replace('all_loans_', '').replace(end_fname, ''))
        print(f'Processing {ym}')
    
        fpath = os.path.join(var_dir, fname)
        fpath1 = os.path.join(var_dir, 'bkp_' + fname)
        df = pd.read_parquet(fpath)
        os.rename(fpath, fpath1)  # a backup copy in case something goes wrong
        df = df.loc[df[datecol]==ym, :]
        df = df.reset_index(drop=True)
        df.to_parquet(fpath)
