import pandas as pd
import requests
import os
from convert_date import *

write_dir = 'D:\\Python\\DQ_exercises\\DQ_exercises\\downloads\\fed_stress_test'

fed_dir = 'https://www.federalreserve.gov/supervisionreg/files/'
fed_file_list = [
    '2024-Table_2A_Historic_Domestic.csv',
    '2024-Table_2B_Historic_International.csv',
    '2024-Table_3A_Supervisory_Baseline_Domestic.csv',
    '2024-Table_3B_Supervisory_Baseline_International.csv',
    '2024-Table_4A_Supervisory_Severely_Adverse_Domestic.csv',
    '2024-Table_4B_Supervisory_Severely_Adverse_International.csv' 
]
write_to_parquet = False
qtype = 'fed'

def download_fed_data(
    file_list,
    url_dir: str,
    write_dir: str,
    qtype: str,
    write_to_parquet: bool
):
    """
    qtype:  'calendar' (Jan-Apr-Jul-Oct) or
            'fiscal' (Nov-Feb-May-Aug) or
            'fed' (Oct-Jan-Apr-Jul)
    """

    for fname in file_list:
        
        print(f'Downloading {fname}')
        
        url = os.path.join(url_dir, fname)
        response = requests.get(url)
        
        if response.status_code == 200:

            fpath = os.path.join(write_dir, fname)
            with open(fpath, 'wb') as f:
                f.write(response.content)
            df = fed_add_date(pd.read_csv(fpath), qtype=qtype)
            newname = fname[:-4] + '_adj.csv'
            print(f'   Saving {newname}')
            df.to_csv(os.path.join(write_dir, newname), index=False)

            if write_to_parquet:
                pname = fname[:-3] + 'parquet'
                print(f'   Saving {pname}')
                df.to_parquet(os.path.join(write_dir, pname))

        else:

            print(f'Could not download {fname}')


if __name__== '__main__':
    download_fed_data(fed_file_list, fed_dir, write_dir, qtype, write_to_parquet)
