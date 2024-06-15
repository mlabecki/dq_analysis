import pandas as pd
import numpy as np
import os
from mapping_freddie_mac import *
from prepare_settings import ReadSettings

conf = ReadSettings()

dir = conf.freddie_mac_dir
dq_subdir = conf.dq_subdir
loancol = conf.loancol
datecol = conf.datecol
monthly_file_prefix = conf.monthly_file_prefix

varcol = 'Total Expenses'  # make a function parameter
varcol_fname = varcol.replace(' ', '_').replace(')', '').replace('(', '')
var_dir = os.path.join(dir, varcol_fname)
dq_dir = os.path.join(dir, dq_subdir)

end_fname = f'_{varcol_fname}.parquet'
file_list = [f for f in os.listdir(var_dir) if f.startswith(monthly_file_prefix) & f.endswith(end_fname)]
date_list = []
for fname in file_list:
    fdate = int(fname.replace(monthly_file_prefix, '').replace(end_fname, ''))
    date_list.append(fdate)

min_date = np.min(date_list)
max_date = np.max(date_list)
df_dq_fname = f'{varcol_fname}_{min_date}_{max_date}_DQ.'

cols_to_exclude = [loancol, datecol]

for pct in pct_list:
    newcol = 'Pct_' + str(pct)
    df_dq_cols.append(newcol)

df_dq_dict = {}
for col in df_dq_cols:
    if (col == 'Date') | col.endswith('Count'):
        df_dq_dict.update({col: int})
    else:
        df_dq_dict.update({col: float})

df_dq = pd.DataFrame(columns=df_dq_cols).astype(df_dq_dict)

for fname in file_list:
    
    print(f'Processing {fname}')
    df = pd.read_parquet(os.path.join(var_dir, fname))
    
    fdate = int(fname.replace(monthly_file_prefix, '').replace(end_fname, ''))
    total_count = len(df)

    # LTV of 999 means 'unknown', so it is treated as null
    if varcol == 'Estimated Loan-to-Value (ELTV)':
        null_count = len(df.loc[df[varcol].isna() | (df[varcol]==999)])
        max_value = df.loc[df[varcol]!=999, varcol].max()
        mean_value = df.loc[df[varcol]!=999, varcol].mean()
    else:
        null_count = len(df.loc[df[varcol].isna()])
        max_value = df[varcol].max()
        mean_value = df[varcol].mean()

    fill_count = total_count - null_count
    fill_rate = fill_count / total_count
    min_value = df[varcol].min()
    values = [
        fdate,
        total_count,
        null_count,
        fill_count,
        fill_rate,
        min_value,
        max_value,
        mean_value
    ]
    for pct in pct_list:
        col = 'Pct_' + str(pct)
        if varcol == 'Estimated Loan-to-Value (ELTV)':
            values.append(df.loc[df[varcol]!=999, varcol].quantile(pct / 100))
        else:
            values.append(df[varcol].quantile(pct / 100))

    df_dq_date = pd.DataFrame([values], columns=df_dq_cols)

    df_dq = pd.concat([df_dq, df_dq_date])    

# Change the Date column type to string so it is treated as a categorical variable in plots
df_dq['Date'] = df_dq['Date'].astype(str)

df_dq.to_parquet(os.path.join(dq_dir, df_dq_fname + 'parquet'))
df_dq.to_csv(os.path.join(dq_dir, df_dq_fname + 'csv'), index=False)
