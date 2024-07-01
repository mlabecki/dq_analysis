import pandas as pd
import pyarrow as pa
import pyarrow.csv as pacsv
import pyarrow.parquet as pq
import numpy as np
import os
import time
from convert_date import *
from mapping_freddie_mac import *
from prepare_settings import ReadSettings

# Note: pyarrow.csv and pyarrow.parquet must be imported separately from pyarrow,
# otherwise pyarrow.csv and pyarrow.parquet may not be recognized as valid modules

class FreddieMac:
    
    def __init__(self):
        
        self.conf = ReadSettings()
        self.fm_dir = self.conf.freddie_mac_dir
        self.layout_file = self.conf.layout_file
        self.quarterly_file_prefix = self.conf.quarterly_file_prefix
        self.monthly_file_prefix = self.conf.monthly_file_prefix
        self.dq_subdir = self.conf.dq_subdir
        self.summary_dir = self.conf.summary_dir
        self.loancol = self.conf.loancol
        self.datecol = self.conf.datecol
        self.min_extract_date = self.conf.min_extract_date
        self.max_extract_date = self.conf.max_extract_date


    def reformat_original_data(self):
        """
        Reformat the pipe-separated Freddie Mac txt files containing quarterly time series 
        data to add header and to limit variable selection as per the 'INCLUDE' flag added
        to the original data layout table. The reformatted output for each year-quarter is 
        saved as a parquet file.
        """

        lpath = os.path.join(self.fm_dir, self.layout_file)

        # Process the data layout file, include only selected variables marked with the INCLUDE flag equal to 1
        ldf = pd.read_csv(lpath)
        allcols = [col for col in ldf['ATTRIBUTE NAME']]
        cols = [col for col in ldf.loc[ldf['INCLUDE'] == 1, 'ATTRIBUTE NAME']]
        dtype_dict = pd.Series(ldf['DTYPE'].values, index=ldf['ATTRIBUTE NAME']).to_dict()

        parquet_list = [f for f in os.listdir(self.fm_dir) if f.startswith(self.quarterly_file_prefix) & f.endswith('.parquet')]
        files_to_process = [
            f for f in os.listdir(self.fm_dir) if
                f.startswith(self.quarterly_file_prefix) & 
                f.endswith('.txt') & 
                ~(f[:-3] + 'parquet' in parquet_list)  # Skip files that already exist
        ]

        for fname in files_to_process:

            print(f'Processing {fname} ...')
            pname = fname[:-3] + 'parquet'
            fpath = os.path.join(self.fm_dir, fname)
            ppath = os.path.join(self.fm_dir, pname)

            # Read in pipe-separated quarterly Freddie Mac file containing no column header
            fr = pd.read_csv(fpath, names=allcols, header=None, sep='|', dtype=dtype_dict)

            # Add column header based on selected variables from time_data_layout.csv
            fr = fr[cols]

            # Write re-formatted quarterly file to parquet, e.g. historical_data_time_2023Q3.parquet
            fr.to_parquet(ppath)


    def summarize_data_pyarrow(self, dataset_type='Monthly Performance'):
        """
        Summarize the time series data by counting records, loans and distinct dates
        in each quarterly file and in the whole dataset.
        dataset_type: either 'Monthly Performance' or 'Origination' 
            (used to populate first column in summary)
        Note: quarterly_file_prefix will need to be changed for 'Origination' (remove '_time')
        Runtime: About 1 min vs. > 3 min using pandas
        """

        summary_fname = dataset_type.replace(' ', '_') + '_Dataset_Summary'
        summary_fname_parquet = summary_fname + '.parquet'
        summary_fname_csv = summary_fname + '.csv'

        summary_columns = [
            'Year-Quarter',
            'Row Count',
            'Unique Loan Count',
            'Unique Date Count'
        ]
        
        total_row_count, total_loan_count = 0, 0

        df0 = pd.DataFrame(columns=summary_columns)
        pa_schema = pa.schema([
            pa.field('Year-Quarter', pa.string()),
            pa.field('Row Count', pa.int64()),
            pa.field('Unique Loan Count', pa.int64()),
            pa.field('Unique Date Count', pa.int64())
        ])
        df_summary = pa.Table.from_pandas(df0, schema=pa_schema)

        files_to_process = [f for f in os.listdir(self.fm_dir) if f.startswith(self.quarterly_file_prefix) & f.endswith('.parquet')]

        start_time_global = time.perf_counter()

        for fname in files_to_process:

            start_time = time.perf_counter()

            fpath = os.path.join(self.fm_dir, fname)
            df = pq.read_table(fpath, columns=[self.loancol, self.datecol])
            
            yyyyqq = fname.replace(self.quarterly_file_prefix, '').replace('.parquet', '')
            row_count = len(df)
            loan_count = len(df[self.loancol].unique())
            date_count = len(df[self.datecol].unique())

            total_row_count += row_count
            total_loan_count += loan_count

            row_pylist = [{
                'Year-Quarter': yyyyqq,
                'Row Count': row_count,
                'Unique Loan Count': loan_count,
                'Unique Date Count': date_count
            }]
            row = pa.Table.from_pylist(row_pylist)
            df_summary = pa.concat_tables([df_summary, row])

            end_time = time.perf_counter()
            print(f'    Finished processing {yyyyqq}, total duration: {end_time - start_time:0.2f}')

        total_date_count = df_summary['Unique Date Count'][0]  # First file (1999Q1) has all the unique dates

        final_row_pylist = [{
            'Year-Quarter': 'ALL',
            'Row Count': total_row_count,
            'Unique Loan Count': total_loan_count,
            'Unique Date Count': total_date_count
        }]

        final_row = pa.Table.from_pylist(final_row_pylist)
        df_summary = pa.concat_tables([df_summary, final_row])
    
        end_time_global = time.perf_counter()
        print(f'    Finished summarizing dataset, total duration: {end_time_global - start_time_global:0.2f}')

        # Write summary table to parquet
        pq.write_table(df_summary, os.path.join(self.summary_dir, summary_fname_parquet))
        # Convert summary table to pandas df before writing to csv (pyarrow seems to corrupt the csv output)
        df_summary_csv = df_summary.to_pandas()
        df_summary_csv.to_csv(os.path.join(self.summary_dir, summary_fname_csv), index=False)


    def summarize_data_pyarrow_2(self, dataset_type='Monthly Performance'):
        """
        Summarize the time series data by counting records, loans and distinct dates
        in each quarterly file and in the whole dataset.
        In contrast to summarize_data_pyarrow(), this function uses column lists to construct
        the final summary table from a dictionary, rather than appending a single row at a time 
        to the initial table.
        dataset_type: either 'Monthly Performance' or 'Origination' 
            (used to populate first column in summary)
        Note: quarterly_file_prefix will need to be changed for 'Origination' (remove '_time')
        Runtime: About 1 min (no significant difference vs. summarize_data_pyarrow())
        """

        summary_fname = dataset_type.replace(' ', '_') + '_Dataset_Summary'
        summary_fname_parquet = summary_fname + '.parquet'
        summary_fname_csv = summary_fname + '.csv'
        
        total_row_count, total_loan_count = 0, 0

        pa_schema = pa.schema([
            pa.field('Year-Quarter', pa.string()),
            pa.field('Row Count', pa.int64()),
            pa.field('Unique Loan Count', pa.int64()),
            pa.field('Unique Date Count', pa.int64())
        ])

        files_to_process = [f for f in os.listdir(self.fm_dir) if f.startswith(self.quarterly_file_prefix) & f.endswith('.parquet')]

        yyyyqq_list = []
        row_count_list = []
        loan_count_list = []
        date_count_list = []

        start_time_global = time.perf_counter()

        for fname in files_to_process:

            start_time = time.perf_counter()

            fpath = os.path.join(self.fm_dir, fname)
            df = pq.read_table(fpath, columns=[self.loancol, self.datecol])
            
            yyyyqq = fname.replace(self.quarterly_file_prefix, '').replace('.parquet', '')
            row_count = len(df)
            loan_count = len(df[self.loancol].unique())
            date_count = len(df[self.datecol].unique())

            yyyyqq_list.append(yyyyqq)
            row_count_list.append(row_count)
            loan_count_list.append(loan_count)
            date_count_list.append(date_count)

            total_row_count += row_count
            total_loan_count += loan_count

            end_time = time.perf_counter()
            print(f'    Finished processing {yyyyqq}, total duration: {end_time - start_time:0.2f}')

        total_date_count = date_count_list[0]  # First file (1999Q1) has all the unique dates

        yyyyqq_list.append('ALL')
        row_count_list.append(total_row_count)
        loan_count_list.append(total_loan_count)
        date_count_list.append(total_date_count)

        df_summary = pa.Table.from_pydict(
            dict(
                zip(pa_schema.names, (yyyyqq_list, row_count_list, loan_count_list, date_count_list))
            ),
            schema=pa_schema
        )
    
        end_time_global = time.perf_counter()
        print(f'    Finished summarizing dataset, total duration: {end_time_global - start_time_global:0.2f}')

        # Write summary table to parquet
        pq.write_table(df_summary, os.path.join(self.summary_dir, summary_fname_parquet))
        # Convert summary table to pandas df before writing to csv (pyarrow seems to corrupt the csv output)
        df_summary_csv = df_summary.to_pandas()
        df_summary_csv.to_csv(os.path.join(self.summary_dir, summary_fname_csv), index=False)


    def summarize_data_pandas(self, dataset_type='Monthly Performance'):
        """
        Summarize the time series data by counting records, loans and distinct dates
        in each quarterly file and in the whole dataset.
        dataset_type: either 'Monthly Performance' or 'Origination' 
            (used to populate first column in summary)
        Note: quarterly_file_prefix will need to be changed for 'Origination' (remove '_time')
        Runtime: Over 3 min (cf. about 1 min using pyarrow)
        """

        summary_fname = dataset_type.replace(' ', '_') + '_Dataset_Summary'
        summary_fname_parquet = summary_fname + '.parquet'
        summary_fname_csv = summary_fname + '.csv'
        
        summary_columns = [
            'Year-Quarter',
            'Row Count',
            'Unique Loan Count',
            'Unique Date Count'
        ]
        
        total_row_count, total_loan_count, total_date_count = 0, 0, 0

        df_summary = pd.DataFrame(columns=summary_columns)

        files_to_process = [f for f in os.listdir(self.fm_dir) if f.startswith(self.quarterly_file_prefix) & f.endswith('.parquet')]

        start_time_global = time.perf_counter()

        for fname in files_to_process:

            start_time = time.perf_counter()

            fpath = os.path.join(self.fm_dir, fname)
            df = pd.read_parquet(fpath, columns=[self.loancol, self.datecol])

            yyyyqq = fname.replace(self.quarterly_file_prefix, '').replace('.parquet', '')
            row_count = len(df)
            loan_count = len(df[self.loancol].unique())
            date_count = len(df[self.datecol].unique())

            total_row_count += row_count
            total_loan_count += loan_count

            row_dict = {
                'Year-Quarter': yyyyqq,
                'Row Count': row_count,
                'Unique Loan Count': loan_count,
                'Unique Date Count': date_count
            }
            row = pd.DataFrame([row_dict], columns=summary_columns)
            df_summary = pd.concat([df_summary, row])

            end_time = time.perf_counter()
            print(f'    Finished processing {yyyyqq}, total duration: {end_time - start_time:0.2f}')

        # total_date_count is the unique date count from the first file (1999Q1), which has all unique dates
        # NOTE: For the purpose of saving to parquet, it is very important to get total_date_count using syntax
        # exactly as below. Although df_summary['Unique Date Count'][0] or df_summary.loc[0, 'Unique Date Count']
        # would also extract the correct value of correct type, saving to parquet would fail for some reason.

        total_date_count = df_summary['Unique Date Count'].values[0]

        final_row_dict = {
            'Year-Quarter': 'ALL',
            'Row Count': total_row_count,
            'Unique Loan Count': total_loan_count,
            'Unique Date Count': total_date_count
        }
        final_row = pd.DataFrame([final_row_dict], columns=summary_columns)
        df_summary = pd.concat([df_summary, final_row])
    
        end_time_global = time.perf_counter()
        print(f'    Finished summarizing dataset, total duration: {end_time_global - start_time_global:0.2f}')

        # Write summary table to disk 
        df_summary.to_parquet(os.path.join(self.summary_dir, summary_fname_parquet))
        df_summary.to_csv(os.path.join(self.summary_dir, summary_fname_csv), index=False)


    def prepare_date_list(
        self,
        min_extract_date: int,
        max_extract_date: int
    ):
        """
        Prepare a reverse-chronologically sorted list of integer dates in the yyyymm format,
        given the min and max extract dates as inputs. 
        This is used by the extract_monthly_by_month() function.
        """

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


    def summarize_loans_pyarrow(self):
        """
        Prepare a summary of loan distribution by origination quarter at each date.
        Runtime: about 33 s (5 times faster than using pandas)
        NOTE: First reporting date for a given loan may not necessarily be the date
        of origination - could identify number of missing dates for those loans.
        """

        summary_fname = 'Loan_Distribution_By_Origination_Quarter'
        summary_fname_parquet = summary_fname + '.parquet'
        summary_fname_csv = summary_fname + '.csv'
        
        pd_pa_dtype_map = {
            str: pa.string(),
            int: pa.int64(),
            'string': pa.string(),
            'Int64': pa.int64(),
            'float64': pa.float64()
        }

        cols_to_extract = {
            self.loancol: str,
            self.datecol : 'Int64'
        }
        selected_cols = list(cols_to_extract.keys())

        pa_schema = pa.schema([
            pa.field(self.loancol, pd_pa_dtype_map[cols_to_extract[self.loancol]]),
            pa.field(self.datecol, pd_pa_dtype_map[cols_to_extract[self.datecol]])
        ])

        files_to_process = [f for f in os.listdir(self.fm_dir) if f.startswith(self.quarterly_file_prefix) & f.endswith('.parquet')]
        
        first_file = min(files_to_process)
        last_file = max(files_to_process)
        df_first = pq.read_table(os.path.join(self.fm_dir, first_file), columns=selected_cols)
        df_last = pq.read_table(os.path.join(self.fm_dir, last_file), columns=selected_cols)
        min_extract_date = np.min(df_first[self.datecol])
        max_extract_date = np.max(df_last[self.datecol])

        ym_list = self.prepare_date_list(min_extract_date, max_extract_date)

        # ym_list is sorted reverse-chronologically
        ym_list_str = list(map(str, ym_list))

        yq_col = 'Year-Quarter'
        cols = [yq_col] + ym_list_str

        pa_summary_schema_list = [pa.field(yq_col, pa.string())]
        for col in ym_list_str:
            pa_summary_schema_list.append(pa.field(col, pa.int64()))
        pa_summary_schema = pa.schema(pa_summary_schema_list)

        df0 = pd.DataFrame(columns=cols)
        df_summary = pa.Table.from_pandas(df0, schema=pa_summary_schema)

        start_time = time.perf_counter()

        for fname in files_to_process:

            print(f'Extracting loan counts from {fname}')
            yyyyqq = fname.replace(self.quarterly_file_prefix, '').replace('.parquet', '')
            fpath = os.path.join(self.fm_dir, fname)
            fr = pq.read_table(fpath, columns=selected_cols)
            grp = fr.group_by(self.datecol).aggregate([(self.loancol, 'count')])

            row_dict = {yq_col: [yyyyqq]}
            for batch in grp.to_batches():
                d = batch.to_pydict()
                for col1, col2 in zip(d[self.datecol], d[self.loancol +'_count']):
                    row_dict.update({str(col1): [col2]})
            
            fullrow_dict = {}
            for field in pa_summary_schema.names:
                if field in row_dict:
                    fullrow_dict[field] = row_dict[field]
                else:
                    fullrow_dict[field] = [0]
            row = pa.Table.from_pydict(fullrow_dict, schema=pa_summary_schema)

            df_summary = pa.concat_tables([df_summary, row])
        
        end_time = time.perf_counter()
        print(f'    Finished processing all quarterly files, total duration: {end_time - start_time:0.2f}')

        print(f'Saving {summary_fname_parquet} and {summary_fname_csv}')
        pq.write_table(df_summary, os.path.join(self.summary_dir, summary_fname_parquet))
        df_summary_pd = df_summary.to_pandas()
        df_summary_pd.to_csv(os.path.join(self.summary_dir, summary_fname_csv), index=False)


    def summarize_loans_pandas(self):
        """
        Prepare a summary of loan distribution by origination quarter at each date.
        Runtime: about 165 s
        NOTE: First reporting date for a given loan may not necessarily be the date
        of origination - could identify number of missing dates for those loans.
        """

        summary_fname = 'Loan_Distribution_By_Origination_Quarter'
        summary_fname_parquet = summary_fname + '.parquet'
        summary_fname_csv = summary_fname + '.csv'
        
        cols_to_extract = {
            self.loancol: str,
            self.datecol : 'Int64'
        }
        selected_cols = list(cols_to_extract.keys())

        files_to_process = [f for f in os.listdir(self.fm_dir) if f.startswith(self.quarterly_file_prefix) & f.endswith('.parquet')]
        
        first_file = min(files_to_process)
        last_file = max(files_to_process)
        df_first = pd.read_parquet(os.path.join(self.fm_dir, first_file), columns=selected_cols)
        df_last = pd.read_parquet(os.path.join(self.fm_dir, last_file), columns=selected_cols)
        min_extract_date = int(df_first[self.datecol].min())
        max_extract_date = int(df_last[self.datecol].max())

        ym_list = self.prepare_date_list(min_extract_date, max_extract_date)

        # ym_list is sorted reverse-chronologically
        ym_list_str = list(map(str, ym_list))

        yq_col = 'Year-Quarter'
        cols = [yq_col] + ym_list_str
        df_summary = pd.DataFrame(columns=cols)

        start_time = time.perf_counter()

        for fname in files_to_process:

            print(f'Extracting loan counts from {fname}')
            yyyyqq = fname.replace(self.quarterly_file_prefix, '').replace('.parquet', '')
            fpath = os.path.join(self.fm_dir, fname)
            fr = pd.read_parquet(fpath, columns=selected_cols)
            grp = fr.groupby(self.datecol)[self.loancol].count()
            row = pd.DataFrame(columns=cols, index=[0])
            row[yq_col] = yyyyqq
            for key in grp.index:
                row[str(key)] = grp[key]
            row = row.fillna(0)
            df_summary = pd.concat([df_summary, row])
            df_summary = df_summary.reset_index(drop=True)
        
        end_time = time.perf_counter()
        print(f'    Finished processing all quarterly files, total duration: {end_time - start_time:0.2f}')

        df_summary.to_parquet(os.path.join(self.summary_dir, summary_fname_parquet))
        df_summary.to_csv(os.path.join(self.summary_dir, summary_fname_csv), index=False)


    def extract_monthly_by_month_pyarrow(
        self,       
        varname,
        vartype
    ):

        """
        Extract all loans at a given date (yyyymm), starting from the latest, i.e. 202312.

        Looping logic: For each year-month (yyyymm), extract that data from all quarterly files
        ('historical_data_time_YYYYQQ.parquet') and add them to each individual monthly file. 
        This has the advantage of each incremental batch output not modifying the previously 
        generated monthly extracts. Also any monthly batch run can be restarted if the code 
        execution gets interrupted in any way. Memory problems should not occur because each 
        monthly extract saved to disk is relatively small (< 200 MB). 
        The output file name starts with the monthly file prefix of 'all_loans_'. Each output 
        parquet file is about half the size of the equivalent file produced using pandas.
        """

        varname_fname = varname.replace(' ', '_').replace(')', '').replace('(', '')
        var_dir = os.path.join(self.fm_dir, varname_fname)
        if not os.path.exists(var_dir):
            os.makedirs(var_dir)

        pd_pa_dtype_map = {
            str: pa.string(),
            int: pa.int64(),
            'string': pa.string(),
            'Int64': pa.int64(),
            'float64': pa.float64()
        }

        cols_to_extract = {
            self.loancol: str,
            self.datecol : 'Int64',
            varname: vartype
        }
        selected_cols = list(cols_to_extract.keys())

        pyarrow_schema = pa.schema([
            pa.field(self.loancol, pd_pa_dtype_map[cols_to_extract[self.loancol]]),
            pa.field(self.datecol, pd_pa_dtype_map[cols_to_extract[self.datecol]]),
            pa.field(varname, pd_pa_dtype_map[vartype])
        ])

        ym_list = self.prepare_date_list(self.min_extract_date, self.max_extract_date)

        # Create a reverse list of parquet data files to start with the most recent ones
        parquet_list = [f for f in os.listdir(self.fm_dir) if f.startswith(self.quarterly_file_prefix) & f.endswith('.parquet')]

        parquet_list.reverse()

        start_time_global = time.perf_counter()

        for ym in ym_list:

            start_time = time.perf_counter()

            df0 = pd.DataFrame(columns=selected_cols)
            df = pa.Table.from_pandas(df0, schema=pyarrow_schema)  # pyarrow table from pandas dataframe

            print(f'Extracting {ym} data from all quarterly files...')

            for pname in parquet_list:

                ppath = os.path.join(self.fm_dir, pname)
                fr = pq.read_table(ppath, columns=selected_cols, filters=[(self.datecol, '=', ym)])
                df = pa.concat_tables([df, fr])

            end_time = time.perf_counter()
            print(f'    Finished extraction for {ym}, duration: {end_time - start_time:0.2f}')
            # Typically 30-33s

            start_time = time.perf_counter()

            dname = self.monthly_file_prefix + str(ym) + '_' + varname_fname + '_pandas.parquet'
            print(f'    Saving {dname}')
            pq.write_table(df, os.path.join(var_dir, dname))

            end_time = time.perf_counter()
            print(f'    Saved {dname}, duration: {end_time - start_time:0.2f}')

        end_time_global = time.perf_counter()
        print(f'    Finished extraction, total duration: {end_time_global - start_time_global:0.2f}')


    def extract_monthly_by_month_pandas(
        self,       
        varname,
        vartype
    ):

        """
        Extract all loans at a given date (yyyymm), starting from the latest, i.e. 202312.
        This differs from group_freddie_mac_monthly(), where each monthly extract contains only
        loans originated at or past a selected starting year-quarter file.

        Looping logic: For each year-month (yyyymm), extract that data from all quarterly files
        ('historical_data_time_YYYYQQ.parquet') and add them to each individual monthly file. 
        This has the advantage of each incremental batch output not modifying the previously 
        generated monthly extracts. Also any monthly batch run can be restarted if the code 
        execution gets interrupted in any way. Memory problems should not occur because each 
        monthly extract saved to disk is relatively small (< 200 MB). 
        The output file name starts with the monthly file prefix of 'all_loans_'.

        """

        varname_fname = varname.replace(' ', '_').replace(')', '').replace('(', '')
        var_dir = os.path.join(self.fm_dir, varname_fname)
        if not os.path.exists(var_dir):
            os.makedirs(var_dir)

        cols_to_extract = {
            self.loancol: str,
            self.datecol : 'Int64',
            varname: vartype
        }
        selected_cols = list(cols_to_extract.keys())

        ym_list = self.prepare_date_list(self.min_extract_date, self.max_extract_date)

        # Create a reverse list of parquet data files to start with the most recent ones
        parquet_list = [f for f in os.listdir(self.fm_dir) if f.startswith(self.quarterly_file_prefix) & f.endswith('.parquet')]
        parquet_list.reverse()

        for ym in ym_list:

            df = pd.DataFrame(columns=selected_cols).astype(cols_to_extract)

            for pname in parquet_list:

                print(f'Extracting {ym} data from {pname}')
                ppath = os.path.join(self.fm_dir, pname)
                fr = pd.read_parquet(ppath, columns=selected_cols, filters=[(self.datecol, '=', ym)])
                df = pd.concat([df, fr])

            dname = self.monthly_file_prefix + str(ym) + '_' + varname_fname + '.parquet'
            df.to_parquet(os.path.join(var_dir, dname))


    def extract_monthly_by_quarter(
        self,       
        varname,
        vartype,
        nfiles
    ):
        """
        NOTE: Not used in the final approach.

        varname:    variable name - 'Estimated LTV', 'Current Unpaid Balance' or 'Total Expenses'
        vartype:    variable type, e.g. 'float64'
        nfiles:     how many quarterly file to omit from processing - discontinued approach

        In this approach, monthly output files (named yyyymm_variable.parquet) are created by 
        extracting Freddie-Mac quarterly loan data for a subset of loans originated at or 
        after a specified date. The process starts by processing the most recent quarterly 
        files, with the monthly data being progressively added to each individual monthly 
        extract file. The earlier the quarterly data file, the longer it would take to process, 
        because it would contain more and more of different yearly-monthly records. Thus, unless 
        we can satisfy ourselves with an incomplete set of active loans existing at each monthly 
        extract date, we would need to continue the extraction process all the way back to the 
        earliest dataset, e.i. 1999Q1. That would take several days to complete.
        As an example, the number of loans originated at or past 2011Q1 is 24,346,304.

        Looping logic: for each quarterly file (containing loans originated at that quarter)
        extract all sets of monthly data and add them to existing monthly extract files.

        Output file name: yyyymm_variable.parquet
        """

        varname_fname = varname.replace(' ', '_').replace(')', '').replace('(', '')
        var_dir = os.path.join(self.fm_dir, varname_fname)
        if not os.path.exists(var_dir):
            os.makedirs(var_dir)

        cols_to_extract = {
            self.loancol: str,
            self.datecol : 'Int64',
            varname: vartype
        }
        selected_cols = list(cols_to_extract.keys())
        
        # Create a reverse list of parquet data files to start with the most recent ones
        parquet_list = [f for f in os.listdir(self.fm_dir) if f.startswith(self.quarterly_file_prefix) & f.endswith('.parquet')]
        parquet_list.reverse()
        parquet_list = parquet_list[:nfiles]  # Change this

        for pname in parquet_list:

            print(f'Reading in {pname}')
            ppath = os.path.join(self.fm_dir, pname)
            fr = pd.read_parquet(ppath)
            fr = fr[selected_cols]
            dates = list(fr[self.datecol].unique())
            dates.sort()
            dates.reverse()

            for di in dates:  # integer dates

                d = str(di)
                print('\033[K', d, '\r', end='')  # Clear the line without scrolling and print the date

                dname = d + '_' + varname_fname + '.parquet'
                fr_date = fr.loc[fr[self.datecol]==di, :]  # Subset of input data for each date

                if dname in os.listdir(var_dir):
                    df_date = pd.read_parquet(os.path.join(var_dir, dname))
                    df_date = pd.concat([df_date, fr_date])
                else:
                    df_date = fr_date.copy()

                df_date = df_date.drop_duplicates()
                df_date = df_date.reset_index(drop=True)
                df_date = df_date.sort_values(by=[self.datecol, self.loancol])
                df_date.to_parquet(os.path.join(var_dir, dname))


    def extract_monthly_to_single_file(
        self,       
        varname,
        vartype,
        nfiles
    ):
        """
        NOTE: Original script name: aggregate_freddie_mac_data. Not used in the final approach.

        varname:    variable name - 'Estimated LTV', 'Current Unpaid Balance' or 'Total Expenses'
        vartype:    variable type, e.g. 'float64'
        nfiles:     how many quarterly file to omit from processing - discontinued approach

        An initial attempt to aggregate all time series data for several variables 
        - and eventually for a single variable due to hitting numpy memory limits - 
        into a single file. This would take a very long time and create impractically 
        large parquet output. For example, aggregating all loans originated 2011Q1 or later
        would have to be done in batches to eventually, after a couple of days, produce
        a 15 GB output for Current UPB. The total number of loans originated at or past 2011Q1 
        is 24,346,304.
        
        NOTE: This approach was not attempted for Estimated LTV and Total Expenses
        and was eventually abandoned.
        """

        var_fname = varname.replace(' ', '_').replace(')', '').replace('(', '')
        input_fname = f'input_{var_fname}.parquet'
        allpath = os.path.join(self.fm_dir, input_fname)

        cols_to_extract = {
            self.loancol: str,
            self.datecol : 'Int64',
            varname: vartype
        }
        selected_cols = list(cols_to_extract.keys())

        # Create a reverse list of parquet data files to start with the most recent ones
        parquet_list = [f for f in os.listdir(self.fm_dir) if f.startswith(self.quarterly_file_prefix) & f.endswith('.parquet')]
        parquet_list.reverse()
        parquet_list = parquet_list[nfiles:]

        for pname in parquet_list:

            print(f'Reading in {pname}')
            ppath = os.path.join(self.fm_dir, pname)
            fr = pd.read_parquet(ppath)
            fr = fr[selected_cols]

            df = pd.read_parquet(allpath)
            df = pd.concat([df, fr])

            df.to_parquet(allpath)

