import pandas as pd
import os
from convert_date import *
from mapping_freddie_mac import *
from prepare_settings import ReadSettings

class FreddieMac:
    
    def __init__(self):
        
        self.conf = ReadSettings()
        self.dir = self.conf.freddie_mac_dir
        self.layout_file = self.conf.layout_file
        self.quarterly_file_prefix = self.conf.quarterly_file_prefix
        self.monthly_file_prefix = self.conf.monthly_file_prefix
        self.dq_subdir = self.conf.dq_subdir
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

        lpath = os.path.join(dir, self.layout_file)

        # Process the data layout file, include only selected variables marked with the INCLUDE flag equal to 1
        ldf = pd.read_csv(lpath)
        allcols = [col for col in ldf['ATTRIBUTE NAME']]
        cols = [col for col in ldf.loc[ldf['INCLUDE'] == 1, 'ATTRIBUTE NAME']]
        dtype_dict = pd.Series(ldf['DTYPE'].values, index=ldf['ATTRIBUTE NAME']).to_dict()

        parquet_list = [f for f in os.listdir(dir) if f.startswith(self.quarterly_file_prefix) & f.endswith('.parquet')]
        files_to_process = [
            f for f in os.listdir(dir) if
                f.startswith(self.quarterly_file_prefix) & 
                f.endswith('.txt') & 
                ~(f[:-3] + 'parquet' in parquet_list)  # Skip files that already exist
        ]

        for fname in files_to_process:

            print(f'Processing {fname} ...')
            pname = fname[:-3] + 'parquet'
            fpath = os.path.join(dir, fname)
            ppath = os.path.join(dir, pname)

            # Read in pipe-separated quarterly Freddie Mac file containing no column header
            fr = pd.read_csv(fpath, names=allcols, header=None, sep='|', dtype=dtype_dict)

            # Add column header based on selected variables from time_data_layout.csv
            fr = fr[cols]

            # Write re-formatted quarterly file to parquet, e.g. historical_data_time_2023Q3.parquet
            fr.to_parquet(ppath)


    def prepare_date_list(
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


    def extract_monthly_by_month(
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
        var_dir = os.path.join(dir, varname_fname)
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
        parquet_list = [f for f in os.listdir(dir) if f.startswith(self.quarterly_file_prefix) & f.endswith('.parquet')]
        parquet_list.reverse()

        for ym in ym_list:

            df = pd.DataFrame(columns=selected_cols).astype(cols_to_extract)

            for pname in parquet_list:

                print(f'Extracting {ym} data from {pname}')
                ppath = os.path.join(dir, pname)
                fr = pd.read_parquet(ppath)
                fr = fr.loc[fr[self.datecol]==ym, selected_cols]
                df = pd.concat([df, fr])

            dname = self.monthly_file_prefix + str(ym) + '_' + varname_fname + '.parquet'
            print(f'Sorting and saving {dname}')
            df = df.reset_index(drop=True)
            df = df.sort_values(by=[self.datecol, self.loancol])
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
        var_dir = os.path.join(dir, varname_fname)
        if not os.path.exists(var_dir):
            os.makedirs(var_dir)

        cols_to_extract = {
            self.loancol: str,
            self.datecol : 'Int64',
            varname: vartype
        }
        selected_cols = list(cols_to_extract.keys())
        
        # Create a reverse list of parquet data files to start with the most recent ones
        parquet_list = [f for f in os.listdir(dir) if f.startswith(self.quarterly_file_prefix) & f.endswith('.parquet')]
        parquet_list.reverse()
        parquet_list = parquet_list[:nfiles]  # Change this

        for pname in parquet_list:

            print(f'Reading in {pname}')
            ppath = os.path.join(dir, pname)
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
        allpath = os.path.join(dir, input_fname)

        cols_to_extract = {
            self.loancol: str,
            self.datecol : 'Int64',
            varname: vartype
        }
        selected_cols = list(cols_to_extract.keys())

        # Create a reverse list of parquet data files to start with the most recent ones
        parquet_list = [f for f in os.listdir(dir) if f.startswith(self.quarterly_file_prefix) & f.endswith('.parquet')]
        parquet_list.reverse()
        parquet_list = parquet_list[nfiles:]

        for pname in parquet_list:

            print(f'Reading in {pname}')
            ppath = os.path.join(dir, pname)
            fr = pd.read_parquet(ppath)
            fr = fr[selected_cols]

            df = pd.read_parquet(allpath)
            df = pd.concat([df, fr])

            df.to_parquet(allpath)

