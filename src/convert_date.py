import pandas as pd
import datetime
import math


def eomonth(input_date: datetime, offset_months: int = None):
    year = input_date.year
    month = input_date.month
    if offset_months is None:
        offset_months = 0
    eomonth_date = (datetime.date(year, month, 1)
                    + pd.DateOffset(months=1 + offset_months)
                    + pd.DateOffset(days=-1)).date()
    return eomonth_date


def convert_yq_to_date(
    datestr: str, 
    qtype: str = 'calendar'
    ):

    """
    datestr:    'YYYY QX', e.g. '2016 Q1'
    qtype:      'calendar' (Jan-Apr-Jul-Oct) or
                'fiscal' (Nov-Feb-May-Aug) or
                'fed' (Oct-Jan-Apr-Jul)
    """

    if qtype is None:
        qtype = 'calendar'
    if qtype not in ['calendar', 'fiscal', 'fed']:
        print('Incorrect qtype, defaulting to "calendar"')

    datestr = datestr.lstrip(' ').rstrip(' ')
    year = int(datestr[0:4])
    q = int(datestr[-1:])

    if (qtype == 'fed') & (q == 1):
        year = year - 1  # e.g. fed 2020 Q1 starts in Oct 2019

    eoq_month = {'calendar': {1: 3, 2: 6, 3: 9, 4: 12},
                 'fiscal': {1: 1, 2: 4, 3: 7, 4: 10},
                 'fed': {1: 12, 2: 3, 3: 6, 4: 9}
                 }

    month = eoq_month[qtype][q]

    return eomonth(datetime.date(year, month, 1))


def convert_str_yyyymm_to_date(datestr: str):
    """
    datestr:    'yyyymm', e.g. '201612'
    """

    datestr = datestr.lstrip(' ').rstrip(' ')
    year = int(datestr[0:4])
    month = int(datestr[-2:])

    return eomonth(datetime.date(year, month, 1))


def convert_int_yyyymm_to_date(dateint: int):
    """
    dateint:    yyyymm, e.g. 201612
    """

    a = math.modf(dateint / 100)
    year = int(a[1])
    month = int(round(a[0] * 100))

    return eomonth(datetime.date(year, month, 1))


def fed_add_date(
    df: pd.DataFrame, 
    datecol: str = 'Date',
    qtype: str = 'calendar'
    ):

    """
    df:         dataframe to modify
    datecol:    name of the original date column
    qtype:      'calendar' (Jan-Apr-Jul-Oct) or
                'fiscal' (Nov-Feb-May-Aug) or
                'fed' (Oct-Jan-Apr-Jul)
    """
    
    newcol = datecol + '_' + qtype
    df[newcol] = df[datecol].apply(lambda x: convert_yq_to_date(x, qtype))  # Note: very slow
    df[newcol] = pd.to_datetime(df[newcol], errors='coerce')

    # relocate the new column to immediately follow the original date column
    date_ix = df.columns.get_loc(datecol)
    df.insert(date_ix + 1, newcol, df.pop(newcol))

    df.rename(columns={'Date': 'Year_Q', newcol: 'Date'}, inplace=True)

    return df


def freddie_mac_add_date(
    df: pd.DataFrame, 
    datecol: str = 'Monthly Reporting Period'
    ):

    """
    df:         dataframe to modify
    datecol:    name of the original date column
    """
    
    newcol = 'Date'
    df[newcol] = df[datecol].apply(lambda x: convert_int_yyyymm_to_date(x))  # Note: very slow
    df[newcol] = pd.to_datetime(df[newcol], errors='coerce')

    # relocate the new column to immediately follow the original date column
    date_ix = df.columns.get_loc(datecol)
    df.insert(date_ix + 1, newcol, df.pop(newcol))

    return df

