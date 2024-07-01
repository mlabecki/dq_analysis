import streamlit as st
import pandas as pd
import os
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
from mapping_freddie_mac import *
from css_freddie_mac import *
from prepare_settings import ReadSettings
from utils import *

conf = ReadSettings()

st.set_page_config(layout="wide")

dir = conf.freddie_mac_dir
loancol = conf.loancol
datecol = conf.datecol
dq_subdir = conf.dq_subdir
summary_dir = conf.summary_dir

dq_dir = os.path.join(dir, dq_subdir)


@st.cache_data
def load_loans_summary(fname='Loan_Distribution_By_Origination_Quarter.csv', transpose=False):
    """
    Load loans summary table
    """

    df_loans = pd.read_csv(os.path.join(summary_dir, fname))

    if transpose:
        # Transpose, reset indexes and column names
        yq_col = df_loans.columns[0]  # 'Year-Quarter'
        df_loans = df_loans.set_index(yq_col).T
        df_loans['Date'] = df_loans.index
        df_loans.insert(0, 'Date', df_loans.pop('Date'))  # Set as the first column
        df_loans = df_loans.reset_index(drop=True).rename_axis(None, axis=1)

    return df_loans


@st.cache_data
def load_dataset_summary(
    fname='Monthly_Performance_Dataset_Summary.csv',
    cols=['Year-Quarter', 'Unique Loan Count']
):
    """
    Load dataset summary table
    """

    df_dataset = pd.read_csv(os.path.join(summary_dir, fname))
    df_dataset = df_dataset[cols]

    return df_dataset


@st.cache_data
def plot_loan_summary(
    date_str: str,
    start_quarter: str,
    end_quarter: str,
    set_y_max: bool = False,
    y_max: float = 1.4e6,
    grid='both'
):
    """
    Create a plot of loan counts by origination quarter for the selected date
    df: dataframe containing loans_summary data
    date_str: selected date in the yyyymm format, string type
    start_quarter: min x value
    end_quarter: max x value
    """

    # Set y-axis lower limit, y_max will come from a user input
    y_min = 0
    
    # Read in df_loans
    df_loans = load_loans_summary()
    yq_col = df_loans.columns[0]

    loan_count_col = 'Unique Loan Count'
    df_dataset = load_dataset_summary()
    
    quarters_range_loans = (df_loans[yq_col] >= start_quarter) & (df_loans[yq_col] <= end_quarter)
    quarters_range_dataset = (df_dataset[yq_col] >= start_quarter) & (df_dataset[yq_col] <= end_quarter)
    n = len(df_loans.loc[quarters_range_loans])
    tick_spacing = max(1, round(n / 32))
    
    fig = plt.figure(figsize=(10,4))  # Set aspect ratio
    ax = fig.add_subplot()

    # plt.bar(df_loans[yq_col], df_loans[date_str], label=date_str, align='center')
    if date_str == 'All Dates':
        plt.bar(df_dataset.loc[quarters_range_dataset, yq_col], df_dataset.loc[quarters_range_dataset, loan_count_col], label=date_str, align='center')
    else:
        plt.bar(df_loans.loc[quarters_range_loans, yq_col], df_loans.loc[quarters_range_loans, date_str], label=date_str, align='center')

    # plt.axis([x_min, x_max, y_min, y_max])
    plt.xlabel('Origination Year-Quarter')
    plt.ylabel('Loan Count')
    plt.xticks(fontsize=9, rotation=90)
    plt.yticks(fontsize=9)
    if grid in ['x', 'y', 'both']:
        plt.grid(axis=grid)

    ax.xaxis.set_major_locator(ticker.MultipleLocator(tick_spacing))
    if set_y_max:
        ax.set_ylim([y_min, y_max])
    ax.yaxis.set_major_formatter(ticker.StrMethodFormatter('{x:,.0f}'))  #  Get comma as a thousands separator
    
    st.pyplot(fig)


def setup_sidebar_2():
    """
    Set up Page 2 sidebar components
    """

    y_max_org = int(1.4e6)

    st.sidebar.markdown(widget_label_style, unsafe_allow_html=True)

    df_loans = load_loans_summary()
    yq_col = df_loans.columns[0]
    quarters = df_loans[yq_col]
    reverse_quarters = list(reversed(quarters))
    min_quarter = quarters.min()
    max_quarter = quarters.max()
    reverse_quarters_nomin = list(df_loans.loc[quarters > min_quarter, yq_col])
    reverse_quarters_nomin.reverse()
    dates = list(df_loans.columns[1:])
    reverse_dates = list(reversed(dates))
    max_date = max(reverse_dates)

    # Find the first year-quarter past which loan counts for a given date are all zero
    last_nonzero_quarter = {}
    for d in dates:
        for q in reverse_quarters:
            if df_loans.loc[df_loans[yq_col]==q, d].values[0] != 0:
                last_nonzero_quarter.update({d: q})
                break

    select_date_dict = {            
        '**Select Single Date From Slider**': 'Slider',
        '**Select Single Date From List**': "Drop-Down",
        '**Select All Reporting Dates**': 'All'
    }
    select_date_type = st.sidebar.radio(
        '**Select Monthly Reporting Date**',
        options = select_date_dict.keys()
    )

    if select_date_dict[select_date_type] == 'Slider':
        date_str = st.sidebar.select_slider('**Select Single Date From Slider:**', options=reverse_dates, value=max_date)
        st.sidebar.selectbox('**Select Single Date From List:**', dates, disabled=True)
    elif select_date_dict[select_date_type] == 'Drop-Down':
        st.sidebar.select_slider('**Select Single Date From Slider:**', options=reverse_dates, value=max_date, disabled=True)
        date_str = st.sidebar.selectbox('**Select Single Date From List:**', dates)
    else:
        date_str = 'All Dates'
        st.sidebar.select_slider('**Select Single Date From Slider:**', options=reverse_dates, value=max_date, disabled=True)
        st.sidebar.selectbox('**Select Single Date From List:**', dates, disabled=True)

    st.sidebar.container(height=2, border=False)  # spacer - can increase height, if needed

    st.sidebar.markdown("<div><b>Select Origination Range</b></div>", unsafe_allow_html=True)

    st.sidebar.container(height=0, border=False)  # spacer - can increase height, if needed
        
    # End quarter auto adjustment checkbox button
    if date_str != 'All Dates':
        auto_end_quarter = st.sidebar.checkbox(
            '**Auto Adjust End Quarter**',
            value=True
        )
    else:
        auto_end_quarter = st.sidebar.checkbox(
            '**Auto Adjust End Quarter**',
            value=False,
            disabled=True
        )

    # Start Quarter and End Quarter selectboxes
    sidebar_col1, sidebar_col2 = st.sidebar.columns(2)

    start_quarter = sidebar_col1.selectbox('**Start Quarter**', df_loans.loc[quarters <= max(reverse_quarters_nomin), yq_col])

    if auto_end_quarter:
        end_quarter = last_nonzero_quarter[date_str]
        sidebar_col2.selectbox('**End Quarter**', reverse_quarters_nomin, disabled=True)
    else:
        end_quarter = sidebar_col2.selectbox('**End Quarter**', reverse_quarters_nomin, disabled=False)

    if date_str != 'All Dates':
        if (start_quarter > end_quarter) | (start_quarter > last_nonzero_quarter[date_str]):
            start_quarter = last_nonzero_quarter[date_str]
            end_quarter = last_nonzero_quarter[date_str]
    else:
        if (start_quarter > end_quarter):
            start_quarter = max_quarter
            end_quarter = max_quarter

    st.sidebar.container(height=2, border=False)  # spacer to adjust height

    # End quarter auto adjustment checkbox button
    set_y_max = st.sidebar.checkbox(
        '**Set y-Axis Upper Limit**'
    )
    # Max y-value slider
    if set_y_max:
        y_max = st.sidebar.slider('**Slide to Set Max y-Value:**', min_value=0, max_value=y_max_org, value=y_max_org, step=50000)
    else:
        y_max = st.sidebar.slider('**Slide to Set Max y-Value:**', min_value=0, max_value=y_max_org, value=y_max_org, step=50000, disabled=True)
        
    # Gridlines selectbox
    select_grid = st.sidebar.selectbox('**Gridlines**', grid_map.keys())
    grid = grid_map[select_grid]

    st.sidebar.markdown(sidebar_header_style, unsafe_allow_html=True)
    st.sidebar.markdown(sidebar_navbar_style, unsafe_allow_html=True)
    st.sidebar.markdown(sidebar_radio_style, unsafe_allow_html=True)

    # Set up a dictionary of input features to return
    input_features_2 = {
        'date_str': date_str,
        'start_quarter': start_quarter,
        'end_quarter': end_quarter,
        'set_y_max': set_y_max,
        'y_max': y_max,
        'grid': grid
    }

    return input_features_2


def setup_main_area_2(input_features):
    """
    Set up the main area: title, columns, plots
    """

    df_loans = load_loans_summary()
    yq_col = df_loans.columns[0]

    dates = list(df_loans.columns[1:])
    dates.reverse()

    loan_count_col = 'Unique Loan Count'
    df_dataset = load_dataset_summary()

    date_str = input_features['date_str']
    start_quarter = input_features['start_quarter']
    end_quarter = input_features['end_quarter']
    set_y_max = input_features['set_y_max']
    y_max = input_features['y_max']
    grid = input_features['grid']
  
    title = 'Freddie Mac Loan Portfolio - Origination Analysis'
    st.markdown(title_style, unsafe_allow_html=True)
    st.title(title)
    spaces_str = add_spaces_html(10)
    
    range_str = f'{start_quarter}-{end_quarter}'
    quarters_range_loans = (df_loans[yq_col] >= start_quarter) & (df_loans[yq_col] <= end_quarter)
    quarters_range_dataset = (df_dataset[yq_col] >= start_quarter) & (df_dataset[yq_col] <= end_quarter)
    if date_str != 'All Dates':
        loan_count = df_loans.loc[quarters_range_loans, date_str].sum()
    else:
        loan_count = df_dataset.loc[quarters_range_dataset, loan_count_col].sum()

    loan_count_str = '{:,.0f}'.format(loan_count)
    str_date = 'Monthly Reporting Date: ' + date_str + spaces_str
    str_orig = 'Origination Range: ' + range_str + spaces_str
    str_loan = "Total Loans Within Range: " + loan_count_str
    st.markdown("<h3>" + str_date + str_orig + str_loan + "</h3>", unsafe_allow_html=True)

    plot_loan_summary(date_str, start_quarter, end_quarter, set_y_max, y_max, grid)


if __name__== '__main__':
    
    input_features = setup_sidebar_2()
    setup_main_area_2(input_features)

