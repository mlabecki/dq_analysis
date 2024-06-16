import streamlit as st
import pandas as pd
import os
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
from mapping_freddie_mac import *
from prepare_settings import ReadSettings

conf = ReadSettings()

st.set_page_config(layout="wide")

freddie_mac_dir = conf.freddie_mac_dir
loancol = conf.loancol
datecol = conf.datecol
dq_subdir = conf.dq_subdir

dq_dir = os.path.join(freddie_mac_dir, dq_subdir)


@st.cache_data
def load_variable_data(varname):
    """
    varname:    variable for which to load data
    """

    vmap = var_map[varname]

    df = pd.read_parquet(os.path.join(dq_dir, vmap['parquet_name']))
    
    # Set the Date column type to string to make sure it is treated as a categorical variable
    df['Date'] = df['Date'].astype(str)

    return df


@st.cache_data
def plot_pct(
    varname,
    df, 
    start_date: str,
    end_date: str,
    legend_location='lower left',
    grid='both'
):
    """
    Create a percentile plot
    varname:    variable to be plotted
    df:         pre-loaded dataframe containing data for variable varname
    start_date and end_date are strings, so 'Date' can be treated as a categorical variable
    """
    
    # Get the map for the variable varname
    vmap = var_map[varname]

    # Set x- and y-axis limits
    x_min = start_date
    x_max = end_date
    y_min = vmap['pct_y_min']
    y_max = vmap['pct_y_max']

    # Set tick spacing based on the total number of date points
    n = len(df.loc[(df['Date'] >= start_date) & (df['Date'] <= end_date)])
    tick_spacing = max(1, round(n / 24))

    pct_labels = [col for col in df.columns if col.startswith('Pct_')]

    fig, ax = plt.subplots(1,1)

    for pct in pct_labels:
        plt.plot(df['Date'], df[pct], label=pct)

    plt.axis([x_min, x_max, y_min, y_max])
    plt.title(f'{varname} - Percentiles')
    plt.xlabel('Date')
    plt.xticks(rotation=90)
    if grid in ['x', 'y', 'both']:
        plt.grid(axis=grid)

    plt.legend(labels=pct_labels, loc=legend_location)
    ax.xaxis.set_major_locator(ticker.MultipleLocator(tick_spacing))
    
    ax.fill_between(x=df['Date'], y1=df['Pct_1'], y2=y_min)
    prev = df['Pct_1']
    for pct in pct_labels[1:]:
        ax.fill_between(x=df['Date'], y1=df[pct], y2=prev)
        prev = df[pct]

    st.pyplot(fig)


@st.cache_data
def plot_metric(
    varname,
    metric,
    df,
    start_date,
    end_date,
    grid='both'
):
    """
    Create a plot for one of the distribution metrics
    varname:    variable to be plotted
    metric:     metric to be plotted, e.g. Fill Rate
    df:         pre-loaded dataframe containing data for variable varname
    """

    # Set x-axis limits
    x_min = start_date
    x_max = end_date

    # Set y-axis limits for each variable to be plotted
    vmap = var_map[varname]
    mapped_metric_max = metric.replace(' ', '').lower() + '_y_max'  # e.g. 'fillrate_y_max'
    y_max = vmap[mapped_metric_max]
    if metric.endswith('Value'):
        # Min Value, Max Value, Mean Value
        mapped_metric_min = metric.replace(' ', '').lower() + '_y_min'  # e.g. 'minvalue_y_min'
        y_min = vmap[mapped_metric_min]
    else:
        # Fill Rate, Total Count, Fill Count, Null Count
        y_min = 0
    
    # Set tick spacing based on the total number of date points
    n = len(df.loc[(df['Date'] >= start_date) & (df['Date'] <= end_date)])
    tick_spacing = max(1, round(n / 24))

    fig, ax = plt.subplots(1,1)
    
    plt.plot(df['Date'], df[metric], label=metric)

    plt.axis([x_min, x_max, y_min, y_max])
    plt.title(f'{varname} - {metric}')
    plt.xlabel('Date')
    plt.xticks(rotation=90)
    if grid in ['x', 'y', 'both']:
        plt.grid(axis=grid)

    ax.xaxis.set_major_locator(ticker.MultipleLocator(tick_spacing))

    st.pyplot(fig)


def squeeze(c: str, s: str):
    """
    Removes repeated instances of character c from string s.
    Used by setup_main_area() to correctly style the title based on a css template.
    
    The css template is defined in mapping_freddie_mac.py and refers to the title 
    object id, which is generically set to 'title' (preceded by '#'):
    <style>
        #title {
        text-align: center;
        vertical-align: top;
        font-face: serif;
        font-size: 36px;
        font-weight: bold;
        color: black;
    }
    </style>
    The title object id is constructed in html by converting to lower case, replacing
    spaces with hyphens, and also removing repeated instances of the hyphen, if present.
    
    In this case, the title is 'Freddie Mac Loan Portfolio - Data Quality Analysis',
    which contains ' - '. The role of the squeeze function is to reduce the '---',
    resulting from a simple replacement of spaces with hyphens, to a single '-'.
    
    Once correctly constructed, the object id of 'title' in the css template
    will be replaced by the object id of 'freddie-mac-loan-portfolio-data-quality-summary'.
    """

    while c*2 in s:
        s = s.replace(c*2, c)
    
    return s


def setup_sidebar():
    """
    Set up sidebar components: widgets and Variable Description
    """

    st.sidebar.markdown(widget_label_style, unsafe_allow_html=True)

    # Variable selectbox
    varname = st.sidebar.selectbox('**Variable**', var_map.keys())

    df = load_variable_data(varname)
    dates = df['Date']

    # Metric selectbox
    metric = st.sidebar.selectbox('**Metric**', metric_list)

    # Start Date and End Date selectboxes
    sidebar_col1, sidebar_col2 = st.sidebar.columns(2)
    start_date = sidebar_col1.selectbox('**Start Date**', df.loc[dates < dates.max(), 'Date'])
    reverse_dates = list(df.loc[dates > dates.min(), 'Date'])
    reverse_dates.reverse()
    end_date = sidebar_col2.selectbox('**End Date**', reverse_dates)
    if end_date <= start_date:
        end_date = dates.max()

    # Gridlines selectbox
    select_grid = st.sidebar.selectbox('**Gridlines**', grid_map.keys())
    grid = grid_map[select_grid]

    # Legend Location selectbox
    legend_location = st.sidebar.selectbox('**Legend Location**', legend_locations).lower()

    # Variable Description
    st.sidebar.container(height=5, border=False)
    st.sidebar.markdown(variable_description_style, unsafe_allow_html=True)
    st.sidebar.markdown("<div>Variable Description</div>", unsafe_allow_html=True)
    st.sidebar.markdown("<p style='font-size: 16px; line-height: 1.25'>" + var_desc_map[varname] + "</p>", unsafe_allow_html=True)

    # Set up a dictionary to return - 'return varname, metric, ...' would only return values but not the variables themselves.
    input_features = {
        'varname': varname,
        'metric': metric,
        'start_date': start_date,
        'end_date': end_date,
        'grid': grid,
        'legend_location': legend_location
    }

    return input_features


def setup_main_area(input_features, header_style):
    """
    Set up the main area: title, columns, plots
    Note: header_style is modified here, so it must be passed as an argument
    """
    
    title = 'Freddie Mac Loan Portfolio - Data Quality Analysis'
    title_id = title.replace(' ', '-').lower()  # 'freddie-mac-loan-portfolio---data-quality-summary'
    title_id = squeeze('-', title_id)           # 'freddie-mac-loan-portfolio-data-quality-summary'
    header_style = header_style.replace('title', title_id)
    st.markdown(header_style, unsafe_allow_html=True)
    st.title(title)

    varname = input_features['varname']
    metric = input_features['metric']
    start_date = input_features['start_date']
    end_date = input_features['end_date']
    grid = input_features['grid']
    legend_location = input_features['legend_location']
    df = load_variable_data(varname)
    
    col1, col2 = st.columns([5, 5])
    with col1:
        plot_pct(varname, df, start_date, end_date, legend_location, grid)
    with col2:
        plot_metric(varname, metric, df, start_date, end_date, grid)
    

if __name__== '__main__':
    input_features = setup_sidebar()
    setup_main_area(input_features, header_style)

