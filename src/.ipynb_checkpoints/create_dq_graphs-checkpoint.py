import streamlit as st
import pandas as pd
import yaml
import os
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
from mapping_freddie_mac import *
from prepare_settings import ReadYaml

conf = ReadYaml()

st.set_page_config(layout="wide")

dir = conf.dir
loancol = conf.loancol
datecol = conf.datecol
dq_subdir = conf.dq_subdir

dq_dir = os.path.join(dir, dq_subdir)

@st.cache_data
def load_variable_data(varname):
    """
    varname:    variable for which to load data
    """
    vmap = var_map[varname]
    df = pd.read_parquet(os.path.join(dq_dir, vmap['fname']))
    # Set the Date column type to string to make sure it is treated as a categorical variable
    df['Date'] = df['Date'].astype(str)

    return df


@st.cache_data
def plot_pct(
    varname,
    df, 
    start_date,
    end_date,
    legend_location='lower left',
    grid='both'
):
    """
    Create a percentile plot
    varname:    variable to be plotted
    df:         pre-loaded dataframe containing data for variable varname
    """
    
    vmap = var_map[varname]

    # Set x- and y-axis limits for each variable to be plotted
    x_min = start_date
    x_max = end_date
    y_min = vmap['pct_y_min']
    y_max = vmap['pct_y_max']

    # Set tick spacing based on the total number of date points
    n = len(df.loc[(df['Date'] >= start_date ) & (df['Date'] <= end_date)])
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
def plot_metric(varname, metric, df, start_date, end_date, grid='both'):
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
    
    # Number of date points
    n = len(df.loc[(df['Date'] >= start_date ) & (df['Date'] <= end_date)])
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


def squeeze(char: str, s: str):
    """
    Removes repeated instances of character char from string s
    """
    while char*2 in s:
        s = s.replace(char*2, char)
    return s


#
# Sidebar
#
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
st.sidebar.container(height=10, border=False)
st.sidebar.markdown(variable_description_style, unsafe_allow_html=True)
st.sidebar.markdown("<div>Variable Description</div>", unsafe_allow_html=True)
st.sidebar.markdown("<p style='font-size: 16px; line-height: 1.3'>" + var_desc_map[varname] + "</p>", unsafe_allow_html=True)

#
# Main area
#
title = 'Freddie Mac Loan Portfolio - Data Quality Analysis'
title_id = title.replace(' ', '-').lower()  # 'freddie-mac-loan-portfolio---data-quality-summary'
title_id = squeeze('-', title_id)           # 'freddie-mac-loan-portfolio-data-quality-summary'
header_style = header_style.replace('title', title_id)
st.markdown(header_style, unsafe_allow_html=True)
st.title(title)

col1, col2 = st.columns([5, 5])
with col1:
    plot_pct(varname, df, start_date, end_date, legend_location, grid)
with col2:
    plot_metric(varname, metric, df, start_date, end_date, grid)
    
