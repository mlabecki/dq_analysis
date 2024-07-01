import streamlit as st
import base64
from css_freddie_mac import *

st.set_page_config(layout="wide")


def setup_sidebar():
    st.sidebar.markdown(sidebar_header_style, unsafe_allow_html=True)
    st.markdown(sidebar_navbar_style, unsafe_allow_html=True)


def setup_main_area():
    """
    Set up the main area: title and description of contents
    """

    title = 'Freddie Mac Loan Portfolio'
    st.title(title)
    st.markdown(front_page_style, unsafe_allow_html=True)
        
    par1 = """Hello and thank you for coming here."""
    
    par2 = """The purpose of this app is to provide a simple interactive interface that could help 
    the user gain an insight into the Freddie Mac single-family loan portfolio and suggest how it
    could be analyzed from the data quality point of view. The dataset was downloaded from 
    <a href='https://www.freddiemac.com/research/datasets/sf-loanlevel-dataset'>
    https://www.freddiemac.com/research/datasets/sf-loanlevel-dataset</a>."""
    
    par3 = """The <a href='Data_Quality_Analysis'>Data Quality Analysis</a> page 
    presents various time-dependent profiles for three selected loan attributes, which could be 
    helpful in the detection of possible temporal discontinuities and other data quality anomalies. 
    This analysis serves mainly as a proof of concept and is not meant to be exhaustive."""
    
    par4 = """The following <a href='Origination_Analysis'>Origination Analysis</a> 
    page summarizes the loan counts as observed either at a particular monthly reporting date or 
    throughout the reporting history. The loan counts are grouped by the year and quarter of origination, 
    which is how the original Freddie Mac data is organized. This analysis serves mainly to help 
    understand better how the whole dataset is structured."""
    
    st.markdown("<p>" + par1 + "</p>", unsafe_allow_html=True)
    st.markdown("<p>" + par2 + "</p>", unsafe_allow_html=True)

    col1, col2, col3 = st.columns([9, 1, 15])
    col1.markdown("<p>" + par3 + "</p>", unsafe_allow_html=True)
    col2.markdown('')  # a spacer column
    col3.markdown(
    """<a href='Data_Quality_Analysis'>
       <img src='data:image/jpg; base64, {}' width='600'></a>"""
       .format(base64.b64encode(open('../img/freddie_mac/Page1_DataQualityAnalysis_w600.jpg', 'rb').read()).decode()), unsafe_allow_html=True)

    st.container(height=1, border=False)

    col1, col2, col3 = st.columns([9, 1, 15])
    col1.markdown("<p>" + par4 + "</p>", unsafe_allow_html=True)
    col2.markdown('')  # a spacer column
    col3.markdown(
    """<a href='Origination_Analysis'>
       <img src='data:image/jpg; base64, {}' width='600'></a>"""
       .format(base64.b64encode(open('../img/freddie_mac/Page2_OriginationAnalysis_w600.jpg', 'rb').read()).decode()), unsafe_allow_html=True)


if __name__ == '__main__':
    setup_sidebar()
    setup_main_area()