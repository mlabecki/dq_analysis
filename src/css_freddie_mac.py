# CSS
title_style = """
<style>
    #title {
        text-align: center;
        vertical-align: top;
        font-face: serif;
        font-size: 36px;
        font-weight: bold;
        color: #004499;
    }
    h1 {
        text-align: center;
        vertical-align: top;
        font-face: serif;
        font-size: 36px;
        font-weight: bold;
        color: #004499;
    }
    h3 {
        text-align: center;
        vertical-align: top;
        font-face: serif;
        font-size: 24px;
        font-weight: bold;
        color: brown;
    }
"""
front_page_style = """
<style>
    h1 {
        text-align: left;
        vertical-align: top;
        font-face: serif;
        font-size: 36px;
        font-weight: bold;
        color: #004499;
    }
    p {
        text-align: left;
        font-face: serif;
        font-size: 18px;
        font-weight: normal;
        color: black;
        line-height: 1.25;
    }
</style>
"""
widget_label_style = """
<style>
    div[class*="stSelectbox"] label p {
        font-size: 18px;
        font-weight: normal;
        color: black;
    }
    .stSelectbox > div {
        font-size: 16px;
    }
    div[class*="stCheckbox"] label p {
       font-size: 18px;
       color: black;
    }
    div[class*="stSlider"] label p {
        font-size: 18px;
        font-weight: normal;
        color: black;
    }
    div[class*="StyledThumbValue"]  {
        font-size: 17px;
        font-weight: bold;      
        color: brown;
    }
    div[data-testid*="stTickBarMin"]  {
        font-size: 14px;
        font-weight: normal;     
        color: black;        
    }
    div[data-testid*="stTickBarMax"]  {
        font-size: 14px;
        font-weight: normal;        
        color: black;        
    }
</style>
"""
sidebar_header_style = """
<style>
    div[data-testid*="stSidebarHeader"] {
        height: 40px;
    }
</style>
"""
sidebar_navbar_style = """
<style>
    ul[data-testid*="stSidebarNavItems"] span {
        font-size: 19px;
        font-weight: bold;
        font-face: serif;
        color: #004499;
    }
</style>
"""
sidebar_radio_style = """
<style>
    div[class*="stMarkdown"] {
        font-size: 20px;
        font-weight: normal;
        font-face: sans-serif;
        color: brown;
    }
    div[class*="stRadio"] > label > div[data-testid="stMarkdownContainer"] > p {
        font-size: 20px;
        font-weight: normal;       
        font-face: sans-serif;
        color: brown;
        max-height: 50;
    }
</style>
"""
variable_description_style = """
<style>
    div[class*="stMarkdown"] {
        font-size: 18px;
        font-weight: bold;
        font-face: serif;
        color: #606063;
    }
</style> 
"""
