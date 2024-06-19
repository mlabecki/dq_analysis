# dq_analysis - Data Quality Analysis
The purpose of this project was to create a simple interactive dashboard visualizing the properties of time-dependent distributions of data such as loan attributes. This could be helpful in the detection of possible temporal discontinuities, anomalies and other data quality issues.

### 1. Dataset
Freddie Mac Single-Family Loan-Level Dataset, chosen for this project, was downloaded from https://www.freddiemac.com/research/datasets/sf-loanlevel-dataset. The data on the website was organized into 99 quarterly zip files starting from 1999 Q1 and ending at 2023 Q3. Each zip archive contained an origination data file, named 'historical_data_yyymm.txt', and a time series data file, named 'historical_data_time_yyyymm.txt'; only the latter files were used in this project. Please note that access to the Freddie Mac data zone requires authentication and, since I was not able to identify a csrf token used in the login process, I had to resort to a manual download.

> ###### Screenshot of single-family quarterly loan dataset links at Freddie Mac download site
> ![](img/freddie_mac/01_StandardDataSet_screenshot_600x160.png)

Each quarterly txt file contained monthly data pertaining to the loans originated at that particular fiscal quarter, including the loan's full history until December 2023, which is the last monthly date in the dataset. For example, 'historical_data_time_2023Q3.txt' would contain all monthly data for loans originated at August, September or October 2023, starting at the year/month of origination and continuing onwards until December 2023. 

The quarterly txt files would be pipe-separated and contain no header, which was provided in a separate Excel file ('file_layout.xlsx'). This required reformatting the txt file into a dataframe, a task handled by the reformat_original_data() function in prepare_data.py. The date column, 'Monthly Reporting Period', could be sorted as type integer, so there was no need to convert it to datetime in Python. However, it had to be converted to a string for the purpose of plotting, so it could be treated as a categorical variable - otherwise we would see step-wise jumps in the temporal plots between December of current year and January of next year.

> ###### Time series loan data file historical_data_time_2023Q3.txt in the original pipe-separated format
> ![](img/freddie_mac/02_PipeSeparatedFile_screenshot.png)

> ###### The same data reformatted and saved as historical_data_time_2023Q3.parquet
> ![](img/freddie_mac/03_ReformattedFile_screenshot.png)


