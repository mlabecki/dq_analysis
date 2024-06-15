upb_map = {
    'fname': 'Current_Actual_UPB_201701_202312_DQ.parquet',
    'totalcount_y_max': 2e7,
    'fillcount_y_max': 2e7,
    'nullcount_y_max': 2e7,
    'fillrate_y_max': 1,
    'minvalue_y_min': 0,
    'minvalue_y_max': 10,
    'maxvalue_y_min': 0,
    'maxvalue_y_max': 2e6,
    'meanvalue_y_min': 0,
    'meanvalue_y_max': 1e6,
    'pct_y_min': 0,
    'pct_y_max': 700000
}
ltv_map = {
    'fname': 'Estimated_Loan-to-Value_ELTV_201704_202312_DQ.parquet',
    'totalcount_y_max': 2e7,
    'fillcount_y_max': 2e7,
    'nullcount_y_max': 2e7,
    'fillrate_y_max': 1,
    'minvalue_y_min': 0,
    'minvalue_y_max': 10,
    'maxvalue_y_min': 0,
    'maxvalue_y_max': 1000,
    'meanvalue_y_min': 0,
    'meanvalue_y_max': 100,
    'pct_y_min': 0,
    'pct_y_max': 105
}
exp_map = {
    'fname': 'Total_Expenses_201701_202312_DQ.parquet',
    'totalcount_y_max': 2e7,
    'fillcount_y_max': 10000,
    'nullcount_y_max': 2e7,
    'fillrate_y_max': 0.001,
    'minvalue_y_min': -1e6,
    'minvalue_y_max': 0,
    'maxvalue_y_min': 0,
    'maxvalue_y_max': 2e6,
    'meanvalue_y_min': -50000,
    'meanvalue_y_max': 0,
    'pct_y_min': -200000,    
    'pct_y_max': 100000
}
var_map = {
    'Estimated LTV': ltv_map,
    'Current Unpaid Balance': upb_map,
    'Total Expenses': exp_map
}
var_desc_map = {
    'Estimated LTV': """Estimated Loan-To_Value (ELTV) is a ratio indicating current
LTV based on the estimated current value of the property obtained
through Freddie Mac’s Automated Valuation Model (AVM). For more
information on our proprietary AVM please visit
https://sf.freddiemac.com/tools-learning/home-value-suite/home-value-explorer. 
Note: Only populated for April 2017 and following periods.""",
    'Current Unpaid Balance': """The Current Actual Unpaid Balance (UPB)
reflects the mortgage ending balance as reported by the servicer for the 
corresponding monthly reporting period. For fixed rate mortgages, this UPB 
is derived from the mortgage balance as reported by the servicer and includes 
any scheduled and unscheduled principal reductions applied to the mortgage.
For mortgages with loan modifications or payment deferrals, the current
actual unpaid principal balance could include non-interest bearing
"deferred" amounts. The Current Actual UPB will equal the sum of the
Current Interest-Bearing UPB (the amortizing principal balance of the
mortgage) and the Current Non-Interest Bearing UPB.""",
    'Total Expenses': """Total Expenses will include allowable
expenses that Freddie Mac bears in the process of acquiring, maintaining 
and/ or disposing a property (excluding selling expenses, which are subtracted
from gross sales proceeds to derive net sales proceeds). This is an
aggregation of Legal Costs, Maintenance and Preservation."""
}
grid_map = {
    'None': None,
    'X': 'x',
    'Y': 'y',
    'X and Y': 'both'
}
function_list = [
    'Fill Rate',
    'Null Count',
    'Fill Count',
    'Total Count',
    'Min Value',
    'Max Value',
    'Mean Value'
]
legend_locations = [
    'Lower Left',
    'Lower Right',
    'Upper Left',
    'Upper Right',
    'Center Left',
    'Center Right',
    'Lower Center',
    'Upper Center',
    'Center'
]