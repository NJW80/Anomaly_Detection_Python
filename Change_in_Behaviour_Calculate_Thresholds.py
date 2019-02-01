# -----------------------------------------------------------------------------------------------------------
# Author:       Nick Windridge
# Date:         Jan 2019
# Program:      Change_in_Behaviour_Calculate_Threshold
# Description:  Loops through 2016 transactions to calculate thresholds which will be used to monitor
#               2017 transactions and flag any transactions above the thresholds as being an anomaly.
#
#               There are 2 thresholds calculated for each Qualitative & Quantitative Segment combination:
#               1. Average increase
#               2. Peak increase
#
#               The threshold values are taken at the 50th, 75th, and 95th percentiles of the relevant data
#               points. The formula for each data point is: Avg or Peak transaction value in a 'Trigger Trade' time
#               period / Avg or Peak transaction value in  a 'lookback' time period. The 'Trigger Trade' and
#               'lookback' time periods are different for each metric
#
# -----------------------------------------------------------------------------------------------------------
#
#  Import Packages
from dateutil.relativedelta import relativedelta
import datetime
import TMFTA_Functions.TMFTA_Functions as TM
import pandas as pd



pd.options.display.float_format = '{:20,.2f}'.format
desired_width=320
pd.set_option('display.width', desired_width)
pd.set_option('display.max_columns',10)

# Get transaction data for CIB Scenario
End_Date = datetime.date(2016, 12, 31)
Start_Date = End_Date - relativedelta(years=1) + relativedelta(days=1)

print("Start_Date is: {} and End_Date is: {}".format(Start_Date, End_Date))
print()


# ------------------------------------------------------------------------------------------------------
# Get transactional data for scenario

# CIB_THRESHOLD_TXNS = TM.Get_Data(Scenario ='CIB', Start_Date = Start_Date, End_Date = End_Date)
# CIB_THRESHOLD_TXNS.to_pickle('CIB_THRESHOLD_TXNS')

# read pickle was used during the development of this program, left in here to demonstrate my knowledge of it alongside reading data from csv
CIB_THRESHOLD_TXNS = pd.read_pickle("CIB_THRESHOLD_TXNS")

Segmentation = pd.read_csv('Segmentation.csv')

CIB_THRESHOLD_TXNS_w_Segmentation = pd.merge(CIB_THRESHOLD_TXNS, Segmentation, left_on=['UnitHolderID'], right_on=['UnitHolderID'], how='left')

CIB_THRESHOLD_TXNS_w_Segmentation.set_index(['DateAlloted'], inplace=True)
CIB_THRESHOLD_TXNS_w_Segmentation.sort_index(inplace=True)
# ------------------------------------------------------------------------------------------------------



# Create the empty dataframes to append each days data to
__Increase_Avg_All_df = pd.DataFrame({'Qualitative_Segment' : [], 'Quantitative_Segment' : [], 'UnitHolderID' : [], 'FundID' : [], 'Trigger_Trade_Avg' : [], 'Lookback_Avg' : [], 'Increase_Avg' : []})
__Increase_Peak_All_df  = pd.DataFrame({'Qualitative_Segment' : [], 'Quantitative_Segment' : [],'UnitHolderID' : [], 'FundID' : [], 'Trigger_Trade_Peak' : [], 'Lookback_Peak' : [], 'Increase_Peak' : []})

for dt in range(0, 365 + 1):
    #Determine date range variables for Trigger Trades
    Trigger_Trade_End_Dt = End_Date - relativedelta(days=dt)
    Trigger_Trade_St_Dt = (Trigger_Trade_End_Dt - relativedelta(months=1)) + relativedelta(days=1)

    #Determine date range for Look back trades
    Lookback_End_Dt = Trigger_Trade_St_Dt - relativedelta(days=1)
    Lookback_St_Dt = (Lookback_End_Dt - relativedelta(years=1)) + relativedelta(days=1)

    # Optional print outs to the log to show which iteration of the loop is being executed
    # print()
    # print("*" * 20)
    # print("Trigger Trade Loop Date is: {}".format(End_Date - relativedelta(days=dt)))
    # print("Trigger Trade Avg Dates are: {} - {}".format(Trigger_Trade_St_Dt, Trigger_Trade_End_Dt))
    # print("Lookback Dates are: {} - {}".format(Lookback_St_Dt, Lookback_End_Dt))
    # print()

    # Select 'Trigger Trade' transactions in the relevant date range.
    # Note that Avg and Peak values have a different time period
    # Aggregate them by Qualitative and Quantitative Segments, UnitHolderID (Customer ID), FundID, Transaction Type
    __Trigger_Trade_Avg = TM.Aggregate_Txns_by_Date_Range(CIB_THRESHOLD_TXNS_w_Segmentation, Trigger_Trade_St_Dt, Trigger_Trade_End_Dt, [('Trigger_Trade_Avg', 'mean')],'thresholds')
    __Trigger_Trade_Peak = TM.Aggregate_Txns_by_Date_Range(CIB_THRESHOLD_TXNS_w_Segmentation, Trigger_Trade_End_Dt, Trigger_Trade_End_Dt, [('Trigger_Trade_Peak', 'max')],'thresholds')
    __Trigger_Trade_df = pd.merge(__Trigger_Trade_Avg, __Trigger_Trade_Peak, left_index=True, right_index=True, how='left')

    # Select 'lookback' transactions in the relevant date range, referred to as Trigger Trades.
    # Note that Avg and Peak values have a different time period
    # Aggregate them by Qualitative and Quantitative Segments, UnitHolderID (Customer ID), FundID, Transaction Type
    __Lookback_df = TM.Aggregate_Txns_by_Date_Range(CIB_THRESHOLD_TXNS_w_Segmentation, Lookback_St_Dt, Lookback_End_Dt, [('Lookback_Avg', 'mean'), ('Lookback_Peak', 'max')],'thresholds')

    # Bring the Trigger Trade and Lookback values together to calculate the data points for the threshold percentiles
    __Txn_Comparison_df = pd.merge(__Trigger_Trade_df, __Lookback_df, left_index=True, right_index=True, how='left')
    __Txn_Comparison_df['Increase_Avg'] = __Txn_Comparison_df.apply(lambda row: TM.CIB_Increase(row, 'Trigger_Trade_Avg', 'Lookback_Avg', 0),  axis=1)
    __Txn_Comparison_df['Increase_Peak'] = __Txn_Comparison_df.apply(lambda row: TM.CIB_Increase(row, 'Trigger_Trade_Peak', 'Lookback_Peak', 0),  axis=1)

    # Only keep valid (+ve) data points to be included in the percentile calculation
    __Increase_Avg_df = __Txn_Comparison_df[__Txn_Comparison_df['Increase_Avg'] > 0].drop(columns=['Trigger_Trade_Peak', 'Lookback_Peak', 'Increase_Peak']).reset_index()
    __Increase_Peak_df = __Txn_Comparison_df[__Txn_Comparison_df['Increase_Peak'] > 0].drop(columns=['Trigger_Trade_Avg', 'Lookback_Avg', 'Increase_Avg']).reset_index()

    # Append the data points for the current iteration to the overall dataframe which contains the data points for all iterations in the loop
    __Increase_Avg_All_df = __Increase_Avg_All_df.append(__Increase_Avg_df)
    __Increase_Peak_All_df = __Increase_Peak_All_df.append(__Increase_Peak_df)


# Drop duplicate data points.
# Note that a UnitHolder (Customer) could have the same transactions in multiple iterations of the above loop due to the time periods. Only 1 instance
# is to be counted when calculating the percentiles. This was more of an issue for the average values
__Increase_Avg_All_df.drop_duplicates(inplace=True)
__Increase_Peak_All_df.drop_duplicates(inplace=True)

# Calculate the 50th, 75th, 95th percentiles for each threshold by Qualitative and Quantitative segments. These will then be matched to the
# appropriate segment during the anomaly detection phase

__Increase_Avg_pctls_df = (__Increase_Avg_All_df
                                                   .groupby(['Qualitative_Segment', 'Quantitative_Segment'])['Increase_Avg'].quantile(q=[0.5, 0.75, 0.95], interpolation='lower')
                                                   .to_frame()
                                                   .reset_index()
                                                   .rename(columns={'level_2' : 'Percentile'})
                           )

__Increase_Peak_pctls_df = (__Increase_Peak_All_df
                           .groupby(['Qualitative_Segment', 'Quantitative_Segment'])['Increase_Peak'].quantile(q=[0.5, 0.75, 0.95], interpolation='lower')
                           .to_frame()
                           .reset_index()
                           .rename(columns={'level_2' : 'Percentile'})
                           )

# Output to csv file to be more easily read in during the anomaly detection phase. These will be the thresholds used in the next phase
__Increase_Avg_pctls_df.to_csv('CIB_Thresholds_Avg.csv', index=False)
__Increase_Peak_pctls_df.to_csv('CIB_Thresholds_Peak.csv', index=False)


