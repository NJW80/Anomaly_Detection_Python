# -----------------------------------------------------------------------------------------------------------
# Author:       Nick Windridge
# Date:         Jan 2019
# Program:      Change_in_Behaviour_Calculate_Anomaly_Detection
# Description:  Loops through 2017 transactions and flags any transactions above the thresholds as being an anomaly.
#
#               Thresholds are applied per  Qualitative & Quantitative Segment combination:
#               1. Average increase
#               2. Peak increase
#
#
# -----------------------------------------------------------------------------------------------------------

# Import Packages
from dateutil.relativedelta import relativedelta
import datetime
import TMFTA_Functions.TMFTA_Functions as TM
import pandas as pd

pd.options.display.float_format = '{:20,.2f}'.format
desired_width = 320
pd.set_option('display.width', desired_width)
# pd.set_option('display.max_columns',10)

# Get transaction data for CIB Scenario
End_Date = datetime.date(2017, 1, 31)
Start_Date = End_Date - relativedelta(years=1) + relativedelta(days=1)

print("Start_Date is: {} and End_Date is: {}".format(Start_Date, End_Date))
print()


# ------------------------------------------------------------------------------------------------------
# Get transactional data for scenario

CIB_ANOMALY_DETECTION_TXNS = TM.Get_Data(Scenario='CIB', Start_Date=Start_Date, End_Date=End_Date)
CIB_ANOMALY_DETECTION_TXNS = CIB_ANOMALY_DETECTION_TXNS.drop(columns=['PriceFrequency', 'MinTransactionAmount',
                                                                      'MinTransactionUnits'])

Segmentation = pd.read_csv('Segmentation.csv')

Thresholds_Avg = pd.read_csv('CIB_Thresholds_Avg.csv').set_index(['Qualitative_Segment', 'Quantitative_Segment',
                                                                  'Percentile'])
Thresholds_Avg = (Thresholds_Avg.pivot_table(index=['Qualitative_Segment', 'Quantitative_Segment'],
                                             columns='Percentile', values='Increase_Avg')
                                .rename(columns={0.50 : 'Avg_50th_Pctl', 0.75 : 'Avg_75th_Pctl', 0.95 : 'Avg_95th_Pctl'})
                 )

Thresholds_Peak = pd.read_csv('CIB_Thresholds_Peak.csv').set_index(['Qualitative_Segment', 'Quantitative_Segment',
                                                                    'Percentile'])
Thresholds_Peak = (Thresholds_Peak.pivot_table(index=['Qualitative_Segment', 'Quantitative_Segment'],
                                               columns='Percentile', values='Increase_Peak')
                                  .rename(columns={0.50 : 'Peak_50th_Pctl', 0.75 : 'Peak_75th_Pctl', 0.95 : 'Peak_95th_Pctl'})
                  )

__CIB_Thresholds = pd.merge(Thresholds_Avg, Thresholds_Peak, left_index=True, right_index=True, how='outer')
__CIB_Thresholds.reset_index(inplace=True)

CIB_ANOMALY_DETECTION_TXNS_w_Segmentation = pd.merge(CIB_ANOMALY_DETECTION_TXNS, Segmentation, left_on=['UnitHolderID'],
                                                     right_on=['UnitHolderID'], how='left')
CIB_ANOMALY_DETECTION_TXNS_w_Segmentation.set_index(['DateAlloted'], inplace=True)
CIB_ANOMALY_DETECTION_TXNS_w_Segmentation.sort_index(inplace=True)

# ------------------------------------------------------------------------------------------------------


# Create the empty dataframe to append each days data to
__Increase_Avg_All_Alerts_df = pd.DataFrame({'Qualitative_Segment' : [], 'Quantitative_Segment' : [],
                                             'Risk_Rating' : [], 'UnitHolderID' : [], 'FundID' : [],
                                             'Trigger_Trade_Avg' : [], 'Lookback_Avg' : [], 'Increase_Avg' : [],
                                             'Avg_50th_Pctl' : [], 'Avg_75th_Pctl' : [], 'Avg_95th_Pctl' : []})
__Increase_Peak_All_Alerts_df  = pd.DataFrame({'Qualitative_Segment' : [], 'Quantitative_Segment' : [],
                                               'Risk_Rating' : [], 'UnitHolderID' : [], 'FundID' : [],
                                               'Trigger_Trade_Peak' : [], 'Lookback_Peak' : [], 'Increase_Peak' : [],
                                               'Avg_50th_Pctl' : [], 'Avg_75th_Pctl' : [], 'Avg_95th_Pctl' : []})

for dt in range(0, 31 + 1):
    # Determine date range variables for Trigger Trades
    Trigger_Trade_End_Dt = End_Date - relativedelta(days=dt)
    Trigger_Trade_St_Dt = (Trigger_Trade_End_Dt - relativedelta(months=1)) + relativedelta(days=1)

    # Determine date range for Look back trades
    Lookback_End_Dt = Trigger_Trade_St_Dt - relativedelta(days=1)
    Lookback_St_Dt = (Lookback_End_Dt - relativedelta(years=1)) + relativedelta(days=1)

    # Select 'Trigger Trade' transactions in the relevant date range.
    # Note that Avg and Peak values have a different time period
    # Aggregate them by Qualitative and Quantitative Segments, UnitHolderID (Customer ID), FundID, Transaction Type
    __Trigger_Trade_Avg = TM.Aggregate_Txns_by_Date_Range(CIB_ANOMALY_DETECTION_TXNS_w_Segmentation,
                                                          Trigger_Trade_St_Dt, Trigger_Trade_End_Dt,
                                                          [('Trigger_Trade_Avg', 'mean')], 'detection')
    __Trigger_Trade_Peak = TM.Aggregate_Txns_by_Date_Range(CIB_ANOMALY_DETECTION_TXNS_w_Segmentation,
                                                           Trigger_Trade_End_Dt, Trigger_Trade_End_Dt,
                                                           [('Trigger_Trade_Peak', 'max')], 'detection')
    __Trigger_Trade_df = pd.merge(__Trigger_Trade_Avg, __Trigger_Trade_Peak, left_index=True, right_index=True, how='left')

    # Select 'lookback' transactions in the relevant date range, referred to as Trigger Trades.
    # Note that Avg and Peak values have a different time period
    # Aggregate them by Qualitative and Quantitative Segments, UnitHolderID (Customer ID), FundID, Transaction Type
    __Lookback_df = TM.Aggregate_Txns_by_Date_Range(CIB_ANOMALY_DETECTION_TXNS_w_Segmentation, Lookback_St_Dt,
                                                    Lookback_End_Dt, [('Lookback_Avg', 'mean'),
                                                                      ('Lookback_Peak', 'max')], 'detection')

    # Bring the Trigger Trade and Lookback values together
    __Txn_Comparison_df = pd.merge(__Trigger_Trade_df, __Lookback_df, left_index=True, right_index=True, how='left')

    # Calculate the data points for comparison to the threshold percentiles
    # Note if a UnitHolder (Customer) has no transactions in the 'lookback' period but has transactions in the
    # 'Trigger Trade' time period they will
    # automatically cause an alert
    __Txn_Comparison_df['Increase_Avg'] = __Txn_Comparison_df.apply(lambda row: TM.CIB_Increase(row, 'Trigger_Trade_Avg', 'Lookback_Avg', 1000000),  axis=1)
    __Txn_Comparison_df['Increase_Peak'] = __Txn_Comparison_df.apply(lambda row: TM.CIB_Increase(row, 'Trigger_Trade_Peak', 'Lookback_Peak', 1000000),  axis=1)
    __Txn_Comparison_df.reset_index(inplace=True)

    # Merge in the thresholds
    __Txn_Comparison_to_Thresholds_df = pd.merge(__Txn_Comparison_df, __CIB_Thresholds, left_on=['Qualitative_Segment', 'Quantitative_Segment'], right_on=['Qualitative_Segment', 'Quantitative_Segment'], how='left')

    # Output only the alerts based on increased AVG
    __Low_Risk_Increase_Avg_df = (__Txn_Comparison_to_Thresholds_df.query('Risk_Rating == "White List" & Increase_Avg > Avg_95th_Pctl')
                                  .drop(columns=['Trigger_Trade_Peak', 'Lookback_Peak', 'Increase_Peak']).reset_index())
    __Medium_Risk_Increase_Avg_df = (__Txn_Comparison_to_Thresholds_df.query('Risk_Rating == "Standard List" & Increase_Avg > Avg_75th_Pctl')
                                     .drop(columns=['Trigger_Trade_Peak', 'Lookback_Peak', 'Increase_Peak']).reset_index())
    __High_Risk_Increase_Avg_df = (__Txn_Comparison_to_Thresholds_df.query('Risk_Rating == "Watch List" & Increase_Avg > Avg_50th_Pctl')
                                   .drop(columns=['Trigger_Trade_Peak', 'Lookback_Peak', 'Increase_Peak']).reset_index())
    __Increase_Avg_All_Alerts_df = __Increase_Avg_All_Alerts_df.append([__Low_Risk_Increase_Avg_df, __Medium_Risk_Increase_Avg_df, __High_Risk_Increase_Avg_df])

    # Output only the alerts based on increased PEAK
    __Low_Risk_Increase_Peak_df = (__Txn_Comparison_to_Thresholds_df.query('Risk_Rating == "White List" & Increase_Peak > Peak_95th_Pctl')
                                  .drop(columns=['Trigger_Trade_Avg', 'Lookback_Avg', 'Increase_Avg']).reset_index())
    __Medium_Risk_Increase_Peak_df = (__Txn_Comparison_to_Thresholds_df.query('Risk_Rating == "Standard List" & Increase_Peak > Peak_75th_Pctl')
                                     .drop(columns=['Trigger_Trade_Avg', 'Lookback_Avg', 'Increase_Avg']).reset_index())
    __High_Risk_Increase_Peak_df = (__Txn_Comparison_to_Thresholds_df.query('Risk_Rating == "Watch List" & Increase_Peak > Peak_50th_Pctl')
                                   .drop(columns=['Trigger_Trade_Avg', 'Lookback_Avg', 'Increase_Avg']).reset_index())
    __Increase_Peak_All_Alerts_df = __Increase_Peak_All_Alerts_df.append([__Low_Risk_Increase_Peak_df, __Medium_Risk_Increase_Peak_df, __High_Risk_Increase_Peak_df])

# Remove any duplicate alerts as they don't need to be re-investigated
__Increase_Avg_All_Alerts_df.drop_duplicates(inplace=True)
__Increase_Peak_All_Alerts_df.drop_duplicates(inplace=True)

# Finally, output the alerts to be investigated
__Increase_Avg_All_Alerts_df.to_csv('CIB_Increase_Avg_Anomalies.csv', index=False)
__Increase_Peak_All_Alerts_df.to_csv('CIB_Increase_Peak_Anomalies.csv', index=False)