
# Import Packages
from dateutil.relativedelta import relativedelta
import datetime
import TMFTA_Functions.TMFTA_Functions as TM
import pandas as pd
import numpy as np



desired_width = 320
pd.set_option('display.width', desired_width)
np.set_printoptions(linewidth=desired_width)
pd.options.display.float_format = '{:20,.2f}'.format

# Get transaction data for CIB Scenario
End_Date = datetime.date(2017, 12, 31)
Start_Date = End_Date - relativedelta(years=1) + relativedelta(days=1)

print("Start_Date is: {} and End_Date is: {}".format(Start_Date, End_Date))

# ------------------------------------------------------------------------------------------------------
# First time running this code will need to get the data fromt the database.
# DATABASE_USD_CLSTR = TM.Get_Data(Scenario = 'CLSTR', Start_Date = Start_Date, End_Date = End_Date)
# DATABASE_USD_CLSTR.to_pickle('DATABASE_USD_CLSTR')

# After the pickle has been created the program doesn't need to get the data from the database, this saves time
DATABASE_USD_CLSTR = pd.read_pickle("DATABASE_USD_CLSTR")

# ------------------------------------------------------------------------------------------------------



# --------------------------------------------------------------------------------------------------------------
# Firstly, calculate the Avg daily values which will feed into the segmentation and produce a scatterplot
# to see what the aggregated data looks like
# --------------------------------------------------------------------------------------------------------------
temp_df = DATABASE_USD_CLSTR

Cluster_Txns_Smry = (temp_df.groupby(['Qualitative_Segment', 'UnitHolderID']).agg({"Trade_Amount_USD": 'sum',
                                                                                  "TransactionNumber": 'count',
                                                                                  "DateAlloted": 'nunique'})
                            .rename(columns={"Trade_Amount_USD": "Total_Trade_Amount", "TransactionNumber": "Total_Trade_Count", "DateAlloted": "Total_Trade_Days"}))

Cluster_Txns_Smry['Avg_Daily_Trade_Amount'], Cluster_Txns_Smry['Avg_Daily_Trade_Count'] = \
    Cluster_Txns_Smry['Total_Trade_Amount'] / Cluster_Txns_Smry['Total_Trade_Days'], \
    Cluster_Txns_Smry['Total_Trade_Count'] / Cluster_Txns_Smry['Total_Trade_Days']

Cluster_Txns_Smry.to_csv('Txns_Smry_by_Qual_Seg.csv')

# Cluster_Txns_Smry = pd.read_csv('Txns_Smry_by_Qual_Seg.csv')

Qual_Seg_mean = (Cluster_Txns_Smry.groupby(['Qualitative_Segment'])['Avg_Daily_Trade_Amount', 'Avg_Daily_Trade_Count'].mean()
                                  .rename(columns={'Avg_Daily_Trade_Amount' : 'Qual_Seg_Mean_ADT_Amount', 'Avg_Daily_Trade_Count' : 'Qual_Seg_Mean_ADT_Count'}))
Qual_Seg_mean.reset_index(inplace=True)

Segmentation = pd.merge(Cluster_Txns_Smry, Qual_Seg_mean, left_on=['Qualitative_Segment'], right_on=['Qualitative_Segment'], how='left')

# --------------------------------------------------------------------------------------------------------------
# Secondly, run the segmentation assignment steps for each qualitative segment
# into a quantitative segment
# Quantitative segments are:
#   High Value, High Volume
#   High Value, Low Volume
#   Low Value, High Volume
#   Low Value, Low Volume
# High / Low assignment for value and volume is based off the mean
#
# --------------------------------------------------------------------------------------------------------------

Segmentation['Quantitative_Segment'] = Segmentation.apply(lambda row: TM.Assign_Qual_Segment(row), axis=1)
Segmentation = Segmentation[['UnitHolderID', 'Quantitative_Segment']]
Segmentation.to_csv('Segmentation.csv', index=False)
