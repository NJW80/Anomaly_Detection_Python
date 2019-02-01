
import mysql.connector
from mysql.connector import Error
import pandas as pd
from pandas import DataFrame as df
from dateutil.relativedelta import relativedelta



def Get_Data_MySQL_to_DF(query):
    """ Connect to MySQL database """
    try:
        conn = mysql.connector.connect(host='localhost', database='', user='', password='')
        if conn.is_connected():
            print('Connected to MySQL database')

        df = pd.read_sql(query, con=conn)
        return df

    except Error as e:
        print(e)

    finally:
        conn.close()
        print("Connection to MySQL Database closed")


def Update_Txn_from_DF(df):
    """ Connect to MySQL database """
    try:
        updt_conn = mysql.connector.connect(host='localhost', database='', user='', password='')

        for row in df[df['SettlementAmtInTxnCcy'] > 0].iterrows():
            Update_Txn_Query = "UPDATE consolidatedtxntbl SET SettlementAmtInTxnCcy = {} WHERE TransactionNumber = '{}'".format(row[1]['SettlementAmtInTxnCcy'], row[1]['TransactionNumber'])
            updt_conn.executemany(Update_Txn_Query)
            # print(Update_Txn_Query)

        for row in df[df['UnitsConfirmed'] > 0].iterrows():
            Update_Txn_Query = "UPDATE consolidatedtxntbl SET SettlementAmtInTxnCcy = {} WHERE TransactionNumber = '{}'".format(row[1]['SettlementAmtInTxnCcy'], row[1]['TransactionNumber'])
            updt_conn.executemany(Update_Txn_Query)

        for row in df[df['UnitsAlloted'] > 0].iterrows():
            Update_Txn_Query = "UPDATE consolidatedtxntbl SET SettlementAmtInTxnCcy = {} WHERE TransactionNumber = '{}'".format(row[1]['SettlementAmtInTxnCcy'], row[1]['TransactionNumber'])
            updt_conn.executemany(Update_Txn_Query)

    except Error as e:
        print(e)

    finally:
        updt_conn.close()
        print("Connection to MySQL Database closed")


def Get_FX_Data(End_Date):

    Get_FX_Query = "SELECT RefCurrency, EffectiveDate, BuyRate " \
                   "FROM exchangeratetbl " \
                   "WHERE EffectiveDate <= '{0}' " \
                   "and upper(PairCurrency) = 'USD' " \
                   "and BuyRate > 0 " \
                   "ORDER BY RefCurrency, EffectiveDate".format(End_Date)

    return Get_Data_MySQL_to_DF(Get_FX_Query)


def Get_NAV_Data(End_Date):

    Get_NAV_Query = "SELECT FundID, EffectiveDate, DeclaredNAV " \
                    "FROM fundpricehdrtbl " \
                    "WHERE EffectiveDate <= '{0}' " \
                    "and DeclaredNAV > 0 " \
                    "ORDER BY FundID, EffectiveDate".format(End_Date)

    return Get_Data_MySQL_to_DF(Get_NAV_Query)


def Get_FundDemographics_Data():

    Get_FundDemographics_Query = "SELECT FundID, FundBaseCurrency " \
                                 "FROM funddemographicstbl " \
                                 "WHERE LatestRule = 1 " \
                                 "ORDER BY FundID"

    return Get_Data_MySQL_to_DF(Get_FundDemographics_Query)


def Get_UH_Data(Risk_Rating_Column):

    Get_UH_Query = "SELECT a.UnitHolderID, case when a.Qualitative_Segment <> 'Individual' then 'Corporate' else 'Individual' end as Qualitative_Segment, b.{} as Risk_Rating " \
                   "FROM unitholdertbl a " \
                   "LEFT JOIN uhadditionalinfotbl b on a.UnitHolderID = b.UnitHolderID " \
                   "ORDER BY a.UnitHolderID".format(Risk_Rating_Column)

    return Get_Data_MySQL_to_DF(Get_UH_Query)



def Get_Fund_Txn_Data():

    Get_Dealing_Cycle_Query = "SELECT a.FundID, a.TransactionType, a.PriceFrequency " \
                              "FROM specificfundpricesetuptbl a " \
                              "INNER JOIN (SELECT FundID, TransactionType, max(RuleEffectiveDate) as Latest_RuleEffectiveDate " \
                              "            FROM specificfundpricesetuptbl " \
                              "            GROUP BY FundID, TransactionType) b " \
                              "ON a.FundID = b.FundID and a.TransactionType = b.TransactionType and a.RuleEffectiveDate = b.Latest_RuleEffectiveDate " \
                              "ORDER BY a.FundID, b.TransactionType"

    Get_Fund_Min_Query = "SELECT a.FundID, a.TransactionType, a.MinTransactionAmount, a.MinTransactionUnits " \
                         "FROM txnprocessingrulestbl a " \
                         "INNER JOIN (SELECT FundID, TransactionType, max(RuleEffectiveDate) as Latest_RuleEffectiveDate " \
                         "            FROM txnprocessingrulestbl " \
                         "            GROUP BY FundID, TransactionType) b " \
                         "ON a.FundID = b.FundID and a.TransactionType = b.TransactionType and a.RuleEffectiveDate = b.Latest_RuleEffectiveDate " \
                         "ORDER BY a.FundID, b.TransactionType"

    # print(Get_Fund_Txn_Query)
    Fund_Dealing_Cycle_Data = Get_Data_MySQL_to_DF(Get_Dealing_Cycle_Query)
    Fund_Min_Txn_Data = Get_Data_MySQL_to_DF(Get_Fund_Min_Query)
    Fund_Txn_Data = pd.merge(Fund_Dealing_Cycle_Data, Fund_Min_Txn_Data, left_on=['FundID', 'TransactionType'], right_on=['FundID', 'TransactionType'], how='outer')

    return Fund_Txn_Data


def Get_Data(Scenario, Start_Date, End_Date):

    # ----------------------------------------------------------------------------------------
    # --------------------------------- Get Transaction Data ---------------------------------
    # ----------------------------------------------------------------------------------------

    __Transactions = Get_Transaction_Data(Scenario, Start_Date, End_Date )
    __Transactions['DateAlloted'] = pd.to_datetime(__Transactions['DateAlloted'])


    # ---------------------------------------------------------------------------------------
    # ------------------------------------- Get UH Data -------------------------------------
    # ---------------------------------------------------------------------------------------

    # Get raw data from MySQL table
    __UH = Get_UH_Data('AdditionalInfo29')
    __UH['Risk_Rating'].fillna('Standard List', inplace=True)


    # ------------------------------------------------------------------------------------------------------
    # ------------------------------------- Get Fund Demographics Data -------------------------------------
    # ------------------------------------------------------------------------------------------------------

    # Get raw data from MySQL table
    __FundDemographics = Get_FundDemographics_Data()


    # ------------------------------------------------------------------------------------------------------
    # ------------------------------------- Get Fund, Transaction Data -------------------------------------
    # ------------------------------------------------------------------------------------------------------

    # Get raw data from MySQL table
    __Fund_Txn = Get_Fund_Txn_Data()


    # ---------------------------------------------------------------------------------------
    # ------------------------------------- Get NAV Data -------------------------------------
    # ---------------------------------------------------------------------------------------

    # Get raw data from MySQL table
    __Initial_NAV = Get_NAV_Data(End_Date)
    __Initial_NAV['EffectiveDate'] = pd.to_datetime(__Initial_NAV['EffectiveDate'])

    # Fill in the missing dates for each FundID
    __NAV = (__Initial_NAV.groupby(['FundID']).resample('D', on='EffectiveDate').mean())

    # Fill forward any missing DeclaredNAV
    __NAV['DeclaredNAV'].fillna(method='ffill', inplace=True)
    __NAV.reset_index( inplace = True)


    # ---------------------------------------------------------------------------------------
    # ------------------------------------- Get FX Data -------------------------------------
    # ---------------------------------------------------------------------------------------

    # Get raw data from MySQL table
    __Initial_ExchangeRate = Get_FX_Data(End_Date)
    __Initial_ExchangeRate['EffectiveDate'] = pd.to_datetime(__Initial_ExchangeRate['EffectiveDate'])

    # Fill in the missing dates for each currency
    __ExchangeRate_USD = df([['USD', 1, End_Date - relativedelta(days=x)] for x in range(0, (365 * 2) + 1)], columns=['RefCurrency', 'BuyRate', 'EffectiveDate'])
    __ExchangeRate_USD['EffectiveDate'] = pd.to_datetime(__ExchangeRate_USD['EffectiveDate'])
    __Initial_ExchangeRate = __Initial_ExchangeRate.append(__ExchangeRate_USD)
    __ExchangeRate = (__Initial_ExchangeRate.groupby(['RefCurrency']).resample('D', on='EffectiveDate').mean())

    # Fill forward any missing BuyRates
    __ExchangeRate['BuyRate'].fillna(method='ffill', inplace=True)
    __ExchangeRate.reset_index(inplace = True)

    # ------------------------------------------------------------------------------------------------------
    # ------------------------------------------------------------------------------------------------------



    # ------------------------------------------------------------------------------------------------------
    # ------------------------------------- Pull all the data together -------------------------------------
    # ------------------------------------------------------------------------------------------------------

    # -- Part 1: Add in Qualitative Segments data to Txn Data
    __Txns_UH = pd.merge(__Transactions, __UH, left_on=['UnitHolderID'], right_on=['UnitHolderID'], how='left')

    # ------------------------------------------------------------------------------------------------------

    # -- Part 2: Add in Fund Base Currency data to Txn Data
    # __Txns_UH_FBC = pd.merge(__Transactions, __FundDemographics, left_on=['FundID'], right_on=['FundID'], how='left')

    __Txns_UH_FBC = pd.merge(__Txns_UH, __FundDemographics, left_on=['FundID'], right_on=['FundID'], how='left')

    # ------------------------------------------------------------------------------------------------------

    # -- Part 3: Add in Fund & TransactionType data (Dealing Cycle, Min Inv Amounts) to Txn Data
    __Txns_UH_FBC_DC_MINS = pd.merge(__Txns_UH_FBC, __Fund_Txn, left_on=['FundID', 'TransactionType'], right_on=['FundID', 'TransactionType'], how='left')

    # ------------------------------------------------------------------------------------------------------

    # -- Part 4: Add in NAV data to Txn Data
    __Txns_UH_FBC_DC_MINS_NAV = pd.merge(__Txns_UH_FBC_DC_MINS, __NAV, left_on=['FundID', 'DateAlloted'], right_on=['FundID', 'EffectiveDate'], how='left')
    __Txns_UH_FBC_DC_MINS_NAV.drop(columns=['EffectiveDate'], inplace=True)

    # ------------------------------------------------------------------------------------------------------

    # -- Part 5: Add in FX data for Txn Currency to Txn Data
    __Txns_UH_FBC_DC_MINS_NAV_TBC_FX = pd.merge(__Txns_UH_FBC_DC_MINS_NAV, __ExchangeRate, left_on=['TransactionCurrency', 'DateAlloted'], right_on=['RefCurrency', 'EffectiveDate'], how='left')
    __Txns_UH_FBC_DC_MINS_NAV_TBC_FX['TBC_ExchangeRate'] = __Txns_UH_FBC_DC_MINS_NAV_TBC_FX.apply(lambda row: Set_TBC_ExchangeRate(row), axis=1 )
    __Txns_UH_FBC_DC_MINS_NAV_TBC_FX.drop(columns=['RefCurrency', 'EffectiveDate', 'BuyRate'], inplace=True)

    # ------------------------------------------------------------------------------------------------------


    # -- Part 6: Add in FX data for FB Currency to Txn Data
    __DATABASE_USD = pd.merge(__Txns_UH_FBC_DC_MINS_NAV_TBC_FX, __ExchangeRate, left_on=['FundBaseCurrency', 'DateAlloted'], right_on=['RefCurrency', 'EffectiveDate'], how='left')
    __DATABASE_USD['FBC_ExchangeRate'] = __DATABASE_USD.apply(lambda row: Set_FBC_ExchangeRate(row), axis=1)
    __DATABASE_USD['Units'] = __DATABASE_USD.apply(lambda row: Set_Units(row), axis=1)
    __DATABASE_USD.drop(columns=['UnitsConfirmed', 'UnitsAlloted', 'RefCurrency', 'EffectiveDate', 'BuyRate'], inplace=True)

    # ------------------------------------------------------------------------------------------------------


    # -- Part 7: Calculate the Trade Amount in USD for all transactions
    __DATABASE_USD['Trade_Amount_USD'] = __DATABASE_USD.apply(lambda row: Set_Amount_USD(row), axis=1)
    __DATABASE_USD.drop(columns=['Units', 'TBC_ExchangeRate', 'FBC_ExchangeRate', 'DeclaredNAV', 'TransactionCurrency', 'FundBaseCurrency', 'SettlementAmtInTxnCcy'], inplace=True)

    # ------------------------------------------------------------------------------------------------------

    return __DATABASE_USD


def Get_Transaction_Data(Scenario, Start_Date, End_Date):

    DateAllotedWhere = "DateAlloted between '{}' and '{}'".format(Start_Date, End_Date)

    if Scenario == "CIB" :
        TransactionTypeWhere = "( (TransactionType = '02' and (TransactionSubType is null or TransactionSubType not in ('1', '2', '3')) )" \
                               "or (TransactionType = '03' and (TransactionSubType is null or TransactionSubType not in ('4', '5', '6')) ) )"
    else:
        TransactionTypeWhere = "( (TransactionType = '02' and (TransactionSubType is null or TransactionSubType not in ('1', '2', '3')) )" \
                               "or (TransactionType = '03' and (TransactionSubType is null or TransactionSubType not in ('4', '5', '6')) )" \
                               "or (TransactionType = '05' and (TransactionSubType is null or TransactionSubType not in ('9')) ) )"

    RemarksWhere = "(Remarks is null or (" \
                   "upper(Remarks) NOT LIKE '%DIVIDEND%' " \
                   "and upper(Remarks) NOT LIKE '%MIGRAT%' " \
                   "and upper(Remarks) NOT LIKE '%SIDEPOCKET%' " \
                   "and Remarks NOT LIKE '%SP%' ))"

    AllotedFlagWhere = "AllotedFlag = 'Y'"

    RefTypeWhere = "(RefType is null or RefType not in ('RE', '41') )"

    Get_Txn_Query = "SELECT TransactionNumber, UnitHolderID, FundID, DateAlloted, TransactionType, SettlementAmtInTxnCcy, TransactionCurrency, UnitsConfirmed, UnitsAlloted " \
                    "FROM consolidatedtxntbl " \
                    "WHERE {0} and {1} and {2} and {3} and {4}".format(DateAllotedWhere, TransactionTypeWhere, RemarksWhere, AllotedFlagWhere, RefTypeWhere )

    return Get_Data_MySQL_to_DF(Get_Txn_Query)


def Aggregate_Txns_by_Date_Range(in_df, start_date, end_date, agg_list, phase):
    try:
        df = in_df.ix[start_date: end_date]

    except KeyError as ke:
        print("No Transactions for that Date")
        # Returns an empty dataframe to allow the loop to continue executing
        if phase == 'thresholds':
            df = pd.DataFrame({'Qualitative_Segment' : [], 'Quantitative_Segment' : [], 'UnitHolderID' : [], 'FundID' : [], 'Trade_Amount_USD' : []})
        elif phase == 'detection':
            df = pd.DataFrame({'Qualitative_Segment' : [], 'Quantitative_Segment' : [], 'Risk_Rating' : [], 'UnitHolderID' : [], 'FundID' : [], 'Trade_Amount_USD' : []})

    except Error as e:
        print("No Transactions for that Date Range")
        # Returns an empty dataframe to allow the loop to continue executing
        if phase == 'thresholds':
            df = pd.DataFrame({'Qualitative_Segment' : [], 'Quantitative_Segment' : [], 'UnitHolderID' : [], 'FundID' : [], 'Trade_Amount_USD' : []})
        elif phase == 'detection':
            df = pd.DataFrame({'Qualitative_Segment' : [], 'Quantitative_Segment' : [], 'Risk_Rating' : [], 'UnitHolderID' : [], 'FundID' : [], 'Trade_Amount_USD' : []})

    finally:
        if phase == 'thresholds':
            out_df = (df.groupby(['Qualitative_Segment', 'Quantitative_Segment', 'UnitHolderID', 'FundID'])['Trade_Amount_USD'].agg(agg_list))
        elif phase == 'detection':
            out_df = (df.groupby(['Qualitative_Segment', 'Quantitative_Segment', 'Risk_Rating', 'UnitHolderID', 'FundID'])['Trade_Amount_USD'].agg(agg_list))
        else:
            out_df  = pd.DataFrame({'Qualitative_Segment' : [], 'Quantitative_Segment' : [],  'Risk_Rating' : [], 'UnitHolderID' : [], 'FundID' : [], 'Trade_Amount_USD' : []})

        return out_df




# ---------------------------------------------------------------------------------------
# ---------------------------------- Lambda Functions------------------------------------
# ---------------------------------------------------------------------------------------
def Set_FBC_ExchangeRate(row):
    if row['TransactionType'] == '05':
        return row['BuyRate']
    else:
        return 0


def Set_TBC_ExchangeRate(row):
    if row['TransactionType'] != '05':
        return row['BuyRate']
    else:
        return 0


def Set_Units(row):
    if row['UnitsConfirmed'] is not None and row['UnitsConfirmed'] > 0:
        row['Units'] = row['UnitsConfirmed']
    elif row['UnitsAlloted'] is not None and row['UnitsAlloted'] > 0:
        row['Units'] = row['UnitsAlloted']
    else:
        row['Units'] = 0

    return row['Units']


def Set_Transfer_Amount_USD(row):

    row['Transfer_Amount_USD'] = row['Units'] * row['DeclaredNAV'] * row['ExchangeRate']
    return row['Transfer_Amount_USD']


def Set_Amount_USD(row):
    if row['TransactionType'] == '05':
        return row['Units'] * row['DeclaredNAV'] * row['FBC_ExchangeRate']
    else:
        return row['SettlementAmtInTxnCcy'] * row['TBC_ExchangeRate']


def Assign_Qual_Segment(row):
    if row['Avg_Daily_Trade_Amount'] >= row['Qual_Seg_Mean_ADT_Amount']:
        Value_Seg = 'High Value '
    else:
        Value_Seg =  'Low Value '

    if row['Avg_Daily_Trade_Count'] >= row['Qual_Seg_Mean_ADT_Count']:
        Volume_Seg = 'High Volume'
    else:
        Volume_Seg = 'Low Volume'

    return Value_Seg + Volume_Seg


def CIB_Increase(row, numerator, denominator, DefaultNoDenominator):
    if (pd.isna(row[numerator]) == False and row[numerator] > 0) and (pd.isna(row[denominator]) == True or row[denominator] == 0):
        res = DefaultNoDenominator
    elif (pd.isna(row[numerator]) == True or row[numerator] == 0):
        res = 0
    elif row[numerator] > row[denominator]:
        res = row[numerator] / row[denominator]
    else:
        res = 0

    return res
