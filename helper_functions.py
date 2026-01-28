import os
import numpy as np
import pandas as pd
from sqlalchemy import create_engine
import datetime as dt

def db_connection(server, database):
    """Creates a database connection to SQL Server."""
    try:
        engine = create_engine(f'mssql+pyodbc://{server}/{database}?driver=ODBC+Driver+17+for+SQL+Server')
        return engine
    except Exception as e:
        print("Error establishing database connection:", e)
        return None
    
henrysconnection = db_connection('localhost\HENRYSSERVER','PropertyCashflows')

def update_swap_rates():
    """We take discount margins from the 10y AUD/JPY swap rates."""
    """Pull the most recent 10y AUD/JPY swap rates from ENA/DataRaw"""
    enaconnection = db_connection("EASQLDEV","ENA")
    swap_rate_query = """SELECT [DATE],[IDENTIFIER],[YIELD]
        FROM  [ENA].[AssetAllocation].[DataRaw]
        where [IDENTIFIER] in ('ADSWAP10 Curncy','JYSO10 BGN Curncy')
        and [DATE] > '2020-01-01'
        """
    swap_rate_table = pd.read_sql(swap_rate_query,con = enaconnection)
    swap_rate_table.to_sql('SwapRates',henrysconnection,index=False,if_exists='replace')
    return None

def update_detailed_swap_rates():
    """Detailed swap rates for constructing a term structure"""
    lifesqlconnection = db_connection('LIFESQL','Rates')

    detailed_swap_query = """
    SELECT [Date],[Mnemonic],[Open],[Last], 0.5*([Open]+[Last]) as [Mean], [BaseCCY],[IST_Code]
  FROM [Rates].[dbo].[vw_rates]
  where (Mnemonic like '%AUDSwap%'
  or Mnemonic like '%AUDBill%'
  or Mnemonic like '%JPY_OIS%') AND
  ([Date] > '2025-01-01')
  order by [Date] asc"""
    
    detailed_swap_rates = pd.read_sql(detailed_swap_query,lifesqlconnection)

    detailed_swap_rates.to_sql('SwapRatesDetailed',
                               con=henrysconnection,
                               index=False,
                               if_exists='replace')
    
    return None

def comma_remover(string_to_convert):
    if type(string_to_convert) is str:
        converted_string = ''.join(string_to_convert.split(','))
    else:
        converted_string = string_to_convert
    return converted_string

def upload_raw_mri_files(filepath,effective_date=None):

    henrysconnection = db_connection('localhost\HENRYSSERVER','PropertyCashflows')
    today = dt.datetime.now()
    if effective_date == None:
        effective_dates = [dt.date(2024+year,month,1) for year in range(5) for month in (1,7)]
        effective_date = max([date for date in effective_dates if date < today])

    os.chdir(filepath)
    file_sets = os.listdir()
    print(file_sets)

    file_dict = {}
    for file in file_sets:
        os.chdir(filepath + "  \  ".strip() + file)
        file_dict[file] = dict()
        subfiles = os.listdir()
        for subfile in subfiles:
            subfile_df = pd.read_csv(subfile)
            file_dict[file][subfile] = subfile_df
        
    for _, subfile_dict in file_dict.items():
        for df in subfile_dict.values():
            df['EffectiveDate'] = effective_date
            df['EffectiveDate'] = pd.to_datetime(df['EffectiveDate'])
            
    column_list = []
    for key,val in file_dict['Japan'].items():
        for column in val.columns:
            if column not in column_list:
                column_list.append(column)
                
    column_dict = {c:str for c in column_list}
    column_dict["PropertyIsPrimary"] = float
    column_dict["CashflowEffectiveDate"] = dt.datetime
    column_dict['ModelEffectiveDate'] = dt.datetime
    column_dict['Amount'] = float
    column_dict['ExtractedDateTune'] = dt.datetime
    column_dict['OwnershipPercentage'] = float
    column_dict['EffectiveDate'] = dt.datetime
    column_dict['ModelVersionEffectiveDate'] = dt.datetime
    column_dict['NetLetteableArea'] = float
    column_dict['WeightedAverageLeaseExpiryByArea'] = float
    column_dict['WeightedAverageLeaseExpiryByValue'] = float
    column_dict['OccupancyByAreaSqm'] = float
    column_dict['OccupancyByValueQC'] = float
    column_dict['OccupancyByArea'] = float
    column_dict['OccupancyByValue'] = float
    column_dict['AcquisitionDate'] = dt.datetime
    column_dict['CashFlowDate'] = dt.datetime
    column_dict['NetLettableSqm'] = float
    column_dict['ReviewDate'] = dt.datetime
    column_dict['LeaseBegin'] = dt.datetime
    column_dict['LeaseEnd'] = dt.datetime
    column_dict['AdoptedValuation'] = float
    column_dict['ExternalValuation'] = float
    column_dict['CapRateValuation'] = float
    column_dict['DCFValuation'] = float
    column_dict['InternalDiscountRate'] = float
    column_dict['ExternalDiscountRate'] = float
    column_dict['CapRate'] = float
    column_dict['TerminalCapRate'] = float

    new_file_dict = {'Australia':dict(),"Japan":dict()}
    for country in ['Australia','Japan']:
        for k,v in file_dict[country].items():
            v = v.dropna(how='all')
            for column in v.columns:
                if column_dict[column] is dt.datetime:
                    v[column] = pd.to_datetime(v[column])
                else:
                    try:
                        v[column] = v[column].astype(column_dict[column])
                    except:
                        v[column] = v[column].map(comma_remover)
                        v[column] = v[column].astype(column_dict[column])
            new_file_dict[country][k] = v.copy()

    effective_date_string = f"{effective_date.year}-{effective_date.month}-{effective_date.day}"
    # print(effective_date_string)
    ccy_dict = {"Japan":'JPY',"Australia":"AUD"}
    for country,ccy in ccy_dict.items():
        for f in new_file_dict[country].keys():
            to_upload = new_file_dict[country][f].copy()
            if "Currency" not in to_upload.columns:
                to_upload["Currency"] = ccy_dict[country]
            current_existing_data_query = f"""
            SELECT * FROM PropertyCashflows.dbo.{f.split('.')[0]}
            Where [EffectiveDate] != '{effective_date_string}'
            or [Currency] != '{ccy}'
            """
            try:
                existing_data = pd.read_sql(current_existing_data_query,con=henrysconnection)
                to_upload = pd.concat([to_upload,existing_data])
            except:
                pass
            print(f)
            print(country)

            to_upload.to_sql(name=f.split('.')[0],
                                        con=henrysconnection,
                                        if_exists='replace',
                                        index=False)
    
    return None

def upload_metrics_file(filepath,add_on=False):
    if add_on:
        replacementQ = 'append'
    else:
        replacementQ = 'replace'
    henrysconnection = db_connection('localhost\HENRYSSERVER','PropertyCashflows')
    os.chdir(filepath)

    metrics_file_data = dict()
    metrics_file_names = os.listdir()
    for file_name in metrics_file_names:
        file_name_date = file_name.split(' ')[0]
        metrics_file = pd.read_excel(file_name)
        metrics_file['MetricsDate'] = file_name_date
        metrics_file_data[file_name_date] = metrics_file
        
    global_metrics_file = pd.concat(metrics_file_data.values())

    def fix_expiry_years(date):
        if date < dt.datetime.now():
            new_date = date + dt.timedelta(days=36525)
        else:
            new_date = date
        return new_date
    global_metrics_file['Expiry FY'] = pd.to_datetime(global_metrics_file['Expiry FY'],errors='coerce')
    global_metrics_file['Expiry FY'] = global_metrics_file['Expiry FY'].map(fix_expiry_years)
    global_metrics_file.to_sql(name='MetricsFile',
                               con=henrysconnection,
                               if_exists=replacementQ,
                               index=False)
    return None


def upload_metrics_summary_file(filepath,add_on=False):
    #Uploads the metrics summary page from the Metrics file (non-MRI)
    #Includes valuer-provided vals, cap rates and discount rates.
    henrysconnection = db_connection('localhost\HENRYSSERVER','PropertyCashflows')
    os.chdir(filepath)

    #Anything to add?
    current_valuation_query = """SELECT DISTINCT [Valuation Date]
    from PropertyCashflows.dbo.PropertyMetricsSummaryNonMRI"""

    current_valuation_dates = pd.read_sql(current_valuation_query,
                                          con=henrysconnection)

    mfs = pd.read_csv('PortfolioMetricsSummary.csv',thousands = ',')
    mf_column_changer_dict = {c: c.split(' (')[0] for c in mfs.columns}

    mf_column_changer_dict['Current Valuation ($m)'] = 'Current Valuation AUD'
    mf_column_changer_dict['Current Valuation (€m/¥m)'] = 'Current Valuation QC'
    mf_column_changer_dict['Prior Valuation ($m)'] = 'Prior Valuation AUD'
    mf_column_changer_dict['Prior Valuation (€m/¥m)'] = 'Prior Valuation QC'

    mf_column_changer_dict['WALE (years) by Income'] = 'WALE by Income'
    mf_column_changer_dict['WALE (years) by Area'] = 'WALE by Area'

    mf_column_changer_dict['Valuation Change ($m)'] = 'Valuation Change AUD'
    mf_column_changer_dict['Valuation Change (%)'] = 'Valuation Change PCT'


    mf_column_changer_dict['Occupancy (%) by Income'] = 'Occupancy by Income'
    mf_column_changer_dict['Occupancy (%) by Area'] = 'Occupancy by Area'
    mf_column_changer_dict['Discount Rate           '] = 'Discount Rate' 

    mf_column_type_dict = {c:str for c in mfs.columns}
    mf_column_type_dict['Valuation Date'] = dt.datetime
    for column_name in mf_column_type_dict.keys():
        if '(' in column_name or '%' in column_name:
            mf_column_type_dict[column_name] = float

    for column,dtype in mf_column_type_dict.items():
        if dtype == dt.datetime:
            mfs[column] = pd.to_datetime(mfs[column])
        elif dtype == float:
            mfs[column] = pd.to_numeric(mfs[column],errors='coerce')
        else:
            mfs[column] = mfs[column].astype(dtype)
        
    mf_column_changer_dict = {k:v.strip() for k,v in mf_column_changer_dict.items()}
            
    mfs = mfs.rename(mf_column_changer_dict,axis=1)

    valuation_dates_to_add = [
        c for c in mfs[
            'Valuation Date'].unique() if c not in current_valuation_dates[
            'Valuation Date'].unique()]
    print(f"Valuation Dates to Add: {valuation_dates_to_add if add_on else mfs['Valuation Date'].unique()}") 

    if add_on:
        mfs = mfs[mfs["Valuation Date"].isin(valuation_dates_to_add)]
    replacementQ = 'append' if add_on else 'replace'

    mfs.to_sql(name='PropertyMetricsSummaryNonMRI',
               con=henrysconnection,
               if_exists= replacementQ,
               index=False
               )
    return None

def construct_consolidated_metrics(replace=False):
    nonmri_metrics_query = """
    Select * 
    from PropertyCashflows.dbo.PropertyMetricsSummaryNonMRI 
    where [Valuation Date] in (select MAX([Valuation Date]) from PropertyCashflows.dbo.PropertyMetricsSummaryNonMRI)
    """
    non_mri_metrics = pd.read_sql(nonmri_metrics_query,con=henrysconnection)

    mri_metrics_query = """
    Select *
    from PropertyCashflows.dbo.PropertyMetricsSummary
    where [EffectiveDate] in (select MAX([EffectiveDate]) from PropertyCashflows.dbo.PropertyMetricsSummary)
    """
    mri_metrics = pd.read_sql(mri_metrics_query,con=henrysconnection)
    effective_date = mri_metrics['EffectiveDate'].unique()[0]

    mapping_table_query = """
    Select * 
    from PropertyCashflows.dbo.PropertyNameMapper"""

    name_mapper = pd.read_sql(mapping_table_query,con=henrysconnection)
    mri_metrics_column_namer = {c:c for c in mri_metrics.columns if c != 'index'}
    mri_metrics_column_namer['NetLettableArea'] = 'Net Lettable Area'
    mri_metrics_column_namer['WeightedAverageLeaseExpiryByArea'] = 'WALE by Area'
    mri_metrics_column_namer['WeightedAverageLeaseExpiryByValue'] = 'WALE by Income'

    mri_metrics_column_namer['NetLettableArea'] = 'Net Lettable Area'


    mri_metrics_column_namer['DiscountRate'] = 'Discount Rate'
    mri_metrics_column_namer['CapRate'] = 'Current Market Cap Rate'
    mri_metrics_column_namer['CLCOwnership'] = 'CLC Ownership Interest'
    mri_metrics_column_namer['Region'] = 'Region'
    mri_metrics_column_namer['Location'] = 'Location'
    mri_metrics_column_namer['Sector'] = 'Sector'

    name_dict = dict(name_mapper[['MRIPropertyCode','MetricsPropertyName']].groupby('MRIPropertyCode')['MetricsPropertyName'].min())
    name_dict = {v:k for k,v in name_dict.items()} 

    if 'PropertyCode' not in non_mri_metrics.columns:
        non_mri_metrics['PropertyCode'] = non_mri_metrics['Asset'].map(name_dict)
        

    nonmri_consol = non_mri_metrics[['Asset']+[c for c in mri_metrics_column_namer.values() if c in non_mri_metrics.columns]]

    mri_consol = mri_metrics[[c for c in mri_metrics_column_namer.values() if c in mri_metrics.columns]]

    metrics_consolidated = pd.merge(mri_consol,nonmri_consol,on='PropertyCode')

    if replace:
        rest_of_consolidated_metrics_query = f"""Select * 
        from PropertyCashflows.dbo.PropertyMetricsConsolidated
        where EffectiveDate != '{effective_date}'
        """
        rest_of_consolidated_metrics = pd.read_sql(rest_of_consolidated_metrics_query,con=henrysconnection)
        metrics_consolidated = pd.concat([metrics_consolidated,rest_of_consolidated_metrics])
        metrics_consolidated.to_sql(name='PropertyMetricsConsolidated',schema='dbo',con=henrysconnection,
                                    if_exists='replace',index=False)
    else:
        metrics_consolidated.to_sql(name='PropertyMetricsConsolidated',schema='dbo',con=henrysconnection,
                                    if_exists='append',index=False)
    return None


def generate_contracted_cashflows(AsAtDate):
    assert type(AsAtDate) is str
    AsAtDateList = AsAtDate.split('-')
    AsAtDateDict = {"Year":AsAtDateList[0],"Month":AsAtDateList[1],"Day":AsAtDateList[2]}

    version_name_query = f"""SELECT MAX(EffectiveDate) FROM PropertyCashflows.dbo.TenancyCashflow
    WHERE EffectiveDate < '{AsAtDate}' """
    version = str(pd.read_sql(version_name_query,con=henrysconnection).iloc[0].values[0])[:10]

    risk_free_rate_query = f"""SELECT *
    from PropertyCashflows.dbo.SwapRates
    where DATE < '{version}'
    """

    rfr = pd.read_sql(risk_free_rate_query,con=henrysconnection)

    rfr_dict = {"AUS":None,"JAP":None}

    for identifier in ['ADSWAP10 Curncy','JYSO10 BGN Curncy']:
        max_date = rfr[rfr['IDENTIFIER'] == identifier]['DATE'].max()
        ccy = "AUS" if identifier[0]=='A' else "JAP"
        rfr_dict[ccy] = list(rfr[(rfr["DATE"]==max_date)&(rfr["IDENTIFIER"]==identifier)]["YIELD"].items())[0][1]

    tcf_query = f"""SELECT *
    from PropertyCashflows.dbo.TenancyCashflow
    WHERE EffectiveDate = '{version}'
    """
    tcf = pd.read_sql(tcf_query,con=henrysconnection)

    tcf_obj_columns = tcf.select_dtypes('object').columns
    tcf[tcf_obj_columns] = tcf[tcf_obj_columns].apply(lambda x: x.str.strip())
    tcf['PropertyCode'] = pd.to_numeric(tcf['PropertyCode']).astype(int)

    plc_query = f"""SELECT *    
    from PropertyCashflows.dbo.PropertyLevelCashflow
    WHERE EffectiveDate = '{version}'
    """
    plc = pd.read_sql(plc_query,con=henrysconnection)
    plc["CashFlowEffectiveDate"] = pd.to_datetime(plc['CashFlowEffectiveDate'],dayfirst=True)

    plc_obj_columns = plc.select_dtypes('object').columns
    plc[plc_obj_columns] = plc[plc_obj_columns].apply(lambda x: x.str.strip())


    cashflow_mapper_query = f"""SELECT * 
    FROM PropertyCashflows.dbo.CashflowTypeMapper
    """
    cashflow_mapper = pd.read_sql(cashflow_mapper_query,con=henrysconnection)


    if 'MRIPropertyCharge' not in tcf.columns:
        tcf = pd.merge(tcf,cashflow_mapper,left_on="CashflowType",right_on='MRITenantCharge')
    else:
        print('decided not to merge more than once :^)')
    grouped_tcf_cashflows = tcf[tcf['ContractedorSpeculative']=='Contractual'].groupby(
            ['PropertyID','PropertyCode','PropertyName','MRIPropertyCharge','CreditRating','CashFlowDate','EffectiveDate'
            ])['Amount'].sum()
    grouped_tcf_cashflows = pd.DataFrame(grouped_tcf_cashflows).reset_index()

    grouped_tcf_cashflows["CreditRating"] = np.where(
        grouped_tcf_cashflows['CreditRating'].str.contains('0'),
        'NR',
        grouped_tcf_cashflows['CreditRating'])

    # grouped_tcf_cashflows.to_sql('GroupedTenancyCashflows',schema='dbo',if_exists='append',con=henrysconnection)

    grouped_property_cashflows = plc[plc['ContractedOrTotal'].str.contains("Total")].groupby(
        ['PropertyID','PropertyCode','PropertyName','CashflowType','CashFlowEffectiveDate','EffectiveDate'])['Amount'].sum()
    grouped_property_cashflows = pd.DataFrame(grouped_property_cashflows).reset_index()


    plc_for_opexp = grouped_property_cashflows[
        grouped_property_cashflows['CashflowType'].isin(['BaseRent',"FreeRent"])
                                            ].groupby(['PropertyID','PropertyCode','PropertyName','CashFlowEffectiveDate','EffectiveDate'])['Amount'].sum()
    plc_for_opexp = pd.DataFrame(plc_for_opexp).reset_index()
    plc_for_opexp = plc_for_opexp.rename({"Amount":"TotalAmount"},axis=1)
    plc_for_opexp['CashFlowEffectiveDate'] = pd.to_datetime(plc_for_opexp['CashFlowEffectiveDate'])


    tcf_for_opexp = grouped_tcf_cashflows.groupby(['PropertyID','PropertyCode','PropertyName','MRIPropertyCharge','CashFlowDate','EffectiveDate'])['Amount'].sum()
    tcf_for_opexp = pd.DataFrame(tcf_for_opexp).reset_index()

    tcf_for_opexp = tcf_for_opexp[tcf_for_opexp['MRIPropertyCharge'].isin(['BaseRent','FreeRent'])].groupby(['PropertyID','PropertyCode','PropertyName','CashFlowDate','EffectiveDate'])['Amount'].sum()
    tcf_for_opexp = pd.DataFrame(tcf_for_opexp).reset_index()

    tcf_for_opexp["PropertyCode"] = pd.to_numeric(tcf_for_opexp["PropertyCode"])
    plc_for_opexp["PropertyCode"] = pd.to_numeric(plc_for_opexp["PropertyCode"]).astype(np.int64)


    tcf_for_opexp = tcf_for_opexp.sort_values(by=["PropertyID","PropertyCode","PropertyName","CashFlowDate","EffectiveDate"])
    tcf_for_opexp = tcf_for_opexp.rename({'Amount':'TotalContractedAmount'},axis=1) 
    plc_for_opexp = plc_for_opexp.sort_values(by=['PropertyID',"PropertyCode","PropertyName","CashFlowEffectiveDate","EffectiveDate"])

    tcf_for_credit_ratings = grouped_tcf_cashflows.groupby(['PropertyID','PropertyCode','PropertyName','MRIPropertyCharge','CashFlowDate','EffectiveDate'])['Amount'].sum()
    tcf_for_credit_ratings = pd.DataFrame(tcf_for_credit_ratings).reset_index()

    tcf_for_credit_ratings = tcf_for_credit_ratings[tcf_for_credit_ratings['MRIPropertyCharge'].isin(['BaseRent'])].groupby(['PropertyID','PropertyCode','PropertyName','CashFlowDate','EffectiveDate'])['Amount'].sum()
    tcf_for_credit_ratings = pd.DataFrame(tcf_for_credit_ratings).reset_index()
    tcf_for_credit_ratings = tcf_for_credit_ratings.rename({'Amount':'TotalContractedAmount'},axis=1) 

    tcf_for_credit_ratings = pd.DataFrame(tcf_for_credit_ratings)

    scalingfactorcalc = pd.merge(tcf_for_opexp,plc_for_opexp,
                                left_on = ["PropertyID","PropertyCode","PropertyName","CashFlowDate","EffectiveDate"],
                                right_on = ['PropertyID',"PropertyCode","PropertyName","CashFlowEffectiveDate","EffectiveDate"])

    scalingfactorcalc['ScalingFactor'] = np.where(scalingfactorcalc['TotalAmount']==0,
                                                0,
                                                scalingfactorcalc['TotalContractedAmount']/scalingfactorcalc['TotalAmount'])

    
    plc_opexp_amounts = plc[
        (plc['CashflowType'] == 'OperatingExpenses')&(plc['ContractedOrTotal'].str.contains("Total"))
                        ].groupby(   ['PropertyID','PropertyCode','PropertyName','CashflowType','CashFlowEffectiveDate','EffectiveDate'])["Amount"].sum()

    plc_opexp_amounts = pd.DataFrame(plc_opexp_amounts).reset_index()
    plc_opexp_amounts['PropertyCode'] = pd.to_numeric(plc_opexp_amounts["PropertyCode"]).astype(int)
    plc_opexp_amounts = plc_opexp_amounts.rename({'Amount':'OpExAmount'},axis=1)

    merged_opexp_amounts = pd.merge(plc_opexp_amounts,scalingfactorcalc,how='left',on=[
        'PropertyID','PropertyCode','PropertyName','CashFlowEffectiveDate','EffectiveDate'])
    merged_opexp_amounts['ScaledOpExpAmount'] = merged_opexp_amounts['OpExAmount'] *  merged_opexp_amounts['ScalingFactor'] 

    if 'TotalContractedAmount' not in grouped_tcf_cashflows.columns:
        grouped_tcf_cashflows = pd.merge(grouped_tcf_cashflows,tcf_for_credit_ratings,how='left',on=[c for c in tcf_for_credit_ratings.columns if 'Amount' not in c])
    else:
        print('merge skipped')

    credit_rating_apportioner = grouped_tcf_cashflows[grouped_tcf_cashflows['MRIPropertyCharge']=='BaseRent']
    credit_rating_apportioner['CreditRatingPortion'] = credit_rating_apportioner[
        "Amount"]/credit_rating_apportioner["TotalContractedAmount"]

    if "ScaledOpExpAmount" not in credit_rating_apportioner.columns:
        credit_rating_apportioner = pd.merge(credit_rating_apportioner,
            merged_opexp_amounts[["PropertyID","PropertyCode",'PropertyName','CashflowType','CashFlowEffectiveDate','EffectiveDate','ScaledOpExpAmount']],
            how='left',
            left_on=['PropertyID','PropertyCode','PropertyName','CashFlowDate',"EffectiveDate"],
            right_on=['PropertyID','PropertyCode','PropertyName','CashFlowEffectiveDate',"EffectiveDate"],
            )[[c for c in credit_rating_apportioner.columns]+["ScaledOpExpAmount"]]
    else:
        pass

    credit_rating_apportioner["OpExpPortionedToCR"] = credit_rating_apportioner['CreditRatingPortion'] * credit_rating_apportioner['ScaledOpExpAmount']

    if 'OpExpPortionedToCR' in credit_rating_apportioner.columns:
        credit_rating_apportioner = credit_rating_apportioner[[c for c in grouped_tcf_cashflows.columns if "Amount" not in c]+["OpExpPortionedToCR"]].rename(
        {"OpExpPortionedToCR":"Amount"},axis=1)
    credit_rating_apportioner['MRIPropertyCharge'] = 'OperatingExpenses'
    credit_rating_apportioner['TotalContractedAmount'] = np.nan
    credit_rating_apportioner = credit_rating_apportioner[grouped_tcf_cashflows.columns]

    if "OperatingExpenses" not in grouped_tcf_cashflows['MRIPropertyCharge'].unique():
        grouped_tcf_cashflows = pd.concat([grouped_tcf_cashflows,credit_rating_apportioner])
        
    consolidated_cashflows = pd.DataFrame(
        grouped_tcf_cashflows[grouped_tcf_cashflows['MRIPropertyCharge'].isin(['BaseRent','FreeRent','Recovery','OperatingExpenses'])
                                                ].groupby([
        'PropertyID','PropertyCode','PropertyName','MRIPropertyCharge','CreditRating','CashFlowDate','EffectiveDate'])['Amount'].sum()).reset_index()
    

    effective_date_string = consolidated_cashflows['EffectiveDate'].unique()[0]
    available_cashflow_data_query = f"""
    select top (3) *
    from PropertyCashflows.dbo.ContractedCashflows
    where [EffectiveDate] = '{effective_date_string}'
    """

    available_cashflow_dates = pd.read_sql(available_cashflow_data_query,con=henrysconnection)
    if len(available_cashflow_dates) > 0:
            rest_of_the_data_query = f"""
        select *
        from PropertyCashflows.dbo.ContractedCashflows
        where [EffectiveDate] != '{effective_date_string}'
        """
            rest_of_the_data = pd.read_sql(rest_of_the_data_query,con=henrysconnection)
            consolidated_cashflows = pd.concat([consolidated_cashflows,rest_of_the_data])
            consolidated_cashflows.to_sql('ContractedCashflows',con=henrysconnection,if_exists='replace',
                                          index=False) 
    else:
        consolidated_cashflows.to_sql('ContractedCashflows',con=henrysconnection,if_exists='append',index=False)

    consolidated_dmadjusted_cashflows = merge_and_calculate_discount_adjustments(
        AsAtDate=AsAtDate,whole_cashflows=consolidated_cashflows)
    
    check_whats_available_query = """
    SELECT DISTINCT [AsAtDate]
    from PropertyCashflows.dbo.ContractedCashflowsDmAdj"""

    try:
        check_whats_available = pd.read_sql(check_whats_available_query,con=henrysconnection)
        check_whats_available = check_whats_available['AsAtDate'].unique()
    except:
        check_whats_available = []


    if AsAtDate in check_whats_available:
        earlier_cashflows_query = f"""SELECT * From PropertyCashflows.dbo.ContractedCashflowsDmAdj
        WHERE [AsAtDate] != '{AsAtDate}'"""
        earlier_cashflows = pd.read_sql(earlier_cashflows_query,con=henrysconnection)
        consolidated_dmadjusted_cashflows = pd.concat([consolidated_dmadjusted_cashflows,earlier_cashflows])
        consolidated_dmadjusted_cashflows = consolidated_dmadjusted_cashflows.drop_duplicates()
        consolidated_dmadjusted_cashflows.to_sql('ContractedCashflowsDmAdj',con=henrysconnection,
                                                if_exists='replace',index=False)
    else:
        consolidated_dmadjusted_cashflows = consolidated_dmadjusted_cashflows.drop_duplicates()
        consolidated_dmadjusted_cashflows.to_sql('ContractedCashflowsDmAdj',con=henrysconnection,
                                             if_exists='append',index=False)

    return consolidated_dmadjusted_cashflows

def merge_and_calculate_discount_adjustments(AsAtDate,whole_cashflows):
    contracted_cashflows = whole_cashflows.copy()
    assert type(AsAtDate) is str 
    assert len(AsAtDate.split('-')) == 3

    metrics_summary_query = f"""SELECT [Asset],[Region],
     [CLC Ownership Interest],[Discount Rate], [Valuation Date] 
     From PropertyCashflows.dbo.PropertyMetricsSummaryNonMRI
    where [Valuation Date] < '{AsAtDate}' 
    """
    metrics_summary_file = pd.read_sql(metrics_summary_query,con=henrysconnection)

    property_mapper_query =  f"""SELECT *
    From PropertyCashflows.dbo.PropertyNameMapper
    """
    property_mapper_file = pd.read_sql(property_mapper_query,con=henrysconnection)

    metrics_summary_file = pd.merge(
        metrics_summary_file,
        property_mapper_file[["MRIPropertyName","MRIPropertyCode","MetricsPropertyName"]],
        how='left',
        left_on='Asset',
        right_on='MetricsPropertyName')

    max_val_date = metrics_summary_file['Valuation Date'].max()
    metrics_summary_file = metrics_summary_file[metrics_summary_file["Valuation Date"]==max_val_date]

    most_recent_discount_rates = metrics_summary_file[
        ['MRIPropertyName',"MRIPropertyCode","Region","CLC Ownership Interest",'Discount Rate']]
    most_recent_discount_rates['MRIPropertyCode'] = pd.to_numeric(most_recent_discount_rates['MRIPropertyCode'])

    if 'Discount Rate' not in contracted_cashflows.columns:
        contracted_cashflows = pd.merge(contracted_cashflows,
                most_recent_discount_rates,
                how='left',
                left_on=['PropertyName','PropertyCode'],
                right_on=['MRIPropertyName','MRIPropertyCode'])
        contracted_cashflows['CLC Ownership Interest'] = contracted_cashflows['CLC Ownership Interest'].fillna(0)

    rfr_dict_query = f"""Select * 
    From PropertyCashflows.dbo.SwapRates
    WHERE [DATE] < '{max_val_date}'
    """

    rfr_table = pd.read_sql(rfr_dict_query,con=henrysconnection)
    rfr_dict = dict()
    for region,identifier in zip(["AUS","JAP"],["ADSWAP10 Curncy","JYSO10 BGN Curncy"]):
        rfr_max_date = rfr_table[rfr_table['IDENTIFIER']==identifier]['DATE'].max()
        rfr_dict[region] = rfr_table[(rfr_table['IDENTIFIER']==identifier)&(rfr_table['DATE']==rfr_max_date)]['YIELD'].values[0]
    
    contracted_cashflows['RFR'] = contracted_cashflows['Region'].map(rfr_dict)

    contracted_cashflows["DiscountMargin"] = (contracted_cashflows['Discount Rate'] - contracted_cashflows['RFR']).fillna(0)

    contracted_cashflows["AsAtDate"] = AsAtDate
    contracted_cashflows['AsAtDate'] = pd.to_datetime(contracted_cashflows['AsAtDate'])
    contracted_cashflows['TimeDiff'] = (contracted_cashflows['CashFlowDate'] - contracted_cashflows['AsAtDate'])
    contracted_cashflows['TimeDiff'] = contracted_cashflows['TimeDiff'].map(lambda x: x.days) /365.2475
    contracted_cashflows['TimeDiff'] = np.where(contracted_cashflows['TimeDiff']<0,0,contracted_cashflows['TimeDiff'])

    contracted_cashflows['DmAdjAmount'] = contracted_cashflows['Amount'] * (
        -1 + np.exp(-contracted_cashflows['DiscountMargin'] * contracted_cashflows['TimeDiff']))

    dm_adj_table = contracted_cashflows[[c for c in contracted_cashflows.columns if 'Amount' != c]]
    dm_adj_table = dm_adj_table.rename({'DmAdjAmount':"Amount"},axis=1)
    dm_adj_table['MRIPropertyCharge'] = dm_adj_table['MRIPropertyCharge'].map(lambda x: ''.join([x,"DmAdj"]))
    dm_adj_table = dm_adj_table[dm_adj_table['Amount']!=0]

    if 'BaseRentDmAdj' not in contracted_cashflows['MRIPropertyCharge'].unique():
        contracted_cashflows = pd.concat([contracted_cashflows[dm_adj_table.columns],dm_adj_table])
        
    contracted_cashflows = contracted_cashflows[contracted_cashflows['AsAtDate'] <= contracted_cashflows['CashFlowDate']]
    contracted_cashflows['CLCAmount'] = contracted_cashflows['Amount'] * contracted_cashflows['CLC Ownership Interest']
    
    return contracted_cashflows


def calculate_dv01(AsAtDate,input_cashflows=None):
    def time_diff_finder(mnemonic):
        assert type(mnemonic) is str
        if "Swap" in mnemonic:
            timeperiod = mnemonic.split('Swap')[1].strip()
        elif "BILL" in mnemonic:
            timeperiod = mnemonic.split("BILL")[1].strip()
        else:
            timeperiod = mnemonic.split("_OIS_")[1].strip()
        if "M" in timeperiod:
            time = timeperiod.split("M")[0]
            return float(time)/12
        elif "Y" in timeperiod:
            time = timeperiod.split("Y")[0]
            return float(time)
        elif "ON" in timeperiod:
            return 1/365
        elif "W" in timeperiod:
            return float(timeperiod.split("W")[0])*1/52

    swap_rates_dates_query = """ SELECT distinct [DATE]
    from PropertyCashflows.dbo.SwapRatesDetailed
    order by [DATE] asc
    """
    swap_rates_dates = pd.read_sql(swap_rates_dates_query,con=henrysconnection)
    swap_rates_date = swap_rates_dates[swap_rates_dates['DATE'] <= AsAtDate]['DATE'].max()

    swap_rates_query = f"""
    SELECT * from PropertyCashflows.dbo.SwapRatesDetailed
    where Date = '{swap_rates_date}'
    """
    swap_rates = pd.read_sql(swap_rates_query,con=henrysconnection)
    swap_rates['time_diff'] = swap_rates['Mnemonic'].map(time_diff_finder) 

    def interpolate_swap_curve(time_diff,aus_or_jpy):
        aus_or_jpy_dict = {a:a for a in ["AUD","JPY"]}
        aus_or_jpy_dict['AUS'] = "AUD"
        aus_or_jpy_dict['JAP'] = "JPY"
        try:
            assert aus_or_jpy in aus_or_jpy_dict.keys()
        except:
            return np.nan
        
        a_or_j = aus_or_jpy_dict[aus_or_jpy]
        
        local_time_diffs = swap_rates[swap_rates['BaseCCY'] == a_or_j]['time_diff'].unique()
        try:
            smaller_t_d = max([td for td in local_time_diffs if td <= time_diff])
        except:
            min_time_diff = swap_rates[(swap_rates['BaseCCY'] == a_or_j)]['time_diff'].min()
            return swap_rates[(swap_rates['BaseCCY'] == a_or_j)&(swap_rates['time_diff']==min_time_diff)]['Mean'].values[0]
        try:
            larger_t_d = min([td for td in local_time_diffs if td >= time_diff])
        except:
            max_time_diff = swap_rates[(swap_rates['BaseCCY'] == a_or_j)]['time_diff'].max()
            return swap_rates[
                (swap_rates['BaseCCY'] == a_or_j)&(swap_rates['time_diff']==max_time_diff)]['Mean'].values[0]

        if smaller_t_d == time_diff or larger_t_d == time_diff:
            return swap_rates[(swap_rates['BaseCCY'] == a_or_j)&(swap_rates['time_diff'] == time_diff)]['Mean'].values[0]
        else:
            later_rate = swap_rates[
                (swap_rates['BaseCCY'] == a_or_j)&(swap_rates['time_diff'] == larger_t_d)]['Mean'].values[0]
            earlier_rate = swap_rates[
                (swap_rates['BaseCCY'] == a_or_j)&(swap_rates['time_diff'] == smaller_t_d)]['Mean'].values[0]
            later_weight = (time_diff - smaller_t_d)/(larger_t_d - smaller_t_d)
            assert later_weight >= 0 and later_weight <= 1
            return later_rate * later_weight + earlier_rate * (1-later_weight)
        
    if type(input_cashflows) is type(None):
        input_cashflows_query = f"""SELECT * 
        from PropertyCashflows.dbo.ContractedCashflowsDmAdj
        where [AsAtDate] = '{AsAtDate}'"""

        contracted_cashflows = pd.read_sql(input_cashflows_query,con=henrysconnection)
    else:
        contracted_cashflows = input_cashflows.copy()
    
    contracted_cashflows['rfr_to_use'] = contracted_cashflows.apply(lambda x: interpolate_swap_curve(x['TimeDiff'],x['Region']),axis=1)

    contracted_cashflows['DmAdjAmount'] = contracted_cashflows['CLCAmount']*(
        -1+np.exp(-contracted_cashflows['DiscountMargin']*contracted_cashflows['TimeDiff']))

    contracted_cashflows[~contracted_cashflows['MRIPropertyCharge'].str.contains("DmAdj")]

    contracted_cashflows['CLCNetAmount'] = contracted_cashflows['CLCAmount'] +  contracted_cashflows['DmAdjAmount']
    contracted_cashflows['CLCAmountRFRShock'] = contracted_cashflows[
    'CLCNetAmount']*(np.exp(-contracted_cashflows['rfr_to_use']*contracted_cashflows['TimeDiff']))
    contracted_cashflows['CLCAmountRFRShock_1bp'] = contracted_cashflows[
    'CLCNetAmount']*(np.exp(-(contracted_cashflows['rfr_to_use']+0.0001)*contracted_cashflows['TimeDiff']))
    contracted_cashflows["CLCAmountRFRShock_diff"] = contracted_cashflows["CLCAmountRFRShock"] - contracted_cashflows["CLCAmountRFRShock_1bp"]

    contracted_cashflows["CLCAmountRFRShock_diff"] = np.where(
    contracted_cashflows['MRIPropertyCharge'].str.contains("DmAdj"),
    0,
    contracted_cashflows["CLCAmountRFRShock_diff"])

    DV01_by_property = contracted_cashflows.groupby(['PropertyID','PropertyCode','PropertyName'])['CLCAmountRFRShock_diff'].sum().reset_index().sort_values(by='PropertyName')

    DV01_by_property['AsAtDate'] = AsAtDate
    
    DV01_by_property.to_sql('DV01_values',con=henrysconnection,if_exists='append',index=False)
    
    return DV01_by_property,contracted_cashflows