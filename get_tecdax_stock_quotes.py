# get_tecdax_stock_quotes.python
# Python Script to fetch historical, monthly closing stock prices and volumes
# for TecDAX companies 2022 to 2025
# Nils Clausen, 2025-08-21

import yfinance as yf
import pandas as pd

# Combined Index Composition TecDAX
# ISIN: DE0007203275, WKN: 720327, Symbol: TDXP
# Current TecDAX stocks
# https://www.boerse-frankfurt.de/indices/tecdax/constituents

tecdax_stocks = [
    {'symbol': 'AIXA.DE', 'isin': 'DE000A0WMPJ6', 'name': 'AIXTRON'},
    {'symbol': 'AOF.DE',  'isin': 'DE0005104400', 'name': 'ATOSS'},
    {'symbol': 'BC8.DE',  'isin': 'DE0005158703', 'name': 'BECHTLE'},
    {'symbol': 'COK.DE',  'isin': 'DE0005419105', 'name': 'CANCOM'},
    {'symbol': 'AFX.DE',  'isin': 'DE0005313704', 'name': 'CARLZEISS MED'},
    {'symbol': 'DTE.DE',  'isin': 'DE0005557508', 'name': 'DEUTSCHE TELEKOM'},
    {'symbol': 'DRW3.DE', 'isin': 'DE0005550636', 'name': 'DRAEGERWERK'},
    {'symbol': 'EUZ.DE',  'isin': 'DE0005659700', 'name': 'ECKERT ZIEGLER'},
    {'symbol': 'ELG.DE',  'isin': 'DE0005677108', 'name': 'ELMOS'},
    {'symbol': 'EVT.DE',  'isin': 'DE0005664809', 'name': 'EVOTEC'},
    {'symbol': 'FYB.DE',  'isin': 'DE000A1EWVY8', 'name': 'FORMYCON'},
    {'symbol': 'FNTN.DE', 'isin': 'DE000A0Z2ZZ5', 'name': 'FREENET'},
    {'symbol': 'HAG.DE',  'isin': 'DE000HAG0005', 'name': 'HENSOLDT'},
    {'symbol': 'IFX.DE',  'isin': 'DE0006231004', 'name': 'INFINEON'},
    # IONOS first traded in 2023-02
    {'symbol': 'IOS.DE',  'isin': 'DE000A3E00M1', 'name': 'IONOS'},
    {'symbol': 'JEN.DE',  'isin': 'DE000A2NB601', 'name': 'JENOPTIK'},
    # KONTRON merged with Katek per 2024-05-16, listed in Austria, therefore excluded
    # {'symbol': 'KTN.DE',  'isin': 'AT0000A0E9W5', 'name': 'KONTRON'},
    {'symbol': 'NA9.DE',  'isin': 'DE000A3H2200', 'name': 'NAGARRO'},
    {'symbol': 'NEM.DE',  'isin': 'DE0006452907', 'name': 'NEMETSCHEK'},
    {'symbol': 'NDX1.DE', 'isin': 'DE000A0D6554', 'name': 'NORDEX'},
    {'symbol': 'PNE3.DE', 'isin': 'DE000A0JBPG2', 'name': 'PNE'},
    {'symbol': 'QIA.DE',  'isin': 'NL0012169213', 'name': 'QIAGEN'},
    {'symbol': 'SAP.DE',  'isin': 'DE0007164600', 'name': 'SAP'},
    {'symbol': 'SRT3.DE', 'isin': 'DE0007165631', 'name': 'SARTORIUS'},
    {'symbol': 'SHL.DE',  'isin': 'DE000SHL1006', 'name': 'SIEMENS HEALTH.'},
    {'symbol': 'WAF.DE',  'isin': 'DE000WAF3001', 'name': 'SILTRONIC'},
    {'symbol': 'S92.DE',  'isin': 'DE000A0DJ6J9', 'name': 'SMA SOLAR'},
    {'symbol': 'SMHN.DE', 'isin': 'DE000A1K0235', 'name': 'SUESS MICROTEC'},
    {'symbol': 'TMV.DE',  'isin': 'DE000A2YN900', 'name': 'TEAMVIEWER'},
    {'symbol': 'UTDI.DE', 'isin': 'DE0005089031', 'name': 'UNITED INTERNET'},
# plus as of January 2022
# https://www.dax-indices.com/documents/dax-indices/Documents/Resources/WeightingFiles/Composition/2022/January/TecDAX_ICR.20220125.xls
    {'symbol': '1U1.DE',  'isin': 'DE0005545503', 'name': '1UND1'},
    # COMPUGROUP was taken private and de-listed on 2025-05-23
    # {'symbol': 'COP.DE',  'isin': 'DE000A288904', 'name': 'COMPUGROUP'}, 
    # MORPHOSYS was taken private by Novartis and de-listed end of 2024-12
    # {'symbol': 'MOR.DE',  'isin': 'DE0006632003', 'name': 'MORPHOSYS'},
    # S&T changed name to Kontron with same ISIN, see above
    # {'symbol': 'SANT.DE', 'isin': 'AT0000A0E9W5', 'name': 'SUNDT'},
    # SOFTWARE AG de-listed per 2024-02-23, see https://cdn.pressetext.com/news/20240220023
    # {'symbol': 'SOW.DE',  'isin': 'DE000A2GS401', 'name': 'SOFTWARE AG'},
    # SUSE de-listed as per 2023-11-13, see https://www.suse.com/news/SUSE-announces-delisting-from-Frankfurt-Stock-Exchange/
    # {'symbol': 'SUSE.DE', 'isin': 'LU2333210958', 'name': 'SUSE'},
    {'symbol': 'O2D.DE',  'isin': 'DE000A1J5RX9', 'name': 'TELEFONICA'},
    {'symbol': 'VTWR.DE', 'isin': 'DE000A3H3LL2', 'name': 'VANTAGE'},
    # Restructuring and devaluation per 2024-02-12
    # https://www.varta-ag.com/fileadmin/varta_ag/publications/ad-hoc_announcements/20241211_VARTA_AG_Ad_hoc_confirmation_restructuring_plan_EN.pdf
    # {'symbol': 'VAR1.DE', 'isin': 'DE000A0TGJ55', 'name': 'VARTA'},
# plus as of January 2023
# https://www.dax-indices.com/documents/dax-indices/Documents/Resources/WeightingFiles/Composition/2023/January/TecDAX_ICR.20230125.xls
    {'symbol': 'ADV.DE',  'isin': 'DE0005103006', 'name': 'ADTRAN'},
    {'symbol': 'VBK.DE',  'isin': 'DE000A0JL9W6', 'name': 'VERBIO'},
# plus as of January 2024
# https://www.dax-indices.com/documents/dax-indices/Documents/Resources/WeightingFiles/Composition/2024/January/TecDAX_ICR.20240125.xls
    {'symbol': 'EKT.DE',  'isin': 'DE0005313506', 'name': 'ENERGIEKONTOR'},
# plus as per listing by Yahoo Finance
# https://finance.yahoo.com/quote/%5ETECDAX/components/
    {'symbol': 'PFV.DE',  'isin': 'DE0006916604', 'name': 'PFEIFFER'},
    {'symbol': 'SBS.DE',  'isin': 'DE000STRA555', 'name': 'STRATEC'} 
]

# Date range definition
start_date = '2022-08-01'
end_date = '2025-07-31'

# Results storage
stock_data = []

# Enhanced data retrieval with ISIN tracking
for stock in tecdax_stocks:
    try:
        # Fetch stock data
        ticker = yf.Ticker(stock['isin'])
        hist_data = ticker.history(start=start_date, end=end_date)
        
        # Monthly aggregation
        monthly_data = hist_data.resample('ME').agg({
            'Close': 'last',
            'Volume': 'sum'
        })
        
        # Add ISIN and name to dataframe
        monthly_data['ISIN'] = stock['isin']
        monthly_data['Company'] = stock['name']
        
        # Store the monthly data in the list
        stock_data.append(monthly_data)
        
        # print(f"\n--- Monthly Data for {stock['name']} ---")
        # print(monthly_data)
        
    except Exception as e:
        print(f"Error retrieving data for {stock['name']}: {e}")

# Concatenate all monthly data into a single DataFrame
final_df = pd.concat(stock_data)
# Reset index to convert the date index to a column
final_df.reset_index(inplace=True)
# Ensure the 'Date' column is in datetime format
final_df['Date'] = pd.to_datetime(final_df['Date'], errors='coerce')
# Check for any NaT values in 'Date' and handle them if necessary
if final_df['Date'].isnull().any():
    print("Warning: Some dates could not be converted and will be dropped.")
    final_df = final_df.dropna(subset=['Date'])
# Format the date to string without time
final_df['Date'] = final_df['Date'].dt.strftime('%Y-%m-%d')
# Reorder columns to have ISIN and Company as the first two columns
final_df = final_df[['ISIN', 'Company', 'Date', 'Close', 'Volume']]
# Save the final DataFrame to a CSV file
final_df.to_csv('stock_monthly_data.csv', index=False)  # Set index=False to avoid writing the default index