import pandas as pd
import yfinance as yf
from datetime import datetime
import numpy as np
from dotenv import load_dotenv
import os
import requests
from db import get_engine
from sqlalchemy import text

load_dotenv()

def query_to_dataframe(params=None):
    try:
        engine = get_engine()
        with engine.connect() as conn:
            raw_conn = conn.connection
            if params:
                df1 = pd.read_sql_query('SELECT "ID", "Symbol", "Desc" FROM stocks', raw_conn, params=params)
            else:
                df1 = pd.read_sql_query('SELECT "ID", "Symbol", "Desc" FROM stocks', raw_conn)
    except Exception as e:
        print(f"Database error: {str(e)}")
        df1 = pd.DataFrame() 

    try:
        engine = get_engine()
        with engine.connect() as conn:
            raw_conn = conn.connection
            if params:
                df2 = pd.read_sql_query('SELECT * FROM markets', raw_conn, params=params)
            else:
                df2 = pd.read_sql_query('SELECT * FROM markets', raw_conn)
    except Exception as e:
        print(f"Database error: {str(e)}")
        df2 = pd.DataFrame() 

    symbols = pd.concat([df1,df2])
    return symbols

# Gather Yahoo Finance Data
def getYFData(symbols):
    # Date
    # SymbolID
    # AttributeID
    # Horizon
    # Value
    horizons = [2,5,20,60,250,1000]
    allData = []
    i = 0
    for symbol in symbols['Symbol']:
        # Set to 33 years, calculate necessary horizons, delete all data prior to 30 years ago.
        data = yf.Ticker(symbol).history(period="24y").reset_index()
        data['Date'] = pd.to_datetime(data['Date']).dt.strftime('%Y-%m-%d')
        data['SymbolID'] = symbols['ID'].iloc[i]
        for horizon in horizons:
            # Rolling Average
            data['Rolling Mean'] = data['Open'].rolling(horizon).mean()
            rolling_mean = data[['Date','Rolling Mean']].dropna()
            data_to_add = pd.DataFrame({
                'Date':rolling_mean['Date'],
                'AttributeID':1,
                'SymbolID':symbols['ID'].iloc[i],
                'Horizon':horizon,
                'Value':rolling_mean['Rolling Mean']
            })
            allData.append(data_to_add)

            # Ratio
            new_data = rolling_mean
            new_data['Ratio'] = data['Open'] / new_data['Rolling Mean']
            new_data = new_data.dropna()
            data_to_add = pd.DataFrame({
                'Date':new_data['Date'],
                'AttributeID':2,
                'SymbolID':symbols['ID'].iloc[i],
                'Horizon':horizon,
                'Value':new_data['Ratio']
            })
            allData.append(data_to_add)

            # Trend
            data['Trend'] = (data['Open']-data.shift(horizon)['Open'])/data.shift(horizon)['Open']*100
            trend = data[['Date','Trend']].dropna()
            data_to_add = pd.DataFrame({
                'Date':trend['Date'],
                'AttributeID':3,
                'SymbolID':symbols['ID'].iloc[i],
                'Horizon':horizon,
                'Value':trend['Trend']
            })
            allData.append(data_to_add)

            # Volatility
            returns = data['Open'].pct_change()
            data['Volatility'] = returns.rolling(horizon).std()
            volatility = data[['Date','Volatility']].dropna()
            data_to_add = pd.DataFrame({
                'Date':volatility['Date'],
                'AttributeID':4,
                'SymbolID':symbols['ID'].iloc[i],
                'Horizon':horizon,
                'Value':volatility['Volatility']
            })
            allData.append(data_to_add)

            # RSI
            returns_whole = data['Open'] - data['Open'].shift(1)
            gains = returns_whole.where(returns_whole >= 0).rolling(horizon, min_periods = 1).mean()
            losses = -returns_whole.where(returns_whole < 0).rolling(horizon, min_periods = 1).mean()
            # Fill with small value to avoid calculation errors
            gains = gains.fillna(0.01)
            losses = losses.fillna(0.01)
            data['RSI'] = 100 - (100 / (1 + (gains / losses)))
            rsi = data[['Date','RSI']].dropna()
            data_to_add = pd.DataFrame({
                'Date':rsi['Date'],
                'AttributeID':5,
                'SymbolID':symbols['ID'].iloc[i],
                'Horizon':horizon,
                'Value':rsi['RSI']
            })
            allData.append(data_to_add)

            # K% + D%
            min = data['Low'].rolling(horizon).min()
            max = data['High'].rolling(horizon).max()
            data['k'] = (data['Open'] - min) / (max - min)
            data['d'] = data['k'].rolling(3).mean()
            k = data[['Date','k']].dropna()
            d = data[['Date','d']].dropna()
            data_to_add = pd.DataFrame({
                'Date':k['Date'],
                'AttributeID':6,
                'SymbolID':symbols['ID'].iloc[i],
                'Horizon':horizon,
                'Value':k['k']
            })
            allData.append(data_to_add)
            data_to_add = pd.DataFrame({
                'Date':d['Date'],
                'AttributeID':7,
                'SymbolID':symbols['ID'].iloc[i],
                'Horizon':horizon,
                'Value':d['d']
            })
            allData.append(data_to_add)

            # OBV
            target = np.where(data['Open'].shift(-1) > data['Open'], 1, 0)
            vol_table = pd.DataFrame({'Target':target, 'Volume':data['Volume']}).dropna()
            vol_table['Volume'] = np.where(vol_table['Target'] == 0, vol_table['Volume'] * (-1), vol_table['Volume'])
            data['OBV'] = vol_table['Volume'].rolling(horizon).sum()
            obv = data[['Date','OBV']].dropna()
            data_to_add = pd.DataFrame({
                'Date':obv['Date'],
                'AttributeID':8,
                'SymbolID':symbols['ID'].iloc[i],
                'Horizon':horizon,
                'Value':obv['OBV']
            })
            allData.append(data_to_add)
             
            # MACD
            if horizon < 1000:
                fast_ema = data['Open'].ewm(horizon, adjust=False).mean()
                new_horizon = horizons[horizons.index(horizon)+1]
                slow_ema = data['Open'].ewm(new_horizon, adjust=False).mean()
                macd_line = fast_ema - slow_ema
                signal_line = macd_line.ewm(9, adjust=False).mean()
                data['MACD'] = macd_line - signal_line
                macd = data[['Date','MACD']].dropna()
                data_to_add = pd.DataFrame({
                    'Date':macd['Date'],
                    'AttributeID':9,
                    'SymbolID':symbols['ID'].iloc[i],
                    'Horizon':horizon,
                    'Value':macd['MACD']
                })
                allData.append(data_to_add)

        # Open, High, Low, Close, Volume
        open = data[['Date','Open']]
        data_to_add = pd.DataFrame({
            'Date':open['Date'],
            'AttributeID':10,
            'SymbolID':symbols['ID'].iloc[i],
            'Horizon':0,
            'Value':open['Open']
        })
        allData.append(data_to_add)

        high = data[['Date','High']]
        data_to_add = pd.DataFrame({
            'Date':high['Date'],
            'AttributeID':11,
            'SymbolID':symbols['ID'].iloc[i],
            'Horizon':0,
            'Value':high['High']
        })
        allData.append(data_to_add)

        low = data[['Date','Low']]
        data_to_add = pd.DataFrame({
            'Date':low['Date'],
            'AttributeID':12,
            'SymbolID':symbols['ID'].iloc[i],
            'Horizon':0,
            'Value':low['Low']
        })
        allData.append(data_to_add)

        close = data[['Date','Close']]
        data_to_add = pd.DataFrame({
            'Date':close['Date'],
            'AttributeID':13,
            'SymbolID':symbols['ID'].iloc[i],
            'Horizon':0,
            'Value':close['Close']
        })
        allData.append(data_to_add)

        volume = data[['Date','Volume']]
        data_to_add = pd.DataFrame({
            'Date':volume['Date'],
            'AttributeID':20,
            'SymbolID':symbols['ID'].iloc[i],
            'Horizon':0,
            'Value':volume['Volume']
        })
        allData.append(data_to_add)

        i += 1

    allData = pd.concat(allData, ignore_index=True)

    # 30 year cutoff
    cutoff = pd.Timestamp.now() - pd.DateOffset(years=20)
    cutoff = pd.to_datetime(cutoff).strftime('%Y-%m-%d')

    allData = allData[allData['Date'] >= cutoff]
    allData['AttributeID'] = allData['AttributeID'].astype('category')
    allData['SymbolID'] = allData['SymbolID'].astype('category')
    return allData

# Gather FRED Data
def getFREDData():
    horizons = [1,3,12,48]
    URL_BASE = 'https://api.stlouisfed.org/'
    ENDPOINT = 'fred/series/observations'
    URL = URL_BASE + ENDPOINT
    fredKey = os.getenv('API_KEY')
    indicators = {
        'interest': 'FEDFUNDS',
        'inflation': 'CPIAUCNS',
        'unemployment': 'UNRATE'
    }
    allData = []
    testData = []
    for indicator, series_id in indicators.items():
        params = {
            'api_key': fredKey,
            'series_id': series_id,
            'file_type': 'json'
        }
        
        res = requests.get(URL, params=params)
        print(f"Status: {res.status_code}")
        print(f"Headers: {res.headers}")
        print(f"Text: {res.text[:500]}")
        try:
            data = res.json()
        except Exception as e:
            print("JSON decode failed. Response was:")
            print(res.text)
            raise e
        data = data['observations']
        data = pd.DataFrame(data)
        if indicator == 'interest':
            data['AttributeID'] = 14
        elif indicator == 'inflation':
            data['AttributeID'] = 15
        else:
            data['AttributeID'] = 16
        data['Horizon'] = 0
        data = data[['date', 'AttributeID', 'Horizon', 'value']].rename(columns={'date':'Date', 'value':'Value'})

        testData.append(data)

    data = pd.concat(testData, ignore_index=True)
    interest = data[data['AttributeID']==14]
    inflation = data[data['AttributeID']==15]
    unemployment = data[data['AttributeID']==16]
    interest = interest.copy()
    interest['Date'] = pd.to_datetime(interest['Date']).dt.strftime('%Y-%m-%d')
    interest['Value'] = pd.to_numeric(interest['Value'])
    inflation = inflation.copy()
    inflation['Date'] = pd.to_datetime(inflation['Date']).dt.strftime('%Y-%m-%d')
    inflation['Value'] = pd.to_numeric(inflation['Value'])
    unemployment = unemployment.copy()
    unemployment['Date'] = pd.to_datetime(unemployment['Date']).dt.strftime('%Y-%m-%d')
    unemployment['Value'] = pd.to_numeric(unemployment['Value'])

    interest = interest.sort_values('Date')
    inflation = inflation.sort_values('Date')
    unemployment = unemployment.sort_values('Date')
    inflation['Value'] = inflation['Value'] / inflation['Value'].shift(12)
    inflation = inflation.dropna()

    try:
        data_to_add = interest
        allData.append(data_to_add)
    except Exception as e:
        print('No data in interest table')

    try:
        data_to_add = inflation
        allData.append(data_to_add)
    except Exception as e:
        print('No data in inflation table')
    
    try:
        data_to_add = unemployment
        allData.append(data_to_add)
    except Exception as e:
        print('No data in unemployment table')
    

    for horizon in horizons:
        interest = interest.copy()
        inflation = inflation.copy()
        unemployment = unemployment.copy()
        interest['Lag'] = interest['Value'] - interest['Value'].shift(horizon)
        inflation['Lag'] = inflation['Value'] - inflation['Value'].shift(horizon)
        unemployment['Lag'] = unemployment['Value'] - unemployment['Value'].shift(horizon)
        try:
            interest_lag = interest[['Date','Lag']].dropna()
            data_to_add = pd.DataFrame({
                'Date':interest_lag['Date'],
                'AttributeID':17,
                'Horizon':horizon,
                'Value':interest_lag['Lag']
            })
            allData.append(data_to_add)
        except Exception as e:
            print('No data in interest table')

        try:
            inflation_lag = inflation[['Date','Lag']].dropna()
            data_to_add = pd.DataFrame({
                'Date':inflation_lag['Date'],
                'AttributeID':18,
                'Horizon':horizon,
                'Value':inflation_lag['Lag']
            })
            allData.append(data_to_add)
        except Exception as e:
            print('No data in inflation table')

        try:
            unemployment_lag = unemployment[['Date','Lag']].dropna()
            data_to_add = pd.DataFrame({
                'Date':unemployment_lag['Date'],
                'AttributeID':19,
                'Horizon':horizon,
                'Value':unemployment_lag['Lag']
            })
            allData.append(data_to_add)
        except Exception as e:
            print('No data in interest table')

    allData = pd.concat(allData, ignore_index=True)
    cutoff = pd.Timestamp.now() - pd.DateOffset(years=20)
    cutoff = pd.to_datetime(cutoff).strftime('%Y-%m-%d')

    allData_before = allData[allData['Date'] <= cutoff]
    allData_before = allData_before.groupby('AttributeID').tail(1)

    allData = allData[allData['Date'] >= cutoff]

    allData = pd.concat([allData, allData_before], ignore_index=True)

    allData = allData.drop_duplicates()
    allData = allData.sort_values('Date')

    return allData