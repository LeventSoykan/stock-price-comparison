from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
import requests
import pandas as pd
import numpy as np
import time
from sqlalchemy import create_engine
import os

class ETL:
    def __init__(self, stocks, api_key):
        self.stocks = stocks
        self.api_key = api_key
        self.uri_dict = {
            'daily': 'https://www.alphavantage.co/query?function=TIME_SERIES_DAILY_ADJUSTED&symbol=%s&apikey=%s',
            'info': 'https://www.alphavantage.co/query?function=OVERVIEW&symbol=%s&apikey=%s',
            'balance_sheet': 'https://www.alphavantage.co/query?function=BALANCE_SHEET&symbol=%s&apikey=%s',
            'income_statement': 'https://www.alphavantage.co/query?function=INCOME_STATEMENT&symbol=%s&apikey=%s'
        }

    #Get stock prices
    def get_price(self, stock, stock_id):
        url = self.uri_dict['daily'] % (stock, self.api_key)
        r = requests.get(url)
        data = r.json()

        df = pd.DataFrame(data['Time Series (Daily)'], dtype='float').T
        df.columns = [col[3:].replace(' ', '_') for col in df.columns]
        df.sort_index(inplace=True, ascending=False)
        df['stock_id'] = stock_id
        df['date'] = pd.to_datetime(df.index)
        df.reset_index(drop=True, inplace=True)
        return df

    #Get stock-company information
    def get_info(self, stock, stock_id):
        url = self.uri_dict['info'] % (stock, self.api_key)
        r = requests.get(url)
        data = r.json()

        df = pd.DataFrame([data])
        df['stock_id'] = 1
        df = df.replace('None', np.nan)
        for col in df.columns:
            if col in ['DividendDate', 'ExDividendDate']:
                df[col] = pd.to_datetime(df[col])
            else:
                try:
                    df[col] = pd.to_numeric(df[col])
                except:
                    pass
        return df

    #Get financials -income statement or balance sheet
    def get_financial(self, stock, stock_id, key='balance_sheet'):
        url = self.uri_dict[key] % (stock, self.api_key)
        r = requests.get(url)
        data = r.json()

        df = pd.DataFrame(data['annualReports'])
        df['stock_id'] = stock_id
        df = df.replace('None', np.nan)
        for col in df.columns:
            if col == 'fiscalDateEnding':
                df[col] = pd.to_datetime(df[col])
            elif col != 'reportedCurrency':
                df[col] = pd.to_numeric(df[col])
        return df

    #Get data for all stocks and merge
    def merge(self, func, key=None):
        df = pd.DataFrame()
        for stock, stock_id in self.stocks.items():
            if key:
                temp_df = func(stock, stock_id, key)
            else:
                temp_df = func(stock, stock_id)
            if df.empty:
                df = pd.DataFrame(temp_df)
            else:
                df = pd.concat([df, temp_df])
        return df

    #Extract merged dataframes to csv
    def extract_to_csv(self):
        self.merge(self.get_info).to_csv('info.csv', index=False)
        time.sleep(65)
        self.merge(self.get_price).to_csv('prices.csv', index=False)
        time.sleep(65)
        self.merge(self.get_financial, 'balance_sheet').to_csv('bs.csv', index=False)
        time.sleep(65)
        self.merge(self.get_financial, 'income_statement').to_csv('inc.csv', index=False)

def update_prices():
    POSTGRES_PASSWORD = '*******'
    LINODE_SERVER = '********'
    AV_API_KEY = '*********'
    stocks = {
        'IBM': 1,
        'GOOG': 2,
        'NVDA': 3,
        'ASML': 4,
        'AMD': 5
    }
    # Create sql engine with sqlalchemy
    engine = create_engine(f'postgresql://postgres:{POSTGRES_PASSWORD}@{LINODE_SERVER}:5432/stockdb')
    # Create etl object
    etl = ETL(stocks, AV_API_KEY)
    etl.merge(etl.get_price).to_sql('stock_prices', engine, schema='public', index=False, if_exists='replace')

etl_dag = DAG(
    'etl_dag',
    start_date=datetime(2023, 5, 10),
    schedule_interval='22 12 * * *'
)

python_task = PythonOperator(
    task_id='python_task',
    python_callable=update_prices,
    dag=etl_dag
)
