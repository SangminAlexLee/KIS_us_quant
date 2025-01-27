import FinanceDataReader as fdr
import pandas as pd
from sqlalchemy import create_engine
import pymysql
import yfinance as yf
import datetime
from datetime import timedelta, date
import time
import requests
import yaml

with open('config.yaml', encoding='UTF-8') as f:
 _cfg = yaml.load(f, Loader=yaml.FullLoader)
DISCORD_WEBHOOK_URL = _cfg['DISCORD_WEBHOOK_URL']

def send_message(msg):
    """디스코드 메세지 전송"""
    now = datetime.datetime.now()
    message = {"content": f"[{now.strftime('%b.%d %H:%M')}] {str(msg)}"}
    requests.post(DISCORD_WEBHOOK_URL, data=message)
    print(message)

#DB 연결
def db_conn(host='db-dfmba.cnm8u4m2cbtp.ap-northeast-2.rds.amazonaws.com' , port=3306):
    engine = create_engine(f'mysql+pymysql://root:alexalex@{host}:{port}/stock_db')
    con = pymysql.connect(user = 'root', 
                        passwd ='alexalex',
                        host = f'{host}', 
                        db='stock_db', 
                        charset = 'utf8',
                        cursorclass = pymysql.cursors.DictCursor)
    mycursor = con.cursor()
    return engine, con, mycursor

send_message(f"[ETF Update] Start ")

# data = { 'Symbol':['SPY', 'ARKQ'], 'Name':['SnP 500', 'ARK Autonomouse Technology & Robotics']}
# df_etf = pd.DataFrame(data)
engine, con, mycursor = db_conn()
sql = "select * from us_etf_list"
mycursor.execute(sql)
result = mycursor.fetchall()
con.close()
df_etf = pd.DataFrame(result)
df_etf['IndustryCode'] = ""
df_etf['Industry'] = ""
df_etf['Market'] = "AMEX"
df_etf['ins_date'] = (date.today() - timedelta(days=1))

print(f'df_eft : {df_etf}')
engine, con, mycursor = db_conn()
df_etf.to_sql(name = 'us_stock_list', con=engine, if_exists='append', index=False, method="multi")
con.close()

send_message(f"[ETF Update]List udpate Finished for {date.today()}")

try: 
    engine, con, mycursor = db_conn()
    base_etf_ticker = 'SPY'
    sql = f"SELECT max(date) max_date FROM us_stock_price where symbol = '{base_etf_ticker}' "
    # sql = "SELECT date max_date FROM us_stock_price GROUP BY date HAVING COUNT(*) >= 5800 ORDER BY date DESC LIMIT 1"
    print(f'sql : {sql}')
    mycursor.execute(sql)
    max_date = mycursor.fetchall()
    con.close()
    print(f'max date is {max_date}')
except Exception as e:
    print('max date is unkwnown')
    today = date.today()
    t_minus_15 = today - timedelta(days=10)
    max_date = [{'max_date' : t_minus_15}]
    
start_date = (max_date[0]['max_date'] + timedelta(days=1)).strftime('%Y-%m-%d')
# start_date = '2024-12-20'
end_date = (date.today()).strftime('%Y-%m-%d')

print(f'start date : {start_date}, end date : {end_date}')

if start_date >= end_date:
    print('No need for udpate')    
    send_message(f"[Stock Update]No need for update")
else:

    df_list = []
    count = 0

    for etf_ticker in df_etf.iterrows():
        print(f'etf_ticker:{etf_ticker}')
        # print(f'type:{type(etf_ticker[1]['Symbol'])}')
        symbol = etf_ticker[1]['Symbol']
        print(f'ticker : {symbol}')
        print(f'start date : {start_date}, end date:{end_date}')
        etf_data = yf.download(symbol, start=start_date, end=end_date)

        etf_data.reset_index(inplace=True)
        etf_data.columns = ['_'.join(col) for col in etf_data.columns]
        etf_data.columns = ['Date', 'Close', 'High', 'Low', 'Open', 'Volume']
        etf_data['Adj Close'] =  etf_data['Close']
        etf_data['Symbol'] =  symbol
        etf_data = etf_data[['Symbol', 'Date', 'Close', 'Adj Close','Volume']]
        print(f'Price date for {symbol} : {etf_data}')
        df_list.append(etf_data)
        count = count + 1

    df_list = pd.concat(df_list, ignore_index=True, axis=0)
    print(f'df_list : {df_list}')

    engine, con, mycursor = db_conn()
    df_list.to_sql(name = 'us_stock_price', con=engine, if_exists='append', index=False)
    con.close()

    send_message(f"[ETF Update]Price update Finished:{count} Stocks")
