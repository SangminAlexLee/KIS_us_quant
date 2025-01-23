import FinanceDataReader as fdr
import pandas as pd
from sqlalchemy import create_engine
import pymysql
import yfinance as yf
from datetime import timedelta, date
import datetime
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

send_message(f"[Stock Update] Start ")

nasdaq =  fdr.StockListing('NASDAQ')
nasdaq['Market'] = 'NASDAQ'
nyse = fdr.StockListing('NYSE')
nyse['Market'] = 'NYSE'
amex = fdr.StockListing('AMEX')
amex['Market'] =  'AMEX'
snp500 = fdr.StockListing('S&P500') 
snp500['Market'] = 'SnP500'
snp500['ins_date'] = date.today()

df = pd.concat([nasdaq, nyse, amex])
df1 = df.copy()
df1 = df.drop_duplicates('Symbol')
df1['ins_date'] = date.today()
df1.head()

print(f'no of list : {len(df1)}')
print(f'head : {df1.head()}')

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

try: 
    engine, con, mycursor = db_conn()
    df1.to_sql(name = 'us_stock_list', con=engine, if_exists='append', index=False, method="multi")
    snp500.to_sql(name = 'us_snp500_list', con=engine, if_exists='append', index=False, method="multi")
    con.close()
    send_message(f"[Stock Update]List update success")
except Exception as e:
    print(f'list update failed with {e}')
    send_message(f"[Stock Update]List update failed")

try:
    # US Stock 종목 최신 리스트 조회
    engine, con, mycursor = db_conn()
    sql = "SELECT * FROM us_stock_list WHERE ins_date = (SELECT ins_date max_date FROM us_stock_list where symbol not in ( select symbol from us_etf_list ) GROUP BY ins_date HAVING COUNT(*) >= 5800 ORDER BY ins_date DESC LIMIT 1)"
    mycursor.execute(sql)
    result = mycursor.fetchall()
    df_us_list = pd.DataFrame(result)
    con.close()
except Exception as e:
    print(f'Stoc list inquiry failed {e}')
    send_message(f"[Stock Update]List Read failed")
    
if len(df_us_list) > 0 : 
    # 주가 테이블 마지막 영업일 조회
    try: 
        engine, con, mycursor = db_conn()
        # sql = "SELECT max(date) max_date FROM us_stock_price"
        # sql = f"SELECT ins_date max_date FROM us_stock_price where symbol not in ( select symbol from us_etf_list ) GROUP BY ins_date HAVING COUNT(*) >= 5800 ORDER BY ins_date DESC LIMIT 1"
        sql = "SELECT date max_date FROM us_stock_price GROUP BY date HAVING COUNT(*) >= 5800 ORDER BY date DESC LIMIT 1"
        mycursor.execute(sql)
        max_date = mycursor.fetchall()
        con.close()
    except Exception as e:
        print('max date is unkwnown')
        today = date.today()
        t_minus_15 = today - timedelta(days=10)
        max_date = [{'max_date' : t_minus_15}]
        
    df_list = [] 
    count = 0
    symbol_list = df_us_list.Symbol[:]
    start_date = (max_date[0]['max_date'] + timedelta(days=1)).strftime('%Y-%m-%d')
    end_date = (date.today()).strftime('%Y-%m-%d')

    print(f'start date : {start_date}, end date : {end_date}')

    if start_date >= end_date:
        print('No need for udpate')    
        send_message(f"[Stock Update]No need for update")
    else:
        try:
            engine, con, mycursor = db_conn()
            sql  = f"delete from us_stock_price where date >= '{start_date}' and symbol not in ( select symbol from us_etf_list )"
            print(f'sql : {sql}')
            mycursor.execute(sql)
            result = mycursor.fetchall()
            print(f'result: {result}')
            con.commit()
            con.close()
        except Exception as e:
            print('Price table deletion failed')

        send_message(f"[Stock Update]Price update start")
        for symbol in symbol_list[:]:
            print(f'{symbol} count : {count}')

            # 재시도 로직 추가
            retries = 1
            stock = None
            for attempt in range(retries):
                try:
                    stock = fdr.DataReader(symbol, start_date, end_date)
                    print(f'stock after download : {stock}')
                    break  # 성공하면 루프 탈출
                except Exception as e:
                    print(f'Error fetching data for {symbol}, attempt {attempt + 1}/{retries}: {e}')
                    time.sleep(2)  # 재시도 전 대기 (2초)
            
            if stock is None:
                print(f'Failed to fetch data for {symbol} after {retries} attempts.')
                count += 1
                continue
            
            stock.reset_index(inplace=True)
            stock = stock.rename(columns={'index':'Date'})
            stock['Symbol'] = symbol
            stock = stock.loc[stock.Date > start_date]

            print(f'stock after reset index : {stock}')

            if len(stock) > 1 :
                stock =  stock[['Symbol', 'Date', 'Close', 'Adj Close', 'Volume']]
            else:
                print(f'no data for {symbol}')
                count = count + 1 
                continue

            # snq500
            # print(f'stock : {stock}')
            df_list.append(stock)

            if ((count % 10 == 0) or (count == (len(symbol_list)-1))):
                print('upload to DB')
                engine, con, mycursor = db_conn()
                df_to_save = pd.concat(df_list, axis=0)
                df_list = []
                # print(f'df_to_save:{df_to_save}')
                df_to_save.to_sql(name = 'us_stock_price', con=engine, if_exists='append', index=False)
                con.close()

            count = count + 1 
    send_message(f"[Stock Update]Price update Finished:{count} Stocks")