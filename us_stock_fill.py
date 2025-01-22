import FinanceDataReader as fdr
import pandas as pd
from sqlalchemy import create_engine
import pymysql
import yfinance as yf
from datetime import timedelta, date
import time

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

# S&P500 종목 최신 리스트 조회
engine, con, mycursor = db_conn()
sql = "SELECT * FROM us_snp500_list WHERE ins_date = (SELECT MAX(ins_date) FROM us_snp500_list)"
mycursor.execute(sql)
result = mycursor.fetchall()
df_us_list = pd.DataFrame(result)
con.close()

# 주가 테이블 마지막 영업일 조회
engine, con, mycursor = db_conn()
sql = "SELECT date max_date FROM us_snp500_price GROUP BY date HAVING COUNT(*) >= 500 ORDER BY date DESC LIMIT 1"
mycursor.execute(sql)
max_date = mycursor.fetchall()
con.close()

df_list = [] 
count = 0
symbol_list = df_us_list.Symbol[:]
start_date = (max_date[0]['max_date'] + timedelta(days=1)).strftime('%Y-%m-%d')
end_date = (date.today()).strftime('%Y-%m-%d')

print(f'start date : {start_date}, end date : {end_date}')

engine, con, mycursor = db_conn()
sql  = f"delete from us_snp500_price where date >= '{start_date}'"
print(f'sql : {sql}')
mycursor.execute(sql)
result = mycursor.fetchall()
print(f'result: {result}')
con.commit()
con.close()

for symbol in symbol_list[:]:
    print(f'{symbol} count : {count}')

    # 재시도 로직 추가
    retries = 3
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

    print(f'stock after reset index : {stock}')

    if len(stock) > 1 :
        stock =  stock[['Symbol', 'Date', 'Close', 'Adj Close', 'Volume']]
    else:
        print(f'no data for {symbol}')
        count = count + 1 
        continue

    # snq500
    print(f'stock : {stock}')
    df_list.append(stock)

    if ((count % 10 == 0) or (count == (len(symbol_list)-1))):
        print('upload to DB')
        print('upload to DB')
        engine, con, mycursor = db_conn()
        df_to_save = pd.concat(df_list, axis=0)
        df_list = []
        print(f'df_to_save:{df_to_save}')
        df_to_save.to_sql(name = 'us_snp500_price', con=engine, if_exists='append', index=False)
        con.close()

    count = count + 1 