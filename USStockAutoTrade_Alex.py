import requests
import json
import datetime
import time
import yaml
from sqlalchemy import create_engine
import pymysql
import pandas as pd
import time
from datetime import timedelta, date
import shared_vars
from KIS_US_Functions import *

with open('config.yaml', encoding='UTF-8') as f:
 _cfg = yaml.load(f, Loader=yaml.FullLoader)
APP_KEY = _cfg['APP_KEY']
APP_SECRET = _cfg['APP_SECRET']
ACCESS_TOKEN = ""
CANO = _cfg['CANO']
ACNT_PRDT_CD = _cfg['ACNT_PRDT_CD']
DISCORD_WEBHOOK_URL = _cfg['DISCORD_WEBHOOK_URL']
URL_BASE = _cfg['URL_BASE']
HTS_ID = _cfg['HTS_ID'] 

# 공용 변수 설정 
shared_vars.URL_BASE = URL_BASE
shared_vars.APP_KEY = APP_KEY
shared_vars.APP_SECRET = APP_SECRET
shared_vars.HTS_ID = HTS_ID
shared_vars.DISCORD_WEBHOOK_URL = DISCORD_WEBHOOK_URL
shared_vars.CANO = CANO
shared_vars.ACNT_PRDT_CD = ACNT_PRDT_CD

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

# 자동매매 시작
try:

    ACCESS_TOKEN = get_access_token() # Access 토큰 조회

    shared_vars.df_fav_stocks = get_group_stocks('001') # 000 관심그룹 조회
    symbol_list = shared_vars.df_fav_stocks.code.values # 000 관심그룹 종목코드 리스트 
    init_buy_amount = 500 
    total_cash = get_balance() # 보유 현금 조회
    stock_dict = get_stock_balance(True) # 보유 주식 조회
    df_pending_orders = get_pending_orders() # 당일자 미체결 주문 내역 조회 
    bought_list = [] # 매수 완료된 종목 리스트)

    for sym in stock_dict.keys():
        bought_list.append(sym)

    target_buy_count = 2 # 매수할 종목 수
    # buy_amount = total_cash * buy_percent  # 종목별 주문 금액 계산
    # soldout = False

    #오늘자 초기 매수 종목 count

    shared_vars.init_bought_count = query_today_init_cnt()

    send_message("=== 자동매매 시작 ===")  
    
    while True:
        # send_message("=== While 문 진입 ===")
        t_now = datetime.datetime.now()
        t_start = t_now.replace(hour=23, minute=20, second=0, microsecond=0)
        t_mid_night = t_now.replace(hour=23, minute=59, second=59, microsecond=99)
        t_exit = t_now.replace(hour=6, minute=00, second=0,microsecond=0)
        today = (datetime.datetime.today()-timedelta(days=1)).weekday()
        if today == 6:  # 토요일이나 일요일이면 자동 종료
            send_message("주말이므로 프로그램을 종료합니다.")
            break
            
        # buy_init_stocks()
        if (t_start < t_now < t_mid_night) or (t_now < t_exit) :  # AM 09:05 ~ PM 03:15 : 매수
            
            print('===== 매수로직 진입 ======')
            # daily 매수 최대 종목수까지 매수 처리 
            if shared_vars.init_bought_count <= target_buy_count: 
                                
                #최초 매수 처리 
                buy_init_stocks(init_buy_amount)

            else:
                print('=== 초기 매수 종목 초과 ===')

            # 추가 매수 처리. 이익금이 구간별 threshhold를 초과하면 추가 매수.
            df_return = update_holding_stock_details()
            # print(f'update_holding_stock_details return : {df_return.loc[df_return.buy_on_up_flag == True]}')
            buy_on_profit(df_return.loc[df_return.buy_on_up_flag == True], shared_vars.df_up_buy_table)

            
            # 보유주식이 있는 경우만 손절 함수 호출 
            if len(shared_vars.l_curr_stock) > 0: 
                # print('======= 손절로직 진입 =========')
                losscut_sell()
                # print('======= 익절로직 진입 =========')
                profitcut_sell()
        
            time.sleep(50)

            # 10분 마다 잔액, 관심종목 확인 
            if t_now.minute % 5 == 0  : 
                send_message("=== 보유/관심종목 조회 ===")
                get_balance() # 보유 현금 조회
                get_stock_balance(True)
                shared_vars.df_fav_stocks = get_group_stocks('001') # 000 관심그룹 조회
                symbol_list = shared_vars.df_fav_stocks.code.values # 000 관심그룹 종목코드 리스트 
                # time.sleep(50)
            # send_message("프로그램을 종료합니다.")
            # break
        
        # 06시 이후 프로그램 종료 
        elif (t_start - timedelta(hours=16) > t_now ) and (t_now > t_exit) :  
            print(f't_start:{t_start}')
            print(f't_now:{t_now}')
            print(f't_exit:{t_exit}')
            send_message("거래시간이 아니므로 프로그램을 종료합니다.") 
            break            
        else: # 거래시간 외 프로그램 종료
            send_message("장 시작 전이므로 1분 sleep ") 
            time.sleep(60)
    
except Exception as e:
    send_message(f"[오류 발생]{e}")
    if e == 'access_token': 
        headers = {"content-type":"application/json"}
        body = {"grant_type":"client_credentials",
        "appkey":APP_KEY, 
        "appsecret":APP_SECRET}
        PATH = "oauth2/tokenP"
        URL = f"{URL_BASE}/{PATH}"
        res = requests.post(URL, headers=headers, data=json.dumps(body))
        ACCESS_TOKEN = res.json()["access_token"]
        save_access_token(ACCESS_TOKEN)
    time.sleep(1)