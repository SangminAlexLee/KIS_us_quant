from sqlalchemy import create_engine
import pymysql
import pandas as pd
import requests
import json
import yaml
import time
from datetime import date, timedelta
import shared_vars
from KIS_Functions import *
import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

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

shared_vars.URL_BASE = URL_BASE
shared_vars.APP_KEY = APP_KEY
shared_vars.APP_SECRET = APP_SECRET
shared_vars.HTS_ID = HTS_ID
shared_vars.DISCORD_WEBHOOK_URL = DISCORD_WEBHOOK_URL
shared_vars.CANO = CANO
shared_vars.ACNT_PRDT_CD = ACNT_PRDT_CD

def save_access_token(token):
    """토큰 DB 저장"""

    engine, con, mycursor = db_conn()
    try : 
        sql = f"delete from kis_token where pgm_name = '{shared_vars.pgm_name}'"
        print(f'token sql : {sql}')
        mycursor.execute(sql)
        result = mycursor.fetchall()
        con.commit()
        con.close()
    except Exception as e:
        print(f"[Access token DB 삭제 오류 발생]{e}")
        time.sleep(1)

    try : 
        df_token = pd.DataFrame([[token, datetime.datetime.now(),shared_vars.pgm_name ]], columns=['token', 'timestamp', 'pgm_name'])
        engine, con, mycursor = db_conn()
        df_token.to_sql(name = 'kis_token', con=engine, if_exists='append', index=False)
        con.close()
    except Exception as e:
        print(f"[Access token DB 저장 오류 발생]{e}")
        time.sleep(1)

def get_access_token():
    """토큰 발급"""

    # DB에서 토큰 가져오기 
    try: 
        engine, con, mycursor = db_conn()
        sql = f"select * from kis_token where pgm_name = '{shared_vars.pgm_name}'"
        print(f'token sql : {sql}')
        mycursor.execute(sql)
        result = mycursor.fetchall()
        con.close()
        df_kis_token = pd.DataFrame(result)

        if datetime.datetime.now() - df_kis_token.timestamp[0] < timedelta(hours=22):
            print('token from DB is valid')
            ACCESS_TOKEN = df_kis_token.token[0]
            shared_vars.ACCESS_TOKEN = ACCESS_TOKEN
            return ACCESS_TOKEN
        else:
            print('token expired')

    except Exception as e:
        print(f"[DB Access token 오류 발생]{e}")
        time.sleep(1)
        
    headers = {"content-type":"application/json"}
    body = {"grant_type":"client_credentials",
    "appkey":shared_vars.APP_KEY, 
    "appsecret":shared_vars.APP_SECRET}
    PATH = "oauth2/tokenP"
    URL = f"{shared_vars.URL_BASE}/{PATH}"
    res = requests.post(URL, headers=headers, data=json.dumps(body))
    print(f'res in access token : {res}')
    ACCESS_TOKEN = res.json()["access_token"]
    print(ACCESS_TOKEN)
    shared_vars.ACCESS_TOKEN = ACCESS_TOKEN
    save_access_token(ACCESS_TOKEN)

    return 
    
def send_message(msg):
    """디스코드 메세지 전송"""
    now = datetime.datetime.now()
    message = {"content": f"[{now.strftime('%b.%d %H:%M')}] {str(msg)}"}
    requests.post(shared_vars.DISCORD_WEBHOOK_URL, data=message)
    print(message)


def update_stock_balance():

    if check_holiday() == False:
        print(f'휴일 이므로 업데이트 생략')
        return False
        
    """주식 잔고조회"""
    PATH = "uapi/domestic-stock/v1/trading/inquire-balance"
    URL = f"{shared_vars.URL_BASE}/{PATH}"
    headers = {"Content-Type":"application/json", 
        "authorization":f"Bearer {shared_vars.ACCESS_TOKEN}",
        "appKey":shared_vars.APP_KEY,
        "appSecret":shared_vars.APP_SECRET,
        "tr_id":"TTTC8434R",
        "custtype":"P",
    }
    params = {
        "CANO": shared_vars.CANO,
        "ACNT_PRDT_CD": shared_vars.ACNT_PRDT_CD,
        "AFHR_FLPR_YN": "N",
        "OFL_YN": "",
        "INQR_DVSN": "02",
        "UNPR_DVSN": "01",
        "FUND_STTL_ICLD_YN": "N",
        "FNCG_AMT_AUTO_RDPT_YN": "N",
        "PRCS_DVSN": "01",
        "CTX_AREA_FK100": "",
        "CTX_AREA_NK100": ""
    }
    # print(f'Header : {headers}')
    # print(f'Params : {params}')
    # print(f'URL : {URL}')
    res = requests.get(URL, headers=headers, params=params)
    stock_list = res.json()['output1']
    evaluation = res.json()['output2']
    df_stock_list = pd.DataFrame(stock_list)

    if len(df_stock_list) == 0:
        print('보유종목 없음')
        send_message('[Daily 잔고 업데이트] 보유종목 없음')
        return False

    df_stock_list['h_date'] = date.today() 
    df_stock_list = df_stock_list[shared_vars.holding_hist_columns]
    print('===== df_stock_list ====')
    print(df_stock_list)
    # send_message(f"====주식 보유잔고====")

    engine, con, mycursor = db_conn()
    df_stock_list.to_sql(name = 'holding_hist_kr', con=engine, if_exists='append', index=False)
    con.close()
    send_message(f"[Daily 잔고 업데이트] {len(df_stock_list)}건")
    return True

def check_holiday():
    """휴장일 조회"""
    q_date = (date.today()-timedelta(days=2))

    print(f'q_date:{q_date}')
    engine, con, mycursor = db_conn()
    sql = f"select is_public_holiday from holidays where date = '{q_date}' and country_code = 'US'"
    print(f'token sql : {sql}')
    mycursor.execute(sql)
    result = mycursor.fetchall()
    con.close()
    try: 
        is_holiday = result[0]['is_public_holiday']
        print(f'is_holiday : {is_holiday}')
        if is_holiday == 1:  
            return True
        else:
            return False
    except Exception as e:
        print(f'[Exception] holiday check from DB failed with {e}')
        return False



def get_buysell_hist(start_date='', end_date=''):
    """주식 체결 내역 조회"""

    if (start_date == '') or (len(start_date) != 8):
        start_date = date.today().strftime('%Y%m%d')
    if (end_date == '') or (len(end_date) != 8):
        end_date = date.today().strftime('%Y%m%d')

    PATH = "uapi/domestic-stock/v1/trading/inquire-daily-ccld"
    URL = f"{shared_vars.URL_BASE}/{PATH}"
    headers = {"Content-Type":"application/json", 
        "authorization":f"Bearer {shared_vars.ACCESS_TOKEN}",
        "appKey":shared_vars.APP_KEY,
        "appSecret":shared_vars.APP_SECRET,
        "tr_id":"TTTC8001R"
    }
    params = {
        "CANO"              : shared_vars.CANO,
        "ACNT_PRDT_CD"      : shared_vars.ACNT_PRDT_CD,
        "INQR_STRT_DT"      : start_date,
        "INQR_END_DT"       : end_date,
        "SLL_BUY_DVSN_CD"   : "00",
        "INQR_DVSN"         : "00",
        "PDNO"              : "",
        "CCLD_DVSN"         : "01", # 채결구분, 01 : 체결
        "ORD_GNO_BRNO"      : "",
        "ODNO"              : "",
        "INQR_DVSN_3"       : "00",
        "INQR_DVSN_1"       : "",
        "CTX_AREA_FK100"    : "",
        "CTX_AREA_NK100"    : ""
    }
    res = requests.get(URL, headers=headers, params=params)
    time.sleep(0.1)

    df_buysell_hist = pd.DataFrame(res.json()['output1'])

    if len(df_buysell_hist) == 0:
        print('매매 종목 없음')
        send_message('[Daily 매매 업데이트] 매매 종목 없음')
        return df_buysell_hist

    df_buysell_hist = df_buysell_hist[shared_vars.buysell_hist_columns]

    print(df_buysell_hist)

    engine, con, mycursor = db_conn()
    df_buysell_hist.to_sql(name = 'buysell_hist_kr', con=engine, if_exists='append', index=False)
    con.close()
    send_message(f"[Daily 매매 업데이트] {len(df_buysell_hist)}건")
    
    return df_buysell_hist

try:

    print("Daily Batch 시작")
    send_message("[Daily Batch] 시작 ")

    ACCESS_TOKEN = get_access_token()

    print(f'check_holiday(): {check_holiday()}')

    # if check_holiday():
    # # if True:
    #     send_message("[Daily Batch] 영업일 ")

    #     # holding_hist_kr(잔고) 이력 테이블 업데이트 
    #     update_stock_balance()
        
    #     # buysell_hist_kr(메매) 이력 테이블 업데이트 
    #     df_orders = get_buysell_hist()
    #     # df_orders.to_excel('order_hist.xlsx')
    # else:
    #     print('영업일 아님')
    #     send_message("[Daily Batch] 영업일 아님 ")

except Exception as e:
    # print(f"[오류 발생]{e}")
    print(f'오류발생 : {e}')
    send_message(f'[Daily Batch] 오류발생 : {e}')
    # time.sleep(1)