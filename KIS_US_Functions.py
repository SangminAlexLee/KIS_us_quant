from sqlalchemy import create_engine
import pymysql
import pandas as pd
from datetime import date
import requests
import json
import yaml
import time
import datetime
from datetime import timedelta, date
import shared_vars
import numpy as np

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

def get_with_retry(url, headers, params, max_retries=3, timeout=5):
    """ """
    retries = 0

    while retries < max_retries:
        try:
            response = requests.get(url, headers=headers, params=params)
            # HTTP 상태 코드가 500 이상인 경우
            if response.status_code >= 500:
                print(f"500 오류 발생. 재시도 중... ({retries + 1}/{max_retries})")
                retries += 1
                time.sleep(timeout)  # 재시도 전 대기
            else:
                return response  # 성공적인 응답
        except requests.exceptions.RequestException as e:
            print(f"예외 발생: {e}. 재시도 중... ({retries + 1}/{max_retries})")
            retries += 1
            time.sleep(timeout)  # 재시도 전 대기

    print("최대 재시도 횟수를 초과했습니다. 요청 실패.")
    send_message("최대 재시도 횟수를 초과했습니다. 요청 실패.")
    return None  # 재시도 후에도 실패한 경우

## 3. 관심종목 그룹별 종목 조회
def get_group_stocks(interest_group):
    PATH = "/uapi/domestic-stock/v1/quotations/intstock-stocklist-by-group"
    URL_BASE = shared_vars.URL_BASE
    URL = f"{URL_BASE}/{PATH}"
    # print(f'URL : {URL}')
    params = {
        "TYPE": "1",
        "USER_ID": shared_vars.HTS_ID,
        "DATA_RANK": "",
        "INTER_GRP_CODE": interest_group,
        "INTER_GRP_NAME": "",
        "HTS_KOR_ISNM": "",                 
        "CNTG_CLS_CODE": "",
        "FID_ETC_CLS_CODE": "4"
    }
    headers = {
        "content-type": "application/json",
        "authorization": f"Bearer {shared_vars.ACCESS_TOKEN}",
        "appKey": shared_vars.APP_KEY,
        "appSecret": shared_vars.APP_SECRET,
        "tr_id": "HHKCM113004C6",  # 실전투자
        "custtype": "P"
    }
    time.sleep(0.05) # 유량제한 예방 (REST: 1초당 20건 제한)
    # print(f'headers : {headers}')
    # print(f'params : {params}')
    res = get_with_retry(URL, headers=headers, params=params)
    print(f'res : {res}')
    s_list = []
    for i in res.json()['output2']:
        s_list.append((i['jong_code'],i['hts_kor_isnm']) )
    df_list = pd.DataFrame(s_list, columns=['code','kor_name'])
    print(df_list)
    return df_list

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

def hashkey(datas):
    """암호화"""
    PATH = "uapi/hashkey"
    URL = f"{shared_vars.URL_BASE}/{PATH}"
    headers = {
    'content-Type' : 'application/json',
    'appKey' : shared_vars.APP_KEY,
    'appSecret' : shared_vars.APP_SECRET,
    }
    res = requests.post(URL, headers=headers, data=json.dumps(datas))
    hashkey = res.json()["HASH"]
    return hashkey

def get_stock_balance(msg_send=False):
    """주식 잔고조회"""
    PATH = "/uapi/overseas-stock/v1/trading/inquire-balance"
    URL = f"{shared_vars.URL_BASE}/{PATH}"
    headers = {"Content-Type":"application/json", 
        "authorization":f"Bearer {shared_vars.ACCESS_TOKEN}",
        "appKey":shared_vars.APP_KEY,
        "appSecret":shared_vars.APP_SECRET,
        "tr_id":"TTTS3012R",
        "custtype":"P",
    }
    params = {
        "CANO": shared_vars.CANO,
        "ACNT_PRDT_CD": shared_vars.ACNT_PRDT_CD,
        "OVRS_EXCG_CD": "NASD", # NASD:미국주식 전체, NAS : 나스닥, NYSE : 뉴욕, AMEX : 아멕스
        "TR_CRCY_CD": "USD",
        "CTX_AREA_FK200": "",
        "CTX_AREA_NK200": ""
    }
    # print(f'Header : {headers}')
    # print(f'Params : {params}')
    # print(f'URL : {URL}')
    res = get_with_retry(URL, headers=headers, params=params)
    stock_list = res.json()['output1']
    evaluation = res.json()['output2']
    print(f'evaluation : {evaluation}')
    stock_dict = {}
    if msg_send:    
        send_message(f"====주식 보유잔고====")
    for stock in stock_list:
        if int(stock['ovrs_cblc_qty']) > 0:
            stock_dict[stock['ovrs_pdno']] = stock['ovrs_cblc_qty']
            amt = round(float(stock['ovrs_stck_evlu_amt']), 1)
            pf_rt = round(float(stock['evlu_pfls_rt']), 1)
            if msg_send:    
                send_message(f"{stock['ovrs_item_name'][:7]}: {amt},{pf_rt}%")
            time.sleep(0.1)
    if msg_send:    
        print(f'evaluation before send_message(): {evaluation}')
        send_message(f"주식 평가 금액: {evaluation['frcr_pchs_amt1']}")
        time.sleep(0.1)
        send_message(f"평가 손익 합계: {evaluation['ovrs_tot_pfls']}")
        time.sleep(0.1)
        send_message(f"총 평가 금액: {evaluation['frcr_buy_amt_smtl1']}")
        time.sleep(0.1)
        send_message(f"=================")
    shared_vars.l_curr_stock = stock_dict.keys() 
    return stock_dict

def get_target_price(code="005930", option='None'):
    # print('현재시세 조회')
    engine, con, mycursor = db_conn() 
    sql = f"""select market 
                from us_stock_list 
                where Symbol = '{code}'"""
    print(f'sql in get_target_price(): {sql}')
    mycursor.execute(sql)
    result = mycursor.fetchall()
    con.close()
    market = result[0]['market']
    print(f'market : {market}')
    if market == 'NASDAQ':
        EXCD = 'NAS'
    elif market == 'NYSE':
        EXCD = 'NYS'
    elif market == 'AMEX':
        EXCD = 'AMS'
    else:
        send_message(f"[주가 조회 실퍠][{code}, 해외거래소코드 불명")
        return False

    PATH = "/uapi/overseas-price/v1/quotations/price"
    URL = f"{shared_vars.URL_BASE}/{PATH}"
    headers = {"Content-Type":"application/json", 
        "authorization": f"Bearer {shared_vars.ACCESS_TOKEN}",
        "appKey":shared_vars.APP_KEY,
        "appSecret":shared_vars.APP_SECRET,
        "tr_id":"HHDFS00000300"}
    params = {
    "AUTH":"",
    "EXCD":EXCD,
    "SYMB":code
    }
    res = get_with_retry(URL, headers=headers, params=params)

    stck_stat = res.json()['output']['ordy'] #매수주문 가능 종목 여부
    stck_prpr = float(res.json()['output']['last']) # 현재가 
    print(f'현재가 : {stck_prpr}')

    return stck_prpr

def get_balance(code='AAPL'):

    symbol = code
    engine, con, mycursor = db_conn() 
    sql = f"""select market 
                from us_stock_list 
                where Symbol = '{symbol}'"""
    print(f'sql: {sql}')
    mycursor.execute(sql)
    result = mycursor.fetchall()
    con.close()
    market = result[0]['market']
    print(f'market : {market}')
    if market == 'NASDAQ':
        OVRS_EXCG_CD = 'NASD'
    elif market == 'NYSE':
        OVRS_EXCG_CD = 'NYSE'
    elif market == 'AMEX':
        OVRS_EXCG_CD = 'AMEX'
    else:
        send_message(f"[주문 실퍠][{code}, 해외거래소코드 불명")
        return False

    """현금 잔고조회"""
    PATH = "/uapi/overseas-stock/v1/trading/inquire-psamount"
    URL = f"{shared_vars.URL_BASE}/{PATH}"
    headers = {"Content-Type":"application/json", 
        "authorization":f"Bearer {shared_vars.ACCESS_TOKEN}",
        "appKey":shared_vars.APP_KEY,
        "appSecret":shared_vars.APP_SECRET,
        "tr_id":"TTTS3007R"
    }
    params = {
        "CANO": shared_vars.CANO,
        "ACNT_PRDT_CD": shared_vars.ACNT_PRDT_CD,
        "OVRS_EXCG_CD": OVRS_EXCG_CD,
        "OVRS_ORD_UNPR": "1",
        "ITEM_CD": symbol
    }
    print(f'headers: {headers}')
    print(f'params: {params}')
    res = get_with_retry(URL, headers=headers, params=params)
    print(f'res.json() : {res.json()}')
    cash = res.json()['output']['ord_psbl_frcr_amt']
    send_message(f"주문 가능 현금 잔고: {cash} USD")
    return float(cash)


def buy(code="005930", qty="1",price="0", option='00'):
    """주식 시장가 매수"""  

    symbol = code
    engine, con, mycursor = db_conn() 
    sql = f"""select market 
                from us_stock_list 
                where Symbol = '{symbol}'"""
    print(f'sql in buy() : {sql}')
    mycursor.execute(sql)
    result = mycursor.fetchall()
    con.close()
    market = result[0]['market']
    print(f'market : {market}')
    if market == 'NASDAQ':
        OVRS_EXCG_CD = 'NASD'
    elif market == 'NYSE':
        OVRS_EXCG_CD = 'NYSE'
    elif market == 'AMEX':
        OVRS_EXCG_CD = 'AMEX'
    else:
        send_message(f"[주문 실퍠][종목코드 : {code}, 해외거래소코드 불명")
        return False

    print(f'price for buy order : {str(float(price))}')

    PATH = "/uapi/overseas-stock/v1/trading/order"
    URL = f"{shared_vars.URL_BASE}/{PATH}"
    data = {
        "CANO": shared_vars.CANO,
        "ACNT_PRDT_CD": shared_vars.ACNT_PRDT_CD,
        "OVRS_EXCG_CD": OVRS_EXCG_CD,
        "PDNO": code,
        "ORD_DVSN": "00",
        "ORD_QTY": str(int(qty)),
        "OVRS_ORD_UNPR": f"{round(float(price),2)}",
        "SLL_TYPE": "",
        "ORD_SVR_DVSN_CD": "0"

    }
    headers = {"Content-Type":"application/json", 
        "authorization":f"Bearer {shared_vars.ACCESS_TOKEN}",
        "appKey":shared_vars.APP_KEY,
        "appSecret":shared_vars.APP_SECRET,
        "tr_id":"JTTT1002U",
        "custtype":"P",
        "hashkey" : hashkey(data)
    }
    print(f'data : {data}')
    print(f'headers : {headers}')
    res = requests.post(URL, headers=headers, data=json.dumps(data))
    # print(f'[주문 상세] 종목코드 : {code}, 수량 : {qty}, 매수가 : {price}')
    if res.json()['rt_cd'] == '0':
        # send_message(f"[주문 성공]{str(res.json())}")
        send_message(f"[주문 성공][종목코드 : {code}, 수량 : {qty}, 매수가 : {price}")
        return True
    else:

        if shared_vars.lack_of_cash_flag == False:
            print(f'주문 실패 : {res.json()}')
            send_message(f"[주문 실패]{str(res.json()['msg1'])}")
            send_message(f"[주문 실패][종목코드 : {code}, 수량 : {qty}, 매수가 : {price}")
        #잔액 부족 발생시 flag 켜서 이후 메시지 send 방지
        if str(res.json()['msg_cd']) == 'APBK0952': 
            shared_vars.lack_of_cash_flag = True
                
        return False

def sell(code="005930", qty="1", price=0 ):
    """주식 지정가(미국) 매도"""
    print(f'sell {code} {qty} stocks at {price}')
    symbol = code
    engine, con, mycursor = db_conn() 
    sql = f"""select market 
                from us_stock_list 
                where Symbol = '{symbol}'"""
    print(f'sql in sell: {sql}')
    mycursor.execute(sql)
    result = mycursor.fetchall()
    con.close()
    market = result[0]['market']
    print(f'market : {market}')
    if market == 'NASDAQ':
        OVRS_EXCG_CD = 'NASD'
    elif market == 'NYSE':
        OVRS_EXCG_CD = 'NYSE'
    elif market == 'AMEX':
        OVRS_EXCG_CD = 'AMEX'
    else:
        send_message(f"[주문 실퍠][종목코드 : {code}, 해외거래소코드 불명")
        return False

    # print('in sell()')
    PATH = "/uapi/overseas-stock/v1/trading/order"
    URL = f"{shared_vars.URL_BASE}/{PATH}"
    data = {
        "CANO": shared_vars.CANO,
        "ACNT_PRDT_CD": shared_vars.ACNT_PRDT_CD,
        "OVRS_EXCG_CD": OVRS_EXCG_CD,
        "PDNO": code,
        "ORD_DVSN": "00",
        "ORD_QTY": str(int(qty)),
        "OVRS_ORD_UNPR": f"{round(price,2)}",
        "SLL_TYPE": "00",
        "ORD_SVR_DVSN_CD": "0"

    }
    headers = {"Content-Type":"application/json", 
        "authorization":f"Bearer {shared_vars.ACCESS_TOKEN}",
        "appKey":shared_vars.APP_KEY,
        "appSecret":shared_vars.APP_SECRET,
        "tr_id":"TTTT1006U",
        "custtype":"P",
        "hashkey" : hashkey(data)
    }

    res = requests.post(URL, headers=headers, data=json.dumps(data))
    print(f'res in sell() : {res}')
    if res.json()['rt_cd'] == '0':
        send_message(f"[매도 성공]{str(res.json())}")
        shared_vars.lack_of_cash_flag = False
        get_stock_balance()
        return True
    else:
        send_message(f"[매도 실패]{str(res.json())}")
        return False

# 최초 매수할 종목 리스트 제공 함수
def get_list_for_init_buy():

    # 미체결 거래 조회
    get_pending_orders()

    # 보유종목 조회 
    get_stock_balance()
    
    # 관심종목에서 이미 보유하고 있는 종목 제외
    list_to_buy = [item for item in shared_vars.df_fav_stocks.code.values if item not in shared_vars.l_curr_stock and item not in shared_vars.df_pending_orders.pdno.values]

    return list_to_buy

# 최초 매수 함수 
def buy_init_stocks(init_amt=500):
    
    #매수 대상 종목 조회
    l_init_buy = get_list_for_init_buy()

    print(f'list to buy in init buy : {l_init_buy}')

    # send_message(f"==== 초기 매수 {len(l_init_buy)}종목 ====")

    if shared_vars.lack_of_cash_flag == True:
        print('잔액 부족. 매수 처리 skip')
        return False

    for stock_code in l_init_buy: 

        #1달 전 수익륙
        end_date = date.today().strftime('%Y%m%d')
        start_date = (date.today() - timedelta(days=30)).strftime('%Y%m%d')
        return_30d = get_stock_return(stock_code, start_date, end_date)
        
        #1주일 전 수익률
        start_date = (date.today() - timedelta(days=7)).strftime('%Y%m%d')
        return_7d = get_stock_return(stock_code, start_date, end_date)

        kor_name = shared_vars.df_fav_stocks[shared_vars.df_fav_stocks.code == stock_code].kor_name.values

        print(f"[{stock_code}]{kor_name} 30d return : {return_30d['ret']}, 7d return : {return_7d['ret']}")
        if (return_30d['ret'] == None ) or (return_7d['ret'] == None) :
            send_message(f"[신규 편입]{kor_name} 기간 수익 누락")
            continue

        if (return_30d['ret'] < 10) or (return_7d['ret'] < 5):
            # print('not enough performance to buy')
            continue

        # 2호가 아래 매수 가격 조회        
        target_price = get_target_price(code=stock_code)
    
        if target_price > init_amt:
            send_message(f"[매수 금액 초과] 종목 : {stock_code}, 매수가격 : {target_price}, 매수금액 : {init_amt}")
            buy_qty = 1
        else: 
            buy_qty = 0  # 매수할 수량 초기화
            buy_qty = int(init_amt // target_price)

        # 매수 주문 성공시 init_bought_count 1 증가 
        if buy(stock_code, buy_qty, target_price, '00'): # 매수주문
            send_message(f"[신규 편입] {stock_code}종목 매수 성공")
            shared_vars.init_bought_count = shared_vars.init_bought_count + 1
        else:
            send_message(f"[신규 편입] {stock_code} 매수 실패")

def get_pending_orders(buysell_flag="00"):
    """주식 미완료 주문 조회"""
    print('get_pending_orders()')
    PATH = "/uapi/overseas-stock/v1/trading/inquire-nccs"
    URL = f"{shared_vars.URL_BASE}/{PATH}"
    headers = {"Content-Type":"application/json", 
        "authorization":f"Bearer {shared_vars.ACCESS_TOKEN}",
        "appKey":shared_vars.APP_KEY,
        "appSecret":shared_vars.APP_SECRET,
        "tr_id":"TTTS3018R"
    }
    params = {
        "CANO": shared_vars.CANO,
        "ACNT_PRDT_CD"  : shared_vars.ACNT_PRDT_CD,
        "OVRS_EXCG_CD"  : "NASD",    # 미국 거래소 전체 
        "SORT_SQN"      : "DS",     # 정렬순서 - DS : 정순
        "CTX_AREA_FK200": "", 
        "CTX_AREA_NK200": ""
    }
    res = get_with_retry(URL, headers=headers, params=params)
    print(f'res in pending orders : {res}')
    print(f'res.json in {res.json()}')
    time.sleep(0.1)

    shared_vars.df_pending_orders = pd.DataFrame(res.json()['output'])
    print(f'shared_vars.df_pending_orders: {shared_vars.df_pending_orders}')

    # 조회내역이 없는 경우 빈 데이터프레임 생성함. 
    if len(shared_vars.df_pending_orders) == 0:
        shared_vars.df_pending_orders = pd.DataFrame(columns=['ord_gno_brno', 'odno', 'orgn_odno', 'pdno',
       'prdt_name', 'ft_ord_qty', 'ft_ord_unpr3', 'ord_tmd','ovrs_excg_cd'])
        df_pending = shared_vars.df_pending_orders
    else:
        
        df_pending = shared_vars.df_pending_orders
        if buysell_flag == "01": # 매도
            df_pending = df_pending[df_pending.sll_buy_dvsn_cd == "01"]
        elif buysell_flag == "02": # 매수
            df_pending = df_pending[df_pending.sll_buy_dvsn_cd == "02"]

    print(f'df_pending: {df_pending}')

    return df_pending

def get_buysell_hist(start_date='', end_date=''):
    """주식 체결 내역 조회"""

    if (start_date == '') or (len(start_date) != 8):
        start_date = datetime.today().strftime('%Y%m%d')
    if (end_date == '') or (len(end_date) != 8):
        end_date = datetime.today().strftime('%Y%m%d')

    PATH = "/uapi/overseas-stock/v1/trading/inquire-ccnl"
    URL = f"{shared_vars.URL_BASE}/{PATH}"
    headers = {"Content-Type":"application/json", 
        "authorization":f"Bearer {shared_vars.ACCESS_TOKEN}",
        "appKey":shared_vars.APP_KEY,
        "appSecret":shared_vars.APP_SECRET,
        "tr_id":"TTTS3035R"
    }
    params = {
        "CANO"              : shared_vars.CANO,
        "ACNT_PRDT_CD"      : shared_vars.ACNT_PRDT_CD,
        "PDNO"              : "%",
        "ORD_STRT_DT"       : start_date,
        "ORD_END_DT"        : end_date,
        "SLL_BUY_DVSN"      : "00",     # 매도매수구분 - 00 : 전체
        "CCLD_NCCS_DVSN"    : "00",     # 체결미체결 구분 - 00 : 전체
        "OVRS_EXCG_CD"      : "NASD",   # 해외거래소코드 - NASD : 미국시장 전체(나스닥, 뉴욕, 아멕스)
        "SORT_SQN"          : "DS",     # 정렬순서 - DS : 정순
        "ORD_DT"            : "",
        "ORD_GNO_DT"        : "",
        "ODNO"              : "",
        "CTX_AREA_FK100"    : "",
        "CTX_AREA_NK100"    : ""
    }
    res = get_with_retry(URL, headers=headers, params=params)
    time.sleep(0.1)

    print(f'res : {res}')
    print(f'res.json() : {res.json()}')

    df_buysell_hist = pd.DataFrame(res.json()['output1'])

    print(df_buysell_hist)
    
    return df_buysell_hist


def losscut_sell(losscut_ratio=-10):
    """주식 잔고조회"""
    PATH = "/uapi/overseas-stock/v1/trading/inquire-balance"
    URL = f"{shared_vars.URL_BASE}/{PATH}"
    headers = {"Content-Type":"application/json", 
        "authorization":f"Bearer {shared_vars.ACCESS_TOKEN}",
        "appKey":shared_vars.APP_KEY,
        "appSecret":shared_vars.APP_SECRET,
        "tr_id":"TTTS3012R",
        "custtype":"P",
    }
    params = {
        "CANO": shared_vars.CANO,
        "ACNT_PRDT_CD": shared_vars.ACNT_PRDT_CD,
        "OVRS_EXCG_CD": "NASD", # NASD:미국주식 전체, NAS : 나스닥, NYSE : 뉴욕, AMEX : 아멕스
        "TR_CRCY_CD": "USD",
        "CTX_AREA_FK200": "",
        "CTX_AREA_NK200": ""
    }
    # print(f'Header : {headers}')
    # print(f'Params : {params}')
    # print(f'URL : {URL}')
    res = get_with_retry(URL, headers=headers, params=params)
    stock_list = res.json()['output1']
    stock_dict = {}
    # send_message(f"====== 손절 확인({losscut_ratio}%) =====")

    for stock in stock_list:
        code = stock['ovrs_pdno']
        profit_ratio = float(stock['evlu_pfls_rt'])
        stock_name = stock['ovrs_item_name']
        curr_price = stock['now_pric2']
        holding_qty = stock['ovrs_cblc_qty']
        print(f'{stock_name} 수익률 : {profit_ratio}')
        if profit_ratio < losscut_ratio:
            stock_dict[code] = profit_ratio
            send_message(f"[손절 종목]{stock_name}({code}) 수익률: {profit_ratio}")
            time.sleep(0.1)
            if sell(code, holding_qty, curr_price):
                delete_stock_from_table(shared_vars.holding_db_table, code)
    
    # send_message(f"================================")
    stock_dict.keys()

    return stock_dict


def get_tier_ratio( amount, tier_t ):

        for index, row in tier_t.iterrows():
                
                # print(index)
                tier = row['amount']
                ratio = row['buy_ratio']
                # print(f'in get_tier_ratio() : {tier}, {ratio}, {amount}')

                if (int(tier) >= int(amount)):
                        return ratio , index +1
        
        return 0


def update_holding_stock_details():
    # send_message(f"[최대 이익금액 업데이트]")
    """주식 잔고조회"""
    PATH = "/uapi/overseas-stock/v1/trading/inquire-balance"
    URL = f"{shared_vars.URL_BASE}/{PATH}"
    headers = {"Content-Type":"application/json", 
        "authorization":f"Bearer {shared_vars.ACCESS_TOKEN}",
        "appKey":shared_vars.APP_KEY,
        "appSecret":shared_vars.APP_SECRET,
        "tr_id":"TTTS3012R",
        "custtype":"P",
    }
    params = {
        "CANO": shared_vars.CANO,
        "ACNT_PRDT_CD": shared_vars.ACNT_PRDT_CD,
        "OVRS_EXCG_CD": "NASD", # NASD:미국주식 전체, NAS : 나스닥, NYSE : 뉴욕, AMEX : 아멕스
        "TR_CRCY_CD": "USD",
        "CTX_AREA_FK200": "",
        "CTX_AREA_NK200": ""
    }
    # print(f'Header : {headers}')
    # print(f'Params : {params}')
    # print(f'URL : {URL}')
    res = get_with_retry(URL, headers=headers, params=params)

    curr_account_stock_list = res.json()['output1'] 
    print(f'len(curr_account_stock_list) : {len(curr_account_stock_list)}')   
    df_curr_account_stock = pd.DataFrame(curr_account_stock_list)

    holding_stock_tbl_col_list= ['ovrs_pdno','ovrs_item_name','init_dt','now_pric2','ovrs_stck_evlu_amt','frcr_evlu_pfls_amt','evlu_pfls_rt', 'buy_on_up_flag']

    df_update_stock_list = pd.DataFrame(columns=holding_stock_tbl_col_list)
    df_update_stock_list_empty = df_update_stock_list  

    # 계좌에 주식이 없으면 빈 데이터프레임 리턴하고 종료 
    if len(df_curr_account_stock) == 0:
        return df_update_stock_list

    df_curr_account_stock = df_curr_account_stock[['ovrs_pdno','ovrs_item_name','now_pric2','ovrs_stck_evlu_amt','frcr_evlu_pfls_amt','evlu_pfls_rt']]
    print(f'df_curr_account_stock : {df_curr_account_stock}')

    try: 
        #DB 에서 보유 주식의 최고 수익율, 수익금액 조회 
        engine, con, mycursor = db_conn()
        sql = f"select * from {shared_vars.holding_db_table}"
        mycursor.execute(sql)
        result = mycursor.fetchall()
        con.close()
        df_holding_stock_details = pd.DataFrame(result)
    except Exception as e: 
        
        if e.args[0] == 1146:
            print('Table does not exist')
            
            df_curr_account_stock['init_dt'] = date.today()
            df_curr_account_stock['buy_on_up_flag'] = False
            df_curr_account_stock = df_curr_account_stock[holding_stock_tbl_col_list]
            engine, con, mycursor = db_conn()
            df_curr_account_stock.to_sql(name = shared_vars.holding_db_table, con=engine, if_exists='replace', index=False)
            con.close()
            df_update_stock_list = df_curr_account_stock
            send_message(f"[최대 이익금액 업데이트] {len(df_update_stock_list)} 종목 업데이트 됨")

            return df_update_stock_list_empty

    df_merged = df_curr_account_stock.merge(df_holding_stock_details,how='outer', on='ovrs_pdno', suffixes=('_a', '_b'))

    print(f'merged : {df_merged}')

    df_merged['profit_index_a'] = df_merged['frcr_evlu_pfls_amt_a'].apply(lambda x : get_tier_ratio(np.nan_to_num(x), shared_vars.df_up_buy_table)).apply(lambda x: x[1]).tolist()
    df_merged['profit_index_b'] = df_merged['frcr_evlu_pfls_amt_b'].apply(lambda x : get_tier_ratio(np.nan_to_num(x), shared_vars.df_up_buy_table)).apply(lambda x: x[1]).tolist()
    df_merged['buy_on_up_flag'] = df_merged['profit_index_a'] > df_merged['profit_index_b']

    # print(f'df_curr_account_stock desc : {df_curr_account_stock.dtypes}')
    # print(f'df_holding_stock_details desc : {df_holding_stock_details.dtypes}')
    # print(f'merged desc : {df_merged.dtypes}')

    # 현재가 기준 이익 금액이 holding_stock_details 테이블의 이익금액 보다 큰 종목과 이익금액 조회
    df_max_profit_for_update = df_merged[df_merged['frcr_evlu_pfls_amt_a'].astype(float) > df_merged['frcr_evlu_pfls_amt_b'].astype(float)][['ovrs_pdno', 'frcr_evlu_pfls_amt_a', 'buy_on_up_flag', 'now_pric2_a', 'ovrs_stck_evlu_amt_a', 'evlu_pfls_rt_a']]


    # 결과 출력
    print(f'df_max_profit_for_update: {df_max_profit_for_update}')

    engine, con, mycursor = db_conn()
    # 개별 종목의 holding_stock_details 의 최고 수익 금액 필드 업데이트 
    for index in df_max_profit_for_update.index:
        print(f'for {index} index')
        sql = f"""
        UPDATE {shared_vars.holding_db_table}
        SET frcr_evlu_pfls_amt = %s,
        now_pric2 = %s, 
        ovrs_stck_evlu_amt = %s, 
        evlu_pfls_rt = %s
        WHERE ovrs_pdno = %s
        """
        
        # 업데이트할 값
        evlu_pfls_amt = df_max_profit_for_update.loc[index, 'frcr_evlu_pfls_amt_a']
        ovrs_pdno = df_max_profit_for_update.loc[index, 'ovrs_pdno']
        now_pric2 = df_max_profit_for_update.loc[index, 'now_pric2_a']
        ovrs_stck_evlu_amt = df_max_profit_for_update.loc[index, 'ovrs_stck_evlu_amt_a']
        evlu_pfls_rt = df_max_profit_for_update.loc[index, 'evlu_pfls_rt_a']

        
        # print(sql % (evlu_pfls_amt, pdno))
        # 쿼리 실행
        mycursor.execute(sql, (evlu_pfls_amt, now_pric2, ovrs_stck_evlu_amt, evlu_pfls_rt, ovrs_pdno))
        
        # 변경 사항 커밋
        con.commit()
    con.close()

    # print(f'len(df_max_profit_for_update) : {len(df_max_profit_for_update)}')

    # 최대이익 증가 종목에 대해 init_dt 오늘자로 update
    if len(df_max_profit_for_update) > 0: 
        df_update_stock_list = df_curr_account_stock[df_curr_account_stock['ovrs_pdno'].isin(df_max_profit_for_update.ovrs_pdno)]
        df_update_stock_list.loc[:,'init_dt'] = date.today()
        df_update_stock_list['buy_on_up_flag'] = df_max_profit_for_update['buy_on_up_flag']
        df_update_stock_list = df_update_stock_list[holding_stock_tbl_col_list]
        # df_update_stock_list.loc[:,'buy_on_up_flag'] = df_max_profit_for_update['buy_on_up_flag']
        print(f'df_update_stock_list in here : {df_update_stock_list}')

    # 신규 편입된 종목에 대해서 holding_stock_details 에 레코드 insert
    for stock_code in df_merged[df_merged['frcr_evlu_pfls_amt_b'].isna()].ovrs_pdno  :
        print(f'New stock will be added to holding detail table : {stock_code}')
        dt_temp = df_curr_account_stock[df_curr_account_stock.ovrs_pdno == stock_code]
        dt_temp.loc[:,'init_dt'] = date.today()
        dt_temp.loc[:,'buy_on_up_flag'] = False
        dt_temp = dt_temp[holding_stock_tbl_col_list]
        engine, con, mycursor = db_conn()
        dt_temp.to_sql(name = shared_vars.holding_db_table, con=engine, if_exists='append', index=False)
        con.close()
        # df_update_stock_list['buy_on_up_flag'] = False
        df_update_stock_list = pd.concat([df_update_stock_list,dt_temp] )

    print(f'df_update_stock_list at the end : {df_update_stock_list}')

    if len(df_update_stock_list) > 0:

        for row in df_update_stock_list.iterrows():
            # print(f'row of df_update_stock_list: {row}')
            # print(f'type : {type(row)}')    
            # print(f'row[1] : {row[1]}')
            item_name = row[1]['ovrs_item_name']
            # print(f'kr_name: {kr_name}')
            pf_amt  = float(row[1]['frcr_evlu_pfls_amt'])
            send_message(f"[이익up][{item_name[:4]} : {round(pf_amt,2)}]")
    
    return df_update_stock_list

# 기간별 주식 수익률 & 수익률 표준편차 계산 함수
def get_stock_return(stock_code, start_date, end_date):

    print(f'will cal stock return for {stock_code} between {start_date} and {end_date}')
    engine, con, mycursor = db_conn() 

    query = f"""
        SELECT symbol, date, close
        FROM {shared_vars.stock_price_db_table}
        WHERE symbol = '{stock_code}' AND Date BETWEEN STR_TO_DATE('{start_date}', '%Y%m%d') AND STR_TO_DATE('{end_date}', '%Y%m%d')
    """
    print(f'query in get_stock_return:{query}')
    mycursor.execute(query)
    result = mycursor.fetchall()
    df_stock_price = pd.DataFrame(result)
    con.close()    

    print(f'query return : {df_stock_price}')

    if df_stock_price.empty:
        return {
            'code': stock_code,
            'start_date': start_date,
            'end_date': end_date,
            'ret': None,
            'daily_std': None
        }

    # 날짜 순으로 정렬
    df_stock_price.sort_values('date', inplace=True)

    # 상승률 계산
    start_price = df_stock_price.iloc[0]['close']

    if date.today().strftime('%Y%m%d') == end_date:
        print('end date equals to today')
        end_price = get_target_price(stock_code)
    else:
        end_price = df_stock_price.iloc[-1]['close']

    price_change_rate = ((end_price - start_price) / start_price) * 100

    print(f'[get_stock_return]start price:{start_price}, end price:{end_price}, return:{price_change_rate}')

    # 일별 주가 변동 표준편차 계산
    daily_price_std = df_stock_price['close'].pct_change().std()



    return {
        'code': stock_code,
        'start_date': start_date,
        'end_date': end_date,
        'ret': round(price_change_rate, 4),
        'daily_std': round(daily_price_std, 5)
    }

def get_tier_ratio( amount, tier_t ):

        for index, row in tier_t.iterrows():
                
                # print(index)
                tier = row['amount']
                ratio = row['buy_ratio']
                # print(f'in get_tier_ratio() : {tier}, {ratio}, {amount}')

                if (float(tier) >= float(amount)):
                        return ratio , index +1
        
        return 0
                
def buy_on_profit(stock_for_update, up_buy_t):

        buy_count = 0

        for index, row in stock_for_update.iterrows():
                print(f'======== ratio check ===========')
                print(f'pdno : {row.ovrs_pdno}')
                profit_amt = row.frcr_evlu_pfls_amt
                print(f'profit_amt: {profit_amt}')
                ratio, index = get_tier_ratio(profit_amt, up_buy_t)
                print(f'ratio : {ratio}')
                print(f'index : {index}')
                print(f'evlu_amt : {row.ovrs_stck_evlu_amt }')
                print(f'ratio/100 : {ratio/100}')
                buy_amt = float(float(row.ovrs_stck_evlu_amt) * (ratio/100))
                print(f'buy amount : {buy_amt}')
                buy_qty = max(int(buy_amt // float(row.now_pric2)), 1)
                print(f'buy quantaty : {buy_qty}')

                if buy(row.ovrs_pdno, buy_qty, row.now_pric2, '00'):
                    buy_count = buy_count + 1
        
        if buy_count > 0:
            send_message(f"[최대이익증가 매수] {buy_count} 종목 추가 매수됨")
        return buy_count

def profitcut_sell(profitcut_ratio=50, cut_amt=50):
    """주식 잔고조회"""
    PATH = "/uapi/overseas-stock/v1/trading/inquire-balance"
    URL = f"{shared_vars.URL_BASE}/{PATH}"
    headers = {"Content-Type":"application/json", 
        "authorization":f"Bearer {shared_vars.ACCESS_TOKEN}",
        "appKey":shared_vars.APP_KEY,
        "appSecret":shared_vars.APP_SECRET,
        "tr_id":"TTTS3012R",
        "custtype":"P",
    }
    params = {
        "CANO": shared_vars.CANO,
        "ACNT_PRDT_CD": shared_vars.ACNT_PRDT_CD,
        "OVRS_EXCG_CD": "NASD", # NASD:미국주식 전체, NAS : 나스닥, NYSE : 뉴욕, AMEX : 아멕스
        "TR_CRCY_CD": "USD",
        "CTX_AREA_FK200": "",
        "CTX_AREA_NK200": ""
    }
    # print(f'Header : {headers}')
    # print(f'Params : {params}')
    # print(f'URL : {URL}')
    res = get_with_retry(URL, headers=headers, params=params)
    stock_list = res.json()['output1']
    stock_dict = {}

    print(f'stock_list in profitcut_sell(): {stock_list}')

    try: 
        #DB 에서 보유 주식의 최고 수익율, 수익금액 조회 
        engine, con, mycursor = db_conn()
        sql = f"select * from {shared_vars.holding_db_table}"
        mycursor.execute(sql)
        result = mycursor.fetchall()
        con.close()
        df_holding_stock_details = pd.DataFrame(result)
    except Exception as e: 
        print(f'[Profit cut] DB조회 실패 : {e}')
    
    df_pending = get_pending_orders("01")

    # send_message(f"==== 익절 처리({profitcut_ratio}%,{cut_amt}) ===")
    print(f'len stock_list : {stock_list}')
    for stock in stock_list:
        if ((stock['ovrs_pdno'] in df_holding_stock_details['ovrs_pdno'].values) & (stock['ovrs_pdno'] not in df_pending['pdno'].values)):
            code = stock['ovrs_pdno']
            # print(f'stock : {code}')
            current_profit = float(stock['frcr_evlu_pfls_amt'])
            # print(f'current_profit : {current_profit}')
            profit_ratio = float(stock['evlu_pfls_rt'])
            # print(f'profit_ratio : {profit_ratio}')
            stock_name = stock['ovrs_item_name']
            curr_price = float(stock['now_pric2'])
            holding_qty = int(stock['ovrs_cblc_qty'])
            # print(f'df_holding_stock_details : {df_holding_stock_details}')
            max_profit = float(df_holding_stock_details.loc[df_holding_stock_details.ovrs_pdno == stock['ovrs_pdno']].frcr_evlu_pfls_amt.values[0])
            print(f'{stock_name} 현재수익 : {current_profit}, 최고수익 : {max_profit}, 익절 최소금액 : {cut_amt}, 익절 비율 : {profitcut_ratio}, 수익률 : {profit_ratio}')
            # print(f' 익절 기준금액 : {int((profitcut_ratio/100)*max_profit)}')
            
            # 최고 이익 금액이 cut_amt(10만) 를 초과 했을때 현재의 이익 금액이 최고 이익 금액 대비 profit_cut_ratio(50%) 이상 하락시 익절
            if (max_profit > cut_amt) and ( int((profitcut_ratio/100)*max_profit) > curr_price ):
                stock_dict[code] = profit_ratio
                send_message(f"[익절 종목]{stock_name}({code}) 수익률: {profit_ratio}")
                time.sleep(0.1)
                if sell(code, holding_qty, curr_price):
                    send_message(f"[익절 종목]{stock_name} 매도 성공")
                    delete_stock_from_table(shared_vars.holding_db_table, code)
                else:
                    send_message(f"[익절 종목]{stock_name} 매도 실패")
    
    # send_message(f"================================")
    stock_dict.keys()

    return stock_dict

def query_today_init_cnt():
    
    engine, con, mycursor = db_conn() 

    query_date = date.today().strftime('%Y%m%d')

    query = f"""
        SELECT count(1) cnt
        FROM {shared_vars.holding_db_table}
        WHERE init_dt = STR_TO_DATE('{query_date}', '%Y%m%d')
    """
    print(f'sql in query_today_init_cnt(): {query}')
        
    try : 
        mycursor.execute(query)
        result = mycursor.fetchall()
    except Exception as e: 
        if e.args[0] == 1146:
            print('Table does not exist')
        else :
            print('Query failed')
        return 0

    df_stock_count = pd.DataFrame(result)
    con.close() 
    today_init_cnt = int(df_stock_count['cnt'].values[0])
    print(f'today init count : {today_init_cnt}')

    return today_init_cnt

def delete_stock_from_table(tname, stock):
    print(f'will deletion {stock} from {tname}')
    if (tname != '') and (stock != ''):
        engine, con, mycursor = db_conn() 
        query_date = date.today().strftime('%Y%m%d')
        query = f"""
            DELETE
            FROM {tname}
            WHERE ovrs_pdno = '{stock}'
        """
        mycursor.execute(query)
        result = mycursor.fetchall()
        con.commit()
        con.close() 
        print(f'Deletion for {stock} from {tname} complete')
        return True

    else:
        print(f'Input Not valid : {tname}, {stock}')
        return False

def get_current_price(code="005930"):
    """현재가 조회"""
    PATH = "uapi/domestic-stock/v1/quotations/inquire-price"
    URL = f"{shared_vars.URL_BASE}/{PATH}"
    headers = {"Content-Type":"application/json", 
            "authorization": f"Bearer {shared_vars.ACCESS_TOKEN}",
            "appKey":shared_vars.APP_KEY,
            "appSecret":shared_vars.APP_SECRET,
            "tr_id":"FHKST01010100"}
    params = {
    "fid_cond_mrkt_div_code":"J",
    "fid_input_iscd":code,
    }
    res = get_with_retry(URL, headers=headers, params=params)
    return int(res.json()['output']['stck_prpr'])