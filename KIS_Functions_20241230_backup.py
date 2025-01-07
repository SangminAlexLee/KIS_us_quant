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
    res = requests.get(URL, headers=headers, params=params)
    print(f'res : {res}')
    s_list = []
    for i in res.json()['output2']:
        s_list.append((i['jong_code'],i['hts_kor_isnm']) )
    df_list = pd.DataFrame(s_list, columns=['code','kor_name'])
    print(df_list)
    return df_list

def save_access_token(token):
    """토큰 DB 저장"""
    df_token = pd.DataFrame([[token, datetime.datetime.now()]], columns=['token', 'timestamp'])
    engine, con, mycursor = db_conn()
    df_token.to_sql(name = 'kis_token', con=engine, if_exists='replace', index=False)
    con.close()

def get_access_token():
    """토큰 발급"""

    # DB에서 토큰 가져오기 
    try: 
        engine, con, mycursor = db_conn()
        sql = "select * from kis_token"
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
    message = {"content": f"[{now.strftime('%Y-%m-%d %H:%M:%S')}] {str(msg)}"}
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
    res = requests.get(URL, headers=headers, params=params)
    return int(res.json()['output']['stck_prpr'])

def get_stock_balance():
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
    stock_dict = {}
    send_message(f"====주식 보유잔고====")
    for stock in stock_list:
        if int(stock['hldg_qty']) > 0:
            stock_dict[stock['pdno']] = stock['hldg_qty']
            send_message(f"{stock['prdt_name']}({stock['pdno']}): {stock['hldg_qty']}주")
            time.sleep(0.1)
    send_message(f"주식 평가 금액: {evaluation[0]['scts_evlu_amt']}원")
    time.sleep(0.1)
    send_message(f"평가 손익 합계: {evaluation[0]['evlu_pfls_smtl_amt']}원")
    time.sleep(0.1)
    send_message(f"총 평가 금액: {evaluation[0]['tot_evlu_amt']}원")
    time.sleep(0.1)
    send_message(f"=================")
    shared_vars.l_curr_stock = stock_dict.keys() 
    return stock_dict


def get_target_price(code="005930", option='None'):
    # print('현재시세 조회')
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
    res = requests.get(URL, headers=headers, params=params)

    stck_stat = res.json()['output']['iscd_stat_cls_code'] #상태구분
    stck_prpr = int(res.json()['output']['stck_prpr']) # 현재가 
    aspr_unit = int(res.json()['output']['aspr_unit']) #호가 단위

    if option == 'MINUS_2_TICK':
        target_price = stck_prpr - (aspr_unit * 2)
    else: 
        target_price = stck_prpr
    # print(f'상태 : {stck_stat}, 현재가 : {stck_prpr}, , 호가단위 : {aspr_unit}, 목표 매수가 : {target_price}')

    return target_price

def get_balance():
    """현금 잔고조회"""
    PATH = "uapi/domestic-stock/v1/trading/inquire-psbl-order"
    URL = f"{shared_vars.URL_BASE}/{PATH}"
    headers = {"Content-Type":"application/json", 
        "authorization":f"Bearer {shared_vars.ACCESS_TOKEN}",
        "appKey":shared_vars.APP_KEY,
        "appSecret":shared_vars.APP_SECRET,
        "tr_id":"TTTC8908R",
        "custtype":"P",
    }
    params = {
        "CANO": shared_vars.CANO,
        "ACNT_PRDT_CD": shared_vars.ACNT_PRDT_CD,
        "PDNO": "005930",
        "ORD_UNPR": "65500",
        "ORD_DVSN": "01",
        "CMA_EVLU_AMT_ICLD_YN": "Y",
        "OVRS_ICLD_YN": "Y"
    }
    res = requests.get(URL, headers=headers, params=params)
    cash = res.json()['output']['ord_psbl_cash']
    send_message(f"주문 가능 현금 잔고: {cash}원")
    return int(cash)


def buy(code="005930", qty="1",price=0):
    """주식 시장가 매수"""  
    PATH = "uapi/domestic-stock/v1/trading/order-cash"
    URL = f"{shared_vars.URL_BASE}/{PATH}"
    data = {
        "CANO": shared_vars.CANO,
        "ACNT_PRDT_CD": shared_vars.ACNT_PRDT_CD,
        "PDNO": code,
        "ORD_DVSN": "00",
        "ORD_QTY": str(int(qty)),
        "ORD_UNPR": str(int(price)),
    }
    headers = {"Content-Type":"application/json", 
        "authorization":f"Bearer {shared_vars.ACCESS_TOKEN}",
        "appKey":shared_vars.APP_KEY,
        "appSecret":shared_vars.APP_SECRET,
        "tr_id":"TTTC0802U",
        "custtype":"P",
        "hashkey" : hashkey(data)
    }
    res = requests.post(URL, headers=headers, data=json.dumps(data))
    # print(f'[주문 상세] 종목코드 : {code}, 수량 : {qty}, 매수가 : {price}')
    if res.json()['rt_cd'] == '0':
        send_message(f"[주문 성공]{str(res.json())}")
        send_message(f"[주문 성공][종목코드 : {code}, 수량 : {qty}, 매수가 : {price}")
        return True
    else:
        send_message(f"[주문 실패]{str(res.json())}")
        send_message(f"[주문 실패][종목코드 : {code}, 수량 : {qty}, 매수가 : {price}")
        return False

def sell(code="005930", qty="1"):
    """주식 시장가 매도"""
    # print('in sell()')
    PATH = "uapi/domestic-stock/v1/trading/order-cash"
    URL = f"{shared_vars.URL_BASE}/{PATH}"
    data = {
        "CANO": shared_vars.CANO,
        "ACNT_PRDT_CD": shared_vars.ACNT_PRDT_CD,
        "PDNO": code,
        "ORD_DVSN": "01",
        "ORD_QTY": qty,
        "ORD_UNPR": "0",
    }
    headers = {"Content-Type":"application/json", 
        "authorization":f"Bearer {shared_vars.ACCESS_TOKEN}",
        "appKey":shared_vars.APP_KEY,
        "appSecret":shared_vars.APP_SECRET,
        "tr_id":"TTTC0801U",
        "custtype":"P",
        "hashkey" : hashkey(data)
    }
    res = requests.post(URL, headers=headers, data=json.dumps(data))
    # print(f'res in sell() : {res}')
    if res.json()['rt_cd'] == '0':
        send_message(f"[매도 성공]{str(res.json())}")
        return True
    else:
        send_message(f"[매도 실패]{str(res.json())}")
        return False

# 최초 매수할 종목 리스트 제공 함수
def get_list_for_init_buy():
    
    # 관심종목에서 이미 보유하고 있는 종목 제외
    list_to_buy = [item for item in shared_vars.df_fav_stocks.code.values if item not in shared_vars.l_curr_stock and item not in shared_vars.df_pending_orders.pdno.values]

    return list_to_buy

# 최초 매수 함수 
def buy_init_stocks(init_amt=500000):
    
    #매수 대상 종목 조회
    l_init_buy = get_list_for_init_buy()

    # print(f'list to buy in init buy : {get_list_for_init_bmiuy()}')

    for stock_code in l_init_buy: 

        # 2호가 아래 매수 가격 조회        
        target_price = get_target_price(code=stock_code, option='MINUS_2_TICK')
    
        if target_price > init_amt:
            send_message(f"[매수 금액 초과] 종목 : {stock_code}, 매수가격 : {target_price}, 매수금액 : {init_amt}")
            buy_qty = 1
        else: 
            buy_qty = 0  # 매수할 수량 초기화
            buy_qty = int(init_amt // target_price)

        if buy(stock_code, buy_qty, target_price): # 매수주문
            shared_vars.init_bought_count = shared_vars.init_bought_count + 1

def get_pending_orders():
    """주식 미완료 주문 조회"""
    # print('get_pending_orders()')
    PATH = "uapi/domestic-stock/v1/trading/inquire-psbl-rvsecncl"
    URL = f"{shared_vars.URL_BASE}/{PATH}"
    headers = {"Content-Type":"application/json", 
        "authorization":f"Bearer {shared_vars.ACCESS_TOKEN}",
        "appKey":shared_vars.APP_KEY,
        "appSecret":shared_vars.APP_SECRET,
        "tr_id":"TTTC8036R"
    }
    params = {
        "CANO": shared_vars.CANO,
        "ACNT_PRDT_CD": shared_vars.ACNT_PRDT_CD,
        "CTX_AREA_FK100": "",
        "CTX_AREA_NK100": "",
        "INQR_DVSN_1": "0",
        "INQR_DVSN_2": "0",
    }
    res = requests.get(URL, headers=headers, params=params)
    time.sleep(0.1)

    shared_vars.df_pending_orders = pd.DataFrame(res.json()['output'])

    # 조회내역이 없는 경우 빈 데이터프레임 생성함. 
    if len(shared_vars.df_pending_orders) == 0:
        shared_vars.df_pending_orders = pd.DataFrame(columns=['ord_gno_brno', 'odno', 'orgn_odno', 'ord_dvsn_name', 'pdno',
       'prdt_name', 'rvse_cncl_dvsn_name', 'ord_qty', 'ord_unpr', 'ord_tmd',
       'tot_ccld_qty', 'tot_ccld_amt', 'psbl_qty', 'sll_buy_dvsn_cd',
       'ord_dvsn_cd', 'mgco_aptm_odno'])

    # print(shared_vars.df_pending_orders)
    
    return shared_vars.df_pending_orders


def get_buysell_hist(start_date='', end_date=''):
    """주식 체결 내역 조회"""

    if (start_date == '') or (len(start_date) != 8):
        start_date = datetime.today().strftime('%Y%m%d')
    if (end_date == '') or (len(end_date) != 8):
        end_date = datetime.today().strftime('%Y%m%d')

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

    print(f'res : {res}')
    print(f'res.json() : {res.json()}')

    df_buysell_hist = pd.DataFrame(res.json()['output1'])

    print(df_buysell_hist)
    
    return df_buysell_hist


def losscut_sell(losscut_ratio=-10):
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
    res = requests.get(URL, headers=headers, params=params)
    stock_list = res.json()['output1']
    stock_dict = {}
    send_message(f"====== 손절 확인({losscut_ratio}%) =====")
    for stock in stock_list:
        code = stock['pdno']
        profit_ratio = float(stock['evlu_pfls_rt'])
        stock_name = stock['prdt_name']
        curr_price = stock['prpr']
        holding_qty = stock['hldg_qty']
        print(f'{stock_name} 수익률 : {profit_ratio}')
        if profit_ratio < losscut_ratio:
            stock_dict[code] = profit_ratio
            send_message(f"[손절 종목]{stock_name}({code}) 수익률: {profit_ratio}")
            time.sleep(0.1)
            sell(code, holding_qty)
    
    send_message(f"================================")
    stock_dict.keys()

    return stock_dict

def get_buysell_hist(start_date='', end_date=''):
    """주식 체결 내역 조회"""

    if (start_date == '') or (len(start_date) != 8):
        start_date = datetime.today().strftime('%Y%m%d')
    if (end_date == '') or (len(end_date) != 8):
        end_date = datetime.today().strftime('%Y%m%d')

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

    print(f'res : {res}')
    print(f'res.json() : {res.json()}')

    df_buysell_hist = pd.DataFrame(res.json()['output1'])

    print(df_buysell_hist)
    
    return df_buysell_hist

def update_holding_stock_details():
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
    res = requests.get(URL, headers=headers, params=params)
    curr_account_stock_list = res.json()['output1']    
    df_curr_account_stock = pd.DataFrame(curr_account_stock_list)
    df_curr_account_stock = df_curr_account_stock[['pdno','prdt_name','prpr','evlu_amt','evlu_pfls_amt','evlu_pfls_rt']]
    df_curr_account_stock.loc[df_curr_account_stock.pdno == '005930','evlu_pfls_amt'] = 10000
    df_curr_account_stock.loc[df_curr_account_stock.pdno == '035720','evlu_pfls_amt'] = 10000
    print(f'df_curr_account_stock : {df_curr_account_stock}')

    df_update_stock_list = pd.DataFrame(columns=['pdno','prdt_name','prpr','evlu_amt','evlu_pfls_amt','evlu_pfls_rt'])

    # 계좌에 주식이 없으면 빈 데이터프레임 리턴하고 종료 
    if len(df_curr_account_stock) == 0:
        return df_update_stock_list

    try: 
        #DB 에서 보유 주식의 최고 수익율, 수익금액 조회 
        engine, con, mycursor = db_conn()
        # print('before select holding_stock_details')
        sql = "select * from holding_stock_details"
        mycursor.execute(sql)
        result = mycursor.fetchall()
        con.close()
        df_holding_stock_details = pd.DataFrame(result)
    except Exception as e: 
        # print(type(e))
        # print(f'Exception in update_holding_stock_details() : {e.args[0]}') 
        if e.args[0] == 1146:
            print('Table does not exist')
            # print(stock_list)
            engine, con, mycursor = db_conn()
            df_curr_account_stock.to_sql(name = 'holding_stock_details', con=engine, if_exists='replace', index=False)
            con.close()
            df_update_stock_list = df_curr_account_stock

    print(f'df_holding_stock_details : {df_holding_stock_details}')

    merged = df_curr_account_stock.merge(df_holding_stock_details, on='pdno', suffixes=('_a', '_b'))

    print(f'merged : {merged}')

    print(f'df_curr_account_stock desc : {df_curr_account_stock.dtypes}')
    print(f'df_holding_stock_details desc : {df_holding_stock_details.dtypes}')
    print(f'merged desc : {merged.dtypes}')

    # a의 'amount' 값이 더 큰 경우 필터링
    df_max_profit_for_update = merged[merged['evlu_pfls_amt_a'].astype(float) > merged['evlu_pfls_amt_b'].astype(float)][['pdno', 'evlu_pfls_amt_a']]

    # 결과 출력
    print(df_max_profit_for_update)

    engine, con, mycursor = db_conn()
    # print('before select holding_stock_details')
    for index in df_max_profit_for_update
        sql = """
        UPDATE holding_stock_details
        SET evlu_pfls_amt = %s
        WHERE pdno = %s
        """
        
        # 업데이트할 값
        evlu_pfls_amt = df_max_profit_for_update.loc[index, 'evlu_pfls_amt']
        pdno = df_max_profit_for_update.loc[index, 'pdno']
        
        # 쿼리 실행
        mycursor.execute(sql, (value_to_update, condition_value))
        
        # 변경 사항 커밋
        connection.commit()
    result = mycursor.fetchall()
    con.close()
    
        
    # for curr_stock in df_curr_account_stock.pdno:
    #     print(f'curr_stock : {curr_stock}')
    #     print(df_curr_account_stock.loc[df_curr_account_stock.pdno == curr_stock].evlu_pfls_amt)
    #     # print(f'curr_stock pdno: {curr_stock.pdno}')
    #     # print(f'curr_stock probit : {curr_stock.evlu_pfls_amt}')
    #     # curr_max_profit = df_holding_stock_details.loc[df_holding_stock_details.pdno == curr_stock.pdno].evlu_pfls_amt
    #     # print(f'max profit : {curr_max_profit}')


    # send_message(f"====== 손절 확인({losscut_ratio}%) =====")
    # for stock in stock_list:
    #     code = stock['pdno']
    #     profit_ratio = float(stock['evlu_pfls_rt'])
    #     stock_name = stock['prdt_name']
    #     curr_price = stock['prpr']
    #     holding_qty = stock['hldg_qty']
    #     print(f'{stock_name} 수익률 : {profit_ratio}')
    #     if profit_ratio < losscut_ratio:
    #         stock_dict[code] = profit_ratio
    #         send_message(f"[손절 종목]{stock_name}({code}) 수익률: {profit_ratio}")
    #         time.sleep(0.1)
    #         sell(code, holding_qty)
    
    # send_message(f"================================")
    # stock_dict.keys()

    return stock_dict
