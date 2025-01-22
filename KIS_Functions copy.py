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


def buy(code="005930", qty="1",price=0, option='01'):
    """주식 시장가 매수"""  
    PATH = "uapi/domestic-stock/v1/trading/order-cash"
    URL = f"{shared_vars.URL_BASE}/{PATH}"
    data = {
        "CANO": shared_vars.CANO,
        "ACNT_PRDT_CD": shared_vars.ACNT_PRDT_CD,
        "PDNO": code,
        "ORD_DVSN": option,
        "ORD_QTY": str(int(qty)),
        "ORD_UNPR": str(int(price)),
    }
    headers = {"Content-Type":"application/json", 
        "authorization":f"Bearer {shared_vars.ACCESS_TOKEN}",
        "appKey":shared_vars.APP_KEY,
        "appSecret":shared_vars.APP_SECRET,
        "tr_id":"TTTC0802U"
        # ,
        # "custtype":"P",
        # "hashkey" : hashkey(data)
    }
    # print(f'data : {data}')
    # print(f'headers : {headers}')
    res = requests.post(URL, headers=headers, data=json.dumps(data))
    # print(f'[주문 상세] 종목코드 : {code}, 수량 : {qty}, 매수가 : {price}')
    if res.json()['rt_cd'] == '0':
        # send_message(f"[주문 성공]{str(res.json())}")
        send_message(f"[주문 성공][종목코드 : {code}, 수량 : {qty}, 매수가 : {price}")
        return True
    else:

        if shared_vars.lack_of_cash_flag == False:
            send_message(f"[주문 실패]{str(res.json()['msg1'])}")
            send_message(f"[주문 실패][종목코드 : {code}, 수량 : {qty}, 매수가 : {price}")
        #잔액 부족 발생시 flag 켜서 이후 메시지 send 방지
        if str(res.json()['msg_cd']) == 'APBK0952': 
            shared_vars.lack_of_cash_flag = True
                
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

    send_message(f"==== 초기 매수 {len(l_init_buy)}종목 ====")

    for stock_code in l_init_buy: 

        #1달 전 수익륙
        end_date = date.today().strftime('%Y%m%d')
        start_date = (date.today() - timedelta(days=30)).strftime('%Y%m%d')
        return_30d = get_stock_return(stock_code, start_date, end_date)
        
        #1주일 전 수익률
        start_date = (date.today() - timedelta(days=7)).strftime('%Y%m%d')
        return_7d = get_stock_return(stock_code, start_date, end_date)

        kor_name = shared_vars.df_fav_stocks[shared_vars.df_fav_stocks.code == stock_code].kor_name.values

        print(f"{kor_name} 30d return : {return_30d['ret']}, 7d return : {return_7d['ret']}")
        if (return_30d['ret'] < 10) or (return_7d['ret'] < 5):
            # print('not enough performance to buy')
            continue

        # 2호가 아래 매수 가격 조회        
        target_price = get_target_price(code=stock_code, option='MINUS_2_TICK')
    
        if target_price > init_amt:
            send_message(f"[매수 금액 초과] 종목 : {stock_code}, 매수가격 : {target_price}, 매수금액 : {init_amt}")
            buy_qty = 1
        else: 
            buy_qty = 0  # 매수할 수량 초기화
            buy_qty = int(init_amt // target_price)

        # 매수 주문 성공시 init_bought_count 1 증가 
        if buy(stock_code, buy_qty, target_price, '00'): # 매수주문
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
            if sell(code, holding_qty):
                delete_stock_from_table('holding_stock_details', code)
    
    # send_message(f"================================")
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
    # df_curr_account_stock.loc[df_curr_account_stock.pdno == '005930','evlu_pfls_amt'] = -30006
    # df_curr_account_stock.loc[df_curr_account_stock.pdno == '071050','evlu_pfls_amt'] = -30002
    # print(f'df_curr_account_stock : {df_curr_account_stock}')

    holding_stock_tbl_col_list= ['pdno','prdt_name','init_dt','prpr','evlu_amt','evlu_pfls_amt','evlu_pfls_rt', 'buy_on_up_flag']

    df_update_stock_list = pd.DataFrame(columns=holding_stock_tbl_col_list)
    df_update_stock_list_empty = df_update_stock_list  

    # 계좌에 주식이 없으면 빈 데이터프레임 리턴하고 종료 
    if len(df_curr_account_stock) == 0:
        return df_update_stock_list

    try: 
        #DB 에서 보유 주식의 최고 수익율, 수익금액 조회 
        engine, con, mycursor = db_conn()
        sql = "select * from holding_stock_details"
        mycursor.execute(sql)
        result = mycursor.fetchall()
        con.close()
        df_holding_stock_details = pd.DataFrame(result)
    except Exception as e: 
        
        if e.args[0] == 1146:
            print('Table does not exist')
            
            df_curr_account_stock['init_dt'] = date.today()
            df_curr_account_stock = df_curr_account_stock[holding_stock_tbl_col_list]
            engine, con, mycursor = db_conn()
            df_curr_account_stock.to_sql(name = 'holding_stock_details', con=engine, if_exists='replace', index=False)
            con.close()
            df_update_stock_list = df_curr_account_stock
            send_message(f"[최대 이익금액 업데이트] {len(df_update_stock_list)} 종목 업데이트 됨")

            return df_update_stock_list_empty

    df_merged = df_curr_account_stock.merge(df_holding_stock_details,how='outer', on='pdno', suffixes=('_a', '_b'))

    # print(f'merged : {df_merged}')

    df_merged['profit_index_a'] = df_merged['evlu_pfls_amt_a'].apply(lambda x : get_tier_ratio(np.nan_to_num(x), shared_vars.df_up_buy_table)).apply(lambda x: x[1]).tolist()
    df_merged['profit_index_b'] = df_merged['evlu_pfls_amt_b'].apply(lambda x : get_tier_ratio(np.nan_to_num(x), shared_vars.df_up_buy_table)).apply(lambda x: x[1]).tolist()
    df_merged['buy_on_up_flag'] = df_merged['profit_index_a'] > df_merged['profit_index_b']

    # print(f'df_curr_account_stock desc : {df_curr_account_stock.dtypes}')
    # print(f'df_holding_stock_details desc : {df_holding_stock_details.dtypes}')
    # print(f'merged desc : {df_merged.dtypes}')

    # 현재가 기준 이익 금액이 holding_stock_details 테이블의 이익금액 보다 큰 종목과 이익금액 조회
    df_max_profit_for_update = df_merged[df_merged['evlu_pfls_amt_a'].astype(float) > df_merged['evlu_pfls_amt_b'].astype(float)][['pdno', 'evlu_pfls_amt_a', 'buy_on_up_flag']]

    df_max_profit_for_update

    # 결과 출력
    # print(f'df_max_profit_for_update: {df_max_profit_for_update}')

    engine, con, mycursor = db_conn()
    # 개별 종목의 holding_stock_details 의 최고 수익 금액 필드 업데이트 
    for index in df_max_profit_for_update.index:
        print(f'for {index} index')
        sql = """
        UPDATE holding_stock_details
        SET evlu_pfls_amt = %s
        WHERE pdno = %s
        """
        
        # 업데이트할 값
        evlu_pfls_amt = df_max_profit_for_update.loc[index, 'evlu_pfls_amt_a']
        pdno = df_max_profit_for_update.loc[index, 'pdno']
        
        # print(sql % (evlu_pfls_amt, pdno))
        # 쿼리 실행
        mycursor.execute(sql, (evlu_pfls_amt, pdno))
        
        # 변경 사항 커밋
        con.commit()
    con.close()

    # print(f'len(df_max_profit_for_update) : {len(df_max_profit_for_update)}')

    # 최대이익 증가 종목에 대해 init_dt 오늘자로 update
    if len(df_max_profit_for_update) > 0: 
        df_update_stock_list = df_curr_account_stock[df_curr_account_stock['pdno'].isin(df_max_profit_for_update.pdno)]
        df_update_stock_list.loc[:,'init_dt'] = date.today()
        df_update_stock_list['buy_on_up_flag'] = df_max_profit_for_update['buy_on_up_flag']
        df_update_stock_list = df_update_stock_list[holding_stock_tbl_col_list]
        # df_update_stock_list.loc[:,'buy_on_up_flag'] = df_max_profit_for_update['buy_on_up_flag']
        print(f'df_update_stock_list in here : {df_update_stock_list}')

    # 신규 편입된 종목에 대해서 holding_stock_details 에 레코드 insert
    for stock_code in df_merged[df_merged['evlu_pfls_amt_b'].isna()].pdno  :
        print(f'New stock will be added to holding detail table : {stock_code}')
        dt_temp = df_curr_account_stock[df_curr_account_stock.pdno == stock_code]
        dt_temp.loc[:,'init_dt'] = date.today()
        dt_temp.loc[:,'buy_on_up_flag'] = False
        dt_temp = dt_temp[holding_stock_tbl_col_list]
        engine, con, mycursor = db_conn()
        dt_temp.to_sql(name = 'holding_stock_details', con=engine, if_exists='append', index=False)
        con.close()
        # df_update_stock_list['buy_on_up_flag'] = False
        df_update_stock_list = pd.concat([df_update_stock_list,dt_temp] )

    # print(f'df_update_stock_list at the end : {df_update_stock_list}')

    if len(df_update_stock_list) > 0:
            send_message(f"[최대 이익금액 업데이트] {len(df_update_stock_list)} 종목 업데이트 됨")
    
    return df_update_stock_list

# 기간별 주식 수익률 & 수익률 표준편차 계산 함수
def get_stock_return(stock_code, start_date, end_date):

    engine, con, mycursor = db_conn() 

    query = f"""
        SELECT code, date, close
        FROM kr_stock_price
        WHERE code = '{stock_code}' AND Date BETWEEN STR_TO_DATE('{start_date}', '%Y%m%d') AND STR_TO_DATE('{end_date}', '%Y%m%d')
    """
    mycursor.execute(query)
    result = mycursor.fetchall()
    df_stock_price = pd.DataFrame(result)
    con.close()    

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
    end_price = df_stock_price.iloc[-1]['close']
    price_change_rate = ((end_price - start_price) / start_price) * 100

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

                if (int(tier) >= int(amount)):
                        return ratio , index +1
        
        return 0
                
def buy_on_profit(stock_for_update, up_buy_t):

        buy_count = 0

        for index, row in stock_for_update.iterrows():
                print(f'======== ratio check ===========')
                print(f'pdno : {row.pdno}')
                profit_amt = row.evlu_pfls_amt
                print(f'profit_amt: {profit_amt}')
                ratio, index = get_tier_ratio(profit_amt, up_buy_t)
                print(f'ratio : {ratio}')
                print(f'index : {index}')
                print(f'evlu_amt : {row.evlu_amt }')
                print(f'ratio/100 : {ratio/100}')
                buy_amt = int(int(row.evlu_amt) * (ratio/100))
                print(f'buy amount : {buy_amt}')
                buy_qty = buy_amt // int(row.prpr)
                print(f'buy quantaty : {buy_qty}')

                if buy(row.pdno, buy_qty, row.prpr, '00'):
                    buy_count = buy_count + 1
        
        if buy_count > 0:
            send_message(f"[최대이익증가 매수] {buy_count} 종목 추가 매수됨")
        return buy_count

def profitcut_sell(profitcut_ratio=50, cut_amt=100000):
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

    try: 
        #DB 에서 보유 주식의 최고 수익율, 수익금액 조회 
        engine, con, mycursor = db_conn()
        sql = "select * from holding_stock_details"
        mycursor.execute(sql)
        result = mycursor.fetchall()
        con.close()
        df_holding_stock_details = pd.DataFrame(result)
    except Exception as e: 
        print(f'[Profit cut] DB조회 실패 : {e}')
    
    send_message(f"==== 익절 처리({profitcut_ratio}%,{cut_amt/10000}만) ===")
    # print(f'len stock_list : {stock_list}')
    for stock in stock_list:
        if stock['pdno'] in df_holding_stock_details['pdno'].values:
            code = stock['pdno']
            # print(f'stock : {code}')
            current_profit = int(stock['evlu_pfls_amt'])
            # print(f'current_profit : {current_profit}')
            profit_ratio = float(stock['evlu_pfls_rt'])
            # print(f'profit_ratio : {profit_ratio}')
            stock_name = stock['prdt_name']
            curr_price = int(stock['prpr'])
            holding_qty = int(stock['hldg_qty'])
            # print(f'df_holding_stock_details : {df_holding_stock_details}')
            max_profit = int(df_holding_stock_details.loc[df_holding_stock_details.pdno == stock['pdno']].evlu_pfls_amt.values[0])
            print(f'{stock_name} 현재수익 : {current_profit}, 최고수익 : {max_profit}, 익절 최소금액 : {cut_amt}, 익절 비율 : {profitcut_ratio}, 수익률 : {profit_ratio}')
            # print(f' 익절 기준금액 : {int((profitcut_ratio/100)*max_profit)}')
            
            # 최고 이익 금액이 cut_amt(10만) 를 초과 했을때 현재의 이익 금액이 최고 이익 금액 대비 profit_cut_ratio(50%) 이상 하락시 익절
            if (max_profit > cut_amt) and ( int((profitcut_ratio/100)*max_profit) > curr_price ):
                stock_dict[code] = profit_ratio
                send_message(f"[익절 종목]{stock_name}({code}) 수익률: {profit_ratio}")
                time.sleep(0.1)
                if sell(code, holding_qty):
                    delete_stock_from_table('holding_stock_details', code)
    
    
    # send_message(f"================================")
    stock_dict.keys()

    return stock_dict

def query_today_init_cnt():
    
    engine, con, mycursor = db_conn() 

    query_date = date.today().strftime('%Y%m%d')

    query = f"""
        SELECT count(1) cnt
        FROM holding_stock_details
        WHERE init_dt = STR_TO_DATE('{query_date}', '%Y%m%d')
    """

    mycursor.execute(query)
    result = mycursor.fetchall()
    df_stock_count = pd.DataFrame(result)
    con.close() 
    today_init_cnt = int(df_stock_count['cnt'].values[0])
    print(f'today init count : {today_init_cnt}')

    return today_init_cnt

def delete_stock_from_table(tname, stock):

    if (tname != '') and (stock != ''):
        engine, con, mycursor = db_conn() 
        query_date = date.today().strftime('%Y%m%d')
        query = f"""
            DELETE
            FROM {tname}
            WHERE pdno = {stock}
        """
        mycursor.execute(query)
        result = mycursor.fetchall()
        con.commit()
        con.close() 
        print(f'Deletion for {stock} from {tname} done')
        return True

    else:
        print(f'Input Not valid : {tname}, {stock}')
        return False