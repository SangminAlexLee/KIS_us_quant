import requests
import json
import datetime
from pytz import timezone
import time
import yaml
import sqlite3
import pandas as pd
import os

with open('config.yaml', encoding='UTF-8') as f:
    _cfg = yaml.load(f, Loader=yaml.FullLoader)
APP_KEY = _cfg['APP_KEY']
APP_SECRET = _cfg['APP_SECRET']
ACCESS_TOKEN = ""
CANO = _cfg['CANO']
ACNT_PRDT_CD = _cfg['ACNT_PRDT_CD']
DISCORD_WEBHOOK_URL = _cfg['DISCORD_WEBHOOK_URL']
URL_BASE = _cfg['URL_BASE']

def send_message(msg):
    """디스코드 메세지 전송"""
    now = datetime.datetime.now()
    message = {"content": f"[{now.strftime('%Y-%m-%d %H:%M:%S')}] {str(msg)}"}
    requests.post(DISCORD_WEBHOOK_URL, data=message)
    print(message)

def get_access_token():
    """토큰 발급"""
    headers = {"content-type":"application/json"}
    body = {"grant_type":"client_credentials",
    "appkey":APP_KEY, 
    "appsecret":APP_SECRET}
    PATH = "oauth2/tokenP"
    URL = f"{URL_BASE}/{PATH}"
    res = requests.post(URL, headers=headers, data=json.dumps(body))
    ACCESS_TOKEN = res.json()["access_token"]
    return ACCESS_TOKEN
    
def hashkey(datas):
    """암호화"""
    PATH = "uapi/hashkey"
    URL = f"{URL_BASE}/{PATH}"
    headers = {
    'content-Type' : 'application/json',
    'appKey' : APP_KEY,
    'appSecret' : APP_SECRET,
    }
    res = requests.post(URL, headers=headers, data=json.dumps(datas))
    hashkey = res.json()["HASH"]
    return hashkey

def get_current_price(market="NAS", code="AAPL"):
    """현재가 조회"""
    PATH = "uapi/overseas-price/v1/quotations/price"
    URL = f"{URL_BASE}/{PATH}"
    headers = {"Content-Type":"application/json", 
            "authorization": f"Bearer {ACCESS_TOKEN}",
            "appKey":APP_KEY,
            "appSecret":APP_SECRET,
            "tr_id":"HHDFS00000300"}
    params = {
        "AUTH": "",
        "EXCD":market,
        "SYMB":code,
    }
    res = requests.get(URL, headers=headers, params=params)
    return float(res.json()['output']['last'])


def get_buy_price(market="NAS", code="AAPL"):
    """현재가 조회"""
    PATH = "uapi/overseas-price/v1/quotations/price"
    URL = f"{URL_BASE}/{PATH}"
    headers = {"Content-Type":"application/json",
            "authorization": f"Bearer {ACCESS_TOKEN}",
            "appKey":APP_KEY,
            "appSecret":APP_SECRET,
            "tr_id":"HHDFS00000300"}
    params = {
        "AUTH": "",
        "EXCD":market,
        "SYMB":code,
    }
    res = requests.get(URL, headers=headers, params=params)
    current_price = float(res.json()['output']['last'])
    buy_price = current_price * 1.005
    return buy_price

def get_sell_price(market="NAS", code="AAPL"):
    """현재가 조회"""
    PATH = "uapi/overseas-price/v1/quotations/price"
    URL = f"{URL_BASE}/{PATH}"
    headers = {"Content-Type":"application/json",
            "authorization": f"Bearer {ACCESS_TOKEN}",
            "appKey":APP_KEY,
            "appSecret":APP_SECRET,
            "tr_id":"HHDFS00000300"}
    params = {
        "AUTH": "",
        "EXCD":market,
        "SYMB":code,
    }
    res = requests.get(URL, headers=headers, params=params)
    current_price = float(res.json()['output']['last'])
    sell_price = current_price * 0.995
    return sell_price


def get_price_change(market="NAS", code="AAPL"):
    """변동률"""
    PATH = "uapi/overseas-price/v1/quotations/dailyprice"
    URL = f"{URL_BASE}/{PATH}"
    headers = {"Content-Type":"application/json", 
        "authorization": f"Bearer {ACCESS_TOKEN}",
        "appKey":APP_KEY,
        "appSecret":APP_SECRET,
        "tr_id":"HHDFS76240000"}
    params = {
        "AUTH":"",
        "EXCD":market,
        "SYMB":code,
        "GUBN":"0",
        "BYMD":"",
        "MODP":"0"
    }
    res = requests.get(URL, headers=headers, params=params)
    price_change = float(res.json()['output2'][0]['rate']) 
    return price_change


def get_prev_last(market="NAS", code="AAPL"):
    """변동률"""
    PATH = "uapi/overseas-price/v1/quotations/dailyprice"
    URL = f"{URL_BASE}/{PATH}"
    headers = {"Content-Type":"application/json", 
        "authorization": f"Bearer {ACCESS_TOKEN}",
        "appKey":APP_KEY,
        "appSecret":APP_SECRET,
        "tr_id":"HHDFS76240000"}
    params = {
        "AUTH":"",
        "EXCD":market,
        "SYMB":code,
        "GUBN":"0",
        "BYMD":"",
        "MODP":"0"
    }
    res = requests.get(URL, headers=headers, params=params)
    price_change = float(res.json()['output1']['nrec']) 
    return price_change

def get_target_price(market="NAS", code="AAPL"):
    """변동성 돌파 전략으로 매수 목표가 조회"""
    PATH = "uapi/overseas-price/v1/quotations/dailyprice"
    URL = f"{URL_BASE}/{PATH}"
    headers = {"Content-Type":"application/json", 
        "authorization": f"Bearer {ACCESS_TOKEN}",
        "appKey":APP_KEY,
        "appSecret":APP_SECRET,
        "tr_id":"HHDFS76240000"}
    params = {
        "AUTH":"",
        "EXCD":market,
        "SYMB":code,
        "GUBN":"0",
        "BYMD":"",
        "MODP":"0"
    }
    res = requests.get(URL, headers=headers, params=params)
    stck_oprc = float(res.json()['output2'][0]['open']) #오늘 시가
    stck_hgpr = float(res.json()['output2'][1]['high']) #전일 고가
    stck_lwpr = float(res.json()['output2'][1]['low']) #전일 저가
    stck_clos = float(res.json()['output2'][1]['clos']) #전일 종가
    target_price = stck_clos * 0.995
    return target_price

def get_init_price(market="NAS", code="AAPL"):
    """변동성 돌파 전략으로 매수 목표가 조회"""
    PATH = "uapi/overseas-price/v1/quotations/dailyprice"
    URL = f"{URL_BASE}/{PATH}"
    headers = {"Content-Type":"application/json", 
        "authorization": f"Bearer {ACCESS_TOKEN}",
        "appKey":APP_KEY,
        "appSecret":APP_SECRET,
        "tr_id":"HHDFS76240000"}
    params = {
        "AUTH":"",
        "EXCD":market,
        "SYMB":code,
        "GUBN":"0",
        "BYMD":"",
        "MODP":"0"
    }
    res = requests.get(URL, headers=headers, params=params)
    stck_oprc = float(res.json()['output2'][0]['open']) #오늘 시가
    return stck_oprc


def get_low_price(market="NAS", code="AAPL"):
    """변동성 돌파 전략으로 매수 목표가 조회"""
    PATH = "uapi/overseas-price/v1/quotations/dailyprice"
    URL = f"{URL_BASE}/{PATH}"
    headers = {"Content-Type":"application/json", 
        "authorization": f"Bearer {ACCESS_TOKEN}",
        "appKey":APP_KEY,
        "appSecret":APP_SECRET,
        "tr_id":"HHDFS76240000"}
    params = {
        "AUTH":"",
        "EXCD":market,
        "SYMB":code,
        "GUBN":"0",
        "BYMD":"",
        "MODP":"0"
    }
    res = requests.get(URL, headers=headers, params=params)
    stck_lwpr = float(res.json()['output2'][0]['low']) #오늘 저가    
    return stck_lwpr

def get_high_price(market="NAS", code="AAPL"):
    """변동성 돌파 전략으로 매수 목표가 조회"""
    PATH = "uapi/overseas-price/v1/quotations/dailyprice"
    URL = f"{URL_BASE}/{PATH}"
    headers = {"Content-Type":"application/json", 
        "authorization": f"Bearer {ACCESS_TOKEN}",
        "appKey":APP_KEY,
        "appSecret":APP_SECRET,
        "tr_id":"HHDFS76240000"}
    params = {
        "AUTH":"",
        "EXCD":market,
        "SYMB":code,
        "GUBN":"0",
        "BYMD":"",
        "MODP":"0"
    }
    res = requests.get(URL, headers=headers, params=params)
    stck_hgpr = float(res.json()['output2'][0]['high']) #오늘 고가
    return stck_hgpr


def get_min_price(market="NAS", code="AAPL"):
    """변동성 돌파 전략으로 매수 목표가 조회"""
    PATH = "uapi/overseas-price/v1/quotations/dailyprice"
    URL = f"{URL_BASE}/{PATH}"
    headers = {"Content-Type":"application/json", 
        "authorization": f"Bearer {ACCESS_TOKEN}",
        "appKey":APP_KEY,
        "appSecret":APP_SECRET,
        "tr_id":"HHDFS76240000"}
    params = {
        "AUTH":"",
        "EXCD":market,
        "SYMB":code,
        "GUBN":"0",
        "BYMD":"",
        "MODP":"0"
    }
    res = requests.get(URL, headers=headers, params=params)
    #stck_oprc = float(res.json()['output2'][0]['open']) #오늘 시가
    #stck_hgpr = float(res.json()['output2'][1]['high']) #전일 고가
    #stck_lwpr = float(res.json()['output2'][1]['low']) #전일 저가
    stck_clos = float(res.json()['output2'][1]['clos']) #전일 종가
    target_price = stck_clos * 0.95
    return target_price

def get_stock_balance():
    """주식 잔고조회"""
    PATH = "uapi/overseas-stock/v1/trading/inquire-balance"
    URL = f"{URL_BASE}/{PATH}"
    headers = {"Content-Type":"application/json", 
        "authorization":f"Bearer {ACCESS_TOKEN}",
        "appKey":APP_KEY,
        "appSecret":APP_SECRET,
        "tr_id":"JTTT3012R",
        "custtype":"P"
    }
    params = {
        "CANO": CANO,
        "ACNT_PRDT_CD": ACNT_PRDT_CD,
        "OVRS_EXCG_CD": "NASD",
        "TR_CRCY_CD": "USD",
        "CTX_AREA_FK200": "",
        "CTX_AREA_NK200": ""
    }
    res = requests.get(URL, headers=headers, params=params)
    stock_list = res.json()['output1']
    evaluation = res.json()['output2']
    stock_dict = {}
    send_message(f"====주식 보유잔고====")
    for stock in stock_list:
        if int(stock['ovrs_cblc_qty']) > 0:
            stock_dict[stock['ovrs_pdno']] = stock['ovrs_cblc_qty']
            send_message(f"{stock['ovrs_item_name']}({stock['ovrs_pdno']}): {stock['ovrs_cblc_qty']}주, 손익{stock['frcr_evlu_pfls_amt']}, 수익률{stock['evlu_pfls_rt']}")
            time.sleep(0.1)
    send_message(f"주식 평가 금액: ${evaluation['tot_evlu_pfls_amt']}")
    time.sleep(0.1)
    send_message(f"평가 손익 합계: ${evaluation['ovrs_tot_pfls']}")
    time.sleep(0.1)
    send_message(f"=================")
    return stock_dict

def get_stock_balance_noprint():
    """주식 잔고조회"""
    PATH = "uapi/overseas-stock/v1/trading/inquire-balance"
    URL = f"{URL_BASE}/{PATH}"
    headers = {"Content-Type":"application/json", 
        "authorization":f"Bearer {ACCESS_TOKEN}",
        "appKey":APP_KEY,
        "appSecret":APP_SECRET,
        "tr_id":"JTTT3012R",
        "custtype":"P"
    }
    params = {
        "CANO": CANO,
        "ACNT_PRDT_CD": ACNT_PRDT_CD,
        "OVRS_EXCG_CD": "NASD",
        "TR_CRCY_CD": "USD",
        "CTX_AREA_FK200": "",
        "CTX_AREA_NK200": ""
    }
    res = requests.get(URL, headers=headers, params=params)
    stock_list = res.json()['output1']    
    stock_dict = {}    
    for stock in stock_list:
        if int(stock['ovrs_cblc_qty']) > 0:
            stock_dict[stock['ovrs_pdno']] = (stock['ovrs_cblc_qty'], stock['evlu_pfls_rt'])
            time.sleep(1)
    return stock_dict

def get_profit_balance(code="AAPL"):
    """주식 수익률 조회"""
    PATH = "uapi/overseas-stock/v1/trading/inquire-balance"
    URL = f"{URL_BASE}/{PATH}"
    headers = {"Content-Type":"application/json", 
        "authorization":f"Bearer {ACCESS_TOKEN}",
        "appKey":APP_KEY,
        "appSecret":APP_SECRET,
        "tr_id":"JTTT3012R",
        "custtype":"P"
    }
    params = {
        "CANO": CANO,
        "ACNT_PRDT_CD": ACNT_PRDT_CD,
        "OVRS_EXCG_CD": "NASD",
        "TR_CRCY_CD": "USD",
        "CTX_AREA_FK200": "",        
        "CTX_AREA_NK200": ""
    }
    res = requests.get(URL, headers=headers, params=params)        
    stock_list = res.json()['output1']        
    for stock in stock_list:
        if code in stock['ovrs_pdno']:            
            profit_return = stock['evlu_pfls_rt']            
            time.sleep(0.1)
    return profit_return
    

def get_st_balance():
    """주식 잔고조회"""
    PATH = "uapi/overseas-stock/v1/trading/inquire-balance"
    URL = f"{URL_BASE}/{PATH}"
    headers = {"Content-Type":"application/json", 
        "authorization":f"Bearer {ACCESS_TOKEN}",
        "appKey":APP_KEY,
        "appSecret":APP_SECRET,
        "tr_id":"JTTT3012R",
        "custtype":"P"
    }
    params = {
        "CANO": CANO,
        "ACNT_PRDT_CD": ACNT_PRDT_CD,
        "OVRS_EXCG_CD": "NASD",
        "TR_CRCY_CD": "USD",
        "CTX_AREA_FK200": "",
        "CTX_AREA_NK200": ""
    }
    res = requests.get(URL, headers=headers, params=params)    
    evaluation = float(res.json()['output2']['tot_evlu_pfls_amt'])
    return evaluation



def get_tot_eval():
    """총 평가금액"""
    PATH = "uapi/domestic-stock/v1/trading/inquire-balance"
    URL = f"{URL_BASE}/{PATH}"
    headers = {"Content-Type":"application/json", 
        "authorization":f"Bearer {ACCESS_TOKEN}",
        "appKey":APP_KEY,
        "appSecret":APP_SECRET,
        "tr_id":"TTTC8434R",
        "custtype":"P",
    }
    params = {
        "CANO": CANO,
        "ACNT_PRDT_CD": ACNT_PRDT_CD,
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
    evaluation = res.json()['output2']
    send_message(f"총 평가 금액: {evaluation[0]['tot_evlu_amt']}원")
    return int(evaluation[0]['tot_evlu_amt'])

def get_balance():
    """현금 잔고조회"""
    PATH = "uapi/domestic-stock/v1/trading/inquire-psbl-order"
    URL = f"{URL_BASE}/{PATH}"
    headers = {"Content-Type":"application/json", 
        "authorization":f"Bearer {ACCESS_TOKEN}",
        "appKey":APP_KEY,
        "appSecret":APP_SECRET,
        "tr_id":"TTTC8908R",
        "custtype":"P",
    }
    params = {
        "CANO": CANO,
        "ACNT_PRDT_CD": ACNT_PRDT_CD,
        "PDNO": "005930",
        "ORD_UNPR": "65500",
        "ORD_DVSN": "01",
        "CMA_EVLU_AMT_ICLD_YN": "Y",
        "OVRS_ICLD_YN": "Y"
    }
    res = requests.get(URL, headers=headers, params=params)
    cash = res.json()['output']['nrcvb_buy_amt']
    send_message(f"주문 가능 현금 잔고: {cash}원")
    return int(cash)

def buy(market="NASD", code="APA", qty="1", price="24"):
    """미국 주식 지정가 매수"""
    PATH = "uapi/overseas-stock/v1/trading/order"
    URL = f"{URL_BASE}/{PATH}"
    data = {
        "CANO": CANO,
        "ACNT_PRDT_CD": ACNT_PRDT_CD,
        "OVRS_EXCG_CD": market,
        "PDNO": code,
        "ORD_DVSN": "00",
        "ORD_QTY": str(int(qty)),
        "OVRS_ORD_UNPR": f"{round(price,2)}",
        "ORD_SVR_DVSN_CD": "0"
    }
    headers = {"Content-Type":"application/json", 
        "authorization":f"Bearer {ACCESS_TOKEN}",
        "appKey":APP_KEY,
        "appSecret":APP_SECRET,
        "tr_id":"JTTT1002U",
        "custtype":"P",
        "hashkey" : hashkey(data)
    }
    print(f'data : {data}')
    print(f'headers : {headers}')
    res = requests.post(URL, headers=headers, data=json.dumps(data))
    if res.json()['rt_cd'] == '0':
        send_message(f"[매수 성공]{str(res.json())}")
        return True
    else:
        send_message(f"[매수 실패]{str(res.json())}")
        return False

def sell(market="NASD", code="AAPL", qty="1", price="0"):
    """미국 주식 지정가 매도"""
    PATH = "uapi/overseas-stock/v1/trading/order"
    URL = f"{URL_BASE}/{PATH}"
    data = {
        "CANO": CANO,
        "ACNT_PRDT_CD": ACNT_PRDT_CD,
        "OVRS_EXCG_CD": market,
        "PDNO": code,
        "ORD_DVSN": "00",
        "ORD_QTY": str(int(qty)),
        "OVRS_ORD_UNPR": f"{round(price*0.995,2)}",
        "ORD_SVR_DVSN_CD": "0"
    }
    headers = {"Content-Type":"application/json", 
        "authorization":f"Bearer {ACCESS_TOKEN}",
        "appKey":APP_KEY,
        "appSecret":APP_SECRET,
        "tr_id":"JTTT1006U",
        "custtype":"P",
        "hashkey" : hashkey(data)
    }
    res = requests.post(URL, headers=headers, data=json.dumps(data))
    if res.json()['rt_cd'] == '0':
        send_message(f"[매도 성공]{str(res.json())}")
        return True
    else:
        send_message(f"[매도 실패]{str(res.json())}")
        return False

def get_exchange_rate():
    """환율 조회"""
    PATH = "uapi/overseas-stock/v1/trading/inquire-present-balance"
    URL = f"{URL_BASE}/{PATH}"
    headers = {"Content-Type":"application/json", 
            "authorization": f"Bearer {ACCESS_TOKEN}",
            "appKey":APP_KEY,
            "appSecret":APP_SECRET,
            "tr_id":"CTRP6504R"}
    params = {
        "CANO": CANO,
        "ACNT_PRDT_CD": ACNT_PRDT_CD,
        "OVRS_EXCG_CD": "NASD",
        "WCRC_FRCR_DVSN_CD": "01",
        "NATN_CD": "840",
        "TR_MKET_CD": "01",
        "INQR_DVSN_CD": "00"
    }
    res = requests.get(URL, headers=headers, params=params)
    exchange_rate = 1270.0
    if len(res.json()['output2']) > 0:
        exchange_rate = float(res.json()['output2'][0]['frst_bltn_exrt'])
    return exchange_rate


# 자동매매 시작
try:
    ACCESS_TOKEN = get_access_token()

    nasd_symbol_list = []
    nyse_symbol_list = [] # 매수 희망 종목 리스트 (NYSE)
    amex_symbol_list = ["SOXL","SOXS"] # 매수 희망 종목 리스트 (AMEX)

    bought_list = [] # 매수 완료된 종목 리스트
    symbol_list = nasd_symbol_list + amex_symbol_list
    total_cash = get_balance() # 보유 현금 조회
    total_eval = get_st_balance() # 총 평가금액        
    exchange_rate = get_exchange_rate() # 환율 조회
    stock_dict = get_stock_balance() # 보유 주식 조회
    eval = get_st_balance()
    for sym in stock_dict.keys():
        bought_list.append(sym)        
    target_buy_count = 1 # 매수할 종목 수
    buy_percent = 0.9 # 종목당 매수 금액 비율
    set_high_price = 0
    set_low_price = 0
    target_price = 0
    buy_amount = ((total_cash / exchange_rate) + total_eval ) * buy_percent  # 종목별 주문 금액 계산 (달러)
    send_message(f"종목별 주문 예정금액 ({buy_amount})")
    soldout = False
    set_target = False
    start_flag = False
    buy_flag = False
    file_path = "/var/stockbot_us/symbol_prices.csv"


    symbol_prices = {
        "SOXL": {"current_price": None, "target_price": None, "set_low_price": None},
        "SOXS": {"current_price": None, "target_price": None, "set_low_price": None}
    }

    send_message("===해외 주식 자동매매 프로그램을 시작합니다===")
    buy('NASD', 'APA', 1, float(24.49))
    while True:

        buy()
        t_now = datetime.datetime.now(timezone('America/New_York')) # 뉴욕 기준 현재 시간
        t_9 = t_now.replace(hour=9, minute=0, second=0, microsecond=0)
        t_start = t_now.replace(hour=9, minute=34, second=59, microsecond=0)
        t_check = t_now.replace(hour=9, minute=35, second=5, microsecond=0) 
        t_sell = t_now.replace(hour=10, minute=2, second=5, microsecond=0)
        t_sell_end = t_now.replace(hour=15, minute=45, second=5, microsecond=0)
        t_exit = t_now.replace(hour=15, minute=50, second=0,microsecond=0)
        today = t_now.weekday()
        if today == 5 or today == 6:  # 토요일이나 일요일이면 자동 종료
            send_message("주말이므로 프로그램을 종료합니다.")
            break

        if t_9 < t_now < t_start and start_flag == False :  # AM 09:00 ~  09:34 : symbol 파일이 있다면 삭제 진행
            if os.path.exists(file_path):
                os.remove(file_path)        
                send_message(f"{file_path} 파일을 삭제했습니다.")
                start_flag = True
            else:        
                send_message(f"{file_path} 파일이 존재하지 않습니다.")  
                start_flag = True

        if t_start < t_now < t_check and buy_flag == False:  # AM 09:34 ~  09:35 : 매수
            for sym in symbol_list:
                market1 = "NASD"
                market2 = "NAS"
                if sym in amex_symbol_list:
                    market1 = "AMEX"
                    market2 = "AMS"
                current_price = get_current_price(market2, sym)
                init_price = get_init_price(market2, sym)
                set_high_price = get_current_price(market2, sym)
                set_low_price = get_low_price(market2, sym)
                target_price = init_price + ( set_high_price-init_price ) * 9
                target_price = round(target_price,2)

                symbol_prices[sym]["current_price"] = current_price
                symbol_prices[sym]["target_price"] = target_price
                symbol_prices[sym]["set_low_price"] = set_low_price

                send_message(f"{sym} 현재가 : {current_price}, 시초가 : {init_price}, 5분고가 {set_high_price}, 5분저가 {set_low_price}, 익절가 {target_price} ")       
                if current_price > init_price :
                    buy_qty = 0  # 매수할 수량 초기화
                    buy_qty = int(buy_amount // current_price)
                    if buy_qty > 0:
                        send_message(f"{sym} 시초가보다 5분봉 기준 현재 가격이 상승 ({current_price} > {init_price}) {buy_qty} 매수를 시도합니다.")
                        market1 = "NASD"
                        market2 = "NAS"
                        if sym in nyse_symbol_list:
                            market1 = "NYSE"
                            market2 = "NYS"
                        if sym in amex_symbol_list:
                            market1 = "AMEX"
                            market2 = "AMS"
                        if sym in bought_list:
                            continue
                        result = buy(market=market1, code=sym, qty=buy_qty, price=get_buy_price(market=market2, code=sym))

                        current_symbol_data = {
                                "Symbol":[sym],
                                "current_price":[symbol_prices[sym]["current_price"]],
                                "target_price":[symbol_prices[sym]["target_price"]],
                                "set_low_price":[symbol_prices[sym]["set_low_price"]]
                        }

                        df_current = pd.DataFrame(current_symbol_data)
                        df_current.to_csv('/var/stockbot_us/symbol_prices.csv', index=False)

                        time.sleep(1)
                        if result:
                            soldout = False
                            buy_flag = True
                            bought_list.append(sym)
                            get_stock_balance()
            time.sleep(2)

        if t_sell < t_now < t_sell_end :  # AM 09:55 ~ PM 03:45 : 매도
            if set_target == False :

                if os.path.exists(file_path):
                     send_message(f"{file_path} 파일이 있습니다!.")
                else:
                     send_message(f"{file_path} 파일이 존재하지 않습니다.프로그램을 종료합니다.")
                     break

                df = pd.read_csv('/var/stockbot_us/symbol_prices.csv', index_col=None)
                set_low_price = df['set_low_price'].values[0]
                target_price  = df['target_price'].values[0]
                sym = df['Symbol'].values[0]
                buy_price = df['current_price'].values[0]
                set_target = True 
                send_message(f"{sym} : 매수가 {buy_price}, 익절가 {target_price}, 손절가 {set_low_price} ")

                stock_dict = get_stock_balance()
                if stock_dict is None or not stock_dict:
                    send_message(f"보유 주식이 없어 프로그램을 종료 합니다.")
                    break

            for sym, qty in stock_dict.items():
                market1 = "NASD"
                market2 = "NAS"
                if sym in nyse_symbol_list:
                    market1 = "NYSE"
                    market2 = "NYS"
                if sym in amex_symbol_list:
                    market1 = "AMEX"
                    market2 = "AMS"
                current_price = get_current_price(market2, sym)
                if current_price < set_low_price:
                    send_message(f"{sym} 손절가 달성({set_low_price}) 매도를 시도합니다.")
                    sell(market=market1, code=sym, qty=qty, price=get_sell_price(market=market2, code=sym))
                    get_stock_balance()

                    if os.path.exists(file_path):
                        os.remove(file_path)        
                        send_message(f"{file_path} 파일을 삭제했습니다.")
                    else:        
                        send_message(f"{file_path} 파일이 존재하지 않습니다.")  

                    stock_dict = get_stock_balance()
                    if stock_dict is None or not stock_dict:
                        send_message(f"보유 주식이 없어 프로그램을 종료 합니다.")
                        break

                    time.sleep(1)
                if current_price > target_price :
                    send_message(f"{sym} 익절가 도달( {current_price} > {target_price} 로 매도를 시도합니다.")
                    sell(market=market1, code=sym, qty=qty, price=get_sell_price(market=market2, code=sym))                        
                    get_stock_balance()

                    if os.path.exists(file_path):
                        os.remove(file_path)        
                        send_message(f"{file_path} 파일을 삭제했습니다.")
                    else:        
                        send_message(f"{file_path} 파일이 존재하지 않습니다.")  

                    stock_dict = get_stock_balance()
                    if stock_dict is None or not stock_dict:
                        send_message(f"보유 주식이 없어 프로그램을 종료 합니다.")
                        break

                    time.sleep(1)
                time.sleep(1)
            if t_now.minute % 20 == 0 and t_now.second <= 5: 
                get_stock_balance()
                time.sleep(1)

        if t_sell_end < t_now < t_exit :  # PM 03:45 ~ PM 03:50 : 일괄 매도
            if soldout == False:
                stock_dict = get_stock_balance()
                for sym, qty in stock_dict.items():
                    market1 = "NASD"
                    market2 = "NAS"
                    if sym in nyse_symbol_list:
                        market1 = "NYSE"
                        market2 = "NYS"
                    if sym in amex_symbol_list:
                        market1 = "AMEX"
                        market2 = "AMS"                                        
                    sell(market=market1, code=sym, qty=qty, price=get_sell_price(market=market2, code=sym))

                if os.path.exists(file_path):
                    os.remove(file_path)        
                    send_message(f"{file_path} 파일을 삭제했습니다.")
                else:        
                    send_message(f"{file_path} 파일이 존재하지 않습니다.")  

                soldout = True
                bought_list = []
                time.sleep(1)

        if t_exit < t_now:  # PM 03:50 ~ :프로그램 종료
            send_message("프로그램을 종료합니다.")
            break
except Exception as e:
    send_message(f"[오류 발생]{e}")
    time.sleep(1)
