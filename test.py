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

def save_access_token(token):
    """토큰 DB 저장"""
    print(f'save access token() called with token : {token}')
    df_token = pd.DataFrame([[token, datetime.datetime.now()]], columns=['token', 'timestamp'])
    engine, con, mycursor = db_conn()
    print(df_token)
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
        df_kis_token = pd.DataFrame(result)
        con.close()

        if datetime.datetime.now() - df_kis_token.timestamp[0] < timedelta(hours=24):
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
    "appkey":APP_KEY, 
    "appsecret":APP_SECRET}
    PATH = "oauth2/tokenP"
    URL = f"{URL_BASE}/{PATH}"
    res = requests.post(URL, headers=headers, data=json.dumps(body))
    ACCESS_TOKEN = res.json()["access_token"]
    save_access_token(ACCESS_TOKEN)
    shared_vars.ACCESS_TOKEN = ACCESS_TOKEN

    return ACCESS_TOKEN

def get_stock_to_buy(strategy="1"):
    """매수 종목 가져오기"""
    engine, con, mycursor = db_conn() 
    sql = f"select * from strategy_{strategy}"
    # print(f'sql : {sql}')
    mycursor.execute(sql)
    result = mycursor.fetchall()
    df_buy_stock = pd.DataFrame(result)
    # print(f'print sql result : {result}')
    # for item in result :
    print(df_buy_stock)
    con.close()    
    for item in df_buy_stock.code:
        print(item)
    return df_buy_stock.code

def upload_stock_list(list=['35900', '35760'], table_name='strategy_1'):

    columns = ['strategy', 'code','upload_date']

    today = date.today()
    print(f'today is {today}')
    stock_list = []

    for stock in list:
        print(f'stock : {stock}') 
        stock_list.append([table_name, stock, today])

    # print(stock_list)
    df_list = pd.DataFrame(stock_list , columns=columns)
    # print(df_list)

    engine, con, mycursor = db_conn()
    df_list.to_sql(name = 'strategy_1', con=engine, if_exists='append', index=False)
    con.close()

def get_stock_balance():
    """주식 잔고조회"""
    print("주식 잔고조회")
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
    # print(f'Header : {headers}')
    # print(f'Params : {params}')
    # print(f'URL : {URL}')
    res = requests.get(URL, headers=headers, params=params)
    print(f'res : {res}')
    stock_list = res.json()['output1']
    evaluation = res.json()['output2']
    stock_dict = {}
    # print(f"====주식 보유잔고====")
    print(f"====주식 보유잔고====")
    for stock in stock_list:
        if int(stock['hldg_qty']) > 0:
            stock_dict[stock['pdno']] = stock['hldg_qty']
            print(f"{stock['prdt_name']}({stock['pdno']}): {stock['hldg_qty']}주 , 평가손익 : {stock['evlu_pfls_amt']}")
            time.sleep(0.1)
    print(f"주식 평가 금액: {evaluation[0]['scts_evlu_amt']}원")
    time.sleep(0.1)
    print(f"평가 손익 합계: {evaluation[0]['evlu_pfls_smtl_amt']}원")
    time.sleep(0.1)
    print(f"총 평가 금액: {evaluation[0]['tot_evlu_amt']}원")
    time.sleep(0.1)
    print(f"=================")
    return stock_dict

def get_target_price(code="005930"):
    print('현재시세 조회')
    PATH = "uapi/domestic-stock/v1/quotations/inquire-price"
    URL = f"{URL_BASE}/{PATH}"
    headers = {"Content-Type":"application/json", 
        "authorization": f"Bearer {ACCESS_TOKEN}",
        "appKey":APP_KEY,
        "appSecret":APP_SECRET,
        "tr_id":"FHKST01010100"}
    params = {
    "fid_cond_mrkt_div_code":"J",
    "fid_input_iscd":code,
    }

    print(f'Header : {headers}')
    print(f'Params : {params}')
    print(f'URL : {URL}')
    res = requests.get(URL, headers=headers, params=params)
    print(f'res : {res}')

    # res.json()['output']['ord_psbl_cash']
    stck_stat = res.json()['output']['iscd_stat_cls_code'] #상태구분
    print(f'상태 : {stck_stat}')
    stck_prpr = int(res.json()['output']['stck_prpr']) # 현재가 
    print(f'현재가 : {stck_prpr}')
    aspr_unit = int(res.json()['output']['aspr_unit']) #호가 단위
    print(f'호가단위 : {aspr_unit}')
    target_price = stck_prpr - (aspr_unit * 2)
    print(f'목표 매수가 : {target_price}')

    return target_price

## 2. 관심종목 그룹 조회
def get_interest_groups():
    PATH = "/uapi/domestic-stock/v1/quotations/intstock-grouplist"
    URL = f"{URL_BASE}/{PATH}"
    params = {
        "TYPE": "1",
        "FID_ETC_CLS_CODE": "00",
        "USER_ID": HTS_ID
    }
    headers = {
        "content-type": "application/json",
        "authorization": f"Bearer {ACCESS_TOKEN}",
        "appKey": APP_KEY,
        "appSecret": APP_SECRET,
        "tr_id": "HHKCM113004C7",  # 실전투자
        "custtype": "P"
    }
    time.sleep(0.05) # 유량제한 예방 (REST: 1초당 20건 제한)
    res = requests.get(URL, headers=headers, params=params)
    print(res.json())
    print(res.json()['output2'][0]['inter_grp_code'])
    print(res.json()['output2'][0]['inter_grp_name'])
    return res.json()

def calculate_stock_metrics_from_db(stock_code, start_date, end_date):

    engine, con, mycursor = db_conn() 

    query = f"""
        SELECT code, date, close
        FROM kr_stock_price
        WHERE code = '{stock_code}' AND Date BETWEEN STR_TO_DATE('{start_date}', '%Y%m%d') AND STR_TO_DATE('{end_date}', '%Y%m%d')
    """
    # print(f'query : {query}')
    mycursor.execute(query)
    result = mycursor.fetchall()
    df_stock_price = pd.DataFrame(result)
    # print(f'print sql result : {result}')
    # for item in result :
    # print(df_stock_price)
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

                if buy(row.pdno, buy_qty,row.prpr, '01'):
                    buy_count = buy_count + 1
        
        send_message(f"[최대이익증가 매수] {buy_count} 종목 추가 매수됨")
        return buy_count


def update_holding_stock_details():
    send_message(f"[최대 이익금액 업데이트]")
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
    print(f'res : {res}')

    engine, con, mycursor = db_conn()
    sql = "select * from holding_stock_details"
    mycursor.execute(sql)
    result = mycursor.fetchall()
    con.close()
    df_holding_stock_details = pd.DataFrame(result)

    df_holding_stock_details.loc[df_holding_stock_details.pdno == '005930', 'evlu_pfls_amt'] = 109999
    df_holding_stock_details.loc[df_holding_stock_details.pdno == '071050', 'evlu_pfls_amt'] = 100002
    df_holding_stock_details.loc[df_holding_stock_details.pdno == '360200', 'evlu_pfls_amt'] = 1000000

    # curr_account_stock_list = res.json()['output1']    
    # print(f'curr_account_stock_list : {curr_account_stock_list}')
    # df_curr_account_stock = pd.DataFrame(curr_account_stock_list)
    # df_curr_account_stock = df_curr_account_stock[['pdno','prdt_name','prpr','evlu_amt','evlu_pfls_amt','evlu_pfls_rt']]

    # Test code
    df_curr_account_stock = df_holding_stock_details 
    # Test Code

    # df_curr_account_stock.loc[df_curr_account_stock.pdno == '005930','evlu_pfls_amt'] = -30006
    # df_curr_account_stock.loc[df_curr_account_stock.pdno == '071050','evlu_pfls_amt'] = -30002
    print(f'df_curr_account_stock : {df_curr_account_stock}')

    holding_stock_tbl_col_list= ['pdno','prdt_name','init_dt','prpr','evlu_amt','evlu_pfls_amt','evlu_pfls_rt']

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

    df_merged['profit_index_a'] = df_merged['evlu_pfls_amt_a'].apply(lambda x : get_tier_ratio(x, shared_vars.df_up_buy_table)).apply(lambda x: x[1]).tolist()
    df_merged['profit_index_b'] = df_merged['evlu_pfls_amt_b'].apply(lambda x : get_tier_ratio(x, shared_vars.df_up_buy_table)).apply(lambda x: x[1]).tolist()
    df_merged['buy_on_up_flag'] = df_merged['profit_index_a'] > df_merged['profit_index_b']

    # print(f'merged : {df_merged}')
    # print(f'df_curr_account_stock desc : {df_curr_account_stock.dtypes}')
    # print(f'df_holding_stock_details desc : {df_holding_stock_details.dtypes}')
    print(f'merged desc : {df_merged.dtypes}')

    # 현재가 기준 이익 금액이 holding_stock_details 테이블의 이익금액 보다 큰 종목과 이익금액 조회
    df_max_profit_for_update = df_merged[df_merged['evlu_pfls_amt_a'].astype(float) > df_merged['evlu_pfls_amt_b'].astype(float)][['pdno', 'evlu_pfls_amt_a', 'buy_on_up_flag']]

    df_max_profit_for_update

    # 결과 출력
    print(f'df_max_profit_for_update: {df_max_profit_for_update}')

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

    print(f'len(df_max_profit_for_update) : {len(df_max_profit_for_update)}')

    # 최대이익 증가 종목에 대해 init_dt 오늘자로 update
    if len(df_max_profit_for_update) > 0: 
        df_update_stock_list = df_curr_account_stock[df_curr_account_stock['pdno'].isin(df_max_profit_for_update.pdno)]
        df_update_stock_list.loc[:,'init_dt'] = date.today()
        df_update_stock_list = df_update_stock_list[holding_stock_tbl_col_list]
        df_update_stock_list.loc[:,'buy_on_up_flag'] = df_max_profit_for_update['buy_on_up_flag']
        print(f'df_update_stock_list : {df_update_stock_list}')

    # 신규 편입된 종목에 대해서 holding_stock_details 에 레코드 insert
    for stock_code in df_merged[df_merged['evlu_pfls_amt_b'].isna()].pdno  :
        print(stock_code)
        dt_temp = df_curr_account_stock[df_curr_account_stock.pdno == stock_code]
        dt_temp.loc[:,'init_dt'] = date.today()
        dt_temp = dt_temp[holding_stock_tbl_col_list]
        engine, con, mycursor = db_conn()
        dt_temp.to_sql(name = 'holding_stock_details', con=engine, if_exists='append', index=False)
        con.close()

        df_update_stock_list = pd.concat([df_update_stock_list,dt_temp] )

    print(f'df_update_stock_list : {df_update_stock_list}')
    
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
    
    send_message(f"====== 익절 처리({profitcut_ratio}%, {cut_amt}) =====")
    for stock in stock_list:
        code = stock['pdno']
        current_profit = int(stock['evlu_pfls_amt'])
        profit_ratio = float(stock['evlu_pfls_rt'])
        stock_name = stock['prdt_name']
        curr_price = int(stock['prpr'])
        holding_qty = int(stock['hldg_qty'])
        max_profit = int(df_holding_stock_details.loc[df_holding_stock_details.pdno == stock['pdno']].evlu_pfls_amt[0])
        # print(f'{stock_name} 현재 수익률 : {current_profit}, 최고 수익 : {max_profit}, 익절 최소금액 : {cut_amt}, 익절 비율 : {profitcut_ratio}, 수익률 : {profit_ratio}')
        # print(f' 익절 기준금액 : {int((profitcut_ratio/100)*max_profit)}')
        
        # 최고 이익 금액이 cut_amt(10만) 를 초과 했을때 현재의 이익 금액이 최고 이익 금액 대비 profit_cut_ratio(50%) 이상 하락시 익절
        if (max_profit > cut_amt) and ( int((profitcut_ratio/100)*max_profit) > curr_price ):
            stock_dict[code] = profit_ratio
            send_message(f"[익절 종목]{stock_name}({code}) 수익률: {profit_ratio}")
            time.sleep(0.1)
            sell(code, holding_qty)
    
    send_message(f"================================")
    stock_dict.keys()

    return stock_dict

def profit_cut():
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

    print(f'merged : {df_merged}')

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

    print(f'len(df_max_profit_for_update) : {len(df_max_profit_for_update)}')

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

    print(f'df_update_stock_list at the end : {df_update_stock_list}')
    
    send_message(f"[최대 이익금액 업데이트] {len(df_update_stock_list)} 종목 업데이트 됨")
    
    return df_update_stock_list    


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
        print(f'Result : {result}')
        con.commit()
        con.close() 
        print(f'Deletion for {stock} from {tname} done')
        return True

    else:
        print(f'Input Not valid : {tname}, {stock}')
        return False

try:

    print("test 시작")

    ACCESS_TOKEN = get_access_token()

    # match ind : 
    ind = -6
    if ind == 0:
        print('ind == 0')
    elif ind == -6:

        # delete_stock_from_table('holding_stock_details_test', '277810')
        shared_vars.df_fav_stocks = get_group_stocks('001') # 000 관심그룹 
        
        print(shared_vars.df_fav_stocks.code[0])
        symbol = shared_vars.df_fav_stocks.code[0]
        engine, con, mycursor = db_conn() 
        sql = f"""select market 
                    from us_stock_list 
                    where Symbol = '{symbol}'"""
        print(f'sql: {sql}')
        mycursor.execute(sql)
        result = mycursor.fetchall()
        market = result[0]['market']
        print(f'market : {market}')


    elif ind == -5:    
        # profitcut_sell()
       
        engine, con, mycursor = db_conn() 

        query_date = date.today().strftime('%Y%m%d')
        stock = '277810'

        query = f"""
            SELECT *
            FROM holding_stock_details
        """
        mycursor.execute(query)
        result = mycursor.fetchall()
        df_stock = pd.DataFrame(result)
        con.close() 
        print(df_stock)
        
        df_stock.to_sql(name = 'holding_stock_details_test', con=engine, if_exists='replace', index=False)

    elif ind == -4:

        df_return = update_holding_stock_details()
        print(f'update_holding_stock_details return : {df_return.loc[df_return.buy_on_up_flag == True]}')
        buy_on_profit(df_return.loc[df_return.buy_on_up_flag == True], shared_vars.df_up_buy_table)
    elif ind == -3:
    
        end_date = date.today().strftime('%Y%m%d')
        start_date = (date.today() - timedelta(days=10)).strftime('%Y%m%d')
        result = calculate_stock_metrics_from_db('005490', start_date, end_date)
        print(result)
        print(result['ret'])

    elif ind == -2:
        get_pending_orders()
        
        print('ind == -2')
        
    elif ind == -1:

        print('test JSON')
        json_string = '''
        [    {
            "ctx_area_fk100": "81055689^01^                                                                                        ",
            "ctx_area_nk100": "                                                                                                    ",
            "output": [  
                {
                "ord_gno_brno": "06010",
                "odno": "0001569139",
                "orgn_odno": "0001569136",
                "ord_dvsn_name": "지정가",
                "pdno": "009150",
                "prdt_name": "SamsungElecMech",
                "rvse_cncl_dvsn_name": "BUY AMEND*",
                "ord_qty": "1",
                "ord_unpr": "140000",
                "ord_tmd": "131438",
                "tot_ccld_qty": "0",
                "tot_ccld_amt": "0",
                "psbl_qty": "1",
                "sll_buy_dvsn_cd": "02",
                "ord_dvsn_cd": "00",
                "mgco_aptm_odno": ""
                },
                {
                "ord_gno_brno": "06010",
                "odno": "0001569138",
                "orgn_odno": "",
                "ord_dvsn_name": "지정가",
                "pdno": "009150",
                "prdt_name": "SamsungElecMech",
                "rvse_cncl_dvsn_name": "",
                "ord_qty": "1",
                "ord_unpr": "200000",
                "ord_tmd": "131421",
                "tot_ccld_qty": "0",
                "tot_ccld_amt": "0",
                "psbl_qty": "1",
                "sll_buy_dvsn_cd": "02",
                "ord_dvsn_cd": "00",
                "mgco_aptm_odno": ""
                }
            ],
            "rt_cd": "0",
            "msg_cd": "KIOK0510",
            "msg1": "조회가 완료되었습니다                                                           "
            }
        ]
        '''

        # JSON 문자열 파싱
        parsed_json = json.loads(json_string)
        print(f'json : {parsed_json}')

        # DataFrame 변환
        df = pd.DataFrame(parsed_json[0]['output'])

        print(df)
        print(df.columns)


    elif ind == 1:

        print('1')
        engine, con, mycursor = db_conn() 
        sql = "select * from krx_list"
        print(f'print sql : {sql}')
        mycursor.execute(sql)
        result = mycursor.fetchall()
        print(f'print sql result : {result[0]}')
        # con.close()
        df_krx_list = pd.DataFrame(result)
        print(f'after dataframe creation')
        con.close()
        print(f'after con.close()')
        df_kospi = df_krx_list.loc[df_krx_list.Market=='KOSPI']
        # print(df_kospi.head())
        # print(f'print KOSPI : {df_kospi}')
        # logging.debug("이것은 DEBUG 로그입니다.")
    elif ind == 9:
        print(f'shared access token : {shared_vars.ACCESS_TOKEN}')
        print(get_group_stocks('000'))
        df_list = pd.DataFrame(get_group_stocks('000'), columns=['code', 'kor_name'])
        print(df_list)

    elif ind == 8:

        get_interest_groups()

    elif ind == 2:

        upload_stock_list(['005930', '000660'])
    elif ind == 3:

        get_stock_balance()
    elif ind == 4:

        get_stock_to_buy()
    elif ind == 5:

        get_target_price()        
    elif ind == 6:

        # get_target_price()

        # df_order_hist = pd.DataFrame(columns = ['code', 'price', 'order_time', 'order_no', 'bought_ind'])

        # item = ['00000', 1234, datetime.now(), 'fdsafds', 'Y']
        # df_new_row = pd.DataFrame([item], columns=df_order_hist.columns)
        # df_order_hist = pd.concat([df_order_hist, df_new_row], ignore_index=True)
        # print(df_order_hist)

        df_token = pd.DataFrame([['eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzUxMiJ9.eyJzdWIiOiJ0b2tlbiIsImF1ZCI6ImEyMTJmZDRmLWY1ZmItNDczMS1hOGIyLTI0ZmEzZjYxOGU2YyIsInByZHRfY2QiOiIiLCJpc3MiOiJ1bm9ndyIsImV4cCI6MTczNDY5MzMyMSwiaWF0IjoxNzM0NjA2OTIxLCJqdGkiOiJQU3dMQnRGMFdWQ3lFSjJ2UklTbjRjanpmSUZyZlpjVzRpa3UifQ.exT_6qOVI7MRQPdWsu-qsPOQb-y88TXbx2iVN_J9UyaEgyvLj5BVxVQOzbAMsvw8uKIdDxC9PyUc-n9gfQmTow', datetime.now()]], columns=['token', 'timestamp'])
        engine, con, mycursor = db_conn()
        df_token.to_sql(name = 'kis_token', con=engine, if_exists='replace', index=False)
        con.close()

    elif ind == 7:

        engine, con, mycursor = db_conn()

        sql = "select * from kis_token"
        # print(f'print sql : {sql}')
        mycursor.execute(sql)
        result = mycursor.fetchall()
        # print(f'print sql result : {result[0]}')
        # con.close()
        df_kis_token = pd.DataFrame(result)
        print(f'after dataframe creation')

    else:
        print('unknown')

except Exception as e:
    # print(f"[오류 발생]{e}")
    print(f'오류발생 : {e}')
    # time.sleep(1)