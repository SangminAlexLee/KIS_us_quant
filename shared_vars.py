import pandas as pd

pgm_name = 'kis_us_quant'
URL_BASE = ''
HTS_ID = ''
ACCESS_TOKEN = ''
APP_KEY = ''
APP_SECRET = ''
curr_stock_list = ''
df_fav_stocks = ''
l_curr_stock = ''
df_order_hist = ''
df_pending_orders = ''
lack_of_cash_flag = False
init_bought_count = 0
tier_data = { 'amount':[ 50, 100, 200, 400, 700, 1100, 1600, 2000, 3000, 5000, 10000, 9999999999], 
        'buy_ratio': [0, 30, 50, 40, 30, 20, 20, 20, 20, 20, 30, 30]}
df_up_buy_table = pd.DataFrame(tier_data)
holding_db_table = 'holding_us_stock_details'
stock_price_db_table = 'us_stock_price'