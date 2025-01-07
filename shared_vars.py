import pandas as pd

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
tier_data = { 'amount':[ 50000, 100000, 200000, 400000, 700000, 1100000, 1600000, 2000000, 3000000, 5000000, 10000000, 9999999999], 
        'buy_ratio': [0, 30, 50, 40, 30, 20, 20, 20, 20, 20, 30, 30]}
df_up_buy_table = pd.DataFrame(tier_data)