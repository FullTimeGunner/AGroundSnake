# modified at 2023/05/18 22::25
import os
import datetime
import random
import tushare as ts
from scipy.constants import golden


def latest_trading_day(days: int = None) -> datetime.datetime:
    dt = datetime.datetime.now()
    if days is None:
        dt_pos = dt
    else:
        dt_pos = dt + datetime.timedelta(days=days)
    if dt_pos > dt:
        str_dt_end = dt_pos.strftime("%Y%m%d")
    else:
        str_dt_end = dt.strftime("%Y%m%d")
    pro = ts.pro_api()
    df_trade = pro.trade_cal(exchange="", start_date="20230401", end_date=str_dt_end)
    df_trade.set_index(keys=["cal_date"], inplace=True)
    str_dt_pos = dt_pos.strftime("%Y%m%d")
    if df_trade.at[str_dt_pos, "is_open"] == 1:
        str_dt_return = str_dt_pos
    else:
        str_dt_return = df_trade.at[str_dt_pos, "pretrade_date"]
    dt_return = datetime.datetime.strptime(str_dt_return, "%Y%m%d")
    return dt_return


def all_ts_code() -> list | None:
    pro = ts.pro_api()
    df_basic = pro.stock_basic(
        exchange="",
        list_status="L",
        fields="ts_code,symbol,name,area,industry,list_date",
    )
    if len(df_basic) == 0:
        return
    else:
        list_ts_code = df_basic["ts_code"].tolist()
        random.shuffle(list_ts_code)
        return list_ts_code


def all_chs_code() -> list | None:
    list_ts_code = all_ts_code()
    if list_ts_code:
        list_chs_code = [item[-2:].lower() + item[:6] for item in list_ts_code]
        return list_chs_code
    else:
        return


# dt_now = datetime.datetime.now()
path_main = os.getcwd()
path_data = os.path.join(path_main, "data")
if not os.path.exists(path_data):
    os.mkdir(path_data)
path_history = os.path.join(path_main, "history")
if not os.path.exists(path_history):
    os.mkdir(path_history)
path_check = os.path.join(path_main, "check")
if not os.path.exists(path_check):
    os.mkdir(path_check)
path_index = os.path.join(path_data, f"index")
if not os.path.exists(path_index):
    os.mkdir(path_index)
path_log = os.path.join(path_data, f"log")
if not os.path.exists(path_log):
    os.mkdir(path_log)
path_mv = os.path.join(path_data, f"mv")
if not os.path.exists(path_mv):
    os.mkdir(path_mv)
dt_trading = latest_trading_day()
dt_trading_last_1T = latest_trading_day(days=-1)
dt_trading_last_T1 = latest_trading_day(days=1)
dt_date_trading = dt_trading.date()
dt_date_trading_last_1T = dt_trading_last_1T.date()
dt_date_trading_last_T1 = dt_trading_last_T1.date()
str_date_trading = dt_date_trading.strftime("%Y%m%d")
time_am_0500 = datetime.time(hour=5, minute=0, second=0, microsecond=0)
time_am_0910 = datetime.time(hour=9, minute=10, second=0, microsecond=0)
time_am_1015 = datetime.time(hour=10, minute=15, second=0, microsecond=0)
time_am_start = datetime.time(hour=9, minute=30, second=0, microsecond=0)
time_am_end = datetime.time(hour=11, minute=30, second=0, microsecond=0)
time_pm_start = datetime.time(hour=13, minute=0, second=0, microsecond=0)
time_pm_1457 = datetime.time(hour=14, minute=57, second=0, microsecond=0)
time_pm_end = datetime.time(hour=15, minute=0, second=0, microsecond=0)
dt_init = datetime.datetime(year=1989, month=1, day=1, hour=15)
dt_am_0500 = datetime.datetime.combine(dt_date_trading, time_am_0500)
dt_am_0910 = datetime.datetime.combine(dt_date_trading, time_am_0910)
dt_am_1015 = datetime.datetime.combine(dt_date_trading, time_am_1015)
dt_am_start = datetime.datetime.combine(dt_date_trading, time_am_start)
dt_am_end = datetime.datetime.combine(dt_date_trading, time_am_end)
dt_pm_start = datetime.datetime.combine(dt_date_trading, time_pm_start)
dt_pm_1457 = datetime.datetime.combine(dt_date_trading, time_pm_1457)
dt_pm_end = datetime.datetime.combine(dt_date_trading, time_pm_end)
dt_pm_end_last_1T = datetime.datetime.combine(dt_date_trading_last_1T, time_pm_end)


def str_date_path() -> str:
    dt_now = datetime.datetime.now()
    if dt_now < dt_pm_end:
        return dt_date_trading_last_1T.strftime("%Y_%m_%d")
    else:
        return dt_date_trading.strftime("%Y_%m_%d")


def str_trading_path() -> str:
    dt_now = datetime.datetime.now()
    if dt_now < dt_am_0500:
        return dt_date_trading_last_1T.strftime("%Y_%m_%d")
    elif dt_am_0500 < dt_now < dt_pm_end:
        return dt_date_trading.strftime("%Y_%m_%d")
    else:
        return dt_trading_last_T1.strftime("%Y_%m_%d")


filename_log = os.path.join(path_log, "log{time}.log")
filename_input = os.path.join(path_main, f"input.xlsx")
filename_trader_template = os.path.join(path_main, f"trader.xlsx")
filename_chip_shelve = os.path.join(path_data, f"chip")
filename_market_values_shelve = os.path.join(path_mv, f"mv")
filename_chip_excel = os.path.join(path_check, f"chip_{str_date_path()}.xlsx")
filename_signal = os.path.join(path_check, f"signal_{str_trading_path()}.xlsx")
filename_data_csv = os.path.join(path_check, f"trader_{str_trading_path()}.csv")
filename_index_charts = os.path.join(path_check, f"index_charts.html")
filename_concentration_rate_charts = os.path.join(
    path_check, f"concentration_rate_charts.html"
)
list_all_stocks = all_chs_code()
fall = -5
rise = 10000 / (100 + fall) - 100  # rise = 5.26315789473683
phi = 1 / golden  # extreme and mean ratio 黄金分割常数
phi_100 = phi * 100
