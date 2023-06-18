# modified at 2023/05/18 22::25
from __future__ import annotations
import os
import datetime
import time
import random
import feather
import pandas as pd
from loguru import logger
import akshare as ak
import analysis.base
from analysis.const import (
    dt_init,
    path_data,
    str_date_path,
    dt_date_trading,
    time_pm_end,
    filename_chip_shelve,
    str_date_trading,
    list_all_stocks,
)


def limit_count(list_symbol: list | str = None) -> bool:
    name: str = "df_limit"
    if list_symbol is None:
        logger.trace("list_code is None")
        list_symbol = list_all_stocks
    elif isinstance(list_symbol, str):
        list_symbol = [list_symbol]
    start_loop_time = time.perf_counter_ns()
    logger.trace(f"{name} Begin")
    file_name_df_limit_temp = os.path.join(
        path_data, f"df_limit_count_temp_{str_date_path()}.ftr"
    )
    dt_delta = dt_date_trading - datetime.timedelta(days=366)
    str_delta = dt_delta.strftime("%Y%m%d")
    dt_limit = dt_init
    if analysis.base.is_latest_version(key=name, filename=filename_chip_shelve):
        logger.trace("Limit Break End")
        return True
    if os.path.exists(file_name_df_limit_temp):
        df_limit = feather.read_dataframe(source=file_name_df_limit_temp)
        df_limit = df_limit.sample(frac=1)
    else:
        list_columns = [
            "times_limit",
            "up_7pct_times",
            "down_7pct_times",
            "correct_7pct_times",
            "up_3pct_times",
            "down_3pct_times",
            "correct_3pct_times",
            "alpha_amplitude",
            "alpha_pct",
            "alpha_turnover",
            "T5_amplitude",
            "T240_amplitude",
            "T5_pct",
            "T240_pct",
            "T5_turnover",
            "T240_turnover",
        ]
        df_limit = pd.DataFrame(index=list_symbol, columns=list_columns)
    df_limit.fillna(value=0, inplace=True)
    i = 0
    count = len(df_limit)
    logger.trace(f"For loop Begin")
    for symbol in df_limit.index:
        i += 1
        if random.randint(0, 5) == 3:
            feather.write_dataframe(df=df_limit, dest=file_name_df_limit_temp)
        str_msg_bar = f"Limit Update: [{i:4d}/{count:4d}] - [{symbol}]"
        if df_limit.at[symbol, "times_limit"] != 0:
            print(f"\r{str_msg_bar} - Exist\033[K", end="")
            continue
        df_stock = pd.DataFrame()
        i_times = 0
        while i_times < 1:
            i_times += 1
            try:
                df_stock = ak.stock_zh_a_hist(
                    symbol=symbol[2:8],
                    period="daily",
                    start_date=str_delta,
                    end_date=str_date_trading,
                )
            except KeyError as e:
                print(f"\r{str_msg_bar} - Sleep({i_times}) - {repr(e)}\033[K")
                time.sleep(1)
            except OSError as e:
                print(f"\r{str_msg_bar} - Sleep({i_times}) - {repr(e)}\033[K")
                time.sleep(1)
            else:
                if df_stock.empty:
                    print(f"\r{str_msg_bar} - Sleep({i_times}) - empty\033[K")
                    time.sleep(1)
                else:
                    df_stock.rename(
                        columns={
                            "日期": "dt",
                            "开盘": "open",
                            "收盘": "close",
                            "最高": "high",
                            "最低": "low",
                            "成交量": "volume",
                            "成交额": "amount",
                            "振幅": "amplitude",
                            "涨跌幅": "pct_chg",
                            "涨跌额": "change",
                            "换手率": "turnover",
                        },
                        inplace=True,
                    )
                    df_stock["dt"] = df_stock["dt"].apply(
                        func=lambda x: datetime.datetime.combine(
                            pd.to_datetime(x).date(), time_pm_end
                        )
                    )
                    df_stock.set_index(keys="dt", inplace=True)
                    df_stock.sort_index(ascending=True, inplace=True)
                    break
        if df_stock.empty:
            print(f"\r{str_msg_bar} - No Data\033[K")
            continue
        dt_stock_latest = df_stock.index.max()
        if dt_limit < dt_stock_latest:
            dt_limit = dt_stock_latest
        df_limit.at[symbol, "times_limit"] = len(df_stock)
        df_up_10pct = df_stock[df_stock["pct_chg"] > 7]
        df_limit.at[symbol, "up_7pct_times"] = len(df_up_10pct)
        df_down_10pct = df_stock[df_stock["pct_chg"] < -7]
        df_limit.at[symbol, "down_7pct_times"] = len(df_down_10pct)
        df_limit.at[symbol, "correct_7pct_times"] = (
            df_limit.at[symbol, "up_7pct_times"]
            - df_limit.at[symbol, "down_7pct_times"]
        )
        df_up_3pct = df_stock[df_stock["pct_chg"] > 3]
        df_limit.at[symbol, "up_3pct_times"] = up_3pct_times = len(df_up_3pct)
        df_down_3pct = df_stock[df_stock["pct_chg"] < -3]
        df_limit.at[symbol, "down_3pct_times"] = down_3pct_times = len(df_down_3pct)
        if up_3pct_times == 0 or down_3pct_times == 0:
            df_limit.at[symbol, "correct_3pct_times"] = 0
        else:
            df_limit.at[symbol, "correct_3pct_times"] = round(
                pow(min(up_3pct_times, down_3pct_times), 2)
                / max(up_3pct_times, down_3pct_times),
                2,
            )
        df_limit.at[symbol, "T240_amplitude"] = df_stock["amplitude"].mean().round(2)
        high_240t = df_stock["high"].max()
        low_240t = df_stock["low"].min()
        df_limit.at[symbol, "T240_pct"] = t240_pct = round(
            ((high_240t - low_240t) / ((high_240t + low_240t) / 2) * 100), 2
        )
        df_limit.at[symbol, "T240_turnover"] = df_stock["turnover"].mean().round(2)
        df_stock_5t = df_stock.iloc[-5:]
        if not df_stock_5t.empty:
            df_limit.at[symbol, "T5_amplitude"] = (
                df_stock_5t["amplitude"].mean().round(2)
            )
            high_5t = df_stock_5t["high"].max()
            low_5t = df_stock_5t["low"].min()
            df_limit.at[symbol, "T5_pct"] = t5_pct = round(
                ((high_5t - low_5t) / ((high_5t + low_5t) / 2) * 100), 2
            )
            df_limit.at[symbol, "T5_turnover"] = df_stock_5t["turnover"].mean().round(2)
        else:
            t5_pct = 0
        df_limit.at[symbol, "alpha_amplitude"] = (
            df_limit.at[symbol, "T5_amplitude"] - df_limit.at[symbol, "T240_amplitude"]
        )
        if t240_pct != 0:
            df_limit.at[symbol, "alpha_pct"] = round((pow(t5_pct, 2) / t240_pct), 2)
        df_limit.at[symbol, "alpha_turnover"] = (
            df_limit.at[symbol, "T5_turnover"] - df_limit.at[symbol, "T240_turnover"]
        )
        print(f"\r{str_msg_bar}\033[K", end="")  # for loop end, progress bar
    if i >= count:
        print("\n", end="")  # 格式处理
        logger.trace(f"For loop End")
        df_limit.sort_values(
            by=["correct_3pct_times", "correct_7pct_times", "alpha_amplitude"],
            ascending=False,
            inplace=True,
        )
        analysis.base.write_obj_to_db(
            obj=df_limit, key=name, filename=filename_chip_shelve
        )
        analysis.base.set_version(key=name, dt=dt_limit)
        if os.path.exists(file_name_df_limit_temp):
            os.remove(path=file_name_df_limit_temp)
    end_loop_time = time.perf_counter_ns()
    interval_time = (end_loop_time - start_loop_time) / 1000000000
    str_gm = time.strftime("%H:%M:%S", time.gmtime(interval_time))
    print(f"Limit Count analysis takes [{str_gm}]")
    logger.trace(f"Limit Count End")
    return True
