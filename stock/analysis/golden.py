# modified at 2023/3/25 16：59
from __future__ import annotations
import os
import sys
import time
import datetime
import random
import feather
import pandas as pd
import numpy as np
from scipy.constants import golden
from loguru import logger
import ashare
import analysis.update_data
import analysis.base


def golden_price(list_code: list | str = None, frequency: str = "1m") -> bool:
    """分析挂仓成本
    :param list_code: e.g.sh600519
    :param frequency: choice of {"1m" ,"5m"}
    :return: pd.DataFrame
    """
    logger.trace("Golden Price Analysis Begin")
    kline: str = f"update_kline_{frequency}"
    name: str = f"df_golden"
    start_loop_time = time.perf_counter_ns()
    phi = 1 / golden  # extreme and mean ratio 黄金分割常数
    if list_code is None:
        logger.trace("list_code is None")
        list_code = analysis.base.all_chs_code()
    if isinstance(list_code, str):
        list_code = [list_code]
    dt_date_trading = analysis.base.latest_trading_day()
    time_pm_end = datetime.time(hour=15, minute=0, second=0, microsecond=0)
    dt_pm_end = datetime.datetime.combine(dt_date_trading, time_pm_end)
    str_date_path = dt_date_trading.strftime("%Y_%m_%d")
    path_main = os.getcwd()
    path_kline = os.path.join(path_main, "data", f"kline_{frequency}")
    path_check = os.path.join(path_main, "check")
    path_data = os.path.join(path_main, "data")
    if not os.path.exists(path_kline):
        os.mkdir(path_kline)
    if not os.path.exists(path_check):
        os.mkdir(path_check)
    if not os.path.exists(path_data):
        os.mkdir(path_data)
    file_name_golden_feather_temp = os.path.join(
        path_data, f"golden_price_temp_{str_date_path}.ftr"
    )
    # 判断Kline是不是最新的
    if analysis.base.is_latest_version(key=kline):
        pass
    else:
        logger.trace("Update the Kline")
        analysis.update_data.update_stock_data()
    if analysis.base.is_latest_version(key=name):
        logger.trace("Golden Price Analysis Break End")
        return True  # df_golden is object
    list_golden_exist = list()
    if os.path.exists(file_name_golden_feather_temp):
        logger.trace(f"{file_name_golden_feather_temp} load feather")
        df_golden = feather.read_dataframe(source=file_name_golden_feather_temp)
        if df_golden.empty:
            logger.trace("df_golden cache is empty")
        else:
            logger.trace("df_golden cache is not empty")
            list_golden_exist = df_golden.index.to_list()
    else:
        logger.trace(f"{file_name_golden_feather_temp} not exists")
        list_columns = [
            "dt",
            "total_volume",
            "now_price",
            "now_price_ratio",
            "now_price_volume",
            "G_price",
            "G_price_volume",
        ]
        df_golden = pd.DataFrame(columns=list_columns)
    df_now_price = ashare.stock_zh_a_spot_em()
    i = 0
    all_record = len(list_code)
    logger.trace(f"for loop Begin")
    for symbol in list_code:
        i += 1
        print(f"\rGolden Price Update:[{i:4d}/{all_record:4d}] -- [{symbol}]", end="")
        if symbol in list_golden_exist:
            continue
        file_name_data_feather = os.path.join(path_kline, f"{symbol}.ftr")
        if os.path.exists(file_name_data_feather):
            # 找到kline，读取腌制数据 df_data
            df_data = feather.read_dataframe(source=file_name_data_feather)
        else:
            # 无Kline，跳过本次[symbol]处理
            continue
        df_data = df_data.iloc[-57600:]  # 取得最近1个整年的交易记录，240x240=57600算头不算尾
        dt_max = df_data.index.max()
        df_pivot = pd.pivot_table(
            df_data, index=["close"], aggfunc={"volume": np.sum, "close": len}
        )
        df_pivot.rename(columns={"close": "count"}, inplace=True)
        df_pivot.sort_values(by=["close"], ascending=False, inplace=True)
        df_pivot.reset_index(inplace=True)
        now_price = df_now_price.at[symbol, "close"]
        total_volume = df_pivot["volume"].sum()
        golden_volume = round(total_volume * phi, 2)
        temp_volume = 0
        df_golden.at[symbol, "dt"] = dt_max
        df_golden.at[symbol, "total_volume"] = total_volume
        signal_price = True
        signal_volume = True
        for tup_row in df_pivot.itertuples():
            temp_volume += tup_row.volume
            if tup_row.close <= now_price and signal_price:
                if tup_row.close == now_price:
                    df_golden.at[symbol, "now_price"] = tup_row.close
                else:
                    df_golden.at[symbol, "now_price"] = (now_price + tup_row.close) / 2
                price_ratio = temp_volume / total_volume
                df_golden.at[symbol, "now_price_ratio"] = round(price_ratio, 4) * 100
                df_golden.at[symbol, "now_price_volume"] = temp_volume
                signal_price = False
            if temp_volume >= golden_volume and signal_volume:
                df_golden.at[symbol, "G_price"] = tup_row.close
                df_golden.at[symbol, "G_price_volume"] = temp_volume
                signal_volume = False
            if not signal_price and not signal_volume:
                break
        if random.randint(0, 5) == 3:
            feather.write_dataframe(df=df_golden, dest=file_name_golden_feather_temp)
    if i >= all_record:
        print("\n", end="")  # 格式处理
        df_golden.index.rename(name="symbol", inplace=True)
        df_golden.sort_values(by=["now_price_ratio"], ascending=False, inplace=True)
        df_golden["dt"] = df_golden["dt"].to_string()
        analysis.base.write_df_to_db(obj=df_golden, key="df_golden")
        logger.trace(f"df_golden save at [pydb_chip]")
        analysis.base.add_chip_excel(df=df_golden, key=name)
        analysis.base.set_version(key=name, dt=dt_pm_end)
        if os.path.exists(file_name_golden_feather_temp):  # 删除临时文件
            os.remove(path=file_name_golden_feather_temp)
            logger.trace(f"[{file_name_golden_feather_temp}] remove")
    end_loop_time = time.perf_counter_ns()
    interval_time = (end_loop_time - start_loop_time) / 1000000000
    str_gm = time.strftime("%H:%M:%S", time.gmtime(interval_time))
    print(f"Golden Price Analysis takes [{str_gm}]")
    logger.trace(f"Golden Price Analysis End--[all_record={all_record}]")
    return True


if __name__ == "__main__":
    logger.remove()
    logger.add(
        sink=sys.stderr, level="INFO"
    )  # choice of {"TRACE","DEBUG","INFO"，"ERROR"}
    golden_price(list_code=["sh600519", "sz002621", "sz000422"])
