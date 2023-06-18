# modified at 2023/05/18 22::25
from __future__ import annotations
import os
import random
import time
import datetime
import requests
import math
import pandas as pd
from loguru import logger
import feather
import tushare as ts
import analysis.base
from analysis.const import (
    dt_init,
    path_data,
    dt_date_trading,
    dt_pm_end,
    str_date_path,
    time_pm_end,
    str_date_trading,
    filename_chip_shelve,
    list_all_stocks,
)


dt_delta = dt_date_trading - datetime.timedelta(days=366)
str_delta = dt_delta.strftime("%Y%m%d")
path_industry = os.path.join(path_data, f"industry_ths")
if not os.path.exists(path_industry):
    os.mkdir(path_industry)


def get_industry_index() -> pd.DataFrame:
    name: str = "df_industry_index"
    logger.trace(f"{name} Begin！")
    pro = ts.pro_api()
    df_ths_index = pro.ths_index()
    df_industry_index = df_ths_index[
        df_ths_index["ts_code"].str.contains("8811").fillna(False)
    ]
    df_industry_index = df_industry_index[["ts_code", "name"]]
    df_industry_index.rename(
        columns={
            "ts_code": "industry_code",
            "name": "industry_name",
        },
        inplace=True,
    )
    df_industry_index.set_index(keys=["industry_code"], inplace=True)
    analysis.base.write_obj_to_db(
        obj=df_industry_index,
        key=name,
        filename=filename_chip_shelve,
    )
    logger.trace(f"{name} End！")
    return df_industry_index


def reset_industry_member() -> bool:
    name = "df_industry_member"
    if analysis.base.is_latest_version(key=name, filename=filename_chip_shelve):
        logger.trace(f"df_industry_member,Break and End")
        return True
    df_industry_member = analysis.base.read_df_from_db(
        key="df_industry_member", filename=filename_chip_shelve
    )
    df_industry_index = analysis.base.read_df_from_db(
        key="df_industry_index", filename=filename_chip_shelve
    )
    if df_industry_index.empty:
        df_industry_index = get_industry_index()
    df_industry_member_all = pd.DataFrame(index=list_all_stocks)
    df_industry_member = pd.concat(
        objs=[
            df_industry_member,
            df_industry_member_all,
        ],
        axis=1,
        join="outer",
    )
    df_industry_member = df_industry_member[
        df_industry_member.index.isin(list_all_stocks)
    ]
    pro = ts.pro_api()
    i = 0
    count_industry_code = len(df_industry_index)
    for industry_code in df_industry_index.index:
        i += 1
        srt_msg = f"[{i:02d}/{count_industry_code}] - [{industry_code}]"
        df_ths_member = pro.ths_member(ts_code=industry_code)
        df_ths_member["symbol"] = df_ths_member["code"].apply(
            func=analysis.base.code_ts_to_ths
        )
        df_ths_member.set_index(keys=["symbol"], inplace=True)
        for symbol in df_ths_member.index:
            if symbol in df_industry_member.index:
                if pd.isnull(df_industry_member.at[symbol, "industry_code"]):
                    print(f"\r{srt_msg} - [{symbol}]\033[K")
                    df_industry_member.at[symbol, "industry_code"] = industry_code
                    df_industry_member.at[
                        symbol, "industry_name"
                    ] = df_industry_index.at[industry_code, "industry_name"]
        print(f"\r{srt_msg}\033[K", end="")
    print("\n", end="")  # 格式处理
    df_industry_member.sort_values(by=["industry_code"], inplace=True)
    if i >= count_industry_code:
        analysis.base.write_obj_to_db(
            obj=df_industry_member,
            key="df_industry_member",
            filename=filename_chip_shelve,
        )
        with pd.ExcelWriter(path="df_industry_member.xlsx", mode="w") as writer_e:
            df_industry_member.to_excel(
                excel_writer=writer_e, sheet_name="df_industry_member"
            )
        analysis.base.set_version(key=name, dt=dt_pm_end)
    logger.trace(f"{name} End")
    return True


def update_industry_index_ths() -> bool:
    name: str = f"index_kline_industry"
    logger.trace(f"{name} Begin！")
    start_loop_time = time.perf_counter_ns()
    if analysis.base.is_latest_version(key=name, filename=filename_chip_shelve):
        logger.trace(f"{name},Break and End")
        return True
    df_industry_index = analysis.base.read_df_from_db(
        key="df_industry_index", filename=filename_chip_shelve
    )
    if df_industry_index.empty:
        df_industry_index = get_industry_index()
    dt_index_kline_industry = dt_init
    pro = ts.pro_api()
    count_industry_index = len(df_industry_index)
    i = 0
    for industry_code in df_industry_index.index:
        i += 1
        symbol_index = analysis.base.code_ts_to_ths(industry_code)
        str_msg_bar = f"{name}:[{i:3d}/{count_industry_index:3d}] - [{symbol_index}]"
        df_ths_daily = pd.DataFrame()
        i_times_ths_daily = 0
        while True:
            i_times_ths_daily += 1
            try:
                df_ths_daily = pro.ths_daily(
                    ts_code=industry_code,
                    start_date=str_delta,
                    end_date=str_date_trading,
                )
            except requests.exceptions.ConnectionError as e:
                str_msg_bar += f" - {repr(e)}"
                logger.trace(repr(e))
                time.sleep(2)
            else:
                if df_ths_daily.empty:
                    time.sleep(2)
                else:
                    break
            if i_times_ths_daily >= 2:
                break
        if df_ths_daily.empty:
            return False
        df_ths_daily["trade_date"] = df_ths_daily["trade_date"].apply(
            func=lambda x: datetime.datetime.combine(
                pd.to_datetime(x).date(), time_pm_end
            )
        )
        df_ths_daily.set_index(keys=["trade_date"], inplace=True)
        df_ths_daily.sort_index(ascending=True, inplace=True)
        filename_ths_daily = os.path.join(path_industry, f"{symbol_index}.ftr")
        feather.write_dataframe(df=df_ths_daily, dest=filename_ths_daily)
        dt_industry_index_temp = df_ths_daily.index.max()
        str_msg_bar += f" - [{dt_industry_index_temp}]"
        dt_now = datetime.datetime.now()
        if dt_now > dt_pm_end and dt_industry_index_temp != dt_pm_end:
            str_msg_bar += f" - Not the latest"
            print(f"\r{str_msg_bar}\033[K")  # Program End
            return False
        if dt_index_kline_industry < dt_industry_index_temp:
            dt_index_kline_industry = dt_industry_index_temp
        print(f"\r{str_msg_bar}\033[K", end="")  # End of this cycle, print progress bar
    if i >= count_industry_index:
        print("\n", end="")  # 格式处理
        analysis.base.set_version(key=name, dt=dt_index_kline_industry)
    end_loop_time = time.perf_counter_ns()
    interval_time = (end_loop_time - start_loop_time) / 1000000000
    str_gm = time.strftime("%H:%M:%S", time.gmtime(interval_time))
    print(f"Industry analysis [{name}] takes {str_gm}")
    logger.trace(f"{name} End")
    return True


def industry_pct() -> bool:
    name: str = f"df_industry_pct"
    kdata: str = f"index_kline_industry"
    logger.trace(f"{name} Begin！")
    start_loop_time = time.perf_counter_ns()
    if analysis.base.is_latest_version(key=name, filename=filename_chip_shelve):
        logger.trace(f"{name},Break and End")
        return True
    if not analysis.base.is_latest_version(key=kdata, filename=filename_chip_shelve):
        if update_industry_index_ths():
            pass
        else:
            return False
    df_industry_index = analysis.base.read_df_from_db(
        key="df_industry_index", filename=filename_chip_shelve
    )
    if df_industry_index.empty:
        df_industry_index = get_industry_index()
    filename_industry_pct = os.path.join(
        path_data, f"industry_pct_temp_{str_date_path()}.ftr"
    )
    if os.path.exists(filename_industry_pct):
        df_industry_pct = feather.read_dataframe(source=filename_industry_pct)
    else:
        df_industry_pct = pd.DataFrame()
    list_industry_pct_exist = set(df_industry_pct.columns.tolist())
    i = 0
    count_industry_index = len(df_industry_index)
    for ts_code_index in df_industry_index.index:
        i += 1
        symbol_index = analysis.base.code_ts_to_ths(ts_code_index)
        str_msg_bar = f"\r{name}:[{i:3d}/{count_industry_index:3d}] - [{symbol_index}]"
        if ts_code_index in list_industry_pct_exist:
            print(f"{str_msg_bar} - exist", end="")
            continue
        if random.randint(0, 5) == 3:
            feather.write_dataframe(df=df_industry_pct, dest=filename_industry_pct)
        filename_ths_daily = os.path.join(path_industry, f"{symbol_index}.ftr")
        df_ths_daily = feather.read_dataframe(source=filename_ths_daily)
        df_ths_daily_pct = df_ths_daily[["pct_change"]].copy()
        df_ths_daily_pct.rename(columns={"pct_change": ts_code_index}, inplace=True)
        df_industry_pct = pd.concat(
            objs=[df_industry_pct, df_ths_daily_pct],
            axis=1,
            join="outer",
        )
        dt_ths_daily = datetime.datetime.combine(df_ths_daily.index.max(), time_pm_end)
        print(f"{str_msg_bar} - {dt_ths_daily}", end="")
    df_industry_pct = df_industry_pct.applymap(func=lambda x: x + 100)
    len_df_industry_pct = len(df_industry_pct)
    i = 0
    while i < len_df_industry_pct:
        mim_pct = df_industry_pct.iloc[i].min()
        df_industry_pct.iloc[i] = df_industry_pct.iloc[i].apply(
            func=lambda x: (x / mim_pct - 1) * 100
        )
        i += 1
    if i >= len_df_industry_pct:
        print("\n", end="")  # 格式处理
        analysis.base.write_obj_to_db(
            obj=df_industry_pct,
            key=name,
            filename=filename_chip_shelve,
        )
        dt_industry_pct = df_industry_pct.index.max()
        analysis.base.set_version(key=name, dt=dt_industry_pct)
    if os.path.exists(filename_industry_pct):  # 删除临时文件
        os.remove(path=filename_industry_pct)
    end_loop_time = time.perf_counter_ns()
    interval_time = (end_loop_time - start_loop_time) / 1000000000
    str_gm = time.strftime("%H:%M:%S", time.gmtime(interval_time))
    print(f"Industry analysis [{name}] takes {str_gm}")
    logger.trace(f"{name} End")
    return True


def industry_rank():
    name: str = f"df_industry_rank"
    kdata: str = "df_industry_pct"
    logger.trace(f"{name} Begin！")
    start_loop_time = time.perf_counter_ns()
    if analysis.base.is_latest_version(key=name, filename=filename_chip_shelve):
        return True
    if analysis.base.is_latest_version(key=kdata, filename=filename_chip_shelve):
        pass
    else:
        if industry_pct():
            pass
        else:
            return False
    df_industry_rank = pd.DataFrame(
        columns=[
            "name",
            "T5",
            "T5_Zeroing_sort",
            "T5_rank",
            "T20",
            "T20_Zeroing_sort",
            "T20_rank",
            "T40",
            "T40_Zeroing_sort",
            "T40_rank",
            "T60",
            "T60_Zeroing_sort",
            "T60_rank",
            "T80",
            "T80_Zeroing_sort",
            "T80_rank",
            "max_min_plus",
            "max_min_minus",
            "max_min",
        ]
    )
    df_industry_pct = analysis.base.read_df_from_db(
        key="df_industry_pct", filename=filename_chip_shelve
    )
    dt_industry_rank = df_industry_pct.index.max()
    df_5_industry_pct = df_industry_pct.iloc[-5:]
    df_20_industry_pct = df_industry_pct.iloc[-20:-5]
    df_40_industry_pct = df_industry_pct.iloc[-40:-20]
    df_60_industry_pct = df_industry_pct.iloc[-60:-40]
    df_80_industry_pct = df_industry_pct.iloc[-80:-60]
    df_industry_rank["T5"] = (df_5_industry_pct.sum(axis=0) / 5 * 20).round(2)
    df_industry_rank["T20"] = (df_20_industry_pct.sum(axis=0) / 15 * 20).round(2)
    df_industry_rank["T40"] = df_40_industry_pct.sum(axis=0).round(2)
    df_industry_rank["T60"] = df_60_industry_pct.sum(axis=0).round(2)
    df_industry_rank["T80"] = df_80_industry_pct.sum(axis=0).round(2)
    df_industry_rank["T5_Zeroing_sort"] = analysis.base.zeroing_sort(
        pd_series=df_industry_rank["T5"]
    )
    df_industry_rank["T5_rank"] = df_industry_rank["T5"].rank(
        axis=0, method="min", ascending=False
    )
    df_industry_rank["T20_Zeroing_sort"] = analysis.base.zeroing_sort(
        pd_series=df_industry_rank["T20"]
    )
    df_industry_rank["T20_rank"] = df_industry_rank["T20"].rank(
        axis=0, method="min", ascending=False
    )
    df_industry_rank["T40_Zeroing_sort"] = analysis.base.zeroing_sort(
        pd_series=df_industry_rank["T40"]
    )
    df_industry_rank["T40_rank"] = df_industry_rank["T40"].rank(
        axis=0, method="min", ascending=False
    )
    df_industry_rank["T60_Zeroing_sort"] = analysis.base.zeroing_sort(
        pd_series=df_industry_rank["T60"]
    )
    df_industry_rank["T60_rank"] = df_industry_rank["T60"].rank(
        axis=0, method="min", ascending=False
    )
    df_industry_rank["T80_Zeroing_sort"] = analysis.base.zeroing_sort(
        pd_series=df_industry_rank["T80"]
    )
    df_industry_rank["T80_rank"] = df_industry_rank["T80"].rank(
        axis=0, method="min", ascending=False
    )
    pro = ts.pro_api()
    df_ths_index = pro.ths_index()
    df_ths_index.set_index(keys="ts_code", inplace=True)
    for ths_index_code in df_industry_rank.index:
        if ths_index_code in df_ths_index.index:
            df_industry_rank.at[ths_index_code, "name"] = df_ths_index.at[
                ths_index_code, "name"
            ]
            rank_max = max(
                df_industry_rank.at[ths_index_code, "T5_rank"],
                df_industry_rank.at[ths_index_code, "T20_rank"],
                df_industry_rank.at[ths_index_code, "T40_rank"],
                df_industry_rank.at[ths_index_code, "T60_rank"],
                df_industry_rank.at[ths_index_code, "T80_rank"],
            )
            rank_min = min(
                df_industry_rank.at[ths_index_code, "T5_rank"],
                df_industry_rank.at[ths_index_code, "T20_rank"],
                df_industry_rank.at[ths_index_code, "T40_rank"],
                df_industry_rank.at[ths_index_code, "T60_rank"],
                df_industry_rank.at[ths_index_code, "T80_rank"],
            )
            df_industry_rank.at[ths_index_code, "max_min_minus"] = max_min_minus = (
                rank_max - rank_min
            )
            df_industry_rank.at[ths_index_code, "max_min_plus"] = max_min_plus = (
                rank_max + rank_min
            )
            df_industry_rank.at[ths_index_code, "max_min"] = math.floor(
                pow(max_min_minus, 2) / max_min_plus
            )
    df_industry_rank.sort_values(
        by=["max_min"], axis=0, ascending=False, inplace=True
    )
    analysis.base.write_obj_to_db(
        obj=df_industry_rank, key="df_industry_rank", filename=filename_chip_shelve
    )
    analysis.base.set_version(key=name, dt=dt_industry_rank)
    df_industry_rank_pool = df_industry_rank[df_industry_rank["max_min"] >= 56]
    df_industry_rank_pool = df_industry_rank_pool[
        (df_industry_rank_pool["T5_rank"] >= 66)
        | (df_industry_rank_pool["T5_rank"] <= 10)
    ]
    df_industry_rank_pool = df_industry_rank_pool[
        (df_industry_rank_pool["T20_rank"] >= 66)
        | (df_industry_rank_pool["T20_rank"] <= 10)
    ]
    df_industry_rank_pool = df_industry_rank_pool[
        (df_industry_rank_pool["T40_rank"] >= 56)
        | (df_industry_rank_pool["T40_rank"] <= 20)
    ]
    if not df_industry_rank_pool.empty:
        df_industry_rank_pool.sort_values(
            by=["T5_rank"], axis=0, ascending=False, inplace=True
        )
        analysis.base.write_obj_to_db(
            obj=df_industry_rank_pool,
            key="df_industry_rank_pool",
            filename=filename_chip_shelve,
        )
        analysis.base.set_version(key="df_industry_rank_pool", dt=dt_industry_rank)
    end_loop_time = time.perf_counter_ns()
    interval_time = (end_loop_time - start_loop_time) / 1000000000
    str_gm = time.strftime("%H:%M:%S", time.gmtime(interval_time))
    print(f"Industry analysis [{name}] takes {str_gm}")
    logger.trace(f"{name} End")
    return True


def ths_industry() -> bool:
    name: str = f"df_industry"
    kdata: str = f"update_industry_index"
    logger.trace(f"{name} Begin！")
    start_loop_time = time.perf_counter_ns()
    if analysis.base.is_latest_version(key=name, filename=filename_chip_shelve):
        logger.trace(f"ths_industry,Break and End")
        return True
    filename_industry_temp = os.path.join(
        path_data, f"industry_temp_{str_date_path()}.ftr"
    )
    if not analysis.base.is_latest_version(key=kdata, filename=filename_chip_shelve):
        if not update_industry_index_ths():
            return False
    if os.path.exists(filename_industry_temp):
        df_industry = feather.read_dataframe(source=filename_industry_temp)
    else:
        df_industry = analysis.base.read_df_from_db(
            key="df_industry_member", filename=filename_chip_shelve
        )
        list_industry_columns = df_industry.columns.tolist() + [
            "times_industry",
            "up_exceed_industry",
            "down_exceed_industry",
            "non_exceed_industry",
            "up_mean_industry",
            "down_mean_industry",
            "times_exceed_correct_industry",
            "mean_exceed_correct_industry",
        ]
        df_industry = df_industry.reindex(columns=list_industry_columns, fill_value=0)
        feather.write_dataframe(df=df_industry, dest=filename_industry_temp)
    pro = ts.pro_api()
    dt_date_daily_max = dt_init
    i = 0
    df_industry = df_industry.sample(frac=1)
    count = len(df_industry)
    for symbol in df_industry.index:
        i += 1
        str_msg_bar = f"{name}:[{i:04d}/{count:4d}] -- [{symbol}]"
        if df_industry.at[symbol, "times_industry"] != 0:  # 己存在，断点继续
            print(f"\r{str_msg_bar} - Exist\033[K", end="")
            continue
        if random.randint(0, 5) == 3:
            feather.write_dataframe(df=df_industry, dest=filename_industry_temp)
        ts_code_index = df_industry.at[symbol, "industry_code"]
        ts_code = analysis.base.code_ths_to_ts(symbol)
        symbol_class = analysis.base.code_ts_to_ths(ts_code_index)
        i_times_daily = 0
        df_daily = pd.DataFrame()
        while i_times_daily < 3:
            i_times_daily += 1
            try:
                df_daily = pro.daily(
                    ts_code=ts_code,
                    start_date=str_delta,
                    end_date=str_date_trading,
                )
            except requests.exceptions.ConnectionError as e:
                print(f"\r{str_msg_bar} - {repr(e)}\033[K")
                time.sleep(1)
            else:
                if df_daily.empty:
                    print(f"\r{str_msg_bar} - df_daily empty({i_times_daily})\033[K")
                    time.sleep(1)
                else:
                    break
        if df_daily.empty:
            print(f"\r{str_msg_bar} - No Data\033[K")
            continue
        else:
            df_daily = df_daily[["ts_code", "trade_date", "pct_chg"]]
            df_daily.rename(
                columns={
                    "pct_chg": "pct_stock",
                },
                inplace=True,
            )
            df_daily["trade_date"] = df_daily["trade_date"].apply(
                func=lambda x: datetime.datetime.combine(
                    pd.to_datetime(x).date(), time_pm_end
                )
            )
            df_daily.set_index(keys=["trade_date"], inplace=True)
            df_daily.sort_index(ascending=True, inplace=True)
        filename_ths_daily = os.path.join(path_industry, f"{symbol_class}.ftr")
        if os.path.exists(filename_ths_daily):
            df_ths_daily = feather.read_dataframe(source=filename_ths_daily)
        else:
            continue
        df_ths_daily = df_ths_daily[["ts_code", "pct_change"]]
        df_ths_daily.rename(
            columns={
                "ts_code": "industry_code",
                "pct_change": "pct_industry",
            },
            inplace=True,
        )
        df_daily = pd.concat(
            objs=[
                df_daily,
                df_ths_daily,
            ],
            axis=1,
            join="outer",
        )
        df_daily.dropna(inplace=True)
        times_industry = len(df_daily)
        str_msg_bar += f" - [{times_industry:3d}]"
        dt_daily = df_daily.index.max()
        dt_ths_daily = df_ths_daily.index.max()
        if dt_date_daily_max < dt_daily:
            dt_date_daily_max = dt_daily
        up_exceed_industry = 0
        down_exceed_industry = 0
        non_exceed_industry = 0
        for index in df_daily.index:
            pct_industry = df_daily.at[index, "pct_industry"]
            pct_stock = df_daily.at[index, "pct_stock"]
            if 0 < pct_industry < pct_stock:
                df_daily.at[index, "exceed"] = pct_stock - pct_industry
                up_exceed_industry += 1
            elif pct_stock < pct_industry < 0:
                df_daily.at[index, "exceed"] = pct_stock - pct_industry
                down_exceed_industry += 1
            else:
                df_daily.at[index, "exceed"] = 0
                non_exceed_industry += 1
        if dt_daily == dt_ths_daily:
            print(f"\r{str_msg_bar}\033[K", end="")
        else:
            print(
                f"\r{str_msg_bar} - [{dt_daily.date()}] - [{dt_ths_daily.date()}]\033[K",
            )
        df_industry.at[symbol, "times_industry"] = times_industry
        df_industry.at[symbol, "up_exceed_industry"] = up_exceed_industry
        df_industry.at[symbol, "down_exceed_industry"] = down_exceed_industry
        df_industry.at[symbol, "non_exceed_industry"] = non_exceed_industry
        df_daily_alpha_up = df_daily[df_daily["exceed"] > 0]
        df_daily_alpha_down = df_daily[df_daily["exceed"] < 0]
        df_industry.at[symbol, "up_mean_industry"] = alpha_up_mean = df_daily_alpha_up[
            "exceed"
        ].mean()
        df_industry.at[
            symbol, "down_mean_industry"
        ] = alpha_down_mean = df_daily_alpha_down["exceed"].mean()
        alpha_times_min = min(abs(up_exceed_industry), abs(down_exceed_industry))
        alpha_times_max = max(abs(up_exceed_industry), abs(down_exceed_industry))
        if alpha_times_max > 0:
            df_industry.at[symbol, "times_exceed_correct_industry"] = math.floor(
                pow(alpha_times_min, 2) / alpha_times_max
            )
        alpha_mean_min = min(abs(alpha_up_mean), abs(alpha_down_mean))
        alpha_mean_max = max(abs(alpha_up_mean), abs(alpha_down_mean))
        if alpha_mean_max > 0:
            df_industry.at[symbol, "mean_exceed_correct_industry"] = round(
                pow(alpha_mean_min, 2) / alpha_mean_max,
                2,
            )
    print("\n", end="")  # 格式处理
    if i >= count:
        df_industry["up_mean_industry"].fillna(value=0, inplace=True)
        df_industry["down_mean_industry"].fillna(value=0, inplace=True)
        df_industry.sort_values(
            by=["times_exceed_correct_industry", "mean_exceed_correct_industry"],
            ascending=False,
            inplace=True,
        )
        analysis.base.write_obj_to_db(
            obj=df_industry, key=name, filename=filename_chip_shelve
        )
        dt_mow = datetime.datetime.now()
        dt_daily_max = dt_date_daily_max
        if (
            dt_mow > dt_pm_end
            and dt_daily_max != dt_pm_end
            and dt_date_daily_max != dt_init
        ):
            print(f"\n{name} is not latest")
        analysis.base.set_version(key=name, dt=dt_daily_max)
        if os.path.exists(filename_industry_temp):
            os.remove(path=filename_industry_temp)
    end_loop_time = time.perf_counter_ns()
    interval_time = (end_loop_time - start_loop_time) / 1000000000
    str_gm = time.strftime("%H:%M:%S", time.gmtime(interval_time))
    print(f"Industry analysis [{name}] takes {str_gm}")
    logger.trace(f"{name} End")
    return True
