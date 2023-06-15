# modified at 2023/05/18 22::25
from __future__ import annotations
import datetime
import time
import pandas as pd
from pandas import DataFrame
from loguru import logger
import analysis.base
import analysis.g_price
import analysis.limit
import analysis.update_data
import analysis.capital
import analysis.st
import analysis.industry
import analysis.index
import analysis.concentration
from analysis.const import filename_chip_shelve, filename_chip_excel


def chip() -> object | DataFrame:
    name: str = "df_chip"
    logger.trace(f"{name} Begin")
    start_loop_time = time.perf_counter_ns()
    if analysis.base.is_latest_version(key=name, filename=filename_chip_shelve):
        df_chip = analysis.base.read_df_from_db(key=name, filename=filename_chip_shelve)
        logger.trace(f"{name} Break End")
        return df_chip
    logger.trace(f"Update {name}")
    analysis.update_data.update_index_data(symbol="sh000001")
    analysis.update_data.update_index_data(symbol="sh000852")
    if analysis.g_price.golden_price():
        df_golden = analysis.base.read_df_from_db(
            key="df_golden", filename=filename_chip_shelve
        )
        logger.trace("load df_golden success")
    else:
        df_golden = pd.DataFrame()
        logger.trace("load df_golden fail")
    if analysis.limit.limit_count():
        df_limit = analysis.base.read_df_from_db(
            key="df_limit", filename=filename_chip_shelve
        )
        logger.trace("load df_limit success")
    else:
        df_limit = pd.DataFrame()
        logger.trace("load df_limit fail")
    if analysis.capital.capital():
        df_cap = analysis.base.read_df_from_db(
            key="df_cap", filename=filename_chip_shelve
        )
        logger.trace("load df_cap success")
    else:
        df_cap = pd.DataFrame()
        logger.error("load df_cap fail")
    if analysis.st.st_income():
        df_st = analysis.base.read_df_from_db(
            key="df_st", filename=filename_chip_shelve
        )
        logger.trace("load df_st success")
    else:
        df_st = pd.DataFrame()
        logger.trace("load df_st fail")
    while True:
        if analysis.industry.industry_rank():
            df_industry_rank = analysis.base.read_df_from_db(
                key="df_industry_rank", filename=filename_chip_shelve
            )
            df_industry_rank_deviation = df_industry_rank[
                df_industry_rank["max_min"] >= 60
            ]
            list_industry_code_deviation = df_industry_rank_deviation.index.tolist()
            break
        else:
            print("Sleep 1 hour")
            dt_now_delta = datetime.datetime.now() + datetime.timedelta(seconds=3600)
            analysis.base.sleep_to_time(dt_time=dt_now_delta, seconds=10)
    if analysis.industry.reset_industry_member():
        pass
    if analysis.base.is_latest_version(
        key="df_stocks_in_ssb", filename=filename_chip_shelve
    ):
        index_ssb = analysis.index.IndexSSB(update=False)
    else:
        index_ssb = analysis.index.IndexSSB(update=True)
        dt_stocks_in_ssb = index_ssb.version()
        analysis.base.set_version(key="df_stocks_in_ssb", dt=dt_stocks_in_ssb)
    df_stocks_in_ssb = index_ssb.stocks_in_ssb()
    if analysis.concentration():
        df_concentration = analysis.base.read_df_from_db(
            key="df_concentration", filename=filename_chip_shelve
        )
    else:
        df_concentration = pd.DataFrame()
        logger.error("load df_concentration fail")
    if analysis.industry.ths_industry():
        df_industry = analysis.base.read_df_from_db(
            key="df_industry", filename=filename_chip_shelve
        )
        logger.trace("load df_industry success")
    else:
        df_industry = pd.DataFrame()
        logger.trace("load df_industry fail")
    df_chip = pd.concat(
        objs=[
            df_cap,
            df_stocks_in_ssb,
            df_st,
            df_concentration,
            df_industry,
            df_golden,
            df_limit,
        ],
        axis=1,
        join="outer",
    )
    analysis.base.write_obj_to_db(obj=df_chip, key=name, filename=filename_chip_shelve)
    df_g_price_1 = df_chip[
        (df_chip["now_price_ratio"] <= 71.8) & (df_chip["now_price_ratio"] >= 51.8)
    ]
    df_limit_2 = df_chip[
        (df_chip["correct_3pct_times"] >= 40)
        & (df_chip["alpha_pct"] >= 10)
        & (df_chip["alpha_amplitude"] >= 0)
        & (df_chip["alpha_turnover"] >= 0)
    ]
    df_exceed_industry_3 = df_chip[
        (df_chip["times_exceed_correct_industry"] >= 70)
        & (df_chip["mean_exceed_correct_industry"] >= 1.5)
    ]
    df_concentration_4 = df_chip[
        (df_chip["rate_concentration"] >= 50)
        & (df_chip["days_latest_concentration"] <= 30)
    ]
    df_stocks_pool = pd.concat(
        objs=[
            df_g_price_1,
            df_limit_2,
            df_exceed_industry_3,
            df_concentration_4,
        ],
        axis=0,
        join="outer",
    )
    df_stocks_pool = df_stocks_pool[~df_stocks_pool.index.duplicated(keep="first")]
    list_ssb_index = ["ssb_tail", "ssb_2000"]
    df_stocks_pool = df_stocks_pool[
        (df_stocks_pool["list_days"] > 365)
        & (df_stocks_pool["up_10pct_times"] >= 4)
        & (df_stocks_pool["industry_code"].isin(values=list_industry_code_deviation))
        & (~df_stocks_pool["name"].str.contains("ST").fillna(False))
        & (~df_stocks_pool["ST"].str.contains("ST").fillna(False))
        & (~df_stocks_pool.index.str.contains("sh68"))
        & (~df_stocks_pool.index.str.contains("bj"))
        & (df_stocks_pool["ssb_index"].isin(values=list_ssb_index))
    ]
    df_stocks_pool["factor_count"] = 1
    df_stocks_pool["factor"] = None
    for symbol in df_stocks_pool.index:
        if symbol in df_g_price_1.index:
            if pd.notnull(df_stocks_pool.at[symbol, "factor"]):
                df_stocks_pool.at[symbol, "factor"] += ",[G_price]"
                df_stocks_pool.at[symbol, "factor_count"] += 1
            else:
                df_stocks_pool.at[symbol, "factor"] = "[G_price]"
        if symbol in df_limit_2.index:
            if pd.notnull(df_stocks_pool.at[symbol, "factor"]):
                df_stocks_pool.at[symbol, "factor"] += ",[limit]"
                df_stocks_pool.at[symbol, "factor_count"] += 1
            else:
                df_stocks_pool.at[symbol, "factor"] = "[limit]"
        if symbol in df_exceed_industry_3.index:
            if pd.notnull(df_stocks_pool.at[symbol, "factor"]):
                df_stocks_pool.at[symbol, "factor"] += ",[exceed_industry]"
                df_stocks_pool.at[symbol, "factor_count"] += 1
            else:
                df_stocks_pool.at[symbol, "factor"] = "[exceed_industry]"
        if symbol in df_concentration_4.index:
            if pd.notnull(df_stocks_pool.at[symbol, "factor"]):
                df_stocks_pool.at[symbol, "factor"] += ",[concentration]"
                df_stocks_pool.at[symbol, "factor_count"] += 1
            else:
                df_stocks_pool.at[symbol, "factor"] = "[concentration]"
    # df_stocks_pool = df_stocks_pool[df_stocks_pool["factor_count"] > 2]
    df_stocks_pool.sort_values(
        by=["factor_count", "factor"], ascending=False, inplace=True
    )
    df_stocks_pool = df_stocks_pool[
        [
            "name",
            "list_days",
            "ssb_index",
            "total_mv_E",
            "factor_count",
            "ST",
            "industry_code",
            "industry_name",
            "times_exceed_correct_industry",
            "mean_exceed_correct_industry",
            "times_concentration",
            "rate_concentration",
            "correct_3pct_times",
            "now_price",
            "now_price_ratio",
            "G_price",
            "dt",
            "factor",
        ]
    ]
    analysis.base.write_obj_to_db(
        obj=df_stocks_pool, key="df_stocks_pool", filename=filename_chip_shelve
    )
    df_config = analysis.base.read_df_from_db(
        key="df_config", filename=filename_chip_shelve
    )
    try:
        df_config_temp = df_config.drop(index=[name])
    except KeyError as e:
        print(f"[{name}] is not found in df_config -Error[{repr(e)}]")
        logger.trace(f"[{name}] is not found in df_config -Error[{repr(e)}]")
        df_config_temp = df_config.copy()
    analysis.base.shelve_to_excel(
        filename_shelve=filename_chip_shelve, filename_excel=filename_chip_excel
    )
    dt_chip = df_config_temp["date"].min()
    analysis.base.set_version(key=name, dt=dt_chip)
    end_loop_time = time.perf_counter_ns()
    interval_time = (end_loop_time - start_loop_time) / 1000000000
    str_gm = time.strftime("%H:%M:%S", time.gmtime(interval_time))
    print(f"Chip analysis takes [{str_gm}]")
    logger.trace("Chip End")
    return df_chip
