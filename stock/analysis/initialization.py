# modified at 2023/05/18 22::25
from loguru import logger
import pandas as pd
import analysis.base
from analysis.const import (
    rise,
    fall,
    filename_chip_shelve,
    dt_init,
    dt_trading,
    phi_100,
)


def init_trader(df_trader: pd.DataFrame, sort: bool = False) -> pd.DataFrame:
    df_chip = analysis.base.read_df_from_db(
        key="df_chip", filename=filename_chip_shelve
    )
    i_realtime = 0
    df_realtime = pd.DataFrame()
    while i_realtime <= 2:
        i_realtime += 1
        df_realtime = analysis.realtime_quotations(
            stock_codes=df_trader.index.to_list()
        )  # 调用实时数据接口
        if not df_realtime.empty:
            break
        else:
            logger.trace("df_realtime is empty")
    dict_trader_default = {
        "name": "股票简称",
        "recent_price": 0,
        "position": 0,
        "now_price": 0,
        "pct_chg": 0,
        "position_unit": 0,
        "trx_unit_share": 0,
        "industry_code": "000000.TI",
        "industry_name": "行业",
        "times_exceed_correct_industry": 0,
        "mean_exceed_correct_industry": 0,
        "total_mv_E": 0,
        "ssb_index": "index_none",
        "stock_index": "stock_index_none",
        "grade": "grade_none",
        "recent_trading": dt_init,
        "ST": "ST_none",
        "date_of_inclusion_first": dt_init,
        "date_of_inclusion_latest": dt_init,
        "times_of_inclusion": 0,
        "rate_of_inclusion": 0,
        "price_of_inclusion": 0,
        "pct_of_inclusion": 0,
        "rise": rise,
        "fall": fall,
    }
    for columns in dict_trader_default:
        df_trader[columns].fillna(value=dict_trader_default[columns], inplace=True)
    for code in df_trader.index:
        if code in df_chip.index:
            t5_amplitude = df_chip.at[code, "T5_amplitude"]
            t5_pct = df_chip.at[code, "T5_pct"]
            correct_7pct_times = int(df_chip.at[code, "correct_7pct_times"])
            g_price = df_chip.at[code, "G_price"]
            now_price_ratio = round(df_chip.at[code, "now_price_ratio"], 1)
            if df_realtime.empty:
                df_trader.at[code, "name"] = df_chip.at[code, "name"]
                df_trader.at[code, "now_price"] = now_price = df_chip.at[
                    code, "now_price"
                ]
            else:
                df_trader.at[code, "name"] = df_realtime.at[code, "name"]
                df_trader.at[code, "now_price"] = now_price = df_realtime.at[
                    code, "close"
                ]
            if df_trader.at[code, "price_of_inclusion"] == 0:
                df_trader.at[code, "price_of_inclusion"] = now_price
            df_trader.at[code, "total_mv_E"] = df_chip.at[code, "total_mv_E"]
            df_trader.at[code, "times_exceed_correct_industry"] = df_chip.at[
                code, "times_exceed_correct_industry"
            ]
            df_trader.at[code, "mean_exceed_correct_industry"] = df_chip.at[
                code, "mean_exceed_correct_industry"
            ]
            df_trader.at[code, "ssb_index"] = df_chip.at[code, "ssb_index"]
            df_trader.at[code, "ST"] = df_chip.at[code, "ST"]
            df_trader.at[code, "industry_code"] = df_chip.at[code, "industry_code"]
            df_trader.at[code, "industry_name"] = df_chip.at[code, "industry_name"]
            df_trader.at[code, "trx_unit_share"] = analysis.transaction_unit(
                price=g_price
            )
            df_trader.at[code, "position_unit"] = (
                df_trader.at[code, "position"] / df_trader.at[code, "trx_unit_share"]
            ).round(2)
            if (
                df_trader.at[code, "position"] == 0
                and df_trader.at[code, "position"] > phi_100
            ):
                df_trader.at[code, "recent_price"] = g_price
            pct_chg = (
                df_trader.at[code, "now_price"] / df_trader.at[code, "recent_price"] - 1
            ) * 100
            pct_chg = round(pct_chg, 2)
            df_trader.at[code, "pct_chg"] = pct_chg
            days_of_inclusion = (
                dt_trading - df_trader.at[code, "date_of_inclusion_first"]
            ).days + 1
            days_of_inclusion = (
                days_of_inclusion // 7 * 5 + days_of_inclusion % 7
            )  # 修正除数，尽可能趋近交易日
            if days_of_inclusion > 0:
                df_trader.at[code, "rate_of_inclusion"] = round(
                    df_trader.at[code, "times_of_inclusion"] / days_of_inclusion * 100,
                    2,
                )
            else:
                df_trader.at[code, "rate_of_inclusion"] = 0
            pct_of_inclusion = (
                df_trader.at[code, "now_price"]
                / df_trader.at[code, "price_of_inclusion"]
                - 1
            ) * 100
            pct_of_inclusion = round(pct_of_inclusion, 2)
            df_trader.at[code, "pct_of_inclusion"] = pct_of_inclusion
            df_trader.at[code, "stock_index"] = (
                f"([{correct_7pct_times:2.0f}]SPT /"
                f"{now_price_ratio:6.2f}% -"
                f"{g_price:6.2f}GP)--"
                f"[T5_amp:{t5_amplitude:5.2f}]-"
                f"[T5_pct:{t5_pct:5.2f}]"
            )
            if correct_7pct_times >= 12:
                grade_ud_limit = "A"
            elif 8 <= correct_7pct_times < 12:
                grade_ud_limit = "B"
            else:
                grade_ud_limit = "Z"
            if 51.80 <= now_price_ratio <= 71.8:  # 61.8 上下10%
                grade_pr = "A"
            elif 71.8 < now_price_ratio <= 81.8 or 41.8 <= now_price_ratio < 51.8:
                grade_pr = "B"
            else:
                grade_pr = "Z"
            if 0 < now_price < g_price:
                grade_g = "Under"
            elif g_price <= now_price:
                grade_g = "Over"
            else:
                grade_g = "#"
            grade = grade_ud_limit + grade_pr + "-" + grade_g
            df_trader.at[code, "grade"] = grade
    if sort:
        df_trader = df_trader.reindex(
            columns=[
                "name",
                "recent_price",
                "position",
                "now_price",
                "pct_chg",
                "position_unit",
                "trx_unit_share",
                "industry_code",
                "industry_name",
                "times_exceed_correct_industry",
                "mean_exceed_correct_industry",
                "total_mv_E",
                "ssb_index",
                "stock_index",
                "grade",
                "recent_trading",
                "ST",
                "date_of_inclusion_first",
                "date_of_inclusion_latest",
                "times_of_inclusion",
                "rate_of_inclusion",
                "price_of_inclusion",
                "pct_of_inclusion",
                "rise",
                "fall",
                "remark",
            ]
        )
    return df_trader
