# modified at 2023/05/18 22::25
from __future__ import annotations
import os
import sys
import datetime
import time
import pandas as pd
import akshare as ak
from loguru import logger
from console import fg
import analysis
from analysis import (
    dt_trading,
    dt_init,
    dt_am_0910,
    dt_am_start,
    dt_am_end,
    dt_pm_start,
    dt_pm_1457,
    dt_pm_end,
    filename_trader_template,
    path_history,
    filename_log,
    filename_input,
    filename_signal,
    path_check,
    filename_chip_shelve,
    sleep_to_time,
    str_trading_path,
)


__version__ = "3.0.0"
logger_console_level = "INFO"  # choice of {"TRACE","DEBUG","INFO"，"ERROR"}


if __name__ == "__main__":
    logger.remove()
    logger.add(sink=sys.stderr, level=logger_console_level)
    logger.add(sink=filename_log, level="TRACE", encoding="utf-8")
    # choice of {"TRACE","DEBUG","INFO"，"ERROR"}
    """
    if analysis.is_trading_day():
        logger.trace("Betting day")
        print("Betting day")
    else:
        logger.trace("Non betting day")
        print("Non betting day")
        logger.trace("Program OFF")
        sys.exit()
    """
    """init Begin"""
    frq = 0
    scan_interval = 20
    amount_all = 0
    amount_top5 = 0
    concentration_rate_amount = 0
    str_msg_concentration_rate = ""
    str_msg_concentration_additional = ""
    logger.trace(f"initialization Begin")
    # 加载df_industry_class Begin
    df_industry_member = analysis.read_df_from_db(
        key="df_industry_member", filename=filename_chip_shelve
    )
    if df_industry_member.empty:
        try:
            df_industry_member = pd.read_excel(
                io="df_industry_member.xlsx", index_col=0
            )
        except FileNotFoundError as e:
            print(f"[df_industry_member.xlsx] - {e.args[1]}")
            sys.exit()
        else:
            if df_industry_member.empty:
                logger.error(
                    f"df_industry_member from [df_industry_member.xlsx] is empty"
                )
                sys.exit()
            else:
                analysis.write_obj_to_db(
                    obj=df_industry_member,
                    key="df_industry_member",
                    filename=filename_chip_shelve,
                )
    # 加载df_industry_class End
    index_ssb = analysis.IndexSSB(update=False)
    # 加载df_industry_rank_pool Begin
    df_industry_rank_pool = analysis.read_df_from_db(
        key="df_industry_rank_pool", filename=filename_chip_shelve
    )
    if df_industry_rank_pool.empty:
        print("Please rerun df_industry_rank_pool function")
        df_industry_rank_pool_buying = pd.DataFrame()
        df_industry_rank_pool_selling = pd.DataFrame()
        list_industry_buying = list()
        list_industry_selling = list()
    else:
        df_industry_rank_pool_buying = df_industry_rank_pool[
            df_industry_rank_pool["T5_rank"] >= 66
        ]
        df_industry_rank_pool_selling = df_industry_rank_pool[
            df_industry_rank_pool["T5_rank"] <= 10
        ]
        list_industry_buying = df_industry_rank_pool_buying["name"].tolist()
        list_industry_selling = df_industry_rank_pool_selling["name"].tolist()
    # 加载df_industry_rank_pool End
    df_industry_rank = analysis.read_df_from_db(
        key="df_industry_rank", filename=filename_chip_shelve
    )
    df_industry_rank_deviation = df_industry_rank[df_industry_rank["max_min"] >= 40]
    # 加载df_industry_rank_pool End
    # 加载df_trader Begin
    df_trader = analysis.read_df_from_db(key="df_trader", filename=filename_chip_shelve)
    if df_trader.empty:
        time.sleep(20)
        list_trader_columns = [
            "name",
            "recent_price",
            "position",
            "position_unit",
            "trx_unit_share",
            "now_price",
            "pct_chg",
            "rise",
            "fall",
            "total_mv_E",
            "ssb_index",
            "stock_index",
            "grade",
            "recent_trading",
            "ST",
            "industry_code",
            "industry_name",
            "date_of_inclusion_first",
            "date_of_inclusion_latest",
            "times_of_inclusion",
            "rate_of_inclusion",
            "price_of_inclusion",
            "pct_of_inclusion",
            "remark",
        ]
        list_trader_symbol = ["sh600519", "sz300750"]
        df_trader = pd.DataFrame(index=list_trader_symbol, columns=list_trader_columns)
        df_trader.index.rename(name="code", inplace=True)
    df_stocks_pool = analysis.read_df_from_db(
        key="df_stocks_pool", filename=filename_chip_shelve
    )
    if not df_stocks_pool.empty:
        dt_inclusion = df_stocks_pool["dt"].max()
        for code in df_stocks_pool.index:
            if code not in df_trader.index:
                df_trader.at[code, "date_of_inclusion_first"] = dt_inclusion
                df_trader.at[code, "price_of_inclusion"] = df_trader.at[
                    code, "recent_price"
                ] = df_stocks_pool.at[code, "now_price"]
                df_trader.at[code, "date_of_inclusion_latest"] = dt_inclusion
                df_trader.at[code, "times_of_inclusion"] = 1
            else:
                if (
                    df_trader.at[code, "date_of_inclusion_first"] == dt_init
                    or df_trader.at[code, "date_of_inclusion_latest"] == dt_init
                ):
                    df_trader.at[code, "date_of_inclusion_first"] = dt_inclusion
                    df_trader.at[code, "date_of_inclusion_latest"] = dt_inclusion
                    df_trader.at[code, "price_of_inclusion"] = df_trader.at[
                        code, "recent_price"
                    ] = df_stocks_pool.at[code, "now_price"]
                    df_trader.at[code, "times_of_inclusion"] = 1
                else:
                    if df_trader.at[code, "date_of_inclusion_latest"] != dt_inclusion:
                        df_trader.at[code, "date_of_inclusion_latest"] = dt_inclusion
                        df_trader.at[code, "times_of_inclusion"] += 1
        df_trader = analysis.init_trader(df_trader=df_trader, sort=False)
    # 保存df_trader----Begin
    analysis.write_obj_to_db(
        obj=df_trader, key="df_trader", filename=filename_chip_shelve
    )
    # 保存df_trader----End
    # 创建df_signal----Begin
    if os.access(path=filename_signal, mode=os.F_OK):
        df_signal_sell = pd.read_excel(
            io=filename_signal, sheet_name="sell", index_col=0
        )
        df_signal_buy = pd.read_excel(io=filename_signal, sheet_name="buy", index_col=0)
        df_signal_sell.sort_values(
            by=["position", "pct_chg"], ascending=False, inplace=True
        )
        df_signal_buy.sort_values(
            by=["position", "pct_chg"], ascending=False, inplace=True
        )
    else:
        df_signal_sell = pd.DataFrame(columns=df_trader.columns)
        df_signal_buy = pd.DataFrame(columns=df_trader.columns)
    list_signal_buy_before = df_signal_buy.index.tolist()
    list_signal_sell_before = df_signal_sell.index.tolist()
    # 创建空的交易员模板 file_name_trader_template End
    if not os.access(path=filename_trader_template, mode=os.F_OK):
        df_modified = pd.DataFrame(columns=df_trader.columns)
        df_modified.index.rename(name="code", inplace=True)
        df_add = pd.DataFrame(columns=df_trader.columns)
        df_add.index.rename(name="code", inplace=True)
        df_delete = pd.DataFrame(columns=df_trader.columns)
        df_delete.index.rename(name="code", inplace=True)
        with pd.ExcelWriter(path=filename_trader_template, mode="w") as writer:
            df_modified.to_excel(excel_writer=writer, sheet_name="modified")
            df_add.to_excel(excel_writer=writer, sheet_name="add")
            df_delete.to_excel(excel_writer=writer, sheet_name="delete")
    else:
        pass
    # 创建空的交易员模板 file_name_trader End
    # 取得仓位控制提示
    str_pos_ctl_zh = analysis.position(index="sh000001")
    str_pos_ctl_csi1000 = analysis.position(index="sh000852")
    """init End"""
    """loop Begin"""
    while True:
        if frq > 2:
            os.system("cls")
        dt_now = datetime.datetime.now()
        dict_index_ssb_now = dict()
        if frq % 3 == 0:  # 3 = 1 minutes, 6 = 2 minutes, 15 = 5 minutes
            (
                str_msg_concentration_rate,
                str_msg_concentration_additional,
            ) = analysis.concentration_rate()
            dict_index_ssb_now = index_ssb.realtime_index()
        # 开盘前：9:10 至 9:30
        if dt_am_0910 < dt_now < dt_am_start:
            print(f"The exchange will open at {dt_am_start}")
            sleep_to_time(dt_am_start)
        # 盘中 9:30 -- 11:30 and 13:00 -- 15:00
        elif (dt_am_start <= dt_now <= dt_am_end) or (
            dt_pm_start <= dt_now <= dt_pm_end
        ):
            start_loop_time = time.perf_counter_ns()
            logger.trace(f"start_loop_time = {start_loop_time}")

            # 主循环块---------Start------Start-----Start-----Start----Start-------Start----Start------
            # 增加修改删除df_data中的项目 Begin
            str_msg_modified = ""
            str_msg_add = ""
            str_msg_del = ""
            if os.path.exists(filename_input):
                df_in_modified = pd.read_excel(
                    io=filename_input, sheet_name="modified", index_col=0
                )
                df_in_add = pd.read_excel(
                    io=filename_input, sheet_name="add", index_col=0
                )
                df_in_del = pd.read_excel(
                    io=filename_input, sheet_name="delete", index_col=0
                )
                # 索引转为小写字母 Begin
                df_in_modified.index = df_in_modified.index.str.lower()
                df_in_add.index = df_in_add.index.str.lower()
                df_in_del.index = df_in_del.index.str.lower()
                # 索引转为小写字母 End
                df_in_modified = df_in_modified[
                    ~df_in_modified.index.duplicated(keep="first")
                ]
                df_in_add = df_in_add[~df_in_add.index.duplicated(keep="first")]
                df_in_del = df_in_del[~df_in_del.index.duplicated(keep="first")]
                list_in_modified = df_in_modified.index.to_list()
                list_in_add = df_in_add.index.to_list()
                list_in_del = df_in_del.index.to_list()
                if len(list_in_modified) > 0:
                    df_in_modified["recent_trading"] = dt_now
                    for code in df_in_modified.index:
                        if code in df_trader.index:
                            series_add_index = df_in_modified.loc[code].index
                            for item in series_add_index:
                                if pd.notnull(df_in_modified.at[code, item]):
                                    df_trader.at[code, item] = df_in_modified.at[
                                        code, item
                                    ]
                        else:
                            list_in_modified.remove(code)
                    if list_in_modified:
                        str_msg_modified = f" {list_in_modified}"
                    else:
                        str_msg_modified = ""
                if len(list_in_add) > 0:
                    df_in_add["recent_trading"] = dt_now
                    df_trader = pd.concat(
                        objs=[df_trader, df_in_add], axis=0, join="outer"
                    )
                    df_trader = df_trader[~df_trader.index.duplicated(keep="first")]
                    for code in df_trader.index:
                        if code in df_in_add.index:
                            if pd.isnull(df_trader.at[code, "date_of_inclusion_first"]):
                                df_trader.at[
                                    code, "date_of_inclusion_first"
                                ] = dt_trading
                            else:
                                df_trader.at[
                                    code, "date_of_inclusion_latest"
                                ] = dt_trading
                            if pd.isnull(df_trader.at[code, "times_of_inclusion"]):
                                df_trader.at[code, "times_of_inclusion"] = 1
                            if pd.isnull(df_trader.at[code, "price_of_inclusion"]):
                                df_trader.at[code, "price_of_inclusion"] = df_trader.at[
                                    code, "now_price"
                                ]
                    str_msg_add = f"{list_in_add}"
                if len(list_in_del) > 0:
                    df_in_del["recent_trading"] = dt_now
                    for code in df_in_del.index:
                        if code in df_trader.index:
                            if df_trader.at[code, "position"] <= 0:
                                df_trader.drop(index=code, inplace=True)
                            else:
                                list_in_del.remove(code)
                    str_msg_del = f"{list_in_del}"
                str_now_input = datetime.datetime.now().strftime("%Y%m%d_%H%M%S_%f")
                file_name_input_rename = os.path.join(
                    path_history, f"input_{str_now_input}.xlsx"
                )
                df_trader = analysis.init_trader(df_trader=df_trader)
                try:
                    os.rename(src=filename_input, dst=file_name_input_rename)
                except Exception as e:
                    logger.error(f"[{filename_input}] rename file fail - {repr(e)}")
            # 增加修改删除df_data中的项目 End
            df_stock_market_activity_legu = ak.stock_market_activity_legu()
            df_stock_market_activity_legu.at[10, "value"] = (
                df_stock_market_activity_legu.at[10, "value"]
                .replace("\n", "")
                .replace("\t", "")
            )
            df_stock_market_activity_legu.at[11, "value"] = pd.to_datetime(
                df_stock_market_activity_legu.at[11, "value"]
            )
            str_stock_market_activity_items = (
                fg.red(f"{df_stock_market_activity_legu.at[0, 'item']}   ")
                + fg.green(f"{df_stock_market_activity_legu.at[4, 'item']}   ")
                + f"{df_stock_market_activity_legu.at[1, 'item']}   "
                f"{df_stock_market_activity_legu.at[5, 'item']}   "
                f"{df_stock_market_activity_legu.at[2, 'item']}   "
                f"{df_stock_market_activity_legu.at[6, 'item']}   "
                f"{df_stock_market_activity_legu.at[3, 'item']}   "
                f"{df_stock_market_activity_legu.at[7, 'item']}   "
                f"{df_stock_market_activity_legu.at[8, 'item']}   "
                f"{df_stock_market_activity_legu.at[9, 'item']}   "
                f"{df_stock_market_activity_legu.at[10, 'item']}   "
                f"{df_stock_market_activity_legu.at[11, 'value'].date()}"
            )
            str_stock_market_activity_value = (
                fg.red(f"{df_stock_market_activity_legu.at[0, 'value']:4.0f}   ")
                + fg.green(f"{df_stock_market_activity_legu.at[4, 'value']:4.0f}  ")
                + f"{df_stock_market_activity_legu.at[1, 'value']:4.0f}   "
                f"{df_stock_market_activity_legu.at[5, 'value']:4.0f}     "
                f"{df_stock_market_activity_legu.at[2, 'value']:4.0f}       "
                f"{df_stock_market_activity_legu.at[6, 'value']:4.0f}         "
                f"{df_stock_market_activity_legu.at[3, 'value']:4.0f}        "
                f"{df_stock_market_activity_legu.at[7, 'value']:4.0f}      "
                f"{df_stock_market_activity_legu.at[8, 'value']:4.0f}  "
                f"{df_stock_market_activity_legu.at[9, 'value']:4.0f}     "
                f"{df_stock_market_activity_legu.at[10, 'value']}   "
                f"{df_stock_market_activity_legu.at[11, 'value'].time()}"
            )
            # 更新df_data，str_msg_rise，str_msg_fall------Begin
            # 调用实时数据接口，更新df_realtime Begin
            i_realtime = 0
            df_realtime = pd.DataFrame()
            while i_realtime <= 2:
                i_realtime += 1
                df_realtime = analysis.realtime_quotations(
                    stock_codes=df_trader.index.to_list()
                )  # 调用实时数据接口
                if not df_realtime.empty:
                    break
            if df_realtime.empty:
                logger.error("df_realtime is empty, sys exit")
                sys.exit()
            # 调用实时数据接口，更新df_realtime End
            list_signal_on_sell = list()
            list_signal_on_buy = list()
            i = 0
            count_trader = len(df_trader.index)
            for code in df_trader.index:
                i += 1
                dt_now = datetime.datetime.now()
                now_price = df_realtime.at[code, "close"]
                pct_chg = (now_price / df_trader.at[code, "recent_price"] - 1) * 100
                pct_chg = round(pct_chg, 2)
                if df_trader.at[code, "price_of_inclusion"] != 0:
                    pct_of_inclusion = (
                        now_price / df_trader.at[code, "price_of_inclusion"] - 1
                    ) * 100
                    pct_of_inclusion = round(pct_of_inclusion, 2)
                else:
                    pct_of_inclusion = 0
                df_trader.at[code, "now_price"] = now_price
                df_trader.at[code, "pct_chg"] = pct_chg
                df_trader.at[code, "pct_of_inclusion"] = pct_of_inclusion
                if (
                    pct_chg >= df_trader.at[code, "rise"]
                    and df_trader.at[code, "position"] > 0
                ):
                    df_signal_sell.loc[code] = df_trader.loc[code]
                    list_signal_on_sell.append(code)
                elif pct_chg <= df_trader.at[code, "fall"]:
                    df_signal_buy.loc[code] = df_trader.loc[code]
                    list_signal_on_buy.append(code)
                if code in df_signal_sell.index:
                    df_signal_sell.loc[code] = df_trader.loc[code]
                if code in df_signal_buy.index:
                    df_signal_buy.loc[code] = df_trader.loc[code]
            analysis.write_obj_to_db(
                obj=df_trader, key="df_trader", filename=filename_chip_shelve
            )
            if frq % 3 == 0:
                filename_data_csv = os.path.join(path_check, f"trader_{str_trading_path()}.csv")
                df_trader.to_csv(path_or_buf=filename_data_csv)
            list_signal_buy_after = df_signal_buy.index.tolist()
            list_signal_sell_after = df_signal_sell.index.tolist()
            list_signal_chg = list()
            for code in list_signal_buy_after:
                if code not in list_signal_buy_before:
                    list_signal_chg.append(code)
            for code in list_signal_sell_after:
                if code not in list_signal_sell_before:
                    list_signal_chg.append(code)
            if list_signal_chg:
                with pd.ExcelWriter(path=filename_signal, mode="w") as writer:
                    df_signal_sell.to_excel(excel_writer=writer, sheet_name="sell")
                    df_signal_buy.to_excel(excel_writer=writer, sheet_name="buy")
            list_signal_sell_before = list_signal_sell_after.copy()
            list_signal_buy_before = list_signal_buy_after.copy()
            df_signal_buy.sort_values(
                by=["rate_of_inclusion", "pct_chg"],
                ascending=[True, False],
                inplace=True,
            )
            df_signal_sell.sort_values(
                by=["rate_of_inclusion", "pct_chg"],
                ascending=[True, False],
                inplace=True,
            )
            list_signal_t0 = list()
            for code in df_signal_buy.index:
                if code in df_signal_sell.index:
                    list_signal_t0.append(code)
            dict_list_signal_on = {
                "Buy": list_signal_on_buy,
                "Sell": list_signal_on_sell,
            }
            dict_df_signal = {
                "Buy": df_signal_buy,
                "Sell": df_signal_sell,
            }
            msg_signal_chg = ""
            msg_signal_t0 = ""
            i = 0
            for item in dict_df_signal:
                msg_signal = ""
                if item in "Buy":
                    str_arrow = "↓"
                elif item in "Sell":
                    str_arrow = "↑"
                else:
                    str_arrow = "|"
                i_records = 0
                df_item = dict_df_signal[item]
                all_records = len(df_item)
                for code in df_item.index:
                    i_records += 1
                    if code not in dict_list_signal_on[item]:
                        continue
                    msg_signal_code_1 = (
                        f"<{item}-{i_records}>-[{code}_{df_item.at[code, 'name']}]-"
                        f"<{df_item.at[code, 'now_price']:5.2f}_{str_arrow}_{df_item.at[code, 'pct_chg']:5.2f}%> - "
                        f"[{df_item.at[code, 'recent_price']:5.2f} * "
                        f"{int(df_item.at[code, 'position']):4d}:( "
                        f"{df_item.at[code, 'position_unit']:3.1f}*"
                        f"{int(df_item.at[code, 'trx_unit_share']):3d})]"
                    )
                    msg_signal_code_2 = f" - [{df_trader.at[code, 'ST']}]"
                    industry_code = df_item.at[code, "industry_code"]
                    msg_signal_code_3 = (
                        f"---- [{df_trader.at[code, 'industry_name']}] - [{industry_code}] - "
                        f"[Diff={df_industry_rank.at[industry_code, 'max_min']:02.0f}]"
                    )
                    if industry_code in df_industry_rank_deviation.index:
                        msg_signal_code_3 += (
                            f" - [{df_industry_rank_deviation.at[industry_code, 'T5_rank']:2.0f} - "
                            f"{df_industry_rank_deviation.at[industry_code, 'T20_rank']:02.0f} - "
                            f"{df_industry_rank_deviation.at[industry_code, 'T40_rank']:02.0f} - "
                            f"{df_industry_rank_deviation.at[industry_code, 'T60_rank']:02.0f} - "
                            f"{df_industry_rank_deviation.at[industry_code, 'T80_rank']:02.0f}]"
                        )
                    msg_signal_code_4 = f"---- [{df_trader.at[code, 'grade']}]"
                    msg_signal_code_5 = f" - {df_trader.at[code, 'stock_index']}"
                    msg_signal_code_6 = (
                        f"---- [T: {df_trader.at[code, 'recent_trading'].date()}]"
                        f" - [R:{df_trader.at[code, 'rate_of_inclusion']:6.2f}%]"
                        f" - [T:{df_trader.at[code, 'times_of_inclusion']:3.0f}]"
                        f" - [F: {df_trader.at[code, 'date_of_inclusion_first'].date()}]"
                        f" - [L: {df_trader.at[code, 'date_of_inclusion_latest'].date()}]"
                    )
                    if item in "Buy":
                        msg_signal_code_1 = fg.lightgreen(msg_signal_code_1)
                    elif item in "Sell":
                        msg_signal_code_1 = fg.red(msg_signal_code_1)
                    if industry_code in df_industry_rank_pool_selling.index:
                        msg_signal_code_3 = fg.red(msg_signal_code_3)
                    elif industry_code in df_industry_rank_pool_buying.index:
                        msg_signal_code_3 = fg.green(msg_signal_code_3)
                    msg_signal_code_4 = fg.yellow(msg_signal_code_4)
                    msg_signal_code_5 = fg.purple(msg_signal_code_5)
                    msg_signal_code = (
                        "\n"
                        + msg_signal_code_1
                        + msg_signal_code_2
                        + "\n"
                        + msg_signal_code_3
                        + "\n"
                        + msg_signal_code_4
                        + msg_signal_code_5
                        + "\n"
                        + msg_signal_code_6
                        + "\n"
                    )
                    if code in list_signal_chg:
                        msg_signal_chg += msg_signal_code
                    elif code in list_signal_t0:
                        msg_signal_t0 += msg_signal_code
                    else:
                        msg_signal += msg_signal_code
                if msg_signal:
                    if item in "Sell":
                        print(f"====<Suggest {item}>====\a", "=" * 60)
                    else:
                        print(f"====<Suggest {item}>====", "=" * 61)
                    print(msg_signal)
                    print("=" * 86)
            if msg_signal_t0:
                print(f"====<T0>====", "=" * 74)
                print(msg_signal_t0)
            if msg_signal_chg:
                print(f"====<Change>====\a", "=" * 70)
                print(msg_signal_chg)
            # 更新df_data，str_msg_rise，str_msg_fall------End
            dt_now = datetime.datetime.now()
            str_dt_now_time = dt_now.strftime("<%H:%M:%S>")
            if str_msg_modified:
                str_msg_modified = (
                    f"{str_dt_now_time}----modified: {fg.blue(str_msg_modified)}"
                )
                print("=" * 86)
                print(str_msg_modified)
            if str_msg_add:
                str_msg_add = f"{str_dt_now_time}----add: {fg.red(str_msg_add)}"
                print("=" * 86)
                print(str_msg_add)
            if str_msg_del:
                str_msg_del = f"{str_dt_now_time}----remove: {fg.green(str_msg_del)}"
                print("=" * 86)
                print(str_msg_del)
            if list_industry_buying:
                print(f"{fg.green(f'Buying: {list_industry_buying}')}")
                print("*" * 86)
            if list_industry_selling:
                print(f"{fg.red(f'Selling: {list_industry_selling}')}")
                print("*" * 86)
            if dict_index_ssb_now:
                print(dict_index_ssb_now)
            print("#" * 108)
            print(str_stock_market_activity_items)
            print(str_stock_market_activity_value)
            print("#" * 108)
            # 主循环块---------End----End-----End----End------End----End------End------End-------End------

            end_loop_time = time.perf_counter_ns()
            interval_time = (end_loop_time - start_loop_time) / 1000000000
            str_gm = time.strftime("%H:%M:%S", time.gmtime(interval_time))
            str_msg_loop_end = f"{dt_now}----[{str_gm}]"
            str_msg_loop_ctl_zh = f"{str_dt_now_time}----{fg.red(str_pos_ctl_zh)}"
            str_msg_loop_ctl_csi1000 = (
                f"{str_dt_now_time}----{fg.red(str_pos_ctl_csi1000)}"
            )
            print(f"{str_dt_now_time}----{str_msg_concentration_rate}")
            print(f"{str_dt_now_time}----{str_msg_concentration_additional}")
            print(str_msg_loop_ctl_zh)
            print(str_msg_loop_ctl_csi1000)
            # 收盘前集合竟价：14:57 -- 15:00 响玲
            if dt_pm_1457 < dt_now <= dt_pm_end:
                print("\a", end="")
                scan_interval = 60
            dt_now = datetime.datetime.now()
            dt_now_delta = dt_now + datetime.timedelta(seconds=scan_interval)
            sleep_to_time(dt_now_delta)
        # 中午体息时间： 11:30 -- 13:00
        elif dt_am_end < dt_now < dt_pm_start:
            # -----当前时间与当日指定时间的间隔时间计算-----
            sleep_to_time(dt_pm_start)
            # -----当前时间与当日指定时间的间隔时间计算-----
        else:
            print("\a\r", end="")
            analysis.unit_net()
            str_pos_ctl_zh = analysis.position(index="sh000001")
            str_pos_ctl_csi1000 = analysis.position(index="sh000852")
            print(str_pos_ctl_zh)
            print(str_pos_ctl_csi1000)
            df_chip = analysis.chip()
            print(df_chip)
            if dt_now < dt_am_0910:
                sleep_to_time(dt_am_start)
            elif dt_now > dt_pm_end:
                logger.trace(f"Program End")
                sys.exit()
        frq += 1
