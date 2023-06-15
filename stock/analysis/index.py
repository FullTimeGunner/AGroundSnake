# modified at 2023/05/18 22::25
import os
import sys
import time
import feather
import numpy as np
import requests
import datetime
import random
import win32file
import shelve
import pandas as pd
from loguru import logger
from pyecharts.charts import Line, Page
import pyecharts.options as opts
import tushare as ts
import analysis.ashare
from analysis.const import (
    dt_am_start,
    dt_am_end,
    dt_pm_start,
    dt_pm_end,
    dt_trading,
)


class IndexSSB(object):
    def __init__(self, update: bool = True, origin: int = 2023):
        self.__name = "IndexSSB"
        logger.trace(f"__init__ Begin")
        self.__pro = ts.pro_api()
        self.date_origin = datetime.date(year=origin, month=1, day=1)
        self.dt_time_1500 = datetime.time(hour=15, minute=0, second=0, microsecond=0)
        self.str_date_origin = self.date_origin.strftime("%Y%m%d")
        self.date_now = datetime.datetime.now().date()
        self.str_date_now = self.date_now.strftime("%Y%m%d")
        path_main = os.getcwd()
        self.path_mv = os.path.join(path_main, "data", "mv")
        self.path_check = os.path.join(path_main, "check")
        self.filename_shelve = os.path.join(self.path_mv, f"mv")
        self.filename_index_charts = os.path.join(self.path_check, "index_charts.html")
        self.filename_index_charts_min = os.path.join(
            self.path_check, "index_charts_min.html"
        )
        df_trade_cal = self.__pro.trade_cal(
            exchange="", start_date=self.str_date_origin, end_date=self.str_date_now
        )
        df_trade_cal.sort_values(
            by=["cal_date"], ascending=True, inplace=True, ignore_index=True
        )
        df_trade_cal = df_trade_cal[["cal_date", "is_open"]]
        df_trade_cal["cal_date"] = df_trade_cal["cal_date"].apply(
            func=lambda x: datetime.datetime.combine(
                pd.to_datetime(x).date(), self.dt_time_1500
            )
        )
        df_trade_cal.set_index(keys=["cal_date"], inplace=True)
        self.df_index_exist = self.__read_df_from_dbm(key="df_index_exist")
        if self.df_index_exist.empty:
            logger.error("df_index_exist is not exist")
            self.df_index_exist = df_trade_cal
            self.df_index_exist = self.df_index_exist.reindex(
                columns=["is_open", "market_value", "index_ssb"]
            )
        else:
            self.df_index_exist = self.df_index_exist.reindex(index=df_trade_cal.index)
            self.df_index_exist["is_open"] = df_trade_cal["is_open"]
        self.df_index_exist.fillna(value=0, inplace=True)
        self.__write_df_from_dbm(df=self.df_index_exist, key="df_index_exist")
        self.df_mv = self.__pro.stock_basic(
            exchange="",
            list_status="L",
            fields="ts_code,symbol,name,list_date",
        )
        self.df_mv["symbol"] = self.df_mv["ts_code"].apply(
            func=lambda ts_code: ts_code[-2:].lower() + ts_code[:6]
        )
        self.df_mv["list_dt"] = pd.to_datetime(self.df_mv["list_date"])
        self.df_mv.drop(columns=["ts_code", "list_date"], inplace=True)
        self.df_mv.set_index(keys=["symbol"], inplace=True)
        self.df_mv.sort_values(by=["list_dt"], ascending=True, inplace=True)
        list_index_columns = self.df_mv.columns.tolist() + [
            "base_mv",
            "now_mv",
            "t1_mv",
            "base_dt",
            "now_dt",
            "t1_dt",
        ]
        self.df_mv = self.df_mv.reindex(columns=list_index_columns)
        self.dt_init = datetime.datetime(year=1989, month=1, day=1)
        self.df_mv["base_dt"] = self.dt_init
        self.df_mv["now_dt"] = self.dt_init
        self.df_mv["t1_dt"] = self.dt_init
        if update is True:
            self.make()  # 更新数据调用
            self.shelve_to_excel()
        logger.trace(f"__init__ End")

    def __read_df_from_dbm(self, key: str) -> pd.DataFrame:
        with shelve.open(filename=self.filename_shelve, flag="r") as py_dbm:
            try:
                return py_dbm[key]
            except KeyError as e:
                logger.error(f"{repr(e.args[0])} does not exist and returns empty df")
                return pd.DataFrame()

    def __write_df_from_dbm(self, df: pd.DataFrame, key: str):
        if isinstance(df, pd.DataFrame):
            with shelve.open(filename=self.filename_shelve, flag="c") as py_dbm:
                py_dbm[key] = df
        else:
            logger.error(f"{key} is not DataFrame")

    def test(self):
        pass
        df = self.__read_df_from_dbm(key="df_index_exist")
        """
        df.iloc[-1 ,1] = 0
        df.iloc[-1 ,2] = 0
        self.__write_df_from_dbm(df=df, key="df_index_exist")
        """
        print(df)

    def version(self) -> pd.Timestamp:
        i = 0
        count = len(self.df_index_exist)
        while i > -count:
            i -= 1
            s = self.df_index_exist.iloc[i].sum()
            if s == 3:
                return self.df_index_exist.iloc[i].name

    def __get_market_values(self, dt_pos: datetime.datetime) -> bool:
        name = "market_value"
        data = "is_open"
        logger.trace(f"{name} Begin")
        if self.df_index_exist.at[dt_pos, data] == 1:
            if self.df_index_exist.at[dt_pos, name] == 1:
                logger.trace(f"{name} Break, and End")
                return True
        else:
            print(f"get_market_values: {dt_pos} is not trading day")
            return False
        str_date_pos_ul = dt_pos.strftime("%Y_%m_%d")
        str_df_mv = f"mv_{str_date_pos_ul}"
        str_date_pos = dt_pos.strftime("%Y%m%d")
        filename_df_mv_temp = os.path.join(
            self.path_mv, f"df_mv_temp_{str_date_pos_ul}.ftr"
        )
        df_mv = self.__read_df_from_dbm(key=str_df_mv)
        if df_mv.empty:
            print(f"{name} - [{str_df_mv}] not in shelve")
            if os.path.exists(filename_df_mv_temp):
                df_mv = feather.read_dataframe(source=filename_df_mv_temp)
            else:
                df_mv = self.df_mv
        diff_date_pos = 0
        same_date_pos = 0
        share_change = 0
        count = len(df_mv)
        df_mv.sort_values(by=["base_mv"], inplace=True)
        df_mv = df_mv.sample(frac=1)
        i = 0
        times_try = 3
        for symbol in df_mv.index:
            ts_code = symbol[2:] + "." + symbol[:2]
            feather.write_dataframe(df=df_mv, dest=filename_df_mv_temp)
            i += 1
            str_msg_bar = f"{name} - {str_df_mv}: [{i:04d}/{count}] - [{symbol}]"
            now_dt = df_mv.at[symbol, "now_dt"]
            if now_dt != dt_pos:
                i_times = 0
                df_daily_basic_symbol = pd.DataFrame()
                while i_times < times_try:
                    i_times += 1
                    try:
                        df_daily_basic_symbol = self.__pro.daily_basic(
                            ts_code=ts_code,
                            start_date=self.str_date_origin,
                            end_date=str_date_pos,
                            fields="trade_date,total_share,total_mv",
                        )
                    except requests.exceptions.ConnectionError as e:
                        print(f"\r{str_msg_bar} - {repr(e)}\033[K")
                        time.sleep(1)
                        df_daily_basic_symbol = pd.DataFrame()
                    else:
                        if df_daily_basic_symbol is None:
                            print(
                                f"\r{str_msg_bar} - df_daily_basic_symbol is None - ({i_times})\033[K"
                            )
                            time.sleep(0.5)
                            df_daily_basic_symbol = pd.DataFrame()
                        if df_daily_basic_symbol.empty:
                            print(
                                f"\r{str_msg_bar} - df_daily_basic_symbol empty - ({i_times})\033[K"
                            )
                            time.sleep(0.5)
                        else:
                            break
                if df_daily_basic_symbol.empty:
                    df_mv.drop(index=symbol, inplace=True)
                    print(f"\r{str_msg_bar} - drop#1\033[K")
                else:
                    df_daily_basic_symbol.dropna(inplace=True)
                    df_daily_basic_symbol["trade_date"] = pd.to_datetime(
                        df_daily_basic_symbol["trade_date"]
                    )
                    df_daily_basic_symbol["trade_date"] = df_daily_basic_symbol[
                        "trade_date"
                    ].apply(
                        func=lambda x: datetime.datetime.combine(
                            pd.to_datetime(x).date(), self.dt_time_1500
                        )
                    )
                    df_daily_basic_symbol.sort_values(
                        by=["trade_date"],
                        ascending=True,
                        inplace=True,
                        ignore_index=True,
                    )
                    count_daily_basic_symbol = len(df_daily_basic_symbol)
                    if count_daily_basic_symbol >= 2:
                        max_index = count_daily_basic_symbol - 1
                        max2_index = count_daily_basic_symbol - 2
                    else:
                        max_index = max2_index = 0
                    min_index = 0
                    now_total_share = df_daily_basic_symbol.at[max_index, "total_share"]
                    base_total_share = df_daily_basic_symbol.at[
                        min_index, "total_share"
                    ]
                    if now_total_share == base_total_share:
                        base_dt = df_daily_basic_symbol.at[min_index, "trade_date"]
                        now_dt = df_daily_basic_symbol.at[max_index, "trade_date"]
                        df_mv.at[symbol, "base_mv"] = df_daily_basic_symbol.at[
                            min_index, "total_mv"
                        ]
                        df_mv.at[symbol, "now_mv"] = df_daily_basic_symbol.at[
                            max_index, "total_mv"
                        ]
                        df_mv.at[symbol, "t1_mv"] = df_daily_basic_symbol.at[
                            max2_index, "total_mv"
                        ]
                        df_mv.at[symbol, "base_dt"] = base_dt
                        df_mv.at[symbol, "now_dt"] = now_dt
                        df_mv.at[symbol, "t1_dt"] = df_daily_basic_symbol.at[
                            max2_index, "trade_date"
                        ]
                        if now_dt != dt_pos:
                            diff_date_pos += 1
                            print(
                                f"\r{str_msg_bar} - [diff = {diff_date_pos}/{same_date_pos}]"
                                f" - [<{now_dt.date()}> / <{dt_pos.date()}>]\033[K"
                            )
                        else:
                            same_date_pos += 1
                            print(
                                f"\r{str_msg_bar} - Update\033[K",
                                end="",
                            )
                    elif now_total_share != base_total_share:
                        share_change += 1
                        str_msg_bar += (
                            f" - [Change = {share_change:2d}]"
                            f" - [{base_total_share.round(2)} - {now_total_share.round(2)}]\033[K"
                        )
                        df_pro_bar_symbol = pd.DataFrame()
                        i_pro_bar = 0
                        while i_pro_bar < times_try:
                            i_pro_bar += 1
                            try:
                                df_pro_bar_symbol = ts.pro_bar(
                                    ts_code=ts_code,
                                    adj="qfq",
                                    start_date=self.str_date_origin,
                                    end_date=str_date_pos,
                                )
                            except requests.exceptions.ConnectionError as e:
                                print(f"\r{str_msg_bar} - {repr(e)}\033[K")
                                time.sleep(1)
                                df_pro_bar_symbol = pd.DataFrame()
                            else:
                                if df_pro_bar_symbol is None:
                                    print(
                                        f"\r{str_msg_bar} - df_pro_bar_symbol is None - ({i_pro_bar})\033[K"
                                    )
                                    time.sleep(0.5)
                                    df_pro_bar_symbol = pd.DataFrame()
                                elif df_pro_bar_symbol.empty:
                                    print(
                                        f"\r{str_msg_bar} - df_pro_bar_symbol empty - ({i_pro_bar})\033[K"
                                    )
                                    time.sleep(0.5)
                                else:
                                    break
                        if df_pro_bar_symbol.empty:
                            df_mv.drop(index=symbol, inplace=True)
                            print(f"\r{str_msg_bar} - drop#2\033[K")
                        else:
                            df_pro_bar_symbol.dropna(inplace=True)
                            df_pro_bar_symbol["trade_date"] = pd.to_datetime(
                                df_pro_bar_symbol["trade_date"]
                            )
                            df_pro_bar_symbol["trade_date"] = df_pro_bar_symbol[
                                "trade_date"
                            ].apply(
                                func=lambda x: datetime.datetime.combine(
                                    pd.to_datetime(x).date(), self.dt_time_1500
                                )
                            )
                            df_pro_bar_symbol.sort_values(
                                by=["trade_date"],
                                ascending=True,
                                inplace=True,
                                ignore_index=True,
                            )
                            count_pro_bar_symbol = len(df_pro_bar_symbol)
                            if count_pro_bar_symbol >= 2:
                                max_index_close = count_pro_bar_symbol - 1
                                max2_index_close = count_pro_bar_symbol - 2
                            else:
                                max_index_close = max2_index_close = 0
                            min_index_close = 0
                            base_dt = df_pro_bar_symbol.at[
                                min_index_close, "trade_date"
                            ]
                            now_dt = df_pro_bar_symbol.at[max_index_close, "trade_date"]
                            df_mv.at[symbol, "base_mv"] = (
                                df_pro_bar_symbol.at[min_index_close, "close"]
                                * now_total_share
                            )
                            df_mv.at[symbol, "now_mv"] = (
                                df_pro_bar_symbol.at[max_index_close, "close"]
                                * now_total_share
                            )
                            df_mv.at[symbol, "t1_mv"] = (
                                df_pro_bar_symbol.at[max2_index_close, "close"]
                                * now_total_share
                            )
                            df_mv.at[symbol, "base_dt"] = base_dt
                            df_mv.at[symbol, "now_dt"] = now_dt
                            df_mv.at[symbol, "t1_dt"] = df_pro_bar_symbol.at[
                                max2_index_close, "trade_date"
                            ]
                            if now_dt != dt_pos:
                                diff_date_pos += 1
                                print(
                                    f"\r{str_msg_bar} - [diff = {diff_date_pos}/{same_date_pos}]"
                                    f" - [<{now_dt.date()}> / <{dt_pos.date()}>]\033[K"
                                )
                            else:
                                same_date_pos += 1
                                print(
                                    f"\r{str_msg_bar}\033[K",
                                )
                    else:
                        logger.error(
                            f"{symbol} - Unknow Error"
                            f" - now_total_share={now_total_share}, base_total_share={base_total_share}"
                        )
            elif now_dt == dt_pos:
                same_date_pos += 1
                print(
                    f"\r{str_msg_bar} - latest\033[K",
                    end="",
                )
            else:
                diff_date_pos += 1
                print(
                    f"\r{str_msg_bar} - [{now_dt.date()}] - [{dt_pos.date()}] - None\033[K"
                )
            if diff_date_pos >= 100:
                print("\n", end="")
                logger.error("diff_date_pos >= 100")
                sys.exit()
        if i >= count:
            print("\n", end="")  # 格式处理
            df_mv.dropna(inplace=True)
            df_mv.applymap(
                func=lambda x: round(x / 10000, 2)
                if (isinstance(x, (int, float)) and x > 100)
                else (round(x, 4) if (isinstance(x, (int, float)) and x < 100) else x)
            )
            date_mv_max = df_mv["now_dt"].max()
            if date_mv_max == dt_pos:
                self.df_index_exist.at[dt_pos, name] = 1
                self.__write_df_from_dbm(df=self.df_index_exist, key="df_index_exist")
                self.__write_df_from_dbm(df=df_mv, key=str_df_mv)
            if os.path.exists(filename_df_mv_temp):
                os.remove(path=filename_df_mv_temp)
        logger.trace(f"{name} End")
        return True

    def __make_index(self, dt_pos: datetime.datetime):
        name = "index_ssb"
        data = "market_value"
        logger.trace(f"{name} Begin")
        str_date_pos_ul = dt_pos.strftime("%Y_%m_%d")
        str_df_mv = f"mv_{str_date_pos_ul}"
        if (
            self.df_index_exist.at[dt_pos, data] == 1
            and self.df_index_exist.at[dt_pos, name] == 1
        ):
            logger.trace(f"{name} Break, and End")
            return True
        bool_get_mv = self.__get_market_values(dt_pos=dt_pos)
        if bool_get_mv:
            df_mv = self.__read_df_from_dbm(key=str_df_mv)
            if df_mv.empty:
                logger.error(f"{str_df_mv} is not exist, sys exit")
                sys.exit()
        else:
            print(f"make_index: {dt_pos.date()} is not trading day")
            return False
        df_mv_non_st = df_mv[~df_mv["name"].str.contains("ST").fillna(False)].copy()
        df_mv_st = df_mv[df_mv["name"].str.contains("ST").fillna(False)].copy()
        df_index_ssb = self.__read_df_from_dbm(key="df_index_ssb")
        if df_index_ssb.empty:
            logger.error(f"load df_index_ssb from py_dbm fail - [non exist]")
            df_index_ssb = pd.DataFrame(
                columns=[
                    "ssb_all",
                    "ssb_green_snake",
                    "ssb_non_st",
                    "ssb_50",
                    "ssb_300",
                    "ssb_500",
                    "ssb_1000",
                    "ssb_2000",
                    "ssb_tail",
                    "ssb_st",
                    "base_mv_all",
                    "base_mv_green_snake",
                    "base_mv_non_st",
                    "base_mv_50",
                    "base_mv_300",
                    "base_mv_500",
                    "base_mv_1000",
                    "base_mv_2000",
                    "base_mv_tail",
                    "base_mv_st",
                    "now_mv_all",
                    "now_mv_green_snake",
                    "now_mv_non_st",
                    "now_mv_50",
                    "now_mv_300",
                    "now_mv_500",
                    "now_mv_1000",
                    "now_mv_2000",
                    "now_mv_tail",
                    "now_mv_st",
                ]
            )
        df_mv_non_st.sort_values(by=["base_mv"], ascending=False, inplace=True)
        df_mv_50 = df_mv_non_st.iloc[:50].copy()
        df_mv_300 = df_mv_non_st.iloc[:300].copy()
        df_mv_500 = df_mv_non_st.iloc[300:800].copy()
        df_mv_1000 = df_mv_non_st.iloc[800:1800].copy()
        df_mv_2000 = df_mv_non_st.iloc[1800:3800].copy()
        df_mv_tail = df_mv_non_st.iloc[3800:].copy()
        df_mv_green_snake = df_mv_tail[
            (~df_mv_tail.index.str.contains("bj"))
            & (~df_mv_tail.index.str.contains("sh68"))
            & (~df_mv_tail["name"].str.contains("退").fillna(False))
        ].copy()
        dict_df_index_n = {
            "all": df_mv,
            "non_st": df_mv_non_st,
            "st": df_mv_st,
            "50": df_mv_50,
            "300": df_mv_300,
            "500": df_mv_500,
            "1000": df_mv_1000,
            "2000": df_mv_2000,
            "tail": df_mv_tail,
            "green_snake": df_mv_green_snake,
        }
        for key in dict_df_index_n:
            df_mv_n = dict_df_index_n[key]
            base_mv_n = df_mv_n["base_mv"].sum()
            now_mv_n = df_mv_n["now_mv"].sum()
            df_index_ssb.at[dt_pos, f"base_mv_{key}"] = round(base_mv_n / 100000000, 2)
            df_index_ssb.at[dt_pos, f"now_mv_{key}"] = round(now_mv_n / 100000000, 2)
            if base_mv_n > 0:
                stocks_index_n = now_mv_n / base_mv_n * 1000
            else:
                stocks_index_n = 0
                logger.error(f"[{key}] - base_mv_n = {base_mv_n}")
            stocks_index_n = round(stocks_index_n, 2)
            df_index_ssb.at[dt_pos, f"ssb_{key}"] = stocks_index_n
            df_mv_n[f"contribution_points_index_{key}"] = (
                (df_mv_n["now_mv"] - df_mv_n["base_mv"]) / base_mv_n * 1000
            )
            df_mv_n[f"t1_contribution_points_index_{key}"] = (
                (df_mv_n["t1_mv"] - df_mv_n["base_mv"]) / base_mv_n * 1000
            )
            df_mv_n[f"now_t1_contribution_points_index_{key}"] = (
                df_mv_n[f"contribution_points_index_{key}"]
                - df_mv_n[f"t1_contribution_points_index_{key}"]
            )
            df_mv_n.sort_values(
                by=[f"now_t1_contribution_points_index_{key}"],
                ascending=False,
                inplace=True,
            )
            df_mv_n = df_mv_n.applymap(
                func=lambda x: round(x / 10000, 2)
                if (isinstance(x, (int, float)) and x > 100)
                else (round(x, 4) if (isinstance(x, (int, float)) and x < 100) else x)
            )
            dict_df_index_n[key] = df_mv_n
        with shelve.open(filename=self.filename_shelve, flag="c") as py_dbm:
            i = 0
            count = len(dict_df_index_n)
            for key in dict_df_index_n:
                i += 1
                df_mv_n_name = f"df_mv_{key}"
                py_dbm[df_mv_n_name] = dict_df_index_n[key]
                print(
                    f"\r{name} - {dt_pos.date()} - [{i:2d}/{count:2d}] - {df_mv_n_name} - save\033[K",
                    end="",
                )
        if i >= count:
            print("\n", end="")
            self.df_index_exist.at[dt_pos, name] = 1
            self.__write_df_from_dbm(df=self.df_index_exist, key="df_index_exist")
            self.__write_df_from_dbm(df=df_index_ssb, key="df_index_ssb")
        logger.trace(f"{name} End")
        return True

    def __make_index_line(self):
        logger.trace("make_index_line Start")
        i = 0
        count = len(self.df_index_exist.index)
        for dt_exist_index in self.df_index_exist.index:
            i += 1
            dt_now = datetime.datetime.now()
            dt_date_now = dt_now.date()
            dt_date_exist_index = dt_exist_index.date()
            if dt_date_exist_index == dt_date_now and dt_now < dt_exist_index:
                break
            str_msg_bar = f"[{dt_now.time()}]Current Date: {dt_date_exist_index} - [{i:03d}/{count:03d}]"
            if self.df_index_exist.at[dt_exist_index, "is_open"] == 1:
                if not self.__make_index(dt_exist_index):
                    logger.error(
                        f"make_index_line: {dt_date_exist_index} is not trading day"
                    )
                    str_msg_bar += " - [non trading day]"
                else:
                    str_msg_bar += " - [trading day]"
            else:
                str_msg_bar += " - [non trading day]"
            print(f"\r{str_msg_bar}\033[K")
        logger.trace("make_index_line End")

    def __make_charts(self):
        name = "make_charts"
        logger.trace(f"{name} Begin")
        df_index_ssb = self.__read_df_from_dbm(key="df_index_ssb")
        if df_index_ssb.empty:
            logger.error(f"df_index_ssb is not exist")
            return
        df_index_ssb.sort_index(inplace=True)
        x_axis = df_index_ssb.index.tolist()
        dict_list_index_n = {
            "all": df_index_ssb["ssb_all"].tolist(),
            "non_st": df_index_ssb["ssb_non_st"].tolist(),
            "50": df_index_ssb["ssb_50"].tolist(),
            "300": df_index_ssb["ssb_300"].tolist(),
            "500": df_index_ssb["ssb_500"].tolist(),
            "1000": df_index_ssb["ssb_1000"].tolist(),
            "2000": df_index_ssb["ssb_2000"].tolist(),
            "tail": df_index_ssb["ssb_tail"].tolist(),
            "green_snake": df_index_ssb["ssb_green_snake"].tolist(),
        }
        list_values = list()
        for key in dict_list_index_n:
            list_values += dict_list_index_n[key]
        list_values = [x for x in list_values if ~np.isnan(x)]
        y_min = min(list_values)
        y_max = max(list_values)
        line_index = Line(
            init_opts=opts.InitOpts(
                width="1800px",
                height="860px",
            ),
        )
        line_index.add_xaxis(xaxis_data=x_axis)
        for key in dict_list_index_n:
            line_index.add_yaxis(
                series_name=f"ssb_{key}",
                y_axis=dict_list_index_n[key],
                is_symbol_show=False,
            )
        line_index.set_global_opts(
            title_opts=opts.TitleOpts(title="SSB Index", pos_top="0%", pos_left="50%"),
            tooltip_opts=opts.TooltipOpts(trigger="axis"),
            toolbox_opts=opts.ToolboxOpts(),
            legend_opts=opts.LegendOpts(
                orient="vertical", pos_right="0px", pos_top="60px"
            ),
            yaxis_opts=opts.AxisOpts(min_=y_min, max_=y_max),
            datazoom_opts=opts.DataZoomOpts(range_start=0, range_end=100),
        )
        line_index.set_colors(
            colors=[
                "red",
                "orange",
                "olive",
                "green",
                "blue",
                "purple",
                "black",
                "brown",
                "deeppink",
            ]
        )
        # st
        line_index_st = Line(
            init_opts=opts.InitOpts(
                width="1800px",
                height="860px",
            ),
        )
        line_index_st.add_xaxis(xaxis_data=x_axis)
        y_axis_st = df_index_ssb["ssb_st"].tolist()
        y_min_st = min(y_axis_st)
        y_max_st = max(y_axis_st)
        line_index_st.add_yaxis(
            series_name=f"ssb_st",
            y_axis=y_axis_st,
            is_symbol_show=False,
        )
        line_index_st.set_global_opts(
            title_opts=opts.TitleOpts(
                title="SSB Index ST", pos_top="0%", pos_left="50%"
            ),
            tooltip_opts=opts.TooltipOpts(trigger="axis"),
            toolbox_opts=opts.ToolboxOpts(),
            legend_opts=opts.LegendOpts(
                orient="vertical", pos_right="0px", pos_top="60px"
            ),
            yaxis_opts=opts.AxisOpts(min_=y_min_st, max_=y_max_st),
            datazoom_opts=opts.DataZoomOpts(range_start=0, range_end=100),
        )
        line_index_st.set_colors(
            colors=[
                "red",
            ]
        )
        page = Page(
            page_title="Stock Index",
        )
        page.add(line_index, line_index_st)
        page.render(path=self.filename_index_charts)
        logger.trace(f"{name} End")

    def make(self):
        self.__make_index_line()
        self.__make_charts()

    def stocks_in_ssb(self) -> pd.DataFrame:
        name = "stocks_in_ssb"
        logger.trace(f"{name} Begin")
        with shelve.open(filename=self.filename_shelve, flag="r") as py_dbm:
            try:
                df_mv_50 = py_dbm["df_mv_50"]
                df_mv_300 = py_dbm["df_mv_300"]
                df_mv_500 = py_dbm["df_mv_500"]
                df_mv_1000 = py_dbm["df_mv_1000"]
                df_mv_2000 = py_dbm["df_mv_2000"]
                df_mv_tail = py_dbm["df_mv_tail"]
                df_mv_st = py_dbm["df_mv_st"]
            except KeyError as e:
                logger.error(f"df_mv_n is not exist - {repr(e)}")
                return pd.DataFrame()
        list_all_stocks = self.df_mv.index.tolist()
        random.shuffle(list_all_stocks)
        df_stocks_in_ssb = pd.DataFrame(index=list_all_stocks, columns=["ssb_index"])
        for symbol in df_stocks_in_ssb.index:
            if symbol in df_mv_300.index:
                if symbol in df_mv_50.index:
                    df_stocks_in_ssb.at[symbol, "ssb_index"] = "ssb_50"
                else:
                    df_stocks_in_ssb.at[symbol, "ssb_index"] = "ssb_300"
            elif symbol in df_mv_500.index:
                df_stocks_in_ssb.at[symbol, "ssb_index"] = "ssb_500"
            elif symbol in df_mv_1000.index:
                df_stocks_in_ssb.at[symbol, "ssb_index"] = "ssb_1000"
            elif symbol in df_mv_2000.index:
                df_stocks_in_ssb.at[symbol, "ssb_index"] = "ssb_2000"
            elif symbol in df_mv_tail.index:
                df_stocks_in_ssb.at[symbol, "ssb_index"] = "ssb_tail"
            elif symbol in df_mv_st.index:
                df_stocks_in_ssb.at[symbol, "ssb_index"] = "ssb_st"
            else:
                df_stocks_in_ssb.at[symbol, "ssb_index"] = "NON"
        self.__write_df_from_dbm(df=df_stocks_in_ssb, key="df_stocks_in_ssb")
        logger.trace(f"{name} End")
        return df_stocks_in_ssb

    def realtime_index(self):
        df_stocks_in_ssb = self.__read_df_from_dbm(key="df_stocks_in_ssb")
        if df_stocks_in_ssb.empty:
            logger.error("df_stocks_in_ssb not exist")
            df_stocks_in_ssb = self.stocks_in_ssb()
        df_realtime = analysis.ashare.stock_zh_a_spot_em()[["total_mv"]]  # 调用实时数据接口
        df_mv_all = self.__read_df_from_dbm(key="df_mv_all")
        dict_return = dict()
        if df_mv_all.empty:
            return dict_return
        df_mv_all = df_mv_all[["base_mv", "now_mv"]]
        df_mv_now = pd.concat(
            objs=[
                df_stocks_in_ssb,
                df_realtime,
                df_mv_all,
            ],
            axis=1,
            join="outer",
        )
        df_mv_now.dropna(subset=["ssb_index"], inplace=True)
        df_mv_now.rename(
            columns={
                "total_mv": "now_mv",
                "now_mv": "t1_mv",
            },
            inplace=True,
        )
        df_index_now = pd.pivot_table(
            data=df_mv_now,
            index=["ssb_index"],
            aggfunc=np.sum,
        )
        if "NON" in df_index_now.index:
            df_index_now.drop(index=["NON"], inplace=True)
        df_index_now.at["ssb_all", "base_mv"] = df_index_now["base_mv"].sum()
        df_index_now.at["ssb_all", "now_mv"] = df_index_now["now_mv"].sum()
        df_index_now.at["ssb_all", "t1_mv"] = df_index_now["t1_mv"].sum()
        df_index_now.at["ssb_300", "base_mv"] += df_index_now.at["ssb_50", "base_mv"]
        df_index_now.at["ssb_300", "now_mv"] += df_index_now.at["ssb_50", "now_mv"]
        df_index_now.at["ssb_300", "t1_mv"] += df_index_now.at["ssb_50", "t1_mv"]
        df_index_now.at["ssb_non_st", "base_mv"] = (
            df_index_now.at["ssb_all", "base_mv"] - df_index_now.at["ssb_st", "base_mv"]
        )
        df_index_now.at["ssb_non_st", "now_mv"] = (
            df_index_now.at["ssb_all", "now_mv"] - df_index_now.at["ssb_st", "now_mv"]
        )
        df_index_now.at["ssb_non_st", "t1_mv"] = (
            df_index_now.at["ssb_all", "t1_mv"] - df_index_now.at["ssb_st", "t1_mv"]
        )
        df_index_now["index_now"] = (
            df_index_now["now_mv"] / df_index_now["base_mv"] * 1000
        )
        df_index_now["index_1t"] = (
            df_index_now["t1_mv"] / df_index_now["base_mv"] * 1000
        )
        df_index_now["index_now"] = df_index_now["index_now"].apply(
            func=lambda x: round(x, 2)
        )
        # df_index_now["index_1t"] = df_index_now["index_1t"].apply(func=lambda x: round(x, 2))
        df_index_ssb_min = self.__read_df_from_dbm(key="df_index_ssb_min")
        if df_index_ssb_min.empty:
            logger.error(f"df_index_ssb_min is not exist")
            df_index_ssb_min = pd.DataFrame(columns=df_index_now.index)
        dt_now = datetime.datetime.now()
        df_index_ssb_min.loc[dt_now] = df_index_now["index_now"]
        if len(df_index_ssb_min) < 2:
            df_index_ssb_min = df_index_ssb_min.reindex(
                columns=[
                    "ssb_all",
                    "ssb_non_st",
                    "ssb_50",
                    "ssb_300",
                    "ssb_500",
                    "ssb_1000",
                    "ssb_2000",
                    "ssb_tail",
                    "ssb_st",
                ]
            )
        if dt_am_start < dt_now < dt_am_end or dt_pm_start < dt_now < dt_pm_end:
            self.__write_df_from_dbm(df=df_index_ssb_min, key="df_index_ssb_min")
        if not df_index_ssb_min.empty:
            self.__make_charts_min()
        dict_return = df_index_ssb_min.iloc[-1].to_dict()
        return dict_return

    def __make_charts_min(self):
        name = "make_charts"
        logger.trace(f"{name} Begin")
        df_index_ssb = self.__read_df_from_dbm(key="df_index_ssb")
        df_index_ssb_min = self.__read_df_from_dbm(key="df_index_ssb_min")
        if df_index_ssb.empty:
            logger.error(f"{name} Error End - df_index_ssb empty")
            return
        if df_index_ssb_min.empty:
            logger.error(f"{name} Error End - df_index_ssb_min empty")
            return
        df_index_ssb_daily = df_index_ssb[
            [
                "ssb_all",
                "ssb_non_st",
                "ssb_50",
                "ssb_300",
                "ssb_500",
                "ssb_1000",
                "ssb_2000",
                "ssb_tail",
                "ssb_st",
            ]
        ]
        df_index_ssb_min_today = df_index_ssb_min[
            (df_index_ssb_min.index.year == dt_trading.year)
            & (df_index_ssb_min.index.month == dt_trading.month)
            & (df_index_ssb_min.index.day == dt_trading.day)
        ]
        df_index_ssb_min_today_tail = df_index_ssb_min_today.iloc[-2:]
        df_index_ssb_min_today_head = df_index_ssb_min_today.iloc[:-2]
        if len(df_index_ssb_min_today_head) > 5:
            df_index_ssb_min_today_head = df_index_ssb_min_today_head.sample(n=5)
        df_index_ssb_concat = pd.concat(
            objs=[
                df_index_ssb_daily,
                df_index_ssb_min_today_tail,
                df_index_ssb_min_today_head,
            ],
            axis=0,
            join="outer",
        )
        df_index_ssb_concat = df_index_ssb_concat[
            ~df_index_ssb_concat.index.duplicated()
        ]
        df_index_ssb_concat.sort_index(inplace=True)
        x_axis_concat = df_index_ssb_concat.index.tolist()
        dict_list_index_concat_n = {
            "all": df_index_ssb_concat["ssb_all"].tolist(),
            "non_st": df_index_ssb_concat["ssb_non_st"].tolist(),
            "50": df_index_ssb_concat["ssb_50"].tolist(),
            "300": df_index_ssb_concat["ssb_300"].tolist(),
            "500": df_index_ssb_concat["ssb_500"].tolist(),
            "1000": df_index_ssb_concat["ssb_1000"].tolist(),
            "2000": df_index_ssb_concat["ssb_2000"].tolist(),
            "tail": df_index_ssb_concat["ssb_tail"].tolist(),
        }
        list_values_concat = list()
        for key in dict_list_index_concat_n:
            list_values_concat += dict_list_index_concat_n[key]
        list_values_concat = [x for x in list_values_concat if ~np.isnan(x)]
        y_min_1 = min(list_values_concat)
        y_max_1 = max(list_values_concat)
        line_index_concat = Line(
            init_opts=opts.InitOpts(
                width="1800px",
                height="860px",
            ),
        )
        line_index_concat.add_xaxis(xaxis_data=x_axis_concat)
        for key in dict_list_index_concat_n:
            line_index_concat.add_yaxis(
                series_name=f"ssb_{key}",
                y_axis=dict_list_index_concat_n[key],
                is_symbol_show=False,
            )
        line_index_concat.set_global_opts(
            title_opts=opts.TitleOpts(title="SSB Index", pos_top="0%", pos_left="50%"),
            tooltip_opts=opts.TooltipOpts(trigger="axis"),
            toolbox_opts=opts.ToolboxOpts(),
            legend_opts=opts.LegendOpts(
                orient="vertical", pos_right="0px", pos_top="60px"
            ),
            yaxis_opts=opts.AxisOpts(min_=y_min_1, max_=y_max_1),
            datazoom_opts=opts.DataZoomOpts(range_start=0, range_end=100),
        )
        line_index_concat.set_colors(
            colors=[
                "red",
                "orange",
                "olive",
                "green",
                "blue",
                "purple",
                "black",
                "brown",
                "deeppink",
            ]
        )
        # Minutes
        line_index_ssb_min = Line(
            init_opts=opts.InitOpts(
                width="1800px",
                height="860px",
            ),
        )
        x_axis_min = df_index_ssb_min.index.tolist()
        line_index_ssb_min.add_xaxis(xaxis_data=x_axis_min)
        dict_list_index_min_n = {
            "all": df_index_ssb_min["ssb_all"].tolist(),
            "non_st": df_index_ssb_min["ssb_non_st"].tolist(),
            "50": df_index_ssb_min["ssb_50"].tolist(),
            "300": df_index_ssb_min["ssb_300"].tolist(),
            "500": df_index_ssb_min["ssb_500"].tolist(),
            "1000": df_index_ssb_min["ssb_1000"].tolist(),
            "2000": df_index_ssb_min["ssb_2000"].tolist(),
            "tail": df_index_ssb_min["ssb_tail"].tolist(),
        }
        list_min_values = list()
        for key in dict_list_index_min_n:
            list_min_values += dict_list_index_min_n[key]
        list_min_values = [x for x in list_min_values if ~np.isnan(x)]
        y_min_2 = min(list_min_values)
        y_max_2 = max(list_min_values)
        line_index_min = Line(
            init_opts=opts.InitOpts(
                width="1800px",
                height="860px",
            ),
        )
        line_index_min.add_xaxis(xaxis_data=x_axis_min)
        for key in dict_list_index_min_n:
            line_index_min.add_yaxis(
                series_name=f"ssb_{key}",
                y_axis=dict_list_index_min_n[key],
                is_symbol_show=False,
            )
        line_index_min.set_global_opts(
            title_opts=opts.TitleOpts(title="SSB Index", pos_top="0%", pos_left="50%"),
            tooltip_opts=opts.TooltipOpts(trigger="axis"),
            toolbox_opts=opts.ToolboxOpts(),
            legend_opts=opts.LegendOpts(
                orient="vertical", pos_right="0px", pos_top="60px"
            ),
            yaxis_opts=opts.AxisOpts(min_=y_min_2, max_=y_max_2),
            datazoom_opts=opts.DataZoomOpts(range_start=0, range_end=100),
        )
        line_index_min.set_colors(
            colors=[
                "red",
                "orange",
                "olive",
                "green",
                "blue",
                "purple",
                "black",
                "brown",
                "deeppink",
            ]
        )
        page = Page(
            page_title="Stock Index Minutes",
        )
        page.add(line_index_concat, line_index_min)
        page.render(path=self.filename_index_charts_min)
        logger.trace(f"{name} End")

    def shelve_to_excel(self):
        logger.trace("shelve_to_excel Begin")

        def is_open(filename) -> bool:
            if not os.access(path=filename, mode=os.F_OK):
                logger.error(f"[{filename}] is not exist")
                return False
            else:
                logger.trace(f"[{filename}] is exist")
            try:
                v_handle = win32file.CreateFile(
                    filename,
                    win32file.GENERIC_READ,
                    0,
                    None,
                    win32file.OPEN_EXISTING,
                    win32file.FILE_ATTRIBUTE_NORMAL,
                    None,
                )
            except Exception as e_in:
                print(f"{filename} - {repr(e_in)}")
                logger.trace(f"{filename} - {repr(e_in)}")
                return True
            else:
                v_handle.close()
                logger.trace("close Handle")
                logger.trace(f"[{filename}] not in use")
                return False

        filename_excel = os.path.join(self.path_check, f"chip_SSB.xlsx")
        i_file = 0
        while True:
            if is_open(filename=filename_excel):
                logger.trace(f"[{filename_excel}] is open")
            else:
                logger.trace(f"[{filename_excel}] is not open")
                break
            i_file += 1
            filename_excel = os.path.join(self.path_check, f"chip_SSB_{i_file}.xlsx")
        with shelve.open(filename=self.filename_shelve, flag="r") as py_dbm:
            keys_df = list(py_dbm.keys())
            keys_df = [x for x in keys_df if "df" in x]
            key_random = random.choice(keys_df)
            try:
                writer = pd.ExcelWriter(
                    path=filename_excel, mode="a", if_sheet_exists="replace"
                )
            except FileNotFoundError as e:
                logger.error(f"{repr(e)}")
                with pd.ExcelWriter(path=filename_excel, mode="w") as writer_e:
                    if isinstance(py_dbm[key_random], pd.DataFrame):
                        py_dbm[key_random].to_excel(
                            excel_writer=writer_e, sheet_name=key_random
                        )
                writer = pd.ExcelWriter(
                    path=filename_excel, mode="a", if_sheet_exists="replace"
                )
            count = len(keys_df)
            i = 0
            for key in keys_df:
                i += 1
                str_shelve_to_excel = f"[{i}/{count}] - {key}"
                df_key = py_dbm[key]
                print(f"\r{str_shelve_to_excel}\033[K", end="")
                if key != key_random:
                    if isinstance(df_key, pd.DataFrame):
                        df_key.to_excel(excel_writer=writer, sheet_name=key)
        writer.close()
        if i >= count:
            print("\n", end="")  # 格式处理
        logger.trace("shelve_to_excel End")

    def reset_index_ssb(self):
        self.df_index_exist["index_ssb"] = 0
        self.make()
        self.stocks_in_ssb()
        self.shelve_to_excel()

    def __del__(self):
        print("IndexSSB __del__")
