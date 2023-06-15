# modified at 2023/05/18 22::25
from __future__ import annotations
import os
import time
import random
import sys
import datetime
import shelve
import dbm
import win32file
import pandas as pd
from console import fg
from pandas import DataFrame
import tushare as ts
from loguru import logger
from analysis.const import (
    dt_am_0910,
    dt_pm_end,
    dt_pm_end_last_1T,
    filename_chip_shelve,
)


def is_trading_day(dt: datetime.datetime = None) -> bool:
    ts.set_token("77f61903681b936f371c34d8abf7603a324ed90d070e4eb6992d0832")
    pro = ts.pro_api()
    if dt is None:
        dt = datetime.datetime.now()
    dt_start = dt - datetime.timedelta(days=14)
    str_date_start = dt_start.strftime("%Y%m%d")
    str_date_now = dt.strftime("%Y%m%d")
    try:
        df_trade = pro.trade_cal(
            exchange="", start_date=str_date_start, end_date=str_date_now
        )
    except Exception as e:
        print(
            f"The token is invalid. Please apply for a token at tushare - Error-[{e}]"
        )
        sys.exit()
    df_trade.set_index(keys=["cal_date"], inplace=True)
    try:
        if df_trade.at[str_date_now, "is_open"] == 1:
            return True
        else:
            return False
    except KeyError as e:
        print(repr(e))
        sys.exit()


def code_ths_to_ts(symbol: str):
    return symbol[2:] + "." + symbol[0:2].upper()


def code_ts_to_ths(ts_code: str):
    return ts_code[-2:].lower() + ts_code[:6]


def get_stock_type_in(code_in: str):
    if code_in[:2].lower() == "sh":
        return "sh"
    elif code_in[:2].lower() == "sz":
        return "sh"
    elif code_in[:2].lower() == "bj":
        return "bj"
    elif code_in[-2:].lower() == "sh":
        return "sh"
    elif code_in[-2:].lower() == "sz":
        return "sz"
    elif code_in[-2:].lower() == "bj":
        return "bj"


def transaction_unit(price: float, amount: float = 1000) -> int:
    if price * 100 > amount:
        return 100
    unit_temp = amount / price
    unit_small = int(unit_temp // 100 * 100)
    unit_big = unit_small + 100
    differ_big = unit_big * price - amount
    differ_small = amount - unit_small * price
    if differ_big < differ_small:
        return unit_big
    else:
        return unit_small


def zeroing_sort(pd_series: pd.Series) -> pd.Series:  # 归零化排序
    min_unit = pd_series.min()
    pd_series_out = pd_series.apply(func=lambda x: (x / min_unit - 1).round(4))
    return pd_series_out


def write_obj_to_db(obj: object, key: str, filename: str):
    with shelve.open(filename=filename, flag="c") as py_dbm_chip:
        py_dbm_chip[key] = obj
        logger.trace(f"{key} save as pydb_chip-[{filename}]")
    return True


def sleep_to_time(dt_time: datetime.datetime, seconds: int = 1):
    dt_now_sleep = datetime.datetime.now()
    while dt_now_sleep <= dt_time:
        int_delay = int((dt_time - dt_now_sleep).total_seconds())
        str_sleep_gm = time.strftime("%H:%M:%S", time.gmtime(int_delay))
        str_sleep_msg = f"Waiting: {str_sleep_gm}"
        str_sleep_msg = fg.cyan(str_sleep_msg)
        str_dt_now_sleep = dt_now_sleep.strftime("<%H:%M:%S>")
        str_sleep_msg = f"{str_dt_now_sleep}----" + str_sleep_msg
        print(f"\r{str_sleep_msg}\033[K", end="")  # 进度条
        time.sleep(seconds)
        dt_now_sleep = datetime.datetime.now()
    print("\n", end="")
    return True


def read_df_from_db(key: str, filename: str) -> DataFrame:
    try:
        with shelve.open(filename=filename, flag="r") as py_dbm_chip:
            logger.trace(f"loading {key} from [{filename}]....")
            try:
                df = py_dbm_chip[key]
                isinstance(df, DataFrame)
                return df
            except KeyError as e:
                print(f"[{key}] is not exist -Error[{repr(e)}]")
                logger.trace(f"[{key}] is not exist - Error[{repr(e)}]")
                return pd.DataFrame()
    except dbm.error as e:
        print(f"[{filename}-{key}] is not exist - Error[{repr(e)}]")
        logger.trace(f"[{filename}-{key}] is not exist - Error[{repr(e)}]")
        return pd.DataFrame()


def is_latest_version(key: str, filename: str) -> bool:
    dt_now = datetime.datetime.now()
    df_config = read_df_from_db(key="df_config", filename=filename)
    if df_config.empty:
        return False
    if key not in df_config.index:
        return False
    dt_latest = df_config.at[key, "date"]
    if not isinstance(dt_latest, datetime.date):
        return False
    if dt_latest == dt_pm_end:
        return True
    else:
        if dt_am_0910 < dt_now < dt_pm_end:
            return True
        elif dt_pm_end_last_1T < dt_now < dt_am_0910:
            if dt_latest == dt_pm_end_last_1T:
                return True
            else:
                return False


def set_version(key: str, dt: datetime.datetime) -> bool:
    df_config = read_df_from_db(key="df_config", filename=filename_chip_shelve)
    df_config.at[key, "date"] = dt
    df_config.sort_values(by="date", ascending=False, inplace=True)
    write_obj_to_db(obj=df_config, key="df_config", filename=filename_chip_shelve)
    return True


def is_exist(date_index: datetime.date, columns: str, filename: str) -> bool:
    df_date_exist = read_df_from_db(key="df_index_exist", filename=filename)
    try:
        if df_date_exist.at[date_index, columns] == 1:
            return True
        else:
            return False
    except KeyError:
        return False


def set_exist(date_index: datetime.date, columns: str, filename: str) -> bool:
    df_date_exist = read_df_from_db(key="df_index_exist", filename=filename)
    df_date_exist.at[date_index, columns] = 1
    write_obj_to_db(obj=df_date_exist, key="df_index_exist", filename=filename)
    return True


def shelve_to_excel(filename_shelve: str, filename_excel: str):
    def is_open(filename) -> bool:
        if not os.access(path=filename, mode=os.F_OK):
            logger.trace(f"[{filename}] is not exist")
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

    i_file = 0
    filename_excel_new = filename_excel
    while True:
        i_file += 1
        if is_open(filename=filename_excel_new):
            logger.trace(f"[{filename_excel_new}] is open")
        else:
            logger.trace(f"[{filename_excel_new}] is not open")
            break
        path, ext = os.path.splitext(filename_excel)
        path += f"_{i_file}"
        filename_excel_new = path + ext
    try:
        logger.trace(f"try open [{filename_shelve}]")
        with shelve.open(filename=filename_shelve, flag="r") as py_dbm_chip:
            key_random = ""
            try:
                writer = pd.ExcelWriter(
                    path=filename_excel, mode="a", if_sheet_exists="replace"
                )
            except FileNotFoundError as e:
                logger.trace(f"{filename_excel} is not exist -Error[{repr(e)}]")
                with pd.ExcelWriter(path=filename_excel, mode="w") as writer_e:
                    key_random = random.choice(list(py_dbm_chip.keys()))
                    if isinstance(py_dbm_chip[key_random], DataFrame):
                        py_dbm_chip[key_random].to_excel(
                            excel_writer=writer_e, sheet_name=key_random
                        )
                    else:
                        logger.trace(f"{key_random} is not DataFrame")
                writer = pd.ExcelWriter(
                    path=filename_excel, mode="a", if_sheet_exists="replace"
                )
                logger.trace(f"create file-[{filename_excel}]")
            count = len(py_dbm_chip)
            i = 0
            for key in py_dbm_chip:
                i += 1
                str_shelve_to_excel = f"[{i}/{count}] - {key}"
                print(f"\r{str_shelve_to_excel}\033[K", end="")
                if key != key_random:
                    if isinstance(py_dbm_chip[key], DataFrame):
                        py_dbm_chip[key].to_excel(excel_writer=writer, sheet_name=key)
                    else:
                        logger.trace(f"{key} is not DataFrame")
                        continue
                else:
                    logger.trace(f"{key} is exist")
                    continue
            writer.close()
            if i >= count:
                print("\n", end="")  # 格式处理
                return
    except dbm.error as e:
        print(f"[{filename_shelve}] is not exist - Error[{repr(e)}]")
        logger.trace(f"[{filename_shelve}] is not exist - Error[{repr(e)}]")
