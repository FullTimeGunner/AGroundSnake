# modified at 2023/05/18 22::25
import os
import sys
import time
import requests
import pandas as pd
import feather
from loguru import logger
import akshare as ak
import analysis.base
import analysis.ashare
from analysis.const import (
    path_data,
    path_index,
    str_date_path,
    dt_init,
    dt_pm_end,
    filename_chip_shelve,
    list_all_stocks,
)


def update_stock_data(frequency: str = "1m") -> bool:
    name: str = f"update_kline_{frequency}"
    logger.trace(f"[{frequency}] A Share Data Update Begin")
    if analysis.base.is_latest_version(key=name, filename=filename_chip_shelve):
        logger.trace(f"update stock Kline Break and End")
        return True
    start_loop_time = time.perf_counter_ns()
    path_kline = os.path.join(path_data, f"kline_{frequency}")
    if not os.path.exists(path_kline):
        os.mkdir(path_kline)
    file_name_catalogue_temp = os.path.join(
        path_data, f"catalogue_temp_{str_date_path()}.ftr"
    )
    if os.path.exists(file_name_catalogue_temp):
        # 读取腌制数据 catalogue
        df_catalogue = feather.read_dataframe(source=file_name_catalogue_temp)
    else:
        df_catalogue = pd.DataFrame(index=list_all_stocks, columns=["start", "end", "count"])
        df_catalogue["start"].fillna(value=dt_init, inplace=True)
        df_catalogue["end"].fillna(value=dt_init, inplace=True)
        df_catalogue["count"].fillna(value=0, inplace=True)
    df_catalogue = df_catalogue.sample(frac=1)
    count = len(df_catalogue)
    quantity = 80000
    i = 0
    for symbol in df_catalogue.index:
        feather.write_dataframe(df=df_catalogue, dest=file_name_catalogue_temp)
        i += 1
        str_msg = f"Kline Update: [{i:4d}/{count:4d}] -- [{symbol}]"
        if df_catalogue.at[symbol, "end"] == dt_pm_end:
            print(f"\r{str_msg} - [{df_catalogue.at[symbol, 'end']}] - Latest\033[K", end="")
            continue
        df_data = pd.DataFrame()
        file_name_feather = os.path.join(path_kline, f"{symbol}.ftr")
        if os.path.exists(file_name_feather):
            df_data = feather.read_dataframe(source=file_name_feather)
        if not df_data.empty:
            df_catalogue.loc[symbol, "end"] = df_data.index.max()
            df_catalogue.loc[symbol, "start"] = df_data.index.min()
            df_catalogue.loc[symbol, "count"] = len(df_data)
            if df_catalogue.at[symbol, "end"] == dt_pm_end:
                print(f"\r{str_msg} - [{df_catalogue.loc[symbol, 'end']}] - Latest\033[K", end="")
                continue
        df_delta = pd.DataFrame()
        i_while_delta = 0
        while i_while_delta <= 1:
            i_while_delta += 1
            try:
                df_delta = analysis.ashare.get_history_n_min_tx(
                    symbol=symbol, frequency=frequency, count=quantity
                )
            except requests.exceptions.Timeout as e:
                print(f"\r{str_msg} - [{i_while_delta}] - {repr(e)}\033[K")
                time.sleep(1)
            else:
                if df_delta.empty:
                    print(
                        f"\r{str_msg} - [Times:{i_while_delta}] - df_delta empty\033[K"
                    )
                    time.sleep(0.5)
                else:
                    break
        if df_delta.empty:
            if df_data.empty:
                print(f"\r{str_msg} - [{df_catalogue.loc[symbol, 'end']}] - No data\033[K")
            else:
                print(f"\r{str_msg} - [{df_catalogue.loc[symbol, 'end']}] - Suspension\033[K")
            continue
        else:
            if df_data.empty:
                df_data = df_delta.copy()
            else:
                df_data = pd.concat([df_data, df_delta], axis=0, join="outer")
                df_data = df_data[~df_data.index.duplicated(keep="last")]
                df_data.sort_values(by=["datetime"], ascending=True, inplace=True)
        feather.write_dataframe(df=df_data, dest=file_name_feather)
        dt_data_max = df_data.index.max()
        if dt_data_max > dt_pm_end:
            dt_data_max = dt_pm_end
        df_catalogue.loc[symbol, "end"] = dt_data_max
        df_catalogue.loc[symbol, "start"] = df_data.index.min()
        df_catalogue.loc[symbol, "count"] = len(df_data)
        print(f"\r{str_msg} - [{df_catalogue.loc[symbol, 'end']}]- Update\033[K", end="")
    if i >= count:
        print("\n", end="")
        df_catalogue.sort_values(by=["count"], ascending=False, inplace=True)
        analysis.base.write_obj_to_db(
            obj=df_catalogue, key="df_catalogue", filename=filename_chip_shelve
        )
        analysis.base.set_version(key=name, dt=df_catalogue["end"].max())
        if os.path.exists(file_name_catalogue_temp):
            os.remove(path=file_name_catalogue_temp)
    end_loop_time = time.perf_counter_ns()
    interval_time = (end_loop_time - start_loop_time) / 1000000000
    str_gm = time.strftime("%H:%M:%S", time.gmtime(interval_time))
    print(f"[{frequency}] A Share Data Update takes {str_gm}")
    logger.trace(f"[{frequency}] A Share Data Update End")
    return True


def update_index_data(
    symbol: str = "sh000001", period: str = "1", adjust: str = ""
) -> pd.DataFrame:
    if symbol == "sh000001":
        name = "index_1kline_sh"
    elif symbol == "sh000852":
        name = "index_1kline_csi1000"
    else:
        name = "index_1kline_other"
    logger.trace(f"[{symbol}] update_index_data Begin")
    file_name_index_feather = os.path.join(path_index, f"{symbol}.ftr")
    if analysis.base.is_latest_version(key=name, filename=filename_chip_shelve):
        df_index = feather.read_dataframe(source=file_name_index_feather)
        logger.trace(f"[{symbol}] update_index_data Break and End")
        return df_index
    if os.path.exists(file_name_index_feather):
        # 读取腌制数据 catalogue
        df_index = feather.read_dataframe(source=file_name_index_feather)
        logger.trace(f"Load index[{symbol}] feather from [{file_name_index_feather}]")
        df_index_delta = ak.stock_zh_a_minute(
            symbol=symbol, period=period, adjust=adjust
        )
        df_index_delta["day"] = pd.to_datetime(df_index_delta["day"])
        df_index_delta.set_index(keys=["day"], inplace=True)
        df_index_delta = df_index_delta.astype(float)
        df_index = pd.concat(objs=[df_index, df_index_delta], axis=0, join="outer")
        df_index = df_index[~df_index.index.duplicated(keep="last")]
    else:
        df_index = ak.stock_zh_a_minute(symbol=symbol, period=period, adjust=adjust)
        df_index["day"] = pd.to_datetime(df_index["day"])
        df_index.set_index(keys=["day"], inplace=True)
        df_index = df_index.astype(float)
        logger.trace(f"Create and feather index[{symbol}]")
    df_index.sort_index(inplace=True)
    if not df_index.empty:
        feather.write_dataframe(df=df_index, dest=file_name_index_feather)
    dt_update_index_data = df_index.index.max()
    analysis.base.set_version(key=name, dt=dt_update_index_data)
    logger.trace(f"[{symbol}] update_index_data End")
    return df_index
