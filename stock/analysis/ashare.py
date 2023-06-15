# -*- coding:utf-8 -*-
# modified date 2023/02/17 09:38
from __future__ import annotations
import re
import random
from pprint import pprint
import requests
import datetime
import pandas as pd
from loguru import logger
from fake_useragent import UserAgent


ua = UserAgent()


headers = {
    "Accept": "*/*",
    "Accept-Encoding": "gzip, deflate, sdch",
    "User-Agent": ua.random,
}


def _get_stock_type(stock_code: str):
    """判断股票ID对应的证券市场
    匹配规则
    ['50', '51', '60', '90', '110'] 为 sh
    ['00', '13', '18', '15', '16', '18', '20', '30', '39', '115'] 为 sz
    ['5', '6', '9'] 开头的为 sh， 其余为 sz
    :param stock_code:股票ID, 若以 'sz', 'sh' 开头直接返回对应类型，否则使用内置规则判断
    :return 'sh' or 'sz'
    """
    assert type(stock_code) is str, "stock code need str type"
    sh_head = (
        "50",
        "51",
        "60",
        "90",
        "110",
        "113",
        "118",
        "132",
        "204",
        "5",
        "6",
        "9",
        "7",
    )
    bj_head = (
        "430",
        "83",
        "87",
    )
    if stock_code.startswith(("sh", "sz", "zz")):
        return stock_code[:2]
    else:
        if stock_code.startswith(sh_head):
            return "sh"
        elif stock_code.startswith(bj_head):
            return "bj"
        else:
            return "sz"


def get_history_n_sina(
    symbol: str, frequency: str = "1d", count: int = 10
) -> pd.DataFrame:
    """
    http://money.finance.sina.com.cn/quotes_service/api/json_v2.php/CN_MarketData.getKLineData?symbol=sh600519&scale=5&ma=5&datalen=1
    :param symbol: eg."sh600519"
    :param count: default "1d",options["5m", "15m", "30m", "60m", "1d", "1w", "1M"]
    :param frequency: default 10
    :return: DataFrame
    """
    if frequency not in ["5m", "15m", "30m", "60m", "1d", "1w", "1M"]:
        frequency = "1d"
    frequency = (
        frequency.replace("1d", "240m").replace("1w", "1200m").replace("1M", "7200m")
    )
    frequency = frequency[:-1]
    if frequency.isdigit():
        frequency = int(frequency)
    else:
        frequency = 240
    url_sina = f"http://money.finance.sina.com.cn/quotes_service/api/json_v2.php/CN_MarketData.getKLineData?symbol={symbol}&scale={frequency}&ma=5&datalen={count}"
    rs = requests.get(url_sina)
    data = rs.json()
    df_sina = pd.DataFrame(
        data, columns=["day", "open", "high", "low", "close", "volume"]
    )
    df_sina.rename(columns={"day": "datetime"}, inplace=True)
    df_sina.datetime = pd.to_datetime(df_sina.datetime)
    df_sina.set_index(keys=["datetime"], inplace=True)
    # df_sina.index.rename(name="datetime", inplace=True)  # 索引改名
    # df_sina.index.name = ""  # 索引改名
    df_sina = df_sina.applymap(func=float)
    df_sina = df_sina.reindex(
        labels=["open", "close", "high", "low", "volume"], axis=1
    )  # 重新排序所有列
    df_sina["volume"] = df_sina["volume"].apply(func=lambda x: round(x, -2) // 100)
    return df_sina


def get_history_n_tx(
    symbol: str, frequency: str = "1d", count: int = 10
) -> pd.DataFrame:
    """
    https://web.ifzq.gtimg.cn/appstock/app/fqkline/get?param=sh600519,day,2022-09-01,2022-10-10,10,qfq
    :param symbol: symbol: eg."sh600519"
    :param count: default 10
    :param frequency: default "1d",options["1d", "1w", "1M"]
    :return: DataFrame
    """
    if frequency not in ["1d", "1w", "1M"]:
        frequency = "1d"
    frq_map = {
        "1d": "day",
        "1w": "week",
        "1M": "month",
    }
    frequency = frq_map.get(frequency, "day")
    url_qq = f"http://web.ifzq.gtimg.cn/appstock/app/fqkline/get?param={symbol},{frequency},,,{count},qfq"
    rs = requests.get(url=url_qq, headers=headers)
    data = rs.json()
    data = data["data"][symbol]
    adjust = "qfq" + frequency
    data = data[adjust] if adjust in data else data[frequency]  # 指数返回不是qfqday,是day
    df_qq = pd.DataFrame(data)  # ["time", "open", "close", "high", "low", "volume"]
    df_qq = df_qq.iloc[:, 0:6]  # 用序号取第0到第5列的切片
    df_qq.rename(
        columns={
            0: "datetime",
            1: "open",
            2: "close",
            3: "high",
            4: "low",
            5: "volume",
        },
        inplace=True,
    )
    # df_qq.columns = ["time", "open", "close", "high", "low", "volume"]
    df_qq.datetime = pd.to_datetime(df_qq.datetime)
    df_qq.set_index(keys=["datetime"], inplace=True)
    # df_qq.index.rename(name="datetime", inplace=True)  # 索引改名
    # df_qq.index.name = ""  # 索引改名
    df_qq = df_qq.round(2)
    return df_qq


def get_history_n_min_tx(
    symbol: str, frequency: str = "5m", count: int = 10
) -> pd.DataFrame:  # 分钟线获取
    """
    http://ifzq.gtimg.cn/appstock/app/kline/mkline?param=sh600519,m1,,10
    :param symbol:
    :param count:
    :param frequency: default "5m",options["1m", "5m", "15m","30m", "60m"]
    :return: DataFrame
    """
    if frequency not in ["1m", "5m", "15m", "30m", "60m"]:
        frequency = "5m"
    frq_map = {
        "1m": "m1",
        "5m": "m5",
        "15m": "m15",
        "30m": "m30",
        "60m": "m60",
    }
    frequency = frq_map.get(frequency, "5m")
    url_qq = f"http://ifzq.gtimg.cn/appstock/app/kline/mkline?param={symbol},{frequency},,{count}"
    try:
        rs = requests.get(url=url_qq, headers=headers)
        data = rs.json()
        data = data["data"][symbol][frequency]
    except TypeError as e:
        logger.error(f"[{symbol}] - {repr(e)}")
        return pd.DataFrame()
    df_qq = pd.DataFrame(
        data=data,
        columns=["datetime", "open", "close", "high", "low", "volume", "n1", "n2"],
    )
    df_qq = df_qq.iloc[:, 0:6]  # 用序号取第0到第5列的切片
    if df_qq.empty:
        return pd.DataFrame()  # return null DataFrame without data
    df_qq["datetime"] = pd.to_datetime(df_qq["datetime"])
    df_qq.set_index(["datetime"], inplace=True)
    df_qq = df_qq.applymap(func=float)
    return df_qq


def stock_zh_a_spot_em(stock_codes: str | list | None = None) -> pd.DataFrame:
    """
    东方财富网-沪深京 A 股-实时行情
    http://82.push2.eastmoney.com/api/qt/clist/get
    :return: 实时行情
    :rtype: pandas.DataFrame
    """
    url = "http://82.push2.eastmoney.com/api/qt/clist/get"
    params = {
        "pn": "1",
        "pz": "50000",
        "po": "1",
        "np": "1",
        "ut": "bd1d9ddb04089700cf9c27f6f7426281",
        "fltt": "2",
        "invt": "2",
        "fid": "f3",
        "fs": "m:0 t:6,m:0 t:80,m:1 t:2,m:1 t:23,m:0 t:81 s:2048",
        "fields": "f1,f2,f3,f4,f5,f6,f7,f8,f9,f10,f12,f13,f14,f15,f16,f17,f18,f20,f21,f23,f24,f25,f22,f11,f62,f128,f136,f115,f152",
        "_": "1623833739532",
    }
    r = requests.get(url, params=params, headers=headers)
    data_json = r.json()
    dt = datetime.datetime.now()
    if not data_json["data"]["diff"]:
        return pd.DataFrame()
    temp_df = pd.DataFrame(data_json["data"]["diff"])
    temp_df.columns = [
        "_",
        "close",
        "pct_chg",
        "change",
        "volume",
        "amount",
        "amplitude",
        "turnover",
        "pe_ttm",
        "volume_ratio",
        "5分钟涨跌",
        "code",
        "_",
        "name",
        "high",
        "low",
        "open",
        "pre_close",
        "total_mv",
        "circ_mv",
        "涨速",
        "PB",
        "60日涨跌幅",
        "年初至今涨跌幅",
        "-",
        "-",
        "-",
        "-",
        "-",
        "-",
        "-",
    ]
    temp_df["datetime"] = dt
    temp_df = temp_df[
        [
            "code",
            "name",
            "close",
            "pct_chg",
            "change",
            "volume",
            "amount",
            "datetime",
            "amplitude",
            "high",
            "low",
            "open",
            "pre_close",
            "volume_ratio",
            "turnover",
            "pe_ttm",
            "PB",
            "total_mv",
            "circ_mv",
        ]
    ]
    temp_df["code"] = temp_df["code"].apply(func=str)
    temp_df["code"] = temp_df["code"].apply(func=lambda x: _get_stock_type(x) + x)

    temp_df["close"] = pd.to_numeric(temp_df["close"], errors="coerce")
    temp_df["pct_chg"] = pd.to_numeric(temp_df["pct_chg"], errors="coerce")
    temp_df["change"] = pd.to_numeric(temp_df["change"], errors="coerce")
    temp_df["volume"] = pd.to_numeric(temp_df["volume"], errors="coerce")
    temp_df["volume"] = temp_df["volume"].apply(func=lambda x: x * 100)
    temp_df["amount"] = pd.to_numeric(temp_df["amount"], errors="coerce")
    temp_df["amplitude"] = pd.to_numeric(temp_df["amplitude"], errors="coerce")
    temp_df["high"] = pd.to_numeric(temp_df["high"], errors="coerce")
    temp_df["low"] = pd.to_numeric(temp_df["low"], errors="coerce")
    temp_df["open"] = pd.to_numeric(temp_df["open"], errors="coerce")
    temp_df["pre_close"] = pd.to_numeric(temp_df["pre_close"], errors="coerce")
    temp_df["volume_ratio"] = pd.to_numeric(temp_df["volume_ratio"], errors="coerce")
    temp_df["turnover"] = pd.to_numeric(temp_df["turnover"], errors="coerce")
    temp_df["pe_ttm"] = pd.to_numeric(temp_df["pe_ttm"], errors="coerce")
    temp_df["PB"] = pd.to_numeric(temp_df["PB"], errors="coerce")
    temp_df["total_mv"] = pd.to_numeric(temp_df["total_mv"], errors="coerce")
    temp_df["total_mv"] = temp_df["total_mv"].apply(
        func=lambda x: round(x / 100000000, 2)
    )
    temp_df["circ_mv"] = pd.to_numeric(temp_df["circ_mv"], errors="coerce")
    temp_df["circ_mv"] = temp_df["circ_mv"].apply(
        func=lambda x: round(x / 100000000, 2)
    )
    temp_df.set_index(keys="code", inplace=True)
    temp_df.fillna(value=0.0, inplace=True)
    for tup_data in temp_df.itertuples():
        if tup_data.close == 0.0:
            temp_df.at[tup_data.Index, "close"] = tup_data.pre_close
    if stock_codes is None:
        return temp_df
    if not isinstance(stock_codes, list):
        stock_codes = [stock_codes]
    df_em = pd.DataFrame(columns=temp_df.columns)
    for stock in stock_codes:
        df_em.loc[stock] = temp_df.loc[stock]
    df_em.index.rename(name="code", inplace=True)
    return df_em


def stock_zh_a_spot_qq(stock_codes: str | list) -> pd.DataFrame:
    """
    http://qt.gtimg.cn/q=sh600519,sz002621
    :return:
    """
    grep_stock_code = re.compile(r"(?<=_)\w+")
    if not isinstance(stock_codes, list):
        stock_codes = [stock_codes]
    str_stocks = ",".join(stock_codes)
    url = "http://qt.gtimg.cn/q="
    url = url + str_stocks
    rs = requests.get(url=url, headers=headers)
    rep_data = rs.text
    stocks_detail = "".join(rep_data)
    stock_details = stocks_detail.split(";")
    stock_dict = dict()
    for stock_detail in stock_details:
        stock = stock_detail.split("~")
        if len(stock) <= 49:
            continue
        stock_code = grep_stock_code.search(stock[0]).group()
        stock_dict[stock_code] = {
            "name": stock[1],
            "code": stock_code,
            "close": float(stock[3]),
            "pre_close": float(stock[4]),
            "open": float(stock[5]),
            "volume": float(stock[6]) * 100,
            "bid_volume": int(stock[7]) * 100,
            "ask_volume": float(stock[8]) * 100,
            "bid1": float(stock[9]),
            "bid1_volume": int(stock[10]) * 100,
            "bid2": float(stock[11]),
            "bid2_volume": int(stock[12]) * 100,
            "bid3": float(stock[13]),
            "bid3_volume": int(stock[14]) * 100,
            "bid4": float(stock[15]),
            "bid4_volume": int(stock[16]) * 100,
            "bid5": float(stock[17]),
            "bid5_volume": int(stock[18]) * 100,
            "ask1": float(stock[19]),
            "ask1_volume": int(stock[20]) * 100,
            "ask2": float(stock[21]),
            "ask2_volume": int(stock[22]) * 100,
            "ask3": float(stock[23]),
            "ask3_volume": int(stock[24]) * 100,
            "ask4": float(stock[25]),
            "ask4_volume": int(stock[26]) * 100,
            "ask5": float(stock[27]),
            "ask5_volume": int(stock[28]) * 100,
            "tick": stock[29],
            "datetime": datetime.datetime.strptime(stock[30], "%Y%m%d%H%M%S"),
            "change": float(stock[31]),
            "pct_chg": float(stock[32]),
            "high": float(stock[33]),
            "low": float(stock[34]),
            "price/volume/amount": stock[35],
            "volume_2": int(stock[36]) * 100,
            "amount": float(stock[37]) * 10000,
            "turnover": float(stock[38]),
            "pe_ttm": float(stock[39]),
            "unknown": stock[40],
            "high_2": float(stock[41]),  # 意义不明
            "low_2": float(stock[42]),  # 意义不明
            "amplitude": float(stock[43]),
            "circ_mv": float(stock[44]),
            "total_mv": float(stock[45]),
            "PB": float(stock[46]),
            "up_limit": float(stock[47]),
            "down_limit": float(stock[48]),
            "volume_ratio": float(stock[49]),
            "weicha": float(stock[50]),
            "ma": float(stock[51]),
            "unknown_2": float(stock[52]),
            "pe": float(stock[53]),
        }
    df_temp = pd.DataFrame(stock_dict).transpose()
    df_temp = df_temp[
        [
            "code",
            "name",
            "close",
            "pct_chg",
            "change",
            "volume",
            "amount",
            "datetime",
            "amplitude",
            "high",
            "low",
            "open",
            "pre_close",
            "volume_ratio",
            "turnover",
            "pe_ttm",
            "PB",
            "total_mv",
            "circ_mv",
        ]
    ]
    df_temp.set_index(keys="code", inplace=True)
    return df_temp


def history_n(symbol: str, frequency: str = "1d", count: int = 10) -> pd.DataFrame:
    source = random.choice(["sina", "tencent"])  # 随机选择数据源，防ban
    logger.trace(f"choice source {source}")
    if frequency in ["1d", "1w", "1M"]:  # 1d日线  1w周线  1M月线
        if source == "sina":
            return get_history_n_sina(symbol=symbol, frequency=frequency, count=count)
        elif source == "tencent":
            return get_history_n_tx(symbol=symbol, frequency=frequency, count=count)
        else:
            return pd.DataFrame()
    elif frequency in ["5m", "15m", "30m", "60m"]:
        if source == "sina":
            return get_history_n_sina(symbol=symbol, frequency=frequency, count=count)
        elif source == "tencent":
            return get_history_n_min_tx(symbol=symbol, frequency=frequency, count=count)
    elif frequency in ["1m"]:
        return get_history_n_min_tx(symbol=symbol, frequency=frequency, count=count)
    else:
        return pd.DataFrame()


def realtime_quotations(stock_codes: str | list) -> pd.DataFrame:
    """
    :param stock_codes: 'sh600519'
    :return:
    """
    if not isinstance(stock_codes, list):
        list_stock_codes = [stock_codes]
    else:
        list_stock_codes = stock_codes.copy()
        random.shuffle(list_stock_codes)  # 打乱list顺序，防ban
    pattern_stock = re.compile(r"\d+")
    count = len(list_stock_codes)
    i = 0
    while i < count:
        symbol = pattern_stock.search(list_stock_codes[i]).group()
        if len(symbol) == 6:
            list_stock_codes[i] = _get_stock_type(symbol) + symbol
            i += 1
        else:
            logger.error(f"remove {list_stock_codes[i]}")
            list_stock_codes.remove(list_stock_codes[i])
            count -= 1
    list_stock_codes = list(set(list_stock_codes))
    source = random.choice(("em", "qq"))  # 随机选择数据源，防ban
    logger.trace(f"choice source {source}")
    if source == "em":
        return stock_zh_a_spot_em(stock_codes=list_stock_codes)
    elif source == "qq":
        return stock_zh_a_spot_qq(stock_codes=list_stock_codes)
    else:
        logger.error(f"realtime_quotations return None")
        return pd.DataFrame()
