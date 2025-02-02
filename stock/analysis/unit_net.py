# modified at 2023/05/18 22::25
import sys
import pandas as pd
from loguru import logger
import analysis.base
from analysis.const import filename_chip_shelve, dt_date_trading


def unit_net():
    name: str = f"df_unit_net"
    total_market_value = 0
    df_unit_net = analysis.base.read_df_from_db(key=name, filename=filename_chip_shelve)
    df_trader = analysis.base.read_df_from_db(
        key="df_trader", filename=filename_chip_shelve
    )
    if df_trader.empty:
        logger.trace("df_trader empty")
        sys.exit()
    for code in df_trader.index:
        if pd.notnull(df_trader.at[code, "position"]):
            total_market_value += (
                df_trader.at[code, "position"] * df_trader.at[code, "now_price"]
            )
    df_unit_net.at[dt_date_trading, "total_market_value"] = total_market_value
    print(df_unit_net.tail(5))
    analysis.base.write_obj_to_db(
        obj=df_unit_net,
        key=name,
        filename=filename_chip_shelve,
    )
