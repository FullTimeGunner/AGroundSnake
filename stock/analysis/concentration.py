# modified at 2023/05/18 22::25
import datetime
import pandas as pd
from pyecharts.charts import Line, Page
import pyecharts.options as opts
from loguru import logger
import analysis.ashare
import analysis.base
from analysis.const import (
    dt_init,
    time_pm_end,
    dt_am_1015,
    dt_am_end,
    dt_pm_start,
    dt_pm_end,
    dt_pm_end_last_1T,
    list_all_stocks,
    filename_chip_shelve,
    filename_concentration_rate_charts,
)


def concentration_rate() -> tuple:
    name: str = f"df_concentration_rate"
    df_all = analysis.ashare.stock_zh_a_spot_em()  # 调用实时数据接口
    df_amount_sort = df_all.sort_values(by=["amount"], ascending=False)
    df_mv_sort = df_all.sort_values(by=["total_mv"], ascending=False)
    top5_stocks = int(round(len(df_all) * 0.05, 0))

    df_amount_sort_top5 = df_amount_sort.iloc[:top5_stocks]
    df_amount_sort_tail95 = df_amount_sort.iloc[top5_stocks:]
    df_mv_sort_top5 = df_mv_sort.iloc[:top5_stocks]

    amount_all = df_all["amount"].sum() / 100000000
    mv_all = df_all["total_mv"].sum() / 100000000
    mean_all = df_all["total_mv"].mean().round(2)

    amount_amount_sort_top5 = df_amount_sort_top5["amount"].sum() / 100000000
    mv_amount_sort_top5 = df_amount_sort_top5["total_mv"].sum() / 100000000
    mean_amount_sort_top5 = df_amount_sort_top5["total_mv"].mean().round(2)

    amount_mv_sort_top5 = df_mv_sort_top5["amount"].sum() / 100000000
    mv_mv_sort_top5 = df_mv_sort_top5["total_mv"].sum() / 100000000
    mean_mv_sort_top5 = df_mv_sort_top5["total_mv"].mean().round(2)

    turnover_amount_sort_top5 = df_amount_sort_top5["turnover"].mean().round(2)
    amplitude_amount_sort_top5 = df_amount_sort_top5["amplitude"].mean().round(2)
    turnover_amount_sort_tail95 = df_amount_sort_tail95["turnover"].mean().round(2)
    amplitude_amount_sort_tail95 = df_amount_sort_tail95["amplitude"].mean().round(2)

    if amount_all != 0:
        rate_amount_amount_sort_top5 = (
            amount_amount_sort_top5 / amount_all * 100
        ).round(2)
        rate_amount_mv_sort_top5 = (amount_mv_sort_top5 / amount_all * 100).round(2)
    else:
        rate_amount_amount_sort_top5 = 0
        rate_amount_mv_sort_top5 = 0

    if mv_all != 0:
        rate_mv_amount_sort_top5 = (mv_amount_sort_top5 / mv_all * 100).round(2)
        rate_mv_mv_sort_top5 = (mv_mv_sort_top5 / mv_all * 100).round(2)
    else:
        rate_mv_amount_sort_top5 = 0
        rate_mv_mv_sort_top5 = 0
    str_msg_concentration = (
        f"[{round(amount_all, 2)}]"
        f" - Top{top5_stocks} - A/S:[Amount-{rate_amount_amount_sort_top5:.2f}"
        f" - MV-{rate_mv_amount_sort_top5:.2f}]"
        f" - Mean_A/S:[{mean_amount_sort_top5}]"
        f" - Mean_MV/S:[{mean_mv_sort_top5}]"
    )
    str_msg_additional = (
        f"T/O:[{turnover_amount_sort_top5:.2f}/{turnover_amount_sort_tail95:.2f}]"
        f" - AMP:[{amplitude_amount_sort_top5:.2f}/{amplitude_amount_sort_tail95:.2f}]"
        f" - MV/S: [Amount-{rate_amount_mv_sort_top5} - MV-{rate_mv_mv_sort_top5}]"
        f" - Mean:[{mean_all}]"
    )
    tuple_str = (str_msg_concentration, str_msg_additional)
    df_concentration_rate = analysis.base.read_df_from_db(
        key=name, filename=filename_chip_shelve
    )
    dt_now = datetime.datetime.now()
    if dt_am_1015 < dt_now < dt_am_end or dt_pm_start < dt_now < dt_pm_end:
        df_concentration_rate.at[dt_now, "amount_all"] = amount_all
        df_concentration_rate.at[dt_now, "top5_stocks"] = top5_stocks
        df_concentration_rate.at[
            dt_now, "rate_amount_amount_sort_top5"
        ] = rate_amount_amount_sort_top5
        df_concentration_rate.at[
            dt_now, "rate_mv_amount_sort_top5"
        ] = rate_mv_amount_sort_top5
        df_concentration_rate.at[
            dt_now, "mean_amount_sort_top5"
        ] = mean_amount_sort_top5
        df_concentration_rate.at[
            dt_now, "turnover_amount_sort_top5"
        ] = turnover_amount_sort_top5
        df_concentration_rate.at[
            dt_now, "turnover_amount_sort_tail95"
        ] = turnover_amount_sort_tail95
        df_concentration_rate.at[
            dt_now, "amplitude_amount_sort_top5"
        ] = amplitude_amount_sort_top5
        df_concentration_rate.at[
            dt_now, "amplitude_amount_sort_tail95"
        ] = amplitude_amount_sort_tail95
        df_concentration_rate.at[
            dt_now, "rate_amount_mv_sort_top5"
        ] = rate_amount_mv_sort_top5
        df_concentration_rate.at[dt_now, "rate_mv_mv_sort_top5"] = rate_mv_mv_sort_top5
        df_concentration_rate.at[dt_now, "mean_mv_sort_top5"] = mean_mv_sort_top5
        df_concentration_rate.at[dt_now, "mean_all"] = mean_all
        analysis.base.write_obj_to_db(
            obj=df_concentration_rate,
            key=name,
            filename=filename_chip_shelve,
        )
    if not df_concentration_rate.empty:
        x_dt = df_concentration_rate.index.tolist()
        y_rate_amount_by_amount = df_concentration_rate[
            "rate_amount_amount_sort_top5"
        ].tolist()
        y_rate_mv_by_amount = df_concentration_rate["rate_mv_amount_sort_top5"].tolist()
        y_rate_amount_by_mv = df_concentration_rate["rate_amount_mv_sort_top5"].tolist()
        y_rate_mv_by_mv = df_concentration_rate["rate_mv_mv_sort_top5"].tolist()
        y_min = min(
            y_rate_amount_by_amount
            + y_rate_mv_by_amount
            + y_rate_amount_by_mv
            + y_rate_mv_by_mv
        )
        y_max = max(
            y_rate_amount_by_amount
            + y_rate_mv_by_amount
            + y_rate_amount_by_mv
            + y_rate_mv_by_mv
        )
        line_concentration_rate = Line(
            init_opts=opts.InitOpts(
                width="1800px",
                height="860px",
                page_title="Concentration Rate",
            )
        )
        line_concentration_rate.add_xaxis(xaxis_data=x_dt)
        line_concentration_rate.add_yaxis(
            series_name="concentration_rate",
            y_axis=y_rate_amount_by_amount,
            is_symbol_show=False,
            markpoint_opts=opts.MarkPointOpts(
                data=[
                    opts.MarkPointItem(name="最大值", type_="max"),
                    opts.MarkPointItem(name="最小值", type_="min"),
                ]
            ),
        )
        line_concentration_rate.add_yaxis(
            series_name="MV_A/S",
            y_axis=y_rate_mv_by_amount,
            is_symbol_show=False,
            markpoint_opts=opts.MarkPointOpts(
                data=[
                    opts.MarkPointItem(name="最大值", type_="max"),
                    opts.MarkPointItem(name="最小值", type_="min"),
                ]
            ),
        )
        line_concentration_rate.add_yaxis(
            series_name="Amount_MV/S",
            y_axis=y_rate_amount_by_mv,
            is_symbol_show=False,
            markpoint_opts=opts.MarkPointOpts(
                data=[
                    opts.MarkPointItem(name="最大值", type_="max"),
                    opts.MarkPointItem(name="最小值", type_="min"),
                ]
            ),
        )
        line_concentration_rate.add_yaxis(
            series_name="MV_MV/S",
            y_axis=y_rate_mv_by_mv,
            is_symbol_show=False,
            markpoint_opts=opts.MarkPointOpts(
                data=[
                    opts.MarkPointItem(name="最大值", type_="max"),
                    opts.MarkPointItem(name="最小值", type_="min"),
                ]
            ),
        )
        line_concentration_rate.set_colors(
            colors=[
                "red",
                "black",
                "orange",
                "green",
            ]
        )
        line_concentration_rate.set_global_opts(
            title_opts=opts.TitleOpts(title="Concentration Rate", pos_left="center"),
            tooltip_opts=opts.TooltipOpts(trigger="axis"),
            toolbox_opts=opts.ToolboxOpts(),
            legend_opts=opts.LegendOpts(orient="vertical", pos_right=0, pos_top="48%"),
            yaxis_opts=opts.AxisOpts(
                min_=y_min,
                max_=y_max,
            ),
            datazoom_opts=opts.DataZoomOpts(
                range_start=0,
                range_end=100,
            ),
        )
        line_mean = Line(
            init_opts=opts.InitOpts(
                width="1800px",
                height="860px",
                page_title="Mean",
            )
        )
        line_mean.add_xaxis(xaxis_data=x_dt)
        y_mean_amount_sort_top5 = df_concentration_rate[
            "mean_amount_sort_top5"
        ].tolist()
        y_mean_mv_sort_top5 = df_concentration_rate["mean_mv_sort_top5"].tolist()
        y_min = min(y_mean_amount_sort_top5 + y_mean_mv_sort_top5)
        y_max = max(y_mean_amount_sort_top5 + y_mean_mv_sort_top5)
        line_mean.add_yaxis(
            series_name="Mean_A/S",
            y_axis=y_mean_amount_sort_top5,
            is_symbol_show=False,
            markpoint_opts=opts.MarkPointOpts(
                data=[
                    opts.MarkPointItem(name="最大值", type_="max"),
                    opts.MarkPointItem(name="最小值", type_="min"),
                ]
            ),
        )
        line_mean.add_yaxis(
            series_name="Mean_MV/S",
            y_axis=y_mean_mv_sort_top5,
            is_symbol_show=False,
            markpoint_opts=opts.MarkPointOpts(
                data=[
                    opts.MarkPointItem(name="最大值", type_="max"),
                    opts.MarkPointItem(name="最小值", type_="min"),
                ]
            ),
        )
        line_mean.set_colors(
            colors=[
                "red",
                "green",
            ]
        )
        line_mean.set_global_opts(
            title_opts=opts.TitleOpts(title="Mean", pos_left="center"),
            tooltip_opts=opts.TooltipOpts(trigger="axis"),
            toolbox_opts=opts.ToolboxOpts(),
            legend_opts=opts.LegendOpts(orient="vertical", pos_right=0, pos_top="48%"),
            yaxis_opts=opts.AxisOpts(
                min_=y_min,
                max_=y_max,
            ),
            datazoom_opts=opts.DataZoomOpts(
                range_start=0,
                range_end=100,
            ),
        )
        page = Page(
            page_title="concentration",
        )
        page.add(line_concentration_rate, line_mean)
        page.render(path=filename_concentration_rate_charts)
        logger.trace(f"{name} End")
    return tuple_str


def concentration() -> bool:
    name: str = "df_concentration"
    if analysis.base.is_latest_version(key=name, filename=filename_chip_shelve):
        logger.trace(f"{name},Break and End")
        return True
    df_realtime = analysis.ashare.stock_zh_a_spot_em()  # 调用实时数据接口
    df_realtime.sort_values(by=["amount"], ascending=False, inplace=True)
    top5_stocks = int(round(len(list_all_stocks) * 0.05, 0))
    df_realtime_top5 = df_realtime.iloc[:top5_stocks]
    df_concentration_old = analysis.base.read_df_from_db(
        key="df_concentration", filename=filename_chip_shelve
    )
    if df_concentration_old.empty:
        df_concentration = pd.DataFrame(
            index=list_all_stocks,
            columns=[
                "first_concentration",
                "latest_concentration",
                "days_first_concentration",
                "days_latest_concentration",
                "times_concentration",
                "rate_concentration",
            ],
        )
    else:
        df_concentration_empty = pd.DataFrame(index=list_all_stocks)
        df_concentration = pd.concat(
            objs=[df_concentration_old, df_concentration_empty],
            axis=1,
            join="outer",
        )
    df_concentration = df_concentration[
        df_concentration.index.isin(values=list_all_stocks)
    ]
    df_concentration["first_concentration"].fillna(value=dt_init, inplace=True)
    df_concentration["latest_concentration"].fillna(value=dt_init, inplace=True)
    df_concentration["days_first_concentration"].fillna(value=0, inplace=True)
    df_concentration["days_latest_concentration"].fillna(value=0, inplace=True)
    df_concentration["times_concentration"].fillna(value=0, inplace=True)
    df_concentration["rate_concentration"].fillna(value=0, inplace=True)
    dt_now = datetime.datetime.now()
    if dt_pm_end_last_1T < datetime.datetime.now() <= dt_pm_end:
        dt_latest = dt_pm_end_last_1T
    else:
        dt_latest = dt_pm_end
    for symbol in df_concentration.index:
        if symbol in df_realtime_top5.index:
            if df_concentration.at[symbol, "first_concentration"] == dt_init:
                df_concentration.at[
                    symbol, "first_concentration"
                ] = df_concentration.at[symbol, "latest_concentration"] = dt_latest
                df_concentration.at[symbol, "times_concentration"] = 1
            else:
                if df_concentration.at[symbol, "latest_concentration"] != dt_latest:
                    df_concentration.at[symbol, "latest_concentration"] = dt_latest
                    df_concentration.at[symbol, "times_concentration"] += 1
        if df_concentration.at[symbol, "first_concentration"] != dt_init:
            df_concentration.at[
                symbol, "days_first_concentration"
            ] = days_first_concentration = (
                dt_now - df_concentration.at[symbol, "first_concentration"]
            ).days + 1
            df_concentration.at[symbol, "days_latest_concentration"] = (
                dt_now - df_concentration.at[symbol, "latest_concentration"]
            ).days + 1
            # 修正除数，尽可能趋近交易日
            days_first_concentration = (
                days_first_concentration // 7 * 5 + days_first_concentration % 7
            )
            if days_first_concentration > 0:
                df_concentration.at[symbol, "rate_concentration"] = round(
                    df_concentration.at[symbol, "times_concentration"]
                    / days_first_concentration
                    * 100,
                    2,
                )
    df_concentration.sort_values(
        by=["times_concentration"], ascending=False, inplace=True
    )
    analysis.base.write_obj_to_db(
        obj=df_concentration,
        key="df_concentration",
        filename=filename_chip_shelve,
    )
    dt_concentration_date = df_concentration["latest_concentration"].max(skipna=True)
    if dt_concentration_date > dt_latest:
        logger.error(
            f"Error - dt_concentration_date[{dt_concentration_date}] greater then date_latest"
        )
        dt_concentration_date = dt_latest
    dt_concentration = datetime.datetime.combine(dt_concentration_date, time_pm_end)
    analysis.base.set_version(key=name, dt=dt_concentration)
    return True
