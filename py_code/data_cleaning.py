import os
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import statsmodels.formula.api as smf
from statsmodels.regression.rolling import RollingOLS

os.chdir("/Users/harrynnh/workspace/misc/rescorptrans_ERC/")

# Cleaning up datasets
def comp_clean():
    """This function load and clean up datasets to be used in the analysis"""

    comp_df = (
        pd.read_feather("data/assignment_3_cmp_fundamentals.feather")
        .sort_values(["gvkey", "datadate"])
        .loc[
            lambda x: (x["indfmt"] == "INDL")
            & (x["datafmt"] == "STD")
            & (x["popsrc"] == "D")
            & (x["consol"] == "C"),
        ]
        .rename(columns={"datadate": "date"})
        .assign(
            date=lambda x: pd.to_datetime(x["date"]),
            yq=lambda x: x["date"].dt.to_period("Q"),
            sale_lag=lambda x: x.groupby("gvkey")["sale"].shift(1),
            ln_ta=lambda x: np.log(1 + x["at"]),
            mtb=lambda x: (x["csho"] * x["prcc_f"]) / x["ceq"],
        )
        .loc[:, ["gvkey", "date", "yq", "ln_ta", "mtb"]]
    )
    sp500_df = (
        pd.read_feather("data/assignment_3_sp500_constituents_with_daily_mdata.feather")
        .sort_values(["gvkey", "date"])
        .assign(
            quarter=lambda x: pd.PeriodIndex(x["date"], freq="Q"),
            num_quarter=lambda x: x.groupby("gvkey")["quarter"].transform("nunique"),
        )
        .loc[
            :,
            # lambda x: x["num_quarter"] == 8,
            ["ncusip", "gvkey", "date", "prc", "ret",],
        ]
        .assign(cumret=lambda x: x["ret"].rolling(3, min_periods=1, center=True).sum())
        .rename(columns={"ncusip": "cusip"})
    )
    ibes_df = (
        pd.read_feather("data/assignment_3_ibes_eps_analyst_estimates.feather")
        .sort_values(["cusip", "anndats_act"])
        .loc[
            lambda x: x["cusip"].isin(sp500_df["cusip"]),
            ["cusip", "anndats_act", "actual", "medest", "meanest"],
        ]
        .assign(
            medest_avg=lambda x: x.groupby(["cusip", "anndats_act"])[
                "medest"
            ].transform(np.mean),
            meanest_avg=lambda x: x.groupby(["cusip", "anndats_act"])[
                "meanest"
            ].transform(np.mean),
        )
        .loc[:, ["cusip", "anndats_act", "actual", "medest_avg", "meanest_avg",],]
        .rename(columns={"anndats_act": "date"})
        .drop_duplicates()
    )
    return comp_df, sp500_df, ibes_df


comp_df, sp500_df, ibes_df = comp_clean()

# Merge crsp to ibes
erc_ibes_df = (
    pd.merge(ibes_df, sp500_df, how="left", on=["cusip", "date"])
    .sort_values(["gvkey", "date"])
    .loc[lambda x: (~x["gvkey"].isna() & (~x["actual"].isna()))]
    .assign(
        earn_sur=lambda x: x["actual"] - x["medest_avg"],
        num_quarter=lambda x: x.groupby("gvkey")["gvkey"].transform("count"),
        year=lambda x: x["date"].dt.year,
    )
    .loc[lambda x: x["num_quarter"] >= 12]
)
# Winsorize at 1 and 99 percentile
winz_col = ["cumret", "earn_sur"]
erc_ibes_df.loc[:, winz_col] = erc_ibes_df.loc[:, winz_col].clip(
    lower=erc_ibes_df[winz_col].quantile(0.01),
    upper=erc_ibes_df[winz_col].quantile(0.99),
    axis=1,
)

# Save dataset for analysis
erc_ibes_df.reset_index(drop=True).to_feather("data/generated/erc_ibes.feather")
comp_df.reset_index(drop=True).to_feather("data/generated/comp_clean.feather")
