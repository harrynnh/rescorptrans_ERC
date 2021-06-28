import os
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import statsmodels.api as sm
import statsmodels.formula.api as smf
from linearmodels import RandomEffects
from statsmodels.regression.rolling import RollingOLS
import matplotlib.pyplot as plt
import seaborn as sns
from scipy import stats
from statsmodels.iolib.summary2 import summary_col
from stargazer.stargazer import Stargazer

os.chdir("/Users/harrynnh/workspace/misc/rescorptrans_ERC/")
erc_ibes_df = pd.read_feather("data/generated/erc_ibes.feather")
comp_df = pd.read_feather("data/generated/comp_clean.feather")

# Rolling regression CAR ~ UE
def rolling_reg(df):
    formula = "cumret ~ earn_sur"
    mod = RollingOLS.from_formula(formula, data=df, window=12)
    rres = mod.fit()
    df["roll_coef"] = rres.params.earn_sur
    return df


roll_res = erc_ibes_df.groupby("gvkey").apply(rolling_reg).dropna()
rolling_erc = roll_res.roll_coef.mean()

# Cross sectional ERC with a single regression
def cs_reg(df):
    formula = "cumret ~ earn_sur"
    mod = smf.ols(formula, data=df)
    res = mod.fit()
    return res


pool_df = erc_ibes_df.loc[
    erc_ibes_df["year"] > 2017, ["gvkey", "year", "cumret", "earn_sur"]
]

pool_reg = cs_reg(pool_df)
print(pool_reg.summary())
cs_erc = pool_reg.params.earn_sur

# Barplot for 2 ERCs
# print(plt.style.available)
# plt.style.use('seaborn')
plt.xkcd()
plt.bar(["Rolling ERC", "Pooled ERC"], [rolling_erc, cs_erc])
plt.xlabel("Methods")
plt.ylabel("ERC")
plt.title("Rolling firm specific ERC and pooled ERC")
plt.tight_layout()
plt.savefig("output/erc_barplot.eps", format="eps")
plt.show()


# What can I do with the firm specific ERCs?
firm_erc = (
    roll_res.loc[roll_res["year"] > 2017, ["gvkey", "roll_coef"]]
    .groupby("gvkey")
    .agg(np.mean)
)
plt.xkcd()
fig_hist = sns.histplot(data=firm_erc, x="roll_coef", kde=True, bins=35)
fig_hist.set(xlabel="ERC", ylabel="Count", title="Firm-specific ERC distribution")
plt.tight_layout()
plt.savefig("output/erc_coef_dist.eps", format="eps")
plt.show()

time_erc = (
    roll_res.loc[roll_res["year"] > 2017, ["year", "roll_coef"]]
    .groupby("year")
    .agg(np.mean)
)
plt.bar(["2018", "2019"], time_erc["roll_coef"])
plt.show()
# Plot pooled regression
# sns.set_theme(color_codes=True)
plt.xkcd()
fig_reg = sns.regplot(x="earn_sur", y="cumret", data=erc_ibes_df)
fig_reg.set(
    xlabel="Earnings surprise",
    ylabel="3-day cumulative return",
    title="Earnings return relation",
)
plt.tight_layout()
plt.savefig("output/erc_regplot.eps", format="eps")
plt.show()

# Determinants of ERCs
erc_ibes_df = erc_ibes_df.assign(yq=erc_ibes_df["date"].dt.to_period("Q")).drop(
    "date", axis=1
)
erc_comp_df = pd.merge(
    comp_df,
    erc_ibes_df.loc[erc_ibes_df["year"] > 2017],
    how="left",
    on=["gvkey", "yq"],
).dropna()
det_erc_res = smf.ols(
    "cumret ~ earn_sur + ln_ta + earn_sur*ln_ta + mtb + earn_sur*mtb", data=erc_comp_df
).fit()

erc_det_res = summary_col(
    [pool_reg, det_erc_res],
    stars=True,
    float_format="%0.2f",
    info_dict={"R-squared": lambda x: "{:.2f}".format(x.rsquared)},
).as_latex()
# f = open("output/erc_table.tex", "w")
# f.write(erc_det_res)
# f.close()
