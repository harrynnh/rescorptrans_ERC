# assignment 3 data preparation

library(tidyverse)
library(lubridate)
library(RPostgres)

# connect with and login to wrds
# run in console (so you do not accidentally push your username/pw into repo):
###       username <- 'username'
###       password <- 'password'
wrds <- dbConnect(Postgres(),
                  host='wrds-pgdata.wharton.upenn.edu',
                  port=9737,
                  dbname='wrds',
                  sslmode='require',
                  user=username,
                  password=password)


#########################
# get S&P500 constituents

# https://wrds-www.wharton.upenn.edu/pages/support/applications/python-replications/historical-sp-500-index-constituents/

# from CRSP
if(!file.exists('raw_data/ass3_sp500_constituents_with_daily_mdata.rds')){
  
  # get SP500 constituents with daily market data from 2019-07-01,
  # datasets: crsp.dsp500list and crsp.dsf
  res <- dbSendQuery(wrds, "select a.start, a.ending, b.*
                            from crsp.dsp500list as a,
                            crsp.dsf as b
                            where a.permno=b.permno
                            and b.date >= a.start and b.date<= a.ending
                            and b.date >='01/07/2019'
                            order by date")
  sp500 <- dbFetch(res, n=-1)
  dbClearResult(res)  
  
  # get other company identifiers from crsp.msenames
  res <- dbSendQuery(wrds, "select comnam, ncusip, namedt, nameendt, 
                            permno, shrcd, exchcd, ticker
                            from crsp.msenames")
  mse <- dbFetch(res, n=-1)
  dbClearResult(res)

  sp500 %>%
    
    # merge with s&p500 data
    left_join(mse, by = "permno") %>%
    
    # if nameendt is missing then set to today date
    mutate(nameendt = ifelse(is.na(nameendt), '2021-05-20', nameendt)) %>%
    
    # impose the date range restrictions (identifier should still be valid)
    filter(date >= namedt, date <= nameendt) -> sp500
  
  # add compustat Identifiers
  res <- dbSendQuery(wrds, "select gvkey, liid as iid, lpermno as permno,
                            linktype, linkprim, linkdt, linkenddt
                            from crsp.ccmxpf_linktable
                            where substr(linktype,1,1)='L'
                            and (linkprim ='C' or linkprim='P')")
  ccm <- dbFetch(res, n=-1)
  dbClearResult(res)
  
  # merge the ccm data with s&p500 data, and select only constituents of 2020
  sp500 %>% 
    left_join(ccm, by = "permno") %>% 
    mutate(linkenddt = ifelse(is.na(linkenddt), '2021-05-20', linkenddt)) %>% 
    filter(date >= linkdt,
           date <= linkenddt,
           start <= '2019-07-01') %>%
    select(cusip, permno, permco, issuno, hexcd, hsiccd, date, bidlo, askhi, 
           prc, vol, ret, bid, ask, shrout, cfacpr, cfacshr, openprc, numtrd, 
           retx, comnam, ncusip, namedt, nameendt, shrcd, exchcd, ticker, 
           gvkey, iid) %>% 
    distinct() -> sp500
  
  saveRDS(sp500, 'raw_data/ass3_sp500_constituents_with_daily_mdata.rds')
} else {
  sp500 <- readRDS('raw_data/ass3_sp500_constituents_with_daily_mdata.rds')
}


###########################################
# get analyst eps estimates, and actual eps

if(!file.exists('raw_data/ass3_ibes_eps_forecasts.rds')){
  
  res <- dbSendQuery(wrds, paste0("select * from ibes.statsum_epsus 
                            where fpedats >= '2019-10-01'
                            and fpedats <= '2020-12-31'
                            and cusip in ( ",
                            paste(
                              paste0("'", unique(sp500$cusip), "'"),
                              collapse = " , "),
                            " ) "))
  
  aneps <- dbFetch(res, n=-1)
  dbClearResult(res)
  
  aneps %>% 
    filter(
      # take quarterly estimates
      fiscalp == 'QTR',
      # take one quarter-ahead forecasts only
      fpi == 6) %>%
    select(ticker:cname, fpedats, statpers:usfirm, actual, anndats_act) -> aneps

  saveRDS(aneps, 'raw_data/ass3_ibes_eps_forecasts.rds')
} else {
  aneps <- readRDS('raw_data/ass3_ibes_eps_forecasts.rds')
}


###########################
# get Compustat annual data

# selected fundamentals for 2020
if(!file.exists('raw_data/ass3_cmp_fundamentals.rds')){
  
  # select variables to pull from wrds compustat north america
  vars <- c(
    "gvkey", "conm", "cik", "fyear", "datadate", "indfmt", "sich",
    "consol", "popsrc", "datafmt", "curcd", "curuscn", "fyr", 
    "act", "ap", "aqc", "aqs", "acqsc", "at", "ceq", "che", "cogs", 
    "csho", "dlc", "dp", "dpc", "dt", "dvpd", "exchg", "gdwl", "ib", 
    "ibc", "intan", "invt", "lct", "lt", "ni", "capx", "oancf", 
    "ivncf", "fincf", "oiadp", "pi", "ppent", "ppegt", "rectr", 
    "sale", "seq", "txt", "xint", "xsga", "costat", "mkvalt", "prcc_f",
    "recch", "invch", "apalch", "txach", "aoloch",
    "gdwlip", "spi", "wdp", "rcp"
  )
  
  var_str <- paste(vars, collapse = ", ")
  
  # get fundamentals for observation where fiscal year is 2020 and gvkey
  # is in our s&p500 dataset
  res <- dbSendQuery(wrds, paste0("select ", var_str, " from comp.funda
                            where fyear between 2019 and 2020 and gvkey in ( ",
                     paste(
                       paste0("'", unique(sp500$gvkey), "'"), 
                       collapse = " , "),
                     " )"))
  cmp <- dbFetch(res, n=-1)
  dbClearResult(res)
  
  
  cmp %>%
    # filter for formats
    filter(indfmt == "INDL", datafmt == "STD") %>%
    select(-curuscn) %>%
    distinct() -> cmp
  
  saveRDS(cmp, 'raw_data/ass3_cmp_fundamentals.rds')
} else {
  cmp <- readRDS('raw_data/ass3_cmp_fundamentals.rds')
}


##########
# datasets 

write_csv(sp500, 'data/generated/assignment_3_sp500_constituents_with_daily_mdata.csv')
write_csv(aneps, 'data/generated/assignment_3_ibes_eps_analyst_estimates.csv')
write_csv(cmp, 'data/generated/assignment_3_cmp_fundamentals.csv')

