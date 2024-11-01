import requests
import pandas as pd
import numpy as np

def get_ba(year:int,quarter:int):#抓資產負債表
    url = 'https://mops.twse.com.tw/mops/web/ajax_t163sb05'
    year = year
    season = quarter
    headers = {"User-Agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36"}
    r = requests.post(url, {
            'encodeURIComponent':1,
            'step':1,
            'firstin':1,
            'off':1,
            'TYPEK':'sii',
            'year':str(year),
            'season':str(season),
        })
    dfs = pd.read_html(r.text, header=None)
    banks = dfs[1]
    securities = dfs[2]
    other = dfs[3]
    fin = dfs[4]
    insur = dfs[5]
    other2 = dfs[6]
    return banks,securities,other,fin,insur,other2

def get_income(year:int,quarter:int):#抓綜合損益表
    url = 'https://mops.twse.com.tw/mops/web/ajax_t163sb04'
    year = year
    season = quarter
    r = requests.post(url, {
            'encodeURIComponent':1,
            'step':1,
            'firstin':1,
            'off':1,
            'TYPEK':'sii',
            'year':str(year),
            'season':str(season),
        })
    dfs = pd.read_html(r.text, header=None)
    banks = dfs[1]
    securities = dfs[2]
    other1 = dfs[3]
    fin = dfs[4]
    insur = dfs[5]
    other2 = dfs[6]
    return banks,securities,other1,fin,insur,other2

def update_eps(year:int,quarter:int):#更新eps
    banks,securities,other,fin,insur,other2 = get_income(year-1911,quarter)
    banks = banks.set_index("公司 代號")
    securities = securities.set_index("公司 代號")
    other = other.set_index("公司 代號")
    fin = fin.set_index("公司 代號")
    insur = insur.set_index("公司 代號")
    other2 = other2.set_index("公司 代號")
    b_eps = banks[["基本每股盈餘（元）"]].T
    s_eps = securities[["基本每股盈餘（元）"]].T
    fin_eps = fin[["基本每股盈餘（元）"]].T
    o_eps = other[["基本每股盈餘（元）"]].T
    i_eps = insur[["基本每股盈餘（元）"]].T
    o2_eps = other2[["基本每股盈餘（元）"]].T
    all_eps = pd.concat([b_eps,s_eps,fin_eps,o_eps,i_eps,o2_eps],axis=1)
    # 将所有列名转换为字符串并添加 "_eps" 后缀
    all_eps.columns = [str(col) + "_eps" for col in all_eps.columns]
    all_eps["year"] = year
    all_eps["quarter"] = quarter 
    all_eps = all_eps.reset_index(drop=True)

    from conn_postgre import insert_data
    insert_data("eps",all_eps)