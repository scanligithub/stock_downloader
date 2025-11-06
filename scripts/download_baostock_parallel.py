import os
import json
import baostock as bs
import pandas as pd
from tqdm import tqdm
from datetime import datetime

# ========== 参数部分 ==========
TASK_INDEX = int(os.getenv("TASK_INDEX", 0))
TASK_COUNT = int(os.getenv("TASK_COUNT", 20))
OUTPUT_DIR = "data"
os.makedirs(OUTPUT_DIR, exist_ok=True)

# ========== 登录 ==========
lg = bs.login()
if lg.error_code != '0':
    raise Exception(f"登录失败：{lg.error_msg}")
print("✅ 登录成功")

# ========== 加载股票列表 ==========
with open("stock_list.json", "r", encoding="utf-8") as f:
    stocks = json.load(f)

chunk_size = len(stocks) // TASK_COUNT + 1
subset = stocks[TASK_INDEX * chunk_size:(TASK_INDEX + 1) * chunk_size]
print(f"📦 当前任务分区 {TASK_INDEX}/{TASK_COUNT}，股票数量：{len(subset)}")

# ========== 定义函数 ==========
def get_k_data(code):
    start_date = "1990-01-01"
    end_date = datetime.today().strftime("%Y-%m-%d")
    rs = bs.query_history_k_data_plus(
        code,
        "date,code,open,high,low,close,volume,amount,turn",
        start_date=start_date,
        end_date=end_date,
        frequency="d",
        adjustflag="3"
    )
    data_list = []
    while rs.next():
        data_list.append(rs.get_row_data())
    if not data_list:
        return pd.DataFrame()
    df = pd.DataFrame(data_list, columns=rs.fields)
    df.to_csv(f"{OUTPUT_DIR}/{code.replace('.', '_')}_kdata.csv", index=False)
    return df

def get_money_flow(code):
    start_date = "1990-01-01"
    end_date = datetime.today().strftime("%Y-%m-%d")
    rs = bs.query_money_flow_data(code, start_date, end_date)
    data_list = []
    while rs.next():
        data_list.append(rs.get_row_data())
    if not data_list:
        return pd.DataFrame()
    df = pd.DataFrame(data_list, columns=rs.fields)
    df.to_csv(f"{OUTPUT_DIR}/{code.replace('.', '_')}_moneyflow.csv", index=False)
    return df

# ========== 下载部分 ==========
for s in tqdm(subset, desc="批量下载中"):
    code = s["code"]
    try:
        get_k_data(code)
        get_money_flow(code)
    except Exception as e:
        print(f"❌ {code} 下载失败：{e}")

bs.logout()
print("✅ 全部分区任务完成")
