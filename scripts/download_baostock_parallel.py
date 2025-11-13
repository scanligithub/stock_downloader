# scripts/download_baostock_parallel.py (修正版)

import os
import json
import baostock as bs
import pandas as pd
from tqdm import tqdm

# --- 获取环境变量 ---
TASK_INDEX = int(os.getenv("TASK_INDEX", 0))
TASK_COUNT = int(os.getenv("TASK_COUNT", 20))
OUTPUT_DIR = "data"
os.makedirs(OUTPUT_DIR, exist_ok=True)

# --- 登录 Baostock ---
lg = bs.login()
if lg.error_code != '0':
    raise Exception(f"登录失败：{lg.error_msg}")
print("✅ 登录成功")

# --- 加载和切分任务 ---
try:
    with open("stock_list.json", "r", encoding="utf-8") as f:
        stocks = json.load(f)
except FileNotFoundError:
    print("❌ 致命错误: 未找到 stock_list.json 文件！")
    exit(1)

chunk_size = (len(stocks) + TASK_COUNT - 1) // TASK_COUNT
subset = stocks[TASK_INDEX * chunk_size:(TASK_INDEX + 1) * chunk_size]
print(f"📦 当前任务分区 {TASK_INDEX + 1}/{TASK_COUNT}，负责 {len(subset)} 支股票。")

# --- 下载函数 ---
def get_stock_data(code):
    # (关键修正 1) 使用被验证过的、能成功获取数据的“安全”日期范围
    start_date = "1990-01-01"
    end_date = ""  # 空字符串表示获取到最新

    rs = bs.query_history_k_data_plus(
        code,
        "date,code,open,high,low,close,preclose,volume,amount,turn,pctChg,isST",
        start_date=start_date,
        end_date=end_date,
        frequency="d",
        adjustflag="3"  # 不复权
    )
    
    if rs.error_code != '0':
        print(f"\n  -> API Error for {code}: {rs.error_msg}")
        return

    data_list = []
    while rs.next():
        data_list.append(rs.get_row_data())
        
    if not data_list:
        return

    df = pd.DataFrame(data_list, columns=rs.fields)
    
    # (关键修正 2) 在文件名中加入可识别的后缀
    output_filename = f"{code.replace('.', '_')}_kdata.csv"
    df.to_csv(os.path.join(OUTPUT_DIR, output_filename), index=False)
    
# --- 主循环 ---
for s in tqdm(subset, desc=f"分区 {TASK_INDEX + 1} 下载中"):
    try:
        get_stock_data(s["code"])
    except Exception as e:
        print(f"\n❌ {s['code']} 下载时发生严重错误：{e}")

bs.logout()
print(f"\n✅ 分区 {TASK_INDEX + 1} 任务完成。")
