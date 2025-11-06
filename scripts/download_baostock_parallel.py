import os
import json
import baostock as bs
import pandas as pd
from datetime import datetime
from tqdm import tqdm

# 环境变量：并行任务编号和总任务数
TASK_INDEX = int(os.getenv("TASK_INDEX", 0))
TASK_COUNT = int(os.getenv("TASK_COUNT", 20))

DATA_DIR = "data"
os.makedirs(DATA_DIR, exist_ok=True)

# ===== 登录 Baostock =====
print("🚀 开始下载 A 股全市场日线 + 资金流向数据...")
lg = bs.login()
if lg.error_code != "0":
    raise Exception(f"登录失败：{lg.error_msg}")
print("✅ 登录成功")

# ===== 加载股票列表 =====
stock_list_path = "stock_list.json"
if not os.path.exists(stock_list_path):
    raise FileNotFoundError("❌ 未找到 stock_list.json，请确认文件存在于项目根目录！")

with open(stock_list_path, "r", encoding="utf-8") as f:
    stocks = json.load(f)

if not stocks:
    raise ValueError("❌ 股票列表为空，请检查 stock_list.json 文件内容")

# 切分任务
chunk_size = len(stocks) // TASK_COUNT + 1
subset = stocks[TASK_INDEX * chunk_size:(TASK_INDEX + 1) * chunk_size]
print(f"📊 当前任务分区 {TASK_INDEX+1}/{TASK_COUNT}，股票数量：{len(subset)}")

# ===== 下载函数定义 =====
def download_kdata(code):
    """下载日线行情"""
    start_date = "2020-01-01"
    end_date = datetime.today().strftime("%Y-%m-%d")
    rs = bs.query_history_k_data_plus(
        "code, date, open, high, low, close, preclose, volume, amount, adjustflag, turn, tradestatus, pctChg",
        code=code,
        start_date=start_date,
        end_date=end_date,
        frequency="d",
        adjustflag="2"
    )

    data_list = []
    while rs.next():
        data_list.append(rs.get_row_data())

    if not data_list:
        return

    df = pd.DataFrame(data_list, columns=rs.fields)
    df.to_csv(os.path.join(DATA_DIR, f"{code.replace('.', '_')}_kdata.csv"), index=False)


def download_moneyflow(code):
    """下载资金流向数据"""
    start_date = "2020-01-01"
    end_date = datetime.today().strftime("%Y-%m-%d")

    # ✅ 正确接口：query_moneyflow_by_date
    rs = bs.query_moneyflow_by_date(
        code=code,
        start_date=start_date,
        end_date=end_date
    )

    data_list = []
    while rs.next():
        data_list.append(rs.get_row_data())

    if not data_list:
        return

    df = pd.DataFrame(data_list, columns=rs.fields)
    df.to_csv(os.path.join(DATA_DIR, f"{code.replace('.', '_')}_moneyflow.csv"), index=False)


# ===== 主循环 =====
for s in tqdm(subset, desc="批量下载中"):
    code = s["code"]
    try:
        download_kdata(code)
        download_moneyflow(code)
    except Exception as e:
        print(f"❌ {code} 下载失败：{e}")

bs.logout()
print("🏁 当前任务完成，已退出登录。")
