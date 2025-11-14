# scripts/download_baostock_parallel.py (最终健壮版)

import os
import json
import baostock as bs
import pandas as pd
from tqdm import tqdm

# --- 配置 ---
OUTPUT_DIR = "data_slice"
# (关键) 使用被反复验证过的、能成功获取数据的“安全”起始日期
START_DATE = "2005-01-01"

# --- 获取环境变量 & 准备目录 ---
TASK_INDEX = int(os.getenv("TASK_INDEX", 0))
os.makedirs(OUTPUT_DIR, exist_ok=True)

def get_kdata(code):
    """获取单只股票的不复权日K线历史数据"""
    rs = bs.query_history_k_data_plus(
        code,
        "date,code,open,high,low,close,preclose,volume,amount,turn,pctChg,isST",
        start_date=START_DATE,
        end_date="",      # 空字符串表示获取到最新
        frequency="d",
        adjustflag="3"  # 不复权
    )
    
    if rs.error_code != '0':
        # 如果 API 本身返回错误，在循环中打印
        # 返回空 DataFrame，让主循环处理
        return pd.DataFrame()

    data_list = []
    while rs.next():
        data_list.append(rs.get_row_data())
        
    if not data_list:
        return pd.DataFrame()
        
    return pd.DataFrame(data_list, columns=rs.fields)


def main():
    print("🚀 开始 Baostock K-Data 分布式下载任务...")
    
    task_file = f"tasks/task_slice_{TASK_INDEX}.json"
    
    try:
        with open(task_file, "r", encoding="utf-8") as f:
            subset = json.load(f)
        print(f"📦 当前为任务分区 {TASK_INDEX + 1}，负责下载 {len(subset)} 支股票。")
    except FileNotFoundError:
        print(f"❌ 致命错误: 未找到任务分片文件 {task_file}！")
        exit(1)

    # 如果任务列表为空，直接成功退出
    if not subset:
        print("🟡 本分区任务列表为空，无需下载。")
        print(f"\n✅ 分区 {TASK_INDEX + 1} 任务完成。")
        return

    lg = bs.login()
    if lg.error_code != '0':
        print(f"❌ 分区 {TASK_INDEX + 1} 登录失败: {lg.error_msg}")
        exit(1)

    total_downloaded_count = 0
    try:
        for s in tqdm(subset, desc=f"分区 {TASK_INDEX + 1} 下载进度"):
            code = s["code"]
            name = s.get("name", "")
            
            try:
                df = get_kdata(code)
                if not df.empty:
                    output_path = f"{OUTPUT_DIR}/{code}.parquet"
                    df.to_parquet(output_path, index=False)
                    total_downloaded_count += 1
            except Exception as e:
                print(f"\n  -> ❌ 在处理 {name} ({code}) 时出错: {e}")
    finally:
        bs.logout()

    # --- (关键) 增加最终检查 ---
    print(f"\n✅ 分区 {TASK_INDEX + 1} 任务完成。")
    print(f"   - 负责股票数: {len(subset)}")
    print(f"   - 成功下载文件数: {total_downloaded_count}")

    if total_downloaded_count == 0 and len(subset) > 0:
        print("\n" + "="*60)
        print("❌ 致命警告: 本分区有 {len(subset)} 个任务，但没有成功下载任何一个文件！")
        print("   这通常意味着 Baostock API 的参数（如 start_date）或环境存在问题。")
        print("="*60)
        # 以失败状态退出，让整个 job 失败
        exit(1)


if __name__ == "__main__":
    main()
