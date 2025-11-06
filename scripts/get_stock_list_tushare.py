# scripts/get_stock_list_tushare.py (v-fallback)

import os
import tushare as ts
import pandas as pd
import json

print("🚀 开始从 TuShare 获取 A 股列表...")

# --- 配置 ---
# 定义输出文件名，方便引用
OUTPUT_FILE = "stock_list.json"
# 定义备份文件名，即仓库中已有的文件名。它们是同一个文件。
FALLBACK_FILE = "stock_list.json" 

def fetch_from_tushare():
    """
    核心函数，负责从 Tushare 获取数据并进行处理。
    如果成功，返回处理好的列表；如果失败，返回 None。
    """
    token = os.getenv("TUSHARE_TOKEN")
    if not token:
        print("⚠️ 警告: 未在环境变量中找到 TUSHARE_TOKEN。")
        return None

    try:
        ts.set_token(token)
        pro = ts.pro_api()

        print("... 正在调用 Tushare 接口...")
        df = pro.stock_basic(exchange="", list_status="L",
                             fields="ts_code,symbol,name,area,industry,list_date")
        
        if df.empty or len(df) < 1000: # 增加一个健壮性检查，A股数量不可能少于1000
            print(f"⚠️ 警告: Tushare 获取结果为空或数据量过少({len(df)}条)，可能触发限制或接口异常。")
            return None
            
        print(f"✅ 成功从 Tushare 获取 {len(df)} 支股票。")

        # 转换为 Baostock 格式
        def ts_to_baostock(ts_code):
            code, market = ts_code.split(".")
            # 增加对北交所代码的支持
            if market == "BJ":
                return f"bj.{code}"
            return f"sh.{code}" if market == "SH" else f"sz.{code}"

        # 过滤掉名称中包含 'ST' 或 '退' 的股票
        df_filtered = df[~df['name'].str.contains('ST|退', na=False)]
        
        stock_list = [{"code": ts_to_baostock(c), "name": n}
                      for c, n in zip(df_filtered["ts_code"], df_filtered["name"])]
        
        return stock_list

    except Exception as e:
        print(f"❌ Tushare 接口调用时发生错误: {e}")
        return None


def main():
    # 1. 尝试从 Tushare 获取最新列表
    latest_stock_list = fetch_from_tushare()

    # 2. 检查结果并决定下一步行动
    if latest_stock_list:
        # 如果成功获取到新列表，则使用它
        print(f"📦 使用最新的 Tushare 列表 ({len(latest_stock_list)} 条)，正在写入 {OUTPUT_FILE}...")
        with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
            json.dump(latest_stock_list, f, ensure_ascii=False, indent=2)
        print(f"✅ 最新的股票列表已保存到 {OUTPUT_FILE}。")
    else:
        # 如果获取失败，则检查是否存在本地备份文件
        print(f"📉 Tushare 获取失败，将尝试使用仓库中已有的备份文件: {FALLBACK_FILE}")
        if os.path.exists(FALLBACK_FILE):
            # 我们什么都不用做，因为文件已经存在了。
            # 只需打印一条信息，确认我们正在使用备份。
            with open(FALLBACK_FILE, "r", encoding="utf-8") as f:
                backup_list = json.load(f)
            print(f"✅ 成功找到并确认将使用本地备份文件，其中包含 {len(backup_list)} 支股票。")
        else:
            # 这是一个灾难性的情况：在线获取失败，本地备份也不存在。
            print(f"❌ 致命错误: Tushare 获取失败，且仓库中的备份文件 {FALLBACK_FILE} 也不存在！")
            print("   请在仓库根目录手动创建一个空的或旧的 stock_list.json 文件。")
            # 生成一个空文件，以防止下游工作流因文件不存在而彻底失败
            with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
                json.dump([], f)
            exit(1) # 以失败状态退出，这样 GitHub Actions 会发送失败通知

if __name__ == "__main__":
    main()
