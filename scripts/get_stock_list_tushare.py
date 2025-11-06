import os
import tushare as ts
import pandas as pd
import json
import time

print("🚀 开始从 TuShare 获取 A 股列表...")

token = os.getenv("TUSHARE_TOKEN")
if not token:
    raise Exception("❌ 未设置环境变量 TUSHARE_TOKEN，请在 workflow secrets 中配置。")

ts.set_token(token)
pro = ts.pro_api()

try:
    df = pro.stock_basic(exchange="", list_status="L",
                         fields="ts_code,symbol,name,area,industry,list_date")
    if df.empty:
        raise Exception("⚠️ 获取结果为空，可能触发访问频率限制。")
    print(f"✅ 成功获取 {len(df)} 支股票。")
except Exception as e:
    print(f"❌ TuShare 接口调用失败: {e}")
    df = pd.DataFrame()

# 转换为 Baostock 格式
def ts_to_baostock(ts_code):
    code, market = ts_code.split(".")
    return f"sh.{code}" if market == "SH" else f"sz.{code}"

stock_list = [{"code": ts_to_baostock(c), "name": n}
              for c, n in zip(df["ts_code"], df["name"])] if not df.empty else []

# 生成 fallback 文件
with open("stock_list.json", "w", encoding="utf-8") as f:
    json.dump(stock_list, f, ensure_ascii=False, indent=2)

print(f"📦 已保存股票列表至 stock_list.json ({len(stock_list)} 条)")
