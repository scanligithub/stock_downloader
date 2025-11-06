import pandas as pd
import glob
import os
from tqdm import tqdm

input_dir = "data"

def merge_files(pattern, output_name):
    csv_files = glob.glob(os.path.join(input_dir, pattern))
    if not csv_files:
        print(f"⚠️ 未找到匹配文件: {pattern}")
        return
    print(f"📦 共找到 {len(csv_files)} 个文件 ({pattern})，开始合并...")
    df_all = []
    for f in tqdm(csv_files):
        try:
            df = pd.read_csv(f)
            df_all.append(df)
        except Exception as e:
            print(f"⚠️ 读取失败: {f}, 错误: {e}")
    merged = pd.concat(df_all, ignore_index=True)
    output_path = os.path.join(input_dir, output_name)
    merged.to_csv(output_path, index=False)
    print(f"✅ 合并完成: {output_path}")

# 合并日线数据
merge_files("*_kdata.csv", "merged_kdata.csv")

# 合并资金流向数据
merge_files("*_moneyflow.csv", "merged_moneyflow.csv")
