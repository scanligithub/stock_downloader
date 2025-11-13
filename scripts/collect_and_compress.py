# scripts/collect_and_compress.py (缩进修正版)

import pandas as pd
import glob
import os
from tqdm import tqdm
import shutil
import json
from pathlib import Path

# --- 配置 ---
INPUT_BASE_DIR = "all_data"
OUTPUT_DIR_SMALL_FILES = "kdata"
FINAL_PARQUET_FILE = "full_kdata.parquet" 
QC_REPORT_FILE = "data_quality_report.json"

# ... run_quality_check 函数保持不变 ...

def main():
    # ... 阶段 1: 收集所有小文件 (保持不变) ...

    # --- 阶段 2: 创建一个经过优化的合并大文件 ---
    print("\n" + "="*50)
    print("🚀 开始创建经过压缩优化的合并文件...")
    
    all_parquet_files = glob.glob(os.path.join(OUTPUT_DIR_SMALL_FILES, "*.parquet"))
    
    if not all_parquet_files:
        print("⚠️ 在收集目录中未找到 Parquet 文件，无法创建合并文件。")
        return
        
    print(f"📦 正在读取 {len(all_parquet_files)} 个 Parquet 文件...")
    all_dfs = [pd.read_parquet(f) for f in tqdm(all_parquet_files, desc="正在读取")]
    
    print("... 正在合并所有数据 ...")
    merged_df = pd.concat(all_dfs, ignore_index=True)

    print("... 正在进行强制数据类型转换 ...")
    numeric_cols = ['open', 'high', 'low', 'close', 'preclose', 'volume', 'amount', 'turn', 'pctChg']
    cols_to_convert = numeric_cols + ['isST']
    for col in cols_to_convert:
        if col in merged_df.columns:
            merged_df[col] = pd.to_numeric(merged_df[col], errors='coerce')
    if 'date' in merged_df.columns:
        merged_df['date'] = pd.to_datetime(merged_df['date'], errors='coerce')
    print("✅ 数据类型转换完成。")
    
    print(f"... 正在按股票代码 ('code') 对 {len(merged_df)} 条记录进行排序以优化压缩...")
    sorted_df = merged_df.sort_values(by='code', ascending=True).reset_index(drop=True)
    
    output_path = FINAL_PARQUET_FILE
    print(f"... 正在将排序后的数据写入最终的合并文件: {output_path} ...")
    
    # (这是修正后的 try...except 块)
    try:
        sorted_df.to_parquet(output_path, index=False, compression='zstd', row_group_size=100000)
        print("\n✅ 最终合并文件创建成功 (使用 zstd 压缩)！")
    except ImportError:
        print("\n⚠️ 警告: 未安装 'zstandard' 库，回退到 'snappy' 压缩。")
        sorted_df.to_parquet(output_path, index=False, compression='snappy', row_group_size=100000)
        print("\n✅ 最终合并文件创建成功 (使用 snappy 压缩)！")

    # --- 阶段 3: 运行数据质量检查 ---
    if not sorted_df.empty:
        run_quality_check(sorted_df)
    else:
        print("\n⚠️ 合并后的数据为空，跳过质量检查。")

if __name__ == "__main__":
    main()

# (注意: run_quality_check 函数需要您从之前的回复中完整复制过来，这里省略以保持简洁)
