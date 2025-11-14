# scripts/collect_and_compress.py (最终健壮版)

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

# ... run_quality_check 函数保持不变 (请确保您使用的是带有该函数的版本) ...
def run_quality_check(df):
    # ... 完整的质检逻辑 ...
    pass # 这里省略以保持简洁，请使用您之前的完整版本

def main():
    """
    1. 收集所有分片文件。
    2. 合并、排序并保存为一个优化的 Parquet 大文件。
    3. 对最终数据进行质量检查。
    """
    
    # --- 阶段 1: 收集所有小文件 ---
    if os.path.exists(OUTPUT_DIR_SMALL_FILES):
        shutil.rmtree(OUTPUT_DIR_SMALL_FILES)
    os.makedirs(OUTPUT_DIR_SMALL_FILES)

    search_pattern = os.path.join(INPUT_BASE_DIR, "**", "*.parquet")
    file_list = glob.glob(search_pattern, recursive=True)
    
    # --- (这是唯一的、关键的修正) ---
    if not file_list:
        print("\n" + "="*60)
        print("❌ 致命错误: 在所有下载产物中，未找到任何 .parquet 文件！")
        print("   这通常意味着上游的 'download' 作业虽然显示成功，但实际上没有下载到任何数据。")
        print("   请检查 'download' 作业的详细日志，确认是否有 '致命警告'。")
        print("="*60)
        exit(1) # 找不到文件就直接报错退出！
    # ------------------------------------

    print(f"📦 共找到 {len(file_list)} 个股票的 Parquet 文件，开始收集...")
    
    # ... 后续的收集、合并、排序、压缩、质检逻辑都保持不变 ...
    for src_path in tqdm(file_list, desc="正在收集中"):
        # ...
    # ...
    
    # (确保 main 函数的其余部分是完整的)
    
if __name__ == "__main__":
    main()
    # (重要) 请确保您将 run_quality_check 函数的完整定义也放在这个文件中
