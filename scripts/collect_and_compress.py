# scripts/collect_and_compress.py

import pandas as pd
import glob
import os
from tqdm import tqdm
import shutil

# --- 配置 ---
INPUT_BASE_DIR = "all_data"
OUTPUT_DIR = "kdata" # 最终输出的扁平化目录
FINAL_PARQUET_FILE = "full_kdata.parquet" # 最终的合并大文件

def main():
    """
    1. 收集所有分片文件到一个干净的目录。
    2. 将所有数据合并、排序并保存为一个优化的 Parquet 大文件。
    """
    
    # --- 阶段 1: 收集所有小文件 ---
    if os.path.exists(OUTPUT_DIR):
        shutil.rmtree(OUTPUT_DIR)
    os.makedirs(OUTPUT_DIR)

    search_pattern = os.path.join(INPUT_BASE_DIR, "**", "*.parquet")
    file_list = glob.glob(search_pattern, recursive=True)
    
    if not file_list:
        print("⚠️ 未找到任何 Parquet 数据分片文件。")
        return

    print(f"📦 共找到 {len(file_list)} 个股票的 Parquet 文件，开始收集...")
    
    for src_path in tqdm(file_list, desc="正在收集中"):
        try:
            filename = os.path.basename(src_path)
            dest_path = os.path.join(OUTPUT_DIR, filename)
            shutil.copy2(src_path, dest_path)
        except Exception as e:
            print(f"\n⚠️ 复制文件 {src_path} 失败: {e}")
            
    print(f"\n✅ 全部 {len(file_list)} 个文件已成功收集到 '{OUTPUT_DIR}' 目录中。")

    # --- 阶段 2: 创建一个经过优化的合并大文件 ---
    print("\n" + "="*50)
    print("🚀 开始创建经过压缩优化的合并文件...")
    
    all_parquet_files = glob.glob(os.path.join(OUTPUT_DIR, "*.parquet"))
    
    if not all_parquet_files:
        print("⚠️ 在收集目录中未找到 Parquet 文件，无法创建合并文件。")
        return
        
    print(f"📦 正在读取 {len(all_parquet_files)} 个 Parquet 文件...")
    all_dfs = [pd.read_parquet(f) for f in tqdm(all_parquet_files, desc="正在读取")]
    
    print("... 正在合并所有数据 ...")
    merged_df = pd.concat(all_dfs, ignore_index=True)
    
    print(f"... 正在按股票代码 ('code') 对 {len(merged_df)} 条记录进行排序以优化压缩...")
    sorted_df = merged_df.sort_values(by='code', ascending=True).reset_index(drop=True)
    
    output_path = FINAL_PARQUET_FILE
    print(f"... 正在将排序后的数据写入最终的合并文件: {output_path} ...")
    
    try:
        sorted_df.to_parquet(output_path, index=False, compression='zstd', row_group_size=100000)
        print("\n✅ 最终合并文件创建成功 (使用 zstd 压缩)！")
    except ImportError:
        print("\n⚠️ 警告: 未安装 'zstandard' 库，回退到 'snappy' 压缩。")
        sorted_df.to_parquet(output_path, index=False, compression='snappy', row_group_size=100000)
        print("\n✅ 最终合并文件创建成功 (使用 snappy 压缩)！")

if __name__ == "__main__":
    main()
