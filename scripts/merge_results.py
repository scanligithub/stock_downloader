# scripts/merge_results.py (重构版)

import pandas as pd
import glob
import os
from tqdm import tqdm
import argparse

# (关键) 输入目录现在是所有 artifacts 被解压的地方
INPUT_BASE_DIR = "all_data"
# (关键) 定义一个专门的输出目录
OUTPUT_DIR = "final_output"
os.makedirs(OUTPUT_DIR, exist_ok=True)

def merge_files(pattern, output_filename):
    """
    递归搜索、合并数据分片，并进行去重。
    """
    # (关键) 使用 recursive=True 深度搜索所有子目录
    search_pattern = os.path.join(INPUT_BASE_DIR, "**", pattern)
    file_list = glob.glob(search_pattern, recursive=True)

    if not file_list:
        print(f"⚠️ 未找到任何匹配 '{pattern}' 的文件，无法合并。")
        return

    print(f"📦 共找到 {len(file_list)} 个 '{pattern}' 文件，开始合并...")

    all_dfs = []
    for f in tqdm(file_list, desc=f"正在读取 {pattern} 分片"):
        try:
            df = pd.read_csv(f)
            all_dfs.append(df)
        except Exception as e:
            print(f"\n⚠️ 读取文件 {f} 失败: {e}")

    if not all_dfs:
        print("⚠️ 所有文件均读取失败，无法合并。")
        return

    print("\n... 所有分片读取完毕，开始合并 ...")
    merged_df = pd.concat(all_dfs, ignore_index=True)
    
    # (关键) 增加去重逻辑，防止因重复运行等原因导致的数据重复
    initial_rows = len(merged_df)
    merged_df.drop_duplicates(inplace=True)
    final_rows = len(merged_df)
    
    if initial_rows > final_rows:
        print(f"ℹ️ 去重操作移除了 {initial_rows - final_rows} 条重复记录。")

    if 'code' in merged_df.columns and 'date' in merged_df.columns:
        merged_df.sort_values(by=['code', 'date'], inplace=True)

    # (关键) 保存为 Parquet 格式
    output_path = os.path.join(OUTPUT_DIR, output_filename)
    merged_df.to_parquet(output_path, index=False, compression='zstd')

    print(f"\n✅ 合并完成！已保存为 Parquet 文件: {output_path}")
    print(f"   - 总计记录数: {len(merged_df)}")
    if 'code' in merged_df.columns:
        print(f"   - 涉及股票数: {merged_df['code'].nunique()}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="合并数据分片并保存为 Parquet 文件。")
    parser.add_argument('--output', type=str, required=True, help="输出的 Parquet 文件名")
    args = parser.parse_args()
    
    # 我们现在只合并日线数据
    merge_files("*_kdata.csv", args.output)
    
    # 如果未来有资金流数据，可以取消这行注释
    # merge_files("*_moneyflow.csv", "full_moneyflow.parquet")
