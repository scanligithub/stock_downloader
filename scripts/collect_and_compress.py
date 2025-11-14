# scripts/collect_and_compress.py

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

def run_quality_check(df):
    """
    对合并后的 DataFrame 进行数据质量检查，并生成报告。
    """
    print("\n" + "="*50)
    print("🔍 [QC] 开始进行数据质量检查 (Data Quality Check)...")
    
    try:
        if not pd.api.types.is_datetime64_any_dtype(df['date']):
            df['date'] = pd.to_datetime(df['date'])
        print("  -> [QC] 日期列类型检查/转换完成。")

        report = {}
        
        # 1. 基础统计
        report['total_records'] = int(len(df))
        report['total_stocks'] = int(df['code'].nunique())
        report['start_date'] = df['date'].min().strftime('%Y-%m-%d')
        report['end_date'] = df['date'].max().strftime('%Y-%m-%d')
        print("  -> [QC] 基础统计完成。")
        
        # 2. 完整性检查
        stock_lengths = df.groupby('code').size()
        long_history_stock = stock_lengths.idxmax()
        df_single = df[df['code'] == long_history_stock].set_index('date').sort_index()
        expected_dates = pd.date_range(start=df_single.index.min(), end=df_single.index.max(), freq='B')
        missing_dates = expected_dates.difference(df_single.index)
        report['completeness_check'] = {
            'sample_stock_for_check': long_history_stock,
            'checked_period_years': round((df_single.index.max() - df_single.index.min()).days / 365.25, 1),
            'business_days_missing_in_sample': int(len(missing_dates))
        }
        print("  -> [QC] 完整性抽样检查完成。")

        # 3. 准确性检查
        report['accuracy_checks'] = {
            'negative_prices': int(df[(df['open'] < 0) | (df['high'] < 0) | (df['low'] < 0) | (df['close'] < 0)].shape[0]),
            'zero_prices_or_volume': int(df[(df['close'] <= 0) | (df['volume'] <= 0)].shape[0]),
            'high_lower_than_low': int(df[df['high'] < df['low']].shape[0]),
        }
        print("  -> [QC] 准确性（异常值）检查完成。")

        # 4. 空值检查
        nan_counts = df.isnull().sum()
        report['nan_values_summary'] = nan_counts[nan_counts > 0].astype(int).to_dict()
        print("  -> [QC] 空值检查完成。")

        # 5. 数据分布统计
        report['distribution_stats'] = {
            'avg_records_per_stock': round(stock_lengths.mean(), 2),
            'median_records_per_stock': int(stock_lengths.median()),
            'stocks_over_15_years': int((stock_lengths > 250*15).sum()),
            'stocks_over_10_years': int((stock_lengths > 250*10).sum()),
            'stocks_over_5_years': int((stock_lengths > 250*5).sum()),
            'stocks_under_1_year': int((stock_lengths < 250*1).sum())
        }
        print("  -> [QC] 数据分布统计完成。")

        print("✅ [QC] 数据质量检查逻辑执行完毕。")
        
        with open(QC_REPORT_FILE, 'w', encoding='utf-8') as f:
            json.dump(report, f, indent=2, ensure_ascii=False)
        print(f"📄 [QC] 质检报告已成功保存到: {QC_REPORT_FILE}")
        
        print("\n--- 数据质量简报 ---")
        # ... (简报打印逻辑) ...

    except Exception as e:
        print(f"\n❌ [QC] 在执行质量检查时发生严重错误: {e}")
        import traceback
        traceback.print_exc()

def main():
    """
    主函数，包含了详细的调试打印。
    """
    print("\n--- [main] 函数开始执行 ---")
    
    # --- 阶段 1: 收集所有小文件 ---
    if os.path.exists(OUTPUT_DIR_SMALL_FILES):
        shutil.rmtree(OUTPUT_DIR_SMALL_FILES)
    os.makedirs(OUTPUT_DIR_SMALL_FILES)
    print(f"  -> [main] 已创建干净的输出目录: {OUTPUT_DIR_SMALL_FILES}")

    search_pattern = os.path.join(INPUT_BASE_DIR, "**", "*.parquet")
    file_list = glob.glob(search_pattern, recursive=True)
    
    if not file_list:
        print("\n❌ [main] 致命错误: 在所有下载产物中未找到任何 .parquet 文件！脚本终止。")
        exit(1)

    print(f"📦 [main] 共找到 {len(file_list)} 个股票的 Parquet 文件，开始收集...")
    
    for src_path in tqdm(file_list, desc="正在收集中"):
        # ... (收集文件的 try...except 逻辑) ...
            
    print(f"\n✅ [main] 全部 {len(file_list)} 个文件已成功收集到 '{OUTPUT_DIR_SMALL_FILES}' 目录中。")

    # --- 阶段 2: 创建一个经过优化的合并大文件 ---
    print("\n" + "="*50)
    print("🚀 [main] 开始创建经过压缩优化的合并文件...")
    
    all_parquet_files = glob.glob(os.path.join(OUTPUT_DIR_SMALL_FILES, "*.parquet"))
    
    if not all_parquet_files:
        print("❌ [main] 错误: 在收集目录中未找到 Parquet 文件，无法创建合并文件。脚本终止。")
        return
        
    print(f"📦 [main] 正在读取 {len(all_parquet_files)} 个 Parquet 文件...")
    all_dfs = [pd.read_parquet(f) for f in tqdm(all_parquet_files, desc="正在读取")]
    
    print("... [main] 正在合并所有数据 ...")
    merged_df = pd.concat(all_dfs, ignore_index=True)
    print(f"... [main] 合并完成，DataFrame 形状: {merged_df.shape}")

    print("... [main] 正在进行强制数据类型转换 ...")
    # ... (数据类型转换逻辑) ...
    print("✅ [main] 数据类型转换完成。")
    
    print(f"... [main] 正在按股票代码 ('code') 排序...")
    sorted_df = merged_df.sort_values(by='code', ascending=True).reset_index(drop=True)
    
    output_path = FINAL_PARQUET_FILE
    print(f"... [main] 正在将排序后的数据写入最终的合并文件: {output_path} ...")
    
    # (保持 to_parquet 的 try...except 逻辑不变)
    try:
        sorted_df.to_parquet(output_path, index=False, compression='zstd', row_group_size=100000)
        print("\n✅ [main] 最终合并文件创建成功 (使用 zstd 压缩)！")
    except ImportError:
        # ...
    
    # --- 阶段 3: 运行数据质量检查 ---
    print("\n--- [main] 准备调用 run_quality_check 函数 ---")
    if sorted_df is not None and not sorted_df.empty:
        run_quality_check(sorted_df)
    else:
        print("\n⚠️ [main] 警告: 合并后的数据 (sorted_df) 为空，跳过质量检查。")
        
    print("\n--- [main] 函数执行完毕 ---")

if __name__ == "__main__":
    # (重要) main 函数也应该被包裹在 try...except 中，以捕获任何未预料的顶层错误
    try:
        main()
    except Exception as e:
        print(f"\n❌❌❌ 在 main 函数顶层捕获到致命异常: {e} ❌❌❌")
        import traceback
        traceback.print_exc()
        exit(1)
