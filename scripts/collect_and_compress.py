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
    print("🔍 开始进行数据质量检查 (Data Quality Check)...")
    
    if not pd.api.types.is_datetime64_any_dtype(df['date']):
        df['date'] = pd.to_datetime(df['date'])

    report = {}
    
    report['total_records'] = int(len(df))
    report['total_stocks'] = int(df['code'].nunique())
    report['start_date'] = df['date'].min().strftime('%Y-%m-%d')
    report['end_date'] = df['date'].max().strftime('%Y-%m-%d')
    
    try:
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
    except Exception as e:
        report['completeness_check'] = f"Error during check: {e}"

    report['accuracy_checks'] = {
        'negative_prices': int(df[(df['open'] < 0) | (df['high'] < 0) | (df['low'] < 0) | (df['close'] < 0)].shape[0]),
        'zero_prices_or_volume': int(df[(df['close'] <= 0) | (df['volume'] <= 0)].shape[0]),
        'high_lower_than_low': int(df[df['high'] < df['low']].shape[0]),
    }

    nan_counts = df.isnull().sum()
    report['nan_values_summary'] = nan_counts[nan_counts > 0].astype(int).to_dict()

    stock_lengths = df.groupby('code').size()
    report['distribution_stats'] = {
        'avg_records_per_stock': round(stock_lengths.mean(), 2),
        'median_records_per_stock': int(stock_lengths.median()),
        'stocks_over_15_years': int((stock_lengths > 250*15).sum()),
        'stocks_over_10_years': int((stock_lengths > 250*10).sum()),
        'stocks_over_5_years': int((stock_lengths > 250*5).sum()),
        'stocks_under_1_year': int((stock_lengths < 250*1).sum())
    }

    print("✅ 数据质量检查完成。")
    
    with open(QC_REPORT_FILE, 'w', encoding='utf-8') as f:
        json.dump(report, f, indent=2, ensure_ascii=False)
    print(f"📄 质检报告已保存到: {QC_REPORT_FILE}")
    
    print("\n--- 数据质量简报 ---")
    print(f"  - 股票总数: {report.get('total_stocks', 'N/A')}")
    print(f"  - 总记录数: {report.get('total_records', 'N/A'):,}")
    # ... (其他简报打印) ...

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
    
    if not file_list:
        print("\n" + "="*60)
        print("❌ 致命错误: 在所有下载产物中，未找到任何 .parquet 文件！")
        # ... (错误提示)
        exit(1)

    print(f"📦 共找到 {len(file_list)} 个股票的 Parquet 文件，开始收集...")
    
    for src_path in tqdm(file_list, desc="正在收集中"):
        try:
            filename = os.path.basename(src_path)
            dest_path = os.path.join(OUTPUT_DIR_SMALL_FILES, filename)
            shutil.copy2(src_path, dest_path)
        except Exception as e:
            print(f"\n⚠️ 复制文件 {src_path} 失败: {e}")
            
    print(f"\n✅ 全部 {len(file_list)} 个文件已成功收集到 '{OUTPUT_DIR_SMALL_FILES}' 目录中。")

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

    # --- (这是唯一的、关键的修正) ---
    if 'date' in merged_df.columns:
        # 将 date (变量) 修改为 'date' (字符串)
        merged_df['date'] = pd.to_datetime(merged_df['date'], errors='coerce')
    # ------------------------------------
    print("✅ 数据类型转换完成。")
    
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

    # --- 阶段 3: 运行数据质量检查 ---
    if not sorted_df.empty:
        run_quality_check(sorted_df)
    else:
        print("\n⚠️ 合并后的数据为空，跳过质量检查。")

if __name__ == "__main__":
    main()
