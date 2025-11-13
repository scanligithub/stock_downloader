# scripts/collect_and_compress.py (带数据质检功能)

import pandas as pd
import glob
import os
from tqdm import tqdm
import shutil
import json

# --- 配置 ---
INPUT_BASE_DIR = "all_data"
OUTPUT_DIR = "kdata"
FINAL_PARQUET_FILE = "full_kdata.parquet"
QC_REPORT_FILE = "data_quality_report.json" # 质检报告文件名

def run_quality_check(df):
    """
    对合并后的 DataFrame 进行数据质量检查，并生成报告。
    """
    print("\n" + "="*50)
    print("🔍 开始进行数据质量检查 (Data Quality Check)...")
    
    report = {}
    
    # 1. 基础统计
    report['total_records'] = len(df)
    report['total_stocks'] = df['code'].nunique()
    report['start_date'] = df['date'].min().strftime('%Y-%m-%d')
    report['end_date'] = df['date'].max().strftime('%Y-%m-%d')
    
    # 2. 完整性检查
    # 检查日期是否连续 (抽样检查一只长历史股票)
    long_history_stock = df.groupby('code').size().idxmax()
    df_single = df[df['code'] == long_history_stock].set_index('date').sort_index()
    missing_dates = pd.date_range(start=df_single.index.min(), end=df_single.index.max(), freq='B').difference(df_single.index)
    report['completeness_check'] = {
        'sample_stock': long_history_stock,
        'business_days_missing': len(missing_dates)
    }

    # 3. 准确性检查 (异常值)
    report['accuracy_checks'] = {
        'negative_prices': df[(df['open'] < 0) | (df['high'] < 0) | (df['low'] < 0) | (df['close'] < 0)].shape[0],
        'zero_prices': df[df['close'] <= 0].shape[0],
        'high_lower_than_low': df[df['high'] < df['low']].shape[0],
        'negative_volume': df[df['volume'] < 0].shape[0]
    }

    # 4. 空值检查
    nan_counts = df.isnull().sum()
    report['nan_values'] = nan_counts[nan_counts > 0].to_dict()

    # 5. 分布统计
    stock_lengths = df.groupby('code').size()
    report['distribution_stats'] = {
        'avg_records_per_stock': round(stock_lengths.mean(), 2),
        'median_records_per_stock': stock_lengths.median(),
        'stocks_over_10_years': (stock_lengths > 250*10).sum(),
        'stocks_over_5_years': (stock_lengths > 250*5).sum(),
        'stocks_under_1_year': (stock_lengths < 250*1).sum()
    }

    print("✅ 数据质量检查完成。")
    
    # 将报告保存为 JSON 文件
    with open(QC_REPORT_FILE, 'w', encoding='utf-8') as f:
        json.dump(report, f, indent=2, ensure_ascii=False)
    print(f"📄 质检报告已保存到: {QC_REPORT_FILE}")
    
    # 在日志中打印一份简报
    print("\n--- 数据质量简报 ---")
    print(f"  - 总记录数: {report['total_records']:,}")
    print(f"  - 股票总数: {report['total_stocks']}")
    print(f"  - 数据区间: {report['start_date']} to {report['end_date']}")
    print(f"  - 异常数据点 (价格<=0): {report['accuracy_checks']['zero_prices']}")
    print(f"  - 异常数据点 (高<低): {report['accuracy_checks']['high_lower_than_low']}")
    print(f"  - 数据超过10年的股票数: {report['distribution_stats']['stocks_over_10_years']}")
    print("----------------------")
    
    # 如果发现严重问题，可以考虑让脚本失败
    if report['accuracy_checks']['zero_prices'] > 0 or report['accuracy_checks']['high_lower_than_low'] > 0:
        print("⚠️ 警告: 发现严重的准确性问题！")
        # exit(1) # 可以取消注释，让工作流在发现问题时失败


def main():
    # ... (收集文件的部分保持不变) ...
    # ... (合并、排序、压缩的部分也保持不变) ...
    
    # (关键) 在所有文件操作完成后，加载最终的排序后 DataFrame，进行质检
    # 为了效率，我们直接使用内存中的 sorted_df
    # 如果 sorted_df 存在且不为空，则执行质检
    # sorted_df = ... 
    
    # 为了让逻辑清晰，我们把之前的代码整合进来
    
    # 阶段 1: 收集
    if os.path.exists(OUTPUT_DIR): shutil.rmtree(OUTPUT_DIR)
    os.makedirs(OUTPUT_DIR)
    search_pattern = os.path.join(INPUT_BASE_DIR, "**", "*.parquet")
    file_list = glob.glob(search_pattern, recursive=True)
    if not file_list: return
    print(f"📦 共找到 {len(file_list)} 个股票文件，开始收集...")
    for src_path in tqdm(file_list, desc="收集中"):
        shutil.copy2(src_path, os.path.join(OUTPUT_DIR, os.path.basename(src_path)))
    print(f"✅ 文件收集完成。")

    # 阶段 2: 合并、排序、压缩
    print("\n🚀 开始创建合并文件...")
    all_parquet_files = glob.glob(os.path.join(OUTPUT_DIR, "*.parquet"))
    if not all_parquet_files: return
    all_dfs = [pd.read_parquet(f) for f in tqdm(all_parquet_files, desc="读取中")]
    merged_df = pd.concat(all_dfs, ignore_index=True)
    
    # (确保 date 列是 datetime 类型，以便进行质检)
    merged_df['date'] = pd.to_datetime(merged_df['date'])

    print(f"... 正在按股票代码排序...")
    sorted_df = merged_df.sort_values(by='code', ascending=True).reset_index(drop=True)
    
    print(f"... 正在写入最终文件: {FINAL_PARQUET_FILE} ...")
    sorted_df.to_parquet(FINAL_PARQUET_FILE, index=False, compression='zstd', row_group_size=100000)
    print("✅ 最终合并文件创建成功！")

    # (关键) 阶段 3: 运行数据质量检查
    run_quality_check(sorted_df)


if __name__ == "__main__":
    main()
