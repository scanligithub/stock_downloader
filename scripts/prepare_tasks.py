# scripts/prepare_tasks.py

import json
import random
import os

# --- 配置 ---
STOCK_LIST_FILE = "stock_list.json" # 从仓库根目录读取
TASK_COUNT = 20 # 与你的 matrix 中的作业总数保持一致
OUTPUT_DIR = "task_slices" # 存放分片文件的临时目录

def main():
    print("🚀 开始准备并行下载任务...")
    
    # 确保输出目录干净
    if not os.path.exists(OUTPUT_DIR):
        os.makedirs(OUTPUT_DIR)

    # 1. 加载完整的股票列表
    try:
        with open(STOCK_LIST_FILE, "r", encoding="utf-8") as f:
            stocks = json.load(f)
        print(f"  -> 成功加载 {len(stocks)} 支股票。")
    except FileNotFoundError:
        print(f"❌ 致命错误: 未在仓库根目录下找到 {STOCK_LIST_FILE} 文件！")
        exit(1)

    # 2. (核心) 随机打乱列表顺序
    random.shuffle(stocks)
    print("  -> 🃏 已将股票列表随机打乱 (洗牌完成)。")

    # 3. 将打乱后的列表，平均切分成 TASK_COUNT 份
    chunk_size = (len(stocks) + TASK_COUNT - 1) // TASK_COUNT
    print(f"  -> 每个任务分片包含约 {chunk_size} 支股票。")
    
    for i in range(TASK_COUNT):
        subset = stocks[i * chunk_size : (i + 1) * chunk_size]
        
        slice_filename = f"task_slice_{i}.json"
        slice_filepath = os.path.join(OUTPUT_DIR, slice_filename)
        
        with open(slice_filepath, "w", encoding="utf-8") as f:
            json.dump(subset, f, ensure_ascii=False) # 紧凑格式，减小体积
            
    print(f"\n✅ 成功生成 {TASK_COUNT} 个随机任务分片文件，存放在 '{OUTPUT_DIR}' 目录中。")

if __name__ == "__main__":
    main()
