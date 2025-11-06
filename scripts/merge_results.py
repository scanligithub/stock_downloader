import pandas as pd
import glob
import os
from tqdm import tqdm

input_dir = "data"
output_file = os.path.join(input_dir, "merged_all.csv")
csv_files = glob.glob(os.path.join(input_dir, "*.csv"))

if not csv_files:
    print("❌ 未找到任何 CSV 文件。")
    exit(1)

print(f"📦 共找到 {len(csv_files)} 个文件，开始合并...")

frames = []
for f in tqdm(csv_files):
    try:
        frames.append(pd.read_csv(f))
    except Exception as e:
        print(f"⚠️ 读取失败 {f}: {e}")

merged = pd.concat(frames, ignore_index=True)
merged.to_csv(output_file, index=False)
print(f"✅ 合并完成，输出文件：{output_file}")
