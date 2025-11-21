import os
import random
import json
import time
import uuid
import pandas as pd
import numpy as np
from datetime import datetime, timedelta

BASE_DIR = "./"
START_DATE_STR = "20251120"
TOTAL_DAYS = 5
TOTAL_FILES = 100

CATEGORIES = ["sales", "marketing", "logs", "users"]
SUB_CATEGORIES = ["raw", "processed", "errors", "archive"]

def generate_csv(filepath, rows=5000):
    df = pd.DataFrame(np.random.randint(0, 1000, size=(rows, 5)), columns=list('ABCDE'))
    df['uuid'] = [str(uuid.uuid4()) for _ in range(rows)]
    df.to_csv(filepath, index=False)

def generate_json(filepath):
    data = [{"id": i, "val": random.random()} for i in range(1000)]
    with open(filepath, 'w') as f:
        json.dump(data, f)

def generate_parquet(filepath):
    df = pd.DataFrame({'A': np.random.rand(5000), 'B': np.random.rand(5000)})
    df.to_parquet(filepath)

def generate_large_file(filepath, size_mb=1024):
    print(f"   -> Generating LARGE FILE ({size_mb}MB): {filepath} ...")
    with open(filepath, "wb") as f:
        f.seek(size_mb * 1024 * 1024 - 1)
        f.write(b"\0")

def main():
    print(f"Starting complex data generation at: {os.path.abspath(BASE_DIR)}")
    
    date_folders = []
    start_date = datetime.strptime(START_DATE_STR, "%Y%m%d")
    for i in range(TOTAL_DAYS):
        day_str = (start_date + timedelta(days=i)).strftime("%Y%m%d")
        date_folders.append(day_str)

    file_types = ['csv', 'json', 'parquet']
    
    for i in range(TOTAL_FILES):
        day_folder = random.choice(date_folders)
        
        depth = random.choice([0, 1, 2])
        path_parts = [BASE_DIR, day_folder]
        
        if depth >= 1: path_parts.append(random.choice(CATEGORIES))
        if depth >= 2: path_parts.append(random.choice(SUB_CATEGORIES))
        
        full_dir_path = os.path.join(*path_parts)
        os.makedirs(full_dir_path, exist_ok=True)
        
        ftype = random.choice(file_types)
        filename = f"data_{i:03d}_{uuid.uuid4().hex[:6]}.{ftype}"
        full_path = os.path.join(full_dir_path, filename)
        
        if ftype == 'csv': generate_csv(full_path)
        elif ftype == 'json': generate_json(full_path)
        elif ftype == 'parquet': generate_parquet(full_path)
        
        rel_path = os.path.relpath(full_path, BASE_DIR)
        print(f"[{i+1}/{TOTAL_FILES}] {rel_path}")

    target_day = random.choice(date_folders)
    large_file_path = os.path.join(BASE_DIR, target_day, "HEAVY_DATA_1GB.bin")
    generate_large_file(large_file_path, size_mb=1024)

    print("\nData Generation Completed!")
    print(f" - Total Files: {TOTAL_FILES + 1}")
    print(f" - Includes 1 file of 1GB for streaming test.")

if __name__ == "__main__":
    main()