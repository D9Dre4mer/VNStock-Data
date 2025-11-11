#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Script để gộp tất cả file parquet thành một file CSV
"""
import os
import sys
import argparse
from pathlib import Path
import pandas as pd
from tqdm import tqdm

# Set encoding
os.environ['PYTHONIOENCODING'] = 'utf-8'


def merge_parquet_to_csv(
    input_dir: str = "data/eod_parquet",
    output_file: str = "data/all_stocks.csv",
    add_symbol_column: bool = True
):
    """
    Gộp tất cả file parquet trong thư mục thành một file CSV
    
    Args:
        input_dir: Thư mục chứa các file parquet
        output_file: File CSV output
        add_symbol_column: Có thêm cột symbol không (lấy từ tên file)
    """
    input_path = Path(input_dir)
    output_path = Path(output_file)
    
    # Tạo thư mục output nếu chưa có
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    # Tìm tất cả file parquet
    parquet_files = list(input_path.glob("*.parquet"))
    
    if not parquet_files:
        print(f"[ERROR] Không tìm thấy file parquet nào trong {input_path}")
        return
    
    print(f"[INFO] Tìm thấy {len(parquet_files)} file parquet")
    print(f"[INFO] Bắt đầu gộp thành {output_path}")
    
    all_data = []
    failed_files = []
    
    # Đọc từng file
    for parquet_file in tqdm(parquet_files, desc="Reading files"):
        try:
            df = pd.read_parquet(parquet_file)
            
            if df.empty:
                continue
            
            # Thêm cột symbol nếu cần
            if add_symbol_column:
                symbol = parquet_file.stem  # Lấy tên file không có extension
                df['symbol'] = symbol
            
            all_data.append(df)
        
        except Exception as e:
            failed_files.append((parquet_file.name, str(e)))
            print(f"\n[WARNING] Lỗi khi đọc {parquet_file.name}: {e}")
    
    if not all_data:
        print("[ERROR] Không có dữ liệu nào để gộp")
        return
    
    # Gộp tất cả DataFrame
    print("[INFO] Đang gộp dữ liệu...")
    combined_df = pd.concat(all_data, ignore_index=True)
    
    # Sắp xếp theo symbol và date (nếu có)
    if 'symbol' in combined_df.columns:
        if 'time' in combined_df.columns:
            combined_df = combined_df.sort_values(['symbol', 'time'])
        elif 'date' in combined_df.columns:
            combined_df = combined_df.sort_values(['symbol', 'date'])
        else:
            combined_df = combined_df.sort_values('symbol')
    
    # Lưu file CSV
    print(f"[INFO] Đang lưu vào {output_path}...")
    combined_df.to_csv(
        output_path,
        index=False,
        encoding='utf-8-sig'  # UTF-8 với BOM để Excel đọc được
    )
    
    # Thống kê
    print(f"\n[SUMMARY]")
    print(f"  - Tổng số file đọc: {len(parquet_files)}")
    print(f"  - File thành công: {len(all_data)}")
    print(f"  - File thất bại: {len(failed_files)}")
    print(f"  - Tổng số dòng dữ liệu: {len(combined_df):,}")
    print(f"  - Số cột: {len(combined_df.columns)}")
    print(f"  - Các cột: {', '.join(combined_df.columns.tolist())}")
    print(f"  - File output: {output_path.absolute()}")
    
    if failed_files:
        print(f"\n[WARNING] Các file thất bại:")
        for file_name, error in failed_files[:10]:  # Hiển thị tối đa 10 file
            print(f"  - {file_name}: {error[:80]}")
        if len(failed_files) > 10:
            print(f"  ... và {len(failed_files) - 10} file khác")


def main():
    parser = argparse.ArgumentParser(
        description="Gộp tất cả file parquet thành một file CSV"
    )
    parser.add_argument(
        "--input",
        type=str,
        default="data/eod_parquet",
        help="Thư mục chứa các file parquet, mặc định: data/eod_parquet"
    )
    parser.add_argument(
        "--output",
        type=str,
        default="data/all_stocks.csv",
        help="File CSV output, mặc định: data/all_stocks.csv"
    )
    parser.add_argument(
        "--no-symbol",
        action="store_true",
        help="Không thêm cột symbol (lấy từ tên file)"
    )
    
    args = parser.parse_args()
    
    try:
        merge_parquet_to_csv(
            input_dir=args.input,
            output_file=args.output,
            add_symbol_column=not args.no_symbol
        )
    except KeyboardInterrupt:
        print("\n[INFO] Đã dừng bởi người dùng")
        sys.exit(0)
    except Exception as e:
        print(f"\n[ERROR] Lỗi: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()

