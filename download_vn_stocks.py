#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Script đơn giản để download dữ liệu lịch sử của tất cả mã chứng khoán Việt Nam
Sử dụng thư viện vnstock
"""
import os
import sys
import argparse
from pathlib import Path
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Optional
import pandas as pd
from tqdm import tqdm
import time
import random
import re
import threading

# Set encoding
os.environ['PYTHONIOENCODING'] = 'utf-8'

# Global lock để đảm bảo chỉ có 1 request khi gặp rate limit
rate_limit_lock = threading.Lock()
last_request_time = 0
min_request_interval = 2.0  # Tối thiểu 2 giây giữa các request


def get_all_symbols():
    """
    Lấy danh sách tất cả mã chứng khoán Việt Nam
    """
    try:
        # Import local để tránh circular import
        from vnstock import Listing
        
        listing = Listing()
        df = listing.all_symbols()
        
        if df is None or df.empty:
            print("[WARNING] Không lấy được danh sách mã từ Listing")
            return []
        
        # Lấy cột symbol hoặc ticker
        if 'symbol' in df.columns:
            symbols = df['symbol'].dropna().unique().tolist()
        elif 'ticker' in df.columns:
            symbols = df['ticker'].dropna().unique().tolist()
        else:
            print(f"[WARNING] Không tìm thấy cột symbol/ticker. Các cột: {df.columns.tolist()}")
            return []
        
        print(f"[INFO] Tìm thấy {len(symbols)} mã chứng khoán")
        return sorted(symbols)
    
    except Exception as e:
        print(f"[ERROR] Lỗi khi lấy danh sách mã: {e}")
        return []


def extract_wait_time(error_msg: str) -> int:
    """
    Trích xuất thời gian chờ từ thông báo lỗi rate limit
    Ví dụ: "Vui lòng thử lại sau 10 giây" -> 10
    """
    # Tìm pattern "sau X giây" hoặc "after X seconds"
    patterns = [
        r'sau\s+(\d+)\s+giây',
        r'after\s+(\d+)\s+seconds',
        r'(\d+)\s+giây',
        r'(\d+)\s+seconds'
    ]
    for pattern in patterns:
        match = re.search(pattern, error_msg, re.IGNORECASE)
        if match:
            return int(match.group(1))
    return None


def wait_with_countdown(wait_time: int, symbol: str = ""):
    """
    Chờ với countdown hiển thị
    """
    if wait_time <= 0:
        return
    
    print(f"\n[INFO] {'Rate limit cho ' + symbol + ': ' if symbol else ''}Chờ {wait_time} giây...")
    
    # Hiển thị countdown mỗi giây
    for remaining in range(wait_time, 0, -1):
        mins, secs = divmod(remaining, 60)
        time_str = f"{mins:02d}:{secs:02d}" if mins > 0 else f"{secs:02d}s"
        print(f"  ⏳ Còn {time_str}...", end='\r', flush=True)
        time.sleep(1)
    
    print(f"  ✅ Đã chờ xong, tiếp tục...{' ' * 20}")


def download_symbol_data(symbol: str, start_date: str, end_date: str, output_dir: Path, max_retries: int = 5) -> tuple:
    """
    Download dữ liệu lịch sử cho một mã chứng khoán với retry logic
    Tự động chờ theo thông báo rate limit và retry
    Trả về (symbol, success, message, row_count)
    """
    global last_request_time, min_request_interval
    
    for attempt in range(max_retries):
        try:
            # Đảm bảo khoảng cách tối thiểu giữa các request
            with rate_limit_lock:
                elapsed = time.time() - last_request_time
                if elapsed < min_request_interval:
                    wait_needed = min_request_interval - elapsed
                    time.sleep(wait_needed)
                last_request_time = time.time()
            
            # Import local để tránh circular import
            from vnstock import Quote
            
            # Quote signature: Quote(source='vci', symbol='', ...)
            # Phải dùng keyword argument để tránh nhầm lẫn
            quote = Quote(symbol=symbol, source='VCI')
            df = quote.history(start=start_date, end=end_date)
            
            if df is None or df.empty:
                return (symbol, False, "Không có dữ liệu", 0)
            
            # Lưu file parquet
            output_file = output_dir / f"{symbol}.parquet"
            df.to_parquet(output_file, index=False, engine='pyarrow')
            
            row_count = len(df)
            first_date = str(df.index[0]) if hasattr(df.index[0], '__str__') else str(df.index[0])
            last_date = str(df.index[-1]) if hasattr(df.index[-1], '__str__') else str(df.index[-1])
            
            return (symbol, True, f"OK: {first_date} -> {last_date}", row_count)
        
        except Exception as e:
            error_msg = str(e)
            
            # Kiểm tra rate limit error - mở rộng pattern matching
            is_rate_limit = (
                'rate limit' in error_msg.lower() or 
                'quá nhiều request' in error_msg.lower() or
                'too many request' in error_msg.lower() or
                'process terminated' in error_msg.lower()
            )
            
            if is_rate_limit:
                wait_time = extract_wait_time(error_msg)
                
                if wait_time is not None:
                    # Chờ đúng thời gian API yêu cầu + buffer 5 giây
                    wait_time = wait_time + 5
                    
                    if attempt < max_retries - 1:
                        # Chờ với countdown
                        wait_with_countdown(wait_time, symbol)
                        
                        # Cập nhật last_request_time sau khi chờ
                        with rate_limit_lock:
                            last_request_time = time.time()
                        
                        continue  # Retry
                    else:
                        return (symbol, False, f"Rate limit (đã retry {max_retries}x): chờ {wait_time}s", 0)
                else:
                    # Nếu không tìm thấy thời gian, dùng exponential backoff
                    wait_time = (2 ** attempt) * 15  # 15s, 30s, 60s, 120s
                    
                    if attempt < max_retries - 1:
                        wait_with_countdown(wait_time, symbol)
                        
                        # Cập nhật last_request_time sau khi chờ
                        with rate_limit_lock:
                            last_request_time = time.time()
                        
                        continue  # Retry
                    else:
                        return (symbol, False, f"Rate limit (retried {max_retries}x): {error_msg[:80]}", 0)
            else:
                # Lỗi khác, không retry
                if len(error_msg) > 100:
                    error_msg = error_msg[:100] + "..."
                return (symbol, False, error_msg, 0)
    
    # Nếu đến đây nghĩa là đã hết retry
    return (symbol, False, f"Failed after {max_retries} attempts", 0)


def download_all_symbols(
    start_date: str = "1990-01-01",
    end_date: Optional[str] = None,
    output_dir: str = "data/eod_parquet",
    max_workers: int = 1,  # Giảm xuống 1 worker để tránh rate limit tốt hơn
    sleep: float = 2.0  # Tăng lên 2.0 giây để an toàn hơn
):
    """
    Download dữ liệu cho tất cả mã chứng khoán
    """
    if end_date is None:
        end_date = datetime.now().strftime("%Y-%m-%d")
    
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    
    print(f"[INFO] Bắt đầu download dữ liệu từ {start_date} đến {end_date}")
    print(f"[INFO] Thư mục output: {output_path.absolute()}")
    print(f"[INFO] Số worker: {max_workers}, Sleep: {sleep}s")
    
    # Lấy danh sách mã
    symbols = get_all_symbols()
    if not symbols:
        print("[ERROR] Không lấy được danh sách mã chứng khoán")
        return
    
    print(f"[INFO] Tổng số mã cần download: {len(symbols)}")
    
    # Kiểm tra các file đã tồn tại
    existing_files = set(f.stem for f in output_path.glob("*.parquet"))
    symbols_to_download = [s for s in symbols if s not in existing_files]
    
    if existing_files:
        print(f"[INFO] Đã có {len(existing_files)} file, còn {len(symbols_to_download)} mã cần download")
    
    if not symbols_to_download:
        print("[INFO] Tất cả mã đã được download")
        return
    
    # Download với ThreadPoolExecutor
    success_count = 0
    fail_count = 0
    total_rows = 0
    results = []
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Submit tasks
        futures = {
            executor.submit(download_symbol_data, symbol, start_date, end_date, output_path): symbol
            for symbol in symbols_to_download
        }
        
        # Process với progress bar
        with tqdm(total=len(symbols_to_download), desc="Downloading") as pbar:
            for future in as_completed(futures):
                symbol = futures[future]
                try:
                    result = future.result()
                    symbol, success, message, row_count = result
                    results.append(result)
                    
                    if success:
                        success_count += 1
                        total_rows += row_count
                        pbar.set_postfix({"OK": success_count, "FAIL": fail_count, "Current": symbol})
                    else:
                        fail_count += 1
                        # Kiểm tra nếu là rate limit error
                        if 'rate limit' in message.lower() or 'quá nhiều request' in message.lower():
                            pbar.set_postfix({"OK": success_count, "FAIL": fail_count, "RATE_LIMIT": "⚠️", "Current": symbol})
                        else:
                            pbar.set_postfix({"OK": success_count, "FAIL": fail_count, "Current": symbol})
                    
                    pbar.update(1)
                    
                    # Sleep với random jitter để tránh pattern
                    # Đảm bảo khoảng cách tối thiểu giữa các request
                    with rate_limit_lock:
                        elapsed = time.time() - last_request_time
                        if elapsed < sleep:
                            sleep_time = sleep - elapsed + random.uniform(0, 0.5)
                        else:
                            sleep_time = random.uniform(0, 0.5)
                        time.sleep(sleep_time)
                        last_request_time = time.time()
                
                except Exception as e:
                    fail_count += 1
                    error_msg = str(e)
                    
                    # Kiểm tra rate limit error
                    is_rate_limit = (
                        'rate limit' in error_msg.lower() or 
                        'quá nhiều request' in error_msg.lower() or
                        'too many request' in error_msg.lower() or
                        'process terminated' in error_msg.lower()
                    )
                    
                    if is_rate_limit:
                        # Nếu rate limit, chờ lâu hơn và retry
                        wait_time = extract_wait_time(error_msg) or 20
                        wait_time = wait_time + 5  # Thêm buffer
                        
                        # Chờ với countdown
                        wait_with_countdown(wait_time, symbol)
                        
                        # Cập nhật last_request_time
                        with rate_limit_lock:
                            last_request_time = time.time()
                        
                        # Retry download cho symbol này
                        retry_result = download_symbol_data(symbol, start_date, end_date, output_path)
                        symbol_retry, success_retry, message_retry, row_count_retry = retry_result
                        results.append(retry_result)
                        
                        if success_retry:
                            success_count += 1
                            total_rows += row_count_retry
                            fail_count -= 1  # Giảm fail_count vì đã retry thành công
                            pbar.set_postfix({"OK": success_count, "FAIL": fail_count, "Current": symbol})
                        else:
                            pbar.set_postfix({"OK": success_count, "FAIL": fail_count, "Current": symbol})
                    else:
                        results.append((symbol, False, error_msg, 0))
                        pbar.set_postfix({"OK": success_count, "FAIL": fail_count, "Current": symbol})
                    
                    pbar.update(1)
    
    # Tạo manifest
    manifest_file = output_path / "manifest.csv"
    manifest_data = []
    for symbol, success, message, row_count in results:
        manifest_data.append({
            "symbol": symbol,
            "status": "SUCCESS" if success else "FAILED",
            "message": message,
            "row_count": row_count,
            "file": f"{symbol}.parquet" if success else None
        })
    
    manifest_df = pd.DataFrame(manifest_data)
    manifest_df.to_csv(manifest_file, index=False, encoding='utf-8-sig')
    
    # Tạo failed list
    failed_symbols = [r[0] for r in results if not r[1]]
    if failed_symbols:
        failed_file = output_path / "failed.csv"
        failed_df = pd.DataFrame({"symbol": failed_symbols})
        failed_df.to_csv(failed_file, index=False, encoding='utf-8-sig')
        print(f"\n[INFO] Có {len(failed_symbols)} mã thất bại, xem {failed_file}")
    
    # Summary
    print(f"\n[SUMMARY]")
    print(f"  - Tổng số mã: {len(symbols)}")
    print(f"  - Thành công: {success_count}")
    print(f"  - Thất bại: {fail_count}")
    print(f"  - Tổng số dòng dữ liệu: {total_rows:,}")
    print(f"  - Manifest: {manifest_file}")
    print(f"  - Thư mục output: {output_path.absolute()}")


def main():
    parser = argparse.ArgumentParser(
        description="Download dữ liệu lịch sử tất cả mã chứng khoán Việt Nam"
    )
    parser.add_argument(
        "--start",
        type=str,
        default="1990-01-01",
        help="Ngày bắt đầu (YYYY-MM-DD), mặc định: 1990-01-01"
    )
    parser.add_argument(
        "--end",
        type=str,
        default=None,
        help="Ngày kết thúc (YYYY-MM-DD), mặc định: hôm nay"
    )
    parser.add_argument(
        "--out",
        type=str,
        default="data/eod_parquet",
        help="Thư mục output, mặc định: data/eod_parquet"
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=1,
        help="Số worker đồng thời, mặc định: 1 (để tránh rate limit tốt hơn)"
    )
    parser.add_argument(
        "--sleep",
        type=float,
        default=2.0,
        help="Thời gian sleep giữa các request (giây), mặc định: 2.0 (để tránh rate limit)"
    )
    
    args = parser.parse_args()
    
    try:
        download_all_symbols(
            start_date=args.start,
            end_date=args.end,
            output_dir=args.out,
            max_workers=args.workers,
            sleep=args.sleep
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

