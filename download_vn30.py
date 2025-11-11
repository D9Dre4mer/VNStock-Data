#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Script để download dữ liệu lịch sử của chỉ số VN30
Sử dụng thư viện vnstock
"""
import os
import sys
import argparse
from pathlib import Path
from datetime import datetime
from typing import Optional
import time
import re
import threading
import types
import importlib
import json

# Set encoding
os.environ['PYTHONIOENCODING'] = 'utf-8'

# Global lock để đảm bảo chỉ có 1 request khi gặp rate limit
rate_limit_lock = threading.Lock()
last_request_time = 0
min_request_interval = 2.0  # Tối thiểu 2 giây giữa các request


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
    
    prefix = f"Rate limit cho {symbol}: " if symbol else ""
    print(f"\n[INFO] {prefix}Chờ {wait_time} giây...")
    
    # Hiển thị countdown mỗi giây
    for remaining in range(wait_time, 0, -1):
        mins, secs = divmod(remaining, 60)
        time_str = f"{mins:02d}:{secs:02d}" if mins > 0 else f"{secs:02d}s"
        print(f"  ⏳ Còn {time_str}...", end='\r', flush=True)
        time.sleep(1)
    
    print(f"  ✅ Đã chờ xong, tiếp tục...{' ' * 20}")


def setup_vnstock_environment():
    """
    Setup môi trường và patch vnai để tránh circular import
    """
    # Set HOME nếu chưa có (Windows)
    home_dir = Path.home()
    if 'HOME' not in os.environ:
        os.environ['HOME'] = str(home_dir)
    
    # Tạo thư mục .vnstock nếu chưa có
    vnstock_dir = home_dir / '.vnstock'
    vnstock_dir.mkdir(exist_ok=True)
    
    # Clear các module liên quan để tránh circular import
    modules_to_clear = [
        'vnstock', 'vnstock3', 'vnai',
        'vnstock.core', 'vnai.flow', 'vnai.flow.relay',
        'vnai.scope', 'vnai.scope.profile'
    ]
    for mod_name in modules_to_clear:
        if mod_name in sys.modules:
            del sys.modules[mod_name]
    
    # Patch vnai trước khi import vnstock để tránh circular import
    # Tạo thư mục id và file environment.json nếu chưa có
    id_dir = vnstock_dir / 'id'
    id_dir.mkdir(exist_ok=True)
    env_file = id_dir / 'environment.json'
    if not env_file.exists():
        env_data = {
            "accepted": True,
            "accepted_agreement": True,
            "timestamp": datetime.now().isoformat()
        }
        with open(env_file, 'w', encoding='utf-8') as f:
            json.dump(env_data, f, indent=2)
    
    # Tạo vnai module chính
    mock_vnai = types.ModuleType('vnai')
    mock_vnai.setup = lambda: None  # No-op setup
    mock_vnai.accept_license_terms = lambda: True  # Accept license
    
    # Tạo optimize_execution decorator
    def optimize_execution(*args, **kwargs):
        if args and callable(args[0]):
            # Used as @optimize_execution (no parens)
            return args[0]
        else:
            # Used as @optimize_execution() (with parens)
            class OptimizeExecution:
                def __init__(self, *args, **kwargs):
                    pass
                def __call__(self, func):
                    return func
            return OptimizeExecution(*args, **kwargs)
    mock_vnai.optimize_execution = optimize_execution
    
    sys.modules['vnai'] = mock_vnai
    
    # Tạo vnai.scope module
    mock_scope = types.ModuleType('vnai.scope')
    sys.modules['vnai.scope'] = mock_scope
    mock_vnai.scope = mock_scope
    
    # Tạo vnai.scope.profile module với Inspector mock
    mock_profile = types.ModuleType('vnai.scope.profile')
    
    # Tạo Inspector class mock
    class MockInspector:
        def __init__(self):
            self.home_dir = home_dir
            self.project_dir = vnstock_dir
        
        def fingerprint(self):
            # Return a dummy fingerprint
            import hashlib
            machine_id = hashlib.md5(str(home_dir).encode()).hexdigest()
            return machine_id
    
    mock_profile.Inspector = MockInspector
    mock_profile.inspector = MockInspector()
    sys.modules['vnai.scope.profile'] = mock_profile
    mock_scope.profile = mock_profile


def download_vn30_data(
    start_date: str,
    end_date: str,
    output_dir: Path,
    max_retries: int = 5
) -> tuple:
    """
    Download dữ liệu lịch sử cho VN30 với retry logic
    Tự động chờ theo thông báo rate limit và retry
    Trả về (symbol, success, message, row_count)
    """
    global last_request_time, min_request_interval
    symbol = "VN30"
    
    for attempt in range(max_retries):
        try:
            # Đảm bảo khoảng cách tối thiểu giữa các request
            with rate_limit_lock:
                elapsed = time.time() - last_request_time
                if elapsed < min_request_interval:
                    wait_needed = min_request_interval - elapsed
                    time.sleep(wait_needed)
                last_request_time = time.time()
            
            # Import vnstock sau khi đã patch vnai
            vnstock_module = importlib.import_module('vnstock')
            Quote = getattr(vnstock_module, 'Quote')
            
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
            first_date = str(df.index[0]) if hasattr(
                df.index[0], '__str__') else str(df.index[0])
            last_date = str(df.index[-1]) if hasattr(
                df.index[-1], '__str__') else str(df.index[-1])
            
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
                        msg = f"Rate limit (retried {max_retries}x): {error_msg[:80]}"
                        return (symbol, False, msg, 0)
            else:
                # Lỗi khác, không retry
                if len(error_msg) > 100:
                    error_msg = error_msg[:100] + "..."
                return (symbol, False, error_msg, 0)
    
    # Nếu đến đây nghĩa là đã hết retry
    return (symbol, False, f"Failed after {max_retries} attempts", 0)


def download_vn30(
    start_date: str = "1990-01-01",
    end_date: Optional[str] = None,
    output_dir: str = "data/eod_parquet",
    sleep: float = 2.0,
    force: bool = False
):
    """
    Download dữ liệu VN30
    """
    # Setup môi trường và patch vnai trước
    print("[INFO] Đang setup môi trường vnstock...")
    setup_vnstock_environment()
    print("[INFO] Setup hoàn tất")
    
    if end_date is None:
        end_date = datetime.now().strftime("%Y-%m-%d")
    
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    
    print(f"[INFO] Bắt đầu download VN30 từ {start_date} đến {end_date}")
    print(f"[INFO] Thư mục output: {output_path.absolute()}")
    print(f"[INFO] Sleep: {sleep}s")
    
    # Kiểm tra file đã tồn tại
    output_file = output_path / "VN30.parquet"
    if output_file.exists() and not force:
        print(f"[INFO] File đã tồn tại: {output_file}")
        response = input("[INFO] Bạn có muốn tải lại? (y/n): ").strip().lower()
        if response != 'y':
            print("[INFO] Bỏ qua download")
            return
    
    # Download
    print("[INFO] Đang download VN30...")
    result = download_vn30_data(start_date, end_date, output_path)
    symbol, success, message, row_count = result

    if success:
        print("\n[SUCCESS] Download thành công!")
        print(f"  - Symbol: {symbol}")
        print(f"  - Số dòng: {row_count:,}")
        print(f"  - Thông tin: {message}")
        print(f"  - File: {output_file.absolute()}")
    else:
        print("\n[ERROR] Download thất bại!")
        print(f"  - Symbol: {symbol}")
        print(f"  - Lỗi: {message}")
        sys.exit(1)


def main():
    parser = argparse.ArgumentParser(
        description="Download dữ liệu lịch sử chỉ số VN30"
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
        "--sleep",
        type=float,
        default=2.0,
        help="Thời gian sleep (giây), mặc định: 2.0"
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Tải lại ngay cả khi file đã tồn tại"
    )
    
    args = parser.parse_args()
    
    try:
        download_vn30(
            start_date=args.start,
            end_date=args.end,
            output_dir=args.out,
            sleep=args.sleep,
            force=args.force
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

