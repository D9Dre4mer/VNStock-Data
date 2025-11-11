#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Script để lấy danh sách các mã chứng khoán đang còn giao dịch
trên 3 sàn HOSE, UPCOM, HNX và lưu vào file CSV
Có thể kiểm tra mã còn giao dịch bằng cách lấy dữ liệu giá gần đây
"""
import os
import sys
import argparse
import json
from pathlib import Path
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Tuple
import pandas as pd
import time
from tqdm import tqdm

# Set encoding
os.environ['PYTHONIOENCODING'] = 'utf-8'
# Fix stdout/stderr encoding for Windows
if sys.platform == 'win32':
    import io
    # Unbuffered output để hiển thị log ngay
    sys.stdout = io.TextIOWrapper(
        sys.stdout.buffer, encoding='utf-8', 
        errors='replace', line_buffering=True
    )
    sys.stderr = io.TextIOWrapper(
        sys.stderr.buffer, encoding='utf-8', 
        errors='replace', line_buffering=True
    )
    # Force flush
    sys.stdout.flush()
    sys.stderr.flush()



def infer_ecosystem_from_organ_name(organ_name: str, symbol: str) -> Optional[str]:
    """
    Suy luận ecosystem từ tên công ty và mã cổ phiếu
    ĐÃ TẮT: Chỉ sử dụng dữ liệu từ vietnam_stock_families.csv
    
    Args:
        organ_name: Tên công ty
        symbol: Mã cổ phiếu
    
    Returns:
        None (không suy luận, chỉ dùng từ CSV)
    """
    # Đã tắt hardcode mapping - chỉ sử dụng từ vietnam_stock_families.csv
    return None


def load_family_mapping(family_file: str = "vietnam_stock_families.csv") -> Dict[str, str]:
    """
    Load mapping từ file vietnam_stock_families.csv
    Tạo mapping từ symbol -> ecosystem name
    
    Args:
        family_file: Đường dẫn file CSV chứa thông tin họ/tập đoàn
    
    Returns:
        Dict với key là symbol (uppercase), value là ecosystem name
    """
    family_mapping = {}
    family_path = Path(family_file)
    
    if not family_path.exists():
        print(f"[WARNING] Không tìm thấy file {family_file}, bỏ qua mapping từ file này", flush=True)
        return family_mapping
    
    try:
        df = pd.read_csv(family_path, encoding='utf-8-sig')
        
        # Tìm các cột cần thiết
        family_col = None
        symbol_col = None
        
        # Tìm cột chứa tên họ/tập đoàn
        for col in df.columns:
            col_lower = str(col).lower()
            if 'họ' in col_lower or 'tập đoàn' in col_lower or 'family' in col_lower:
                family_col = col
                break
        
        # Tìm cột chứa mã niêm yết
        for col in df.columns:
            col_lower = str(col).lower()
            if 'mã' in col_lower or 'symbol' in col_lower or 'ticker' in col_lower or 'code' in col_lower:
                symbol_col = col
                break
        
        if not family_col or not symbol_col:
            print(f"[WARNING] Không tìm thấy cột cần thiết trong {family_file}", flush=True)
            return family_mapping
        
        # Xử lý từng dòng
        for _, row in df.iterrows():
            family_name = str(row[family_col]).strip()
            symbols_str = str(row[symbol_col]).strip()
            
            # Bỏ qua dòng header hoặc dòng trống
            if not family_name or family_name.startswith('#') or family_name == 'Họ/Tập đoàn':
                continue
            
            # Xử lý tên họ: loại bỏ "Họ " ở đầu nếu có, và normalize
            if family_name.startswith('Họ '):
                ecosystem_name = family_name[3:].strip().upper()  # Bỏ "Họ " và uppercase
            else:
                ecosystem_name = family_name.strip().upper()
            
            # Xử lý các mã: có thể có dấu ngoặc kép và dấu phẩy
            # Loại bỏ dấu ngoặc kép nếu có
            symbols_str = symbols_str.strip('"').strip("'").strip()
            
            # Tách các mã bằng dấu phẩy
            symbols = [s.strip().upper() for s in symbols_str.split(',') if s.strip()]
            
            # Tạo mapping cho từng mã
            for symbol in symbols:
                if symbol and len(symbol) == 3:  # Chỉ lấy mã 3 ký tự
                    family_mapping[symbol] = ecosystem_name
        
        print(f"[INFO] Đã load {len(family_mapping)} mapping từ {family_file}", flush=True)
        
    except Exception as e:
        print(f"[WARNING] Không đọc được file {family_file}: {e}", flush=True)
        import traceback
        traceback.print_exc()
    
    return family_mapping


def get_company_info(symbol: str) -> Dict[str, Optional[str]]:
    """
    Lấy thông tin công ty từ nguồn uy tín (vnstock Quote)
    
    Args:
        symbol: Mã chứng khoán cần lấy thông tin
    
    Returns:
        Dict với keys: exchange, industry, ecosystem
    """
    result = {
        'exchange': None,
        'industry': None,
        'ecosystem': None
    }
    
    try:
        from vnstock import Quote
        
        quote = Quote(symbol=symbol, source='VCI')
        
        # Thử các method khác nhau để lấy thông tin
        profile_data = None
        
        # Method 1: Thử profile()
        if hasattr(quote, 'profile'):
            try:
                profile_data = quote.profile()
                if profile_data is not None:
                    # Debug: log một vài lần đầu
                    if symbol in ['VIC', 'VCB', 'FPT', 'VNM']:
                        print(f"[DEBUG] {symbol}.profile() returned: {type(profile_data)}", flush=True)
                        if isinstance(profile_data, dict):
                            print(f"[DEBUG] {symbol} profile keys: {list(profile_data.keys())[:10]}", flush=True)
            except Exception as e:
                if symbol in ['VIC', 'VCB', 'FPT', 'VNM']:
                    print(f"[DEBUG] {symbol}.profile() error: {e}", flush=True)
                pass
        
        # Method 2: Thử company_info()
        if not profile_data and hasattr(quote, 'company_info'):
            try:
                profile_data = quote.company_info()
                if symbol in ['VIC', 'VCB', 'FPT', 'VNM']:
                    print(f"[DEBUG] {symbol}.company_info() returned: {type(profile_data)}", flush=True)
            except Exception as e:
                if symbol in ['VIC', 'VCB', 'FPT', 'VNM']:
                    print(f"[DEBUG] {symbol}.company_info() error: {e}", flush=True)
                pass
        
        # Method 3: Thử info()
        if not profile_data and hasattr(quote, 'info'):
            try:
                profile_data = quote.info()
                if symbol in ['VIC', 'VCB', 'FPT', 'VNM']:
                    print(f"[DEBUG] {symbol}.info() returned: {type(profile_data)}", flush=True)
            except Exception as e:
                if symbol in ['VIC', 'VCB', 'FPT', 'VNM']:
                    print(f"[DEBUG] {symbol}.info() error: {e}", flush=True)
                pass
        
        # Method 4: Thử lấy từ Listing với symbol cụ thể
        if not profile_data:
            try:
                from vnstock import Listing
                listing = Listing()
                # Thử lấy thông tin từ listing với symbol
                listing_df = listing.all_symbols()
                if listing_df is not None and not listing_df.empty and 'symbol' in listing_df.columns:
                    symbol_row = listing_df[listing_df['symbol'] == symbol]
                    if not symbol_row.empty:
                        profile_data = symbol_row.iloc[0].to_dict()
                        if symbol in ['VIC', 'VCB', 'FPT', 'VNM']:
                            print(f"[DEBUG] {symbol} from Listing: {list(profile_data.keys())[:10]}", flush=True)
            except Exception as e:
                if symbol in ['VIC', 'VCB', 'FPT', 'VNM']:
                    print(f"[DEBUG] {symbol} Listing error: {e}", flush=True)
                pass
        
        # Xử lý profile_data
        if profile_data:
            # Nếu là dict
            if isinstance(profile_data, dict):
                # Tìm exchange
                for key in ['exchange', 'comGroupCode', 'market', 'floor', 'organCode']:
                    if key in profile_data and profile_data[key]:
                        val = str(profile_data[key]).strip().upper()
                        exchange_map = {'HOSE': 'HOSE', 'HSX': 'HOSE', 
                                       'HNX': 'HNX', 'UPCOM': 'UPCOM'}
                        result['exchange'] = exchange_map.get(val, val)
                        break
                
                # Tìm industry
                for key in ['industry', 'sector', 'industryName', 'sectorName', 
                           'icb_name', 'icbName', 'nganh', 'nhom_nganh',
                           'organTypeName', 'organType']:
                    if key in profile_data and profile_data[key]:
                        result['industry'] = str(profile_data[key]).strip()
                        break
            
            # Nếu là DataFrame
            elif hasattr(profile_data, 'columns'):
                try:
                    # Tìm exchange trong các cột
                    for col in profile_data.columns:
                        col_lower = str(col).lower()
                        if any(x in col_lower for x in ['exchange', 'market', 'floor', 'comgroup']):
                            val = str(profile_data[col].iloc[0] if len(profile_data) > 0 else '').strip().upper()
                            if val:
                                exchange_map = {'HOSE': 'HOSE', 'HSX': 'HOSE', 
                                               'HNX': 'HNX', 'UPCOM': 'UPCOM'}
                                result['exchange'] = exchange_map.get(val, val)
                                break
                    
                    # Tìm industry trong các cột
                    for col in profile_data.columns:
                        col_lower = str(col).lower()
                        if any(x in col_lower for x in ['industry', 'sector', 'icb', 'nganh']):
                            val = str(profile_data[col].iloc[0] if len(profile_data) > 0 else '').strip()
                            if val:
                                result['industry'] = val
                                break
                except Exception:
                    pass
        
        # BỎ QUA việc lấy từ history vì quá chậm
        # Nếu không có profile, không lấy từ history (tránh chậm)
        
        # Suy luận exchange từ symbol pattern nếu vẫn chưa có
        if not result['exchange']:
            # Một số pattern có thể suy luận
            # Nhưng tốt nhất là để UNKNOWN nếu không chắc chắn
            pass
        
    except Exception:
        # Nếu lỗi, trả về None cho tất cả
        pass
    
    return result


def check_symbol_active(
    symbol: str,
    days_back: int = 90
) -> Tuple[bool, Optional[str]]:
    """
    Kiểm tra xem mã chứng khoán có còn giao dịch không
    bằng cách lấy dữ liệu giá trong khoảng thời gian gần đây
    
    Args:
        symbol: Mã chứng khoán cần kiểm tra
        days_back: Số ngày gần đây để kiểm tra (mặc định 90 ngày)
    
    Returns:
        Tuple[bool, Optional[str]]: (is_active, last_trade_date)
        - is_active: True nếu mã còn giao dịch, False nếu không
        - last_trade_date: Ngày giao dịch cuối cùng (YYYY-MM-DD) hoặc None
    """
    try:
        from vnstock import Quote
        
        # Tính ngày bắt đầu và kết thúc
        end_date = datetime.now()
        start_date = end_date - timedelta(days=days_back)
        
        # Format dates
        start_str = start_date.strftime("%Y-%m-%d")
        end_str = end_date.strftime("%Y-%m-%d")
        
        # Lấy dữ liệu giá
        quote = Quote(symbol=symbol, source='VCI')
        df = quote.history(start=start_str, end=end_str)
        
        if df is None or df.empty:
            return (False, None)
        
        # Lấy ngày giao dịch cuối cùng
        # DataFrame có thể có index là date hoặc có cột date
        if hasattr(df.index, 'max'):
            last_date = df.index.max()
            if isinstance(last_date, pd.Timestamp):
                last_trade_date = last_date.strftime("%Y-%m-%d")
            else:
                last_trade_date = str(last_date)
        elif 'date' in df.columns:
            last_trade_date = str(df['date'].max())
        else:
            # Nếu không có date, lấy dòng cuối cùng
            last_trade_date = None
        
        # Kiểm tra nếu có dữ liệu trong 90 ngày gần đây
        if last_trade_date:
            try:
                last_date_obj = datetime.strptime(
                    last_trade_date, "%Y-%m-%d"
                )
                days_since_last = (end_date - last_date_obj).days
                # Nếu có giao dịch trong 90 ngày gần đây thì coi là active
                is_active = days_since_last <= days_back
                return (is_active, last_trade_date)
            except (ValueError, TypeError):
                # Nếu parse date lỗi, nhưng có dữ liệu thì coi là active
                return (True, last_trade_date)
        
        # Có dữ liệu nhưng không parse được date
        return (True, None)
        
    except Exception:
        # Nếu lỗi khi lấy dữ liệu, coi như không active
        return (False, None)


def get_active_symbols(
    check_trading: bool = True,
    days_back: int = 90,
    fetch_company_info: bool = False,
    existing_data: Optional[Dict[str, Dict]] = None,
    output_file: Optional[str] = None,
    save_batch_size: int = 20
) -> List[Dict]:
    """
    Lấy danh sách các mã chứng khoán trên 3 sàn HOSE, UPCOM, HNX
    Chỉ lấy danh sách mã, không kiểm tra dữ liệu giá
    """
    print("[FUNC] get_active_symbols() bắt đầu...", flush=True)
    sys.stdout.flush()
    try:
        # Setup môi trường trước khi import vnstock
        print("[STEP 1] Importing modules...", flush=True)
        sys.stdout.flush()
        from pathlib import Path
        import importlib
        
        print("[STEP 2] Setting up environment...", flush=True)
        sys.stdout.flush()
        # Set HOME nếu chưa có (Windows)
        home_dir = Path.home()
        if 'HOME' not in os.environ:
            os.environ['HOME'] = str(home_dir)
        
        # Tạo thư mục .vnstock nếu chưa có
        vnstock_dir = home_dir / '.vnstock'
        vnstock_dir.mkdir(exist_ok=True)
        print(f"[STEP 3] Created vnstock_dir: {vnstock_dir}", flush=True)
        sys.stdout.flush()
        
        # Clear các module liên quan để tránh circular import
        print("[STEP 4] Clearing modules...", flush=True)
        sys.stdout.flush()
        modules_to_clear = [
            'vnstock', 'vnstock3', 'vnai', 
            'vnstock.core', 'vnai.flow', 'vnai.flow.relay',
            'vnai.scope', 'vnai.scope.profile'
        ]
        for mod_name in modules_to_clear:
            if mod_name in sys.modules:
                del sys.modules[mod_name]
        print("[STEP 5] Modules cleared", flush=True)
        sys.stdout.flush()
        
        # Patch vnai trước khi import vnstock để tránh circular import
        print("[STEP 6] Patching vnai...", flush=True)
        sys.stdout.flush()
        # Tạo đầy đủ cấu trúc package cho vnai
        import types
        
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
        print("[STEP 7] vnai patched", flush=True)
        sys.stdout.flush()
        
        # Sử dụng importlib để import động, tránh circular import
        print("[STEP 8] Importing vnstock module (có thể mất vài giây)...", flush=True)
        sys.stdout.flush()
        # Import vnstock module trước
        vnstock_module = importlib.import_module('vnstock')
        print("[STEP 9] vnstock imported successfully", flush=True)
        sys.stdout.flush()
        
        # Lấy Listing class từ module
        print("[STEP 10] Getting Listing class...", flush=True)
        sys.stdout.flush()
        Listing = getattr(vnstock_module, 'Listing')
        
        print("[STEP 11] Creating Listing instance...", flush=True)
        sys.stdout.flush()
        listing = Listing()
        
        # Load family mapping từ file CSV (ưu tiên cao nhất cho ecosystem)
        print("[STEP 11.5] Loading family mapping from vietnam_stock_families.csv...", flush=True)
        sys.stdout.flush()
        family_mapping = load_family_mapping("vietnam_stock_families.csv")
        if family_mapping:
            print(f"[INFO] Đã load {len(family_mapping)} ecosystem mapping từ vietnam_stock_families.csv", flush=True)
        else:
            print("[WARNING] Không load được family mapping, sẽ sử dụng suy luận", flush=True)
        
        # Lấy mapping exchange từ symbols_by_exchange() - CÁCH TỐT NHẤT
        print("[STEP 11] Getting exchange mapping from symbols_by_exchange()...", flush=True)
        sys.stdout.flush()
        exchange_symbol_map = {}
        try:
            if hasattr(listing, 'symbols_by_exchange'):
                sym_exch_df = listing.symbols_by_exchange()
                if sym_exch_df is not None and not sym_exch_df.empty:
                    print(f"[INFO] symbols_by_exchange() returned {len(sym_exch_df)} rows", flush=True)
                    print(f"[DEBUG] Columns: {sym_exch_df.columns.tolist()}", flush=True)
                    
                    # Tìm cột symbol và exchange
                    symbol_col = None
                    exchange_col = None
                    for col in ['symbol', 'ticker', 'code']:
                        if col in sym_exch_df.columns:
                            symbol_col = col
                            break
                    for col in ['exchange', 'market', 'floor', 'comGroupCode', 'exchange_name']:
                        if col in sym_exch_df.columns:
                            exchange_col = col
                            print(f"[INFO] Sử dụng cột exchange: {col}", flush=True)
                            break
                    
                    if symbol_col and exchange_col:
                        for _, row in sym_exch_df.iterrows():
                            symbol = str(row[symbol_col]).strip().upper()
                            if pd.isna(row[exchange_col]):
                                continue
                            exchange = str(row[exchange_col]).strip().upper()
                            # Normalize exchange values
                            exchange_map = {'HOSE': 'HOSE', 'HSX': 'HOSE', 
                                           'HNX': 'HNX', 'UPCOM': 'UPCOM'}
                            exchange = exchange_map.get(exchange, exchange)
                            if exchange in ['HOSE', 'HNX', 'UPCOM']:
                                exchange_symbol_map[symbol] = exchange
                        print(f"[INFO] ✓ Đã lấy được exchange cho {len(exchange_symbol_map)} mã từ symbols_by_exchange()", flush=True)
                    else:
                        print(f"[WARNING] Không tìm thấy cột symbol/exchange. Các cột: {sym_exch_df.columns.tolist()}", flush=True)
                else:
                    print("[WARNING] symbols_by_exchange() trả về None hoặc empty", flush=True)
            else:
                print("[WARNING] Listing không có method symbols_by_exchange()", flush=True)
        except Exception as e:
            print(f"[WARNING] Không lấy được từ symbols_by_exchange(): {e}", flush=True)
            import traceback
            traceback.print_exc()
        
        # Lấy mapping industry từ symbols_by_industries() - CÁCH TỐT NHẤT
        print("[STEP 12] Getting industry mapping from symbols_by_industries()...", flush=True)
        sys.stdout.flush()
        industry_symbol_map = {}
        try:
            sym_ind_df = listing.symbols_by_industries()
            if sym_ind_df is not None and not sym_ind_df.empty:
                print(f"[INFO] symbols_by_industries() returned {len(sym_ind_df)} rows", flush=True)
                print(f"[DEBUG] Columns: {sym_ind_df.columns.tolist()}", flush=True)
                
                # Tìm cột symbol
                symbol_col = None
                for col in ['symbol', 'ticker', 'code']:
                    if col in sym_ind_df.columns:
                        symbol_col = col
                        break
                
                if symbol_col:
                    # Tìm cột industry - ưu tiên icb_name3 (ngành cấp 3), sau đó icb_name2, icb_name4
                    industry_col = None
                    for col in ['icb_name3', 'icb_name2', 'icb_name4', 'industry', 'industry_name', 'industryName', 'icb_name', 'icbName', 'sector', 'sector_name']:
                        if col in sym_ind_df.columns:
                            industry_col = col
                            print(f"[INFO] Sử dụng cột industry: {col}", flush=True)
                            break
                    
                    if industry_col:
                        for _, row in sym_ind_df.iterrows():
                            symbol = str(row[symbol_col]).strip().upper()  # Normalize symbol
                            # Kiểm tra NaN trước khi convert sang string
                            if pd.isna(row[industry_col]):
                                continue
                            industry = str(row[industry_col]).strip()
                            # Kiểm tra kỹ hơn: loại bỏ NaN, None, empty, và 'nan' string
                            if industry and industry != 'nan' and industry.lower() != 'none' and len(industry) > 0:
                                industry_symbol_map[symbol] = industry
                                # Debug: log một vài entry đầu
                                if len(industry_symbol_map) <= 5:
                                    print(f"[DEBUG] Added to map: {symbol} -> {repr(industry)}", flush=True)
                        print(f"[INFO] ✓ Đã lấy được industry cho {len(industry_symbol_map)} mã từ symbols_by_industries()", flush=True)
                    else:
                        print(f"[WARNING] Không tìm thấy cột industry. Các cột: {sym_ind_df.columns.tolist()}", flush=True)
                else:
                    print(f"[WARNING] Không tìm thấy cột symbol. Các cột: {sym_ind_df.columns.tolist()}", flush=True)
            else:
                print("[WARNING] symbols_by_industries() trả về None hoặc empty", flush=True)
        except Exception as e:
            print(f"[WARNING] Không lấy được từ symbols_by_industries(): {e}", flush=True)
            import traceback
            traceback.print_exc()
            pass
        
        # Lấy DataFrame từ all_symbols để có đầy đủ thông tin
        print("[STEP 13] Calling all_symbols() (có thể mất vài giây)...", flush=True)
        sys.stdout.flush()
        df = listing.all_symbols()
        print(f"[STEP 14] DataFrame received, shape: {df.shape if df is not None else 'None'}", flush=True)
        sys.stdout.flush()
        
        if df is None or df.empty:
            print("[WARNING] Không lấy được danh sách mã từ Listing", flush=True)
            return []
        
        # Debug: In ra các cột có sẵn
        print(f"[DEBUG] Các cột trong DataFrame: {df.columns.tolist()}", flush=True)
        
        # Tìm cột symbol
        symbol_col = None
        if 'symbol' in df.columns:
            symbol_col = 'symbol'
        elif 'ticker' in df.columns:
            symbol_col = 'ticker'
        elif 'code' in df.columns:
            symbol_col = 'code'
        
        if not symbol_col:
            print(f"[WARNING] Không tìm thấy cột symbol. Các cột: {df.columns.tolist()}", flush=True)
            return []
        
        # Tìm cột industry/sector trong DataFrame (nếu chưa có từ symbols_by_industries)
        industry_col = None
        if not industry_symbol_map:  # Chỉ tìm trong DataFrame nếu chưa có từ symbols_by_industries
            possible_industry_cols = [
                'industry', 'sector', 'industry_name', 'sector_name',
                'industryName', 'sectorName', 'nganh', 'nhom_nganh',
                'icb_name', 'icbName', 'organ_type_name'
            ]
            for col in possible_industry_cols:
                if col in df.columns:
                    industry_col = col
                    print(f"[INFO] Tìm thấy cột nhóm ngành trong DataFrame: {col}", flush=True)
                    break
        
        # Tìm cột ecosystem/parent company/group
        ecosystem_col = None
        possible_ecosystem_cols = [
            'ecosystem', 'parent_company', 'parentCompany', 'group_name',
            'groupName', 'cong_ty_me', 'tap_doan', 'ecosystem_name',
            'organ_parent_name', 'parentOrganName', 'holding_company',
            'holdingCompany', 'group', 'conglomerate'
        ]
        
        for col in possible_ecosystem_cols:
            if col in df.columns:
                ecosystem_col = col
                print(f"[INFO] Tìm thấy cột hệ sinh thái: {col}", flush=True)
                break
        
        # Tạo map symbol -> exchange, symbol -> industry, symbol -> ecosystem, symbol -> organ_name
        # KHÔNG ghi đè exchange_symbol_map và industry_symbol_map đã tạo từ symbols_by_exchange() và symbols_by_industries() ở trên!
        exchanges = ['HOSE', 'UPCOM', 'HNX']
        # exchange_symbol_map đã được tạo từ symbols_by_exchange() ở trên, không tạo mới
        # exchange_symbol_map = {}  # ĐÃ BỊ XÓA - không được ghi đè map đã tạo
        # industry_symbol_map đã được tạo từ symbols_by_industries() ở trên, không tạo mới
        # industry_symbol_map = {}  # ĐÃ BỊ XÓA - không được ghi đè map đã tạo
        ecosystem_symbol_map = {}
        organ_name_map = {}
        
        # Lấy thông tin từ DataFrame
        for _, row in df.iterrows():
            symbol = row[symbol_col]
            if pd.isna(symbol):
                continue
            
            symbol_str = str(symbol)
            
            # Lấy organ_name nếu có
            if 'organ_name' in df.columns:
                organ_name_val = row['organ_name']
                if not pd.isna(organ_name_val):
                    organ_name_map[symbol_str] = str(organ_name_val).strip()
                else:
                    organ_name_map[symbol_str] = None
            else:
                organ_name_map[symbol_str] = None
            
            # Lấy exchange từ DataFrame (chỉ nếu chưa có từ symbols_by_exchange)
            # Normalize symbol để khớp với map đã tạo (uppercase)
            symbol_normalized = symbol_str.strip().upper()
            if symbol_normalized not in exchange_symbol_map:
                exchange = None
                if 'exchange' in df.columns:
                    exchange_val = row['exchange']
                    if not pd.isna(exchange_val):
                        exchange = str(exchange_val).strip().upper()
                        # Normalize exchange values
                        exchange_map = {'HOSE': 'HOSE', 'HSX': 'HOSE', 
                                       'HNX': 'HNX', 'UPCOM': 'UPCOM'}
                        exchange = exchange_map.get(exchange, exchange)
                        if exchange in exchanges:
                            exchange_symbol_map[symbol_normalized] = exchange
                elif 'comGroupCode' in df.columns:
                    exchange_val = row['comGroupCode']
                    if not pd.isna(exchange_val):
                        exchange = str(exchange_val).strip().upper()
                        # Normalize exchange values
                        exchange_map = {'HOSE': 'HOSE', 'HSX': 'HOSE', 
                                       'HNX': 'HNX', 'UPCOM': 'UPCOM'}
                        exchange = exchange_map.get(exchange, exchange)
                        if exchange in exchanges:
                            exchange_symbol_map[symbol_normalized] = exchange
                # Không set None nếu không có - để giữ nguyên giá trị từ symbols_by_exchange() nếu có
            
            # Lấy industry từ DataFrame (chỉ nếu chưa có từ symbols_by_industries)
            symbol_normalized = symbol_str.strip().upper()
            if symbol_normalized not in industry_symbol_map:
                if industry_col:
                    industry_val = row[industry_col]
                    if not pd.isna(industry_val):
                        industry_str = str(industry_val).strip()
                        if industry_str and industry_str != 'nan' and len(industry_str) > 0:
                            industry_symbol_map[symbol_normalized] = industry_str
                    # Không set None nếu không có - để giữ nguyên giá trị từ symbols_by_industries() nếu có
                # Không set None nếu không có industry_col - để giữ nguyên giá trị từ symbols_by_industries() nếu có
            
            # Lấy ecosystem/parent company (nếu có)
            if ecosystem_col:
                ecosystem_val = row[ecosystem_col]
                if not pd.isna(ecosystem_val):
                    ecosystem_symbol_map[symbol_str] = str(ecosystem_val).strip()
                else:
                    ecosystem_symbol_map[symbol_str] = None
            else:
                # Nếu không có cột ecosystem, thử suy luận từ mã cổ phiếu và tên công ty
                ecosystem = None
                
                # CHỈ sử dụng family_mapping từ file CSV (không hardcode)
                symbol_normalized = symbol_str.strip().upper()
                if family_mapping and symbol_normalized in family_mapping:
                    ecosystem = family_mapping[symbol_normalized]
                else:
                    # Không có trong CSV -> None (sẽ được set thành UNKNOWN sau)
                    ecosystem = None
                
                ecosystem_symbol_map[symbol_str] = ecosystem
        
        tqdm.write(f"[INFO] Tìm thấy {len(exchange_symbol_map)} mã từ all_symbols()", file=sys.stdout)
        
        # Tạo danh sách mã với thông tin exchange, industry và ecosystem
        all_symbols = []
        if fetch_company_info:
            tqdm.write("=" * 70, file=sys.stdout)
            tqdm.write("[INFO] Đang lấy thông tin chi tiết từ nguồn uy tín...", file=sys.stdout)
            tqdm.write("[WARNING] Quote API không có industry/ecosystem, chỉ test 10 mã đầu", file=sys.stdout)
            tqdm.write("[INFO] Sẽ sử dụng dữ liệu từ Listing và suy luận ecosystem", file=sys.stdout)
            tqdm.write("=" * 70, file=sys.stdout)
        
        processed_count = 0
        api_call_count = 0
        start_time = datetime.now()
        all_symbols = []  # Danh sách tất cả mã để lưu tăng dần
        
        # Normalize keys trong exchange_symbol_map để đảm bảo khớp
        # Tạo một map mới với keys đã normalize
        # CHỈ GIỮ LẠI CÁC MÃ CÓ 3 KÝ TỰ
        normalized_exchange_map = {}
        for key, value in exchange_symbol_map.items():
            normalized_key = str(key).strip().upper()
            if len(normalized_key) == 3:
                normalized_exchange_map[normalized_key] = value
        
        # Log số lượng mã sau khi filter
        original_count = len(exchange_symbol_map)
        filtered_count = len(normalized_exchange_map)
        tqdm.write(f"[INFO] Đã lọc: {original_count} mã -> {filtered_count} mã (chỉ giữ mã 3 ký tự)", file=sys.stdout)
        
        # Tạo progress bar (luôn enable để hiển thị progress)
        # Sử dụng số lượng mã sau khi filter
        total_symbols = len(normalized_exchange_map)
        # Dùng tqdm.write() để tránh conflict với progress bar
        tqdm.write(f"[INFO] Bắt đầu xử lý {total_symbols} mã (chỉ mã 3 ký tự)...", file=sys.stdout)
        pbar = tqdm(
            total=total_symbols,
            desc="Processing",
            unit="symbol",
            ncols=120,
            bar_format='{desc}: {percentage:3.0f}%|{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}]',
            file=sys.stdout,
            disable=False,
            dynamic_ncols=True
        )
        
        for symbol, exchange in normalized_exchange_map.items():
            processed_count += 1
            
            stock_info = {
                'symbol': str(symbol),
                'exchange': str(exchange) if exchange else None,
                'industry': None,
                'ecosystem': None,
                'organ_name': None
            }
            
            # Lấy thông tin từ DataFrame trước
            # Normalize symbol để đảm bảo khớp (uppercase, strip)
            symbol_normalized = str(symbol).strip().upper()
            if symbol_normalized in industry_symbol_map:
                industry = industry_symbol_map[symbol_normalized]
                # Debug: kiểm tra giá trị thực tế trong map
                if processed_count <= 5:
                    tqdm.write(f"[DEBUG] {symbol} (normalized: {symbol_normalized}): Found in map, raw value = {repr(industry)}, type = {type(industry)}", file=sys.stdout)
                # Chỉ gán nếu industry không phải None, empty, hoặc 'nan'
                if industry and str(industry).strip() != 'nan' and str(industry).strip().lower() != 'none' and len(str(industry).strip()) > 0:
                    stock_info['industry'] = str(industry).strip()
                else:
                    if processed_count <= 5:
                        tqdm.write(f"[DEBUG] {symbol}: Industry value is invalid: {repr(industry)}", file=sys.stdout)
            else:
                # Debug: kiểm tra tại sao không tìm thấy
                if processed_count <= 5:
                    # Kiểm tra một vài key gần đó
                    sample_keys = list(industry_symbol_map.keys())[:5]
                    tqdm.write(f"[DEBUG] {symbol} (normalized: {symbol_normalized}): NOT FOUND in industry_symbol_map (map has {len(industry_symbol_map)} entries, sample keys: {sample_keys})", file=sys.stdout)
            
            # ƯU TIÊN 1: Kiểm tra trong family_mapping từ file CSV (nếu có)
            symbol_normalized = str(symbol).strip().upper()
            if family_mapping and symbol_normalized in family_mapping:
                stock_info['ecosystem'] = family_mapping[symbol_normalized]
            elif symbol in ecosystem_symbol_map:
                # ƯU TIÊN 2: Sử dụng ecosystem từ DataFrame hoặc suy luận
                ecosystem = ecosystem_symbol_map[symbol]
                stock_info['ecosystem'] = ecosystem if ecosystem else None
            else:
                stock_info['ecosystem'] = None
            
            if symbol in organ_name_map:
                organ_name = organ_name_map[symbol]
                stock_info['organ_name'] = organ_name if organ_name else None
            
            # Kiểm tra dữ liệu đã có (nếu có)
            # CHỈ sử dụng existing_data nếu stock_info chưa có giá trị hợp lệ
            if existing_data and symbol in existing_data:
                existing = existing_data[symbol]
                # Chỉ dùng existing nếu stock_info chưa có giá trị hợp lệ
                if not stock_info.get('industry') or stock_info['industry'] == 'UNKNOWN':
                    if existing.get('industry') and existing['industry'] != 'UNKNOWN':
                        stock_info['industry'] = existing['industry']
                if not stock_info.get('ecosystem') or stock_info['ecosystem'] == 'UNKNOWN':
                    if existing.get('ecosystem') and existing['ecosystem'] != 'UNKNOWN':
                        stock_info['ecosystem'] = existing['ecosystem']
                if not stock_info.get('exchange') or stock_info['exchange'] == 'UNKNOWN':
                    if existing.get('exchange') and existing['exchange'] != 'UNKNOWN':
                        stock_info['exchange'] = existing['exchange']
                if not stock_info.get('organ_name'):
                    if existing.get('organ_name'):
                        stock_info['organ_name'] = existing['organ_name']
            
            # Nếu thiếu thông tin và được yêu cầu, lấy từ Quote (nguồn uy tín)
            # NHƯNG: Quote API không có industry/ecosystem, nên chỉ gọi khi cần exchange
            # Và chỉ gọi cho một số mã mẫu để test, không gọi cho tất cả
            if fetch_company_info and not stock_info['exchange']:
                # CHỈ gọi API cho 10 mã đầu tiên để test, sau đó dừng
                # Vì Quote API không có industry/ecosystem
                if api_call_count < 10:
                    try:
                        api_call_count += 1
                        print(f"[API] Đang lấy thông tin cho {symbol} (API call #{api_call_count})...", flush=True)
                        sys.stdout.flush()
                        
                        company_info = get_company_info(symbol)
                        
                        # Cập nhật exchange nếu chưa có
                        if not stock_info['exchange'] and company_info['exchange']:
                            stock_info['exchange'] = company_info['exchange']
                            print(f"[API] ✓ {symbol}: exchange = {company_info['exchange']}", flush=True)
                            sys.stdout.flush()
                        
                        # Cập nhật industry nếu chưa có (nhưng thường không có)
                        if not stock_info['industry'] and company_info['industry']:
                            stock_info['industry'] = company_info['industry']
                            print(f"[API] ✓ {symbol}: industry = {company_info['industry']}", flush=True)
                            sys.stdout.flush()
                        
                        # Cập nhật ecosystem nếu chưa có
                        if not stock_info['ecosystem'] and company_info['ecosystem']:
                            stock_info['ecosystem'] = company_info['ecosystem']
                        
                        # Delay nhỏ để tránh rate limit
                        import time
                        time.sleep(0.05)
                    except Exception as e:
                        print(f"[API] ✗ Lỗi khi lấy thông tin {symbol}: {str(e)[:50]}", flush=True)
                        pass
                elif api_call_count == 10:
                    # Sau 10 lần test, thông báo và dừng gọi API
                    print(f"[WARNING] Quote API không có industry/ecosystem. Đã test 10 mã, dừng gọi API.", flush=True)
                    print(f"[WARNING] Sẽ sử dụng dữ liệu từ Listing và suy luận ecosystem.", flush=True)
                    api_call_count += 1  # Tăng để không gọi nữa
            
            # Đảm bảo không có None, thay bằng UNKNOWN (trừ organ_name)
            stock_info['exchange'] = stock_info['exchange'] or 'UNKNOWN'
            # CHỈ set UNKNOWN nếu thực sự không có dữ liệu (không ghi đè dữ liệu đã crawl)
            # Debug: kiểm tra trước khi set UNKNOWN
            symbol_normalized = str(symbol).strip().upper()
            if processed_count <= 5:
                tqdm.write(f"[DEBUG] {symbol}: Before UNKNOWN check - industry = {repr(stock_info.get('industry'))}, in map = {symbol_normalized in industry_symbol_map}", file=sys.stdout)
            # Chỉ set UNKNOWN nếu thực sự không có (None, empty string, hoặc chưa được set)
            if not stock_info.get('industry') or stock_info['industry'] is None or stock_info['industry'] == '':
                stock_info['industry'] = 'UNKNOWN'
                if processed_count <= 5:
                    tqdm.write(f"[DEBUG] {symbol}: Set industry to UNKNOWN (was: {repr(stock_info.get('industry'))})", file=sys.stdout)
            if not stock_info.get('ecosystem') or stock_info['ecosystem'] is None:
                stock_info['ecosystem'] = 'UNKNOWN'
            # organ_name giữ nguyên None nếu không có
            
            all_symbols.append(stock_info)
        
            # Cập nhật progress bar - refresh thường xuyên để hiển thị đúng
            pbar.set_description(f"Processing {symbol}")
            pbar.set_postfix({
                'ind': '✓' if stock_info.get('industry') and stock_info['industry'] != 'UNKNOWN' else '-',
                'eco': '✓' if stock_info.get('ecosystem') and stock_info['ecosystem'] != 'UNKNOWN' else '-'
            })
            pbar.update(1)
            pbar.refresh()  # Force refresh để hiển thị ngay
            
            # Lưu tăng dần sau mỗi batch_size mã
            if output_file and processed_count % save_batch_size == 0:
                try:
                    save_incremental(output_file, all_symbols)
                except Exception:
                    pass
        
        # Đóng progress bar
        pbar.close()
        
        # Lưu lần cuối nếu còn dữ liệu chưa lưu
        if output_file and all_symbols:
            try:
                save_incremental(output_file, all_symbols)
            except Exception:
                pass
        
        if fetch_company_info:
            total_time = (datetime.now() - start_time).total_seconds()
            total_min = int(total_time // 60)
            total_sec = int(total_time % 60)
            print(f"\n[INFO] Hoàn thành! Tìm thấy {len(all_symbols)} mã từ danh sách", flush=True)
            print(f"[INFO] Tổng thời gian: {total_min}ph {total_sec}s", flush=True)
        else:
            print(f"\n[INFO] Tìm thấy {len(all_symbols)} mã từ danh sách", flush=True)
        
        # Kiểm tra mã còn giao dịch nếu được yêu cầu
        if check_trading:
            tqdm.write(f"[INFO] Đang kiểm tra mã còn giao dịch (trong {days_back} ngày gần đây)...", file=sys.stdout)
            tqdm.write(f"[INFO] Tổng số mã cần kiểm tra: {len(all_symbols)}", file=sys.stdout)
            active_symbols = []
            checked_count = 0
            
            # Progress bar cho việc kiểm tra trading
            check_pbar = tqdm(
                total=len(all_symbols),
                desc="Checking trading",
                unit="symbol",
                ncols=120,
                bar_format='{desc}: {percentage:3.0f}%|{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}]',
                file=sys.stdout,
                dynamic_ncols=True
            )
            
            for stock_info in all_symbols:
                symbol = stock_info['symbol']
                checked_count += 1
                
                check_pbar.set_description(f"Checking {symbol}")
                
                is_active, last_trade_date = check_symbol_active(
                    symbol, days_back=days_back
                )
                
                if is_active:
                    stock_info['last_trade_date'] = last_trade_date
                    active_symbols.append(stock_info)
                
                check_pbar.update(1)
                
                # Thêm delay nhỏ để tránh rate limit
                time.sleep(0.1)
            
            check_pbar.close()
            tqdm.write(f"\n[INFO] Tìm thấy {len(active_symbols)} mã còn giao dịch trên 3 sàn HOSE/UPCOM/HNX", file=sys.stdout)
            return active_symbols
        else:
            tqdm.write(f"[INFO] Bỏ qua kiểm tra trạng thái giao dịch, trả về tất cả {len(all_symbols)} mã", file=sys.stdout)
            return all_symbols
    
    except Exception as e:
        print(f"[ERROR] Lỗi khi lấy danh sách mã: {e}", flush=True)
        import traceback
        traceback.print_exc()
        return []


def load_existing_data(output_file: str) -> Dict[str, Dict]:
    """
    Load dữ liệu đã có từ file CSV (nếu có)
    
    Returns:
        Dict với key là symbol, value là dict chứa thông tin đã có
    """
    existing_data = {}
    output_path = Path(output_file)
    
    if output_path.exists():
        try:
            df = pd.read_csv(output_path, encoding='utf-8-sig')
            if 'symbol' in df.columns:
                for _, row in df.iterrows():
                    symbol = str(row['symbol'])
                    existing_data[symbol] = {
                        'industry': str(row.get('industry', 'UNKNOWN')) if pd.notna(row.get('industry')) else 'UNKNOWN',
                        'ecosystem': str(row.get('ecosystem', 'UNKNOWN')) if pd.notna(row.get('ecosystem')) else 'UNKNOWN',
                        'exchange': str(row.get('exchange', 'UNKNOWN')) if pd.notna(row.get('exchange')) else 'UNKNOWN',
                        'organ_name': str(row.get('organ_name', '')) if pd.notna(row.get('organ_name')) else None
                    }
                print(f"[INFO] Đã load {len(existing_data)} mã từ file CSV hiện có", flush=True)
        except Exception as e:
            print(f"[WARNING] Không đọc được file CSV hiện có: {e}", flush=True)
    
    return existing_data


def stat_ecosystem_from_families(active_stocks_file: str, family_file: str = "vietnam_stock_families.csv"):
    """
    Thống kê ecosystem theo danh sách trong vietnam_stock_families.csv
    
    Args:
        active_stocks_file: Đường dẫn file active_stocks.csv
        family_file: Đường dẫn file vietnam_stock_families.csv
    """
    try:
        from collections import defaultdict
        
        # Đọc file families
        family_path = Path(family_file)
        if not family_path.exists():
            print(f"[STAT] Không tìm thấy file {family_file}, bỏ qua thống kê", flush=True)
            return
        
        families = pd.read_csv(family_path, encoding='utf-8-sig')
        
        # Đọc file active stocks
        active_path = Path(active_stocks_file)
        if not active_path.exists():
            print(f"[STAT] Không tìm thấy file {active_stocks_file}, bỏ qua thống kê", flush=True)
            return
        
        active = pd.read_csv(active_path, encoding='utf-8-sig')
        
        print("\n" + "=" * 100, flush=True)
        print("=== THỐNG KÊ ECOSYSTEM THEO vietnam_stock_families.csv ===", flush=True)
        print("=" * 100, flush=True)
        
        # Tạo dictionary để thống kê
        stats = defaultdict(lambda: {
            'total_in_family': 0,
            'mapped': 0,
            'not_mapped': [],
            'wrong_mapping': []
        })
        
        # Tạo mapping từ symbol -> ecosystem trong active_stocks
        symbol_to_ecosystem = dict(zip(active['symbol'].str.upper(), active['ecosystem']))
        
        # Tìm các cột cần thiết
        family_col = None
        symbol_col = None
        
        for col in families.columns:
            col_lower = str(col).lower()
            if 'họ' in col_lower or 'tập đoàn' in col_lower or 'family' in col_lower:
                family_col = col
                break
        
        for col in families.columns:
            col_lower = str(col).lower()
            if 'mã' in col_lower or 'symbol' in col_lower or 'ticker' in col_lower or 'code' in col_lower:
                symbol_col = col
                break
        
        if not family_col or not symbol_col:
            print(f"[STAT] Không tìm thấy cột cần thiết trong {family_file}", flush=True)
            return
        
        # Xử lý từng họ/tập đoàn
        for _, row in families.iterrows():
            family_name = str(row[family_col]).strip()
            
            # Bỏ qua dòng header hoặc dòng trống
            if not family_name or family_name.startswith('#') or family_name == 'Họ/Tập đoàn':
                continue
            
            # Xử lý tên họ: loại bỏ "Họ " ở đầu nếu có, và normalize (giống như trong load_family_mapping)
            if family_name.startswith('Họ '):
                ecosystem_name = family_name[3:].strip().upper()  # Bỏ "Họ " và uppercase
            else:
                ecosystem_name = family_name.strip().upper()
            
            symbols_str = str(row[symbol_col])
            # Loại bỏ dấu ngoặc kép và tách các mã
            symbols_str = symbols_str.replace('"', '').replace("'", "")
            symbols = [s.strip().upper() for s in symbols_str.split(',') if s.strip()]
            
            stats[family_name]['total_in_family'] = len(symbols)
            
            for sym in symbols:
                if sym in symbol_to_ecosystem:
                    mapped_eco = symbol_to_ecosystem[sym]
                    if mapped_eco == ecosystem_name:
                        stats[family_name]['mapped'] += 1
                    elif mapped_eco and mapped_eco != 'UNKNOWN':
                        stats[family_name]['wrong_mapping'].append(f"{sym}->{mapped_eco}")
                    else:
                        stats[family_name]['not_mapped'].append(sym)
                else:
                    stats[family_name]['not_mapped'].append(sym)
        
        # In thống kê
        print(f"\n{'Họ/Tập đoàn':<35} {'Tổng mã':<10} {'Đã mapping':<12} {'Chưa mapping':<15} {'Sai mapping':<15}", flush=True)
        print("-" * 100, flush=True)
        
        total_families = 0
        total_symbols = 0
        total_mapped = 0
        total_not_mapped = 0
        
        for family, data in sorted(stats.items()):
            total_families += 1
            total_symbols += data['total_in_family']
            total_mapped += data['mapped']
            total_not_mapped += len(data['not_mapped'])
            
            not_mapped_count = len(data['not_mapped'])
            wrong_mapped_count = len(data['wrong_mapping'])
            
            print(f"{family:<35} {data['total_in_family']:<10} {data['mapped']:<12} {not_mapped_count:<15} {wrong_mapped_count:<15}", flush=True)
            
            # In chi tiết nếu có vấn đề
            if data['not_mapped']:
                not_mapped_list = ', '.join(data['not_mapped'][:10])
                print(f"  → Chưa mapping: {not_mapped_list}", flush=True)
                if len(data['not_mapped']) > 10:
                    print(f"  → ... và {len(data['not_mapped']) - 10} mã khác", flush=True)
            
            if data['wrong_mapping']:
                wrong_mapped_list = ', '.join(data['wrong_mapping'][:5])
                print(f"  → Sai mapping: {wrong_mapped_list}", flush=True)
                if len(data['wrong_mapping']) > 5:
                    print(f"  → ... và {len(data['wrong_mapping']) - 5} mã khác", flush=True)
        
        print("-" * 100, flush=True)
        print(f"{'TỔNG CỘNG':<35} {total_symbols:<10} {total_mapped:<12} {total_not_mapped:<15}", flush=True)
        print(f"\nTổng số họ/tập đoàn: {total_families}", flush=True)
        if total_symbols > 0:
            print(f"Tỷ lệ mapping thành công: {total_mapped}/{total_symbols} ({total_mapped*100/total_symbols:.1f}%)", flush=True)
        print("=" * 100, flush=True)
        
    except Exception as e:
        print(f"[STAT] Lỗi khi thống kê ecosystem: {e}", flush=True)
        import traceback
        traceback.print_exc()


def save_incremental(output_file: str, all_symbols: List[Dict], batch_size: int = 20):
    """
    Lưu dữ liệu tăng dần vào CSV sau mỗi batch_size mã
    
    Args:
        output_file: Đường dẫn file CSV
        all_symbols: Danh sách tất cả mã đã xử lý
        batch_size: Số mã cần xử lý trước khi lưu
    """
    output_path = Path(output_file)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    # Chuyển đổi list of dicts thành DataFrame
    df = pd.DataFrame(all_symbols)
    
    # Đảm bảo thứ tự cột
    columns_order = ['symbol', 'exchange']
    if 'industry' in df.columns:
        columns_order.append('industry')
    if 'ecosystem' in df.columns:
        columns_order.append('ecosystem')
    if 'organ_name' in df.columns:
        columns_order.append('organ_name')
    if 'last_trade_date' in df.columns:
        columns_order.append('last_trade_date')
    
    df = df[columns_order]
    df.to_csv(
        output_path,
        index=False,
        encoding='utf-8-sig'
    )


def download_active_stocks_csv(
    output_file: str = "data/active_stocks.csv",
    check_trading: bool = True,
    days_back: int = 90,
    fetch_company_info: bool = False
):
    """
    Lấy danh sách các mã chứng khoán trên 3 sàn HOSE, UPCOM, HNX và lưu vào CSV
    Có thể kiểm tra mã còn giao dịch bằng cách lấy dữ liệu giá gần đây
    
    Args:
        output_file: Đường dẫn file CSV output
        check_trading: True để kiểm tra mã còn giao dịch, False để lấy tất cả
        days_back: Số ngày gần đây để kiểm tra (mặc định 90 ngày)
    """
    output_path = Path(output_file)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    # Load dữ liệu đã có (nếu có)
    existing_data = load_existing_data(output_file)
    
    print("[INFO] Lấy danh sách mã chứng khoán", flush=True)
    print(f"[INFO] File output: {output_path.absolute()}", flush=True)
    if existing_data:
        print(f"[INFO] Đã có dữ liệu cho {len(existing_data)} mã, sẽ chỉ crawl các mã còn thiếu", flush=True)
    if check_trading:
        print(f"[INFO] Sẽ kiểm tra mã còn giao dịch (trong {days_back} ngày gần đây)", flush=True)
        print("[WARNING] Quá trình này có thể mất 15-30 phút với ~1700 mã", flush=True)
    else:
        print("[INFO] Không kiểm tra trạng thái giao dịch, lấy tất cả mã", flush=True)
    
    if fetch_company_info:
        print("[INFO] Sẽ lấy thông tin chi tiết từ các nguồn:", flush=True)
        print("  - Industry: từ Vietstock.vn", flush=True)
        print("  - Ecosystem: từ CafeF.vn", flush=True)
        print("[INFO] Sẽ crawl TẤT CẢ các mã (~1700 mã)", flush=True)
        print("[WARNING] Quá trình này có thể mất 10-15 phút (delay 0.2s/mã × 2 requests/mã)", flush=True)
    
    # Lấy danh sách mã
    active_symbols = get_active_symbols(
        check_trading=check_trading,
        days_back=days_back,
        fetch_company_info=fetch_company_info,
        existing_data=existing_data,
        output_file=str(output_path),
        save_batch_size=20
    )
    
    if not active_symbols:
        print("[ERROR] Không tìm thấy mã nào đang giao dịch", flush=True)
        return
    
    # Lưu vào CSV
    print(f"\n[INFO] Đang lưu danh sách {len(active_symbols)} mã vào {output_path}...", flush=True)
    
    # Chuyển đổi list of dicts thành DataFrame
    df = pd.DataFrame(active_symbols)
    
    # Đảm bảo thứ tự cột: symbol, exchange, industry, ecosystem, organ_name, last_trade_date
    columns_order = ['symbol', 'exchange']
    if 'industry' in df.columns:
        columns_order.append('industry')
    if 'ecosystem' in df.columns:
        columns_order.append('ecosystem')
    if 'organ_name' in df.columns:
        columns_order.append('organ_name')
    if 'last_trade_date' in df.columns:
        columns_order.append('last_trade_date')
    
    # Sắp xếp lại cột và lưu CSV
    df = df[columns_order]
    df.to_csv(
        output_path,
        index=False,
        encoding='utf-8-sig'  # UTF-8 with BOM để Excel mở được
    )
    
    # Summary
    print("\n[SUMMARY]", flush=True)
    print(f"  - Tổng số mã active: {len(active_symbols)}", flush=True)
    print(f"  - File output: {output_path.absolute()}", flush=True)
    print(f"  - Kích thước file: {output_path.stat().st_size / 1024:.2f} KB", flush=True)
    
    # Thống kê ecosystem theo vietnam_stock_families.csv
    stat_ecosystem_from_families(str(output_path), "vietnam_stock_families.csv")


def main():
    # Print ngay đầu để đảm bảo script đã chạy
    print("=" * 60, flush=True)
    print("[START] Bắt đầu script download_active_stocks_json.py", flush=True)
    print("=" * 60, flush=True)
    
    parser = argparse.ArgumentParser(
        description="Lấy danh sách các mã chứng khoán đang còn giao dịch và lưu vào CSV"
    )
    parser.add_argument(
        "--output",
        type=str,
        default="data/active_stocks.csv",
        help="File CSV output, mặc định: data/active_stocks.csv"
    )
    parser.add_argument(
        "--no-check-trading",
        action="store_true",
        help="Không kiểm tra mã còn giao dịch, lấy tất cả mã"
    )
    parser.add_argument(
        "--days-back",
        type=int,
        default=90,
        help="Số ngày gần đây để kiểm tra mã còn giao dịch (mặc định: 90)"
    )
    parser.add_argument(
        "--fetch-company-info",
        action="store_true",
        help="Lấy thông tin chi tiết từ Quote API (exchange, industry) - chậm hơn nhưng chính xác hơn"
    )
    args = parser.parse_args()
    
    try:
        download_active_stocks_csv(
            output_file=args.output,
            check_trading=not args.no_check_trading,
            days_back=args.days_back,
            fetch_company_info=args.fetch_company_info
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
    # Đảm bảo unbuffered output
    import sys
    sys.stdout.reconfigure(line_buffering=True) if hasattr(sys.stdout, 'reconfigure') else None
    
    main()

