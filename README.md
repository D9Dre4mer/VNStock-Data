# Download Dữ Liệu Chứng Khoán Việt Nam

Bộ script để download dữ liệu lịch sử của tất cả mã chứng khoán Việt Nam và các chỉ số từ thư viện `vnstock`.

## Yêu Cầu

- Python 3.7+
- Conda environment `vnstock_download` (hoặc môi trường có cài vnstock)

## Cài Đặt

```bash
# Tạo môi trường conda (nếu chưa có)
conda create -n vnstock_download python=3.9
conda activate vnstock_download

# Cài đặt các thư viện cần thiết
pip install -r requirements.txt

# Hoặc cài đặt thủ công
pip install vnstock3 pandas pyarrow tqdm
```

## Sử Dụng

### 1. Download Tất Cả Mã Chứng Khoán

#### Cách 1: Chạy với cấu hình mặc định (khuyến nghị)

```bash
conda activate vnstock_download
$env:PYTHONIOENCODING='utf-8'
python download_vn_stocks.py
```

#### Cách 2: Chạy với tham số tùy chỉnh

```bash
conda activate vnstock_download
$env:PYTHONIOENCODING='utf-8'
python download_vn_stocks.py \
    --start 1990-01-01 \
    --end 2025-11-09 \
    --out data/eod_parquet \
    --workers 1 \
    --sleep 2.0
```

**Tham số:**
- `--start`: Ngày bắt đầu (YYYY-MM-DD), mặc định: `1990-01-01`
- `--end`: Ngày kết thúc (YYYY-MM-DD), mặc định: hôm nay
- `--out`: Thư mục lưu file, mặc định: `data/eod_parquet`
- `--workers`: Số worker đồng thời, mặc định: `1` (để tránh rate limit)
- `--sleep`: Thời gian sleep giữa các request (giây), mặc định: `2.0`

### 2. Download Chỉ Số VNINDEX

```bash
conda activate vnstock_download
$env:PYTHONIOENCODING='utf-8'
python download_vnindex.py
```

**Tham số:**
- `--start`: Ngày bắt đầu (YYYY-MM-DD), mặc định: `1990-01-01`
- `--end`: Ngày kết thúc (YYYY-MM-DD), mặc định: hôm nay
- `--out`: Thư mục output, mặc định: `data/eod_parquet`
- `--sleep`: Thời gian sleep (giây), mặc định: `2.0`
- `--force`: Tải lại ngay cả khi file đã tồn tại

### 3. Download Chỉ Số VN30

```bash
conda activate vnstock_download
$env:PYTHONIOENCODING='utf-8'
python download_vn30.py
```

**Tham số:** (tương tự như download_vnindex.py)
- `--start`: Ngày bắt đầu (YYYY-MM-DD), mặc định: `1990-01-01`
- `--end`: Ngày kết thúc (YYYY-MM-DD), mặc định: hôm nay
- `--out`: Thư mục output, mặc định: `data/eod_parquet`
- `--sleep`: Thời gian sleep (giây), mặc định: `2.0`
- `--force`: Tải lại ngay cả khi file đã tồn tại

### 4. Download Danh Sách Mã Chứng Khoán Đang Giao Dịch

```bash
conda activate vnstock_download
$env:PYTHONIOENCODING='utf-8'
python download_active_stocks.py
```

**Tham số:**
- `--no-check-trading`: Không kiểm tra mã có đang giao dịch không
- `--output`: File CSV output, mặc định: `data/active_stocks.csv`

## Kết Quả

Sau khi chạy, bạn sẽ có:

- **Thư mục output**: Chứa các file `.parquet` cho từng mã chứng khoán
- **manifest.csv**: Danh sách tất cả mã và trạng thái download
- **failed.csv**: Danh sách các mã download thất bại (nếu có)

## Đọc Dữ Liệu

```python
import pandas as pd

# Đọc dữ liệu một mã
df = pd.read_parquet("data/eod_parquet/VCI.parquet")
print(df.head())

# Đọc tất cả mã
import glob
files = glob.glob("data/eod_parquet/*.parquet")
all_data = []
for file in files:
    df = pd.read_parquet(file)
    df['symbol'] = Path(file).stem
    all_data.append(df)

combined = pd.concat(all_data, ignore_index=True)
```

## Gộp Dữ Liệu Thành CSV

Sau khi download xong, bạn có thể gộp tất cả file parquet (bao gồm cả chỉ số) thành một file CSV:

```bash
conda activate vnstock_download
$env:PYTHONIOENCODING='utf-8'
python merge_parquet_to_csv.py
```

Hoặc với tham số tùy chỉnh:

```bash
python merge_parquet_to_csv.py --input data/eod_parquet --output data/all_stocks.csv
```

**Tham số:**
- `--input`: Thư mục chứa các file parquet, mặc định: `data/eod_parquet`
- `--output`: File CSV output, mặc định: `data/all_stocks.csv`
- `--no-symbol`: Không thêm cột symbol (lấy từ tên file)

## Tính Năng

- ✅ **Tự động xử lý rate limit**: Script tự động phát hiện và chờ đúng thời gian API yêu cầu
- ✅ **Retry logic**: Tự động retry tối đa 5 lần khi gặp rate limit
- ✅ **Resume download**: Tự động bỏ qua các mã đã download (kiểm tra file tồn tại)
- ✅ **Progress bar**: Hiển thị tiến trình download với thống kê real-time
- ✅ **Countdown**: Hiển thị countdown khi chờ rate limit
- ✅ **Thread-safe**: Sử dụng lock để đảm bảo chỉ có 1 request tại một thời điểm

## Lưu Ý

- Script sử dụng import local để tránh circular import với vnstock
- Dữ liệu được lưu dưới dạng Parquet để tiết kiệm dung lượng
- Script tự động bỏ qua các mã đã download (kiểm tra file tồn tại)
- Có thể resume download bằng cách chạy lại script
- **Rate limit**: Script tự động xử lý rate limit, nhưng nếu vẫn gặp vấn đề, hãy:
  - Giảm số workers: `--workers 1`
  - Tăng thời gian sleep: `--sleep 3.0` hoặc cao hơn
  - Chạy vào giờ thấp điểm để tránh rate limit

## Xử Lý Lỗi

### Lỗi Circular Import

Nếu gặp lỗi circular import với vnstock:

1. Đảm bảo đã cài đúng phiên bản: `pip install vnstock3 --upgrade`
2. Thử giảm số workers: `--workers 1`
3. Tăng thời gian sleep: `--sleep 2.0`

### Lỗi Rate Limit

Script đã được cấu hình để tự động xử lý rate limit:
- Tự động phát hiện rate limit errors
- Tự động trích xuất thời gian chờ từ thông báo lỗi
- Tự động chờ và retry sau khi hết thời gian

Nếu vẫn gặp vấn đề, hãy:
- Giảm số workers xuống 1: `--workers 1`
- Tăng sleep time: `--sleep 3.0` hoặc cao hơn
- Chạy vào giờ thấp điểm

