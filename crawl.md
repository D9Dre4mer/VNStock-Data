# 🇻🇳 Hướng Dẫn Lấy Ngành Nghề & Họ Cổ Phiếu Việt Nam (Miễn Phí & Chính Xác)

Tài liệu này hướng dẫn bạn cách thu thập dữ liệu **ngành nghề (sector/industry)** và **họ cổ phiếu (corporate family)** cho toàn bộ mã chứng khoán Việt Nam — hoàn toàn **miễn phí** và **hợp pháp** — bằng Python.

---

## 🧱 1. Mục Tiêu

Chúng ta sẽ:
- Lấy toàn bộ mã cổ phiếu (HOSE, HNX, UPCOM) bằng thư viện `vnstock`.
- Lấy **ngành / lĩnh vực** của từng mã từ Vietstock.vn.
- Lấy **họ / hệ sinh thái công ty** (Vingroup, Masan, FLC, v.v.) từ CafeF.vn.
- Xuất file CSV chứa metadata đầy đủ.

---

## ⚙️ 2. Cài Đặt Môi Trường

```bash
pip install -U vnstock pandas beautifulsoup4 requests
