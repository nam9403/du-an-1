# Deployment Guide

Tài liệu triển khai tối thiểu cho môi trường staging/production.

## 1) Chuẩn bị môi trường

- Python 3.12 (khuyến nghị đồng nhất với CI).
- Cài dependencies:

```bash
python -m pip install -r requirements.txt
python -m pip install -r requirements-dev.txt
```

- Thiết lập biến môi trường bắt buộc:
  - `II_APP_SECRET_KEY`
  - ít nhất một trong các nhóm key LLM nếu dùng AI live:
    - `GROQ_API_KEYS`
    - `OPENAI_API_KEYS`
    - `GEMINI_API_KEYS`

## 2) Kiểm tra trước deploy

```bash
python scripts/check_secrets.py
python -m pytest -q
python scripts/health_check.py
```

Staging smoke (có mạng):

```bash
python scripts/health_check.py --full
python scripts/verify_streamlit_startup.py
```

## 3) Chạy ứng dụng

```bash
python -m streamlit run app.py --server.headless true
```

Hoặc Windows local:

```bash
run_app.bat
```

## 4) Dữ liệu runtime cần backup

- `data/app_state.db`
- `data/app_state.db-wal` (nếu tồn tại)
- `data/app_state.db-journal` (nếu tồn tại)
- `data/.app_secret.key` (nếu không dùng `II_APP_SECRET_KEY` từ env)

## 5) Rollback nhanh

1. Dừng service bản hiện tại.
2. Checkout tag ổn định gần nhất.
3. Khôi phục DB backup tương ứng (nếu schema khác biệt).
4. Thiết lập lại env vars của phiên bản cũ.
5. Khởi động lại app.
6. Chạy smoke test:
   - mở dashboard
   - chạy 1 phân tích mã
   - xuất PDF
   - kiểm tra cảnh báo

## 6) Sau deploy

- Theo dõi lỗi runtime và timeout upstream.
- Kiểm tra hàng đợi notification/background job.
- Kiểm tra tỷ lệ request thất bại trong 30-60 phút đầu.

