# Investment Intelligence Dashboard

Ứng dụng web phân tích cổ phiếu Việt Nam bằng Streamlit, kết hợp định giá, chiến lược, cảnh báo và báo cáo PDF.

## Tính năng chính

- Snapshot dữ liệu tài chính và giá cho mã cổ phiếu.
- Báo cáo chiến lược tự động (valuation, MOS, luận điểm hành động).
- Xuất báo cáo PDF chuyên nghiệp.
- Backtest chiến lược cơ bản.
- Cảnh báo theo ngưỡng và theo dõi watchlist.

## Yêu cầu môi trường

- Python 3.11+ (khuyến nghị 3.12)
- Windows/macOS/Linux

## Cài đặt nhanh

```bash
python -m venv .venv
.venv\Scripts\activate
python -m pip install -r requirements.txt
python -m pip install -r requirements-dev.txt
```

## Cấu hình biến môi trường

1. Sao chép `.env.example` thành `.env` (hoặc set trực tiếp vào hệ thống).
2. Thiết lập API keys theo nhu cầu:

```bash
setx GROQ_API_KEYS "gsk_key_1;gsk_key_2"
setx OPENAI_API_KEYS "sk_key_1;sk_key_2"
setx GEMINI_API_KEYS "AIza_key_1;AIza_key_2"
```

3. (Khuyến nghị production) đặt key mã hóa app:

```bash
setx II_APP_SECRET_KEY "your_fernet_key_here"
```

> Lưu ý: không commit `.env` và không hardcode API key vào mã nguồn/batch script.

## Chạy ứng dụng

```bash
python -m streamlit run app.py
```

Hoặc dùng script Windows:

```bash
run_app.bat
```

## Kiểm thử & health check

```bash
python -m pytest -q
python scripts/check_secrets.py
python scripts/health_check.py
python scripts/health_check.py --full
```

## Bảo mật

- Xem thêm tài liệu tại `SECURITY.md`.
- Nếu nghi ngờ lộ key: rotate key ngay, thu hồi key cũ, và kiểm tra log truy cập.

## Phát hành (Release)

- Checklist phát hành và rollback: `RELEASE_CHECKLIST.md`
- Hướng dẫn triển khai staging/production: `DEPLOYMENT.md`
- Checklist UAT trước public beta: `UAT_CHECKLIST.md`
- CI tự động: `.github/workflows/ci.yml`
- Release workflow theo tag `vX.Y.Z`: `.github/workflows/release.yml`
- Cấu hình phân loại release notes: `.github/release.yml`
- Phiên bản hiện tại lấy từ file `VERSION`

Chuẩn bị phát hành nhanh:

```bash
python scripts/bump_version.py patch
python scripts/preflight_check.py
python scripts/release_prep.py
python scripts/release_prep.py --full
python scripts/generate_release_notes.py
python scripts/release_all_in_one.py --bump patch
```

`release_all_in_one.py` đã bao gồm bước sinh `dist/release-notes.md` thông qua `release_prep.py`.

## Trạng thái thương mại hóa

Project đã có nền tảng kỹ thuật tốt để chạy pilot/private beta. Trước public launch, cần đảm bảo:

- Quy trình CI/CD ổn định.
- Chính sách bảo mật và vận hành rõ ràng.
- Giám sát lỗi runtime và cảnh báo vận hành (Sentry/log aggregation).

