# Release Checklist

Checklist tối thiểu trước khi đưa bản mới ra production.

## 1) Pre-release (T-1 đến T-0)

- [ ] Bump version nếu cần (`python scripts/bump_version.py patch|minor|major`).
- [ ] Hoặc chạy trọn bộ không cần thủ công: `python scripts/release_all_in_one.py --bump patch`.
- [ ] Chạy preflight môi trường: `python scripts/preflight_check.py`.
- [ ] Xác nhận không còn secret hardcode trong source/script.
- [ ] Rotate API keys nếu có thay đổi nhân sự hoặc nghi ngờ rò rỉ.
- [ ] Cập nhật `.env`/secret manager cho môi trường deploy.
- [ ] Chạy CI xanh (`pytest` + `health_check.py`).
- [ ] Chạy probe có mạng trên máy staging:
  - [ ] `python scripts/health_check.py --full`
  - [ ] `python scripts/verify_streamlit_startup.py`
- [ ] Xác nhận DB backup mới nhất khả dụng.
- [ ] Chốt release note (điểm mới, bug fix, breaking changes).

## 2) Deploy

- [ ] Gắn tag release dạng `vX.Y.Z`.
- [ ] Theo dõi GitHub Actions `Release`.
- [ ] Triển khai artifact đúng môi trường.
- [ ] Xác nhận app trả HTTP 200 sau deploy.
- [ ] Smoke test nhanh:
  - [ ] Mở dashboard thành công.
  - [ ] Chạy phân tích 1-2 mã tiêu biểu.
  - [ ] Xuất PDF thành công.
  - [ ] Cảnh báo scan chạy không lỗi.

## 3) Post-release (30-60 phút đầu)

- [ ] Theo dõi log lỗi và latency.
- [ ] Kiểm tra tỷ lệ lỗi API upstream (nguồn dữ liệu, LLM provider).
- [ ] Xác nhận không có tăng đột biến retry/background jobs.
- [ ] Chụp snapshot trạng thái vận hành sau release.

## 4) Rollback Plan

- [ ] Điều kiện rollback rõ ràng (ví dụ: lỗi đăng nhập, lỗi tạo báo cáo, downtime > 5 phút).
- [ ] Sẵn sàng rollback về tag ổn định gần nhất.
- [ ] Khôi phục config/secret tương ứng phiên bản trước.
- [ ] Chạy lại smoke test sau rollback.

## 5) Security Verification

- [ ] File runtime nhạy cảm không bị commit (`data/*.db`, `data/secrets_store.json`, `data/.app_secret.key`).
- [ ] Không có key thật trong `run_app.bat`, docs hoặc workflow logs.
- [ ] Chính sách phản ứng sự cố theo `SECURITY.md` đã sẵn sàng.

