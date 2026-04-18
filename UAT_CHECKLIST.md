# UAT Checklist

Checklist kiểm thử chấp nhận người dùng trước public beta.

## A. Truy cập & phiên làm việc

- [ ] Mở ứng dụng thành công, không lỗi giao diện.
- [ ] Đăng ký/đăng nhập người dùng hoạt động đúng.
- [ ] Sai PIN bị từ chối đúng cách.
- [ ] Đăng nhập đúng cho phép truy cập đầy đủ chức năng được cấp.

## B. Phân tích cổ phiếu

- [ ] Nhập mã phổ biến (ví dụ `FPT`, `VNM`) trả kết quả hợp lệ.
- [ ] Trường hợp mã không hợp lệ có thông báo rõ ràng.
- [ ] Báo cáo chiến lược hiển thị đầy đủ các phần chính.
- [ ] Không xuất hiện lỗi timeout/blocking bất thường khi thao tác liên tục.

## C. Báo cáo & xuất file

- [ ] Xuất PDF thành công với nội dung đầy đủ.
- [ ] PDF có biểu đồ hoặc fallback hợp lệ.
- [ ] File xuất mở được trên máy người dùng.

## D. Cảnh báo & theo dõi

- [ ] Tạo/sửa/xóa alert hoạt động đúng.
- [ ] Alert scan không lỗi ở chu kỳ quét đầu.
- [ ] Giới hạn theo gói (free/pro/expert) áp dụng đúng.

## E. Chất lượng dữ liệu

- [ ] Snapshot có `price` và `source` hợp lệ cho các mã test.
- [ ] Các chỉ số chính không bị rỗng bất thường.
- [ ] Hành vi fallback hoạt động khi nguồn dữ liệu chính chậm/lỗi.

## F. Bảo mật vận hành

- [ ] Không có key thật xuất hiện trong UI/log/script.
- [ ] Secret scan pass (`python scripts/check_secrets.py`).
- [ ] Credentials test đã được rotate trước go-live.

## G. Sign-off

- [ ] Product Owner đồng ý phát hành.
- [ ] Technical Owner đồng ý phát hành.
- [ ] Có phương án rollback đã kiểm tra.

