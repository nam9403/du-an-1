# Security Policy

## Supported Versions

Phiên bản đang phát triển trên branch hiện tại được hỗ trợ bảo mật.

## Báo cáo lỗ hổng

Nếu bạn phát hiện vấn đề bảo mật, vui lòng:

1. Không public issue chứa thông tin nhạy cảm.
2. Gửi mô tả lỗ hổng, mức độ ảnh hưởng, bước tái hiện, và đề xuất khắc phục tới đội vận hành nội bộ.
3. Đính kèm thời gian phát hiện và phạm vi hệ thống bị ảnh hưởng.

## Nguyên tắc bảo mật bắt buộc

- Không hardcode secret/API key trong source code, script hoặc tài liệu public.
- Luôn dùng biến môi trường hoặc secret manager cho thông tin nhạy cảm.
- Rotate key định kỳ và ngay lập tức khi nghi ngờ lộ.
- Không commit file dữ liệu runtime nhạy cảm trong `data/`.
- Ưu tiên mã hóa chuẩn (ví dụ Fernet/KMS), không tự thiết kế thuật toán mã hóa.

## Incident Response (tối thiểu)

1. Cô lập khóa hoặc thành phần nghi ngờ bị lộ.
2. Rotate toàn bộ credentials liên quan.
3. Xác minh log truy cập bất thường.
4. Triển khai bản vá và theo dõi hậu kiểm.
5. Cập nhật checklist phòng ngừa tái diễn.

