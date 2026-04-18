# Roadmap Ky Thuat 30 Ngay (6.5 -> 8.0)

## 1) Muc tieu sau 30 ngay

- Nang do tin cay he thong len muc production co kiem soat.
- Tang chat luong du bao co the do luong duoc theo tuan/thang.
- Chuan hoa van hanh: monitoring, drift response, release safety.
- Dat nen tang cho scale user va tang truong doanh thu som.

### KPI ket qua (target)

- Forecast hit-rate (30/60/90 tong hop): +5 -> +10 diem phan tram.
- Forecast MAPE trung binh: giam 10% -> 20%.
- Ti le phan tich thanh cong end-to-end: >= 98%.
- p95 latency phien phan tich: giam 15% -> 25%.
- Drift_down false alarm: giam >= 30%.
- Ti le user quay lai 7 ngay (neu co event): +10% tuy cohort.

## 2) Nguyen tac trien khai

- Uu tien tinh on dinh truoc tinh nang moi.
- Moi tinh nang moi phai co metric truoc/sau.
- Co che fallback phai duoc test voi tinh huong loi thuc.
- Moi thay doi risk/forecast deu phai co guardrail.

## 3) Ke hoach theo 4 tuan

## Tuan 1 - Hardening & Observability

### Muc tieu
- Nhin thay ro suc khoe he thong theo thoi gian that.
- Giam su co "khong biet loi o dau".

### Viec can lam
- Chuan hoa event schema:
  - `analysis_started/completed/failed`
  - `llm_provider_selected/fallback`
  - `drift_guardrails_applied`
  - `forecast_exported`
- Them SLO dashboard nho trong app:
  - success rate, p50/p95 latency, fallback rate.
- Them error taxonomy:
  - data_source_error, llm_error, parse_error, timeout_error.
- Bo sung health report hang ngay:
  - luu file JSON tong hop trong `data/reports/`.

### Deliverable
- 1 dashboard SLO trong app.
- 1 report suc khoe hang ngay co timestamp.
- 1 bang mapping loi + action khac phuc.

## Tuan 2 - Forecast Quality & Calibration v2

### Muc tieu
- Nang do chinh xac du bao va giam bias theo tung nhom ma.

### Viec can lam
- Tinh calibration theo horizon rieng (30/60/90), khong gom chung.
- Them subgroup calibration:
  - theo industry subtype
  - theo regime (breakout/accumulation/distribution/neutral)
- Nang cap forecast score:
  - ket hop hit-rate + mape + alpha + sample confidence.
- Them backtest sanity gate:
  - neu sample qua it -> giam trong so AI text, uu tien risk plan.

### Deliverable
- Calibration v2 co metadata "global + subgroup".
- Bieu do truoc/sau cho MAPE, hit-rate, alpha.
- Rulebook dieu chinh he so calibration.

## Tuan 3 - Drift Automation & Safe Autopilot

### Muc tieu
- Tu dong ung pho drift xau ma khong can can thiep tay.

### Viec can lam
- Multi-level drift:
  - level 1: canh bao
  - level 2: ep mode `quality`
  - level 3: giam vi the + nang gate confidence
- Cooldown strategy:
  - khi drift level 2/3, giu trong 3-7 ngay roi danh gia lai.
- Guardrail risk plan v2:
  - gioi han max position theo drift level.
- Add "why-this-guardrail" message cho user.

### Deliverable
- Trang thai drift level hien ro trong UI.
- Lich su guardrail trigger theo ngay/ma.
- A/B so sanh truoc/sau drift automation.

## Tuan 4 - Productionization & Scale Readiness

### Muc tieu
- San sang mo rong nguoi dung va release an toan.

### Viec can lam
- Release workflow:
  - canary config (10% user/nhom ma)
  - rollback config nhanh (env flag).
- Data integrity jobs:
  - check missing fields
  - stale data detector
  - duplicate forecast cleanup.
- Performance:
  - cache key toi uu theo task_mode + horizon + profile.
  - prewarm chu dong cho watchlist active.
- Security basics:
  - khong log secrets
  - mask key o moi log path
  - report audit events toi thieu.

### Deliverable
- Checklist production readiness pass >= 90%.
- Canary + rollback script/huong dan.
- Bao cao hieu nang p95 truoc/sau.

## 4) Backlog uu tien (P0/P1/P2)

### P0 (bat buoc)
- SLO dashboard + event schema + error taxonomy.
- Calibration v2 theo horizon.
- Drift multi-level + auto guardrails.
- Release canary/rollback bang env flags.

### P1 (nen lam)
- Subgroup calibration theo industry/regime.
- Portfolio performance board (weekly/monthly).
- Drift false-alarm reduction by adaptive thresholds v2.

### P2 (mo rong)
- Model ensemble theo loai tac vu.
- Cost optimizer theo provider latency/quality.
- Cohort monetization insight dashboard.

## 5) Muc do hoan thanh moi tuan (Definition of Done)

- Co test chay pass cho logic moi.
- Co metric truoc/sau duoc ghi ro.
- Co fallback khi loi data/LLM.
- Co tai lieu van hanh ngan gon (runbook 1 trang).

## 6) RACI gon

- Owner ky thuat: ban + agent (implementation loop).
- Owner san pham: ban (uu tien KPI va trade-off).
- Owner van hanh: ban (go-live + review report hang tuan).

## 7) Nghi thuc thuc thi 30 ngay

- Moi ngay:
  - xem report health 1 lan
  - xem drift panel 1 lan
- Moi tuan:
  - chot 3 metric chinh (hit-rate, mape, latency)
  - quyet dinh tiep tuc/rollback 1-2 thay doi.

## 8) Tieu chi dat 8.0

- He thong on dinh production (SLO dat target 2 tuan lien tuc).
- Forecast quality cai thien ro rang va ben vung (khong tang error tro lai).
- Drift duoc xu ly tu dong co giai thich ro.
- Co release process an toan, co rollback nhanh.

---

Neu ban muon, buoc tiep theo toi se chuyen roadmap nay thanh backlog task cu the theo ngay (Day 1 -> Day 30) de team chi viec execute.
