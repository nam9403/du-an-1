# Ke Hoach Thuc Thi Day-by-Day (Ngay 1 -> 30)

Tai lieu nay chuyen `ROADMAP_30D_6.5_to_8.0.md` thanh checklist hang ngay de execute lien tuc.

## Cach dung

- Moi ngay chi chot 1-3 dau viec chinh, khong dan trai.
- Cuoi ngay cap nhat:
  - Done/Not done
  - Metric truoc/sau
  - Blocker + cach xu ly ngay hom sau

---

## Tuan 1: Hardening + Observability

### Day 1
- Chuan hoa event schema (analysis, provider, drift, forecast).
- Tao bang mapping event -> y nghia -> dashboard field.
- Output: `docs/EVENT_SCHEMA.md`.

### Day 2
- Them logging thong nhat cho `analysis_started/completed/failed`.
- Bo sung error taxonomy co ma loi.
- Output: pull checklist "error types covered >= 90%".

### Day 3
- Them chi so SLO co ban (success rate, p50, p95).
- Hien trong app 1 panel SLO nho.
- Output: panel hoat dong voi du lieu that.

### Day 4
- Ghi log fallback path (provider fallback, data fallback).
- Them metric fallback_rate theo ngay.
- Output: fallback report JSON.

### Day 5
- Tao report suc khoe hang ngay tu dong (health summary).
- Luu vao `data/reports/`.
- Output: 1 file report/day.

### Day 6
- Viet runbook xu ly su co top 10 loi thuong gap.
- Output: `docs/INCIDENT_RUNBOOK.md`.

### Day 7
- Review Tuan 1:
  - Success rate hien tai
  - p95 hien tai
  - Loi top 3
- Output: weekly review note.

---

## Tuan 2: Forecast Quality + Calibration v2

### Day 8
- Tach metric du bao theo horizon 30/60/90.
- Output: table metric theo horizon.

### Day 9
- Trien khai calibration rieng cho horizon.
- Output: field calibration metadata theo horizon.

### Day 10
- Thiet lap calibration theo subgroup nganh.
- Output: subgroup calibration report.

### Day 11
- Thiet lap calibration theo regime (breakout/neutral/...).
- Output: regime calibration summary.

### Day 12
- Refine forecast score (hit-rate + mape + alpha + sample weight).
- Output: score formula doc + code.

### Day 13
- Add sanity gate khi sample it (giam overconfidence).
- Output: gate trigger log.

### Day 14
- Review Tuan 2:
  - Delta hit-rate
  - Delta MAPE
  - Delta alpha
- Output: before/after chart.

---

## Tuan 3: Drift Automation + Safe Autopilot

### Day 15
- Chuan hoa drift levels (L1/L2/L3).
- Output: `docs/DRIFT_POLICY.md`.

### Day 16
- Ap L1 canh bao + UI signal.
- Output: drift panel update.

### Day 17
- Ap L2 ep mode quality tu dong.
- Output: log `drift_guardrails_applied`.

### Day 18
- Ap L3 giam position sizing tu dong.
- Output: risk plan changed audit.

### Day 19
- Them cooldown strategy sau guardrail.
- Output: cooldown state tracking.

### Day 20
- Them message "why this guardrail" de user hieu.
- Output: UI explain block.

### Day 21
- Review Tuan 3:
  - False alarm rate
  - Guardrail precision
  - User impact
- Output: drift automation review.

---

## Tuan 4: Productionization + Scale Readiness

### Day 22
- Tao env flags cho canary release.
- Output: `docs/CANARY_CONFIG.md`.

### Day 23
- Tao rollback flow 1 lenh.
- Output: `docs/ROLLBACK_RUNBOOK.md`.

### Day 24
- Data integrity checks (missing/stale/duplicate).
- Output: integrity job report.

### Day 25
- Toi uu cache key theo task_mode + horizon + profile.
- Output: cache hit-rate delta.

### Day 26
- Prewarm watchlist active theo khung gio.
- Output: p95 latency delta.

### Day 27
- Security sweep:
  - no secrets in log
  - mask key
  - audit events
- Output: security checklist pass.

### Day 28
- Load test nho:
  - multi-user simulation
  - latency + error rate
- Output: load-test summary.

### Day 29
- Go-live dry-run:
  - canary 10%
  - monitor 1 ngay
- Output: go/no-go decision sheet.

### Day 30
- Final review 30 ngay:
  - KPI/SLO dat bao nhieu %
  - muc do truong thanh moi
  - backlog 30 ngay tiep theo
- Output: `MILESTONE_30D_REVIEW.md`.

---

## Daily standup template (copy-paste)

- Hom qua da xong:
- Hom nay lam:
- Metric truoc/sau:
- Blocker:
- Can ho tro:

## Weekly review template

- KPI chinh: hit-rate / MAPE / p95 / success rate
- So lan drift_down:
- So lan guardrail kick-in:
- Co issue nghiem trong khong:
- Quyet dinh tuan toi:

