"""
BCTC nhiều kỳ (tóm tắt số liệu) qua API công khai — ưu tiên VNDirect Finfo.

Không parse file PDF BCTC; dữ liệu là bảng số đã cấu trúc từ nhà cung cấp.
Dùng để ước lượng YoY / xu hướng và bổ sung piotroski khi thiếu.
"""

from __future__ import annotations

import os
from typing import Any

import requests

from scrapers.finance_scraper import VNDIRECT_HEADERS


def _to_float(v: Any) -> float | None:
    if v is None:
        return None
    try:
        return float(str(v).replace(",", "").strip())
    except (TypeError, ValueError):
        return None


def _pick(row: dict[str, Any], *keys: str) -> Any:
    for k in keys:
        if k in row and row[k] is not None:
            return row[k]
    return None


def _normalize_row(raw: dict[str, Any]) -> dict[str, Any]:
    """Chuẩn hóa một dòng báo cáo (tên trường khác nhau giữa các phiên bản API)."""
    yr = _pick(raw, "yearReport", "year", "nam", "fiscalYear")
    q = _pick(raw, "quarterReport", "quarter", "quy", "fiscalQuarter")
    try:
        year = int(float(str(yr))) if yr is not None else 0
    except (TypeError, ValueError):
        year = 0
    try:
        quarter = int(float(str(q))) if q is not None else 0
    except (TypeError, ValueError):
        quarter = 0

    rev = _to_float(
        _pick(
            raw,
            "revenue",
            "revenueTotal",
            "totalRevenue",
            "doanhThuThuan",
            "netRevenue",
        )
    )
    ni = _to_float(
        _pick(
            raw,
            "netIncome",
            "netProfit",
            "profitAfterTax",
            "lnst",
            "netIncomeAfterTax",
        )
    )
    ta = _to_float(_pick(raw, "totalAssets", "totalAsset", "tongTaiSan"))
    eq = _to_float(_pick(raw, "equity", "ownerEquity", "vonChuSoHuu", "totalEquity"))

    return {
        "year": year,
        "quarter": quarter,
        "revenue": rev,
        "net_income": ni,
        "total_assets": ta,
        "equity": eq,
        "_raw_keys": list(raw.keys())[:12],
    }


def fetch_vndirect_financial_statement_rows(ticker: str, *, size: int = 12) -> tuple[list[dict[str, Any]], str]:
    """
    Tải các dòng financialStatements từ Finfo (nếu endpoint khả dụng).
    Trả (danh_sách_row_đã_chuẩn_hóa, nhãn_nguồn).
    """
    sym = (ticker or "").strip().upper()
    if not sym:
        return [], "empty_ticker"

    timeout = float(os.environ.get("II_FSTATEMENT_TIMEOUT", "20"))
    urls = [
        (
            "v4_financialStatements_q",
            f"https://finfo-api.vndirect.com.vn/v4/financialStatements"
            f"?sort=yearReport:desc,quarterReport:desc&size={size}&q=code:{sym}~",
        ),
        (
            "v4_financialStatements_filter",
            f"https://finfo-api.vndirect.com.vn/v4/financialStatements"
            f"?filter=code:eq:{sym}&sort=yearReport:desc&size={size}",
        ),
    ]
    last_err = ""
    for label, url in urls:
        try:
            r = requests.get(url, headers=VNDIRECT_HEADERS, timeout=timeout)
            if r.status_code != 200:
                last_err = f"{label}:HTTP{r.status_code}"
                continue
            js = r.json()
            rows = js.get("data") if isinstance(js, dict) else None
            if not isinstance(rows, list) or not rows:
                last_err = f"{label}:empty"
                continue
            out = []
            for raw in rows:
                if not isinstance(raw, dict):
                    continue
                norm = _normalize_row(raw)
                norm["_source_row"] = raw
                out.append(norm)
            if out:
                return out, label
        except requests.RequestException as e:
            last_err = f"{label}:{e}"
            continue
    return [], last_err or "no_data"


def compute_trend_metrics(rows: list[dict[str, Any]]) -> dict[str, Any]:
    """
    Từ các kỳ đã sắp (mới nhất trước), tính YoY doanh thu/LNST nếu đủ 2 điểm.
    """
    if len(rows) < 2:
        return {
            "periods": len(rows),
            "yoy_compare_label_vi": None,
            "revenue_yoy_pct": None,
            "profit_yoy_pct": None,
            "revenue_cagr_3y_pct": None,
            "trend_label": "Không đủ kỳ",
        }

    cur = rows[0]
    prev_y = None
    cy, cq = int(cur.get("year") or 0), int(cur.get("quarter") or 0)
    for r in rows[1:]:
        ry, rq = int(r.get("year") or 0), int(r.get("quarter") or 0)
        if cy > 0 and ry == cy - 1 and (cq == rq or (cq == 0 and rq == 0)):
            prev_y = r
            break
    if prev_y is None and len(rows) >= 5:
        prev_y = rows[4]
    if prev_y is None:
        prev_y = rows[1]

    def _yoy(cur_v: float | None, prev_v: float | None) -> float | None:
        if cur_v is None or prev_v is None or prev_v == 0:
            return None
        return (cur_v / prev_v - 1.0) * 100.0

    rev_yoy = _yoy(cur.get("revenue"), prev_y.get("revenue"))
    ni_yoy = _yoy(cur.get("net_income"), prev_y.get("net_income"))

    cagr = None
    newest = cur.get("revenue")
    oldest_row = None
    for r in rows:
        if cur.get("year") and r.get("year") and int(r["year"]) <= int(cur["year"]) - 2:
            oldest_row = r
    if newest and oldest_row and oldest_row.get("revenue") and oldest_row["revenue"] > 0:
        try:
            years = max(1.0, float(int(cur["year"]) - int(oldest_row["year"])))
            cagr = ((float(newest) / float(oldest_row["revenue"])) ** (1.0 / years) - 1.0) * 100.0
        except Exception:
            cagr = None

    label = "Trung tính"
    if rev_yoy is not None:
        if rev_yoy >= 15:
            label = "Doanh thu tăng mạnh (YoY)"
        elif rev_yoy >= 5:
            label = "Doanh thu tăng (YoY)"
        elif rev_yoy <= -10:
            label = "Doanh thu suy giảm (YoY)"
        elif rev_yoy < 0:
            label = "Doanh thu giảm nhẹ (YoY)"

    def _fmt_period(y: int, q: int) -> str:
        if y <= 0:
            return "—"
        if q and q > 0:
            return f"Q{q}/{y}"
        return f"Năm {y}"

    py = int(prev_y.get("year") or 0)
    pq = int(prev_y.get("quarter") or 0)
    yoy_compare_label_vi = f"{_fmt_period(cy, cq)} so với {_fmt_period(py, pq)}"

    return {
        "periods": len(rows),
        "latest_year": cur.get("year"),
        "latest_quarter": cur.get("quarter"),
        "yoy_prior_year": py,
        "yoy_prior_quarter": pq,
        "yoy_compare_label_vi": yoy_compare_label_vi,
        "revenue_yoy_pct": rev_yoy,
        "profit_yoy_pct": ni_yoy,
        "revenue_cagr_3y_pct": cagr,
        "trend_label": label,
        "compare_note": "YoY: ưu tiên cùng quý năm trước trong chuỗi; nếu không có thì dùng kỳ gần tương đương.",
    }
