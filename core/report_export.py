"""Xuất báo cáo định giá (Markdown / HTML) — mở HTML trong trình duyệt và In → PDF."""

from __future__ import annotations

import html
from datetime import datetime, timezone
from typing import Any


def _fmt_num(v: Any, *, digits: int = 0) -> str:
    if v is None:
        return "—"
    try:
        f = float(v)
    except (TypeError, ValueError):
        return str(v)
    if digits == 0:
        return f"{f:,.0f}"
    return f"{f:,.{digits}f}"


def investment_report_markdown(summary: dict[str, Any]) -> str:
    sym = summary.get("symbol", "")
    name = summary.get("name") or sym
    cur = summary.get("currency", "VND")
    lines = [
        f"# Báo cáo định giá — {name} (`{sym}`)",
        "",
        f"- **Thời điểm xuất:** {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}",
        f"- **Nguồn dữ liệu:** {summary.get('data_source', 'unknown')}",
        "",
        "## Chỉ số chính",
        "",
        f"| Chỉ tiêu | Giá trị |",
        f"| --- | --- |",
        f"| Giá thị trường ({cur}) | {_fmt_num(summary.get('price'))} |",
        f"| Giá trị Graham | {_fmt_num(summary.get('intrinsic_value_graham'))} |",
        f"| Mục tiêu P/E forward | {_fmt_num(summary.get('target_price_forward_pe'))} |",
        f"| Mục tiêu P/B (fair) | {_fmt_num(summary.get('target_price_pb_fair'))} |",
        f"| Mục tiêu tổng hợp | {_fmt_num(summary.get('composite_target_price'))} |",
        f"| MOS Graham (%) | {_fmt_num(summary.get('margin_of_safety_pct'), digits=1) if summary.get('margin_of_safety_pct') is not None else '—'} |",
        f"| MOS tổng hợp (%) | {_fmt_num(summary.get('margin_of_safety_composite_pct'), digits=1) if summary.get('margin_of_safety_composite_pct') is not None else '—'} |",
        f"| F-Score | {summary.get('piotroski_score', '—')}/9 |",
        "",
        "## Ngành & trọng số",
        "",
        summary.get("valuation_transparency_line", "").replace("**", ""),
        "",
        "## Gợi ý",
        "",
        summary.get("advice", ""),
        "",
        "---",
        "_Báo cáo mang tính tham khảo, không phải tư vấn đầu tư._",
    ]
    return "\n".join(lines)


def investment_report_html(summary: dict[str, Any]) -> str:
    sym = html.escape(str(summary.get("symbol", "")))
    name = html.escape(str(summary.get("name") or summary.get("symbol") or ""))
    cur = html.escape(str(summary.get("currency", "VND")))
    trans = html.escape(
        (summary.get("valuation_transparency_line") or "").replace("**", "").replace("_", "")
    )
    advice = html.escape(str(summary.get("advice", "")))

    def cell(label: str, val: Any) -> str:
        v = val
        if isinstance(val, float):
            v = _fmt_num(val, digits=1 if "MOS" in label or "%" in label else 0)
        elif val is None:
            v = "—"
        return f"<tr><td>{html.escape(label)}</td><td>{html.escape(str(v))}</td></tr>"

    rows = [
        cell(f"Giá thị trường ({cur})", _fmt_num(summary.get("price"))),
        cell("Giá trị Graham", _fmt_num(summary.get("intrinsic_value_graham"))),
        cell("Mục tiêu P/E forward", _fmt_num(summary.get("target_price_forward_pe"))),
        cell("Mục tiêu P/B (fair)", _fmt_num(summary.get("target_price_pb_fair"))),
        cell("Mục tiêu tổng hợp", _fmt_num(summary.get("composite_target_price"))),
        cell(
            "MOS Graham (%)",
            _fmt_num(summary.get("margin_of_safety_pct"), digits=1)
            if summary.get("margin_of_safety_pct") is not None
            else "—",
        ),
        cell(
            "MOS tổng hợp (%)",
            _fmt_num(summary.get("margin_of_safety_composite_pct"), digits=1)
            if summary.get("margin_of_safety_composite_pct") is not None
            else "—",
        ),
        cell("F-Score", f"{summary.get('piotroski_score', '—')}/9"),
        cell("Nguồn", summary.get("data_source", "unknown")),
    ]

    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    return f"""<!DOCTYPE html>
<html lang="vi">
<head>
<meta charset="utf-8"/>
<meta name="viewport" content="width=device-width, initial-scale=1"/>
<title>Báo cáo {sym}</title>
<style>
body {{ font-family: Segoe UI, system-ui, sans-serif; margin: 2rem; color: #1a1a1a; }}
h1 {{ font-size: 1.35rem; }}
table {{ border-collapse: collapse; width: 100%; max-width: 520px; margin: 1rem 0; }}
td {{ border: 1px solid #ccc; padding: 0.5rem 0.75rem; }}
td:first-child {{ background: #f6f6f6; width: 52%; }}
.note {{ font-size: 0.9rem; color: #444; margin-top: 1.5rem; }}
.footer {{ margin-top: 2rem; font-size: 0.8rem; color: #666; }}
</style>
</head>
<body>
<h1>Báo cáo định giá — {name} ({sym})</h1>
<p><strong>Thời điểm:</strong> {html.escape(now)}</p>
<table>
{"".join(rows)}
</table>
<p class="note">{trans}</p>
<p><strong>Gợi ý:</strong> {advice}</p>
<p class="footer">Báo cáo mang tính tham khảo, không phải tư vấn đầu tư. Dùng chức năng In của trình duyệt để lưu PDF.</p>
</body>
</html>
"""


def premium_storytelling_report_markdown(report: dict[str, Any]) -> str:
    """
    Story-style report for premium users (backward-compatible API expected by tests).
    """
    val = report.get("valuation") if isinstance(report.get("valuation"), dict) else {}
    fin = report.get("financials") if isinstance(report.get("financials"), dict) else {}
    phase = report.get("phase") if isinstance(report.get("phase"), dict) else {}
    rp = report.get("risk_plan") if isinstance(report.get("risk_plan"), dict) else {}
    news = report.get("news") if isinstance(report.get("news"), list) else []

    sym = str(report.get("ticker") or val.get("symbol") or "")
    name = str(val.get("name") or sym)
    cluster = str(val.get("industry_cluster_id") or "").strip().lower()
    cluster_label = {
        "tech_telecom": "Công nghệ/Viễn thông",
        "bank_finance": "Tài chính/Ngân hàng",
        "consumer": "Tiêu dùng",
    }.get(cluster, "Ngành khác")

    lines = [
        f"# Premium Story Report - {name} ({sym})",
        "",
        "## Câu chuyện doanh nghiệp",
        f"- Doanh nghiệp thuộc nhóm **{cluster_label}**.",
        f"- Tăng trưởng doanh thu YoY: {_fmt_num(fin.get('revenue_growth_yoy'), digits=1)}%.",
        f"- Đòn bẩy D/E: {_fmt_num(fin.get('debt_to_equity'), digits=2)}.",
        "",
        "## Góc nhìn định giá Graham",
        f"- Giá hiện tại: {_fmt_num(val.get('price'))} VND.",
        f"- Giá trị hợp lý tổng hợp: {_fmt_num(val.get('composite_target_price'))} VND.",
        f"- Margin of Safety: {_fmt_num(val.get('margin_of_safety_composite_pct'), digits=1)}%.",
        f"- F-Score: {val.get('piotroski_score', '—')}/9.",
        "",
        "## Kế hoạch hành động",
        f"- Pha kỹ thuật: {phase.get('phase', 'neutral')} ({phase.get('reason', '')}).",
        f"- Vùng mua: {rp.get('buy_zone', {}).get('low', '—')} - {rp.get('buy_zone', {}).get('high', '—')}.",
        f"- Chốt lời / Cắt lỗ: {rp.get('take_profit_price', '—')} / {rp.get('stop_loss_price', '—')}.",
    ]
    if news:
        lines.extend(["", "## Điểm tin gần nhất", f"- {news[0].get('title', '')}"])
    lines.extend(["", "_Tài liệu tham khảo, không phải khuyến nghị đầu tư bắt buộc._"])
    return "\n".join(lines)
