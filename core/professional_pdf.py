"""Báo cáo PDF chuyên nghiệp (ReportLab + font Unicode DejaVu)."""

from __future__ import annotations

from datetime import datetime, timezone
from io import BytesIO
from typing import Any

from reportlab.lib import colors
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
from reportlab.lib.units import cm, mm
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont
from reportlab.platypus import (
    Image,
    PageBreak,
    Paragraph,
    SimpleDocTemplate,
    Spacer,
    Table,
    TableStyle,
)

from app_constants import action_vi, phase_vi


def _register_unicode_font() -> str:
    """Đăng ký DejaVu Sans (matplotlib) để hiển thị tiếng Việt."""
    name = "DejaVuSansCustom"
    try:
        pdfmetrics.getFont(name)
        return name
    except Exception:
        pass
    try:
        import matplotlib.font_manager as fm

        path = fm.findfont(fm.FontProperties(family="DejaVu Sans"))
        if path and path.lower().endswith((".ttf", ".ttc")):
            pdfmetrics.registerFont(TTFont(name, path))
            return name
    except Exception:
        pass
    try:
        from matplotlib import get_data_path
        from pathlib import Path

        p = Path(get_data_path()) / "fonts" / "ttf" / "DejaVuSans.ttf"
        if p.is_file():
            pdfmetrics.registerFont(TTFont(name, str(p)))
            return name
    except Exception:
        pass
    raise RuntimeError(
        "Không tìm thấy font DejaVu Sans. Cài matplotlib hoặc đặt font Unicode."
    )


def _page_num(canvas: Any, doc: Any) -> None:
    canvas.saveState()
    canvas.setFont(doc._font_name, 9)
    canvas.setFillColor(colors.HexColor("#444444"))
    w, h = A4
    canvas.drawCentredString(w / 2, 12 * mm, f"Trang {doc.page}")
    canvas.restoreState()


def build_professional_report_pdf(
    ticker: str,
    report: dict[str, Any],
    valuation: dict[str, Any],
    *,
    ohlcv_png: bytes | None = None,
    allocation_png: bytes | None = None,
    analysis_date: datetime | None = None,
) -> bytes:
    """
    PDF khổ A4: Executive summary, 7 Whys, hình ảnh biểu đồ, kế hoạch vốn (100 triệu).
    """
    font = _register_unicode_font()
    when = analysis_date or datetime.now(timezone.utc)
    sym = (ticker or "—").strip().upper()
    buf = BytesIO()
    doc = SimpleDocTemplate(
        buf,
        pagesize=A4,
        rightMargin=18 * mm,
        leftMargin=18 * mm,
        topMargin=16 * mm,
        bottomMargin=20 * mm,
    )
    doc._font_name = font

    styles = getSampleStyleSheet()
    title_style = ParagraphStyle(
        name="TitleVI",
        parent=styles["Heading1"],
        fontName=font,
        fontSize=16,
        leading=20,
        textColor=colors.HexColor("#0d1b2a"),
    )
    h2 = ParagraphStyle(
        name="H2VI",
        parent=styles["Heading2"],
        fontName=font,
        fontSize=12,
        leading=15,
        spaceBefore=10,
        spaceAfter=6,
        textColor=colors.HexColor("#1b263b"),
    )
    body = ParagraphStyle(
        name="BodyVI",
        parent=styles["Normal"],
        fontName=font,
        fontSize=9.5,
        leading=12,
    )
    small = ParagraphStyle(
        name="SmallVI",
        parent=styles["Normal"],
        fontName=font,
        fontSize=8,
        leading=10,
        textColor=colors.HexColor("#555555"),
    )

    story: list[Any] = []

    # --- Header ---
    story.append(Paragraph("Investment Intelligence — Báo cáo chuyên gia", title_style))
    story.append(Spacer(1, 0.2 * cm))
    story.append(
        Paragraph(
            f"<b>Mã:</b> {sym} &nbsp;|&nbsp; <b>Ngày phân tích:</b> {when.strftime('%d/%m/%Y %H:%M UTC')}",
            body,
        )
    )
    story.append(
        Paragraph(
            "[Logo] &nbsp; <i>Placeholder thương hiệu — thay bằng logo của bạn</i>",
            small,
        )
    )
    story.append(Spacer(1, 0.35 * cm))

    # Phần 1: Executive Summary
    story.append(Paragraph("Phần 1 — Tóm tắt điều hành (Executive Summary)", h2))
    fa = str(report.get("final_action") or "WATCH")
    fa_vi = action_vi(fa)
    phase = (report.get("phase") or {}) or {}
    ph_label = phase_vi(str(phase.get("phase", "neutral")))
    mos = valuation.get("margin_of_safety_composite_pct")
    mos_s = f"{float(mos):,.1f}%" if mos is not None else "—"
    conf = report.get("confidence_score")
    conf_s = f"{float(conf):,.0f}%" if conf is not None else "—"
    advice = str(valuation.get("advice") or report.get("analysis_text") or "")[:900]
    exec_lines = [
        f"<b>Khuyến nghị hành động (hệ thống):</b> {fa_vi} ({fa})",
        f"<b>Pha thị trường:</b> {ph_label}",
        f"<b>MOS tổng hợp:</b> {mos_s} &nbsp;|&nbsp; <b>Độ tin cậy ước lượng:</b> {conf_s}",
        f"<b>Gợi ý / nhận định:</b> {advice}",
    ]
    for line in exec_lines:
        story.append(Paragraph(line, body))
    story.append(Spacer(1, 0.25 * cm))
    story.append(
        Paragraph(
            "<i>Báo cáo mang tính tham khảo, không phải tư vấn đầu tư có phép. "
            "Đối chiếu BCTC niêm yết và quyết định theo rủi ro cá nhân.</i>",
            small,
        )
    )

    # Phần 2: 7 Whys
    story.append(PageBreak())
    story.append(Paragraph("Phần 2 — Phân tích 7 Whys", h2))
    whys = report.get("whys_steps") or []
    if not whys:
        story.append(Paragraph("Chưa có các bước 7 Whys (thiếu LLM hoặc dữ liệu).", body))
    else:
        for i, text in enumerate(whys, start=1):
            t = str(text).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
            story.append(Paragraph(f"<b>{i}.</b> {t}", body))
            story.append(Spacer(1, 0.15 * cm))
    concl = str(report.get("analysis_text") or "")
    if concl:
        story.append(Spacer(1, 0.2 * cm))
        story.append(Paragraph("<b>Kết luận chiến lược</b>", body))
        story.append(Paragraph(concl.replace("&", "&amp;").replace("<", "&lt;")[:2000], body))

    # Phần 3: đồ thị
    story.append(PageBreak())
    story.append(Paragraph("Phần 3 — Biểu đồ", h2))
    if ohlcv_png:
        story.append(
            Image(BytesIO(ohlcv_png), width=16.5 * cm, height=8.5 * cm, kind="proportional")
        )
    else:
        story.append(Paragraph("Không có hình nến/định giá (thiếu dữ liệu OHLCV).", body))
    story.append(Spacer(1, 0.3 * cm))
    if allocation_png:
        story.append(
            Image(BytesIO(allocation_png), width=12 * cm, height=7 * cm, kind="proportional")
        )

    # Phần 4: kế hoạch vốn — quy mô 100 triệu
    story.append(PageBreak())
    story.append(Paragraph("Phần 4 — Kế hoạch đi vốn (tham chiếu 100.000.000 VND)", h2))
    plan = report.get("risk_plan") or {}
    total_ref = 100_000_000.0
    cap_user = float(report.get("total_capital_vnd") or total_ref)
    scale = (total_ref / cap_user) if cap_user > 0 else 1.0

    entry = float(plan.get("entry_price") or 0)
    sl = float(plan.get("stop_loss_price") or 0)
    tp = float(plan.get("take_profit_price") or 0)
    if tp <= 0:
        try:
            tp = float(report.get("take_profit") or 0)
        except (TypeError, ValueError):
            tp = 0.0
    alloc = float(plan.get("allocated_capital_vnd") or 0) * scale
    max_pct = float(plan.get("max_position_pct") or 0)

    vm = bool(plan.get("value_investing_mode", True))
    sl_label = "Cắt lỗ kỹ thuật (SL)" if not vm else "Cắt lỗ KT (mặc định tắt)"
    sl_show = f"{sl:,.2f} VND" if sl > 0 else ("Không — theo định giá" if vm else "—")
    data = [
        ["Chỉ tiêu", "Giá trị"],
        ["Giá mua tham chiếu (Entry)", f"{entry:,.2f} VND"],
        ["Mục tiêu chốt lời (TP)", f"{tp:,.2f} VND" if tp > 0 else "—"],
        [sl_label, sl_show],
        ["Tỷ trọng gợi ý", f"{max_pct:.1f}% tổng danh mục"],
        ["Vốn phân bổ (ước ~100tr)", f"{alloc:,.0f} VND"],
        ["Rủi ro xấu nhất (ước)", f"{float(plan.get('worst_case_loss_vnd') or 0) * scale:,.0f} VND"],
    ]
    t = Table(data, colWidths=[7.5 * cm, 9 * cm])
    t.setStyle(
        TableStyle(
            [
                ("FONT", (0, 0), (-1, -1), font, 9),
                ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#e0e1dd")),
                ("GRID", (0, 0), (-1, -1), 0.5, colors.grey),
                ("VALIGN", (0, 0), (-1, -1), "TOP"),
                ("LEFTPADDING", (0, 0), (-1, -1), 6),
                ("RIGHTPADDING", (0, 0), (-1, -1), 6),
            ]
        )
    )
    story.append(t)
    story.append(Spacer(1, 0.4 * cm))
    dq = valuation.get("data_confidence_score")
    prov = valuation.get("data_provenance")
    if dq is not None:
        story.append(Paragraph(f"<b>Độ tin cậy dữ liệu (ước):</b> {dq}/5", body))
    if isinstance(prov, dict) and prov:
        fs = prov.get("fundamentals_source", "—")
        ps = prov.get("price_source", "—")
        story.append(Paragraph(f"<b>Nguồn dữ liệu:</b> cơ bản `{fs}` · giá `{ps}`", small))

    doc.build(story, onFirstPage=_page_num, onLaterPages=_page_num)
    return buf.getvalue()
