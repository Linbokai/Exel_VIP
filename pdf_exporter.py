"""
PDF 日报导出
============
将 ReportBuilder 实例的数据导出为中文 PDF 文件。
自动检测系统中文字体（Windows/Linux），降级使用 Helvetica。
"""
import os
import logging
from datetime import datetime


logger = logging.getLogger(__name__)

# 中文字体候选路径（按优先级排列）
_FONT_PATHS = [
    "C:/Windows/Fonts/msyh.ttc",
    "C:/Windows/Fonts/simhei.ttf",
    "C:/Windows/Fonts/simsun.ttc",
    "/usr/share/fonts/truetype/wqy/wqy-microhei.ttc",
    "/usr/share/fonts/opentype/noto/NotoSansCJK-Regular.ttc",
]

# 字体名称缓存：避免每次生成 PDF 都重复执行文件 I/O 和注册
_cached_font_name = None


def _register_chinese_font():
    """注册中文字体，返回字体名称（重复调用直接返回缓存结果）"""
    global _cached_font_name
    if _cached_font_name is not None:
        return _cached_font_name

    from reportlab.pdfbase import pdfmetrics
    from reportlab.pdfbase.ttfonts import TTFont

    for fp in _FONT_PATHS:
        if os.path.exists(fp):
            try:
                pdfmetrics.registerFont(TTFont("ChineseFont", fp))
                _cached_font_name = "ChineseFont"
                return _cached_font_name
            except Exception:
                continue

    logger.warning("未找到中文字体，PDF可能无法正确显示中文")
    _cached_font_name = "Helvetica"
    return _cached_font_name


def build_pdf(builder, filepath=None):
    """
    生成 PDF 日报并写入文件，返回文件路径。
    :param builder: ReportBuilder 实例
    :param filepath: 输出路径，None 则使用默认路径
    :return: 保存路径字符串
    """
    from reportlab.lib import colors
    from reportlab.lib.pagesizes import A4
    from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
    from reportlab.lib.units import mm
    from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle

    filepath = filepath or builder._output_path("pdf")
    font_name = _register_chinese_font()

    doc = SimpleDocTemplate(
        str(filepath), pagesize=A4,
        leftMargin=15*mm, rightMargin=15*mm,
        topMargin=15*mm, bottomMargin=15*mm,
    )

    styles = getSampleStyleSheet()
    title_style = ParagraphStyle(
        'ChTitle', parent=styles['Title'],
        fontName=font_name, fontSize=16, spaceAfter=12,
    )
    heading_style = ParagraphStyle(
        'ChHeading', parent=styles['Heading2'],
        fontName=font_name, fontSize=12, spaceAfter=6,
        textColor=colors.HexColor("#2F5496"),
    )
    body_style = ParagraphStyle(
        'ChBody', parent=styles['Normal'],
        fontName=font_name, fontSize=9, leading=14,
        spaceAfter=4,
    )
    footer_style = ParagraphStyle(
        'Footer', parent=styles['Normal'],
        fontName=font_name, fontSize=8, textColor=colors.grey,
    )

    elements = []
    date_str_cn = builder.report_date.strftime("%Y年%m月%d日")
    elements.append(Paragraph(f"VIP客服日报 - {date_str_cn}", title_style))
    elements.append(Spacer(1, 6*mm))

    # 概览统计表
    overview_data = [
        ["当日工单", "待跟进", "总会话量", "超R工单", "预流失", "预投诉"],
        [
            str(len(builder.daily_tickets)),
            str(len(builder.pending_tickets)),
            str(builder.total_sessions),
            str(len(builder._super_r)),
            str(len(builder._pre_churn)),
            str(len(builder._pre_complaint)),
        ],
    ]
    overview_table = Table(overview_data, colWidths=[28*mm]*6)
    overview_table.setStyle(TableStyle([
        ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor("#2F5496")),
        ('TEXTCOLOR', (0, 0), (-1, 0), colors.white),
        ('FONTNAME', (0, 0), (-1, -1), font_name),
        ('FONTSIZE', (0, 0), (-1, 0), 9),
        ('FONTSIZE', (0, 1), (-1, 1), 14),
        ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
        ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
        ('GRID', (0, 0), (-1, -1), 0.5, colors.grey),
        ('BOTTOMPADDING', (0, 0), (-1, -1), 6),
        ('TOPPADDING', (0, 0), (-1, -1), 6),
    ]))
    elements.append(overview_table)
    elements.append(Spacer(1, 8*mm))

    # 各板块文本（复用 builder.build() 生成的文本）
    report_text = builder.build()
    for line in report_text.split("\n"):
        line = line.rstrip()
        if not line:
            elements.append(Spacer(1, 2*mm))
            continue
        if line.startswith("="):
            continue
        if line.lstrip().startswith(("一、", "二、", "三、", "四、", "五、", "六、", "七、", "附：")):
            elements.append(Paragraph(line.strip(), heading_style))
        else:
            safe_line = (line.replace("&", "&amp;")
                         .replace("<", "&lt;")
                         .replace(">", "&gt;"))
            elements.append(Paragraph(safe_line, body_style))

    # 页脚
    elements.append(Spacer(1, 10*mm))
    elements.append(Paragraph(
        f"报告生成时间：{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
        footer_style,
    ))

    doc.build(elements)
    logger.info(f"PDF日报已保存: {filepath}")
    return str(filepath)
