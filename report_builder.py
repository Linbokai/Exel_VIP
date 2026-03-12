"""
客服日报生成器
==============
将七鱼 OpenAPI 返回的工单数据处理为标准化日报。

日报结构：
  顶部：问题类型统计表
  1. 重大事件报备（批量问题）
  2. 待跟进问题总计
  3. 客服未回访/未跟进问题总计
  4. 超R反馈问题（10w+）
  5. 预流失报备
  6. 预投诉报备
  7. 其他VIP用户反馈
"""
import os
import re
import logging
import tempfile
from datetime import datetime
from collections import Counter, defaultdict
from pathlib import Path

from config import (
    SUPER_R_THRESHOLD, ISSUE_KEYWORDS,
    OUTPUT_DIR, HANDLER_DEV_KEYWORD, HANDLER_SYSTEM_KEYWORD,
    CF_ISSUE_TYPE, CF_RECHARGE, CF_ISSUE_SELECT,
    STATUS_SOLVED, STATUS_CLOSED,
    ts_to_str,
)

logger = logging.getLogger(__name__)


# ==================== 工单字段提取工具 ====================

def get_custom_field(custom_list, field_name, default=""):
    """从自定义字段列表中提取值"""
    if isinstance(custom_list, list):
        for item in custom_list:
            name = item.get("name", "") or item.get("key", "")
            if name == field_name:
                return item.get("value", default)
    elif isinstance(custom_list, dict):
        return custom_list.get(field_name, default)
    return default


def parse_amount(raw):
    """解析金额字符串为数值（元）"""
    if isinstance(raw, (int, float)):
        return float(raw)
    if isinstance(raw, str):
        raw = raw.replace(",", "").replace("，", "").strip()
        m = re.match(r"([\d.]+)\s*[万wW]", raw)
        if m:
            return float(m.group(1)) * 10000
        try:
            return float(raw)
        except ValueError:
            return 0
    return 0


def enrich_ticket_fields(t):
    """为工单提取常用便捷属性，以 _ 前缀存储"""
    t.setdefault("_handler", "")
    t["_id"] = t.get("id", "")
    t["_title"] = t.get("title", "")
    t["_content"] = t.get("content", "") or t.get("title", "")

    custom = t.get("custom", [])

    # 发起人优先级：角色名(自定义字段) > 标题中提取 > crmUserName > userName > creator
    role_name = get_custom_field(custom, "角色名")
    title_name = ""
    title = t.get("title", "")
    m = re.search(r"】(.+?)[:：]", title)
    if m:
        title_name = m.group(1).strip()
    t["_creator"] = (
        role_name or title_name
        or t.get("crmUserName", "") or t.get("userName", "") or t.get("creator", "")
    )

    t["_create_time"] = t.get("createTime", 0)
    t["_update_time"] = t.get("updateTime", 0) or t.get("createTime", 0)
    t["_status"] = t.get("status", -1)

    t["_issue_select"] = get_custom_field(custom, CF_ISSUE_SELECT)
    t["_issue_type"] = get_custom_field(custom, CF_ISSUE_TYPE)
    t["_recharge"] = parse_amount(get_custom_field(custom, CF_RECHARGE, "0"))


def classify_ticket(t):
    """根据关键词对工单进行问题分类"""
    text = (t.get("_content") or "") + (t.get("_title") or "")
    for category, keywords in ISSUE_KEYWORDS.items():
        if any(kw in text for kw in keywords):
            return category
    return "其他问题"


# ==================== 统计计算 ====================

def compute_category_stats(session_data, daily_tickets):
    """
    计算问题类型统计数据（从会话监控数据中提取）。
    返回 (categories, cat_alarm, cat_dev) 三元组。
      - categories:  类别名称列表
      - cat_alarm:   Counter {类别: 报警会话数}
      - cat_dev:     Counter {类别: 运营/研发介入数}
    """
    categories = list(ISSUE_KEYWORDS.keys()) + ["其他问题"]

    dev_creators = {
        t["_creator"] for t in daily_tickets
        if HANDLER_DEV_KEYWORD in (t.get("_handler") or "")
    }

    cat_alarm = Counter()
    cat_dev = Counter()
    for sess in session_data:
        content = sess.get("content", "") or sess.get("message", "") or ""
        visitor = sess.get("visitorName", "") or sess.get("userId", "")
        matched = False
        for category, keywords in ISSUE_KEYWORDS.items():
            if any(kw in content for kw in keywords):
                cat_alarm[category] += 1
                if visitor in dev_creators:
                    cat_dev[category] += 1
                matched = True
                break
        if not matched:
            cat_alarm["其他问题"] += 1
            if visitor in dev_creators:
                cat_dev["其他问题"] += 1

    return categories, cat_alarm, cat_dev


# ==================== 日报构建器 ====================

class ReportBuilder:
    """客服日报构建器"""

    def __init__(self, daily_tickets=None, pending_tickets=None,
                 total_sessions=0, report_date=None, session_data=None):
        """
        :param daily_tickets:   当日工单列表（昨日18:00~当日17:59）
        :param pending_tickets: 近30天待跟进工单列表
        :param total_sessions:  总会话量（来自坐席报表）
        :param report_date:     日报日期
        :param session_data:    会话监控数据（预留，暂未接入）
        """
        self.daily_tickets = daily_tickets or []
        self.pending_tickets = pending_tickets or []
        self.total_sessions = total_sessions
        self.report_date = report_date or datetime.now()
        self.session_data = session_data or []

        # 预处理：提取每条工单的便捷属性
        for t in self.daily_tickets + self.pending_tickets:
            enrich_ticket_fields(t)

    # ==================== 内容摘要 ====================

    def _summarize(self, t, max_len=80):
        """精简总结工单内容"""
        parts = []
        title = t.get("_title", "")
        content = t.get("_content", "")
        if title:
            parts.append(title)
        if content and content != title:
            parts.append(content)

        for entry in reversed(t.get("_log", [])):
            for info in entry.get("info", []):
                c = info.get("content", "")
                if c and "回复" in info.get("title", info.get("titleLang", "")):
                    parts.append(f"[回复]{c}")
                    break
            if len(parts) >= 3:
                break

        summary = "；".join(parts)
        if len(summary) > max_len:
            summary = summary[:max_len - 3] + "..."
        return summary or "（无内容）"

    # ==================== 格式化 ====================

    def _format_order(self, t, show_amount=False, show_resolved=False):
        """格式化单条工单"""
        oid = t["_id"]
        summary = self._summarize(t)
        creator = t["_creator"] or "未知"
        ct = ts_to_str(t["_create_time"])
        ut = ts_to_str(t["_update_time"])

        line = f"工单号：{oid}，工单内容：{summary}"
        if show_amount:
            amt = t["_recharge"]
            amt_str = f"{amt/10000:.1f}W" if amt >= 10000 else f"{amt:.0f}元"
            line += f"（{amt_str}）"

        line += f"\n    发起人：{creator}，创建时间：{ct}，更新时间：{ut}"

        if show_resolved:
            resolved = t["_status"] in (STATUS_SOLVED, STATUS_CLOSED)
            line += f"，{'已解决' if resolved else '未解决'}"

        return line

    def _format_list(self, orders, **kwargs):
        """格式化工单列表"""
        if not orders:
            return "  无\n"
        lines = []
        for i, t in enumerate(orders, 1):
            lines.append(f"  {i}. {self._format_order(t, **kwargs)}")
        return "\n".join(lines) + "\n"

    # ==================== 七个板块 ====================

    def section_1_major_events(self):
        """一、重大事件报备（批量问题）"""
        title = "一、重大事件报备（批量问题）"
        if not self.session_data:
            return f"{title}\n  无\n"

        keyword_users = defaultdict(set)
        for sess in self.session_data:
            content = sess.get("content", "") or sess.get("message", "") or ""
            visitor = sess.get("visitorName", "") or sess.get("userId", "unknown")
            for category, keywords in ISSUE_KEYWORDS.items():
                if any(kw in content for kw in keywords):
                    keyword_users[category].add(visitor)
                    break

        daily_creators = {t["_creator"] for t in self.daily_tickets if t["_creator"]}
        batch_issues = [
            (cat, len(users), list(users)[:5])
            for cat, users in keyword_users.items()
            if len(users) >= 2 and (users & daily_creators)
        ]

        if not batch_issues:
            return f"{title}\n  无\n"

        lines = [title]
        for cat, count, users in batch_issues:
            lines.append(f"  【{cat}】{count}位用户反馈，涉及用户：{'、'.join(users)}")
        return "\n".join(lines) + "\n"

    def section_2_pending(self):
        """二、待跟进问题总计"""
        title = "二、待跟进问题总计（近30天，受理方：飞鱼科技）"
        orders = [
            t for t in self.pending_tickets
            if HANDLER_DEV_KEYWORD in (t.get("_handler") or "")
        ]
        return f"{title}\n  共 {len(orders)} 条\n{self._format_list(orders)}"

    def section_3_unvisited(self):
        """三、客服未回访/未跟进问题总计"""
        title = "三、客服未回访/未跟进问题总计（近30天，受理方：工单系统）"
        orders = [
            t for t in self.pending_tickets
            if HANDLER_SYSTEM_KEYWORD in (t.get("_handler") or "")
            or (t.get("_handler") or "").strip() in ("", "空")
        ]
        return f"{title}\n  共 {len(orders)} 条\n{self._format_list(orders)}"

    def section_4_super_r(self):
        """四、超R反馈问题（10w+）"""
        title = "四、超R反馈问题（10w+）"
        orders = sorted(
            [t for t in self.daily_tickets if t["_recharge"] >= SUPER_R_THRESHOLD],
            key=lambda x: x["_recharge"], reverse=True,
        )
        if not orders:
            return f"{title}\n  无\n"
        return f"{title}\n  共 {len(orders)} 条\n{self._format_list(orders, show_amount=True)}"

    def section_5_pre_churn(self):
        """五、预流失报备"""
        title = "五、预流失报备"
        orders = [t for t in self.daily_tickets if t.get("_issue_select") == "预流失"]
        if not orders:
            return f"{title}\n  无\n"
        return f"{title}\n  共 {len(orders)} 条\n{self._format_list(orders)}"

    def section_6_pre_complaint(self):
        """六、预投诉报备"""
        title = "六、预投诉报备"
        orders = [t for t in self.daily_tickets if t.get("_issue_select") == "我要投诉"]
        if not orders:
            return f"{title}\n  无\n"
        return f"{title}\n  共 {len(orders)} 条\n{self._format_list(orders)}"

    def section_7_other(self):
        """七、其他VIP用户反馈"""
        title = "七、其他VIP用户反馈"
        classified_selects = {"预流失", "我要投诉"}
        orders = [
            t for t in self.daily_tickets
            if t.get("_issue_select") not in classified_selects
            and t["_recharge"] < SUPER_R_THRESHOLD
        ]
        if not orders:
            return f"{title}\n  无\n"

        grouped = defaultdict(list)
        for t in orders:
            grouped[classify_ticket(t)].append(t)

        lines = [title, f"  共 {len(orders)} 条"]
        for cat, group in grouped.items():
            lines.append(f"\n  【{cat}】({len(group)}条)")
            for i, t in enumerate(group, 1):
                lines.append(f"  {i}. {self._format_order(t, show_resolved=True)}")
        return "\n".join(lines) + "\n"

    # ==================== 统计表 ====================

    def section_stats(self):
        """问题类型统计（文本版）"""
        title = "附：问题类型统计"
        categories, cat_alarm, cat_dev = compute_category_stats(
            self.session_data, self.daily_tickets,
        )
        total_alarm = sum(cat_alarm.values())
        total_dev = sum(cat_dev.values())
        total_agent = total_alarm - total_dev

        lines = [
            title, "",
            f"  {'问题类型':<10} {'客服处理':>8} {'运营/研发介入':>12}",
            f"  {'─'*10} {'─'*8} {'─'*12}",
        ]
        for cat in categories:
            a = cat_alarm.get(cat, 0)
            d = cat_dev.get(cat, 0)
            if a > 0:
                lines.append(f"  {cat:<10} {a-d:>8} {d:>12}")

        lines.extend([
            f"  {'─'*10} {'─'*8} {'─'*12}",
            f"  {'问题总量':<10} {total_agent:>8} {total_dev:>12}    合计：{total_alarm}",
            f"  总会话量：{self.total_sessions}",
        ])

        if not self.session_data:
            lines.insert(1, "  [注] 暂无会话监控数据，统计表待补充")

        return "\n".join(lines) + "\n"

    # ==================== 生成完整日报 ====================

    def build(self):
        """生成完整日报文本"""
        date_str = self.report_date.strftime("%Y年%m月%d日")
        header = (
            f"{'='*50}\n"
            f"  VIP客服日报 - {date_str}\n"
            f"{'='*50}\n"
        )
        sections = [
            header,
            self.section_1_major_events(),
            self.section_2_pending(),
            self.section_3_unvisited(),
            self.section_4_super_r(),
            self.section_5_pre_churn(),
            self.section_6_pre_complaint(),
            self.section_7_other(),
            self.section_stats(),
        ]
        report = "\n".join(sections)
        report += f"\n{'='*50}\n  报告生成时间：{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
        return report

    # ==================== 文件输出 ====================

    def _output_path(self, ext):
        """生成默认输出文件路径"""
        date_str = self.report_date.strftime("%Y%m%d")
        return Path(OUTPUT_DIR) / f"VIP客服日报_{date_str}.{ext}"

    def save_text(self, filepath=None):
        """保存文本日报"""
        filepath = filepath or self._output_path("txt")
        report = self.build()
        Path(filepath).write_text(report, encoding="utf-8")
        return str(filepath)

    def save_excel(self, filepath=None):
        """保存Excel日报"""
        from openpyxl import Workbook
        from openpyxl.styles import Font, Alignment, PatternFill, Border, Side

        filepath = filepath or self._output_path("xlsx")
        wb = Workbook()
        ws = wb.active

        _illegal_chars_re = re.compile(r'[\x00-\x08\x0b\x0c\x0e-\x1f]')

        def safe(val):
            return _illegal_chars_re.sub('', val) if isinstance(val, str) else val

        ws.title = "VIP客服日报"

        # ---- 样式 ----
        title_font = Font(bold=True, size=14)
        header_font = Font(bold=True, size=10, color="FFFFFF")
        section_font = Font(bold=True, size=11, color="2F5496")
        normal_font = Font(size=10)
        bold_font = Font(bold=True, size=10)
        header_fill = PatternFill(start_color="2F5496", end_color="2F5496", fill_type="solid")
        light_fill = PatternFill(start_color="D6E4F0", end_color="D6E4F0", fill_type="solid")
        thin_border = Border(
            left=Side(style="thin"), right=Side(style="thin"),
            top=Side(style="thin"), bottom=Side(style="thin"),
        )
        center = Alignment(horizontal="center")

        row = 1
        date_str_cn = self.report_date.strftime("%Y年%m月%d日")

        # ---- 标题 ----
        ws.merge_cells(start_row=row, start_column=1, end_row=row, end_column=8)
        ws.cell(row=row, column=1, value=safe(f"VIP客服日报 - {date_str_cn}")).font = title_font
        row += 2

        # ---- 统计表（使用共享计算逻辑） ----
        stat_headers = ["问题类型", "报警会话", "客服处理", "运营/研发介入", "问题总量"]
        for col, h in enumerate(stat_headers, 1):
            cell = ws.cell(row=row, column=col, value=h)
            cell.font = header_font
            cell.fill = header_fill
            cell.border = thin_border
            cell.alignment = center
        row += 1

        categories, cat_alarm, cat_dev = compute_category_stats(
            self.session_data, self.daily_tickets,
        )

        for cat in categories:
            a = cat_alarm.get(cat, 0)
            d = cat_dev.get(cat, 0)
            vals = [cat, a, a - d, d, a]
            for col, v in enumerate(vals, 1):
                cell = ws.cell(row=row, column=col, value=safe(v))
                cell.font = normal_font
                cell.border = thin_border
                if col > 1:
                    cell.alignment = center
            row += 1

        # 合计行
        total_a = sum(cat_alarm.values())
        total_d = sum(cat_dev.values())
        for col, v in enumerate(["合计", total_a, total_a - total_d, total_d, total_a], 1):
            cell = ws.cell(row=row, column=col, value=safe(v))
            cell.font = bold_font
            cell.border = thin_border
            cell.fill = light_fill
            if col > 1:
                cell.alignment = center
        row += 1

        ws.cell(row=row, column=1, value=safe(f"总会话量：{self.total_sessions}")).font = bold_font
        row += 2

        # ---- 七个板块 ----
        sections_text = [
            self.section_1_major_events(),
            self.section_2_pending(),
            self.section_3_unvisited(),
            self.section_4_super_r(),
            self.section_5_pre_churn(),
            self.section_6_pre_complaint(),
            self.section_7_other(),
        ]

        ws.merge_cells(start_row=row, start_column=1, end_row=row, end_column=8)
        ws.cell(row=row, column=1, value=safe("昨日VIP反馈")).font = section_font
        row += 1

        section_prefixes = ("一、", "二、", "三、", "四、", "五、", "六、", "七、")
        for section in sections_text:
            for line in section.strip().split("\n"):
                line = line.strip()
                if not line:
                    continue
                ws.merge_cells(start_row=row, start_column=1, end_row=row, end_column=8)
                font = section_font if line.startswith(section_prefixes) else normal_font
                ws.cell(row=row, column=1, value=safe(line)).font = font
                row += 1
            row += 1

        # ---- 列宽 ----
        ws.column_dimensions["A"].width = 20
        for c in ["B", "C", "D", "E"]:
            ws.column_dimensions[c].width = 15

        # 写入临时文件再替换（避免文件被占用时写入失败）
        tmp_fd, tmp_path = tempfile.mkstemp(suffix=".xlsx", dir=Path(filepath).parent)
        try:
            os.close(tmp_fd)
            wb.save(tmp_path)
            target = Path(filepath)
            if target.exists():
                try:
                    target.unlink()
                except PermissionError:
                    ts = datetime.now().strftime("%H%M%S")
                    filepath = str(target.with_stem(target.stem + f"_{ts}"))
            Path(tmp_path).replace(filepath)
        except Exception:
            Path(tmp_path).unlink(missing_ok=True)
            raise

        logger.info(f"Excel日报已保存: {filepath}")
        return str(filepath)
