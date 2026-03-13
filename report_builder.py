"""
客服日报构建器
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

导出格式：
  - 文本：save_text()
  - Excel：save_excel()  →  excel_exporter.build_excel()
  - PDF：  save_pdf()    →  pdf_exporter.build_pdf()
"""
import logging
from datetime import datetime
from collections import defaultdict
from pathlib import Path

from config import (
    SUPER_R_THRESHOLD, ISSUE_KEYWORDS,
    OUTPUT_DIR, HANDLER_DEV_KEYWORD, HANDLER_SYSTEM_KEYWORD,
    ISSUE_SELECT_CHURN, ISSUE_SELECT_COMPLAINT,
    STATUS_SOLVED, STATUS_CLOSED,
    ts_to_str, LLM_ENABLED,
)
from ticket_utils import (
    get_custom_field, parse_amount,
    enrich_ticket_fields, classify_ticket, dedup_tickets,
    compute_category_stats, compute_ticket_category_stats,
)

logger = logging.getLogger(__name__)


class ReportBuilder:
    """客服日报构建器"""

    def __init__(self, daily_tickets=None, pending_tickets=None,
                 total_sessions=0, report_date=None, session_data=None,
                 trend_data=None, errors=None):
        """
        :param daily_tickets:   当日工单列表
        :param pending_tickets: 近30天待跟进工单列表
        :param total_sessions:  总会话量
        :param report_date:     日报日期
        :param session_data:    会话监控数据
        :param trend_data:      趋势对比数据 {"prev_daily_count": int, ...}
        :param errors:          数据获取错误列表
        """
        self.daily_tickets = daily_tickets or []
        self.pending_tickets = pending_tickets or []
        self.total_sessions = total_sessions
        self.report_date = report_date or datetime.now()
        self.session_data = session_data or []
        self.trend_data = trend_data or {}
        self.errors = errors or []

        # 预处理：提取每条工单的便捷属性
        for t in self.daily_tickets + self.pending_tickets:
            enrich_ticket_fields(t)

        # 工单去重
        original_daily = len(self.daily_tickets)
        self.daily_tickets = dedup_tickets(self.daily_tickets)
        self.dedup_removed = original_daily - len(self.daily_tickets)

        # 预计算常用过滤列表（section_*、build_structured、exporters 共享，避免重复遍历）
        _classified = {ISSUE_SELECT_CHURN, ISSUE_SELECT_COMPLAINT}
        self._pending_dev = [
            t for t in self.pending_tickets if HANDLER_DEV_KEYWORD in (t.get("_handler") or "")
        ]
        self._unvisited = [
            t for t in self.pending_tickets
            if HANDLER_SYSTEM_KEYWORD in (t.get("_handler") or "")
            or (t.get("_handler") or "").strip() in ("", "空")
        ]
        self._super_r = sorted(
            [t for t in self.daily_tickets if t["_recharge"] >= SUPER_R_THRESHOLD],
            key=lambda x: x["_recharge"], reverse=True,
        )
        self._pre_churn = [
            t for t in self.daily_tickets if t.get("_issue_select") == ISSUE_SELECT_CHURN
        ]
        self._pre_complaint = [
            t for t in self.daily_tickets if t.get("_issue_select") == ISSUE_SELECT_COMPLAINT
        ]
        self._other = [
            t for t in self.daily_tickets
            if t.get("_issue_select") not in _classified
            and t["_recharge"] < SUPER_R_THRESHOLD
        ]

        # 预计算问题类型统计（section_stats、build_structured、Excel 共享）
        if self.session_data:
            self._cat_stats = compute_category_stats(self.session_data, self.daily_tickets)
        else:
            self._cat_stats = compute_ticket_category_stats(self.daily_tickets)

        # LLM 分类（可选）
        if LLM_ENABLED:
            try:
                from ai_classifier import batch_classify
                batch_classify(self.daily_tickets)
            except Exception as e:
                logger.warning(f"AI分类失败: {e}")

    # ==================== 内容摘要 ====================

    def _get_latest_reply(self, t):
        """获取工单最新回复内容"""
        for entry in reversed(t.get("_log", [])):
            for info in entry.get("info", []):
                c = info.get("content", "")
                if c and "回复" in info.get("title", info.get("titleLang", "")):
                    return c
        return ""

    def _summarize(self, t, max_len=0):
        """
        工单内容摘要。
        优先级：AI摘要 > 最新回复完整内容 > 标题+工单内容
        max_len=0 表示不截断（Web端）。
        """
        ai_summary = t.get("_ai_summary")
        if ai_summary:
            return ai_summary

        title = t.get("_title", "")
        content = t.get("_content", "")
        latest_reply = self._get_latest_reply(t)

        if latest_reply:
            summary = f"{title}；[最新回复]{latest_reply}" if title else latest_reply
        else:
            parts = []
            if title:
                parts.append(title)
            if content and content != title:
                parts.append(content)
            summary = "；".join(parts)

        if max_len and len(summary) > max_len:
            summary = summary[:max_len - 3] + "..."
        return summary or "（无内容）"

    # ==================== 趋势标记 ====================

    def _trend_mark(self, current, prev_key):
        """生成趋势对比标记"""
        prev = self.trend_data.get(prev_key)
        if prev is None:
            return ""
        diff = current - prev
        if diff > 0:
            return f" (+{diff})"
        elif diff < 0:
            return f" ({diff})"
        return " (持平)"

    # ==================== 格式化 ====================

    def _format_order(self, t, show_amount=False, show_resolved=False):
        """格式化单条工单"""
        summary = self._summarize(t)
        creator = t["_creator"] or "未知"
        ut = ts_to_str(t["_update_time"])
        handler = t.get("_handler", "") or ""

        line = f"工单内容：{summary}"
        if show_amount:
            amt = t["_recharge"]
            amt_str = f"{amt/10000:.1f}W" if amt >= 10000 else f"{amt:.0f}元"
            line += f"（{amt_str}）"

        line += f"\n    发起人：{creator}，更新时间：{ut}，受理人：{handler or '无'}"

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
        orders = self._pending_dev
        trend = self._trend_mark(len(orders), "prev_pending_dev_count")
        return f"{title}\n  共 {len(orders)} 条{trend}\n{self._format_list(orders)}"

    def section_3_unvisited(self):
        """三、客服未回访/未跟进问题总计"""
        title = "三、客服未回访/未跟进问题总计（近30天，受理方：工单系统）"
        orders = self._unvisited
        trend = self._trend_mark(len(orders), "prev_unvisited_count")
        return f"{title}\n  共 {len(orders)} 条{trend}\n{self._format_list(orders)}"

    def section_4_super_r(self):
        """四、超R反馈问题（10w+）"""
        title = "四、超R反馈问题（10w+）"
        orders = self._super_r
        if not orders:
            return f"{title}\n  无\n"
        trend = self._trend_mark(len(orders), "prev_super_r_count")
        return f"{title}\n  共 {len(orders)} 条{trend}\n{self._format_list(orders, show_amount=True)}"

    def section_5_pre_churn(self):
        """五、预流失报备"""
        title = "五、预流失报备"
        orders = self._pre_churn
        if not orders:
            return f"{title}\n  无\n"
        trend = self._trend_mark(len(orders), "prev_pre_churn_count")
        return f"{title}\n  共 {len(orders)} 条{trend}\n{self._format_list(orders)}"

    def section_6_pre_complaint(self):
        """六、预投诉报备"""
        title = "六、预投诉报备"
        orders = self._pre_complaint
        if not orders:
            return f"{title}\n  无\n"
        trend = self._trend_mark(len(orders), "prev_pre_complaint_count")
        return f"{title}\n  共 {len(orders)} 条{trend}\n{self._format_list(orders)}"

    def section_7_other(self):
        """七、其他VIP用户反馈"""
        title = "七、其他VIP用户反馈"
        orders = self._other
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
        categories, cat_alarm, cat_dev = self._cat_stats

        total_alarm = sum(cat_alarm.values())
        total_dev = sum(cat_dev.values())
        total_agent = total_alarm - total_dev

        lines = [title]
        if not self.session_data:
            lines.append("  [注] 统计基于工单数据（会话监控数据补充中）")
        lines.extend([
            "",
            f"  {'问题类型':<10} {'工单/会话数':>10} {'客服处理':>8} {'运营/研发介入':>12}",
            f"  {'─'*10} {'─'*10} {'─'*8} {'─'*12}",
        ])
        for cat in categories:
            a = cat_alarm.get(cat, 0)
            d = cat_dev.get(cat, 0)
            if a > 0:
                lines.append(f"  {cat:<10} {a:>10} {a-d:>8} {d:>12}")

        lines.extend([
            f"  {'─'*10} {'─'*10} {'─'*8} {'─'*12}",
            f"  {'问题总量':<10} {total_alarm:>10} {total_agent:>8} {total_dev:>12}",
            f"  总会话量：{self.total_sessions}",
        ])

        return "\n".join(lines) + "\n"

    # ==================== 结构化数据（供前端渲染） ====================

    def build_structured(self):
        """生成结构化日报数据（JSON 友好格式，供 Web 前端渲染）"""
        categories, cat_alarm, cat_dev = self._cat_stats

        other_grouped = defaultdict(list)
        for t in self._other:
            other_grouped[classify_ticket(t)].append(t)

        def _ticket_to_dict(t):
            return {
                "id": t.get("_id", ""),
                "title": t.get("_title", ""),
                "summary": self._summarize(t, max_len=0),
                "creator": t.get("_creator", ""),
                "create_time": ts_to_str(t.get("_create_time", 0)),
                "update_time": ts_to_str(t.get("_update_time", 0)),
                "recharge": t.get("_recharge", 0),
                "recharge_str": f"{t.get('_recharge',0)/10000:.1f}W" if t.get("_recharge", 0) >= 10000 else f"{t.get('_recharge',0):.0f}",
                "status": "已解决" if t.get("_status") in (STATUS_SOLVED, STATUS_CLOSED) else "未解决",
                "handler": t.get("_handler", ""),
                "category": classify_ticket(t),
                "issue_select": t.get("_issue_select", ""),
            }

        return {
            "date": self.report_date.strftime("%Y-%m-%d"),
            "date_cn": self.report_date.strftime("%Y年%m月%d日"),
            "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "has_session_data": bool(self.session_data),
            "dedup_removed": self.dedup_removed,
            "errors": self.errors,
            "stats": {
                "daily_count": len(self.daily_tickets),
                "pending_count": len(self.pending_tickets),
                "total_sessions": self.total_sessions,
                "super_r_count": len(self._super_r),
                "pre_churn_count": len(self._pre_churn),
                "pre_complaint_count": len(self._pre_complaint),
                "pending_dev_count": len(self._pending_dev),
                "unvisited_count": len(self._unvisited),
            },
            "trend": self.trend_data,
            "category_stats": [
                {
                    "name": cat,
                    "total": cat_alarm.get(cat, 0),
                    "agent": cat_alarm.get(cat, 0) - cat_dev.get(cat, 0),
                    "dev": cat_dev.get(cat, 0),
                }
                for cat in categories if cat_alarm.get(cat, 0) > 0
            ],
            "category_stats_total": {
                "total": sum(cat_alarm.values()),
                "agent": sum(cat_alarm.values()) - sum(cat_dev.values()),
                "dev": sum(cat_dev.values()),
            },
            "sections": {
                "super_r": [_ticket_to_dict(t) for t in self._super_r],
                "pending_dev": [_ticket_to_dict(t) for t in self._pending_dev],
                "unvisited": [_ticket_to_dict(t) for t in self._unvisited],
                "pre_churn": [_ticket_to_dict(t) for t in self._pre_churn],
                "pre_complaint": [_ticket_to_dict(t) for t in self._pre_complaint],
                "other": {
                    cat: [_ticket_to_dict(t) for t in group]
                    for cat, group in other_grouped.items()
                },
            },
        }

    # ==================== 生成完整日报 ====================

    def build(self):
        """生成完整日报文本"""
        date_str = self.report_date.strftime("%Y年%m月%d日")
        header = (
            f"{'='*50}\n"
            f"  VIP客服日报 - {date_str}\n"
            f"{'='*50}\n"
        )

        warnings = []
        if self.errors:
            warnings.append("  [!] 以下数据获取不完整：" + "、".join(self.errors))
        if self.dedup_removed > 0:
            warnings.append(f"  [i] 已自动去除 {self.dedup_removed} 条重复工单")
        warning_section = "\n".join(warnings) + "\n" if warnings else ""

        sections = [
            header,
            warning_section,
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
        """保存 Excel 日报 → excel_exporter.build_excel()"""
        from excel_exporter import build_excel
        return build_excel(self, filepath)

    def save_pdf(self, filepath=None):
        """保存 PDF 日报 → pdf_exporter.build_pdf()"""
        from pdf_exporter import build_pdf
        return build_pdf(self, filepath)
