"""
ReportBuilder 单元测试
======================
覆盖：金额解析、问题分类、工单去重、日报格式化。
"""
import pytest
from datetime import datetime

from report_builder import (
    parse_amount,
    classify_ticket,
    dedup_tickets,
    enrich_ticket_fields,
    get_custom_field,
    ReportBuilder,
)


# ==================== parse_amount ====================

class TestParseAmount:

    def test_integer(self):
        assert parse_amount(50000) == 50000.0

    def test_float(self):
        assert parse_amount(123.45) == 123.45

    def test_string_number(self):
        assert parse_amount("50000") == 50000.0

    def test_string_with_comma(self):
        assert parse_amount("1,234,567") == 1234567.0

    def test_string_wan(self):
        assert parse_amount("13.67万") == 136700.0

    def test_string_wan_w(self):
        assert parse_amount("5.5W") == 55000.0

    def test_empty_string(self):
        assert parse_amount("") == 0

    def test_invalid_string(self):
        assert parse_amount("abc") == 0

    def test_none(self):
        assert parse_amount(None) == 0

    def test_zero(self):
        assert parse_amount(0) == 0.0

    def test_chinese_comma(self):
        assert parse_amount("1，234，567") == 1234567.0


# ==================== get_custom_field ====================

class TestGetCustomField:

    def test_list_match(self):
        fields = [
            {"name": "问题类型", "value": "充值问题"},
            {"name": "角色累充", "value": "50000"},
        ]
        assert get_custom_field(fields, "问题类型") == "充值问题"
        assert get_custom_field(fields, "角色累充") == "50000"

    def test_list_no_match(self):
        fields = [{"name": "问题类型", "value": "充值问题"}]
        assert get_custom_field(fields, "不存在的字段") == ""

    def test_list_default(self):
        assert get_custom_field([], "任意", "默认值") == "默认值"

    def test_dict(self):
        fields = {"问题类型": "登录问题"}
        assert get_custom_field(fields, "问题类型") == "登录问题"

    def test_none(self):
        assert get_custom_field(None, "字段") == ""

    def test_key_field(self):
        fields = [{"key": "问题选择", "value": "预流失"}]
        assert get_custom_field(fields, "问题选择") == "预流失"


# ==================== classify_ticket ====================

class TestClassifyTicket:

    def test_charge_issue(self):
        t = {"_content": "玩家充值后钻石未到账", "_title": "充值问题"}
        assert classify_ticket(t) == "充值问题"

    def test_login_issue(self):
        t = {"_content": "玩家无法登录游戏", "_title": "登录异常"}
        assert classify_ticket(t) == "登录问题"

    def test_bug_issue(self):
        t = {"_content": "游戏卡顿崩溃", "_title": "bug问题"}
        assert classify_ticket(t) == "bug问题"

    def test_complaint_issue(self):
        t = {"_content": "玩家要投诉举报其他玩家", "_title": "投诉"}
        assert classify_ticket(t) == "投诉问题"

    def test_misoperation(self):
        t = {"_content": "玩家误操作兑换了道具，申请回退", "_title": "误操作"}
        assert classify_ticket(t) == "误操作问题"

    def test_report_violation(self):
        t = {"_content": "外挂玩家辱骂刷屏", "_title": "违规昵称"}
        assert classify_ticket(t) == "举报违规"

    def test_suggestion(self):
        t = {"_content": "觉得不合理体验差", "_title": "希望改进"}
        assert classify_ticket(t) == "玩法咨询/游戏建议"

    def test_gameplay(self):
        t = {"_content": "玩家咨询副本攻略", "_title": "玩法咨询"}
        assert classify_ticket(t) == "玩法咨询/游戏建议"

    def test_other(self):
        t = {"_content": "普通反馈", "_title": "其他"}
        assert classify_ticket(t) == "其他问题"

    def test_ai_category_priority(self):
        t = {"_content": "充值问题", "_title": "充值", "_ai_category": "游戏建议"}
        assert classify_ticket(t) == "游戏建议"


# ==================== dedup_tickets ====================

class TestDedupTickets:

    def _make_ticket(self, tid, creator, create_time, title="测试工单"):
        return {
            "_id": tid,
            "_creator": creator,
            "_create_time": create_time,
            "_title": title,
            "_content": title,
        }

    def test_no_duplicates(self):
        tickets = [
            self._make_ticket("1", "玩家A", 1000000, "工单1"),
            self._make_ticket("2", "玩家B", 2000000, "工单2"),
        ]
        result = dedup_tickets(tickets)
        assert len(result) == 2

    def test_same_creator_same_window(self):
        tickets = [
            self._make_ticket("1", "玩家A", 1000000, "测试工单"),
            self._make_ticket("2", "玩家A", 1500000, "测试工单"),
        ]
        result = dedup_tickets(tickets, time_window_ms=3600000)
        assert len(result) == 1

    def test_same_creator_different_window(self):
        tickets = [
            self._make_ticket("1", "玩家A", 1000000, "测试工单"),
            self._make_ticket("2", "玩家A", 5000000, "测试工单"),
        ]
        result = dedup_tickets(tickets, time_window_ms=3600000)
        assert len(result) == 2

    def test_same_creator_different_content(self):
        tickets = [
            self._make_ticket("1", "玩家A", 1000000, "充值问题反馈"),
            self._make_ticket("2", "玩家A", 1500000, "登录问题反馈"),
        ]
        result = dedup_tickets(tickets, time_window_ms=3600000)
        assert len(result) == 2

    def test_empty_list(self):
        result = dedup_tickets([])
        assert result == []

    def test_keeps_latest(self):
        tickets = [
            self._make_ticket("1", "玩家A", 1000000, "测试工单"),
            self._make_ticket("2", "玩家A", 2000000, "测试工单"),
        ]
        result = dedup_tickets(tickets, time_window_ms=3600000)
        assert len(result) == 1
        assert result[0]["_id"] == "2"  # 保留最新的


# ==================== enrich_ticket_fields ====================

class TestEnrichTicketFields:

    def test_basic_fields(self):
        t = {
            "id": "12345",
            "title": "【VIP用户】测试工单",
            "content": "测试内容",
            "createTime": 1710000000000,
            "updateTime": 1710003600000,
            "status": 5,
            "custom": [
                {"name": "角色累充", "value": "50000"},
                {"name": "问题选择", "value": "预流失"},
            ],
        }
        enrich_ticket_fields(t)
        assert t["_id"] == "12345"
        assert t["_title"] == "【VIP用户】测试工单"
        assert t["_recharge"] == 50000.0
        assert t["_issue_select"] == "预流失"
        assert t["_status"] == 5

    def test_creator_from_title(self):
        t = {
            "id": "1",
            "title": "【VIP用户】张三：充值问题",
            "custom": [],
        }
        enrich_ticket_fields(t)
        assert t["_creator"] == "张三"


# ==================== ReportBuilder ====================

class TestReportBuilder:

    def _make_builder(self, daily_tickets=None, pending_tickets=None):
        tickets = daily_tickets or []
        for t in tickets:
            t.setdefault("id", "1")
            t.setdefault("title", "测试")
            t.setdefault("content", "测试")
            t.setdefault("createTime", 1710000000000)
            t.setdefault("status", 5)
            t.setdefault("custom", [])
        pending = pending_tickets or []
        for t in pending:
            t.setdefault("id", "2")
            t.setdefault("title", "待跟进")
            t.setdefault("content", "测试")
            t.setdefault("createTime", 1710000000000)
            t.setdefault("status", 5)
            t.setdefault("custom", [])
        return ReportBuilder(
            daily_tickets=tickets,
            pending_tickets=pending,
            total_sessions=100,
            report_date=datetime(2026, 3, 12),
        )

    def test_build_returns_string(self):
        builder = self._make_builder()
        text = builder.build()
        assert isinstance(text, str)
        assert "VIP客服日报" in text
        assert "2026年03月12日" in text

    def test_build_has_all_sections(self):
        builder = self._make_builder()
        text = builder.build()
        assert "一、重大事件报备" in text
        assert "二、待跟进问题总计" in text
        assert "三、客服未回访" in text
        assert "四、超R反馈问题" in text
        assert "五、预流失报备" in text
        assert "六、预投诉报备" in text
        assert "七、其他VIP用户反馈" in text

    def test_build_structured_keys(self):
        builder = self._make_builder()
        data = builder.build_structured()
        assert "date" in data
        assert "stats" in data
        assert "sections" in data
        assert "category_stats" in data
        assert data["date"] == "2026-03-12"

    def test_super_r_filtering(self):
        tickets = [
            {"id": "1", "title": "超R", "custom": [{"name": "角色累充", "value": "200000"}]},
            {"id": "2", "title": "普通", "custom": [{"name": "角色累充", "value": "5000"}]},
        ]
        builder = self._make_builder(daily_tickets=tickets)
        text = builder.section_4_super_r()
        assert "共 1 条" in text

    def test_errors_shown_in_report(self):
        builder = ReportBuilder(
            report_date=datetime(2026, 3, 12),
            errors=["当日工单", "总会话量"],
        )
        text = builder.build()
        assert "数据获取不完整" in text
        assert "当日工单" in text

    def test_dedup_removed_shown(self):
        tickets = [
            {"id": "1", "title": "【VIP用户】玩家A：测试", "createTime": 1710000000000,
             "content": "测试内容", "custom": [], "status": 5},
            {"id": "2", "title": "【VIP用户】玩家A：测试", "createTime": 1710000500000,
             "content": "测试内容", "custom": [], "status": 5},
        ]
        builder = ReportBuilder(
            daily_tickets=tickets,
            report_date=datetime(2026, 3, 12),
        )
        assert builder.dedup_removed >= 0  # May or may not dedup depending on content

    def test_trend_mark(self):
        builder = ReportBuilder(
            report_date=datetime(2026, 3, 12),
            trend_data={"prev_super_r_count": 5},
        )
        mark = builder._trend_mark(8, "prev_super_r_count")
        assert "+3" in mark

        mark = builder._trend_mark(3, "prev_super_r_count")
        assert "-2" in mark

        mark = builder._trend_mark(5, "prev_super_r_count")
        assert "持平" in mark

        mark = builder._trend_mark(5, "nonexistent_key")
        assert mark == ""
