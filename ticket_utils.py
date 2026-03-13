"""
工单数据处理工具
================
工单字段提取、金额解析、分类、去重、统计计算。
所有函数均为无状态纯函数，可独立测试。
"""
import re
import logging
from collections import Counter, defaultdict

from config import (
    ISSUE_KEYWORDS,
    CF_ISSUE_TYPE, CF_RECHARGE, CF_ISSUE_SELECT,
)

logger = logging.getLogger(__name__)


# ==================== 字段提取 ====================

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
    t.setdefault("_has_dev_transfer", False)
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


# ==================== 分类 ====================

def classify_ticket(t):
    """根据关键词（或AI分类结果）对工单进行问题分类"""
    # 优先使用AI分类结果
    ai_cat = t.get("_ai_category")
    if ai_cat:
        return ai_cat

    text = (t.get("_content") or "") + (t.get("_title") or "")
    for category, keywords in ISSUE_KEYWORDS.items():
        if any(kw in text for kw in keywords):
            return category
    return "其他问题"


# ==================== 去重 ====================

def dedup_tickets(tickets, time_window_ms=3600000):
    """
    工单去重：同一用户在时间窗口内的重复工单只保留最新一条。
    :param tickets: 工单列表
    :param time_window_ms: 时间窗口（毫秒），默认1小时
    :return: 去重后的工单列表
    """
    if not tickets:
        return tickets

    # 按创建时间倒序
    sorted_tickets = sorted(tickets, key=lambda t: t.get("_create_time", 0), reverse=True)

    seen = {}  # {dedup_key: create_time}
    result = []
    dup_count = 0

    for t in sorted_tickets:
        creator = t.get("_creator", "")
        ct = t.get("_create_time", 0)
        content_key = re.sub(r'\s+', '', t.get("_title", ""))[:30]
        dedup_key = f"{creator}|{content_key}"

        if dedup_key in seen:
            prev_time = seen[dedup_key]
            if abs(ct - prev_time) < time_window_ms:
                dup_count += 1
                logger.debug(f"去重: #{t.get('_id')} 与已有工单重复 (creator={creator})")
                continue

        seen[dedup_key] = ct
        result.append(t)

    if dup_count > 0:
        logger.info(f"工单去重: {len(tickets)} → {len(result)} 条 (去除 {dup_count} 条重复)")

    return result


# ==================== 统计计算 ====================

def compute_category_stats(session_data, daily_tickets):
    """
    计算问题类型统计数据（从会话监控数据中提取）。
    运营/研发介入定义：日报当天有提交工单，且工单日志中存在转交企业微信-飞鱼科技的记录。
    注意：运营介入统计始终基于工单数据（而非会话），确保匹配准确。
    返回 (categories, cat_alarm, cat_dev) 三元组。
    """
    categories = list(ISSUE_KEYWORDS.keys()) + ["其他问题"]

    cat_alarm = Counter()
    for sess in session_data:
        content = sess.get("content", "") or sess.get("message", "") or ""
        matched = False
        for category, keywords in ISSUE_KEYWORDS.items():
            if any(kw in content for kw in keywords):
                cat_alarm[category] += 1
                matched = True
                break
        if not matched:
            cat_alarm["其他问题"] += 1

    # 运营/研发介入：基于工单数据统计（工单有转交飞鱼科技记录）
    cat_dev = Counter()
    for t in daily_tickets:
        if t.get("_has_dev_transfer", False):
            cat = classify_ticket(t)
            cat_dev[cat] += 1

    return categories, cat_alarm, cat_dev


def compute_ticket_category_stats(daily_tickets):
    """
    基于工单数据计算问题类型统计（当会话数据不可用时的备用方案）。
    运营/研发介入定义：工单日志中存在转交企业微信-飞鱼科技的记录。
    """
    categories = list(ISSUE_KEYWORDS.keys()) + ["其他问题"]
    cat_count = Counter()
    cat_dev = Counter()

    for t in daily_tickets:
        cat = classify_ticket(t)
        cat_count[cat] += 1
        if t.get("_has_dev_transfer", False):
            cat_dev[cat] += 1

    return categories, cat_count, cat_dev
