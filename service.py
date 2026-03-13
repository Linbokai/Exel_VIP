"""
日报生成服务层
==============
封装「获取数据 → 生成日报」的完整流程，供 CLI 和 Web 入口共用。

增强功能：
  - 本地缓存（避免重复 API 调用）
  - 错误追踪（标记数据不完整）
  - 趋势对比（与前一天数据对比）
  - 会话数据获取
"""
import logging
from datetime import datetime, timedelta
from dataclasses import dataclass, field

from config import (
    get_report_time_range, get_pending_time_range,
    SUPER_R_THRESHOLD, CF_RECHARGE, CF_ISSUE_SELECT,
)
from qiyu_client import QiyuClient
from report_builder import ReportBuilder
from ticket_utils import get_custom_field, parse_amount
from cache import TicketCache
from alert import check_and_alert

logger = logging.getLogger(__name__)


@dataclass
class ReportResult:
    """日报生成结果"""
    daily_tickets: list = field(default_factory=list)
    pending_tickets: list = field(default_factory=list)
    total_sessions: int = 0
    session_data: list = field(default_factory=list)
    builder: ReportBuilder = None
    report_text: str = ""
    structured: dict = field(default_factory=dict)
    trend_data: dict = field(default_factory=dict)
    errors: list = field(default_factory=list)


def _fetch_trend_data(client, report_date):
    """
    获取前一天的统计数据，用于趋势对比。
    """
    trend = {}
    try:
        prev_date = report_date - timedelta(days=1)
        prev_start, prev_end = get_report_time_range(prev_date)

        # 前一天的工单（只搜索，不做 enrich）
        prev_tickets = client.search_all_tickets(start=prev_start, end=prev_end)
        tmpl_id = client.get_vip_template_id()
        if tmpl_id:
            prev_tickets = [t for t in prev_tickets if t.get("templateId") == tmpl_id]

        trend["prev_daily_count"] = len(prev_tickets)

        super_r = 0
        pre_churn = 0
        pre_complaint = 0
        for t in prev_tickets:
            custom = t.get("custom", [])
            recharge = parse_amount(get_custom_field(custom, CF_RECHARGE, "0"))
            issue_select = get_custom_field(custom, CF_ISSUE_SELECT)
            if recharge >= SUPER_R_THRESHOLD:
                super_r += 1
            if issue_select == "预流失":
                pre_churn += 1
            if issue_select == "我要投诉":
                pre_complaint += 1

        trend["prev_super_r_count"] = super_r
        trend["prev_pre_churn_count"] = pre_churn
        trend["prev_pre_complaint_count"] = pre_complaint

        # 注意：不再为趋势数据调用 get_total_session_count，
        # 因为 staffworklod API 频率限制极严，一次报告生成周期内多次调用必定失败。
        # 趋势中的 prev_total_sessions 暂不提供。

        logger.info(f"趋势数据: {trend}")

    except Exception as e:
        logger.warning(f"获取趋势数据失败: {e}")

    return trend


def generate_report(report_date: datetime, client: QiyuClient = None,
                    on_progress=None, use_cache=True,
                    fetch_trends=True, fetch_sessions=True) -> ReportResult:
    """
    执行完整的日报生成流程：
      1. 获取总会话量（优先调用，避免被后续大量API消耗频率配额）
      2. 获取当日工单
      3. 获取待跟进工单
      4. 获取会话明细数据
      5. 获取趋势对比数据
      6. 构建日报 + 告警检查
    """
    if client is None:
        client = QiyuClient()

    cache = TicketCache() if use_cache else None
    total_steps = 6
    step_counter = [0]

    def _progress(desc):
        step_counter[0] += 1
        logger.info(desc)
        if on_progress:
            on_progress(step_counter[0], total_steps, desc)

    result = ReportResult()

    # 尝试读取日报缓存（非强制刷新时）
    date_key = report_date.strftime("%Y%m%d")
    if cache and use_cache:
        cached_report = cache.get_report(f"report_{date_key}")
        if cached_report:
            logger.info(f"命中日报缓存: report_{date_key}")
            result.report_text = cached_report.get("text", "")
            result.structured = cached_report
            _progress("从缓存加载日报（跳过 API 调用）")
            _progress("缓存命中")
            _progress("缓存命中")
            _progress("缓存命中")
            _progress("构建完成")
            return result

    # 1. 获取总会话量（最先调用，避免被后续大量工单API消耗频率配额）
    # 会话统计使用完整自然日（00:00~23:59），与七鱼坐席工作量报表对齐
    session_day = report_date.replace(hour=0, minute=0, second=0, microsecond=0)
    session_start = int(session_day.timestamp() * 1000)
    session_end = int(session_day.replace(hour=23, minute=59, second=59).timestamp() * 1000)
    _progress("获取会话统计...")
    try:
        result.total_sessions = client.get_total_session_count(session_start, session_end)
        logger.info(f"总会话量: {result.total_sessions}")
    except Exception as e:
        logger.error(f"获取总会话量失败: {e}", exc_info=True)
        result.errors.append("总会话量")

    # 2. 获取当日工单
    daily_start, daily_end = get_report_time_range(report_date)
    _progress("获取当日工单...")
    try:
        result.daily_tickets = client.fetch_daily_tickets(daily_start, daily_end)
        logger.info(f"当日工单: {len(result.daily_tickets)} 条")
        if cache and result.daily_tickets:
            cache.set_tickets_batch(result.daily_tickets)
    except Exception as e:
        logger.error(f"获取当日工单失败: {e}", exc_info=True)
        result.errors.append("当日工单")

    # 3. 获取待跟进工单
    pending_start, pending_end = get_pending_time_range()
    _progress("获取待跟进工单...")
    try:
        result.pending_tickets = client.fetch_pending_tickets(pending_start, pending_end)
        logger.info(f"待跟进工单: {len(result.pending_tickets)} 条")
        if cache and result.pending_tickets:
            cache.set_tickets_batch(result.pending_tickets)
    except Exception as e:
        logger.error(f"获取待跟进工单失败: {e}", exc_info=True)
        result.errors.append("待跟进工单")

    # 4. 获取会话明细数据
    if fetch_sessions:
        _progress("获取会话明细数据...")
        try:
            result.session_data = client.export_session_data(session_start, session_end)
            logger.info(f"会话数据: {len(result.session_data)} 条")
        except Exception as e:
            logger.warning(f"获取会话数据失败（非关键）: {e}")

    # 5. 获取趋势数据
    _progress("获取趋势对比数据...")
    if fetch_trends:
        result.trend_data = _fetch_trend_data(client, report_date)

    # 6. 构建日报
    _progress("构建日报...")
    result.builder = ReportBuilder(
        daily_tickets=result.daily_tickets,
        pending_tickets=result.pending_tickets,
        total_sessions=result.total_sessions,
        report_date=report_date,
        session_data=result.session_data,
        trend_data=result.trend_data,
        errors=result.errors,
    )
    try:
        result.report_text = result.builder.build()
        result.structured = result.builder.build_structured()
    except Exception as e:
        logger.error(f"日报构建失败: {e}", exc_info=True)
        result.report_text = f"[日报构建失败: {e}]"
        result.structured = {}
        result.errors.append("日报构建")

    # 缓存日报
    if cache:
        date_key = report_date.strftime("%Y%m%d")
        cache.set_report(f"report_{date_key}", {
            "text": result.report_text,
            "stats": result.structured.get("stats", {}),
        })

    # 告警检查
    try:
        check_and_alert(result, report_date.strftime("%Y-%m-%d"))
    except Exception as e:
        logger.warning(f"告警检查失败: {e}")

    return result
