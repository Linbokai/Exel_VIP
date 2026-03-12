"""
日报生成服务层
==============
封装「获取数据 → 生成日报」的完整流程，供 CLI 和 Web 入口共用。
"""
import logging
from datetime import datetime
from dataclasses import dataclass, field

from config import get_report_time_range, get_pending_time_range
from qiyu_client import QiyuClient
from report_builder import ReportBuilder

logger = logging.getLogger(__name__)


@dataclass
class ReportResult:
    """日报生成结果"""
    daily_tickets: list = field(default_factory=list)
    pending_tickets: list = field(default_factory=list)
    total_sessions: int = 0
    builder: ReportBuilder = None
    report_text: str = ""


def generate_report(report_date: datetime, client: QiyuClient = None) -> ReportResult:
    """
    执行完整的日报生成流程：
      1. 获取当日工单
      2. 获取待跟进工单
      3. 获取总会话量
      4. 构建日报

    :param report_date: 日报日期
    :param client:      QiyuClient 实例（可选，默认新建）
    :return: ReportResult
    """
    if client is None:
        client = QiyuClient()

    result = ReportResult()

    # 1. 获取当日工单
    daily_start, daily_end = get_report_time_range(report_date)
    logger.info("获取当日工单...")
    try:
        result.daily_tickets = client.fetch_daily_tickets(daily_start, daily_end)
        logger.info(f"当日工单: {len(result.daily_tickets)} 条")
    except Exception as e:
        logger.error(f"获取当日工单失败: {e}", exc_info=True)

    # 2. 获取待跟进工单
    pending_start, pending_end = get_pending_time_range()
    logger.info("获取待跟进工单...")
    try:
        result.pending_tickets = client.fetch_pending_tickets(pending_start, pending_end)
        logger.info(f"待跟进工单: {len(result.pending_tickets)} 条")
    except Exception as e:
        logger.error(f"获取待跟进工单失败: {e}", exc_info=True)

    # 3. 获取总会话量
    logger.info("获取总会话量...")
    try:
        result.total_sessions = client.get_total_session_count(daily_start, daily_end)
        logger.info(f"总会话量: {result.total_sessions}")
    except Exception as e:
        logger.error(f"获取总会话量失败: {e}", exc_info=True)

    # 4. 构建日报
    result.builder = ReportBuilder(
        daily_tickets=result.daily_tickets,
        pending_tickets=result.pending_tickets,
        total_sessions=result.total_sessions,
        report_date=report_date,
    )
    result.report_text = result.builder.build()

    return result
