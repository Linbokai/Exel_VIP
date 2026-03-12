"""
告警通知模块
============
当日报数据超过阈值时，通过企微/钉钉机器人推送告警。
"""
import json
import logging
import requests

from config import (
    ALERT_ENABLED,
    ALERT_WECOM_WEBHOOK,
    ALERT_DINGTALK_WEBHOOK,
    ALERT_SUPER_R_THRESHOLD,
    ALERT_COMPLAINT_THRESHOLD,
    ALERT_PENDING_THRESHOLD,
    SUPER_R_THRESHOLD,
)

logger = logging.getLogger(__name__)


def check_and_alert(report_result, report_date_str=""):
    """
    检查日报数据是否触发告警阈值，触发则推送通知。
    :param report_result: ReportResult 对象
    :param report_date_str: 日报日期字符串
    """
    if not ALERT_ENABLED:
        return

    alerts = []

    # 超R工单数
    super_r_count = sum(
        1 for t in report_result.daily_tickets
        if t.get("_recharge", 0) >= SUPER_R_THRESHOLD
    )
    if super_r_count >= ALERT_SUPER_R_THRESHOLD:
        alerts.append(f"超R工单数达 {super_r_count} 条（阈值 {ALERT_SUPER_R_THRESHOLD}）")

    # 预投诉数
    complaint_count = sum(
        1 for t in report_result.daily_tickets
        if t.get("_issue_select") == "我要投诉"
    )
    if complaint_count >= ALERT_COMPLAINT_THRESHOLD:
        alerts.append(f"预投诉工单数达 {complaint_count} 条（阈值 {ALERT_COMPLAINT_THRESHOLD}）")

    # 待跟进工单数
    pending_count = len(report_result.pending_tickets)
    if pending_count >= ALERT_PENDING_THRESHOLD:
        alerts.append(f"待跟进工单数达 {pending_count} 条（阈值 {ALERT_PENDING_THRESHOLD}）")

    if not alerts:
        logger.info("日报数据未触发告警阈值")
        return

    title = f"VIP客服日报告警 - {report_date_str}"
    content = "\n".join(f"- {a}" for a in alerts)
    message = f"**{title}**\n\n{content}\n\n请及时关注处理。"

    logger.warning(f"触发告警: {alerts}")

    if ALERT_WECOM_WEBHOOK:
        _send_wecom(message)
    if ALERT_DINGTALK_WEBHOOK:
        _send_dingtalk(title, message)


def _send_wecom(content):
    """发送企业微信机器人消息"""
    try:
        payload = {
            "msgtype": "markdown",
            "markdown": {"content": content},
        }
        resp = requests.post(
            ALERT_WECOM_WEBHOOK,
            json=payload, timeout=10,
        )
        resp.raise_for_status()
        data = resp.json()
        if data.get("errcode") != 0:
            logger.error(f"企微告警发送失败: {data}")
        else:
            logger.info("企微告警发送成功")
    except Exception as e:
        logger.error(f"企微告警发送异常: {e}")


def _send_dingtalk(title, content):
    """发送钉钉机器人消息"""
    try:
        payload = {
            "msgtype": "markdown",
            "markdown": {
                "title": title,
                "text": content,
            },
        }
        resp = requests.post(
            ALERT_DINGTALK_WEBHOOK,
            json=payload, timeout=10,
        )
        resp.raise_for_status()
        data = resp.json()
        if data.get("errcode") != 0:
            logger.error(f"钉钉告警发送失败: {data}")
        else:
            logger.info("钉钉告警发送成功")
    except Exception as e:
        logger.error(f"钉钉告警发送异常: {e}")


def send_daily_report_notification(report_text, report_date_str=""):
    """推送日报生成完成通知（定时调度后使用）"""
    if not ALERT_ENABLED:
        return

    # 截取摘要（前500字）
    summary = report_text[:500]
    if len(report_text) > 500:
        summary += "\n..."

    title = f"VIP客服日报已生成 - {report_date_str}"
    message = f"**{title}**\n\n```\n{summary}\n```\n\n日报已自动生成，请登录系统查看完整内容。"

    if ALERT_WECOM_WEBHOOK:
        _send_wecom(message)
    if ALERT_DINGTALK_WEBHOOK:
        _send_dingtalk(title, message)
