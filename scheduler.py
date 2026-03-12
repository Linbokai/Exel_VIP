"""
定时调度模块
============
使用 APScheduler 实现每日自动生成日报并推送通知。

启动方式：
  python scheduler.py                     # 使用默认配置
  SCHEDULER_CRON_HOUR=18 python scheduler.py  # 自定义时间
"""
import sys
sys.stdout.reconfigure(encoding='utf-8')

import logging
from datetime import datetime

from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger

from config import SCHEDULER_CRON_HOUR, SCHEDULER_CRON_MINUTE
from service import generate_report
from alert import send_daily_report_notification

logger = logging.getLogger("scheduler")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%H:%M:%S",
)


def scheduled_generate():
    """定时任务：生成当日日报并推送"""
    report_date = datetime.now()
    date_str = report_date.strftime("%Y-%m-%d")

    logger.info(f"=== 定时生成日报: {date_str} ===")

    try:
        result = generate_report(report_date)

        # 保存文件
        txt_path = result.builder.save_text()
        logger.info(f"文本日报: {txt_path}")

        try:
            xlsx_path = result.builder.save_excel()
            logger.info(f"Excel日报: {xlsx_path}")
        except Exception as e:
            logger.warning(f"Excel生成失败: {e}")

        try:
            pdf_path = result.builder.save_pdf()
            logger.info(f"PDF日报: {pdf_path}")
        except Exception as e:
            logger.warning(f"PDF生成失败: {e}")

        # 推送通知
        send_daily_report_notification(result.report_text, date_str)

        logger.info(f"=== 日报生成完成: {date_str} ===")

    except Exception as e:
        logger.error(f"定时生成日报失败: {e}", exc_info=True)


def start_scheduler():
    """启动定时调度器"""
    scheduler = BlockingScheduler()

    trigger = CronTrigger(
        hour=SCHEDULER_CRON_HOUR,
        minute=SCHEDULER_CRON_MINUTE,
    )

    scheduler.add_job(
        scheduled_generate,
        trigger=trigger,
        id="daily_report",
        name="每日VIP客服日报",
        misfire_grace_time=3600,
    )

    print(f"\n{'='*45}")
    print(f"  VIP客服日报 - 定时调度")
    print(f"  每日 {SCHEDULER_CRON_HOUR:02d}:{SCHEDULER_CRON_MINUTE:02d} 自动生成日报")
    print(f"{'='*45}\n")

    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        logger.info("调度器已停止")


if __name__ == "__main__":
    start_scheduler()
