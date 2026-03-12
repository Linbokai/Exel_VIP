"""
客服日报统计 - 配置文件
=======================
七鱼 OpenAPI 认证 + 业务参数。
"""
import os
from datetime import datetime, timedelta

# ======================== 七鱼 OpenAPI 认证 ========================
APP_KEY = os.getenv("QIYU_APP_KEY", "c162d65bae3fc1bdfe20d0c65ab5cb60")
APP_SECRET = os.getenv("QIYU_APP_SECRET", "A691F4F7EA4B47C6A42AB9987C3E2D6B")
BASE_URL = "https://qiyukf.com"

# 七鱼控制台账号（备用：Playwright 抓取会话监控时使用）
QIYU_DOMAIN = "smylxxkjyxgs"
QIYU_CONSOLE_URL = f"https://{QIYU_DOMAIN}.qiyukf.com"
QIYU_USERNAME = os.getenv("QIYU_USERNAME", "openclaw")
QIYU_PASSWORD = os.getenv("QIYU_PASSWORD", "beite999")

# ======================== API 端点 ========================
API = {
    # 工单
    "ticket_search":         "/openapi/v2/ticket/search",
    "ticket_detail":         "/openapi/v2/ticket/new/detail",
    "ticket_log":            "/openapi/v2/ticket/log",
    "ticket_filter_list":    "/openapi/v2/ticket/filter/list",
    "ticket_filter_count":   "/openapi/v2/ticket/filter/count",
    "ticket_list":           "/openapi/v2/ticket/list",
    "ticket_template_list":  "/openapi/v2/ticket/template/list",
    "ticket_template_fields":"/openapi/v2/ticket/template/fields",
    "category_list":         "/openapi/category/list",
    # 报表统计
    "stat_overview":         "/openapi/statistic/overview",
    "stat_staff_workload":   "/openapi/statistic/staffworklod",
    "stat_staff_quality":    "/openapi/statistic/staffquality",
    "stat_realtime_session": "/openapi/data/overview/session",
    # 会话导出（异步）
    "export_session":        "/openapi/export/session",
    "export_session_check":  "/openapi/export/session/check",
}

# ======================== 时间参数 ========================

def get_report_time_range(date=None):
    """
    日报时间窗口：昨日 18:00 ~ 当日 17:59
    返回 (start_ms, end_ms) 毫秒级时间戳
    """
    today = (date or datetime.now()).replace(hour=0, minute=0, second=0, microsecond=0)
    start = (today - timedelta(days=1)).replace(hour=18, minute=0, second=0)
    end = today.replace(hour=17, minute=59, second=59)
    return int(start.timestamp() * 1000), int(end.timestamp() * 1000)


def get_pending_time_range():
    """待跟进/未回访工单范围：近 30 天"""
    now = datetime.now()
    start = now - timedelta(days=30)
    return int(start.timestamp() * 1000), int(now.timestamp() * 1000)


def ts_to_str(ts_ms):
    """毫秒时间戳 → 可读字符串"""
    if not ts_ms:
        return "未知"
    try:
        return datetime.fromtimestamp(ts_ms / 1000).strftime("%Y-%m-%d %H:%M")
    except (OSError, ValueError):
        return "未知"


# ======================== 业务参数 ========================
WORKORDER_TEMPLATE_NAME = "VIP用户运营工单"
WORKORDER_TEMPLATE_ID = 6093346  # 通过API确认的模板ID
AGENT_GROUP = "倍特VIP工单组"
SUPER_R_THRESHOLD = 100000  # 超R阈值：10万元

# 受理方匹配关键词（API日志中的格式与后台显示不同，用关键词模糊匹配）
HANDLER_DEV_KEYWORD = "飞鱼"       # 运营/研发介入（日志中显示为"【一步】飞鱼"等）
HANDLER_SYSTEM_KEYWORD = "工单系统"  # 未回访（系统自动分配的）

# 工单状态码（从API实际返回值确认）
STATUS_PENDING = 5   # 受理中
STATUS_SOLVED = 10   # 已解决
STATUS_CLOSED = 15   # 已关闭

# ======================== 问题类型关键词 ========================
# 自定义字段名（从API实际返回值确认）
CF_ISSUE_TYPE = "问题类型"       # 自定义字段：问题类型（充值问题/登录问题/...）
CF_RECHARGE = "角色累充"         # 自定义字段：角色累充金额
CF_ISSUE_SELECT = "问题选择"     # 自定义字段：问题选择（预流失/我要投诉/...）

ISSUE_KEYWORDS = {
    "充值问题": ["充值", "充钱", "付款", "支付", "扣费", "到账", "钻石", "月卡", "代充"],
    "登录问题": ["登录", "登陆", "进不去", "闪退", "无法进入", "账号", "密码", "验证码"],
    "玩法咨询": ["玩法", "攻略", "怎么玩", "规则", "活动", "副本", "任务", "装备"],
    "投诉问题": ["投诉", "举报", "12345", "消协", "退款", "工商", "维权"],
    "bug问题":  ["bug", "BUG", "故障", "异常", "卡顿", "报错", "卡死", "崩溃"],
}

# ======================== 输出 ========================
OUTPUT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "output")
os.makedirs(OUTPUT_DIR, exist_ok=True)
