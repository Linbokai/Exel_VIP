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
AGENT_GROUP = "倍特VIP"  # 前缀匹配，覆盖所有VIP组（工单组、外包组、VIP二组等）
SUPER_R_THRESHOLD = 100000  # 超R阈值：10万元

# 受理方匹配关键词（API日志中的格式与后台显示不同，用关键词模糊匹配）
HANDLER_DEV_KEYWORD = "飞鱼"       # 运营/研发介入（日志中显示为"【一步】飞鱼"等）
HANDLER_SYSTEM_KEYWORD = "工单系统"  # 未回访（系统自动分配的）

# 工单状态码（从API实际返回值确认）
STATUS_PENDING = 5   # 受理中
STATUS_SOLVED = 10   # 已解决
STATUS_CLOSED = 15   # 已关闭

# ======================== 问题类型关键词（扩充版） ========================
# 自定义字段名（从API实际返回值确认）
CF_ISSUE_TYPE = "问题类型"       # 自定义字段：问题类型（充值问题/登录问题/...）
CF_RECHARGE = "角色累充"         # 自定义字段：角色累充金额
CF_ISSUE_SELECT = "问题选择"     # 自定义字段：问题选择（预流失/我要投诉/...）

ISSUE_KEYWORDS = {
    "充值问题": ["充值", "充钱", "付款", "支付", "扣费", "到账", "钻石", "月卡", "代充"],
    "登录问题": ["登录", "登陆", "进不去", "闪退", "无法进入", "账号", "密码", "验证码"],
    "举报违规": ["外挂", "开挂", "辱骂", "刷屏", "违规昵称", "言论违规", "威胁",
                "伤害异常", "战力与伤害不符", "违规发言"],
    "投诉问题": ["投诉", "12345", "消协", "退款", "工商", "维权"],
    "bug问题":  ["bug", "BUG", "故障", "异常", "卡顿", "报错", "卡死", "崩溃"],
    "误操作问题": ["误操作", "误点", "误触", "误购", "误兑换", "误升级", "回退"],
    "玩法咨询/游戏建议": ["玩法", "攻略", "怎么玩", "规则", "活动", "副本", "任务", "装备",
                       "建议", "希望", "优化", "改进", "体验差", "不合理"],
}

# 运营/研发介入判定：工单日志中转交记录包含此关键词
DEV_TRANSFER_KEYWORD = "飞鱼科技"

# ======================== 并发与速率控制 ========================
API_MAX_WORKERS = 8           # 并发请求线程数
API_RATE_LIMIT = 10           # 每秒最大请求数
API_RATE_BURST = 15           # 突发最大请求数

# ======================== 缓存 ========================
CACHE_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "cache")
CACHE_DB_PATH = os.path.join(CACHE_DIR, "tickets.db")
CACHE_TTL_SECONDS = 3600      # 缓存有效期（秒）

# ======================== LLM 智能分类/摘要 ========================
LLM_ENABLED = os.getenv("LLM_ENABLED", "false").lower() == "true"
LLM_API_URL = os.getenv("LLM_API_URL", "https://api.openai.com/v1/chat/completions")
LLM_API_KEY = os.getenv("LLM_API_KEY", "")
LLM_MODEL = os.getenv("LLM_MODEL", "gpt-4o-mini")
LLM_TIMEOUT = 15  # 秒

# ======================== 告警 ========================
ALERT_ENABLED = os.getenv("ALERT_ENABLED", "false").lower() == "true"
# 企微机器人 webhook
ALERT_WECOM_WEBHOOK = os.getenv("ALERT_WECOM_WEBHOOK", "")
# 钉钉机器人 webhook
ALERT_DINGTALK_WEBHOOK = os.getenv("ALERT_DINGTALK_WEBHOOK", "")
# 告警阈值
ALERT_SUPER_R_THRESHOLD = 5     # 超R工单数 >= N 触发告警
ALERT_COMPLAINT_THRESHOLD = 3   # 预投诉数 >= N 触发告警
ALERT_PENDING_THRESHOLD = 10    # 待跟进工单数 >= N 触发告警

# ======================== 定时调度 ========================
SCHEDULER_ENABLED = os.getenv("SCHEDULER_ENABLED", "false").lower() == "true"
SCHEDULER_CRON_HOUR = int(os.getenv("SCHEDULER_CRON_HOUR", "18"))
SCHEDULER_CRON_MINUTE = int(os.getenv("SCHEDULER_CRON_MINUTE", "30"))

# ======================== Web 认证 ========================
WEB_AUTH_ENABLED = os.getenv("WEB_AUTH_ENABLED", "false").lower() == "true"
WEB_AUTH_USERNAME = os.getenv("WEB_AUTH_USERNAME", "admin")
WEB_AUTH_PASSWORD = os.getenv("WEB_AUTH_PASSWORD", "vip2026")

# ======================== 输出 ========================
OUTPUT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "output")
os.makedirs(OUTPUT_DIR, exist_ok=True)
os.makedirs(CACHE_DIR, exist_ok=True)
