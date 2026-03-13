"""
七鱼 OpenAPI 客户端
===================
通过 appKey/appSecret 签名认证调用七鱼开放接口。
认证方式：查询参数 appKey + time + checksum, 其中
  checksum = SHA1(appSecret + MD5(requestBody) + time)

增强功能：
  - 令牌桶速率控制
  - 线程池并发请求
  - 会话数据导出
"""
import hashlib
import time
import json
import logging
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed

from config import (
    APP_KEY, APP_SECRET, BASE_URL, API,
    WORKORDER_TEMPLATE_NAME, WORKORDER_TEMPLATE_ID,
    STATUS_PENDING, API_MAX_WORKERS, API_RATE_LIMIT, API_RATE_BURST,
    DEV_TRANSFER_KEYWORD, AGENT_GROUP,
)
from rate_limiter import TokenBucketRateLimiter

logger = logging.getLogger(__name__)


class QiyuClient:
    """七鱼 OpenAPI 客户端"""

    def __init__(self, app_key=None, app_secret=None):
        self.app_key = app_key or APP_KEY
        self.app_secret = app_secret or APP_SECRET
        self.session = requests.Session()
        self._template_id = None  # VIP工单模板ID（懒加载）
        self._rate_limiter = TokenBucketRateLimiter(
            rate=API_RATE_LIMIT, burst=API_RATE_BURST,
        )

    # ==================== 签名与请求 ====================

    def _checksum(self, body_bytes, timestamp):
        """计算签名: SHA1(appSecret + MD5(body) + time)"""
        md5 = hashlib.md5(body_bytes).hexdigest()
        raw = self.app_secret + md5 + str(timestamp)
        return hashlib.sha1(raw.encode("utf-8")).hexdigest()

    def _request(self, path, body, retries=2):
        """
        发送已签名的 POST 请求（带速率控制）。
        七鱼API的 message 字段可能是 JSON 字符串，自动解析。
        """
        # 速率控制
        self._rate_limiter.acquire()

        body_json = json.dumps(body, ensure_ascii=False)
        body_bytes = body_json.encode("utf-8")
        ts = str(int(time.time()))
        checksum = self._checksum(body_bytes, ts)

        url = f"{BASE_URL}{path}"
        params = {
            "appKey": self.app_key,
            "time": ts,
            "checksum": checksum,
        }
        headers = {"Content-Type": "application/json;charset=utf-8"}

        last_error = None
        for attempt in range(retries + 1):
            try:
                logger.debug(f"POST {path} body={body}")
                resp = self.session.post(
                    url, params=params, data=body_bytes,
                    headers=headers, timeout=30,
                )
                resp.raise_for_status()
                data = resp.json()

                code = data.get("code", -1)
                if code != 200:
                    logger.warning(f"API业务错误: {path} → code={code}, msg={data.get('message', '')[:200]}")
                else:
                    logger.debug(f"API成功: {path}")

                # 七鱼API的message字段经常是JSON字符串，自动解析
                msg = data.get("message")
                if isinstance(msg, str):
                    try:
                        data["message"] = json.loads(msg)
                    except (json.JSONDecodeError, TypeError):
                        pass  # 保留原始字符串

                return data

            except requests.RequestException as e:
                last_error = e
                logger.warning(f"请求失败 [{attempt+1}/{retries+1}]: {path} → {e}")
                if attempt < retries:
                    time.sleep(1)

        raise ConnectionError(f"API请求失败: {path} → {last_error}")

    # ==================== 工单模板 ====================

    def get_templates(self):
        """获取已启用的工单模板列表"""
        data = self._request(API["ticket_template_list"], {"status": 1})
        return data.get("message", [])

    def get_vip_template_id(self):
        """获取 VIP用户运营工单 模板ID（优先用配置值，否则从API查找）"""
        if self._template_id is not None:
            return self._template_id

        # 优先使用配置中的固定值
        if WORKORDER_TEMPLATE_ID:
            self._template_id = WORKORDER_TEMPLATE_ID
            logger.info(f"使用配置模板ID: {self._template_id}")
            return self._template_id

        # 从API查找
        templates = self.get_templates()
        for t in templates:
            if t.get("name") == WORKORDER_TEMPLATE_NAME:
                self._template_id = t["id"]
                logger.info(f"找到VIP工单模板: id={self._template_id}")
                return self._template_id

        for t in templates:
            if "VIP" in t.get("name", ""):
                self._template_id = t["id"]
                logger.info(f"模糊匹配VIP工单模板: id={self._template_id}, name={t['name']}")
                return self._template_id

        logger.warning(f"未找到模板 '{WORKORDER_TEMPLATE_NAME}'")
        return None

    # ==================== 工单搜索 ====================

    def search_tickets(self, start=None, end=None, op_start=None, op_end=None,
                       with_custom_field=True, limit=50, offset=0):
        """
        搜索工单。
        :param start/end:       创建时间范围(ms)
        :param op_start/op_end: 操作时间范围(ms), 最大90天
        :param with_custom_field: 是否返回自定义字段
        :return: (total, tickets_list)
        """
        body = {
            "limit": limit,
            "offset": offset,
            "sortBy": "ct",
            "order": "desc",
        }
        if with_custom_field:
            body["withCustomField"] = True
        if start is not None:
            body["start"] = start
        if end is not None:
            body["end"] = end
        if op_start is not None:
            body["opStart"] = op_start
        if op_end is not None:
            body["opEnd"] = op_end

        data = self._request(API["ticket_search"], body)
        msg = data.get("message", {})
        if isinstance(msg, dict):
            return msg.get("total", 0), msg.get("tickets", [])
        return 0, []

    def search_all_tickets(self, **kwargs):
        """分页搜索全部工单"""
        all_tickets = []
        offset = 0
        while True:
            total, tickets = self.search_tickets(offset=offset, **kwargs)
            if not tickets:
                break
            all_tickets.extend(tickets)
            offset += len(tickets)
            if offset >= total:
                break
            logger.info(f"分页获取中: {offset}/{total}")
        return all_tickets

    # ==================== 工单详情与日志 ====================

    def get_ticket_detail(self, ticket_id):
        """获取工单详情"""
        data = self._request(API["ticket_detail"], {"ticketId": ticket_id})
        return data.get("data", {})

    def get_ticket_log(self, ticket_id):
        """获取工单日志（操作记录）"""
        data = self._request(API["ticket_log"], {"ticketId": ticket_id})
        return data.get("data", [])

    # ==================== 工单数据增强 ====================

    def enrich_ticket(self, ticket):
        """
        为工单补充详情 + 日志，提取关键字段。
        调用2次API（detail + log），适合对筛选后的少量工单调用。
        """
        tid = ticket.get("id")
        if not tid:
            return ticket

        # 获取详情
        try:
            detail = self.get_ticket_detail(tid)
            if detail:
                # 保留搜索结果中的字段，用详情补充
                for k, v in detail.items():
                    if k not in ticket or ticket[k] is None:
                        ticket[k] = v
        except Exception as e:
            logger.warning(f"获取工单详情失败 #{tid}: {e}")

        # 从工单字段直接提取受理人（优先级最高）
        handler = self._extract_handler_from_ticket(ticket)
        if handler:
            logger.debug(f"工单 #{tid} 从详情字段提取受理人: {handler}")
        else:
            # 首次获取时记录工单中的受理人相关字段，便于排查
            handler_fields = {k: v for k, v in ticket.items()
                              if any(kw in k.lower() for kw in ("staff", "handler", "assignee", "group"))
                              and v}
            if handler_fields:
                logger.info(f"工单 #{tid} 受理人相关字段: {handler_fields}")

        # 获取日志，补充受理方信息
        try:
            log_entries = self.get_ticket_log(tid)
            ticket["_log"] = log_entries
            # 日志解析作为补充：如果工单字段没有受理人，才从日志推断
            if not handler:
                handler = self._parse_handler(log_entries)
            ticket["_handler"] = handler
            ticket["_has_dev_transfer"] = self._has_dev_transfer(log_entries)
        except Exception as e:
            logger.warning(f"获取工单日志失败 #{tid}: {e}")
            ticket["_log"] = []
            ticket["_handler"] = handler
            ticket["_has_dev_transfer"] = False

        return ticket

    @staticmethod
    def _extract_handler_from_ticket(ticket):
        """
        从工单详情/搜索结果的直接字段中提取当前受理人。
        七鱼工单 API 可能返回以下受理人相关字段：
          staffName, groupName, handlerName, assigneeName 等
        """
        # 按优先级尝试多个可能的字段名
        for field in ("staffName", "handlerName", "assigneeName",
                       "staff_name", "handler_name", "assignee_name"):
            val = ticket.get(field)
            if val and str(val).strip():
                return str(val).strip()

        # 尝试从 staffInfo / handler 嵌套对象中提取
        for obj_field in ("staffInfo", "handler", "assignee", "staff"):
            obj = ticket.get(obj_field)
            if isinstance(obj, dict):
                name = obj.get("name") or obj.get("staffName") or obj.get("nickName")
                if name and str(name).strip():
                    return str(name).strip()

        # 尝试 groupName（受理组）
        group = ticket.get("groupName") or ticket.get("group_name")
        if group and str(group).strip():
            return str(group).strip()

        return ""

    def enrich_tickets_concurrent(self, tickets, max_workers=None):
        """
        并发批量补充工单详情（线程池）。
        比串行快 5-10 倍。
        """
        workers = max_workers or API_MAX_WORKERS
        if not tickets:
            return tickets

        logger.info(f"并发补充 {len(tickets)} 条工单详情 (workers={workers})")

        def _enrich_one(idx_ticket):
            idx, t = idx_ticket
            try:
                self.enrich_ticket(t)
            except Exception as e:
                logger.warning(f"补充工单详情失败 [{idx+1}] #{t.get('id')}: {e}")
            return t

        with ThreadPoolExecutor(max_workers=workers) as executor:
            futures = {
                executor.submit(_enrich_one, (i, t)): i
                for i, t in enumerate(tickets)
            }
            for future in as_completed(futures):
                idx = futures[future]
                try:
                    future.result()
                except Exception as e:
                    logger.error(f"工单增强异常 [{idx}]: {e}")

        logger.info(f"并发补充完成: {len(tickets)} 条")
        return tickets

    @staticmethod
    def _parse_handler(log_entries):
        """
        从工单日志推断当前受理方。
        日志格式示例:
          title="受理人", content="由张三更改为李四"
          title="受理组", content="由【一步】悦风更改为【一步】飞鱼"
        """
        if not log_entries:
            return ""

        import re
        handler_keywords = ("受理人", "受理组", "转交", "分配", "指派")

        for entry in reversed(log_entries):
            info_list = entry.get("info", [])
            for info in info_list:
                title = info.get("title", "") or info.get("titleLang", "")
                content = info.get("content", "")
                # 匹配受理人/受理组/转交等关键词（避免匹配"受理状态"等无关项）
                if not any(kw in title for kw in handler_keywords):
                    continue
                if not content:
                    continue
                # 多种格式兼容：
                # "由A更改为B" / "由A变更为B" / "由A转交给B" / "由A转为B"
                m = re.search(r"(?:更改为|变更为|转交给|转给|转为|改为|分配给|指派给)(.+?)$", content)
                if m:
                    return m.group(1).strip()
                # "由X更改为Y" 没匹配到，尝试提取 "→" 或 "->" 后面的部分
                m = re.search(r"[→\->]+\s*(.+?)$", content)
                if m:
                    return m.group(1).strip()
                # 如果都不匹配，且不是纯状态描述，返回原始内容
                # 排除"受理状态"类的干扰内容
                if not any(skip in content for skip in ("已解决", "已关闭", "待处理", "处理中")):
                    return content
        return ""

    @staticmethod
    def _has_dev_transfer(log_entries):
        """
        检查工单日志中是否存在转交给企业微信-飞鱼科技的记录。
        遍历所有日志条目，匹配含"转交"的操作中是否涉及飞鱼科技。
        """
        if not log_entries:
            return False
        for entry in log_entries:
            info_list = entry.get("info", [])
            for info in info_list:
                title = info.get("title", "") or info.get("titleLang", "")
                if "转交" not in title:
                    continue
                # 检查整个 entry 是否包含飞鱼科技（覆盖 content、operator 等各种字段）
                entry_text = json.dumps(entry, ensure_ascii=False)
                if DEV_TRANSFER_KEYWORD in entry_text:
                    return True
        return False

    # ==================== 便捷方法：批量获取 ====================

    def fetch_daily_tickets(self, start_ms, end_ms):
        """
        获取日报时间范围内的所有VIP工单（含自定义字段）。
        并为每条工单并发补充详情和受理方。
        """
        logger.info(f"搜索当日工单: {start_ms} ~ {end_ms}")
        tickets = self.search_all_tickets(start=start_ms, end=end_ms)

        # 按模板过滤（如果能获取到模板ID）
        tmpl_id = self.get_vip_template_id()
        if tmpl_id:
            before = len(tickets)
            tickets = [t for t in tickets if t.get("templateId") == tmpl_id]
            logger.info(f"模板过滤: {before} → {len(tickets)} 条 (templateId={tmpl_id})")

        # 并发补充工单详情
        self.enrich_tickets_concurrent(tickets)

        return tickets

    def fetch_pending_tickets(self, start_ms, end_ms):
        """
        获取近30天状态为"受理中"的VIP工单（待跟进 + 未回访）。
        优化：先按模板+状态过滤，再只对少量工单并发调用详情API。
        """
        logger.info(f"搜索待跟进工单: {start_ms} ~ {end_ms}")
        tickets = self.search_all_tickets(op_start=start_ms, op_end=end_ms)
        logger.info(f"搜索到 {len(tickets)} 条工单（全部模板）")

        # 按VIP模板过滤
        tmpl_id = self.get_vip_template_id()
        if tmpl_id:
            tickets = [t for t in tickets if t.get("templateId") == tmpl_id]
            logger.info(f"VIP模板过滤后: {len(tickets)} 条")

        # 按"受理中"状态过滤
        tickets = [t for t in tickets if t.get("status") == STATUS_PENDING]
        logger.info(f"状态=受理中 过滤后: {len(tickets)} 条")

        # 并发补充工单详情
        self.enrich_tickets_concurrent(tickets)

        return tickets

    # ==================== 会话数据导出 ====================

    def export_session_data(self, start_ms, end_ms, max_wait=120):
        """
        异步导出会话数据。
        1. 提交导出任务
        2. 轮询检查导出状态
        3. 下载并解析数据

        :param start_ms: 开始时间（毫秒）
        :param end_ms:   结束时间（毫秒）
        :param max_wait:  最大等待秒数
        :return: 会话列表
        """
        logger.info(f"提交会话导出任务: {start_ms} ~ {end_ms}")

        # 1. 提交导出
        try:
            export_resp = self._request(API["export_session"], {
                "startTime": start_ms,
                "endTime": end_ms,
            })
            code = export_resp.get("code", -1)
            if code != 200:
                logger.warning(f"会话导出提交失败: code={code}")
                return []

            task_id = None
            msg = export_resp.get("message", {})
            if isinstance(msg, dict):
                task_id = msg.get("taskId") or msg.get("id")
            elif isinstance(msg, str):
                task_id = msg

            if not task_id:
                logger.warning("会话导出未返回 taskId")
                return []

            logger.info(f"会话导出任务ID: {task_id}")

        except Exception as e:
            logger.error(f"会话导出提交异常: {e}")
            return []

        # 2. 轮询状态
        deadline = time.time() + max_wait
        download_url = None
        while time.time() < deadline:
            time.sleep(5)
            try:
                check_resp = self._request(API["export_session_check"], {
                    "taskId": task_id,
                })
                check_msg = check_resp.get("message", {})
                if isinstance(check_msg, dict):
                    status = check_msg.get("status", "")
                    if status in ("completed", "done", "finished", "3"):
                        download_url = check_msg.get("url") or check_msg.get("downloadUrl")
                        break
                    elif status in ("failed", "error", "-1"):
                        logger.error(f"会话导出任务失败: {check_msg}")
                        return []
                logger.debug(f"会话导出进行中: {check_msg}")
            except Exception as e:
                logger.warning(f"检查导出状态失败: {e}")

        if not download_url:
            logger.warning("会话导出超时或未获取到下载链接")
            return []

        # 3. 下载数据
        try:
            logger.info(f"下载会话数据: {download_url}")
            resp = self.session.get(download_url, timeout=60)
            resp.raise_for_status()

            # 尝试解析 JSON
            try:
                data = resp.json()
                if isinstance(data, list):
                    return data
                if isinstance(data, dict):
                    return data.get("data", data.get("sessions", []))
            except json.JSONDecodeError:
                pass

            # CSV 格式解析
            lines = resp.text.strip().split("\n")
            if len(lines) <= 1:
                return []
            headers = lines[0].split(",")
            sessions = []
            for line in lines[1:]:
                values = line.split(",")
                row = dict(zip(headers, values))
                sessions.append(row)
            return sessions

        except Exception as e:
            logger.error(f"下载会话数据失败: {e}")
            return []

    def get_realtime_session_stats(self):
        """获取实时会话概览统计"""
        try:
            data = self._request(API["stat_realtime_session"], {})
            return data.get("message", {})
        except Exception as e:
            logger.warning(f"获取实时会话概览失败: {e}")
            return {}

    # ==================== 报表统计 ====================

    @staticmethod
    def _to_seconds(ts_ms):
        """毫秒时间戳转秒级（统计接口用秒级时间戳）"""
        if ts_ms > 10**12:  # 判断是毫秒
            return ts_ms // 1000
        return ts_ms

    def get_staff_workload(self, start_time, end_time, model=1, retries=3):
        """
        客服工作量报表（带 14009 频控自动重试）。
        model: 1=全部, 2=客服组, 3=客服
        注意：统计接口使用秒级时间戳。
        """
        for attempt in range(retries):
            data = self._request(API["stat_staff_workload"], {
                "startTime": self._to_seconds(start_time),
                "endTime": self._to_seconds(end_time),
                "model": model,
            })
            code = data.get("code", -1)
            if code == 14009:
                wait = 15 * (attempt + 1)
                logger.warning(f"频率限制(14009)，等待 {wait}s 后重试 [{attempt+1}/{retries}]")
                time.sleep(wait)
                continue
            if code == 14500:
                wait = 15 * (attempt + 1)
                logger.warning(f"内部错误(14500)，等待 {wait}s 后重试 [{attempt+1}/{retries}]")
                time.sleep(wait)
                continue
            msg = data.get("message", data)
            if isinstance(msg, dict):
                # 兼容多种响应结构：result / resultList / data
                for key in ("result", "resultList", "data"):
                    items = msg.get(key)
                    if isinstance(items, list):
                        if not items:
                            logger.info(f"工作量报表(model={model}) 返回空列表, key={key}, 完整响应: {json.dumps(data, ensure_ascii=False)[:800]}")
                        return items
                # 没有找到列表字段，记录原始响应用于诊断
                logger.warning(f"工作量报表(model={model}) dict响应无可解析列表, keys={list(msg.keys())}, 完整响应: {json.dumps(data, ensure_ascii=False)[:800]}")
                return []
            if isinstance(msg, list):
                if not msg:
                    logger.warning(f"工作量报表(model={model}) message直接返回空列表, code={code}, 完整响应: {json.dumps(data, ensure_ascii=False)[:800]}")
                return msg
            logger.warning(f"工作量报表返回非预期类型: code={code}, msg_type={type(msg).__name__}, msg={str(msg)[:200]}")
            return []
        logger.error(f"工作量报表重试 {retries} 次仍被频控，放弃")
        return []

    def get_overview(self, start_time, end_time):
        """
        历史数据总览（秒级时间戳）。
        返回含 sessions, effectSessions 等字段。
        """
        data = self._request(API["stat_overview"], {
            "startTime": self._to_seconds(start_time),
            "endTime": self._to_seconds(end_time),
        })
        return data.get("message", data)

    @staticmethod
    def _extract_session_count(item):
        """从工作量报表项中提取会话总量（兼容多种字段名）"""
        for key in ("sessionCount", "totalSession", "sessions",
                     "session_count", "total_session"):
            val = item.get(key)
            if val is not None:
                return int(val or 0)
        return 0

    @staticmethod
    def _extract_group_name(item):
        """从工作量报表项中提取组名（兼容多种字段名）"""
        for key in ("groupName", "name", "group", "group_name",
                     "staffGroupName", "departName"):
            val = item.get(key)
            if val and str(val).strip():
                return str(val).strip()
        return ""

    def get_total_session_count(self, start_time, end_time):
        """
        获取VIP部门所有客服组的会话总量合计（与七鱼坐席工作量报表对齐）。

        策略（按优先级）：
          1. model=3（按坐席）→ 筛选属于匹配组的坐席 → 求和 sessionCount
             （model=2 按客服组在当前账号下始终返回空数据，已弃用）
          2. 历史概览 API → sessions 字段
          3. 实时会话概览 API → sessionInCount 作为近似值（仅当天有效）
        """
        now_ms = int(time.time() * 1000)
        if end_time > now_ms:
            end_time = now_ms

        time.sleep(3)

        # ---------- 方案1: model=3（按坐席）→ 筛选VIP组 → 求和 ----------
        try:
            agents = self.get_staff_workload(start_time, end_time, model=3)
            logger.info(f"工作量报表(按坐席)返回: {len(agents) if isinstance(agents, list) else type(agents).__name__}")
            if isinstance(agents, list) and agents:
                total = 0
                matched = []
                all_groups = set()
                for a in agents:
                    gn = self._extract_group_name(a)
                    all_groups.add(gn or "(空)")
                    if AGENT_GROUP in str(gn):
                        sc = self._extract_session_count(a)
                        total += sc
                        matched.append(f"{a.get('staffName', '?')}[{gn}]={sc}")
                if matched:
                    logger.info(f"按坐席汇总「{AGENT_GROUP}*」: {', '.join(matched)}, 合计={total}")
                    if total > 0:
                        return total
                logger.warning(f"model=3 未匹配包含「{AGENT_GROUP}」的组，可用组: {sorted(all_groups)}")
                if agents:
                    logger.info(f"API返回首条原始数据(诊断): {json.dumps(agents[0], ensure_ascii=False)[:500]}")
        except Exception as e:
            logger.warning(f"获取工作量报表(按坐席)失败: {e}")

        # ---------- 方案3: 历史概览 API → sessions 字段 ----------
        logger.warning("工作量报表API无可用数据，尝试历史概览API(stat_overview)")
        time.sleep(3)
        try:
            overview = self.get_overview(start_time, end_time)
            if isinstance(overview, dict):
                sessions = overview.get("sessions")
                if sessions is not None and int(sessions) > 0:
                    count = int(sessions)
                    logger.info(f"历史概览 sessions={count}")
                    return count
                logger.info(f"历史概览返回: sessions={sessions}, 完整字段={json.dumps({k:v for k,v in overview.items() if isinstance(v,(int,float)) and v>0}, ensure_ascii=False)[:300]}")
        except Exception as e:
            logger.warning(f"获取历史概览失败: {e}")

        # ---------- 方案4: 实时会话概览 → sessionInCount（仅当天有效） ----------
        logger.warning("历史概览无数据，尝试实时会话概览API作为最终兜底（注意：仅当天数据准确）")
        time.sleep(3)
        try:
            realtime = self.get_realtime_session_stats()
            if isinstance(realtime, dict):
                session_in = realtime.get("sessionInCount")
                if session_in is not None and int(session_in) > 0:
                    count = int(session_in)
                    logger.info(f"实时概览 sessionInCount={count} (近似值，仅当天准确)")
                    return count
                logger.info(f"实时概览可用字段: { {k: v for k, v in realtime.items() if 'ession' in k.lower()} }")
        except Exception as e:
            logger.warning(f"获取实时会话概览失败: {e}")

        logger.error("所有统计方案均未获取到会话量，返回0")
        return 0
