"""
AI 智能分类与摘要
=================
使用 LLM API 对工单进行智能分类和一句话摘要。
支持 OpenAI 兼容接口（OpenAI / 通义千问 / 本地部署等）。
"""
import json
import logging
import requests

from config import (
    LLM_ENABLED, LLM_API_URL, LLM_API_KEY,
    LLM_MODEL, LLM_TIMEOUT, ISSUE_KEYWORDS,
)

logger = logging.getLogger(__name__)

CATEGORIES = list(ISSUE_KEYWORDS.keys()) + ["其他问题"]

CLASSIFY_SYSTEM_PROMPT = f"""你是一个游戏客服工单分类助手。根据工单标题和内容，完成以下两个任务：
1. 将工单分类到以下类别之一：{', '.join(CATEGORIES)}
2. 用一句话（不超过50字）概括工单核心问题

输出严格的JSON格式：
{{"category": "分类名称", "summary": "一句话摘要"}}

注意：
- 只输出JSON，不要输出其他内容
- 分类名称必须是上述类别之一
- 摘要要简洁明了，包含关键信息（玩家名、问题类型、金额等）"""


def classify_and_summarize(title, content, timeout=None):
    """
    使用 LLM 对单条工单进行分类和摘要。
    :return: {"category": str, "summary": str} 或 None（失败时）
    """
    if not LLM_ENABLED or not LLM_API_KEY:
        return None

    user_text = f"工单标题：{title}\n工单内容：{content[:500]}"

    try:
        headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {LLM_API_KEY}",
        }
        payload = {
            "model": LLM_MODEL,
            "messages": [
                {"role": "system", "content": CLASSIFY_SYSTEM_PROMPT},
                {"role": "user", "content": user_text},
            ],
            "temperature": 0.1,
            "max_tokens": 200,
        }
        resp = requests.post(
            LLM_API_URL, headers=headers,
            json=payload, timeout=timeout or LLM_TIMEOUT,
        )
        resp.raise_for_status()
        data = resp.json()
        text = data["choices"][0]["message"]["content"].strip()

        # 尝试提取JSON（有时LLM会包裹在markdown代码块中）
        if text.startswith("```"):
            text = text.split("```")[1]
            if text.startswith("json"):
                text = text[4:]
            text = text.strip()

        result = json.loads(text)
        # 验证分类合法性
        if result.get("category") not in CATEGORIES:
            result["category"] = "其他问题"
        return result

    except Exception as e:
        logger.warning(f"LLM分类失败: {e}")
        return None


def batch_classify(tickets, max_batch=10):
    """
    批量分类工单。为每条工单添加 _ai_category 和 _ai_summary 字段。
    失败的工单保持原有关键词分类。
    """
    if not LLM_ENABLED or not LLM_API_KEY:
        logger.info("LLM未启用，跳过AI分类")
        return

    for i, t in enumerate(tickets):
        title = t.get("_title", "") or t.get("title", "")
        content = t.get("_content", "") or t.get("content", "")
        if not title and not content:
            continue

        result = classify_and_summarize(title, content)
        if result:
            t["_ai_category"] = result["category"]
            t["_ai_summary"] = result["summary"]
            logger.debug(f"AI分类 [{i+1}/{len(tickets)}]: {result['category']} - {result['summary']}")
        else:
            logger.debug(f"AI分类失败 [{i+1}/{len(tickets)}]，使用关键词分类")

        # 控制速率
        if (i + 1) % max_batch == 0:
            import time
            time.sleep(1)
