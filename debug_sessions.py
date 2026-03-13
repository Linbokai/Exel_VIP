"""
诊断会话总量：打印倍特VIP工单组的所有API字段，找出=381的字段。
用法: python debug_sessions.py
"""
import json
from datetime import datetime
from qiyu_client import QiyuClient
from config import AGENT_GROUP

client = QiyuClient()

# 3月12日完整自然日
day = datetime(2026, 3, 12)
start = int(day.replace(hour=0, minute=0, second=0).timestamp() * 1000)
end = int(day.replace(hour=23, minute=59, second=59).timestamp() * 1000)

print(f"查询: {day.strftime('%Y-%m-%d')} 00:00~23:59, 目标组: {AGENT_GROUP}")
print(f"后台坐席报表: 会话总量=381, 接入=213, 有效=211, 主动发起=168")
print("=" * 70)

# 按客服组查询
print("\n[工作量报表 model=2 按客服组]")
try:
    wl2 = client.get_staff_workload(start, end, model=2)
    if isinstance(wl2, list):
        for g in wl2:
            name = g.get("groupName", g.get("name", "?"))
            if AGENT_GROUP in str(name):
                print(f"\n  >>> 匹配组: {name}")
                print(f"  所有字段:")
                for k, v in sorted(g.items()):
                    print(f"    {k}: {v}")
            else:
                print(f"  [跳过] {name}")
    else:
        print(f"  返回类型: {type(wl2).__name__}")
        print(json.dumps(wl2, ensure_ascii=False, indent=2)[:500])
except Exception as e:
    print(f"  失败: {e}")

# 按全部查询（看总计行）
print("\n\n[工作量报表 model=1 全部]")
try:
    wl1 = client.get_staff_workload(start, end, model=1)
    if isinstance(wl1, list):
        print(f"  记录数: {len(wl1)}")
        for s in wl1[:3]:
            name = s.get("staffName", s.get("name", "?"))
            print(f"  {name}: ", end="")
            for k in ("totalSessionCount", "sessionCount", "effectSessionCount",
                       "acceptSessionCount", "inSessionCount", "activeSessionCount"):
                if k in s:
                    print(f"{k}={s[k]}, ", end="")
            print()
        if len(wl1) > 3:
            print(f"  ... 共 {len(wl1)} 条")
    elif isinstance(wl1, dict):
        print("  返回dict，所有字段:")
        for k, v in sorted(wl1.items()):
            print(f"    {k}: {v}")
except Exception as e:
    print(f"  失败: {e}")

print("\n" + "=" * 70)
print("找到值=381的字段名，告诉我即可修复")
