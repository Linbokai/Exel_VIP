"""
诊断会话总量：对比 model=1 逐坐席求和 vs model=2 组级聚合，找出与后台一致的值。
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
print("=" * 70)

# ===== model=1 逐个坐席，按组过滤后求和（与页面"总计"行对齐）=====
print("\n[model=1 逐个坐席 → 按组过滤求和]")
try:
    wl1 = client.get_staff_workload(start, end, model=1)
    if isinstance(wl1, list):
        # 找出目标组的所有坐席
        matched = [s for s in wl1 if AGENT_GROUP in (s.get("groupName", "") or s.get("group_name", ""))]
        print(f"  总坐席数: {len(wl1)}, 匹配「{AGENT_GROUP}」: {len(matched)} 人")

        if matched:
            # 收集所有数值型字段名
            num_fields = sorted(set(
                k for s in matched for k, v in s.items()
                if isinstance(v, (int, float)) and v != 0
            ))
            # 打印每个坐席的关键字段
            print(f"\n  逐个坐席明细:")
            for s in matched:
                name = s.get("staffName", s.get("name", "?"))
                print(f"    {name}:")
                for k in num_fields:
                    if k in s and s[k]:
                        print(f"      {k}: {s[k]}")

            # 求和并打印
            print(f"\n  *** 各字段求和（= 页面总计行）***")
            for k in num_fields:
                total = sum(int(s.get(k, 0) or 0) for s in matched)
                if total > 0:
                    print(f"    sum({k}) = {total}")
        else:
            all_groups = sorted(set(s.get("groupName", s.get("group_name", "?")) for s in wl1))
            print(f"  未匹配! 所有组名: {all_groups}")
    else:
        print(f"  返回类型: {type(wl1).__name__}")
except Exception as e:
    print(f"  失败: {e}")

# ===== model=2 按客服组（对比用）=====
print("\n\n[model=2 按客服组（对比用）]")
try:
    wl2 = client.get_staff_workload(start, end, model=2)
    if isinstance(wl2, list):
        for g in wl2:
            name = g.get("groupName", g.get("name", "?"))
            if AGENT_GROUP in str(name):
                print(f"  >>> 匹配组: {name}")
                for k, v in sorted(g.items()):
                    if isinstance(v, (int, float)) and v != 0:
                        print(f"    {k}: {v}")
except Exception as e:
    print(f"  失败: {e}")

print("\n" + "=" * 70)
print("对比 model=1 求和值 与 model=2 组值，找出与后台一致的数字")
