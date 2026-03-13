"""
诊断会话总量：对比各API返回值，找出与后台657匹配的数据源。
用法: python debug_sessions.py
"""
import json
from datetime import datetime
from qiyu_client import QiyuClient

client = QiyuClient()

# 3月12日完整自然日
day = datetime(2026, 3, 12)
start = int(day.replace(hour=0, minute=0, second=0).timestamp() * 1000)
end = int(day.replace(hour=23, minute=59, second=59).timestamp() * 1000)

print(f"查询时间: {day.strftime('%Y-%m-%d')} 00:00:00 ~ 23:59:59")
print(f"时间戳(ms): {start} ~ {end}")
print(f"时间戳(s):  {start//1000} ~ {end//1000}")
print(f"后台显示: 657")
print("=" * 60)

# 1. 历史数据总览
print("\n[1] 历史数据总览 (stat_overview)")
try:
    overview = client.get_overview(start, end)
    print(json.dumps(overview, ensure_ascii=False, indent=2)[:1000])
except Exception as e:
    print(f"  失败: {e}")

# 2. 工作量报表 model=1 (全部)
print("\n[2] 工作量报表 model=1 (全部)")
try:
    wl1 = client.get_staff_workload(start, end, model=1)
    if isinstance(wl1, list):
        total = sum(s.get("totalSessionCount", 0) for s in wl1)
        print(f"  记录数: {len(wl1)}, sum(totalSessionCount)={total}")
        for s in wl1[:5]:
            print(f"    {s.get('staffName','?')}: totalSessionCount={s.get('totalSessionCount',0)}, "
                  f"effectSessionCount={s.get('effectSessionCount',0)}, "
                  f"sessionCount={s.get('sessionCount',0)}")
        if len(wl1) > 5:
            print(f"    ... 共 {len(wl1)} 条")
    else:
        print(json.dumps(wl1, ensure_ascii=False, indent=2)[:500])
except Exception as e:
    print(f"  失败: {e}")

# 3. 工作量报表 model=2 (按客服组)
print("\n[3] 工作量报表 model=2 (按客服组)")
try:
    wl2 = client.get_staff_workload(start, end, model=2)
    if isinstance(wl2, list):
        total = sum(s.get("totalSessionCount", 0) for s in wl2)
        print(f"  记录数: {len(wl2)}, sum(totalSessionCount)={total}")
        for s in wl2:
            name = s.get("groupName", s.get("staffName", "?"))
            print(f"    {name}: totalSessionCount={s.get('totalSessionCount',0)}, "
                  f"effectSessionCount={s.get('effectSessionCount',0)}, "
                  f"sessionCount={s.get('sessionCount',0)}")
    else:
        print(json.dumps(wl2, ensure_ascii=False, indent=2)[:500])
except Exception as e:
    print(f"  失败: {e}")

# 4. 实时会话概览
print("\n[4] 实时会话概览 (stat_realtime_session)")
try:
    rt = client.get_realtime_session_stats()
    print(json.dumps(rt, ensure_ascii=False, indent=2)[:500])
except Exception as e:
    print(f"  失败: {e}")

print("\n" + "=" * 60)
print("对比以上输出，找出哪个字段 = 657")
