"""
令牌桶速率限制器
================
控制 API 请求频率，避免触发七鱼接口频率限制。
"""
import time
import threading


class TokenBucketRateLimiter:
    """令牌桶算法速率限制器（线程安全）"""

    def __init__(self, rate=10, burst=15):
        """
        :param rate:  每秒补充的令牌数
        :param burst: 桶最大容量（突发上限）
        """
        self.rate = rate
        self.burst = burst
        self._tokens = burst
        self._last_refill = time.monotonic()
        self._lock = threading.Lock()

    def _refill(self):
        now = time.monotonic()
        elapsed = now - self._last_refill
        self._tokens = min(self.burst, self._tokens + elapsed * self.rate)
        self._last_refill = now

    def acquire(self, timeout=30):
        """
        获取一个令牌，必要时阻塞等待。
        :param timeout: 最大等待秒数
        :return: True 成功获取, False 超时
        """
        deadline = time.monotonic() + timeout
        while True:
            with self._lock:
                self._refill()
                if self._tokens >= 1:
                    self._tokens -= 1
                    return True
            # 等待一个令牌补充的时间
            wait = 1.0 / self.rate
            if time.monotonic() + wait > deadline:
                return False
            time.sleep(wait)

    def try_acquire(self):
        """非阻塞尝试获取令牌"""
        with self._lock:
            self._refill()
            if self._tokens >= 1:
                self._tokens -= 1
                return True
            return False
