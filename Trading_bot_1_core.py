#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
업비트 자동매매 봇 v1.0 - Core Module
========================================
"""

import logging
import time
import re
import asyncio
import sys
from pathlib import Path
from enum import Enum
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Dict, Optional, Any, List, Callable
from global_constants import *
import functools

class DailyRotatingFileHandler(logging.Handler):
    """날짜가 파일명에 포함되는 커스텀 핸들러"""
    
    def __init__(self, log_dir: Path, base_name: str = "trading_bot", 
                 encoding: str = 'utf-8', level=logging.INFO):
        super().__init__(level)
        self.log_dir = log_dir
        self.base_name = base_name
        self.encoding = encoding
        self.current_date = None
        self.current_handler = None
        self._update_handler()
    
    def _update_handler(self):
        """날짜가 바뀌면 새 파일 핸들러 생성"""
        today = datetime.now().strftime('%Y%m%d')
        
        if self.current_date != today:
            if self.current_handler:
                self.current_handler.close()
            
            log_file = self.log_dir / f"{self.base_name}_{today}.log"
            self.current_handler = logging.FileHandler(
                log_file,
                mode='a',
                encoding=self.encoding
            )
            # Windows CRLF 삽입 방지: 스트림을 newline='\n' 으로 교체
            import io
            self.current_handler.stream.close()
            self.current_handler.stream = io.open(
                log_file, mode='a', encoding=self.encoding, newline='\n'
            )
            if hasattr(self, 'formatter') and self.formatter:
                self.current_handler.setFormatter(self.formatter)
            else:
                # 기본 포맷터 생성
                self.current_handler.setFormatter(
                    logging.Formatter(
                        '%(asctime)s - %(levelname)s - %(message)s',
                        datefmt='%Y-%m-%d %H:%M:%S'
                    )
                )
            self.current_date = today
    
    def emit(self, record):
        """로그 기록"""
        try:
            self._update_handler()
            if self.current_handler:
                self.current_handler.emit(record)
        except Exception:
            self.handleError(record)
    
    def close(self):
        """핸들러 종료"""
        if self.current_handler:
            self.current_handler.close()
        super().close()

# ========================================
# 로깅 설정
# ========================================
log_dir = Path("outputs") / "logs"
log_dir.mkdir(parents=True, exist_ok=True)

logger = logging.getLogger("trading_bot")
logger.setLevel(logging.INFO)

if not logger.handlers:
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    
    file_handler = DailyRotatingFileHandler(
        log_dir=log_dir,
        base_name="trading_bot",
        encoding='utf-8-sig',
        level=logging.INFO
    )
    
    formatter = logging.Formatter(
        '%(asctime)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    console_handler.setFormatter(formatter)
    file_handler.setFormatter(formatter)
    
    logger.addHandler(console_handler)

    class _ScanThrottleFilter(logging.Filter):
        """per(TAG, 종목) _INTERVAL초당 1회만 파일 기록 — 이벤트/WARNING/ERROR는 항상 통과"""
        # 🔒 [SCAN-THROTTLE-EVENT-TAGS-0903] ★502-R1
        # 실제 throttle은 200초. 값은 유지하고 문서만 실제 동작에 맞춘다.
        _INTERVAL = 200
        # 🔒 [SCAN-THROTTLE-EVENT-TAGS-0903] ★502-R1
        # 아래 태그는 매스캔 센서가 아니라 진입 판정/최종 veto/양성 provenance 이벤트다.
        # t6 [JUDGE-SRC]~[STRATEGY-DECISION] 사이 final 분기와
        # J3 Strategy-ingress 양성 봉인을 파일에서 전수 보존한다.
        # STALE-BLOCK은 logger.warning이라 별도 등록 없이도 항상 통과한다.
        # JUDGE1~4는 매 스캔 산출이라 면제하지 않아 로그 폭증을 막는다.
        # 🔒 [J1J3-FIDELITY-COMPACT-0915] ★676-R1
        # ★§675 : Gate① miss 12건 중 UNKNOWN-DATA 5건의 최초 divergence 입력이
        #  [JUDGE3-CYCLE](791B) / [JUDGE1-GAJAMI](803B) 에만 있는데 200s 에 잘린다.
        #  두 원본은 계속 스로틀에 두고, t5 가 같은 자리에서 찍는 compact 복제본
        #  [J3-FID](~173B) / [J1-FID](~163B) 만 면제한다.
        #  ★관측 대상 상태로 면제를 게이팅하지 않는다 — 전 종목 동일 적용이다
        #  (상태로 거르면 그 상태에 못 들어간 사례가 다시 사라진다).
        _EVENT_TAGS = frozenset([
            "BUY", "SELL", "STOP_LOSS", "TAKE_PROFIT",
            "MA15-STRUCTURE-EXIT", "MA15-SL-HARD-CAP",
            "JUDGE-SRC", "JUDGE-APPROVAL", "J3-INGRESS-OK",
            "NO-JUDGE-APPROVAL", "STRATEGY-DECISION",
            "RR-INVERTED-BLOCK", "ENTRY-PREM-BLOCK",
            "ENTRY-ATR-HIGH-BLOCK",
            "J3-FID", "J1-FID",
        ])
        _TAG_RE = re.compile(r'\[([A-Z0-9_\-]+)\]\s+(KRW-[A-Z0-9]+)')

        def __init__(self):
            super().__init__()
            self._last: dict = {}

        def filter(self, record: logging.LogRecord) -> bool:
            # WARNING/ERROR 이상은 항상 통과
            if record.levelno >= logging.WARNING:
                return True
            m = self._TAG_RE.search(record.getMessage())
            if not m:
                return True  # 태그/종목 미매칭 → 통과
            tag, symbol = m.group(1), m.group(2)
            if tag in self._EVENT_TAGS:
                return True  # 이벤트 태그 항상 통과
            key = (tag, symbol)
            now = record.created
            if now - self._last.get(key, 0) >= self._INTERVAL:
                self._last[key] = now
                return True
            return False

    file_handler.addFilter(_ScanThrottleFilter())
    logger.addHandler(file_handler)

def setup_logging(level: str = 'INFO'):
    """로깅 설정 초기화"""
    log_level = getattr(logging, level.upper(), logging.INFO)
    logger.setLevel(log_level)
    
    for handler in logger.handlers:
        handler.setLevel(log_level)

# ========================================
# 유틸리티 클래스
# ========================================
class TimerManager:
    """타이머 관리 클래스"""
    
    def __init__(self, intervals: Dict[str, float] = None):
        self.timers = {}
        self.intervals = intervals or {}
    
    def start_timer(self, name: str):
        """타이머 시작"""
        self.timers[name] = time.time()
    
    def get_elapsed(self, name: str) -> float:
        """경과 시간 반환"""
        if name in self.timers:
            return time.time() - self.timers[name]
        return 0
    
    def reset_timer(self, name: str):
        """타이머 리셋"""
        self.timers[name] = time.time()
    
    def check_interval(self, name: str, interval_seconds: float = None) -> bool:
        """간격 체크"""
        if interval_seconds is None:
            interval_seconds = self.intervals.get(name, 60)
        
        if name not in self.timers:
            self.start_timer(name)
            return True
        
        if self.get_elapsed(name) >= interval_seconds:
            self.reset_timer(name)
            return True
        
        return False
    
    def check_and_reset(self, name: str, interval_seconds: float = None) -> bool:
        """간격 체크 및 리셋"""
        return self.check_interval(name, interval_seconds)


class AdaptiveRateLimiter:
    """적응형 레이트 리미터"""
    
    def __init__(self, calls_per_second: int = 5, calls_per_minute: int = 280):
        self.calls_per_second = calls_per_second
        self.calls_per_minute = calls_per_minute
        self.second_calls = []
        self.minute_calls = []
        self.lock = asyncio.Lock()
    
    async def wait_if_needed(self):
        """필요시 대기"""
        async with self.lock:
            now = time.time()
            
            self.second_calls = [t for t in self.second_calls if now - t < 1]
            if len(self.second_calls) >= self.calls_per_second:
                wait_time = 1 - (now - self.second_calls[0])
                if wait_time > 0:
                    await asyncio.sleep(wait_time)
                    now = time.time()
            
            self.minute_calls = [t for t in self.minute_calls if now - t < 60]
            if len(self.minute_calls) >= self.calls_per_minute:
                wait_time = 60 - (now - self.minute_calls[0])
                if wait_time > 0:
                    await asyncio.sleep(wait_time)
                    now = time.time()
            
            self.second_calls.append(now)
            self.minute_calls.append(now)


class CircuitBreaker:
    """서킷 브레이커"""
    
    def __init__(self, failure_threshold: int = 5, timeout: float = 60.0, 
                 success_threshold: int = 3):
        self.failure_threshold = failure_threshold
        self.timeout = timeout
        self.success_threshold = success_threshold
        self.failures = 0
        self.successes = 0
        self.last_failure_time = None
        self.state = "CLOSED"
    
    async def can_attempt(self) -> bool:
        """시도 가능 여부"""
        if self.state == "CLOSED":
            return True
        
        if self.state == "OPEN":
            if (self.last_failure_time and 
                time.time() - self.last_failure_time >= self.timeout):
                self.state = "HALF_OPEN"
                self.successes = 0
                return True
            return False
        
        if self.state == "HALF_OPEN":
            return True
        
        return False
    
    async def record_success(self):
        """성공 기록"""
        if self.state == "HALF_OPEN":
            self.successes += 1
            if self.successes >= self.success_threshold:
                self.state = "CLOSED"
                self.failures = 0
        elif self.state == "CLOSED":
            self.failures = max(0, self.failures - 1)
    
    async def record_failure(self):
        """실패 기록"""
        self.failures += 1
        self.last_failure_time = time.time()
        
        if self.failures >= self.failure_threshold:
            self.state = "OPEN"
        elif self.state == "HALF_OPEN":
            self.state = "OPEN"

# ========================================
# 데코레이터
# ========================================
def singleton(cls):
    """싱글톤 패턴 데코레이터"""
    instances = {}
    def get_instance(*args, **kwargs):
        if cls not in instances:
            instances[cls] = cls(*args, **kwargs)
        return instances[cls]
    return get_instance

def with_retry(max_attempts: int = 3, delay: float = 1.0):
    """재시도 데코레이터"""
    def decorator(func):
        @functools.wraps(func)
        async def wrapper(*args, **kwargs):
            for attempt in range(max_attempts):
                try:
                    return await func(*args, **kwargs)
                except Exception as e:
                    if attempt == max_attempts - 1:
                        raise
                    logger.warning(f"{func.__name__} 실패 (시도 {attempt + 1}/{max_attempts}): {e}")
                    await asyncio.sleep(delay * (2 ** attempt))
            return None
        return wrapper
    return decorator

def with_timeout(timeout_seconds: float = 30.0):
    """타임아웃 데코레이터"""
    def decorator(func):
        @functools.wraps(func)
        async def wrapper(*args, **kwargs):
            try:
                return await asyncio.wait_for(
                    func(*args, **kwargs),
                    timeout=timeout_seconds
                )
            except asyncio.TimeoutError:
                logger.warning(f"{func.__name__} 타임아웃 ({timeout_seconds}초)")
                return None
        return wrapper
    return decorator

def with_error_handling(default_return=None):
    """에러 처리 데코레이터"""
    def decorator(func):
        @functools.wraps(func)
        async def async_wrapper(*args, **kwargs):
            try:
                return await func(*args, **kwargs)
            except Exception as e:
                logger.error(f"{func.__name__} 오류: {e}")
                return default_return
        
        @functools.wraps(func)
        def sync_wrapper(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except Exception as e:
                logger.error(f"{func.__name__} 오류: {e}")
                return default_return
        
        if asyncio.iscoroutinefunction(func):
            return async_wrapper
        else:
            return sync_wrapper
    return decorator

def requires_candles(min_candles: int):
    """최소 캔들 수 검증 데코레이터"""
    def decorator(func):
        @functools.wraps(func)
        def wrapper(self, df, *args, **kwargs):
            if df is None or len(df) < min_candles:
                return 0 if 'signal' in func.__name__ else None
            return func(self, df, *args, **kwargs)
        return wrapper
    return decorator

# ========================================
# 열거형 정의
# ========================================

class MarketRegime(Enum):
    """시장 체제 - 기술적 분석 기반"""
    TRENDING_UP = "TRENDING_UP"      # 상승 추세
    TRENDING_DOWN = "TRENDING_DOWN"  # 하락 추세
    RANGING = "RANGING"              # 횡보/레인징
    BREAKOUT = "BREAKOUT"            # 돌파
    BREAKDOWN = "BREAKDOWN"          # 붕괴    
    BULL_MARKET = "BULL_MARKET"      # 불장 (강한 상승)

class TimeFrame(Enum):
    """시간 프레임"""
    M1 = "1m"
    M3 = "3m"
    M5 = "5m"
    M10 = "10m"
    M15 = "15m"
    M30 = "30m"
    H1 = "1h"
    H4 = "4h"
    D1 = "1d"
    
    @classmethod
    def from_string(cls, value: str):
        """문자열에서 TimeFrame 생성"""
        for member in cls:
            if member.value == value:
                return member
        return cls.M5
    
    @property
    def minutes(self):
        """분 단위 반환"""
        mapping = {
            "1m": 1, "3m": 3, "5m": 5, "10m": 10,
            "15m": 15, "30m": 30, "1h": 60, "4h": 240, "1d": 1440
        }
        return mapping.get(self.value, 1)
    
    @property
    def trading_interval(self):
        """거래 간격 (초)"""
        intervals = {
            "1m": 30, "3m": 60, "5m": 90, "10m": 120,
            "15m": 180, "30m": 300, "1h": 600, "4h": 1200, "1d": 3600
        }
        return intervals.get(self.value, 60)
    
    @property
    def config(self):
        """타임프레임별 설정"""
        return TIMEFRAME_CONFIGS.get(self.value, TIMEFRAME_CONFIGS[DEFAULT_TIMEFRAME])

# ========================================
# 데이터 클래스
# ========================================
@dataclass
class TradingSignal:
    """거래 신호"""
    symbol: str
    action: str
    price: float
    reason: str
    confidence: float = 0.5
    signal_components: Dict[str, float] = field(default_factory=dict)
    market_regime: Optional[MarketRegime] = None
    primary_signal: str = ""
    sell_ratio: float = 1.0
    urgency: str = "normal"
    indicators: Dict[str, Any] = field(default_factory=dict)
    timestamp: Optional[datetime] = field(default_factory=lambda: datetime.now())  # 신호 생성 시각

@dataclass
class Position:
    """포지션 정보"""
    symbol: str
    volume: float
    avg_price: float
    entry_time: datetime
    entry_signals: Dict[str, float] = field(default_factory=dict)
    entry_reason: str = ""
    stop_loss: float = 0
    take_profit: float = 0
    peak_price: float = 0
    trailing_stop_price: float = 0
    trailing_stop_distance: float = 0
    entry_fee_paid: float = 0
    entry_atr: float = 0
    entry_regime: Optional['MarketRegime'] = None
    metadata: Dict[str, Any] = field(default_factory=dict)
    partial_sell_count: int = 0  # 부분 매도 실행 횟수 (0~3)
    original_volume: float = 0  # 🆕 최초 진입 수량 (불타기 시 합산, 부분 매도 계산 기준)
    partial_sell_history: List[Dict] = field(default_factory=list)  # 부분 매도 이력
    
    # ========================================
    # 🆕 체제별 동적 추적손절 개선 (2025-11-06)
    # ========================================
    def update_peak(self, current_price: float, regime: Optional['MarketRegime'] = None):
        """
        체제별 동적 추적손절 업데이트 (설정 기반 개선 버전)
        --------------------------------------------------------
        - peak_price: 항상 최고가 갱신
        - trailing_stop: 체제별 activation 이상일 때만 갱신
        - 손실 구간은 비활성화
        - ✅ trailing_activation_pct 설정값 우선 사용        
        Args:
            current_price: 현재가
            regime: 시장 체제 (None 가능)
        """
        if self.avg_price <= 0:
            return

        profit_pct = (current_price - self.avg_price) / self.avg_price

        # 손실 구간에서는 추적손절 작동하지 않음
        if profit_pct < 0:
            # 단, peak은 갱신 (하락 후 반등 대비)
            if current_price > self.peak_price:
                self.peak_price = current_price
            return

        # 1️⃣ 최고가 추적은 항상 수행
        if current_price > self.peak_price:
            self.peak_price = current_price

        # 2️⃣ ✅ 추적손절 활성화 기준을 "설정 기반"으로 전환
        # trailing_stop_distance는 RiskManager에서 이미 계산됨
        base_activation = self.metadata.get("trailing_activation_pct")
        
        # 설정값이 없을 경우에만 최소 fallback
        if base_activation is None:
            base_activation = 0.005  # 안전 fallback (기본 0.5%)
        
        if regime is not None:
            if regime.name == "RANGING":
                activation_pct = base_activation * 1.8   # 과도한 조기 청산 방지 (2.0→1.8)
            elif regime.name == "TRENDING_UP":
                activation_pct = base_activation * 0.7   # 수익 보호 조기 개입
            elif regime.name == "BREAKOUT":
                activation_pct = base_activation * 1.0   # 유지
            elif regime.name in ("TRENDING_DOWN", "BREAKDOWN"):
                return  # 하락 체제에서는 비활성
            else:
                activation_pct = base_activation
        else:
            activation_pct = base_activation * 3.0  # 체제 미확정 시 보수적 (0.02→동적)

        # 3️⃣ 체제별 조건 이상 시 trailing stop 활성화
        if profit_pct >= activation_pct and self.trailing_stop_distance > 0:
            self.trailing_stop_price = self.peak_price - self.trailing_stop_distance
    
    def get_return_pct(self, current_price: float) -> float:
        """수익률 계산 (%)"""
        if self.avg_price <= 0:
            return 0
        return ((current_price - self.avg_price) / self.avg_price) * 100
    
    def should_timeout(self, timeout_hours: Optional[float] = None) -> bool:
        """타임아웃 체크"""
        if timeout_hours is None:
            # metadata에서 가져오기 시도
            timeout_hours = self.metadata.get('timeout_hours')
            
            # 없으면 CONFIG에서 가져오기
            if timeout_hours is None:
                from Trading_bot_2_config import ConfigManager
                config = ConfigManager.get_instance()
                timeout_hours = config.get_cached('timeout_hours', fallback=6.0)
        
        holding_hours = (datetime.now() - self.entry_time).total_seconds() / 3600
        return holding_hours >= timeout_hours
    
    def get_holding_hours(self) -> float:
        """보유 시간 반환 (시간 단위)"""
        return (datetime.now() - self.entry_time).total_seconds() / 3600
    
    def get_timeout_info(self, timeout_hours: Optional[float] = None) -> Dict[str, Any]:
        """
        타임아웃 정보 반환 (디버깅용)
        Args:
            timeout_hours: 타임아웃 기준 시간. None이면 자동 결정
        Returns:
            타임아웃 상태 정보
        """
        # timeout_hours 결정 (should_timeout과 동일 로직)
        if timeout_hours is None:
            timeout_hours = self.metadata.get('timeout_hours')
            if timeout_hours is None:
                from Trading_bot_2_config import ConfigManager
                config = ConfigManager.get_instance()
                timeout_hours = config.get_cached('timeout_hours', fallback=6.0)
        
        holding_hours = self.get_holding_hours()
        
        return {
            'holding_hours': round(holding_hours, 2),
            'timeout_hours': timeout_hours,
            'remaining_hours': round(timeout_hours - holding_hours, 2),
            'is_timeout': holding_hours >= timeout_hours,
            'timeout_source': 'metadata' if 'timeout_hours' in self.metadata else 'config'
        }
        
    # ========================================
    # 🆕 부분 매도 관련 메서드
    # ========================================
    
    def record_partial_sell(self, stage: int, price: float, volume: float, ratio: float):
        """부분 매도 기록
        
        Args:
            stage: 단계 (1, 2, 3)
            price: 매도가
            volume: 매도 수량
            ratio: 매도 비율
        """
        self.partial_sell_count += 1
        
        return_pct = (price - self.avg_price) / self.avg_price * 100
        
        self.partial_sell_history.append({
            'stage': stage,
            'time': datetime.now(),
            'price': price,
            'volume': volume,
            'ratio': ratio,
            'return_pct': return_pct
        })
        
        logger.debug(
            f"[부분매도] {self.symbol} {stage}차 기록: "
            f"{volume:.8f}개 @ {price:,.0f}원 ({ratio*100:.0f}%) "
            f"수익률: {return_pct:+.2f}%"
        )
    
    def get_next_partial_sell_stage(self) -> int:
        """다음 부분 매도 단계 반환
        
        Returns:
            int: 1, 2, 3 또는 0 (완료)
        """
        if self.partial_sell_count >= 3:
            return 0  # 완료
        return self.partial_sell_count + 1
    
    def is_partial_sell_complete(self) -> bool:
        """부분 매도 완료 여부
        
        Returns:
            bool: True이면 3단계 모두 완료
        """
        return self.partial_sell_count >= 3
    
    def get_partial_sell_summary(self) -> Dict[str, Any]:
        """부분 매도 요약 정보
        
        Returns:
            Dict: 부분 매도 통계
        """
        if not self.partial_sell_history:
            return {
                'completed_stages': 0,
                'total_stages': 3,
                'is_complete': False,
                'avg_return_pct': 0.0,
                'total_volume_sold': 0.0
            }
        
        total_volume = sum(h['volume'] for h in self.partial_sell_history)
        avg_return = sum(h['return_pct'] for h in self.partial_sell_history) / len(self.partial_sell_history)
        
        return {
            'completed_stages': len(self.partial_sell_history),
            'total_stages': 3,
            'is_complete': self.is_partial_sell_complete(),
            'avg_return_pct': avg_return,
            'total_volume_sold': total_volume,
            'history': self.partial_sell_history.copy()
        }    
    
@dataclass
class ExecutionResult:
    """주문 실행 결과"""
    success: bool = False
    order_id: str = ""
    symbol: str = ""
    side: str = ""
    executed_price: float = 0
    executed_volume: float = 0
    fee: float = 0
    error: str = ""
    avg_price: float = 0
    total_fee: float = 0
    slippage_pct: float = 0.0   # [SELLSLIP 11-14] 매도 응답 미끄러짐 (%) · 3_performance L1530 getattr 로 DB 기록
    
@dataclass
class TradingSettings:
    """거래 설정"""
    stop_loss_pct: float = 0.025
    atr_stop_loss_multiplier: float = 0.0
    take_profit_pct: float = 0.045
    trailing_stop_pct: float = 0.015
    enable_trailing_stop: bool = True
    enable_timeout_exit: bool = True
    timeout_hours: float = 4
    timeout_only_if_losing: bool = True
    enable_15min_ma_exit: bool = True
    ma_breach_threshold: float = 0.003
    max_positions: int = 8
    ma_exit_applicable_timeframes: List[str] = field(default_factory=list)
    
    @staticmethod
    def from_config(config) -> 'TradingSettings':
        """
        Config에서 안전하게 TradingSettings 생성
        - 섹션/키 매핑을 통해 스코프 모호성 제거
        - get_cached 존재 시 우선 사용, 없으면 ConfigParser 표준 접근
        - 값 범위 + 상호 제약 검증
        """
        
        # ========================================
        # 0. 섹션/키 매핑 (INI 구조 명시화)
        # ========================================
        KEY_MAP = {
            # RISK_MANAGEMENT
            'stop_loss_pct':             ('RISK_MANAGEMENT', 'stop_loss_pct'),
            'atr_stop_loss_multiplier':  ('ATR_SETTINGS', 'atr_stop_loss_multiplier'),
            'take_profit_pct':           ('RISK_MANAGEMENT', 'take_profit_pct'),
            'trailing_stop_pct':         ('RISK_MANAGEMENT', 'trailing_stop_pct'),
            'enable_trailing_stop':      ('RISK_MANAGEMENT', 'enable_trailing_stop'),
            'enable_timeout_exit':       ('RISK_MANAGEMENT', 'enable_timeout_exit'),
            'timeout_hours':             ('RISK_MANAGEMENT', 'timeout_hours'),
            'timeout_only_if_losing':    ('RISK_MANAGEMENT', 'timeout_only_if_losing'),
            'max_positions':             ('RISK_MANAGEMENT', 'max_positions'),
            
            # MA_EXIT
            'enable_15min_ma_exit':      ('MA_EXIT', 'enable_15min_ma_exit'),
            'ma_breach_threshold':       ('MA_EXIT', 'ma_breach_threshold'),
            'applicable_timeframes':     ('MA_EXIT', 'applicable_timeframes'),
        }
        
        def _has_get_cached(obj) -> bool:
            """get_cached 메서드 존재 여부 확인"""
            return hasattr(obj, "get_cached") and callable(getattr(obj, "get_cached"))
        
        def _raw_get(key: str, fallback: str):
            """get_cached 또는 섹션/키 접근 통합 처리"""
            section, opt = KEY_MAP[key]
            
            # 1순위: get_cached 사용 (ConfigManager)
            if _has_get_cached(config):
                val = config.get_cached(key)
                if val is not None:
                    return val
            
            # 2순위: 표준 ConfigParser 접근
            try:
                return config.get(section, opt, fallback=fallback)
            except Exception:
                return fallback
        
        # ========================================
        # 1. 안전 추출 함수들
        # ========================================
        def safe_float(key: str, default: float, min_val=0.0, max_val=1.0) -> float:
            """안전하게 float 추출 (범위 검증 포함)"""
            raw = _raw_get(key, str(default))
            try:
                val = float(raw)
                if not (min_val <= val <= max_val):
                    logger.warning(
                        f"⚠️ [설정범위] {key}={val} "
                        f"(허용: {min_val}~{max_val}) → 기본값 {default} 사용"
                    )
                    return default
                return val
            except Exception as e:
                logger.warning(
                    f"⚠️ [설정오류] {key}: {e} → 기본값 {default} 사용"
                )
                return default
        
        def safe_int(key: str, default: int, min_val=1, max_val=100) -> int:
            """안전하게 int 추출 (범위 검증 포함)"""
            raw = _raw_get(key, str(default))
            try:
                val = int(float(raw))
                if not (min_val <= val <= max_val):
                    logger.warning(
                        f"⚠️ [설정범위] {key}={val} "
                        f"(허용: {min_val}~{max_val}) → 기본값 {default} 사용"
                    )
                    return default
                return val
            except Exception as e:
                logger.warning(
                    f"⚠️ [설정오류] {key}: {e} → 기본값 {default} 사용"
                )
                return default
        
        def safe_bool(key: str, default: bool) -> bool:
            """안전하게 bool 추출"""
            raw = _raw_get(key, str(default))
            try:
                if isinstance(raw, bool):
                    return raw
                s = str(raw).strip().lower()
                return s in ("true", "1", "yes", "on")
            except Exception as e:
                logger.warning(
                    f"⚠️ [설정오류] {key}: {e} → 기본값 {default} 사용"
                )
                return default
        
        # ========================================
        # 2. applicable_timeframes 파싱 (정규화)
        # ========================================
        try:
            tf_raw = _raw_get('applicable_timeframes', '1m')
            applicable_timeframes = [
                t.strip().lower() 
                for t in str(tf_raw).split(',') 
                if t.strip()  # 빈 문자열 제거
            ]
            if not applicable_timeframes:
                raise ValueError("empty timeframes")
        except Exception as e:
            logger.warning(
                f"⚠️ [설정오류] applicable_timeframes: {e} → "
                f"기본값 ['1m'] 사용"
            )
            applicable_timeframes = ['1m']
        
        # ========================================
        # 3. 개별 값 로드
        # ========================================
        stop_loss_pct       = safe_float('stop_loss_pct',       0.010, 0.001, 0.10)
        atr_stop_loss_multiplier = safe_float('atr_stop_loss_multiplier', 0.0, 0.0, 5.0)
        take_profit_pct     = safe_float('take_profit_pct',     0.015, 0.001, 0.20)
        trailing_stop_pct   = safe_float('trailing_stop_pct',   0.005, 0.0005, 0.10)
        
        enable_trailing_stop    = safe_bool('enable_trailing_stop', True)
        enable_timeout_exit     = safe_bool('enable_timeout_exit',  True)
        timeout_hours           = safe_float('timeout_hours',       0.5,   0.01, 24.0)
        timeout_only_if_losing  = safe_bool('timeout_only_if_losing', True)
        
        enable_15min_ma_exit    = safe_bool('enable_15min_ma_exit', True)
        ma_breach_threshold     = safe_float('ma_breach_threshold', 0.003, 0.0001, 0.05)
        
        max_positions           = safe_int('max_positions', 3, 1, 10)
        
        # ========================================
        # 4. 상호 제약 검증 (관계식)
        # ========================================
        # 제약 1: 익절 > 손절
        if take_profit_pct <= stop_loss_pct:
            new_val = round(stop_loss_pct * 1.5, 6)
            logger.warning(
                f"⚠️ [관계식] take_profit_pct({take_profit_pct}) ≤ "
                f"stop_loss_pct({stop_loss_pct}) → "
                f"take_profit_pct 재설정: {new_val}"
            )
            take_profit_pct = new_val
        
        # 제약 2: 추적손절 < 익절
        if trailing_stop_pct >= take_profit_pct:
            new_val = round(take_profit_pct * 0.5, 6)
            logger.warning(
                f"⚠️ [관계식] trailing_stop_pct({trailing_stop_pct}) ≥ "
                f"take_profit_pct({take_profit_pct}) → "
                f"trailing_stop_pct 재설정: {new_val}"
            )
            trailing_stop_pct = new_val
        
        # 제약 3: 논리 의존성
        if timeout_only_if_losing and not enable_timeout_exit:
            logger.warning(
                "⚠️ [논리] timeout_only_if_losing=True 이면 "
                "enable_timeout_exit=True가 필요 → "
                "enable_timeout_exit 강제 On"
            )
            enable_timeout_exit = True
        
        # ========================================
        # 5. TradingSettings 생성
        # ========================================
        return TradingSettings(
            stop_loss_pct=stop_loss_pct,
            atr_stop_loss_multiplier=atr_stop_loss_multiplier,
            take_profit_pct=take_profit_pct,
            trailing_stop_pct=trailing_stop_pct,
            
            enable_trailing_stop=enable_trailing_stop,
            
            enable_timeout_exit=enable_timeout_exit,
            timeout_hours=timeout_hours,
            timeout_only_if_losing=timeout_only_if_losing,
            
            enable_15min_ma_exit=enable_15min_ma_exit,
            ma_breach_threshold=ma_breach_threshold,
            
            max_positions=max_positions,
            
            ma_exit_applicable_timeframes=applicable_timeframes
        )

@dataclass
class DataPackage:
    """분석용 데이터 패키지"""
    df: Any
    market: str
    current_price: float
    regime: Optional[MarketRegime] = None
    indicators: Dict = field(default_factory=dict)
    ma_data: Dict = field(default_factory=dict)
    pattern_data: Dict = field(default_factory=dict)
    timeframe: Optional[str] = None

# ========================================
# 유틸리티 함수
# ========================================
def is_valid_df(df) -> bool:
    """DataFrame 유효성 체크"""
    if df is None or df.empty:
        return False
    
    required = ['open', 'high', 'low', 'close', 'volume']
    return all(col in df.columns for col in required)

def calculate_returns(prices: List[float]) -> List[float]:
    """수익률 계산"""
    if len(prices) < 2:
        return []
    
    returns = []
    for i in range(1, len(prices)):
        if prices[i-1] > 0:
            ret = (prices[i] - prices[i-1]) / prices[i-1]
            returns.append(ret)
    
    return returns

def get_tick_size(price: float) -> float:
    """
    업비트 KRW 마켓 실제 호가단위(틱) 반환
    참조: https://docs.upbit.com/docs/market-info-trade-price-detail
    """
    # [TICK-FIX 2026-10-17] 업비트 KRW 2025-07-31 변경 후 호가 단위
    if price >= 1_000_000:
        return 1000.0
    elif price >= 500_000:
        return 500.0
    elif price >= 100_000:
        return 100.0
    elif price >= 50_000:
        return 50.0
    elif price >= 10_000:
        return 10.0
    elif price >= 5_000:
        return 5.0
    elif price >= 1_000:
        return 1.0
    elif price >= 100:
        return 1.0
    elif price >= 10:
        return 0.1
    elif price >= 1:
        return 0.01
    elif price >= 0.1:
        return 0.001
    elif price >= 0.01:
        return 0.0001
    elif price >= 0.001:
        return 0.00001
    elif price >= 0.0001:
        return 0.000001
    elif price >= 0.00001:
        return 0.0000001
    else:
        return 0.00000001


def snap_to_upbit_tick(price: float, direction: str = "round") -> float:
    """
    업비트 KRW 호가단위에 맞춰 가격 스냅(정렬)

    direction:
      "floor" → 내림 (손절가용: 더 넓은 SL 보장)
      "ceil"  → 올림 (익절가용: 더 보수적 TP 보장)
      "round" → 반올림 (기본)

    단순 소수점 반올림과 실제 업비트 호가 정렬의 역할을 분리한다.
    """
    from decimal import Decimal, ROUND_CEILING, ROUND_FLOOR, ROUND_HALF_EVEN
    # [TICK-SNAP-FIX 2026-10-17] float 나눗셈 한 틱 오차 · 6 자리 반올림 제거
    try:
        p = Decimal(str(float(price or 0.0)))
    except Exception:
        raise ValueError(f"snap_to_upbit_tick: 가격 변환 불가 {price!r}")
    if not p.is_finite():
        raise ValueError(f"snap_to_upbit_tick: 비유한 가격 {price!r}")
    
    tick = Decimal(str(get_tick_size(float(p))))
    if tick <= 0:
        return float(p)
    mode = {"floor": ROUND_FLOOR, "ceil": ROUND_CEILING}.get(direction, ROUND_HALF_EVEN)
    q = (p / tick).quantize(Decimal("1e-9"), rounding=ROUND_HALF_EVEN)
    return float(q.to_integral_value(rounding=mode) * tick)

# ========================================
# 🆕 가격 포맷팅 헬퍼 (초저가 SHIB 등 대응)
# ========================================

def format_price_krw(price) -> str:
    """
    업비트 KRW 마켓 가격 표시용 공통 포맷터
    - 1000원 이상 : 정수 (천 단위 구분)
    - 100~1000원  : 소수점 1자리
    - 1~100원     : 소수점 2자리
    - 0.01~1원    : 소수점 4자리
    - 0.0001~0.01원 : 소수점 6자리
    - 그 이하     : 소수점 8자리    
    예시:
    - 50000.0 → "50,000"
    - 123.456 → "123.5"
    - 0.0138 → "0.0138"
    - 0.000667 → "0.000667"
    """
    if price is None:
        return "N/A"

    try:
        p = float(price)
    except (TypeError, ValueError):
        return str(price)

    if p >= 1000:
        return f"{p:,.0f}"
    elif p >= 100:
        return f"{p:.1f}"
    elif p >= 1:
        return f"{p:.2f}"
    elif p >= 0.01:
        return f"{p:.4f}"
    elif p >= 0.0001:
        return f"{p:.6f}"
    elif p > 0:
        return f"{p:.8f}"
    else:
        return "0"

# ========================================
# Float 정밀도 보정 헬퍼
# ========================================

def round_money(value: float, decimals: int = 0) -> float:
    """금액 반올림 (원 단위)
    Args:
        value: 금액
        decimals: 소수점 자리수 (기본 0 = 정수)
    Returns:
        반올림된 금액
    """
    return round(value, decimals)

def round_percent(value: float) -> float:
    """퍼센트 반올림 (소수점 4자리)
    Args:
        value: 퍼센트 값
    Returns:
        반올림된 값
    """
    return round(value, 4)

def safe_compare(a: float, b: float, epsilon: float = 1e-8) -> int:
    """안전한 float 비교
    Args:
        a, b: 비교할 값
        epsilon: 허용 오차
    Returns:
        1: a > b, 0: a == b, -1: a < b
    """
    diff = a - b
    if abs(diff) < epsilon:
        return 0
    return 1 if diff > 0 else -1

def is_positive(value: float, epsilon: float = 1e-8) -> bool:
    """양수 여부 (오차 고려)"""
    return value > epsilon

def is_negative(value: float, epsilon: float = 1e-8) -> bool:
    """음수 여부 (오차 고려)"""
    return value < -epsilon

# **END**