#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# SSoT-META
# FILE: Trading_bot_6_strategy.py
# GENERATED_AT_UTC: 2026-05-27T01:49:36Z
# CONTENT_SHA256: EXTERNAL_MANIFEST_ONLY
# STATUS: ACTIVE
# ============================================================

"""
# NOTE:
# 본 모듈에서 사용하는 모든 파라미터는
# ConfigManager / TradingSettings를 통해 주입된다.
# global_constants 직접 참조는 설계 위반이다. 단, CONFIG 파일과 중복이 없고, 더 복잡해지는 예외 몇개 허용.

========================================
업비트 자동매매 봇 v1.0 - Strategy Module
========================================
책임: 매매 전략, 신호 생성, 포지션 사이징, 손익 계산
- 추세추종 전략
- 횡보장 Range Trading - 통합 신호 분석
- 포지션 사이징 계산
- 손절/익절 계산 (ATR 기반)

제공 인터페이스:
  - generate_signal(): 매매 신호 생성
  - calculate_position_size(): 포지션 크기
  - calculate_stops(): 손절/익절 계산
  
금지사항:
  - 주문 실행 (bot 모듈에서 처리)
  - DB 접근 (각 모듈에서 처리)
  - 성과 기록 (performance 모듈에서 처리)
  - 시장 데이터 수집 (data 모듈에서 처리)
  - 지표 계산 (analysis 모듈에서 처리)
  - MA 분석 (analysis 모듈에서 처리)
  - 가중치 조정 (performance 모듈에서 처리)
  
데이터 흐름:
  Analysis → Strategy (신호 생성) → Bot
  
개선사항:
  - 과도한 필터 제거
  - 전략 선택 로직 단순화
  - 중복 계산 제거
  - 불필요한 로그 제거
========================================
"""

import numpy as np
import pandas as pd
import time
from typing import Dict, Optional, Tuple
from datetime import datetime
from enum import Enum

from Trading_bot_1_core import (
    logger, MarketRegime, TimeFrame,
    TradingSignal, Position, requires_candles, TradingSettings,
    DataPackage, is_valid_df, format_price_krw,
    get_tick_size
)
from Trading_bot_2_config import ConfigManager
from Trading_bot_5_analysis import (
        SENTINEL_FLOAT,
        SoleusGajamiConstants,
        _long_ma_alive_verdict,
    )

# ✅ 엔진 상수: global_constants에서 가져오기
from global_constants import (
    VOLUME_MA_PERIOD,
    DEFAULT_ATR_PERIOD,
    ATR_LEVEL_VERY_LOW,
    ATR_LEVEL_LOW,
    ATR_LEVEL_NORMAL,
    ATR_LEVEL_HIGH,
    ATR_LOW_POSITION_EXPANSION,
    ATR_HIGH_POSITION_REDUCTION,
    REGIME_FILTER_PROFILES,
    STRONG_GOLDEN_COUNT,
    STRONG_ALIGNMENT_LEVEL,
    MIN_ORDER_AMOUNT,
    SHORT_TERM_TIMEFRAMES,
    PENALTY_WEIGHTS,
    PENALTY_FLOOR_STRONG_SIGNAL,
    BULL_RELAX_VOL_FACTOR,
    BULL_RELAX_VOL_MIN,
    BULL_RELAX_THRESHOLD_FACTOR,
    BULL_RELAX_PATTERN_BASE,
    BULL_RELAX_PATTERN_MAX,
    BULL_STRENGTH_BASE,
    BULL_STRENGTH_SYMBOL_MULTIPLIER,
    SYMBOL_BULL_LEVEL_1,
    SYMBOL_BULL_LEVEL_2,
    SYMBOL_BULL_LEVEL_3,
    # 차단 사유 상수
    BLOCK_REASON_CONF_LOW,
    BLOCK_REASON_EEE_LOW,
    BLOCK_REASON_NOT_SOLEUS,
    BLOCK_REASON_NOT_GAJAMI,
    BLOCK_REASON_VOL_LOW,
    BLOCK_REASON_REGIME_BREAKDOWN,
    BLOCK_REASON_TD_GATE,
    BLOCK_REASON_TU_NO_MA,
    BLOCK_REASON_MA_REVERSE,
    BLOCK_REASON_MA5_DIST_BREAK,
    BLOCK_REASON_SELLING_PRESSURE,
    BLOCK_REASON_STRONG_SELLING,
    BLOCK_REASON_ATR_BLOCK,
    BLOCK_REASON_OVER_RALLY,
    BLOCK_REASON_DEAD_BOUNCE,
    BLOCK_REASON_CRASH_GATE,
    BLOCK_REASON_RANGING_WEAK,
    PERCENT_CONVERSION
)

# 🎯 신뢰도 체제별 조정 상수

class ConfidenceAdjustmentConstants:
    """신뢰도 체제별 조정 상수"""
    
    # 🔒 MIN_CONF_SOLEUS 제거 (2026-02-07)
    # - 기존: base = max(base, 0.55) → 92% 동일 점수
    # - 현재: GAJAMI-FLOOR는 self.min_confidence(threshold)로 단일화
    # - 회귀 방지: 상수 자체를 제거하여 참조 원천 차단
    
    # [REMOVED] 가자미근 체제 무관 진입 적용으로 더 이상 사용되지 않음
    # REGIME_FACTOR_TRENDING_UP = 1.00
    # REGIME_FACTOR_RANGING = 0.90
    # REGIME_FACTOR_TRENDING_DOWN = 0.80
    # REGIME_FACTOR_TD_CAP = 0.75
    # LONG_BEAR_PENALTY = 0.90

# 차단 사유 코드 (SSoT - Single Source of Truth)
# 목적: DB 저장명 = 로그 출력명 1:1 매핑
# 원칙: 한글 제거, 코드형 상수로 통일

def safe_extract_scalar(value, default: float = 0.0) -> float:
    """
    pandas Series/numpy array를 안전하게 스칼라로 변환
    Args:
        value: 변환할 값 (float, Series, ndarray, None 등)
        default: 변환 실패 시 반환할 기본값
    Returns:
        float: 스칼라 값
    Examples:
        >>> safe_extract_scalar(pd.Series([1.5, 2.0, 2.5]))
        2.5
        >>> safe_extract_scalar(2.5)
        2.5
        >>> safe_extract_scalar(None)
        0.0
    """
    if value is None:
        return default
    
    # pandas Series
    if hasattr(value, 'iloc'):
        try:
            return float(value.iloc[-1]) if len(value) > 0 else default
        except (IndexError, ValueError, TypeError):
            return default
    
    # numpy scalar
    if hasattr(value, 'item'):
        try:
            return float(value.item())
        except (ValueError, TypeError):
            return default
    
    # 일반 숫자
    try:
        return float(value)
    except (ValueError, TypeError):
        return default

# 하위 호환성 유지
safe_extract_atr = safe_extract_scalar
# 🔒 스칼라 강제 추출 별칭 (가독성용)
S = safe_extract_scalar

def _soleus_ssot(indicators: dict, ma_data: dict = None) -> bool:
    gmeta = ((ma_data or {}).get("gajami_meta") or {})
    return bool(gmeta.get("soleus_zone", False))

# NOTE:
# Strategy에서는 analysis / indicators 값 비교 시
# 반드시 le/ge/lt/gt/between 사용 (None-safe 계약)
def le(a, b):
    """a <= b : a 또는 b가 None이면 False"""
    return (a is not None) and (b is not None) and (a <= b)

def ge(a, b):
    """a >= b : a 또는 b가 None이면 False"""
    return (a is not None) and (b is not None) and (a >= b)

def lt(a, b):
    """a < b : a 또는 b가 None이면 False"""
    return (a is not None) and (b is not None) and (a < b)

def gt(a, b):
    """a > b : a 또는 b가 None이면 False"""
    return (a is not None) and (b is not None) and (a > b)

def between(x, min_val, max_val):
    """범위 체크 (None-safe)"""
    x = nz(x, 0.0)
    return min_val <= x <= max_val

# 🔧 None-safe 산술 연산 헬퍼 (TypeError 방지)
def nz(x, default=0.0):
    """None-to-Zero: None 또는 비숫자를 안전하게 변환"""
    return x if isinstance(x, (int, float)) and x is not None else default

# ============================================================
# 🔒 SENTINEL CONTRACT (SSoT)
# ============================================================
# [핵심 철학]
# - 본 시스템은 숫자 기반 자동매매 시스템이다.
# - Strategy 모듈의 판정 로직은 "숫자"를 전제로 설계된다.
#
# [SENTINEL 정의]
# - 계산 불가 / 미계산 / 비정상 상태는
#   SENTINEL_FLOAT (-9999.0) 으로 통일한다.
# - SENTINEL_FLOAT는 정상 수치 범위와 절대 충돌하지 않는 값이다.
#
# [책임 분리]
# - Analysis 모듈 (Producer):
#   * None / NaN / Inf / 계산 실패 상태를 사전에 정규화한다.
#   * 모든 수치 결과는 숫자(float)로 Strategy에 전달한다.
#   * 비정상 값은 SENTINEL_FLOAT로 변환한다.
#
# - Strategy 모듈 (Consumer):
#   * SENTINEL_FLOAT 기반 판정을 기본 원칙으로 사용한다.
#   * 핵심 지표가 SENTINEL일 경우 즉시 차단(Hard Block)한다.
#   * 보조 지표가 SENTINEL일 경우 감점(Soft Penalty)만 적용한다.
#
# [운영 규칙]
# 1. Strategy 신규 판정 로직은 SENTINEL_FLOAT 기반으로 작성한다.
# 2. 신규 코드에서 None 비교 / None 가드 / None 보정은 작성하지 않는다.
# 3. 기존 None-safe 헬퍼(le/ge/lt/gt/between/nz 등)는
#    이미 안정적으로 동작하므로 제거하지 않는다.
#    (동작하는 코드는 건드리지 않는다 — 운영 원칙)
#
# [로깅 / 출력 규칙]
# - fmt() / pct()는 출력(로그) 전용 함수이다.
# - 판정 로직을 보호하거나 대체하는 용도로 사용하지 않는다.
#
# [허용 예외]
# - 가격, 수량, 체결가, 손익률 등
#   "항상 float 보장"되는 값은 직접 포맷 허용.
#
# [장애 대응 원칙]
# - TypeError / NoneType 오류 발생 시
#   Strategy가 아니라 Analysis 정규화를 먼저 점검한다.
#
# [상태 선언]
# - Analysis: SENTINEL 정규화 완료
# - Strategy: SENTINEL 판정 + Hard Block 적용 완료
# - 본 계약은 코드 수정이 아닌 운영 안정성을 위한 기준이다.
# ============================================================

def fmt(v, fmt_str=".2f", suffix=""):
    """
    출력 전용 포맷터.
    Strategy 판정 로직에서는 절대 사용하지 않는다.    
    Args:
        v: 포맷할 값
        fmt_str: 포맷 문자열
        suffix: 접미사    
    Returns:
        포맷된 문자열 또는 "N/A"
    """
    try:
        return f"{float(v):{fmt_str}}{suffix}"
    except Exception:
        return "N/A"

def pct(v, fmt_str=".2f"):
    """
    None-safe 퍼센트 포맷    
    Args:
        v: 포맷할 값 (None 가능)
        fmt_str: 포맷 문자열 (예: ".2f", "+.2f")    
    Returns:
        퍼센트 포맷 문자열 또는 "N/A"    
    Examples:
        pct(1.234, ".2f") → "1.23%"
        pct(None, ".2f") → "N/A"
        pct(-2.5, "+.1f") → "-2.5%"
    """
    return fmt(v, fmt_str, "%")

def add(a, b):
    """None-safe 덧셈: a + b (None 자동 0 처리)"""
    return nz(a) + nz(b)

def is_sentinel(v) -> bool:
    """
    SENTINEL 값 판정    
    Args:
        v: 체크할 값    
    Returns:
        bool: SENTINEL_FLOAT(-9999.0)인 경우 True
    """
    return v == SENTINEL_FLOAT

class StrategyType(Enum):
    """전략 타입"""
    TREND_FOLLOWING = "trend_following"
    RANGE_TRADING = "range_trading"
    CASH_PRESERVATION = "cash_preservation"
    BALANCED = "balanced"

class RiskCalculator:
    """리스크 계산기"""
    
    def __init__(self, config):
        self.config = config
        self.settings = TradingSettings.from_config(config)
        # 🔒 SSoT: 신뢰도 하한은 config가 최종 판사
        self.min_confidence = float(self.config.get_cached('min_confidence_threshold') or 0.50)
    
    def calculate_stops(self, price: float, atr: float, timeframe: str, 
               strategy_type=None, regime=None, symbol: str = "") -> Tuple[float, float, float]:
        """
        SSoT(config) 기반 손절/익절/추적손절 산출

        규약:
        - 본 함수는 체제별 퍼센트/승수 하드코딩 및 재계산을 하지 않는다.
        - 최종 손절/익절/추적손절은 config_3m.ini에서 로드된 self.settings 값만 사용한다.
        - Strategy는 "계산"을 하지 않고 "선택"만 한다.
        """
        # ========================================
        # 1. ATR 스칼라 변환
        # ========================================
        atr = safe_extract_atr(atr)
        
        # ========================================
        # 2. 최종 SSoT(config) 기반 손절/익절/추적손절 산출
        # ========================================
        # 규약:
        # - 체제별 퍼센트/승수를 하드코딩하거나 재계산하지 않는다.
        # - config(SSoT)에서 로드된 self.settings 값만 사용한다.
        stop_loss_pct = self.settings.stop_loss_pct
        take_profit_pct = self.settings.take_profit_pct
        trailing_pct = self.settings.trailing_stop_pct

        # 🔒 SSoT-META: config 기본 손절과 ATR 손절 하한을 단일 stop_loss_pct로 합성
        # 🔒 atr_stop_loss_multiplier는 config SSoT 값만 사용한다.
        #  현행 숫자를 소스 주석에 중복 기록하지 않는다.
        #  이 함수(calculate_stops)엔 indicators 파라미터가 없어 atr_pct 대신
        #  이미 인자로 들어온 atr(절대값)·price로 직접 비율을 구한다.
        #  MIN_STOP_LOSS_PCT가 MAX_STOP_LOSS_PCT를 넘지 않도록 상한 캡 포함(고ATR 방어).
        #  ★MAX_STOP_LOSS_PCT는 손절폭 결정값이 아니라 비정상 폭주를 막는 극단 안전망이다.
        #    실제 계획 손절폭은 max(stop_loss_pct, ATR×multiplier)와 후단 호가 스냅이 결정한다.
        MAX_STOP_LOSS_PCT = 0.10    # 최대 10% 극단 안전망
        _atr_mult = float(self.settings.atr_stop_loss_multiplier or 0.0)
        _atr_stop_floor = (
            (atr / price) * _atr_mult
            if price > 0.0 and atr > 0.0 and _atr_mult > 0.0
            else 0.0
        )
        MIN_STOP_LOSS_PCT = min(
            MAX_STOP_LOSS_PCT,
            max(stop_loss_pct, _atr_stop_floor),
        )

        original_stop_loss_pct = stop_loss_pct
        
        # 하한 보정 (너무 타이트한 손절 방지)
        if stop_loss_pct < MIN_STOP_LOSS_PCT:
            logger.warning(
                f"⚠️ [StopLoss-Clamp] {symbol if 'symbol' in locals() else ''} 손절 비율 하한 보정 | "
                f"{original_stop_loss_pct*100:.2f}% → {MIN_STOP_LOSS_PCT*100:.2f}% (최소값 보장)"
            )
            stop_loss_pct = MIN_STOP_LOSS_PCT
        
        # 상한 보정 (너무 넓은 손절 방지)
        if stop_loss_pct > MAX_STOP_LOSS_PCT:
            logger.warning(
                f"⚠️ [StopLoss-Clamp] {symbol if 'symbol' in locals() else ''} 손절 비율 상한 보정 | "
                f"{original_stop_loss_pct*100:.2f}% → {MAX_STOP_LOSS_PCT*100:.2f}% (최대값 제한)"
            )
            stop_loss_pct = MAX_STOP_LOSS_PCT

        # 최종 손절/익절가 계산 (퍼센트 SSoT 기준)
        stop_loss = price * (1 - stop_loss_pct)
        take_profit = price * (1 + take_profit_pct)
        trailing_stop = price * trailing_pct

        # ========================================
        # 3. 호가 단위 조정 (실제 업비트 KRW 틱 스냅)
        # ========================================
        from Trading_bot_1_core import snap_to_upbit_tick

        stop_loss = snap_to_upbit_tick(stop_loss, "floor")
        take_profit = snap_to_upbit_tick(take_profit, "ceil")

        # 🔒 PREVENT-2: tick 조정 후에도 config/ATR 최소 손절폭이 실제 가격으로 보장
        min_stop_price = price * (1 - MIN_STOP_LOSS_PCT)
        if stop_loss > min_stop_price:
            stop_loss = snap_to_upbit_tick(min_stop_price * (1 - 0.002), "floor")
            logger.warning(
                f"⚠️ [PREVENT-StopLoss-TickClamp] {symbol} | "
                f"tick_adjusted_stop > min_stop | "
                f"min={min_stop_price:.8f} → stop={stop_loss:.8f}"
            )

        # 🔒 [PREVENT-3-RESTORE-0817] Tick-Count Floor. 05-24 원본 소실분 복원.
        # %/ATR 하한과 별개로 호가 양자화 1~2틱 관통을 방지한다.
        MIN_SL_TICKS = 3
        _tick = get_tick_size(price)
        if _tick > 0:
            _sl_ticks = (price - stop_loss) / _tick
            if _sl_ticks < MIN_SL_TICKS:
                stop_loss = snap_to_upbit_tick(
                    price - MIN_SL_TICKS * _tick,
                    "floor",
                )
                logger.warning(
                    f"⚠️ [PREVENT-TickFloor] {symbol} | "
                    f"SL틱={_sl_ticks:.1f} < {MIN_SL_TICKS} → "
                    f"SL={stop_loss:.6f} ({MIN_SL_TICKS}틱 확보)"
                )

        # 🔒 PREVENT-TP: tick 조정 후에도 최소 익절폭이 실제 가격으로 보장 (대칭)
        MIN_TAKE_PROFIT_PCT = getattr(self.settings, "min_take_profit_pct", 0.0)
        if MIN_TAKE_PROFIT_PCT and MIN_TAKE_PROFIT_PCT > 0:
            min_take_price = price * (1 + MIN_TAKE_PROFIT_PCT)
            if take_profit < min_take_price:
                take_profit = snap_to_upbit_tick(min_take_price * (1 + 1e-6), "ceil")
                logger.warning(
                    f"⚠️ [PREVENT-TakeProfit-TickClamp] {symbol} | "
                    f"min_take={min_take_price:.8f} → take={take_profit:.8f}"
                )

        # 🔒 [RR-FINAL-OBS-0817 철거] 최종 RR 판정은 TP-BOOST 이후 RR-FINAL-BLOCK-0817로 이관.

        # 백분율 계산 (로깅용)
        stop_loss_pct_actual = (stop_loss - price) / price
        take_profit_pct_actual = (take_profit - price) / price

        # 🔒 SSoT-META: RiskCalculator는 손절/익절 가격 계산만 수행
        # - 최종 손익비 판정은 _check_buy_conditions()의 RR-FINAL-BLOCK 1곳에서 수행
        # - 여기서는 계산된 값을 반환만 함

        # ========================================
        # 4. 로깅
        # ========================================
        atr_pct = (atr / price * 100) if price > 0 else 0

        regime_str = f" | 체제: {regime.value}" if regime else ""

        _entry_tick_log = get_tick_size(price) if price > 0 else 0.0
        _stop_tick_log = get_tick_size(stop_loss) if stop_loss > 0 else 0.0
        if (
            _entry_tick_log > 0.0
            and abs(_entry_tick_log - _stop_tick_log) < 1e-12
        ):
            _stop_tick_text = f"{(price - stop_loss) / _entry_tick_log:.1f}틱"
        else:
            _stop_tick_text = (
                f"TICK_BAND({_entry_tick_log:g}->{_stop_tick_log:g})"
            )
        logger.info(
            f"[손익계산{regime_str}] "
            f"{symbol if symbol else 'UNKNOWN'} | "
            f"진입: {price:,.4f} | "
            f"ATR: {fmt(atr, '.2f')} ({pct(atr_pct, '.2f')}) | "
            f"ATR-SL={_atr_mult:.2f}x floor={MIN_STOP_LOSS_PCT*100:.2f}% | "
            f"손절: {stop_loss:,.4f} "
            f"({stop_loss_pct_actual*100:.2f}%={_stop_tick_text}) | "
            f"익절: {take_profit:,.4f} ({take_profit_pct_actual*100:.2f}%)"
        )

        return stop_loss, take_profit, trailing_stop
    
    # ========================================
    # 🆕 Confidence → Position Ratio 매핑 (SSoT)
    # ========================================
    def _map_confidence_to_ratio(
        self,
        confidence: float,
        regime=None,
        confidence_threshold: float = None,
    ) -> float:
        """
        신뢰도 → 진입 자격 + 기준 포지션 비율 매핑.
        SSoT 규칙:
          - conf < confidence_threshold : 진입 불가 (0%)
          - conf >= confidence_threshold : 기준 비율 10%
        Note:
          - 진입 하한은 전달된 confidence_threshold, 없으면 self.min_confidence가 SSoT
          - confidence는 진입 자격에만 사용하고 포지션 크기 증폭에는 사용하지 않는다
          - regime 인자는 호출 계약 호환을 위해 유지하며 기준 비율에는 영향하지 않는다
        """
        try:
            conf = float(confidence or 0.0)
        except (TypeError, ValueError):
            conf = 0.0
        try:
            _conf_threshold = float(
                self.min_confidence
                if confidence_threshold is None
                else confidence_threshold
            )
        except (TypeError, ValueError):
            _conf_threshold = float(self.min_confidence)
        # 🔒 [CONF-SIZING-AUTH-PURGE-0904] ★511-R1
        # 12일 완결 346건: confidence↔실현수익 Spearman +0.0180.
        # 예측 순위력이 확인되지 않은 confidence의 베팅 증폭 권한만 회수한다.
        if conf < _conf_threshold:
            return 0.0
        return 0.10
    
    def calculate_position_size(self, balance: float, entry_price: float, confidence: float,
                           risk_metrics: Dict, existing_positions: Dict = None,
                           atr: float = 0, atr_pct: float = 0,
                           regime=None, confidence_threshold: float = None) -> float:
        """
        포지션 크기 계산 (ATR 변동성 반영)
        
        Args:
            balance: 현재 잔고
            entry_price: 진입 예정 가격
            confidence: 신호 신뢰도 (0~1)
            risk_metrics: 리스크 메트릭
            existing_positions: 기존 포지션들
            atr: ATR 값 (선택)
            atr_pct: ATR 비율 (%) (선택)
            regime: 시장 체제 (호출 계약 호환용)
        
        Returns:
            포지션 크기 (원)
        """
        # ========================================
        # 1. confidence threshold → 진입 자격 / base ratio 10%
        # ========================================
        try:
            conf = float(confidence or 0.0)
        except (TypeError, ValueError):
            conf = 0.0
        try:
            _position_conf_threshold = float(
                self.min_confidence
                if confidence_threshold is None
                else confidence_threshold
            )
        except (TypeError, ValueError):
            _position_conf_threshold = float(self.min_confidence)
        
        base_ratio = self._map_confidence_to_ratio(
            conf,
            regime=regime,
            confidence_threshold=_position_conf_threshold,
        )
        
        # 진입 불가 (conf < threshold)
        if base_ratio <= 0:
            logger.debug(
                f"❌ [PositionSize] conf={conf:.3f} "
                f"< {_position_conf_threshold:.3f} → 진입 불가"
            )
            return 0.0
        
        # 기본 포지션 크기: confidence threshold 통과 시 base 10%.
        # 이후 리스크/가용잔고 제한은 축소만 담당한다.
        position_size = balance * base_ratio
        
        # ========================================
        # 2. 리스크 상한(CAP) - 축소만 허용, 확대 금지
        # ========================================
        risk_cap = 1.0
        cap_reasons = []
        
        # 2-1. 연속 손실 / 드로다운
        consecutive_losses = risk_metrics.get('consecutive_losses', 0)
        current_drawdown = abs(risk_metrics.get('current_drawdown', 0))
        
        if consecutive_losses >= 5 or current_drawdown > 0.15:
            risk_cap = min(risk_cap, 0.5)
            cap_reasons.append(f"연속손실{consecutive_losses}회/DD{current_drawdown*100:.1f}%→50%")
        elif consecutive_losses >= 3 or current_drawdown > 0.08:
            risk_cap = min(risk_cap, 0.7)
            cap_reasons.append(f"연속손실{consecutive_losses}회/DD{current_drawdown*100:.1f}%→70%")
        
        # 2-2. ATR 과다 시 상한 축소
        if gt(atr_pct, ATR_LEVEL_HIGH):
            risk_cap = min(risk_cap, 0.6)
            cap_reasons.append(f"ATR{atr_pct:.1f}%→60%")
        elif gt(atr_pct, ATR_LEVEL_NORMAL):
            risk_cap = min(risk_cap, 0.8)
            cap_reasons.append(f"ATR{atr_pct:.1f}%→80%")
        
        # 리스크 캡 적용
        position_size *= risk_cap
        
        # ========================================
        # 3. 최소/최대 한계 적용
        # ========================================
        # 🔒 confidence threshold 통과 시 최소 10% (risk_cap 축소 반영)
        min_size = balance * 0.10 * risk_cap
        # 🔒 max_position_size 상한: config 기준
        max_size = balance * float(self.config.get_cached('max_position_size') or 0.20)
        
        # 하한 보장 (confidence threshold 통과 후보는 최소 10%)
        if position_size < min_size:
            position_size = min_size
        
        # 상한 제한
        if position_size > max_size:
            position_size = max_size
        
        # ========================================
        # 4. 가용 잔고 체크
        # ========================================
        if existing_positions:
            total_positioned = sum(p.volume * p.avg_price for p in existing_positions.values())
            available = balance - total_positioned
            
            if position_size > available * 0.95:
                position_size = max(MIN_ORDER_AMOUNT, available * 0.95)
                cap_reasons.append(f"가용잔고{available:,.0f}원")
        
        # 최소 주문금액 보장
        position_size = max(position_size, MIN_ORDER_AMOUNT)
        
        # ========================================
        # 5. 로깅
        # ========================================
        cap_str = " | ".join(cap_reasons) if cap_reasons else "없음"
        logger.info(
            f"📊 [PositionSize] conf={conf:.2f} → 비율={base_ratio*100:.0f}% | "
            f"잔고={balance:,.0f} | 리스크캡={risk_cap:.0%} ({cap_str}) | "
            f"최종={position_size:,.0f}원 ({position_size/balance*100:.1f}%)"
        )
        
        return position_size
    
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# CrossState: 골든크로스 생애주기 관리
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
class CrossState:
    """
    MA5>MA10 골든크로스 이벤트 상태 관리
    
    설계 철학:
    1. 크로스는 이벤트 - 한 번 발생하면 기록
    2. birth_price는 크로스 시점 고정, 불변
    3. rise_pct는 고정 birth_price 기준
    4. 종료: 역전/SHOULDER/수명/포지션완전종료
    5. BACKFILL: 스캔 공백 복원 정식 지원
    6. 재생성 방지: 시간 + 인덱스 조합 추적
    7. REJECT 철학: 영구 차단 (재진입 불가)
    
    SSoT: birth_price는 이 클래스만 관리
    
    인덱스 일관성:
    - 실시간: len(df) - 현재 봉 위치
    - BACKFILL: -dur - df 기준 음수 인덱스 (GC 성립 확정봉과 일치, get_cross_candle_index_from_dur SSoT)
    → 둘 다 df.index 기준으로 저장되어야 일치
    """
    
    def __init__(self):
        self.active = False
        self.cross_time = None
        self.birth_price = None
        self.cross_candle_index = None
        self.had_position = False
        self.last_seen_time = None
        self.last_processed_cross_time = None
        self.last_processed_cross_idx = None
        
    def activate(self, candle_time, birth_price, candle_index=None, is_backfill=False):
        """
        크로스 이벤트 활성화
        
        Args:
            candle_time: 크로스 발생 봉 시각
            birth_price: 크로스 시점 가격
            candle_index: 봉 인덱스 (df 기준 일관된 값)
            is_backfill: 백필 활성화 여부
        """
        self.active = True
        self.cross_time = candle_time
        self.birth_price = float(birth_price)
        self.cross_candle_index = candle_index
        self.last_seen_time = datetime.now()
        
        # 🔧 재생성 방지 키는 백필에서만 갱신
        if is_backfill:
            self.last_processed_cross_time = candle_time
            self.last_processed_cross_idx = candle_index
        
    def mark_position_opened(self):
        """포지션 오픈 마킹"""
        self.had_position = True
        
    def deactivate(self):
        """크로스 상태 종료 (부분 리셋)"""
        self.active = False
        self.cross_time = None
        self.birth_price = None
        self.cross_candle_index = None
        
    def reset(self):
        """완전 리셋 (cleanup 또는 포지션 종료 시)"""
        self.active = False
        self.cross_time = None
        self.birth_price = None
        self.cross_candle_index = None
        self.had_position = False
        self.last_seen_time = None
        self.last_processed_cross_time = None
        self.last_processed_cross_idx = None
        
    def get_rise_pct(self, current_price):
        """현재가 기준 상승률"""
        if not self.active or self.birth_price is None or self.birth_price <= 0:
            return 0.0
        return (current_price - self.birth_price) / self.birth_price
    
    def update_seen_time(self):
        """마지막 체크 시각 갱신"""
        self.last_seen_time = datetime.now()
    
    def was_processed(self, cross_candle_time, cross_idx):
        """
        재생성 방지: 시간 + 인덱스 조합 확인
        
        Args:
            cross_candle_time: 확인할 크로스 시각
            cross_idx: 확인할 크로스 인덱스
            
        Returns:
            bool: 이미 처리되었으면 True
        """
        return (
            self.last_processed_cross_time is not None and
            self.last_processed_cross_time == cross_candle_time and
            self.last_processed_cross_idx is not None and
            self.last_processed_cross_idx == cross_idx
        )
    
    def mark_processed(self, cross_candle_time, cross_idx):
        """
        처리 완료 기록 (REJECT 시 사용)
        
        Args:
            cross_candle_time: 처리한 크로스 시각
            cross_idx: 처리한 크로스 인덱스
        """
        self.last_processed_cross_time = cross_candle_time
        self.last_processed_cross_idx = cross_idx
       
class SignalAnalyzer:
    """신호 분석기"""

    def __init__(self, config: ConfigManager, timeframe: TimeFrame, ma_cross_tracker=None):
        """신호 분석기 초기화
        
        Args:
            config: 설정 관리자
            timeframe: 타임프레임
            ma_cross_tracker: MA 정배열/골든크로스 추적 딕셔너리 (선택)
        """
        self.config = config
        self.timeframe = timeframe
        
        # 🆕 MA 정배열/골든크로스 추적기 (2025-11-25)
        # Strategy에서 전달받아 공유, None이면 빈 dict로 초기화
        self.ma_cross_tracker = ma_cross_tracker or {}
        
        logger.debug(
            "[SignalAnalyzer] ma_cross_tracker 초기화 | symbols: %d | keys: %s",
            len(self.ma_cross_tracker),
            list(self.ma_cross_tracker.keys())[:3] if self.ma_cross_tracker else []
        )
        
        # 전략 설정
        # min_confidence는 진입 필터 단계에서만 결정 (여기서는 캐시만 보관)
        self.min_confidence = config.get_cached('min_confidence_threshold')
        self.use_regime_filter = config.get_cached('use_regime_filter') or True
        self.enable_regime_adaptive = config.get_cached('enable_regime_adaptive') or True
        self.rg_max_ma20_gap_pct = config.getfloat(
            'STRATEGY',
            'rg_max_ma20_gap_pct',
            fallback=0.0,
        )
        
        # ATR 필터 설정
        self.atr_filter_enabled = config.getboolean('ATR_SETTINGS', 'enable_atr_entry_filter', fallback=True)
        filter_levels_str = config.get('ATR_SETTINGS', 'atr_filter_levels', fallback='VERY_LOW,EXTREME')
        self.atr_filter_levels = [level.strip() for level in filter_levels_str.split(',')]
        self.min_atr_percent = config.getfloat(
            'MARKET_SELECTION',
            'min_atr_percent',
            fallback=0.15
        )
        self.tu_min_atr_percent = config.getfloat(
            'ATR_SETTINGS',
            'tu_min_atr_percent',
            fallback=0.50,
        )    
        self.rg_min_atr_percent = config.getfloat(
            'ATR_SETTINGS',
            'rg_min_atr_percent',
            fallback=0.30,
        )    
        
        # ========================================
        # 긴급청산 설정 (2025-10-18 조정)
        # ========================================
        # 변경 이력:
        # - 기존: -12/-8/-5 (너무 느슨함, -40% 손실 발생)
        # - Phase 1: -6/-4/-3 (50% 강화, 보수적 조정)
        # - Phase 2: -4/-3/-2.5 (ATR 제한 후 재조정 예정)
        # config 형식: "임계:비율,임계:비율,..." (예: "-10:1.0,-7:0.7,-5:0.5")
        emergency_exits_str = self.config.get(
            'RISK_MANAGEMENT', 
            'emergency_exits',
            fallback='-10:1.0,-7:0.7,-5:0.5'  # 기본값
        )
        
        # 파싱: "임계:비율" → {임계(float): 비율(float)}
        emergency_exits = {}
        try:
            for item in emergency_exits_str.split(','):
                threshold_str, ratio_str = item.strip().split(':')
                threshold = float(threshold_str)
                ratio = float(ratio_str)
                emergency_exits[threshold] = ratio
            
            logger.info(
                "[긴급청산] 설정 로드 완료: %s",
                emergency_exits_str
            )
        except (ValueError, AttributeError) as e:
            # 파싱 실패 시 기본값 사용
            logger.warning(
                "[긴급청산] 설정 파싱 실패: %s | 오류: %s | 기본값 사용: -10:1.0,-7:0.7,-5:0.5",
                emergency_exits_str, e
            )
            emergency_exits = {-10.0: 1.0, -7.0: 0.7, -5.0: 0.5}
        
        self.signal_config = {'emergency_exits': emergency_exits}
        
        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        # 🆕 CrossState 맵 및 GAJAMI_LIFECYCLE Config
        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        
        # CrossState 맵 초기화
        self.cross_state_map = {}
        self._dead_wave_gc_id: Dict[str, int] = {}
        
        # 🔒 PATCH-2: STALE 복구 감지용 (종목별 이전 STALE 상태)
        self._prev_stale = {}
        
        # 🆕 포지션 종료 후 1회성 BACKFILL 확장 플래그
        # {symbol: bars}  (예: {"KRW-SOL": 6})
        self._recovery_backfill_symbols = {}
        
        # 🔧 개선 2: regime 타입 체크용 (종목별)
        self._regime_checked_symbols = set()
        
        # GAJAMI_LIFECYCLE Config 로드 (SSoT)
        self.gajami_config = {
            'shoulder_max': float(
                config.get('GAJAMI_LIFECYCLE', 'shoulder_max_pct', fallback='0.015')
            ),
            'stale_multiplier': float(
                config.get('GAJAMI_LIFECYCLE', 'stale_threshold_multiplier', fallback='2.0')
            ),
            'max_backfill_bars': int(
                config.get('GAJAMI_LIFECYCLE', 'max_backfill_bars', fallback='4')
            ),
            'max_lifecycle_bars': int(
                config.get('GAJAMI_LIFECYCLE', 'max_lifecycle_bars', fallback='8')
            ),
            'reentry_block_bars': int(
                config.get('GAJAMI_LIFECYCLE', 'reentry_block_bars', fallback='9')
            ),
            'backfill_allowed_regimes': set(
                r.strip() for r in 
                config.get('GAJAMI_LIFECYCLE', 'backfill_allowed_regimes', 
                          fallback='TRENDING_UP,RANGING').split(',')
                if r.strip()
            )
        }
        
        logger.info(
            f"[GAJAMI-CONFIG] "
            f"SHOULDER_MAX={self.gajami_config['shoulder_max']:.2%} | "
            f"MAX_BACKFILL={self.gajami_config['max_backfill_bars']}봉 | "
            f"MAX_LIFECYCLE={self.gajami_config['max_lifecycle_bars']}봉 | "
            f"BACKFILL_REGIMES={self.gajami_config['backfill_allowed_regimes']}"
        )
        
        # 거래 설정 추가
        from Trading_bot_1_core import TradingSettings
        
        # 🔒 SSoT: Override 로직 상수 (클래스 레벨 정의)
        # Override는 Analysis 비후보를 "예외적으로" 허용하는 로직
        # 가자미근 철학 반영: 저점+0.3~2.0% 구간 골든크로스 진입
        self.OVERRIDE_MIN_VOLUME = 0.30      # 최소 거래량 배율
        self.STRONG_GOLDEN_MIN = 2           # 강한 골든크로스 기준
        self.SOFT_PENALTY_CAP = 0.75         # SoftPenalty 최소 보장선 (SSoT)
        self.settings = TradingSettings.from_config(config)
        
        # MA15 Exit 설정 (config.ini 기반)
        self.enable_15min_ma_exit = config.getboolean(
            "MA_EXIT", "enable_15min_ma_exit", fallback=True
        )
        
        # 적용 타임프레임 (쉼표 구분 문자열)
        self.ma_exit_timeframes = [
            tf.strip()
            for tf in config.get(
                "MA_EXIT", "applicable_timeframes", fallback="3m"
            ).split(",")
        ]
        
        # MA15 이탈 임계값 (%)
        self.ma15_breach_threshold = config.getfloat(
            "MA_EXIT", "breach_threshold", fallback=0.0
        )
        
        logger.info(
            "[SignalAnalyzer] MA15 이탈 설정 | "
            f"활성화: {self.enable_15min_ma_exit} | "
            f"적용 TF: {self.ma_exit_timeframes} | "
            f"임계값: {self.ma15_breach_threshold}%"
        )
        
        # 리스크 계산기
        self.risk_calculator = RiskCalculator(config)
        
        # ========================================
        # 🔥 가중치 설정 (config에서 로드!) - 핵심 수정!
        # ========================================
        # 문제: performance.get_adjusted_weights()가 동적 조정하지만
        #       기본 가중치가 config에서 로드 안 됨!
        # 해결: 여기서 config 로드 후 performance에 전달
        
        self.base_weights = {
            'ma_signal': config.getfloat('SIGNAL_WEIGHTS', 'ma_signal_weight', fallback=0.35),
            'volume_signal': config.getfloat('SIGNAL_WEIGHTS', 'volume_signal_weight', fallback=0.30),
            'atr_signal': config.getfloat('SIGNAL_WEIGHTS', 'atr_signal_weight', fallback=0.20),
            'adx_signal': config.getfloat('SIGNAL_WEIGHTS', 'adx_signal_weight', fallback=0.10),
            # bw_signal: 60봉 BW 압축 백분위(진입 타이밍) 축.
            # 2026-08-01 개명 — 옛 이름 candle_signal_weight는 캔들 형상 신호를 뜻해 오해를 유발했다.
            'bw_signal': config.getfloat('SIGNAL_WEIGHTS', 'bw_signal_weight', fallback=0.05)
        }
        
        # 가중치 합계 체크 (정규화)
        total_weight = sum(self.base_weights.values())
        if abs(total_weight - 1.0) > 0.01:  # 1% 이상 차이
            logger.warning(
                "⚠️ 가중치 합계가 1.0이 아닙니다: %.3f → 자동 정규화합니다",
                total_weight
            )
            # 정규화
            for key in self.base_weights:
                self.base_weights[key] /= total_weight
        
        logger.info(
            "[SignalAnalyzer] 기본 가중치 로드: MA=%.2f, Volume=%.2f, ATR=%.2f, ADX=%.2f, BW=%.2f",
            self.base_weights['ma_signal'],
            self.base_weights['volume_signal'],
            self.base_weights['atr_signal'],
            self.base_weights['adx_signal'],
            self.base_weights['bw_signal']
        )
        
        # 가중치 캐시
        self._cached_weights = {}
        self._weight_cache_time = None
        
        # 마지막 거부 이유 (디버깅용)
        self.last_rejection_reason = ""
               
        # 🆕 매수 신호 신뢰도 추적 (매도 시 재사용)
        # - symbol -> entry_confidence 매핑
        # - 매도 리포트에 진입 품질 정확히 반영
        self.last_entry_confidence = {}
        
        # ========================================
        # 🆕 부분 매도 설정 로드
        # ========================================
        # 1. 부분 매도 활성화 여부
        self.enable_partial_sell = config.getboolean(
            'PARTIAL_SELL', 'enable_partial_sell', fallback=True
        )
        
        # 2. 체제별 동적 조정 여부
        self.enable_regime_adaptive_sell = config.getboolean(
            'PARTIAL_SELL', 'enable_regime_adaptive_sell', fallback=True
        )
        
        # 3. 체제별 익절 트리거 로드 (config.ini [TAKE_PROFIT] — SSoT)
        _TP = 'TAKE_PROFIT'
        self.regime_tp_triggers = {
            'TRENDING_UP': [
                config.getfloat(_TP, 'tp1_tu', fallback=0.015),
                config.getfloat(_TP, 'tp2_tu', fallback=0.030),
            ],
            'RANGING': [
                config.getfloat(_TP, 'tp1_rg', fallback=0.010),
                config.getfloat(_TP, 'tp2_rg', fallback=0.020),
            ],
            'TRENDING_DOWN': [
                config.getfloat(_TP, 'tp1_td', fallback=0.012),
                config.getfloat(_TP, 'tp2_td', fallback=0.025),
            ],
        }
        self.regime_trailing_pcts = {
            'TRENDING_UP':   config.getfloat(_TP, 'trailing_tu', fallback=0.008),
            'RANGING':       config.getfloat(_TP, 'trailing_rg', fallback=0.006),
            'TRENDING_DOWN': config.getfloat(_TP, 'trailing_td', fallback=0.007),
        }
        # 호환성: base_triggers는 TU 기본값 (로그용)
        self.base_triggers = self.regime_tp_triggers['TRENDING_UP']

        logger.debug(
            "[부분익절] 트리거 로드(config.ini) → TU:%s RG:%s TD:%s",
            ', '.join([f'{x*100:.1f}%' for x in self.regime_tp_triggers['TRENDING_UP']]),
            ', '.join([f'{x*100:.1f}%' for x in self.regime_tp_triggers['RANGING']]),
            ', '.join([f'{x*100:.1f}%' for x in self.regime_tp_triggers['TRENDING_DOWN']])
        )
        
        # 4. 매도 비율 로드 (공통 config.ini)
        self.sell_ratios = [
            config.getfloat('PARTIAL_SELL', 'partial_sell_ratio_1st', fallback=0.40),
            config.getfloat('PARTIAL_SELL', 'partial_sell_ratio_2nd', fallback=0.30),
            config.getfloat('PARTIAL_SELL', 'partial_sell_ratio_3rd', fallback=0.30)
        ]
        
        # 5. 초기화 로그
        logger.info(
            "[부분매도] 초기화 완료 | 활성: %s | 타임프레임: %s | 기본트리거: %s | 비율: %s",
            self.enable_partial_sell, timeframe.value,
            ', '.join([f'{x*100:.1f}%' for x in self.base_triggers]),
            ', '.join([f'{x*100:.0f}%' for x in self.sell_ratios])
        )
        
        # 🆕 Shoulder Zone을 위한 변수 초기화 (2025-12-09)
        
        # 타임프레임 초 단위 변환
        tf_map = {
            '1m': 60,
            '3m': 180,
            '5m': 300,
            '15m': 900,
            '1h': 3600,
            '4h': 14400,
            '1d': 86400
        }
        self.tf_seconds = tf_map.get(timeframe.value, 180)  # 기본값: 3분
        
        # Shoulder Zone 쿨다운 추적 딕셔너리
        self.recent_context = {}
        
        logger.info(
            "[ShoulderZone] 초기화 완료 | 타임프레임: %s | tf_seconds: %d초",
            timeframe.value, self.tf_seconds
        )
        
        logger.info("신호 분석기 초기화 완료 (타임프레임: %s)", timeframe.value)
    
    def _block(self, reason: str, details: str = "") -> None:
        """
        진입 차단 사유 기록 (단일 반환 구조)        
        Args:
            reason: 차단 사유 상수 (BLOCK_REASON_*)
            details: 상세 정보 (선택, 수치 등)        
        Returns:
            None: 항상 None 반환 (호출 측에서 return None 처리)        
        Examples:
            >>> self._block(BLOCK_REASON_NOT_SOLEUS); return None
            >>> self._block(BLOCK_REASON_CONF_LOW, "0.38<0.40"); return None
        """
        if details:
            self.last_rejection_reason = f"{reason}({details})"
        else:
            self.last_rejection_reason = reason
        return None
    
    def _gajami_core_ok(
        self,
        symbol: str,
        indicators: dict,
        data_package,
        current_price: float,
        dur_5x10: int,
    ) -> tuple:
        gmeta = (data_package.ma_data or {}).get("gajami_meta") or {}
        glc = gmeta.get("gajami_lifecycle") or {}
        state = str(glc.get("state", "DEAD") or "DEAD")

        try:
            dur = int(glc.get("dur")) if glc.get("dur", None) is not None else int(dur_5x10)
        except Exception:
            dur = 999

        birth_state = bool(indicators.get("gajami_birth_state_ok", False))

        # 🆕 [핀셋#1-Strategy SSoT] Analysis trend_alive 선취득
        # dur>5 차단보다 먼저 읽어야 dur=6~12 추세생존봉이 미아가 되지 않음
        # dur>12는 late momentum 보호 유지
        _trend_alive_core = bool(int(indicators.get("gajami_trend_alive", 0) or 0))

        # 🔒 확정봉 5x10 GC 필수 안전망 — 현재봉 RT GC / birth 우회 차단
        _closed_gc_ok = bool(
            int(
                indicators.get(
                    "closed_gc_5x10_ok",
                    (data_package.ma_data or {}).get("closed_gc_5x10_ok", 0)
                ) or 0
            ) == 1
        )
        # 🔒 [RT-JANGDAE-REMOVE-0817] 확정봉 5x10 GC 필수 규약의 예외 진입로 제거.
        if not _closed_gc_ok:
            # 🔒 [CLOSED-GC-FAIL-DECISION-VAR-0830] ★453 — ★452 자기정정
            # ★452가 찍은 `dur`은 gajami_lifecycle의 dur이며 CLOSED-GC 판정변수가 아니다.
            # 실제 CLOSED-GC 판정축은 Analysis가 export한 dur_5x10_bars다.
            #   dur5x10=None → GC 없음/계산 불가
            #   dur5x10>4    → 3m GC 신선도 초과
            # lcdur는 lifecycle 진단용 보조값으로만 함께 남긴다.
            # 판정 변화 0 · 계산 변화 0 · 사유 관측값만 실제 판정축과 정합화.
            return False, (
                f"CLOSED_GC5x10_FAIL("
                f"dur5x10={indicators.get('dur_5x10_bars', None)},"
                f"lcdur={dur})"
            )

        # 🔒 [GC-ORIGIN-COMMON-PHYSICS-0829] ★416
        # 확정 GC의 존재뿐 아니라 GC-origin 진위도 모든 판사 공통 계약이다.
        # CONSTITUTION_FAIL은 J2/J3/J4 중 하나만 PASS해도 중립화되는데,
        # 그 안에 J1 고유 사유(SHAPE)와 공통 GC-origin 사유(fake_late_reset_gc)
        # 가 섞여 있어 판사 중립화가 물리까지 함께 뚫었다(실측: 08-29
        # CORE-JUDGE-NEUTRAL 266건 중 fake_reset=1 10건 — J3 9건·J2 1건·
        # J4 1건=BTC 02:22). t5 L7984가 이미 생산한 SSoT를 그대로 옮겨
        # 소비만 한다 — 신규 지표·신규 임계값 없음.
        _fake_late_reset_gc = bool(
            int(indicators.get("gajami_fake_late_reset_gc", 0) or 0)
        )
        if _fake_late_reset_gc:
            return False, "FAKE_LATE_RESET_GC_PHYSICS"

        # 0️⃣ Analysis 최종 헌법 판정을 Strategy Core가 그대로 소비한다.
        # ACTIVE BIRTH에서 slow-MA 판사만 Analysis가 격하한 effective 값을 사용하고,
        # Strategy는 raw constitution을 다시 재판정하지 않는다.
        _constitution_effective_ok = bool(
            gmeta.get("gajami_constitution_effective_ok", False)
        )
        if not _constitution_effective_ok:
            # 🔒 [J1-GATE-JUDGE-NEUTRAL-0826]
            # Constitution은 J1 가자미근 헌법이다.
            # J1 판정 자체는 그대로 유지하되,
            # J2/J3/J4의 독립 PASS까지 Strategy Core가 다시 죽이지 않는다.
            _const_j234_pass = bool(
                int(indicators.get("judge2_osc_pass", 0) or 0)
                or int(indicators.get("judge3_cycle_pass", 0) or 0)
                or int(indicators.get("judge4_reversal_pass", 0) or 0)
            )
            if not _const_j234_pass:
                return False, "CONSTITUTION_FAIL"
            logger.info(
                f"🟢 [CORE-JUDGE-NEUTRAL] {symbol} "
                f"reason=CONSTITUTION_FAIL → "
                f"J2/J3/J4 판결권 유지 | "
                f"J2={int(indicators.get('judge2_osc_pass', 0) or 0)} "
                f"J3={int(indicators.get('judge3_cycle_pass', 0) or 0)} "
                f"J4={int(indicators.get('judge4_reversal_pass', 0) or 0)}"
            )

        # 🔒 [MA20-ALIVE-OR-RECOVERING-CONSUME-0902] ★486-R2
        # Analysis의 확정 MA20 상승생존/음수악화 SSoT를 Strategy Core가 그대로 소비한다.
        # 🔒 [J3-EXHAUSTION-MA20-LANE-SPLIT-0905] ★515-R1
        # EXH_TURN은 MA20이 아직 후행 하락 중인 DOWN_LEG 소진→fresh GC 전환을 맡는 J3 전용 lane이다.
        # 이 lane까지 MA20 회복을 다시 요구하면 추세전환 판사가 자기 전제 때문에 영구 deadlock 된다.
        # J1·J2·J4·기존 J3 CONT의 MA20 계약은 그대로 두고 EXH_TURN만 전역권을 회수한다.
        _core_regime = getattr(data_package, "regime", None)
        _ma20_not_decel_raw = bool(
            int(indicators.get("gajami_ma20_not_decel", 1) or 0)
        )
        _j3_exhaustion_turn_relief = bool(
            int(indicators.get("judge3_cycle_pass", 0) or 0)
            and int(indicators.get("judge3_exhaustion_turn_pass", 0) or 0)
        )
        _td_rg_ma20_not_decel_ok = bool(
            _core_regime not in (MarketRegime.TRENDING_DOWN, MarketRegime.RANGING)
            or _ma20_not_decel_raw
            or _j3_exhaustion_turn_relief
        )
        if not _td_rg_ma20_not_decel_ok:
            return False, "TD_RG_MA20_NEG_WORSEN"
        if (
            _core_regime in (MarketRegime.TRENDING_DOWN, MarketRegime.RANGING)
            and not _ma20_not_decel_raw
            and _j3_exhaustion_turn_relief
        ):
            logger.info(
                f"🟢 [CORE-J3-MA20-NEUTRAL] {symbol} "
                f"reason=TD_RG_MA20_NEG_WORSEN → EXH_TURN 판결권 유지 | "
                f"J3={int(indicators.get('judge3_cycle_pass', 0) or 0)} "
                f"exh={int(indicators.get('judge3_exhaustion_turn_pass', 0) or 0)}"
            )

        # 🔒 [J1-LIFECYCLE-JUDGE-NEUTRAL-0906] ★527
        # lifecycle은 가자미근/J1 상태법이다. 위의 closed-GC·fake-reset 같은
        # 공통 물리는 유지하고 J2/J3/J4 독립 PASS만 STATE_ 전역차단에서 분리한다.
        if state != "VALID":
            if state == "ANALYSIS_INVALID_ERROR":
                return False, f"STATE_{state}"
            _state_j234_pass = bool(
                int(indicators.get("judge2_osc_pass", 0) or 0)
                or int(indicators.get("judge3_cycle_pass", 0) or 0)
                or int(indicators.get("judge4_reversal_pass", 0) or 0)
            )
            if not _state_j234_pass:
                return False, f"STATE_{state}"
            logger.info(
                f"🟢 [CORE-JUDGE-NEUTRAL] {symbol} "
                f"reason=STATE_{state} → J2/J3/J4 판결권 유지 | "
                f"J2={int(indicators.get('judge2_osc_pass', 0) or 0)} "
                f"J3={int(indicators.get('judge3_cycle_pass', 0) or 0)} "
                f"J4={int(indicators.get('judge4_reversal_pass', 0) or 0)}"
            )

        # ② TOO_LATE dur 차단
        _late_trend_alive_exempt = bool(_trend_alive_core and 5 < dur <= 12)
        _late_trend_alive_exempt = False
        indicators["gajami_core_late_trend_alive_exempt"] = int(_late_trend_alive_exempt)

        if (
            dur > 5
            and not _late_trend_alive_exempt
        ):
            # 🔒 [J1-GATE-JUDGE-NEUTRAL-0826]
            # J1의 GC freshness 법률이 J2/J3/J4 독립 판결까지
            # 전역 BLOCK하지 않도록 권한만 중립화한다.
            _late_j234_pass = bool(
                int(indicators.get("judge2_osc_pass", 0) or 0)
                or int(indicators.get("judge3_cycle_pass", 0) or 0)
                or int(indicators.get("judge4_reversal_pass", 0) or 0)
            )
            if not _late_j234_pass:
                return False, f"TOO_LATE_DUR({dur})"
            logger.info(
                f"🟢 [CORE-JUDGE-NEUTRAL] {symbol} "
                f"reason=TOO_LATE_DUR({dur}) → "
                f"J2/J3/J4 판결권 유지 | "
                f"J2={int(indicators.get('judge2_osc_pass', 0) or 0)} "
                f"J3={int(indicators.get('judge3_cycle_pass', 0) or 0)} "
                f"J4={int(indicators.get('judge4_reversal_pass', 0) or 0)}"
            )

        # 🔒 [CORE-J1-MA5-LANE-SPLIT-0828]
        # 아래 MA5_NOT_UP / MA5_NOT_LEADING은 J1의 세부 법률이다.
        # MA5 음수(MA5_DEAD)는 공통 방향성 안전축으로 그대로 유지한다.
        _core_j234_ma5_pass = bool(
            int(indicators.get("judge2_osc_pass", 0) or 0)
            or int(indicators.get("judge3_cycle_pass", 0) or 0)
            or int(indicators.get("judge4_reversal_pass", 0) or 0)
        )

        # ③ MA5 판사1 — 기존 강한 head는 유지, newborn 얕은 dip만 별도 lane
        ma5_s = float(indicators.get("ma5_slope", 0.0) or 0.0)
        ma10_s = float(indicators.get("ma10_slope", 0.0) or 0.0)
        ma5_min_ratio = float(getattr(SoleusGajamiConstants, "MA5_SLOPE_DEFAULT", 0.00015))
        ma5_min_pct = ma5_min_ratio * 100.0
        _ma5_alive_floor_pct = (
            -SoleusGajamiConstants.MA5_SLOPE_ALIVE_DIP_RATIO * 100.0
        )
        # 공통 물리(MA5_DEAD)는 어떤 판사도 우회하지 않는다.
        # J1 세부법률만 아래에서 J2/J3/J4와 lane을 분리한다.

        if ma5_s < _ma5_alive_floor_pct:
            return False, (
                f"MA5_DEAD(s5={ma5_s:+.5f}%"
                f"<floor={_ma5_alive_floor_pct:+.5f}%)"
            )

        if ma5_s <= ma5_min_pct:
            if not _core_j234_ma5_pass:
                return False, (
                    f"MA5_NOT_UP(s5={ma5_s:+.5f}%,"
                    f"min={ma5_min_pct:.5f}%)"
                )
            logger.info(
                f"🟢 [CORE-JUDGE-NEUTRAL] {symbol} "
                f"reason=MA5_NOT_UP(s5={ma5_s:+.5f}%,min={ma5_min_pct:.5f}%) → "
                f"J2/J3/J4 판결권 유지 | "
                f"J2={int(indicators.get('judge2_osc_pass', 0) or 0)} "
                f"J3={int(indicators.get('judge3_cycle_pass', 0) or 0)} "
                f"J4={int(indicators.get('judge4_reversal_pass', 0) or 0)}"
            )

        # ④ MA5가 MA10을 리드하는가
        lead_ok = bool(indicators.get("shape_v2_lead_ok", ma5_s > ma10_s))
        _both_positive = ma5_s > 0 and ma10_s > 0

        # 🆕 [핀셋#1-Strategy] Analysis SSoT read — 성숙 추세생존 lead veto 면제
        indicators["gajami_core_trend_alive_lead_exempt"] = int(
            (not lead_ok) and _trend_alive_core
        )

        if not lead_ok and not (
            birth_state
            and dur <= SoleusGajamiConstants.NEWBORN_MAX_DUR
            and _both_positive
        ) and not _trend_alive_core:
            if not _core_j234_ma5_pass:
                return False, f"MA5_NOT_LEADING(s5={ma5_s:+.5f},s10={ma10_s:+.5f})"
            logger.info(
                f"🟢 [CORE-JUDGE-NEUTRAL] {symbol} "
                f"reason=MA5_NOT_LEADING(s5={ma5_s:+.5f},s10={ma10_s:+.5f}) → "
                f"J2/J3/J4 판결권 유지 | "
                f"J2={int(indicators.get('judge2_osc_pass', 0) or 0)} "
                f"J3={int(indicators.get('judge3_cycle_pass', 0) or 0)} "
                f"J4={int(indicators.get('judge4_reversal_pass', 0) or 0)}"
            )

        # ⑦ 역가자미근 차단
        # 🔒 [2026-08-02] 맨 float() → safe_extract_scalar 통일.
        #  이 파일 다른 소비처는 전부 safe_extract_scalar 를 쓰는데 여기만 예외였다.
        #  비스칼라(Series/ndarray) 유입 시 TypeError 위험이 있어 타입 정합 차원에서 통일한다.
        #  (WLD 08-02 11:12:18·13:18:08 게이트 우회 진입의 직접 원인이라는 인과는 미확정 —
        #   이 함수를 감싸는 except Exception이 return None으로 종료되는 것을 확인했다.
        #   해당 우회는 별도 재확인 대상.)
        ma5_s_p1 = float(
            safe_extract_scalar(indicators.get("ma5_slope_prev1", 0.0), 0.0)
        )
        mas = (data_package.ma_data or {}).get("current_mas") or {}
        ma5_now = float(
            safe_extract_scalar(mas.get("ma5", 0.0), 0.0)
        )
        if ma5_s_p1 < -ma5_min_pct and ma5_now > 0 and current_price < ma5_now:
            return False, f"REVERSE_GAJAMI(s5_p1={ma5_s_p1:+.5f}%,px<ma5)"

        return True, "PASS"

    def _apply_soft_penalty(self, penalty_factor: float, mult: float,
                           penalty_reasons: list, reason: str) -> float:
        """
        SoftPenalty 적용 + CAP 보정
        
        Args:
            penalty_factor: 현재 페널티 팩터
            mult: 곱셈 계수 (예: 0.85)
            penalty_reasons: 페널티 사유 리스트
            reason: 추가할 사유
            
        Returns:
            보정된 페널티 팩터 (최소 0.75 보장)
        """
        # SOFT_PENALTY_CAP은 클래스 상수 (self.SOFT_PENALTY_CAP)
        penalty_factor *= mult
        penalty_reasons.append(reason)
        return max(penalty_factor, self.SOFT_PENALTY_CAP)

    async def generate_signal(self, data_package, position: Optional[Position], weights: Dict[str, float],
                              performance, risk_metrics: Dict) -> Optional[TradingSignal]:
        """통합 신호 생성
        
        Args:
            data_package: 시장 데이터 패키지
            position: 현재 포지션 (None이면 진입 신호 체크)
            weights: 신호 가중치
            performance: 성과 관리자
            risk_metrics: 리스크 메트릭
        
        Returns:
            TradingSignal 또는 None
        """
        try:
            symbol = data_package.market
            
            # 매수 신호 체크 (포지션 없을 때)
            if not position:
                # RANGING 체제 보조 신호 (지지선 근처 반등) - ❌ 우회 경로 제거(안전 우선)
                # - 문제: _check_buy_conditions() 우회 → MA 5>10>20 체크 미실행
                # - 해결: 모든 진입이 _check_buy_conditions() 필수 통과하도록 비활성화
                # regime = data_package.regime
                # df = data_package.df
                #     range_signal = self._generate_range_signal(df, symbol, data_package.current_price)
                #         # 진입 신뢰도 저장 (매도 시 사용)
                #         self.last_entry_confidence[symbol] = range_signal.confidence
                
                # 🔒 [WARMUP 철거-0817] 시간이 데이터 품질을 대신할 수 없다.
                
                signal = await self._check_buy_conditions(
                    data_package, weights, performance, risk_metrics
                )
                
                if signal is None:
                    # 🆕 거부 이유는 이미 _check_buy_conditions에서 설정됨
                    pass
                
                return signal
            
            # 매도 신호 체크 (포지션 있을 때)
            else:
                signal = await self._check_sell_conditions(
                    position, data_package.market, data_package.current_price, 
                    data_package.regime, data_package.ma_data, data_package.df,
                    data_package.indicators  # ← indicators 추가
                )
                
                if signal is None:
                    self.last_rejection_reason = "매도 조건 미충족"
                else:
                    # 🔒 재진입 방어: 손실성 청산 시 블록 타임 기록
                    _rsn = signal.reason or ""
                    _is_loss_exit = (
                        _rsn.startswith("손절")
                        or _rsn.startswith("체제전환")
                        or _rsn.startswith("MA15-Exit")
                        or _rsn.startswith("긴급청산")
                    )
                    if _is_loss_exit:
                        _block_bars = self.gajami_config.get('reentry_block_bars', 9)
                        _ctx = self.recent_context.setdefault(symbol, {})
                        _ctx["reentry_block_until"] = time.time() + _block_bars * self.tf_seconds
                        _ctx["reentry_block_reason"] = _rsn
                        logger.info(
                            f"🔒 [REENTRY-BLOCK-SET] {symbol} | "
                            f"사유: {_rsn} | 블록: {_block_bars}봉={_block_bars*self.tf_seconds//60:.0f}분"
                        )

                return signal
        
        except OSError as e:
            # WinError 233: stdout 파이프 닫힘 (무시)
            if '233' in str(e) and '파이프' in str(e):
                self.last_rejection_reason = "시스템 에러 (무시)"
                return None
            else:
                logger.warning(f"[신호분석] {data_package.market} OSError: {e}")
                self.last_rejection_reason = f"OSError: {str(e)}"
                return None
        
        except Exception as e:
            error_msg = str(e)
            # WinError 233도 체크
            if '233' in error_msg and '파이프' in error_msg:
                self.last_rejection_reason = "시스템 에러 (무시)"
                return None
            
            logger.error(f"[신호분석] {data_package.market} 매수 조건 체크 오류: {e}")
            import traceback
            logger.debug(f"상세 오류:\n{traceback.format_exc()}")
            
            self.last_rejection_reason = f"오류 발생: {str(e)}"
    
    def _get_timeout_hours_for_timeframe(self) -> float:
        """
        타임아웃 시간 반환 - SSoT 원칙 준수        
        우선순위:
        1. config_*.ini [RISK_MANAGEMENT].timeout_hours (SSoT 단일 경로)
        2. fallback: 4.0시간 (안전망)        
        철학:
        - timeout은 ConfigManager cached_values에서만 조회
        - [EXIT] 레거시 경로 완전 제거
        - TIMEFRAME_CONFIGS 의존 제거        
        Returns:
            float: 타임아웃 시간 (시간 단위)        
        Examples:
            # config_3m.ini
            [RISK_MANAGEMENT]
            timeout_hours = 0.33  # 20분 (3m 전용)
        """
        # ✅ SSoT 단일화: ConfigManager.cached_values만 사용
        return self.config.get_cached('timeout_hours', 4.0)
    
    def _generate_range_signal(self, df, market: str, current_price: float) -> Optional[TradingSignal]:
        """(SSoT) Strategy 재계산 금지: Range 신호는 Analysis에서만 생성/전달."""
        return None
    
    def _find_support_line(self, df: pd.DataFrame, symbol: str, lookback: int = 30, 
                          min_touches: int = 2) -> tuple:
        """(SSoT) Strategy 재계산 금지: 지지선 탐지는 Analysis에서 수행."""
        return None, 0
    
    def _support_retest_pass(
        self, 
        df: pd.DataFrame, 
        support: float, 
        volume_ratio: float,
        regime: MarketRegime, 
        *,
        tol_pct: float = 0.02,
        margin_pct: float = 0.003,
        min_vol_ranging: float = 1.2,
        min_vol_trending: float = 1.5
    ) -> bool:
        """(SSoT) Strategy 재계산 금지: 재테스트 판정은 Analysis에서 수행."""
        return True
    
    def _find_resistance_line(self, df: pd.DataFrame, symbol: str, lookback: int = 30, 
                              min_touches: int = 2) -> tuple:
        """(SSoT) Strategy 재계산 금지: 저항선 탐지는 Analysis에서 수행."""
        return None, 0
    
    def _check_ma_signals(self, indicators: Dict, ma_analysis: Dict = None, regime: MarketRegime = None) -> float:
        """이동평균 신호 체크 - Analysis Producer 결과 소비
        
        SSoT:
        - MA 신호값은 Analysis의 ma_analysis['signal']만 사용한다.
        - Strategy는 MA 배열 계단값을 새로 만들지 않는다.
        - Analysis 신호가 없거나 SENTINEL이면 0.0, 그 외에는 Producer 값을 그대로 반환한다.
        Args:
            indicators: 기술적 지표 딕셔너리
            ma_analysis: Analysis 모듈의 MA 분석 결과 (골든크로스 정보 포함)
        """
        try:
            # ========================================
            # ✅ 0단계: Analysis 모듈 결과 우선 반영 (신규!)
            # ========================================
            
            # 🆕 디버깅: ma_analysis 내용 확인
            if ma_analysis:
                ma_signal = safe_extract_scalar(ma_analysis.get('signal', 0.0), 0.0)
                return 0.0 if is_sentinel(ma_signal) else ma_signal
            return 0.0
        
        except Exception as e:
            logger.debug(f"MA 신호 체크 오류: {e}")
            return 0.0
     
    def _calculate_final_confidence(self, components: Dict[str, float], weights: Dict[str, float],
                                    regime: MarketRegime, performance) -> float:
        """최종 신뢰도 계산        
        Args:
            components: 신호 컴포넌트 {신호타입: 값}
            weights: 신호 가중치
            regime: 시장 체제
            performance: 성과 관리자
        Returns:
            최종 신뢰도 (0.0 ~ 1.0)
        """
        # ✅ 정석: 입력 검증 - dict만 허용
        if not isinstance(components, dict) or len(components) == 0:
            return 0.0
        
        try:
            # ========================================
            # 🔥 1. 가중치 보정 (config 키와 일치시키기)
            # ========================================
            # weights가 비어있으면 base_weights 사용
            if not weights:
                weights = self.base_weights
                # config weights 없으면 base_weights 사용 (정상 경로, 로그 불필요)
                # logger.debug("ℹ️ [가중치폴백] config weights 없음 → base_weights 사용")
            
            # 키 정규화 (하위 호환성)
            normalized_weights = {}
            for key, value in weights.items():
                # '_weight' 제거: 'ma_signal_weight' → 'ma_signal'
                if key.endswith('_weight'):
                    clean_key = key.replace('_weight', '')
                    normalized_weights[clean_key] = value
                else:
                    clean_key = key
                    # '_signal'이 없으면 추가 (예: 'ma' → 'ma_signal')
                    if not clean_key.endswith('_signal'):
                        clean_key = f"{clean_key}_signal"
                    normalized_weights[clean_key] = value
            
            # ========================================
            # 🆕 1-1. 누락된 가중치 보완 (base_weights 폴백)
            # ========================================
            for signal_type in components.keys():
                if signal_type not in normalized_weights:
                    fallback_weight = self.base_weights.get(signal_type, 0.0)
                    if fallback_weight > 0:
                        normalized_weights[signal_type] = fallback_weight
                        logger.debug(
                            f"🔧 [가중치보완] {signal_type}에 base_weights 적용: {fallback_weight:.3f}"
                        )
            
            # ========================================
            # 🆕 시장 체제별 가중치 동적 조정 (Config 기반)
            # ========================================
            # 개선 이유:
            # - 기존: 체제별 하드코딩 → config_3m.ini 값 완전히 무시
            # - 변경: config 값을 베이스로 체제별 미세 배수 조정
            # - 효과: 타임프레임별 최적화(MA=0.18) 보존 + 체제별 적응
            # 
            # Config 3분봉 기준:
            # - MA=0.18, Volume=0.34, Candle=0.38, ATR=0.06, ADX=0.04
            # 
            # 철학: "무릎과 발목 사이에서 사서 어깨와 입 사이에서 판다"
            # - 직접 증폭은 금지하고 체제별 위험 축만 감액한다.
            # - RANGING/BREAKDOWN: Volume 감액 → 나머지 축 상대비중 상승
            # - TRENDING_DOWN: Volume 감액 → MA/ADX/Candle 등 상대비중 상승
            # - TRENDING_UP/BREAKOUT: Candle 감액 → MA/Volume 등 상대비중 상승
            # ========================================
            
            # Step 1: Config 기본값 로드 (1160-1170줄에서 이미 처리됨)
            base_ma = normalized_weights.get('ma_signal', 0.18)
            base_vol = normalized_weights.get('volume_signal', 0.34)
            base_atr = normalized_weights.get('atr_signal', 0.06)
            base_adx = normalized_weights.get('adx_signal', 0.04)
            base_bw = normalized_weights.get('bw_signal', 0.38)
            
            # Step 2: 체제별 미세 조정 배수 결정
            if regime in [MarketRegime.RANGING, MarketRegime.BREAKDOWN]:
                # 📉 횡보/붕괴: Volume 직접 10% 감액
                ma_multiplier = 1.00
                vol_multiplier = 0.90
                bw_multiplier = 1.00
                regime_label = "횡보/붕괴"

            elif regime == MarketRegime.TRENDING_DOWN:
                # 📉 하락: Volume 직접 25% 감액
                ma_multiplier = 1.00
                vol_multiplier = 0.75
                bw_multiplier = 1.00
                regime_label = "하락장"

            elif regime in [MarketRegime.TRENDING_UP, MarketRegime.BREAKOUT]:
                # 📈 상승/돌파: BW 직접 5% 감액
                ma_multiplier = 1.00
                vol_multiplier = 1.00
                bw_multiplier = 0.95
                regime_label = "상승/돌파"

            else:
                # ⚡ 중립: config 그대로
                ma_multiplier = 1.00
                vol_multiplier = 1.00
                bw_multiplier = 1.00
                regime_label = "중립"
            
            # ATR, ADX는 체제 무관 (리스크 필터 역할)
            atr_multiplier = 1.00
            adx_multiplier = 1.00
            
            # Step 3: Config × 배수 적용 (체제별 위험 축 직접 감액)
            # 감액 후 합계 1.00으로 재정규화하여 상대비중만 조정
            normalized_weights['ma_signal'] = min(base_ma * ma_multiplier, base_ma)
            normalized_weights['volume_signal'] = min(base_vol * vol_multiplier, base_vol)
            normalized_weights['bw_signal'] = min(base_bw * bw_multiplier, base_bw)
            normalized_weights['atr_signal'] = min(base_atr * atr_multiplier, base_atr)
            normalized_weights['adx_signal'] = min(base_adx * adx_multiplier, base_adx)
            
            # 🔒 [2026-08-08 SSoT-P1/P2] 초저가 MA/VOL 재가공 제거.
            # 가격별 신호 보정이 필요하면 Analysis Producer에서 산출한다.
            
            # Step 4: 정규화 (합계 1.00 보장)
            total = sum(normalized_weights.values())
            if total > 0:
                for key in normalized_weights.keys():
                    normalized_weights[key] /= total
            else:
                logger.error("❌ 가중치 합계가 0입니다! config 확인 필요!")
                return 0.0
            
            # Step 5: 로그 출력
            logger.debug(
                f"📊 [가중치-{regime_label}] "
                f"MA={normalized_weights['ma_signal']:.3f} "
                f"(cfg:{base_ma:.2f}×{ma_multiplier:.2f}), "
                f"Vol={normalized_weights['volume_signal']:.3f} "
                f"(cfg:{base_vol:.2f}×{vol_multiplier:.2f}), "
                f"BW={normalized_weights['bw_signal']:.3f} "
                f"(cfg:{base_bw:.2f}×{bw_multiplier:.2f}), "
                f"ATR={normalized_weights['atr_signal']:.3f}, "
                f"ADX={normalized_weights['adx_signal']:.3f}"
            )
                        
            # 🆕 가중치 합계 확인만 수행 (정규화 없음)
            weight_sum = sum(normalized_weights.values())
            if weight_sum == 0:
                logger.error("❌ 모든 가중치가 0입니다! config 설정 확인 필요!")
                return 0.0
            
            # 🧭 MA 방향성 필터 + 가중치 축소 (2025-11-09)
            # 목적: MA 후행성 완화, 역배열 진입 차단 강화
            # 구조:
            #  1. MA 가중치 40% 축소 (0.18 → 0.108)
            #     → Candle/Volume 비중 상대적 증가
            #  2. 역배열(ma_signal < 0) 시 20% 추가 페널티
            #     → 역배열 진입 더욱 제한
            #  3. MA 기울기 보너스 반영
            #     → analysis 모듈에서 계산된 보너스 적용
            # 효과:
            #  - MA 후행성 영향 축소
            #  - 역배열 진입 차단 강화
            #  - 무릎 구간 진입 성공률 향상
            
            # 📍 Step 1: MA 신호와 기울기 보너스 가져오기
            ma_signal = safe_extract_scalar(components.get('ma_signal', 0.0), 0.0)
            ma_slope_bonus = 0.0
            
            # ============================================================
            # 🔒 [2026-08-08 SSoT-P1/P2/P3] ADX signal 재생산 금지.
            # Producer 값이 없으면 후단 components.get('adx_signal', 0.0)이
            # 신호 없음(0.0)으로 소비한다.
            # ============================================================
            
            # analysis 모듈에서 계산된 ma_slope_bonus 확인
            # (calculate_ma_analysis의 result['ma_slope_bonus'])
            if 'ma_slope_bonus' in components:
                ma_slope_bonus = safe_extract_scalar(components.get('ma_slope_bonus', 0.0), 0.0)
            
            # 📍 Step 2: 방향성 페널티 계산
            ma_direction_penalty = 1.0
            if ma_signal < 0:
                # 역배열: 20% 페널티
                ma_direction_penalty = 0.80
                logger.debug(
                    f"⚠️ [MA방향] 역배열 감지 | "
                    f"ma_signal={fmt(ma_signal, '.2f')} | "
                    f"방향 페널티 20% 적용"
                )
            
            # 📍 Step 3: MA 방향성 페널티 (강제 축소 제거)
            # - CONFIG(SSoT)에서 이미 MA/Volume 비중 조정
            # - 방향성 페널티만 적용 (역배열일 때)
            # - 가중치 변경 후 반드시 재정규화 (합계 1.0 유지)
            original_ma_weight = float(normalized_weights.get('ma_signal', 0.0) or 0.0)
            
            # ❌ 강제 40% 축소 제거
            # reduced_ma_weight = original_ma_weight * 0.60
            
            # ✅ 방향성 페널티만 적용
            final_ma_weight = original_ma_weight * ma_direction_penalty
            
            # 가중치 업데이트
            normalized_weights['ma_signal'] = final_ma_weight
            
            # 재정규화 (합계 1.0 유지) - 가중치 변경 후 필수
            _total_w = sum(float(v or 0.0) for v in normalized_weights.values())
            if _total_w > 0:
                for _k in normalized_weights.keys():
                    normalized_weights[_k] = float(normalized_weights[_k] or 0.0) / _total_w
            
            # 🔒 [2026-08-08 SSoT-P1/P2] MA signal은 Producer 원본을 변경하지 않는다.
            if ma_slope_bonus != 0.0 or ma_direction_penalty != 1.0:
                logger.debug(
                    f"👁️ [MA필터-OBSERVE] 원본신호={fmt(ma_signal, '.2f')} | "
                    f"slope_bonus={ma_slope_bonus:+.2f} | "
                    f"가중치: {original_ma_weight:.3f} → {final_ma_weight:.3f} | "
                    f"MA signal은 Producer 원본 유지"
                )
            
            # ========================================
            # 2. Evidence 기반 비선형 스케일링 + 가중 합계 계산
            # ========================================
            # 개선: 선형 → √스케일로 실제 차트 체감 강도 반영
            # - ma 0.50 → 0.71
            # - volume 0.37 → 0.61
            # - adx 0.66 → 0.81
            # ========================================
            
            # 개별 신호 스케일링
            # ✅ 스칼라 계약 강제 (Series 완전 제거)
            ma_raw = safe_extract_scalar(components.get('ma_signal', 0.0), 0.0)
            vol_raw = safe_extract_scalar(components.get('volume_signal', 0.0), 0.0)
            # bw_raw: 실제 신호 = candle_entry_signal(60봉 BW 압축 백분위 변환).
            # 키 이름은 DB signal_components·harness 호환 위해 유지, 가중치 축 이름만 bw_signal.
            bw_raw = safe_extract_scalar(
                components.get('candle_entry_signal', 0.0),
                0.0,
            )
            adx_raw = safe_extract_scalar(components.get('adx_signal', 0.0), 0.0)
            atr_raw = safe_extract_scalar(components.get('atr_signal', 0.0), 0.0)
            
            # 스케일링 제거 → 선형 복원 (20251015 방식)
            ma_s     = nz(ma_raw)
            vol_s    = nz(vol_raw)
            bw_s     = nz(bw_raw)
            adx_s    = nz(adx_raw)
            atr_s    = nz(atr_raw)
            
            # 🆕 가자미근 초입 모드: MA/Candle/ADX floor 0.42
            # floor 제거 (2026-02-15): floor=0.42가 약한 신호를 0.48~0.52에 수렴시켜
            # if _is_early_gajami:  # DISABLED
            #     ma_s = max(ma_s, 0.42)
            #     candle_s = max(candle_s, 0.42)
            #     adx_s = max(adx_s, 0.42)
            
            weighted_sum = (
                ma_s * normalized_weights.get('ma_signal', 0.0) +
                vol_s * normalized_weights.get('volume_signal', 0.0) +
                bw_s * normalized_weights.get('bw_signal', 0.0) +
                adx_s * normalized_weights.get('adx_signal', 0.0) +
                atr_s * normalized_weights.get('atr_signal', 0.0)
            )
            
            # 시장 체제 multiplier 복원 (20251015 방식, BULL_MARKET 추가)
            _regime_mult = {
                MarketRegime.BULL_MARKET:   1.05,
                MarketRegime.TRENDING_UP:   1.03,
                MarketRegime.BREAKOUT:      1.05,
                MarketRegime.RANGING:       1.00,
                MarketRegime.TRENDING_DOWN: 0.92,
                MarketRegime.BREAKDOWN:     0.70,
            }.get(regime, 1.00)
            
            base_confidence = weighted_sum * _regime_mult

            # ========================================
            # 4. 디버그 로그
            # ========================================
            _eee_tag = "Evidence신호"
            logger.debug(
                f"📊 [{_eee_tag}] "
                f"MA:{ma_s:.2f} "
                f"Vol:{vol_s:.2f} "
                f"BW:{bw_s:.2f} "
                f"ADX:{fmt(adx_s, '.2f')} "
                f"ATR:{atr_s:.2f} | "
                f"Base: {base_confidence:.3f}"
            )
            
            # 상한: 0.85 → 0.88 (과열 방지 완화)
            return min(max(base_confidence, 0.0), 0.88)
        
        except Exception as e:
            logger.error(f"신뢰도 계산 오류: {e}")
            return 0.0
    
    def _pick_top_rejection_reason(
        self,
        indicators: dict,
        penalty_reasons: list,
        regime
    ) -> str:
        """
        ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        ✅ 8순위(SSoT): rejection_reason 단일화 (Top-1)
        - 차단/미진입 사유를 1개로 요약
        - Soft 페널티는 차단이 아닌 감점으로 구분
        ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        """
        # 1) Analysis 통과했으나 신뢰도 부족인 경우 최우선
        if indicators.get("soleus_zone_reason") == "ANALYSIS_PASS":
            return "진입 신뢰도 부족"
        
        # 2) Over-Rally(과도 반등) — 차단 제거 (2026-02-17)
        # 
        # 단죄 사유:
        #   ① 판단 부정확: 저점 기준이 가자미근 시작점과 무관, pullback_ratio는 과열 판별 불가
        #   ② 라이프사이클 충돌: BIRTH/ALIVE 정상 성장을 과열로 오판하여 차단
        #   ③ 차단 3위(17.9%): 시스템 진입에 과도한 영향
        # 
        # 보존 사항:
        #   - Analysis의 over_rally_penalty 계산은 유지 (점수 페널티 역할)
        #   - 차단 기능만 제거, 품질 보정 기능은 보존
        # 
        # (차단 로직 삭제됨)
        
        # 3) Soft 페널티 Top-1
        if isinstance(penalty_reasons, list) and penalty_reasons:
            return penalty_reasons[0]
        
        # 4) 체제 보호(정보성)
        if regime is not None:
            regime_value = getattr(regime, 'value', str(regime))
            return f"Regime={regime_value}"
        
        return "UNKNOWN"
    
    def _log_entry_evidence(
        self,
        symbol,
        decision,
        price=None,
        ma5=None,
        ma10=None,
        ma20=None,
        slope5=None,
        volume_ratio=None,
        atr_pct=None,
        regime_name=None,
        tier=None,
        entry_score=None,
        rejection_reason=None,
        final_conf=None,
        threshold=None,
        penalty_factor=None,
        soleus_reason=None,        # ✅ 7순위: SSoT soleus 상태
        penalty_reasons=None       # ✅ 7순위: Top-2 페널티 이유
    ):
        """
        ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        Entry Evidence Snapshot Engine (EEE) - Final v3.0
        - 목적: 진입/차단 순간의 핵심 지표를 1줄로 기록
        - 효과: 1초 안에 원인 진단, ChatGPT 분석 최적화
        ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        """
        # 🔧 포맷팅 헬퍼 함수
        def _fmt_pct(v):
            """퍼센트 포맷 (+/- 기호 포함)"""
            if v is None:
                return "N/A"
            try:
                return f"{float(v):+.2f}%"
            except (ValueError, TypeError, AttributeError):
                return "N/A"
        
        def _fmt_x(v):
            """배수 포맷 (x 단위)"""
            if v is None:
                return "N/A"
            try:
                return f"{float(v):.2f}x"
            except (ValueError, TypeError, AttributeError):
                return "N/A"
        
        def _fmt_num(v):
            """숫자 포맷 (정수형)"""
            if v is None:
                return "N/A"
            try:
                return f"{int(float(v))}"
            except (ValueError, TypeError, AttributeError):
                return "N/A"
        
        def _fmt_score(v):
            """점수 포맷 (소수점 2자리)"""
            if v is None:
                return "N/A"
            try:
                return f"{float(v):.2f}"
            except (ValueError, TypeError, AttributeError):
                return "N/A"
        
        # 🔧 체제/티어 축약
        regime_map = {
            "TRENDING_UP": "TU",
            "TRENDING_DOWN": "TD",
            "RANGING": "R",
            "BREAKOUT": "BO",
            None: "N/A"
        }
        regime_short = regime_map.get(regime_name, regime_name if regime_name else "N/A")
        
        tier_map = {
            "tier1": "T1",
            "tier2": "T2",
            "tier3": "T3",
            None: "N/A"
        }
        tier_short = tier_map.get(tier, tier if tier else "N/A")
        
        # 🔧 reason 축약 (핵심만 추출)
        reason_short = "N/A"
        if rejection_reason:
            # 신뢰도 부족 패턴
            if "최종 신뢰도 부족" in rejection_reason or "신뢰도" in rejection_reason:
                try:
                    # "0.29 < 0.55" 형태 추출
                    import re
                    match = re.search(r'(\d+\.\d+)\s*<\s*(\d+\.\d+)', rejection_reason)
                    if match:
                        reason_short = f"신뢰도{match.group(1)}<{match.group(2)}"
                    else:
                        reason_short = "신뢰도부족"
                except Exception:
                    reason_short = "신뢰도부족"
            
            # 거래량 부족 패턴
            elif "거래량 부족" in rejection_reason or "거래량" in rejection_reason:
                try:
                    import re
                    match = re.search(r'(\d+\.\d+)x?\s*<\s*(\d+\.\d+)x?', rejection_reason)
                    if match:
                        reason_short = f"거래량{match.group(1)}x<{match.group(2)}x"
                    else:
                        reason_short = "거래량부족"
                except Exception:
                    reason_short = "거래량부족"
            
            # 기타 패턴 매칭
            elif "STRONG_SELLING" in rejection_reason or "매도압력" in rejection_reason:
                reason_short = "압력SELL"
            elif "반등 패턴" in rejection_reason:
                reason_short = "패턴X"
            elif "ADX" in rejection_reason:
                reason_short = "ADX↓"
            elif "MA20 하락" in rejection_reason or "MA20" in rejection_reason:
                reason_short = "MA20↓"
            elif "역배열" in rejection_reason:
                reason_short = "역배열"
            elif "체제" in rejection_reason:
                reason_short = "체제불가"
            elif "박스" in rejection_reason:
                reason_short = "박스차단"
            else:
                # 매칭 안되면 원본 축약 (40자 이내)
                reason_short = rejection_reason[:40]
        
        # 🔥 최종 한 줄 출력 (고정 순서)
        symbol_short = symbol.replace("KRW-", "")  # KRW-COMP → COMP
        
        # 🔥 신뢰도 비교 포맷 (핵심!)
        conf_str = "N/A"
        if final_conf is not None and threshold is not None:
            try:
                fc = float(final_conf)
                th = float(threshold)
                if fc >= th:
                    conf_str = f"{fc:.3f}≥{th:.2f}✓"
                else:
                    conf_str = f"{fc:.3f}<{th:.2f}✗"
            except (ValueError, TypeError, AttributeError):
                conf_str = "N/A"
        
        try:
            penalty_str = f"{float(penalty_factor):.3f}" if penalty_factor is not None else "N/A"
        except (ValueError, TypeError, AttributeError):
            penalty_str = "N/A"
        
        # ✅ 7순위(SSoT): soleus, penalty_reasons 추가
        soleus_str = soleus_reason if soleus_reason else "N/A"
        
        # penalty_reasons 요약 (Top-2)
        penalty_reasons_str = "N/A"
        if penalty_reasons and isinstance(penalty_reasons, list) and len(penalty_reasons) > 0:
            penalty_reasons_str = ",".join(penalty_reasons[:2])
        
        logger.info(
            f"🔍 [EEE] {symbol_short} | {decision} | "
            f"final={conf_str} | "
            f"penalty={penalty_str} | "
            f"P={_fmt_num(price)} | "
            f"MA(5/10/20)={_fmt_num(ma5)}/{_fmt_num(ma10)}/{_fmt_num(ma20)} | "
            f"slope5={_fmt_pct(slope5)} | "
            f"vol={_fmt_x(volume_ratio)} | "
            f"ATR={_fmt_pct(atr_pct)} | "
            f"regime={regime_short} | "
            f"tier={tier_short} | "
            f"soleus={soleus_str} | "               # ✅ SSoT soleus
            f"penalties={penalty_reasons_str} | "   # ✅ Top-2 페널티
            f"score={_fmt_score(entry_score)} | "
            f"reason={reason_short}"
        )
    
    def check_fast_entry_with_ma_gap(
        self,
        ma_gap_score: float,
        ma_gap_meta: dict,
        regime,
        atr_pct: float,
        volume_ratio: float,
        adx: float,
        confidence: float
    ) -> tuple:
        """
        MA 간격 기반 FastEntry 조건 체크 (2025-11-12)        
        Returns:
            tuple: (FastEntry 허용 여부, 사유)
        """
        # ⭐ 중요: MarketRegime은 Trading_bot_1_core에서 import!
        from Trading_bot_1_core import MarketRegime
        
        # FastEntry 최적화: MA 간격만 체크, 나머지는 상위 레이어 위임
        # 철학:
        # - FastEntry = 빠른 진입
        # - 여기까지 왔다는 건 상위 조건을 이미 통과했다는 의미
        # - MA 간격이 좋으면 즉시 진입한다
        
        # 1. MA 간격 점수만 체크 (FastEntry의 유일 조건)
        gap_threshold = self.config.get_cached('ma_gap_score_threshold', 0.75)
        
        if ma_gap_score < gap_threshold:
            return False, f"MA 간격 점수 부족 ({ma_gap_score:.2f} < {gap_threshold})"
        
        # 2. 모든 조건 통과 → 즉시 진입
        return True, (
            f"FastEntry 허용 | "
            f"MA간격:{ma_gap_score:.2f} | "
            f"단계:{ma_gap_meta.get('stage', 'N/A')}"
        )

    def get_cross_candle_index_from_dur(self, df, dur_5x10):
        """
        dur_5x10 → 크로스 발생 봉 인덱스 변환 (SSoT)
        
        dur 정의 (Analysis 구현이 SSoT):
        - dur = 1: 현재 RT 봉에서 골든크로스
        - dur = N: 현재 RT 봉을 포함한 N번째 봉에 골든크로스
        
        인덱스 변환: dur = N → idx = -N
        
        Args:
            df: DataFrame
            dur_5x10: MA5>MA10 지속 봉 수
            
        Returns:
            (cross_idx, cross_candle_time, cross_birth_price) 또는 (None, None, None)
        """
        if df is None or len(df) < dur_5x10:
            return None, None, None
        
        cross_idx = -dur_5x10
        
        try:
            cross_candle_time = df.index[cross_idx]
            cross_birth_price = float(df['close'].iloc[cross_idx])
            
            return cross_idx, cross_candle_time, cross_birth_price
            
        except (IndexError, KeyError) as e:
            logger.error(f"[CROSS-INDEX-ERROR] dur={dur_5x10}, idx={cross_idx} | {e}")
            return None, None, None
    
    def register_recovery_backfill(self, symbol: str, bars: int = 6) -> None:
        """포지션 종료 직후 1회성 BACKFILL 확장 등록 (기본 6봉=18분)"""
        try:
            if not hasattr(self, "_recovery_backfill_symbols") or self._recovery_backfill_symbols is None:
                self._recovery_backfill_symbols = {}
            self._recovery_backfill_symbols[symbol] = int(bars)
        except Exception as e:
            logger.warning(f"⚠️ [RECOVERY-BACKFILL-REGISTER] {symbol} 실패: {e}")
    
    def cleanup_cross_states(self, current_symbols):
        """
        CrossState 메모리 관리
        
        제거 조건:
        1. 스캔 대상 아님 → 즉시 제거
        2. 비활성 + 1시간 경과 → 제거
           단, REJECT 영구 차단 기록(last_processed_cross_time 존재)은 유지
        
        Args:
            current_symbols: 현재 스캔 대상 심볼 리스트
        """
        active_symbols = set(current_symbols)
        removed_count = 0
        now = datetime.now()
        
        for symbol in list(self.cross_state_map.keys()):
            state = self.cross_state_map[symbol]
            
            # 1) 스캔 대상이 아니면 즉시 제거
            if symbol not in active_symbols:
                del self.cross_state_map[symbol]
                removed_count += 1
                continue
            
            # 2) 비활성 + 1시간 경과 + (영구 차단 기록 없음) → 제거
            if not state.active:
                # last_seen_time 없으면 스킵
                if state.last_seen_time is None:
                    continue
                
                elapsed = (now - state.last_seen_time).total_seconds()
                if elapsed > 3600:
                    # REJECT 영구 차단 기록이 있으면 유지
                    if state.last_processed_cross_time is None:
                        del self.cross_state_map[symbol]
                        removed_count += 1
        
        if removed_count > 0:
            logger.info(
                f"[CROSS-STATE-CLEANUP] "
                f"제거={removed_count} | "
                f"유지={len(self.cross_state_map)}"
            )
            
    # 🧪 [J2-DIRECT-1031] J2 단독 직행 진입 (config.ini [STRATEGY] j2_direct_enabled)
    # 근거 (실측 10-19 ~ 10-30 · 봇 청산 replay):
    #  - J2 PASS 88 자리 중 실제 매수 2 건 (나머지는 후단 Strategy 게이트 차단)
    #  - J2 PASS 전부 매수 −0.046 %/건 vs 같은 날 실제 봇 매수 −0.240 %/건
    #    (차 +0.220 %p [+0.004, +0.432])
    #  - J2 PASS ∧ GC 없음 +0.026 % (51) vs GC 있음 −0.144 % (37)
    # 범위: 스위치 true ∧ 3m ∧ sim 일 때만. 기본 false = 기존 동작 그대로.
    # 유지: STALE 실주문 차단 · calculate_stops · TP-BOOST · RR_INVERTED
    #       · Bot 실행 가드 전부 (BAR_OPEN_GUARD · 쿨다운 · 최대 포지션 · kill switch 등).
    # 우회: J2 판정 이후 Strategy 진입 게이트 전부 (GC · 가자미 · EEE · RANGING · ATR · CONF 등).
    # 지표 재계산 없음 — Analysis indicators 값만 읽는다 (tools_scan_contract 계약).
    def _j2_direct_enabled(self) -> bool:
        try:
            if not self.config.getboolean(
                "STRATEGY", "j2_direct_enabled", fallback=False
            ):
                return False
            _jd_tf = str(getattr(self.timeframe, "value", self.timeframe) or "")
            _jd_mode = str(
                self.config.get("TRADING", "mode", fallback="") or ""
            ).strip().lower()
            return bool(_jd_tf == "3m" and _jd_mode == "sim")
        except Exception:
            return False

    def _build_j2_direct_signal(self, data_package, indicators, symbol,
                                current_price, regime, data_stale):
        """J2 직행 BUY 신호 생성.

        Returns:
            (handled, signal)
            - handled=False : 직행 불가 (ATR/가격/체제 없음) → 기존 경로 계속
            - handled=True, signal=None : STALE 또는 RR_INVERTED 로 진입 금지
            - handled=True, signal=TradingSignal : 직행 BUY
        """
        try:
            atr_value = safe_extract_atr(indicators.get("atr", 0))
            # 종목 범위 필터 (TIER1 완전제외 · 저가 체결비용) 는 진입 게이트가 아니므로
            # 직행하지 않고 기존 경로로 돌려보낸다 → 기존 경로가 그대로 차단한다.
            _jd_excl = [
                m.strip()
                for m in str(
                    self.config.get("VOLUME", "exclude_tier1", fallback="") or ""
                ).split(",")
                if m.strip()
            ]
            _jd_low_px = self.config.getfloat(
                "GLOBAL_FILTER", "low_price_threshold", fallback=0.0
            )
            _jd_atr_pct = float(safe_extract_scalar(indicators.get("atr_pct", 0.0), 0.0))
            _jd_skip = (
                not np.isfinite(current_price)  # 🔒 [J2-DIRECT-v2.1] NaN · 무한대 가격 (NaN <= 0 은 거짓이라 아래 줄로는 못 거름)
                or not (np.isfinite(_jd_atr_pct) and _jd_atr_pct > 0.0)  # 🔒 [J2-DIRECT-v2.1] atr_pct 없으면 직행 안 함 (거래 기록 0.0 방지)
                or current_price <= 0
                or not np.isfinite(atr_value)
                or atr_value <= 0
                or regime is None
                or str(symbol) in _jd_excl
                or (_jd_low_px > 0.0 and current_price < _jd_low_px)
            )
            if _jd_skip:
                logger.info(
                    f"⚠️ [J2-DIRECT-SKIP] {symbol} "
                    f"price={current_price} atr={atr_value} "
                    f"regime={getattr(regime, 'value', regime)} "
                    f"excl={int(str(symbol) in _jd_excl)} low_px={_jd_low_px:g} → 기존 경로"
                )
                return False, None

            # 기존 [STALE-BLOCK] 과 같은 실주문 차단 (신호 생성 후 반환 금지)
            if data_stale:
                self.last_rejection_reason = "STALE_EXEC_BLOCK"
                logger.warning(f"[STALE-BLOCK] {symbol} J2-DIRECT 실주문 차단")
                return True, None

            # 손절/익절 = 기존 일반 경로와 같은 RiskCalculator SSoT
            risk_calc = RiskCalculator(self.config)
            stop_loss, take_profit, trailing_stop = risk_calc.calculate_stops(
                current_price, atr_value, self.timeframe.value, regime=regime,
                symbol=symbol
            )
            # 🔒 [J2-DIRECT-v2] 비유한 · 방향 역전 손절 / 익절이면 직행하지 않고 기존 경로
            if not (np.isfinite(stop_loss) and np.isfinite(take_profit) and 0.0 < stop_loss < current_price < take_profit):
                logger.info(f"⚠️ [J2-DIRECT-SKIP] {symbol} SL/TP 비정상 SL={stop_loss} price={current_price} TP={take_profit} → 기존 경로")
                return False, None
            # 기존 [TP-BOOST] 와 같은 상대 보정 (alignment_level >= 5 → ×1.35)
            _jd_ma = data_package.ma_data or {}
            if int(_jd_ma.get("alignment_level", 0) or 0) >= 5:
                take_profit = current_price + (take_profit - current_price) * 1.35

            # 기존 [RR-INVERTED-BLOCK] 과 같은 식 (TP ≤ SL → 진입 금지)
            _rr_sl = abs(current_price - stop_loss)
            _rr_tp = abs(take_profit - current_price)
            if _rr_sl > 0.0 and _rr_tp > 0.0 and _rr_tp <= _rr_sl:
                self.last_rejection_reason = "RR_INVERTED"
                logger.info(
                    f"🚫 [RR-INVERTED-BLOCK] {symbol} J2-DIRECT 최종 손익비 역전 → 진입 금지 | "
                    f"SL={_rr_sl:.6g}({(_rr_sl / current_price * 100):.2f}%) "
                    f"TP={_rr_tp:.6g}({(_rr_tp / current_price * 100):.2f}%) | "
                    f"price={current_price:,.4f} tick={get_tick_size(current_price):g}"
                )
                return True, None

            # 포지션 크기 자격: conf = threshold → 기준 10 % (confidence 는 크기 증폭 권한 없음)
            _jd_conf = float(self.min_confidence or 0.50)
            if not (np.isfinite(_jd_conf) and 0.0 < _jd_conf <= 1.0):  # 🔒 [J2-DIRECT-v2.1]
                _jd_conf = 0.50
            _jd_glc = (
                (_jd_ma.get("gajami_meta") or {}).get("gajami_lifecycle") or {}
            )
            _jd_raw_judges = "%d%d%d%d" % (
                int(indicators.get("judge1_gajami_pass", 0) or 0),
                int(indicators.get("judge2_osc_pass", 0) or 0),
                int(indicators.get("judge3_cycle_pass", 0) or 0),
                int(indicators.get("judge4_reversal_pass", 0) or 0),
            )
            _jd_payload = {
                "atr": atr_value,
                "stop_loss": stop_loss,
                "take_profit": take_profit,
                "trailing_stop": trailing_stop,
                "gajami_entry": False,
                "entry_regime": regime.value if regime else None,
                "position_size_ratio": 1.0,
                "entry_stage": "STD",
                "position_confidence_mult": 1.0,
                "entry_confidence": _jd_conf,
                "detect_candle_time": (
                    indicators.get("detect_candle_time", "")
                    or datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                ),
                "gajami_lifecycle": _jd_glc,
                "golden_count": int(indicators.get("golden_count", 0) or 0),
                "rise_pct": float(_jd_glc.get("rise_pct", 0) or 0),
                # 실행 판사표는 J2 단독 (J1/J3/J4 무관). 원판결은 j2_direct_raw_judges 에 보존.
                "judge1_gajami_pass": 0,
                "judge2_osc_pass": 1,
                "judge3_cycle_pass": 0,
                "judge4_reversal_pass": 0,
                "judges": "0100",
                "j2_direct": 1,
                "j2_direct_raw_judges": _jd_raw_judges,
                "judge_lane_authorized": 1,
                # Bot 이 J234 표를 JUDGE 로 정규화하면 원래 값은
                # judge_entry_source_type 에 남는다 (t7 setdefault).
                "entry_type": "J2_DIRECT",
                "judge_entry_source_type": "J2_DIRECT",
                "entry_authorization_route": "J2_DIRECT",
                "effective_conf_threshold": _jd_conf,
                "dur_5x10_bars": int(
                    safe_extract_scalar(indicators.get("dur_5x10_bars", -1), -1)
                ),
            }
            for _jd_key in (
                "prev_candle_close",
                "indicator_shadow_bar",
                "current_candle_time",
                "atr_pct",
                "closed_gc_5x10_relation",
                "cycle_active_structure",
                "rsi14",
                "macd_a_above_zero",
                "judge2_prov_ready",
                "judge2_prov_pass",
                "judge2_prov_verdict",
                "judge2_prov_bar",
                "judge2_prov_hidgo",
            ):
                if _jd_key in indicators:
                    _jd_payload[_jd_key] = indicators[_jd_key]

            _jd_signal = TradingSignal(
                symbol=symbol,
                action="BUY",
                price=current_price,
                confidence=_jd_conf,
                reason=f"{regime.value} J2 직행 진입 (J2-DIRECT)",
                market_regime=regime,
                signal_components={
                    "current_price": current_price,
                    "atr_pct": _jd_atr_pct,
                    "entry_type": "J2_DIRECT",
                    "judges": "0100",
                    "j2_direct": 1,
                },
                indicators=_jd_payload,
            )
            _jd_signal.metadata = {"current_price": current_price}
            self.last_entry_confidence[symbol] = _jd_conf
            self.last_rejection_reason = ""

            logger.info(
                f"🧪 [J2-DIRECT] {symbol} "
                f"bar={indicators.get('indicator_shadow_bar', 'NA')} "
                f"cur={indicators.get('current_candle_time', 'NA')} "
                f"rsi={fmt(safe_extract_scalar(indicators.get('rsi14'), SENTINEL_FLOAT), '.1f')} "
                f"macdA={int(bool(indicators.get('macd_a_above_zero', False)))} "
                f"struct={indicators.get('cycle_active_structure', 'NA')} "
                f"gc={int(indicators.get('closed_gc_5x10_relation', 0) or 0)} "
                f"regime={regime.value} raw_judges={_jd_raw_judges} | "
                f"price={current_price:,.4f} "
                f"SL={(_rr_sl / current_price * 100):.2f}% "
                f"TP={(_rr_tp / current_price * 100):.2f}% "
                f"→ Strategy 진입 게이트 우회 BUY"
            )
            # 기존 로그 파서 (판사 승인 · 최종 결정) 와 같은 형식의 줄을 함께 남긴다.
            logger.info(
                f"🎫 [JUDGE-APPROVAL] {symbol} 찬성 1표 | "
                f"J1=0 J2=1 J3=0 J4=0 | "
                f"entry=J2_DIRECT dur={_jd_payload['dur_5x10_bars']}"
            )
            logger.info(
                f"[STRATEGY-DECISION] {symbol} | "
                f"soleus=0 "
                f"score={_jd_conf:.3f} "
                f"thr={_jd_conf:.3f} "
                f"route=J2_DIRECT "
                f"entry_type=J2_DIRECT "
                f"final=PASS | "
                f"reason=PASS"
            )
            return True, _jd_signal
        except Exception as e:
            # 직행 경로 오류는 기존 경로로 되돌린다 (fail-back, 진입 판단 변경 없음)
            logger.error(f"[J2-DIRECT] {symbol} 직행 신호 생성 오류: {e} → 기존 경로")
            return False, None

    async def _check_buy_conditions(self, data_package, weights: Dict[str, float], performance,
                                    risk_metrics: Dict) -> Optional[TradingSignal]:
        """매수 조건 체크 - 개선 버전 + TD-BASELINE 추가
        
        ╔══════════════════════════════════════════════════════════════════╗
        ║                         진입 로직                                 ║
        ╠══════════════════════════════════════════════════════════════════╣
        ║ 1. 조건 충족 → 즉시 진입 (망설임 제거)                            ║
        ║ 2. 조건 미충족 → 즉시 패스 (기다림 제거)                          ║
        ║ 3. 체제별 필터 적용 (정의된 규칙 기계적 실행)                     ║
        ║ 4. 변동성 범위 체크 (ATR 기반 하드 필터)                          ║
        ║ 5. 거래량/MA 조건 확인 (수치 기반 판단)                           ║
        ╚══════════════════════════════════════════════════════════════════╝
        Args:
            data_package: 시장 데이터 패키지
            weights: 신호 가중치
            performance: 성과 관리자
            risk_metrics: 리스크 메트릭
        Returns:
            TradingSignal 또는 None
        """
        try:
            # 🔒 즉시 안전 초기화 (except 블록 안전성 보장)
            indicators = {}
            penalty_reasons = []
            bonus_reasons = []
            allow_execution = True  # 🆕 기본값 안전 선언
            
            # 🔧 데이터 패키지 초기화 (최우선)
            symbol = data_package.market if data_package else "UNKNOWN"
            current_price = data_package.current_price if data_package else 0
            regime = data_package.regime if data_package else None
            indicators = data_package.indicators or {} if data_package else {}
            _judge_lane_authorized = bool(
                int(indicators.get("judge_lane_authorized", 0) or 0)
            )

            # 🔒 BUY 판정 1회 단위 reason 초기화
            # 이전 종목/이전 루프의 차단 사유가 PASS 로그에 섞이는 것 방지
            self.last_rejection_reason = ""
            self.last_scalp_min = 0

            # 🔒 후보 신호 SSoT: 모든 BUY 후보는 여기서 시작해 SINGLE EXIT까지 유지
            final_signal = None
            entry_type = str(indicators.get("entry_type", "NONE") or "NONE")
            
            # 🆕 STALE 플래그 처리
            meta = indicators.get("_meta", {})
            data_stale = meta.get("data_stale", False)
            
            # 🔒 재진입 방어: 손실성 청산 후 블록 타임 확인
            _ctx_rb = self.recent_context.get(symbol, {})
            _block_until = _ctx_rb.get("reentry_block_until", 0)
            if _block_until:
                if time.time() < _block_until:
                    logger.debug(
                        f"🚫 [REENTRY-BLOCK] {symbol} | "
                        f"사유: {_ctx_rb.get('reentry_block_reason', '?')} | "
                        f"잔여: {(_block_until - time.time()) / 60:.1f}분"
                    )
                    self.last_rejection_reason = "REENTRY_BLOCK"
                    return None
                else:
                    _ctx_rb.pop("reentry_block_until", None)
                    _ctx_rb.pop("reentry_block_reason", None)

            if data_stale:
                allow_execution = False
                
                logger.info(
                    f"[STALE-EVAL] {symbol} "
                    f"| 최신={meta.get('stale_latest_time')} "
                    f"| 지연={meta.get('delay_seconds', 0)/60:.1f}분 "
                    f"→ 신호 생성은 허용, 실주문 차단"
                )
            
            # 🔒 PATCH-2: STALE → 정상 복구 감지 (Strategy 레벨)
            prev_stale = bool(self._prev_stale.get(symbol, False))
            force_backfill_scan = bool(prev_stale and (not data_stale))
            
            # 🔒 PATCH-2E 준비: 복구 1회성 진입 허용 플래그 (초기값)
            # - BACKFILL 복구 성공 시 True로 설정됨
            recovery_grace = False
            
            if force_backfill_scan:
                logger.info(
                    f"🔄 [STALE-RECOVERY] {symbol} STALE→정상 복구 감지 → BACKFILL 강제"
                )
            
            # 상태 저장 (다음 루프 대비)
            self._prev_stale[symbol] = bool(data_stale)
            
            # 🔧 golden_count 전달 누락 수정 (2/9)
            # Analysis result['golden_count']가 indicators에 미전달 → 항상 0이었음
            # 영향: RANGING_WEAK(초입 차단), GAJAMI-LATE(목덜미 미차단) 전부 오작동
            indicators["golden_count"] = int((data_package.ma_data or {}).get("golden_count", 0) or 0)
            
            # ============================================================
            # PATCH A3: Strategy 레벨 체제 보정 (보조 안전장치)
            # - Analysis에서 3분봉 고정으로 안정화된 regime을 신뢰
            # - Strategy는 '명백한 불일치'만 보정 (판정자 아님, 안전요원)
            # ============================================================
            # ❌ 과반수 계산 제거
            # ❌ regime_history 사용 제거
            # ✅ 체제-가격 불일치 검증만 수행
            # ============================================================
            
            # (참고) 기존 PATCH H3는 Analysis PATCH A1로 대체됨
            # === END PATCH A3 ===
            
            df = getattr(data_package, 'df', None)  # 🔒 DataPackage.df SSoT
            
            # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
            # 🆕 골든크로스 감지 (실시간 / BACKFILL 완전 분리)
            # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
            
            # CrossState 가져오기 또는 생성
            if symbol not in self.cross_state_map:
                self.cross_state_map[symbol] = CrossState()
            
            cross_state = self.cross_state_map[symbol]
            cross_state.update_seen_time()
            
            # MA 현재/이전 값
            current_mas = indicators.get('current_mas', {})
            ma5 = current_mas.get('ma5', 0)
            ma10 = current_mas.get('ma10', 0)
            ma5_prev = indicators.get('ma5_prev', 0)
            ma10_prev = indicators.get('ma10_prev', 0)
            
            # dur_5x10 Defensive 처리
            ma_data = data_package.ma_data or {}
            dur_5x10_raw = int(indicators.get("dur_5x10_bars", 0) or 0)
            dur_5x10 = max(dur_5x10_raw, 0)
            # dur_5x10_raw는 indicators["dur_5x10_bars"] 기반 (미전달 시 0 기본값)
            # 🔒 [J2-STRATEGY-INGRESS-INTEGRITY-0902] ★479
            # Strategy가 J2를 처음 소비하기 전에 T5 Producer provenance를 검증한다.
            # 원본 scalar/verdict는 감사용으로 보존하고, 이후 Strategy 전체에는
            # Producer가 증명한 effective J2만 노출한다.
            _j2_ingress_raw = bool(
                int(indicators.get("judge2_osc_pass", 0) or 0)
            )
            _j2_ingress_verdict = str(
                indicators.get("judge2_osc_verdict", "NEUTRAL")
                or "NEUTRAL"
            )
            _j2_ingress_prov_ready = bool(
                int(indicators.get("judge2_prov_ready", 0) or 0)
            )
            _j2_ingress_prov_pass = bool(
                int(indicators.get("judge2_prov_pass", 0) or 0)
            )
            _j2_ingress_prov_verdict = str(
                indicators.get("judge2_prov_verdict", "NEUTRAL")
                or "NEUTRAL"
            )
            _j2_ingress_prov_bar = str(
                indicators.get("judge2_prov_bar", "") or ""
            )
            _j2_ingress_prov_hidgo = bool(
                int(indicators.get("judge2_prov_hidgo", 0) or 0)
            )
            _j2_ingress_shadow_bar = str(
                indicators.get("indicator_shadow_bar", "") or ""
            )
            _j2_ingress_prov_ok = bool(
                _j2_ingress_prov_ready
                and _j2_ingress_raw == _j2_ingress_prov_pass
                and _j2_ingress_verdict == _j2_ingress_prov_verdict
                and _j2_ingress_prov_pass == _j2_ingress_prov_hidgo
                and _j2_ingress_prov_pass
                == (_j2_ingress_prov_verdict == "PASS")
                and _j2_ingress_prov_bar == _j2_ingress_shadow_bar
            )
            _j2_ingress_effective = bool(
                _j2_ingress_raw
                and _j2_ingress_prov_ok
            )
            indicators["judge2_osc_pass_raw"] = int(_j2_ingress_raw)
            indicators["judge2_osc_verdict_raw"] = _j2_ingress_verdict
            indicators["judge2_osc_pass"] = int(_j2_ingress_effective)
            # 🔒 [DIV-CLASS-GATE-1109] 사장님 「① W / M ② W 일 때 기울기로 J2 · J4 · J4-1 · J4-2 · 곧바로 적용」
            # 입구에서 한 번만 교체 → 뒤의 모든 소비처 (헌법 구제 _gajami_core_ok · 생애 구제 · 최종 표 · 7_bot) 가 같은 표를 봄
            # J2 = 기존 effective ∧ W ∧ J2 히든 · J4 = judge4_class_pass (W ∧ J4 일반 · J4-1 과장 · J4-2 트리플) · M · 기타 · 오류 · 분류 없음 = 표 없음
            # 끄기 = config.ini [DIVERGENCE] div_class_gate = false (옛 표 그대로)
            indicators["judge2_osc_pass_old"] = int(_j2_ingress_effective)
            indicators["judge4_reversal_pass_old"] = int(indicators.get("judge4_reversal_pass", 0) or 0)
            indicators["div_class_gate_on"] = 0
            if bool(self.config.getboolean("DIVERGENCE", "div_class_gate", fallback=True)):
                _dcg_shape = str(indicators.get("div_class_shape", ""))
                _dcg_type = str(indicators.get("div_class_type", ""))
                _dcg_j2_new = int(bool(_j2_ingress_effective) and _dcg_shape == "W" and _dcg_type == "J2_HIDDEN")
                _dcg_j4_new = int(int(indicators.get("judge4_class_pass", 0) or 0) == 1)
                indicators["judge2_osc_pass"] = _dcg_j2_new
                indicators["judge4_reversal_pass"] = _dcg_j4_new
                indicators["div_class_gate_on"] = 1
                # J2 직행 (L2619 · 기본 꺼짐) 도 분류 표를 우회하지 못하게 같은 값 (Fable R1333)
                _j2_ingress_effective = bool(_dcg_j2_new)
                if (
                    _dcg_j2_new != indicators["judge2_osc_pass_old"]
                    or _dcg_j4_new != indicators["judge4_reversal_pass_old"]
                ):
                    logger.info(
                        f"🧭 [DIV-CLASS-GATE] {symbol} 표 바뀜 | "
                        f"J2 {indicators['judge2_osc_pass_old']}→{_dcg_j2_new} "
                        f"J4 {indicators['judge4_reversal_pass_old']}→{_dcg_j4_new} | "
                        f"모양={_dcg_shape or 'NA'} 유형={_dcg_type or 'NA'}"
                    )
            if (_j2_ingress_raw or _j2_ingress_prov_pass):
                if _j2_ingress_prov_ok:
                    # 🔒 [J2-INGRESS-POSITIVE-OBSERVE-0902] ★482-R1
                    # ★479의 정상 양성 경로를 Strategy 최초 소비 지점에서 기록한다.
                    # MISMATCH만 기록하면 J2=1이어도 후단 게이트에서 일찍 차단된
                    # 종목은 effective J2가 관측되지 않는다(KRW-ANKR 10:21).
                    # provenance 판정에 이미 존재하는 값과 T5 hidden 구성값만 재사용.
                    # 판정식·threshold·effective 계산은 변경하지 않는다.
                    logger.info(
                        f"🟢 [J2-INGRESS-OK] {symbol} "
                        f"raw={int(_j2_ingress_raw)} "
                        f"eff={int(_j2_ingress_effective)} "
                        f"pready={int(_j2_ingress_prov_ready)} "
                        f"pJ2={int(_j2_ingress_prov_pass)} "
                        f"v2={_j2_ingress_verdict} "
                        f"pv2={_j2_ingress_prov_verdict} "
                        f"phidgo={int(_j2_ingress_prov_hidgo)} "
                        f"rsihid={int(bool(indicators.get('div_rsi_hid', False)))} "
                        f"macdhid={int(bool(indicators.get('div_macd_hid', False)))} "
                        f"dblhid={int(bool(indicators.get('div_double_hidden', False)))} "
                        f"pbar={_j2_ingress_prov_bar or 'NA'} "
                        f"shadow={_j2_ingress_shadow_bar or 'NA'}"
                    )
                else:
                    logger.error(
                        f"🚫 [J2-PROV-MISMATCH-EARLY] {symbol} "
                        f"raw={int(_j2_ingress_raw)} "
                        f"v2={_j2_ingress_verdict} | "
                        f"pready={int(_j2_ingress_prov_ready)} "
                        f"pJ2={int(_j2_ingress_prov_pass)} "
                        f"pv2={_j2_ingress_prov_verdict} "
                        f"phidgo={int(_j2_ingress_prov_hidgo)} "
                        f"pbar={_j2_ingress_prov_bar or 'NA'} "
                        f"shadow={_j2_ingress_shadow_bar or 'NA'} | "
                        f"effective={int(_j2_ingress_effective)} "
                        f"→ Strategy 최초 소비 전 J2 fail-closed"
                    )
            # 🧪 [J2-DIRECT-1031] J2 단독 직행 진입 분기 (기본 off)
            # _j2_ingress_effective = raw J2 ∧ provenance ok
            # = 최종 관문(L10976)의 _j2_ok 와 같은 식이다.
            # 스위치 on ∧ 3m ∧ sim 일 때만 이후 Strategy 진입 게이트를 건너뛴다.
            if (
                _j2_ingress_effective
                and self._j2_direct_enabled()
                # 🔒 [J2-DIRECT-v2] J1 · J3 · J4 도 통과한 봉은 원 판결 · 기존 경로 그대로
                and not any(int(indicators.get(_k, 0) or 0) for _k in ("judge1_gajami_pass", "judge3_cycle_pass", "judge4_reversal_pass"))
            ):
                _jd_handled, _jd_signal = self._build_j2_direct_signal(
                    data_package,
                    indicators,
                    symbol,
                    current_price,
                    regime,
                    data_stale,
                )
                if _jd_handled:
                    return _jd_signal
            # 🔒 [J3-STRATEGY-INGRESS-INTEGRITY-0903] ★501-R3
            # T5 ★489가 봉인한 J3 Producer를 Strategy 최초 소비 전에 검증한다.
            # _gajami_core_ok(L1178·L1221·L1240)가 raw judge3_cycle_pass를 직접
            # 읽고 L2364·L10555 두 곳에서 호출되므로, L10725(Final Jury)보다
            # 먼저 이 지점에서 effective 값으로 덮어써야 두 호출 모두 보호된다.
            # bar축: t5 _cyc_bar 산식(current_candle_time 우선, 없으면
            # indicator_shadow_bar)을 t6에서 동일하게 재계산해 대조한다 —
            # 같은 스캔의 같은 indicators dict이므로 정상 흐름에서는 항상 일치하고,
            # 어긋나면 그것이 곧 낡은/이월된 snapshot 신호다.
            # raw는 감사용으로 보존하고 이후 Core/Jury에는 effective J3만 노출한다.
            _j3_ingress_raw = bool(
                int(indicators.get("judge3_cycle_pass", 0) or 0)
            )
            _j3_ingress_verdict = str(
                indicators.get("judge3_cycle_verdict", "NEUTRAL")
                or "NEUTRAL"
            )
            _j3_ingress_prov_ready = bool(
                int(indicators.get("judge3_prov_ready", 0) or 0)
            )
            _j3_ingress_prov_pass = bool(
                int(indicators.get("judge3_prov_pass", 0) or 0)
            )
            _j3_ingress_prov_verdict = str(
                indicators.get("judge3_prov_verdict", "NEUTRAL")
                or "NEUTRAL"
            )
            _j3_ingress_prov_bar = str(
                indicators.get("judge3_prov_bar", "") or ""
            )
            _j3_ingress_expected_bar = str(
                indicators.get("current_candle_time")
                or indicators.get("indicator_shadow_bar")
                or ""
            )
            _j3_ingress_cur_bar = str(
                indicators.get("current_candle_time", "") or ""
            )
            _j3_ingress_shadow_bar = str(
                indicators.get("indicator_shadow_bar", "") or ""
            )
            _j3_ingress_prov_det = str(
                indicators.get("judge3_prov_det", "") or ""
            )
            _j3_ingress_prov_fake_top = bool(
                int(indicators.get("judge3_prov_fake_top", 0) or 0)
            )
            _j3_ingress_prov_ok = bool(
                _j3_ingress_prov_ready
                and bool(_j3_ingress_prov_bar)
                and bool(_j3_ingress_expected_bar)
                and _j3_ingress_raw == _j3_ingress_prov_pass
                and _j3_ingress_verdict == _j3_ingress_prov_verdict
                and _j3_ingress_prov_pass
                == (_j3_ingress_prov_verdict == "PASS")
                and _j3_ingress_prov_bar == _j3_ingress_expected_bar
            )
            _j3_ingress_effective = bool(
                _j3_ingress_raw
                and _j3_ingress_prov_ok
            )
            indicators["judge3_cycle_pass_raw"] = int(_j3_ingress_raw)
            indicators["judge3_cycle_verdict_raw"] = _j3_ingress_verdict
            indicators["judge3_cycle_pass"] = int(_j3_ingress_effective)
            if (
                not _j3_ingress_prov_ok
                and (
                    _j3_ingress_raw
                    or _j3_ingress_prov_ready
                )
            ):
                logger.error(
                    f"🚫 [J3-PROV-MISMATCH-EARLY] {symbol} "
                    f"raw={int(_j3_ingress_raw)} "
                    f"eff={int(_j3_ingress_effective)} "
                    f"v3={_j3_ingress_verdict} | "
                    f"pready={int(_j3_ingress_prov_ready)} "
                    f"pJ3={int(_j3_ingress_prov_pass)} "
                    f"pv3={_j3_ingress_prov_verdict} "
                    f"pbar={_j3_ingress_prov_bar or 'NA'} "
                    f"expect={_j3_ingress_expected_bar or 'NA'} "
                    f"cur={_j3_ingress_cur_bar or 'NA'} "
                    f"shadow={_j3_ingress_shadow_bar or 'NA'} "
                    f"det={_j3_ingress_prov_det or 'NA'} "
                    f"ft={int(_j3_ingress_prov_fake_top)} "
                    f"→ Strategy 최초 소비 전 J3 fail-closed"
                )
            elif _j3_ingress_effective:
                logger.info(
                    f"🟢 [J3-INGRESS-OK] {symbol} "
                    f"raw={int(_j3_ingress_raw)} "
                    f"eff={int(_j3_ingress_effective)} "
                    f"pready={int(_j3_ingress_prov_ready)} "
                    f"pJ3={int(_j3_ingress_prov_pass)} "
                    f"v3={_j3_ingress_verdict} "
                    f"pv3={_j3_ingress_prov_verdict} "
                    f"pbar={_j3_ingress_prov_bar or 'NA'} "
                    f"expect={_j3_ingress_expected_bar or 'NA'} "
                    f"det={_j3_ingress_prov_det or 'NA'} "
                    f"ft={int(_j3_ingress_prov_fake_top)}"
                )
            # 👁️ [CORE-FIRST-SHADOW · 2026-08-14]
            # 실제 return 순서는 유지하고, Core 판정만 복제 indicators로 선측정한다.
            # downstream 상태 갱신(CrossState/candidate_hold/recent_context)과 운영 로그 집계는 보존한다.
            _cg_probe_indicators = dict(indicators)
            _cg_probe_ok, _cg_probe_reason = self._gajami_core_ok(
                symbol=symbol,
                indicators=_cg_probe_indicators,
                data_package=data_package,
                current_price=current_price,
                dur_5x10=int(dur_5x10),
            )
            _cg_probe_j3ready = int(
                bool(indicators.get("_cycle_phase_shadow_ready", False))
            )
            _cg_probe_j3has = int(
                "judge3_cycle_pass" in indicators
                and "judge3_cycle_veto" in indicators
                and "judge3_cycle_verdict" in indicators
            )
            logger.info(
                f"👁️ [CORE-FIRST-SHADOW] {symbol} "
                f"bar={indicators.get('current_candle_time', indicators.get('indicator_shadow_bar', ''))} "
                f"ok={int(_cg_probe_ok)} reason={_cg_probe_reason} | "
                f"regime={regime.value if regime else '?'} dur={dur_5x10} | "
                f"j3ready={_cg_probe_j3ready} j3has={_cg_probe_j3has} "
                # 🔒 [CORE-SHADOW-JURY-OBSERVE-0905] ★513 — 관측 전용(판정 변화 0)
                # 헌법 FAIL 로 t6 판사 단계에 도달하지 못한 자리에서도
                # 4판사 상태를 남긴다. 정답봉에서 "누가 사야 했는가"를 세기 위한
                # 유일한 관측점이다. 키는 t6 L10746·L10797·L10817·L10820 과 동일.
                f"j1={int(indicators.get('judge1_gajami_pass', 0) or 0)} "
                f"j2={int(indicators.get('judge2_osc_pass', 0) or 0)} "
                f"j3={int(indicators.get('judge3_cycle_pass', 0) or 0)} "
                f"j4={int(indicators.get('judge4_reversal_pass', 0) or 0)} "
                f"tlc={indicators.get('cycle_phase_shadow', 'NOKEY')} "
                f"det={indicators.get('cycle_detail_shadow', 'NOKEY')} "
                f"align={int(bool(indicators.get('cycle_ma_aligned', False)))} "
                f"ma5r={int(bool(indicators.get('cycle_ma5_rising', False)))} "
                f"tage={indicators.get('cycle_phase_age_shadow', -1)} "
                f"tvd={indicators.get('judge3_cycle_verdict', 'NOKEY')}"
            )
            
            # Config
            recovery_max = None
            try:
                recovery_max = getattr(self, "_recovery_backfill_symbols", {}).get(symbol)
            except Exception:
                recovery_max = None
            
            if recovery_max is not None:
                MAX_BACKFILL_BARS = int(recovery_max)
                logger.debug(f"[BACKFILL-RECOVERY] {symbol} MAX_BACKFILL_BARS={MAX_BACKFILL_BARS}")
            else:
                MAX_BACKFILL_BARS = self.gajami_config['max_backfill_bars']
            
            MAX_LIFECYCLE_BARS = self.gajami_config['max_lifecycle_bars']
            shoulder_max = self.gajami_config['shoulder_max']
            allowed_regimes = self.gajami_config['backfill_allowed_regimes']
            
            # 공통 전제 조건
            is_golden_now = (ma5 > ma10)
            is_valid_prev = (ma5_prev > 0 and ma10_prev > 0)
            
            # 1️⃣ 실시간 크로스 감지 (STALE 차단)
            is_realtime_cross = (
                not data_stale and
                not cross_state.active and
                is_valid_prev and
                ma5_prev <= ma10_prev and
                is_golden_now and
                dur_5x10 <= 1
            )
            
            # 2️⃣ BACKFILL 후보 (STALE 허용)
            # 🔒 PATCH-2: 복구 시 강제 스캔(단, GC 유지 + dur 상한 보장)
            is_backfill_candidate = (
                (not cross_state.active)
                and is_golden_now
                and (
                    (force_backfill_scan and (1 <= dur_5x10 <= MAX_BACKFILL_BARS))
                    or (2 <= dur_5x10 <= MAX_BACKFILL_BARS)
                )
            )
            
            # 실시간 크로스 처리
            if is_realtime_cross:
                current_candle_close = indicators.get('close', current_price)
                candle_time = indicators.get('candle_time')
                candle_index = -1  # 현재봉 (일관성 위해)
                
                cross_state.activate(
                    candle_time=candle_time,
                    birth_price=current_candle_close,
                    candle_index=candle_index,
                    is_backfill=False
                )
                
                logger.info(
                    f"🔥 [GAJAMI-CROSS] {symbol} 골든크로스! "
                    f"| time={cross_state.cross_time} "
                    f"| birth={cross_state.birth_price:.0f}원 "
                    f"| MA5={ma5:.1f} > MA10={ma10:.1f} "
                    f"| idx={candle_index}"
                )
            
            # BACKFILL 복원 처리
            elif is_backfill_candidate:
                # 🔧 개선 3: df 없음 경고 강화
                if df is None:
                    logger.warning(
                        f"⚠️ [BACKFILL-FAIL] {symbol} df 없음 "
                        f"| stale={data_stale} "
                        f"| dur={dur_5x10} "
                        f"| has_df={'df' in indicators}"
                    )
                else:
                    cross_idx, cross_candle_time, cross_birth_price = \
                        self.get_cross_candle_index_from_dur(df, dur_5x10)
                    
                    if cross_idx is None:
                        logger.warning(f"⚠️ [BACKFILL-FAIL] {symbol} 인덱스 계산 실패")
                    else:
                        # 재생성 방지
                        if cross_state.was_processed(cross_candle_time, cross_idx):
                            pass
                        else:
                            # 보호 조건
                            current_candle_close = indicators.get('close', current_price)
                            estimated_rise = (current_candle_close - cross_birth_price) / cross_birth_price
                            
                            # regime 타입 검증
                            regime_value = getattr(regime, 'value', None) if regime else None
                            regime_ok = regime_value in allowed_regimes if regime_value else False
                            
                            # 🔧 개선 2: 종목별 타입 검증
                            if symbol not in self._regime_checked_symbols:
                                logger.debug(
                                    f"[REGIME-CHECK] {symbol} "
                                    f"regime={regime} | "
                                    f"value={regime_value} | "
                                    f"allowed={allowed_regimes}"
                                )
                                self._regime_checked_symbols.add(symbol)
                            
                            rise_ok = estimated_rise <= shoulder_max
                            ma_ok = is_golden_now
                            
                            if ma_ok and rise_ok and regime_ok:
                                if cross_state.cross_time != cross_candle_time:
                                    cross_state.activate(
                                        candle_time=cross_candle_time,
                                        birth_price=cross_birth_price,
                                        candle_index=cross_idx,
                                        is_backfill=True
                                    )
                                    
                                    # 🔒 PATCH-2D: 복구 성공 시 1회성 진입 허용 플래그 ON
                                    # - activate() 성공 확인 후 grace 설정 (방어적 코딩)
                                    if force_backfill_scan and cross_state.active:
                                        recovery_grace = True
                                        indicators["_stale_recovery_grace"] = True
                                        logger.warning(
                                            f"🔄 [BACKFILL-RECOVERY] {symbol} "
                                            f"| time={cross_state.cross_time} "
                                            f"| birth={cross_state.birth_price:.0f}원 "
                                            f"| rise={estimated_rise:.2%} | dur={dur_5x10} | idx={cross_idx} "
                                            f"| recovery_grace=True"
                                        )
                                    else:
                                        logger.warning(
                                            f"🔄 [GAJAMI-CROSS-BACKFILL] {symbol} "
                                            f"| time={cross_state.cross_time} "
                                            f"| birth={cross_state.birth_price:.0f}원 "
                                            f"| rise={estimated_rise:.2%} | dur={dur_5x10} | idx={cross_idx}"
                                        )
                                    
                                    # 🆕 recovery backfill은 1회성 (force_backfill_scan 여부와 무관하게 성공 시 항상 제거)
                                    try:
                                        if hasattr(self, "_recovery_backfill_symbols"):
                                            self._recovery_backfill_symbols.pop(symbol, None)
                                    except Exception:
                                        pass
                            else:
                                # REJECT = 영구 차단
                                cross_state.mark_processed(cross_candle_time, cross_idx)
                                
                                logger.warning(
                                    f"⚠️ [BACKFILL-REJECT] {symbol} 영구 차단 "
                                    f"| cross_time={cross_candle_time} "
                                    f"| idx={cross_idx} "
                                    f"| dur={dur_5x10} "
                                    f"| rise={estimated_rise:.2%} (max={shoulder_max:.2%}) "
                                    f"| regime={regime_value} (ok={regime_ok})"
                                )
                                
                                logger.debug(
                                    f"[GAJAMI-CROSS-END] {symbol} 크로스 생성 차단 "
                                    f"| 사유: BACKFILL-REJECT "
                                    f"| dur={dur_5x10}봉 | rise={estimated_rise:.2%}"
                                )
            
            # 포지션 SSoT
            position = None
            if hasattr(self, 'position_manager') and self.position_manager:
                try:
                    position = self.position_manager.get_position(symbol)
                except Exception as e:
                    logger.debug(f"[POSITION-CHECK] {symbol} position_manager 접근 실패: {e}")
            elif hasattr(self, 'positions') and self.positions:
                position = self.positions.get(symbol)
            
            if position is not None and cross_state.active:
                cross_state.mark_position_opened()
            
            # CrossState 종료 조건
            if cross_state.active:
                current_candle_close = indicators.get('close', current_price)
                current_rise_pct = cross_state.get_rise_pct(current_candle_close)
                
                should_deactivate = False
                end_reason = []
                
                if ma5 < ma10:
                    should_deactivate = True
                    end_reason.append("역전")
                elif current_rise_pct > shoulder_max:
                    should_deactivate = True
                    end_reason.append(f"SHOULDER초과({current_rise_pct:.2%})")
                elif dur_5x10 > MAX_LIFECYCLE_BARS:
                    should_deactivate = True
                    end_reason.append(f"수명초과(dur={dur_5x10}>{MAX_LIFECYCLE_BARS})")
                elif cross_state.had_position and position is None:
                    should_deactivate = True
                    end_reason.append("포지션종료")
                
                if should_deactivate:
                    logger.debug(
                        f"[GAJAMI-CROSS-END] {symbol} "
                        f"| 사유: {', '.join(end_reason)} "
                        f"| 지속: {dur_5x10}봉 "
                        f"| rise: {current_rise_pct:.2%}"
                    )
                    
                    if "포지션종료" in end_reason:
                        cross_state.reset()
                    else:
                        cross_state.deactivate()
            
            df = None  # (SSoT) Strategy에서 df 직접 사용 금지
            
            # ================================
            # 🔧 공통 판정 함수: 형상+추세 확인 (HEAD-BLOCK, VOL_LOW 공통)
            # ================================
            # 원칙: 형상 > 추세 > 위치 > regime
            # - soleus_pass >= 1.0: HCG 4단계 형상 판정 완료
            # - MA20>0 + MA10>0: 상승 추세 확인
            # - MA10>=MA20: 재가속 중 (발목→종아리 전환)
            # - regime 의존 제거: MA slope = 실시간 구조적 추세
            def _shape_trend_confirm(indicators: dict) -> bool:
                """형상+추세 직접 판정 (HEAD-BLOCK, VOL_LOW 공통)"""
                soleus_pass_val = float(safe_extract_scalar(indicators.get("soleus_pass", 0.0), 0.0))
                s10 = float(safe_extract_scalar(indicators.get("ma10_slope", 0.0), 0.0))
                s20 = float(safe_extract_scalar(indicators.get("ma20_slope", 0.0), 0.0))
                
                return (
                    soleus_pass_val >= 1.0
                    and s20 > 0
                    and s10 > 0
                    and s10 >= s20
                )
            
            # ================================
            # 🏰 GAJAMI-LIFECYCLE-GATE (J1 상태법)
            # ================================
            # 가자미근 lifecycle은 J1 자격을 판정한다.
            # J2/J3/J4 독립 PASS까지 이 상태법으로 전역 종료하지 않는다.
            # Analysis 계산 오류만 공통 fail-closed로 유지한다.
            # ================================
            _gajami_meta_lc = ((data_package.ma_data or {}).get("gajami_meta") or {}) if data_package else {}
            _lc = _gajami_meta_lc.get("gajami_lifecycle")

            if isinstance(_lc, dict) and _lc:
                _state = str(_lc.get("state", "DEAD") or "DEAD")
                _rise = _lc.get("rise_pct")
                _death = _lc.get("death_reason")
                _rise_log = (
                    f"{_rise*100:.2f}%"
                    if _rise is not None
                    else "NA"
                )

                # 🔒 [J1-LIFECYCLE-JUDGE-NEUTRAL-0906] ★527
                # lifecycle은 가자미근/J1 상태법이다. J2/J3/J4 독립 PASS의
                # 전역 return 권한만 회수하고 Analysis 오류는 fail-closed로 유지한다.
                _life_j234_pass = bool(
                    int(indicators.get("judge2_osc_pass", 0) or 0)
                    or int(indicators.get("judge3_cycle_pass", 0) or 0)
                    or int(indicators.get("judge4_reversal_pass", 0) or 0)
                )
                if (
                    _state != "VALID"
                    and (_state == "ANALYSIS_INVALID_ERROR" or not _life_j234_pass)
                ):
                    self.last_rejection_reason = f"GAJAMI_LIFECYCLE:{_state}"
                    _cross_state = self.cross_state_map.get(symbol)
                    _cur_gc_id = int(_cross_state.cross_candle_index if _cross_state and _cross_state.active else -1)
                    if _cur_gc_id != -1:
                        self._dead_wave_gc_id[symbol] = _cur_gc_id
                    logger.warning(
                        f"🚫 [GAJAMI-LIFECYCLE] {symbol} "
                        f"state={_state} reason={_death} rise={_rise_log}"
                    )
                    try:
                        logger.info(
                            f"📊 [SHAPE-V2-BLOCK] {symbol} | stage=GAJAMI_LIFECYCLE | "
                            f"gajami_ok={indicators.get('shape_v2_gajami_ok', 'NA')} | "
                            f"body_growth={indicators.get('shape_v2_body_growth', 'NA')} | "
                            f"body_recent={float(indicators.get('shape_v2_body_recent', 0.0) or 0.0):.5f} | "
                            f"body_past={float(indicators.get('shape_v2_body_past', 0.0) or 0.0):.5f} | "
                            f"head={indicators.get('shape_v2_head_ok', 'NA')} | "
                            f"lead={indicators.get('shape_v2_lead_ok', 'NA')} | "
                            f"align={indicators.get('shape_v2_align_ok', 'NA')} | "
                            f"source={indicators.get('shape_v2_source', 'NA')} | "
                            f"eee={float(indicators.get('eee_confidence', 0.0) or 0.0):.3f} | "
                            f"final={float(locals().get('final_score_capped', 0.0) or 0.0):.3f} | "
                            f"soleus={int(bool(locals().get('soleus_gajami', False)))} | "
                            f"reason={self.last_rejection_reason}"
                        )
                    except Exception:
                        pass
                    return None
                elif _state != "VALID":
                    _cross_state = self.cross_state_map.get(symbol)
                    _cur_gc_id = int(_cross_state.cross_candle_index if _cross_state and _cross_state.active else -1)
                    if _cur_gc_id != -1:
                        # J1 동일-wave 재탄생 방지 메모리는 그대로 유지한다.
                        self._dead_wave_gc_id[symbol] = _cur_gc_id
                    logger.info(
                        f"🟢 [LIFECYCLE-JUDGE-NEUTRAL] {symbol} "
                        f"state={_state} reason={_death} → J2/J3/J4 판결권 유지 | "
                        f"J2={int(indicators.get('judge2_osc_pass', 0) or 0)} "
                        f"J3={int(indicators.get('judge3_cycle_pass', 0) or 0)} "
                        f"J4={int(indicators.get('judge4_reversal_pass', 0) or 0)}"
                    )
                else:
                    _cross_state = self.cross_state_map.get(symbol)
                    _cur_gc_id = int(_cross_state.cross_candle_index if _cross_state and _cross_state.active else -1)
                    _dead_gc_id = self._dead_wave_gc_id.get(symbol, -999)
                    if (
                        _cur_gc_id == _dead_gc_id
                        and _dead_gc_id != -999
                        and not _life_j234_pass
                    ):
                        self.last_rejection_reason = "GAJAMI_LIFECYCLE:DEAD_WAVE_REBORN"
                        logger.warning(
                            f"🚫 [DEAD-WAVE-BLOCK] {symbol} | 동일 GC 파동 재탄생 차단 "
                            f"| gc_id={_cur_gc_id} | rise={_rise_log}"
                        )
                        try:
                            logger.info(
                                f"📊 [SHAPE-V2-BLOCK] {symbol} | stage=DEAD_WAVE_REBORN | "
                                f"gajami_ok={indicators.get('shape_v2_gajami_ok', 'NA')} | "
                                f"body_growth={indicators.get('shape_v2_body_growth', 'NA')} | "
                                f"body_recent={float(indicators.get('shape_v2_body_recent', 0.0) or 0.0):.5f} | "
                                f"body_past={float(indicators.get('shape_v2_body_past', 0.0) or 0.0):.5f} | "
                                f"head={indicators.get('shape_v2_head_ok', 'NA')} | "
                                f"lead={indicators.get('shape_v2_lead_ok', 'NA')} | "
                                f"align={indicators.get('shape_v2_align_ok', 'NA')} | "
                                f"source={indicators.get('shape_v2_source', 'NA')} | "
                                f"eee={float(indicators.get('eee_confidence', 0.0) or 0.0):.3f} | "
                                f"final={float(locals().get('final_score_capped', 0.0) or 0.0):.3f} | "
                                f"soleus={int(bool(locals().get('soleus_gajami', False)))} | "
                                f"reason={self.last_rejection_reason}"
                            )
                        except Exception:
                            pass
                        return None
                    if (
                        _cur_gc_id == _dead_gc_id
                        and _dead_gc_id != -999
                        and _life_j234_pass
                    ):
                        logger.info(
                            f"🟢 [LIFECYCLE-JUDGE-NEUTRAL] {symbol} "
                            f"state=DEAD_WAVE_REBORN gc_id={_cur_gc_id} "
                            f"→ J2/J3/J4 판결권 유지 | "
                            f"J2={int(indicators.get('judge2_osc_pass', 0) or 0)} "
                            f"J3={int(indicators.get('judge3_cycle_pass', 0) or 0)} "
                            f"J4={int(indicators.get('judge4_reversal_pass', 0) or 0)}"
                        )
                    logger.info(
                        f"✅ [GAJAMI-LIFECYCLE] {symbol} "
                        f"state={_state} rise={_rise_log}"
                    )
            else:
                logger.debug(f"⚠️ [GAJAMI-LIFECYCLE] {symbol} lifecycle missing")
            
            # ==========================================================
            # 🔒 _check_buy_conditions 핵심 변수 선초기화 (2026-01-03)
            # ==========================================================
            # 1군: 치명 오류 발생군
            volume_ratio = 0.0
            gajami_zone = _soleus_ssot(indicators, data_package.ma_data)
            atr_pct = None
            adx_value = None
            buying_ratio = 0.0
            
            # 2군: Series → scalar 문제군
            adx_raw_safe = None
            atr_raw_safe = None
            vol_raw_safe = None
            
            # 3군: 가자미근 / Knee 계열
            
            # 4군: 체제·신호 관련 점수
            signal_strength = 0.0
            penalty_factor = 1.0
            # penalty_reasons = []  # ❌ 중복 초기화 제거 (위에서 선초기화됨)
            confidence = None
            
            # 5군: Boolean 플래그
            has_golden_cross = False
            is_pullback = False
            is_upper_shadow = False
            is_ranging = False
            is_trending_down = False
            
            # 6군: 거래량·랠리·패턴
            rally_pct = 0.0
            rally_duration = 0
            is_fast_rally = False
            is_slow_rally = False
            
            # REGIME COOLDOWN 체크 제거: 체제 판단은 상위 레이어에서 종료
            # 트라우마 회피 제거 - 조건 맞으면 즉시 진입
            
            # 👁️ OBSERVE: 5봉 완전 꺾임(모멘텀 소진) 관측.
            # 여기서 계산되는 penalty_factor는 아래 공용 초기화에서 1.0으로 재설정되어
            # 최종 scoring에는 반영되지 않는다.
            close_down5 = int(safe_extract_scalar(indicators.get('close_down5', 0.0), 0.0))
            if close_down5 == 1:
                penalty_factor = self._apply_soft_penalty(
                    penalty_factor, 0.85, penalty_reasons, "MomentumExhausted-OBS"
                )
                logger.info(
                    f"👁️ [MomentumExhausted-OBSERVE] {symbol} 5봉 완전 꺾임 관측 | "
                    f"intermediate_pf={penalty_factor:.2f} | "
                    f"후단 초기화로 최종 scoring 미반영"
                )
            
            # 👁️ OBSERVE: 피뢰침(위꼬리) 매도 압력 관측
            # - 현재 경로에서는 최종 scoring 감산 없음.
            # - 하드 veto 필요시 별도 검증 후 판단.
            upper_shadow = int(safe_extract_scalar(indicators.get('upper_shadow_alert', 0.0), 0.0))
            if upper_shadow == 1:
                penalty_reasons.append("위꼬리-관측")
                logger.info(
                    f"👁️ [UpperShadow-OBSERVE] {symbol} | "
                    f"위꼬리 감지 → 관측만, 최종 신뢰도 감산 없음"
                )
            
            # 👁️ 거래량 스파이크 관측 — 현재 경로에서는 scoring 감산 없음.
            volume_spike_penalty = float(indicators.get('volume_spike_penalty', 1.0))
            if volume_spike_penalty < 1.0:
                penalty_reasons.append(f"거래량스파이크-관측({volume_spike_penalty:.2f})")
                logger.info(
                    f"👁️ [VolumeSpike-OBSERVE] {symbol} | "
                    f"거래량 스파이크 감지 | "
                    f"observed_factor={volume_spike_penalty:.2f} | "
                    f"최종 scoring 감산 없음"
                )
            
            # SENTINEL 기반 변수 추출 (SOFT - 보조 지표)
            # 보조 지표는 SENTINEL이어도 차단하지 않고 감점만 적용
            # 🔒 보조 지표 스칼라화 (Series → float 강제 변환)
            # Strategy는 스칼라만 허용 - Series/DataFrame은 Analysis 전용
            # pandas Series가 indicators에 들어있을 경우 자동 변환
            ma5_slope = safe_extract_scalar(indicators.get('ma5_slope', SENTINEL_FLOAT), SENTINEL_FLOAT)
            ma10_slope = safe_extract_scalar(indicators.get('ma10_slope', SENTINEL_FLOAT), SENTINEL_FLOAT)
            ma20_slope = safe_extract_scalar(indicators.get('ma20_slope', SENTINEL_FLOAT), SENTINEL_FLOAT)
            adx_raw_safe = safe_extract_scalar(indicators.get('adx', SENTINEL_FLOAT), SENTINEL_FLOAT)
            ma_deviation = safe_extract_scalar(indicators.get('ma_deviation', SENTINEL_FLOAT), SENTINEL_FLOAT)
            
            # 추가 정규화: 비교 연산에 자주 사용되는 변수들
            buying_ratio_raw = indicators.get('buying_ratio', None)
            buying_ratio = nz(buying_ratio_raw, 0.0)
            
            vol_accel_raw = indicators.get('vol_accel', None)
            vol_accel = nz(vol_accel_raw, 1.0)
            
            ma5_raw = indicators.get('ma5', None)  # 통일된 키
            ma5 = nz(ma5_raw, 0.0)
            
            ma10_raw = indicators.get('ma10', None)  # 통일된 키
            ma10 = nz(ma10_raw, 0.0)
            
            ma15_raw = indicators.get('ma15', None)  # 통일된 키
            ma15 = nz(ma15_raw, 0.0)
            
            ma20_raw = indicators.get('ma20', None)  # 이미 올바름
            ma20 = nz(ma20_raw, 0.0)
            
            ma60_raw = indicators.get('ma60', None)  # 이미 올바름
            ma60 = nz(ma60_raw, 0.0)
            
            ma120_raw = indicators.get('ma120', None)  # 통일된 키
            ma120 = nz(ma120_raw, 0.0)
            
            recent_high_raw = indicators.get('recent_high', None)
            recent_high = nz(recent_high_raw, current_price)
            
            recent_low_raw = indicators.get('recent_low', None)
            recent_low = nz(recent_low_raw, current_price)
            
            volume_slope_raw = indicators.get('volume_slope', None)
            volume_slope = nz(volume_slope_raw, 0.0)
            
            prev_gap_510_raw = indicators.get('prev_ma_gap_510', None)
            prev_gap_510 = nz(prev_gap_510_raw, 0.0)
            
            resistance_raw = indicators.get('resistance_level', None)
            resistance = nz(resistance_raw, 0.0)
            
            resistance_level_raw = indicators.get('resistance_level', None)
            resistance_level = nz(resistance_level_raw, current_price)
            
            trust_score_raw = indicators.get('confidence', None)
            trust_score = nz(trust_score_raw, 0.0)
            
            current_confidence_raw = indicators.get('confidence', None)
            current_confidence = nz(current_confidence_raw, 0.0)
            
            adx_current_raw = indicators.get('adx', None)
            adx_current = nz(adx_current_raw, 0.0)
            
            price_4h_raw = indicators.get('price_4h', None)
            price_4h = nz(price_4h_raw, current_price)

            atr_init = indicators.get('atr', None)
            if atr_init is None:
                atr = 0.0  # 기본값
            else:
                atr = float(atr_init) if isinstance(atr_init, (int, float)) else 0.0
            
            # 추가 정규화 4: 우변 None 방지 (TypeError 완전 차단)
            vol_accel_threshold_raw = indicators.get('vol_accel_threshold', None)
            vol_accel_threshold = nz(vol_accel_threshold_raw, 1.0)
            
            vol_threshold_raw = indicators.get('vol_threshold', None)
            vol_threshold = nz(vol_threshold_raw, 0.0)
            
            # ✅ Soft 페널티 공용 초기화 (STEP3 이전 필수)
            # 🔒 _apply_soft_penalty는 클래스 메서드로 정의됨 (라인 760 부근)
            
            penalty_factor = 1.0
            # === PATCH G8: penalty/bonus 초기화 (v2 SSoT) ===
            penalty = 0.0  # Soft 페널티 누적용
            bonus = 0.0    # Bonus 누적용 (penalty와 동급 처리)
            # penalty_reasons, bonus_reasons는 이미 위에서 초기화됨
            penalty_details = {}

            # ⚠️ SOFT PENALTY: 보조 지표 결측 감점
            # Analysis에서 계산 실패한 보조 지표는 차단이 아니라 감점만 적용
            # SENTINEL은 "정보 없음"이지 "위험"이 아님
            if is_sentinel(adx_raw_safe):
                penalty_reasons.append("ADX_MISSING")
                logger.debug(
                    f"⚠️ [SoftPenalty] {symbol} ADX 결측(SENTINEL) → penalty 0.90x"
                )

            # =====================================================
            # 🔒 STEP 3: 체제 전환 거래 금지 존 (Safety Guard)
            # =====================================================
            try:
                adx_raw = indicators.get("adx", 0)
                adx_prev_raw = indicators.get("adx_prev", adx_raw)
                
                def _to_scalar_float(x, default=0.0) -> float:
                    """모든 타입을 스칼라 float로 변환 (item() 사용 안 함)"""
                    try:
                        # pandas Series
                        if hasattr(x, "iloc"):
                            return float(x.iloc[-1])
                        # numpy array / list / tuple
                        if isinstance(x, (list, tuple)):
                            return float(x[-1]) if len(x) > 0 else float(default)
                        if hasattr(x, "shape"):  # numpy ndarray 등
                            return float(x.ravel()[-1]) if x.size > 0 else float(default)
                        # numpy scalar / python scalar
                        return float(x)
                    except Exception:
                        return float(default)
                
                adx = _to_scalar_float(adx_raw, 0.0)
                adx_prev = _to_scalar_float(adx_prev_raw, adx)
                
                # 🔥 1단계: 약한 추세 차단 (체제별 차별화)
                # TRENDING_DOWN만 엄격 차단 (위험)
                # RANGING/TRENDING_UP은 허용 (전환 초입 기회)
                # 🔥 ADX 약화 강도 계산 (1.5pt 이상만 차단)
                adx_decline = adx_prev - adx if (adx and adx_prev) else 0
                
                if lt(adx, adx_prev) and lt(adx, 20):
                    if regime == MarketRegime.TRENDING_DOWN:
                        # 강한 약화 (1.5pt 이상) → 반등 신호 관측
                        if adx_decline >= 1.5:
                            # ADX 약화 반등 관측
                            penalty_reasons.append(f"ADX약화반등({adx_decline:.1f}pt,관측)")
                            logger.info(
                                f"🟡 [WeakTrendRelax-Down] {symbol} | "
                                f"하락 체제 + 강한 ADX 약화: {fmt(adx, '.1f')}↓{fmt(adx_prev, '.1f')} "
                                f"(-{adx_decline:.1f}pt ≥ 1.5pt) → 반등 신호 관측"
                            )
                        # 🆕 미세 약화 (1.5pt 미만)
                        else:
                            penalty_reasons.append(f"ADX미세약화({adx_decline:.1f}pt,관측)")
                            logger.debug(
                                f"👁️ [WeakTrend-OBSERVE] {symbol} | "
                                f"하락 체제 + 미세 ADX 약화: {fmt(adx, '.1f')}↓{fmt(adx_prev, '.1f')} "
                                f"(-{adx_decline:.1f}pt < 1.5pt) → 관측만, 최종 신뢰도 감산 없음"
                            )

                    else:
                        logger.debug(
                            f"✅ [WeakTrend-Allow] {symbol} | "
                            f"체제={regime} + ADX 약화: {fmt(adx, '.1f')}↓{fmt(adx_prev, '.1f')} "
                            f"(전환 초입 가능)"
                        )
                        # RANGING / TRENDING_UP 은 전환 기회로 인정 → 통과
                
                # 🔥 2단계: 강한 하락 처리
                if regime == MarketRegime.TRENDING_DOWN and gt(adx, 25):
                    tfv = self.timeframe.value
                    
                    # ✅ 단타(1m/3m/5m)는 ADX 차단 금지 (공격적 전략)
                    if tfv in SHORT_TERM_TIMEFRAMES:
                        logger.info(
                            f"⚠️ [StrongDown-Pass] {symbol} | "
                            f"단타({tfv}) → ADX 차단 면제 | ADX={fmt(adx, '.1f')}"
                        )
                        # 통과 (차단 안함)
                    else:
                        logger.debug(
                            f"🚫 [StrongDownBlock] {symbol} | "
                            f"강한 하락 ADX={fmt(adx, '.1f')} > 25"
                        )
                        return None
                
                # 🔥 3단계: 극단 변동성 차단
                # ✅ 단타(1m/3m/5m)는 '차단'이 아니라 'Soft 페널티'로만 처리 (진입 0건 방지)
                if gt(adx, 50) and regime in (MarketRegime.RANGING, MarketRegime.TRENDING_DOWN):
                    tfv_local = self.timeframe.value if hasattr(self.timeframe, "value") else str(self.timeframe)

                    if tfv_local in SHORT_TERM_TIMEFRAMES:
                        penalty_factor = self._apply_soft_penalty(
                            penalty_factor, 0.85, penalty_reasons,
                            f"ExtremeVolatility-Soft(ADX={fmt(adx, '.1f')})"
                        )
                        logger.debug(
                            f"⚠️ [ExtremeVolatility-Soft] {symbol} | "
                            f"단타({tfv_local}) → 차단 대신 penalty *0.85 | ADX={fmt(adx, '.1f')} | regime={regime}"
                        )
                    else:
                        logger.debug(
                            f"🚫 [ExtremeVolatilityBlock] {symbol} | "
                            f"ADX={fmt(adx, '.1f')} > 50 | regime={regime}"
                        )
                        return None
                    
            except Exception as e:
                logger.warning(f"⚠️ [STEP3] {symbol} 예외 발생, 스킵: {e}")
                pass
            
            # 🔥 4단계: 늦은 진입 차단 (가자미근 오판 방지)
            try:
                # SUDDEN_BUYING 늦은 진입 체크 (형상 기반)
                gajami_meta_local = (data_package.ma_data or {}).get("gajami_meta") or {}
                volume_pressure_late = str(indicators.get("volume_pressure", "NEUTRAL") or "NEUTRAL")
                soleus_zone_hint = bool(gajami_meta_local.get("soleus_zone", False))

                # 가자미근 확정 없이 SUDDEN_BUYING만으로 진입하는 경우 추격 위험
                # 🐟 탄생 초입(rise ≤ 3%) 면제: 거래량 집중은 탄생 신호, 추격 아님
                # 🔧 경로 수정: ma_data SSoT (soleus=False 탄생봉에서도 lifecycle 접근 가능)
                _gmeta_sbl = (data_package.ma_data or {}).get("gajami_meta") or {}
                _lc_sbl = _gmeta_sbl.get("gajami_lifecycle") or {}
                _rise_sbl = float(_lc_sbl.get("rise_pct", 0) or 0)
                _is_birth_early = (0 < _rise_sbl <= 0.030)
                _lc_sbl_dur_src = _lc_sbl.get("dur", None)
                _lc_sbl_dur = int(_lc_sbl_dur_src) if _lc_sbl_dur_src is not None else 999
                _is_birth_valid_sbl = (
                    _lc_sbl.get("state") == "VALID"
                    and _lc_sbl_dur <= SoleusGajamiConstants.NEWBORN_MAX_DUR
                )
                if not soleus_zone_hint and volume_pressure_late == "SUDDEN_BUYING" \
                        and not _is_birth_early and not _is_birth_valid_sbl:

                    extra_penalty = PENALTY_WEIGHTS.get("sudden_buying_late", 0.25)
                    penalty_reasons.append(f"SUDDEN_BUYING_LATE(+{extra_penalty:.2f})")
                    penalty = add(penalty, extra_penalty)
                    penalty_details["sudden_buying_late"] = extra_penalty
                    logger.warning(
                        f"⚠️ [SuddenBuying-Late-Soft] {symbol} 추격 위험 → 페널티 적용"
                    )
                    
            except Exception:
                pass
            
            # ✅ 6순위: Soft 페널티 CAP (과도 감점 방지)
            # - Soft 페널티가 여러 번 적용되어도 최소 보장선 유지
            # - 하드 차단 제거 이후 재차 "0건" 방지 목적
            # 🛡️ UnboundLocalError 전수 방지 (2025-12-19)
            # - components: 신호 컴포넌트 (예외 발생 시 안전 기본값)
            # - final_score_capped: 최종 신뢰도 (early return 시 로그 안정화)
            # - effective_threshold: 임계값 (미계산 시 보수적 기본값)
            components = {
                "ma_signal": 0.0,
                "volume_signal": 0.0,
                "candle_signal": 0.0,
                "atr_signal": 0.0,
                "adx_signal": 0.0,
            }
            final_score_capped = 0.0
            # === PATCH G7: threshold 하향 완화 제거 ===
            # 기본 판사는 self.min_confidence.
            # 단, RANGING은 RangingHybrid가 산출한 상향 threshold만 허용한다.
            
            # 📦 BoxMode 제거 - lifecycle로 대체
            rally_info_cached = indicators.get('rally_info') or None  # 🆕 연대기 패턴 필터용 캐시
            skip_flat_filter = False
            early_entry_flag = False
            # 🔒 SSoT: ma_analysis는 Producer(Analysis) 결과를 단일 진실로 사용
            ma_analysis = data_package.ma_data or {}

            # 🔒 패턴/캔들 로컬변수 기본값 (UnboundLocalError 방지)
            # - 일부 후단 로직(거래량 둔화/불장 완화)에서 c0,c1,c2 / is_v_pattern / is_u_pattern을 참조함
            # - df 길이 부족/fast_entry로 패턴 검증 스킵 시에도 안전하게 존재해야 함
            c0 = c1 = c2 = 0.0
            try:
                _df_c = getattr(data_package, 'df', None)
                if _df_c is not None and len(_df_c) >= 3:
                    c0 = float(_df_c["close"].iloc[-3])
                    c1 = float(_df_c["close"].iloc[-2])
                    c2 = float(_df_c["close"].iloc[-1])
            except Exception:
                pass
            is_v_pattern = False
            is_u_pattern = False

            # 🔒 분기 변수 기본값 (타임프레임/체제 무관)
            #  - UnboundLocalError 완전 종결
            #  - 목적: 조건문 내 정의 여부와 무관하게 항상 존재 보장
            #  - 동작: 이후 profile 로딩 시 정상 재할당 (라인 4473)
            EARLY_ENTRY_EXCEPTION = False
            profile = {}

            # 🆕 FastKnee / RegimeEarly 공통 정의 (LSK/ETH 지연 진입 방지)
            # ─────────────────────────────────────────────────────────────
            # ─────────────────────────────────────────────────────────────
            # 목적: 가자미근 초입 감지를 초기에 통합 정의
            # 효과: 코드 중복 제거, 전체 로직에서 일관되게 사용
            # ─────────────────────────────────────────────────────────────
            # - 가자미근은 "형상" 단일 판정
            # - 거리/저점대비 기반 진입 타이밍 보정 금지
            
            # RegimeEarly: TD인데 사실상 TU 전환 초입(정배열 임박) 상태
            is_regime_early = False
            if regime == MarketRegime.TRENDING_DOWN:
                ma_spacing_turn = safe_extract_scalar(indicators.get("ma_spacing_pct", 0.0))
                
                # indicators ma*_slope는 % 단위 → ratio로 변환해서 비교 (정석 계약)
                ma5_slope_turn = safe_extract_scalar(indicators.get("ma5_slope", 0.0)) / 100.0
                ma120_slope_turn = safe_extract_scalar(indicators.get("ma120_slope", 0.0)) / 100.0
                
                ma5_turn = safe_extract_scalar(indicators.get("sma_5", indicators.get("ma5", current_price)))
                
                # 골든크로스 누적(전환 신호)도 함께 본다 (초입은 slope가 0 근처일 수 있음)
                ma_data_turn = data_package.ma_data or {}
                golden_count_turn = int(ma_data_turn.get("golden_count", 0) or 0)

                # 🔧 RegimeEarly 저점 가드
                _re_low_area = bool(indicators.get("is_low_area", False))
                if (
                    abs(ma_spacing_turn) < 0.15
                    and ma120_slope_turn >= -0.0005
                    and ma5_slope_turn > -0.001
                    and golden_count_turn >= 4
                    and current_price >= ma5_turn
                    and _re_low_area  # 저점 가드
                ):
                    is_regime_early = True
                    indicators["regime_early"] = True  # RegimeEarly: TD 반등 초입 힌트 (완화 신호)
                    logger.info(
                        f"🔄 [RegimeEarly] {symbol} TD→TU 전환 초입 감지 | "
                        f"ma_spacing={ma_spacing_turn:.3f}% | ma5_slope={fmt(ma5_slope_turn, '.6f')} | "
                        f"ma120_slope={ma120_slope_turn:.6f} | golden_count={golden_count_turn} | price>=ma5"
                    )
            atr = safe_extract_scalar(indicators.get('atr', 0))
            if current_price > 0 and atr > 0:
                # ==============================================================
                # 🆕 [BTC ATR 확장] 소수점 4자리 (0.0000%) — BTC는 변동폭이 매우 낮음
                # ==============================================================
                if symbol == "KRW-BTC":
                    atr_pct = round((atr / current_price) * 100, 4)
                else:
                    atr_pct = round((atr / current_price) * 100, 2)
            else:
                atr_pct = 0.0
                                    
            # 🆕 EEE 점수 항상 계산 (차단 시에도) - 2025-11-30
            # 목적: 진입 차단된 경우에도 실제 점수를 확인하여 필터 정확성 검증
            # 1) EEE 점수는 "관측용"으로만 계산한다.
            #    - 주 판사: [CONF] → indicators['confidence']
            #    - 보조 판사(EEE): indicators['eee_confidence'] (절대 덮어쓰기 금지)
            eee_trust_score = None
            eee_components = {}

            # 🔒 [2026-08-03] TD_TREND_BLOCK 진단 로그 전용 rise/ATR 입력.
            #  TU_LOW_MOMENTUM(TREND·KNEE) 철거로 _rise_src 소비처 소멸 → 제거.
            _rise_raw = indicators.get("gajami_rise_atr_ratio", None)
            _rise_atr_r = safe_extract_scalar(_rise_raw, 0.0)

            try:
                # indicators에 이미 주 판사 신뢰도가 있으면 그 값을 관측에 활용
                eee_trust_score = indicators.get('confidence', None)
                eee_trust_score = S(eee_trust_score) if eee_trust_score is not None else None
                
                # 계산되지 않았으면 관측용으로만 계산 (주 신뢰도는 건드리지 않음)
                if eee_trust_score is None or eee_trust_score == 0.0:
                    if not indicators.get("_valid", True):
                        logger.debug(
                            f"⛔ [EEE-SKIP] {data_package.market} "
                            f"indicators invalid → EEE 계산 스킵"
                        )
                        eee_trust_score = 0.0
                        eee_components = {}
                    else:
                        prelim_components = self._calculate_signal_components(
                            df, data_package, ma_analysis
                        )
                        
                        # ✅ 정석: EEE 계산 입력은 dict만 허용 (Series/DataFrame이면 즉시 배제)
                        if isinstance(prelim_components, dict) and len(prelim_components) > 0:
                            eee_weights = self.config.get_cached('signal_weights') or {}
                            eee_performance = getattr(self, 'signal_performance', {})
                            
                            # =====================================================
                            # 🔒 EEE 입력 정렬 (가자미근 초입 대응)
                            # =====================================================
                            # 철학:
                            # - 가자미근은 MA 형상 확정 신호
                            # - Volume/Candle은 후행 → 아직 안 나왔다고 0점 취급 금지
                            # - "부정 신호(0점)"가 아니라 "미확정(0.40)" 처리
                            # =====================================================
                            # SSoT: gajami_meta에서 직접 가져옴 (indicators 설정 전)
                            _gajami_meta_eee = (data_package.ma_data or {}).get("gajami_meta") or {}
                            soleus_zone_eee = bool(safe_extract_scalar(
                                _gajami_meta_eee.get("soleus_zone", 0.0), 0.0
                            ))
                            
                            # 🔒 정석: EEE 보정은 "가자미근 확정"에서만 허용
                            
                            # EEE는 관측용이다. 점수 입력을 보정(증폭)하지 않는다.
                            # soleus 여부는 로그로만 남긴다.
                            if soleus_zone_eee:
                                logger.debug(
                                    f"ℹ️ [EEE-SOLEUS] {symbol} soleus=True (EEE는 보정 없음)"
                                )
                            
                            gc_count = int(indicators.get("golden_count", 0) or 0)
                            
                            # 🆕 MA-CAP 판정은 반드시 "최신 봉 기준 정배열"로만 수행
                            # - 버그 수정: indicators에서 최신 MA 값을 정확히 추출
                            # - 정배열(MA5 > MA10 > MA20)이면 CAP 적용 금지
                            ma5_now = safe_extract_scalar(indicators.get("ma5", 0.0), 0.0)
                            ma10_now = safe_extract_scalar(indicators.get("ma10", 0.0), 0.0)
                            ma20_now = safe_extract_scalar(indicators.get("ma20", 0.0), 0.0)
                            
                            # 🔧 준골든크로스 MA 캡핑 면제 (형상 보완)
                            # - MA5<MA10 이더라도 gap이 0.1% 이내이며 기울기가 상승이면
                            #   "역배열"이 아니라 "골든 직전 구조"로 본다.
                            # indicators ma*_slope는 % 단위 → ratio로 변환
                            ma5_slope_now = float(safe_extract_scalar(indicators.get("ma5_slope", 0.0), 0.0)) / 100.0
                            ma10_slope_now = float(safe_extract_scalar(indicators.get("ma10_slope", 0.0), 0.0)) / 100.0
                            gap_5_10_now = abs(ma5_now - ma10_now) / ma10_now if ma10_now > 0 else 999

                            near_golden_exempt = (
                                gap_5_10_now <= 0.001          # 0.1% 이내
                                and ma5_slope_now > 0.0        # MA5 상승
                                and ma10_slope_now >= -0.0001  # MA10 하락 아님
                            )

                            # [REMOVED] 가자미근 MA10<MA20 방화벽
                            # - 가자미근 초입은 MA10 < MA20 상태에서 시작하므로 이 방화벽은 개념 충돌
                            # - TTL(4~7봉) + 박스 필터가 늦은 진입을 차단하므로 방화벽 불필요
                            
                            # 🔒 MA-CAP: 압축형 초입 존중 (MA5≈MA10 허용)
                            # near_golden_exempt: 준골든크로스 (|5-10|≤0.1%)
                            
                            # 🔒 MA-CAP (집행 완충층)
                            # 역할:
                            # - Analysis(SSoT)가 "가자미근 형상"을 승인하더라도
                            #   집행(Strategy) 단계에서 미세 노이즈를 완충하기 위한 캡핑
                            # - 재판정이 아니라 "리스크 완충"으로만 사용
                            # 
                            # 원칙:
                            # - 가자미근 초입(MA10<MA20 가능)을 존중하되
                            # - 약한 역배열(MA5<MA10)은 캡핑으로 완충
                            
                            is_bear_alignment = (ma5_now < ma10_now)  # 약한 역배열만
                            
                            if (is_bear_alignment and not near_golden_exempt) and gc_count < 7:
                                ma_signal_current = safe_extract_scalar(prelim_components.get('ma_signal', 0.0), 0.0)
                                if ma_signal_current > 0.30:
                                    logger.debug(
                                        f"👁️ [MA-CAP-OBSERVE] {symbol} 약한 역배열 | "
                                        f"MA={ma_signal_current:.2f}, Producer 원본 유지 "
                                        f"(MA5={ma5_now:.1f}, MA10={ma10_now:.1f})"
                                    )
                            elif near_golden_exempt and is_bear_alignment:
                                logger.debug(
                                    f"✅ [NEAR-GOLDEN-EXEMPT] {symbol} 준골든크로스 → MA 캡핑 면제 | "
                                    f"gap={gap_5_10_now*100:.3f}% s5={ma5_slope_now:.5f} s10={ma10_slope_now:.5f}"
                                )
                            elif gc_count >= 7:
                                logger.debug(
                                    f"✅ [MA-CAP-EXEMPT] {symbol} 골든 {gc_count}개 → MA-CAP 면제"
                                )
                            eee_trust_score = self._calculate_final_confidence(
                                prelim_components, eee_weights, regime, eee_performance
                            )
                            eee_trust_score = S(eee_trust_score)
                            
                            # 🔒 EEE-CAP (Dead Code 제거)
                            # - MA10<MA20는 위 방화벽에서 return 처리됨
                            # - 여기 도달 = MA10>=MA20 확정
                            # - is_strong_bear 미정의로 인한 NameError 방지
                            # (삭제됨)
                            
                            eee_components = prelim_components

                            # ✅ 정석: 횡보장(RANGING)에서 가자미근(무릎) 미충족이면
                            # EEE가 "진입 자격" 점수가 될 수 없도록 상한 고정
                            
                            # ✅ Series → bool 금지: 스칼라 계약 강제
                            # 🔒 [EEE-KNEE-SSOT-0826]
                            # is_soleus/is_td_knee/is_up_knee export는 이미 삭제됐다.
                            # EEE의 가자미근 SSoT는 위에서 gajami_meta로 계산한 soleus_zone_eee.
                            has_knee = bool(soleus_zone_eee)

                            if regime == MarketRegime.RANGING and not has_knee:
                                _glc_eee = ((data_package.ma_data or {}).get("gajami_meta") or {}).get("gajami_lifecycle") or {}
                                _glc_eee_dur_src = _glc_eee.get("dur", None)
                                _glc_eee_dur = int(_glc_eee_dur_src) if _glc_eee_dur_src is not None else 999
                                _is_birth_valid_eee = (
                                    _glc_eee.get("state") == "VALID"
                                    and _glc_eee_dur <= SoleusGajamiConstants.NEWBORN_MAX_DUR
                                )
                                # 🔒 [EEE-CAP-ORIGIN-OBSERVE-0906-R1] ★522-O-R1
                                # 캡 0.29 + 문턱 0.30(L9513)이 짝을 이뤄 "비탄생=자동차단"이
                                # 된다. 실측 4일 EEE_LOW 34,482건 중 42.0%가 캡값 0.290이다.
                                # 캡을 걷어내려면 캡 전 원값이 필요한데 min()이 덮어써서
                                # 로그에 남지 않는다. 판정은 그대로 두고 원값만 기록한다.
                                # R1(ChatGPT 크로스체크 보강): pre 값만으로는 그것이 주
                                # confidence를 그대로 물려받은 seed 값(L3256)인지 EEE 자체
                                # 재계산값(L3360)인지 구분 불가하다. seeded 필드로 분리하고,
                                # (symbol, bar) dedup을 위해 bar도 함께 기록한다.
                                _eee_pre_cap = float(eee_trust_score or 0.0)
                                _eee_seed_src = indicators.get("confidence", None)
                                _eee_seeded = int(
                                    _eee_seed_src is not None
                                    and safe_extract_scalar(_eee_seed_src, 0.0) != 0.0
                                )
                                if not _is_birth_valid_eee:
                                    eee_trust_score = min(float(eee_trust_score or 0.0), 0.29)
                                    logger.info(
                                        f"👁️ [EEE-CAP-ORIGIN] {symbol} "
                                        f"bar={indicators.get('current_candle_time', 'NA')} "
                                        f"pre={_eee_pre_cap:.3f} post={float(eee_trust_score):.3f} "
                                        f"seeded={_eee_seeded} "
                                        f"state={_glc_eee.get('state', 'NA')} dur={_glc_eee_dur} "
                                        f"would_pass={int(_eee_pre_cap >= 0.30)}"
                                    )

                            # ✅ 관측용 저장 (주 판사 confidence 절대 덮어쓰기 금지)
                            indicators['eee_confidence'] = eee_trust_score
                        else:
                            eee_trust_score = 0.0
                            eee_components = {}
                                            
                # 2) EEE 로그 출력 (진입 여부와 무관)
                
                # 🔧 Series → Scalar 안전 변환 함수
                def _to_scalar(v, default=0.0):
                    """pandas Series나 any 타입을 안전하게 float로 변환"""
                    try:
                        if v is None:
                            return float(default)
                        # Series면 마지막 값 사용 (safe_extract_scalar와 동일)
                        if hasattr(v, "iloc"):
                            return float(v.iloc[-1]) if len(v) > 0 else float(default)
                        if hasattr(v, "item"):  # pandas Series/Index
                            return float(v.item())
                        return float(v)
                    except (ValueError, TypeError, AttributeError):
                        return float(default)
                
                _er  = float(data_package.indicators.get('er_10', 0.0) or 0.0)
                _bp  = float(data_package.indicators.get('buying_pressure', 0.0) or 0.0)
                logger.debug(
                    f"[EEE-DEBUG] {symbol} | "
                    f"관측점수={eee_trust_score:.3f} | "
                    f"MA:{_to_scalar(eee_components.get('ma_signal')):.2f} "
                    f"Vol:{_to_scalar(eee_components.get('volume_signal')):.2f} "
                    f"Candle:{_to_scalar(eee_components.get('candle_entry_signal', 0.0)):.2f} "
                    f"ATR:{_to_scalar(eee_components.get('atr_signal')):.2f} "
                    f"ADX:{_to_scalar(eee_components.get('adx_signal')):.2f} | "
                    f"ER:{_er:.3f} "
                    f"dur={data_package.indicators.get('dur_5x10_bars','?')} "
                    f"bp={_bp:.3f} | "
                    f"체제={regime.name if hasattr(regime, 'name') else regime}"
                )                

            except Exception as e:
                logger.warning(f"[EEE-DEBUG] {symbol} 점수 계산 실패: {e}")
                eee_trust_score = 0.0
            
            # ======================================================
            # 🟢 TD-BASELINE ENTRY (초보 수준 추세 기준선)
            # 목적: 골든크로스만 따라가도 먹는 장 복구
            # ======================================================
            # ─────────────────────────────────────────────
            # 🔒 SOLEUS SSoT (Analysis → Strategy 소비)
            # - analysis.gajami_meta['soleus_zone'] = 단일 기준
            # - Strategy 독자 판정 금지 (우회 경로 제거됨)
            # ─────────────────────────────────────────────
            _gajami_meta = ((data_package.ma_data or {}).get("gajami_meta") or {})
            logger.debug(f"[SOLEUS-READ] {symbol} | soleus_zone={_gajami_meta.get('soleus_zone','MISSING')} constitution={_gajami_meta.get('gajami_constitution_ok','MISSING')}")
            analysis_soleus = bool(_gajami_meta.get("soleus_zone", False))
            logger.debug(
                f"[STRATEGY-CONSUME] {symbol} | "
                f"constitution={_gajami_meta.get('gajami_constitution_ok', '?')} "
                f"regime_gate={_gajami_meta.get('regime_gate_ok', '?')} "
                f"soleus={analysis_soleus}"
            )
            
            soleus_gajami = bool(analysis_soleus)

            # 🔒 SSoT LOCK: HARD-GATE에서 확정된 soleus 판정은 이후 절대 뒤집지 않는다
            soleus_gajami_ssot = bool(soleus_gajami)
            # 🔒 [J1-POSITIVE-PRODUCER-OWNER-0828]
            # soleus는 J1의 형상 컨텍스트, 실제 J1 긍정 판결은 judge1_gajami_pass다.
            # J1 전용 positive producer는 raw soleus가 아니라 이 판결값을 소비한다.
            _j1_vote_pass = bool(
                int(indicators.get("judge1_gajami_pass", 0) or 0)
            )
            allow_hardcut = not soleus_gajami_ssot  # J1 자체 HardCut 자격은 현행 유지
            # 🔒 [HARDCUT-JUDGE-LANE-SPLIT-0828]
            # J2/J3/J4 독립 PASS SSoT. J1(soleus) 부결이 이 판사들까지
            # 전역 차단하는 것을 막는 데만 사용하며 각 판사 판정 자체는 바꾸지 않는다.
            _j234_hardcut_pass = bool(
                int(indicators.get("judge2_osc_pass", 0) or 0)
                or int(indicators.get("judge3_cycle_pass", 0) or 0)
                or int(indicators.get("judge4_reversal_pass", 0) or 0)
            )
            # 🔒 [A-OWNER-LANE-SPLIT-0828]
            # HARDCUT-OWNER-AUDIT의 마지막 두 소비처(BREAKOUT_CONFIRM,
            # RANGING_DOWNTURN)도 J234 lane 분리 완료. 감사 helper는 역할 종료.
            # _j234_hardcut_pass는 J1 전용 법률의 전역 return 권한 회수에만 사용한다.
            
            # ======================================================
            # 📋 HARD-GATE → 최종판사 이관 (1단계)
            # ======================================================
            # 변경: 가자미근=False여도 즉시 차단하지 않음
            # → 점수 계산 진행 → 최종판사(신뢰도 AND 가자미근)에서 판정
            # ======================================================
            if not soleus_gajami:
                regime_name = regime.name if hasattr(regime, 'name') else str(regime)
                logger.debug(
                    f"📋 [HARD-GATE-LOG] {symbol} soleus=False | regime={regime_name} → 최종판사 이관"
                )
            
            # ======================================================
            # 🔒 PATCH-B3: 천장/어깨 절대 차단 (가자미근 우선 원칙)
            # ======================================================
            # - 목적: LPT 19:16처럼 높은 신뢰도(0.749)인데 천장/어깨인 경우 원천 차단
            # - 철학: 가자미근은 반드시 저점권 또는 고점 아님
            # - 정석: soleus=True여도 위치가 틀리면 "가짜 가자미근"
            # ======================================================
            gajami_meta_early = (data_package.ma_data or {}).get("gajami_meta") or {}
            
            # ✅ Analysis indicators['is_low_area'] 저장 확인 완료 (20260419)
            # 🔒 리스크 관리: 포지션 사이즈 초기화 (명시적 선언)
            # 가자미근이면 진입, 리스크는 사이즈로 조절
            position_size_ratio = 1.0
            
            # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
            # 🔥 SOLEUS-BOX-GUARD 제거 (2026-02-16)
            # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
            # 제거 사유:
            # - 박스는 상대적 계산 (lookback 의존)
            # - 신고점 갱신 중에도 pos=1.00 오판
            # - 구조적 신호 아닌 계산 산물
            # - BIRTH 초입 차단 (SOL 21:29)
            # - Lifecycle(rise_pct, ma10_slope)이 실제 구조
            # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

            # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
            # 🔥 TOP-SOFT 제거 (2026-02-16)
            # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
            # 제거 사유:
            # - not_near_high 판정 오류 (박스 기반)
            # - 포지션 사이즈는 신뢰도+ATR로 관리
            # - Lifecycle 단독 판사 원칙
            # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
            
            # 🔒 리스크 조절: 고위험 패턴 → 강한 포지션 축소 (차단 아님)
            # 철학: 가자미근이면 진입, 위험은 사이즈로 관리
            # 1) 체제 BREAKDOWN
            # 2) DEAD_BOUNCE (급락 후 기술반등)
            # 3) 역가자미근 (하향 부채꼴)
            if regime == MarketRegime.BREAKDOWN:
                position_size_ratio = min(position_size_ratio, 0.30)
                indicators["position_size_ratio"] = position_size_ratio
                self.last_rejection_reason = "REGIME_BREAKDOWN_SOFT"
                logger.warning(
                    f"⚠️ [REGIME-BREAKDOWN-SOFT] {symbol} BREAKDOWN 관측 → 진입 유지, 포지션 30% 축소 | "
                    f"ratio={position_size_ratio:.2f}"
                )

            dead_bounce_flag = bool(indicators.get("dead_bounce", False) or indicators.get("dead_bounce_alert", False))
            if dead_bounce_flag:
                position_size_ratio = min(position_size_ratio, 0.30)
                indicators["position_size_ratio"] = position_size_ratio
                self.last_rejection_reason = "DEAD_BOUNCE_SOFT"
                logger.warning(
                    f"⚠️ [DEAD-BOUNCE-SOFT] {symbol} DEAD_BOUNCE 관측 → 진입 유지, 포지션 30% 축소 | "
                    f"ratio={position_size_ratio:.2f}"
                )

            # 역가자미근(하향 부채꼴): 형상 반대이므로 강한 축소
            ma5_abs = safe_extract_scalar(indicators.get("sma_5", indicators.get("ma5", 0.0)))
            ma10_abs = safe_extract_scalar(indicators.get("sma_10", indicators.get("ma10", 0.0)))
            ma20_abs = safe_extract_scalar(indicators.get("sma_20", indicators.get("ma20", 0.0)))
            ma5_slope_abs = safe_extract_scalar(indicators.get("ma5_slope", 0.0))

            if (ma5_abs > 0 and ma10_abs > 0 and ma20_abs > 0 and
                current_price < ma5_abs < ma10_abs < ma20_abs and ma5_slope_abs < 0):
                position_size_ratio = min(position_size_ratio, 0.30)
                indicators["position_size_ratio"] = position_size_ratio
                self.last_rejection_reason = "REVERSE_GAJAMI_SOFT"
                logger.warning(
                    f"⚠️ [REVERSE-GAJAMI-SOFT] {symbol} 역가자미근 관측 → 진입 유지, 포지션 30% 축소 | "
                    f"ratio={position_size_ratio:.2f} | "
                    f"price<{ma5_abs:.2f}<{ma10_abs:.2f}<{ma20_abs:.2f} slope5={ma5_slope_abs:.6f}"
                )

            # ======================================================
            # 🔒 signal 변수 초기화 (공통 파이프라인 합류용)
            # ======================================================
            signal = None
            
            try:
                ma5 = safe_extract_scalar(indicators.get("sma_5", indicators.get("ma5", 0.0)))
                ma10 = safe_extract_scalar(indicators.get("sma_10", indicators.get("ma10", 0.0)))
                ma20 = safe_extract_scalar(indicators.get("sma_20", indicators.get("ma20", 0.0)))
                ma20_slope = safe_extract_scalar(indicators.get("ma20_slope", 0.0))
                vol_ratio = safe_extract_scalar(indicators.get("volume_ratio", 0.0))
                
                # 🔒 TD-BASELINE 품질 게이트 (POL 오진입 대응)
                # ① EEE 최소 신뢰도: 시스템 종합 판단이 극도로 낮으면 차단
                _td_eee = float(indicators.get("eee_confidence", 0.0) or 0.0)
                _TD_EEE_MIN = 0.35
                
                # ② 확정 캔들 거래량: RT 스파이크 부풀리기 방지
                _td_vol_smooth = float(indicators.get("volume_ratio_smooth", vol_ratio) or vol_ratio)
                _TD_VOL_MIN = 0.60
                
                # ③ 골든크로스 최소 개수: 데드 압도 상태 차단
                _td_gc = int(indicators.get("golden_count", 0) or 0)
                _TD_GC_MIN = 4

                # 🔒 [ENTRY-FIXED-BOX-PURGE-0828]
                # TD baseline은 fixed BOX 위치/높이를 소비하지 않는다.
                _gap_10_20_pct = (ma10 - ma20) / ma20 * 100 if ma20 > 0 else 0.0
                _td_lc = (
                    indicators.get("gajami_lifecycle")
                    or ((data_package.ma_data or {}).get("gajami_meta") or {}).get("gajami_lifecycle")
                    or {}
                )
                try:
                    _td_raw_dur = int(dur_5x10) if dur_5x10 is not None else 999
                except Exception:
                    _td_raw_dur = 999
                try:
                    _td_lc_src = (
                        _td_lc.get("dur", None)
                        if isinstance(_td_lc, dict)
                        else None
                    )
                    _td_birth_dur = int(_td_lc_src) if _td_lc_src is not None else _td_raw_dur
                except Exception:
                    _td_birth_dur = _td_raw_dur

                _common_td_ok = (
                    ma5 > ma10 and
                    ma20_slope > 0 and
                    vol_ratio >= 1.0 and
                    _td_birth_dur <= SoleusGajamiConstants.NEWBORN_MAX_DUR
                )
                _pullback_ok = (      # 2번 눌림형: 완전 정배열 + 중기 이격
                    ma10 > ma20 and
                    _gap_10_20_pct >= 0.30
                )
                _sideways_observe = (  # 관측 전용: 수렴~약역전, TD 진입 허용 금지
                    _gap_10_20_pct >= -0.10 and
                    _gap_10_20_pct < 0.30
                )
                td_baseline_ok = _common_td_ok and _pullback_ok

                logger.info(
                    f"📐 [TD-BASELINE] {symbol} | "
                    f"ok={td_baseline_ok} "
                    f"common={_common_td_ok} "
                    f"pullback={_pullback_ok} "
                    f"sideways_observe={_sideways_observe} | "
                    f"gap10_20={_gap_10_20_pct:+.3f}% "
                    f"dur={_td_birth_dur}(raw={dur_5x10}) "
                    f"ma10>ma20={ma10>ma20}"
                )

                # 🔒 TD-BASELINE 보조화
                # → 가자미근(soleus_gajami) 단일 Gate를 반드시 통과해야 진입 가능
                # 🔒 [J1-POSITIVE-PRODUCER-OWNER-0828]
                # TD_BASELINE_SOLEUS는 J1 positive producer.
                # J1 판결이 0이면 신호를 만들지 않고 후단 공통/J234 경로로 내려간다.
                if (
                    td_baseline_ok
                    and soleus_gajami
                    and _j1_vote_pass
                ):
                    
                    # 🔒 품질 게이트 (EEE/거래량/골든비율)
                    _td_block_reason = None
                    if _td_eee < _TD_EEE_MIN:
                        _td_block_reason = f"EEE={_td_eee:.3f}<{_TD_EEE_MIN}"
                    elif _td_vol_smooth < _TD_VOL_MIN:
                        _td_block_reason = f"vol_smooth={_td_vol_smooth:.2f}<{_TD_VOL_MIN}"
                    elif _td_gc < _TD_GC_MIN:
                        _td_block_reason = f"GC={_td_gc}<{_TD_GC_MIN}"
                    
                    if _td_block_reason:
                        logger.info(
                            f"⚠️ [TD-BASELINE-SKIP] {symbol} 품질 미달 → 숏컷 미적용 | "
                            f"{_td_block_reason} | "
                            f"EEE={_td_eee:.3f} vol_s={_td_vol_smooth:.2f} GC={_td_gc}"
                        )
                    else:
                        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
                        # 🔒 GAJAMI-DIVERGENCE-BLOCK: 수렴만 허용, 발산 차단
                        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
                        _s5_div = float(safe_extract_scalar(
                            indicators.get("ma5_slope", 0.0), 0.0
                        )) / 100.0  # pct → ratio
                        _gap_5_10 = abs(ma5 - ma10) / ma10 if ma10 > 0 else 0.0

                        # 🔒 체제별 허용 dur 기준 (SSoT: bot6)
                        # TRENDING_DOWN: 하락 체제 재GC는 탄생봉(dur=1)만 허용
                        # TRENDING_UP/RANGING: 탄생~종아리(dur≤2) 허용
                        if regime == MarketRegime.TRENDING_DOWN:
                            _dur_max = 1
                        else:
                            _dur_max = 2   # TRENDING_UP, RANGING 동일 (RANGING=3은 검증 후)

                        # 🔒 [JUDGE-OWNER-LANE-SPLIT-0828]
                        # TD-BASELINE 발산·지연은 J1/TD lane 전용 자격검사다.
                        # J1/TD가 실패하면 TD 신호는 만들지 않는다(가짜 PASS 방지).
                        # J2/J3/J4가 살아 있으면 이 실패로 전역 return하지 않고
                        # _build_entry_signal() 공통 경로에 판결권을 넘긴다.
                        _td_j1_block_reason = None
                        # 🔒 [UNBLOCK-DIVERGENCE-1031] R819 : 막힌 24 건 +0.277 % · 통과 대비 +0.546 %p [+0.066, +0.997] → config.ini [STRATEGY] gajami_divergence_block_enabled (기본 true = 현행 차단 · false = 해제) · TD_LATE_ENTRY_BLOCK 은 그대로
                        _div_hit = _s5_div > 0.005 or _gap_5_10 > 0.005
                        _div_off = _div_hit and not self.config.getboolean("STRATEGY", "gajami_divergence_block_enabled", fallback=True)
                        if _div_off:
                            logger.info(f"🟢 [UNBLOCK-DIVERGENCE] {symbol} s5_div={_s5_div:.4f} gap_5_10={_gap_5_10:.4f} 차단 꺼짐 → 진행")
                        if _div_hit and not _div_off:
                            _td_j1_block_reason = "GAJAMI_DIVERGENCE_BLOCK"
                        elif _td_birth_dur > _dur_max:
                            _td_j1_block_reason = "TD_LATE_ENTRY_BLOCK"

                        if _td_j1_block_reason is not None:
                            if not _j234_hardcut_pass:
                                _td_j1_block_detail = (
                                    f"s5={_s5_div:.5f}(max=0.005) "
                                    f"gap={_gap_5_10:.4f}(max=0.005)"
                                    if _td_j1_block_reason == "GAJAMI_DIVERGENCE_BLOCK"
                                    else
                                    f"dur={_td_birth_dur}(raw={dur_5x10}) "
                                    f"> max={_dur_max}"
                                )
                                logger.info(
                                    f"🚫 [{_td_j1_block_reason}] {symbol} | "
                                    f"{_td_j1_block_detail} | "
                                    f"regime={regime.value if hasattr(regime,'value') else regime}"
                                )
                                self.last_rejection_reason = _td_j1_block_reason
                                return None
                            logger.info(
                                f"🟡 [TD-J1-LANE-DENY] {symbol} | "
                                f"reason={_td_j1_block_reason} → "
                                f"J1/TD lane만 탈락 · J2/J3/J4 판결권 유지"
                            )
                        else:
                            # 🔧 감지시각 = 실시간 감지 시각
                            indicators["detect_candle_time"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                            
                            # 🔧 gajami_lifecycle 전달 (Bot rise 로그용)
                            gmeta = (data_package.ma_data or {}).get("gajami_meta") or {}
                            glc = gmeta.get("gajami_lifecycle")
                            if isinstance(glc, dict):
                                indicators["gajami_lifecycle"] = glc
                            
                            logger.debug(
                                f"🕐 [감지시각설정] {symbol} TD-BASELINE+SOLEUS 판결 PASS → 감지시각={indicators['detect_candle_time']}"
                            )
                            
                            logger.info(
                                f"🟢 [TD-BASELINE+SOLEUS] {symbol} 골든크로스 추세 + 가자미근 진입 | "
                                f"MA5>{ma5:.2f}, MA10>{ma10:.2f}, MA20>{ma20:.2f} | "
                                f"slope20={ma20_slope:.4f} | vol={vol_ratio:.2f}x | "
                                f"EEE={_td_eee:.3f} vol_s={_td_vol_smooth:.2f} GC={_td_gc}"
                            )

                            # TD-BASELINE+SOLEUS 확정 → Bot RANGING-Weak 면제 보장
                            indicators['gajami_entry'] = True

                            signal = TradingSignal(
                                action="BUY",
                                symbol=symbol,
                                price=current_price,
                                confidence=_td_eee,
                                market_regime=regime,
                                reason="TD-BASELINE+SOLEUS",
                                indicators=indicators
                            )

                            # (TD-BASELINE GAJAMI-LATE 차단 제거로 관련 변수 정리)
                            # 🔒 TD-BASELINE 변수 계산 (히스토리 참조용)
                            # _td_rally = int(indicators.get("rally_duration", 0) or 0)
                            # _td_late = (
                            #     _td_rally >= 60 or
                            #     _td_gc >= 5
                            # )
                            # _td_compressed = False

                            # _td_gap_score = float(
                            #     (data_package.ma_data or {}).get('ma_gap_score', 0.0) or 0.0
                            # )
                            # _TD_FRESH_GC_MIN = 0.90
                            # _td_fresh_gc = (_td_gap_score >= _TD_FRESH_GC_MIN)

                            # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
                            # TD-BASELINE GAJAMI-LATE 차단 제거
                            # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
                            # 이유: GAJAMI-LIFECYCLE-GATE가 초반에 단일 차단 담당
                            # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

                            logger.info(
                                f"🔒 [FINAL-ENTRY] {symbol} 최종 진입 후보 | reason=TD-BASELINE+SOLEUS → SINGLE_EXIT"
                            )
                            final_signal = signal
                            entry_type = "TD_BASELINE_SOLEUS"
                            indicators["entry_type"] = entry_type
                    
                elif td_baseline_ok and not soleus_gajami:
                    # TD-BASELINE 조건 충족하지만 가자미근 미통과 → 로그만
                    logger.debug(
                        f"🟡 [TD-BASELINE-SKIP] {symbol} 가자미근 미통과 → 진입 보류 | "
                        f"MA5>{ma5:.2f}, MA10>{ma10:.2f}, MA20>{ma20:.2f}"
                    )

            except Exception as e:
                logger.debug(f"[TD-BASELINE] {symbol} 계산 오류: {e}")
            
            # 🔒 SENTINEL 기반 핵심값 검증 (HARD BLOCK)
            # ✅ confidence 계산 후 체크 (PATCH ⑩)
            # - 기존엔 confidence가 비어있는 상태에서 SENTINEL 체크가 먼저 실행되어
            #   ANALYSIS_CORE_INVALID(SENTINEL) 폭증 및 신뢰도 부족 0건 현상을 유발했다.
            # - 이제 indicators['confidence']가 채워진 뒤에 최종 검증한다.
            
            # 🔒 SENTINEL 검증 - 개별 사유 구분 (PATCH ⑩-2 개선)
            # 뭉뚱그리지 않고 첫 번째 발견된 문제를 명확히 차단
            # → 리포트에서 신규상장/데이터부족/계산실패/구조버그 구분 가능
            
            # Box-Override 블록 제거 - lifecycle만 사용
            if signal is not None:
                logger.info(f"⚠️ [PIPELINE-SKIP] {symbol} pre-signal 생성 감지 → 즉시반환 금지 | reason={signal.reason}")
                signal = None

            # ============================================================
            # 🔒 Analysis Core Invalid - SENTINEL 상세 원인 기록
            # ============================================================
            confidence = indicators.get('confidence', SENTINEL_FLOAT)
            atr_pct = indicators.get('atr_pct', SENTINEL_FLOAT)
            volume_ratio = indicators.get('volume_ratio', SENTINEL_FLOAT)            
            
            # ======================================================
            # 🔒 PATCH-B7: NoneType 비교 방어 (신호분석 안정화)
            # ======================================================
            # 목적: DOGE, SHIB 등 초미세 가격대 종목에서 TypeError 방지
            # 철학: "None은 0.0으로 안전하게 변환, SENTINEL은 유지"
            # ======================================================
            atr_pct = safe_extract_scalar(atr_pct, 0.0)
            volume_ratio = safe_extract_scalar(volume_ratio, SENTINEL_FLOAT)
            confidence = safe_extract_scalar(confidence, 0.0)
            
            # 🧩 BOX 하단 예외: 박스 하단이면 "판사(confidence) 공백" 때문에 재판 자체가 죽으면 안 됨
            # - Box-SSoT가 BOTTOM/하단을 확정했을 때만 적용
            # - 목적: Box-Override/Box-Entry 로직이 실행될 기회를 확보
            # box_bottom_candidate 제거 - pass만 하고 사용처 없음
            
            invalid_codes = []
            
            # 핵심값 미계산/무효 → PRECHECK
            # 🔒 판사(confidence)는 Strategy 산출물 → 여기서 하드블록 금지
            # PRECHECK는 원자료(ATR 등) 무효만 차단
            if is_sentinel(atr_pct):
                invalid_codes.append("ANALYSIS_INVALID_PRECHECK")
            
            # 거래량 판단 불가/무의미 → VOLUME
            if is_sentinel(volume_ratio):
                invalid_codes.append("ANALYSIS_INVALID_VOLUME")
            
            if invalid_codes:
                analysis_reason = ','.join(sorted(set(invalid_codes)))
                self.last_rejection_reason = analysis_reason
                logger.info(
                    f"🚫 [HARD-BLOCK] {symbol} Analysis 핵심값 무효 → 진입 차단 | "
                    f"reason={analysis_reason}"
                )
                return None
            
            # ============================================================================
            # 🔥 거래량 TIER 필터 (3단 구조) - config.ini 기반
            # ============================================================================
            # TIER 1: BTC/ETH 완전 제외
            # TIER 2: 고가·저유동성 코인 (8만원 이상) → vol >= 0.45
            # TIER 3: 일반 알트 코인 → vol >= 0.30
            # ============================================================================

            # TIER 설정 읽기 (config.ini [VOLUME])
            if self.config.has_section('VOLUME'):
                exclude_tier1_str = self.config.get('VOLUME', 'exclude_tier1', fallback='')
                high_price_threshold = self.config.getfloat('VOLUME', 'high_price_threshold', fallback=80000.0)
                high_price_vol_min = self.config.getfloat('VOLUME', 'high_price_vol_ratio_min', fallback=0.45)
                normal_vol_min = self.config.getfloat('VOLUME', 'normal_vol_ratio_min', fallback=0.30)
                # 🆕 메이저 코인(BTC/ETH) 절대 거래대금 최소 기준 (3m 1캔들 기준)
                major_min_krw = self.config.getfloat('VOLUME', 'major_min_krw_3m', fallback=50_000_000.0)  # 5천만원
            else:
                exclude_tier1_str = ''
                high_price_threshold = 80000.0
                high_price_vol_min = 0.45
                normal_vol_min = 0.30
                major_min_krw = 50_000_000.0

            # Tier1 완전 제외 코인 목록 파싱
            exclude_list = [m.strip() for m in exclude_tier1_str.split(',') if m.strip()]

            # 심볼 문자열 변환
            symbol_str = str(symbol) if symbol else ''

            # STEP 1: TIER 1 - BTC/ETH 특별 처리 (메이저 전용 모드)
            # 메이저(Tier1) 종목 여부 - SSoT: CONFIG [VOLUME].exclude_tier1
            exclude_tier1 = self.config.get_cached("exclude_tier1", [])
            is_tier1_btc_eth = symbol_str in exclude_tier1
            
            # 기존 완전 제외 목록 (BTC/ETH 제외)
            if symbol_str in exclude_list and not is_tier1_btc_eth:
                self.last_rejection_reason = f"TIER1 제외 ({symbol_str})"
                logger.info(f"🚫 [GlobalFilter] {symbol_str} TIER1(완전제외) → 진입 금지")
                return None

            # 🚫 [2026-08-04] 저가 구간 실측 체결비용 하드 게이트
            #  가격 <99.2원  n=183(40.0%)  실현 -0.531%  승률 19.1%  [전체 -0.396%]
            #  매수 슬리피지 ~99원 +0.1289% vs 99~255원 +0.0272%
            #  사유명은 SPREAD(추정) 아닌 EXECUTION_COST(실측 슬리피지·체결비용) 사용
            _low_px_thr = self.config.getfloat(
                'GLOBAL_FILTER',
                'low_price_threshold',
                fallback=0.0,
            )
            if _low_px_thr > 0.0 and 0.0 < current_price < _low_px_thr:
                self.last_rejection_reason = "LOW_PRICE_EXECUTION_COST"
                logger.info(
                    f"🚫 [GlobalFilter] {symbol_str} 저가배제 | "
                    f"가격={current_price:,.4g}원 < {_low_px_thr:,.4g}원 "
                    f"→ 실측 체결비용 과다·진입 금지"
                )
                return None

            # [D1/D2 ENTRY-BLOCK] 3분봉 공통 매수 차단
            _tick_size = (
                get_tick_size(current_price)
                if current_price and current_price > 0 else 0.0
            )
            _tick_pct = (
                _tick_size / current_price * 100.0
                if _tick_size > 0 else 0.0
            )
            indicators["tick_execution_cost_pct"] = float(_tick_pct)
            indicators["tick_execution_cost_high"] = int(_tick_pct >= 0.50)

            _entry_block_tf = (
                self.timeframe.value
                if hasattr(self.timeframe, "value") else str(self.timeframe)
            )
            if _entry_block_tf == "3m":
                _atr_ticks = (
                    atr_pct / 100.0 * current_price / _tick_size
                    if _tick_size > 0 and atr_pct > 0 else 0.0
                )
                indicators["atr_ticks"] = float(_atr_ticks)
                _min_atr_ticks = self.config.getfloat(
                    "GLOBAL_FILTER", "min_atr_ticks", fallback=3.0
                )
                if _min_atr_ticks > 0.0 and _atr_ticks < _min_atr_ticks:
                    _atr_code = "ATR_TICKS_ZERO" if _atr_ticks <= 0.0 else "ATR_TICKS_LOW"
                    self.last_rejection_reason = _atr_code
                    logger.info(
                        f"🚫 [GlobalFilter-ATR-TICKS] {symbol_str} code={_atr_code} | "
                        f"ATR={_atr_ticks:.3f}틱 < {_min_atr_ticks:.3f}틱 | "
                        f"atr_pct={atr_pct:.4f}% · 1틱={_tick_pct:.4f}%"
                    )
                    return None

                _ma20_far_gate = self.config.getboolean(
                    "GLOBAL_FILTER", "block_ma20_far_above", fallback=True
                )
                if _ma20_far_gate and int(indicators.get("ma20_far_above", 0) or 0) == 1:
                    self.last_rejection_reason = "MA20_FAR_ABOVE"
                    logger.info(
                        f"🚫 [GlobalFilter-MA20-FAR] {symbol_str} code=MA20_FAR_ABOVE | "
                        f"완성봉 저가 > MA20 · "
                        f"이격={float(indicators.get('ma20_dev_closed', 0.0) or 0.0) * 100:.3f}% > "
                        f"직전 160봉 중앙={float(indicators.get('ma20_dev_med160', 0.0) or 0.0) * 100:.3f}%"
                    )
                    return None

            if _tick_pct >= 0.50:
                logger.info(
                    f"👁️ [GlobalFilter-TICK-OBSERVE] {symbol_str} "
                    f"1틱={_tick_pct:.2f}% | 가격={current_price:,.4f}원 "
                    f"→ 1틱 비율 관측"
                )

            # STEP 2: TIER 분류 및 거래량 체크
            # 🔒 [VOL-CONTRACT-FIX-0814] 판단=volume_ratio, smooth=관측/점수 전용.
            volume_ratio_smooth = indicators.get('volume_ratio_smooth')
            volume_ratio = safe_extract_scalar(indicators.get('volume_ratio', 0))
            volume_ratio = float(volume_ratio) if volume_ratio is not None else 0.0
            vol_check = volume_ratio
            vol_check_smooth_obs = (
                float(volume_ratio_smooth)
                if volume_ratio_smooth is not None
                else volume_ratio
            )

            # 🔧 vol_ratio_min 기본값 초기화 (UnboundLocalError 방지)
            # - 모든 실행 경로에서 변수 정의 보장
            # - 이후 TIER별/조건별로 값 조정
            vol_ratio_min = normal_vol_min  # 기본값: 0.30
            
            # 🔰 TIER 1: BTC/ETH 메이저 (별도 로직)
            if is_tier1_btc_eth:
                # 🔥 메이저는 0.30x 고정 (normal_vol_min 사용)
                tier_label = "TIER1(BTC/ETH)"
                vol_ratio_min = normal_vol_min  # 0.30x

                logger.debug(
                    f"✅ [GlobalFilter] {symbol_str} {tier_label} 통과 | "
                    f"vol={fmt(vol_check, '.2f')}x >= {fmt(vol_ratio_min, '.2f')}x | 가격={current_price:,.0f}원"
                )

            # 🔰 TIER 2/3: 알트코인 (가격 기반 분류)
            # TIER 2: 고가(>=80,000원) → 저유동성 위험 → 0.15x
            # TIER 3: 일반(<80,000원) → 0.30x (기본)

            # 🔧 심볼 기반 변수 통일 (UnboundLocalError 방지)
            tier_label = "미분류"  # 기본값

            # 📌 체제별 거래량 "상한선" (공통 적용)
            # 목적: 다크풀 함정 차단 (이상 과열 진입 방지)
            # 적용: 모든 체제 (메이저 포함)
            max_volume_for_regime = None

            if regime == MarketRegime.TRENDING_DOWN:
                max_volume_for_regime = 12.0  # TD 중 과열 차단
            elif regime == MarketRegime.BREAKDOWN:
                max_volume_for_regime = 8.0   # BD 중 패닉 차단

            # 🔰 TIER 2/3 하한선 설정
            # 로직: 가격으로 먼저 분류 → 이후 체제별 보정 가능
            if not is_tier1_btc_eth:
                # 🧩 기존 Tier2/3 로직 유지
                if current_price >= high_price_threshold:
                    vol_ratio_min = high_price_vol_min  # TIER 2: 고가·저유동성
                    tier_label = f"TIER2(고가≥{high_price_threshold:,.0f}원)"
                    
                    # ============================================================
                    # 🔥 TIER2 전용 거래량 완화 정책 (기술 신호 우선)
                    # ============================================================
                    # 철학: TIER2는 거래량 터지기 전에 기술 신호로 선진입
                    # 적용: golden_cnt ≥ 6 → 0.20x, ≥ 8 → 0.15x
                    # ============================================================
                    
                    # ============================================================
                    # 🔥 TIER2 전용 거래량 완화 (Primary 골크 + 지속성 기반)
                    # ============================================================
                    # - golden_count(총 10개 합산)는 "정렬 완료(늦음)" 구간에서 과대해짐
                    # - 3m 초입 진입은 Primary(5개) 기준이 정합
                    # - 지속성(초)로 순간 스냅샷 오탐 방지
                    # ============================================================

                    primary_cnt = int(safe_extract_scalar(indicators.get("golden_primary_count", 0), default=0))
                    
                    # ============================================================
                    # 🔥 A + C: 지속시간 동적 조정 + 체제별 차등
                    # ============================================================
                    
                    # 1) 변동성 기반 기본 지속시간 (A)
                    #    - ATR이 높을수록 빠른 확인
                    atr_pct = float(indicators.get("atr_pct", 0.0) or 0.0)
                    
                    if atr_pct >= 1.2:
                        base_required_sec = 30
                    elif atr_pct >= 0.6:
                        base_required_sec = 60
                    else:
                        base_required_sec = 90
                    
                    # 2) 체제별 차등 (C)
                    #    - 상승은 완화, 횡보는 중립, 하락은 엄격
                    if regime == MarketRegime.TRENDING_UP:
                        required_sec = base_required_sec
                    elif regime == MarketRegime.RANGING:
                        required_sec = int(base_required_sec * 1.5)
                    else:  # TRENDING_DOWN
                        # TD→TU 전환 초입은 완화
                        if indicators.get("regime_early", False):
                            required_sec = base_required_sec
                        else:
                            required_sec = int(base_required_sec * 2)
                    
                    # 3) Primary 골크 지속성 충족 개수 재산정
                    #    - 기존 primary_stable_60(고정 60s) 대신 동적 기준 사용
                    primary_durations = indicators.get("golden_duration_by_key_sec", {})
                    
                    # Primary 조합만 필터링
                    PRIMARY_KEYS = ("5x10", "5x20", "10x20", "20x60", "60x120")
                    primary_stable_dyn = sum(
                        1 for k in PRIMARY_KEYS
                        if k in primary_durations and primary_durations.get(k, 0.0) >= required_sec
                    )
                    
                    # 최소 지속시간 (로그용)
                    primary_min_dur = min(
                        (primary_durations.get(k, 0.0) for k in PRIMARY_KEYS if k in primary_durations),
                        default=0.0
                    )
                    
                    logger.info(
                        f"🧭 [DYN-GC] {symbol_str} "
                        f"atr={atr_pct:.2f}% regime={regime.name} "
                        f"req={required_sec}s primary_stable={primary_stable_dyn}/{primary_cnt}"
                    )
                    
                    # 4) TIER2 거래량 완화 결정 (기존 규칙 유지, 기준만 동적)
                    if primary_cnt >= 4 and primary_stable_dyn >= 2:
                        vol_ratio_min = 0.15
                        logger.info(
                            f"🔥 [TIER2-PRIMARY4] {symbol_str} primary={primary_cnt}, stable_dyn={primary_stable_dyn}, "
                            f"min_dur={primary_min_dur:.0f}s → vol_min 0.15x"
                        )
                    elif primary_cnt >= 3 and primary_stable_dyn >= 1:
                        vol_ratio_min = 0.20
                        logger.info(
                            f"🔥 [TIER2-PRIMARY3] {symbol_str} primary={primary_cnt}, stable_dyn={primary_stable_dyn}, "
                            f"min_dur={primary_min_dur:.0f}s → vol_min 0.20x"
                        )

                    # - 가자미근 초입 여부는 Analysis.soleus_zone으로만 판단
                    # - TIER2 거래량 완화는 primary_cnt 기반 로직으로 충분
                    
                else:
                    vol_ratio_min = normal_vol_min      # TIER 3: 일반 알트
                    tier_label = "TIER3(일반알트)"

                # BOTTOM_WAITING 블록 제거 - "BOTTOM_WAITING" 설정하는 곳 없음

            # 🔒 [VOLUME-LAW-SSOT-0823]
            # 헌법 계약: 거래량 부족은 Soft Penalty이며 단독 Hard Veto가 아니다.
            # 여기서는 하한 미달을 관측만 하고, 후단 수급 penalty 단일 경로에 위임한다.
            if vol_check < vol_ratio_min:
                logger.info(
                    f"⚠️ [GlobalFilter-Volume-OBSERVE] {symbol_str} {tier_label} 거래량 부족 | "
                    f"vol={vol_check:.2f}x < 기준={vol_ratio_min:.2f}x | "
                    f"smooth_obs={vol_check_smooth_obs:.2f}x → hard block 없음"
                )
                
            # 메이저(BTC/ETH)는 위에서 이미 통과/차단 결정
                        
            # 🆕 불장 모드 강도 계산 (2025-11-26)
            # 🆕 불장 모드 강도 계산 (개별 종목 중심, 2025-12-06)
            # 철학:
            #   - 개별 종목 불장(symbol_bull_level)은 항상 계산
            #   - BTC는 개별 불장 후보에서 제외
            #   - global_bull_mode는 보너스만
            symbol_str = str(symbol) if symbol else ""
            global_bull_mode = indicators.get('global_bull', False)
            symbol_bull_level = 0
            bull_strength = 0.0
            
            # 🔥 개별 종목 불장은 항상 계산 (BTC 제외)
            try:
                # BTC는 개별 불장 판단에서 제외
                if symbol_str != "KRW-BTC":
                    # 4H 데이터 추출
                    regime_4h = indicators.get('regime_4h', MarketRegime.RANGING)
                    adx_4h = safe_extract_scalar(indicators.get('adx_4h', 0))
                    atr_4h = safe_extract_scalar(indicators.get('atr_4h', 0))
                    change_24h = safe_extract_scalar(indicators.get('change_24h_pct', 0.0))
                    
                    # 4H ATR을 %로 변환
                    price_4h = indicators.get('price_4h', current_price)
                    if gt(price_4h, 0) and atr_4h > 0:
                        atr_pct_4h = (atr_4h / price_4h) * 100.0
                    else:
                        atr_pct_4h = 0.0
                    
                    # Level 3: 강한 심볼 불장
                    if (regime_4h == MarketRegime.BREAKOUT and
                        adx_4h >= SYMBOL_BULL_LEVEL_3['adx_min'] and
                        atr_pct_4h >= SYMBOL_BULL_LEVEL_3['atr_min'] and
                        change_24h >= SYMBOL_BULL_LEVEL_3['change_24h_min']):
                        symbol_bull_level = 3
                    
                    # Level 2: 보통 심볼 불장
                    elif (regime_4h in [MarketRegime.TRENDING_UP, MarketRegime.BREAKOUT] and
                          adx_4h >= SYMBOL_BULL_LEVEL_2['adx_min'] and
                          atr_pct_4h >= SYMBOL_BULL_LEVEL_2['atr_min'] and
                          change_24h >= SYMBOL_BULL_LEVEL_2['change_24h_min']):
                        symbol_bull_level = 2
                    
                    # Level 1: 약한 심볼 불장
                    elif (regime_4h == MarketRegime.TRENDING_UP and
                          adx_4h >= SYMBOL_BULL_LEVEL_1['adx_min'] and
                          atr_pct_4h >= SYMBOL_BULL_LEVEL_1['atr_min'] and
                          change_24h >= SYMBOL_BULL_LEVEL_1['change_24h_min']):
                        symbol_bull_level = 1
                        
            except Exception as e:
                logger.debug(f"{symbol} 로컬 불장 레벨 계산 실패: {e}")
                symbol_bull_level = 0
            
            # 🔥 bull_strength: 개별 불장 우선, 글로벌은 보너스만
            base_strength = BULL_STRENGTH_SYMBOL_MULTIPLIER * symbol_bull_level
            if global_bull_mode and base_strength > 0:
                # 글로벌 불장일 때 base 보너스 추가
                bull_strength = min(1.0, base_strength + BULL_STRENGTH_BASE)
            else:
                bull_strength = base_strength
            
            # 로그 출력
            if symbol_bull_level > 0:
                logger.info(
                    f"🔥 [로컬불장] {symbol} | "
                    f"Lv={symbol_bull_level} | "
                    f"강도={bull_strength:.2f} | "
                    f"글로벌={'ON' if global_bull_mode else 'OFF'} | "
                    f"완화={'약간' if symbol_bull_level == 1 else '보통' if symbol_bull_level == 2 else '강함' if symbol_bull_level == 3 else '기본'}"
                )
            elif global_bull_mode:
                logger.debug(f"🛡️  [글로벌불장] {symbol} | 로컬Lv=0 | 기본 기준")
            else:
                logger.debug(f"🛡️  [보수모드] {symbol} | 불장=OFF | 엄격 기준")
            
            # 🔥 로컬 BULL_MARKET 승격 (오판 방지 강화)
            # 철학: 개별 종목이 강하면 해당 종목만 BULL_MARKET 모드
            # 조건: bull_strength >= 0.6 (레벨2 이상) + TRENDING_UP + 구조 확인
            # 효과: 박스 무시 + 완화 필터 적용 → 20251015 반복 매매
            # ✅ 오판 방지: 수급 순간 튐만으로 불장 승격 금지, 구조 + 방향성 필수
            
            # 구조 확인: 가자미근 + MA 정배열 + 상승 기울기
            alignment_level = int((data_package.ma_data or {}).get("alignment_level", 0) or 0)
            ma5 = safe_extract_scalar(indicators.get("ma5", indicators.get("sma_5", 0.0)))
            ma10 = safe_extract_scalar(indicators.get("ma10", indicators.get("sma_10", 0.0)))
            ma20 = safe_extract_scalar(indicators.get("ma20", indicators.get("sma_20", 0.0)))
            ma5_slope = safe_extract_scalar(indicators.get("ma5_slope", 0.0))
            
            bull_structure_ok = (
                _soleus_ssot(indicators, data_package.ma_data)
                and ma5 > 0 and ma10 > 0 and ma20 > 0
                and (ma5 > ma10 > ma20 or alignment_level >= 3)
                and ma5_slope >= 0.0
            )
            
            is_local_bull = (
                bull_strength >= 0.6
                and regime == MarketRegime.TRENDING_UP
                and bull_structure_ok
            )
            
            if is_local_bull:
                # 🔒 Strategy는 체제를 "변경"하지 않는다. (Analysis SSoT 보존)
                indicators["local_bull_mode"] = True

                logger.info(
                    f"🔥 [{symbol}] LocalBullMode=ON (flag only) | "
                    f"strength={bull_strength:.2f} | Lv={symbol_bull_level}"
                )
            else:
                if bull_strength >= 0.6 and regime == MarketRegime.TRENDING_UP:
                    logger.info(
                        f"🚫 [BULL_UPGRADE_BLOCK] {symbol} "
                        f"bull_strength={bull_strength:.2f} 구조 미충족 → BULL_MARKET 승격 금지 | "
                        f"gajami={bool(indicators.get('gajami_zone', False))} "
                        f"align={alignment_level} "
                        f"ma5/10/20={ma5:.4f}/{ma10:.4f}/{ma20:.4f} "
                        f"slope5={ma5_slope:.6f}"
                    )
            
            # 📌 MA Trend Unified Filter (최종 통합 - 유일한 MA 필터)
            # 목적: 5개 분산 필터 → 1개 통합 (일관성 보장)
            # HardCut: strong_reverse(5<10<20) OR clear_reverse(5<20*0.99) OR slope≤-0.04%
            # SoftPenalty: 현재가 < MA5*0.997
            ma_data = data_package.ma_data or {}
            
            # 🚫 vFinal HARD GATE: 2단계 가자미근 검증 (최우선)
            # 철학: "진입은 가자미근에서만"
            # - Standard Gajami (확정형): 기존 조건, 풀사이즈 진입
            # - Preliminary Gajami (초입형): 완화 조건, 소액 진입
            # - 진흙 속에서 진주 찾기: 초입을 먹되 다치지 않게
            gajami_meta = ma_data.get('gajami_meta') or {}  # ✅ None 방어 (키 존재+None 포함)
            
            # - Strategy 내부 재계산/혼용 금지
            # - 단위: % 통일 (0.5 = 0.5%)
            # 🎯 가자미근 단일 Gate (Strategy 진입 판단)
            # 철학: "Analysis가 soleus_gajami=True라고 말하면 그대로 집행"
            # - Strategy는 더 이상 가자미근을 재판정하지 않음
            # - gajami_meta, is_candidate 등 중간 변수 제거
            # 🔒 SSoT LOCK 재사용: 이후 구간에서는 gajami_meta로 soleus를 재판정하지 않는다
            # (중간에 meta가 비거나 덮어씌워져도 HARD-GATE 판정이 진실)
            # ✅ Consumer: Producer 메타(gajami_meta) 수정 금지 (SSoT 침범 금지)
            soleus_gajami = bool(soleus_gajami_ssot)
            soleus_zone = soleus_gajami  # 🔧 별칭 초기화 (3733, 3772 등에서 사용)
            sideways_detected = bool(indicators.get("sideways_detected", False))

            # ❌ is_candidate 기반 Override 로직 전체 제거
            # 철학: "가자미근 아니면 진입 없음" (우회 경로 금지)
            # - soleus_gajami=False면 어떤 조건도 진입 불가
            # - Strategy는 판단하지 않고 Analysis를 신뢰
            
            # (로그/표시용) MA/거래량 값은 추출하되, 가자미근 판정에는 사용하지 않는다 (SSoT 준수)
            ma5 = safe_extract_scalar(indicators.get("sma_5", indicators.get("ma5", 0.0))) or 0.0
            ma10 = safe_extract_scalar(indicators.get("sma_10", indicators.get("ma10", 0.0))) or 0.0
            volume_ratio = safe_extract_scalar(indicators.get("volume_ratio", 0.0)) or 0.0

            # ✅ HARD-GATE (SSoT): 가자미근 판정은 Analysis 결과만 소비
            # 🆕 TD_KNEE 후보 포함 - 가자미근 계열 여부만 확인 (SSoT)
            analysis_gajami = bool(soleus_gajami_ssot)  # SSoT LOCK 사용
            
            # ❌ SSoT override / softfail 제거
            # → 가자미근(soleus_gajami) 외 예외 통과 금지
            
            # 📋 SAFETY-NET → 최종판사 이관 (1단계)
            # - HARD-GATE 제거로 soleus=False도 여기 도달 가능 (정상)
            # - 최종판사(신뢰도 AND 가자미근)에서 판정
            if not analysis_gajami:
                regime_str = regime.name if hasattr(regime, 'name') else str(regime)
                logger.debug(
                    f"📋 [SAFETY-NET-LOG] {symbol} soleus=False | regime={regime_str} → 최종판사 이관"
                )

            gajami_type = "확정"           
            regime_str = regime.name if hasattr(regime, 'name') else str(regime)
            
            # ✅ SSoT 원칙: Analysis 형상 판정 통과 시 즉시 진입 자격 부여
            # ✅ Consumer: Producer 플래그(soleus_final_locked/hard_gajami_pass/ignore_selling_pressure) 재설정 금지
            # - Strategy는 실행 플래그만 설정 (주문 단계에서 사용)
            indicators["gajami_entry"] = bool(analysis_gajami)
            
            if analysis_gajami:
                logger.info(
                    f"✅ [HARD-GATE] {symbol} Analysis 가자미근 통과 ({gajami_type}) | "
                    f"regime={regime_str}"
                )
            
            if ma_data:
                # MA 값은 indicators(SMA)에서 직접 취득
                # 🔒 완성 캔들 기준 MA 사용 (육안 일치)
                ma5 = safe_extract_scalar(
                    indicators.get("sma_5_closed", indicators.get("sma_5", indicators.get("ma5", 0.0)))
                )
                ma10 = safe_extract_scalar(
                    indicators.get("sma_10_closed", indicators.get("sma_10", indicators.get("ma10", 0.0)))
                )
                ma20 = safe_extract_scalar(
                    indicators.get("sma_20_closed", indicators.get("sma_20", indicators.get("ma20", 0.0)))
                )
                ma5_slope = safe_extract_scalar(ma_data.get("slope_ma5", 0.0))  # slope는 ma_data 유지
                
                if gt(ma5, 0) and ma10 > 0 and ma20 > 0:
                    # 🟢 무릎 구간: 모든 HardCut 무시
                    # 🔴 HardCut 조건 → 강도 기준 분리 (0.5% 기준)
                        strong_reverse = (lt(ma5, ma10) and ma10 < ma20)
                        clear_reverse = (lt(ma5, ma20 * 0.99))
                        
                        # 🔒 [A-OWNER-LANE-SPLIT-0828]
                        # 비가자미근 MA 역배열 법률은 J1 lane 소유.
                        # J2/J3/J4 독립 PASS를 전역차단하거나 감점하지 않는다.
                        if (
                            (strong_reverse or clear_reverse)
                            and not soleus_gajami_ssot
                            and not _j234_hardcut_pass
                        ):
                            # 🔒 역배열 강도 계산 (MA5 vs MA20 기준)
                            # NOTE: 이 단계의 MA 페널티는 '최종 안전판(HARD-GATE)' 역할
                            # - PATCH D(Line 3867)에서 제거한 중복 감점과 다른 책임 영역임
                            # - SSoT-OVERRIDE 2단계: 초입 보호 (RANGING/TU + GC≥4 면제)
                            # - HARD-GATE 3단계: 최종 방어선 (가자미근에서 최대 -15% 제한)
                            # - 설계 철학: "초입은 살리고, 후반은 보수적"
                            if ma5 > 0 and ma20 > 0:
                                gap_pct = abs((ma5 - ma20) / ma20 * 100.0)
                                _cp_raw = indicators.get("current_price", 0.0)
                                cur_price = float(safe_extract_scalar(_cp_raw, 0.0))
                                
                                # 미세 역배열(<0.5%) → 차단 해제, 위험도 관측
                                if gap_pct < 0.50:
                                    penalty_reasons.append(f"MA역배열-미세({gap_pct:.2f}%,관측)")
                                    logger.info(
                                        f"👁️ [MA-OBSERVE] {symbol} 미세 역배열 관측 | "
                                        f"gap={gap_pct:.2f}% < 0.5% | "
                                        f"MA5:{ma5:.1f} MA10:{ma10:.1f} MA20:{ma20:.1f} | "
                                        f"최종 신뢰도 감산 없음"
                                    )
                                # 🔥 강한 역배열(≥0.5%) → 차단 유지
                                # 🦵 PATCH F: 가자미근 구간에서는 SoftPenalty 전환 (2026-01-06)
                                else:
                                    # 가자미근 컨텍스트 확인
                                    # 묶음 v5: pre_gajami와 pre_gajami_candidate가 없어도 gajami_entry가 True이면 gajami_ctx는 True이다.
                                    gajami_ctx = bool(
                                        indicators.get("gajami_entry", False)
                                        or indicators.get("pre_gajami", False)
                                        or indicators.get("pre_gajami_candidate", False)
                                    )
                                    
                                    recent_low_break = bool(indicators.get("recent_low_break", False))
                                    ma5_slope_val = float(indicators.get("ma5_slope", 0.0) or 0.0)
                                    
                                    # 1) 무릎 + Soleus (기존 허용)
                                    # 2) 🆕 가자미근 초입 (완성 캔들 기준)
                                    if (
                                        gajami_ctx
                                        and not recent_low_break
                                        and ma5_slope_val >= 0.0
                                    ):
                                        penalty_factor = self._apply_soft_penalty(
                                            penalty_factor, 0.85, penalty_reasons,
                                            f"MA역배열-가자미EARLY({gap_pct:.2f}%,-15%)"
                                        )
                                        logger.info(
                                            f"⚠️ [MA-EARLY-SOFT] {symbol} MA 역배열(가자미근 초입) → 페널티 전환 | "
                                            f"gap={gap_pct:.2f}% | ma5_slope={ma5_slope_val:.5f} | "
                                            f"MA5:{ma5:.1f} MA10:{ma10:.1f} MA20:{ma20:.1f}"
                                        )
                                    
                                    # 3) A안: 가자미근 True는 후단 차단으로 뒤집지 않는다
                                    else:
                                        if soleus_gajami:
                                            penalty_factor = self._apply_soft_penalty(
                                                penalty_factor, 0.70, penalty_reasons,
                                                f"ReverseFan-Soft({gap_pct:.2f}%,*0.70)"
                                            )
                                            logger.info(
                                                f"⚠️ [ReverseFan-Soft] {symbol} 강한 역배열 관측 → 차단 금지, 페널티 | "
                                                f"gap={gap_pct:.2f}% | MA5:{ma5:.1f} MA10:{ma10:.1f} MA20:{ma20:.1f}"
                                            )
                                        else:
                                            self.last_rejection_reason = "MA 역배열 강한하락"
                                            logger.info(
                                                f"🚫 [MA-HardCut] {symbol} 강한 역배열 차단 | "
                                                f"gap={gap_pct:.2f}% ≥ 0.5% | "
                                                f"MA5:{ma5:.1f} MA10:{ma10:.1f} MA20:{ma20:.1f}"
                                            )
                                            return None
                            else:
                                # MA 값 없음 → 동일하게 가자미근 예외 처리
                                if not soleus_gajami_ssot:
                                    if soleus_gajami:
                                        penalty_factor = self._apply_soft_penalty(
                                            penalty_factor, 0.70, penalty_reasons,
                                            "ReverseFan-Soft(MA_missing,*0.70)"
                                        )
                                        logger.info(
                                            f"⚠️ [ReverseFan-Soft] {symbol} MA 값 누락/역배열 관측 → 차단 금지, 페널티 | "
                                            f"MA5:{ma5:.1f} MA10:{ma10:.1f} MA20:{ma20:.1f}"
                                        )
                                    else:
                                        self.last_rejection_reason = "MA 역배열 강한하락"
                                        logger.info(
                                            f"🚫 [MA-HardCut] {symbol} 강한 역배열 차단 | "
                                            f"MA5:{ma5:.1f} MA10:{ma10:.1f} MA20:{ma20:.1f} | "
                                            f"5<10<20:{strong_reverse} 5<20*0.99:{clear_reverse}"
                                        )
                                        return None
                        elif (
                            (strong_reverse or clear_reverse)
                            and not soleus_gajami_ssot
                            and _j234_hardcut_pass
                        ):
                            logger.info(
                                f"🟢 [MA-REVERSE-JUDGE-NEUTRAL] {symbol} "
                                f"비가자미근 MA 역배열 J1 lane 비적용 | "
                                f"strong={int(strong_reverse)} clear={int(clear_reverse)} "
                                f"J2={int(indicators.get('judge2_osc_pass', 0) or 0)} "
                                f"J3={int(indicators.get('judge3_cycle_pass', 0) or 0)} "
                                f"J4={int(indicators.get('judge4_reversal_pass', 0) or 0)}"
                            )
                        
                        # 🟡 MA5 머리 숙임 처리 (가자미근 핵심 원칙)
                        # - soleus_gajami(가자미근 확정) 상태에서는 "MA5 아래" 진입 금지
                        # - 비가자미근에서는 기존처럼 SoftPenalty로만 처리
                        if current_price < ma5 * 0.997:
                            if soleus_gajami:
                                # 🔒 불합리 veto 제거: 가자미근 초입에서 MA5 바로 아래는 자연스러운 스윙 구간
                                # - 차단이 아니라 페널티/리스크로만 반영
                                self.last_rejection_reason = "MA5 머리숙임(가자미근-소프트)"
                                penalty_reasons.append("MA5-HEADDOWN-SOFT")
                                score = PENALTY_WEIGHTS.get('ma5_below', 0.03)
                                penalty = add(penalty, score)
                                penalty_details['ma5_below'] = score
                                indicators["gajami_ma5_headdown_soft"] = True

                                logger.info(
                                    f"⚠️ [MA5-HEADDOWN-SOFT] {symbol} 가자미근 확정 + MA5 아래 → 차단 대신 소프트 | "
                                    f"현재:{current_price:.1f} < MA5:{ma5:.1f}"
                                )
                            else:
                                penalty_reasons.append("MA5이탈(-0.3%)")
                                score = PENALTY_WEIGHTS.get('ma5_below', 0.03)
                                penalty = add(penalty, score)
                                penalty_details['ma5_below'] = score
                                logger.debug(
                                    f"⚠️ [MA-Soft] {symbol} MA5 이탈 | "
                                    f"현재:{current_price:.1f} < MA5*0.997 | 페널티 0.85x"
                                )
            
            # 🆕 PATCH 1: 초기 상승 조기 감지 (Early-Entry) - 2025-11-27
            # 목적: 04:33~04:36 같은 초기 상승 구간 포착
            # 조건: MA5/MA20 기울기 모두 양수 + MA5 가속도 증가
            # 효과: 1~2봉 더 빨리 진입 → 어깨 진입 방지
            # ❌ 레거시 Early-Entry df 재계산 제거
            # - Analysis.ma5_slope, ma20_slope, vol_accel 사용
            indicators["early_entry_flag"] = False
            
            # 🔄 PATCH 1: Regime Turn Candidate (TD → TU 전환 초입 감지)
            # 목적: ETH 05:39 같은 "하락장 끝 → 상승장 초입" 가자미근 포착
            # 조건: TD 체제 + MA 거의 정렬 + 골든크로스 임박 + 저점대비 가자미근
            # 효과: PrelimKnee 완화, Early-Block 우회, Volume 하한 보정
            # 철학: 일반 TD 반등은 여전히 차단, 레짐 전환만 예외
            regime_turn_candidate = False
            
            # MA 데이터 및 골든크로스 정보 추출
            ma_data_turn = data_package.ma_data or {}
            avg_spacing_turn = ma_data_turn.get('avg_spacing', 0.0)  # MA 평균 간격
            golden_count_turn = ma_data_turn.get('golden_count', 0)
            ma5_slope_turn = safe_extract_scalar(ma_data_turn.get('slope_ma5', 0.0))
            ma120_slope_turn = safe_extract_scalar(ma_data_turn.get('slope_ma120', 0.0))
            
            # 메이저(Tier1) 종목 여부 - SSoT: CONFIG [VOLUME].exclude_tier1
            exclude_tier1 = self.config.get_cached("exclude_tier1", [])
            is_major_coin = symbol_str in exclude_tier1
            
            # 🔄 레짐 전환 후보 조건 (5개 충족 시)
            if regime == MarketRegime.TRENDING_DOWN and is_major_coin:
                # 🔒 None 안전 가드 (regime_turn_candidate)
                regime_turn_candidate = (
                    abs(avg_spacing_turn) < 0.10        # MA 거의 정렬 (역배열이지만 좁음)
                    and ma5_slope_turn > 0              # 단기 상승 기울기
                    and ma120_slope_turn > 0            # 중기 추세 전환
                    and golden_count_turn >= 1          # 골든크로스 발생/임박
                )
                
                if regime_turn_candidate:
                    logger.info(
                        f"🔄 [RegimeTurnCandidate] {symbol} TD→TU 전환 초입 감지 | "
                        f"ma_spacing={avg_spacing_turn:.3f}% | "
                        f"ma5_slope={fmt(ma5_slope_turn, '.5f')} | "
                        f"ma120_slope={ma120_slope_turn:.5f} | "
                        f"GC={golden_count_turn}개 | "
                        f"→ PrelimKnee/Early-Block 예외 허용"
                    )
                    
            # 🔧 indicators에 저장 (PATCH 4에서 사용)
                    indicators['regime_turn_candidate'] = True
            
            # 🔒 [ENTRY-FIXED-BOX-PURGE-0828-R2]
            # fixed BOX 진입 위치/HardCut 잔여 정의 제거 완료.
            # 🆕 PATCH 2: 불장 피크 차단 (Peak-Avoid) - 2025-11-27
            # 목적: 정수리 근처 추격매수 완전 제거
            # 조건 3개 동시 만족 시 차단:
            #   1. buying_ratio < 55% (매수세 급락)
            #   2. volume_slope < 0   (거래량 감소)
            #   3. ma_gap(5-10) 확대  (MA 벌어짐 = 힘 빠짐)
            # 효과: 어깨~정수리 진입 100% 차단
            if regime == MarketRegime.BULL_MARKET:
                try:
                    buying_ratio = safe_extract_scalar(indicators.get("buying_ratio", 0))
                    volume_slope = indicators.get("volume_slope", 0.0)
                    
                    ma5 = safe_extract_scalar(indicators.get("sma_5", 0))
                    ma10 = safe_extract_scalar(indicators.get("sma_10", 0))
                    
                    if ma5 > 0 and ma10 > 0:
                        ma_gap_510 = abs(ma5 - ma10) / ma10 * 100.0
                        prev_gap_510 = indicators.get("prev_ma_gap_510", ma_gap_510)
                        indicators["prev_ma_gap_510"] = ma_gap_510
                        
                        peak_detected = (
                            lt(buying_ratio, 0.55) and
                            lt(volume_slope, 0) and
                            gt(ma_gap_510, prev_gap_510)
                        )
                        
                        if peak_detected:
                            # ✅ HardCut 대신 SoftPenalty 적용
                            penalty_reasons.append("BullPeak(매수↓/거래량↓/GAP↑)")
                            
                            logger.info(
                                f"⚠️ [{symbol}] 불장 피크 신호 감지 → SoftPenalty(0.93x) 적용 | "
                                f"buying_ratio:{fmt(buying_ratio*100, '.2f')}%, "
                                f"volume_slope:{volume_slope:.3f}, "
                                f"ma_gap(5-10):{ma_gap_510:.2f}% > prev:{prev_gap_510:.2f}%"
                            )
                except Exception as e:
                    logger.debug(f"[BULL-Peak] {symbol} 피크 감지 실패: {e}")
            
            # 🔧 Force SSoT 기본값: RANGING 외 체제는 기존 동작 유지
            _soleus_force_ok = True

            # 🔥 박스권 Early-Return (RANGING 전용 추가 로직) - 2025-11-20
            # 철학: "무릎에서 사서 어깨에서 판다"
            # 목적: RANGING 구간에서 골든/ADX/거래량 신호보다 먼저
            #       박스권 위치(지지/저항)를 최우선 판단
            # 효과: KRW-AVNT 같은 "어깨 위 매수" 100% 차단
            if regime == MarketRegime.RANGING:
                
                # SSoT: 저항선 탐지는 Analysis 책임
                # - Strategy에서 재계산 금지
                # - stub 호출 제거 (혼란 방지)
                
                # NOTE: 과거 저항선 차단 로직은 SSoT 위반으로 폐기
                # 현재는 soleus_gajami + confidence 단일 파이프라인만 사용
                # 🆕 PATCH (2025-12-10): RANGING 가자미근 FORCE 필터
                # 철학: "힘있는 반등(Force 3요소)이 있는 박스 하단에서만 진입"
                # 목적: ID 20:32 같은 Dead Zone 진입 100% 차단
                # 
                # Force 2 (거래량 복원력): volume_ratio ≥ 1.20x
                # Force 3 (매수흐름 전환): buying_ratio ≥ 0.58
                # 박스 하단: box_pos ≤ 0.30
                
                # - 가자미근은 형상 단일 판정
                ma5_slope = safe_extract_scalar(indicators.get("ma5_slope", 0.0))
                ma10_slope = safe_extract_scalar(indicators.get("ma10_slope", 0.0))
                volume_ratio = safe_extract_scalar(indicators.get("volume_ratio", 0.0))
                early_entry_flag = bool(indicators.get("early_entry_flag", False))

                # Force 1: 가격 복원력
                # ✅ 가자미근 "초극초입(≤0.30%)"은 0.10%부터 허용 (0.14% 같은 정답 타점 보호)
                force1_min = 0.30

                force1_ok = (
                    gt(ma5_slope, 0.0)
                    and ge(ma10_slope, 0.0)
                )

                force1_detail = (
                    f"ma5:{'✓' if ma5_slope > 0 else '✗'}, "
                    f"ma10:{'✓' if ma10_slope >= 0 else '✗'}"
                )
                
                # Force 2: 거래량 복원력
                #    '거래량 1.20x' 강제 대신, 과도한 오진입을 피하는 최소치만 적용
                knee_relaxed = bool(indicators.get("global_knee_volume_relaxed", False))
                knee_relaxed = False

                # 기본값(기존 유지)
                force2_min = 0.60 if knee_relaxed else 1.20
                force2_ok = (volume_ratio >= force2_min)
                force2_detail = f"V:{fmt(volume_ratio, '.2f')}x≥{force2_min:.2f}x"

                # Force 3: 매수흐름 전환
                # ✅ knee_relaxed(전역 무릎 거래량 완화)일 때만 52% 기준 사용
                force3_min = 0.52 if knee_relaxed else 0.58

                force3_ok = (buying_ratio >= force3_min)
                force3_detail = f"B:{fmt(buying_ratio*100, '.1')}%≥{force3_min:.0%}"

                # 🔧 Force SSoT: 스코프 밖(8297) Birth-Soleus-BASE 재사용
                _soleus_force_ok = bool(force1_ok and force2_ok and force3_ok)

                # 🆕 SoleusForce 역할 제한 (2025-12-23)
                # 목적: SoleusForce는 '보조'이지 '최종 승인권자' 아님
                # 효과: 하락 중간 거래량 폭주 진입 차단
                
                # 🔒 SSoT 적용
                # - Force 부족은 Soft 페널티로만 반영
                # 최종 판정 (SSoT: 가자미근 승인 이후 HardCut 금지)
                if not _soleus_force_ok:
                    failed_forces = []
                    if not force1_ok: failed_forces.append(f"F1({force1_detail})")
                    if not force2_ok: failed_forces.append(f"F2({force2_detail})")
                    if not force3_ok: failed_forces.append(f"F3({force3_detail})")
                    
                    # ✅ Force는 "보조" 규칙: 최종 차단권을 갖지 않는다.
                    # - 후보/가자미근/SSoT 오버라이드가 있는 경우: 항상 Soft
                    # - 그 외에도: RANGING 초입(≤0.80%)이면 Soft로 내려서 '99번 기회 상실형' 방지
                    soft_force_allowed = (
                        soleus_gajami
                        or bool(indicators.get("hard_gajami_pass", False))
                        or regime == MarketRegime.RANGING
                    )

                    if soft_force_allowed:
                        penalty_reasons.append("SoleusForce-Soft")
                        logger.info(
                            f"⚠️ [SoleusForce-Soft] {symbol} Force 부족 → 관측 태그 기록(scoring 제외) | 부족: {', '.join(failed_forces)}"
                        )
                    else:
                        # Soft가 아닌 경우에도, Force 단독으로 하드 리턴은 금지.
                        # (최종 판사는 신뢰도/점수 체계가 담당)
                        penalty_reasons.append("SoleusForce-Soft-Default")
                        logger.info(
                            f"⚠️ [SoleusForce-Soft-Default] {symbol} Force 부족 → 관측 태그 기록(scoring 제외) | 부족: {', '.join(failed_forces)}"
                        )
                
                # 🟢 모든 Force 충족 → 진입 허용 (로그 버그 수정: Force 부족 시 미출력)
                if _soleus_force_ok:
                    logger.info(
                        f"🟢 [SoleusForce-OK] {symbol} 가자미근 Force 충족 → 진입 허용 | "
                        f"F1({force1_detail}) | F2({force2_detail}) | F3({force3_detail})"
                    )
                
                # SSoT: 지지선 탐지는 Analysis 책임
                # - Strategy에서 재계산 금지
                # - stub 호출 제거 (혼란 방지)
                                
            # 박스권 Early-Return 끝
            # → RANGING 이외 체제 또는 박스권 통과 시 아래로 진행
                       
            # ============================================================
            # 🆕 Regime 보정 (2봉 Evidence 기반 전환) 2025-11-22
            # ============================================================
            # 목적: LAYER처럼 실제 상승인데 TRENDING_DOWN으로 오판된 케이스 보정
            # 조건: 2봉 연속 상승 + slope(MA5)>0 + 거래량 스파이크 + 저점대비 2~8%
            # 효과: RANGING/TRENDING_DOWN → TRENDING_UP 강제 전환
            # ============================================================
            # ❌ 레거시 Regime Override (2bar) 제거
            # - 이미 regime 변경 없이 로그만 출력 (비활성화됨)
            # - Analysis.regime 결과만 사용
            # ============================================================
                
            # ==========================================================
            # 🆕 [MA Negative Filter] — MA 역배열 즉시 차단 (2025-11-05)
            # ==========================================================
            # 목적: PENGU 케이스 재발 방지 (MA5 < MA10 < MA20 역배열)
            # 근거: 과거 MA 신호 -0.55에서 진입 → 즉시 손절 케이스
            # ==========================================================
            ma5 = safe_extract_scalar(indicators.get('sma_5', 0))
            ma10 = safe_extract_scalar(indicators.get('sma_10', 0))
            ma20 = safe_extract_scalar(indicators.get('sma_20', 0))
            
            # 🔒 안전: 스칼라 변환 확인
            ma5 = float(ma5) if ma5 is not None else 0.0
            ma10 = float(ma10) if ma10 is not None else 0.0
            ma20 = float(ma20) if ma20 is not None else 0.0
            
            # 🆕 예비 Knee 조건 사전 계산 (Hard Cut 예외용) - FIX-2 (2025-11-19)
            # 목적: MA 역배열/MA60 하회 Hard Cut에서 "무릎 구간"은 예외 허용
            # 개선: 저점 구조까지 확인해 "진짜 로컬 바닥"만 Knee 인정
            #    2) 최근 3개 저점이 local bottom: low[-3] ≥ low[-2] ≤ low[-1]
            #    3) 최근 2개 종가가 반등 시작: close[-2] ≤ close[-1]
            #    4) slope_ma5 > 0
            
            # 🆕 단타(3분봉) 진입 보호 플래그 (2025-12-13)
            # 목적: PEARL-GC / Early-Entry 같은 단타 진입 예외 허용 시,
            #       스윙 방어용 페널티(MA60-Hard, ADX급락, 과도반등)를 
            #       우선순위 분리하여 적용
            # 철학: "열어놓고 뒤에서 죽이지 않는다"
            is_pearl_gc = indicators.get("is_pearl_gc", False)
            is_early_entry = indicators.get("early_entry_flag", False)
            volume_ratio_scalp = safe_extract_scalar(indicators.get("volume_ratio", 0.0))
            
            # 🛡️ ScalpProtect (2025-12-13)
            # NOTE: 현재 로그 목적이며, 실제 차단/완화 로직은 없음 (2025-12-19 기준)
            #       - PEARL-GC/Early-Entry 판정만 수행
            #       - 향후 단타 보호 로직 추가 시 활용 가능
            is_scalp_entry = is_pearl_gc or (is_early_entry and volume_ratio_scalp >= 0.7)
            
            if is_scalp_entry:
                logger.info(
                    f"🛡️ [ScalpProtect] {symbol} 단타 진입 보호 활성화 | "
                    f"PEARL-GC={is_pearl_gc}, Early-Entry={is_early_entry}, Vol={fmt(volume_ratio_scalp, '.2f')}x"
                )
                        
            # 🆕 KneeMode 조기 판정 (2025-12-09)
            # 목적: 상단 필터(거래량, 패턴, ADX)가 무릎 모드를 인식하도록 조기 판정
                                
            # ==========================================================
            # 🆕 [MA Long-term Filter] — 장기 MA 역전 차단 (2025-11-06)
            # ==========================================================
            # 목적: MA60/120 하회 진입 차단
            # 근거: MA60 > 현재가 상태에서 진입 → 구조적 손실
            # 철학: "장기 MA 아래는 하락 추세 진행 중 → 진입 금지"
            # ==========================================================
            ma60 = safe_extract_scalar(indicators.get('sma_60', 0))
            # 🔥 MA60/MA120 역할 재정의 (2025-12-06)
            # MA60: 메인 진입 필터 (3시간 추세)
            # MA120: 장기 경고 (SoftPenalty만)
            ma60_value = safe_extract_scalar(indicators.get('ma60', 0))
            ma120 = safe_extract_scalar(indicators.get('sma_120', 0))

            # 🔒 안전: 스칼라 변환 확인
            ma60 = float(ma60) if ma60 is not None else 0.0
            ma60_value = float(ma60_value) if ma60_value is not None else 0.0
            ma120 = float(ma120) if ma120 is not None else 0.0
            
            # 🆕 [MA120 기울기 계산] 횡보성 판단 (2025-12-01)
            # 목적: 횡보성 MA120은 저항이 아니라 지지선 역할
            # 기울기 ±0.5% 이내 = 횡보로 판단 → 페널티 대폭 완화
            # 계산: 최근 20봉(60분) MA120 변화율
            # is_ma120_sideways / ma120_slope 로컬 블록 제거
            # (df=None 고정으로 블록 미진입 + 로컬 변수 후속 사용 없음)
            
            # 🔥 [MA60 필터] 메인 진입 제한 (3시간 추세 기반) (2025-12-06)
            # 목적: MA120 대신 MA60을 주 필터로 승격
            # 철학: 3시간 추세면 충분한 안전장치 + 단타 민첩성 확보
            if ma60_value > 0 and current_price < ma60_value:
                gap_ma60 = ((current_price - ma60_value) / ma60_value) * 100
                
                # 🎯 긍정 신호 카운트
                positive_signals_ma60 = 0
                
                ma_data_ma60 = data_package.ma_data or {}
                golden_count_ma60 = ma_data_ma60.get('golden_count', 0)
                if golden_count_ma60 >= 2:
                    positive_signals_ma60 += 1
                
                volume_ratio_ma60 = safe_extract_scalar(indicators.get('volume_ratio'))
                if volume_ratio_ma60 and volume_ratio_ma60 >= 0.8:
                    positive_signals_ma60 += 1
                
                slope_ma5_ma60 = safe_extract_scalar(ma_data_ma60.get('slope_ma5', 0))
                if slope_ma5_ma60 and slope_ma5_ma60 > 0:
                    positive_signals_ma60 += 1
                
                # 🎯 MA60 기반 차등 페널티
                if positive_signals_ma60 >= 3:
                    # 긍정 신호 3개+ = 무릎/전환 초입
                    if gap_ma60 >= -3.0:
                        penalty_reasons.append(f"MA60하단-완화({gap_ma60:.2f}%)")
                        score = 0.10
                        penalty = add(penalty, score)
                        penalty_details['ma60_below'] = score
                        
                        logger.info(
                            f"🟢 [MA60-Soft] {symbol} MA60 하단({gap_ma60:.2f}%) BUT "
                            f"긍정신호 {positive_signals_ma60}개 (무릎/전환) | "
                            f"페널티 완화: 0.90x, 감점 +{fmt(score, '.2f')}"
                        )
                    else:
                        # -3% 이하는 중간 페널티
                        penalty_reasons.append(f"MA60하단-중({gap_ma60:.2f}%)")
                        score = 0.25
                        penalty = add(penalty, score)
                        penalty_details['ma60_below'] = score
                        
                        logger.warning(
                            f"⚠️ [MA60-Mid] {symbol} MA60 하단({gap_ma60:.2f}%) | "
                            f"긍정신호 {positive_signals_ma60}개 BUT 간격 과다 | "
                            f"중간 페널티: 0.70x, 감점 +{fmt(score, '.2f')}"
                        )
                
                elif positive_signals_ma60 >= 1:
                    # 🔒 [MA60-BIRTH-PENALTY-0826]
                    # lifecycle은 한 번만 읽고, 기존 TD 동작은 보존한다.
                    # 추가 변경은 RANGING fresh BIRTH(VALID, dur<=2)에서
                    # MA60 아래라는 이유만으로 0.35 강감점을 주지 않는 것뿐이다.
                    _gmeta_ma60 = (data_package.ma_data or {}).get("gajami_meta") or {}
                    _glc_ma60 = _gmeta_ma60.get("gajami_lifecycle") or {}
                    _glc_ma60_dur_src = _glc_ma60.get("dur", None)
                    _glc_ma60_dur = (
                        int(_glc_ma60_dur_src)
                        if _glc_ma60_dur_src is not None
                        else 999
                    )
                    _is_birth_valid = bool(
                        _glc_ma60.get("state") == "VALID"
                        and _glc_ma60_dur
                        <= SoleusGajamiConstants.NEWBORN_MAX_DUR
                    )

                    # 기존 TD relief 보존:
                    #   TD + gap>=-4 + (soleus OR fresh birth) → 0.03
                    # 신규 RG 정합:
                    #   RG + fresh birth → 0.03
                    _ma60_birth_relief = bool(
                        (
                            regime == MarketRegime.TRENDING_DOWN
                            and gap_ma60 >= -4.0
                            and (soleus_gajami_ssot or _is_birth_valid)
                        )
                        # 🔒 [MA60-RELIEF-GAP-PARITY-0906] ★524 (분류: 안전성 정책 보강)
                        # ChatGPT 크로스체크로 분류 정정: TD는 gap>=-4.0 AND (soleus OR
                        # birth), RG는 birth 단독 — 이 비대칭은 새로 발견된 결함이 아니라
                        # 원래 의도된 설계다(TD는 조건을 느슨히, RG는 gap 제한 없이 fresh
                        # birth만 요구). 따라서 "확정 오류성"이 아니라 안전성 정책 보강이다.
                        # 실측 6일 RANGING 9,824건의 gap 최소값이 -41.35%까지 내려가는 것이
                        # 확인됐고, TD와 동일한 -4.0을 RG에 적용해도 65건(0.66%)만 바뀌는
                        # 저위험 안전판이라 적용한다(새 숫자 0 · TD 값 재사용).
                        or (
                            regime == MarketRegime.RANGING
                            and gap_ma60 >= -4.0
                            and _is_birth_valid
                        )
                    )

                    if _ma60_birth_relief:
                        score = 0.03
                        penalty_reasons.append(
                            f"MA60하단-BIRTH({gap_ma60:.2f}%)"
                        )
                        logger.info(
                            f"🦵 [MA60-BIRTH-PENALTY] {symbol} "
                            f"MA60 하단({gap_ma60:.2f}%) | "
                            f"regime={regime.value if hasattr(regime, 'value') else regime} "
                            f"state={_glc_ma60.get('state', 'NA')} "
                            f"dur={_glc_ma60_dur} "
                            f"soleus={int(soleus_gajami_ssot)} "
                            f"→ fresh BIRTH 최소 감점 +{fmt(score, '.2f')}"
                        )

                    elif (
                        regime == MarketRegime.TRENDING_DOWN
                        and gap_ma60 >= -4.0
                    ):
                        # 기존 코드의 최종 유효값은 두 번째 대입의 0.20이었다.
                        # 비-BIRTH TD 동작은 그대로 보존한다.
                        score = 0.20
                        penalty_reasons.append(
                            f"MA60하단-약({gap_ma60:.2f}%)"
                        )
                        logger.info(
                            f"⚠️ [MA60-Weak] {symbol} "
                            f"MA60 하단({gap_ma60:.2f}%) | "
                            f"체제:TRENDING_DOWN "
                            f"긍정신호 {positive_signals_ma60}개 | "
                            f"감점 +{fmt(score, '.2f')}"
                        )

                    else:
                        score = 0.35
                        penalty_reasons.append(
                            f"MA60하단-강({gap_ma60:.2f}%)"
                        )
                        logger.warning(
                            f"⚠️ [MA60-Strong] {symbol} "
                            f"MA60 하단({gap_ma60:.2f}%) | "
                            f"긍정신호 {positive_signals_ma60}개 | "
                            f"강한 감점 +{fmt(score, '.2f')}"
                        )

                    penalty = add(penalty, score)
                    penalty_details['ma60_below'] = score
                
                else:
                    # 긍정 신호 없음 = 강한 페널티
                    if not is_scalp_entry:
                        if gap_ma60 <= -5.0:
                            score = 0.50
                        elif gap_ma60 <= -3.0:
                            score = 0.40
                        else:
                            score = 0.25
                        
                        penalty_reasons.append(f"MA60하단-매우강({gap_ma60:.2f}%)")
                        penalty = add(penalty, score)
                        penalty_details['ma60_below'] = score
                        
                        logger.warning(
                            f"🔴 [MA60-Hard] {symbol} MA60 하단({gap_ma60:.2f}%) | "
                            f"긍정신호 없음 (칼날 구간) | "
                            f"강한 페널티: {penalty_factor:.2f}x, 감점 +{fmt(score, '.2f')}"
                        )
                    else:
                        logger.debug(
                            f"🛡️ [MA60-Skip] {symbol} 단타 진입 보호 → MA60-Hard 페널티 미적용 | "
                            f"MA60 하단({gap_ma60:.2f}%)"
                        )
            
            # 🔥 [MA120 필터] HardCut → SoftPenalty 전환 (2025-12-06)
            # 목적: 장기 하락 경고만 유지, 단기 반등 허용
            # 철학: "MA60이 메인 필터, MA120은 보조 경고"
            # 페널티: 최대 0.85~0.90x (약한 경고 수준)
            if ma120 > 0 and current_price < ma120:
                gap_ma120 = ((current_price - ma120) / ma120) * 100
                
                # 🎯 MA120은 이제 "약한 경고"만 (HardCut 제거)
                if gap_ma120 >= -5.0:
                    # 🦵 KneeMode 체크 (기본 + UP-KNEE + TD-KNEE)
                    _ma120_soleus_relief = bool(soleus_gajami_ssot)
                    
                    if _ma120_soleus_relief:
                        # 가자미근 확정: MA120 아주 약한 경고
                        score = 0.02
                        penalty_reasons.append(f"MA120경고-가자미({gap_ma120:.2f}%)")
                        
                        logger.debug(
                            f"🦵 [MA120-Soleus] {symbol} MA120 하단({gap_ma120:.2f}%) | "
                            f"가자미근 확정 → 관측 경고 score={fmt(score, '.2f')} (scoring 제외)"
                        )
                    else:
                        # 일반 모드: 기존 로직
                        score = 0.05
                        penalty_reasons.append(f"MA120경고-약({gap_ma120:.2f}%)")
                        
                        logger.debug(
                            f"💡 [MA120-VeryWeak] {symbol} MA120 하단({gap_ma120:.2f}%) | "
                            f"장기 경고만, 진입 허용 | 페널티: 0.95x, 감점 +{fmt(score, '.2f')}"
                        )
                elif gap_ma120 >= -10.0:
                    # -10% 이내: 약한 경고
                    penalty_reasons.append(f"MA120경고-중({gap_ma120:.2f}%)")
                    score = 0.10
                    
                    logger.info(
                        f"💡 [MA120-Weak] {symbol} MA120 하단({gap_ma120:.2f}%) | "
                        f"장기 경고, 단기 반등 허용 | 페널티: 0.90x, 감점 +{fmt(score, '.2f')}"
                    )
                else:
                    # -10% 초과: 중간 경고
                    penalty_reasons.append(f"MA120경고-강({gap_ma120:.2f}%)")
                    score = 0.15
                    
                    logger.warning(
                        f"⚠️ [MA120-Moderate] {symbol} MA120 하단({gap_ma120:.2f}%) | "
                        f"장기 하락 경고, 주의 필요 | 페널티: 0.85x, 감점 +{fmt(score, '.2f')}"
                    )
                
                # 🔒 [2026-08-08] MA120경고-무릎만 additive penalty에서 제외 (8건, 무변별 확인).
                #   약/중/강 MA120 경고는 기존 판정을 유지한다.
                if not (gap_ma120 >= -5.0 and _ma120_soleus_relief):
                    penalty = add(penalty, score)
                penalty_details['ma120_below'] = score
                            
            # ============================================================
            # 🔧 PATCH 1 준비: 하락추세 Hard Cut용 MA slope 추출 (2025-11-17)
            # ============================================================
            ma_analysis_hardcut = data_package.ma_data or {}
            ma5_slope_hardcut = safe_extract_scalar(ma_analysis_hardcut.get('slope_ma5', 0.0), 0.0)
            # ============================================================
            # ============================================================
            
            # ==========================================================
            # 🆕 [3m BREAKOUT 조건부 진입] — (2025-11-06 개선)
            # ==========================================================
            # 변경: "전면 금지" → "확인된 돌파 + 얕은 되돌림만 허용"
            # 목적: 확인된 돌파 + 얕은 되돌림만 허용 (꼭지 매수 방지)
            # 근거: 꼭지 매수 vs 초기 돌파를 구별
            # 
            # [허용 조건 3가지 - 모두 충족 시에만 진입]
            # 1) 저항선 확정 돌파 (종가 > 저항선)
            # 2) 되돌림 0~0.3% 범위 (과열 아님)
            # 3) 거래량 1.2배 이상 (확실한 모멘텀)
            # ==========================================================
            if regime == MarketRegime.BREAKOUT and self.timeframe.value == '3m':
                # ✅ last_close, resistance는 Analysis에서 계산되어 indicators로 전달됨
                last_close = current_price  # 현재가 사용
                
                # 저항선은 indicators에서 제공
                resistance = indicators.get('resistance_level', 0.0)
                
                # 🔒 안전: resistance를 스칼라로 변환
                res_val = S(resistance, 0.0)
                
                # 조건 체크
                pullback_ok = False
                vol_ok = False
                pullback_pct = 0.0
                volume_ratio = 0.0
                
                if res_val > 0 and last_close > 0:  # ✅ 스칼라 비교
                    # 1) 종가가 저항선 위 (확정 돌파)
                    # 2) 되돌림 폭 0% ~ 0.3% 범위 (과열되지 않은 초기 돌파) -> 0.8 조정 20251106
                    # pullback_ok = (0 <= pullback_pct <= 1.0)  # 0.3 → 1.0 (안전형)
                    # pullback_ok = (0 <= pullback_pct <= 2.0)  # 0.3 → 2.0 (균형형)
                    # pullback_ok = (0 <= pullback_pct <= 3.0)  # 0.3 → 3.0 (공격형)
                    pullback_pct = ((last_close - res_val) / res_val) * 100.0
                    pullback_ok = (0.0 <= pullback_pct <= 0.8)
                
                # 3) 거래량 1.2배 이상 (확실한 모멘텀)
                volume_ratio = safe_extract_scalar(indicators.get('volume_ratio', 0))
                vol_ok = (ge(volume_ratio, 1.2))
                
                # 모든 조건 충족 시에만 통과
                if not (pullback_ok and vol_ok):
                    if allow_hardcut and not _j234_hardcut_pass:
                        logger.info(
                            f"🚫 [3m_BREAKOUT_Filter] {symbol} 진입 차단 | "
                            f"확인·되돌림 미충족 | "
                            f"저항: {resistance:.1f} → 종가: {last_close:.1f} "
                            f"(되돌림: {pullback_pct:.2f}%, OK={pullback_ok}) | "
                            f"거래량: {fmt(volume_ratio, '.2f')}x (OK={vol_ok})"
                        )
                        self.last_rejection_reason = "BREAKOUT_CONFIRM_FAIL"
                        return None
                    _bo_confirm_owner = (
                        "J234_PASS" if _j234_hardcut_pass else "J1_SOLEUS"
                    )
                    logger.info(
                        f"🟢 [BREAKOUT-CONFIRM-HARDCUT-SKIP] {symbol} "
                        f"BREAKOUT 확인 HardCut 면제 | "
                        f"owner={_bo_confirm_owner} "
                        f"soleus={int(soleus_gajami_ssot)} "
                        f"J2={int(indicators.get('judge2_osc_pass', 0) or 0)} "
                        f"J3={int(indicators.get('judge3_cycle_pass', 0) or 0)} "
                        f"J4={int(indicators.get('judge4_reversal_pass', 0) or 0)}"
                    )
                else:
                    logger.info(
                        f"✅ [3m_BREAKOUT_Pass] {symbol} 조건부 진입 허용 | "
                        f"확정 돌파 + 되돌림 {pullback_pct:.2f}% + "
                        f"거래량 {fmt(volume_ratio, '.2f')}x | "
                        f"초기 돌파 단계로 판단"
                    )
                
            # ==========================================================
            # 🆕 [Entry Filter 강화] — 진입 타점 정교화 (2025-11-04)
            # ==========================================================
            # 목적: 저신뢰/단발 거래량/미확정 돌파 차단
            # 적용: config 설정 존중 + 체제별 차등 적용
            # 근거: AUCTION/ENA 케이스 분석 (과거 대화 반영)
            # ==========================================================

            # 1️⃣ 신뢰도 필터 (config 기반 + 체제별 완화)
            trust_score = indicators.get('confidence', None)
            
            # 🔒 안전: Series일 수 있으므로 스칼라 변환
            trust_score = S(trust_score) if trust_score is not None else None
            
            # 🔥 신뢰도가 아직 계산 안 됐으면 즉시 계산 (on-demand)
            if trust_score is None or trust_score == 0.0:
                # 🔒 PATCH D-2: indicators 유효성 검증
                if not indicators.get("_valid", True):
                    logger.debug(
                        f"⛔ [CONFIDENCE-SKIP] {data_package.market} "
                        f"indicators invalid → 신뢰도 계산 스킵"
                    )
                    trust_score = 0.0
                else:
                    prelim_components = self._calculate_signal_components(df, data_package, data_package.ma_data or {})
                    if prelim_components:  # 신호가 있을 때만
                        trust_score = self._calculate_final_confidence(prelim_components, weights, regime, performance)
                        # 🔒 안전: 계산된 결과도 스칼라 변환
                        trust_score = S(trust_score)
                        # 🔒 SSoT: confidence는 L9086(final_score_capped)에서만 저장
                        # 중간 저장 제거 — L4805~L9086 사이 읽는 코드 없음
                    else:
                        trust_score = 0.0  # 신호 없으면 0
            
            # 🔒 안전: trust_score가 float임을 보장
            trust_score = float(trust_score) if trust_score is not None else 0.0
                        
            # 🆕 ADX 동적 필터: 추세 약화 시점 진입 차단
            adx_current = indicators.get('adx', 0)
            adx_prev = indicators.get('adx_prev', adx_current)
            
            # 🔒 안전: ADX 값들을 스칼라로 변환
            adx_current = S(adx_current, 0.0)
            adx_prev = S(adx_prev, 0.0)
            adx_delta = adx_current - adx_prev
            
            # ADX 약화 필터
            if adx_current > 0.0:  # ✅ 스칼라 비교
                if adx_current < 18:
                    # 🆕 RANGING에서는 ADX 페널티 완전 제거 (횡보장 정상 범위)
                    #  - 이유: 횡보장은 본질적으로 ADX < 18 (추세 약함)
                    #          페널티 부과는 논리적 모순
                    if regime == MarketRegime.RANGING:
                        # RANGING에서는 ADX 페널티 완전 제거
                        logger.info(
                            f"✅ [ADX필터-RangingSkip] {symbol} RANGING에서 ADX 페널티 제외 | "
                            f"ADX={fmt(adx_current, '.1f')} (횡보장 정상 범위)"
                        )
                        # 페널티 없음!
                    
                    else:
                        # 비-RANGING: 기존 로직
                        score = PENALTY_WEIGHTS['adx_weak']
                        
                        if True:
                            # 🆕 BULL_MARKET: ADX 약함 페널티 면제
                            if regime != MarketRegime.BULL_MARKET:
                                # 🔒 [2026-08-08] 곱셈형 penalty_factor 제거 — L8615 재대입이 폐기하는 死코드
                                #   (실측: TRENDING_UP 3,031건 전부 PF=1.000)
                                if adx_current < 15:
                                    score *= 0.4             # 0.05 → 0.02 완화
                                penalty_reasons.append(f"ADX약함({fmt(adx_current, '.1f')})")
                            else:
                                # 불장에서는 ADX 약함 무시
                                logger.debug(
                                    f"🔥 [ADX필터-Bull] {symbol} ADX 약함 페널티 면제 (불장) | "
                                    f"ADX={fmt(adx_current, '.1f')}"
                                )
                        
                        # penalty 누적
                        if score > 0:
                            penalty = add(penalty, score)
                            penalty_details['adx_weak'] = score
                            
                            logger.info(
                                    f"⚠️ [ADX필터] {symbol} ADX 약함 | "
                                    f"({fmt(adx_current, '.1f')} < 18) → 신뢰도 {penalty_factor:.2f}x 페널티, 감점 +{fmt(score, '.2f')}"
                                )
                
                if adx_delta < -2:
                    # 🔥 수정: RANGING에서 ADX 급락 페널티 완전 제거
                    # - 횡보장에서는 ADX 변동이 정상적 (추세 없음)
                    if regime == MarketRegime.RANGING:
                        # RANGING에서는 ADX 급락 페널티 완전 제거
                        logger.info(
                            f"✅ [ADX필터-RangingSkip] {symbol} RANGING에서 ADX 급락 무시 | "
                            f"ADX={fmt(adx_current, '.1f')} → {fmt(adx_delta, '+.1f')} (횡보장 정상)"
                        )
                        # 페널티 없음!
                    
                    else:
                        # 비-RANGING: 기존 로직
                        score = PENALTY_WEIGHTS['adx_falling']
                        
                        # ADX 급락 컨텍스트 분리 (가자미근 vs 일반)
                        # - 가자미근: ADX 급락 = 추세 전환 초기의 자연스러운 현상
                        # - 일반: ADX 급락 = 추세 약화 위험
                        if soleus_gajami_ssot:
                            score = 0.0  # 가자미근 ADX 급락 = 추세 전환 초기 정상
                        else:
                            # 단타 진입 보호
                            if not is_scalp_entry:
                                # 일반 페널티
                                penalty_reasons.append(f"ADX급락({fmt(adx_delta, '+.1f')})")
                            else:
                                # 단타 진입: ADX 급락 무시
                                score = 0.0
                                logger.debug(
                                    f"🛡️ [ADX-Skip] {symbol} 단타 진입 보호 → ADX 급락 페널티 미적용 | "
                                    f"prev={fmt(adx_prev, '.1f')} → now={fmt(adx_current, '.1f')} (Δ={fmt(adx_delta, '+.1f')})"
                                )
                        
                        # penalty 누적
                        if score > 0:
                            penalty = add(penalty, score)
                            penalty_details['adx_falling'] = score
                            
                            if not is_scalp_entry:
                                logger.info(
                                    f"⚠️ [ADX필터] {symbol} ADX 급락 | "
                                    f"prev={fmt(adx_prev, '.1f')} → now={fmt(adx_current, '.1f')} (Δ={fmt(adx_delta, '+.1f')}) → "
                                    f"신뢰도 0.90x 페널티, 감점 +{fmt(score, '.2f')}"
                                )            
            
            # 🔒 SSoT: indicators['ma_analysis']는 미설정 키 → 항상 {} 반환 버그
            # data_package.ma_data가 유일한 진실 (TRENDING_UP/RANGING에서도 동일)
            ma_analysis = data_package.ma_data or {}
            ma_gap_meta = ma_analysis.get('ma_gap_meta', {})
            
            # E-GC 플래그 확인
            if ma_gap_meta.get('threshold_lift_eligible', False):
                # [제거] 이미 정규화됨: atr_pct = indicators.get('atr', 0)
                # [제거] 이미 정규화됨: volume_ratio = indicators.get('volume_ratio', 0)
                
                # 🔒 안전: ATR과 Volume을 스칼라로 변환
                atr_pct = S(atr_pct, 0.0)
                volume_ratio = S(volume_ratio, 0.0)
                
                # 메이저(Tier1) 종목 여부 - SSoT: CONFIG [VOLUME].exclude_tier1
                exclude_tier1 = self.config.get_cached("exclude_tier1", [])
                if symbol in exclude_tier1:
                    atr_min, atr_max = 0.01, 5.0  # BTC/ETH 완화
                else:
                    atr_min, atr_max = 0.45, 3.5  # 알트 유지
                
                # ATR 범위 & Volume ≥0.8x
                if atr_min <= atr_pct <= atr_max and volume_ratio >= 0.8:
                    # 문턱 리프트 적용
                    original_trust = trust_score
                    trust_score = add(trust_score, 0.02)  # 🔥 None-safe 덧셈
                    
                    logger.info(
                        f"🎯 [문턱리프트] {symbol} | "
                        f"신뢰도 {fmt(original_trust, '.2f')} → {trust_score:.2f} (+0.02) | "
                        f"ATR:{pct(atr_pct, '.4f')} Vol:{fmt(volume_ratio, '.2f')}x"
                    )
            
            # 🆕 Progressive Entry Filter (2025-11-07)
            # 목적: 어깨 진입 차단 ("무릎에서 사고 어깨에서 판다")
            # 근거: MA5 이격 +0.6% 이상 진입 → 손절 빈발
            #        MA5 이격 +0.3% 이하 진입 → 무릎 구간 포착
            
            from math import isfinite
            
            # ─────────────── 임계값 상수 (튜닝 포인트) ───────────────
            # 시장 상황 변화 시 이 값들만 조정
            # ─────────────── 기존 상수 유지 (폴백용) ───────────────
            # 🆕 체제별 필터 프로파일 적용
            from global_constants import REGIME_FILTER_PROFILES
            
            # 1. 체제 이름 추출
            regime_name = regime.name if hasattr(regime, "name") else "DEFAULT"
            profile = REGIME_FILTER_PROFILES.get(regime_name, REGIME_FILTER_PROFILES["DEFAULT"])
            
            # 2. config.ini에서 TIER 분류 기준 읽기
            tier2_threshold = self.config.getfloat('GLOBAL_FILTER', 'high_price_threshold', fallback=80000)
            
            if current_price >= tier2_threshold:
                tier = "tier2"  # 고가종목
            else:
                tier = "tier3"  # 일반 알트
            
            # 3. 프로파일에서 임계값 추출
            MA5_MAX_DEV = profile["ma5_max_dev"]
            MA5_MIN_DEV = profile["ma5_min_dev"]

            # 🔥 MA5 상한 완화 (상승 초입 정상 이격도 허용)
            # - BULL_MARKET 이외 체제에만 1.0% 상한 적용
            # - BULL_MARKET은 프로파일 값(2.5%) 그대로 사용
            if regime_name != "BULL_MARKET":
                MA5_MAX_DEV = min(MA5_MAX_DEV, 1.00)  # 1%를 최대 상한으로 캡

            EARLY_ENTRY_EXCEPTION = profile["early_entry_exception"]
            
            # 4. 거래량 가중 완화 (BREAKOUT 전용)
            if profile.get("volume_boost", False):
                vol_threshold = profile["volume_boost_threshold"]
                vol_multiplier = profile["volume_boost_multiplier"]
                
                if volume_ratio >= vol_threshold:
                    original_ma5_max = MA5_MAX_DEV  # 원래 값 저장
                    MA5_MAX_DEV *= vol_multiplier
                    logger.debug(
                        f"🔥 [BREAKOUT-볼륨완화] {symbol} | "
                        f"거래량 {fmt(volume_ratio, '.2f')}x ≥ {vol_threshold}x → "
                        f"MA5 허용치 {original_ma5_max:.1f}% → {MA5_MAX_DEV:.1f}% (×{vol_multiplier})"
                    )
            
            # 5. 디버그 로그
            logger.debug(
                f"📊 [필터프로파일] {symbol} | "
                f"체제={regime_name} | TIER={tier}(기준={tier2_threshold:,.0f}원) | "
                f"MA5=[{MA5_MIN_DEV:.1f}%, {MA5_MAX_DEV:.1f}%]"
            )
            
            # 변수 초기화 (로그용, NaN 방어)
            dev_pct = 0.0
            pattern_status = "미확인"
            
            # Stage 1. MA5 이격도 필터 (어깨 차단)
            ma5 = safe_extract_scalar(indicators.get('sma_5', 0))
            
            if current_price > 0 and ma5 > 0:
                dev_pct = (current_price - ma5) / ma5 * 100
                
                # 🔒 NaN/Inf 가드 (실전 안전장치)
                if not isfinite(dev_pct):
                    logger.warning(
                        f"⚠️ [Entry-MA5] {symbol} 계산 오류 | "
                        f"MA5={ma5:.1f}, Price={current_price:.1f} | dev_pct={dev_pct}"
                    )
                    self.last_rejection_reason = "MA5 계산 불가"
                    return None
                
                # 🔧 PATCH G′: MA5 과이격 판정 단일화 (SSoT 플래그)
                # - 이 블록에서 '적용/면제'를 확정
                # - 이후 단계(KneeMode/일반 모드)에서는 이 플래그를 존중하고 재적용 금지
                
                ma5_overdev_exempt = False
                
                # 면제 조건 판정 (PATCH G 철학)
                golden_cnt_local = 0
                try:
                    golden_cnt_local = int(safe_extract_scalar(
                        (data_package.ma_data or {}).get("golden_count", indicators.get("golden_count", 0)), 0
                    ))
                except Exception:
                    golden_cnt_local = 0
                
                gajami_context = bool(
                    _soleus_ssot(indicators, data_package.ma_data)
                )
                
                ma5_slope_local = safe_extract_scalar(indicators.get("ma5_slope", 0.0), 0.0)
                
                # 강한 골든 + 가자미근 + 상승 기울기 → 면제
                if (
                    gajami_context
                    and golden_cnt_local >= 7
                    and ma5_slope_local > 0.0
                    and dev_pct <= 2.0
                ):
                    ma5_overdev_exempt = True
                    indicators["ma5_overdev_exempt"] = False  # 실제 면제 미적용
                    logger.info(
                        f"ℹ️ [Entry-MA5-Relax] {symbol} 강한 골든+초입 컨텍스트 → "
                        f"면제 조건 충족 · 면제 미적용 | dev={dev_pct:.2f}% (<=2.0) | "
                        f"golden={golden_cnt_local} | slope5={ma5_slope_local:.6f}"
                    )
                else:
                    indicators["ma5_overdev_exempt"] = False
                ma5_overdev_exempt = False
                
                # 🆕 프로파일 기반 MA5 이격도 체크
                # 🦵 KneeMode 전용 MA5 이격 Safe-Zone + 완화 (2025-12-07)
                #  - 목적: 무릎/반등 초입에서 MA5 이격은 정상 패턴
                #  - 효과: Safe-Zone(±2%) 완전 정상, 단계별 완화
                #  - 근거: BCH/SXP/CKB 모두 반등 초입 MA5 이격 1~2%
                
                SAFE_DEV = 2.0   # ±2% 이격은 완전 정상
                MID_DEV = 3.0    # 2~3%는 약한 경고
                HARD_DEV = 4.0   # 3~4%는 강경고
                
                # 과이격 체크 (상방)
                # 🔧 PATCH G′: 면제된 경우 전체 블록 스킵
                # NOTE: 외부 조건에서 이미 `not ma5_overdev_exempt` 보장
                if dev_pct > MA5_MAX_DEV and not ma5_overdev_exempt:
                    # 일반 모드: 고점 추격 차단 강화
                        # 🚫 PATCH: QTUM 09:06 유형 차단 (2026-01-07)
                        # ✅ 예외: 강한 추세 초입은 허용 (KRW-G 10:00~10:09 유형)
                        
                        # 강한 추세 초입 판정
                        strong_trend_start = (
                            indicators.get("triple_golden_cross", False)
                            and safe_extract_scalar(indicators.get("signal_strength", 0.0)) >= 0.95
                            and safe_extract_scalar(indicators.get("ma5_slope", 0.0)) > 0.0
                        )
                        
                        if dev_pct > 1.5:
                            if not strong_trend_start:
                                # ❌ 삭제됨 (GAJAMI-ABSOLUTE RULE로 완전 대체)
                                # 가자미근 외부는 이미 차단, 가자미근 내부는 허용
                                pass
                            else:
                                # 강한 추세 초입: 면제
                                logger.info(
                                    f"⚠️ [MA5-OverExtension-OVERRIDE] {symbol} "
                                    f"강한 추세 초입으로 과이격 차단 면제 | "
                                    f"dev={dev_pct:.2f}% "
                                    f"signal={safe_extract_scalar(indicators.get('signal_strength', 0.0)):.3f} | "
                                    f"3중골든={indicators.get('triple_golden_cross', False)} "
                                    f"slope={safe_extract_scalar(indicators.get('ma5_slope', 0.0)):.3f}"
                                )
                        
                        # 🔧 HOTFIX: 바깥 조건에서 이미 면제 체크 완료
                        # - ma5_overdev_exempt = False 확정 (바깥 조건 통과)
                        # - 불필요한 재체크 제거
                        penalty_reasons.append(f"MA5과이격(+{dev_pct:.2f}%)")
                        score = PENALTY_WEIGHTS['ma5_over_dev']
                        # 🔒 [MA5-OVERDEV-PENALTY-RESTORE-0811]
                        # MA5 과이격 발화군의 불리한 forward가 재확인되어
                        # 기존 additive penalty를 최종 confidence 판사에 다시 연결한다.
                        penalty = add(penalty, score)
                        penalty_details['ma5_over_dev'] = score
                        
                        logger.info(
                            f"⚠️ [Entry-MA5] {symbol} MA5 과이격 | "
                            f"+{dev_pct:.2f}% > +{MA5_MAX_DEV:.1f}% | "
                            f"체제={regime_name} → 감점 +{fmt(score, '.2f')}"
                        )
                
                # 과하회 체크 (하방)
                if dev_pct < MA5_MIN_DEV:
                    # 일반 모드: 기존 로직
                    penalty_reasons.append(f"MA5과하회({dev_pct:.2f}%)")
                    
                    score = PENALTY_WEIGHTS['ma5_under_dev']
                    penalty = add(penalty, score)
                    penalty_details['ma5_under_dev'] = score
                    
                    logger.info(
                        f"⚠️ [Entry-MA5] {symbol} MA5 과하회 | "
                        f"{dev_pct:.2f}% < {MA5_MIN_DEV}% → 신뢰도 0.75x 페널티, 감점 +{fmt(score, '.2f')}"
                    )
            
            # Stage 2. 저점 형성 확인 (무릎 포착) - 20봉 확장!
            # Analysis가 제공하는 recent_low/high(indicators)를 SSoT로 사용
            recent_low = float(safe_extract_scalar(indicators.get('recent_low', current_price), current_price))
            recent_high = float(safe_extract_scalar(indicators.get('recent_high', current_price), current_price))

            # 저점대비 상승률 (%)
            if recent_low > 0:
                pass
                
                # ✅ 4순위(SSoT): Analysis 통과로 SoleusZone 확정된 경우
                # - Stage2 SoleusZone 계산/Assist 자체를 스킵 (불필요한 간섭/로그 제거)
                # - SSoT 잠금 상태에서는 이미 soleus_zone=True가 확정이므로 재평가 금지
                # 🔒 이중 가드: soleus_final_locked 우선 체크 (Analysis 결과 보호)
                if indicators.get("soleus_final_locked", False):
                    # Analysis 결과 확정 → Stage2/Assist 완전 차단
                    ssot_locked = True
                else:
                    ssot_locked = (
                        soleus_zone and
                        indicators.get("soleus_zone_reason") == "ANALYSIS_PASS"
                    )
                
                # 🔒 안전 초기화 (UnboundLocalError 방지)
                zg_max = 0.0
                
                # 🆕 SoleusZone 확정 시: 매도압력 필터 권한 격하 플래그
                # 🔥 2025-12-22: SoleusAssist 반영을 위해 indicators에서 최종값 재확인
                final_soleus = soleus_zone
                # ✅ Consumer: ignore_selling_pressure는 Producer(Analysis) SSoT만 소비
                # (재설정 금지)
                pass
                
                if final_soleus:
                    logger.info(
                        f"🛡️ [SoleusZone] {symbol} 활성화 | "
                        f"vol={fmt(volume_ratio, '.2f')}x | buy={fmt(buying_ratio, '.1f')}%"
                    )
                
                # 🆕 soleus 상단가 계산 (추격 방지용)
                if final_soleus:
                    recent_low = float(safe_extract_scalar(indicators.get('recent_low', current_price), current_price))
                    
                    # 디버그: zg_max 값 확인
                    logger.debug(
                        f"[ZG_MAX] {symbol} | regime={regime.value if hasattr(regime, 'value') else regime}, "
                        f"zg_max={zg_max:.2f}%, ssot_locked={ssot_locked}"
                    )
                    
                    soleus_upper_price = recent_low * (1 + zg_max / 100)
                    indicators['soleus_upper_price'] = soleus_upper_price
                else:
                    indicators['soleus_upper_price'] = 0.0

                # → final_soleus/confirmed_knee는 더 이상 knee_mode를 변경하지 않음
                pass
                               
            # ============================================================
            # 🔧 PATCH C: RANGING 체제 MA 하락전환 Hard Cut (2025-11-18)
            # 조건: RANGING AND slope_ma5 < 0 AND MA5 < MA60
            # 목적: 횡보장에서 "하락이 시작된 뒤" 늦은 진입 차단 (KRW-TREE 케이스)
            # ============================================================
            if regime == MarketRegime.RANGING:
                ma_data_for_slope = data_package.ma_data or {}
                ma5_slope_val = safe_extract_scalar(ma_data_for_slope.get('slope_ma5', 0.0))
                
                # MA5 기울기 음수 + MA5 < MA60 = 횡보 → 하락전환 시작
                if ma5_slope_val < 0 and ma5 < ma60:
                    # ① 강한 하락 전환 (slope < -0.02) — J1 lane HardCut
                    if ma5_slope_val < -0.02:
                        if allow_hardcut and not _j234_hardcut_pass:
                            logger.info(
                                f"🚫 [RANGING-DownTurn-HardCut] {symbol} 횡보장 강한 하락전환 진입 차단 | "
                                f"MA5_slope:{fmt(ma5_slope_val, '.6f')} | MA5:{ma5:.1f} < MA60:{ma60:.1f} | "
                            )
                            self.last_rejection_reason = (
                                f"RANGING 강한하락전환 (slope5={fmt(ma5_slope_val, '.6f')}, MA5<{ma60:.1f})"
                            )
                            return None
                        _rg_downturn_owner = (
                            "J234_PASS" if _j234_hardcut_pass else "J1_SOLEUS"
                        )
                        logger.info(
                            f"🟢 [RANGING-DOWNTURN-HARDCUT-SKIP] {symbol} "
                            f"강한 하락전환 HardCut 면제 | "
                            f"owner={_rg_downturn_owner} "
                            f"soleus={int(soleus_gajami_ssot)} "
                            f"J2={int(indicators.get('judge2_osc_pass', 0) or 0)} "
                            f"J3={int(indicators.get('judge3_cycle_pass', 0) or 0)} "
                            f"J4={int(indicators.get('judge4_reversal_pass', 0) or 0)}"
                        )
                    else:
                        # ② 약한 하락/초기 흔들림은 HardCut 대신 강한 페널티만 부여
                        extra_penalty = PENALTY_WEIGHTS.get('long_downtrend', 0.15)
                        penalty_reasons.append(f"RANGING하락전환(slope={fmt(ma5_slope_val, '.4f')})")
                        penalty = add(penalty, extra_penalty)
                        penalty_details['ranging_downturn'] = extra_penalty
                        
                        logger.info(
                            f"⚠️ [RANGING-DownTurn-Soft] {symbol} 횡보장 하락전환 감지 → "
                            f"HardCut 대신 페널티 적용 (0.75x, +{extra_penalty:.2f}) | "
                            f"MA5_slope:{fmt(ma5_slope_val, '.6f')}, MA5:{ma5:.1f} < MA60:{ma60:.1f}"
                        )
            # ============================================================
            
            # ============================================================
            # 🔧 PATCH 1: 하락추세 Hard Cut (2025-11-17)
            # 개선: HardCut 제거, 강한 페널티만 적용 (2025-11-22)
            # 조건: MA5 < MA20 AND MA20 < MA60 AND MA5_slope < 0
            # 목적: 하락추세 중 미약한 기술적 반등에 대한 신뢰도 강력 감점
            # ============================================================
            if (ma5 > 0 and ma20 > 0 and ma60 > 0 and 
                ma5 < ma20 and ma20 < ma60 and 
                ma5_slope_hardcut < 0):

                extra_penalty = PENALTY_WEIGHTS.get('long_downtrend', 0.15)
                penalty_reasons.append(
                    f"하락추세미약반등(slope={ma5_slope_hardcut:.4f})"
                )
                penalty = add(penalty, extra_penalty)
                penalty_details['trenddown_rebound'] = extra_penalty

                logger.info(
                    f"⚠️ [TrendDown-Soft] {symbol} 하락추세 중 미약한 반등 → "
                    f"HardCut 대신 강한 페널티 적용 (0.70x, +{extra_penalty:.2f}) | "
                    f"MA5:{ma5:.1f} < MA20:{ma20:.1f} < MA60:{ma60:.1f} | "
                    f"MA5_slope:{ma5_slope_hardcut:.4f}"
                )
            
            # =============================================================================
            # 🦵 Knee Mode 판정 (무릎 전용 진입 모드) - 조기 판정으로 이동
            # =============================================================================
            # → 진입 판정은 soleus_gajami 단일 Gate만 사용
            
            # Stage 3. 반등 패턴 확인 (V자/U자)
            # ============================================================
            # 🔧 PATCH 6: 초저가 코인 패턴 예외 (2025-11-17)
            # ============================================================
            # 🔒 [V-U-PATTERN-DENOMINATOR-FIX2-0906] ★519-R2
            # ★519-R1은 FastEntry 분기 앞으로 옮겨 무릎특례를 고쳤으나,
            # 그 위치가 `if current_price < 0.1:`의 else 블록 안이라
            # 초저가 코인(<0.1원)은 여전히 V/U 계산에 도달하지 못한다.
            # 실측 994건/일이 rawVU=0/0으로 고정돼 분모를 오염시킨다.
            # 판정은 그대로 두고 raw 계산만 두 분기보다 앞으로 올린다.
            # c0/c1/c2는 L3169~3175(12칸)에서 이미 계산돼 있어 NameError 없음.
            is_v_pattern = bool(
                lt(c1, c0)
                and gt(c2, c1 * 1.003)
            )
            is_u_pattern = bool(
                le(c1, c0)
                and gt(c2, c1 * 1.003)
                and not is_v_pattern
            )

            if current_price < 0.1:
                pattern_status = "초저가예외"
                logger.debug(f"⏭️ [SmallPricePatternSkip] {symbol} 초저가 코인 ({current_price:.8f}) → 패턴 검증 스킵")
            else:
                # (삭제됨) 패턴 검증 df 계산 - Analysis 캔들 정보 사용
                
                # 🔥 수정: FastEntry 조건 대폭 완화
                # 🆕 FastEntry가 감지된 경우 패턴 검증 스킵 (무릎에서는 패턴 미형성)
                # ============================================================
                # 🔧 PATCH 3: FastEntry 강화 (2025-11-17)
                # 조건: ma5 > ma20 AND ma5_slope > 0
                #       AND regime != TRENDING_DOWN
                # 목적: 무릎 구간 조기 진입 허용
                # ============================================================
                # 🆕 Evidence-Based MA 체크 (기존 ma5 > ma20 대체)
                ma5_fe = safe_extract_scalar(indicators.get('sma_5', 0))
                ma10_fe = safe_extract_scalar(indicators.get('sma_10', 0))
                ma20_fe = safe_extract_scalar(indicators.get('sma_20', 0))
                ma5_slope_fe = safe_extract_scalar(indicators.get('ma5_slope', 0))
                ma10_slope_fe = safe_extract_scalar(indicators.get('ma10_slope', 0))
                
                # Evidence-Based 골든크로스 (3가지 중 1개라도 충족)
                evidence_ok = (
                    (ma5_fe >= ma10_fe and ma5_slope_fe > 0.01) or
                    (ma5_fe >= ma20_fe and ma5_slope_fe > 0.01) or
                    (ma10_fe >= ma20_fe and ma10_slope_fe > 0.01)
                )
                
                # 🆕 TF팀 합의: 거래량 폭발 초입 감지 (가자미근 Fast-Exception)
                # 철학: "수급 주도 전환"만으로 진입 허용
                # 조건: 거래량 3.0x 이상 (직전은 조용한 상태)
                volume_ratio_cur = safe_extract_scalar(indicators.get("volume_ratio", 0))
                
                # 🔒 안전: 어떤 경로에서도 NameError 없도록 선초기화
                fast_entry_detected = False
                
                # 거래량 폭발 조건: 비율 3.0x 이상 + 절대량 검증
                # (로그/통계 목적으로만 사용, 진입 판단에는 관여 안 함)
                current_volume = safe_extract_scalar(indicators.get("volume", 0))
                avg_volume_base = safe_extract_scalar(indicators.get("avg_volume", 1))
                if avg_volume_base is None:  # None 방어 추가
                    avg_volume_base = 1.0
                min_abs_volume = avg_volume_base * 0.5  # 평균의 50% 이상
                
                if volume_ratio_cur >= 3.0 and current_volume >= min_abs_volume:
                    fast_entry_detected = True
                    indicators["volume_explosion"] = True
                    
                    logger.info(
                        f"🚀 [VolumeExplosion] {symbol} 거래량 폭발 초입 감지 | "
                        f"Vol={fmt(volume_ratio_cur, '.2f')}x → 로그 목적 (진입 무관)"
                    )
                else:
                    indicators["volume_explosion"] = False
                
                fast_entry_detected = (
                    fast_entry_detected
                    or (evidence_ok and ma5_slope_hardcut > 0 and regime != MarketRegime.TRENDING_DOWN)
                )
                
                # 🧩 연결: 이후 KneeBaseFloor / 로깅이 인지할 수 있게 플래그 설정
                if fast_entry_detected:
                    indicators["fast_entry"] = True

                # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
                # 🔥 FastEntry 박스 의존성 제거 (2026-02-16)
                # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
                # 제거 사유:
                # - box_pos, not_near_high 박스 기반 판정 제거
                # - Lifecycle 단독 판사 원칙
                # - Lifecycle 통과 = 초입 확정 = FastEntry 허용
                # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
                
                # 🔒 [V-U-PATTERN-DENOMINATOR-FIX2-0906] ★519-R2
                # raw V/U 계산은 위 초저가 분기(if current_price<0.1) 앞으로 이동했다(중복 제거).
                # 새 숫자·조건 0개 — 계산 위치만 초저가 분기보다 앞으로 정합화.

                # 🔒 [FASTENTRY-SAFE-NOOP-PURGE-0905] ★519
                # 상시 True 안전변수는 실질 게이트가 아니므로 제거한다.
                # FastEntry의 패턴 스킵 정책 자체는 유지한다.
                if fast_entry_detected:
                    logger.debug(f"⏭️ [Entry-Pattern] {symbol} FastEntry → 패턴 검증 스킵")
                    pattern_status = "무릎특례"
                else:
                    # 🔒 [U-PATTERN-ORDER-FIX-0811]
                    # U자는 직전 바닥이 평평/하락한 뒤 현재봉이 실제로 회복한 경우만 인정.
                    # 천장 -1% 조건은 제거하되, 계단상승을 U자로 세탁하지 않는다.
                    if not (is_v_pattern or is_u_pattern):
                        pattern_status = "부적합"
                        
                        # 🆕 TRENDING_UP 초입 패턴 페널티 완화
                        # 상승 초입: 계단식 상승(V/U 부적합)도 정상 진입
                        # 조건: TRENDING_UP + 무릎 근처 + MA신호 강함
                        is_trending_up_early = (
                            regime == MarketRegime.TRENDING_UP and
                            ma_analysis.get('ma_signal', 0) >= 0.4
                        )
                        
                        if is_trending_up_early:
                            # 추세 초입: 페널티 최소화
                            score = 0.02  # 0.07 → 0.02
                            penalty_reasons.append("반등패턴약함(초입)")
                            logger.info(
                                f"🟡 [Entry-Pattern] {symbol} TRENDING_UP 초입 → 패턴 페널티 완화 | "
                                f"({c0:.1f}→{c1:.1f}→{c2:.1f}) | 신뢰도 0.98x, 감점 +{fmt(score, '.2f')}"
                            )
                        else:
                            # 🆕 무릎/예비무릎 구간 페널티 완화 (2025-12-09)
                            # 철학: 가자미근 구간은 패턴 없어도 진입 가치 높음
                            # 근거: APT 1:20/4:30 패턴 페널티로 0.39 정체
                            # ❌ 삭제됨 (머리/어깨 판별은 가자미근 판결 단계로 이관)
                            # 가자미근 외부: GAJAMI-ABSOLUTE에서 차단
                            # 가자미근 내부: GAJAMI-VERDICT SCORE에서 처리 (향후)
                            score = 0.0  # 페널티 없음 (가자미근 자격으로 충분)
                        
                        penalty = add(penalty, score)
                        penalty_details['no_pattern'] = score
                    else:  # ✅ else 추가
                        pattern_status = "V자" if is_v_pattern else "U자"
            
            # ✅ 통과 시 로그 (SSoT 기준: Soleus 여부 명시)
            soleus_flag = "Soleus ON" if soleus_gajami_ssot else "Soleus OFF"

            logger.info(
                f"✅ [Entry-Progressive] {symbol} 3단계 통과 | "
                f"MA5:{dev_pct:+.4f}% | "
                # 🔒 [V-U-PATTERN-DENOMINATOR-0905] ★519
                # 결과와 무관하게 raw V/U를 남겨 성능 분모를 확보한다.
                f"패턴:{pattern_status} rawVU={int(is_v_pattern)}/{int(is_u_pattern)} | "
                f"{soleus_flag}"
            )

            # ⚠️ 중요: 이후 함수 내에서 신뢰도를 다시 체크하는 부분이 있다면
            # 모두 confidence_threshold 변수를 사용하도록 수정
            # 예: if trust_score < 0.40 (❌) → if trust_score < confidence_threshold (✅)

            # 2️⃣ 거래량 비율 체크 (체제별 차등)
            # 단일 봉 오탐 방지: 스무딩 거래량 우선 사용
            volume_ratio = safe_extract_scalar(indicators.get('volume_ratio_smooth', 0))
            if not volume_ratio or le(volume_ratio, 0.0):
                volume_ratio = safe_extract_scalar(indicators.get('volume_ratio', 0))
            
            # 🔒 안전: volume_ratio가 float임을 보장
            volume_ratio = float(volume_ratio) if volume_ratio is not None else 0.0
            
            # 🔥 체제별 거래량 기준 (2025-11-08 최종 조정)
            # 개선 근거:
            # 1. TRENDING_UP 초입: 거래량 낮지만 배열·ADX 강력
            # 2. 0.80x는 과도 → 레벨5 신호도 차단 (KRW-0G 케이스)
            # 3. 약세·횡보는 유지 → 과진입 방지
            # 🔥 체제별 거래량 임계값 설정 (TRENDING_DOWN 완화)
            if regime == MarketRegime.TRENDING_DOWN:
                # 🆕 하락장 반등 기회 포착 (0.50 → 0.30 완화)
                vol_threshold = 0.30  # 하락장 반등: 0.30x
            elif regime == MarketRegime.BREAKDOWN:
                # 붕괴장은 엄격 유지 (진짜 위험)
                vol_threshold = 0.50  # 붕괴장: 0.50x (유지)
            elif regime == MarketRegime.RANGING:
                # 🆕 3m 타임프레임 거래량 기준 완화 (2025-11-30)
                tf_value = self.timeframe.value if hasattr(self.timeframe, 'value') else str(self.timeframe)
                vol_threshold = 0.35 if tf_value == '3m' else 0.40
            elif regime in [MarketRegime.TRENDING_UP, MarketRegime.BREAKOUT, MarketRegime.BULL_MARKET]:
                # 🆕 정배열전환 직후 거래량 기준 완화 (2025-11-26 XPL 반영)
                # MMT 케이스: 추세전환 초입은 거래량 낮음 허용
                # 해결: 레벨전환/신선한 GC(4h/1d 전용) 시 0.25x까지 완화
                level_change = self.ma_cross_tracker.get(symbol, {}).get('level_change', 0)
                gc_elapsed = self.ma_cross_tracker.get(symbol, {}).get('gc_elapsed_seconds', 99999)
                
                # 🔥 타임프레임 체크: GC 신선도는 중기/장기 전용
                tf_value = self.timeframe.value if hasattr(self.timeframe, 'value') else str(self.timeframe)
                is_mid_long_term = tf_value in ['4h', '1d']
                # 🆕 불장에서는 GC 신선도 무시
                is_fresh_gc = (is_mid_long_term and gc_elapsed < 900 and 
                               regime != MarketRegime.BULL_MARKET)

                # 정배열전환 직후(15분) 감지
                # 🔧 GC 신선도는 4h/1d에서만 적용
                if level_change > 0 or is_fresh_gc:
                    vol_threshold = 0.25
                    reason = []
                    if level_change > 0: reason.append(f"level_change:{level_change}")
                    if is_fresh_gc: reason.append(f"신선GC:{gc_elapsed}s({tf_value})")
                    
                    logger.debug(
                        f"🟢 [{symbol}] TRENDING_UP 전환 초입 → 거래량 기준 완화 | "
                        f"threshold: 0.35 → 0.25 ({', '.join(reason)})"
                    )
                else:
                    if regime == MarketRegime.BULL_MARKET:
                        # 🔥 불장: 기본 기준 자체를 낮게
                        vol_threshold = 0.20
                        threshold_label = "0.20x (BULL_MARKET)"
                        logger.debug(
                            f"🔥 [{symbol}] BULL_MARKET 거래량 기준 설정 | "
                            f"threshold={vol_threshold:.2f}x"
                        )
                    else:
                        vol_threshold = 0.30 if tf_value == '3m' else 0.35
                        threshold_label = f"{vol_threshold:.2f}x (발목 진입)"
                
                # 🆕 불장 모드: 거래량 기준 추가 완화
                if bull_strength > 0:
                    vol_threshold_relaxed = max(
                        BULL_RELAX_VOL_MIN,
                        vol_threshold * (1 - BULL_RELAX_VOL_FACTOR * bull_strength)
                    )
                    logger.debug(
                        f"🔥 [불장-거래량완화] {symbol} | "
                        f"기본={vol_threshold:.2f}x → 완화={vol_threshold_relaxed:.2f}x | "
                        f"강도={bull_strength:.2f}"
                    )
                    vol_threshold = vol_threshold_relaxed
            else:
                # 타임프레임별 기본 거래량 기준 (3m은 0.55로 완화)
                ENTRY_VOL_DEFAULTS = {
                    '1m': 0.50, '3m': 0.55, '5m': 0.60, '10m': 0.65,  # ⬅️ 3m: 0.55
                    '15m': 0.70, '30m': 0.75, '1h': 0.80, '4h': 0.80, '1d': 0.80
                }
                timeframe = self.config.get('TRADING', 'timeframe', fallback='5m')
                vol_threshold = ENTRY_VOL_DEFAULTS.get(timeframe, 0.80)
            
            # 🆕 BTC/ETH 메이저는 EntryFilter 거래량 기준에서 예외
            # (Global TIER 필터에서 이미 절대 거래대금으로 검증됨)
            symbol_str = str(symbol) if symbol else ""
            # 메이저(Tier1) 종목 여부 - SSoT: CONFIG [VOLUME].exclude_tier1
            exclude_tier1 = self.config.get_cached("exclude_tier1", [])
            is_tier1_btc_eth = symbol_str in exclude_tier1

            # 🔥 PATCH AXS-2: 거래량 부족 감점 완화 (정교화)
            # - 강한 골든크로스 + TREND/PULLBACK 준비 구간에서는 "조용한 구간"을 정상으로 본다
            # - 목표: 기회 99개를 볼륨 1개로 날리지 않기
            
            # golden_count = "현재 프레임에서 골든 상태인 MA 조합 수 (스냅샷)"
            # - Analysis: 8개 이상 = soleus 차단 (recent_cross_ok = False)
            # - Strategy: soleus 외 경로가 있으므로 8개 이상도 도달 가능
            # - 역할: 점수 보호 (과열 완화, 하드 차단 아님)
            golden_count = int(safe_extract_scalar(indicators.get("golden_count", 0), 0))
            ma_signal = float(safe_extract_scalar(indicators.get("ma_signal", 0.0), 0.0))
            entry_type = str(indicators.get("entry_type", "NONE") or "NONE")
            vol_accel = float(safe_extract_scalar(indicators.get("vol_accel", 1.0), 1.0))
            
            is_strong_gc = (golden_count >= 6)
            # is_ultra_gc: 강한 정배열 추세 보조 경로용
            # - soleus 차단 후에도 거래량 완화 적용 가능
            # - Analysis 자격 판단과 독립적 작동
            is_ultra_gc = (golden_count >= 9 and ma_signal >= 0.70)
            is_trend_setup = (entry_type in ("TREND", "PULLBACK"))
            is_prebreak_quiet_ok = (is_strong_gc and is_trend_setup and regime in (MarketRegime.RANGING, MarketRegime.TRENDING_UP))
            
            if is_prebreak_quiet_ok:
                # 기본 기준을 완화 (최대 0.20x, 초강력 GC면 0.15x까지)
                vol_threshold = min(vol_threshold, 0.20)
                if is_ultra_gc:
                    vol_threshold = min(vol_threshold, 0.15)
            
            # 🔧 [구조개선] 거래량 부족 시 페널티 적용
            # 정석: EntryFilter는 '판결용 거래량'을 하나로 정합화한다.
            # - raw vs smooth가 충돌할 때, 가자미근/강매수압 구간은 "더 강한 값"을 사용 (기회 박탈 방지)
            vol_raw = float(safe_extract_scalar(indicators.get("volume_ratio", 0.0), 0.0))
            vol_smooth = float(safe_extract_scalar(indicators.get("volume_ratio_smooth", vol_raw), vol_raw))
            
            buy_ratio_local = float(safe_extract_scalar(indicators.get("buying_ratio", 0.0), 0.0))
            pressure_local = str(indicators.get("volume_pressure", "NEUTRAL") or "NEUTRAL")
            
            is_gajami_ctx = _soleus_ssot(indicators, data_package.ma_data)
            is_strong_buy_flow = (pressure_local in ("STRONG_BUYING", "BUYING") and buy_ratio_local >= 0.65)
            
            # 판결용 거래량(정합화)
            volume_ratio_judgement = max(vol_raw, vol_smooth) if (is_gajami_ctx or is_strong_buy_flow) else vol_smooth
            
            if not is_tier1_btc_eth and volume_ratio_judgement < vol_threshold:
                logger.info(
                    f"⚠️ [EntryFilter] {symbol} 거래량 부족 | "
                    f"{fmt(volume_ratio_judgement, '.2f')}x < {vol_threshold:.2f}x → 신뢰도 감점 적용 | "
                    f"체제: {regime.value}"
                )
                
                # 부족한 정도에 비례한 페널티 강도 산출 (최대 30% 감점)
                # 예: 기준 0.30, 실제 0.20 → deficit=0.10 → penalty≈0.20 (20% 감점)
                # 예: 기준 0.30, 실제 0.10 → deficit=0.20 → penalty=0.30 (30% 감점 상한)
                deficit = max(0.0, vol_threshold - volume_ratio_judgement)
                volume_penalty = min(0.30, deficit * 2.0)
                
                # ✅ 강한 GC+트렌드 준비 구간은 감점 상한을 더 낮게(최대 15%)
                if is_prebreak_quiet_ok:
                    volume_penalty = min(volume_penalty, 0.15)
                
                # 기존 penalty_factor/penalty_reasons 시스템 활용
                penalty_reasons.append(
                    f"거래량부족({fmt(volume_ratio_judgement, '.2f')}x<{vol_threshold:.2f}x,-{volume_penalty*100:.0f}%)"
                )
                penalty_details["entry_volume"] = volume_penalty
                
                logger.debug(
                    f"   └─ 페널티: -{volume_penalty*100:.0f}% | "
                    f"누적 penalty_factor: {penalty_factor:.2f}"
                )
                
                # last_rejection_reason는 여기서 고정하지 않음
                # → 최종적으로 final_score_capped < effective_threshold에서 설정
            else:
                logger.debug(
                    f"✅ [EntryFilter] {symbol} 거래량 기준 통과 | "
                    f"{fmt(volume_ratio, '.2f')}x ≥ {vol_threshold:.2f}x"
                )

            # 🔥 PATCH AXS-2C: 거래량 축 중복 제거 (Strategy SSoT)
            # 철학:
            # - 거래량은 EntryFilter / TF Penalty에서만 판단한다.
            # - Strategy 판결부에서는 거래량을 심판에서 제거한다.
            # - 강한 추세 전환(초입)은 '조용함'이 정상이다.
            # 🔒 [2026-08-04] Analysis Producer 원본 변조 제거 — 중립화는
            #  _calculate_signal_components 이후 components 딕셔너리에서만 수행 (아래 참조)

            # ✅ 체제별 EntryFilter 통과 표시 (후단 공통 필터에서 재검사 방지)
            indicators['entry_volume_checked'] = True

            # 3️⃣ BREAKOUT 체제: 종가 확정 체크
            if regime == MarketRegime.BREAKOUT:
                # df에서 last_candle 직접 추출
                last_candle = None
                # ✅ last_candle, resistance_level은 Analysis에서 제공됨
                last_candle = {
                    'close': current_price,
                    'high': indicators.get('last_high', current_price),
                    'low': indicators.get('last_low', current_price),
                    'volume': indicators.get('last_volume', 0.0)
                }
                
                # resistance_level은 indicators에서 제공
                resistance_level = indicators.get('resistance_level', None)
                
                # 검증 데이터 확인
                if not last_candle or not resistance_level:
                    logger.debug(
                        f"ℹ️ [EntryFilter] {symbol} BREAKOUT 검증데이터 부족 | "
                        f"last_candle: {last_candle is not None}, "
                        f"resistance: {resistance_level is not None}"
                    )
                    # 데이터 부족 시 차단하지 않고 통과 (너무 보수적 방지)
                else:
                    # 종가 vs 저항선 비교
                    close_price = last_candle['close']
                    if le(close_price, resistance_level):
                        gap_pct = ((close_price - resistance_level) / resistance_level) * 100
                        
                        # -0.5% 이상 차이 나면 미확정
                        # 🔒 [BREAKOUT-LEGACY-LANE-SPLIT-0828]
                        # 이 중복 BREAKOUT 확인은 legacy route 법률이다.
                        # J2/J3/J4 독립 PASS를 전역차단하지 않는다.
                        if gap_pct < -0.5:
                            if not _j234_hardcut_pass:
                                logger.info(
                                    f"🚫 [EntryFilter] {symbol} 진입 차단 | "
                                    f"BREAKOUT 미확정 (종가 {close_price:.2f} ≤ "
                                    f"저항 {resistance_level:.2f}, 갭 {gap_pct:.2f}%)"
                                )
                                self.last_rejection_reason = "BREAKOUT 미확정"
                                return None

                            logger.info(
                                f"🟢 [BREAKOUT-LEGACY-JUDGE-NEUTRAL] {symbol} "
                                f"legacy BREAKOUT 미확정(gap={gap_pct:.2f}%) → "
                                f"J2/J3/J4 판결권 유지"
                            )
                        else:
                            logger.debug(
                                f"⚠️ [EntryFilter] {symbol} BREAKOUT 근접 허용 | "
                                f"갭 {gap_pct:.2f}% (±0.5% 이내)"
                            )
                    else:
                        logger.debug(
                            f"✅ [EntryFilter] {symbol} BREAKOUT 확정 통과 | "
                            f"종가 {close_price:.2f} > 저항 {resistance_level:.2f}"
                        )

            # ============================================================
            # ✅ GAJAMI (SSoT): Analysis 결과만 소비
            # ============================================================
            # Strategy는 가자미근을 재정의하지 않는다
            # soleus_zone / gajami_zone은 리스크·로그 용도로만 유지
            # SSoT: 가자미근 후보는 Analysis의 soleus_gajami만 신뢰
            # ✅ GAJAMI (SSoT): Analysis 결과만 소비
            is_gajami = soleus_gajami
            soleus_zone = soleus_gajami  # SSoT: soleus_gajami_ssot 체인 통일
            is_soleus_zone = soleus_zone  # ← 추가!

            # ✅ Consumer: gajami_zone 키 생성 금지 (SSoT 혼탁화 방지)
            # ✅ Consumer: soleus_zone 키도 Producer가 제공하는 값만 소비 (덮어쓰기 금지)
            pass

            if is_gajami:
                logger.info(
                    f"🦵 [SSoT-GAJAMI] {symbol} 가자미근 승인(Analysis) | "
                    f"regime={regime.value}"
                )
                indicators["gajami_entry"] = True
            else:
                logger.debug(f"❌ [SSoT-GAJAMI] {symbol} 가자미근 아님(Analysis)")
            
            # ============================================================
            # 🆕 PRE_GAJAMI_BASE: 저점 횡보 구간 감지 (대기 상태)
            # ============================================================
            # 목적: "저점 횡보" 구간을 사전 감지
            # 효과: 진입도 차단도 아닌 "대기 상태" 마킹
            # ============================================================
            pre_gajami_base = False
            
            recent_low_pg = float(safe_extract_scalar(indicators.get("recent_low", 0.0), 0.0))
            vol_ratio = float(safe_extract_scalar(indicators.get("volume_ratio", 0.0), 0.0))
            ma5_slope = float(safe_extract_scalar(indicators.get("ma5_slope", 0.0), 0.0))

            try:
                recent_range_pct = safe_extract_scalar(indicators.get("recent_range_pct", 99.0))
                
                if (
                    vol_ratio <= 0.60       # 거래량 침체
                    and abs(ma5_slope) <= 0.6   # 하락 멈춤/완만
                    and recent_range_pct <= 1.2 # 변동성 수렴
                ):
                    pre_gajami_base = True
                    indicators["pre_gajami_base"] = True
                    logger.info(
                        f"🟡 [PRE-GAJAMI-BASE] {symbol} 저점 횡보 감지 (대기) | "
                        f"vol={vol_ratio:.2f}x slope5={ma5_slope:.3f} "
                        f"range={recent_range_pct:.2f}%"
                    )
            except Exception as e:
                logger.debug(f"[PRE-GAJAMI-BASE] 계산 실패: {e}")
            
            # ============================================================
            # 🆕 HEAD_LIFT_TRIGGER: 저점 횡보 이후 '고개 드는 순간'
            # ============================================================
            # 목적: 저점 횡보 이후 "고개 드는 순간" 포착
            # 조건: PRE_GAJAMI_BASE 상태 + (기울기 반전 OR 돌파) + 거래량 회복
            # 효과: 60% 포지션으로 초입 진입
            # ============================================================
            head_lift_trigger = False
            
            try:
                prev_high = safe_extract_scalar(indicators.get("prev_high", current_price))
                close = safe_extract_scalar(indicators.get("close", current_price))
                
                if (
                    indicators.get("pre_gajami_base", False)
                    and (
                        ma5_slope > 0.0              # 기울기 반전 (핵심!)
                        or (close > prev_high)       # 횡보 상단 돌파
                    )
                    and vol_ratio >= 1.20            # 거래량 회복 시작
                ):
                    head_lift_trigger = True
                    indicators["head_lift_trigger"] = True
                    logger.info(
                        f"🔥 [HEAD-LIFT] {symbol} 고개 드는 트리거 발동! | "
                        f"slope5={ma5_slope:.3f} (prev:{prev_high:.2f}<cur:{close:.2f}) "
                        f"vol={vol_ratio:.2f}x"
                    )
            except Exception as e:
                logger.debug(f"[HEAD-LIFT] 계산 실패: {e}")
            
            # 🔥 패치 2: TRENDING_DOWN에서 MA5 이격 Hard-Block
            # 목적: MA5 한참 위에서 늦은 진입 차단 (MED 20:56 케이스)
            # 기준: MA5 위 +0.5% 초과 시 진입 차단
            if regime == MarketRegime.TRENDING_DOWN:
                ma5 = safe_extract_scalar(indicators.get('sma_5', 0))
                if ma5 > 0:
                    ma5_deviation_pct = ((current_price - ma5) / ma5) * 100
                    
                    if ma5_deviation_pct > 0.5:  # MA5 위 0.5% 초과
                        # 🔒 권력 분립(최종 판사=신뢰도) 복원:
                        # - 가자미근/승인 후보/override/softfail 상태에서는 즉결(return None) 금지
                        # - SoftPenalty로 넘겨 최종 신뢰도에서 판결
                        if (
                            _soleus_ssot(indicators, data_package.ma_data)
                            or bool(indicators.get("hard_gajami_pass", False))
                        ):
                            penalty_factor = self._apply_soft_penalty(
                                penalty_factor, 0.85, penalty_reasons,
                                f"TD-MA5-Soft({pct(ma5_deviation_pct, '.2f')}>0.5%)"
                            )
                            logger.info(
                                f"⚠️ [TD-MA5-Soft] {symbol} TD MA5 이격 → 차단 대신 페널티 | "
                                f"dev={pct(ma5_deviation_pct, '.2f')} | penalty={penalty_factor:.2f}"
                            )
                        else:
                            # ✅ TD 하드차단 단일화: TD-MA5-Block은 페널티로만 처리
                            penalty_factor = self._apply_soft_penalty(
                                penalty_factor, 0.80, penalty_reasons,
                                f"TD-MA5-Soft({pct(ma5_deviation_pct, '.2f')}>0.5%)"
                            )
                            indicators["td_ma5_soft"] = True

                            self.last_rejection_reason = f"TD-MA5-Block (이격 {pct(ma5_deviation_pct, '.2f')} > 0.5%)"
                            logger.info(
                                f"⚠️ [TD-MA5-SOFT] {symbol} MA5 이격 과다 → 차단 대신 페널티 | "
                                f"dev={pct(ma5_deviation_pct, '.2f')} | penalty_factor={penalty_factor:.2f}"
                            )
            
            # 🔥 패치 3: TRENDING_DOWN에서 골든크로스 신선도 Hard-Block
            # 목적: 골든크로스 150초 전 발생 같은 늦은 진입 차단 (MED 20:56 케이스)
            # 기준: 3m 타임프레임에서 골든크로스 120초(2분) 초과 시 진입 차단
            if regime == MarketRegime.TRENDING_DOWN:
                gc_elapsed_sec = self.ma_cross_tracker.get(symbol, {}).get('gc_elapsed_seconds', None)
                
                if gc_elapsed_sec is not None and gc_elapsed_sec > 120:  # 2분 초과
                    # ❌ ssot_candidate_override / analysis_candidate_softfail 제거 → 단순화
                    penalty_factor = self._apply_soft_penalty(
                        penalty_factor, 0.85, penalty_reasons,
                        f"TD-GC-Stale-Soft({gc_elapsed_sec}s>120s)"
                    )
                    indicators["td_gc_stale_soft"] = True
                    self.last_rejection_reason = f"TD-GC-Stale (경과 {gc_elapsed_sec}초 > 120초)"
                    logger.info(
                        f"⚠️ [TD-GC-Stale-SOFT] {symbol} GC 신선도 부족 → 페널티 | "
                        f"elapsed={gc_elapsed_sec}s | penalty_factor={penalty_factor:.2f}"
                    )
            
            # 가자미근 구간은 Early Block 면제
            if is_soleus_zone:
                logger.debug(
                    f"🟢 [Early-Block] {symbol} 가자미근 구간 → Early Block 면제"
                )
            
            # 🔒 모든 체제 공통: SoleusZone이 아니면 진입 불가
            # 철학: 가자미근 구간에서만 진입
            # 효과: NXPC 09:02 같은 TRENDING_UP 착시 진입 완전 차단
            # SoleusZone 사전 체크 (차단은 최종부에서 단일 처리)
            # ❌ 삭제됨 (GAJAMI-ABSOLUTE RULE이 이미 검증)
            # soleus_zone과 gajami_zone은 동일한 자격 체크
            # 중복 검증 불필요
                
            # ========================================
            # 1. 시장 체제 필터 (철학: "틀린 진입을 하지 않는다")
            # ========================================
            # 🔧 골든크로스 카운트 공통 추출 (모든 체제에서 사용)
            ma_data_td = data_package.ma_data or {}
            golden_count_td = ma_data_td.get('golden_count', 0)
            
            # 🔒 TRENDING_DOWN 약한 골든크로스 차단 (5x10 단독 골든 오판 방지)
            # 목적: DOGE 05:08 같은 약한 골든크로스 함정 방지
            # 기준: 5x10 단독 + MA 간격 < 0.20% + 거래량 < 1.50x
            if regime == MarketRegime.TRENDING_DOWN:
                if golden_count_td == 1:  # 5x10 단독 골든
                    # MA 간격 추출
                    ma_gap_info = ma_data_td.get('ma_gap_meta', {})
                    ma_gap_5_10 = abs(ma_gap_info.get('gap_5_10', 0.0))
                    
                    # 거래량 추출
                    vol_ratio_gc = safe_extract_scalar(indicators.get("volume_ratio", 0.0))
                    
                    # 약한 골든크로스 판정: MA 간격 좁고 거래량 약함
                    # ✅ 단, TD-KNEE(PrelimKnee 승격)에서는 중복 태클 금지
                    # ✅ 또한 "가자미근 확정/승인 후보"는 HardCut 금지 → Soft로 넘겨 최종 신뢰도에서 판결
                    if (
                        ma_gap_5_10 < 0.20
                        and vol_ratio_gc < 1.50
                        and not _soleus_ssot(indicators, data_package.ma_data)
                    ):
                        # ✅ TD 하드차단 단일화: TD-WeakGolden은 하드 리턴 금지 (TD-STRICT만 하드)
                        penalty_factor = self._apply_soft_penalty(
                            penalty_factor, 0.80, penalty_reasons, "TD-WeakGolden"
                        )
                        indicators["td_weak_golden_soft"] = True
                        indicators["td_weak_golden_soft_reason"] = f"gap={ma_gap_5_10:.2f}%,vol={vol_ratio_gc:.2f}x"

                        self.last_rejection_reason = (
                            f"TD-WeakGoldenCross (MA간격:{ma_gap_5_10:.2f}%, vol:{vol_ratio_gc:.2f}x)"
                        )
                        logger.info(
                            f"⚠️ [TD-WeakGolden-SOFT] {symbol} 약한 골든크로스 → 차단 대신 페널티 | "
                            f"MA5-MA10간격:{ma_gap_5_10:.2f}% (<0.20%) | "
                            f"거래량:{vol_ratio_gc:.2f}x (<1.50x) | "
                            f"penalty_factor={penalty_factor:.2f}"
                        )
                    elif ma_gap_5_10 < 0.20 and vol_ratio_gc < 1.50:
                        # 후보 유지(가자미근/승인) 케이스: Soft 페널티만 적용
                        penalty_reasons.append(f"TD-WeakGolden(soft,{ma_gap_5_10:.2f}%,{vol_ratio_gc:.2f}x)")
                        logger.info(
                            f"⚠️ [TD-WeakGolden-Soft] {symbol} 가자미근/후보 유지 → 차단 대신 페널티 | "
                            f"gap={ma_gap_5_10:.2f}% vol={vol_ratio_gc:.2f}x"
                        )
            
            # 🆕 메이저 코인 체제 필터 공통 정의 (2025-12-04 패치 9)
            symbol_str = str(symbol) if symbol else ""
            # 메이저(Tier1) 종목 여부 - SSoT: CONFIG [VOLUME].exclude_tier1
            exclude_tier1 = self.config.get_cached("exclude_tier1", [])
            is_tier1_btc_eth = symbol_str in exclude_tier1
            
            # 🆕 TRENDING_DOWN 조건부 허용 (2025-11-01)
            # 🔧 박스 하단(BoxMode)에서는 체제 기반 하락장 필터를 건너뛰고
            #    박스 전략 기준으로 평가 (하락장 기술적 반등 필터 무력화) - 2025-11-23
            if regime == MarketRegime.TRENDING_DOWN:
                # 🔒 TRENDING_DOWN 체제: FAIL-SAFE (SoleusZone 기준)
                # 철학: 가자미근 = 저점 후 힘있는 초기 반등 구간(Zone)
                # 🔥 PATCH TD-BREAKOUT: TD FailSafe 돌파 성립 시 무력화
                # 철학: 체제는 "환경", 돌파는 "사실" → 사실 우선
                # 효과: 급등 초입(3분 체제 지연)에서도 진입 가능
                # 조건: 골든크로스 6개 + 거래량 1.2배 + EEE 신뢰도 0.50 이상
                
                # 🔒 [TD-BREAKOUT-FAKE-BYPASS-REMOVE-0817]
                # 판정권 없는 선행 reason/로그 분기 제거.
                
                # 데이터 추출
                ma_analysis = data_package.ma_data or {}
                ma_deviation_value = ma_analysis.get('ma_deviation', {}).get('value', 0)
                
                # ✅ vol_accel은 Analysis에서 계산되어 indicators로 전달됨
                # [제거] 이미 정규화됨: vol_accel = indicators.get('vol_accel', 1.0)
                
                # ADX Delta
                try:
                    adx_series = indicators.get('adx_series', pd.Series())
                    if len(adx_series) >= 3:
                        adx_delta = adx_series.iloc[-3] - adx_series.iloc[-1]
                    else:
                        adx_delta = 0
                except Exception as e:
                    logger.warning(f"[{symbol}] ADX delta 계산 실패: {e}")
                    adx_delta = 0
                
                # 🆕 TRENDING_DOWN 예외 조건 재설계 (2025-11-27 확정)
                # 근거: 실제 로그 샘플 분석 결과 기존 조건(4개 모두 충족) 불가능
                #       - 이격도 ≤ -2.0%: 거의 달성 불가 (실제 대부분 +0.1~+0.7%)
                #       - ADX 약화 > 2.25: 극단적 반전만 포착 (실제 대부분 0.0)
                # 
                # 설계: 필수 조건(거래량+ATR) + 보조 조건(이격도 또는 ADX) 구조
                #       하락장 반등 패턴 = 거래량 증가 + 변동성 확대 + (과매도 또는 추세 약화)
                
                # 🔥 골든크로스에 따른 조건 완화 (2025-11-30)
                # 철학: 골든크로스 강도에 비례해 거래량 기준 완화
                # 🎯 거래량 가속 기준 - 골든크로스 강도별 차등
                # 🆕 메이저 코인은 0.45로 통일 (2025-12-04 패치 9)
                # 🔥 가자미근(Knee+Soleus) 반등 초입은 거래량 완화 (2026-01-06)
                
                # 🦵 가자미근 반등 초입 우선 판정
                if is_tier1_btc_eth:
                    vol_accel_threshold = 0.45
                    threshold_label = "0.45x (메이저 완화)"
                elif golden_count_td >= 3:
                    # 3중 골크: 대폭 완화
                    vol_accel_threshold = 0.60  # 0.80 → 0.60 완화
                    threshold_label = "0.60x (3중 골크: 강력)"
                elif golden_count_td >= 2:
                    # 🔥 2중 골크 기준 완화: 0.50 → 0.30 (SUI 케이스)
                    vol_accel_threshold = 0.30
                    threshold_label = "0.30x (2중 골크: 반등 초입 허용)"
                else:
                    # 🔥 NXPC 등 소형 알트 반등 패턴 반영 (0.20 → 0.15)
                    vol_accel_threshold = 0.15
                    threshold_label = "0.15x (발목 진입: T3 특화)"

                # 필수 조건: 반등의 핵심 신호 (거래량 + 변동성)
                core_ok = (
                    vol_accel >= vol_accel_threshold and  # 🔥 골크별 차등 적용
                    ge(atr_pct, 0.10)                       # ATR 변동성 유지
                )

                # 🎯 보조 조건 - 이격도/ADX 조합 (진짜 오판만 차단)
                # 철학:
                #   - 골든크로스가 있는 경우(2중/3중 골크)는 기존 조건 유지
                #   - 골든크로스가 없는 순수 하락장(TD)에서는
                #       ▷ 아직 MA 위/근처(+ ma_deviation)이고
                #       ▷ ADX도 거의 안 꺾인 경우(adx_delta ≈ 0)
                #     → "하락 중간 눌림" 패턴으로 간주하여 진입 금지
                try:
                    ma5_slope = safe_extract_scalar(indicators.get('ma5_slope', 0))
                    is_ma5_rising = ma5_slope >= 0.0
                except Exception:
                    ma5_slope = 0
                    is_ma5_rising = False

                if golden_count_td >= 2:  # 🔥 2중/3중 골크만 완화
                    # 강한 골크: 완화된 조건 적용
                    secondary_ok = (
                        (ma_deviation_value <= 0.5 and is_ma5_rising) or  # 🔥 0.3 → 0.5 완화
                        adx_delta >= 0.3
                    )
                elif golden_count_td == 1:  # 🆕 단일 골크: 중간 완화 (APT 02:58:18)
                    # 단일 골크 + MA5 상승 → 완화
                    secondary_ok = (
                        (ma_deviation_value <= 0.5 and is_ma5_rising) or  # 🔥 0.3 → 0.5 완화
                        adx_delta >= 0.3
                    )
                    
                    # 디버그 로그 (Secondary 판단 근거 기록)
                    if not secondary_ok:
                        logger.debug(
                            f"⚠️ [Secondary-Single-GC] {symbol} | "
                            f"골크:1개 | ma_dev={pct(ma_deviation_value, '+.2f')} (≤0.5), "
                            f"MA5기울기={fmt(ma5_slope, '+.4f')} (≥0.0), "
                            f"ADX약화={fmt(adx_delta, '+.2f')} (≥0.3) | "
                            f"→ secondary_ok={secondary_ok}"
                        )
                else:
                    # 골크 없음: 엄격한 조건 유지
                    # 🔥 개선 (2025-12-09): ma_deviation 단독 허용 불가
                    # - 문제: MEW 02:31 하락 중 거래량 폭증만으로 진입 → 손실
                    # - 개선: ma_deviation <= 0.3 조건에 MA5 상승 조건 추가
                    # - 철학: 하락 중간이 아닌 진짜 가자미근에서만 허용
                    
                    secondary_ok = (
                        (ma_deviation_value <= 0.3 and is_ma5_rising) or  # 🔥 MA5 상승 동반 필수
                        adx_delta >= 0.3                                 # 추세 약화는 그대로 유지
                    )
                    
                    # 디버그 로그 (Secondary 판단 근거 기록)
                    if not secondary_ok:
                        logger.debug(
                            f"⚠️ [Secondary-Check] {symbol} | "
                            f"ma_dev={pct(ma_deviation_value, '+.2f')} (≤0.3), "
                            f"MA5기울기={fmt(ma5_slope, '+.4f')} (≥0.0), "
                            f"ADX약화={fmt(adx_delta, '+.2f')} (≥0.3) | "
                            f"→ secondary_ok={secondary_ok}"
                        )
                
                # 🔧 [2단계 구조] 2025-12-07: 극단만 HardCut, 나머지 Penalty
                # - core_fail (진짜 쓰레기) → HardCut
                # - core_ok + secondary_fail (애매한 TD) → Penalty 35%
                
                # 1단계: core 조건 체크 (필수: vol_accel + ATR)
                if not core_ok:
                    # ✅ 최종 판사=신뢰도 원칙:
                    # - 가자미근/승인 후보/override/analysis softfail 상태에서는 즉결(return None) 금지
                    # - SoftPenalty로 넘겨 최종 신뢰도에서 판결
                    if (
                        _soleus_ssot(indicators, data_package.ma_data)
                    ):
                        td_core_penalty = 0.25  # "극단"이지만 후보 유지면 차단 대신 강 페널티
                        penalty_reasons.append(
                            f"TD극단-Soft(vol={fmt(vol_accel, '.2f')}<{vol_accel_threshold:.2f},"
                            f"atr={pct(atr_pct, '.2f')}<0.10,-25%)"
                        )
                        logger.info(
                            f"⚠️ [체제필터-Soft] {symbol} TRENDING_DOWN 극단 조건이나 후보 유지 → 차단 대신 페널티 | "
                            f"vol={fmt(vol_accel, '.2f')}x(thr={vol_accel_threshold:.2f},{threshold_label}) | "
                            f"atr={pct(atr_pct, '.2f')} | penalty_factor={penalty_factor:.2f}"
                        )
                    else:
                        if not is_regime_early:
                            penalty_factor = self._apply_soft_penalty(
                                penalty_factor, 0.65, penalty_reasons, "TD-Extreme-Soft"
                            )
                            indicators["td_extreme_soft"] = True
                            logger.info(
                                f"⚠️ [체제필터-Extreme-SOFT] {symbol} TRENDING_DOWN 극단 조건 → 차단 대신 페널티 | "
                                f"penalty_factor={penalty_factor:.2f}"
                            )
                        else:
                            indicators["td_extreme_soft_skipped"] = True
                            logger.debug(
                                f"🟢 [체제필터-Early-Skip] {symbol} RegimeEarly=True → TD-Extreme-Soft 생략"
                            )
                
                # 2단계: secondary 조건 체크 (보조: ma_deviation + ADX)
                elif not secondary_ok:
                    # 🆕 [무릎 모드] TD 페널티 완화 (2025-12-08)
                    # 철학: 골든크로스 + 매수 급증 + 무릎 반등 시 페널티 완화
                    # 근거: SOL 00:07:43 케이스 (EEE=0.60 → 페널티로 0.22 급락)
                    
                    # 무릎 모드 조건 체크
                    try:
                        # MA 구조 확인
                        # [제거] 이미 정규화됨: ma5 = indicators.get('ma5', 0)
                        # [제거] 이미 정규화됨: ma10 = indicators.get('ma10', 0)
                        ma5_above_ma10 = (ma5 > ma10 * 0.998) if ma5 > 0 and ma10 > 0 else False
                        
                        # 거래량 비율
                        volume_ratio_val = safe_extract_scalar(indicators.get('volume_ratio', 0))
                        
                        # 매수 압력 (거래량 분석에서 추출)
                        buying_ratio = safe_extract_scalar(indicators.get('buying_ratio', 0))
                        
                        # 🔧 PATCH 4-MINIMAL: 억울한 TD 차단만 해소
                        # 철학: Analysis(Soleus)가 인정한 TD-KNEE는 신뢰한다
                        
                        # TD-KNEE 기본 조건 (Analysis 신뢰)
                        is_td_knee_basic = is_soleus_zone
                        
                        # 🔧 PATCH 5-MINIMAL: TD-KNEE 기본 보호용 선행 패널티 캡
                        # 철학: Analysis가 인정한 TD-KNEE는 '죽이지 않는다'
                        if is_td_knee_basic:
                            # TD 패널티 적용 전 누적 패널티 보호 (억울한 차단 방지)
                            if penalty_factor < 0.80:
                                logger.info(
                                    f"🛡️ [TD-KNEE-PROTECT] {symbol} "
                                    f"선행 패널티 보호 | penalty_factor {penalty_factor:.2f} → 0.80"
                                )
                                penalty_factor = 0.80
                        
                        # TD-KNEE 강화 조건 (기존 로직 유지)
                        is_td_knee_strong = (
                            is_td_knee_basic
                            and ma5_above_ma10
                            and volume_ratio_val >= 0.7
                            and golden_count_td >= 1
                            and buying_ratio > 0.60
                        )
                        
                    except Exception as e:
                        logger.warning(f"[{symbol}] 무릎 모드 체크 실패: {e}")
                        is_td_knee_basic = False
                        is_td_knee_strong = False
                    
                    if is_td_knee_strong:
                        # TD 위험도 관측값
                        td_regime_penalty = 0.10
                        penalty_reasons.append(
                            "TD무릎모드(강화,관측)"
                        )
                        penalty_details["regime_td_knee_strong"] = td_regime_penalty
                        
                        logger.info(
                            f"✅ [TD-KNEE-STRONG] {symbol} 강화 조건 충족 | "
                            f"골크:{golden_count_td}개 | "
                            f"거래량:{fmt(volume_ratio_val, '.2f')}x ≥0.7 | "
                            f"매수:{buying_ratio*100:.1f}% >60% | "
                            f"TD위험도=0.10 관측 (scoring 제외)"
                        )
                    
                    elif is_td_knee_basic:
                        # TD 위험도 관측값
                        td_regime_penalty = 0.20  # 🔑 0.35 → 0.20
                        penalty_reasons.append(
                            "TD무릎모드(기본,관측)"
                        )
                        penalty_details["regime_td_knee_basic"] = td_regime_penalty
                        
                        logger.info(
                            f"✅ [TD-KNEE-BASIC] {symbol} Analysis 인정 TD-KNEE | "
                            f"soleus_zone=True | "
                            f"TD위험도=0.20 관측 (scoring 제외)"
                        )
                        
                    else:
                        # ❌ 기존 TD 페널티 유지
                        td_regime_penalty = 0.35
                        penalty_reasons.append(
                            f"TD체제-부정적(ma_dev={fmt(ma_deviation_value, '+.2f')},adx_Δ={fmt(adx_delta, '+.2f')},-35%)"
                        )
                        penalty_details["regime_td"] = td_regime_penalty
                        
                        logger.info(
                            f"⚠️ [체제필터-Penalty] {symbol} TRENDING_DOWN 반등 불확실 | "
                            f"[필수] 거래량가속:{fmt(vol_accel, '.2f')}x ✅ ATR:{pct(atr_pct, '.2f')} ✅ | "
                            f"[보조] 이격도:{pct(ma_deviation_value, '+.2f')} ∈[-0.5,+1.5] "
                            f"({'✅' if -0.5 <= ma_deviation_value <= 1.5 else '❌'}) | "
                            f"[보조] ADX약화:{fmt(adx_delta, '+.2f')} ≥0.0 ({'✅' if adx_delta >= 0.0 else '❌'}) | "
                            f"골크:{golden_count_td}개 → 진단 penalty=35% 기록 (scoring 제외)"
                        )
                    
                    logger.debug(
                        f"   └─ 누적 penalty_factor: {penalty_factor:.2f}"
                    )
                
                else:
                    # ============================================================
                    # 🔒 TRENDING_DOWN 체제예외도 GAJAMI_ZONE 필수
                    # - XLM 15:59 케이스: pos=1.00(박스 상단)인데 체제예외로 진입 발생
                    # - 체제예외가 gajami_zone 우회 → 구조적으로 차단
                    # ============================================================
                    # ⚠️ GAJAMI-ABSOLUTE 중복 체크 (안전장치로 유지)
                    # - 상위에서 GAJAMI-ABSOLUTE가 이미 체크했으므로 이론상 중복
                    # - else 블록 구조상 안전장치로 유지 (삭제 시 하위 로직 노출)
                    # ============================================================
                    # ✅ B안: 가자미근 후보면 TD에서도 차단 금지 (페널티만)
                    if not soleus_gajami:
                        # ✅ TD 하드차단 단일화: 여기서는 하드 리턴 금지 (TD-STRICT만 하드)
                        # - 비가자미근이면 대부분 어차피 SoleusZone에서 최종 차단되므로
                        #   여기서는 "사유 기록 + 페널티"로만 처리한다.
                        penalty_factor = self._apply_soft_penalty(
                            penalty_factor, 0.60, penalty_reasons, BLOCK_REASON_TD_GATE
                        )
                        indicators["td_exception_soft"] = True
                        indicators["td_exception_soft_reason"] = BLOCK_REASON_TD_GATE

                        logger.info(
                            f"⚠️ [{BLOCK_REASON_TD_GATE}-SOFT] {symbol} TRENDING_DOWN 비가자미근 → 차단 대신 페널티 | "
                            f"penalty_factor={penalty_factor:.2f}"
                        )
                    else:
                        # 🔒 TD 예외: golden<4는 hard 차단 금지
                        # Analysis SSoT에서 soleus=True면 B안 원칙상 최종판사로 이관
                        if golden_count_td < 4:
                            _td_align_penalty = 0.05
                            penalty = add(penalty, _td_align_penalty)
                            penalty_reasons.append(f"TD-ALIGN-SOFT(+{_td_align_penalty:.2f})")
                            penalty_details["td_align_soft"] = _td_align_penalty
                            logger.info(
                                f"⚠️ [TD-ALIGN-SOFT] {symbol} TRENDING_DOWN + "
                                f"golden={golden_count_td} < 4 → 가자미근 확정, hard 차단 금지 → "
                                f"penalty +{_td_align_penalty:.2f}"
                            )
                        # ======================================================
                        # 🔒 PATCH-B8: TD 체제예외 "부적합 패턴" 절대 차단
                        # ======================================================
                        # 목적: PUMP처럼 "차트상 가자미근 없음(=패턴 부적합)"은 진입 금지
                        # 철학: "가자미근은 형상(5·10·20 상향 접힘)이 필수"
                        # 정석: TD 체제예외라도 형상 불충족이면 절대 차단
                        # ======================================================
                        status = (pattern_status or "").strip()
                        if (
                            status == "부적합"
                            and not _j234_hardcut_pass
                        ):
                            # 🔒 [J1-LEGACY-VETO-LANE-SPLIT-0828]
                            # TD 캔들패턴 법률은 J1 가자미 체제예외 전용.
                            # J234 PASS를 전역차단하지 않는다.
                            # 🔒 [2026-08-02] 死코드가 된 하드 차단 복원 (우회로 철거)
                            #  이 블록은 상위 else(soleus_gajami==True 확정) 안에 있어
                            #  기존 `if soleus_gajami:`는 항상 참이었고 else의 return None은
                            #  도달 불가능했다. 로그 실측(07-31~08-02):
                            #    [TD-EXCEPTION-BLOCK]  0건  ← 3일간 한 번도 실행 안 됨
                            #    [TD-EXCEPTION-SOFT] 345건
                            #  페널티 0.80도 PF_MIN_THRESHOLD 클램프로 실효 없음.
                            #  진입 실측: 체제예외 20건 중 이 경로 14건(70%),
                            #             오진입 93%, 실현평균 -0.557%, 합계 -7.79%,
                            #             기회있음(fw10 max>=+0.8%) 희생 1건.
                            logger.info(
                                f"🚫 [TD-EXCEPTION-BLOCK] {symbol} TRENDING_DOWN + "
                                f"캔들패턴=부적합 → 진입 금지 (정석: 형상/접힘 불충족)"
                            )
                            self.last_rejection_reason = "TD_EXCEPTION_NO_PATTERN"
                            return None
                        
                        _td_ma5  = float(safe_extract_scalar(indicators.get('ma5', 0.0), 0.0))
                        _td_ma10 = float(safe_extract_scalar(indicators.get('ma10', 0.0), 0.0))
                        if _td_ma10 > 0 and abs(_td_ma5 - _td_ma10) / _td_ma10 <= 0.0001:
                            penalty_factor = self._apply_soft_penalty(
                                penalty_factor, 0.80, penalty_reasons, "TD-GC-Birth-Soft"
                            )
                            logger.info(f"⚠️ [TD-GC-Birth-SOFT] {symbol} TD + MA5≈MA10(GC탄생) → 차단 대신 페널티")
                        penalty_reasons.append("TD-GAJAMI-RELAX")
                        logger.info(
                            f"⚠️ [TD-GAJAMI-RELAX] {symbol} TRENDING_DOWN + 가자미근 → 페널티 적용"
                        )

                    # core + secondary 모두 통과 + 가자미근 확인 → 진입 허용
                    ma5_slope = safe_extract_scalar(indicators.get('ma5_slope', 0.0))  # 🔥 기본값 명시
                    logger.info(
                        f"✅ [체제예외] {symbol} TRENDING_DOWN 반등 패턴 진입 허용 (가자미근) | "
                        f"거래량가속:{fmt(vol_accel, '.2f')}x | ATR:{pct(atr_pct, '.2f')} | "
                        f"이격도:{pct(ma_deviation_value, '+.2f')} | ADX약화:{fmt(adx_delta, '+.2f')} | "
                        f"MA5기울기:{fmt(ma5_slope, '+.4f')} | "
                        f"골크:{golden_count_td}개 → 필수+보조 조건 충족"
                    )
                    

            # 🆕 TRENDING_UP 거래량가속 필터 (2025-12-03 추가)
            # 철학: 상승장에서 과열/막차 방지를 위한 거래량 검증
            # 기준: TRENDING_DOWN(0.65x) 대비 강화 (과도한 FOMO 방지)
            # 🔧 PATCH (2025-12-04): 1.00 → 0.70 완화 (정상 계단 상승 포착)
            elif regime == MarketRegime.TRENDING_UP:
                
                # 🆕 SHOULDER ZONE (폭등 어깨 구간) 차단 로직
                # - 사례: SUPER 09:01, 저점대비 +5.6%, V:6.8x, 매수:87%
                #   → 폭등 직후 어깨에서 진입 → 12분 후 TRENDING_DOWN 전환 → 손실
                # - 철학: 가자미근은 살리고, 어깨·꼭대기는 무조건 차단
                # - 조건: 고점 직후(조정<0.2%) + 급등(3.5%+) + 거래량(3.0x+) + 매수(75%+)
                
                # 🆕 SHOULDER ZONE + COOLDOWN (통합 로직)
                
                # 1. current_timestamp 확보 (최우선)
                try:
                    current_timestamp = data_package.timestamp
                except Exception:
                    current_timestamp = time.time()
                
                # 2. 고점 대비 조정률 계산
                # ✅ recent_high는 Analysis에서 계산되어 indicators로 전달됨
                recent_high = indicators.get('recent_high', current_price)
                correction_pct = ((recent_high - current_price) / recent_high * 100) if recent_high > 0 else 0.0
                
                buying_ratio = indicators.get("buying_ratio", 0.0)
                volume_ratio = safe_extract_scalar(indicators.get("volume_ratio", 0.0))
                spike_up_pct = safe_extract_scalar(indicators.get("spike_up_pct", 0.0))  # 🔧 누락된 변수 추가
                
                # 🆕 PATCH 2: Adaptive Shoulder Thresholds
                # - 종목별 변동성·거래량·매수강도 기반 자동 조절
                atr_pct_shoulder = atr_pct / 100  # 이미 정규화된 변수 사용 (예: 1.2 → 0.012)
                
                # ATR 기반 급등 기준 (기본 3.5%, ATR 높으면 비례 증가)
                spike_up_threshold = max(3.5, atr_pct_shoulder * 500)  # ATR=1% → 5% 급등 기준
                
                # 거래량 기준 (기본 3.0x, 필요시 동적 조절 가능)
                vol_threshold = 3.0
                
                # 매수 비율 기준 (기본 75%)
                buy_threshold = 0.75
                
                # 3. Shoulder Zone 조건 체크 (Adaptive)
                is_shoulder_zone = (
                    spike_up_pct is not None
                    and correction_pct <= 0.2                       # 고점 바로 직후
                    and spike_up_pct >= spike_up_threshold          # ATR 기반 급등
                    and volume_ratio >= vol_threshold               # 거래량 과열
                    and buying_ratio >= buy_threshold               # 급격한 매수 우세
                )
                
                # 4. Shoulder Zone 감지 → 쿨다운 정보 저장 + 즉시 차단
                if is_shoulder_zone:
                    # 쿨다운 정보는 그대로 저장
                    ctx = self.recent_context.setdefault(symbol, {})
                    ctx["shoulder_flag"] = True
                    ctx["shoulder_time"] = current_timestamp

                    # ✅ 정석: 어깨는 '즉결 차단'이 아니라 '강페널티 + 최종 판사'
                    extra_penalty = PENALTY_WEIGHTS.get("sudden_buying_late", 0.25)
                    penalty_reasons.append("ShoulderZone-Soft")
                    penalty = add(penalty, extra_penalty)
                    penalty_details["shoulder_zone"] = extra_penalty

                    logger.warning(
                        f"⚠️ [ShoulderZone-Soft] {symbol} 어깨(추격 위험) 감지 → HardCut 제거, 페널티 적용 | "
                        f"penalty_factor={penalty_factor:.2f}"
                    )
                
                # 🆕 PATCH 1: Shoulder 쿨다운 즉시 해제
                # - Knee/PreKnee/가자미근 발생 시 ShoulderCooldown 해제
                
                has_soleus_pattern = (
                    spike_up_pct is not None
                    and 0.3 <= spike_up_pct <= 1.5
                    and between(volume_ratio, 1.0, 2.0)
                    and 0.60 <= buying_ratio <= 0.80
                )
                
                # Knee/PreKnee/가자미근 발생 시 즉시 쿨다운 해제
                if has_soleus_pattern:
                    ctx = self.recent_context.setdefault(symbol, {})
                    if ctx.get("shoulder_flag", False):
                        logger.info(
                            f"🟢 [ShoulderCooldown-Release] {symbol} 새 가자미근 감지 → 쿨다운 해제 | "
                            f"Soleus:{has_soleus_pattern}"
                        )
                        ctx["shoulder_flag"] = False
                        ctx["shoulder_time"] = None
                
                # 5. Shoulder Cooldown 체크 (9봉 = 27분)
                ctx = self.recent_context.get(symbol, {})
                shoulder_flag = ctx.get("shoulder_flag", False)
                shoulder_time = ctx.get("shoulder_time", None)
                
                if shoulder_flag and shoulder_time:
                    cooldown_duration = 9 * self.tf_seconds  # 9봉 = 27분
                    elapsed = current_timestamp - shoulder_time
                    
                    if elapsed <= cooldown_duration:
                        remaining_minutes = (cooldown_duration - elapsed) / 60
                        
                        # ✅ 정석: 쿨다운은 즉결 차단이 아니라 강한 페널티
                        extra_penalty = PENALTY_WEIGHTS.get("shoulder_cooldown", 0.20)
                        penalty_reasons.append("ShoulderCooldown-Soft")
                        penalty = add(penalty, extra_penalty)
                        penalty_details["shoulder_cooldown"] = extra_penalty

                        logger.warning(
                            f"⚠️ [ShoulderCooldown-Soft] {symbol} 쿨다운 중 → HardCut 제거, 페널티 적용 | "
                            f"경과: {elapsed/60:.1f}분 / 총: {cooldown_duration/60:.1f}분 | "
                            f"penalty_factor={penalty_factor:.2f}"
                        )
                    else:
                        # 쿨다운 해제 (시간 초과)
                        ctx["shoulder_flag"] = False
                        logger.debug(f"[ShoulderCooldown] {symbol} 쿨다운 해제 (경과: {elapsed/60:.1f}분)")
                        
                # Volume Acceleration 계산 (개선: 추세 기반)
                # ✅ vol_accel은 Analysis에서 계산되어 indicators로 전달됨
                vol_accel = indicators.get('vol_accel', 1.0)
                
                # 상승장 거래량 기준 (TRENDING_DOWN 대비 강화)
                # 🆕 메이저 코인은 0.45로 완화 (2025-12-04 패치 9)
                # 🔥 가자미근(Knee+Soleus) 반등 초입은 거래량 완화 (2026-01-06)
                
                # 🦵 가자미근 반등 초입 우선 판정
                if is_tier1_btc_eth:
                    vol_accel_threshold = 0.45
                    threshold_label = "0.45x (메이저 완화)"
                else:
                    vol_accel_threshold = 0.30  # 🔥 0.60 → 0.30 (B안: 발목 진입 허용)
                    threshold_label = "0.30x (발목 진입)"
                
                # ATR 변동성 확인
                atr_raw = indicators.get('atr', 0)
                atr = safe_extract_atr(atr_raw)
                atr_pct = (atr / current_price) * 100 if current_price > 0 and atr > 0 else 0
                # TRENDING_UP 알트 저변동 상태 분리
                # 실측 근거는 GEOD/MIRA 등 알트 표본이므로 BTC/ETH 기존 계약은 유지한다.
                # 🔒 ★474-R2 [ATR-HARDCUT-RELIEF-PARITY-0901-R2] → 09-01 §208 ★폐기.
                # RANGING(위 [ATR-LOW-BLOCK] 주석)과 ★동일 사유 — 잘못된 relief owner 철회.
                # allow_hardcut / _j234_hardcut_pass 기반 저ATR 면제는 ★재사용 금지.
                # LOW / INVALID 모두 이 앞단에서 HardCut 한다.
                _tu_alt_low_or_invalid_atr = bool(
                    not is_tier1_btc_eth
                    and atr_pct < self.tu_min_atr_percent
                )
                if _tu_alt_low_or_invalid_atr:
                    self.last_rejection_reason = "ATR_LOW_BLOCK_TU"
                    logger.info(
                        f"🚫 [ATR-LOW-BLOCK-TU] {symbol} "
                        f"TRENDING_UP 알트 저변동성/ATR무효 진입 차단 | "
                        f"ATR:{pct(atr_pct, '.2f')} "
                        f"<{self.tu_min_atr_percent:.2f}% | "
                        f"거래량가속:{fmt(vol_accel, '.2f')}x | "
                        f"kind={'INVALID' if not (current_price > 0.0 and atr > 0.0) else 'LOW'} "
                        f"soleus_ctx={int(soleus_gajami_ssot)} "
                        f"j1={int(indicators.get('judge1_gajami_pass', 0) or 0)} "
                        f"J2={int(indicators.get('judge2_osc_pass', 0) or 0)} "
                        f"J3={int(indicators.get('judge3_cycle_pass', 0) or 0)} "
                        f"J4={int(indicators.get('judge4_reversal_pass', 0) or 0)} "
                        f"j234={int(_j234_hardcut_pass)} | "
                        f"→ LOW/INVALID 모두 HardCut (★474 폐기)"
                    )
                    return None
                _tu_atr_eval_min = (
                    0.10
                    if is_tier1_btc_eth
                    else self.tu_min_atr_percent
                )
                # 🔧 [2단계 구조] 2025-12-07: 극단만 HardCut, 나머지 Penalty
                # - 극단 케이스 (vol<0.10 OR atr<0.05) → HardCut
                # - 애매한 TU → Penalty 25%
                
                # 1단계: 극단 케이스 체크
                # 🔒 ATR 미계산(0.00%)은 극단 판단에서 제외 (전달/사용 오류 방어)
                is_extreme_case = (
                    vol_accel < 0.10 or
                    (atr_pct > 0 and atr_pct < 0.05)
                )
                
                if (
                    is_extreme_case
                    and allow_hardcut
                    and not _j234_hardcut_pass
                ):
                    # 극단 횡보/거래량 고갈 → J1 lane HardCut
                    self.last_rejection_reason = "TU_EXTREME_FLAT_VOL"
                    logger.info(
                        f"🚫 [체제필터-HardCut] {symbol} TRENDING_UP 극단 횡보/거래량고갈 진입 차단 | "
                        f"거래량가속:{fmt(vol_accel, '.2f')}x <0.10 ({'✅' if vol_accel >= 0.10 else '❌'}) | "
                        f"ATR:{pct(atr_pct, '.2f')} <0.05 ({'✅' if not (atr_pct > 0 and atr_pct < 0.05) else '❌'}) | "
                        f"→ 완전 횡보/거래량 고갈 (진입 금지)"
                    )
                    return None
                elif is_extreme_case:
                    _tu_hardcut_owner = (
                        "J234_PASS"
                        if _j234_hardcut_pass
                        else "J1_SOLEUS"
                    )
                    logger.info(
                        f"⚠️ [체제필터-HardCut-SKIP] {symbol} "
                        f"TRENDING_UP 극단 → HardCut 면제 | "
                        f"거래량가속:{fmt(vol_accel, '.2f')}x "
                        f"ATR:{pct(atr_pct, '.2f')} "
                        f"owner={_tu_hardcut_owner} "
                        f"soleus={int(soleus_gajami_ssot)} "
                        f"J2={int(indicators.get('judge2_osc_pass', 0) or 0)} "
                        f"J3={int(indicators.get('judge3_cycle_pass', 0) or 0)} "
                        f"J4={int(indicators.get('judge4_reversal_pass', 0) or 0)}"
                    )
                
                # 2단계: 기준 미달 체크
                elif (
                    vol_accel < vol_accel_threshold
                    or (is_tier1_btc_eth and atr_pct < 0.10)
                ):
                    tu_regime_penalty = 0.25  # 일반: 0.25 유지
                    penalty_label = "불확실"
                    
                    _tu_penalty_axis = (
                        "VOL_LOW" if vol_accel < vol_accel_threshold else "ATR_LOW"
                    )
                    penalty_reasons.append(
                        f"TU체제-{penalty_label}({_tu_penalty_axis},관측)"
                    )
                    penalty_details["regime_tu"] = tu_regime_penalty
                    
                    logger.info(
                        f"⚠️ [체제필터-Penalty] {symbol} TRENDING_UP 추세확인 부족 | "
                        f"거래량가속:{fmt(vol_accel, '.2f')}x <{vol_accel_threshold:.2f} ({threshold_label}) "
                        f"({'✅' if vol_accel >= vol_accel_threshold else '❌'}) | "
                        f"ATR:{pct(atr_pct, '.2f')} <{_tu_atr_eval_min:.2f} "
                        f"({'✅' if atr_pct >= _tu_atr_eval_min else '❌'}) | "
                        f"→ 진단 penalty={int(tu_regime_penalty*100)}% 기록 (scoring 제외)"
                    )
                    logger.debug(
                        f"   └─ 중간 penalty_factor={penalty_factor:.2f} (최종값 아님)"
                    )
                
                else:
                    # 모든 조건 통과 → 진입 허용
                    logger.info(
                        f"✅ [체제필터] {symbol} TRENDING_UP 진입 허용 | "
                        f"거래량가속:{fmt(vol_accel, '.2f')}x ≥{vol_accel_threshold:.2f} | "
                        f"ATR:{pct(atr_pct, '.2f')} ≥{_tu_atr_eval_min:.2f}"
                    )

            # 🆕 RANGING (횡보장) 거래량가속 필터 (2025-12-03 추가)
            # 철학: 횡보장에서 중립적 접근 (상승/하락 중간값)
            # 기준: TRENDING_DOWN과 TRENDING_UP의 중간 수준
            elif regime == MarketRegime.RANGING:
                # Volume Acceleration 계산 (개선: 추세 기반)
                # ✅ vol_accel은 Analysis에서 계산되어 indicators로 전달됨
                vol_accel = indicators.get('vol_accel', 1.0)
                
                # 횡보장 거래량 기준 (중립적, 발목 진입 허용)
                vol_accel_threshold = 0.35  # 🔥 0.80 → 0.35 (B안: 균형형 완화)
                threshold_label = "0.35x (횡보장 발목 진입)"
                
                # ATR 변동성 확인
                atr_raw = indicators.get('atr', 0)
                atr = safe_extract_atr(atr_raw)
                atr_pct = (atr / current_price) * 100 if current_price > 0 and atr > 0 else 0
                
                # 🔧 RANGING 상태 분리: 저ATR/ATR무효 HardCut, 거래량 부족 SoftPenalty.
                # 🔒 [ROLLBACK-0815] P-A2(ATR-SSOT-RG) 원복 — ★현행 유효.
                #   ★474-R2가 이 원복을 한 번 뒤집었으나 09-01 §208에서 폐기되어 복원됐다.
                #   (이전 주석의 [OVERRIDDEN by ★474-R2] 표기는 무효 — 되돌리지 말 것)
                #
                # 🔒 ★474 / ★474-R2 폐기 사유 — ATR 자체가 아니라 ★구제 소유권 변수의 오류다.
                #   allow_hardcut = not soleus_gajami_ssot = ★형상 context이며
                #     J1의 실제 판결 bit가 아니다.
                #   _j234_hardcut_pass 에는 ★J2가 포함되어 J2 단독까지 저ATR HardCut을 열었다.
                #   실측(배포 16:20 이후): 면제 SKIP 14건 중 owner=SOLEUS_CTX 6건의 2건이
                #     ★판사 0표, owner=J234_PASS 7건의 71%가 ★J2 단독.
                #     청산 5건 ★전부 손실 · 평균 -0.721% (대조군 -0.293%의 2.5배).
                #
                # ★재제안 금지 조건: allow_hardcut / _j234_hardcut_pass 를
                #   relief owner 로 ★재사용하지 않는다. judge1_gajami_pass 는 이미 t6에서
                #   읽히므로 "변수가 없어서"가 아니다.
                #   재검토 시 ★judge bit 별 소유권과 하류 생존을 먼저 독립 검증한다.
                #
                # ★이 패치가 해결하지 않는 것: 저ATR 종목의 진입 기회 자체.
                #   저ATR 진입 기회 문제와 손절·익절 RR 구조 문제는 별도 축으로 검증한다.
                RG_ATR_MIN_PCT = self.rg_min_atr_percent
                _vol_accel_extreme = vol_accel < 0.10
                _rg_low_or_invalid_atr = atr_pct < RG_ATR_MIN_PCT
                if _rg_low_or_invalid_atr:
                    self.last_rejection_reason = "ATR_LOW_BLOCK"
                    logger.info(
                        f"🚫 [ATR-LOW-BLOCK] {symbol} "
                        f"RANGING 저변동성/ATR무효 진입 차단 | "
                        f"ATR:{pct(atr_pct, '.2f')} <{RG_ATR_MIN_PCT:.2f}% | "
                        f"거래량가속:{fmt(vol_accel, '.2f')}x | "
                        f"kind={'INVALID' if not (current_price > 0.0 and atr > 0.0) else 'LOW'} "
                        f"soleus_ctx={int(soleus_gajami_ssot)} "
                        f"j1={int(indicators.get('judge1_gajami_pass', 0) or 0)} "
                        f"J2={int(indicators.get('judge2_osc_pass', 0) or 0)} "
                        f"J3={int(indicators.get('judge3_cycle_pass', 0) or 0)} "
                        f"J4={int(indicators.get('judge4_reversal_pass', 0) or 0)} "
                        f"j234={int(_j234_hardcut_pass)} | "
                        f"→ LOW/INVALID 모두 HardCut (★474 폐기)"
                    )
                    return None
                
                # 🚫 [RG-MA20-GAP-HIGH] RANGING 확정봉 MA20 이격 과대 → 차단
                # [2026-08-01 19:xx 격하 원복] 15:10 격하 후 실측:
                #   RG-MA20-GAP-OBSERVE 97건 발동 → 실제 진입 7건 → 승률 0% · 평균 -0.922%
                #   (POKT -1.37 / TAIKO -1.12 / ERA -0.90 / BABY -0.87 / T -0.80 / IRYS -0.79 / RE -0.60)
                #   T 17:24 오진입도 이 게이트가 막았어야 할 자리 → 차단 복구.
                # 중기선은 Analysis의 prev_mas 확정봉 SSoT만 소비한다.
                # 실측 07-28~31 n=92: gap20<0.5% 손절26.7%·실현+0.06% / 1.0~1.5% 75.0%·-0.61% / 1.5%+ 76.9%·-0.69%
                # TD(비단조 n17)·TU(역방향)는 미적용 — RANGING 분기 안에만 둔다.
                _rg_ma20_closed = safe_extract_scalar(
                    (ma_data.get("prev_mas") or {}).get("ma20", SENTINEL_FLOAT),
                    SENTINEL_FLOAT,
                )
                _rg_gap20 = (
                    (current_price / _rg_ma20_closed - 1.0) * 100.0
                    if (
                        current_price > 0
                        and not is_sentinel(_rg_ma20_closed)
                        and _rg_ma20_closed > 0
                    )
                    else SENTINEL_FLOAT
                )
                _rg_ma20_gap_high = bool(
                    self.rg_max_ma20_gap_pct > 0.0
                    and not is_sentinel(_rg_gap20)
                    and _rg_gap20 >= self.rg_max_ma20_gap_pct
                )
                if _rg_ma20_gap_high:
                    self.last_rejection_reason = "RG_MA20_GAP_HIGH"
                    logger.info(
                        f"🚫 [RG-MA20-GAP-HIGH] {symbol} "
                        f"RANGING MA20 확정봉 이격 과대 | "
                        f"gap20={pct(_rg_gap20, '.2f')} "
                        f">= {self.rg_max_ma20_gap_pct:.2f}% | "
                        f"price={current_price:,.2f} "
                        f"ma20_closed={_rg_ma20_closed:,.2f} "
                        f"→ 진입 금지"
                    )
                    return None
                    
                # 거래량 극단값은 J1 lane 권한:
                # J1 가자미근 또는 J2/J3/J4 독립 PASS면 전역 HardCut하지 않는다.
                if (
                    _vol_accel_extreme
                    and allow_hardcut
                    and not _j234_hardcut_pass
                ):
                    self.last_rejection_reason = "RG_VOL_EXHAUSTED"
                    logger.info(
                        f"🚫 [체제필터-HardCut] {symbol} "
                        f"RANGING 거래량고갈 진입 차단 | "
                        f"거래량가속:{fmt(vol_accel, '.2f')}x <0.10 (❌) | "
                        f"ATR:{pct(atr_pct, '.2f')} | "
                        f"→ 거래량 고갈 (진입 금지)"
                    )
                    return None
                elif _vol_accel_extreme:
                    _rg_hardcut_owner = (
                        "J234_PASS"
                        if _j234_hardcut_pass
                        else "J1_SOLEUS"
                    )
                    logger.info(
                        f"⚠️ [체제필터-HardCut-SKIP] {symbol} "
                        f"RANGING 거래량 극단 → HardCut 면제 | "
                        f"거래량가속:{fmt(vol_accel, '.2f')}x "
                        f"ATR:{pct(atr_pct, '.2f')} "
                        f"owner={_rg_hardcut_owner} "
                        f"soleus={int(soleus_gajami_ssot)} "
                        f"J2={int(indicators.get('judge2_osc_pass', 0) or 0)} "
                        f"J3={int(indicators.get('judge3_cycle_pass', 0) or 0)} "
                        f"J4={int(indicators.get('judge4_reversal_pass', 0) or 0)}"
                    )
                # 정상 저ATR은 위에서 재차단하지 않는다.
                # 이 분기는 거래량 부족만 기존 -30% SoftPenalty로 처리한다.
                elif vol_accel < vol_accel_threshold:
                    ranging_regime_penalty = 0.30
                    penalty_reasons.append(
                        f"RANGING체제-불확실("
                        f"vol={fmt(vol_accel, '.2f')}"
                        f"<{vol_accel_threshold:.2f},-30%)"
                    )
                    penalty_details["regime_ranging"] = ranging_regime_penalty
                    logger.info(
                        f"⚠️ [체제필터-Penalty] {symbol} "
                        f"RANGING 거래량 확인 부족 | "
                        f"거래량가속:{fmt(vol_accel, '.2f')}x "
                        f"<{vol_accel_threshold:.2f} ({threshold_label}) | "
                        f"ATR:{pct(atr_pct, '.2f')} | "
                        f"→ 신뢰도 -30% 감점 (하드 차단 아님)"
                    )
                    logger.debug(
                        f"   └─ 누적 penalty_factor: "
                        f"{penalty_factor:.2f}"
                    )
                else:
                    logger.info(
                        f"✅ [체제필터] {symbol} RANGING 진입 허용 | "
                        f"거래량가속:{fmt(vol_accel, '.2f')}x "
                        f"≥{vol_accel_threshold:.2f} | "
                        f"ATR:{pct(atr_pct, '.2f')} "
                        f"≥{RG_ATR_MIN_PCT:.2f}%"
                    )
            
            # 🔥 MA 역배열 필터 → 라인 2117 통합 완료 (불필요한 계산 제거됨)
            
            # ========================================
            # 3. 변동성 필터 (하드 필터 #3) ✅
            # ========================================
            # 철학: "극단적 변동성에서는 예측 불가"
            
            # indicators는 라인 2101에서 이미 초기화됨 (중복 제거)
            atr_raw = indicators.get('atr', 0)
            atr = safe_extract_atr(atr_raw)
            
            if current_price > 0 and atr > 0:
                # ==============================================================
                # 🆕 [BTC ATR 확장] 소수점 4자리 (0.0000%)
                # ==============================================================
                if symbol == "KRW-BTC":
                    atr_pct = round((atr / current_price) * 100, 4)
                else:
                    atr_pct = round((atr / current_price) * 100, 2)
                
                # 🆕 [BTC/ETH 전용] ATR 초저변동성 허용 (0.01% 이상)
                # - BTC/ETH는 변동성이 원래 낮음 (BTC: 0.03~0.08%, ETH: 0.12~0.20% 정상 범위)
                # - 알트코인은 기존 필터 유지 (시스템 안정성)
                # 메이저(Tier1) 종목 여부 - SSoT: CONFIG [VOLUME].exclude_tier1
                exclude_tier1 = self.config.get_cached("exclude_tier1", [])
                if symbol_str in exclude_tier1:
                    min_atr_threshold = 0.01    # BTC/ETH는 0.01%만 넘으면 허용
                else:
                    min_atr_threshold = self.min_atr_percent
                
                # ATR 극단값 차단
                if atr_pct < min_atr_threshold:
                    extra_penalty = PENALTY_WEIGHTS.get("atr_too_low", 0.10)
                    penalty_reasons.append(f"ATR_TOO_LOW-Soft({pct(atr_pct,'.4f')}<{pct(min_atr_threshold,'.1f')})")
                    penalty = add(penalty, extra_penalty)
                    penalty_details["atr_too_low"] = extra_penalty

                    logger.info(
                        f"⚠️ [ATR-TooLow-Soft] {symbol} 변동성 과소 → HardCut 제거, 페널티 적용 | "
                        f"penalty_factor={penalty_factor:.2f}"
                    )
                    
                elif atr_pct > 8.0:  # 변동성 과다 → 페널티
                    extra_penalty = PENALTY_WEIGHTS.get("atr_extreme", 0.20)
                    penalty_reasons.append(f"ATR_EXTREME(+{extra_penalty:.2f})")
                    penalty = add(penalty, extra_penalty)
                    penalty_details["atr_extreme"] = extra_penalty
                    logger.warning(
                        f"⚠️ [ATR-Extreme-Soft] {symbol} 극단 변동(ATR {pct(atr_pct, '.2f')} > 8%) → 페널티 적용"
                    )
                               
            # ========================================
            # 🆕 [추가] 전 체제 공통 거래량 필수 체크
            # ========================================
            volume_ratio = safe_extract_scalar(indicators.get('volume_ratio'))

            raw_volume_ratio = safe_extract_scalar(indicators.get('raw_volume_ratio', volume_ratio))
            buying_ratio = safe_extract_scalar(indicators.get('buying_ratio', 0.5))
            recent_buying_ratio = safe_extract_scalar(indicators.get('recent_buying_ratio', 0.5))
            previous_buying_ratio = safe_extract_scalar(indicators.get('previous_buying_ratio', 0.5))
            volume_pressure = indicators.get('volume_pressure', 'NEUTRAL')
            is_sudden_change = indicators.get('is_sudden_change', False)
            pressure_strength = safe_extract_scalar(indicators.get('pressure_strength', 0), 0.0) or 0.0

            # 🆕 완벽한 거래량 압력 로깅
            logger.debug(
                f"📊 [거래량분석] {symbol} | "
                f"원본: {fmt(raw_volume_ratio, '.2f')}x | "
                f"조정: {fmt(volume_ratio, '.2f')}x | "
                f"매수: {fmt(buying_ratio*100, '.1')}% | "
                f"최근5: {recent_buying_ratio:.1%} | "
                f"이전20: {previous_buying_ratio:.1%} | "
                f"압력: {volume_pressure} ({pressure_strength:.2f})"
            )
            
            # ========================================
            # 🆕 체제별 완화형 매도 압력 필터
            # ========================================
            # 철학: "약세장에서는 강력 차단, 횡보/상승장에서는 선택적 차단"
            # 개선: 체제별 차별화로 횡보/상승장 기회 회복
            
            if regime in [MarketRegime.TRENDING_DOWN, MarketRegime.BREAKDOWN]:
                # 약세장: 기존대로 강력 차단
                if is_sudden_change and 'SELLING' in volume_pressure:
                    
                    # 🐟 가자미근 매도압력 완전 면제 (SSoT 원칙)
                    #  - Analysis가 가자미근 확정하면 Strategy는 차단 금지
                    #  - HardCut 제거, SoftPenalty로만 처리
                    #  - 효과: KRW-MON 00:39 같은 최적 진입 포착
                    
                    # 가자미근 상태 확인
                    gajami_entry = indicators.get('gajami_entry', False)
                    
                    # 가자미근 구간 특례
                    if gajami_entry:
                        # ✅ 가자미근에서는 매도압력 HardCut 금지 (SSoT 준수)
                        if buying_ratio >= 0.12:
                            # 매수비율 12% 이상: 위험도 관측
                            penalty_reasons.append("가자미근매도압력완화(관측)")
                            penalty_details["gajami_sell_pressure"] = penalty_details.get("gajami_sell_pressure", 0.0) + 0.15
                            logger.info(
                                f"🐟 [{symbol}] 가자미근 매도압력 특례 | "
                                f"{volume_pressure}(매수:{fmt(buying_ratio*100, '.1')}%) | "
                                f"진입 허용 유지 · 최종 scoring 감산 없음"
                            )
                        else:
                            # 12% 미만도 차단하지 않고 위험도만 관측
                            penalty_reasons.append(f"가자미근강매도-관측({volume_pressure},{fmt(buying_ratio*100, '.1')}%)")
                            penalty_details["gajami_sell_pressure"] = penalty_details.get("gajami_sell_pressure", 0.0) + 0.30
                            logger.info(
                                f"🐟 [{symbol}] 가자미근 강매도(관측) | "
                                f"{volume_pressure}(매수:{fmt(buying_ratio*100, '.1')}%) | "
                                f"진입 유지 · 최종 scoring 감산 없음"
                            )
                    else:
                        # 🔧 비가자미근: 약세장 매도압력 차단 (단일화)
                        # - TIER2 가자미근은 Soft 처리 (저점 횡보 → 반등 초입)
                        tier_info = gajami_meta.get('tier', 1)
                        is_tier2_soleus = (tier_info == 2 and gajami_meta.get('soleus_zone', False))
                        
                        if is_tier2_soleus:
                            # TIER2 가자미근: Soft Penalty (차단 대신 감점)
                            penalty_factor = self._apply_soft_penalty(
                                penalty_factor, 0.75, penalty_reasons,
                                f"TIER2-매도압력Soft({fmt(buying_ratio*100, '.1')}%,-25%)"
                            )
                            logger.info(
                                f"⚠️ [TIER2-Soft] {symbol} 저점 횡보 매도압력 → Soft | "
                                f"매수:{fmt(buying_ratio*100, '.1')}% | penalty=0.75x"
                            )
                        else:
                            # 🔒 [HARDCUT-JUDGE-LANE-SPLIT-0828]
                            # 비가자미근 매도압력은 J1 lane의 HardCut이다.
                            # J2/J3/J4 독립 PASS가 있으면 그 판사의 생존권은 유지한다.
                            if allow_hardcut and not _j234_hardcut_pass:
                                self.last_rejection_reason = (
                                    f"매도압력 {volume_pressure} "
                                    f"({fmt(buying_ratio*100, '.1f')}%)"
                                )
                                logger.warning(
                                    f"🚫 [{symbol}] 진입 차단: 약세장 매도압력 | "
                                    f"압력:{volume_pressure} "
                                    f"매수:{fmt(buying_ratio*100, '.1')}%"
                                )
                                return None
                            _td_hardcut_owner = (
                                "J234_PASS"
                                if _j234_hardcut_pass
                                else "J1_SOLEUS"
                            )
                            logger.info(
                                f"🟢 [TD-SELL-PRESSURE-HARDCUT-SKIP] {symbol} "
                                f"약세장 매도압력 HardCut 면제 | "
                                f"owner={_td_hardcut_owner} "
                                f"soleus={int(soleus_gajami_ssot)} "
                                f"J2={int(indicators.get('judge2_osc_pass', 0) or 0)} "
                                f"J3={int(indicators.get('judge3_cycle_pass', 0) or 0)} "
                                f"J4={int(indicators.get('judge4_reversal_pass', 0) or 0)}"
                            )
                                   
            elif regime in [MarketRegime.RANGING, MarketRegime.TRENDING_UP]:
                # 횡보/상승장: 완화 적용 (신규)
                
                # 🆕 개선: STRONG_SELLING 중 거래량 회복 예외 (2025-11-03)
                # ENA 대응: 매도압력 지속 중에도 거래량 회복 시 진입 허용
                # 근거: buying_ratio 27~34%는 바닥권 정상 범위
                
                # 1. STRONG_SELLING: 거래량 회복 여부 체크
                if volume_pressure == 'STRONG_SELLING':
                    # ✅ vol_accel은 Analysis에서 계산되어 indicators로 전달됨
                    vol_accel = indicators.get('vol_accel', 1.0)
                    
                    # 🆕 골든크로스 확인
                    golden_count = ma_data_td.get('golden_count', 0)
                    
                    # 🆕 2중 이상 골든크로스: STRONG_SELLING 무시 (SUI 케이스)
                    if golden_count >= 2:
                        # 🔥 2중 골크 보너스 (v2 SSoT: bonus 경로 사용)
                        current_confidence = indicators.get('confidence', 0.0)
                        if current_confidence < 0.70:
                            double_gc_boost = 0.10  # 2중 골크 보정치
                            bonus = add(bonus, double_gc_boost)
                            bonus_reasons.append(f"DOUBLE_GC_BOOST(+{double_gc_boost:.2f})")
                            logger.debug(
                                f"🔧 [2중골크보너스] {symbol} golden={golden_count}개 → bonus +{double_gc_boost:.2f} (current_conf={fmt(current_confidence, '.3f')})"
                            )
                        
                        logger.info(
                            f"📊 [매도압력완화-골크] {symbol} 2중 골든 발생 → STRONG_SELLING 무시 | "
                            f"골크:{golden_count}개, buy={fmt(buying_ratio*100, '.1')}%, accel={fmt(vol_accel, '.2f')}x"
                        )
                        
                        # ✅ 골크 완화 발동 → 이후 급매도전환 차단 우회 플래그
                        # indicators["bypass_sudden_sell_block"] = True  # 🔧 [매도클라이맥스 우회 철거]
                    
                    # 🆕 1중 골든크로스 + 매수비중 28% 이상: 진입 허용
                    elif golden_count >= 1 and buying_ratio >= 0.28:
                        logger.info(
                            f"📊 [매도압력완화-골크] {symbol} 골든 + buy≥28% → 진입 허용 | "
                            f"골크:{golden_count}개, buy={fmt(buying_ratio*100, '.1')}%"
                        )
                        
                        # ✅ 골크 완화 발동 → 이후 급매도전환 차단 우회 플래그
                        # indicators["bypass_sudden_sell_block"] = True  # 🔧 [매도클라이맥스 우회 철거]
                    
                    # 기존 TD/RANGING 예외 규칙 유지
                    elif regime in [MarketRegime.RANGING, MarketRegime.TRENDING_DOWN] and vol_accel >= 1.0:
                        logger.info(
                            f"📊 [매도압력완화] {symbol} 하락장+거래량회복 → 진입 허용 | "
                            f"압력: STRONG_SELLING, 매수비율: {fmt(buying_ratio*100, '.1')}%, 가속도: {fmt(vol_accel, '.2f')}x"
                        )
                    
                    else:
                        # 🛡️ SoleusZone 우선권 체크
                        if indicators.get("ignore_selling_pressure", False):
                            # SoleusZone 내에서는 차단 대신 경고/감점
                            logger.info(
                                f"🛡️ [SoleusZone-Bypass] {symbol} 매도압력 우회 | "
                                f"매수:{fmt(buying_ratio, '.1f')}% 가속:{fmt(vol_accel, '.2f')}x | "
                                f"가자미근 확정 구간 → 진입 허용"
                            )
                            # 위험도 관측값만 기록 — 후단 최종 PF에는 반영되지 않는다.
                            penalty_reasons.append("SoleusZone매도압력완화(관측)")
                            penalty_details["soleus_selling"] = penalty_details.get("soleus_selling", 0.0) + 0.10
                            
                            # ✅ SoleusZone 우회 발동 → 이후 급매도전환 차단 우회 플래그
                            # indicators["bypass_sudden_sell_block"] = True  # 🔧 [매도클라이맥스 우회 철거]
                        else:
                            if soleus_gajami:
                                penalty_reasons.append(f"가자미근-매도압력관측({fmt(buying_ratio*100, '.1')}%,{fmt(vol_accel, '.2f')}x)")
                                penalty_details["gajami_selling_up"] = penalty_details.get("gajami_selling_up", 0.0) + 0.20
                                
                                logger.info(
                                    f"🐟 [{symbol}] 가자미근 매도압력(관측) | "
                                    f"압력:{volume_pressure} 매수:{fmt(buying_ratio*100, '.1')}% 가속:{fmt(vol_accel, '.2f')}x | "
                                    f"진입 유지 · 최종 scoring 감산 없음"
                                )

                    # STRONG_SELLING 완화/허용 판단만 수행
                    # SuddenSell-Soft는 공통 급전환 블록에서 단일 처리
                    pass
                
                # 2. SELLING은 점수 감산 (즉시 차단 아님)
                elif volume_pressure == 'SELLING':
                    selling_penalty = 0.15 * pressure_strength  # 강도 비례 감산
                    penalty = add(penalty, selling_penalty)
                    penalty_reasons.append(f"SELLING_PRESSURE({selling_penalty:.3f})")
                    logger.debug(
                        f"⚙️ [{symbol}] 매도압력 감산 | "
                        f"SELLING 감지 → penalty +{selling_penalty:.3f} "
                        f"(강도: {pressure_strength:.2f})"
                    )
                
            # 🆕 급격한 매도 전환 차단
            # 주의: 이 로직은 모든 체제에서 독립적으로 실행됨

            # 🔧 [매도클라이맥스 우회 철거] bypass_sudden_sell_block/ignore_selling_pressure → 급매도전환 HardCut 항상 실행
            # (기존 급매도전환 차단 로직 그대로 실행)
            # 강한 급매도 전환 기준
            is_strong_sudden_sell = (
                is_sudden_change and
                volume_pressure in ['SELLING', 'STRONG_SELLING'] and
                pressure_strength >= 0.6
            )

            if regime in [MarketRegime.TRENDING_DOWN, MarketRegime.BREAKDOWN]:
                # 약세장: 기존대로 강력 차단
                if is_sudden_change and 'SELLING' in volume_pressure:
                    penalty_factor = self._apply_soft_penalty(
                        penalty_factor, 0.60, penalty_reasons,
                        f"SuddenSell-Soft({fmt(recent_buying_ratio*100, '.1f')}%)"
                    )
                    indicators["td_sudden_sell_soft"] = True

                    self.last_rejection_reason = f"급격한 매도전환 ({fmt(recent_buying_ratio*100, '.1f')}%)"
                    logger.warning(
                        f"⚠️ [{symbol}] 약세장 급격한 매도전환 → 차단 대신 페널티 | "
                        f"이전:{previous_buying_ratio:.1%}→현재:{recent_buying_ratio:.1%} | "
                        f"penalty_factor={penalty_factor:.2f}"
                    )

            elif regime in [MarketRegime.RANGING, MarketRegime.TRENDING_UP]:
                # 횡보/상승장: 강한 급매도만 차단
                if is_strong_sudden_sell:
                    self.last_rejection_reason = f"급격한 매도전환 ({fmt(recent_buying_ratio*100, '.1f')}%)"

                    if soleus_gajami:
                        # 🔒 불합리 veto 제거: 가자미근 초입의 일시적 매도 쏟아짐은 자연스러울 수 있음
                        # - 차단 대신 소프트 페널티로 리스크만 반영
                        sudden_sell_penalty = 0.10 * pressure_strength
                        penalty = add(penalty, sudden_sell_penalty)
                        penalty_reasons.append(f"SUDDEN_SELL_GAJAMI({sudden_sell_penalty:.3f})")
                        indicators["gajami_sudden_sell_soft"] = True

                        logger.warning(
                            f"⚠️ [{symbol}] 가자미근 확정 + 강한 급매도 전환 → 차단 대신 소프트 | "
                            f"이전:{previous_buying_ratio:.1%}→현재:{recent_buying_ratio:.1%} 강도:{pressure_strength:.2f}"
                        )
                    else:
                        logger.warning(
                            f"🚫 [{symbol}] 진입 차단: 강한 급매도 전환 (상승/횡보) | "
                            f"이전:{previous_buying_ratio:.1%}→현재:{recent_buying_ratio:.1%} 강도:{pressure_strength:.2f}"
                        )
                        return None

                # 약한 전환: 매도/매수 구분하여 처리
                elif is_sudden_change and pressure_strength >= 0.5:
                    if volume_pressure in ('SELLING', 'STRONG_SELLING'):
                        # 🔴 진짜 급매도 전환일 때만 감산
                        sudden_sell_penalty = 0.10 * pressure_strength
                        penalty = add(penalty, sudden_sell_penalty)
                        penalty_reasons.append(f"SUDDEN_SELL({sudden_sell_penalty:.3f})")
                        logger.debug(
                            f"⚙️ [{symbol}] 급매도 완화 | "
                            f"단기 매도 전환 감지 (강도: {pressure_strength:.2f}) → "
                            f"penalty +{sudden_sell_penalty:.3f}"
                        )
                    elif volume_pressure in ('BUYING', 'STRONG_BUYING'):
                        # 🆕 급매수 전환이면 보너스
                        sudden_buy_bonus = 0.05 * pressure_strength
                        bonus = add(bonus, sudden_buy_bonus)
                        bonus_reasons.append(f"SUDDEN_BUY(+{sudden_buy_bonus:.3f})")
                        logger.debug(
                            f"⚡ [{symbol}] 급매수 강화 | "
                            f"단기 매수 전환 감지 (강도: {pressure_strength:.2f}) → "
                            f"bonus +{sudden_buy_bonus:.3f}"
                        )
                    
            # 🚨 급격한 전환 경고
            if is_sudden_change:
                if 'SELLING' in volume_pressure:
                    logger.warning(
                        f"⚠️ [거래량경고] {symbol} 급격한 매도 전환 감지! | "
                        f"이전: {previous_buying_ratio:.1%} → 현재: {recent_buying_ratio:.1%} | "
                        f"거래량 {raw_volume_ratio:.2f}x → 조정 {fmt(volume_ratio, '.2f')}x"
                    )
                    
                    # ⭐ FIX-3: 매도압력 급락 차단
                    buy_ratio_drop = previous_buying_ratio - recent_buying_ratio
                    
                    if buy_ratio_drop >= 0.20:
                        self.last_rejection_reason = (
                            f"매도압력 급락 "
                            f"(BuyRatio: {buy_ratio_drop:.1%}↓)"
                        )

                        if soleus_gajami:
                            # 🔒 불합리 veto 제거: 가자미근 확정이면 '차단'이 아니라 리스크로만 반영
                            
                            drop_penalty = 0.08
                            penalty = add(penalty, drop_penalty)
                            penalty_reasons.append(f"SELL_PRESSURE_DROP_GAJAMI({drop_penalty:.2f})")
                            indicators["gajami_sell_pressure_drop_soft"] = True

                            # 🔒 [BOX-TOP-LOG-PARITY-0904] ★507
                            # 조건은 buy_ratio_drop>=0.20 하나뿐이며 box_pos/height 판정 없음.
                            # 박스 producer 자체가 철거됐으므로(L10640-10644) 문구만 사실과 맞춘다.
                            logger.info(
                                f"⚠️ [{symbol}] 가자미근 확정 + 매도압력 급락 → 차단 대신 소프트 | "
                                f"매수비율: {previous_buying_ratio:.1%}→{recent_buying_ratio:.1%} "
                                f"({buy_ratio_drop:.1%} 하락)"
                            )
                        else:
                            logger.info(
                                f"🚫 [{symbol}] 진입 차단: 매도압력 급락 | "
                                f"매수비율: {previous_buying_ratio:.1%}→{recent_buying_ratio:.1%} "
                                f"({buy_ratio_drop:.1%} 하락)"
                            )
                            return None
                
                elif 'BUYING' in volume_pressure:
                    logger.info(
                        f"✅ [거래량정보] {symbol} 급격한 매수 전환 감지! | "
                        f"이전: {previous_buying_ratio:.1%} → 현재: {recent_buying_ratio:.1%} | "
                        f"거래량 {raw_volume_ratio:.2f}x → 조정 {fmt(volume_ratio, '.2f')}x (강화!)"
                    )
            
            # 🚨 폭풍 매도 — 관측만. 이 단독 조건으로 Analysis GajamiDecision을 재차단하지 않는다.
            # 🔒 [2026-08-14 STORM-BLOCK-ROLLBACK] 정답 BIRTH 차단 및 후속 판정 소실 방지.
            elif volume_pressure in ['STRONG_SELLING', 'SELLING'] and raw_volume_ratio > 1.5:
                logger.warning(
                    f"⚠️ [거래량경고] {symbol} 폭풍 매도 진행 중! | "
                    f"거래량 {fmt(raw_volume_ratio, '.2f')}x 증가 + 매도 압력 {fmt(buying_ratio*100, '.1f')}%"
                )

            # 타임프레임별 최소 거래량 비율
            timeframe = self.config.get('TRADING', 'timeframe', fallback='5m')
            
            # # ✅ 보수적 모드
            # MIN_VOLUME_RATIOS = {
            #     '1m': 0.3, '3m': 0.4, '5m': 0.5, '10m': 0.6,
            #     '15m': 0.7, '30m': 0.7, '1h': 0.8, '4h': 0.8, '1d': 0.8
            # }
            # ✅ 적극적 모드
            MIN_VOLUME_RATIOS = {
                '1m': 0.25, '3m': 0.30, '5m': 0.35, '10m': 0.40,
                '15m': 0.45, '30m': 0.50, '1h': 0.55, '4h': 0.60, '1d': 0.65
            }
            min_ratio = MIN_VOLUME_RATIOS.get(timeframe, 0.5)

            symbol_str = str(symbol) if symbol else ""
            # 메이저(Tier1) 종목 여부 - SSoT: CONFIG [VOLUME].exclude_tier1
            exclude_tier1 = self.config.get_cached("exclude_tier1", [])
            is_tier1_btc_eth = symbol_str in exclude_tier1

            # 🔑 2025-12-03: VolumeSkip 완전 삭제 - 거래량 필터 항상 적용
            # 🔧 2025-12-07: 하드 차단 → 페널티로 전환 (중앙 게이트 일원화)
            # 이유: EntryFilter 통과만으로 타임프레임 거래량 기준 무시하던 문제 수정
            # EGLD 사례: "급격한 매수 전환" 감지 → VolumeSkip 발동 → 손절
            
            # 🔥 BTC/ETH 메이저는 여기서도 ratio로 차단하지 않음
            if not is_tier1_btc_eth:
                # 🚨 volume_ratio가 0이면 계산 실패 → 경고 후 다음 필터로
                if volume_ratio == 0:
                    logger.warning(
                        f"⚠️ [거래량계산] {symbol} volume_ratio 계산 실패 (0.00x) - "
                        f"데이터 부족으로 필터 우회, 다음 필터로 진행"
                    )
                    # 그대로 다음 단계 진행 (차단/페널티 없음)
                
                # 정상 거래량이지만 기준 미달 → 강한 페널티 (하드컷 아님)
                # 🔥 PATCH AXS-2B-TF: TF 거래량 페널티 정합화
                # 철학: EntryFilter에서 "조용한 급등 초입"을 정상으로 인정했으면,
                #       후단(TF 거래량)에서도 같은 철학을 유지한다
                # 효과: 동일 축(거래량)에 대한 이중·삼중 감점 제거
                elif volume_ratio < min_ratio:
                    # TF 거래량 페널티 계산을 위한 컨텍스트 추출
                    golden_count = int(safe_extract_scalar(indicators.get("golden_count", 0), 0))
                    ma_signal = float(safe_extract_scalar(indicators.get("ma_signal", 0.0), 0.0))
                    entry_type = str(indicators.get("entry_type", "NONE") or "NONE")
                    
                    is_strong_gc = (golden_count >= 6)
                    is_ultra_gc = (golden_count >= 9 and ma_signal >= 0.70)
                    is_trend_setup = (entry_type in ("TREND", "PULLBACK"))
                    
                    # EntryFilter에서 이미 인정한 '조용한 급등 초입' 컨텍스트
                    is_prebreak_quiet_ok = (
                        is_strong_gc
                        and is_trend_setup
                        and regime in (MarketRegime.RANGING, MarketRegime.TRENDING_UP)
                    )
                    
                    deficit_tf = max(0.0, min_ratio - volume_ratio)
                    # 타임프레임 기준 부족분에 비례한 페널티 (최대 25%)
                    # 예: min=0.30, actual=0.20 → deficit=0.10 → penalty=0.15 (15%)
                    # 예: min=0.30, actual=0.10 → deficit=0.20 → penalty=0.25 (25% 상한)
                    tf_volume_penalty = min(0.25, deficit_tf * 1.5)
                    
                    # ✅ 정합 패치: 강한 GC + 트렌드 준비 구간에서는 TF 거래량 감점 상한 제한
                    if is_prebreak_quiet_ok:
                        tf_volume_penalty = min(tf_volume_penalty, 0.15)
                        logger.debug(
                            f"✅ [TF-Vol-Adjust] {symbol} 조용한 급등 초입 보호 | "
                            f"GC={golden_count} entry_type={entry_type} → TF감점 상한 15%"
                        )

                    penalty_reasons.append(
                        f"TF거래량부족({fmt(volume_ratio, '.2f')}x<{fmt(min_ratio, '.2f')}x,-{fmt(tf_volume_penalty*100 if tf_volume_penalty is not None else None, '.0f')}%)"
                    )
                    penalty_details["timeframe_volume"] = tf_volume_penalty

                    logger.info(
                        f"⚠️ [신호분석] {symbol} {timeframe} 거래량 부족 | "
                        f"{fmt(volume_ratio, '.2f')}x < {fmt(min_ratio, '.2f')}x → "
                        f"진단 penalty={fmt(tf_volume_penalty*100 if tf_volume_penalty is not None else None, '.0f')}% 기록 (scoring 제외)"
                    )
                    logger.debug(
                        f"   └─ 누적 penalty_factor: {fmt(penalty_factor, '.2f')}"
                    )
                    
                    # last_rejection_reason는 여기서 고정하지 않음
                    # → 최종적으로 final_score_capped < effective_threshold에서 설정
            # BTC/ETH는 ratio 기준 미적용 (이미 위에서 절대 거래대금으로 필터링됨)
            
            # ========================================
            # 3. ATR 값 추출 및 변동성 계산
            # ========================================
            atr_raw = indicators.get('atr', 0)
            atr_value = safe_extract_atr(atr_raw)
            
            # ==============================================================
            # 🆕 [BTC ATR 확장] 소수점 4자리 (0.0000%)
            # ==============================================================
            if symbol == "KRW-BTC":
                atr_pct = round((atr_value / current_price * 100), 4) if current_price > 0 else 0
            else:
                atr_pct = round((atr_value / current_price * 100), 2) if current_price > 0 else 0
            
            # 변동성 레벨 판단
            if atr_pct < 0.5:
                volatility_level = "VERY_LOW"
            elif atr_pct < 1.0:
                volatility_level = "LOW"
            elif atr_pct < 2.0:
                volatility_level = "NORMAL"
            elif atr_pct < 3.5:
                volatility_level = "HIGH"
            else:
                volatility_level = "EXTREME"
            
            # ========================================
            # 4. ATR 필터 체크
            # ========================================
            if self.atr_filter_enabled and volatility_level in self.atr_filter_levels:
                # ✅ 정석: ATR 필터는 "즉결 차단"이 아니라 "강 페널티"
                extra_penalty = PENALTY_WEIGHTS.get("atr_extreme", 0.20)
                penalty_reasons.append(f"ATR_FILTER-Soft({volatility_level})")
                penalty = add(penalty, extra_penalty)
                penalty_details["atr_filter"] = extra_penalty

                logger.warning(
                    f"⚠️ [ATR-Filter-Soft] {symbol} ATR 레벨({volatility_level}) → HardCut 제거, 페널티 적용 | "
                    f"penalty_factor={penalty_factor:.2f}"
                )
            
            # ========================================
            # 5. MA 분석 (체제별 이격도 포함!)
            # ========================================
            # ✅ [제거] ma_analysis는 이미 위에서 계산됨 (850줄 근처)
                        
            # ========================================
            # 🆕 [추가] MA5 추세 일관성 체크 (3봉 연속 확인)
            # ========================================
            # ✅ ma5_series는 Analysis에서 계산되어 indicators로 전달됨
            ma5_series = indicators.get('ma5_series', pd.Series())
            if len(ma5_series) >= 4:
                ma5_recent = ma5_series.tail(4).values
                
                # 최근 3봉 연속 상승 확인
                is_consistent_uptrend = all(
                    ma5_recent[i] < ma5_recent[i+1] 
                    for i in range(len(ma5_recent)-1)
                )
                
                # ✅ 보수적: (3봉 연속 상승 필수)
                #     self.last_rejection_reason = "MA5 추세 불안정"
                #     logger.info(f"🚫 [신호분석] {symbol} MA5 추세 불안정...")

                # ✅ 적극적: 2봉만 상승하면 OK
                is_consistent_uptrend = (ma5_recent[-2] < ma5_recent[-1])

                if not is_consistent_uptrend:
                    # 하락 중이면 차단
                    pass

            # 이격도 상태 체크
            ma_deviation_state = ma_analysis.get('ma_deviation', {}).get('state', 'neutral')
            ma_deviation_value = ma_analysis.get('ma_deviation', {}).get('value', 0)
            
            # 🆕 횡보 탈출 감지용: 이전 MA 간격 추적
            prev_ma_deviation = indicators.get('ma_deviation_prev', ma_deviation_value)
            indicators['ma_deviation_prev'] = ma_deviation_value  # 다음 번을 위해 저장


            # ========================================
            # 🆕 모든 체제 공통 이격도 체크 (기본!)
            # ========================================
            # 1. 과매수는 모든 체제에서 페널티
            if ma_deviation_state == 'overbought':
                penalty_reasons.append(f"과매수({pct(ma_deviation_value, '.1f')})")
                
                # 🔥 감점 기반 추가
                score = PENALTY_WEIGHTS['overbought']
                penalty = add(penalty, score)
                penalty_details['overbought'] = score
                
                logger.info(
                    f"⚠️ [신호분석] {symbol} 이격도 과매수 | "
                    f"({pct(ma_deviation_value, '.1f')}) → 신뢰도 0.85x 페널티, 감점 +{fmt(score, '.2f')}"
                )
            
            # 🆕 MA 기울기 필터 (정배열 유지 중 하락 전환 감지)
            ma_trend = ma_analysis.get('trend', 0)
            
            # 정배열이지만 MA20 기울기가 하락 중
            if ma_analysis.get('alignment_level', 0) >= 3 and ma_trend < -0.001:
                penalty_reasons.append(
                    f"MA20하락({fmt(ma_trend*100 if ma_trend is not None else None, '.2f')}%)"
                )
                
                # 🔥 감점 기반 추가
                score = PENALTY_WEIGHTS['ma20_falling']
                penalty = add(penalty, score)
                penalty_details['ma20_falling'] = score
                
                logger.info(
                    f"⚠️ [모멘텀차단] {symbol} 정배열 유지 중 MA20 하락 | "
                    f"(기울기: {fmt(ma_trend*100 if ma_trend is not None else None, '.2f')}%) → "
                    f"신뢰도 0.90x 페널티, 감점 +{fmt(score, '.2f')}"
                )
            
            # 🆕 거래량 가속도 필터 (체제별 완화) (2025-11-03)
            # 대응: 완만 회복 중 구간 허용
            # 근거: 0.5x~0.7x는 완만 둔화, 피크 후 급락 아님
            # ✅ vol_accel은 Analysis에서 계산되어 indicators로 전달됨
            # 🆕 거래량 가속도 필터 (SSoT: indicators['vol_accel'])
            vol_accel = indicators.get('vol_accel', 1.0)
            
            # 체제별 둔화 기준
            if regime in [MarketRegime.RANGING, MarketRegime.TRENDING_DOWN]:
                # 횡보/하락장: 완화 (0.8 → 0.6)
                threshold = 0.6
                regime_label = "완화모드"
            else:
                # 상승/돌파장: 기존 (0.8)
                threshold = 0.8
                regime_label = "기본모드"
            
            # 거래량 가속도 < threshold = 둔화
            if vol_accel < threshold:
                # TRENDING_UP 체제 여부. 이것만으로 '초입'을 의미하지 않는다(dur/BIRTH 미확인).
                is_trending_up_regime = (
                    regime == MarketRegime.TRENDING_UP
                )
                
                if regime == MarketRegime.BULL_MARKET:
                    # 🆕 BULL_MARKET: 거래량 둔화 페널티 완전 면제
                    logger.info(
                        f"🔥 [거래량둔화-Bull] {symbol} 거래량 둔화 페널티 면제 (불장) | "
                        f"accel={fmt(vol_accel, '.2f')}x → 페널티 없음"
                    )
                elif is_trending_up_regime:
                    # 🔒 [TU-VOL-DECEL-SOFT-0825]
                    # TRENDING_UP의 vol_accel<threshold 둔화를 기존 0.02 Soft로 단일화.
                    # 10일 실측에서 기존 Hard 대상(0.65~0.80)이
                    # 기존 Soft 대상(<0.65)보다 오히려 양호하여 중간구간 Hard 권한을 제거한다.
                    penalty_reasons.append(
                        f"거래량둔화약함({fmt(vol_accel, '.2f')}x)"
                    )
                    score = 0.02
                    penalty = add(penalty, score)
                    penalty_details['vol_decel'] = score
                    logger.info(
                        f"🟡 [거래량둔화] {symbol} TRENDING_UP 거래량 둔화 → Soft | "
                        f"accel={fmt(vol_accel, '.2f')}x "
                        f"< {fmt(threshold, '.2f')}x, "
                        f"score={fmt(score, '.2f')}"
                    )
                else:
                    # 🆕 BULL_MARKET: 패턴 페널티 완전 면제
                    if regime == MarketRegime.BULL_MARKET:
                        # 불장에서는 패턴 없어도 진입 허용
                        logger.info(
                            f"🔥 [Entry-Pattern-Bull] {symbol} 반등 패턴 없음 (불장 면제) | "
                            f"({c0:.1f}→{c1:.1f}→{c2:.1f}) | "
                            f"V자:{is_v_pattern}, U자:{is_u_pattern} → 페널티 없음 (균형형 불장)"
                        )
                    elif bull_strength > 0:
                        # 불장 강도 기반 완화 (기존 로직 유지)
                        pattern_penalty_relaxed = (
                            BULL_RELAX_PATTERN_BASE + 
                            (BULL_RELAX_PATTERN_MAX * min(bull_strength, 1.0))
                        )
                        score = PENALTY_WEIGHTS['no_pattern'] * (1 - bull_strength * 0.5)
                        penalty_reasons.append(f"반등패턴없음(불장완화{pattern_penalty_relaxed:.2f})")
                        logger.info(
                            f"🔥 [Entry-Pattern-Bull] {symbol} 반등 패턴 없음 (불장 완화) | "
                            f"({c0:.1f}→{c1:.1f}→{c2:.1f}) | "
                            f"V자:{is_v_pattern}, U자:{is_u_pattern} → 관측 score={fmt(score, '.2f')} (scoring 제외) | "
                            f"강도={bull_strength:.2f}"
                        )
                    else:
                        # 🔥 TRENDING_DOWN 분기 추가
                        if regime == MarketRegime.TRENDING_DOWN:
                            if vol_accel >= 1.50 and recent_buying_ratio >= 0.80:
                                # 거래량 기준 충족: 패턴 면제
                                logger.info(
                                    f"✅ [Entry-Pattern-Down] {symbol} 거래량 기준 충족 | "
                                    f"거래량:{fmt(vol_accel, '.2f')}x, 매수압:{recent_buying_ratio:.0%}"
                                )
                            else:
                                score = PENALTY_WEIGHTS['no_pattern'] * 0.43
                                penalty_reasons.append("반등패턴없음(DOWN완화)")
                                
                                logger.info(
                                    f"⚠️ [Entry-Pattern-Down] {symbol} 반등 패턴 + 거래량 부족 | "
                                    f"({c0:.1f}→{c1:.1f}→{c2:.1f}) | "
                                    f"거래량:{fmt(vol_accel, '.2f')}x, 매수압:{recent_buying_ratio:.0%} → "
                                    f"관측 score={fmt(score, '.2f')} (scoring 제외)"
                                )
                        else:
                            score = PENALTY_WEIGHTS['no_pattern']
                            penalty_reasons.append("반등패턴없음")
                            logger.info(
                                f"⚠️ [Entry-Pattern] {symbol} 반등 패턴 없음 | "
                                f"({c0:.1f}→{c1:.1f}→{c2:.1f}) | "
                                f"V자:{is_v_pattern}, U자:{is_u_pattern} → 관측 score={fmt(score, '.2f')} (scoring 제외)"
                            )
            elif vol_accel < 0.8 and regime in [MarketRegime.RANGING, MarketRegime.TRENDING_DOWN]:
                # 완화 구간 통과 로깅
                logger.debug(
                    f"📊 [완화] {symbol} 완만 둔화 허용 ({fmt(vol_accel, '.2f')}x) | "
                    f"체제: {regime.value}"
                )
            
            # ========================================
            # 🆕 횡보 탈출 감지 함수
            # ========================================
            def is_sideway_breakout(
                ma_spacing,
                prev_ma_spacing,
                ma5_slope,
                ma20_slope,
                atr_pct,
            ):
                """
                횡보 정배열 탈출 감지 (HUNT 케이스 대응)                
                철학: 횡보정배열 완화가 아니라, 탈출 "첫 순간"을 진입으로 정의                
                Args:
                    ma_spacing: 현재 MA 간격 (%)
                    prev_ma_spacing: 이전 MA 간격 (%)
                    ma5_slope: MA5 기울기
                    ma20_slope: MA20 기울기
                    atr_pct: ATR 변동성 (%)                
                Returns:
                    bool: 횡보 탈출 여부
                """
                # 1. 직전까지 횡보 (±0.05% 이내)
                was_sideway = abs(prev_ma_spacing) <= 0.05
                
                # 2. 현재는 탈출 (±0.05% 초과)
                now_breaking = abs(ma_spacing) > 0.05
                
                # 3. 상승 방향성 (MA5 상승)
                slope_ok = gt(ma5_slope, 0)
                
                # 4. MA20 하락 아님 (하락 추세 차단)
                ma20_ok = ge(ma20_slope, 0)
                
                # 5. 최소 변동성 (노이즈 필터)
                atr_ok = ge(atr_pct, 0.20)
                
                return was_sideway and now_breaking and slope_ok and ma20_ok and atr_ok        

            # ========================================
            # 🆕 골든크로스 예외 처리
            # ========================================
            # 골든크로스 발생 시 횡보 정배열 필터 우회
            cross_signals = ma_analysis.get('cross_signals', [])
            legacy_golden_cross = any('GC_' in signal for signal in cross_signals)
            
            # ==========================================================
            # 🆕 Evidence-based Golden Entry (선행적 MA 반전 감지)
            #    3가지 OR 조건 + 기울기 AND (노이즈 방지)
            # ==========================================================
            ma5 = safe_extract_scalar(indicators.get('sma_5', 0))
            ma10 = safe_extract_scalar(indicators.get('sma_10', 0))
            ma20 = safe_extract_scalar(indicators.get('sma_20', 0))
            ma5_slope = safe_extract_scalar(indicators.get('ma5_slope', 0))
            ma10_slope = safe_extract_scalar(indicators.get('ma10_slope', 0))
            
            # 최종 골든크로스 = Analysis 모듈의 판단 사용
            has_golden_cross = legacy_golden_cross

            # ❌ 제거됨: VolumeExplosion→Knee
            # 사유:
            #  - SoleusZone 철학 위반
            #  - 거래량만으로 가자미근 판단 불가
            #  - NXPC 09:02 잘못된 진입의 직접 원인
            #     logger.debug(
            #         f"🚀 [VolumeExplosion→Knee] {symbol} 거래량 폭발로 가자미근 모드 인정"
            #     )

            # 🆕 Candidate Persistence (2025-12-25)
            # - 목적: Analysis(SSoT) 통과 후에도 이벤트가 없어서 "영원히 미진입" 되는 문제 해결
            # - 기준: soleus_zone 또는 gajami_meta.is_candidate가 연속이면 hold_count 누적
            # - 사용처: (1) 횡보 정배열 필터 우회, (2) TU-NoMA 우회
            if not hasattr(self, "_candidate_hold"):
                self._candidate_hold = {}
            
            gajami_meta = (data_package.ma_data or {}).get("gajami_meta") or {}
            is_candidate_now = soleus_gajami  # SSoT: Analysis 단일
            
            prev = self._candidate_hold.get(symbol, {"count": 0, "regime": None})
            prev_regime = prev.get("regime", None)
            
            # 체제 바뀌면 누적 리셋 (상태 오염 방지)
            if prev_regime is not None and prev_regime != regime:
                prev = {"count": 0, "regime": regime}
            
            if is_candidate_now:
                prev["count"] = int(prev.get("count", 0)) + 1
            else:
                prev["count"] = 0
            
            prev["regime"] = regime
            self._candidate_hold[symbol] = prev
            
            hold_count = prev["count"]
            candidate_persistent = (hold_count >= 3)  # 3봉 연속(=9분) 후보 유지
            
            # 디버그/후속 분기에서 사용
            indicators["candidate_hold_count"] = hold_count
            indicators["candidate_persistent"] = candidate_persistent
            
            # 🔥 횡보 정배열 필터 - 예외 조건 추가 (2025-11-30)
            # 철학: "정배열 + 변동성 충분" = 상승 초입, 박스장 아님
            # 2. 횡보 정배열 체크 (이격도 너무 작으면 의미 없음)
            # 기준: -0.05~+0.05% (극도의 횡보)
            # ✅ 골든크로스 발생 시 예외
            # ✅ Knee Mode 예외
            # ✅ ATR/ADX/Vol 충분 시 예외 (2025-11-30 신규)
            # ✅ 횡보 탈출 첫 순간 예외 (2025-12-12 HUNT 대응)
            # ✅ PEARL-GC: 골든크로스 초입 가자미근 예외 (2025-12-13 SXP 대응)
            
            # 🆕 횡보 정배열 조건 확장 (골든크로스 여부 무관)
            # 🔒 PATCH ②-A: 횡보 정배열 완화 (반등 초입 허용)
            # - 기존: ±0.05% (너무 좁음 → 반등 초입 차단)
            # - 개선: 가자미근 ±0.30%, 일반 ±0.20%
            # - 철학: 반등 초입일수록 이격도가 작은 게 정상 (ETH 18:22 케이스)
            
            gajami_zone = _soleus_ssot(indicators, data_package.ma_data)
            sideways_threshold = 0.30 if gajami_zone else 0.20
            
            if abs(ma_deviation_value) <= sideways_threshold:
                skip_flat_filter = False
                skip_reason = ""

                # 🆕 TF팀 합의: 극도의 횡보 예외는 "수급 변화"로만 허용
                # 철학: "횡보를 깨는 건 지표가 아니라 수급이다"
                # 조건: 신뢰도 65% + 거래량 2.0x (둘 다 충족 필수)
                volume_ratio_cur = safe_extract_scalar(indicators.get("volume_ratio", 0))
                
                # ✅ 일관성: on-demand로 확정된 신뢰도(trust_score)를 사용
                if trust_score >= 0.65 and volume_ratio_cur >= 2.0:
                    skip_flat_filter = True
                    skip_reason = "극도횡보예외(신뢰도+거래량폭발)"
                    logger.info(
                        f"⚡ [Flat-VolumeBreak] {symbol} 극도횡보 예외 통과 | "
                        f"Conf={trust_score:.2f}, Vol={fmt(volume_ratio_cur, '.2f')}x"
                    )
                
                # ✅ 정석: 강한 상승 구조이면 "극도의 횡보" 판정 자체가 성립하지 않음
                # - 예외(PEARL)로 우회하지 않고, 횡보 게이트의 '오판'을 제거한다.
                if has_golden_cross:
                    # 변수 추출
                    buying_ratio = safe_extract_scalar(indicators.get('buying_ratio', 0.0))
                    volume_ratio_gc = safe_extract_scalar(indicators.get('volume_ratio', 0.0))

                # 기존 예외 로직 (골든크로스 없을 때만)
                if not has_golden_cross and not skip_flat_filter:
                    # 0) 횡보 탈출 (최우선)        
                    ma20_slope = safe_extract_scalar(indicators.get('ma20_slope', 0))
                    if is_sideway_breakout(ma_deviation_value, prev_ma_deviation, ma5_slope, ma20_slope, atr_pct):
                        skip_flat_filter = True
                        skip_reason = f"횡보탈출(prev={prev_ma_deviation:+.2f}%→now={pct(ma_deviation_value, '+.2f')})"
                        logger.info(
                            f"🚀 [SidewayBreakout] {symbol} 횡보 정배열 탈출 감지! | "
                            f"prev={prev_ma_deviation:+.3f}% → now={pct(ma_deviation_value, '+.3f')}"
                        )
                
                # 2) ATR+ADX+거래량 예외
                if not skip_flat_filter and regime in (MarketRegime.RANGING, MarketRegime.TRENDING_UP):
                    adx_flat = safe_extract_scalar(indicators.get('adx', 0))
                    volume_ratio_flat = safe_extract_scalar(indicators.get('volume_ratio', 0))

                    if (atr_pct >= 0.25 and adx_flat >= 22 and volume_ratio_flat >= 0.35 and ma5 > ma10
                            and soleus_gajami):
                        skip_flat_filter = True
                        skip_reason = f"Vol={fmt(volume_ratio_flat, '.2f')}x+MA정배열"
                        logger.debug(f"⚡ [횡보예외완화] ATR/ADX/Vol 충족 → 통과")

                    # 골든크로스 강력 + 거래량 충분
                    # 🆕 GlobalFilter 가자미근 완화가 이미 적용된 경우엔 (초입 저유동성 정상)
                    #    볼륨 0.70 조건으로 재차단하지 않음
                    elif (
                        golden_count_td >= 2
                        and ma5 > ma10
                        and (
                            volume_ratio_flat >= 0.70
                            or bool(indicators.get("global_knee_volume_relaxed", False))
                        )
                    ):
                        skip_flat_filter = True
                        if bool(indicators.get("global_knee_volume_relaxed", False)) and volume_ratio_flat < 0.70:
                            skip_reason = f"골크{golden_count_td}개+GlobalKneeVolRelax(Vol={fmt(volume_ratio_flat, '.2f')}x)"
                        else:
                            skip_reason = f"골크{golden_count_td}개+Vol={fmt(volume_ratio_flat, '.2f')}x"

                # ✅ SSoT: 진짜 가자미근(soleus_gajami=True)이면 횡보 정배열(Flat) 차단을 무조건 통과
                # - 거래량/MA는 3m에서 들쭉날쭉 → "진입 조건"이 아니라 "사후 성과/포지션 크기" 영역
                _analysis_gajami_approved = bool(
                    soleus_gajami
                    or bool(indicators.get("is_soleus_zone", 0))
                    or bool(indicators.get("is_gajami_candidate", 0))
                )

                if (not skip_flat_filter) and _analysis_gajami_approved:
                    _er_ssot = float(indicators.get('er_10', 0.0) or 0.0)
                    skip_flat_filter = True
                    skip_reason = f"SSoT-GAJAMI (ER=observe:{_er_ssot:.3f})"
                    logger.info(
                        f"🦵 [FlatBypassed-SSoT-GAJAMI] {symbol} "
                        f"Analysis 가자미근 승인 → flat filter 통과 | "
                        f"ER={_er_ssot:.3f}"
                    )
                
                # 3) Positive Evidence 예외
                if not skip_flat_filter:
                    positive_signals = 0
                    
                    if golden_count_td >= 2:
                        positive_signals += 2                        
                    if buying_ratio >= 0.70:
                        positive_signals += 2
                    if volume_ratio >= 2.0:
                        positive_signals += 2
                    if indicators.get("early_entry_flag"):
                        positive_signals += 1
                    
                    if positive_signals >= 4:
                        # 🔒 추가 안전장치: Positive만으로 횡보 정배열을 뚫되,
                        # MA 구조 신호(최소 ma5>ma10) 없으면 "가짜 진주"로 간주하고 차단
                        ma5 = safe_extract_scalar(indicators.get("sma_5", 0.0))
                        ma10 = safe_extract_scalar(indicators.get("sma_10", 0.0))
                        ma_ok = (ma5 > 0 and ma10 > 0 and ma5 > ma10)

                        if ma_ok:
                            skip_flat_filter = True
                            skip_reason = f"Positive={positive_signals}"
                            logger.info(
                                f"✅ [FlatBypassed-Positive] {symbol} 횡보 정배열 예외 통과 | "
                                f"pos_sig={positive_signals} | ma_dev={pct(ma_deviation_value, '.2f')}"
                            )
                        else:
                            logger.info(
                                f"🚫 [FlatBypassed-Positive-Blocked] {symbol} Positive={positive_signals}이나 "
                                f"MA구조 미흡(ma5<=ma10) → 횡보 정배열 예외 차단 | ma_dev={pct(ma_deviation_value, '.2f')}"
                            )

                # 3.5) 가자미근 강화 조건 예외 (2025-12-13)
                # 철학: 횡보 정배열이지만 "저점 근처 + 거래량 + 매수우위"가 강하면 진입
                if not skip_flat_filter:
                    buying_ratio = safe_extract_scalar(indicators.get('buying_ratio', 0.5))
                    
                    # 가자미근 구간 + 거래량 충분 + 매수 우위 (SSoT: soleus_zone 기준)
                    _gajami_meta_flat = (data_package.ma_data or {}).get("gajami_meta") or {}
                    is_soleus_flat = bool(_gajami_meta_flat.get("soleus_zone", False))
                    if (is_soleus_flat and volume_ratio >= 0.80 and buying_ratio >= 0.65):
                        skip_flat_filter = True
                        skip_reason = f"가자미근강화(soleus_zone,Vol={fmt(volume_ratio, '.2f')}x,매수={buying_ratio*100:.0f}%)"
                        logger.info(
                            f"🟡 [FlatBypassed-Knee강화] {symbol} 횡보 정배열 예외 통과 | "
                            f"soleus_zone=True | Vol={fmt(volume_ratio, '.2f')}x | 매수={buying_ratio*100:.0f}% | "
                            f"ma_dev={pct(ma_deviation_value, '.2f')}"
                        )

                # 3.9) Candidate Persistence Eventless 관측
                # 후보 지속만으로 Flat 필터를 우회하지 않는다.
                if not skip_flat_filter and bool(indicators.get("candidate_persistent", False)):
                    volume_ratio_cur = volume_ratio
                    buying_ratio_cur = buying_ratio
                    atr_pct_cur = atr_pct

                    if volume_ratio_cur >= 0.30 and buying_ratio_cur >= 0.55 and atr_pct_cur >= 0.15:
                        logger.info(
                            f"👁️ [FlatBypass-Hold-OBSERVE] {symbol} 후보지속 Eventless 조건 충족 | "
                            f"hold={indicators.get('candidate_hold_count', 0)} | "
                            f"Vol={fmt(volume_ratio_cur, '.2f')}x | "
                            f"buy={fmt(buying_ratio_cur, '.2f')} | "
                            f"atr={atr_pct_cur:.2f}% "
                            f"(관측만·FlatBypass 우회 미사용)"
                        )
                
                # 4) 최종 판단
                if skip_flat_filter:
                    # 🚫 막차 진입 차단 ①: 모멘텀 소진 감지
                    momentum_exhausted = False
                    mom3 = indicators.get("momentum_3", None)
                    if isinstance(mom3, (list, tuple)) and len(mom3) >= 3:
                        try:
                            m0 = float(mom3[0]); m1 = float(mom3[1]); m2 = float(mom3[2])
                            # 3봉 연속 모멘텀 감소 → 상승 동력 소진
                            if (m0 > m1 > m2):
                                momentum_exhausted = True
                        except Exception:
                            momentum_exhausted = False

                    if momentum_exhausted:
                        self.last_rejection_reason = "횡보 막차 진입 차단 (모멘텀 소진)"
                        logger.warning(
                            f"🚫 [FlatBypassed-BLOCK] {symbol} 모멘텀 소진 감지 | "
                            f"momentum_3=[{mom3[0]:.2f}, {mom3[1]:.2f}, {mom3[2]:.2f}]"
                        )
                        return None

                    # 🚫 막차 진입 차단 ②: MA 하향 정배열 3/3
                    _ma10c = float(safe_extract_scalar(indicators.get("ma10_slope_exit_closed", SENTINEL_FLOAT), SENTINEL_FLOAT))
                    _ma10_slope_now = float(indicators.get("ma10_slope", 0) or 0) if is_sentinel(_ma10c) else _ma10c
                    down_slopes = 0
                    try:
                        if indicators.get("ma5_slope", 0) < 0:
                            down_slopes += 1
                        if _ma10_slope_now < 0:
                            down_slopes += 1
                        if indicators.get("ma20_slope", 0) < 0:
                            down_slopes += 1
                    except Exception:
                        pass

                    if down_slopes == 3:
                        self.last_rejection_reason = "횡보 막차 진입 차단 (MA 하향 정배열 3/3)"
                        logger.warning(
                            f"🚫 [FlatBypassed-BLOCK] {symbol} MA 하향 정배열 3/3 | "
                            f"ma5/10/20_slope 모두 < 0"
                        )
                        return None

                    # 🔒 [FLATBYPASS-MA10-GAJAMI-RESTORE-0830] ★457
                    # MA10은 후행 지표라 골크 직후에도 slope가 아직 음수일 수 있다.
                    # Analysis가 그 상태까지 포함해 soleus를 최종 승인했는데
                    # Strategy가 MA10 부호 하나로 다시 hard veto하면 초입 승인을 뒤집는다.
                    # KRW-QUID 08:18:53:
                    #   J1=1 PASS · dur=2 · Analysis soleus 승인
                    #   → ma10_slope=-0.0323, analysis_gajami=1 상태에서 FlatBypass 재차단.
                    # _analysis_gajami_approved는 J1 자체가 아니라 Analysis soleus 승인 SSoT다.
                    # MA10<=0은 Analysis 미승인일 때만 차단한다.
                    # 반사실로 확정되는 것은 첫 직접 차단기의 제거까지이며,
                    # 이후 하류 게이트 통과·실제 체결 여부는 런타임에서 별도 확인한다.
                    if (
                        (_ma10_slope_now < 0.0 or _ma10_slope_now == 0.0)
                        and not _analysis_gajami_approved
                    ):
                        self.last_rejection_reason = (
                            "횡보 막차 진입 차단 "
                            "(FlatBypass MA10<=0 ∧ Analysis 미승인)"
                        )
                        logger.info(
                            f"🚫 [FlatBypassed-MA10-BLOCK] {symbol} | "
                            f"ma10_slope={_ma10_slope_now:+.4f} "
                            f"analysis_gajami={int(_analysis_gajami_approved)} | "
                            f"MA10<=0은 Analysis 미승인일 때만 차단"
                        )
                        return None

                    if _ma10_slope_now == 0.0 and _analysis_gajami_approved:
                        logger.info(
                            f"🟢 [FlatBypassed-GAJAMI-FLAT-ONLY] {symbol} "
                            f"Analysis 가자미근 승인 → MA10 정확히 FLAT만 면제 | "
                            f"ma10_slope={_ma10_slope_now:+.4f}"
                        )

                    # ✅ FlatBypassed 최종 통과
                    logger.info(
                        f"🔥 [FlatBypassed] {symbol} 횡보 정배열 예외 통과 | "
                        f"이격도 {pct(ma_deviation_value, '.2f')} | 사유: {skip_reason}"
                    )
                else:
                    self.last_rejection_reason = f"횡보 정배열 ({pct(ma_deviation_value, '.1f')})"
                    logger.info(
                        f"🚫 [신호분석] {symbol} 횡보 정배열 "
                        f"(이격도 {pct(ma_deviation_value, '+.2f')} ≤ ±{sideways_threshold:.2f}%) - 변동성 부족"
                    )
                            
            #  골든크로스로 횡보 필터 통과 시 로깅
            if has_golden_cross and (-0.05 <= ma_deviation_value <= 0.05):
                logger.info(
                    f"✅ [신호분석] {symbol} 골든크로스 감지 "
                    f"- 횡보 정배열 필터 우회 (이격도 {pct(ma_deviation_value, '.1f')})"
                )
                
            # 🆕 횡보 탈출 신호 부여 (약한 신호)
            if skip_flat_filter and "횡보탈출" in skip_reason:
                # 횡보 탈출 첫 순간 → 약한 긍정 신호
                sideway_breakout_signal = 0.15
                logger.info(
                    f"✨ [SidewayBreakout] {symbol} 횡보 탈출 신호 +{sideway_breakout_signal:.2f}"
                )
            else:
                sideway_breakout_signal = 0.0    

            # ===============================================
            # 🆕 RANGING: 지지선 재테스트 간소화
            # ===============================================
            # 철학: "무릎에서 사서 어깨에서 판다"
            # 로직: TOUCH → BREAK → MOMENTUM → VOLUME
            
            # 🆕 RANGING: 지지선 재테스트 간소화
            # - allow_mid_box_entry 제거 - 사용처 없음
            # - SSoT: 지지선 탐지는 Analysis 책임
            # - 현재는 soleus_gajami + confidence 단일 파이프라인만 사용

            # 🧩 TRENDING_UP Oversold 체크 (Pullback 보정)
            if regime == MarketRegime.TRENDING_UP:
                
                if ma_deviation_state == "oversold":
                    ma5 = safe_extract_scalar(indicators.get("sma_5", 0))
                    
                    # 3봉 상승 패턴 확인
                    is_rising = False
                    
                    price_above_ma5 = (ge(current_price, ma5))
                    
                    if price_above_ma5 or is_rising:
                        logger.info(
                            f"🟢 [{symbol}] Pullback 진입 허용 | oversold "
                            f"MA5:{ma5:.1f} ≤ price:{current_price:.1f}, 상승:{is_rising}"
                        )
                    else:
                        analysis_reason = "ANALYSIS_INVALID_EARLY_TURN"
                        self.last_rejection_reason = analysis_reason
                        logger.info(
                            f"🚫 [{symbol}] Pullback 차단 | oversold + 하락 지속 "
                            f"(price {current_price:.1f} < MA5 {ma5:.1f}) | "
                            f"reason={analysis_reason}"
                        )
                        return None

            # ========================================
            # 6. 신호 컴포넌트 계산 (_calculate_signal_components 활용!)
            # ========================================
            
            # 🔥 함수 호출로 통합 신호 생성
            components = self._calculate_signal_components(df, data_package, ma_analysis)
            
            # 신호 없으면 조기 종료
            if not components:
                self.last_rejection_reason = "유효한 신호 없음"
                logger.info(f"🚫 [신호분석] {symbol} 유효한 신호 컴포넌트 없음")
                return None
            
            # 🔒 AXS-2C: Analysis Producer 원본은 보존하고 최종 판결용 축만 중립화
            if is_prebreak_quiet_ok or is_ultra_gc:
                vol_signal_original = float(components.get("volume_signal", 0.0))
                components["volume_signal"] = 0.0
                logger.debug(
                    f"🔧 [Vol-Neutralize] {symbol} 거래량 축 중복 제거 | "
                    f"orig_vol_signal={vol_signal_original:.3f} → 0.000"
                )
            
            logger.debug(
                f"[신호생성] {symbol} | 체제: {regime.value} | "
                f"Components: {components}"
            )

            # ========================================
            # 7. 최종 신뢰도 계산 (_calculate_final_confidence 활용!)
            # ========================================
            
            # ============================================================
            # 🦵 가자미근 초입: components 변조 금지 (SSoT 준수)
            # ============================================================
            # ✅ SSoT 정석: components(신호 원본)는 절대 변조하지 않는다.
            # - 가자미근 초입 보정은 bonus/threshold(후단)에서만 수행해야 한다.
            # - 여기서 volume/candle을 올리면 BASE(weighted_sum)가 오염되어 시간대 비교가 깨진다.
            # 
            # 제거 근거:
            # - 01:51 진입 시 volume_signal=0.0 → 0.30 강제 증폭
            # - 이후 초저가 스케일링으로 0.30×10.68 → 1.00 폭주
            # - 결과: BASE가 부풀려져 하락 중인데도 높은 점수
            pass
            
            # 함수 호출
            base_confidence = self._calculate_final_confidence(
                components, weights, regime, performance
            ) or 0.0  # ⚠️ None 방어
            
            # 🆕 횡보 탈출 신호 반영 (2025-12-12 HUNT 대응)
            # SSoT: SidewayBreakout은 base_confidence를 변경하지 않음
            pass
            
            # ⚡ 빠른 진입 로직 개선 (무릎 밑 진입 허용 - Phase 1.5 균형형)
            is_alignment_transition = ma_analysis.get('is_alignment_transition', False)
            transition_strength = ma_analysis.get('transition_strength', 0)
            atr_pct = ma_analysis.get('atr_pct', 0)
            avg_spacing = abs(ma_analysis.get('avg_spacing', 0))
            
            # 전환 직후 조건 완화
            # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
            # 🔥 quick_entry 블록 완전 제거 (2026-02-16)
            # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
            # 제거 사유:
            # - if-else 구조 포함 전체 제거 (7656~7689줄)
            # - 실제 작동 안 함 (pass로 종료)
            # - box_pos >= 0.80 박스 기반 차단 (계산 오류 가능)
            # - Lifecycle 단독 판사 원칙
            # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
            
            # STEP 3: SSoT 보너스 적용 (Analysis 결과만 사용)
            # 철학: Analysis가 품질 평가, Strategy는 결과만 적용
            # 근거: ZBT 09:08 (0.277 → 0.327, 제곱근과 합쳐 통과)
            # SSoT: 가자미근 점수는 Strategy가 shape_bonus로 직접 계산
            # - Analysis의 gajami_bonus 제거 완료 (역할 분리)
            
            # 🔧 신뢰도 임계값 2단계 동적 조정 (2025-11-08)
            # 철학: "무릎에서 사서 어깨에서 판다"
            # 구조: base_threshold → threshold_factor 누적 → effective_threshold 산출
            # 🔒 재판관 고정 원칙 (2026-01-06)
            # 철학: 신뢰도 기준(재판관)은 절대 흔들지 않음
            # 좋은 조건이면 점수(피고인)를 높여서 통과시킴
            # ❌ 제거: 체제별/Override별 threshold 완화 로직
            # ============================================================
            # 🔒 TD 오진입 차단 (무릎 정의 수정)
            # ============================================================
            # 목적: TD 손실 주범인 가짜 무릎 진입 제거
            # 허용: gajami_entry, pre_gajami, knee_mode만
            # ============================================================
            
            if regime == MarketRegime.TRENDING_DOWN:
                gajami_entry_flag = bool(indicators.get("gajami_entry", False))
                # 묶음 v5: pre_gajami와 pre_gajami_candidate가 없으면 pre_gajami_flag는 False이며, gajami_entry_flag는 별도로 판정한다.
                pre_gajami_flag = bool(
                    indicators.get("pre_gajami", False)
                    or indicators.get("pre_gajami_candidate", False)
                )
                if not (gajami_entry_flag or pre_gajami_flag):
                    extra_penalty = PENALTY_WEIGHTS.get("td_strict_no_gajami", 0.30)
                    penalty_reasons.append(f"TD_NO_GAJAMI(+{extra_penalty:.2f})")
                    penalty = add(penalty, extra_penalty)
                    penalty_details["td_no_gajami"] = extra_penalty
                    logger.warning(
                        f"⚠️ [TD-NoGajami-Soft] {symbol} 하락장 비가자미근 → 페널티 적용 | "
                        f"gajami={gajami_entry_flag}, pre_gajami={pre_gajami_flag}"
                    )
            
            base_threshold = self.min_confidence  # config 기준값 (예: 0.50)
            threshold_factor = 1.0  # 🔒 고정 (재판관 완화 금지)
            confidence_threshold = base_threshold  # 항상 동일하게 유지
            
            logger.debug(
                f"🎯 [재판관고정] 체제={regime.value if regime else 'None'} | "
                f"임계값={base_threshold:.2f} (고정) | base={base_confidence:.3f}"
            )
            
            # ========================================
            # 8. 타임프레임 가중치 적용
            # ========================================
            timeframe_weight = weights.get(self.timeframe.value, 1.0)
            
            # 🆕 강한 신호 페널티 하한 적용 (2025-11-17)
            ma_analysis_for_cap = ma_analysis  # 🔒 SSoT: 확정된 값 사용
            ma_alignment_level = ma_analysis_for_cap.get('alignment_level', 0)
            ma_golden_count = ma_analysis_for_cap.get('golden_count', 0)

            is_strong_signal = (
                ma_alignment_level >= STRONG_ALIGNMENT_LEVEL
                or ma_golden_count >= STRONG_GOLDEN_COUNT
            )

            # RG 체제에서 이미 계산된 SoftPenalty만 최종 판단에 복원한다.
            # 🔒 [2026-08-08] 선행 곱셈 penalty_factor를 폐기하는 PF 최종 SSoT.
            # 🔎 [LOG-TRUTH-PENALTY-0821]
            # 이 대입 이전의 penalty_factor 곱셈값과
            # penalty_details의 regime_ranging 이외 값은 최종 PF에 반영되지 않는다.
            # 따라서 이 지점 이전의 penalty 로그는 "판정 감점"이 아니라 관측값으로 해석한다.
            penalty_factor = 1.0 - float(
                penalty_details.get("regime_ranging", 0.0) or 0.0
            )

            if int(indicators.get("gajami_shape_slope_soft_penalty", 0)) == 1:
                penalty_factor = self._apply_soft_penalty(
                    penalty_factor,
                    0.93,
                    penalty_reasons,
                    "SHAPE_SLOPE_SOFT",
                )
                logger.info(
                    f"⚠️ [SHAPE-SLOPE-PENALTY] {symbol} "
                    f"완만 골크 slope hardcut 격하 → 신뢰도 -7% 감점"
                )
            
            # 🧱 Volume CAP (2025-12-07)
            #  - 목적: 거래량 축 페널티 누적 폭발 방지
            # ============================================================
            # ✅ SSoT: 선형 신뢰도 시스템 (최종 명세 적용)
            # final = base + bonus - penalty
            # - 곱셈 penalty_factor 제거
            # - penalty/combined_confidence 제거
            # - SoleusWeight / GAJAMI EARLY SCORE 제거
            # ============================================================
            
            # base (weighted_sum)
            base = float(base_confidence or 0.0)
            
            # === PATCH H1: BASE 재계산 (체제 + 이격 반영) ===
            # 목적: 하락/횡보에서 BASE 과대평가 억제
            base_original = base
            
            # soleus(가자미근 확정)인 경우, base 점수에서 체제 감점으로 죽이지 않는다.
            # 체제 리스크는 포지션/리스크 계층에서 처리한다.
            is_soleus = bool(
                safe_extract_scalar(indicators.get("hard_gajami_pass", 0.0), 0.0) or
                safe_extract_scalar(indicators.get("soleus_final_locked", 0.0), 0.0)
            )
            
            # 🎯 가자미근 신뢰도 조정 (형상 확정 → 체제 무관 진입)
            # 원칙: 가자미근 SSoT 확정 = 무조건 진입
            #   - 체제는 포지션 사이징·매도 전략에서 반영
            #   - 진입 게이트로 사용하면 불안정한 체제 판단이 유효 진입을 차단
            if is_soleus and soleus_gajami_ssot:
                # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
                # 🦵 base 인위 상향 제거 (2026-02-07)
                #
                # 기존 문제:
                # - base = max(base, 0.55)
                # - 90% 이상 거래가 동일 신뢰도
                #
                # 원칙:
                # - base는 '측정값'
                # - 진입 보장은 threshold(GAJAMI-FLOOR)에서 담당
                # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
                # base = max(base, ConfidenceAdjustmentConstants.MIN_CONF_SOLEUS)  # 제거

                # 체제 정보는 로깅만 (진입 신뢰도에 곱하지 않음)
                regime_factor = 1.0
                
                logger.info(
                    f"📊 [GAJAMI-CONFIDENCE] {symbol} | "
                    f"base={base:.3f} × regime={regime_factor:.2f} → "
                    f"final={base:.3f} | regime={regime}"
                )
            
            # regime: _calculate_final_confidence에서 이미 적용 → 중복 제거
            regime_weight = 1.0
            # base *= regime_weight  # 비활성화
            
            # 2. MA5 이격도 기반 감점
            ma5_gap_pct = float(safe_extract_scalar(indicators.get("ma5_gap_pct", 0.0), 0.0))
            if abs(ma5_gap_pct) > 5.0:  # 절댓값 5% 이상
                over_gap = abs(ma5_gap_pct) - 5.0
                gap_penalty_rate = over_gap * 0.02  # 1%당 2% 감점
                base *= max(0.0, 1.0 - gap_penalty_rate)
            
            # 3. 위치 축 BASE 감점 — 확정봉 고점 대비 신고점권만 감점
            # 🔒 [POSITION-AXIS-0807] recent_high(현재봉 포함) → recent_high_closed(확정봉,
            #    df.iloc[-21:-1] 20봉, t5 L3630 기존 정의)로 교체. t5 L9774 경고 준수.
            #    실측 comparison n=1,333: dd -0.3~0% 21.5%(최악) · -5~-3% 43.8%(최선)
            #    구코드는 정확히 반대(깊을수록 강한 감점)였다. 가자미근 면제도 제거
            #    (그 면제로 08-07 12건이 무감점 통과했다). 직전 고점 돌파(dd>0)는
            #    실측 표본(-0.3~0%) 밖이므로 감점하지 않는다.
            #    is_gajami_zone/golden_count/entry_type/is_strong_gc/is_trend_setup은
            #    이 블록 전용 변수라 함께 제거(사용처 재확인 완료).
            recent_high_temp = safe_extract_scalar(
                indicators.get("recent_high_closed", current_price),
                current_price,
            )
            decline_from_high = SENTINEL_FLOAT
            if recent_high_temp > 0 and current_price > 0:
                decline_from_high = ((current_price - recent_high_temp) / recent_high_temp) * 100.0
                if -0.30 <= decline_from_high <= 0.0:
                    base *= 0.70
                elif -1.00 <= decline_from_high < -0.30:
                    base *= 0.85
                    
            # 신뢰도는 5개 개별 신호의 실제 가중합을 그대로 유지한다.
            # GC·Soleus·birth는 자격과 문맥이며 BASE 하한을 만들지 않는다.

            # 로그 출력
            if base != base_original:
                logger.info(
                    f"🔧 [BASE-ADJUST] {symbol} | "
                    f"original={base_original:.3f} → adjusted={base:.3f} | "
                    f"regime={regime.name if regime else 'None'}({regime_weight:.2f}) "
                    f"gap={ma5_gap_pct:+.1f}% "
                    f"dd={fmt(decline_from_high, '+.2f')}%"
                )
            # === END PATCH H1 ===
            
            # 🔥 PATCH CLEAN-VOL-EXEMPT: AXS-4 제거 (정책 충돌 해소)
            # 
            # 제거 사유:
            # - AXS-2C(Vol-Neutralize)와 정책 충돌
            # - 동일 조건(is_ultra_gc)에서 서로 다른 값 설정:
            #   * AXS-2C: volume_signal = 0.0 (무력화)
            #   * AXS-4: volume_signal = 0.20 (양전환)
            # 
            # 정책 단일화:
            # - 초강력 GC 구간의 거래량 축 처리는 AXS-2C로 통일
            # - EntryFilter에서 이미 거래량 검증 완료
            # - Strategy 판결부에서는 거래량 신호를 심판에서 제외
            
            # (제거됨) Vol-Exempt 로직
            # AXS-2C(Vol-Neutralize)가 EntryFilter에서 이미 처리함
            pass
            
            # 공통 스칼라 (스코프/NameError 방지)
            current_price = float(current_price or 0.0)
            
            ma5 = safe_extract_scalar(indicators.get("ma5", 0.0))
            ma10 = safe_extract_scalar(indicators.get("ma10", 0.0))
            ma20 = safe_extract_scalar(indicators.get("ma20", 0.0))
            
            vol_ratio = safe_extract_scalar(indicators.get("volume_ratio", 0.0))
            buy_ratio = safe_extract_scalar(indicators.get("buying_ratio", 0.0))
            
            atr = safe_extract_scalar(indicators.get("atr", 0.0))
            atr_pct = (atr / current_price * 100.0) if (atr > 0 and current_price > 0) else 0.0
            
            # 🔒 [POSITION-AXIS-0807] 위치 축 고점/하락률은 위 decline_from_high 단일 SSoT만
            #    사용한다. 여기서 recent_high(현재봉 포함)를 재계산하면 위에서 고친 오류가
            #    복원되므로 제거. gmeta도 이 라인 전용이라 사용처 0 확인 후 함께 삭제.
            
            ma_analysis = data_package.ma_data or {}
            
            # ----------------------------
            # BONUS (계열별 배타적 최대값)
            # ----------------------------
            # bonus, bonus_reasons는 이미 위에서 초기화됨
            
            # 1) 구조 계열: 가자미근 형상 완성도 보너스 — 2026-08-01 삭제(死코드)
            #    사유: 진입조건이 components['candle_signal'] >= 0.50 이었으나
            #          이 키는 상수 0.0(legacy 슬롯)이라 구조적으로 발동 불가(발동 0건).
            #          가점 자체도 `bonus += shape_bonus`가 이미 주석처리된 상태였음.

            # 2) 추세 계열: 정배열(강/약) - 배타적
            trend_bonus = 0.0
            if ma5 > 0 and ma10 > 0:
                gap_5_10 = ((ma5 - ma10) / ma10 * 100.0) if ma10 > 0 else 0.0
                if ma20 > 0 and ma5 > ma10 > ma20:
                    gap_10_20 = ((ma10 - ma20) / ma20 * 100.0) if ma20 > 0 else 0.0
                    if gap_5_10 > 1.0 and gap_10_20 > 0.5:
                        trend_bonus = 0.03
                        bonus_reasons.append("강한정배열(+0.03)")
                if trend_bonus == 0.0 and ma5 > ma10 and gap_5_10 > 0.5:
                    trend_bonus = 0.02
                    bonus_reasons.append("정배열(+0.02)")
            
            bonus += trend_bonus
            
            # 3) 수급 계열: 수급우위/거래량폭발/매수세 - 배타적
            demand_bonus = 0.0
            
            # ✅ 수급우위(+0.03) 조건 강화: 순간 매수쏠림 과대평가 방지
            if vol_ratio > 3.0 and buy_ratio > 0.70:
                demand_bonus = 0.03
                bonus_reasons.append("수급우위(+0.03)")
            elif vol_ratio > 3.0:
                demand_bonus = 0.02
                bonus_reasons.append("거래량폭발(+0.02)")
            elif buy_ratio > 0.70:
                demand_bonus = 0.02
                bonus_reasons.append("매수세(+0.02)")
            
            bonus += demand_bonus
            
            # 4) 변동성 계열: "안정 구간" 보너스 (+0.02)
            # ✅ 정석: 단타 가자미근은 '안정'이 우선
            # - 0.5% 이상은 보너스 대상이 아니라 정상/리스크 구간으로 본다.
            # 
            # 개선 근거:
            # - 기존: 0.5~2.0% → 보너스 (중간값 우대)
            # - 개선: 0~0.5% → 보너스 (안정 우대)
            if atr_pct > 0 and atr_pct < 0.50:
                bonus += 0.02
                bonus_reasons.append("ATR안정(+0.02)")

            # [P2-DIP-CONF 제거 06-29] p2(실제눌림) 신뢰도 가감 철회. 사유: 신뢰도 점수는 변별력 무기력
            #   (정답봉은 score=NA로 confidence 前 차단·진입 고신뢰도 전부 손실) → ±0.03이 닿지도/효과도 없음.
            #   p2는 게이트로 이전: 5_ana TOP-RISE 사면 트리거(_p2_dip_early). p2는 게이트 한 곳에만 둠(중복 제거).

            # Phase 2: Bonus Consolidation (v2 SSoT)
            # 위치: bonus 상한 적용 직전
            # 목적: final_score 계산 이전에 모든 bonus 반영
            
            # 선행 변수 정의
            TD_KNEE_MIN_CONF_THRESHOLD = 0.28

            # 🔒 [2026-08-07] 보너스 기준값 고정.
            #   기존: bonus 계산의 분모(base_threshold)·곱셈인자(self.min_confidence)가
            #   둘 다 self.min_confidence 라 임계 인하 시 이중 축소되어 0이 된다.
            #   실측: 현재 BONUS 최빈값 0.020 = (1 - 0.28/0.30) × 0.30 — 이 값을 유지해야 한다.
            #   임계(진입 하한)와 보너스(점수 가산)는 다른 축이다. 연동을 끊는다.
            CONF_BONUS_REF = 0.30

            # 1) Scalp Bonus
            if is_scalp_entry:
                bonus += (1.0 - 0.875) * CONF_BONUS_REF
                logger.debug(
                    f"🎯 [ScalpBonus] {symbol} 단타 진입 보너스 | "
                    f"bonus=+{(1.0 - 0.875) * CONF_BONUS_REF:.4f}"
                )
            
            # 2-6) Knee 계열 Bonus
            # 🎯 PATCH ⑧-C-2: confirmed_knee 체크 (일반 케이스는 확정 무릎만 허용)
                        
            # 7) Bull Market Bonus
            if regime == MarketRegime.BULL_MARKET:
                bonus += max(0.0, (1.0 - (0.38 / CONF_BONUS_REF)) * CONF_BONUS_REF)
                logger.debug(
                    f"🔥 [BullMarketBonus] {symbol} bonus=+{(1.0 - (0.38 / CONF_BONUS_REF)) * CONF_BONUS_REF:.4f} (목표 0.38)"
                )
            
            # 8) Uptrend Early Bonus
            elif regime == MarketRegime.TRENDING_UP:
                try:
                    # ✅ 정석: "상승 초입"은 ADX가 아니라 '전환 이벤트'가 먼저다.
                    # - 목표: 00:45 같은 "MA5 상향 전환 + 저점 구간"에서 점수가 먼저 올라오도록
                    # - 재판관(threshold)은 고정, 피고인(confidence)만 보너스로 끌어올림

                    adx_val_uptrend = safe_extract_scalar(indicators.get('adx', 0), 0.0)

                    # 필요한 지표 추출
                    ma5 = float(safe_extract_scalar(indicators.get("sma_5", indicators.get("ma5", 0)), 0.0))
                    ma5_slope = float(safe_extract_scalar(indicators.get("ma5_slope", 0.0), 0.0))
                    ma5_slope_prev1 = float(safe_extract_scalar(indicators.get("ma5_slope_prev1", 0.0), 0.0))
                    ma10_slope = float(safe_extract_scalar(indicators.get("ma10_slope", 0.0), 0.0))
                    ma20_slope = float(safe_extract_scalar(indicators.get("ma20_slope", 0.0), 0.0))  # ← 추가!
                    
                    # MA5 이격 계산 (현재가 기준, 이 블록 한정 인트라바값)
                    ma5_gap_intrabar = ((current_price - ma5) / ma5 * 100.0) if ma5 > 0 else 0.0

                    head_lift = bool(indicators.get("head_lift_trigger", False))
                    
                    # (A) MA5 전환(하락→상승) = "첫 고개" 이벤트
                    ma5_turnup = (ma5_slope > 0.0 and ma5_slope_prev1 <= 0.0)

                    # 패치 ②: Δslope 기반 "발생형" 보너스 (최우선)
                    # - 기울기 자체가 아니라 '증가 시작'에 점수
                    slope_delta = ma5_slope - ma5_slope_prev1

                    # 🔧 정석 철학: 위치가 아닌 재가속 기반
                    # - pos 제한 제거 (상단 눌림 재가속도 포함)
                    # - 형상+추세 확인으로 대체 (HEAD-BLOCK, VOL_LOW와 동일)
                    # - 안전장치: ma5 >= ma20 (재가속 확인, 꼭대기 과열 방지)
                    slope_turnup_ctx = (
                        slope_delta > 0.00005          # 미세하지만 명확한 증가
                        and ma5_slope > 0.0
                        and ma5_slope_prev1 <= 0.00010 # 직전은 거의 평행/하락
                        and _shape_trend_confirm(indicators)  # 형상+추세 판정
                        and ma5_slope >= ma20_slope    # 재가속 확인 (안전장치)
                    )

                    if slope_turnup_ctx:
                        target = 0.42
                        bonus += max(0.0, (1.0 - (target / CONF_BONUS_REF)) * CONF_BONUS_REF)
                        logger.info(
                            f"⚡ [Slope-TurnupBonus] {symbol} 재가속 기반 보너스 | "
                            f"Δslope={slope_delta:.6f} | "
                            f"s5={ma5_slope:.6f} prev={ma5_slope_prev1:.6f} | "
                            f"s20={ma20_slope:.6f} (재가속={ma5_slope >= ma20_slope}) | "
                            f"bonus=+{(1.0 - (target / CONF_BONUS_REF)) * CONF_BONUS_REF:.4f} (목표 {target:.2f})"
                        )

                    # 패치 ①: 전환 컨텍스트 보너스
                    # (B) 초입 조건: 저점 영역 + MA5 근처 + 고개듦/전환
                    # 🔧 정석 철학: 위치가 아닌 재가속 기반 (동일 적용)
                    elif (
                        _shape_trend_confirm(indicators)  # 형상+추세 판정
                        and abs(ma5_gap_intrabar) <= 1.20
                        and (head_lift or ma5_turnup)
                        and ma10_slope >= -0.00010
                        and ma5_slope >= ma20_slope       # 재가속 확인 (안전장치)
                    ):
                        target = 0.40
                        bonus += max(0.0, (1.0 - (target / CONF_BONUS_REF)) * CONF_BONUS_REF)
                        logger.debug(
                            f"⚡ [Uptrend-EarlyTurnupBonus] {symbol} 형상+추세 확인 | "
                            f"turnup={ma5_turnup} head_lift={head_lift} | "
                            f"gap={ma5_gap_intrabar:+.2f}% | "
                            f"s5={ma5_slope:.5f} (prev={ma5_slope_prev1:.5f}) s10={ma10_slope:.5f} s20={ma20_slope:.6f} | "
                            f"재가속={ma5_slope >= ma20_slope} | "
                            f"bonus=+{(1.0 - (target / CONF_BONUS_REF)) * CONF_BONUS_REF:.4f} (목표 {target:.2f})"
                        )

                    # (C) 기존 로직 유지: ADX 기반 "상승 지속" 보너스 (늦게라도 추가 근거)
                    elif adx_val_uptrend >= 20:
                        target = 0.40
                        bonus += max(0.0, (1.0 - (target / CONF_BONUS_REF)) * CONF_BONUS_REF)
                        logger.debug(
                            f"⚡ [Uptrend-EarlyBonus] {symbol} "
                            f"ADX={fmt(adx_val_uptrend, '.1f')} | bonus=+{(1.0 - (target / CONF_BONUS_REF)) * CONF_BONUS_REF:.4f} (목표 {target:.2f})"
                        )

                except Exception as e:
                    logger.debug(f"상승초입 완화 처리 오류 (factor 방식): {e}")
            
            # 8-2) Early Knee Timing Bonus (초입 타이밍 보너스)
            # 🔴 CRITICAL: MA5 돌파 초입 가산점
            # - 목표: 00:45 같은 "무릎 시작점"에서 점수 조기 상승
            # - 철학: 가격이 MA5를 상향 돌파하는 순간 = 최적 진입 타이밍
            try:
                ma5 = float(safe_extract_scalar(indicators.get("sma_5", indicators.get("ma5", 0)), 0.0))
                ma10 = float(safe_extract_scalar(indicators.get("sma_10", indicators.get("ma10", 0)), 0.0))
                volume_ratio = float(safe_extract_scalar(indicators.get("volume_ratio", 0.0), 0.0))
                
                if ma5 > 0 and ma10 > 0:
                    gap_5_10 = abs(ma5 - ma10) / ma10
                    
                    # MA5 돌파 + 수렴 + 거래량 = 초입 타이밍
                    early_knee_ctx = (
                        current_price > ma5            # 가격 > MA5 (돌파)
                        and ma5 >= ma10                # 골든크로스 구조
                        and gap_5_10 <= 0.005          # MA5-MA10 수렴 (0.5% 이내)
                        and volume_ratio >= 1.0        # 거래량 동반
                    )
                    
                    if early_knee_ctx:
                        target = 0.38
                        bonus += max(0.0, (1.0 - (target / CONF_BONUS_REF)) * CONF_BONUS_REF)
                        logger.debug(
                            f"🎯 [Early-Knee-Bonus] {symbol} "
                            f"price={current_price:.0f} > MA5={ma5:.0f} | "
                            f"gap_5_10={gap_5_10:.3%} | vol={volume_ratio:.2f}x | "
                            f"bonus=+{(1.0 - (target / CONF_BONUS_REF)) * CONF_BONUS_REF:.4f} (목표 {target:.2f})"
                        )
            except Exception as e:
                logger.debug(f"Early-Knee 보너스 처리 오류: {e}")
            
            # 9) RANGING Calibration Bonus
            if regime == MarketRegime.RANGING:
                ma_sig = safe_extract_scalar(components.get('ma_signal', 0.0), 0.0)
                vol_sig = safe_extract_scalar(components.get('volume_signal', 0.0), 0.0)
                # 🔒 [BW-QUALITY-RECONNECT-0823]
                # 2026-08-01 candle_signal → bw_signal 개명 때 이 소비처만 누락됐다.
                # candle_signal은 DB/로그 호환용 legacy 슬롯으로 항상 0.0이고,
                # 실제 Entry Timing 신호는 candle_entry_signal(= Analysis bw_signal)이다.
                candle_sig = safe_extract_scalar(
                    components.get('candle_entry_signal', 0.0),
                    0.0,
                )

                # 🔒 [RANGING-MA-REJUDGE-PURGE-0822-R1]
                # Analysis가 헌법·shape·lifecycle에서 MA 자격을 판정한 뒤,
                # Strategy는 ma_sig 하나로 같은 MA를 다시 hard veto하지 않는다.
                # 음수 MA는 관측 + quality 0으로만 반영한다.
                if ma_sig < 0.0:
                    indicators["ranging_ma_weak_observe"] = 1
                    logger.info(
                        f"⚠️ [RANGING-MA-WEAK-OBSERVE] {symbol} "
                        f"ma_sig={ma_sig:.2f} → hard block 금지, quality_ma=0.00"
                    )
                ma_sig_quality = max(ma_sig, 0.0)
                
                candle_sig_quality = max(candle_sig, 0.0)
                vol_sig_quality = max(vol_sig, 0.0)
                
                quality = (
                    0.40 * ma_sig_quality +
                    0.40 * vol_sig_quality +
                    0.20 * candle_sig_quality
                )
                quality = max(0.0, min(quality, 1.0))
                if quality < 0.35:
                    # 🔒 [RANGING-QUALITY-AUTH-CUT-0822] 판사 승인이 품질 하한을
                    #    면제하지 못한다. 가자미근 형성만 면제 자격을 갖는다.
                    if soleus_gajami_ssot:
                        indicators["ranging_quality_low_observe"] = 1
                        logger.info(
                            f"⚠️ [RANGING-QUALITY-OBSERVE] {symbol} "
                            f"bar={indicators.get('current_candle_time', 'NA')} "
                            f"quality={quality:.6f}<0.350000 "
                            f"(ma={ma_sig_quality:.6f}, vol={vol_sig_quality:.6f}, "
                            f"bw={candle_sig_quality:.6f}) "
                            f"soleus={int(bool(soleus_gajami_ssot))} "
                            f"judge_auth_obs={int(_judge_lane_authorized)} "
                            f"dur={dur_5x10} px={current_price:.8g} "
                            f"→ hard block 금지"
                        )
                    else:
                        # 🔒 [RANGING-QUALITY-BLOCK-TRACE-0825]
                        #  면제(OBSERVE)만 로그하고 차단은 무로그였다. 0.35 절벽의
                        #  정당성을 재려면 차단측 quality 값이 반드시 필요하다.
                        #  행동 변화 없음 — 조건·반환값 불변, 관측만 대칭화한다.
                        self.last_rejection_reason = "RANGING_QUALITY_LOW"
                        logger.info(
                            f"🚫 [RANGING-QUALITY-BLOCK] {symbol} "
                            f"bar={indicators.get('current_candle_time', 'NA')} "
                            f"quality={quality:.6f}<0.350000 "
                            f"(ma={ma_sig_quality:.6f}, vol={vol_sig_quality:.6f}, "
                            f"bw={candle_sig_quality:.6f}) "
                            f"soleus={int(bool(soleus_gajami_ssot))} "
                            f"judge_auth_obs={int(_judge_lane_authorized)} "
                            f"dur={dur_5x10} px={current_price:.8g} "
                            f"→ 진입 차단"
                        )
                        return None

                base_thr = base_threshold * threshold_factor
                min_conf_thr = float(self.min_confidence)
                
                loose_thr = max(min_conf_thr, base_thr - 0.10)
                # 🔒 [DEAD-MINMAX-PURGE-0828] min(a, a)는 항상 a — 죽은 연산 제거.
                # 후단 calibrated_thr 상한은 그대로 유지하므로 동작 변화 0.
                strict_thr = base_thr + 0.07
                
                calibrated_thr = strict_thr - (strict_thr - loose_thr) * quality
                
                calibrated_thr = max(min_conf_thr, min(calibrated_thr, base_thr + 0.07))
                indicators["ranging_calibrated_threshold"] = float(calibrated_thr)
                
                logger.info(
                    f"📐 [RangingHybrid] {symbol} RANGING 캘리브레이션 | "
                    f"quality={quality:.2f} "
                    f"(ma_sig={ma_sig:+.2f}→{ma_sig_quality:.2f}, "
                    f"vol={vol_sig:.2f}, candle={candle_sig:+.2f}) | "
                    f"thr_base={base_thr:.2f}, thr_range=[{loose_thr:.2f},{strict_thr:.2f}] → "
                    f"thr_final={calibrated_thr:.2f}"
                )
                
                bonus += max(0.0, (1.0 - (calibrated_thr / CONF_BONUS_REF)) * CONF_BONUS_REF)
                logger.debug(
                    f"📐 [RangingBonus] {symbol} bonus=+{(1.0 - (calibrated_thr / CONF_BONUS_REF)) * CONF_BONUS_REF:.4f}"
                )
            
            # 10) Bull Strength Bonus
            if bull_strength > 0:
                bull_factor = 1 - BULL_RELAX_THRESHOLD_FACTOR * min(bull_strength, 1.0)
                bonus += (1.0 - bull_factor) * CONF_BONUS_REF
                
                logger.debug(
                    f"🔥 [불장-Bonus] {symbol} | "
                    f"강도={bull_strength:.2f} | "
                    f"bonus=+{(1.0 - bull_factor) * CONF_BONUS_REF:.4f} (factor={bull_factor:.3f})"
                )
            
            # End Phase 2
            
            # 프레임 A: bonus 상한 (base*0.30)
            bonus_before = bonus
            bonus = min(bonus, base * 0.30)
            if bonus < bonus_before:
                bonus_reasons.append("BONUS_CAP")
            
            # ----------------------------
            # PENALTY (선형, 배타/단계 포함)
            # ----------------------------
            # ✅ 앞에서 누적된 penalty/penalty_reasons 보존 (리셋 금지)
            penalty = float(penalty) if isinstance(penalty, (int, float)) else 0.0
            penalty_reasons = penalty_reasons if isinstance(penalty_reasons, list) else []
            
            # 1) 구조 계열: MA 역배열(-0.05)
            # - 가자미근 컨텍스트에서는 MA는 '확인자' → 차단/감점 권한 제거
            if (not is_soleus) and (ma5 > 0 and ma10 > 0 and ma5 < ma10):
                penalty += 0.05
                penalty_reasons.append("MA역배열(-0.05)")
            
            # 2) 수급 계열: 수급악화/거래량부족/매도압력 - 배타적
            if vol_ratio < 0.5 and buy_ratio < 0.4:
                penalty += 0.05
                penalty_reasons.append("수급악화(-0.05)")
            elif vol_ratio < 0.5:
                penalty += 0.03
                penalty_reasons.append("거래량부족(-0.03)")
            elif buy_ratio < 0.4:
                penalty += 0.03
                penalty_reasons.append("매도압력(-0.03)")
            
            # 3) 위치 축 soft penalty는 BASE에서 단일 처리(위 decline_from_high).
            #    급락 안전 Gate만 유지 — <=-5%는 실측 표본(-5~-3%) 바깥이라
            #    "좋다"고 입증되지 않았으므로 증거 없이 철거하지 않는다.
            # 🔒 [UNBLOCK-DECLINE-1031] R819 : 막힌 41 건 +0.166 % · 통과 대비 +0.433 %p [+0.051, +0.768] → config.ini [STRATEGY] decline_gate_enabled (기본 true = 현행 차단 · false = 해제)
            if (
                decline_from_high != SENTINEL_FLOAT
                and decline_from_high <= -5.0
                and not self.config.getboolean("STRATEGY", "decline_gate_enabled", fallback=True)
            ):
                logger.info(f"🟢 [UNBLOCK-DECLINE] {symbol} 급락 {decline_from_high:.2f}% 차단 꺼짐 → 진행")
            elif decline_from_high != SENTINEL_FLOAT and decline_from_high <= -5.0:
                self.last_rejection_reason = f"급락 Gate ({decline_from_high:.2f}%)"
                logger.info(f"❌ [GATE-BLOCK] {symbol} 급락 {decline_from_high:.2f}%")
                return None
            
            # 4) 변동성 계열: ATR 과다/부족
            if atr_pct > 5.0:
                penalty += 0.03
                penalty_reasons.append(f"ATR과다(-0.03,{atr_pct:.1f}%)")
            elif 0.0 < atr_pct < 0.3:
                penalty += 0.02
                penalty_reasons.append(f"ATR부족(-0.02,{atr_pct:.1f}%)")
            
            # === PATCH H2: PENALTY 강화 (과도이격/연속하락/과도랠리) ===
            
            # 5) 과도 이격 페널티 (신규)
            # - ma5_gap_pct는 8439에서 Analysis 지표값으로 계산됨
            # - 현재가 기준 인트라바 이격(ma5_gap_intrabar)과 혼용 금지
            if abs(ma5_gap_pct) > 5.0:
                over_gap = abs(ma5_gap_pct) - 5.0
                gap_penalty = over_gap * 0.02  # 1%당 -0.02
                penalty += gap_penalty
                penalty_reasons.append(f"과도이격(-{gap_penalty:.2f},MA5{ma5_gap_pct:+.1f}%)")
                logger.debug(
                    f"⚠️ [PENALTY-GAP] {symbol} MA5이격 {ma5_gap_pct:+.1f}% → penalty +{gap_penalty:.2f}"
                )
            
            # 🔒 [POSITION-AXIS-0807] "연속 하락 페널티" 삭제 — 같은 dd를 세 번째
            #    재판정하는 중복 로직이었다(BASE 감점 + 급락Gate/단계별penalty +
            #    본 블록). 위치 축은 이제 decline_from_high 단일 SSoT로 처리한다.
            
            # 7) 과도랠리 페널티 지속 (강화)
            # - rally_time_sec은 추후 구현 필요, 현재는 항상 적용
            # === END PATCH H2 ===
            
            # === PATCH G3-②: penalty 통합 완료 ===
            # penalty는 이제 모든 위치에서 직접 penalty에 추가됨
            
            # 프레임 A: penalty 상한 (Analysis SSoT 기준 분기)
            penalty_before = penalty
            if soleus_gajami:                        # Analysis SSoT → 가자미근 확정
                penalty = min(penalty, base * 0.20)  # 최대 -20%
            else:
                penalty = min(penalty, base * 0.40)  # 최대 -40%
            if penalty < penalty_before:
                penalty_reasons.append("PENALTY_CAP")
            
            # 🦵 가자미근 penalty 미세 완화
            # - 목표: 0.50 근처 촘촘한 분포
            # - 보너스 증폭 ❌ / penalty만 완화 ⭕
            if is_soleus and soleus_gajami_ssot:
                penalty_original = penalty
                penalty = penalty * 0.67  # 33% 감소
                logger.debug(
                    f"🦵 [GAJAMI-PENALTY-REDUCE] {symbol} | "
                    f"{penalty_original:.3f} → {penalty:.3f} (×0.67)"
                )
            
            # 🦵 가자미근 컨텍스트 보너스 상한 (트리거 전용)
            # - 가자미근은 이미 base가 강하게 끌어올려짐
            # - 보너스는 최소한으로 제한 (트리거 역할만)
            # - 상한: 0.06 (일반의 60%)
            if is_soleus and soleus_gajami_ssot:
                GAJAMI_BONUS_CAP = 0.06
                if bonus > GAJAMI_BONUS_CAP:
                    logger.info(
                        f"🦵 [GAJAMI-BONUS-CAP] {symbol} | "
                        f"bonus {bonus:.3f} > {GAJAMI_BONUS_CAP} → 가자미근 상한 적용 | "
                        f"이유: base 이미 강화됨 ({base:.3f})"
                    )
                    bonus = GAJAMI_BONUS_CAP
            
            # 🎯 PATCH 2: 보너스 상한 적용 (인플레이션 방지)
            # - CONFIG 가중치 이미 반영 → 보너스는 타이밍 보정만
            # - 상한: 0.10 (최대 10% 보정)
            BONUS_CAP = 0.10
            if bonus > BONUS_CAP:
                logger.info(
                    f"⚠️ [BONUS-CAP] {symbol} bonus {bonus:.3f} > {BONUS_CAP} → 상한 적용"
                )
                bonus = BONUS_CAP
            
            # === PATCH G4: Decision Architecture v2 Final Score ===
            # penalty를 먼저 base에 적용 (신뢰도 하향)
            effective_base = max(0.0, base - penalty)
            
            # 선형 결합 복원 (20251015 방식, 확률 결합 제거)
            # 🔒 [2026-08-08] BONUS를 판정에서 제외 — 통째로 0으로 해도 진입 -2.8%(3건)·성적 +0.003%p.
            #   bonus 계산과 [SCORE_BREAKDOWN] BONUS= 필드는 관측용으로 그대로 둔다.
            final_score = min(effective_base, 1.0)
            
            # 🔒 정석: 형상 가자미근(soleus_gajami) 확정이면 품질 임계로 재차단 금지
            if (final_score < 0.35) and (not soleus_gajami):
                _diag_threshold = float(self.min_confidence)
                if regime == MarketRegime.RANGING:
                    _diag_threshold = max(
                        float(self.min_confidence),
                        float(indicators.get("ranging_calibrated_threshold", self.min_confidence) or self.min_confidence)
                    )

                # === PATCH G6: ENTRY_DIAG v2 (BONUS 단일화) ===
                logger.info(
                    f"🧾 [ENTRY_DIAG] {symbol} | "
                    f"BASE={base:.3f} "
                    f"BONUS=+{bonus:.3f} "
                    f"PENALTY=-{penalty:.3f} "
                    f"FINAL={final_score:.3f} "
                    f"THRESHOLD={_diag_threshold:.3f} "
                    f"VERDICT=TRACE(최종판사대기)"
                )
                
                # last_rejection_reason 설정 제거: 조건 충족 시 기계적으로 진입
                # 품질 임계 미달로 인한 차단 로직 제거
            
            # 🔒 신뢰도 상한 캡 (1.0 생성 방지)
            # - 목적: "완벽한 자리"도 0.95~0.97에서 멈추도록 설계
            # - 효과: 로그/리포트/매도 출력 어디에서도 1.00 미출력
            # - 철학: 점수는 비교용 지표, "1.0이면 손실 나면 안 된다"는 착시 제거
            CONFIDENCE_MAX = 0.97
            
            # === PATCH G4: final_score 사용 ===
            # 최종 신뢰도 (하한 0.0, 상한 0.97)
            final_score_capped = max(final_score, 0.0)  # 하한만 적용
            
            # v2 SSoT: SoftPenalty 계층 최종 반영
            pf = penalty_factor if isinstance(penalty_factor, (int, float)) else 1.0
            
            # 🔥 PATCH PF-LIMIT: pf 하한 도입 + 골든크로스 보호
            # 철학: "1번 손실 막겠다고 99번 기회 놓침" 방지
            # 효과: 누적 페널티 최대 -15% 제한, 골든크로스는 최대 -10%
            
            # ✅ pf 하한(최대 감점 제한)
            PF_MIN_THRESHOLD = 0.85  # 최대 -15% 감점까지만 허용
            pf = max(PF_MIN_THRESHOLD, min(pf, 1.20))
            
            # ✅ 골든크로스(추세 전환) 보호: pf 추가 완화
            gc_count = int(indicators.get("golden_count", 0) or 0)
            if gc_count >= 2:
                pf = max(pf, 0.90)  # 2중 골크 이상이면 최대 -10% 감점까지만

            penalty_factor = pf
            final_score_capped *= penalty_factor
            
            # 상한 캡핑 (1.0 방지)
            if final_score_capped >= 1.0:
                final_score_capped = CONFIDENCE_MAX
                logger.debug(f"⚠️ [CONF-CAP] {symbol} 신뢰도 1.0 이상 감지 → {CONFIDENCE_MAX}로 캡핑")
            
            # 부동소수 오차 방어
            final_score_capped = min(final_score_capped, CONFIDENCE_MAX)
            
            # =========================================================
            # ✅ OverRally-Soft factor 반영 (2026-02-17 정석화)
            # =========================================================
            # 철학: gate 제거 후 factor로 전환 (곱셈형)
            # - over_rally_penalty는 이미 계수 설계 (0.75/0.85/1.0)
            # - 곱셈형이 고정 차감보다 과열 강도 비례 반영
            # - capped 이후 적용 (MAX_CONFIDENCE 의미 보존)
            # - MA 상한 보정 제거됨 (이중 패널티 방지)
            # 
            # 본질: OverRally는 전체 위치 품질 문제
            # - MA 문제만 아님
            # - 거래량, ATR, ADX와 무관
            # - 따라서 전체 점수 비율 조정이 정석
            # =========================================================
            over_rally_val = float(indicators.get("over_rally_penalty", 1.0) or 1.0)
            
            if over_rally_val < 1.0:
                final_score_before = final_score_capped
                final_score_capped *= over_rally_val
                logger.debug(
                    f"[OverRally-Factor] {symbol} "
                    f"{final_score_before:.3f} × {over_rally_val:.2f} "
                    f"= {final_score_capped:.3f}"
                )
            # 🔒 [CONF-LOG-SSOT-0809] 후속 down-cap 전 점수 보존 (로그 전용)
            _final_before_down_cap = float(final_score_capped)
            
            # =========================================================
            
            # =========================================================
            # 🔒 PATCH: 하락 추세 신뢰도 상한 캡 (0.45)
            # =========================================================
            # 철학: 장기 MA 전부 하방이면 "확신"이라는 개념 자체를 제한
            #       conf >= 0.5 진입 불가 → "0.5 넘으면 손실 거의 없음" 원칙 유지
            # =========================================================
            CONFIDENCE_DOWNTREND_CAP = 0.45

            ma20_slope_val = float(safe_extract_scalar(indicators.get('ma20_slope', 0), 0))
            ma60_slope_val = float(safe_extract_scalar(indicators.get('ma60_slope', 0), 0))
            ma120_slope_val = float(safe_extract_scalar(indicators.get('ma120_slope', 0), 0))

            is_strong_downtrend = (
                regime == MarketRegime.TRENDING_DOWN and
                ma20_slope_val < 0 and
                ma60_slope_val < 0 and
                not is_sentinel(ma120_slope_val) and
                ma120_slope_val < 0
            )

            # 🚂 RAILROAD-GUARD: 기차길 빨래줄 = 가격이 MA120 근처에 붙어서 미끄러짐
            # - 조건: MA60<MA120(역배열) + 가격<MA120 + MA120 대비 이격 작음
            # - 본질: 반등 공간 없음 → 가자미근도 기대값 음수
            # - S자 보호: 가격이 MA120에서 크게 이격(-2.5% 이하)되면 반등 공간 있음 → 허용
            ma60_rr = float(safe_extract_scalar(indicators.get("ma60", 0), 0))
            ma120_rr = float(safe_extract_scalar(indicators.get("ma120", 0), 0))
            RR_DIST_LIMIT = -2.5
            is_railroad_down = False
            if ma60_rr > 0 and ma120_rr > 0 and ma60_rr < ma120_rr and current_price < ma120_rr:
                price_to_ma120_pct = ((current_price - ma120_rr) / ma120_rr) * 100.0
                is_railroad_down = (price_to_ma120_pct > RR_DIST_LIMIT)

            if is_railroad_down and final_score_capped > CONFIDENCE_DOWNTREND_CAP:
                logger.info(
                    f"🚂 [RAILROAD-GUARD] {symbol} 기차길 빨래줄 → CAP | "
                    f"{final_score_capped:.2f} → {CONFIDENCE_DOWNTREND_CAP} | "
                    f"price_to_ma120={price_to_ma120_pct:.2f}% (limit={RR_DIST_LIMIT}%) | "
                    f"ma60={ma60_rr:.1f} ma120={ma120_rr:.1f} price={current_price:.1f}"
                )
                final_score_capped = CONFIDENCE_DOWNTREND_CAP

            elif is_strong_downtrend and final_score_capped > CONFIDENCE_DOWNTREND_CAP:
                # ✅ soleus 면제 제거: MA20/60/120 모두 하락이면 가자미근이라도 캡 적용
                # 근거: 3개 장기선 전부 음수 = 구조적 하락, 단기 형상만으로 면제 불가
                # 효과: 0.45 캡 → GAJAMI-NO-FLOOR에서 conf < thr 차단
                logger.info(
                    f"🔒 [STRONG-DOWN-CAP] {symbol} 하락추세 신뢰도 캡 (soleus 포함) | "
                    f"{final_score_capped:.2f} → {CONFIDENCE_DOWNTREND_CAP} | "
                    f"MA기울기: 20={ma20_slope_val:.4f}, 60={ma60_slope_val:.4f}, 120={ma120_slope_val:.4f} | "
                    f"soleus={is_soleus and soleus_gajami_ssot}"
                )
                final_score_capped = CONFIDENCE_DOWNTREND_CAP
            
            _effective_conf_threshold = float(self.min_confidence)
            if regime == MarketRegime.RANGING:
                _effective_conf_threshold = max(
                    float(self.min_confidence),
                    float(indicators.get("ranging_calibrated_threshold", self.min_confidence) or self.min_confidence)
                )

            indicators["effective_conf_threshold"] = float(_effective_conf_threshold)
            indicators["confidence"] = final_score_capped
            
            # --- Contribution Trace (DEBUG only, no logic impact) ---
            try:
                # bonus_reasons / penalty_reasons / penalty_details는 이미 상단에서 사용 중
                if logger.isEnabledFor(10):  # logging.DEBUG == 10, import logging 불필요
                    bonus_sum = float(bonus) if isinstance(bonus, (int, float)) else 0.0
                    penalty_sum = float(penalty) if isinstance(penalty, (int, float)) else 0.0
                    pf_dbg = float(penalty_factor) if isinstance(penalty_factor, (int, float)) else 1.0

                    logger.info(
                        f"🧾 [CONF-TRACE] {symbol} | "
                        f"base={float(base):.3f} "
                        f"bonus={bonus_sum:.3f} "
                        f"penalty={penalty_sum:.3f} "
                        f"pf={pf_dbg:.3f} "
                        f"raw={float(base)+bonus_sum-penalty_sum:.3f} "
                        f"final={float(final_score_capped):.3f} "
                        f"thr={_effective_conf_threshold:.3f} "
                        f"pass={int(float(final_score_capped) >= _effective_conf_threshold)} "
                        f"over_rally={float(indicators.get('over_rally_penalty', 1.0) or 1.0):.2f} "
                        f"soleus={int(bool(soleus_gajami))} "
                        f"dur={indicators.get('dur_5x10_bars','?')} "
                    )
            except Exception:
                pass
            # --- End Contribution Trace ---
            
            logger.info(
                f"[CONF] {symbol} | "
                f"base={base:.3f} "
                f"penalty_abs={penalty:.3f} "
                f"effective={effective_base:.3f} "
                f"bonus_obs={bonus:.3f}({','.join(bonus_reasons[:3]) if bonus_reasons else 'none'};판정제외) "
                f"pf={penalty_factor:.3f} "
                f"over_rally={over_rally_val:.2f} "
                f"pre_cap={_final_before_down_cap:.3f} "
                f"final={final_score_capped:.3f} "
                f"thr={_effective_conf_threshold:.3f}"
            )

            # =============================================================================
            # 8. Knee Mode: 신뢰도 기준 완화 적용
            # =============================================================================
            # effective_threshold = confidence_threshold  # 기본값 유지
            
            # 🆕 단타 진입 보호: EEE 컷 완화 (2025-12-13)
            # 목적: 페널티 Skip은 했지만 컷은 그대로면 여전히 탈락 가능
            # 해결: 단타 보호 상태에서는 컷 -0.05 완화
            # 철학: 진입 유형별 기준 분리 (완화 ❌ / 분리 ⭕)
            # ========================================
            # 9. 리스크 체크
            # ========================================
            
            # 연속 손실 체크 (정보 제공만, Confidence 패널티 없음)
            # 🆕 철학: 개별 종목 신호는 독립적, 리스크는 포지션 크기로 관리
            if risk_metrics.get('consecutive_losses', 0) >= 3:
                logger.info(
                    f"ℹ️ [신호분석] {symbol} 연속 손실 {risk_metrics.get('consecutive_losses', 0)}회 "
                    f"(Confidence 패널티 없음, 포지션 크기로 리스크 관리)"
                )
            
            # 일일 손실 체크 (정보 제공만, Confidence 패널티 없음)
            # 🆕 철학: 다른 종목 손실이 현재 종목 진입 기회를 박탈하면 안됨
            if risk_metrics.get('daily_loss_pct', 0) < -0.01:  # -1% 이상 손실
                daily_loss_pct = risk_metrics.get('daily_loss_pct')
                logger.info(
                    f"ℹ️ [신호분석] {symbol} 일일 손실 {fmt(daily_loss_pct*100 if daily_loss_pct is not None else None, '.2f')}% "
                    f"(Confidence 패널티 없음, 포지션 크기로 리스크 관리)"
                )

            # 🔒 MA BASIC GATE - 가자미근 기본 조건
            # 1) MA5 > MA10 (GC)
            # 2) MA5 slope > 0
            # 3) MA10 slope >= 0 OR MA20 slope >= 0
            # 4) 장기선 생존: MA60/MA120 중 하나 이상 상향,
            #    단 강하향 선의 반대편 장기선도 죽어 있으면 차단

            ma5_slope_val = safe_extract_scalar(indicators.get('ma5_slope', SENTINEL_FLOAT), SENTINEL_FLOAT)
            ma10_slope_val = safe_extract_scalar(indicators.get('ma10_slope', SENTINEL_FLOAT), SENTINEL_FLOAT)
            ma20_slope_val = safe_extract_scalar(indicators.get('ma20_slope', SENTINEL_FLOAT), SENTINEL_FLOAT)
            ma60_slope_val = safe_extract_scalar(indicators.get('ma60_slope', SENTINEL_FLOAT), SENTINEL_FLOAT)
            ma120_slope_val = safe_extract_scalar(indicators.get('ma120_slope', SENTINEL_FLOAT), SENTINEL_FLOAT)

            ma5_val = safe_extract_scalar(indicators.get('sma_5', SENTINEL_FLOAT), SENTINEL_FLOAT)
            ma10_val = safe_extract_scalar(indicators.get('sma_10', SENTINEL_FLOAT), SENTINEL_FLOAT)

            # MA 기본 조건 게이트
            # MA5>MA10 AND MA5고개듦 AND (MA10상향 OR MA20상향)
            # AND 장기선 생존: MA60 상향 OR MA120 near-flat 이상,
            # 단 강하향 선의 반대편 장기선도 죽어 있으면 차단
            _LONG_STRONG_DN_PCT = -float(SoleusGajamiConstants.MA_LONG_SLOPE_STEEP_DEAD) * 100.0
            _LONG_ALIVE_FLOOR = float(
                SoleusGajamiConstants.MA_LONG_SLOPE_ALIVE_FLOOR
            ) * 100.0

            # 🔒 [GC-CLOSED-STRICT-SSOT-0822-R2] MA-HALT도 canonical closed GC만 소비.
            _halt_gc_ok = bool(
                int(indicators.get("closed_gc_5x10_relation", 0) or 0) == 1
            )
            _halt_s5_ok = SoleusGajamiConstants.ma5_alive_ratio(
                ma5_slope_val / 100.0
            )
            _halt_lc = (
                ((data_package.ma_data or {}).get("gajami_meta") or {})
                .get("gajami_lifecycle")
                or {}
            )
            try:
                _halt_lc_state = str(_halt_lc.get("state", "") or "")
                _halt_lc_dur_src = _halt_lc.get("dur", None)
                _halt_lc_dur = int(_halt_lc_dur_src) if _halt_lc_dur_src is not None else 999
            except Exception:
                _halt_lc_state = ""
                _halt_lc_dur = 999

            _halt_birth_valid = bool(
                _halt_lc_state == "VALID"
                and _halt_lc_dur <= SoleusGajamiConstants.NEWBORN_MAX_DUR
            )

            # MA10/MA20 중기선은 fresh BIRTH에서 후행 지연을 허용한다.
            # 기존 MA-HALT 계약 복원 — LONG 패치와 독립된 기존 판정이다.
            _halt_mid_ok = bool(
                ma10_slope_val >= 0.0
                or ma20_slope_val >= 0.0
                or _halt_birth_valid
            )

            # 🔒 [LONG-MA-BIRTH-ROLE-0826]
            # Analysis SSoT와 동일 helper를 한 번만 호출한다.
            _halt_long_ok, _halt_long_fail = _long_ma_alive_verdict(
                ma60_slope_val / 100.0,
                ma120_slope_val / 100.0,
                regime,
                ma5_slope_val / 100.0,
                _halt_lc_dur,
            )
            # fresh BIRTH에서 MA120만 strong-down이고
            # 현재 파동에 가까운 MA60은 strong-down이 아닐 때,
            # 장기 기억의 hard-veto 격하가 실제 작동했는지 기록한다.
            _halt_long_birth_relief = bool(
                _halt_long_ok
                and _halt_birth_valid
                and regime in (
                    MarketRegime.TRENDING_DOWN,
                    MarketRegime.RANGING,
                )
                and (ma5_slope_val / 100.0)
                >= SoleusGajamiConstants.MA5_SLOPE_CORE_MIN
                and ma60_slope_val > _LONG_STRONG_DN_PCT
                and ma120_slope_val <= _LONG_STRONG_DN_PCT
            )
            if _halt_long_birth_relief:
                logger.info(
                    f"🟢 [MA-HALT-LONG-BIRTH-RELIEF] {symbol} | "
                    f"fresh BIRTH 장기기억 격하 | "
                    f"s5={ma5_slope_val:+.4f}% "
                    f"s60={ma60_slope_val:+.4f}% "
                    f"s120={ma120_slope_val:+.4f}% "
                    f"dur={_halt_lc_dur} "
                    f"birth_valid={int(_halt_birth_valid)}"
                )

            _ma60_alive = bool(ma60_slope_val > 0.0)
            _ma120_alive = bool(ma120_slope_val > 0.0)
            _ma120_alive_db = bool(
                ma120_slope_val >= -_LONG_ALIVE_FLOOR
            )
            if (
                _halt_long_ok
                and not _ma120_alive
                and _ma120_alive_db
                and not _ma60_alive
            ):
                logger.info(
                    f"🟢 [MA-HALT-LONG-DEADBAND] {symbol} "
                    f"MA120 near-flat alive 인정 | "
                    f"s60={ma60_slope_val:+.4f}% "
                    f"s120={ma120_slope_val:+.4f}% "
                    f"floor=-{_LONG_ALIVE_FLOOR:.2f}% "
                    f"strong_dn={_LONG_STRONG_DN_PCT:.3f}%"
                )

            if not (
                _halt_gc_ok
                and _halt_s5_ok
                and _halt_mid_ok
                and _halt_long_ok
            ):
                if not _halt_gc_ok:
                    _halt_reason = "MA_HALT_DC"
                elif not _halt_s5_ok:
                    _halt_reason = "MA_HALT_S5"
                elif not _halt_mid_ok:
                    _halt_reason = "MA_HALT_MID"
                else:
                    _halt_reason = "MA_HALT_LONG"

                self.last_rejection_reason = _halt_reason
                logger.info(
                    f"🚫 [MA-HALT-GATE] {symbol} | "
                    f"closed_gc={int(_halt_gc_ok)} "
                    f"rt_gc={int(indicators.get('entry_ma5_gt_ma10', 0) or 0)} "
                    f"s5={fmt(ma5_slope_val,'.4f')}% "
                    f"floor={-SoleusGajamiConstants.MA5_SLOPE_ALIVE_DIP_RATIO*100.0:+.4f}% "
                    f"s10={fmt(ma10_slope_val,'.4f')}% "
                    f"s20={fmt(ma20_slope_val,'.4f')}% "
                    f"s60={fmt(ma60_slope_val,'.4f')}% "
                    f"s120={fmt(ma120_slope_val,'.4f')}% "
                    f"strong_dn={_LONG_STRONG_DN_PCT:.3f}% "
                    f"long_fail={_halt_long_fail or 'NONE'} "
                    f"dur={_halt_lc_dur}(raw={dur_5x10}) "
                    f"birth_valid={int(_halt_birth_valid)} "
                    f"| cause={_halt_reason}"
                )
                # 🔒 [J1-GATE-JUDGE-NEUTRAL-0826]
                # MA-HALT는 J1/가자미 MA 문맥의 법률이다.
                # J2/J3/J4 중 독립 판사가 PASS했다면 그 판결까지 전역 BLOCK하지 않는다.
                # J1의 MA 판정 자체는 변경하지 않고 전역 return 권한만 회수한다.
                _j1gate_judge_relief = bool(
                    int(indicators.get("judge2_osc_pass", 0) or 0)
                    or int(indicators.get("judge3_cycle_pass", 0) or 0)
                    or int(indicators.get("judge4_reversal_pass", 0) or 0)
                )
                if _j1gate_judge_relief:
                    logger.info(
                        f"🟢 [MA-HALT-JUDGE-NEUTRAL] {symbol} "
                        f"cause={_halt_reason} → "
                        f"J2/J3/J4 찬성으로 전역차단 회수 | "
                        f"J2={int(indicators.get('judge2_osc_pass', 0) or 0)} "
                        f"J3={int(indicators.get('judge3_cycle_pass', 0) or 0)} "
                        f"J4={int(indicators.get('judge4_reversal_pass', 0) or 0)} | "
                        f"J1 자격은 불변"
                    )
                if allow_hardcut and not _j1gate_judge_relief:
                    return None
            
            # 🔒 [PINPOINT-VOLUME-REJUDGE-REMOVE-0823]
            # 거래량 부족은 공통 수급 Soft 법에서 이미 평가하므로 PinPoint에서 재판정하지 않는다.
            
            # ========================================
            # 10. 최종 신호 생성
            # ========================================                        
            
            # 🔒 최종 SoleusOnly 게이트 (단일 차단 지점)
            # ⚠️ 긴급 비활성화 (2025-12-21)
            # 현상: soleus_zone 조건 미충족으로 전체 진입 차단 (34,349건)
            # 원인: volume/buying_ratio 조건이 과도하게 엄격
            # 조치: 게이트 제거, 기존 필터(PinPoint/STEP3) 유지
            # 복원: soleus_zone 로직 개선 후 재활성화 예정
            #     self.last_rejection_reason = "SoleusZone OFF (final)"
            #     logger.info(
            #         f"🚫 [SoleusOnly-FINAL] {symbol} 가자미근 아님 → 진입 차단 | 체제={regime.value}"
            #     )
            
            # 🆕 완화된 체크: soleus_zone 경고만 (차단 없음)
            if not soleus_zone:
                logger.debug(
                    f"⚠️ [SoleusZone-Warning] {symbol} 가자미근 조건 미충족 | "
                    f"vol={safe_extract_scalar(indicators.get('volume_ratio', 0)):.2f}x | "
                    f"buy={safe_extract_scalar(indicators.get('buying_ratio', 0)):.2f} | "
                    f"체제={regime.value}"
                )
            
            # 🔒 threshold 최종 산출 (단 1회)
            # 🦴 Priority 1: KneeMode 신뢰도 임계값 특례 (2025-12-18)
            #  - PRE-GAJAMI (초입): 0.50 → 0.25
            #  - STD-GAJAMI (확정): 0.50 → 0.30
            #  - 효과: AERGO 케이스 해결 (신뢰도 0.316 진입 가능)

            # 가자미근 상태 확인
            gajami_entry = indicators.get('gajami_entry', False)

            # === PATCH G1/G7: Final Judge 임계값 SSoT ===
            # 기본 임계값은 CONFIG(self.min_confidence).
            # threshold_factor 하향 완화는 bonus로만 이동.
            # 단, RANGING은 RangingHybrid가 산출한 상향 calibrated threshold만 최종 판사에 반영한다.

            _ranging_thr_msg = ""
            if regime == MarketRegime.RANGING:
                _ranging_calibrated_dbg = float(
                    indicators.get("ranging_calibrated_threshold", 0.0) or 0.0
                )
                _ranging_thr_msg = f" | ranging_calibrated={_ranging_calibrated_dbg:.3f}"

            logger.info(
                f"🎯 [Threshold-FINAL] {symbol} | "
                f"base={base_threshold:.2f} × factor={threshold_factor:.3f} = "
                f"effective={_effective_conf_threshold:.2f}"
                f"{_ranging_thr_msg}"
            )
            
            # ============================================================
            # 🦵 가자미근(Soleus/Knee) 초입 전용 신뢰도 문턱 완화
            # ============================================================
            # 🔧 최종 신뢰도 체크 - 완화된 임계값 사용
            # (1606줄에서 이미 완화된 confidence_threshold로 체크했으므로
            #  여기서 min_confidence를 다시 사용하면 중복 차단 발생!)
            
            # 🔍 Dead Cat Bounce 차단 (가자미근 + 골든 + 기울기 예외)
            #  - TRENDING_UP인데 MA 신호 없음 = 반등 함정
            #  - 단, 가자미근 + 골든크로스 + 우상향 기울기이면 차단 금지
            #  - 효과: KRW-MON 00:39 같은 진짜 상승 추세 진입 허용
            ma_signal = safe_extract_scalar(components.get('ma_signal', 0.0), 0.0)
            
            # ✅ 기존 플래그 + 🆕 후보 유지(hold)까지 포함
            gajami_entry = bool(indicators.get('gajami_entry', False)) or bool(indicators.get("candidate_persistent", False))
            
            # ✅ 가자미근 확정(또는 후보 유지)면 TU-NoMA 차단 금지 (SSoT 존중)
            if (
                regime == MarketRegime.TRENDING_UP
                and ma_signal < 0.10
                and not gajami_entry
                and not _j234_hardcut_pass
            ):
                self.last_rejection_reason = f"{BLOCK_REASON_TU_NO_MA}(ma={fmt(ma_signal, '.2f')})"
                logger.warning(
                    f"🚫 [TU-NoMA] {symbol} TRENDING_UP이나 MA 신호 없음 | "
                    f"ma_signal={fmt(ma_signal, '.2f')} < 0.10 (Dead Cat Bounce 의심)"
                )
                return None
            
            # 포지션 배수 계산 (클램프: 0.6 ~ 1.4)
            position_confidence_mult = max(
                0.6,
                min(1.4, 0.6 + final_score_capped * 0.8)
            )

            logger.debug(
                f"[PositionSizing] {symbol} confidence={final_score_capped:.2f} → "
                f"mult={position_confidence_mult:.2f}"
            )

            # final_signal 신뢰도 동기화
            try:
                if final_signal is not None:
                    final_signal.confidence = final_score_capped
                    if isinstance(getattr(final_signal, "indicators", None), dict):
                        final_signal.indicators["entry_confidence"] = final_score_capped
                        final_signal.indicators["position_confidence_mult"] = position_confidence_mult
            except Exception as _conf_sync_e:
                logger.debug(f"[CONF-SYNC-ERR] {symbol} {_conf_sync_e}")
            
            # 🔍 관측용: 진입 후보 평가 로그 (2025-12-21)
            logger.info(
                f"🧪 [ENTRY_CANDIDATE] {symbol} | "
                f"score={final_score_capped:.3f}/{_effective_conf_threshold:.3f} | "
                f"regime={regime.value} | "
                f"soleus={soleus_zone} | "
                f"ma_sig={safe_extract_scalar(components.get('ma_signal', 0.0), 0.0):.2f} | "
                f"vol_sig={safe_extract_scalar(components.get('volume_signal', 0.0), 0.0):.2f}"
            )
            
                        # 🔍 [WEIGHT-MINING] 확정봉 단위 5개 신호 실효 입력값 적재
            # - 동일 종목·확정봉 반복 스캔은 1회만 기록
            # - 관측 실패가 판정·점수·포지션에 영향을 주지 않도록 격리
            _score_bar = indicators.get("indicator_shadow_bar")
            _score_px_raw = safe_extract_scalar(
                indicators.get("indicator_shadow_px"),
                SENTINEL_FLOAT,
            )
            if (
                isinstance(_score_bar, str)
                and bool(_score_bar.strip())
                and _score_px_raw > 0.0
            ):
                _score_ctx = self.recent_context.setdefault(symbol, {})
                if _score_ctx.get("_score_breakdown_bar") != _score_bar:
                    try:
                        _score_tf = (
                            self.timeframe.value
                            if hasattr(self.timeframe, "value")
                            else str(self.timeframe)
                        )
                        _score_px = _score_px_raw
                        _score_base = safe_extract_scalar(base, 0.0)
                        _score_bonus = safe_extract_scalar(bonus, 0.0)
                        _score_penalty = safe_extract_scalar(penalty, 0.0)
                        _score_pf = safe_extract_scalar(penalty_factor, 1.0)
                        _score_total = safe_extract_scalar(final_score_capped, 0.0)
                        _score_thr = safe_extract_scalar(
                            _effective_conf_threshold,
                            safe_extract_scalar(self.min_confidence, 0.0),
                        )

                        logger.info(
                            f"📉 [SCORE_BREAKDOWN] {symbol} | "
                            f"tf={_score_tf} "
                            f"bar={_score_bar} "
                            f"px={_score_px:.10g} "
                            f"regime={regime.value if hasattr(regime, 'value') else regime} "
                            f"dur={indicators.get('dur_5x10_bars', dur_5x10)} "
                            f"soleus={int(bool(soleus_zone))} | "
                            f"MA={safe_extract_scalar(components.get('ma_signal', 0.0), 0.0):.6f} "
                            f"MA_RAW={safe_extract_scalar((data_package.ma_data or {}).get('signal', 0.0), 0.0):.6f} "
                            f"VOL={safe_extract_scalar(components.get('volume_signal', 0.0), 0.0):.6f} "
                            f"ADX={safe_extract_scalar(components.get('adx_signal', 0.0), 0.0):.6f} "
                            f"ATR={safe_extract_scalar(components.get('atr_signal', 0.0), 0.0):.6f} "
                            f"ATR_RAW={safe_extract_scalar(indicators.get('atr_signal', 0.0), 0.0):.6f} "
                            f"BWSIG={safe_extract_scalar(components.get('candle_entry_signal', 0.0), 0.0):.6f} "
                            f"BW_RAW={safe_extract_scalar(indicators.get('bw_signal', 0.0), 0.0):.6f} "
                            f"BW_PCT60={safe_extract_scalar(indicators.get('cycle_bw_percentile_60', SENTINEL_FLOAT), SENTINEL_FLOAT):.6f} "
                            f"CDLBIRTH={int(bool(indicators.get('gajami_birth_state_ok', False)))} | "
                            # 🔒 [2026-08-08] RT 판정 SSoT: rt_valid + rt_volume_delta만 유지.
                            f"RTVALID={int(bool(indicators.get('rt_valid', False)))} "
                            f"RTVD={safe_extract_scalar(indicators.get('rt_volume_delta', -1.0), -1.0):.6f} | "
                            f"BASE={_score_base:.6f} "
                            f"BONUS={_score_bonus:.6f} "
                            f"PENALTY={_score_penalty:.6f} "
                            f"PF={_score_pf:.6f} "
                            f"TOTAL={_score_total:.6f} "
                            f"THR={_score_thr:.6f} "
                            f"CONF_PASS={int(_score_total >= _score_thr)}"
                        )
                        _score_ctx["_score_breakdown_bar"] = _score_bar
                    except Exception as _score_log_e:
                        logger.debug(
                            f"[SCORE_BREAKDOWN-ERR] {symbol} {_score_log_e}"
                        )
            
            # 기존 차단 로직을 로그 전용으로 변경 (디버깅용)
            # ============================================================
            # 🔒 FINAL ENTRY THRESHOLD (3중 재판관)
            # ============================================================
            # 1. min_confidence (CONFIG 절대 기준)
            # 2. effective_threshold (체제/리스크 반영)
            # 3. relaxed_threshold (가자미근 특례, 있을 경우)
            #
            # → 어떤 경로도 min_confidence를 우회할 수 없음
            # ============================================================
            
            # 🔒 RANGING 가자미근 보호: TRENDING 기준 강제 적용 금지
            # - RANGING 체제에서 가자미근일 때는 effective_threshold만 사용
            # - min_confidence(0.50)를 강제하면 횡보 가자미근 진입 불가
            # 🔥 가자미근(Soleus/Knee) 초입이면 체제 무관 threshold 완화
            # - RANGING뿐만 아니라 TRENDING_UP/DOWN 가자미근도 동일하게 처리
            # - 가자미근 = Soleus Zone + Knee Mode 모두 활성화된 상태
            # SSoT: SOLEUS-BONUS 제거 (통합 bonus 시스템에서 처리)
            #     indicators.get("soleus_zone", False)
            #     and indicators.get("gajami_zone", False)
            # ):
            #     bonus = 0.05
            #     original_conf = final_score_capped
            #     final_score_capped = min(add(final_score_capped, bonus), 1.0)
            #     logger.info(...)
            #     logger.debug(...)
            pass

            # ============================================================
            # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
            # 🔒 ENTRY VERDICT — 계층별 차단 (단일 사유, 조기 반환)
            # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
            # 계층 순서: 가자미근 → EEE → 최종 신뢰도
            # 철학: 형상(가자미근) 우선 → 위치 품질(EEE) → 종합 평가(신뢰도)
            # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

            # POSITION_GATE_RISE_ATR / RELATIVE_OVERHEAT 제거
            # 사유: 후행 ATR + 누적 GC — 추세 판단 없이 가자미근 선차단

            # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
            # 📍 매크로 위치 게이트 (retrace_from_high 기반)
            # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
            # 목적: "이번 파동 내 위치(rise_pct)"가 아닌
            #       "최근 20봉 고점 대비 얼마나 내려와 있는가" 판단
            # 구조: Analysis.gajami_meta['retrace_from_high'] SSoT 사용
            # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
            # ⚠️  DEFAULT = 1.0 → 완전 비활성 (손실 로그 실측 후 결정)
            #     조정 시 이 값 하나만 변경
            #     예: 0.03 → 최근 20봉 고점 대비 3% 미만 이격 시 차단
            # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
            # POSITION_GATE_MACRO_HIGH 제거
            # 사유: retrace_from_high 항상 1.0 미만 → 전건 차단, 위치 기반 동일 계열

            # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
            # 🆕 우선 재체크 등록 트리거 (스캔 공백 보완)
            # 조건: '형성 중' 신호가 보이지만 아직 최종 통과 전
            # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
            try:
                golden_count = ma_analysis.get('golden_count', 0) if ma_analysis else 0
                ma5_slope_ratio = float(ma_analysis.get('slope_ma5', 0.0) or 0.0) if ma_analysis else 0.0
                vol_ratio = float(indicators.get('volume_ratio', 0.0) or 0.0) if indicators else 0.0

                partial_signal = (
                    golden_count >= 1
                    and ma5_slope_ratio >= SoleusGajamiConstants.MA5_SLOPE_CORE_MIN
                    and vol_ratio >= 0.3         # 극저유동성 제외
                    and not soleus_gajami        # 아직 완성은 아님
                )

                if partial_signal:
                    # 최대 2회만 우선 재체크
                    if hasattr(self, 'bot') and hasattr(self.bot, '_priority_recheck'):
                        self.bot._priority_recheck[symbol] = max(
                            self.bot._priority_recheck.get(symbol, 0),
                            2
                        )
                        logger.debug(f"[우선재체크] {symbol} 부분 가자미근 형성 중 (2회 등록)")
            except Exception:
                pass

            # 1️⃣ 가자미근 판사 (최우선 - 형상 체크)
            _true_gc_birth_s = bool(
                int(indicators.get("gajami_true_gc_birth", 0) or 0)
            )
            _prev_already_gc_s = bool(
                int(indicators.get("gajami_prev_already_gc", 0) or 0)
            )
            _true_gc_shape_birth_lane = bool(
                _true_gc_birth_s
                and bool(indicators.get("shape_v2_gajami_ok", False))
                and dur_5x10 <= SoleusGajamiConstants.NEWBORN_MAX_DUR
                and bool(
                    int(indicators.get("gajami_trend_alive", 0) or 0)
                )
            )
            # 🔒 [SOLEUS-SSoT-0814] Strategy는 T5 최종 soleus 판정을 그대로 소비한다.
            # legacy MA/TLC 관측값은 T5에서 판정권이 없고,
            # 독립 FAKE_TOP/TOP_FLAT 계약만 T5 soleus에 반영된다.
            # Strategy는 TLC/MA context를 여기서 다시 재판정하지 않는다.
            _soleus_ok = bool(soleus_gajami)

            if _true_gc_shape_birth_lane and not soleus_gajami:
                logger.info(
                    f"🌱 [TRUE-GC-SHAPE-BIRTH-LANE] {symbol} | "
                    f"true_gc=1 shape=1 trend_alive=1 "
                    f"dur={dur_5x10} "
                    f"→ 관측만, soleus 복구 없음"
                )

            if not _soleus_ok:
                # 🔒 [J1-FAKETOP-LANE-0825]
                # soleus=False는 J1 부재이지 절대부정이 아니다.
                # FAKE_TOP_BIRTH 판정권은 Analysis/J1 내부에 유지하고,
                # Strategy가 J2/J3/J4를 선행 전역 차단하지 않는다.
                logger.info(
                    f"📋 [SOLEUS-DEFER] {symbol} soleus=0 "
                    f"→ J2/J3/J4 최종 긍정 판사까지 진행"
                )

            # 1.5️⃣ TLC 반짝회복 차단 (추세 사망 후 반짝 회복 가짜 가자미근)
            _ma5_prev1_pct = float(safe_extract_scalar(indicators.get("ma5_slope_prev1", 0.0), 0.0))
            _snap_birth_state_ok = bool(indicators.get("gajami_birth_state_ok", False))

            # dur=2 눌림 후 재상승 BIRTH는 정상 생애주기.
            # SNAP 차단은 BIRTH_STATE가 아닌 가짜 반짝회복에만 적용.
            # 🔒 [JUDGE-OWNER-LANE-SPLIT-0828] 가짜 반짝회복 차단은 J1 전용.
            if (soleus_gajami and dur_5x10 == 2 and _ma5_prev1_pct < 0
                    and not _snap_birth_state_ok
                    and not _j234_hardcut_pass):
                self.last_rejection_reason = "TLC_SNAP_RECOVERY"
                logger.info(
                    f"🚫 [TLC-SNAP-BLOCK] {symbol} 추세 사망 후 반짝 회복 차단 | "
                    f"dur={dur_5x10} ma5_prev1={_ma5_prev1_pct:.4f}% "
                    f"birth_state={int(_snap_birth_state_ok)}"
                )
                return None

            # 1.7️⃣ 탄생봉 중기선 동반사망
                        
            # 재진입 차단 (TLC 기반)
            _ma10c6 = float(safe_extract_scalar(indicators.get("ma10_slope_exit_closed", SENTINEL_FLOAT), SENTINEL_FLOAT))
            _tlc_ma10_st = float(indicators.get("ma10_slope", 0.0) or 0.0) if is_sentinel(_ma10c6) else _ma10c6
            _tlc_ma20_st = float(indicators.get("ma20_slope", 0.0) or 0.0)
            # 🔒 [FIXED-BOX-AUTHORITY-PURGE-0828]
            # fixed trimmed-box state는 TLC 판단/문맥 축에서 제거한다.
            # 🔒 [TLC step2 PhaseA] Analysis export 소비 — MA series 직접 .iloc 제거(계약 복원)
            _gap_5_10 = float(indicators.get("gajami_gap_5_10_closed", 1.0))
            _reborn_birth_state_ok = bool(indicators.get("gajami_birth_state_ok", False))

            if (soleus_gajami and dur_5x10 <= SoleusGajamiConstants.NEWBORN_MAX_DUR
                    and not _reborn_birth_state_ok
                    and regime != MarketRegime.TRENDING_UP
                    and _tlc_ma10_st <= 0.0
                    and _tlc_ma20_st < 0.0
                    # 🔒 [JUDGE-OWNER-LANE-SPLIT-0828] J1 newborn 세부법률.
                    and not _j234_hardcut_pass):
                self.last_rejection_reason = "NEWBORN_MID_MA_DEAD_BLOCK"
                logger.info(
                    f"🚫 [NEWBORN-MID-MA-DEAD] {symbol} 탄생봉 중기선 동반사망 차단 | "
                    f"gap={_gap_5_10*100:.4f}% "
                    f"ma10={_tlc_ma10_st:.4f} ma20={_tlc_ma20_st:.4f} "
                    f"dur={dur_5x10} birth_state={int(_reborn_birth_state_ok)}"
                )
                logger.info(
                    f"📋 [ENTRY-SUMMARY] {symbol} | "
                    f"regime={regime.value if regime else '?'} "
                    f"dur={((((data_package.ma_data or {}).get('gajami_meta') or {}).get('gajami_lifecycle') or {}).get('dur', dur_5x10))} soleus={soleus_gajami} "
                    f"gajami={bool(indicators.get('shape_v2_gajami_ok',False))} "
                    f"score=NA block=NEWBORN_MID_MA_DEAD_BLOCK birth_state={int(_reborn_birth_state_ok)}"
                )
                return None

            # 1.8️⃣ GC BIRTH 과열 차단 (BIRTH=dur<=NEWBORN_MAX_DUR)
            # 🔒 [TLC-RISE-ATR-BIRTH-SCOPE-0823]
            # 생애법 BIRTH(dur<=2)와 GC freshness(dur<=4)를 혼용하지 않는다.
            # XLM/PIEVERSE의 dur=2 근거는 유지하고, dur=3~4는 이 BIRTH 법의 대상에서 제외한다.
            _tlc_birth_state_ok = bool(indicators.get("gajami_birth_state_ok", False))
            _glc = (((data_package.ma_data or {}).get("gajami_meta") or {}).get("gajami_lifecycle") or {})
            try:
                _tlc_effective_dur = int(_glc.get("dur")) if _glc.get("dur", None) is not None else int(dur_5x10)
            except Exception:
                _tlc_effective_dur = 999
            if (
                soleus_gajami
                and _tlc_effective_dur <= SoleusGajamiConstants.NEWBORN_MAX_DUR
                # 🔒 [JUDGE-OWNER-LANE-SPLIT-0828] J1 newborn 세부법률.
                # 임계 1.0x/1.5x 는 건드리지 않는다.
                and not _j234_hardcut_pass
            ):
                _rise_val = float(_glc.get("rise_pct", 0) or 0)
                _atr_val  = float(safe_extract_scalar(indicators.get("atr_pct", 0), 0))
                # _rise_val=ratio(e.g.0.00617), _atr_val=percent(e.g.0.13) → atr/100으로 단위 정합
                _rise_atr = (_rise_val / (_atr_val / 100.0)) if _atr_val > 0 else 0.0
                _rise_atr_limit = (
                    1.5
                    if regime in (
                        MarketRegime.TRENDING_UP,
                        MarketRegime.BREAKOUT,
                        MarketRegime.BULL_MARKET,
                    )
                    else 1.0
                )
                
                # 🔒 [TLC-RISE-ATR-BOOL-SSOT-0816]
                # Rise/ATR 임계 도달 시 true_gc는 관측 경로를 구분하는 SSoT다.
                # true_gc=0은 ★468-R1부터 OBSERVE, true_gc=1은 BIRTH-EXEMPT 로그로 기록한다.
                # 기존 birth_state/J2/J3 조합은 이 분기의 판정식에 영향이 없는 死분기이며,
                # J2/J3/J4 판결권은 바깥 _j234_hardcut_pass에서 이미 분리한다.
                if (
                    _rise_atr >= _rise_atr_limit
                    and not _true_gc_birth_s
                ):
                    # 🔒 [TLC-RISE-ATR-DEMOTE-0831-R1] ★468-R1
                    # J1 newborn의 Rise/ATR 과열값은 계속 계측하되,
                    # 이 축 단독의 Hard Block 권한은 회수한다.
                    # `and False` 방식은 if 내부 로그까지 죽이므로 사용하지 않는다.
                    # 하류 EEE/confidence/PREM/BOX/공통 물리 게이트는 그대로 평가한다.
                    logger.info(
                        f"👁️ [TLC-RISE-ATR] {symbol} GC 생애초기 과열 관측 | "
                        f"decision=OBSERVE "
                        f"rise={_rise_val*100:.3f}% atr={_atr_val:.3f}% "
                        f"ratio={_rise_atr:.2f}x limit={_rise_atr_limit:.2f}x "
                        f"birth_state={int(_tlc_birth_state_ok)} "
                        f"true_gc={int(_true_gc_birth_s)} "
                        f"prev_gc={int(_prev_already_gc_s)} "
                        f"dur={_tlc_effective_dur}(raw={dur_5x10})"
                    )
                
                # 🔒 차단과 동일한 임계에서 실제 true GC 면제만 기록한다.
                if (
                    _rise_atr >= _rise_atr_limit
                    and _true_gc_birth_s
                ):
                    logger.info(
                        f"✅ [TLC-RISE-ATR-BIRTH-EXEMPT] {symbol} 정답 BIRTH 과열차단 면제 | "
                        f"rise={_rise_val*100:.3f}% atr={_atr_val:.3f}% "
                        f"ratio={_rise_atr:.2f}x limit={_rise_atr_limit:.2f}x "
                        f"birth_state={int(_tlc_birth_state_ok)} "
                        f"true_gc={int(_true_gc_birth_s)} "
                        f"prev_gc={int(_prev_already_gc_s)} "
                        f"dur={_tlc_effective_dur}(raw={dur_5x10})"
                    )

            # 2️⃣ EEE 판사 (2순위 - 위치 품질 체크)
            # 가자미근 컨텍스트가 아닐 때만 EEE 체크
            is_gajami_ctx = soleus_gajami
            try:
                eee_conf = float(indicators.get("eee_confidence", -1.0) or -1.0)
            except Exception:
                eee_conf = -1.0

            MIN_EEE_FOR_NORMAL = 0.30
            if eee_conf >= 0.0 and (not is_gajami_ctx):
                # 🔧 [STEP B] EEE judge-lane 우회 제거.
                if eee_conf < MIN_EEE_FOR_NORMAL:
                    # 🔒 [EEE-J234-LANE-NEUTRAL-0830-R1-ROLLBACK-0831]
                    # ★458-R1 철회.
                    # EEE<0.30은 J2/J3/J4 판결과 별개인 최종 품질 하한으로 복원한다.
                    # CAP은 EEE-JUDGE-NEUTRAL 직후 J2 단독 진입→손실의 직접 인과가 확인됐고,
                    # NEAR도 동일 완화 경로에서 J3 단독 진입→손실이 재현됐다.
                    # 임계 0.30 및 is_gajami_ctx 면제 계약은 변경하지 않는다.
                    logger.warning(
                        f"🚫 [EEE-BLOCK] {symbol} 위치 품질 부족 | "
                        f"EEE={eee_conf:.3f} < {MIN_EEE_FOR_NORMAL:.2f}"
                    )
                    self._block(
                        BLOCK_REASON_EEE_LOW,
                        f"{eee_conf:.3f}<{MIN_EEE_FOR_NORMAL:.2f}"
                    )
                    return None

            # 3️⃣ 최종 신뢰도 판사 (3순위 - 종합 평가)
            _regime_early_gajami = (
                bool(indicators.get("regime_early", False))
                and gajami_entry
            )
            _effective_threshold = (
                TD_KNEE_MIN_CONF_THRESHOLD
                if _regime_early_gajami
                else float(indicators.get("effective_conf_threshold", self.min_confidence) or self.min_confidence)
            )
            indicators["effective_conf_threshold"] = float(_effective_threshold)
            # 🔒 CONF SSoT: TLC AUTH는 confidence 판정에 관여하지 않는다.
            _conf_ok = bool(final_score_capped >= _effective_threshold)
            # 🔧 [STEP B] confidence를 모든 lane의 최종 관문으로 고정한다.
            if not _conf_ok:
                logger.info(
                    f"⏸️ [CONF-BLOCK] {symbol} 최종 신뢰도 부족 | "
                    f"score={final_score_capped:.3f} < thr={_effective_threshold:.3f}"
                    f"{'(RegimeEarly)' if _regime_early_gajami else ''} | "
                    f"base={base:.3f} bonus={bonus:.3f} penalty={penalty:.3f} | "
                    f"reasons={','.join(penalty_reasons[:3]) if penalty_reasons else 'none'}"
                )

                # EEE 상세 로깅 (기존 완전 보존)
                logger.info(
                    f"🧾 [ENTRY_DIAG] {symbol} | "
                    f"BASE={base:.3f} BONUS=+{bonus:.3f} PENALTY=-{penalty:.3f} "
                    f"FINAL={final_score_capped:.3f} THRESHOLD={_effective_threshold:.3f}"
                    f"{'(RegimeEarly→0.28)' if _regime_early_gajami else ''}"
                )

                self._block(BLOCK_REASON_CONF_LOW, f"{final_score_capped:.3f}<{_effective_threshold:.3f}")
                logger.info(
                    f"📋 [ENTRY-SUMMARY] {symbol} | "
                    f"regime={signal.market_regime.value if (signal and signal.market_regime) else '?'} "
                    f"dur={((((data_package.ma_data or {}).get('gajami_meta') or {}).get('gajami_lifecycle') or {}).get('dur', dur_5x10))} soleus={soleus_gajami} "
                    f"gajami={bool(indicators.get('shape_v2_gajami_ok',False))} "
                    f"score={final_score_capped:.3f} block=CONF-BLOCK"
                )
                return None

            # 🔒 CONF SSoT: 실제 승인 경로는 confidence 단일.
            _entry_auth_route = "CONF"
            indicators["entry_authorization_route"] = _entry_auth_route

            # 🚫 [NO-GC-ENTRY] 골든크로스 없는 진입 차단 (전제 위반 = 오류성)
            #  시스템 전제 = "MA5>MA10 골크 근방 진입". gc=0 자리는 전제 자체가 없다.
            #  실측 07-28~08-01: 페어링 289건 중 31건(10.7%)이 gc=0으로 진입
            #   - 전량 route=CONF (ENTRY-KNEE 24 / ENTRY-RANGING-GAJAMI 7)
            #   - 승률 23% · 합계 -3.94% (손실 -12.33% / 수익 +8.39%)
            #  SSoT: Analysis closed_gc_5x10_relation = 최신 확정봉 strict MA5>MA10.
            #  현재봉 RT pair는 진단만 하고 canonical GC를 뒤집지 않는다.
            _gc_now = int(indicators.get("closed_gc_5x10_relation", 0) or 0)
            if _gc_now != 1:
                self.last_rejection_reason = "NO_GC_ENTRY"
                logger.info(
                    f"🚫 [NO-GC-ENTRY] {symbol} 확정봉 골든크로스 없음 → 진입 차단 | "
                    f"entry_type={entry_type} route={_entry_auth_route} "
                    f"closed_ma5={fmt(indicators.get('entry_ma5_closed'), '.6f')} "
                    f"closed_ma10={fmt(indicators.get('entry_ma10_closed'), '.6f')} "
                    f"rt_gc={int(indicators.get('entry_ma5_gt_ma10', 0) or 0)} "
                    f"regime={regime.value if hasattr(regime, 'value') else regime} "
                    f"score={final_score_capped:.3f} thr={_effective_threshold:.3f}"
                )
                return None

            # 🔒 [GC-BIRTH-BAR-BLOCK 철거-0817] 봇 dur=1은 정답봉.

            # ✅ 모든 차단 통과 → 진입 가능
            # (이후 진입 로직 계속)
                            
            # ATR 기반 손익 계산
            risk_calc = RiskCalculator(self.config)
            stop_loss, take_profit, trailing_stop = risk_calc.calculate_stops(
                current_price, atr_value, self.timeframe.value, regime=regime,
                symbol=symbol
            )
            
            # 🔒 SSoT-META: 손절/익절 가격은 RiskCalculator가 1회 계산한다.
            # - 손절/익절 퍼센트의 출처는 self.settings(=config_3m.ini 우선) 단일
            # - 체제별 고정 퍼센트 하드코딩/재계산 금지
            # - 최종 손익비 검증은 모든 전략 veto 통과 뒤 RR-FINAL-BLOCK에서 수행
            
            # ✅ 여기서 stop_loss/take_profit을 재계산하지 않는다.
            
            # 🆕 PATCH 3: 레벨5 익절 강화 (2025-11-27)
            # 목적: 손절 1건 = 익절 3건 문제 해소
            # 효과: +2~3% 익절을 +3~4.5%로 확대
            # 🔒 SSoT-META NOTE:
            # - 절대 익절 기준은 calculate_stops()에서만 결정됨
            # - 본 로직은 신호 품질(alignment)에 따른 상대적 보정임
            # - pct 검증/경계값 판단 없음 (Bot에서 단일 처리)
            ma_analysis_for_tp = data_package.ma_data or {}
            alignment_level = ma_analysis_for_tp.get('alignment_level', 0)
            if alignment_level >= 5:
                original_tp = take_profit
                tp_boost_multiplier = 1.35  # 35% 증가
                take_profit = current_price + (take_profit - current_price) * tp_boost_multiplier
                logger.info(
                    f"🎯 [TP-BOOST] {symbol} alignment={alignment_level} | "
                    f"multiplier={tp_boost_multiplier} (relative adjustment) | "
                    f"원래: {original_tp:.0f}원 (+{(original_tp-current_price)/current_price*100:.2f}%) → "
                    f"강화: {take_profit:.0f}원 (+{(take_profit-current_price)/current_price*100:.2f}%)"
                )
            
            # ─────────────────────────────────────────
            # 🧪 Activity Snapshot #3: ENTER 직전
            # ─────────────────────────────────────────
            self._log_activity_snapshot(symbol, {
                "stage": "pre_enter",
                "final_conf": round(final_score_capped, 3),
                "threshold": round(_effective_threshold, 3),
                "penalty_factor": round(penalty_factor, 3),
                "volume_ratio": round(safe_extract_scalar(indicators.get("volume_ratio", 0.0)), 2),
                "buying_ratio": round(safe_extract_scalar(indicators.get("buying_ratio", 0.0)), 1),
            })
            
            # 🚫 연대기 패턴 하드 게이트 (2025-12-15)
            # 목적: 급등 후 깊은 조정 패턴을 "보조 점수"가 아닌 "절대 차단"으로 처리
            # 철학: "먹어서는 안 되는 가짜를 절대 먹지 않는다"
            # 효과: MA/GC 점수가 아무리 높아도 가짜 가자미근 진입 차단
            if rally_info_cached is not None and rally_info_cached.get('has_rally', False):
                max_price_gate = rally_info_cached.get('max_price', 0)
                rally_pct_gate = rally_info_cached.get('rally_pct', 0)
                
                if max_price_gate > 0 and rally_pct_gate > 0:
                    # 조정 비율 계산
                    pullback_gate = (max_price_gate - current_price) / max_price_gate
                    pullback_ratio_gate = pullback_gate / rally_pct_gate
                    
                    # 급등 후 50% 이상 되돌림 → 가짜 가자미근 → 하드 차단
                    if (
                        pullback_ratio_gate >= 0.5
                        and not _j234_hardcut_pass
                    ):
                        _rally_duration_gate = int(rally_info_cached.get('rally_duration', 0) or 0)

                        self.last_rejection_reason = f"ChronologyGate (급등 {fmt(rally_pct_gate*100, '.1f')}% → 조정비율 {fmt(pullback_ratio_gate*100, '.1f')}%)"
                        logger.info(
                            f"🚫 [ChronologyGate] {symbol} 급등→조정 패턴 하드 차단 | "
                            f"급등={rally_pct_gate*100:.2f}% | "
                            f"지속={_rally_duration_gate}분 | "
                            f"조정비율={pullback_ratio_gate*100:.1f}% (>=50%)"
                        )
                        return None
            
            # 🔒 횡보장 기대익절 하한 필터 (손익비 구조 개선)
            # 철학: 횡보장에서 작은 익절(+0.6~0.8%)로 큰 손절(-3%)을 감당 불가
            # 효과: 기대값 음수 거래 제거 → 손익비 개선
            if regime == MarketRegime.RANGING:
                expected_profit_pct = (
                    (take_profit - current_price) / current_price * 100.0
                )
                
                if expected_profit_pct < 1.0:
                    self.last_rejection_reason = (
                        f"RANGING 기대익절 부족 ({expected_profit_pct:.2f}% < 1.0%)"
                    )
                    logger.info(
                        f"🚫 [Ranging-Expectancy-Block] {symbol} "
                        f"기대익절 {expected_profit_pct:.2f}% → 진입 차단 | "
                        f"손익비 보호 (익절:{expected_profit_pct:.2f}% vs 손절:-3%)"
                    )
                    return None
                        
            # 🔎 진입 단계 명시 (탐색 vs 확정)
            position_ratio = float(locals().get('position_size_ratio') or 1.0)
            entry_stage = "PRE" if position_ratio < 0.99 else "STD"

            # 판사 전용 종결 경로와 일반 경로가 같은 신호 생성 SSoT를 사용한다.
            def _build_entry_signal() -> TradingSignal:
                _glc = (
                    ((data_package.ma_data or {}).get("gajami_meta") or {})
                    .get("gajami_lifecycle")
                    or {}
                )
                _entry_signal = TradingSignal(
                    symbol=symbol,
                    action="BUY",
                    price=current_price,
                    confidence=final_score_capped,
                    reason=(
                        f"{regime.value} 조건 충족 "
                        f"(신호강도: {final_score_capped:.2f})"
                    ),
                    market_regime=regime,
                    signal_components=components,
                    indicators={
                        "atr": atr_value,
                        "stop_loss": stop_loss,
                        "take_profit": take_profit,
                        "trailing_stop": trailing_stop,
                        "gajami_entry": bool(
                            indicators.get("gajami_entry", False)
                        ),
                        "entry_regime": regime.value if regime else None,
                        "position_size_ratio": position_ratio,
                        "entry_stage": entry_stage,
                        "position_confidence_mult": position_confidence_mult,
                        "entry_confidence": final_score_capped,
                        "detect_candle_time": indicators.get(
                            "detect_candle_time", ""
                        ),
                        "gajami_lifecycle": _glc,
                        "golden_count": int(
                            indicators.get("golden_count", 0) or 0
                        ),
                        "rise_pct": float(_glc.get("rise_pct", 0) or 0),
                        # 🔒 [REPORT-JUDGES-0822] 리포트 진입체제 컬럼용 판사 조합 SSoT.
                        "judges": "%d%d%d%d" % (
                            int(indicators.get("judge1_gajami_pass", 0) or 0),
                            int(indicators.get("judge2_osc_pass", 0) or 0),
                            int(indicators.get("judge3_cycle_pass", 0) or 0),
                            int(indicators.get("judge4_reversal_pass", 0) or 0),
                        ),
                        "judge_lane_authorized": int(
                            _judge_lane_authorized
                        ),
                        "entry_authorization_route": _entry_auth_route,
                    },
                )
                _entry_signal.metadata = {"current_price": current_price}
                return _entry_signal

            # ============================================================
            # 🔒 FINAL ENTRY GATE (SSoT)
            # 허용된 Entry Context 4종만 통과:
            #  1) GAJAMI_ZONE (명시적 가자미근)
            #  2) TRENDING_UP PULLBACK (상승 눌림 재진입)
            #  3) BREAKOUT CONFIRM (돌파 확인)
            #  4) RANGING LOWER BOX (박스 하단 매수)
            # ============================================================
            
            # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
            # 🔥 박스 의존성 완전 제거 (2026-02-16)
            # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
            # 제거 사유:
            # - trimmed_box_pos: 계산 오류 (신고점 갱신 중 pos=1.00)
            # - not_near_high: 박스 파생 변수
            # - Context 판정은 구조적 신호(기울기/이격/거래량)만 사용
            # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
            
            # --- 공통 지표 (구조적 신호만) ---
            ma5_gap_pct = float(safe_extract_scalar(indicators.get("ma5_gap_pct", 0.0), 0.0))
            ma5_slope = float(safe_extract_scalar(indicators.get("ma5_slope", 0.0), 0.0))
            vol_ratio = float(safe_extract_scalar(indicators.get("volume_ratio", 0.0), 0.0))
            # 묶음 v5: recent_low_break 키가 없으면 False이며, not recent_low_break 조건은 True가 된다.
            recent_low_break = bool(indicators.get("recent_low_break", False))

            # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
            # 🔥 ma5_reversal 계산 제거 (2026-02-16)
            # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
            # 제거 사유:
            # - MA5-Reversal 차단 이미 제거됨 (9525줄)
            # - 사용되지 않는 불용 변수
            # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
            # (A) GAJAMI_ZONE (명시적 가자미근)
            # ✅ SSoT: Analysis 통과(hard_gajami_pass)는 최종 Entry Context에서 존중한다.
            # 🔧 SSoT 단일화: gajami_meta.soleus_zone + hard_gajami_pass
            ctx_gajami = bool(
                soleus_gajami
                or gajami_meta.get("soleus_zone", False)
                or indicators.get("hard_gajami_pass", False)
            )

            # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
            # 🔥 Near-High 차단 제거 (2026-02-16)
            # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
            # 제거 사유:
            # - 박스 pos 계산 오류 (신고점 갱신 중에도 pos=1.00)
            # - BIRTH 초입을 오판하여 차단 (SOL 21:29)
            # - 목덜미 방지는 Lifecycle(SHOULDER)이 담당
            # - Lifecycle 단독 판사 원칙
            # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

            # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
            # 🔥 MA5-Reversal 차단 제거 (2026-02-16)
            # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
            # 제거 사유:
            # - 박스 pos 기반 판정 (계산 오류 가능)
            # - MA10_slope가 실제 추세 판사 (Lifecycle에서 담당)
            # - MA5는 단기 노이즈, 구조적 신호 아님
            # - Lifecycle 단독 판사 원칙
            # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
            
            # (B) TRENDING_UP PULLBACK (상승 추세 눌림 재진입)
            # ✅ 철학: 가자미근 + 상승추세 + 양방향 기울기 + 낮은 이격
            # 🔥 박스(pos) 조건 제거: 구조적 신호만 사용
            ctx_up_pullback = (
                regime == MarketRegime.TRENDING_UP
                and _soleus_ssot(indicators, data_package.ma_data)
                and ma5_slope >= 0.0
                and abs(ma5_gap_pct) <= 1.0
            )
            
            # (C) BREAKOUT CONFIRM (돌파 확인)
            # ✅ 철학: 돌파 체제 + 낮은 이격 + 거래량 확인
            # 🔥 박스(pos) 조건 제거: 구조적 신호만 사용
            ctx_breakout = (
                regime == MarketRegime.BREAKOUT
                and abs(ma5_gap_pct) <= 1.0
                and vol_ratio >= 0.80
            )
            
            # (D) RANGING 진입 (횡보 체제)
            # ✅ 철학: 횡보 + 낮은 이격 + 저점 붕괴 아님
            # 🔥 박스(pos) 조건 제거: "하단만"은 Lifecycle이 담당
            ctx_ranging = (
                regime == MarketRegime.RANGING
                and abs(ma5_gap_pct) <= 2.0
                and not recent_low_break
            )
            
            # (E) HEAD_LIFT_TRIGGER (저점 횡보 후 고개 드는 순간)
            # ✅ PRE_GAJAMI_BASE 이후 트리거는 공식 진입 Context로 인정
            # ✅ 시간 민감한 트리거 - 가장 우선순위 높음
            ctx_head_lift = bool(indicators.get("head_lift_trigger", False))
                       
            # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
            # 🔥 Entry Context 페널티 제거 (2026-02-16)
            # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
            # 제거 사유:
            # - Context는 로그 기록용
            # - 점수 조작은 우회도로 (bypass road)
            # - Lifecycle 단독 판사 원칙
            # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
            
            # Context 기록 (분석용만)
            context_name = (
                'HEAD_LIFT' if ctx_head_lift else
                'GAJAMI' if ctx_gajami else
                'UP_PULLBACK' if ctx_up_pullback else
                'BREAKOUT' if ctx_breakout else
                'RANGING_LOWER' if ctx_ranging else
                'LIFECYCLE'
            )
            logger.debug(
                f"📊 [ENTRY-CONTEXT] {symbol} context={context_name} | "
                f"gap={ma5_gap_pct:+.2f}% | vol={vol_ratio:.2f}x"
            )
            
            # ============================================================
            # 🚫 GAJAMI-FLOOR: 제거 (2026-02-08)
            # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
            # 제거 사유:
            # - 실 신뢰도 0.139를 0.500으로 강제 상승 → 신뢰도 시스템 무력화
            # - 모든 진입이 0.50 고정 → 품질 구분 완전 소멸 (만년 고질병 원인)
            # - soleus=True여도 점수가 threshold 미달이면 "진입 불가"가 정석
            # - 가자미근은 Analysis(솔레우스 판정)에서 이미 보호
            #   → Strategy에서 점수를 조작하는 것은 우회도로(bypass road)
            # ============================================================
            # 🔒 confidence 최종판사는 위 _conf_ok 단일 경로만 사용한다.
            # soleus floor·softpass·judge 예외로 점수를 다시 승인하지 않는다.

            # context_name은 위 [ENTRY-CONTEXT] 산출값을 그대로 재사용한다.
            # 중간에 ctx_* 재할당이 없으므로 2차 재정의 금지.
            # LIFECYCLE fallback 누락으로 [ENTRY-FINAL] 맥락 오집계가 발생했음.
            
            # ENTRY-FINAL은 confidence 최종관문 통과 경로만 기록한다.
            if _conf_ok:
                logger.debug(
                    f"✅ [ENTRY-FINAL] {symbol} 진입 허용 | "
                    f"context={context_name} "
                    f"route={_entry_auth_route} | "
                    f"conf={fmt(final_score_capped,'.3f')} "
                    f"≥ thr={fmt(_effective_threshold,'.3f')} | "
                    f"regime={regime.value}"
                )
            
            # 🔒 [MA5-DIST-GUARD] 가자미근 이격도 추세 게이트
            # 가격↔MA5 거리가 2봉 사이 급격히 악화 → 위치 에너지 붕괴
            # ✅ 정석: 형상+추세(재가속) 확인되면 "발목 근접"을 붕괴로 오판하지 않도록 면제
            MA5_DIST_DROP_MIN = -0.45

            _dist_exempt = False
            try:
                _dist_exempt = bool(_shape_trend_confirm(indicators))
                # 탄생봉(dur≤2) + lifecycle VALID: 크로스 직후 s10<s20은 물리적 정상
                # _shape_trend_confirm의 s10>=s20 조건이 탄생 초입을 오차단
                _dist_dur_src = (((data_package.ma_data or {}).get("gajami_meta") or {}).get("gajami_lifecycle") or {}).get("dur", None)
                _dist_dur = int(_dist_dur_src) if _dist_dur_src is not None else 999
                if not _dist_exempt and _dist_dur <= SoleusGajamiConstants.NEWBORN_MAX_DUR:
                    _lc_state = str(
                        (_gajami_meta_lc.get("state") or "")
                        if isinstance(_gajami_meta_lc, dict) else ""
                    )
                    if _lc_state == "VALID":
                        _dist_exempt = True
            except Exception:
                _dist_exempt = False
            _dist_exempt = False

            if (
                ctx_gajami
                and ma5 > 0
                and not _dist_exempt
                and not _j234_hardcut_pass
            ):
                _dist_now = (current_price - ma5) / ma5 * 100.0
                _dist_prev2 = float(indicators.get('price_to_ma5_prev2_pct', 0.0) or 0.0)
                _delta_dist = _dist_now - _dist_prev2
                if _delta_dist <= MA5_DIST_DROP_MIN:
                    self.last_rejection_reason = "MA5_DIST_BREAK"
                    logger.info(
                        f"🚫 [MA5-DIST-GUARD] {symbol} "
                        f"이격도추세 악화 | now={_dist_now:.2f}% prev2={_dist_prev2:.2f}% "
                        f"Δ={_delta_dist:.2f}% ≤ {MA5_DIST_DROP_MIN}% → 차단"
                    )
                    return None
            elif ctx_gajami and ma5 > 0 and _dist_exempt:
                logger.debug(
                    f"✅ [MA5-DIST-LIFT] {symbol} 형상+추세 확인 → MA5_DIST_GUARD 면제"
                )
            
            # ============================================================
            # 🔒 [MA15-SLOPE-GUARD] 중기 추세 필터 (힘 없는 가자미근 제거)
            # ============================================================
            # 철학: 가자미근 형상이어도 MA15가 하락 중이면
            #        MA15 밑으로 밀려 MA이탈/타임아웃으로 끝남
            # 예외: TD는 반등 초입에서 MA15가 아직 음수일 수 있으므로 허용
            # ============================================================
            _ma15_slope = float(safe_extract_scalar(indicators.get('ma15_slope', 0.0), 0.0))
            _ma15_dur_src = ((((data_package.ma_data or {}).get("gajami_meta") or {}).get("gajami_lifecycle") or {})).get("dur", None)
            _ma15_dur = int(_ma15_dur_src) if _ma15_dur_src is not None else 999
            if regime != MarketRegime.TRENDING_DOWN and _ma15_dur > 2:
                if _ma15_slope <= 0 and not _j234_hardcut_pass:
                    self.last_rejection_reason = "MA15_SLOPE_WEAK"
                    logger.info(
                        f"🚫 [MA15-SLOPE-GUARD] {symbol} ma15_slope={_ma15_slope:.4f}% ≤ 0 dur={_ma15_dur} → "
                        f"중기 추세 약세 진입 차단 | regime={regime.value}"
                    )
                    return None
            
            # ============================================================
            # 🆕 HEAD_LIFT_TRIGGER 기반 초입 진입 (우선 체크)
            # ============================================================
            # 목적: 저점 횡보 → 고개 드는 순간 포착
            # 조건: head_lift_trigger + 신뢰도 95% 문턱
            # 포지션: 60% 축소 (보수적)
            # ============================================================
            # 🆕 HEAD_LIFT_TRIGGER 기반 초입 진입 (우선 체크)
            # ============================================================
            if (
                indicators.get("head_lift_trigger", False)
                and soleus_gajami
                and _j1_vote_pass
            ):
                head_lift_threshold = self.min_confidence * 0.95
                
                if final_score_capped >= head_lift_threshold:
                    # 🔒 PATCH-3: 감지시각 = 진입 확정 시각 (항상 현재)
                    indicators["detect_candle_time"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    
                    # 🔧 gajami_lifecycle 전달
                    gmeta = (data_package.ma_data or {}).get("gajami_meta") or {}
                    glc = gmeta.get("gajami_lifecycle")
                    if isinstance(glc, dict):
                        indicators["gajami_lifecycle"] = glc
                    
                    logger.debug(
                        f"🕐 [감지시각설정] {symbol} HEAD_LIFT 판결 PASS → 감지시각={indicators['detect_candle_time']}"
                    )
                    logger.info(
                        f"🚀 [ENTRY-HEAD-LIFT] {symbol} 초입 가자미근 진입! | "
                        f"conf={final_score_capped:.3f} ≥ thr={head_lift_threshold:.3f} | "
                        f"포지션=60% (보수적)"
                    )
                    
                    # HEAD_LIFT 전용 신호 생성
                    signal = TradingSignal(
                        symbol=symbol,
                        action="BUY",
                        price=current_price,
                        confidence=final_score_capped,
                        reason=f"HEAD_LIFT_TRIGGER (신호강도: {final_score_capped:.2f})",
                        market_regime=regime,
                        signal_components=components,
                        indicators={
                            'atr': atr_value,
                            'stop_loss': stop_loss,
                            'take_profit': take_profit,
                            'trailing_stop': trailing_stop,
                            'gajami_entry': True,  # HEAD_LIFT도 가자미근 계열
                            'entry_regime': (regime.value if regime else None),
                            'position_size_ratio': 0.6,  # 60% 포지션
                            'entry_stage': 'HEAD_LIFT',  # 전용 스테이지
                            'position_confidence_mult': position_confidence_mult,
                            
                            # 🔒 PATCH ③-B: HEAD_LIFT도 매수 신뢰도 명시적 전달
                            'entry_confidence': final_score_capped,
                            # 🆕 감지봉 시간 전달 (HEAD_LIFT용)
                            'detect_candle_time': indicators.get('detect_candle_time', ''),
                            # 🔧 gajami_lifecycle 전달 (Bot rise 로그용)
                            'gajami_lifecycle': indicators.get('gajami_lifecycle', {}),
                        }
                    )
                    
                    _det_t = indicators.get('detect_candle_time', '')
                    logger.info(
                        f"✅ [신호분석] {symbol} HEAD_LIFT 매수 후보 | "
                        f"감지시각={_det_t} | "
                        f"체제: {regime.value} | 신뢰도: {fmt(final_score_capped, '.2f')} "
                        f"(base={fmt(base_confidence, '.2f')}, penalty={fmt(penalty_factor, '.2f')}) | "
                        f"손절: {fmt(stop_loss, '.0f')} | 익절: {fmt(take_profit, '.0f')} | "
                        f"포지션: 60% → SINGLE_EXIT"
                    )
                    final_signal = signal
                    entry_type = "HEAD_LIFT"
                    indicators["entry_type"] = entry_type
            
            # ============================================================
            # 기존 진입 로직 (HEAD_LIFT 실패 시)
            # ============================================================
            # 🔒 SSoT: HARD GATE 통과 필수 → 여기 도달 시 soleus_gajami=True 보장
            
            # 🔒 PATCH-3: 감지시각 = 진입 확정 시각 (항상 현재)
            indicators["detect_candle_time"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            
            # 🔧 gajami_lifecycle 전달
            gmeta = (data_package.ma_data or {}).get("gajami_meta") or {}
            glc = gmeta.get("gajami_lifecycle")
            if isinstance(glc, dict):
                indicators["gajami_lifecycle"] = glc
            
            logger.debug(
                f"🕐 [감지시각설정] {symbol} 판결 PASS → 감지시각={indicators['detect_candle_time']}"
                )
            
            # 신호 생성 — 판사 전용 종결 경로와 동일 SSoT 사용
            signal = _build_entry_signal()

            logger.info(
                f"🟡 [신호분석] {symbol} 매수 후보 생성 | "
                f"체제: {regime.value} | 신뢰도: {fmt(final_score_capped, '.2f')} "
                f"(base={fmt(base_confidence, '.2f')}, penalty={fmt(penalty_factor, '.2f')}) | "
                f"손절: {fmt(stop_loss, '.0f')} | 익절: {fmt(take_profit, '.0f')}"
            )
                       
            # 🆕 EEE: 진입 성공 Snapshot (v3.0 개선 - 실제 값 전달)
            # ✅ 7순위(SSoT): EEE 출력 단일화
            # - SSoT 값만 출력 (혼선 제거)
            eee_ma5 = ma5 if ma5 > 0 else None
            eee_ma10 = ma10 if ma10 > 0 else None
            eee_ma20 = ma20 if ma20 > 0 else None
            eee_slope5 = safe_extract_scalar(ma_data.get("slope_ma5", None)) if ma_data else None
            eee_soleus_reason = indicators.get("soleus_zone_reason", None)               # ✅ SSoT
            
            # penalty_reasons 요약 (Top-2)
            try:
                eee_penalty_reasons = penalty_reasons[:2] if isinstance(penalty_reasons, list) else []
            except Exception:
                eee_penalty_reasons = []
            
            eee_volume_ratio = volume_ratio if 'volume_ratio' in locals() else None
            eee_atr_pct = atr_pct if atr_pct > 0 else None
            eee_regime_name = regime.value if hasattr(regime, 'value') else str(regime)
            eee_tier = tier if 'tier' in locals() else None

            self._log_entry_evidence(
                symbol=symbol,
                decision="🟡 CANDIDATE",
                price=current_price,
                ma5=eee_ma5,
                ma10=eee_ma10,
                ma20=eee_ma20,
                slope5=eee_slope5,
                volume_ratio=eee_volume_ratio,
                atr_pct=eee_atr_pct,
                regime_name=eee_regime_name,
                tier=eee_tier,
                entry_score=final_score_capped,
                rejection_reason=None,
                final_conf=final_score_capped,
                threshold=_effective_threshold,
                penalty_factor=penalty_factor,         # ✅ CAP 적용된 최종값
                soleus_reason=eee_soleus_reason,       # ✅ SSoT soleus 상태
                penalty_reasons=eee_penalty_reasons    # ✅ Top-2 페널티 이유
            )
            
            # ✅ 모든 체제별 검증 완료 - 신호 반환
            
            # 🔒 FINAL HARD CUT (TRENDING_UP 전용)
            # 목적: "UP인데 구조는 하락/붙음" 문제 종결
            # 적용: TRENDING_UP 체제만 (TD/RANGING 영향 없음)
            if regime == MarketRegime.TRENDING_UP:
                # ─────────────────────────────────────
                # 기존 MA 변수 활용 (같은 함수 내 이미 정의됨)
                # ─────────────────────────────────────
                # 핵심: 같은 함수에서 같은 값을 두 번 다른 방식으로 구하지 않음
                # ma5, ma10, ma20는 이미 앞에서 정의되어 있음 (line ~9774)
                ma5_hc = locals().get('ma5', 0.0) or 0.0
                ma10_hc = locals().get('ma10', 0.0) or 0.0
                ma20_hc = locals().get('ma20', 0.0) or 0.0
                
                # slope 추출
                slope5 = safe_extract_scalar(ma_data.get("slope_ma5", None)) if ma_data else None
                slope10 = safe_extract_scalar(ma_data.get("slope_ma10", None)) if ma_data else None
                slope20 = safe_extract_scalar(ma_data.get("slope_ma20", None)) if ma_data else None
                
                # 하드 컷의 책임 범위: MA 구조 판단 (데이터 무결성 검증 아님)
                # - MA 값이 없으면 → 하드 컷 스킵 (차단 아님)
                # - MA 값이 있는데 구조가 나쁘면 → 하드 컷 차단
                
                # 1) 값 없으면 하드 컷 스킵
                if ma5_hc <= 0 or ma10_hc <= 0 or ma20_hc <= 0:
                    logger.debug(
                        f"⚠️ [MA-STRUCTURE-SKIP] {symbol} | MA 값 부재, 구조 검사 스킵 | "
                        f"ma5={ma5_hc}, ma10={ma10_hc}, ma20={ma20_hc}"
                    )
                    # pass (하드 컷 스킵, 진입은 다른 조건으로 판단)
                
                elif slope5 is None or slope10 is None or slope20 is None:
                    logger.debug(
                        f"⚠️ [MA-STRUCTURE-SKIP] {symbol} | Slope 값 부재, 구조 검사 스킵"
                    )
                    # pass (하드 컷 스킵)
                
                else:
                    # 여기서부터 진짜 하드 컷 (MA 구조 판단)
                    
                    # 2) 정배열 필수
                    if not (ma5_hc > ma10_hc > ma20_hc):
                        # ✅ 근소 역배열 예외: gap≤0.05% + intrabar_gc + 가격>MA5
                        if ma5_hc > 0 and ma10_hc > 0:
                            gap_pct = (ma10_hc - ma5_hc) / ma10_hc * 100.0
                            _cp_raw = indicators.get("current_price", 0.0)
                            cur_price = float(safe_extract_scalar(_cp_raw, 0.0))
                            
                            _intrabar_gc_hc = bool(indicators.get("ma5_intrabar_cross_up", False))
                            if (
                                gap_pct <= 0.05
                                and _intrabar_gc_hc
                                and cur_price > ma5_hc
                            ):
                                logger.info(
                                    f"✅ [MA-STRUCTURE-MICRO-PASS] {symbol} 근소 역배열 예외 통과 | "
                                    f"gap={gap_pct:.3f}% | price>{ma5_hc:.2f}"
                                )
                            else:
                                # MA 역배열 → 페널티로 전환
                                penalty_reasons.append(f"MA역배열-Soft")
                                logger.info(
                                    f"⚠️ [MA역배열-Soft] {symbol} | MA 역배열 → 페널티 | "
                                    f"ma5={fmt(ma5_hc, '.4f')}, ma10={ma10_hc:.4f}, ma20={ma20_hc:.4f} | "
                                    f"penalty=0.75"
                                )
                        else:
                            # MA 역배열 → 페널티로 전환
                            penalty_reasons.append(f"MA역배열-Soft")
                            logger.info(
                                f"⚠️ [MA역배열-Soft] {symbol} | MA 역배열 → 페널티 | "
                                f"ma5={fmt(ma5_hc, '.4f')}, ma10={ma10_hc:.4f}, ma20={ma20_hc:.4f} | "
                                f"penalty=0.75"
                            )
                    
                    # 3) 우상향 필수 (RANGING 가자미근 보호: 미세 하락 허용)
                    # - slope5는 반드시 상승 (0 초과)
                    # - slope10/20은 -0.001 (-0.1%) 이상 허용 (미세 하락/수평 OK)
                    if slope5 <= 0 or slope10 < -0.001 or slope20 < -0.001:
                        self.last_rejection_reason = (
                            f"SOFT-PENALTY: MA 하락 (s5={slope5:.6f}, s10={slope10:.6f}, s20={slope20:.6f})"
                        )
                        # MA 하락 → 페널티로 전환
                        penalty_reasons.append(f"MA하락-Soft")
                        logger.info(
                            f"⚠️ [MA하락-Soft] {symbol} | MA 하락 → 페널티 | "
                            f"slope5={slope5:.6f}, slope10={slope10:.6f}, slope20={slope20:.6f} | "
                            f"penalty=0.8"
                        )
                    
                    # 4) 최소 간격 유지 (0.05% = 0.0005)
                    #    ⚠️ 단, 가자미근 초입에서는 간격이 넓을 수 없음 → 예외 처리
                    gap_5_10 = (ma5_hc - ma10_hc) / ma10_hc
                    gap_10_20 = (ma10_hc - ma20_hc) / ma20_hc
                    
                    # ✅ 가자미근(soleus_zone) 예외: 초입에서는 간격 검사 면제
                    # 철학: 가자미근 초입은 '완성된 부채꼴'이 아니라
                    #       '곧 위로 접힐 부채꼴의 시작점'
                    if soleus_zone:
                        logger.debug(
                            f"✅ [MA간격-SOLEUS-SKIP] {symbol} | 가자미근 확정 → 간격 검사 면제 | "
                            f"5-10={gap_5_10:.4%}, 10-20={gap_10_20:.4%}"
                        )
                    elif not (gap_5_10 >= 0.0005 and gap_10_20 >= 0.0005):
                        self.last_rejection_reason = (
                            f"SOFT-PENALTY: MA 간격 부족 (5-10={gap_5_10:.4%}, 10-20={gap_10_20:.4%})"
                        )
                        # MA 간격 부족 → 페널티로 전환
                        penalty_reasons.append(f"MA간격부족-Soft")
                        logger.info(
                            f"⚠️ [MA간격부족-Soft] {symbol} | MA 간격 부족 → 페널티 | "
                            f"5-10={gap_5_10:.4%}, 10-20={gap_10_20:.4%} | "
                            f"penalty=0.85"
                        )
                    
                    # 모두 통과
                    logger.debug(
                        f"✅ [MA-STRUCTURE-PASS] {symbol} | "
                        f"정배열={fmt(ma5_hc, '.2f')}>{ma10_hc:.2f}>{ma20_hc:.2f}, "
                        f"우상향=({slope5:.6f},{slope10:.6f},{slope20:.6f}), "
                        f"간격=({gap_5_10:.4%},{gap_10_20:.4%})"
                    )
            
            # 🔀 Entry Type 분기 (REVERSAL / TREND / KNEE)
            if final_signal is None:
                entry_type = str(indicators.get('entry_type', "NONE") or "NONE")
            else:
                entry_type = str(entry_type or indicators.get('entry_type', "NONE") or "NONE")
            
            # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
            # 🔒 글로벌 하드게이트: 형상 없으면 무조건 차단
            # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
            # 위치: entry_type 값 무관 (entry_type 분기 전)
            # 철학: 형상 > 체제 (완화 ❌ 봉합 ✅)
            # 
            # 차단 대상:
            # - Analysis → TREND (golden_cross)
            # - Analysis → REVERSAL (early_turn)
            # - Analysis → PULLBACK (pullback_bounce)
            # - Strategy → 모든 우회 경로
            # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
            # 🔒 [TOP-FLAT-GLOBAL-OWNER-PURGE-0828]
            # TOP_FLAT은 Analysis soleus/J1에서 이미 소비한다.
            # Strategy 전역 hard return은 J1에는 중복이고
            # J2/J3/J4 판결만 선행 차단하므로 제거.
            # 비J1 진입 자격은 후단 [FINAL-JUDGE-OR-0817]이 그대로 판정한다.

            # 🔒 [JUDGE-ROUTE-INDEPENDENCE-0828-R1]
            # J2/J3/J4가 하나라도 PASS하면 J1 동시승인 여부와 무관하게
            # 공통 _build_entry_signal() / JUDGE route가 집행 SSoT다.
            # J1 전용 KNEE/HEAD/TD/RANGING profile을 복수승인이 빌려 쓰지 않는다.
            if _j234_hardcut_pass:
                _judge_source_entry_type = entry_type
                entry_type = "JUDGE"
                indicators["entry_type"] = entry_type
                indicators["judge_entry_source_type"] = (
                    _judge_source_entry_type
                )
                logger.info(
                    f"🟢 [JUDGE-ROUTE-NORMALIZE] {symbol} "
                    f"legacy={_judge_source_entry_type} → JUDGE | "
                    f"J1={int(_j1_vote_pass)} "
                    f"J2={int(indicators.get('judge2_osc_pass', 0) or 0)} "
                    f"J3={int(indicators.get('judge3_cycle_pass', 0) or 0)} "
                    f"J4={int(indicators.get('judge4_reversal_pass', 0) or 0)}"
                )

            # J1 단독 REVERSAL만 KNEE로 정규화한다.
            elif entry_type == "REVERSAL" and _j1_vote_pass:
                logger.info(
                    f"🦵 [REVERSAL-TO-KNEE] {symbol} | "
                    f"J1=1/J234=0 → Analysis J1 판결 기반 KNEE 집행"
                )
                entry_type = "KNEE"
                indicators["entry_type"] = entry_type
            # 판사 승인도 없고 Analysis 유형도 없으면 기존대로 차단.
            if entry_type == "NONE":
                self.last_rejection_reason = "ENTRY_TYPE_NONE"
                logger.info(
                    f"🚫 [ENTRY-TYPE-NONE] {symbol} "
                    f"Analysis entry_type 미확정 + J234 승인 없음 → 진입 금지"
                )
                return None
                
                # ❌ 제거된 경로 (구멍 봉쇄)
                # - FAST: 조기 진입 철학 폐기
                # - TREND: 체제 단독 진입 차단
                # - REVERSAL: 횡보 자동 진입 차단
            
            # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
            # 🔒 RANGING 체제 보호 (구조 재정렬)
            # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
            # 철학: Analysis(CORE GATE)가 형상 확정했으면 존중한다.
            # RANGING 추가 판정은 기존 SSoT·MA10 추세 경로만 사용한다.
            # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
            if regime == MarketRegime.RANGING:
                ma5_slope_now = float(safe_extract_scalar(indicators.get("ma5_slope", 0.0), 0.0))
                golden_cnt = int((data_package.ma_data or {}).get("golden_count", 0) or 0)
                
                # gap_5_10_ratio 직접 계산
                ma5_now = float(safe_extract_scalar(indicators.get("sma_5", indicators.get("ma5", 0.0)), 0.0))
                ma10_now = float(safe_extract_scalar(indicators.get("sma_10", indicators.get("ma10", 0.0)), 0.0))
                gap_5_10_ratio = abs(ma5_now - ma10_now) / ma10_now if ma10_now > 0 else 1.0
                
                # 1️⃣ 진짜 바닥 예외 → 통과
                recent_low_check = float(safe_extract_scalar(indicators.get("recent_low", current_price), current_price))
                is_true_bottom = bool(
                    recent_low_check > 0
                    and current_price <= recent_low_check * 1.005
                    and bool(indicators.get("is_low_area", False))
                )

                if is_true_bottom:
                    logger.debug(
                        f"✅ [RANGING-TRUE-BOTTOM] {symbol} 구조바닥 통과 | "
                        f"low_area=1"
                    )
                else:
                    # 2️⃣ 가자미근 SSoT 확정 + GC>=2 → 통과
                    _gajami_ssot_confirmed = bool(indicators.get("gajami_entry", False))
                    _gc_enough = (golden_cnt >= 2)
                    _ma10_series = indicators.get("ma10")
                    # MA10 기준은 lifecycle dur 기준으로만 정한다.
                    # dur_5x10은 3707라인에서 999로 덮어쓰임 → SSoT(gajami_lifecycle.dur) 직접 참조
                    _lc_dur_src = (((data_package.ma_data or {}).get("gajami_meta") or {}).get("gajami_lifecycle") or {}).get("dur", None)
                    _lc_dur = int(_lc_dur_src) if _lc_dur_src is not None else 999
                    _S_NOW_MIN = (
                        0.010
                        if _lc_dur <= SoleusGajamiConstants.NEWBORN_MAX_DUR
                        else 0.030
                    )
                    _S_TREND_MIN = 0.060
                    if _ma10_series is not None and len(_ma10_series) >= 6:
                        _m1 = float(_ma10_series.iloc[-2])
                        _m2 = float(_ma10_series.iloc[-3])
                        _m3 = float(_ma10_series.iloc[-4])
                        _m4 = float(_ma10_series.iloc[-5])
                        _s_now   = (_m1 - _m2) / _m2 * 100.0 if _m2 > 0 else 0.0
                        _s_trend = (_m1 - _m3) / _m3 * 100.0 if _m3 > 0 else 0.0
                        _dir_ok = (
                            (_m1 > _m2 and _m2 > _m3) or
                            (_m2 < _m3 and _m1 > _m3) or
                            (_m4 > _m3 and _m1 > _m2)
                        )
                        # 🔒 탄생봉(dur<=2): MA10 기울기 추세가 구조적으로 0 → s_trend 면제
                        if _lc_dur <= SoleusGajamiConstants.NEWBORN_MAX_DUR:
                            _ma10_trend_ok = (_s_now >= _S_NOW_MIN)
                            _ma10_trend_mode = "birth_s_now"
                        else:
                            _ma10_dir_state_ok = bool(_dir_ok and _s_now > 0.0 and _s_trend > 0.0)
                            _ma10_threshold_ok = bool(_s_now >= _S_NOW_MIN and _s_trend >= _S_TREND_MIN)
                            _ma10_trend_ok = _ma10_dir_state_ok or _ma10_threshold_ok
                            _ma10_trend_mode = "dir_state" if _ma10_dir_state_ok else ("threshold" if _ma10_threshold_ok else "fail")
                    else:
                        _dir_ok = False
                        _s_now = 0.0
                        _s_trend = 0.0
                        _ma10_trend_ok = False
                        _ma10_trend_mode = "no_series"

                    _lc = (
                        indicators.get('gajami_lifecycle')
                        or ((data_package.ma_data or {}).get('gajami_meta') or {}).get('gajami_lifecycle')
                        or {}
                    )
                    _lc_rise = float(_lc.get('rise_pct', 0) or 0)
                    _birth_state_ok = bool(indicators.get("gajami_birth_state_ok", False))
                    _train_life_ok = bool(indicators.get("gajami_birth_traintrack_life_ok", True))

                    # 🆕 [가짜꼭지차단] 방어 안전망
                    _fake_top_birth_s = bool(int(indicators.get("gajami_fake_top_birth", 0) or 0))

                    # 🔒 [J1-POSITIVE-PRODUCER-OWNER-0828]
                    # RANGING_GAJAMI는 J1 positive producer다.
                    # raw soleus/birth가 아니라 실제 J1 PASS까지 있어야 생산한다.
                    _ranging_entry_candidate = bool(
                        _j1_vote_pass
                        and not _j234_hardcut_pass
                        and _birth_state_ok
                        and _true_gc_birth_s
                        and not _fake_top_birth_s
                    )

                    # 🔒 [RANGING-J1-PRODUCER-LANE-SPLIT-0828]
                    # 아래 train-life / ER / newborn-top은 RANGING_GAJAMI(J1)
                    # 생산자 전용 법률이다.
                    # J234 PASS 시 전역 return도 하지 않고,
                    # 실패한 J1 RANGING 신호도 만들지 않는다.
                    _ranging_j1_lane_deny_reason = None
                    if _ranging_entry_candidate and not _train_life_ok:
                        _ranging_j1_lane_deny_reason = (
                            "RANGING_TRAINTRACK_LIFE_FAIL"
                        )
                    if (
                        _ranging_entry_candidate
                        and _ranging_j1_lane_deny_reason is None
                    ):
                        # 🔒 ER-GATE — J1 RANGING producer 전용
                        _er_gate = float(
                            indicators.get('er_10', 0.0) or 0.0
                        )
                        _er_dur = int(_lc_dur)
                        try:
                            _close = float(df['close'].iloc[-2])
                            _open = float(df['open'].iloc[-2])
                            _high = float(df['high'].iloc[-2])
                            _low = float(df['low'].iloc[-2])
                            _c_body = abs(_close - _open)
                            _c_range = _high - _low
                            _c_atr = (
                                float(
                                    indicators.get(
                                        'atr_pct',
                                        0.0,
                                    ) or 0.0
                                )
                                / 100.0
                            ) * _close
                            _min_body = _close * 0.001
                            _is_breakout = (
                                (_c_range > 0 and _c_body > _c_range * 0.6)
                                and (_c_body > _c_atr * 0.5)
                                and (_c_body > _min_body)
                            )
                        except Exception:
                            _is_breakout = False
                            _c_body = 0.0
                            _c_range = 0.0
                        if (
                            _er_gate <= 0.30
                            and _er_dur > 1
                            and not _is_breakout
                        ):
                            _ranging_j1_lane_deny_reason = "RANGING_ER_LOW"
                    if (
                        _ranging_entry_candidate
                        and _ranging_j1_lane_deny_reason is None
                    ):
                        # 🔒 RANGING-TOP-BLOCK — J1 producer 전용
                        _newborn_rg_block = bool(
                            _birth_state_ok
                            and _lc_dur
                            <= SoleusGajamiConstants.NEWBORN_MAX_DUR
                            and not _true_gc_shape_birth_lane
                            and not (
                                _gajami_ssot_confirmed
                                and _gc_enough
                                and _ma10_trend_ok
                            )
                        )
                        indicators["ranging_newborn_rg_block"] = int(
                            _newborn_rg_block
                        )
                        if _newborn_rg_block:
                            _ranging_j1_lane_deny_reason = (
                                "RANGING_TOP_BLOCK"
                            )
                    if (
                        _ranging_entry_candidate
                        and _ranging_j1_lane_deny_reason is not None
                    ):
                        if not _j234_hardcut_pass:
                            self.last_rejection_reason = (
                                _ranging_j1_lane_deny_reason
                            )
                            logger.info(
                                f"🚫 [RANGING-J1-LANE-BLOCK] {symbol} "
                                f"reason={_ranging_j1_lane_deny_reason} | "
                                f"birth_state={int(_birth_state_ok)} "
                                f"train_life={int(_train_life_ok)} "
                                f"ER={float(indicators.get('er_10', 0.0) or 0.0):.3f} "
                                f"dur={_lc_dur}"
                            )
                            return None
                        logger.info(
                            f"🟡 [RANGING-J1-LANE-DENY] {symbol} "
                            f"reason={_ranging_j1_lane_deny_reason} → "
                            f"RANGING_GAJAMI 신호 미생성 · "
                            f"J2/J3/J4 판결권 유지"
                        )
                    if (
                        _ranging_entry_candidate
                        and _ranging_j1_lane_deny_reason is None
                    ):
                        _pass_mode = (
                            "birth_state"
                            if _birth_state_ok
                            else _ma10_trend_mode
                        )
                        logger.info(
                            f"✅ [RANGING-GAJAMI-PASS] {symbol} "
                            f"가자미근 SSoT + GC={golden_cnt} "
                            f"birth_state={int(_birth_state_ok)} "
                            f"train_life={int(_train_life_ok)} "
                            f"dur={_lc_dur} "
                            f"s_now={_s_now:.3f}% "
                            f"s_trend={_s_trend:.3f}% "
                            f"mode={_pass_mode} → 통과"
                        )
                        # 세 J1 법률을 모두 통과했을 때만
                        # RANGING_GAJAMI 신호를 생산한다.
                        indicators["detect_candle_time"] = (
                            datetime.now().strftime(
                                "%Y-%m-%d %H:%M:%S"
                            )
                        )
                        logger.info(
                            f"🟢 [ENTRY-RANGING-GAJAMI] {symbol} | "
                            f"route={_entry_auth_route} "
                            f"score={final_score_capped:.3f} "
                            f"{'≥' if _conf_ok else '<'} "
                            f"thr={_effective_threshold:.3f} | "
                            f"regime={regime.value} "
                            f"birth_state={int(_birth_state_ok)} | "
                            f"entry_price={current_price:.0f}원"
                        )
                        logger.info(
                            f"📋 [ENTRY-SUMMARY] {symbol} | "
                            f"regime={regime.value} "
                            f"dur={_lc_dur} "
                            f"soleus={soleus_gajami} "
                            f"gajami="
                            f"{bool(indicators.get('shape_v2_gajami_ok', False))} "
                            f"birth_state={int(_birth_state_ok)} "
                            f"score={final_score_capped:.3f} "
                            f"result=PASS "
                            f"stage=ENTRY-RANGING-GAJAMI"
                        )
                        final_signal = signal
                        entry_type = "RANGING_GAJAMI"
                        
                    # 🔒 [JUDGE-OWNER-LANE-SPLIT-0828] MA10_TREND_WEAK 은 J1 세부법률.
                    # J2/J3/J4 독립 PASS 를 전역차단하지 않는다.
                    elif (
                        _gajami_ssot_confirmed
                        and _gc_enough
                        and not _ma10_trend_ok
                        and not _j234_hardcut_pass
                    ):
                        self.last_rejection_reason = "MA10_TREND_WEAK"
                        logger.info(
                            f"🚫 [RANGING-GAJAMI-BLOCK] {symbol} "
                            f"MA10 추세 미흡 dir={_dir_ok} "
                            f"s_now={_s_now:.3f}%(min={_S_NOW_MIN}) "
                            f"s_trend={_s_trend:.3f}%(min={_S_TREND_MIN}) "
                            f"mode={_ma10_trend_mode} → 차단"
                        )
                        return None
            
            # =====================================================
            # 🔒 SINGLE EXIT 준비: 최종 신호 변수 (SSoT)
            # =====================================================
            
            # REVERSAL legacy 실행 경로는 폐기 상태 유지.
            # J234 승인 REVERSAL/NONE은 위에서 JUDGE route로 정규화된다.
            if entry_type == "REVERSAL":
                self.last_rejection_reason = "REVERSAL_ROUTE_RETIRED"
                logger.info(
                    f"🚫 [REVERSAL-ROUTE-RETIRED] {symbol} "
                    f"REVERSAL legacy route + J234 승인 없음 → 진입 금지 | "
                    f"regime={regime.value if hasattr(regime, 'value') else regime} "
                    f"score={final_score_capped:.3f}"
                )
                return None
            # 🔒 [JUDGE-ROUTE-INDEPENDENCE-0828] J2/J3/J4 공통 집행 경로.
            elif entry_type == "JUDGE":
                logger.info(
                    f"🟢 [ENTRY-JUDGE] {symbol} | "
                    f"source={indicators.get('judge_entry_source_type', 'NA')} "
                    f"J1={int(indicators.get('judge1_gajami_pass', 0) or 0)} "
                    f"J2={int(indicators.get('judge2_osc_pass', 0) or 0)} "
                    f"J3={int(indicators.get('judge3_cycle_pass', 0) or 0)} "
                    f"J4={int(indicators.get('judge4_reversal_pass', 0) or 0)} | "
                    f"score={final_score_capped:.3f} thr={_effective_threshold:.3f}"
                )
                final_signal = signal
            
            # TREND 진입 (추세 초입형 - ZBT)
            elif entry_type == "TREND":
                # TD_TREND_BLOCK은 legacy TREND lane 법률.
                # J234 독립 PASS는 전역차단하지 않는다.
                if regime == MarketRegime.TRENDING_DOWN and not _j234_hardcut_pass:
                    self.last_rejection_reason = "TD_TREND_BLOCKED"
                    logger.info(
                        f"🚫 [TD-TREND-BLOCK] {symbol} 하락장 순수상승지속(TREND) 진입 모순 → 차단 | "
                        f"regime={regime.value} entry_type=TREND "
                        f"rise_atr={_rise_atr_r:.2f}x score={final_score_capped:.3f}"
                    )
                    return None
                elif regime == MarketRegime.TRENDING_DOWN and _j234_hardcut_pass:
                    logger.info(
                        f"🟢 [TD-TREND-JUDGE-NEUTRAL] {symbol} "
                        f"legacy TREND veto 비적용 → J2/J3/J4 판결권 유지"
                    )
                # 🔒 [2026-08-03] TU_LOW_MOMENTUM 철거. 상단 게이트(L10005~)가
                #  dur<=4·체제별 limit로 rise/ATR 과열을 이미 판정하므로 하단 역방향
                #  veto는 중복·모순. KNEE 경로(아래)와 동시 철거.
                _det_t = indicators.get('detect_candle_time', '')
                logger.info(
                    f"🔵 [ENTRY-TREND] {symbol} | "
                    f"감지시각={_det_t} | "
                    f"route={_entry_auth_route} "
                    f"score={final_score_capped:.3f} "
                    f"{'≥' if _conf_ok else '<'} thr={_effective_threshold:.3f} | "
                    f"regime={regime.value} | "
                    f"entry_price={current_price:.0f}원 | "
                    f"stop={stop_loss:.0f}원 | target={take_profit:.0f}원"
                )
                final_signal = signal
                logger.info(
                    f"📋 [ENTRY-SUMMARY] {symbol} | "
                    f"regime={regime.value} "
                    f"dur={((((data_package.ma_data or {}).get('gajami_meta') or {}).get('gajami_lifecycle') or {}).get('dur', dur_5x10))} soleus={soleus_gajami} "
                    f"gajami={bool(indicators.get('shape_v2_gajami_ok',False))} "
                    f"score={final_score_capped:.3f} result=PASS stage=ENTRY-TREND"
                )
            
            # PULLBACK 진입 (메이저 눌림 반등형 - ETH)
            elif entry_type == "PULLBACK":
                _det_t = indicators.get('detect_candle_time', '')
                logger.info(
                    f"🟡 [ENTRY-PULLBACK] {symbol} | "
                    f"감지시각={_det_t} | "
                    f"route={_entry_auth_route} "
                    f"score={final_score_capped:.3f} "
                    f"{'≥' if _conf_ok else '<'} thr={_effective_threshold:.3f} | "
                    f"regime={regime.value} | "
                    f"entry_price={current_price:.0f}원 | "
                    f"stop={stop_loss:.0f}원 | target={take_profit:.0f}원"
                )
                final_signal = signal
            
            # KNEE 진입 (가자미근 초입형)
            elif entry_type == "KNEE":
                # 🔒 [2026-08-03] TU_LOW_MOMENTUM 철거 — TREND 경로(A-2)와 동일 사유.
                logger.info(
                    f"🦵 [ENTRY-KNEE] {symbol} | "
                    f"route={_entry_auth_route} "
                    f"score={final_score_capped:.3f} "
                    f"{'≥' if _conf_ok else '<'} thr={_effective_threshold:.3f} | "
                    f"regime={regime.value} | "
                    f"entry_price={current_price:.0f}원 | "
                    f"stop={stop_loss:.0f}원 | target={take_profit:.0f}원"
                )
                final_signal = signal
            
            # FAST 진입 (빠른 진입형)
            elif entry_type == "FAST":
                logger.info(
                    f"⚡ [ENTRY-FAST] {symbol} | "
                    f"route={_entry_auth_route} "
                    f"score={final_score_capped:.3f} "
                    f"{'≥' if _conf_ok else '<'} thr={_effective_threshold:.3f} | "
                    f"regime={regime.value} | "
                    f"entry_price={current_price:.0f}원 | "
                    f"stop={stop_loss:.0f}원 | target={take_profit:.0f}원"
                )
                final_signal = signal
            
            # RANGING-GAJAMI 진입 (final_signal 이미 설정됨)
            elif entry_type in ("RANGING_GAJAMI", "TD_BASELINE_SOLEUS", "HEAD_LIFT"):
                # ⚠️ [2026-07-31 정정] HIGH_NEAR_TOP_GUARD 제거 당시
                # "RANGING-TOP-BLOCK(box_pos>=0.95)이 담당" 근거는 사실이 아니다.
                # 해당 갈래는 07-23부터 False 고정이었고 box 계열은 실측상 무변별이다.
                # 새 꼭지 차단은 만들지 않고 J3·다이버전스·손절 경로에 맡긴다.
                pass
            
            # =====================================================
            # 🔒 SINGLE EXIT POINT (SSoT)
            # - 여기서만 detect_candle_time / gajami_lifecycle 주입
            # - 여기서만 last_entry_confidence 저장
            # =====================================================
            # 🦵 가자미근 진입 자격 Core Gate
            # Strategy fallback이 실제 판정 SSoT
            if final_signal is not None:
                # core_gate_mode 제거 — Phase 5 Step1에서 _cg_ok=_cg_strategy_ok 고정.
                # shadow 분기 불필요. 판정은 항상 block 동작.
                _cg_strategy_ok, _cg_strategy_reason = self._gajami_core_ok(
                    symbol=symbol,
                    indicators=indicators,
                    data_package=data_package,
                    current_price=current_price,
                    dur_5x10=int(dur_5x10),
                )

                _cg_src = "strategy_fallback"
                _cg_ok = _cg_strategy_ok
                _cg_reason = _cg_strategy_reason

                if (
                    _cg_probe_ok != _cg_strategy_ok
                    or _cg_probe_reason != _cg_strategy_reason
                ):
                    logger.warning(
                        f"⚠️ [CORE-FIRST-MISMATCH] {symbol} | "
                        f"early={int(_cg_probe_ok)}:{_cg_probe_reason} "
                        f"late={int(_cg_strategy_ok)}:{_cg_strategy_reason}"
                    )

                logger.info(
                    f"🦵 [CORE-GATE] {symbol} entry={entry_type} "
                    f"src={_cg_src} ok={int(_cg_ok)} reason={_cg_reason} | "
                    f"ctn_eff={int(bool(((data_package.ma_data or {}).get('gajami_meta') or {}).get('gajami_constitution_effective_ok', False)))}"
                )

                if not _cg_ok:
                    self.last_rejection_reason = f"GAJAMI_CORE_FAIL({_cg_reason})"
                    logger.info(
                        f"📋 [ENTRY-SUMMARY] {symbol} | "
                        f"regime={regime.value if regime else '?'} "
                        f"dur={((((data_package.ma_data or {}).get('gajami_meta') or {}).get('gajami_lifecycle') or {}).get('dur', dur_5x10))} soleus={soleus_gajami} "
                        f"gajami={bool(indicators.get('shape_v2_gajami_ok', False))} "
                        f"score={final_score_capped:.3f} "
                        f"block=GAJAMI_CORE_FAIL reason={_cg_reason}"
                    )
                    return None

                # 👁️ [2026-08-13] 직전 확정봉 품질 — 관측만. 차단 철거.
                #  pbody>=+0.30% 통과군이 승률 17.8%(n=46)로 차단군 30.3%(n=175)
                #  보다 낮고 무작위 모집단 22.8% 에도 미달 = 역변별.
                #  판정 소스는 t5 에서 관측용으로 유지, 여기는 로그만 남긴다.
                if self.timeframe.value == "3m":
                    _prev_bar_ready = bool(
                        int(safe_extract_scalar(
                            indicators.get("prev_bar_quality_ready", 0), 0
                        )) == 1
                    )
                    _prev_bar_body = float(safe_extract_scalar(
                        indicators.get("prev_bar_body_pct", SENTINEL_FLOAT),
                        SENTINEL_FLOAT,
                    ))
                    _prev_bar_vol = float(safe_extract_scalar(
                        indicators.get("prev_bar_volume_ratio", SENTINEL_FLOAT),
                        SENTINEL_FLOAT,
                    ))

                    logger.info(
                        f"👁️ [PREV-BAR-QUALITY] {symbol} | "
                        f"ready={int(_prev_bar_ready)} "
                        f"pbody={_prev_bar_body:+.3f}%(OBS) "
                        f"pvr={_prev_bar_vol:.3f}x(OBS) "
                        f"result=OBSERVE"
                    )

            # 🌊 [TLC-FRESH] 2026-08-05: 계산·판단·로그는 Analysis Final
            #  GajamiDecision(soleus_gajami)으로 일원화. Strategy 중복
            #  재판정 제거 — CORE-GATE 이후 재조사하던 구조가 SSoT 위반.
            #  (참고: 과거엔 여기서 OBSERVE 로그만 찍고 미차단 → 이후 잠깐
            #   return None 차단을 얹었다가, Analysis 합류점으로 재이관함)
               
            # 🔒 [BODY-GROWTH-J1-OWNER-0828]
            # body_growth는 Analysis의 J1 판결식이 소유한다. Strategy는 관측만 하며
            # J2/J3/J4 독립 PASS를 이 값으로 전역차단하지 않는다.
            _body_growth_ok = bool(
                indicators.get("shape_v2_body_growth", True)
            )
            _body_recent = float(indicators.get("shape_v2_body_recent", 0) or 0)
            _dur_bg_src = ((((data_package.ma_data or {}).get("gajami_meta") or {}).get("gajami_lifecycle") or {})).get("dur", None)
            _dur_bg = int(_dur_bg_src) if _dur_bg_src is not None else 999
            if final_signal is not None and not _body_growth_ok:
                _body_current_log = float(indicators.get("shape_v2_body_current", 0) or 0)
                _body_birth_current_log = bool(indicators.get("shape_v2_body_birth_current", False))
                logger.info(
                    f"🟡 [BODY-GROWTH-J1-LANE] {symbol} "
                    f"body_growth=0 body_recent={_body_recent:.3f} "
                    f"body_cur={_body_current_log:+.3f} birth_cur={int(_body_birth_current_log)} "
                    f"dur={_dur_bg} | "
                    f"J1={int(indicators.get('judge1_gajami_pass', 0) or 0)} "
                    f"J2={int(indicators.get('judge2_osc_pass', 0) or 0)} "
                    f"J3={int(indicators.get('judge3_cycle_pass', 0) or 0)} "
                    f"J4={int(indicators.get('judge4_reversal_pass', 0) or 0)} "
                    f"→ Strategy 전역차단 없음"
                )

            # 🔒 [FINAL-JUDGE-OR-0817] 모든 Hard Veto 통과 후 최종 긍정 관문.
            if final_signal is not None:
                _j1_ok = bool(int(indicators.get("judge1_gajami_pass", 0) or 0))
                # 🔒 [J2-STRATEGY-INGRESS-INTEGRITY-0902] ★479
                # ingress에서 보존한 원본 J2와 T5 provenance를 다시 대조한다.
                # 현재 judge2_osc_pass는 Strategy 권한 소비용 effective bit이다.
                _j2_raw = bool(
                    int(
                        indicators.get(
                            "judge2_osc_pass_raw",
                            indicators.get("judge2_osc_pass", 0),
                        )
                        or 0
                    )
                )
                _j2_verdict_raw = str(
                    indicators.get(
                        "judge2_osc_verdict_raw",
                        indicators.get("judge2_osc_verdict", "NEUTRAL"),
                    )
                    or "NEUTRAL"
                )
                _j2_effective = bool(
                    int(indicators.get("judge2_osc_pass", 0) or 0)
                )
                _j2_prov_ready = bool(
                    int(indicators.get("judge2_prov_ready", 0) or 0)
                )
                _j2_prov_pass = bool(
                    int(indicators.get("judge2_prov_pass", 0) or 0)
                )
                _j2_prov_verdict = str(
                    indicators.get("judge2_prov_verdict", "NEUTRAL")
                    or "NEUTRAL"
                )
                _j2_prov_bar = str(
                    indicators.get("judge2_prov_bar", "") or ""
                )
                _j2_prov_hidgo = bool(
                    int(indicators.get("judge2_prov_hidgo", 0) or 0)
                )
                _j2_shadow_bar = str(
                    indicators.get("indicator_shadow_bar", "") or ""
                )
                _j2_prov_ok = bool(
                    _j2_prov_ready
                    and _j2_raw == _j2_prov_pass
                    and _j2_verdict_raw == _j2_prov_verdict
                    and _j2_prov_pass == _j2_prov_hidgo
                    and _j2_prov_pass
                    == (_j2_prov_verdict == "PASS")
                    and _j2_prov_bar == _j2_shadow_bar
                )
                _j2_ok = bool(
                    _j2_effective
                    and _j2_prov_ok
                )
                if (
                    (_j2_raw or _j2_prov_pass)
                    and not _j2_prov_ok
                ):
                    logger.error(
                        f"🚫 [J2-PROV-MISMATCH] {symbol} "
                        f"raw={int(_j2_raw)} "
                        f"v2={_j2_verdict_raw} | "
                        f"pready={int(_j2_prov_ready)} "
                        f"pJ2={int(_j2_prov_pass)} "
                        f"pv2={_j2_prov_verdict} "
                        f"phidgo={int(_j2_prov_hidgo)} "
                        f"pbar={_j2_prov_bar or 'NA'} "
                        f"shadow={_j2_shadow_bar or 'NA'} | "
                        f"→ J2 표만 fail-closed"
                    )
                _j3_ok = bool(int(indicators.get("judge3_cycle_pass", 0) or 0))
                # 🔒 [JUDGE4-REVERSAL-0822]
                # regular bullish ∧ MACD<=0 = 하락 중 반등 판사.
                _j4_ok = bool(
                    int(indicators.get("judge4_reversal_pass", 0) or 0)
                )
                _judge_votes = (
                    int(_j1_ok)
                    + int(_j2_ok)
                    + int(_j3_ok)
                    + int(_j4_ok)
                )
                # 🔍 [JUDGE-SRC-OBSERVE-0826]
                # J2는 raw scalar와 Analysis 봉인값을 함께 출력한다.
                logger.info(
                    f"🔍 [JUDGE-SRC] {symbol} | "
                    f"J1={int(_j1_ok)} "
                    f"J2={int(_j2_ok)}(raw={int(_j2_raw)}) "
                    f"J3={int(_j3_ok)} J4={int(_j4_ok)} | "
                    f"has1={int('judge1_gajami_pass' in indicators)} "
                    f"has2={int('judge2_osc_pass' in indicators)} "
                    f"has3={int('judge3_cycle_pass' in indicators)} "
                    f"has4={int('judge4_reversal_pass' in indicators)} | "
                    f"v2={_j2_verdict_raw} "
                    f"pJ2={int(_j2_prov_pass)} "
                    f"pv2={_j2_prov_verdict} "
                    f"phidgo={int(_j2_prov_hidgo)} "
                    f"pbar={_j2_prov_bar or 'NA'} "
                    f"pok={int(_j2_prov_ok)} | "
                    f"v3={indicators.get('judge3_cycle_verdict', 'NOKEY')} "
                    f"shadow_bar={indicators.get('indicator_shadow_bar', 'NOKEY')} "
                    f"cur_bar={indicators.get('current_candle_time', 'NOKEY')}"
                )
                if not (_j1_ok or _j2_ok or _j3_ok or _j4_ok):
                    self.last_rejection_reason = "NO_JUDGE_APPROVAL"
                    logger.info(
                        f"🚫 [NO-JUDGE-APPROVAL] {symbol} 찬성 판사 없음 → 진입 금지 | "
                        f"J1={int(_j1_ok)} J2={int(_j2_ok)} "
                        f"J3={int(_j3_ok)} J4={int(_j4_ok)} | "
                        f"entry={entry_type} dur={dur_5x10} "
                        f"score={final_score_capped:.3f}"
                    )
                    return None

                # 🔒 [RR-FINAL-BLOCK-0817][FINAL-JUDGE-ELIGIBILITY-FIRST-0901] ★471
                # 판사 찬성 자격을 먼저 확정한 후보에 대해서만 최종 SL/TP 역전을 검사한다.
                # 판사 0표 후보가 RR_INVERTED로 먼저 종료되어
                # NO_JUDGE_APPROVAL first-blocker가 가려지는 것을 방지한다.
                _rr_sl = abs(current_price - stop_loss)
                _rr_tp = abs(take_profit - current_price)
                if (
                    current_price > 0.0
                    and _rr_sl > 0.0
                    and _rr_tp > 0.0
                    and _rr_tp <= _rr_sl
                ):
                    self.last_rejection_reason = "RR_INVERTED"
                    logger.info(
                        f"🚫 [RR-INVERTED-BLOCK] {symbol} 최종 손익비 역전 → 진입 금지 | "
                        f"SL={_rr_sl:.6g}({(_rr_sl / current_price * 100):.2f}%) "
                        f"TP={_rr_tp:.6g}({(_rr_tp / current_price * 100):.2f}%) | "
                        f"price={current_price:,.4f} tick={get_tick_size(current_price):g}"
                    )
                    return None

                # 🔒 [FINAL-JUDGE-LAST-0822] 여기서는 판사 「찬성표 존재」만 확인했다.
                #    실제 승인 로그는 후단 veto·STALE 실행가드 통과 뒤에 기록한다.
                # 🔒 [ENTRY-PREM-BLOCK-0819] 판사 찬성 후보만 실제 집행 직전 추격가를 검사한다.
                #    prem = (현재가 - 직전 확정봉 종가) / 직전 확정봉 종가 × 100.
                #    Analysis 기존 SSoT(prev_candle_close, atr_pct)만 소비 — 신규 지표 없음.
                #    근거: 모집단 77,327건 부호 9/9(EV -0.2530 vs 잔존 -0.1394),
                #          실거래 40건 -141,516원(부호 8/9).
                _prem_prev_close = float(
                    indicators.get("prev_candle_close", 0.0) or 0.0
                )
                _prem_atr_pct = float(
                    indicators.get("atr_pct", 0.0) or 0.0
                )
                _prem_pct = (
                    ((current_price - _prem_prev_close) / _prem_prev_close) * 100.0
                    if _prem_prev_close > 0.0 and current_price > 0.0
                    else 0.0
                )
                _prem_floor = max(0.40, 0.80 * _prem_atr_pct)
                # 🔬 [ENTRY-PREM-ANCHOR-GAP-OBSERVE-0904] ★505-O
                # PREM 원계약(prev_candle_close 대비)은 유지한다.
                # 다만 현재봉과 PREM 앵커(df.iloc[-2])의 실제 시간 간격을 기록해
                # 연속 3분봉과 무거래 gap 차단군의 성적을 분리 측정한다. 판정 변화 0.
                _prem_anchor_gap_sec = -1
                try:
                    _prem_cur_bar = pd.to_datetime(
                        indicators.get("current_candle_time"), errors="coerce"
                    )
                    _prem_prev_bar = pd.to_datetime(
                        indicators.get("indicator_shadow_bar"), errors="coerce"
                    )
                    if not pd.isna(_prem_cur_bar) and not pd.isna(_prem_prev_bar):
                        _prem_anchor_gap_sec = int(
                            (_prem_cur_bar - _prem_prev_bar).total_seconds()
                        )
                except Exception:
                    _prem_anchor_gap_sec = -1
                indicators["entry_prem_anchor_gap_sec"] = int(_prem_anchor_gap_sec)
                if _prem_prev_close > 0.0 and _prem_pct >= _prem_floor:
                    self.last_rejection_reason = "ENTRY_PREM_CHASE"
                    logger.info(
                        f"🚫 [ENTRY-PREM-BLOCK] {symbol} 직전 확정봉 대비 추격 진입 차단 | "
                        f"prem={_prem_pct:+.3f}% floor={_prem_floor:.3f}% "
                        f"atr={_prem_atr_pct:.3f}% gap={_prem_anchor_gap_sec}s "
                        f"J1={int(_j1_ok)} J2={int(_j2_ok)} J3={int(_j3_ok)} J4={int(_j4_ok)} | "
                        f"entry={entry_type} dur={dur_5x10}"
                    )
                    return None

                # 👁️ [BOX-BREAK-FAKEOUT-HARD-AUTH-PURGE-0829]
                # 직전 20봉 fixed range는 판사 승인 뒤 Strategy가 진입을 재판정하는
                # hard veto로 쓰지 않는다. 계산/로그는 사후 검증용 관측으로 유지한다.
                _bx_hi = float(indicators.get("recent_high_closed", 0.0) or 0.0)
                _bx_lo = float(indicators.get("recent_low_closed", 0.0) or 0.0)
                _box_break_fakeout_observe = False
                if _bx_hi > 0.0 and _bx_lo > 0.0 and _bx_hi > _bx_lo:
                    _bx_over = (current_price - _bx_hi) / _bx_hi * 100.0
                    _bx_h = (_bx_hi - _bx_lo) / _bx_lo * 100.0
                    _box_break_fakeout_observe = bool(
                        0.0 <= _bx_over < 0.05
                        and _bx_h < 2.0
                    )
                indicators["box_break_fakeout_observe"] = int(
                    _box_break_fakeout_observe
                )
                if _box_break_fakeout_observe:
                    logger.info(
                        f"👁️ [BOX-BREAK-FAKEOUT-OBSERVE] {symbol} 좁은박스 미세돌파 관측 | "
                        f"over={_bx_over:+.3f}% h={_bx_h:.2f}% "
                        f"hi={_bx_hi:.6g} lo={_bx_lo:.6g} | "
                        f"J1={int(_j1_ok)} J2={int(_j2_ok)} J3={int(_j3_ok)} J4={int(_j4_ok)} | "
                        f"entry={entry_type} dur={dur_5x10}"
                    )

                # 🔒 [ENTRY-ATR-SL-CAP-0823] 판사 승인+PREM 통과 후 진입 직전.
                # ATR 자체가 아니라 ATR이 만드는 손절 하한을 1개 cap으로 제한한다.
                # 최종 SL은 calculate_stops()의 기본 stop_loss_pct·호가/3틱 floor를 추가로 거친다.
                _entry_atr_mult = float(
                    self.settings.atr_stop_loss_multiplier or 0.0
                )
                _entry_atr_sl_cap = float(
                    self.config.getfloat(
                        "ATR_SETTINGS",
                        "max_entry_atr_sl_pct",
                        fallback=0.0,
                    )
                    or 0.0
                )
                _entry_atr_limit_pct = (
                    _entry_atr_sl_cap / _entry_atr_mult
                    if _entry_atr_sl_cap > 0.0 and _entry_atr_mult > 0.0
                    else 0.0
                )
                _entry_atr_sl_pct = _prem_atr_pct * _entry_atr_mult
                if (
                    _entry_atr_limit_pct > 0.0
                    and _prem_atr_pct >= _entry_atr_limit_pct
                ):
                    self.last_rejection_reason = "ENTRY_ATR_HIGH"
                    logger.info(
                        f"🚫 [ENTRY-ATR-HIGH-BLOCK] {symbol} ATR-SL 상한 차단 | "
                        f"atr={_prem_atr_pct:.3f}% "
                        f"atr_sl={_entry_atr_sl_pct:.3f}% "
                        f"cap={_entry_atr_sl_cap:.3f}% | "
                        f"J1={int(_j1_ok)} J2={int(_j2_ok)} J3={int(_j3_ok)} J4={int(_j4_ok)} | "
                        f"entry={entry_type} dur={dur_5x10} "
                        f"regime={regime.value if regime else 'None'}"
                    )
                    return None

            # 🔒 [BODY-GROWTH-J1-OWNER-0828]
            # 최종 승인 신호의 메타데이터 전달은 J1 body_growth와 무관한 공통 집행 책임이다.
            if final_signal is not None:
                # signal.indicators 안전 보장
                if (
                    not hasattr(final_signal, "indicators")
                    or final_signal.indicators is None
                ):
                    final_signal.indicators = {}
                if not isinstance(final_signal.indicators, dict):
                    final_signal.indicators = {}

                final_signal.indicators["judge_lane_authorized"] = int(
                    _j1_ok or _j2_ok or _j3_ok or _j4_ok
                )
                # 🔒 [JUDGE-VOTE-HANDOFF-0828]
                # Strategy가 실제 소비한 최종 판사표와 실행 route를 Bot까지 1:1 전달한다.
                final_signal.indicators["judge1_gajami_pass"] = int(_j1_ok)
                final_signal.indicators["judge2_osc_pass"] = int(_j2_ok)
                final_signal.indicators["judge3_cycle_pass"] = int(_j3_ok)
                final_signal.indicators["judge4_reversal_pass"] = int(_j4_ok)
                final_signal.indicators["judges"] = (
                    f"{int(_j1_ok)}{int(_j2_ok)}"
                    f"{int(_j3_ok)}{int(_j4_ok)}"
                )
                final_signal.indicators["entry_type"] = str(
                    entry_type or "NONE"
                )
                if "judge_entry_source_type" in indicators:
                    final_signal.indicators["judge_entry_source_type"] = (
                        indicators["judge_entry_source_type"]
                    )
                if "prev_candle_close" in indicators:
                    final_signal.indicators["prev_candle_close"] = (
                        indicators["prev_candle_close"]
                    )
                final_signal.indicators["effective_conf_threshold"] = float(
                    _effective_threshold
                )
                final_signal.indicators["entry_authorization_route"] = (
                    _entry_auth_route
                )
                _entry_dur_5x10 = safe_extract_scalar(
                    indicators.get("dur_5x10_bars", -1),
                    -1,
                )
                final_signal.indicators["dur_5x10_bars"] = int(
                    _entry_dur_5x10 if _entry_dur_5x10 >= 0 else -1
                )

                # 🔒 [OWNER-GAJAMI-3COND-BUYLOG-0828]
                # Analysis의 og3 관측 스냅샷을 실제 BUY 신호까지 전달한다.
                # 일반 진입(_build_entry_signal)과 HEAD_LIFT 모두
                # TradingSignal 생성 시 indicators를 새 dict로 만들기 때문에
                # owner_gajami_*가 자동 전달되지 않는다.
                # 판정에는 사용하지 않고 t7 실제 BUY 성공 로그용으로만 복사한다.
                for _og3_key in (
                    "owner_gajami_ready",
                    "owner_gajami_count",
                    "owner_gajami_v_prev",
                    "owner_gajami_surge_cur",
                    "owner_gajami_trend_hold",
                    "owner_gajami_varA",
                    "owner_gajami_varB",
                    "owner_gajami_ok",
                    "indicator_shadow_bar",
                    # 🔒 [J3-PROV-EXEC-HANDOFF-0903] ★501-R3
                    # T5 Producer provenance를 실제 BUY 로그까지 그대로 전달한다.
                    # 판정 재사용 없음 — calculate→judge→execution log 정합용.
                    # t7 L4142~4146에 이미 j3prov= 출력 코드가 있으나 화이트리스트
                    # 부재로 전 건 j3prov=-1/-1@NA:NA/ft=-1 이었다.
                    "judge3_prov_ready",
                    "judge3_prov_pass",
                    "judge3_prov_verdict",
                    "judge3_prov_bar",
                    "judge3_prov_det",
                    "judge3_prov_fake_top",
                    # 🔒 [RISE-DECISION-VAR-PARITY-0903] ★498-R1
                    # T5 판정변수를 T7 실제 BUY 로그까지 관측용으로만 중계한다.
                    # 이 dict는 _build_entry_signal()이 새로 만들기 때문에(L10934 주석)
                    # 화이트리스트에 넣지 않으면 T7에서 전 건 na가 된다.
                    "gajami_rise_cur_pct",
                    "gajami_rise_atr_ratio",
                    # 🔒 [TLC-POS-BUYLOG-0828-R1]
                    # window / 실제 parent swing / active leg 위치를
                    # Bot 실제 BUY 로그까지 동일 이름으로 전달한다.
                    "cycle_window_pos",
                    "cycle_parent_ready",
                    "cycle_parent_pos",
                    "cycle_parent_pos_closed",
                    "cycle_parent_low",
                    "cycle_parent_high",
                    "cycle_parent_wave_id",
                    # 🔒 [J1-PARENT-EXEC-HANDOFF-0829] 5-5
                    # Analysis의 J1 parent 상태를 실제 BUY signal까지 전달한다.
                    # 판정은 하지 않고 Bot의 실제 체결 feedback용으로만 복사한다.
                    "j1_low_window_open",
                    "j1_wave_state",
                    "cycle_active_pos_closed",
                    "cycle_active_progress",
                    "cycle_active_leg_bars",
                ):
                    if _og3_key in indicators:
                        final_signal.indicators[_og3_key] = indicators[_og3_key]

                # 🔧 감지시각 = 이미 설정되어 있으면 유지 (최초 판단 시각 보존)
                if "detect_candle_time" not in final_signal.indicators or not final_signal.indicators["detect_candle_time"]:
                    # 🔒 PATCH-3: 감지시각 = 진입 확정 시각 (항상 현재)
                    final_signal.indicators["detect_candle_time"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                
                # 2) gajami_lifecycle 전달 (Bot 로그용)
                try:
                    gmeta = (data_package.ma_data or {}).get("gajami_meta") or {}
                    glc = gmeta.get("gajami_lifecycle", None)
                    if isinstance(glc, dict):
                        final_signal.indicators["gajami_lifecycle"] = glc
                except Exception:
                    pass
                
                # 3) 매수 시점 신뢰도 저장 (매도 시 재사용)
                self.last_entry_confidence[symbol] = final_score_capped

                # 📊 SHAPE-V2-ENTRY 관측
                try:
                    logger.info(
                        f"📊 [SHAPE-V2-ENTRY] {symbol} | "
                        f"gajami_ok={indicators.get('shape_v2_gajami_ok', 'NA')} | "
                        f"body_growth={indicators.get('shape_v2_body_growth', 'NA')} | "
                        f"body_recent={float(indicators.get('shape_v2_body_recent', 0.0) or 0.0):.5f} | "
                        f"body_past={float(indicators.get('shape_v2_body_past', 0.0) or 0.0):.5f} | "
                        f"head={indicators.get('shape_v2_head_ok', 'NA')} | "
                        f"lead={indicators.get('shape_v2_lead_ok', 'NA')} | "
                        f"align={indicators.get('shape_v2_align_ok', 'NA')} | "
                        f"source={indicators.get('shape_v2_source', 'NA')} | "
                        f"eee={float(indicators.get('eee_confidence', 0.0) or 0.0):.3f} | "
                        f"final={final_score_capped:.3f} | "
                        f"soleus={int(bool(soleus_gajami))}"
                    )
                except Exception:
                    pass

            # 🆕 실주문 차단
            if final_signal is not None and not allow_execution:
                logger.warning(
                    f"[STALE-BLOCK] {symbol} 실주문 차단 "
                    f"| 신뢰도={final_score_capped:.3f}"
                )
                self.last_rejection_reason = "STALE_EXEC_BLOCK"
                return None
            # 🔒 [FINAL-JUDGE-LAST-0822] 판사 승인은 실제 반환 직전의 최종 긍정 판결.
            #    이 로그 뒤에는 진입을 뒤집는 전략/실행 차단 게이트가 없다.
            if final_signal is not None:
                logger.info(
                    f"🎫 [JUDGE-APPROVAL] {symbol} 찬성 {_judge_votes}표 | "
                    f"J1={int(_j1_ok)} J2={int(_j2_ok)} "
                    f"J3={int(_j3_ok)} J4={int(_j4_ok)} | "
                    f"entry={entry_type} dur={dur_5x10}"
                )
            # ━━ STRATEGY FINAL DECISION ━━
            if final_signal is None:
                self.last_rejection_reason = f"NO_FINAL_SIGNAL(soft={self.last_rejection_reason or '-'})"
            else:
                self.last_rejection_reason = ""
            _st_reason = "PASS" if final_signal is not None else (self.last_rejection_reason or "UNKNOWN")
            logger.info(
                f"[STRATEGY-DECISION] {symbol} | "
                f"soleus={int(bool(soleus_gajami))} "
                f"score={final_score_capped:.3f} "
                f"thr={_effective_threshold:.3f} "
                f"route={_entry_auth_route} "
                f"entry_type={entry_type} "
                f"final={'PASS' if final_signal is not None else 'BLOCK'} | "
                f"reason={_st_reason}"
            )
            # 📊 ER-TAG — 최종 결정 후 관측 전용 (스코어 미반영)
            try:
                _er_tag_val = float(indicators.get('er_10', 0.0) or 0.0)
                _er_tag = (
                    "GOOD" if _er_tag_val >= 0.60 else
                    "BAD"  if _er_tag_val <= 0.30 else
                    "MID"
                )
                _final_result = "ENTRY" if final_signal is not None else "BLOCK"
                logger.info(
                    f"📊 [ER-TAG] {symbol} ER={_er_tag_val:.3f} → {_er_tag} | "
                    f"score={final_score_capped:.3f} result={_final_result}"
                )
            except Exception:
                pass
            return final_signal
            
        except OSError as e:
            # WinError 233: stdout 파이프 닫힘 (무시)
            if '233' in str(e) and '파이프' in str(e):
                self.last_rejection_reason = "시스템 에러 (무시)"
                return None
            else:
                logger.warning(f"[신호분석] {data_package.market} OSError: {e}")
                self.last_rejection_reason = f"OSError: {str(e)}"
                return None
        
        except Exception as e:
            error_msg = str(e)
            # WinError 233도 체크
            if '233' in error_msg and '파이프' in error_msg:
                self.last_rejection_reason = "시스템 에러 (무시)"
                return None
            
            logger.error(f"[신호분석] {data_package.market} 매수 조건 체크 오류: {e}")
            import traceback
            logger.debug(f"상세 오류:\n{traceback.format_exc()}")
            
            self.last_rejection_reason = f"오류 발생: {str(e)}"
            try:
                logger.info(
                    f"📊 [SHAPE-V2-BLOCK] {symbol} | stage=FINAL_FALLBACK | "
                    f"gajami_ok={indicators.get('shape_v2_gajami_ok', 'NA')} | "
                    f"body_growth={indicators.get('shape_v2_body_growth', 'NA')} | "
                    f"body_recent={float(indicators.get('shape_v2_body_recent', 0.0) or 0.0):.5f} | "
                    f"body_past={float(indicators.get('shape_v2_body_past', 0.0) or 0.0):.5f} | "
                    f"head={indicators.get('shape_v2_head_ok', 'NA')} | "
                    f"lead={indicators.get('shape_v2_lead_ok', 'NA')} | "
                    f"align={indicators.get('shape_v2_align_ok', 'NA')} | "
                    f"source={indicators.get('shape_v2_source', 'NA')} | "
                    f"eee={float(indicators.get('eee_confidence', 0.0) or 0.0):.3f} | "
                    f"final={float(locals().get('final_score_capped', 0.0) or 0.0):.3f} | "
                    f"soleus={int(bool(locals().get('soleus_gajami', False)))} | "
                    f"reason={self.last_rejection_reason}"
                )
            except Exception:
                pass

            return None
    
    async def _check_sell_conditions(self, position: Position, market: str,
                                current_price: float, market_regime: MarketRegime,
                                ma_data: Dict, df: pd.DataFrame = None,
                                indicators: Dict = None) -> Optional[TradingSignal]:
        """매도 조건 체크 - 부분 매도 우선        
        Args:
            position: 현재 포지션
            market: 종목 코드
            current_price: 현재가
            market_regime: 시장 체제
            ma_data: MA 데이터
            df: 캔들 데이터 (저항선 탐지용, optional)            
        Returns:
            TradingSignal 또는 None
        """
        # 🔒 안전 초기화: 매도 로직에서도 indicators dict 보장 (NameError 방지)
        # 우선순위: 1) 파라미터로 전달된 indicators, 2) ma_data의 indicators, 3) ma_data 자체
        if indicators is None:
            indicators = {}
            if isinstance(ma_data, dict):
                _cand = ma_data.get("indicators")
                indicators = _cand if isinstance(_cand, dict) else ma_data
        
        # mr_regime 존재 확인 로그 + Exit 체제 SSoT 정합
        if "mr_regime" in indicators:
            _mr_regime = indicators["mr_regime"]
            logger.debug(
                f"✅ [MR-REGIME-FOUND] {market} "
                f"mr_regime={_mr_regime.value if hasattr(_mr_regime, 'value') else _mr_regime}"
            )
            if isinstance(_mr_regime, MarketRegime) and _mr_regime != market_regime:
                logger.warning(
                    f"🔁 [SELL-REGIME-SSOT] {market} | "
                    f"exit체제={market_regime.value if hasattr(market_regime, 'value') else market_regime} "
                    f"→ mr_regime={_mr_regime.value}"
                )
                market_regime = _mr_regime
        else:
            logger.warning(f"⚠️ [MR-REGIME-MISSING] {market} indicators keys={list(indicators.keys())}")
        
        # 🔑 매수 시점 신뢰도 가져오기 (매도 신호에 사용)
        # - 매도 신호의 confidence는 매수 시점 진입 품질을 그대로 반영
        # - 다단계 폴백: position 속성 → 메모리 저장값 → 기본값 0.5
        # - 안전장치: None 방지로 NameError/TypeError 차단
        # 🔒 PATCH ④: 매수 신뢰도 가져오기 (우선순위 수정)
        # 1순위: indicators['entry_confidence'] (신호 생성 시 저장)
        # 2순위: position.entry_confidence
        # 3순위: last_entry_confidence 딕셔너리
        # 4순위: 기본값 0.5
        entry_conf = float(
            indicators.get('entry_confidence', 0.0) or  # ← 1순위 추가
            getattr(position, "entry_confidence", 0.0) or 
            self.last_entry_confidence.get(market, 0.0) or 
            0.5
        )
        
        # 디버그 로그
        logger.debug(
            f"[매도신뢰도] {market} | "
            f"indicators={indicators.get('entry_confidence', 'N/A')} | "
            f"position={getattr(position, 'entry_confidence', 'N/A')} | "
            f"memory={self.last_entry_confidence.get(market, 'N/A')} | "
            f"final={entry_conf:.3f}"
        )
        
        loss = position.get_return_pct(current_price)

        # ── SSoT 닫힌봉 스냅샷 (1회 산출 → 출구 소비) ──────────────────
        _closed_price = None
        try:
            _v = indicators.get("prev_candle_close", None)
            if _v is not None and float(_v) > 0:
                _closed_price = float(_v)
        except Exception:
            _closed_price = None

        if _closed_price is None and df is not None and len(df) >= 2:
            try:
                _closed_price = float(df["close"].iloc[-2])
            except Exception:
                _closed_price = None

        _entry_price = float(getattr(position, "avg_price", 0.0) or 0.0)

        if _closed_price is not None and _entry_price > 0:
            _closed_profit_ratio = (_closed_price - _entry_price) / _entry_price
            _closed_loss_pct = _closed_profit_ratio * 100.0
        else:
            _closed_profit_ratio = None
            _closed_loss_pct = None
        # ─────────────────────────────────────────────────────────────────
        
        # ========================================
        # 매도 분석 로그 헤더
        # ========================================
        logger.info(f"[매도분석] {market} ━━━━━━━━━━━━━━━━━━━━")
        logger.info(
            "[매도분석] %s tick현재가: %.2f | tick수익률: %+.2f%% | "
            "closed가격: %s | closed수익률: %s | 보유시간: %.1f시간",
            market,
            current_price,
            loss,
            f"{_closed_price:.2f}" if _closed_price is not None else "N/A",
            f"{_closed_loss_pct:+.2f}%" if _closed_loss_pct is not None else "N/A",
            (datetime.now() - position.entry_time).total_seconds() / 3600
        )
        
        # ========================================
        # 1. 긴급 청산 체크 (최우선!)
        # ========================================
        for threshold, ratio in sorted(self.signal_config['emergency_exits'].items()):
            if loss < threshold:
                logger.warning("🚨 긴급청산 발동: %s | 손실:%.1f%% < %.1f%%", market, loss, threshold)
                
                return TradingSignal(
                    symbol=market, action="SELL", price=current_price,
                    reason=f"긴급청산 {loss:.1f}", confidence=entry_conf,
                    sell_ratio=ratio, market_regime=market_regime,
                    urgency="critical",
                    indicators={}
                )
        
        # ======================================================
        # 🔒 MA15 보호 게이트 (손절 평가 전에 판정)
        #     - MA15 위에서는 손절 평가 자체를 하지 않음
        #     - 하류 로직(BOX EXIT/MA15 구조 이탈/타임아웃/부분익절)이 정상 작동
        # ======================================================
        _ma15_for_sl = None
        if ma_data:
            _sl_mas = ma_data.get('current_mas', {}) if isinstance(ma_data, dict) else {}
            _ma15_for_sl = _sl_mas.get('ma15') if isinstance(_sl_mas, dict) else None
        
        # MA15를 틱 그리드로 정규화 (이산 가격 vs 연속 MA 해상도 정합)
        _ma15_tick_floored = _ma15_for_sl
        if _ma15_for_sl is not None and _ma15_for_sl > 0:
            _tick_sl = get_tick_size(current_price)
            if _tick_sl > 0:
                _ma15_tick_floored = round(_ma15_for_sl / _tick_sl) * _tick_sl
        
        _ma15_protected = (
            _ma15_for_sl is not None
            and _ma15_for_sl > 0
            and current_price >= _ma15_tick_floored
        )
        
        # 🔒 [2026-08-03] E2+E3 누락분. position.stop_loss 를 ATR 연동으로 넓혔는데
        #  이 캡만 config 고정 0.5% 라 L11710 OR 에서 항상 이겨 ATR 손절이 무력화됐다.
        #  실증 HOLO 15:00: [포지션생성] 손절 98.20(-1.60%) 인데 -0.50% 에서 청산(8초).
        #        ARDR 14:23: 손절 25.30(-1.94%) 인데 -0.78% 에서 청산(70초).
        #  L11713 매수후 30초 유예도 _sl_hard_cap_hit 이면 무효화되어 함께 죽었다.
        _sp_cap = float(getattr(position, "stop_loss", 0.0) or 0.0)
        _SL_HARD_CAP = (
            -((position.avg_price - _sp_cap) / position.avg_price) * 100.0
            if (_sp_cap > 0.0 and _sp_cap < position.avg_price)
            else -abs(float(self.settings.stop_loss_pct)) * 100.0
        )
        _stop_price_hit = bool(current_price <= position.stop_loss)
        _sl_hard_cap_hit = bool(loss <= _SL_HARD_CAP)
        if _ma15_protected and _stop_price_hit:
            # -0.5% ~ -0.8%: MA15 보호 유지
            if not getattr(position, "_ma15_sl_protected", False):
                position._ma15_sl_protected = True
                logger.info(
                    f"🛡️ [MA15-SL-PROTECT] {market} 손절가 도달이나 MA15 상위 → 손절 SKIP | "
                    f"현재가={current_price:.0f} > MA15={_ma15_for_sl:.0f} | "
                    f"손절가={position.stop_loss:.0f} | 손익={loss:+.2f}% | cap={_SL_HARD_CAP:.1f}%"
                )
            # return 없음 → 기존처럼 하류 직행

        # ========================================
        # 1-1. 손절 체크 (MA15 이탈 또는 hard cap 초과)
        # ========================================
        elif _stop_price_hit or _sl_hard_cap_hit:
            _held_sec_grace = (datetime.now() - position.entry_time).total_seconds()
            _post_buy_grace_sec = 30
            # 🔒 [GRACE-REVIVE-0818] _SL_HARD_CAP 이 position.stop_loss 기반이라
            #    _stop_price_hit 과 항상 같아진다(L10680~10686).
            #    `not _sl_hard_cap_hit` 이 항상 False 라 30초 유예가 死였다.
            _post_buy_sl_tick_grace = bool(
                _held_sec_grace < _post_buy_grace_sec
            )

            if _post_buy_sl_tick_grace:
                logger.info(
                    f"🛡️ [POST-BUY-SL-TICK-GRACE] {market} | "
                    f"매수 후 {_held_sec_grace:.0f}초 < {_post_buy_grace_sec}초 "
                    f"→ tick SL 보류 | "
                    f"현재가={current_price:.2f} 손절가={position.stop_loss:.2f} "
                    f"손익={loss:.2f}% cap={_SL_HARD_CAP:.1f}%"
                )
            else:
                if _sl_hard_cap_hit:
                    logger.info(
                        f"🚨 [SL-HARD-CAP] {market} 절대 손실캡 강제청산 | "
                        f"loss={loss:.2f}% <= cap={_SL_HARD_CAP:.2f}% | "
                        f"stop_hit={int(_stop_price_hit)} "
                        f"ma15_protected={int(_ma15_protected)} | "
                        f"현재가={current_price:.2f} "
                        f"손절가={position.stop_loss:.2f}"
                    )
                if hasattr(position, "_ma15_sl_protected"):
                    del position._ma15_sl_protected
                logger.warning(
                    f"⛔ [손절] {market} 손절가 도달 | "
                    f"손익={loss:.2f}% | "
                    f"손절가={position.stop_loss:.0f}원 >= 현재가={current_price:.0f}원"
                    f"{f' | MA15={_ma15_for_sl:.0f}원' if _ma15_for_sl else ''}"
                )
                try:
                    _held_min = (datetime.now() - position.entry_time).total_seconds() / 60.0
                    _entry_rise_atr = float(
                        indicators.get("entry_rise_atr",
                        getattr(position, "entry_rise_atr", -1.0)) or -1.0
                    )
                    _exit_closed = float(indicators.get("prev_candle_close", current_price) or current_price)
                    _tick_vs_closed = (
                        (current_price - _exit_closed) / _exit_closed * 100.0
                        if _exit_closed > 0 else 0.0
                    )
                    logger.info(
                        f"🧾 [EXIT-FORENSIC] {market} | "
                        f"reason=손절 basis=tick "
                        f"exit_tick={current_price:.2f} exit_closed={_exit_closed:.2f} "
                        f"gap={_tick_vs_closed:+.3f}% "
                        f"| held={_held_min:.1f}m pnl={loss:.2f}% "
                        f"entry_rise_atr={_entry_rise_atr:.2f}x"
                    )
                except Exception as _exit_forensic_e:
                    logger.debug(f"[EXIT-FORENSIC-ERR] {market} {_exit_forensic_e}")
                return TradingSignal(
                    symbol=market, action="SELL", price=current_price,
                    reason=f"손절 {loss:.1f}", confidence=entry_conf,
                    market_regime=market_regime, urgency="critical",
                    indicators={}
                )
        
        # ========================================
        # 🆕 1-1.7. [WHIP-CUT] 휩소 조기탈출 — 진입 해방의 보완책 (초석)
        # 휩소=진입직후 확정봉 MA5<MA10(DC역전) ∧ loss<=0. 찐=MA5>MA10·이익→미발동(라이드).
        # MA15-Exit의 3봉(9분) early-lock 구멍을 메움. loss>0이면 절대 미발동=승자보호.
        # ========================================
        try:
            if ma_data and loss <= 0.0:
                _wc_prev = ma_data.get('prev_mas') or {}
                _wc_ma5  = _wc_prev.get('ma5')
                _wc_ma10 = _wc_prev.get('ma10')
                _wc_held = (datetime.now() - position.entry_time).total_seconds()
                _wc_tf   = int(getattr(self, "tf_seconds", 180) or 180)
                _wc_in_window = bool(_wc_tf <= _wc_held <= _wc_tf * 4)   # 진입 후 1~4봉
                if (_wc_in_window
                        and _wc_ma5 is not None and _wc_ma10 is not None
                        and float(_wc_ma5) < float(_wc_ma10)):
                    logger.info(
                        f"⚡ [WHIP-CUT] {market} 휩소 조기탈출 | "
                        f"진입후 {_wc_held:.0f}초 확정봉 MA5({_wc_ma5:.0f})<MA10({_wc_ma10:.0f}) ∧ loss={loss:.2f}% "
                        f"→ 휩소확정 컷 (찐이면 MA5>MA10·이익이라 미발동)"
                    )
                    return TradingSignal(
                        symbol=market, action="SELL", price=current_price,
                        reason=f"WHIP-CUT {loss:.1f}", confidence=entry_conf,
                        market_regime=market_regime, urgency="high",
                        indicators={}
                    )
        except Exception as _whip_e:
            logger.debug(f"[WHIP-CUT-ERR] {market} {_whip_e}")

        # ========================================
        # 🆕 1-1.5. BOX TRADE EXIT PRIORITY (박스 청산 최우선)
        # ========================================
        # 철학: box_trade_active == True이면 청산 주권은 박스가 가짐
        # 우선순위: 긴급청산/손절 다음, 모든 추세 청산보다 우선
        # ========================================
        
        # ======================================================
        # 🆕 BOX/TD-BASELINE 진입 보호 (MA15-Exit 차단)
        # ======================================================
        # BOX-SUPPORT 또는 TD-BASELINE 진입은 최소 보유 시간 확보
        # MA15-Exit은 최소 3분 이후에만 발동
        # ======================================================
        # entry_reason, skip_ma15_exit 제거 - BOX-SUPPORT 보호 블록 제거됨
        
        # (1) 저항 도달 → 즉시 청산 (최우선)
        # (2) 중앙선 회귀 실패 → 조기 정리 (차선)
        # ========================================
        
        # box_trade_active 블록 제거 - True 설정하는 곳 없음 (항상 False)
        
        # 1-0. Runner Break-Even Stop (1차익절 후 잔여물량 본전 보호)
        _ms_partial = int(getattr(position, "partial_sell_count", 0) or 0)
        if _ms_partial >= 1:
            _ms_floor = position.avg_price
            if current_price <= _ms_floor:
                _ms_profit = (current_price - position.avg_price) / position.avg_price * 100
                logger.info(
                    f"🔒 [Runner-MoveStop] {market} 1차익절 후 본전 보호 | "
                    f"floor={_ms_floor:.0f}(본전) cur={current_price:.0f} "
                    f"수익={_ms_profit:+.2f}% 부분={_ms_partial}"
                )
                return TradingSignal(
                    symbol=market, action="SELL", price=current_price,
                    reason=f"러너본전보호 {_ms_profit:+.1f}%", confidence=entry_conf,
                    sell_ratio=1.0,
                    market_regime=market_regime, urgency="medium", indicators={}
                )

        # ========================================
        # 1-2. TD Break-even Lock (본전 보호)
        # ========================================
        # 목적: +0.4% 이상 수익 후 본전 이하 하락 시 즉시 청산
        # 효과: KRW-SUI 05:40~06:00 케이스 방지 (0% 근처 6시간 방치)
        # 우선순위: 손절 다음, 타임아웃보다 우선
        
        # max_profit_pct 초기화 및 갱신 (TD BE-Lock용)
        current_profit_pct = (current_price - position.avg_price) / position.avg_price
        
        # 최초 접근 시 초기화
        if not hasattr(position, "max_profit_pct"):
            position.max_profit_pct = current_profit_pct
        else:
            # 최대 수익률 갱신 (TD BE-Lock용 tick 기반 유지)
            position.max_profit_pct = max(position.max_profit_pct, current_profit_pct)

        # 닫힌봉 고점 별도 추적 (TU-Giveback 전용, TD BE-Lock 회귀 없음)
        if _closed_profit_ratio is not None:
            # 🆕 [phantom-peak 방지] prev확정봉이 진입 後 마감(df.index[-1] > entry_time)일 때만 반영
            _bar_after_entry = False
            try:
                if df is not None and len(df) >= 1 and getattr(position, "entry_time", None) is not None:
                    _bar_after_entry = bool(df.index[-1] > position.entry_time)
            except Exception:
                _bar_after_entry = False
            if _bar_after_entry:
                if not hasattr(position, "max_profit_closed"):
                    position.max_profit_closed = _closed_profit_ratio
                else:
                    position.max_profit_closed = max(position.max_profit_closed, _closed_profit_ratio)
        
        if market_regime == MarketRegime.TRENDING_DOWN:
            if position.max_profit_pct >= 0.004:  # +0.4% 이상 도달했었으면
                fee_buffer = 0.001  # 수수료 버퍼 0.1%
                be_price = position.avg_price * (1.0 - fee_buffer)
                
                if current_price <= be_price:
                    logger.warning(
                        f"🔒 [TD-BE-Lock] {market} 본전 보호 | "
                        f"최고: +{position.max_profit_pct*100:.2f}% → "
                        f"현재: {loss:.2f}% | "
                        f"본전가: {be_price:.0f}원"
                    )
                    return TradingSignal(
                        symbol=market, action="SELL", price=current_price,
                        reason=f"TD본전잠금 {loss:.1f}", confidence=entry_conf,
                        market_regime=market_regime, urgency="medium",
                        indicators={}
                    )
        
        # 🔒 RANGING 전용 본전 잠금 (완화 + 동력 소진 조건)
        # ========================================
        # 목적: 횡보 +0.6% 도달 후 중단기 동력 소진이 확인될 때만 본전 보호
        # 차별점: 추세 진입 포지션은 면제하고, MA5/MA10 동시 하락 전환만 인정
        # 효과: 일시적 MA5 흔들림에 의한 조기 청산 방지
        # 한계: +0.6% 미달 케이스(POKT)는 발동 안 함 (의도)
        # ========================================
        if market_regime == MarketRegime.RANGING:
            if position.max_profit_pct >= 0.006:  # +0.6% 이상 도달
                ma5_slope_pct = float(indicators.get("ma5_slope", 0.0) or 0.0)
                _ma10c = float(safe_extract_scalar(indicators.get("ma10_slope_exit_closed", SENTINEL_FLOAT), SENTINEL_FLOAT))
                ma10_slope_pct = float(indicators.get("ma10_slope", 0.0) or 0.0) if is_sentinel(_ma10c) else _ma10c

                _entry_regime = getattr(position, "entry_regime", None)
                _entry_regime_key = str(
                    getattr(_entry_regime, "name", getattr(_entry_regime, "value", _entry_regime)) or ""
                ).upper()

                # 🔒 추세 진입 포지션은 RANGING 단독 본전잠금으로 청산하지 않는다.
                _trend_entry = _entry_regime_key in ("TRENDING_UP", "BREAKOUT", "BULL_MARKET")

                # 동력 소진: MA5 + MA10이 둘 다 의미 있게 꺾였을 때만 본전잠금
                _rg_be_momentum_dead = (
                    not _trend_entry
                    and ma5_slope_pct < -0.30
                    and ma10_slope_pct < -0.20
                )

                if _rg_be_momentum_dead:
                    be_price = position.avg_price * 1.0005  # +0.05% 확보
                    if current_price <= be_price:
                        logger.warning(
                            f"🔒 [RG-BE-Lock] {market} 횡보 본전 보호 | "
                            f"최고: +{position.max_profit_pct*100:.2f}% → "
                            f"MA5({ma5_slope_pct:+.2f}%)+MA10({ma10_slope_pct:+.2f}%) 하락 전환 | "
                            f"entry_regime={_entry_regime_key or 'N/A'} | "
                            f"본전가: {be_price:.0f}원"
                        )
                        return TradingSignal(
                            symbol=market, action="SELL", price=current_price,
                            reason="RG본전잠금", confidence=entry_conf,
                            market_regime=market_regime, urgency="medium",
                            indicators={}
                        )
        
        # 1-3. ⭐ MA15 구조 이탈 (Config 기반 단기 TF 조기 청산)
        # 철학: 단기 타임프레임 = 빠른 진출입
        # 조건: MA15>=MA20(중기 구조 유지) + breach threshold 충족 + confirm(FAST/PREV_1BAR/CUR_PREV_2BAR)
        # 목적: 중기 추세 사망을 확정봉 기준으로 빠르게 감지하되, 인트라바 잡음은 배제
        # 우선순위: 타임아웃보다 먼저 체크 (OR 구조)
        # 설정: config.ini [MA_EXIT] 섹션
        #   - enable_15min_ma_exit: 활성화 여부
        #   - applicable_timeframes: 적용 TF (1m,3m,5m)
        if (self.enable_15min_ma_exit and 
            self.timeframe.value in self.ma_exit_timeframes and 
            ma_data):
            
            # 🔒 진입 직후 즉사 방지: 1캔들 그레이스
            # 🆕 [MA15-EXIT-GRACE-FIX] 실제 캔들 길이 기준 복원
            # 3m=60초는 1/3캔들 → 승자 조기절단 원인. SSoT(self.tf_seconds) 활용.
            _tf_sec_map = {
                "1m": 60, "3m": 180, "5m": 300,
                "10m": 600, "15m": 900,
                "30m": 1800, "1h": 3600, "4h": 14400, "1d": 86400,
            }
            grace_sec = int(_tf_sec_map.get(
                self.timeframe.value,
                getattr(self, "tf_seconds", 180) or 180
            ))
            holding_seconds = (datetime.now() - position.entry_time).total_seconds()

            # 3캔들 early-lock: 갓 진입 구간 FAST/1봉확정 금지
            _ma15_early_lock_sec = grace_sec * 3
            _ma15_early_locked = bool(holding_seconds < _ma15_early_lock_sec)
            
            if holding_seconds < grace_sec:
                # 1캔들 미만: MA15 구조 이탈 청산 건너뛰기
                logger.info(
                    f"🔒 [MA15-Grace] {market} | "
                    f"진입 후 {holding_seconds:.0f}초 < {grace_sec}초 → MA15-Exit 보류"
                )
            else:
                # 1캔들 이상: 정상 MA15-Exit 로직 실행
                # 🔒 완성봉(prev_mas) 우선: 인트라바 흔들림으로 인한 조기 손절 방지
                # 첫 3봉 early-lock은 _ma15_early_locked(L12085)가 단독 소유
                _prev = ma_data.get('prev_mas') or {}
                _cur  = ma_data.get('current_mas') or {}
                mas_for_exit = _prev or _cur

                _exit_mas   = mas_for_exit or {}
                _exit_basis = "NONE" if mas_for_exit is None else ("PREV" if mas_for_exit is _prev else "CUR")
                _prev_ma5  = (_prev or {}).get('ma5')
                _prev_ma10 = (_prev or {}).get('ma10')
                _prev_ma20 = (_prev or {}).get('ma20')

                ma5  = _exit_mas.get('ma5')
                ma15 = _exit_mas.get('ma15')
                ma20 = _exit_mas.get('ma20')
                if ma15 is None:
                    _em10 = _exit_mas.get('ma10')
                    _em20 = _exit_mas.get('ma20')
                    if _em10 is not None and _em20 is not None:
                        ma15 = (_em10 + _em20) / 2

                if ma5 is not None and ma15 is not None and ma15 > 0:
                    ma15_breach_pct = (ma5 - ma15) / ma15 * 100.0

                    _prev_bar_breach = False
                    try:
                        if _prev_ma5 is not None and _prev_ma10 is not None and _prev_ma20 is not None:
                            _prev_ma15 = (_prev_ma10 + _prev_ma20) / 2
                            _prev_bar_breach = (_prev_ma5 < _prev_ma15)
                    except Exception:
                        _prev_bar_breach = False

                    _exit_ma20    = float(ma20 or 0)
                    _exit_bar_breach = bool(ma5 < ma15)
                    _fast_breach  = bool(ma15_breach_pct <= -0.20)
                    _thr_breach   = bool(ma15_breach_pct <= -self.ma15_breach_threshold)
                    _ma20_gate    = bool(ma15 is not None and _exit_ma20 > 0)

                    _confirm_mode = "NONE"

                    if _ma15_early_locked:
                        # 🆕 [승자 조기절단 방지] ≤3캔들: MA15-Exit 전체 보류
                        # prev2_mas 미제공 → 진짜 2봉 확정 판단 불가.
                        # 진짜 급락은 손절(stop)이 담당.
                        _confirm_mode = "NONE"
                    else:
                        if _fast_breach:
                            _confirm_mode = "FAST"
                        elif _exit_basis == "PREV" and _exit_bar_breach:
                            _confirm_mode = "PREV_1BAR"
                        elif _exit_basis == "CUR" and _prev_bar_breach:
                            _confirm_mode = "CUR_PREV_2BAR"

                    _confirm_ok = _confirm_mode != "NONE"
                    _ma15_exit_raw = bool(_ma20_gate and _thr_breach and _confirm_ok)

                    _ma5_s  = f"{float(ma5):.4f}"      if ma5  is not None else "N/A"
                    _ma15_s = f"{float(ma15):.4f}"     if ma15 is not None else "N/A"
                    _ma20_s = f"{float(_exit_ma20):.4f}" if _exit_ma20 > 0  else "N/A"

                    logger.debug(
                        f"[MA15-EXIT-CHECK] {market} | basis={_exit_basis} | "
                        f"ma5={_ma5_s} ma15={_ma15_s} ma20={_ma20_s} | "
                        f"breach={ma15_breach_pct:+.3f}% "
                        f"thr_hit={int(_thr_breach)} "
                        f"ma20_gate={int(_ma20_gate)} "
                        f"exit_bar_breach={int(_exit_bar_breach)} "
                        f"prev_bar_breach={int(_prev_bar_breach)} "
                        f"fast={int(_fast_breach)} "
                        f"early_lock={int(_ma15_early_locked)} "
                        f"lock_sec={int(_ma15_early_lock_sec)} "
                        f"held={holding_seconds:.0f}s "
                        f"confirm={_confirm_mode} "
                        f"thr={self.ma15_breach_threshold:.3f}%"
                    )

                    if _ma15_exit_raw:
                        logger.info(
                            f"📉 [MA15-Structure-Exit] {market} | "
                            f"basis={_exit_basis} confirm={_confirm_mode} | "
                            f"TF:{self.timeframe.value} | EXIT-MA5:{ma5:,.0f} < EXIT-MA15:{ma15:,.0f} | "
                            f"이탈:{ma15_breach_pct:+.2f}% (임계값:-{self.ma15_breach_threshold:.2f}%) | "
                            f"손익:{loss:+.2f}%"
                        )
                        try:
                            _held_min = (datetime.now() - position.entry_time).total_seconds() / 60.0
                            _entry_rise_atr = float(
                                indicators.get("entry_rise_atr",
                                getattr(position, "entry_rise_atr", -1.0)) or -1.0
                            )
                            _exit_closed = float(indicators.get("prev_candle_close", current_price) or current_price)
                            _tick_vs_closed = (
                                (current_price - _exit_closed) / _exit_closed * 100.0
                                if _exit_closed > 0 else 0.0
                            )
                            logger.info(
                                f"🧾 [EXIT-FORENSIC] {market} | "
                                f"reason=MA15-Exit basis={_exit_basis} "
                                f"exit_tick={current_price:.2f} exit_closed={_exit_closed:.2f} "
                                f"gap={_tick_vs_closed:+.3f}% "
                                f"breach={ma15_breach_pct:+.3f}% confirm={_confirm_mode} "
                                f"| held={_held_min:.1f}m pnl={loss:.2f}% "
                                f"entry_rise_atr={_entry_rise_atr:.2f}x"
                            )
                        except Exception as _exit_forensic_e:
                            logger.debug(f"[EXIT-FORENSIC-ERR] {market} {_exit_forensic_e}")
                        return TradingSignal(
                            symbol=market, action="SELL", price=current_price,
                            reason=f"MA15-Exit-{self.timeframe.value}({ma15_breach_pct:.2f}%/{_confirm_mode})",
                            confidence=entry_conf,
                            sell_ratio=1.0,   # MA15 구조 이탈 → 전량 청산, 부분청산: 50% 청산했다가 2번째 손실이 더 커서 수정 
                            market_regime=market_regime, urgency="high",
                            indicators={}
                        )
                    
        # 1-4. 타임아웃 체크 (구조 미이탈 시 보험)
        # 글로벌 표준: 타임아웃은 손실 포지션에만 적용
        # 수익 포지션은 추가 상승 기회 보존 (TP/TS가 담당)
        base_timeout = self._get_timeout_hours_for_timeframe()
        
        # 체제별 배수 (TD는 빠른 탈출, TU는 여유)
        regime_multipliers = {
            MarketRegime.TRENDING_DOWN: 1.0,   # 기본 (빠른 탈출)
            MarketRegime.RANGING: 1.5,          # 1.5배
            MarketRegime.TRENDING_UP: 2.0,      # 2배 (여유)
        }
        
        _glc = (
            (indicators or {}).get('gajami_lifecycle')
            or (ma_data or {}).get('gajami_meta', {}).get('gajami_lifecycle')
            or {}
        )
        if market_regime == MarketRegime.TRENDING_DOWN and _glc.get('state') == 'VALID':
            multiplier = 1.5
        else:
            multiplier = regime_multipliers.get(market_regime, 1.0)
        timeout_limit = base_timeout * multiplier
        
        hold_hours = (datetime.now() - position.entry_time).total_seconds() / 3600
        
        # 타임아웃 조건: 시간 초과 AND 손실 확정
        # tick 손실만 음수이고 closed 손익이 양수면 stale tick 오염으로 보고 차단
        timeout_only_if_losing = getattr(self.settings, 'timeout_only_if_losing', True)
        should_timeout = hold_hours >= timeout_limit

        if timeout_only_if_losing:
            _stale_mismatch = bool(
                loss < 0
                and _closed_loss_pct is not None
                and _closed_loss_pct >= 0
            )
            if _stale_mismatch:
                logger.warning(
                    f"🛡️ [TIMEOUT-LOSS-MISMATCH-SKIP] {market} | "
                    f"tick={loss:+.2f}% closed={_closed_loss_pct:+.2f}% "
                    f"→ stale 의심, timeout 차단"
                )
            should_timeout = should_timeout and (loss < 0) and not _stale_mismatch

        if should_timeout:
            logger.warning(
                f"⏰ [타임아웃] {market} {market_regime.name} | "
                f"보유: {hold_hours:.1f}h >= 제한: {timeout_limit:.1f}h | "
                f"tick손익: {loss:+.2f}% | "
                f"closed손익: {(_closed_loss_pct if _closed_loss_pct is not None else 9999):+.2f}% | "
                f"(base: {base_timeout:.2f}h × {multiplier:.1f}x)"
            )
            return TradingSignal(
                symbol=market, action="SELL", price=current_price,
                reason=f"타임아웃 {hold_hours:.1f}h", confidence=entry_conf,
                market_regime=market_regime, urgency="high",
                indicators={
                    "timeout_tick_price": current_price,
                    "timeout_tick_return_pct": loss,
                    "timeout_closed_price": _closed_price,
                    "timeout_closed_return_pct": _closed_loss_pct,
                }
            )
        
        # ============================================================
        # 🔥 2-0. 체제 전환 강제 청산 (2025-11-24 비활성화)
        # ============================================================
        # ❌ 비활성화 사유:
        #   - 3분봉 체제는 노이즈가 많아 TRENDING_UP ↔ RANGING ↔ TRENDING_DOWN 플리핑 빈번
        #   - 체제 전환만으로 청산하면 정상 손절/익절 기회 박탈
        #   - 글로벌 CTA 전략: 체제는 "진입 필터" 역할만, 청산은 가격/ATR 기반만 사용
        #   - 손절/익절/트레일링/타임아웃이 청산 담당
        # ============================================================
        # 🆕 vSell-Clean v1: 체제 전환 매도 (2단계 전환만 허용)
        # 철학: "매도는 가격 구조 우선, 체제는 보조"
        # 
        # 허용 전환 (의미 있음):
        #   BULL_MARKET → TRENDING_UP (거시 둔화)
        #   RANGING → BREAKDOWN (구조 붕괴)
        #   BREAKOUT → RANGING (돌파 실패)
        #
        # 무시 전환 (노이즈):
        #   TRENDING_UP ↔ RANGING (단기 흔들림)
        #   RANGING ↔ TRENDING_DOWN (단기 흔들림)
        # 🔥 체제 전환 즉시 대응 (REGIME-IGNORE 제거)
        # 철학: 체제 전환 = 원인 신호 → 즉시 반영
        # 근거: 손익(결과)으로 체제(원인)를 무시하는 것은 SSoT 위반
        
        # 위험 체제 전환 정의 (상승 → 하락/횡보)
        RISKY_REGIME_SHIFTS = {
            (MarketRegime.TRENDING_UP, MarketRegime.TRENDING_DOWN),
            (MarketRegime.TRENDING_UP, MarketRegime.RANGING),
            (MarketRegime.BREAKOUT, MarketRegime.TRENDING_DOWN),
            (MarketRegime.BREAKOUT, MarketRegime.RANGING),
            (MarketRegime.BULL_MARKET, MarketRegime.TRENDING_DOWN),
            (MarketRegime.BULL_MARKET, MarketRegime.RANGING),
        }
        
        def is_risky_regime_shift(prev_regime, curr_regime):
            """위험한 체제 전환 판단 (상승 → 하락/횡보)"""
            return (prev_regime, curr_regime) in RISKY_REGIME_SHIFTS
        
        entry_regime = getattr(position, "entry_regime", None)
        
        # 🔒 FATAL: mr_regime이 없으면 체제전환 매도는 절대 하지 않는다 (오염 차단)
        if not isinstance(indicators, dict) or ("mr_regime" not in indicators):
            logger.error(
                f"🚨 [FATAL-REGIME] {market} mr_regime missing → REGIME-SELL SKIP | "
                f"keys={list(indicators.keys()) if isinstance(indicators, dict) else 'N/A'}"
            )
        else:
            mr_regime = indicators["mr_regime"]
            
            # 🔒 혼용 추적용 로그 (필수)
            logger.debug(
                f"[REGIME-CHECK] {market} | "
                f"entry={entry_regime.value if entry_regime else 'N/A'} | "
                f"mr={mr_regime.value} | "
                f"market={market_regime.value if market_regime else 'N/A'} | "
                f"mr_candle={indicators.get('mr_candle_time','N/A')}"
            )
            
            if entry_regime is not None and entry_regime != mr_regime:
                # 위험 체제 전환 감지 + 손실 조건 확인
                if is_risky_regime_shift(entry_regime, mr_regime):
                    # 🔥 PATCH REGIME-LOSS: 손절 근접 조건부 Exit
                    # 철학: 손절이 유일한 Hard Exit, 체제 전환은 보조
                    # 허용 조건: 손실률 <= -stop_loss_pct × 0.8
                    # 효과: 얕은 손실 구간 조기 청산 방지, 정식 손절 로직 보호
                    stop_loss_pct = self.settings.stop_loss_pct
                    regime_exit_threshold = -stop_loss_pct * 0.8 * 100  # % 단위 통일
                    
                    if loss <= regime_exit_threshold:
                        # 손절 근접 → 체제 전환 Exit 실행
                        logger.info(
                            f"🚨 [REGIME-SELL] {market} | "
                            f"추세 붕괴(3m확정) + 손절 근접 → 즉시 매도 | "
                            f"{entry_regime.value}→{mr_regime.value} | "
                            f"mr_candle={indicators.get('mr_candle_time', 'N/A')} | "
                            f"손익: {loss:+.2f}% (임계: {regime_exit_threshold:+.2f}%)"
                        )
                        
                        # REGIME COOLDOWN 설정 제거: 체제 판단은 상위 레이어에서 종료
                        
                        return TradingSignal(
                            symbol=market, action="SELL", price=current_price,
                            reason=f"체제전환 {entry_regime.value}→{mr_regime.value}",
                            confidence=entry_conf,
                            sell_ratio=1.0, market_regime=mr_regime,
                            urgency="high",
                            indicators={}
                        )
                    else:
                        # 손실 얕음 → Exit 금지, 정식 로직에 위임
                        logger.info(
                            f"ℹ️ [REGIME-IGNORE] {market} | "
                            f"체제 전환 감지했으나 손실 작음 → Exit 보류 | "
                            f"{entry_regime.value}→{mr_regime.value} | "
                            f"mr_candle={indicators.get('mr_candle_time', 'N/A')} | "
                            f"손익: {loss:+.2f}% (임계: {regime_exit_threshold:+.2f}%) | "
                            f"정식 손절/익절 로직에 위임"
                        )
                else:
                    # 기타 전환 (예: TD→TU) - 정보만 로깅
                    logger.debug(
                        f"[REGIME-INFO] {market} 체제 전환 | "
                        f"{entry_regime.value}→{mr_regime.value} | "
                        f"손익: {loss:+.2f}%"
                    )
            else:
                # 체제 전환 없음
                if entry_regime:
                    logger.debug(
                        f"✅ [REGIME-OK] {market} | "
                        f"{entry_regime.value} == {mr_regime.value} | "
                        f"mr_candle={indicators.get('mr_candle_time', 'N/A')}"
                    )
        
        # ========================================
        # 2-1-A. RANGING 박스권 Trimmed 기반 매도 로직
        # ========================================
        # 철학:
        #   - 매수와 동일하게 60/30/15 박스를 보고 매도해야 일관성이 맞는다.
        #   - Trimmed-Median(극단값 제거)으로 "실제 시장이 인식하는 박스"를 선택한다.
        #   - 상단(>=85%)에서는 가차없이 전량 익절, 75~85%는 조기 청산,
        #     하단 이탈은 박스 전략 관점 손절.
        if market_regime == MarketRegime.RANGING and df is not None and len(df) >= 2:
            try:
                box_configs = [
                    {'name': '장기', 'lookback': 60, 'min_touches': 4},
                    {'name': '중기', 'lookback': 30, 'min_touches': 3},
                    {'name': '단기', 'lookback': 15, 'min_touches': 2},
                ]

                box_results = []

                for config in box_configs:
                    lb = min(len(df), config['lookback'])
                    if lb < 10:
                        continue

                    rc = df.tail(lb)
                    support = float(rc['low'].min())
                    resistance = float(rc['high'].max())
                    if support <= 0 or resistance <= support:
                        continue

                    tol = 0.01
                    sup_touches = int(sum(
                        (rc['low'] <= support * (1 + tol)) &
                        (rc['low'] >= support * (1 - tol))
                    ))
                    height_pct = (resistance - support) / support * 100.0

                    # "진짜 박스"만 사용
                    if height_pct <= 3.5 and sup_touches >= config['min_touches']:
                        pos = (current_price - support) / (resistance - support)
                        pos = max(0.0, min(1.0, pos))

                        box_results.append({
                            'name': config['name'],
                            'lookback': lb,
                            'support': support,
                            'resistance': resistance,
                            'height_pct': height_pct,
                            'position': pos,
                            'sup_touches': sup_touches,
                        })

                        logger.debug(
                            f"📦 [BoxSell-{config['name']}] {market} | "
                            f"S:{support:.1f} R:{resistance:.1f} "
                            f"h:{height_pct:.2f}% pos:{pos*100:.1f}% | "
                            f"touch:{sup_touches}"
                        )

                # ----- Trimmed-Median: 3개 이상일 때 극단값 제거 후 중간들만 사용 -----
                if len(box_results) >= 3:
                    sorted_boxes = sorted(box_results, key=lambda x: x['position'])
                    trimmed = sorted_boxes[1:-1]  # 최소/최대 제거

                    logger.debug(
                        f"🔧 [BoxSell-Trimmed] {market} extremums removed | "
                        f"removed: {sorted_boxes[0]['name']}({sorted_boxes[0]['position']*100:.1f}%), "
                        f"{sorted_boxes[-1]['name']}({sorted_boxes[-1]['position']*100:.1f}%) | "
                        f"used: {[b['name'] for b in trimmed]}"
                    )

                    if trimmed:
                        # 대표 박스: 위치 기준 중간값 1개 선택
                        mid_box = sorted(trimmed, key=lambda x: x['position'])[len(trimmed)//2]
                        s = mid_box['support']
                        r = mid_box['resistance']
                        pos = mid_box['position']

                        # 1) 박스 하단 붕괴: 지지선 1% 이탈 → 손절
                        if current_price < s * 0.99 and loss < 0.0:
                            logger.info(
                                f"📉 [BoxSell-Breakdown] {market} 박스 하단 이탈 손절 | "
                                f"S:{s:.1f} < 현재:{current_price:.1f} | 손익:{loss:.2f}%"
                            )
                            return TradingSignal(
                                symbol=market,
                                action="SELL",
                                price=current_price,
                                reason=f"박스하단이탈 {loss:.1f}",
                                confidence=entry_conf,
                                market_regime=market_regime,
                                urgency="high",
                                indicators={},
                            )                            
            except Exception as e:
                logger.debug(f"[BoxSell] {market} 박스권 매도 로직 오류: {e}")
        
        # ========================================
        # 2-1-B. RANGING fallback 매도 (박스 로직 미작동 시만)
        # ========================================
        # 철학:
        #   - 2-1-A 박스권 매도가 작동하지 않았다면 (박스 미형성/미감지)
        #   - fallback으로 수익률 기반 매도 적용
        #   - 3% 익절 + max_profit 하락 매도 모두 유지 (수익증발 방지)
        if market_regime == MarketRegime.RANGING:
            # max_profit 체크 (공통)
            if hasattr(position, 'max_profit_pct'):
                max_profit = position.max_profit_pct
            else:
                max_profit = 0.0
            
            # (A) 3% 수익 도달 → 조기 익절
            if loss >= 3.0:
                logger.info(
                    f"✅ [Ranging-Fallback-TakeProfit] {market} | "
                    f"수익률 {loss:.2f}% ≥ 3% | 박스 로직 미작동"
                )
                return TradingSignal(
                    symbol=market,
                    action="SELL",
                    price=current_price,
                    reason=f"RANGING-Fallback익절 {loss:.1f}",
                    confidence=0.88,
                    market_regime=market_regime,
                    urgency="medium",
                    indicators={}
                )
            
            # (B) max_profit 하락 매도 → 수익증발 방지
            if max_profit >= 0.030 and loss < 0.5:
                logger.info(
                    f"⚠️ [Ranging-Fallback-DecaySell] {market} | "
                    f"최고수익 {max_profit*100:.2f}% 이후 하락 → 현재 {loss:.2f}% | "
                    f"박스 로직 미작동"
                )
                return TradingSignal(
                    symbol=market,
                    action="SELL",
                    price=current_price,
                    reason=f"RANGING-Fallback하락매도 {loss:.1f}",
                    confidence=0.85,
                    market_regime=market_regime,
                    urgency="medium",
                    indicators={}
                )
        
        # 3. 부분 매도 체크 (익절 전에!)
        if self.enable_partial_sell and not position.is_partial_sell_complete():
            logger.debug("[부분매도체크] %s 시작 | 현재가: %s원 | 수익률: %.2f%%", position.symbol, f"{current_price:,.0f}", (current_price/position.avg_price-1)*100)
            partial_signal = self._check_partial_sell(
                position, current_price, market_regime, entry_conf
            )
            if partial_signal:
                return partial_signal
            else:
                logger.debug(f"[부분매도체크] {market} 조건 미충족 (트리거 미도달)")
        
        # 🔒 신호매도 가드: 부분매도 1차 이후에만 허용
        # 철학: "가자미근 → 목덜미 → 부분매도" 위계 강제
        # 효과: 부분매도 전 신호매도 물리적 차단
        partial_sell_started = False
        if hasattr(position, 'get_next_partial_sell_stage'):
            next_stage = position.get_next_partial_sell_stage()
            partial_sell_started = (next_stage > 1)
        
        # 🔒 신호매도 실행 자격 가드 (부분매도 1차 이후)
        # 철학: TP/TS/방어는 항상 작동, 신호매도만 부분매도 후 허용
        # 효과: 목덜미까지 기다리되, 명확한 익절/방어는 실행
        allow_signal_sell = bool(partial_sell_started)
                    
        # ========================================
        # 4. 체제 기반 익절 (Take Profit Priority)
        # ========================================
        # [기존 신호매도 코드 500줄이 이 아래에 그대로 유지됨]
        # ✨ TP > TS 우선순위: 추적손절보다 먼저 실행
        # 목적: 수익 극대화 + 노이즈 손절 방지
        # 구조:
        #   1) 선행 캔들 기반 익절 (추세 전환 직전 조기 청산)
        #   2) 체제별 다단계 익절 (TRENDING_UP: 3단계, RANGING: 2단계, TD: 1단계)
        
        # 현재 수익률 계산
        profit_pct = position.get_return_pct(current_price) / 100.0  # 0.01 = +1%
        
        # Config 로드 (선행 캔들 패턴용)
        try:
            tp_conf = self.config
        except AttributeError:
            tp_conf = None
        
        if tp_conf:
            candle_min_ratio = tp_conf.getfloat("TAKE_PROFIT", "candle_exit_min_ratio", fallback=0.6)
        else:
            candle_min_ratio = 0.6
        
        # (삭제됨) indicators는 Analysis에서만 생성
        
        # 통합 익절/방어청산 프로파일 (SSoT: init에서 로드한 dict)
        _exit_key = self._normalize_exit_regime_key(market_regime)
        _tp_triggers = self.regime_tp_triggers.get(
            _exit_key, self.regime_tp_triggers['TRENDING_DOWN']
        )
        tp1 = _tp_triggers[0]
        trailing_pct = self.regime_trailing_pcts.get(
            _exit_key, self.regime_trailing_pcts['TRENDING_DOWN']
        )
                
        # 1) 선행 캔들 패턴 기반 조기 익절 (Leading TP Layer)
        # 🔒 신호매도 가드: 부분매도 1차 이후에만 실행
        if allow_signal_sell:
            # 🎯 목적: TS보다 상위, 일반 TP보다도 상위 레이어
            # 📈 효과: 고점 징후 선행 감지 → 수익 극대화 + 손익 반납 방지
            # 
            # 패턴: ② Bearish Engulfing (강세→약세 반전)
            # (2026-08-01 삭제) ① Bearish Exhaustion · ③ Momentum Shift
            #   사유: 두 조건 모두 indicators['candle_signal']이 필수였으나
            #         Analysis가 이 키를 더 이상 생산하지 않음(死배선 — 08-06 셰도 v2까지 함께 제거)
            #         → 항상 0.0 → 발동 불가능한 死갈래였음. 발동 0건.

            # Config 기반 민감도 (TP1 대비 최소 수익 비율)
            min_profit_for_candle = tp1 * candle_min_ratio

            # ────────────────────────────────────────
            # ② Bearish Engulfing (강세→약세 반전)
            # ────────────────────────────────────────
            # 조건: 직전 양봉을 통째로 감싸는 음봉
            # 효과: 강한 반전 신호 → 즉시 익절
            engulfing = indicators.get("bearish_engulfing", False)
            
            if engulfing and profit_pct >= min_profit_for_candle:
                logger.info(
                    "💎 [LeadingTP-Engulfing] %s | 수익:%.2f%% | 패턴:BearishEngulfing",
                    market, profit_pct*100
                )
                return TradingSignal(
                    symbol=market, action="SELL", price=current_price,
                    reason=f"선행익절-Engulfing {profit_pct*100:.2f}",
                    confidence=entry_conf, market_regime=market_regime, urgency="high",
                    indicators={}
                )

        # 2) 부분익절은 _check_partial_sell()에서만 처리한다.
        # 목적: enable_partial_sell 우회 및 TP1/TP2 판단 중복 방지
        # SSoT: self.regime_tp_triggers + self.sell_ratios

        # ========================================
        # 2-5) TRENDING_UP Profit Giveback 방어청산
        # 목적: 기존 trailing_tu 설정을 SELL 판단 경로에 연결
        # 주의: min_profit_lock TS 아님 → max_profit 반납폭 기반
        # ========================================
        if _exit_key == 'TRENDING_UP':
            _ts_act = getattr(self.settings, 'trailing_stop_activation', 0.004)
            _peak = getattr(position, "max_profit_closed", None)

            if _peak is not None and _closed_profit_ratio is not None:
                _giveback_pct = _peak - _closed_profit_ratio

                if _peak >= _ts_act and _giveback_pct >= trailing_pct:
                    logger.warning(
                        f"📉 [TU-Giveback] {market} | "
                        f"고점:{_peak*100:.2f}% → "
                        f"현재:{_closed_profit_ratio*100:.2f}% | "
                        f"반납:{_giveback_pct*100:.2f}% ≥ {trailing_pct*100:.1f}% | "
                        f"basis=peak_closed"
                    )
                    return TradingSignal(
                        symbol=market,
                        action="SELL",
                        price=current_price,
                        reason=f"TU-Giveback {_closed_profit_ratio*100:.1f}",
                        confidence=entry_conf,
                        market_regime=market_regime,
                        urgency="high",
                        indicators={}
                    )

        # 3) Fallback: ATR 기반 익절 (안전망, TP2 이후 잔량만)
        # 조건: position.take_profit 도달 (ATR 기반 계산값)
        # 제약: TP2 완료 후에만 작동 (전량 조기 익절 방지)
        # 목적: 비상 안전망 (급등 후 급락, 슬리피지, API 지연)
        if position.partial_sell_count >= 2 and current_price >= position.take_profit:
            logger.info(
                "🎯 [ATR익절-잔량] %s | 현재:%s원 ≥ 목표:%s원 | 수익:%.2f%% | 부분익절:%d회",
                market, f"{current_price:,}", f"{position.take_profit:,}", 
                profit_pct*100, position.partial_sell_count
            )
            return TradingSignal(
                symbol=market, action="SELL", price=current_price,
                reason=f"ATR익절(잔량) {profit_pct*100:.1f}", confidence=entry_conf,
                market_regime=market_regime, urgency="high",
                indicators={}
            )
               
        # 🆕 5-B. MA5 < MA20 데드크로스 손절 (2025-12-05 신규 추가)
        # 목적: 단기(MA5) < 중기(MA20) 추세 전환 시 즉시 청산
        # 우선순위: 15분 MA 손절보다 먼저 체크
        # 적용: 3m, 5m 초단타 전용
        # 근거: BARD 케이스, 데드크로스 발생 시 -3% 손실 누적
        
        # MA5, MA20 추출 (15분봉 데이터에서)
        ma5_15 = S(ma_data.get('ma5', 0.0))
        ma20_15 = S(ma_data.get('ma20', 0.0))
        
        if ma5_15 > 0 and ma20_15 > 0 and ma5_15 < ma20_15:
            # 데드크로스 강도 계산 (갭 %)
            death_cross_gap = (ma20_15 - ma5_15) / ma20_15 * 100.0
            
            # 타임프레임별 임계값
            dc_thresholds = {
                '1m': 0.3,
                '3m': 0.3,  # 3m은 0.3% 이상 갭이면 즉시
                '5m': 0.5,
            }
            dc_threshold = dc_thresholds.get(self.timeframe.value, 0.5)
            
            # 강한 데드크로스: 갭 충분 + 현재가 < MA20
            if death_cross_gap >= dc_threshold and current_price < ma20_15:
                logger.warning(
                    f"💀 [데드크로스손절] {market} | "
                    f"MA5={ma5_15:,.0f} < MA20={ma20_15:,.0f} | "
                    f"갭:{death_cross_gap:.2f}% (임계:{dc_threshold:.2f}%) | "
                    f"현재가:{current_price:,.0f} | 손익:{loss:+.2f}%"
                )
                return TradingSignal(
                    symbol=market,
                    action="SELL",
                    price=current_price,
                    reason=f"데드크로스손절 {loss:.1f}",
                    confidence=entry_conf,
                    sell_ratio=1.0,
                    market_regime=market_regime,
                    urgency="high",
                    indicators={}
                )
                        
        # ================================================
        # 🧩 15봉 단기 매도 필터 개선 (이격도 기반)
        # ================================================
        # 목적: 15봉 과민 매도 방지 + 과열 구간 조기 청산
        # 적용: 15봉 전략 활성화 시, 15/20 이평선 간격에 따라 조건 선택
        if (self.settings.enable_15min_ma_exit and 
            self.timeframe.minutes <= 15 and 
            S(ma_data.get('ma15', 0.0)) > 0 and  # ✅ 스칼라 비교
            S(ma_data.get('ma20', 0.0)) > 0):    # ✅ 스칼라 비교
            
            # ma15, ma20 추출
            ma15 = S(ma_data.get('ma15', 0.0))
            ma20 = S(ma_data.get('ma20', 0.0))
            
            # ✅ 이미 스칼라이므로 안전
            if ma15 > 0.0 and ma20 > 0.0:
                # ========================================
                # 15봉-20봉 간격 계산 (%)
                # ========================================
                ma_gap_pct = abs(ma15 - ma20) / ma20 * 100
                
                # ========================================
                # 기준:
                #   < 0.2%: 안정 (완만형: 매도 보류)
                #   0.2~0.5%: 중립 (표준: 20봉 기준)
                #   ≥ 0.5%: 과열 (급등형: 15봉 조기)
                # ========================================
                
                # 타임프레임별 기본 임계값
                # ⚙️ 개선 (2025-12-05): 초단타 손절 임계값 완화
                # - 기존: 3m 기준 -4.0%라 초단타 손절이 너무 느림
                # - 개선: 3m/5m은 -1.5~-2.5% 구간 반전이 잦으므로
                #        손절 이격도를 1.5~2.0% 앞당김
                # - 근거: BARD 케이스 (-3.03% 강제손절)
                base_thresholds = {
                    '1m': -2.5,
                    '3m': -2.5,  # ✅ -4.0 → -2.5 (초단타 빠른 손절)
                    '5m': -3.5,  # ✅ -5.0 → -3.5
                    '10m': -4.5, # ✅ -6.0 → -4.5
                    '15m': -5.5  # ✅ -7.0 → -5.5
                }
                base_threshold = base_thresholds.get(self.timeframe.value, -2.5)
                # 🔥 간격 기반 3단계 판단
                
                if ma_gap_pct >= 0.5:
                    # ✅ 과열 구간 (급등형) → 15봉 조기 매도 (빠른 보호)
                    ma_deviation = (current_price - ma15) / ma15 * 100
                    ma_exit_threshold = base_threshold  # 기본값 사용
                    
                    if ma_deviation < ma_exit_threshold and current_price < ma15:
                        breach_ratio = (ma15 - current_price) / ma15
                        breach_threshold = self.settings.ma_breach_threshold if hasattr(self.settings, 'ma_breach_threshold') else 0.01
                        
                        if breach_ratio > breach_threshold:
                            # 🆕 vSell-Clean v1: MA15 매도 격하 (보조 신호)
                            # MA15 단독 매도 금지, 보조 조건 필요:
                            #   1) 목덜미 구간 (저점대비 +3% 이상)
                            #   2) 추적손절 트리거
                            #   3) 거래량 급감 (<0.3x)
                            
                            # 보조 조건 확인
                            is_neck_zone = loss >= 3.0  # 목덜미 (저점대비 +3%)
                            is_trailing_triggered = False  # 추적손절 (추후 연동)
                            volume_ratio_val = safe_extract_scalar(indicators.get('volume_ratio', 1.0))
                            is_volume_collapse = (volume_ratio_val < 0.3)
                            
                            # 보조 조건 충족 시에만 매도
                            if is_neck_zone or is_trailing_triggered or is_volume_collapse:
                                logger.info(
                                    f"📉 [MA15-SELL] {market} | "
                                    f"15/20 간격: {ma_gap_pct:.2f}% (과열) | "
                                    f"15봉: {ma15:,.0f} | 20봉: {ma20:,.0f} | "
                                    f"현재가: {current_price:,.0f} | "
                                    f"이격도: {fmt(ma_deviation, '.2f')}% | "
                                    f"임계값: {ma_exit_threshold:.2f}% | "
                                    f"최종수익률: {loss:+.2f}% | "
                                    f"보조조건: 목덜미={is_neck_zone} "
                                    f"거래량붕괴={is_volume_collapse}"
                                )
                                
                                return TradingSignal(
                                    symbol=market, action="SELL", price=current_price,
                                    reason=f"MA15+보조({ma_gap_pct:.2f}) {loss:.1f}", 
                                    confidence=0.85,
                                    sell_ratio=1.0, market_regime=market_regime,
                                    urgency="high",
                                    indicators={}
                                )
                            else:
                                # 보조 조건 미충족 - MA15 이탈 무시
                                logger.debug(
                                    f"[MA15-IGNORE] {market} MA15 이탈 무시 "
                                    f"(보조조건 미충족) | "
                                    f"이격도: {fmt(ma_deviation, '.2f')}% | "
                                    f"손익: {loss:+.2f}%"
                                )
                
                elif 0.2 <= ma_gap_pct < 0.5:
                    # ⚖️ 중립 구간 (표준) → 20봉 기준 매도 (1% 완화)
                    ma_deviation = (current_price - ma20) / ma20 * 100
                    ma_exit_threshold = base_threshold - 1.0  # 20봉은 1% 완화
                    
                    if ma_deviation < ma_exit_threshold and current_price < ma20:
                        breach_ratio = (ma20 - current_price) / ma20
                        breach_threshold = self.settings.ma_breach_threshold if hasattr(self.settings, 'ma_breach_threshold') else 0.01
                        
                        if breach_ratio > breach_threshold:
                            # 🧱 vSell-Clean v1.1: 중립 구간 보조 조건 + 2단계 체제 전환
                            # 철학: 중립 구간은 MA20 단독 이탈 금지
                            # 조건: 아래 중 1개 이상 충족 시만 매도 허용
                            #   1) 목덜미 구간 (수익 ≥3%)
                            #   2) 거래량 붕괴 (<0.3x)
                            #   3) 추적손절 트리거 (추후 연동)
                            #   4) 의미 있는 2단계 체제 전환
                            #
                            # 설계 관점:
                            #   "가격 구조 + 맥락"이 동시에 무너질 때만 판다.
                            #   체제는 그 맥락을 확인해 주는 보조 신호다.
                            
                            # 보조 조건 확인
                            is_neck_zone = loss >= 3.0  # 목덜미
                            is_trailing_triggered = False
                            volume_ratio_val = safe_extract_scalar(indicators.get('volume_ratio', 1.0))
                            is_volume_collapse = (volume_ratio_val < 0.3)
                            
                            # 2단계 체제 전환 확인
                            entry_regime = getattr(position, "entry_regime", None)
                            is_major_shift = False
                            if entry_regime is not None and market_regime is not None:
                                # 주요 체제 전환 감지 (TRENDING_UP ↔ TRENDING_DOWN)
                                is_major_shift = (
                                    (entry_regime == MarketRegime.TRENDING_UP and market_regime == MarketRegime.TRENDING_DOWN) or
                                    (entry_regime == MarketRegime.TRENDING_DOWN and market_regime == MarketRegime.TRENDING_UP)
                                )
                            
                            # 보조 조건 통합
                            neutral_aux_ok = (
                                is_neck_zone or
                                is_trailing_triggered or
                                is_volume_collapse or
                                is_major_shift
                            )
                            
                            if neutral_aux_ok:
                                logger.info(
                                    f"📊 [표준매도] {market} | "
                                    f"15/20 간격: {ma_gap_pct:.2f}% (중립) | "
                                    f"15봉: {ma15:,.0f} | 20봉: {ma20:,.0f} | "
                                    f"현재가: {current_price:,.0f} | "
                                    f"이격도: {fmt(ma_deviation, '.2f')}% | "
                                    f"임계값: {ma_exit_threshold:.2f}% | "
                                    f"최종수익률: {loss:+.2f}% | "
                                    f"보조조건: 목덜미={is_neck_zone} "
                                    f"거래량붕괴={is_volume_collapse} "
                                    f"체제2단계={is_major_shift}"
                                )
                                
                                return TradingSignal(
                                    symbol=market, action="SELL", price=current_price,
                                    reason=f"MA20+보조({ma_gap_pct:.2f}) {loss:.1f}", 
                                    confidence=0.80,
                                    sell_ratio=1.0, market_regime=market_regime,
                                    urgency="high",
                                    indicators={}
                                )
                            else:
                                # 보조 조건 미충족 - MA20 이탈 무시
                                regime_str = f"{entry_regime.value if entry_regime else 'N/A'}→{market_regime.value}"
                                logger.debug(
                                    f"[MA20-IGNORE] {market} 중립구간 MA20 이탈 보류 "
                                    f"(보조조건 미충족) | "
                                    f"손익: {loss:+.2f}% | "
                                    f"breach={breach_ratio:.3f} | "
                                    f"체제={regime_str}"
                                )
                
                else:
                    # 💧 안정 구간 (완만형) - 🔥 3봉 연속 하락 강제 손절 추가
                    # ⚙️ 개선 (2025-12-05): 안정 구간 로직 강화
                    # 조건 1: 3봉 연속 하락 + MA20 이탈 → 즉시 손절
                    # 조건 2: 손실 -2% 초과 → 기존대로 청산
                    # 조건 3: 손실 -1% 이상 + MA20 이탈 → 손절
                    # 근거: NXPC 케이스 (3봉 하락인데 안정구간으로 보류 → 버그)
                    
                    # 🔥 조건 1: 3봉 연속 하락 + MA20 이탈 체크
                    down_bars = 0
                    
                    if down_bars >= 2 and lt(current_price, ma20):
                        logger.info(
                            f"📉 [안정구간강제손절] {market} | "
                            f"15/20 간격: {ma_gap_pct:.2f}% (안정) | "
                            f"{down_bars}봉 연속하락 + MA20 이탈 | "
                            f"15봉: {ma15:,.0f} | 20봉: {ma20:,.0f} | "
                            f"현재가: {current_price:,.0f} | "
                            f"손실: {loss:+.2f}% | "
                            f"→ 안정구간이지만 하락 패턴 명확, 즉시 손절"
                        )
                        return TradingSignal(
                            symbol=market,
                            action="SELL",
                            price=current_price,
                            reason=f"안정구간강제손절({down_bars}봉하락) {loss:.1f}",
                            confidence=0.80,
                            sell_ratio=1.0,
                            market_regime=market_regime,
                            urgency="high",
                            indicators={}
                        )
                    
                    # 🔥 조건 2: 손실 -1% 이상 + MA20 이탈
                    if loss <= -1.0 and lt(current_price, ma20):
                        logger.info(
                            f"📉 [안정구간MA손절] {market} | "
                            f"15/20 간격: {ma_gap_pct:.2f}% (안정) | "
                            f"손실 {loss:.2f}% + MA20 이탈 | "
                            f"15봉: {ma15:,.0f} | 20봉: {ma20:,.0f} | "
                            f"현재가: {current_price:,.0f} | "
                            f"→ 손실 확대 방지 청산"
                        )
                        return TradingSignal(
                            symbol=market,
                            action="SELL",
                            price=current_price,
                            reason=f"안정구간MA손절 {loss:.1f}",
                            confidence=0.78,
                            sell_ratio=1.0,
                            market_regime=market_regime,
                            urgency="high",
                            indicators={}
                        )
                    
                    # 🔥 조건 3: 손실 -2% 초과 (기존 로직 유지)
                    if loss <= -2.0:
                        logger.info(
                            f"📉 [안정구간손절] {market} | "
                            f"15/20 간격: {ma_gap_pct:.2f}% (안정) | "
                            f"15봉: {ma15:,.0f} | 20봉: {ma20:,.0f} | "
                            f"현재가: {current_price:,.0f} | "
                            f"손실: {loss:+.2f}% | "
                            f"→ 손실 -2% 초과, 안정구간이라도 청산"
                        )
                        return TradingSignal(
                            symbol=market,
                            action="SELL",
                            price=current_price,
                            reason=f"안정구간손절 {loss:.1f}",
                            confidence=0.75,
                            sell_ratio=1.0,
                            market_regime=market_regime,
                            urgency="medium",
                            indicators={}
                        )
                    
                    # 손실이 -1% 이내면 기존대로 보류
                    logger.debug(
                        f"💧 [안정유지] {market} | "
                        f"15/20 간격: {ma_gap_pct:.2f}% (안정) | "
                        f"15봉: {ma15:,.0f} | 20봉: {ma20:,.0f} | "
                        f"현재가: {current_price:,.0f} | "
                        f"손실: {loss:+.2f}% | "
                        f"→ 손실 작아 보류"
                    )
        
        # ========================================
        # 매도 조건 미충족
        # ========================================
                
        return None

    @staticmethod
    def _normalize_exit_regime_key(regime) -> str:
        """익절/방어청산 설정 키 정규화."""
        _name = getattr(regime, "name", None)
        if _name is None:
            _name = str(regime or "TRENDING_DOWN")
            if "." in _name:
                _name = _name.rsplit(".", 1)[-1]
        if _name in ("TRENDING_UP", "BULL_MARKET", "BREAKOUT"):
            return "TRENDING_UP"
        if _name == "RANGING":
            return "RANGING"
        return "TRENDING_DOWN"

    def _check_partial_sell(self, position: Position, current_price: float,
                       regime: MarketRegime, entry_conf: Optional[float] = None) -> Optional[TradingSignal]:
        """부분 매도 체크        
        Args:
            position: 현재 포지션
            current_price: 현재가
            regime: 시장 체제            
        Returns:
            TradingSignal 또는 None
        """
        # 다음 단계 확인
        stage = position.get_next_partial_sell_stage()
        if stage == 0:
            return None  # 완료
        
        # 단계별 기본 트리거 (체제별 config.ini SSoT)
        _regime_key = self._normalize_exit_regime_key(regime)
        _triggers = self.regime_tp_triggers.get(_regime_key, self.regime_tp_triggers['TRENDING_DOWN'])
        if stage - 1 < len(_triggers):
            base_trigger = _triggers[stage - 1]
        else:
            return None  # 해당 체제에 정의된 단계 초과 → 완료
        
        # 체제 전환 방어: 진입 체제 vs 현재 체제 중 낮은 TP 사용
        _entry_regime = getattr(position, "entry_regime", None)
        _entry_key = self._normalize_exit_regime_key(_entry_regime) if _entry_regime else _regime_key
        if _entry_key != _regime_key:
            _entry_triggers = self.regime_tp_triggers.get(_entry_key, _triggers)
            if stage - 1 < len(_entry_triggers):
                base_trigger = min(base_trigger, _entry_triggers[stage - 1])

        adjusted_trigger = base_trigger
        
        # 목표가 계산
        target_price = position.avg_price * (1 + adjusted_trigger)
        
        # 목표가 도달 체크
        if current_price >= target_price:
            sell_ratio = self.sell_ratios[stage - 1]
            
            # Bid 가격 계산 (실제 체결 예상가)
            # 중소형 알트코인 평균 스프레드 0.22% (검증된 현실값)
            spread_pct = 0.0022
            bid_price = current_price * (1 - spread_pct)
            
            # 실제 수익률 계산 (Bid 기준)
            expected_profit_pct = (bid_price - position.avg_price) / position.avg_price * 100
            
            logger.info(
                "[부분매도] %s %d차 발동! | Ask호가: %s원 | Bid예상: %s원 (실제체결) | 예상수익: +%.2f%% | 트리거: %.2f%% | 비율: %.0f%% | 체제: %s",
                position.symbol, stage, f"{current_price:,.2f}", f"{bid_price:,.2f}", 
                expected_profit_pct, adjusted_trigger*100, sell_ratio*100, regime.value if regime else 'N/A'
            )
            
            # 🔧 수정: metadata 제거하고 생성 후 추가
            signal = TradingSignal(
                symbol=position.symbol,
                action="SELL",
                price=current_price,
                reason=f"{stage}차익절 +{adjusted_trigger*100:.2f} ({regime.value if regime else 'RANGING'})",
                confidence=entry_conf,
                sell_ratio=sell_ratio,
                market_regime=regime,
                urgency="normal",
                indicators={}
            )
            
            # metadata는 생성 후 수동으로 추가
            signal.metadata = {
                'partial_sell_stage': stage,
                'target_trigger': adjusted_trigger * 100,
                'regime': regime.value if regime else 'RANGING'
            }
            
            return signal
        
        return None
    
    def _calculate_signal_components(self, df, data_package: DataPackage,
                                    ma_analysis: Dict) -> Dict[str, float]:
        """
        신호 컴포넌트 계산        
        핵심 철학: "선행성 지표 + 캔들 패턴"
        1. MA (추세)
        2. Volume (압력)
        3. ATR (변동성)
        4. ADX (추세 강도)
        5. Candle (패턴) ✅ 복원!
        """
        components = {}

        # 🔧 공통 변수 초기화 (최우선 - 예외 발생 전)
        indicators = data_package.indicators or {} if data_package else {}
        regime = data_package.regime or MarketRegime.RANGING

        # 🆕 current_price 추가
        components['current_price'] = data_package.current_price if data_package else 0
        
        # ========================================
        # 1. MA 신호 (추세 + 정배열) - ✅ OR 조건 적용 ---> 로직 왔다 갔다 함
        # ========================================
        components['ma_signal'] = self._check_ma_signals(indicators, ma_analysis, regime)
        
        # 🐟 Priority 4: 데드크로스 + 가자미근 특례 (2025-12-18)
        #  - 가자미근 반등 초입에서 데드크로스는 흔한 현상
        #  - MA 신호 0점으로 즉사하는 문제 제거
        #  - 효과: SAHARA 케이스 개선 (최소 MA 신호 보장)
        gajami_entry = indicators.get('gajami_entry', False)
        has_deadcross = ma_analysis.get('deadcross', False) if ma_analysis else False

        # 가자미근 + 데드크로스 특례는
        # ▶ 실제 상승 전환 구간에서만 허용한다.
        # (regime은 함수 시작 부분에서 이미 정의됨)
        ma20_slope = float(safe_extract_scalar(indicators.get("ma20_slope", 0.0), 0.0))

        if (
            gajami_entry
            and has_deadcross
            and components['ma_signal'] < 0.05
            and regime == MarketRegime.TRENDING_UP
            and ma20_slope > 0
        ):
            logger.info(
                f"👁️ [Gajami-DeadCross-OBSERVE] {data_package.market} "
                f"조건 충족, MA Producer 원본 유지: {fmt(components['ma_signal'], '.3f')}"
            )
        
        # ========================================
        # 2. Volume 신호 (거래량 압력) - SSoT 준수
        # ========================================
        # ✅ SSoT 원칙: Analysis에서 계산된 volume_signal 직접 사용
        # ❌ Strategy에서 재계산 금지 (역할 분리)
        # 
        # 철학:
        # - Analysis = 신호 계산 (volume_ratio, buying_ratio → volume_signal)
        # - Strategy = 신호 조합·판결 (재계산 없음)
        # ========================================
        
        # ==============================
        # Volume Signal (SSoT 정석)
        # ==============================
        volume_signal = safe_extract_scalar(
            data_package.indicators.get('volume_signal', 0.0),
            0.0
        )
        logger.debug(
            f"📊 [Volume-SSoT] {data_package.market} | "
            f"Analysis 신호 사용: {volume_signal:.3f}"
        )
        
        # 🆕 실시간 거래량 교차검증 (과대보정 억제)
        # vol_ratio=0.23x인데 volume_signal=1.00 같은 케이스 방지
        if indicators.get('rt_valid', False) and volume_signal > 0:
            rt_vol_delta = float(indicators.get('rt_volume_delta', 0.0))
            volume_ma = safe_extract_scalar(indicators.get('volume_ma', 0), 0)
            if volume_ma > 0 and rt_vol_delta >= 0:
                rt_vol_ratio = (rt_vol_delta * 6.0) / volume_ma  # 30초→3분 투영
                if rt_vol_ratio < 0.3:
                    volume_signal = min(volume_signal, 0.50)
        
        components['volume_signal'] = volume_signal
        
        # ========================================
        # 3. ATR 신호 (변동성)
        # ========================================
        # ✅ ATR은 환경 지표 → SOLEUS와 독립 (2026-02-08 순환의존 제거)
        # 기존: soleus=False → ATR=0 강제 → 가자미근 초입에서 영구 차단
        # 수정: ATR은 변동성 "사실"만 전달, 진입 판단은 상위 계층이 수행
        # 🔒 [2026-08-08 SSoT-P1/P2]
        # ATR signal은 Analysis Producer 값을 그대로 소비한다.
        atr_signal = safe_extract_scalar(
            indicators.get('atr_signal', 0.0),
            0.0,
        )
        atr_meta = indicators.get('atr_signal_meta') or {}
        components['atr_signal'] = atr_signal
        # ATR 메타데이터 저장(관측 호환)
        if not hasattr(self, '_current_atr_meta'):
            self._current_atr_meta = {}
        self._current_atr_meta[data_package.market] = atr_meta
        
        # ========================================
        # 5. Entry Timing — 60봉 BW 압축 백분위
        # ========================================
        # candle_signal: DB/로그파서 호환용 legacy 슬롯(항상 0.0).
        # 실제 신호는 candle_entry_signal(=60봉 BW 압축 백분위 변환)이다.
        # 🔒 [BW-QUALITY-RECONNECT-0823]
        # RANGING quality도 candle_entry_signal을 소비한다.
        # legacy candle_signal은 판정용으로 사용하지 않는다.
        components["candle_signal"] = 0.0
        # 🔒 [2026-08-08 SSoT-P1/P2]
        # BW signal은 Analysis Producer 값을 그대로 소비한다.
        components["candle_entry_signal"] = safe_extract_scalar(
            indicators.get("bw_signal", 0.0),
            0.0,
        )

        # (2026-08-01 삭제) [Patch-B] Candle-MA Gate
        #   사유: 발동 조건이 `components['candle_signal'] > 0.0` 였으나
        #         이 키는 상수 0.0(legacy 슬롯) → 구조적으로 발동 불가. 발동 0건.

        # 🔒 SSoT: Series 완전 제거 (근본 해결) - 최우선 처리!
        # - Analysis에서 받은 데이터가 pandas Series일 수 있음
        # - 다른 로직 실행 전에 먼저 정리하여 Strategy 전역에서 안전 보장
        # - 모든 components 값을 float로 통일
        for key in list(components.keys()):
            val = components[key]
            # pandas Series나 Index인 경우 scalar로 변환
            if hasattr(val, 'item'):
                try:
                    components[key] = float(val.item())
                except (ValueError, TypeError, AttributeError):
                    components[key] = 0.0
            # 이미 숫자형이면 float 변환
            elif isinstance(val, (int, float)):
                components[key] = float(val)
            # None이면 0.0 처리
            elif val is None:
                components[key] = 0.0
        
        # 🔒 [2026-08-08 SSoT-P3] 최소신호 폴백 제거 — Producer 값이 없으면 0.0을 유지한다.
        
        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        # 🔒 components는 Dict[str, float] 엄격 준수
        # - gajami_lifecycle는 signal.indicators로 전달 (메타데이터)
        # - components는 float만 저장 (타입 안정성)
        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        components['atr_pct'] = safe_extract_scalar(
            indicators.get('atr_pct', 0.0),
            0.0
        )
        
        return components

    def _log_activity_snapshot(self, symbol: str, payload: dict):
        """
        관찰 전용 액티비티 스냅샷 로깅 (진입 판단에 영향 없음)
        - 목적: 사후 분석/튜닝
        """
        try:
            logger.info(f"🧪 [ACTIVITY] {symbol} | {payload}")
        except Exception:
            # 로깅 실패가 전략 흐름을 깨지 않도록 보호
            pass
        
# **END**