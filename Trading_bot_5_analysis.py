#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
# NOTE:
# 본 모듈에서 사용하는 모든 파라미터는
# ConfigManager / TradingSettings를 통해 주입된다.
# global_constants 직접 참조는 설계 위반이다. 단, CONFIG 파일과 중복이 없고, 더 복잡해지는 예외 몇개 허용.

========================================
업비트 자동매매 봇 v1.0 - Analysis Module
========================================
책임: 시장 분석, 지표 계산, 마켓 선정, 체제 판단
- 시장 체제 판단 (BULL_MARKET, TRENDING_UP/DOWN, RANGING, BREAKOUT/BREAKDOWN)
- 선행성 지표 계산 (ATR, ADX, MA, 거래량)
- 거래대금 기반 마켓 선정
- KRW 마켓 전용

제공 인터페이스:
  - perform_integrated_analysis(): 통합 분석
  - get_current_top_markets(): 상위 마켓
  - analyze_market_regime(): 시장 체제 (선행성 지표 기반)
  - TechnicalIndicators.calculate_all_indicators(): 선행성 지표 + 확정봉 보조지표 계산
  - TechnicalIndicators.calculate_ma_analysis(): MA 분석 + 진입 판사 계산
  
역할 제한:
  - Hidden bullish 다이버전스는 판사2, regular bullish 다이버전스는 판사4 입력으로 사용
  - BB·KC·Cycle Phase는 판사3 입력으로만 사용
  - RSI·BB·MACD를 confidence 5신호에 직접 편입 금지
  - 거래 실행은 bot 모듈에서 처리
  
개선사항:
  - 후행성 지표를 confidence와 분리하여 판사2·3·4 보조판정에만 사용
  - 선행성 지표 기반 체제 판단 유지
  - MA + 거래량 + ATR 중심 기본 분석 유지
========================================
"""

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 🛡️ WinError 233 차단: pandas/numpy C-확장 보호
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
import os, sys
os.environ["OPENBLAS_VERBOSE"] = "0"    # 🆕 추가! (핵심!)
os.environ["NUMEXPR_MAX_THREADS"] = "1" # 🆕 추가!

# ⚠️ stdout/stderr 전역 리다이렉션 제거
# - WinError 233 대응은 Bot 레벨 SafeLogger로 처리
# - 실제 스택 트레이스 확보 가능 (디버깅 복원)
# - 빗썸/주식/영웅문 확장 시 장애 분석 가능

import logging
logging.raiseExceptions = False

# ============================================================
# 🔒 NUMERIC SENTINEL (SSoT)
# ============================================================
# 숫자 파이프라인에서 None 사용 금지
# 계산 불가/미계산 상태는 SENTINEL로 통일
# Strategy 모듈의 NoneType 오류 원천 차단
# ============================================================
SENTINEL_FLOAT = -9999.0

import pandas as pd
import numpy as np
import asyncio
import math
from typing import List, Dict, Optional, Tuple, Any
from datetime import datetime
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 🔒 INTRABAR (Analysis는 관측만)
# - 봉 내부 1회 진입 "차단/상태"는 Strategy/Execution의 책임
# - Analysis는 intrabar 이벤트를 관측/로그로만 제공
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 🔒 SYSTEM CONSTITUTION: TIME BASE (절대 규칙)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# [헌법 1조] 시장 체제(Regime) 판단 = 3분봉 "확정봉" ONLY
#   - 적용 위치: MarketAnalyzer.analyze_market_regime()
#   - 구현 방식: 해당 함수 내부에서만 drop_incomplete_candle()로 확정봉 강제
#
# [헌법 2조] MA 시간 기준 분리
#   - MA5 값: BIRTH / 가자미근 조기 진입용 현재봉 반응 허용
#   - MA5 slope: 현재봉 반응 허용
#   - MA10 / MA20 이상 값: 확정봉 기준
#   - MA10 / MA20 / MA15 이상 slope: 확정봉 기준
#   - 금지: 중기 추세 방향 판단(MA15 이상, Regime)에 현재봉 직접 사용
#
# [헌법 3조] 체제(Regime)와 MA는 시간 기준이 다르며 서로 침범 금지
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def nzs(v: float) -> float:
    """
    None → SENTINEL_FLOAT 변환
    숫자 아닌 값도 SENTINEL로 강제    
    Args:
        v: 입력값 (None 가능)
    Returns:
        float: 유효한 숫자 또는 SENTINEL_FLOAT
    """
    if v is None:
        return SENTINEL_FLOAT
    if not isinstance(v, (int, float)):
        return SENTINEL_FLOAT
    if np.isnan(v) or np.isinf(v):
        return SENTINEL_FLOAT
    return float(v)

from Trading_bot_1_core import (
    logger, MarketRegime, TimeFrame,
    DataPackage, is_valid_df, get_tick_size
)
from Trading_bot_4_data import MarketDataService

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# ✅ 엔진 상수: global_constants에서 가져오기
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
from global_constants import (
    # 기술적 지표 엔진 상수
    VOLUME_MA_PERIOD,
    DEFAULT_ATR_PERIOD,
    ADX_DEFAULT_VALUE,
    EPSILON,
    DEFAULT_ANALYSIS_WINDOW,
    DEFAULT_VOLATILITY_VALUE,
    
    # 인덱스 상수
    LAST_INDEX,
    SECOND_LAST_INDEX,
    FIRST_INDEX,
    
    # 변환/범위 상수
    PERCENT_CONVERSION,
    SIGNAL_STRENGTH_MIN,
    SIGNAL_STRENGTH_MAX,
    DEFAULT_SCORE,
    
    # 가자미근
    GAJAMI_MIN_GAP,
    GAJAMI_MAX_GAP,
    
    # 페널티
    BEARISH_PENALTY,
    LOW_VOLUME_PENALTY,
    
    # 거래량
    MIN_VOLUME_RATIO,
    VOLUME_MA_PERIODS,
    
    # 불장
    BULL_ENTRY_COUNT,
    BULL_ENTRY_THRESHOLD,
    BULL_EXIT_COUNT,
    BULL_EXIT_THRESHOLD,
    BULL_FORCE_EXIT_RATIO,
    SYMBOL_BULL_LEVEL_1,
    SYMBOL_BULL_LEVEL_2,
    SYMBOL_BULL_LEVEL_3,
    
    # MA 분석
    MA_ANALYSIS_DEFAULT,
    MA_ANALYSIS_THRESHOLDS,
    MA_DIVERGENCE_THRESHOLDS as MA_DEVIATION_THRESHOLDS,
    MA_SLOPE_TREND_THRESHOLD,
    
    # 체제
    REGIME_HYSTERESIS_THRESHOLDS,
    REGIME_STABILIZATION_BY_TIMEFRAME,
    REGIME_STABILIZATION_SECONDS,
    
    # 시장 선택
    MARKET_SELECTION_CANDLES,
    MAX_MOMENTUM_WATCHLIST,
    MIN_TRADE_PRICE,
    
    # Track2 전략 품질 필터
    WATCHLIST_CHANGE_15M_MIN,
    WATCHLIST_CHANGE_30M_MAX,
    WATCHLIST_MIN_PRICE,
    
    # 지지/저항
    SUPPORT_RESISTANCE_LOOKBACK,
    
    # 기타
    DEFAULT_TIMEFRAME,
    DEFAULT_MIN_VOLUME_KRW,
    SECONDS_PER_MINUTE,
    WEBSOCKET_PING_INTERVAL,
    TIMEFRAME_CONFIGS
)

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 🛡️ 타입 안전 헬퍼 함수
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def safe_price(value) -> float:
    """
    가격 값을 안전하게 float로 변환
    
    Args:
        value: 변환할 값 (None, str, int, float 등)
        
    Returns:
        float: 변환된 값 또는 0.0
        
    Examples:
        >>> safe_price(100)
        100.0
        >>> safe_price("100.5")
        100.5
        >>> safe_price("NaN")
        0.0
        >>> safe_price("INF")
        0.0
        >>> safe_price(None)
        0.0
    """
    try:
        result = float(value)
        if not math.isfinite(result):
            return 0.0
        return result
    except (TypeError, ValueError):
        return 0.0

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 📊 MA 기울기 기준 (3분봉, 실측 ratio + 체감각(°) 주석)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# slope 단위: ratio (소수)
# 계산식: (MA_now - MA_prev) / MA_prev
#
# [⚠️ 중요]
# - 여기의 "체감각(°)"은 차트의 확대/축소(y축 스케일, 창 크기)에 따라 크게 달라지는
#   '시각적 체감'입니다. 실제 기하학 각도가 아닙니다.
# - 따라서 본 각도는 로직 판정에 절대 사용하지 않고, 사람-로그 눈높이 통일용으로만 씁니다.
#
# [체감각 환산식 — 참고용]
#   체감각(°) ≈ atan(slope_ratio * ANGLE_K_3M) * 180 / π
#
# [캘리브레이션 기준(사용자 체감 기준)]
#   - Upbit BTC 3분봉 (현재 화면 줌 기준)
#   - slope=0.00022 (0.022%) 가 차트에서 약 60°로 보임
#   ⇒ ANGLE_K_3M ≈ tan(60°) / 0.00022 ≈ 7873
#
ANGLE_K_3M = 7873  # 3분봉 "체감각" 보정 상수 (주석/로그 해석용, 판정 미사용)

# 참고표(대략치, 줌 바뀌면 달라짐)
# slope(ratio) | %변화   | 체감각(≈) | 의미(해석용)
# ---------------------------------------------------
# 0.00003      | 0.003%  |   ~13°    | 거의 평행
# 0.00005      | 0.005%  |   ~21°    | 미세 상승
# 0.00010      | 0.010%  |   ~38°    | 상승 시작
# 0.00015      | 0.015%  |   ~50°    | 명확한 상승(가자미근 후보)
# 0.00020      | 0.020%  |   ~57°    | 강한 상승(추격 주의)
# 0.00022      | 0.022%  |   ~60°    | 매우 강한 상승(사용자 체감 기준)
# 0.00025      | 0.025%  |   ~63°    | 과가속(리스크 증가)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

class SoleusGajamiConstants:
    """
    가자미근(soleus) 형상 판정 상수
    - 이 값들은 전략의 '정의'이며 CONFIG로 외부 조정하지 않음
    - slope 단위: ratio (소수)
    """

    NEWBORN_MAX_DUR = 2

    # 🔒 형상 판정용 신생 한계 — NEWBORN_MAX_DUR(=2, 생애판정)와 별개 축
    #   기존 L936·994·1003·1141·1155 에 하드코딩 "<=3" 으로 흩어져 있던 값
    NEWBORN_SHAPE_MAX_DUR = 3

    # ────────────────────────────────────────────
    # 🔒 MA5-MA10 간격 밴드 — SSoT (여기 한 곳에서만 계산)
    # ────────────────────────────────────────────
    @staticmethod
    def gap_band(dur_5x10_bars):
        """(하한, 상한) — gap_5_10_pct 단위 percent.
        실측 comparison 거래소원본 n=908 (2026-08-06~07, 로그 dur 조인)
          탄생봉과 성숙봉은 gap 의미가 정반대다
            dur<=2  gap 0.03~0.05 51.7%(n=29) · 0.50~0.80 10.0%(n=10)
            dur>=3  gap 0.50~0.80 47.8%(n=67) · 0.80~1.20 63.6%(n=22)
          dur 경계 민감도 1/2/3/4 전부 부호 유지 → 축 채택 요건 충족
        """
        _birth = (dur_5x10_bars is not None
                  and int(dur_5x10_bars) <= SoleusGajamiConstants.NEWBORN_MAX_DUR)
        if _birth:
            return 0.03, 0.50      # 탄생봉: 밀착만. 벌어지면 사망(10.0%)
        return 0.05, 1.20          # 성숙봉: 확산 허용
    
    # ────────────────────────────────────────────
    # MA5 기울기 기준
    # ────────────────────────────────────────────
    MA5_SLOPE_DEFAULT = 0.00015
    # 0.015% ≈ 체감각 ~17.6°
    # → MA5가 '고개를 들었다'고 인정하는 최소 기준
    # → 가자미근 "완성"이 아니라, '진입 자격'의 하한선

    MA5_SLOPE_CONVERGE = 0.00010
    # 0.01% ≈ 체감각 ~12°
    # → 수렴 직후, 막 접히기 시작한 상태 (완화 조건)

    MA5_SLOPE_PRICE_ON_MA5 = 0.0
    # 가격이 MA5 위/아래에서 바로 붙어 있는 경우
    # → slope 요구치 면제 (체감각 의미 없음)
    
    # 🆕 MA5 강한 양수 (MA20 음수 보완용)
    MA5_SLOPE_STRONG_POS = 0.00020
    # 0.02% ≈ 체감각 ~60°
    # → MA20이 약한 음수일 때, MA5가 명확히 끌고 있어야 함

    MA5_SLOPE_PRECHECK_RATIO = 0.00030
    # 초입 포착 프리체크 하한, ratio 단위 (GROUP3 SSoT, t7 L2325 기존값)

    MA5_SLOPE_PULLBACK_RATIO = 0.00050
    # 중기 눌림 회복 시 MA5 최소 리드

    MA5_SLOPE_ALIVE_DIP_RATIO = 0.00000
    # 판사1 MA5 생존 공용 하한: -0.00% 얕은 U/V 눌림까지 허용

    MA5_TICK_UP_MIN = 0.20
    # 🔒 [MA5-TICK-UP-0816] MA5 고개듦 판정을 확정봉·틱 기준으로.
    # 🔒 [MA5-TICK-POSITIVE-LATTICE-0829] shared 헌법 전체(J1 전용 아님) 계약 변경.
    # MA5는 5봉 평균이라 동일 tick regime에서 1봉 변화는 0.2 틱 격자
    # (0.0, ±0.2, ±0.4, ...) 위에 놓인다. 기존 0.30은 이 격자 위에 없는
    # 값이라 실질 문턱이 0.40으로 동작했고, "확정봉 대비 실제 상승"을
    # 뜻하는 유일한 양수 격자점(+0.20)이 전부 차단됐다.
    # 이 값 인하는 완화다 — 판정 통과 집합이 넓어진다는 사실을 감추지
    # 않는다. 근거는 실측 승률이 아니라 "격자 위 최소 양수 스텝을
    # MA5 확정상승으로 인정한다"는 헌법적 정의이며, 이 함수는
    # J1/lowopen 전용이 아닌 전체 진입 판정 공용 게이트다.
    # COW 0.13·ENSO 0.28 두 예시는 이제 (BLOCK)이 아니라 (구 BLOCK,
    # 0.20 계약에서는 PASS)로 재해석해야 한다. CAP 0.40은 여전히 PASS.

    MA5_TICK_UP_EPS = 1e-6
    # 🔒 [MA5-TICK-LATTICE-EPS-0830]
    # +0.20 격자점이 float 계산에서 0.20보다 미세하게 작아지는 경우만 흡수한다.
    # 관측된 다음 하위 양수값은 +0.14이므로 물리적 격자 문턱은 0.20 그대로다.
    
    @classmethod
    def ma5_alive_ratio(cls, slope_ratio) -> bool:
        """판사1 MA5 생존 SSoT — ratio 단위."""
        try:
            return float(slope_ratio or 0.0) >= -cls.MA5_SLOPE_ALIVE_DIP_RATIO
        except Exception:
            return False

    @staticmethod
    def ma5_tick_up(ma5_series):
        """확정봉 1봉 MA5 변화를 틱으로 환산. 실패 시 None.
        🔒 [MA5-TICK-UP-SSOT-0817] L1553·L9340·L9817 3중복 통합. 동작 변화 0."""
        try:
            _now = float(ma5_series.iloc[-2])
            _prev = float(ma5_series.iloc[-3])
            _tick = float(get_tick_size(_now) or 0.0)
            return (_now - _prev) / _tick if _tick > 0.0 else 0.0
        except Exception:
            return None

    MA5_SLOPE_STRONG_LEAD_RATIO = 0.00170
    # MA10이 더 빨라도 MA5 강주도로 인정하는 승자 수준

    MA5_SLOPE_LIFT_RATIO = 0.00200
    # GC 생존·높이 확인 공용 MA5 lift floor

    MA5_SLOPE_BIRTH_FLOOR_RATIO = 0.00150
    # dur≤3 탄생 형상 전용 판별선
    # ATH형 정답봉은 살리되 momentum_confirmed의 0.00200 기준은 유지

    MA5_SLOPE_QUANT_RATIO = 0.00300
    # 강한 형상·양자화 회복 판정용 MA5 slope
    
    # ────────────────────────────────────────────
    # MA10 기울기 기준
    # ────────────────────────────────────────────
    MA10_SLOPE_DEFAULT = 0.00005
    # 0.005% ≈ 체감각 ~6~10°
    # → MA10이 하락을 멈추고, 방향 전환 준비가 됐는지 확인

    MA10_SLOPE_CONVERGE = 0.00003
    # 0.003% ≈ 체감각 ~3~5°
    # → 거의 평행, 수렴 구간의 바닥 허용치
    NEWBORN_TLC_MA10_SLOPE_MIN_RATIO = 0.0
    # newborn_tlc_alive 전용: JUDGE1 s10 ratio 하한
    MA10_SLOPE_HALT = -0.00030
    # -0.03% ≈ 체감각 -10° 내외

    MA10_POSITIVE_ABS_FLOOR = 0.00050
    # +0.05%: retain 50% 미달이어도 절대 양수 기울기 유지 시 추세 생존 인정
    # 250~1000원대 양자화 둔화 구간(SOON 13:39류) 억울 차단 방지

    # ────────────────────────────────────────────
    # 골든크로스(GC) 유효기간 (타임프레임별)
    # ────────────────────────────────────────────
    # 3m: (slope_ratio_threshold, bars) — MA5 lookback=2 (3봉) 기준
    # 🔒 [2026-08-02] 골크 유효기간을 사장님 기준(DUR4)으로 통일.
    #  기준: 확정봉 골크 다음봉(DUR2) 진입이 정석, 1봉 밀림 감안해 DUR4까지.
    #  불일치 실태: 미니GC(편입 문지기) 0~3봉 / 진입로직 10~12봉(30~36분)
    #              1m=5봉 · 5m·10m=2봉 · 15m+=1봉 인데 3m만 12봉으로 3배 관대했다.
    #  2026-04-27까지 [4,5,6,7] → 05-12 사이에 2배 완화된 것이 원인(스냅샷 대조).
    #  실측 KRW-ONDO 08-02: 열십자 골크 12:30 → 진입 12:45(dur=6, 15분 지연) −0.70%.
    #  ※ dur=5+ 승률(24%)은 dur=3~4(26%)와 큰 차이 없다.
    #    이 패치의 근거는 EV가 아니라 세 곳으로 갈라진 신선도 정의의 통일이다.
    # 🔒 [GC-FRESH-TABLE-DEAD-0817] 모든 구간이 동일한 4봉이므로 죽은 기울기 분기 제거.
    RECENT_GC_BARS_3M = [
        (0.0000, 4),
    ]
    
    # 🆕 MA10 양수 (MA20 음수 보완용)
    # 🔒 [MA10-SLOPE-POS-RESTORE-0817] 08-09 부호반전 오류 복원. 새 튜닝 아님.
    MA10_SLOPE_POS = 0.00005

    # ────────────────────────────────────────────
    # MA20 기울기 기준 : 로직 변경 없이 여기만 최적으로 조정
    # ────────────────────────────────────────────
    # 보수적
    # MA20_SLOPE_WEAK_NEG = -0.00008      # Freezing : -0.008% (약음수 허용 구간 축소)
    # MA20_SLOPE_PULLBACK_MIN = -0.00038  # -0.038% (전환 초입 + Patch 7 공용)
    # MA20_SLOPE_DECEL_LIMIT = -0.00100   # Freezing : -0.10% (HCG ④ 비초입 보수화)
    # MA20_SLOPE_DECEL_LIMIT_EARLY = -0.00200  # Freezing : -0.20% (HCG ④ 초입 소폭 강화)
    # 조정 중
    MA20_SLOPE_MIN = -0.0003            # Freezing : -0.03% (가자미근 기본 공식)
    # 🆕 MA20 약한 음수 허용 구간 (단기선 강함 필요)
    MA20_SLOPE_WEAK_NEG = -0.00010      # Freezing : -0.01% (단기선 보완용 약음수 허용)
    # 🔥 PULLBACK 허용 하한(정석)
    MA20_SLOPE_PULLBACK_MIN = -0.00025  # -0.025% (MA20 하락 초기 차단, 중간 보수)
    MA20_SLOPE_DECEL_LIMIT = -0.00150   # Freezing : 
    MA20_SLOPE_DECEL_LIMIT_EARLY = -0.00260 # Freezing : 

    # ────────────────────────────────────────────
    # MA5 기울기 기준 : 로직 변경 없이 여기만 최적으로 조정
    # ────────────────────────────────────────────
    MA5_SLOPE_CORE_MIN = 0.00080        # Freezing : 0.10% → 0.08%(거의 평행)
    MA10_SLOPE_NEWBORN_MIN = -0.00055   # 탄생봉(dur≤3) MA10 GC 직후 음수 허용 하한 # -0.00005 -> -0.00055  20260326
    
    # 🆕 MA5 1봉 방향 TIER별 기준 (방향 전환 확인용 노이즈 필터)
    # # 보수적 (1봉 기준)
    # MA5_DIR_MIN_TIER1 = 0.0010        # 대형 (BTC, ETH – 초입 허용)
    # MA5_DIR_MIN_TIER2 = 0.0015        # 고가 (≥ high_price_threshold)
    # MA5_DIR_MIN_TIER3 = 0.0020        # 일반 (STX 20:12 실측 Δ=0.00098 통과)
    # # 조정 중 : (2봉 기준) 
    MA5_DIR_MIN_TIER1 = 0.0007          # 대형 (BTC, ETH – 2봉 기준 x1.2)
    MA5_DIR_MIN_TIER2 = 0.0010          # 고가 (≥ high_price_threshold, 2봉 기준 x1.2) # 0.0015→0.0010
    MA5_DIR_MIN_TIER3 = 0.0030          # 일반 (fallback: 동적 계산 실패 시)  # 🔧 0.28%→0.30% 미세 상향
    MA5_DIR_TICK_MULTIPLIER = 1.0       # 🆕 틱 기반 동적 배수 (유일한 튜닝 노브)
    # # 너무 느슨함(2봉 기준)
    # MA5_DIR_MIN_TIER1 = 0.0005        # 대형 (BTC, ETH – 초입 허용)
    # MA5_DIR_MIN_TIER2 = 0.0008        # 고가 (≥ high_price_threshold)
    # MA5_DIR_MIN_TIER3 = 0.0010        # 일반

    # 수렴 후보 인식
    GAP_5_10_TIGHT = 0.001          # 초근접 (0.1%, GC 불문)
    GAP_5_10_EARLY = 0.005          # 조기 수렴 (0.5%)
    GC_COUNT_EARLY = 2              # 조기 수렴 최소 GC (기존 3 → 완화)
    GC_CONVERGE_MIN = 3             # 기본 수렴 최소 GC (기존 6 → 완화)
    MA_LONG_SLOPE_STEEP_DEAD = 0.00300  # TD/RG 장기선 강하향 사망 임계 ≤-0.30%
    MA_LONG_SLOPE_ALIVE_FLOOR = 0.00100  # MA120 near-flat 생존 하한 -0.10%
    MA60_STRUCT20_REQUIRED_BARS = 81      # 확정 MA60의 20봉 구조 slope 성립 최소 봉수
    # 👁️ [PREV-BAR-QUALITY-OBS-0813] PREV_BAR 품질값은 관측값으로만 유지하며
    # threshold 판정은 사용하지 않는다 (구 PREV_BAR_BODY_MIN_PCT_3M=0.30,
    # PREV_BAR_VOLUME_MIN_RATIO_3M=0.80 — 사용처 0건 확인 후 제거).
    # 🔒 [2026-08-02] MA 교차 판정 문턱의 가격 대비 상한.
    #  호가 양자화 노이즈 방어는 유지하되, 굵은 틱 종목에서 문턱이 가격의 0.2%까지
    #  치솟아 교차 자체를 못 보는 것을 막는다.
    DC_MARGIN_MAX_RATIO = 0.0003          # 0.03%

def _long_ma_alive_verdict(
    ma60_slope,
    ma120_slope,
    regime,
    ma5_slope=0.0,
    dur_5x10_bars=None,
) -> tuple:
    """장기선 생존 판정 SSoT."""
    _steep = SoleusGajamiConstants.MA_LONG_SLOPE_STEEP_DEAD
    _alive_floor = SoleusGajamiConstants.MA_LONG_SLOPE_ALIVE_FLOOR

    def _read_long_slope(value):
        # 미생성/미계산(SENTINEL_FLOAT=-9999.0)은 "값 없음"으로 분리.
        # 0.0으로 위조하면 near-flat 생존(>=-alive_floor)으로 오판됨.
        try:
            _v = float(value)
        except Exception:
            return 0.0, False
        return _v, bool(math.isfinite(_v) and _v != SENTINEL_FLOAT)

    _s60, _has60 = _read_long_slope(ma60_slope)
    _s120, _has120 = _read_long_slope(ma120_slope)
    try:
        _s5 = float(ma5_slope)
    except Exception:
        _s5 = 0.0
    try:
        _dur = (
            int(dur_5x10_bars)
            if dur_5x10_bars is not None
            else 999
        )
    except Exception:
        _dur = 999
    # 🔒 [LONG-MA-BIRTH-ROLE-0826] + [FRESH-BIRTH-MID-DEAD-EXEMPT-0829]
    # fresh GC BIRTH에서는 MA120의 오래된 장기 하락 기억이
    # 단독으로 현재 상승 전환을 hard veto하지 않는다.
    #
    # ★380-R2는 MID_DEAD만 먼저 개방한다. 현재 파동에 더 가까운
    # MA60의 strong-down(<= -0.30%)은 08-26 계약대로
    # LONG_STRONG_DOWN hard veto를 유지한다.
    #
    # 신규 임계 없음:
    # - GC 나이    : 기존 NEWBORN_MAX_DUR
    # - MA5 고개듦 : 기존 MA5_SLOPE_CORE_MIN
    # - MA60 사망  : 기존 MA_LONG_SLOPE_STEEP_DEAD
    _fresh_birth_ctx = bool(
        regime in (
            MarketRegime.TRENDING_DOWN,
            MarketRegime.RANGING,
        )
        and _dur
        <= SoleusGajamiConstants.NEWBORN_MAX_DUR
        and _s5
        >= SoleusGajamiConstants.MA5_SLOPE_CORE_MIN
        and _has60
    )
    if _fresh_birth_ctx:
        if _s60 <= -_steep:
            return False, "LONG_STRONG_DOWN"
        # 🔒 [FRESH-BIRTH-LONG-DEAD-KEEP-0829] ★432
        # ★380-R2는 MID_DEAD만 개방한다.
        # fresh BIRTH에서도 기존 장기선 생존 계약은 유지한다:
        # MA60 상승 OR MA120 near-flat 이상이 아니면 LONG_DEAD.
        if not (
            (_has60 and _s60 > 0.0)
            or (_has120 and _s120 >= -_alive_floor)
        ):
            return False, "LONG_DEAD"
        return True, None
    # BIRTH 밖에서는 현행 장기선 계약을 그대로 유지한다.
    _strong_down = bool(
        regime in (MarketRegime.TRENDING_DOWN, MarketRegime.RANGING)
        and (
            (_has60 and _s60 <= -_steep)
            or (_has120 and _s120 <= -_steep)
        )
    )
    if _strong_down:
        return False, "LONG_STRONG_DOWN"
    if not (
        (_has60 and _s60 > 0.0)
        or (_has120 and _s120 >= -_alive_floor)
    ):
        return False, "LONG_DEAD"
    return True, None

def _struct20_slope_from_closes(closes, window: int) -> float:
    """미니 closed close 배열 기준 STRUCT20 slope(fraction).
    c[-1]=iloc[-2] 확정봉 대응. c[-21]=iloc[-22] STRUCT20 기준.
    ⚠️ [단일로직-동기] 가자미 헌법 slope(L5382~5459)와 동일 로직. 한쪽 바꾸면 양쪽 같이.
    """
    try:
        n = len(closes)
        window = int(window)
        if n < window + 1:
            return 0.0
        _now = sum(float(x) for x in closes[-window:]) / window
        if n >= window + 20:
            _prev = sum(float(x) for x in closes[-(window + 20):-20]) / window
        else:
            _prev = sum(float(x) for x in closes[-(window + 1):-1]) / window
        if not math.isfinite(_now) or not math.isfinite(_prev) or _prev <= 0.0:
            return 0.0
        return (_now - _prev) / _prev
    except Exception:
        return 0.0
    
def _gc_newborn_context(
    dur_5x10_bars,
    ma5_slope: float,
    ma10_slope: float = None,
) -> bool:
    """GC 탄생봉 보호 컨텍스트 — 외부 솔레우스 계산부 전용

    [is_gajami_shape 내부 _is_gc_newborn과 구분]
    - dur<=2 기준 (shape 내부는 dur<=3)
    - ma5_slope >= MA5_SLOPE_CORE_MIN (실제 코드 기준)
    - 2단계에서 veto3 / TOP-RISE-BLOCK / MID-TREND-BEAR 교체 예정
    """
    dur = int(dur_5x10_bars) if dur_5x10_bars is not None else 999
    return bool(
        dur <= SoleusGajamiConstants.NEWBORN_MAX_DUR
        and ma5_slope >= SoleusGajamiConstants.MA5_SLOPE_CORE_MIN
        and (ma5_slope - ma10_slope) > 0.0001
    )

def _calc_lifecycle_state(
    *,
    ma5_now: float,
    ma10_now: float,
    dur_5x10_bars,
    structure_ok: bool,
    ma10_dead_gate: bool,
    regime_is_td: bool,
    trend_alive: bool,
) -> tuple:
    """가자미근 lifecycle의 최종 state/reason SSoT."""
    try:
        _dur = int(dur_5x10_bars) if dur_5x10_bars is not None else None
    except (TypeError, ValueError):
        _dur = None

    _newborn = bool(_dur is not None and _dur <= SoleusGajamiConstants.NEWBORN_MAX_DUR)

    if ma5_now > 0.0 and ma10_now > 0.0 and ma5_now < ma10_now and not _newborn:
        return "DEAD", "DEAD_CROSS"

    if _dur is None or _dur < 1:
        return "DEAD", "NO_GOLDEN"

    if not structure_ok:
        return "DEAD", "BREAK_STRUCTURE_LOW"

    # legacy는 비TD MA10-SLOPE-RELEASE에서도 초기값 DEAD/INIT을 유지한다.
    if ma10_dead_gate:
        return (
            ("DEAD", "MA10_SLOPE_DEAD")
            if regime_is_td
            else ("DEAD", "INIT")
        )

    # 🔒 [TREND-DEAD-PURGE-0825]
    # trend_alive는 관측/타 판사 입력으로 유지한다.
    # Lifecycle은 이미 위의 GC·structure·MA10 사망 조건으로 구조적 생존을 판정하므로,
    # 여기서 다시 추세 정배열을 요구해 DEAD로 중복 판결하지 않는다.
    return "VALID", "NONE"


def _confirmed_fractal_positions(values, mode: str, k: int = 2) -> list:
    """확정봉 fractal pivot 위치 SSoT — J2 divergence 전용."""
    try:
        _arr = np.asarray(values, dtype=float)
        _k = max(1, int(k))
        if len(_arr) < (_k * 2 + 1):
            return []

        _out = []
        for _i in range(_k, len(_arr) - _k):
            _center = float(_arr[_i])
            _left = _arr[_i - _k:_i]
            _right = _arr[_i + 1:_i + _k + 1]

            if not (
                math.isfinite(_center)
                and np.all(np.isfinite(_left))
                and np.all(np.isfinite(_right))
            ):
                continue

            if mode == "HIGH":
                _is_pivot = bool(
                    _center > float(np.max(_left))
                    and _center > float(np.max(_right))
                )
            else:
                _is_pivot = bool(
                    _center < float(np.min(_left))
                    and _center < float(np.min(_right))
                )

            if _is_pivot:
                _out.append(_i)

        return _out
    except Exception:
        return []


def _div_class_mw(highs, lows, closes, rsi, macd, atr, tick_fn):
    """🔒 [DIV-CLASS-GATE-1109] 사장님 순서 : ① W 인지 M 인지 ② W 일 때만 기울기로 유형 (J2 · J4 · J4-1 · J4-2).

    기준 그림 = outputs/images/다이버전스.png (Bullish = W · Bearish = M).
    입력 = 확정봉 마지막 70 개 (highs · lows · closes · rsi · macd 같은 길이) · atr = 확정봉 ATR (가격 단위).
    ① W = 저점 둘이 70 봉 범위 아래 1/3 · 두 번째 저점 >= 첫 저점 - 1 ATR · 넥라인 >= 두 저점 + 1 ATR ·
         좌우 간격 3 봉 이상 · 앞 하락 다리 2 ATR 이상
       M = 고점 둘이 위 1/3 · 높이 차 1 ATR 이하 · 지금 종가 범위 위 1/2
    ② W 일 때 W 두 저점 a · b (그 앞 저점 c) 의 가격선 · 오실레이터선 기울기 (기존 J2 · J4 품질 조건 유지) :
       최소 가격 차 g = max(1 틱, 0.20 x ATR) (기존 _price_quality · _pivot_gap_atr_ok 와 같은 뜻)
       J4_2_TRIPLE = 저점 셋 c > a > b (각 차 >= 1 틱) + 같은 오실레이터가 세 저점에서 연속 상승 (RSI c < a < b 또는 MACD c < a < b)
       J4_1_EXAG   = 두 저점 같은 높이 (차 <= 1 틱) + 오실레이터 상승 (반대 오실레이터는 내려가지 않음)
       J4_REGULAR  = 가격 b <= a - g + 오실레이터 상승 (반대 오실레이터는 내려가지 않음 · 기존 coherence)
       J2_HIDDEN   = 가격 b >= a + g + 오실레이터 하락 (반대 오실레이터는 올라가지 않음 · 기존 coherence)
       W_NODIV     = W 인데 위 어느 것도 아님
    반환 (shape, type, pos) · shape = W / M / OTHER · type 은 W 일 때만 (아니면 NONE).
    """
    try:
        _h = np.asarray(highs, dtype=float)
        _l = np.asarray(lows, dtype=float)
        _c = np.asarray(closes, dtype=float)
        _r = np.asarray(rsi, dtype=float)
        _m = np.asarray(macd, dtype=float)
        if len(_h) < 20 or not (atr and atr > 0):
            return "OTHER", "NONE", float("nan")
        _top, _bot = float(np.nanmax(_h)), float(np.nanmin(_l))
        _rng = _top - _bot
        if not (_rng > 0):
            return "OTHER", "NONE", float("nan")

        def _pos(v):
            return (v - _bot) / _rng

        _P = _confirmed_fractal_positions(_l, "LOW", 2)
        _Q = _confirmed_fractal_positions(_h, "HIGH", 2)
        _isW = False
        _a = _b = -1
        if len(_P) >= 2:
            _a, _b = _P[-2], _P[-1]
            if _b - _a >= 2 and _a >= 1:
                _ni = _a + 1 + int(np.argmax(_h[_a + 1:_b]))
                _neck = _h[_ni]
                _pre = float(np.max(_h[:_a]))
                _isW = bool(
                    _pos(_l[_a]) <= 1 / 3 and _pos(_l[_b]) <= 1 / 3
                    and _l[_b] >= _l[_a] - atr
                    and _neck - max(_l[_a], _l[_b]) >= atr
                    and _ni - _a >= 3 and _b - _ni >= 3
                    and _pre - _l[_a] >= 2 * atr
                )
        _isM = False
        if len(_Q) >= 2:
            _x, _y = _Q[-2], _Q[-1]
            _isM = bool(
                _pos(_h[_x]) >= 2 / 3 and _pos(_h[_y]) >= 2 / 3
                and abs(_h[_y] - _h[_x]) <= atr and _pos(_c[-1]) >= 0.5
            )
        _shape = "M" if _isM else ("W" if _isW else "OTHER")
        if _shape != "W":
            return _shape, "NONE", float(_pos(_c[-1]))
        if not all(np.isfinite(v) for v in (_r[_a], _r[_b], _m[_a], _m[_b])):
            return _shape, "W_NODIV", float(_pos(_c[-1]))
        _p1, _p2 = _l[_a], _l[_b]
        _tk = max(float(tick_fn(_p1) or 0.0), float(tick_fn(_p2) or 0.0))
        _g = max(_tk, 0.20 * float(atr))
        _up_coh = bool((_r[_b] > _r[_a] and _m[_b] >= _m[_a]) or (_m[_b] > _m[_a] and _r[_b] >= _r[_a]))
        _dn_coh = bool((_r[_b] < _r[_a] and _m[_b] <= _m[_a]) or (_m[_b] < _m[_a] and _r[_b] <= _r[_a]))
        _triple = False
        if len(_P) >= 3:
            _k = _P[-3]
            if np.isfinite(_r[_k]) and np.isfinite(_m[_k]):
                _triple = bool(
                    _l[_k] - _p1 >= _tk and _p1 - _p2 >= _tk
                    and ((_r[_k] < _r[_a] < _r[_b]) or (_m[_k] < _m[_a] < _m[_b]))
                )
        if _triple:
            _typ = "J4_2_TRIPLE"
        elif abs(_p2 - _p1) <= _tk + 1e-12 and _up_coh:
            _typ = "J4_1_EXAG"
        elif _p2 <= _p1 - _g + 1e-12 and _up_coh:
            _typ = "J4_REGULAR"
        elif _p2 >= _p1 + _g - 1e-12 and _dn_coh:
            _typ = "J2_HIDDEN"
        else:
            _typ = "W_NODIV"
        return _shape, _typ, float(_pos(_c[-1]))
    except Exception:
        return "ERR", "ERR", float("nan")


def _adaptive_tlc_swing_pivots(highs, lows, closes, atr_closed):
    """Directional-Change 기반 능동형 swing pivot.

    고정 k(프랙탈 창)·고정 ATR 배수를 쓰지 않는다. reversal 폭을
    sqrt(noise × wave_amplitude)로 파동 크기에 비례해 자동 확대한다.
    noise = 해당 확정봉 ATR (0/SENTINEL이면 해당봉 True Range로 대체).
    wave = 현재 swing 시작점(anchor) → 현재 극점까지의 실제 가격폭.

    반환: (pivots, last_reversal, last_wave_atr)
      pivots: [(idx, "H"/"L", price), ...] — 기존 _pivots와 동일한 형식.
      last_reversal: 가장 최근에 쓰인 reversal 폭(가격 단위).
      last_wave_atr: 가장 최근 wave_amp / noise (배수, 진단용).
    """
    try:
        _n = len(highs)
        if _n < 2 or len(lows) != _n or len(closes) != _n:
            return [], SENTINEL_FLOAT, SENTINEL_FLOAT

        _pivots = []
        _mode = 0          # 0=미확정, 1=상승탐색(고점 갱신 대기), -1=하락탐색
        _hi_i, _hi = 0, float(highs[0])
        _lo_i, _lo = 0, float(lows[0])
        _anchor_price = float(closes[0])
        _last_reversal = SENTINEL_FLOAT
        _last_wave_atr = SENTINEL_FLOAT
        _tlc_dc_reversal_scale = 1.5

        for _i in range(_n):
            _h_i = float(highs[_i])
            _l_i = float(lows[_i])

            try:
                _tr_i = float(atr_closed[_i])
            except Exception:
                _tr_i = SENTINEL_FLOAT
            if (
                not math.isfinite(_tr_i)
                or _tr_i <= 0.0
                or _tr_i == SENTINEL_FLOAT
            ):
                _prev_close = float(closes[_i - 1]) if _i > 0 else _l_i
                _tr_i = max(
                    _h_i - _l_i,
                    abs(_h_i - _prev_close),
                    abs(_l_i - _prev_close),
                )
            _noise = _tr_i
            if not math.isfinite(_noise) or _noise <= 0.0:
                continue

            _mid = max((_h_i + _l_i) * 0.5, EPSILON)
            try:
                _tick = float(get_tick_size(_mid) or 0.0)
            except Exception:
                _tick = 0.0

            if _mode == 0:
                if _h_i >= _hi:
                    _hi_i, _hi = _i, _h_i
                if _l_i <= _lo:
                    _lo_i, _lo = _i, _l_i
                _wave_amp = max(_hi - _lo, _noise)
                _reversal = max(
                    _tlc_dc_reversal_scale * math.sqrt(_noise * _wave_amp),
                    _tick,
                )
                _last_reversal = _reversal
                _last_wave_atr = _wave_amp / _noise
                if (_hi - _lo) >= _reversal:
                    if _lo_i < _hi_i:
                        _pivots.append((_lo_i, "L", _lo))
                        _mode = 1
                        _anchor_price = _lo
                    elif _hi_i < _lo_i:
                        _pivots.append((_hi_i, "H", _hi))
                        _mode = -1
                        _anchor_price = _hi
                continue

            if _mode == 1:
                if _h_i >= _hi:
                    _hi_i, _hi = _i, _h_i
                _wave_amp = max(_hi - _anchor_price, _noise)
                _reversal = max(
                    _tlc_dc_reversal_scale * math.sqrt(_noise * _wave_amp),
                    _tick,
                )
                _last_reversal = _reversal
                _last_wave_atr = _wave_amp / _noise
                if (
                    _hi_i < _i
                    and _l_i <= (_hi - _reversal)
                ):
                    _pivots.append((_hi_i, "H", _hi))
                    _mode = -1
                    _anchor_price = _hi
                    _lo_i, _lo = _i, _l_i
            else:
                if _l_i <= _lo:
                    _lo_i, _lo = _i, _l_i
                _wave_amp = max(_anchor_price - _lo, _noise)
                _reversal = max(
                    _tlc_dc_reversal_scale * math.sqrt(_noise * _wave_amp),
                    _tick,
                )
                _last_reversal = _reversal
                _last_wave_atr = _wave_amp / _noise
                if (
                    _lo_i < _i
                    and _h_i >= (_lo + _reversal)
                ):
                    _pivots.append((_lo_i, "L", _lo))
                    _mode = 1
                    _anchor_price = _lo
                    _hi_i, _hi = _i, _h_i

        return (
            _pivots,
            float(_last_reversal),
            float(_last_wave_atr),
        )
    except Exception:
        return [], SENTINEL_FLOAT, SENTINEL_FLOAT


def _active_tlc_price_structure(
    df,
    atr_series=None,
    timeframe="3m",
) -> dict:
    """현재 진행 가격파동 기반 TLC 부모 생애주기 SSoT."""
    _out = {
        "ready": False,
        "phase": "COMPRESSION",
        "leg": "SIDE",
        "structure": "UNCONFIRMED",
        "active_pos": 0.5,
        "active_pos_closed": 0.5,
        "retrace": 0.0,
        "progress": 0.0,
        "leg_bars": 0,
        "prev_leg_bars": 0,
        "leg_time_ratio": SENTINEL_FLOAT,
        "efficiency": 0.0,
        "active_low": SENTINEL_FLOAT,
        "active_high": SENTINEL_FLOAT,
        "anchor_price": SENTINEL_FLOAT,
        # 전체 가용 window 통계축 — 부모 swing과 별개.
        "window_pos": 0.5,
        "window_hi_recent": 0.0,
        "window_lo_recent": 0.0,
        "window_width_pct": 0.0,
        # 🔒 [ACTIVE-TLC-PARENT-RANGE-SSOT-0828]
        # 마지막 두 확정 opposite DC pivot이 만드는 부모 swing range.
        "parent_ready": False,
        "parent_pos": SENTINEL_FLOAT,
        "parent_pos_closed": SENTINEL_FLOAT,
        "parent_low": SENTINEL_FLOAT,
        "parent_high": SENTINEL_FLOAT,
        "parent_wave_id": "NONE",
        "pivot_highs": 0,
        "pivot_lows": 0,
        "swing_source": "NONE",
        "swing_reversal": SENTINEL_FLOAT,
        "swing_wave_atr": SENTINEL_FLOAT,
        "obs_first_high_break_bars": -1,
        "obs_first_high_break_age": -1,
    }

    try:
        if (
            df is None
            or len(df) < 6
            or not {"high", "low", "close"}.issubset(df.columns)
        ):
            return _out

        # Parent swing = 확정봉 directional-change.
        # 고정 k=2 프랙탈/고정 ATR 배수를 쓰지 않고, 파동 진폭에 따라
        # reversal 폭이 sqrt(ATR(or TR) × wave_amplitude)로 능동 확대된다.
        # 현재봉은 기존대로 active extreme/위치 계산에만 참가한다.
        # 🔒 [ACTIVE-TLC-CLOSED-BAR-SSOT-0904] ★508
        # 마지막 row를 무조건 미완성으로 버리지 않고 실제 종료시각으로 판정한다.
        # live active extreme/위치 계산(아래 full df)은 그대로 intrabar를 유지한다.
        _closed = TechnicalIndicators.drop_incomplete_candle(df, timeframe)
        _ch = _closed["high"].astype(float).to_numpy()
        _cl = _closed["low"].astype(float).to_numpy()
        _cc = _closed["close"].astype(float).to_numpy()

        if (
            hasattr(atr_series, "iloc")
            and len(atr_series) >= len(df)
        ):
            _atr_base = atr_series.astype(float).iloc[-len(df):]
            _atr_closed = (
                _atr_base.iloc[:len(_closed)]
                .to_numpy()
            )
        else:
            _atr_closed = np.full(
                len(_closed),
                SENTINEL_FLOAT,
                dtype=float,
            )

        (
            _pivots,
            _swing_reversal,
            _swing_wave_atr,
        ) = _adaptive_tlc_swing_pivots(
            _ch,
            _cl,
            _cc,
            _atr_closed,
        )

        _out["pivot_highs"] = sum(
            1 for _p in _pivots
            if _p[1] == "H"
        )
        _out["pivot_lows"] = sum(
            1 for _p in _pivots
            if _p[1] == "L"
        )
        _out["swing_source"] = "ADAPTIVE_DC"
        _out["swing_reversal"] = float(
            _swing_reversal
        )
        _out["swing_wave_atr"] = float(
            _swing_wave_atr
        )

        _ah = df["high"].astype(float).to_numpy()
        _al = df["low"].astype(float).to_numpy()
        _ac = df["close"].astype(float).to_numpy()

        _end = len(df) - 1
        # 🔒 [ACTIVE-TLC-CLOSED-BAR-SSOT-0904] ★508
        # 확정 구조(HH/LL break) 전용 경계. 정상 intrabar에서는 _end와 동일하고,
        # 마지막 row가 이미 종료된 sparse 구간에서만 그 row까지 포함한다.
        _closed_stop = len(_closed)
        _closed_last = _closed_stop - 1
        _cur = float(_ac[-1])

        # Window Context — 가용 데이터 전체.
        # 부모 swing 위치가 아니라 장기 window 통계축으로만 유지한다.
        _p_hi_i = int(np.argmax(_ah))
        _p_lo_i = int(np.argmin(_al))
        _p_hi = float(_ah[_p_hi_i])
        _p_lo = float(_al[_p_lo_i])
        _p_rng = _p_hi - _p_lo

        if _p_rng > 0.0:
            _out["window_pos"] = float(
                np.clip(
                    (_cur - _p_lo) / _p_rng,
                    0.0,
                    1.0,
                )
            )
            _out["window_width_pct"] = (
                (_p_rng / _p_lo) * 100.0
                if _p_lo > 0.0
                else 0.0
            )

        _out["window_hi_recent"] = (
            _p_hi_i / float(_end)
            if _end > 0
            else 0.0
        )
        _out["window_lo_recent"] = (
            _p_lo_i / float(_end)
            if _end > 0
            else 0.0
        )

        # Pivot 없는 단조 추세.
        _live_structure = "UNCONFIRMED"
        if not _pivots:
            if _cur >= float(_ac[0]):
                _anchor = int(np.argmin(_al))
                _leg = "UP_LEG"
                _phase = "EXPANSION"
            else:
                _anchor = int(np.argmax(_ah))
                _leg = "DOWN_LEG"
                _phase = "EXHAUSTION"

            _structure = "UNCONFIRMED"
            _prev_opp = None

        else:
            _high_pivots = [
                p for p in _pivots
                if p[1] == "H"
            ]
            _low_pivots = [
                p for p in _pivots
                if p[1] == "L"
            ]

            _hs = (
                "HH"
                if len(_high_pivots) >= 2
                and _high_pivots[-1][2] > _high_pivots[-2][2]
                else "LH"
                if len(_high_pivots) >= 2
                else None
            )
            _ls = (
                "HL"
                if len(_low_pivots) >= 2
                and _low_pivots[-1][2] > _low_pivots[-2][2]
                else "LL"
                if len(_low_pivots) >= 2
                else None
            )

            _structure = (
                f"{_hs}_{_ls}"
                if _hs and _ls
                else "UNCONFIRMED"
            )

            # anchor와 leg는 마지막 확정 swing pivot으로만 교체한다.
            # provisional high/low는 anchor를 뒤집을 권한이 없다.
            _last = _pivots[-1]
            _anchor = int(_last[0])

            _break_high = False
            _break_low = False

            if _last[1] == "L":
                _leg = "UP_LEG"

                _prev_highs = [
                    p for p in _high_pivots
                    if p[0] < _anchor
                ]
                _prev_opp = (
                    int(_prev_highs[-1][0])
                    if _prev_highs
                    else None
                )
                _prev_high = (
                    float(_prev_highs[-1][2])
                    if _prev_highs
                    else None
                )

                _active_high_now = float(
                    np.max(_ah[_anchor:])
                )
                _break_high = bool(
                    _prev_high is not None
                    and _active_high_now > _prev_high
                )
                # 🔬 [ACTIVE-TLC-P3-OBS-0815] UP_LEG 대칭 관측
                # 부모 구조 후보는 확정봉까지만 사용한다. 현재가는 anchor L
                # 하방 이탈 관측에만 사용하며 leg/phase/anchor는 바꾸지 않는다.
                _out["obs_up_downbreak"] = int(
                    _cur < float(_last[2])
                )
                _out["obs_fix_high_state"] = "NA"
                _out["obs_fix_high_anchor_bars"] = -1

                # L pivot 봉 자체의 high와 현재 미확정봉은 제외한다.
                # L pivot 봉 자체의 high는 제외한다. 실제 미완성봉만 제외한다.
                _seg_hi_closed = _ah[_anchor + 1:_closed_stop]
                if len(_seg_hi_closed) > 0:
                    _obs_fix_high_anchor = (
                        _anchor + 1 + int(np.argmax(_seg_hi_closed))
                    )
                    _obs_fix_high = float(_ah[_obs_fix_high_anchor])

                    _out["obs_fix_high_anchor_bars"] = (
                        _obs_fix_high_anchor - _anchor
                    )

                    if _prev_high is not None:
                        _out["obs_fix_high_state"] = (
                            "HH"
                            if _obs_fix_high > float(_prev_high)
                            else "LH"
                            if _obs_fix_high < float(_prev_high)
                            else "EQH"
                        )

                        # 🔬 [ACTIVE-TLC-P3-HH-EVENT-OBS-0815]
                        # 최고점 위치(fixhlag)가 아니라
                        # 직전 H를 처음 넘은 확정봉과 그 경과를 계측한다.
                        _hh_cross_pos = np.flatnonzero(
                            _seg_hi_closed > float(_prev_high)
                        )
                        if len(_hh_cross_pos) > 0:
                            _first_hh_idx = (
                                _anchor + 1 + int(_hh_cross_pos[0])
                            )
                            _out["obs_first_high_break_bars"] = (
                                _first_hh_idx - _anchor
                            )
                            _out["obs_first_high_break_age"] = (
                                _closed_last - _first_hh_idx
                            )

            else:
                _leg = "DOWN_LEG"

                _prev_lows = [
                    p for p in _low_pivots
                    if p[0] < _anchor
                ]
                _prev_opp = (
                    int(_prev_lows[-1][0])
                    if _prev_lows
                    else None
                )
                _prev_low = (
                    float(_prev_lows[-1][2])
                    if _prev_lows
                    else None
                )

                _active_low_now = float(
                    np.min(_al[_anchor:])
                )
                _break_low = bool(
                    _prev_low is not None
                    and _active_low_now < _prev_low
                )
                # 🔬 [ACTIVE-TLC-P3-OBS-0815] DOWN_LEG 전환 선행 관측
                # 마지막 확정 H 이후의 저점 구조는 돌파 전부터 계속 추적한다.
                # 구조 후보는 확정봉까지만 사용하고, 현재가는 H 상방 돌파
                # 관측에만 사용한다. leg/phase/anchor는 바꾸지 않는다.
                _out["obs_down_upbreak"] = int(
                    _cur > float(_last[2])
                )
                _out["obs_fix_low_state"] = "NA"
                _out["obs_fix_anchor_bars"] = -1
                # H pivot 봉 자체와 현재 미확정봉은 제외한다.
                # H pivot 봉 자체는 제외한다. 실제 미완성봉만 제외한다.
                _seg_lo_closed = _al[int(_last[0]) + 1:_closed_stop]
                if len(_seg_lo_closed) > 0:
                    _obs_fix_anchor = (
                        int(_last[0]) + 1
                        + int(np.argmin(_seg_lo_closed))
                    )
                    _obs_fix_low = float(_al[_obs_fix_anchor])
                    _out["obs_fix_anchor_bars"] = (
                        _obs_fix_anchor - int(_last[0])
                    )
                    if _prev_low is not None:
                        _out["obs_fix_low_state"] = (
                            "HL"
                            if _obs_fix_low > float(_prev_low)
                            else "LL"
                            if _obs_fix_low < float(_prev_low)
                            else "EQ"
                        )

            # 🔒 [ACTIVE-TLC-LIVE-STRUCT-0816]
            # 확정 DC 피벗은 부모 구조로 보존하고, 현재 진행 leg의 확정봉
            # 고/저점 관측을 결합해 현재 자식 구조를 별도로 만든다.
            # obs_fix_* 는 현재 미확정봉을 이미 제외한다.
            _live_hs = _hs
            _live_ls = _ls
            if _leg == "UP_LEG":
                _obs_hs = str(_out.get("obs_fix_high_state", "NA") or "NA")
                if _obs_hs in ("HH", "LH"):
                    _live_hs = _obs_hs
            else:
                _obs_ls = str(_out.get("obs_fix_low_state", "NA") or "NA")
                if _obs_ls in ("HL", "LL"):
                    _live_ls = _obs_ls
            _live_structure = (
                f"{_live_hs}_{_live_ls}"
                if _live_hs and _live_ls
                else _structure
            )
            # 🔒 [ACTIVE-TLC-LIVE-PHASE-SSOT-0829] ★418
            # 현재 자식구조 × leg × swing 돌파 → 현재 phase.
            # 유일한 번역 지점. branch별 독립 phase 판정 금지.
            if _leg == "UP_LEG":
                # UP_LEG의 _live_structure는 현재 진행 leg의 확정 H축을 갱신하고,
                # L축은 확정 부모구조를 유지한다.
                # 따라서 LH_LL / HH_LL의 기존 LL 방어는 그대로 보존하면서,
                # HH_HL → LH_HL 같은 현재 구조 악화는 즉시 phase에 반영한다.
                if _live_structure in ("LH_LL", "HH_LL"):
                    _phase = "COMPRESSION"

                elif _live_structure == "HH_HL":
                    _phase = (
                        "EXPANSION"
                        if _break_high
                        else "IGNITION"
                    )

                elif _live_structure == "LH_HL":
                    _phase = (
                        "IGNITION"
                        if _break_high
                        else "COMPRESSION"
                    )

                else:
                    _phase = (
                        "IGNITION"
                        if _break_high
                        else "COMPRESSION"
                    )

            else:
                _phase = (
                    "EXHAUSTION"
                    if _break_low
                    else "COMPRESSION"
                )

        # 🔒 [ACTIVE-TLC-PARENT-RANGE-SSOT-0828]
        # 부모 위치 = 마지막 두 확정 opposite DC pivot 사이의 가격 range.
        # - 구조 anchor는 확정봉만 사용한다.
        # - live/closed 현재가만 각각 parent_pos/parent_pos_closed로 분리한다.
        # - pivot pair가 없으면 SENTINEL fail-open. window_pos로 대체하지 않는다.
        _parent_ready = False
        _parent_pos = SENTINEL_FLOAT
        _parent_pos_closed = SENTINEL_FLOAT
        _parent_low = SENTINEL_FLOAT
        _parent_high = SENTINEL_FLOAT
        _parent_wave_id = "NONE"
        try:
            if _pivots and _prev_opp is not None:
                _parent_prev = next(
                    (
                        _p for _p in reversed(_pivots[:-1])
                        if int(_p[0]) == int(_prev_opp)
                    ),
                    None,
                )
                _parent_last = _pivots[-1]
                if _parent_prev is not None:
                    _parent_a = float(_parent_prev[2])
                    _parent_b = float(_parent_last[2])
                    _parent_low = min(_parent_a, _parent_b)
                    _parent_high = max(_parent_a, _parent_b)
                    _parent_rng = _parent_high - _parent_low
                    if (
                        _parent_low > 0.0
                        and _parent_rng > 0.0
                    ):
                        _parent_ready = True
                        _parent_pos = float(
                            np.clip(
                                (_cur - _parent_low) / _parent_rng,
                                0.0,
                                1.0,
                            )
                        )
                        _parent_pos_closed = float(
                            np.clip(
                                (float(_cc[-1]) - _parent_low) / _parent_rng,
                                0.0,
                                1.0,
                            )
                        )
                        try:
                            _parent_from_bar = (
                                df.index[int(_prev_opp)].isoformat()
                            )
                        except Exception:
                            _parent_from_bar = str(
                                df.index[int(_prev_opp)]
                            )
                        try:
                            _parent_anchor_bar = (
                                df.index[int(_anchor)].isoformat()
                            )
                        except Exception:
                            _parent_anchor_bar = str(
                                df.index[int(_anchor)]
                            )
                        _parent_wave_id = (
                            f"{_parent_from_bar}>"
                            f"{_parent_anchor_bar}:{_leg}"
                        )
        except Exception:
            _parent_ready = False
            _parent_pos = SENTINEL_FLOAT
            _parent_pos_closed = SENTINEL_FLOAT
            _parent_low = SENTINEL_FLOAT
            _parent_high = SENTINEL_FLOAT
            _parent_wave_id = "NONE"
        # provisional extreme은 Active Wave 계측에만 사용
        _active_low = float(
            np.min(_al[_anchor:])
        )
        _active_high = float(
            np.max(_ah[_anchor:])
        )
        _active_rng = (
            _active_high - _active_low
        )

        _active_pos = (
            float(
                np.clip(
                    (_cur - _active_low) / _active_rng,
                    0.0,
                    1.0,
                )
            )
            if _active_rng > 0.0
            else 0.5
        )

        # 🔒 [J3-APOS-CLOSED-0823]
        # J3 구조 위치는 확정봉만 사용한다. live active_pos는 관측/조기반응용으로 유지.
        _active_pos_closed = 0.5
        try:
            _closed_end = len(_cc) - 1
            if _closed_end >= 0:
                if _pivots:
                    _closed_anchor = max(
                        0,
                        min(int(_pivots[-1][0]), _closed_end),
                    )
                elif float(_cc[-1]) >= float(_cc[0]):
                    _closed_anchor = int(np.argmin(_cl))
                else:
                    _closed_anchor = int(np.argmax(_ch))
                _closed_low = float(np.min(_cl[_closed_anchor:]))
                _closed_high = float(np.max(_ch[_closed_anchor:]))
                _closed_rng = _closed_high - _closed_low
                if _closed_rng > 0.0:
                    _active_pos_closed = float(
                        np.clip(
                            (float(_cc[-1]) - _closed_low) / _closed_rng,
                            0.0,
                            1.0,
                        )
                    )
        except Exception:
            _active_pos_closed = 0.5

        if _leg == "UP_LEG":
            _retrace = (
                float(
                    np.clip(
                        (_active_high - _cur) / _active_rng,
                        0.0,
                        1.0,
                    )
                )
                if _active_rng > 0.0
                else 0.0
            )
            _progress = _active_pos
            _anchor_price = float(
                _al[_anchor]
            )

        else:
            _retrace = (
                float(
                    np.clip(
                        (_cur - _active_low) / _active_rng,
                        0.0,
                        1.0,
                    )
                )
                if _active_rng > 0.0
                else 0.0
            )
            _progress = 1.0 - _active_pos
            _anchor_price = float(
                _ah[_anchor]
            )

        _leg_bars = max(
            0,
            _end - _anchor,
        )

        _prev_leg_bars = (
            abs(_anchor - _prev_opp)
            if _prev_opp is not None
            else 0
        )

        _leg_time_ratio = (
            _leg_bars / float(_prev_leg_bars)
            if _prev_leg_bars > 0
            else SENTINEL_FLOAT
        )

        _seg_close = _ac[_anchor:]
        _path = (
            float(
                np.abs(
                    np.diff(_seg_close)
                ).sum()
            )
            if len(_seg_close) >= 2
            else 0.0
        )
        _net = (
            abs(
                float(_seg_close[-1])
                - float(_seg_close[0])
            )
            if len(_seg_close) > 0
            else 0.0
        )

        _efficiency = (
            float(
                np.clip(
                    _net / _path,
                    0.0,
                    1.0,
                )
            )
            if _path > 0.0
            else 0.0
        )

        _out.update({
            "ready": True,
            "phase": _phase,
            "leg": _leg,
            "structure": _live_structure,
            "parent_structure": _structure,
            # 🔬 [TLC-BREAK-DECISION-OBSERVE-0830] ★440
            # _break_*는 pivot이 있을 때만 지역변수로 생성된다.
            # no-pivot 정상 경로는 break decision 부재이므로 0/0으로 export한다.
            "break_high": int(bool(_break_high)) if _pivots else 0,
            "break_low": int(bool(_break_low)) if _pivots else 0,
            "active_pos": _active_pos,
            "active_pos_closed": _active_pos_closed,
            "retrace": _retrace,
            "progress": _progress,
            "leg_bars": int(_leg_bars),
            "prev_leg_bars": int(_prev_leg_bars),
            "leg_time_ratio": float(_leg_time_ratio),
            "efficiency": _efficiency,
            "active_low": _active_low,
            "active_high": _active_high,
            "anchor_price": _anchor_price,
            "parent_ready": bool(_parent_ready),
            "parent_pos": float(_parent_pos),
            "parent_pos_closed": float(_parent_pos_closed),
            "parent_low": float(_parent_low),
            "parent_high": float(_parent_high),
            "parent_wave_id": str(_parent_wave_id),
        })

        return _out

    except Exception:
        return _out


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 🏛️ GAJAMI STRUCTURE CONSTITUTION — 헌법 / 상태법 / 개별법 / 집행법 / 매도법
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#
# 이 주석은 가자미근 판단 구조의 최상위 설계 원칙이다.
# 앞으로 조건 추가·예외 추가·우회 패치를 하기 전에 반드시 이 구조를 먼저 따른다.
#
# 목적:
#   1. 10개월간 반복된 패치 루프 차단
#   2. 같은 가자미근을 여러 로직이 재판정하는 구조 제거
#   3. 헌법과 개별법을 분리
#   4. 정답봉은 살리고 오답봉은 차단
#   5. 계산 → 로그 → 판단 정합성 유지
#
# 핵심 원칙:
#   - 헌법은 최상위 원칙이다.
#   - 개별법은 헌법 아래에서만 작동한다.
#   - 개별법은 헌법을 우회하거나 뒤집을 수 없다.
#   - Strategy는 Analysis의 가자미근 판정을 재판정하지 않는다.
#   - 최종 PASS/BLOCK은 단일 Decision 객체에서만 결정한다.
#
# ─────────────────────────────────────────────────────────────────────────────────────────
# 0. 용어 정의
# ─────────────────────────────────────────────────────────────────────────────────────────
#   헌법:    가자미근 최상위 기본 자격. False이면 어떤 rescue/override도 PASS 불가.
#   상태법:  생애주기 판단. NO_GC / BIRTH / VALID / LATE / DEAD 중 하나.
#   개별법:  헌법 통과 후 보조·품질·위험·체제·박스·TLC·거래량·ATR 규칙.
#            단독으로 헌법 통과 신호를 죽이면 안 된다.
#   집행법:  Strategy 단계 주문 전 최종 집행. Analysis GajamiDecision 소비만.
#   매도법:  진입 이후 손절·익절·MA15 Exit·재진입 제한. 진입 자격 판단에 역개입 금지.
#
# ─────────────────────────────────────────────────────────────────────────────────────────
# 1. 전체 판단 순서 — 절대 순서
# ─────────────────────────────────────────────────────────────────────────────────────────
#   Data Snapshot → Constitution(헌법) → Fish Shape → Life Cycle →
#   Individual Laws(개별법) → Final GajamiDecision → Strategy Execution → Exit Rules
#
#   이 순서를 거꾸로 타면 안 된다.
#   예: BOX, TOP_RISE, TLC, volume이 헌법보다 먼저 hard block하면 안 된다.
#
# ─────────────────────────────────────────────────────────────────────────────────────────
# 2. 가자미근 헌법 — 최상위 5조건 (calculate_gajami_constitution 참조)
# ─────────────────────────────────────────────────────────────────────────────────────────
#   1) MA5 > MA10          : 골든크로스 필수. 데드크로스 절대 불가.
#   2) MA5 slope > 0       : MA5 고개듦 필수.
#   3) MA10↑ OR MA20↑      : 기본. 단 fresh BIRTH(dur≤2)+확정 GC+MA5 생존이면
#                            중기선 후행만으로 MID_DEAD 처리하지 않는다.
#   4) MA60↑ OR MA120↑      : 장기선은 별도 판정. fresh BIRTH에서 MA120 단독
#                            strong-down은 hard veto하지 않지만, TD/RG MA60 strong-down
#                            (≤-0.30%)은 현행 LONG_STRONG_DOWN을 유지한다.
#   5) NOT(TD/RG + MA20 slope < MA20_SLOPE_DECEL_LIMIT) : TD/RG에서 MA20 변곡 이하이면 차단.
#
#   중요: MA5 < MA20은 헌법 위반이 아님. GC 초입 구조적 정상. 단독 hard block 금지.
#
# ─────────────────────────────────────────────────────────────────────────────────────────
# 3. 상태법 — 생애주기
# ─────────────────────────────────────────────────────────────────────────────────────────
#   BIRTH: dur≤2 골크 직후. 핵심 진입 구간.
#   VALID: 살아 있는 가자미근. 추격 아닌 생존 구간.
#   LATE:  TOP_RISE·과열·오래된 GC → 여기로 귀속.
#   DEAD:  구조 붕괴. MA10_SLOPE_DEAD, BREAK_STRUCTURE 등.
#   상태법에서 하면 안 되는 것: 체제 재판정, 박스 단독 차단, 헌법 우회 PASS.
#
# ─────────────────────────────────────────────────────────────────────────────────────────
# 4. 개별법 3등급
# ─────────────────────────────────────────────────────────────────────────────────────────
#   A. 공통 Hard Veto : FAKE_GC_SUBTICK / 확정 GC 부재·가짜 GC origin
#      J1 상태법 Hard Veto : STATE_DEAD / 동일 DEAD_WAVE 재진입
#      J2/J3/J4 독립 PASS는 J1 상태법으로 전역 차단하지 않는다.
#   B. Soft Penalty (단독 차단 금지): MA5<MA20 / box_pos=TOP / TOP_RISE / TLC미완성 /
#                                    volume부족 / ATR낮음 / price deviation
#   C. Observe Only: box_pos원시값 / golden_count / rally_duration / GC stage score
# 
# ─────────────────────────────────────────────────────────────────────────────────────────
# 5. 금지 패턴
# ─────────────────────────────────────────────────────────────────────────────────────────
#   ❌ soleus = constitution_ok OR newborn_strong_impulse_ok  (헌법 False → rescue PASS 금지)
#   ❌ box_pos >= 0.95 → 단독 hard block  (LATE/DEAD 결합 시만 Hard Veto 가능)
#   ❌ Strategy에서 MA5/MA10/box/TLC로 가자미근 재판정
#   ❌ 한 봉에서 여러 final reason 동시 출력 (first_block 하나만)
#
# ─────────────────────────────────────────────────────────────────────────────────────────
# 6. 최종 판정 목표 구조
# ─────────────────────────────────────────────────────────────────────────────────────────
#   final_ok = (
#       constitution_ok
#       and shape_ok
#       and lifecycle_state in ("BIRTH", "VALID")
#       and not hard_veto
#   )
#   현재는 legacy constitution 권한 유지 중이며, V2는 계산 flag만 downstream에서 소비한다.
#
# ─────────────────────────────────────────────────────────────────────────────────────────
# 7. Phase별 정리 순서
# ─────────────────────────────────────────────────────────────────────────────────────────
#   Phase 1: V2 헌법 계산 flag 정리 ✅ 완료 — shadow 장식 제거, 소비 flag 유지
#   Phase 2: 헌법 단일화 🔄 진행 중 — 2-A/2-B/2-C 구조분해 완료, legacy 권한 유지 중
#   Phase 3: 상태법 단일화 — TOP_RISE_BLOCK/BIRTH_HEIGHT 등 → lifecycle_state로 흡수
#   Phase 4: 개별법 격하 — BOX/TOP_RISE/TLC/DEV → quality/risk/soft_penalty
#   Phase 4-1: slope hardcut(CORE FAIL/MA5_NOT_LEADING/리드부족) soft 격하 완료. 잔존: BOX/TOP_RISE/TLC
#   Phase 5: Strategy 재판정 제거 — GajamiDecision 소비만
#   Phase 6: 회귀검증 — 정답봉PASS·오답봉BLOCK diff 기반 검증
#
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 🏛️ 가자미근 헌법 SSoT — 순수 5조건만 (생애주기·박스·ATR·TLC·volume 판단 절대 금지)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
def calculate_gajami_constitution(
    ma5: float,
    ma10: float,
    ma5_slope: float,
    ma10_slope: float,
    ma20_slope: float,
    ma60_slope: float,
    ma120_slope: float,
    regime,
    dur_5x10_bars=None,
    ma5_tick_up: float = None,
    gc_5x10_confirmed: bool = None,
) -> tuple:
    """
    가자미근 헌법 판정 (SSoT).
    반환: (ok: bool, fail: str)  — fail은 첫 번째 위반 조건명.
    헌법 False → 개별법 심사 없이 무조건 차단.
    recent_cross_ok / gc_5_10 / intrabar 등 생애주기 조건은 상태법에서 판단.
    NEWBORN(dur≤2): 확정 GC + MA5 생존이 확인되면 MA10/MA20 후행만으로
    MID_DEAD 처리하지 않는다. 장기선은 _long_ma_alive_verdict()가 별도 판정한다.
    """
    try:
        _dur = int(dur_5x10_bars) if dur_5x10_bars is not None else 999
    except Exception:
        _dur = 999

    # 🔒 [MA5-TICK-UP-0816] P-0(현재 slope 생존)과 D-1(확정봉 tick 상승)을 AND로 결합.
    #  ma5_tick_up 미전달 시 D-1 검증 불가 상태이므로 P-0 단독 판정으로 폴백.
    _ma5_alive_ratio_ok = SoleusGajamiConstants.ma5_alive_ratio(ma5_slope)
    if ma5_tick_up is not None:
        # 🔒 [MA5-TICK-LATTICE-EPS-0830]
        # 경제적 격자 문턱 0.20은 유지하고 float 경계오차만 허용한다.
        _ma5_tick_up_ok = bool(
            ma5_tick_up
            >= (
                SoleusGajamiConstants.MA5_TICK_UP_MIN
                - SoleusGajamiConstants.MA5_TICK_UP_EPS
            )
        )
    else:
        _ma5_tick_up_ok = True
    _ma5_alive = bool(_ma5_alive_ratio_ok and _ma5_tick_up_ok)
    # 🔒 [GC-CLOSED-STRICT-SSOT-0822-R2] canonical GC는 strict MA5>MA10 유지.
    # 호출부가 확정봉 자격을 전달하면 현재봉 관계로 GC를 재판정하지 않는다.
    _gc_5x10_ok = (
        bool(ma5 > ma10)
        if gc_5x10_confirmed is None
        else bool(gc_5x10_confirmed)
    )
    _newborn_mid_exempt = bool(
        _dur <= SoleusGajamiConstants.NEWBORN_MAX_DUR
        and _gc_5x10_ok
        and _ma5_alive
        and ma10_slope >= SoleusGajamiConstants.MA10_SLOPE_NEWBORN_MIN
        and ma20_slope >= SoleusGajamiConstants.MA20_SLOPE_DECEL_LIMIT
    )
    _newborn_mid_exempt = False

    if not _gc_5x10_ok:
        return False, "GC_FAIL"
    if not _ma5_alive:
        if not _ma5_tick_up_ok:
            return False, f"MA5_NOT_UP(tick={ma5_tick_up:+.2f}<{SoleusGajamiConstants.MA5_TICK_UP_MIN:.2f})"
        return False, "MA5_NOT_UP"
    _long_backed_mid_pullback_const = bool(
        _dur <= SoleusGajamiConstants.NEWBORN_SHAPE_MAX_DUR
        and _gc_5x10_ok
        and ma5 > 0.0 and ma10 > 0.0
        and _ma5_alive
        and ma60_slope > 0.0
        and ma120_slope > 0.0
        and ma20_slope >= SoleusGajamiConstants.MA20_SLOPE_DECEL_LIMIT
    )

    # 신선 GC 탄생봉에서 중기선 후행만으로 MID_DEAD 처리하지 않는다.
    # 🔒 [FRESH-BIRTH-MID-DEAD-EXEMPT-0829] ★380-R2
    # fresh BIRTH의 MID_DEAD는 중기선(MA10/MA20) 후행만으로 확정하지 않는다.
    # 5-1 R2 이후 LOW_OPEN dedup 30건 중 MID_DEAD가 7건(23.3%)의
    # first blocker로 확인됐다. 이번 라운드는 이 항만 개방한다.
    #
    # 주의: 이 함수는 LOW_OPEN/J1을 직접 받지 않는 shared 헌법 SSoT다.
    # 따라서 J1 전용 우회가 아니라 fresh BIRTH 공용 세칙의 정합화다.
    # 장기선 위험은 아래 _long_ma_alive_verdict()가 별도로 판정하며,
    # TD/RG MA60 strong-down(<= -0.30%)은 08-26 계약대로 그대로 유지한다.
    _fresh_birth_trend_alive = bool(
        _dur <= SoleusGajamiConstants.NEWBORN_MAX_DUR
        and _gc_5x10_ok
        and _ma5_alive
    )
    # 🔬 [MID-EXEMPT-MEASURE] 관측전용 — 실제 MID 면제 소비자를 모두 기록한다.
    # fb=1이 이번 ★380-R2의 신규 발화다.
    if (
        ma10_slope <= 0.0
        and ma20_slope < 0.0
        and (
            _newborn_mid_exempt
            or _long_backed_mid_pullback_const
            or _fresh_birth_trend_alive
        )
    ):
        logger.info(
            f"[MID-EXEMPT-MEASURE] "
            f"nb={int(_newborn_mid_exempt)} "
            f"lb={int(_long_backed_mid_pullback_const)} "
            f"fb={int(_fresh_birth_trend_alive)} "
            f"dur={_dur} "
            f"ma10s={ma10_slope:+.5f} "
            f"ma20s={ma20_slope:+.5f} "
            f"ma60s={ma60_slope:+.5f}"
        )

    if not (
        ma10_slope > 0.0
        or ma20_slope >= 0.0
        or _newborn_mid_exempt
        or _long_backed_mid_pullback_const
        or _fresh_birth_trend_alive
    ):
        return False, "MID_DEAD"
    _long_ok, _long_fail = _long_ma_alive_verdict(
        ma60_slope,
        ma120_slope,
        regime,
        ma5_slope,
        dur_5x10_bars,
    )
    if not _long_ok:
        return False, _long_fail
    if (
        regime in (MarketRegime.TRENDING_DOWN, MarketRegime.RANGING)
        and ma20_slope < SoleusGajamiConstants.MA20_SLOPE_DECEL_LIMIT
    ):
        return False, "TD_RG_MA20_DOWN"
    return True, "PASS"


def calculate_coil_setup(
    squeeze_ready: bool,
    squeeze_on: bool,
    squeeze_ratio: float,
    ma5_slope_ratio: float,
) -> tuple:
    """확정봉 KC Squeeze + MA5 방향가드 SSoT."""
    try:
        _ratio = float(squeeze_ratio)
        _s5 = float(ma5_slope_ratio)
        _ready = bool(
            squeeze_ready
            and math.isfinite(_ratio)
            and math.isfinite(_s5)
            and _ratio > 0.0
            and _s5 != SENTINEL_FLOAT
        )
        if not _ready:
            return False, False, SENTINEL_FLOAT

        _ok = bool(squeeze_on and _s5 >= 0.0)
        return True, _ok, _ratio
    except Exception:
        return False, False, SENTINEL_FLOAT
    
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 🎯 가자미근 기본 공식 SSoT 함수
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
def is_gajami_shape(
    ma5: float,
    ma10: float,
    ma20: float,
    ma5_slope: float,
    ma10_slope: float,
    ma20_slope: float,
    ma60_slope: float,
    gap_5_10_pct: float,
    ma120_slope: float = 0.0,
    ma5_prev_prev: float = None,
    ma5_prev: float = None,
    ma10_prev: float = None,
    ma5_dir_min: float = 0.0030,
    current_price: float = None,
    regime = None,
    dur_5x10_bars = None,
    newborn_max_bars: int = 5,      # 🆕 RECENT_GC_MAX_BARS 동기화
    ma5_series = None,
    ma10_series = None,
    is_birth_bottom: bool = False,
    intrabar_gc: bool = False,
    df: "pd.DataFrame" = None,
    symbol: str = "",
    birth_traintrack_ok: bool = False,
    subtick_body_birth: bool = False,
    long_backed_mid_pullback_ok: bool = False,
    newborn_ma5_turn_up: bool = False,
    gc_5x10_confirmed: bool = None,
) -> dict:
    """
    GAJAMI CORE DEFINITION — 헌법/형상 판정

    [가자미근 헌법 — 최상위 5조건]  ← calculate_gajami_constitution() SSoT
    ① MA5 > MA10
    ② MA5 slope > 0
    ③ MA10 slope > 0 OR MA20 slope > 0
       단 fresh BIRTH(dur≤2) + 확정 GC + MA5 생존이면
       중기선 후행만으로 MID_DEAD 처리하지 않는다.
    ④ MA60 slope > 0 OR MA120 slope > 0
       fresh BIRTH에서 MA120 단독 strong-down은 hard veto하지 않지만,
       TD/RG MA60 strong-down(≤ -0.30%)은 LONG_STRONG_DOWN으로 유지한다.
    ⑤ NOT(TD/RG AND MA20 slope < MA20_SLOPE_DECEL_LIMIT)

    [주의]
    - MA5<MA20은 GC 초입에서 자연스러운 상태이므로 단독 차단 금지.
    - recent_cross_ok / dur / intrabar는 헌법이 아니라 상태법.
    - box / ATR / TLC / volume은 헌법이 아니라 개별법.
    - 헌법 False → 어떤 rescue/override도 PASS 불가.

    Args:
        ma5, ma10, ma20: 이동평균값
        ma5_slope, ma10_slope, ma20_slope, ma60_slope, ma120_slope: 기울기
        gap_5_10_pct: MA5-MA10 간격 (%)

    Returns:
        dict: {shape_ok, ma20_ok, aligned_ok, gajami_ok, reason}
    """
    result = {
        'shape_ok': False,
        'ma20_ok': False,
        'aligned_ok': False,
        'gajami_ok': False,
        'reason': ''
    }
    
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    # 가자미근 판정 구조 (3단계) Start
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    # 철학: 형상 > 체제
    #   - 가자미근 = 명확한 상승 초입 형상
    #   - 우회 경로 없음 (단일 AND 구조)
    #
    # 구조:
    #   1️⃣ PRE-FILTER     (명백한 역배열 차단)
    #   2️⃣ CORE-GATE      (형상 slope 필수)
    #   3️⃣ QUALITY-FILTER (보조 품질 필터)
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━    
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    # 0️⃣ 가자미근 헌법 0조 (최우선 차단 — 모든 체크 위에)
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    # 기본이 안 되면 무조건 차단.
    # MA5<MA20은 단독 차단 사유 아님 — GC 초입 구조적 정상.
    # 차단 기준: DC / MA5 꺾임 / MA10·MA20 둘 다 비상향 / MA60·MA120 둘 다 비상향 또는 TD/RG 강하향(≤-0.30%) / TD·RG+MA20하향
    # 🏛️ 헌법 0조 — calculate_gajami_constitution() SSoT 호출 (중복 계산 제거)
    # 🔒 [MA5-TICK-UP-0816] 확정봉 1봉 변화를 틱으로 재계산 (SSoT 동일 기준).
    # 🔒 [MA5-TICK-UP-SSOT-0817] 확정봉 MA5 틱 변화는 단일 헬퍼만 사용.
    _ma5_tick_up_shape = SoleusGajamiConstants.ma5_tick_up(ma5_series)
    _gc_5x10_const_ok = (
        bool(ma5 > ma10)
        if gc_5x10_confirmed is None
        else bool(gc_5x10_confirmed)
    )
    _ctn_shape_ok, _ctn_shape_fail = calculate_gajami_constitution(
        ma5=float(ma5 or 0.0),
        ma10=float(ma10 or 0.0),
        ma5_slope=float(ma5_slope or 0.0),
        ma10_slope=float(ma10_slope or 0.0),
        ma20_slope=float(ma20_slope or 0.0),
        ma60_slope=float(ma60_slope or 0.0),
        ma120_slope=float(ma120_slope or 0.0),
        regime=regime,
        dur_5x10_bars=dur_5x10_bars,
        ma5_tick_up=_ma5_tick_up_shape,
        gc_5x10_confirmed=_gc_5x10_const_ok,
    )
    gajami_basic_constitution = bool(_ctn_shape_ok)
    if not gajami_basic_constitution:
        try:
            _ctn_log_dur = int(dur_5x10_bars) if dur_5x10_bars is not None else 999
        except Exception:
            _ctn_log_dur = 999
        # 🔒 [FRESH-BIRTH-MID-DEAD-EXEMPT-0829-R2-LOG]
        # 헌법 본체의 _ma5_alive와 동일하게 slope + 확정봉 tick을 함께 반영한다.
        _ctn_log_ma5_alive = bool(
            SoleusGajamiConstants.ma5_alive_ratio(ma5_slope)
            and (
                _ma5_tick_up_shape is None
                # 🔒 [MA5-TICK-LATTICE-EPS-0830]
                # 0.20 격자점의 부동소수점 경계오차만 흡수한다.
                or _ma5_tick_up_shape >= (
                    SoleusGajamiConstants.MA5_TICK_UP_MIN
                    - SoleusGajamiConstants.MA5_TICK_UP_EPS
                )
            )
        )
        _ctn_log_fresh_mid_exempt = bool(
            _ctn_log_dur <= SoleusGajamiConstants.NEWBORN_MAX_DUR
            and _gc_5x10_const_ok
            and _ctn_log_ma5_alive
        )
        _gc = 'GC' if _gc_5x10_const_ok else 'DC'
        _s5 = 'ALIVE' if _ctn_log_ma5_alive else 'DEAD'
        _mid = 'OK' if (
            ma10_slope > 0.0
            or ma20_slope >= 0.0
            or _ctn_log_fresh_mid_exempt
        ) else '↓↓'
        _ctn_log_long_ok, _ctn_log_long_fail = _long_ma_alive_verdict(
            ma60_slope,
            ma120_slope,
            regime,
            ma5_slope,
            _ctn_log_dur,
        )
        _long = (
            '↓↓(STEEP)'
            if _ctn_log_long_fail == "LONG_STRONG_DOWN"
            else ('OK' if _ctn_log_long_ok else '↓↓')
        )
        # R2에서 fresh BIRTH MID 면제가 살아 있으므로
        # mid 로그도 실제 헌법 판정과 동일 계약을 표시한다.
        _td   = '차단' if (
            ma20_slope < SoleusGajamiConstants.MA20_SLOPE_DECEL_LIMIT
            and regime in (MarketRegime.TRENDING_DOWN, MarketRegime.RANGING)
        ) else 'OK'
        result['reason'] = (
            f"헌법0조({_ctn_shape_fail}; {_gc},s5={_s5},"
            f"mid={_mid},long={_long},td_ma20={_td})"
        )
        return result

    # 🔒 MA5 생존 단일 floor — dur<=3 형상 판정 공용
    # 0.0020 공용 momentum 기준과 분리하여 탄생 형상만 0.0015 적용
    _s5_floor = SoleusGajamiConstants.MA5_SLOPE_BIRTH_FLOOR_RATIO

    # 상태법의 TF별 GC 생애와 Shape의 최근성 기준을 일치시킨다.
    try:
        _dur_shape = int(dur_5x10_bars) if dur_5x10_bars is not None else None
    except (TypeError, ValueError):
        _dur_shape = None

    try:
        _shape_age_limit = max(3, int(newborn_max_bars))
    except (TypeError, ValueError):
        _shape_age_limit = 3

    if _dur_shape is not None and _dur_shape > _shape_age_limit:
        result['reason'] = (
            f"STALE_GC(dur={_dur_shape}>{_shape_age_limit})"
        )
        return result

    # MA5_NOT_LEADING 보조 체크 (헌법 0조 통과 후 slope 정밀도)
    # 🔒 [GAP-BAND-SSOT-0807] 아래 L1001에서 _is_gc_newborn이 재정의되므로
    #    이름 충돌 방지를 위해 이 지점 전용 이름으로 분리 (행위 무변경)
    _lead_exempt_newborn = bool(_dur_shape is not None and _dur_shape <= SoleusGajamiConstants.NEWBORN_SHAPE_MAX_DUR)
    _lead_exempt_newborn = False
    _is_body_birth = bool(subtick_body_birth)
    _fail_slope_min = ma5_slope < SoleusGajamiConstants.MA5_SLOPE_CORE_MIN

    # 🆕 [LEAD-DUAL-STRONG] MA5가 승자수준(≥0.0017)이면
    # MA10이 더 빨라도 lead-fail 아님. 약한 MA5는 _fail_slope_min이 그대로 차단.
    _fail_lead = not (
        ma10_slope is None
        or (ma5_slope - ma10_slope) > 0.0001
        or ma5_slope >= SoleusGajamiConstants.MA5_SLOPE_STRONG_LEAD_RATIO
    )

    # 🔍 [LEAD-DUAL-STRONG] 예외 발동 측정 — 관측전용, 행위무변경
    if (
        ma10_slope is not None
        and (ma5_slope - ma10_slope) <= 0.0001
        and ma5_slope >= SoleusGajamiConstants.MA5_SLOPE_STRONG_LEAD_RATIO
    ):
        logger.info(
            f"💪 [LEAD-DUAL-STRONG] {symbol} 오탐정정 살림 | "
            f"s5={ma5_slope:+.5f} s10={ma10_slope:+.5f} "
            f"diff={ma5_slope-ma10_slope:+.5f} "
            f"(구코드면 MA5_NOT_LEADING 차단)"
        )

    if (_fail_slope_min or _fail_lead):
        if not (_lead_exempt_newborn or _is_body_birth):
            if _fail_lead:
                result['reason'] = f"MA5_NOT_LEADING(s5={ma5_slope:.5f} s10={(ma10_slope or 0):.5f})"
            else:
                result['reason'] = f"MA5_SLOPE_LOW(s5={ma5_slope:.5f}<{SoleusGajamiConstants.MA5_SLOPE_CORE_MIN:.5f})"
            return result
    
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    # 2️⃣ CORE GATE (형상 slope 필수 - 우회 불가)
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    # 형상 필수 조건 (AND):
    #   ① MA5 slope >= 0.00047 (머리 듦)
    #   ② MA10 slope >= 0.00005 (추세 동참, 명시화)
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    
    MA5_SLOPE_HARDCUT_MIN = ma5_dir_min   # 가격·틱 비례 동적 기준 (Tier별 계산값)
    MA10_SLOPE_MIN = 0.00005
    _SLOPE_EPS = 1e-8   # 부동소수점 경계 안정화

    # dur≤2 탄생형은 2봉 블렌드 slope와 현재봉 1봉 turn을 역할 분리한다.
    # 수치 floor는 유지하고, 판정 시점만 현재봉으로 보강한다.
    _ma5_lift_ok = bool(
        ma5_slope >= _s5_floor
        or newborn_ma5_turn_up
    )
    _newborn_ma5_lift_ok = _ma5_lift_ok

    # 🆕 NEWBORN 예외: GC 탄생 직후(dur≤2) MA10은 구조적으로 음수일 수 있음
    # 🔒 하락추세 차단: MA20 slope >= 0.0 필수 (중기선 하방이면 예외 없음)
    _ma10_min = MA10_SLOPE_MIN
    if (
        _dur_shape is not None
        and _dur_shape <= SoleusGajamiConstants.NEWBORN_SHAPE_MAX_DUR
        and ma5_slope >= MA5_SLOPE_HARDCUT_MIN
        and _newborn_ma5_lift_ok
        and (ma20_slope >= 0.0 or is_birth_bottom)
    ):
        _ma10_min = SoleusGajamiConstants.MA10_SLOPE_NEWBORN_MIN

    _is_gc_newborn = (
        _dur_shape is not None
        and _dur_shape <= SoleusGajamiConstants.NEWBORN_SHAPE_MAX_DUR
        and ma10_slope >= -0.003
        and _newborn_ma5_lift_ok
    )

    _is_convergence_birth_core = bool(
        _dur_shape is not None
        and _dur_shape <= SoleusGajamiConstants.NEWBORN_MAX_DUR
        and float(ma5 or 0.0) > float(ma10 or 0.0) > 0.0
        and float(ma10 or 0.0) < float(ma20 or 0.0)
        and _newborn_ma5_lift_ok
        and ma10_slope >= SoleusGajamiConstants.MA10_SLOPE_NEWBORN_MIN
        and ma20_slope >= SoleusGajamiConstants.MA20_SLOPE_DECEL_LIMIT
        and _long_ma_alive_verdict(
            ma60_slope,
            ma120_slope,
            regime,
            ma5_slope,
            _dur_shape,
        )[0]
    )

    result['convergence_birth'] = int(_is_convergence_birth_core)

    if _is_convergence_birth_core:
        _ma10_min = min(_ma10_min, SoleusGajamiConstants.MA10_SLOPE_NEWBORN_MIN)
        try:
            _cvg_gap = ((float(ma10) - float(ma20)) / float(ma20) * 100.0) if float(ma20 or 0.0) > 0 else 0.0
            logger.info(
                f"✅ [CONVERGENCE-BIRTH-CORE] {symbol} "
                f"수렴형 GC 탄생 CORE 인정 | "
                f"dur={dur_5x10_bars} "
                f"s5={ma5_slope:+.5f} s10={ma10_slope:+.5f} "
                f"s20={ma20_slope:+.5f} "
                f"s60={ma60_slope:+.5f} s120={ma120_slope:+.5f} "
                f"gap10_20={_cvg_gap:+.4f}% px={current_price}"
            )
        except Exception:
            pass

    _is_gc_newborn = bool(
        _is_gc_newborn
        or _is_convergence_birth_core
    )
    _is_td_rg_regime = regime in (MarketRegime.TRENDING_DOWN, MarketRegime.RANGING)

    # 🔒 [LIFT-VETO-ONLY-0810] MA5_LIFT_LOW 하드 veto만 CORE_MIN으로 정정
    # _ma5_lift_ok(BIRTH_FLOOR_RATIO)는 newborn 완화 자격선(L1001/1010/1018)으로 유지
    if (
        _dur_shape is not None
        and float(ma5 or 0.0) > float(ma10 or 0.0) > 0.0
        and ma5_slope > 0.0
        and not (ma5_slope >= SoleusGajamiConstants.MA5_SLOPE_CORE_MIN or newborn_ma5_turn_up)
    ):
        result['reason'] = (
            f"MA5_LIFT_LOW(s5={ma5_slope:.5f}<{SoleusGajamiConstants.MA5_SLOPE_CORE_MIN:.5f},"
            f"dur={_dur_shape})"
        )
        return result

    if not (
        (ma5_slope >= MA5_SLOPE_HARDCUT_MIN - _SLOPE_EPS or (_is_gc_newborn and (ma5_slope >= MA5_SLOPE_HARDCUT_MIN - _SLOPE_EPS or ma10_slope >= _ma10_min - _SLOPE_EPS)))
        and (ma10_slope >= _ma10_min - _SLOPE_EPS or _is_gc_newborn)
    ):
        result['reason'] = (
            f"CORE FAIL | "
            f"s5={ma5_slope:.5f} (>={MA5_SLOPE_HARDCUT_MIN:.5f}?), "
            f"s10={ma10_slope:.5f} (>={_ma10_min:.5f}?)"
        )
        return result
    
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    # 3️⃣ QUALITY FILTER (형상 품질 보강)
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    
    # MA20 slope 하락 지속 차단
    MA20_SLOPE_HARDCUT_MIN = -0.0013  # -0.05% → -0.15% → -0.13%: 탄생 초입 MA20 하락 관성 허용
    # 근거: MA20(1시간선)은 반등 탄생 초입에서 -0.10%대 음수가 구조상 정상
    #       -0.15%는 SoleusGajamiConstants.MA20_SLOPE_DECEL_LIMIT(-0.0015)와 정합
    #       데드캣(-0.15%~-0.20%)은 이후 HCG ①②③에서 2차 차단
    _is_td_birth = (
        dur_5x10_bars is not None
        and int(dur_5x10_bars) <= 1
        and ma5_slope > 0
    )
    if ma20_slope < MA20_SLOPE_HARDCUT_MIN and not _is_td_birth:
        result['reason'] = f"QUALITY: MA20 하락 지속 (s20={ma20_slope:.5f}<{MA20_SLOPE_HARDCUT_MIN:.5f})"
        return result
    
    # 이전봉 역배열 체크 (골든크로스 탄생 예외)
    if ma5_prev is not None and ma10_prev is not None:
        is_golden_cross_birth = (ma5 >= ma10) and (ma5_prev <= ma10_prev)
        
        if ma5_prev < ma10_prev and not is_golden_cross_birth and not intrabar_gc \
                and not (dur_5x10_bars is not None and int(dur_5x10_bars) <= SoleusGajamiConstants.NEWBORN_MAX_DUR):
            result['reason'] = f"QUALITY: 이전봉 역배열 ({ma5_prev:.2f}<{ma10_prev:.2f})"
            return result
    
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    # 가자미근 판정 구조 (3단계) End
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━    

    
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    # 🔒 HARD CORE GUARD — GAJAMI IS SHAPE, NOT NOISE
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    # 🔒 조건 삭제/건너뛰기 금지 — 임계값 동적 조정만 허용
    #
    # 가자미근 초입(발목) 튜닝:
    #   단기 정배열(GC) + MA5 고개 듦 → 초입 인정
    #   발동 시 ①-1/③/④ 문턱을 함께 동적 완화 (분기 추가 없이 최소 변경)
    #   ①-1 완화: MA5 slope 자체가 충분하면 1봉 Δma5 면제
    #   ③   완화: 초입에서 MA10 하락 관성(-0.03%까지) 허용
    #   ④   완화: 급락 후 반등 초입에서 MA20 하방(-0.26%까지) 허용
    #   근거: 수렴 구간에서 Δma5≈0, ma10_slope<0, ma20_slope<-0.15%은 물리적으로 정상
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    # 🔧 Patch 8: ATR 제거 — 가자미근은 형상, TIER 직접 적용
    _eff_ma5_dir_min = ma5_dir_min
    _eff_core_min = SoleusGajamiConstants.MA5_SLOPE_CORE_MIN
    # _eff_core_min: 현재 동적 완화 없음 (BIRTH/RANGING 예외 검토 후 재적용 예정)
    _eff_ma10_slope_min = SoleusGajamiConstants.MA10_SLOPE_POS           # 0.00005
    _eff_ma20_decel_limit = SoleusGajamiConstants.MA20_SLOPE_DECEL_LIMIT # -0.00150
    _eff_deviation_max = 0.020 if is_birth_bottom else 0.010

    _is_early_gc = (
        dur_5x10_bars is not None
        and int(dur_5x10_bars) <= SoleusGajamiConstants.NEWBORN_SHAPE_MAX_DUR
        and ma5_slope >= 0.0
        and ma5 >= ma10
    )
    if _is_early_gc:
        _eff_deviation_max = (
            0.050 if regime == MarketRegime.TRENDING_DOWN else   # TD탄생봉: 5% (추세 급등 이격)
            0.030 if regime == MarketRegime.TRENDING_UP else     # TU: 기존 유지
            0.020                                                 # RANGING: 기존 유지
        )

    # 🆕 탄생봉(dur≤2) MA10 평탄 허용
    # 근거: GC 직후 MA10 극미세 음수는 구조상 정상. CORE GATE에 dur≤2 예외가 있으나 HCG가 재차단.
    # 탄생구간(dur≤2)은 slope 방향이 아니라 구조로 판단한다
    if dur_5x10_bars is not None and int(dur_5x10_bars) <= SoleusGajamiConstants.NEWBORN_SHAPE_MAX_DUR:
        _eff_ma10_slope_min = SoleusGajamiConstants.MA10_SLOPE_NEWBORN_MIN

    # 🆕 중기 추세 필터 (선형회귀 기울기 기반, 퍼센트화 + dead-zone)
    # 철학: 점 비교는 스파이크 착시 발생. 회귀기울기로 진짜 방향 판단
    # SSoT: mas Series 직접 사용 / numpy는 파일 상단 import 사용
    # 체제 의존 없음: 구조 판단만으로 결정
    try:
        if ma5_series is not None and ma10_series is not None:
            if len(ma5_series) >= 6 and len(ma10_series) >= 6:
                _x        = np.arange(5, dtype=float)
                _s5_vals  = np.array([float(v) for v in ma5_series.iloc[-6:-1]])
                _s10_vals = np.array([float(v) for v in ma10_series.iloc[-6:-1]])
                s5_slope  = np.polyfit(_x, _s5_vals,  1)[0]
                s10_slope = np.polyfit(_x, _s10_vals, 1)[0]
                s5_slope_pct  = s5_slope  / _s5_vals[-1]  if _s5_vals[-1]  != 0 else 0.0
                s10_slope_pct = s10_slope / _s10_vals[-1] if _s10_vals[-1] != 0 else 0.0
                _dur = int(dur_5x10_bars or 999)
                _mtb_thr = -0.001 if _dur <= newborn_max_bars else -0.0001
                if s5_slope_pct < _mtb_thr and s10_slope_pct < _mtb_thr:
                    result['reason'] = (
                        f"MID-TREND-BEAR-BLOCK("
                        f"s5_reg={s5_slope_pct:.5f}, s10_reg={s10_slope_pct:.5f})"
                    )
                    return result
    except Exception:
        pass

    # 🔒 [NEWBORN-MID-MA60-ABSORB-0817] TD/RG newborn의 MA60-backed 중기 생존을 본선에 흡수.
    _newborn_mid_alive = bool(
        ma10_slope > 0.0
        or ma20_slope > 0.0
        or (
            _is_gc_newborn
            and ma5 >= ma10
            and ma5_slope > 0.0
            and ma10_slope >= _eff_ma10_slope_min
            and ma20_slope >= _eff_ma20_decel_limit
        )
        or (
            _is_td_rg_regime
            and _is_gc_newborn
            and ma5 >= ma10
            and ma5_slope > 0.0
            and ma60_slope > 0.0
            and ma20_slope >= _eff_ma20_decel_limit
        )
    )
    # 🔒 [SSoT 일원화] 장기선 생존 = _long_ma_alive_verdict 단일함수 (2026-07-11)
    _newborn_long_ok, _ = _long_ma_alive_verdict(
        ma60_slope,
        ma120_slope,
        regime,
        ma5_slope,
        _dur_shape,
    )
    _newborn_long_alive = bool(_newborn_long_ok)
    _newborn_ma5_10_ok = bool(
        _is_gc_newborn
        and (ma5 >= ma10 or birth_traintrack_ok)
        and ma5_slope > 0.0
        and _newborn_mid_alive
        and _newborn_long_alive
    )

    if not (
        # ① MA5가 '확실히' 고개를 든다 (ATR 연동: 저변동 시 완화)
        # 탄생봉(dur≤3): GC 직후 slope 음수(-0.1%까지) 허용 → _is_gc_newborn 면제
        (ma5_slope >= _eff_core_min
         or (_is_gc_newborn and ma5_slope > 0.0)
         or (ma5 > ma10 and (ma10 >= ma20 or ma5 >= ma20) and ma10_slope >= 0.0))
        # ①-1 MA5 연속 상승 확인 (1봉 점프 노이즈 차단)
        # 탄생봉(dur≤2): MA5 평탄은 구조적 정상 → ①-1 전체 면제
        and (
            _is_gc_newborn
            or (ma5 > ma10 and (ma10 >= ma20 or ma5 >= ma20) and ma5_slope > 0 and ma10_slope >= 0.0)  # 🆕 TREND CONTINUING
            or (
                ma5_prev_prev is not None and ma5_prev_prev > 0
                and ma5_prev is not None and ma5_prev > 0
                and (
                    (ma5 - min(ma5_prev_prev, ma5_prev)) / min(ma5_prev_prev, ma5_prev) >= _eff_ma5_dir_min
                    or (
                        ma10_prev is not None and ma10_prev > 0
                        and abs(ma5_prev - ma10_prev) / ma10_prev <= 0.003
                        and ma5 > ma10
                    )
                )
                and (ma5 >= ma5_prev)
                and (ma5 > ma5_prev_prev or ma5_prev < ma5_prev_prev)
            )
        )
        # ② MA5가 MA10 위에서 주도권을 가진다
        # newborn: MA5>=MA10 + MA5상향 + 중장기 생존 확인
        # 일반: MA5>MA10만 허용
        # MA5<MA10 데드크로스는 절대 차단
        and (
            (_is_gc_newborn and _newborn_ma5_10_ok)
            or ((not _is_gc_newborn) and ma5 > ma10)
        )
        # 가격-MA5 이격도는 Fish Shape hard veto가 아니라 관측/Strategy soft penalty로만 소비한다.
        # ③ MA10 방향 (동적 임계값: 초입 시 하락 관성 허용)
        and (ma10_slope >= _eff_ma10_slope_min or _is_gc_newborn)
    ):
        result['reason'] = (
            f"HARD-CORE-GUARD("
            f"s5:{ma5_slope:.5f}, "
            f"Δma5:{((ma5 - min(ma5_prev_prev, ma5_prev)) / min(ma5_prev_prev, ma5_prev) if ma5_prev_prev and ma5_prev and min(ma5_prev_prev, ma5_prev) > 0 else 0):.5f}(min:{_eff_ma5_dir_min:.4f}), "
            f"ma5>=10:{ma5>=ma10}, "
            f"birth_traintrack:{birth_traintrack_ok}, "
            f"newborn_ma5_10:{_newborn_ma5_10_ok}, "
            f"mid_alive:{_newborn_mid_alive}, "
            f"long_alive:{_newborn_long_alive}, "
            f"s10:{ma10_slope:.5f}(min:{_eff_ma10_slope_min:.5f}), "
            f"s20:{ma20_slope:.5f}(min:{_eff_ma20_decel_limit:.5f}), "
            f"ma5>prev:{(ma5>ma5_prev) if ma5_prev is not None else None}(ma5:{ma5:.0f} prev:{(ma5_prev if ma5_prev is not None else 0):.0f} Δ:{(ma5-ma5_prev if ma5_prev is not None else 0):.0f}), "
            f"prev<prev2:{(ma5_prev<ma5_prev_prev) if (ma5_prev is not None and ma5_prev_prev is not None) else None}"
            f"(prev:{(ma5_prev if ma5_prev is not None else 0):.4f} "
            f"prev2:{(ma5_prev_prev if ma5_prev_prev is not None else 0):.4f}), "
            f"dev_observe:{(((current_price - ma5) / ma5) if (current_price is not None and ma5 and ma5 > 0) else 0):.5f}"
            f"(soft_max:{_eff_deviation_max:.5f}), "
            f"gc_newborn:{_is_gc_newborn}"
        )
        try:
            logger.debug(
                f"[HARDCORE-VALUES] {symbol} | "
                f"idx=-1 close={df['close'].iloc[-1]:.4f} "
                f"idx=-2 close={df['close'].iloc[-2]:.4f} "
                f"ma5={ma5:.4f} prev={ma5_prev:.4f} prev2={ma5_prev_prev:.4f} | "
                f"s5={ma5_slope:.5f} eff_core={_eff_core_min:.5f} eff_dir={_eff_ma5_dir_min:.5f} | "
                f"gc_newborn={_is_gc_newborn} dur={dur_5x10_bars}"
            )
        except Exception:
            pass
        return result
    
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    # ② MA20 하락 중단 (평행 이상) - 정석 복원
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    # 철학: "미세 음수 허용은 항상 더 위의 선이 책임진다"
    # 
    # 기본: MA20 >= 0 (양수)
    # 예외: MA20이 약한 음수(-0.01% ~ 0)일 때
    #       → MA5 & MA10이 뚜렷한 양수여야 함
    #       → 단기선 주도 초입 가자미근만 허용
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    MA20_SLOPE_WEAK_NEG = SoleusGajamiConstants.MA20_SLOPE_WEAK_NEG
    MA20_SLOPE_PULLBACK_MIN = SoleusGajamiConstants.MA20_SLOPE_PULLBACK_MIN
    # MA20_SLOPE_DECEL_LIMIT: 감속형 ma20_ok 경로에서는 제거됨, HARD CORE GUARD ④에서 사용 중
    MA5_SLOPE_STRONG_POS = SoleusGajamiConstants.MA5_SLOPE_STRONG_POS
    MA10_SLOPE_POS = SoleusGajamiConstants.MA10_SLOPE_POS
    
    # ✅ MA20 게이트 (정석)
    # 1) 기본: MA20 양수
    # 2) 약음수: 단기선(5/10) 강한 양수로 보완
    # 3) 눌림 허용: 상위(MA60) 상승 + MA20 눌림(-0.6%까지) + MA5 고개듦(필수)
    # 4) 단기 주도 전환 초입: MA60 음수여도 단기선 강정배열이면 허용
    result['ma20_ok'] = (
        (ma20_slope >= 0)
        or (
            (ma20_slope >= MA20_SLOPE_WEAK_NEG)
            and (ma5_slope >= MA5_SLOPE_STRONG_POS)
            and (ma10_slope >= MA10_SLOPE_POS)
        )
        or (
            (ma60_slope > 0.0)
            and (ma20_slope >= MA20_SLOPE_PULLBACK_MIN)
            and (ma5_slope >= 0.0)
        )
        or (
            (ma20_slope >= MA20_SLOPE_PULLBACK_MIN)
            and (ma5_slope >= SoleusGajamiConstants.MA5_SLOPE_PULLBACK_RATIO)
            and (ma10_slope >= 0.00020)
            and (ma5 > ma10 > 0)
        )
        or (
            ma10_slope >= 0.0
            and ma20_slope >= MA20_SLOPE_HARDCUT_MIN
        )
        or (
            is_birth_bottom
            and ma5_slope >= 0.0
            and ma10_slope >= -0.00010
            and ma20_slope >= MA20_SLOPE_HARDCUT_MIN
        )
        or (
            dur_5x10_bars is not None and int(dur_5x10_bars) <= SoleusGajamiConstants.NEWBORN_MAX_DUR
            and ma5_slope > 0
            and ma20_slope >= MA20_SLOPE_HARDCUT_MIN
        )
    )
    # MA20 하락 중 진입 차단 — 탄생 초입 2봉(6분)만 구조적 지연으로 허용
    result['ma20_ok'] = result['ma20_ok'] and (
        ma20_slope >= 0
        or (dur_5x10_bars is not None and int(dur_5x10_bars) <= SoleusGajamiConstants.NEWBORN_MAX_DUR)
        or long_backed_mid_pullback_ok
    )

    if not result['ma20_ok']:
        if ma20_slope >= MA20_SLOPE_WEAK_NEG:
            # 약한 음수 구간인데 단기선 부족
            result['reason'] = (
                f"MA20약음수(s20:{ma20_slope:.5f}) but "
                f"단기선부족(s5:{ma5_slope:.5f}<{MA5_SLOPE_STRONG_POS:.5f} "
                f"or s10:{ma10_slope:.5f}<{MA10_SLOPE_POS:.5f})"
            )
        else:
            # 강한 음수 → 무조건 차단
            result['reason'] = f"MA20하락(s20:{ma20_slope:.5f}<{MA20_SLOPE_WEAK_NEG:.5f})"
        return result
    
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    # 🔒 shape_ok 판정용 최소값 (HARD CORE GUARD가 양수를 보장)
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    ma5_slope_min = SoleusGajamiConstants.MA5_SLOPE_DEFAULT    # 0.00015
    ma10_slope_min = SoleusGajamiConstants.MA10_SLOPE_DEFAULT  # 0.00005
    
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    # ③ 5·10·20 상향 접힘 + MA5 리드 (고개 듦)
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    # 철학:
    #   - 기존: ma5_slope, ma10_slope 양수만 체크
    #   - 개선: MA5가 MA10보다 '리드(고개 듦)' 필수
    #   - 예외: 초입 수렴(-0.10%~0.10%)에서는 리드 완화
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    
    # 리드 조건 (MA5가 MA10보다 확실히 위로)
    LEAD_MIN = 0.00005  # 0.005%p (미세 리드)
    TIGHT_CONVERGE_RANGE = 0.40  # -0.20% ~ +0.20% (MA10 catch-up 수렴 허용) 0.2->0.4 20260427

    # 초입 수렴 판정 (방향성 고려)
    is_tight_converge = (-TIGHT_CONVERGE_RANGE <= gap_5_10_pct <= TIGHT_CONVERGE_RANGE)

    # 🆕 [LEAD-DUAL-STRONG②] L813과 동일원칙
    # MA5가 승자수준(≥0.0017)이면 MA10이 더 빨라도 lead OK.
    # 복어형(s5<0.0017)은 기존 lead/shape 게이트가 그대로 차단.
    lead_ok = (
        ma5_slope >= (ma10_slope + LEAD_MIN)
        or is_tight_converge
        or ma5_slope >= SoleusGajamiConstants.MA5_SLOPE_STRONG_LEAD_RATIO
    )
    
    # 🆕 탄생봉(dur<=2): MA10 평탄 허용 (HCG 563라인과 동일 원칙)
    _eff_ma10_shape_min = (
        SoleusGajamiConstants.MA10_SLOPE_NEWBORN_MIN
        if (dur_5x10_bars is not None and int(dur_5x10_bars) <= SoleusGajamiConstants.NEWBORN_MAX_DUR)
        else ma10_slope_min
    )
    
    # MA5-MA10 간격 상한은 gap_band()에서 유지하고,
    # 하한은 GC 직후 진입 지연을 유발하므로 shape 자격에서 사용하지 않는다.
    #  실측 n=908: dur<=2 gap 0.03~0.05=51.7%(29) / dur>=3 gap 0.50~0.80=47.8%(67)
    #  08-02 결론(gap<0.05 정답률 17.3%)과 충돌 아님 — dur<=2 0.00~0.03=20%(n=10, 정합) /
    #  0.03~0.05=51.7%(n=29, 여기가 잘렸었다) → 구간 세분 부족이었다
    #  철거 대상(기록 보존): _birth_tight_gc_shape / _is_birth_traintrack_shape /
    #    (dur<=2 and ma10_slope<=0). MEGA 08-01 17:24 오답봉이 gap 0.035%로 이 우회로를 통과했다.
    _, _GAP_MAX = SoleusGajamiConstants.gap_band(dur_5x10_bars)

    result['shape_ok'] = (
        (ma5_slope >= ma5_slope_min) and
        (ma10_slope >= _eff_ma10_shape_min or _is_gc_newborn) and
        lead_ok
    )

    if not result['shape_ok']:
        if not lead_ok:
            result['reason'] = f"리드부족(s5-s10={ma5_slope-ma10_slope:.5f}<{LEAD_MIN:.5f}, gap={gap_5_10_pct:.3f}%)"
        else:
            result['reason'] = f"형상미달(s5:{ma5_slope:.5f}<{ma5_slope_min:.5f} or s10:{ma10_slope:.5f}<{_eff_ma10_shape_min:.5f})"
        return result
    
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    # ④ 5·10·20 정배열 (완전 or 압축형)
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    full_aligned = (ma5 > ma10 > ma20)
    # intrabar_gc는 PRE-FILTER에서만 처리. 정배열 판정 면제 없음.

    if not full_aligned:
        # 최소 정배열 체크 (둘 다 MA20 위)
        min_aligned = ( ((ma5 >= ma20) and (ma10 > ma20)) or ((ma5 > ma10) and (ma5 >= ma20)) )

        if min_aligned:
            # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
            # 🔒 압축형 허용 (방향성 고려)
            # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
            COMPRESS_MIN = -0.15
            COMPRESS_MAX = _GAP_MAX      # 🔒 [GAP-BAND-SSOT-0807] gap_band()로 단일화

            _GAP_EPS = 1e-6
            result['aligned_ok'] = (
                COMPRESS_MIN - _GAP_EPS
                <= gap_5_10_pct
                <= COMPRESS_MAX + _GAP_EPS
            )

            if not result['aligned_ok']:
                # 🚫 CONVERGE-OVERRIDE 제거 (가자미근 헌법 위반)
                # - 헌법 0조에서 MA5>MA10(GC)은 이미 보장
                # - MA5<MA20은 GC 초입에서 자연스러운 상태이므로 단독 차단 금지
                # - 여기서는 MA5-MA10 압축 범위만 판단
                if gap_5_10_pct < COMPRESS_MIN:
                    result['reason'] = f"역배열(gap={gap_5_10_pct:.2f}%<{COMPRESS_MIN}%)"
                else:
                    # 🔒 [GAP-BAND-SSOT-0807] 이 분기는 min_aligned=True에서만 도달한다.
                    #    "정배열미달"은 의미가 정반대였다 — 실제는 과확장(gap 상한 초과)이다.
                    result['reason'] = f"과확장(gap={gap_5_10_pct:.2f}%>{COMPRESS_MAX}%)"
                return result

        else:
            # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
            # ✅ 정석: "초입 정렬(pre-aligned)" 허용
            # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
            # 반등 초입(±1~2분 허용)에서는 MA20 위로 완전히 올라오기 전
            # 구간이 존재한다. 'MA20 바로 아래'에서 수렴+상향접힘이
            # 확인되면 정렬로 인정한다.
            #
            # 조건:
            # 1) MA20 아래지만 근접(MA10 ≤ 0.15%, MA5 ≤ 0.25%)
            # 2) MA5/MA10 기울기 상향(초입 수렴 포함)
            # 3) gap_5_10_pct는 압축 범위 내
            #    - 일반: -0.15 ~ +0.50
            #    - newborn(dur≤2): -0.50 ~ +0.50
            # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
            try:
                if ma20 > 0:
                    dist10 = ((ma20 - ma10) / ma20) * 100.0
                    dist5 = ((ma20 - ma5) / ma20) * 100.0
                else:
                    dist10, dist5 = 999.0, 999.0

                _is_newborn = (dur_5x10_bars is not None and int(dur_5x10_bars) <= SoleusGajamiConstants.NEWBORN_MAX_DUR)
                COMPRESS_MIN = -0.50 if _is_newborn else -0.15
                COMPRESS_MAX = _GAP_MAX      # 🔒 [GAP-BAND-SSOT-0807] gap_band()로 단일화

                # 🆕 거리 한도 재확장: 3분봉 V자/반등 초입(정답 타점)에서 MA가 더 벌어지는 케이스 커버
                # - 기존 0.50%는 여전히 "초입"에서 부족할 수 있음
                # - 정배열 완성 전 단계의 "근접 정렬"만 인정(그 외 조건은 그대로 유지)
                PRE_ALIGN_DIST10_MAX = 1.00 if _is_newborn else 0.80
                PRE_ALIGN_DIST5_MAX  = 0.80

                if _is_newborn:
                    # 장기선 생존·중기선 구조는 위 newborn SSoT 판결을 그대로 소비한다.
                    pre_aligned = bool(_newborn_ma5_10_ok)
                else:
                    pre_aligned = bool(
                        (0.0 <= dist10 <= PRE_ALIGN_DIST10_MAX)
                        and (0.0 <= dist5 <= PRE_ALIGN_DIST5_MAX)
                        and (ma5_slope >= 0.0)
                        and (ma10_slope >= SoleusGajamiConstants.MA10_SLOPE_HALT)
                        and (COMPRESS_MIN - 1e-6 <= gap_5_10_pct <= COMPRESS_MAX + 1e-6)
                    )

                if pre_aligned:
                    result['aligned_ok'] = True
                else:
                    result['reason'] = f"정배열미달(5<20 or 10<20)"
                    return result

            except Exception:
                result['reason'] = f"정배열미달(5<20 or 10<20)"
                return result

    else:
        result['aligned_ok'] = True
    
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    # ✅ 최종 판정 (①은 외부에서 recent_cross_ok로 체크)
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    result['gajami_ok'] = True
    result['reason'] = 'OK'
    
    return result

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 🛡️ SafeLogger 래퍼: WinError 233 완전 차단 (자동 보호)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# (라인 63-82 전체 삭제)
# SafeLogger는 Bot 레벨에서만 사용
# Analysis는 예외를 명확히 노출하여 디버깅 신뢰도 유지

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 🛠️ 헬퍼 함수: 안전한 스칼라 변환
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
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
    
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 🛡️ 안전한 비교 함수: None 방어
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
def safe_leq(a, b) -> bool:
    """
    안전한 <= 비교 (None 방어)
    Args:
        a: 왼쪽 피연산자
        b: 오른쪽 피연산자
    Returns:
        bool: a <= b (둘 중 하나라도 None이면 False)
    Examples:
        >>> safe_leq(1.5, 2.0)
        True
        >>> safe_leq(None, 2.0)
        False
        >>> safe_leq(1.5, None)
        False
    """
    if a is None or b is None:
        return False
    try:
        return a <= b
    except (TypeError, ValueError):
        return False

def safe_geq(a, b) -> bool:
    """안전한 >= 비교 (None 방어)"""
    if a is None or b is None:
        return False
    try:
        return a >= b
    except (TypeError, ValueError):
        return False

def safe_in_range(value, min_val, max_val) -> bool:
    """
    안전한 범위 체크 (None 방어)
    Args:
        value: 체크할 값
        min_val: 최소값
        max_val: 최대값
    Returns:
        bool: min_val <= value <= max_val (하나라도 None이면 False)
    """
    if value is None or min_val is None or max_val is None:
        return False
    try:
        return min_val <= value <= max_val
    except (TypeError, ValueError):
        return False

class TechnicalIndicators:
    """선행성 지표와 확정봉 판사 보조지표 계산"""
    
    # RSI + RSI/MACD 다이버전스: 판사2 입력
    # BB·KC·Cycle Phase: 판사3 입력
    # MACD 단독 GO 금지

    _keltner_squeeze_ttl_state: dict = {}

    # 🔒 [ACTIVE-TLC-STATE-PERSIST-0816]
    # 프로세스 재시작 시 _cycle_phase_shadow_state(클래스 속성)가 소실되어
    # age/phase_start_bar가 매번 1부터 재기산되는 것을 방지한다.
    @staticmethod
    def _load_cycle_phase_state_cache() -> dict:
        import json
        from pathlib import Path
        path = Path("outputs") / "cache" / "tlc_cycle_state.json"
        try:
            with open(path, "r", encoding="utf-8") as f:
                data = json.load(f)
            states = data.get("states", {}) or {}
            restored = {}
            for key, state in states.items():
                if "|" not in key:
                    continue
                tf_key, symbol_key = key.split("|", 1)
                if tf_key and symbol_key:
                    restored[(tf_key, symbol_key)] = state
            logger.info(
                f"🔄 [ACTIVE-TLC-STATE-LOAD] restored={len(restored)} "
                f"path={path}"
            )
            return restored
        except FileNotFoundError:
            logger.info(
                f"[ACTIVE-TLC-STATE-LOAD] cache 없음 → fresh | "
                f"path={path}"
            )
            return {}
        except Exception as e:
            logger.warning(
                f"[ACTIVE-TLC-STATE-LOAD] 실패 → fresh | {e}"
            )
            return {}

    @staticmethod
    def _save_cycle_phase_state_cache(force: bool = False) -> None:
        import json
        import time as _time
        from pathlib import Path
        now = _time.time()
        last_save = float(
            getattr(
                TechnicalIndicators,
                "_cycle_phase_state_last_save",
                0.0,
            ) or 0.0
        )
        if not force and now - last_save < 60.0:
            return
        # 저장 실패 시 매 스캔마다 파일 I/O를 반복하지 않도록
        # 저장 시도 시점부터 60초 throttle을 건다.
        TechnicalIndicators._cycle_phase_state_last_save = now
        try:
            state = getattr(
                TechnicalIndicators,
                "_cycle_phase_shadow_state",
                {},
            ) or {}
            path = Path("outputs") / "cache" / "tlc_cycle_state.json"
            tmp = str(path) + ".tmp"
            path.parent.mkdir(parents=True, exist_ok=True)
            data = {
                "version": 1,
                "saved_at": now,
                "states": {
                    f"{key[0]}|{key[1]}": value
                    for key, value in state.items()
                    if (
                        isinstance(key, tuple)
                        and len(key) == 2
                        and isinstance(value, dict)
                    )
                },
            }
            with open(tmp, "w", encoding="utf-8") as f:
                json.dump(
                    data,
                    f,
                    ensure_ascii=False,
                )
            os.replace(tmp, path)
        except Exception as e:
            logger.debug(
                f"[ACTIVE-TLC-STATE-SAVE] 실패: {e}"
            )

    @staticmethod
    def mark_j1_parent_used(
        timeframe,
        symbol: str,
        parent_wave_id: str,
    ) -> bool:
        """실제 J1 BUY 체결 후 같은 parent의 J1 소유권을 USED로 확정."""
        _tf = (
            timeframe.value
            if hasattr(timeframe, "value")
            else str(timeframe)
        )
        _symbol = str(symbol or "")
        _wave = str(parent_wave_id or "NONE")
        if (
            not _tf
            or not _symbol
            or _symbol == "UNKNOWN"
            or _wave == "NONE"
        ):
            return False
        if not hasattr(
            TechnicalIndicators,
            "_cycle_phase_shadow_state",
        ):
            setattr(
                TechnicalIndicators,
                "_cycle_phase_shadow_state",
                TechnicalIndicators._load_cycle_phase_state_cache(),
            )
        _state = getattr(
            TechnicalIndicators,
            "_cycle_phase_shadow_state",
            {},
        ) or {}
        _key = (_tf, _symbol)
        _record = dict(_state.get(_key, {}) or {})
        _saved_wave = str(
            _record.get("j1_wave", "NONE") or "NONE"
        )
        # 신호가 생성된 뒤 parent가 바뀌었다면 오래된 신호가
        # 새 parent 소유권을 덮어쓰지 못하게 한다.
        if _saved_wave not in ("NONE", _wave):
            logger.warning(
                f"⚠️ [J1-PARENT-USED-MARK] {_symbol} wave mismatch | "
                f"signal={_wave} cache={_saved_wave} → USED 미기록"
            )
            return False
        _prev_wst = str(
            _record.get("j1_wst", "UNSEEN") or "UNSEEN"
        )
        _record["j1_wave"] = _wave
        _record["j1_wst"] = "USED"
        _state[_key] = _record
        setattr(
            TechnicalIndicators,
            "_cycle_phase_shadow_state",
            _state,
        )
        TechnicalIndicators._save_cycle_phase_state_cache(
            force=True
        )
        logger.info(
            f"🔒 [J1-PARENT-USED-MARK] {_symbol} | "
            f"pwave={_wave} wst={_prev_wst}->USED"
        )
        return True

    @staticmethod
    def calculate_atr(df: pd.DataFrame, period: int = DEFAULT_ATR_PERIOD,
                      atr_min_pct: float = 0.1) -> pd.Series:
        """ATR 계산 - 극소값 필터링"""
        if df is None or len(df) < period + 1:
            return pd.Series([0] * len(df) if df is not None else [])
        
        high_low = df['high'] - df['low']
        high_close = abs(df['high'] - df['close'].shift())
        low_close = abs(df['low'] - df['close'].shift())
        
        true_range = pd.concat([high_low, high_close, low_close], axis=1).max(axis=1)
        atr = true_range.rolling(window=period).mean()
        
        # ✅ 극소값 필터링
        atr = atr.fillna(0)
        
        # 현재가 대비 atr_min_pct% 미만은 0 처리 (TF별 분리)
        if len(df) > 0:
            current_price = df['close'].iloc[-1]
            if current_price > 0:
                atr_pct = (atr / current_price) * 100
                atr[atr_pct < atr_min_pct] = 0
        
        return atr
    
    @staticmethod
    def calculate_adx(df: pd.DataFrame, period: int = DEFAULT_ATR_PERIOD) -> pd.Series:
        """ADX 계산 (Average Directional Index - 추세 강도)"""
        if df is None or len(df) < period * 2:
            return pd.Series([ADX_DEFAULT_VALUE] * len(df) if df is not None else [])
        
        try:
            high = df['high']
            low = df['low']
            close = df['close']
            
            plus_dm = high.diff()
            minus_dm = -low.diff()
            
            plus_dm[plus_dm < 0] = 0
            minus_dm[minus_dm < 0] = 0
            
            tr = pd.concat([
                high - low,
                abs(high - close.shift()),
                abs(low - close.shift())
            ], axis=1).max(axis=1)
            
            atr = tr.rolling(window=period).mean()
            
            plus_di = 100 * (plus_dm.rolling(window=period).mean() / atr)
            minus_di = 100 * (minus_dm.rolling(window=period).mean() / atr)
            
            dx = 100 * abs(plus_di - minus_di) / (plus_di + minus_di + EPSILON)
            adx = dx.rolling(window=period).mean()
            
            return adx.fillna(ADX_DEFAULT_VALUE)
        except:
            return pd.Series([ADX_DEFAULT_VALUE] * len(df))
    
    @staticmethod
    def _is_candle_dense_enough(df, timeframe: str = '3m', symbol: str = '') -> tuple:
        """캔들 밀도 검사. (bool, reason_str) 반환."""
        try:
            if df is None:
                return False, "NO_DF"

            df_len = len(df)
            # 🔒 장기구조 hard floor: 확정 MA60 STRUCT20 성립 최소 봉수
            # - 헌법4조는 MA60 slope>0 OR MA120 slope>0 (OR조건, _long_ma_alive_verdict)
            # - MA120 미확보는 MA120_WINDOW_COUNT SOFT관측으로 하위 처리(계산은 지속)
            _REQUIRED_MA_BARS = SoleusGajamiConstants.MA60_STRUCT20_REQUIRED_BARS
            if df_len < _REQUIRED_MA_BARS:
                return False, f"DF_LEN_{df_len}<{_REQUIRED_MA_BARS}"

            if not hasattr(df.index[-1], 'to_pydatetime'):
                return True, "NO_DATETIME_INDEX_LEN_ONLY"

            # timeframe 정규화 (Enum / str 모두 대응)
            _tf_key = timeframe.value if hasattr(timeframe, "value") else str(timeframe)

            # timeframe별 동적 기준
            _tf_minutes = {
                '1m': 1, '3m': 3, '5m': 5, '10m': 10,
                '15m': 15, '30m': 30, '1h': 60, '4h': 240, '1d': 1440,
            }
            _tf_min          = _tf_minutes.get(_tf_key, 3)
            _expected_60     = max(1, int(60 / _tf_min))
            # 최근 60분 캔들 수는 데이터 품질 관측값으로만 사용하고 계산은 지속한다.
            _min_recent = max(1, math.ceil(_expected_60 * 0.70))
            last_ts   = df.index[-1]
            recent_60 = df[df.index >= last_ts - pd.Timedelta(minutes=60)]
            # 🔒 [CACHE-MAX-GAP-REST-0822-R3] 최근 20봉 국소 연속 결손을
            # count형 밀도보다 먼저 판정한다. 3m 기준 6분 초과 = 2봉 이상 연속 결손.
            if len(df) >= 20:
                _t20_idx = pd.DatetimeIndex(df.index[-20:])
                _t20_gaps = (
                    _t20_idx.to_series().diff().dropna().dt.total_seconds() / 60.0
                )
                _gap_max = float(_t20_gaps.max()) if len(_t20_gaps) > 0 else 0.0
                if _gap_max > _tf_min * 2.0:
                    return False, f"MAX_GAP_{_gap_max:.1f}m>{_tf_min * 2.0:.1f}m"

            # 🟡 DATA-QUALITY ②: 전체 밀도 50% 미만은 진입 hard block이 아니다.
            # False 반환은 downstream 캐시 REST 재구축 트리거로 유지한다.
            # t5의 진입 판정은 TOTAL_DENSITY 자체를 hard block으로 사용하지 않는다.
            if len(df) >= 2 and hasattr(df.index[0], 'to_pydatetime'):
                _span_min = (df.index[-1] - df.index[0]).total_seconds() / 60.0
                if _span_min > 0:
                    _expected_total = max(1.0, (_span_min / _tf_min) + 1.0)
                    _density_total = min(1.0, len(df) / _expected_total)
                    if _density_total < 0.50:
                        logger.info(
                            f"🟡 [CANDLE-SPARSE-SOFT] {symbol or 'UNKNOWN'} | "
                            f"density={_density_total:.2f}<0.50 "
                            f"tf={_tf_key} "
                            f"(span={_span_min:.0f}m n={len(df)} "
                            f"expected={_expected_total:.0f})"
                        )
                        return False, f"TOTAL_DENSITY_{_density_total:.2f}<0.50"

            # 🟡 SOFT 관측값: 계산은 계속하고 downstream에 reason만 전달
            _ma120_window_min = _tf_min * 120
            _ma120_min_count = max(1, math.ceil(120 * 0.60))
            _recent_ma120 = df[df.index >= last_ts - pd.Timedelta(minutes=_ma120_window_min)]
            if len(_recent_ma120) < _ma120_min_count:
                return False, (
                    f"MA120_WINDOW_COUNT_{len(_recent_ma120)}"
                    f"<{_ma120_min_count}"
                )

            if len(recent_60) < _min_recent:
                return False, f"RECENT60_COUNT_{len(recent_60)}<{_min_recent}"

            return True, "OK"

        except Exception as e:
            return False, f"DENSITY_ERR:{str(e)[:50]}"

    @staticmethod
    def detect_rally_pattern(df: pd.DataFrame, symbol: str = None) -> Dict[str, Any]:
        """급등 패턴 감지 (연대기 필터 핵심)

        Args:
            df: OHLCV 데이터프레임 (최신이 아래)
            symbol: 심볼명

        Returns:
            dict: {
                'has_rally': bool,
                'rally_pct': float,
                'rally_duration': int (분),
                'max_price': float,
                'max_price_idx': int,
                'low_price': float,
                'low_price_idx': int,
                'is_fast_rally': bool,
                'is_slow_rally': bool
            }
        """
        if not symbol or symbol == "UNKNOWN":
            logger.error(f"❌ [SSoT-BREAK] detect_rally_pattern() called without valid symbol")
            return {'rally_detected': False}
        try:
            # 최근 180분(60캔들 for 3m) 데이터 확인
            lookback = min(120, len(df))    ## 60->120 20260424
            # ✅ 최신이 아래(시간 오름차순) 전제로 통일
            recent_df = df.tail(lookback).copy()
            recent_df = recent_df.reset_index(drop=True)
            
            if len(recent_df) < 5:
                return {
                    'has_rally': False,
                    'rally_pct': 0.0,
                    'rally_duration': 0,
                    'max_price': 0.0,
                    'max_price_idx': 0,
                    'low_price': 0.0,
                    'low_price_idx': 0,
                    'is_fast_rally': False,
                    'is_slow_rally': False,
                    'pullback_ratio': 0.0
                }
            
            # 저점 찾기 (argmin으로 안전하게)
            low_position = int(recent_df['low'].values.argmin())
            low_price = float(recent_df['low'].iloc[low_position])
            
            # ✅ 저점 이후 구간(저점 다음 봉들)에서 최고가 탐색
            after_low_df = recent_df.iloc[low_position:]
            
            if len(after_low_df) == 0:
                return {
                    'has_rally': False,
                    'rally_pct': 0.0,
                    'rally_duration': 0,
                    'max_price': low_price,
                    'max_price_idx': low_position,
                    'low_price': low_price,
                    'low_price_idx': low_position,
                    'is_fast_rally': False,
                    'is_slow_rally': False,
                    'pullback_ratio': 0.0
                }
            
            max_position_local = int(after_low_df['high'].values.argmax())
            max_position = low_position + max_position_local
            max_price = float(after_low_df['high'].iloc[max_position_local])
            
            # 급등 계산 (시간 순서대로)
            rally_pct = (max_price - low_price) / low_price if low_price > 0 else 0.0
            rally_duration = (max_position - low_position) * 3  # 분
            
            # 급등 유형 판단
            is_fast_rally = rally_pct >= 0.05 and rally_duration <= 30  # 30분 이내 5%
            # 🔒 [RALLY-LIFESPAN-0808] 120봉 lookback을 그대로 rally 수명 상한으로 사용.
            #   기존 180분 상한은 이미 120봉(360분) 읽어 놓고 180분에 rally를 지워
            #   버려, 여전히 고점권인 장기 부모파동(PIEVERSE 08-08 rally=201분)이
            #   과열 판정(over_rally_penalty) 계산에서 아예 빠지는 결과를 낳았다.
            #   시간 상한 자체를 제거하고 lookback(120봉) 안의 rally는 전부 본다.
            is_slow_rally = rally_pct >= 0.05
            
            has_rally = is_fast_rally or is_slow_rally
            
            # pullback_ratio 계산 (고점 이후 되밀림 비율, 0~1)
            last_close = float(recent_df['close'].iloc[-1])
            denom = (max_price - low_price)
            pullback_ratio = ((max_price - last_close) / denom) if denom > 0 else 0.0
            
            return {
                'has_rally': has_rally,
                'rally_pct': rally_pct,
                'rally_duration': rally_duration,
                'max_price': max_price,
                'max_price_idx': max_position,
                'low_price': low_price,
                'low_price_idx': low_position,
                'is_fast_rally': is_fast_rally,
                'is_slow_rally': is_slow_rally,
                'pullback_ratio': pullback_ratio,
            }
            
        except Exception as e:
            logger.error(f"❌ [Rally-Pattern] {symbol} 급등 패턴 감지 실패: {e}")
            return {
                'has_rally': False,
                'rally_pct': 0.0,
                'rally_duration': 0,
                'max_price': 0.0,
                'max_price_idx': 0,
                'low_price': 0.0,
                'low_price_idx': 0,
                'is_fast_rally': False,
                'is_slow_rally': False,
                'pullback_ratio': 0.0
            }
    
    @staticmethod
    def calculate_all_indicators(df: pd.DataFrame, timeframe: str = '5m', symbol: str = None) -> Dict[str, Any]:
        """선행성 지표만 계산"""
        if not is_valid_df(df):
            return {}

        # 🆕 캔들 밀도 검사 — hard block / soft warning 분리
        _dense_ok, _dense_reason = TechnicalIndicators._is_candle_dense_enough(df, timeframe, symbol or '')

        # 🔒 캔들 밀도 / MA 신뢰성 실패 중 구조 불능만 hard block.
        # MA120_WINDOW_COUNT / RECENT60_COUNT는 관측값으로 전달하고 계산은 지속한다.
        _density_hard_block = bool(
            not _dense_ok
            and (
                str(_dense_reason).startswith("NO_DF")
                or str(_dense_reason).startswith("DF_LEN_")
                or str(_dense_reason).startswith("DENSITY_ERR")
            )
        )

        if _density_hard_block:
            if symbol:
                # 🔧 [DENSITY-BLOCK] debug→info 승격
                # - POWR류 calculate_all_indicators_returned_empty 사유 운영로그 가시화
                # - staticmethod이므로 self 사용 금지
                # - 동일 (symbol, reason) 반복은 debug로 낮춰 로그 비대화 방지
                _dseen = getattr(TechnicalIndicators, "_density_block_seen", None)
                if _dseen is None:
                    _dseen = {}
                    setattr(TechnicalIndicators, "_density_block_seen", _dseen)
                if _dseen.get(symbol) != _dense_reason:
                    _dseen[symbol] = _dense_reason
                    logger.info(
                        f"⏭️ [DENSITY-BLOCK] {symbol} | "
                        f"reason={_dense_reason} "
                        f"df_len={len(df) if df is not None else 0} "
                        f"→ 핵심 데이터 신뢰성 부족, 진입체크 스킵"
                    )
                else:
                    logger.debug(
                        f"⏭️ [DENSITY-BLOCK] {symbol} | reason={_dense_reason} (반복)"
                    )
            return {}

        if not _dense_ok and not _density_hard_block and symbol:
            _sseen = getattr(TechnicalIndicators, "_density_soft_seen", None)
            if _sseen is None:
                _sseen = {}
                setattr(TechnicalIndicators, "_density_soft_seen", _sseen)
            if _sseen.get(symbol) != _dense_reason:
                _sseen[symbol] = _dense_reason
                logger.info(
                    f"🟡 [DENSITY-SOFT] {symbol} | "
                    f"reason={_dense_reason} "
                    f"df_len={len(df) if df is not None else 0} "
                    f"→ 계산 지속"
                )

        # hard 사유만 위 DENSITY-BLOCK에서 return {} 처리한다.
        # MA120_WINDOW_COUNT / RECENT60_COUNT는 candle_dense_reason으로 전달하고 계산은 지속한다.
        
        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        # 🔒 SSoT 강제: symbol 필수 검증
        # - UNKNOWN/None 허용 금지: 상태 추적 불가능
        # - 방어적 fallback 제거: 문제를 즉시 노출
        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        if not symbol or symbol == "UNKNOWN":
            logger.error(
                f"❌ [SSoT-BREAK] calculate_all_indicators() called without valid symbol | "
                f"symbol={symbol} | df_len={len(df) if df is not None else 0}"
            )
            return {}
        
        indicators = {}
        indicators["symbol"] = symbol
        # 🔒 [2026-08-21] candle_dense_ok / _reason / _density_hard_block 3키 ★제거 (소비처 0곳).
        #  밀도 판정은 t5 내부 `_dense_ok` 로 계속 수행되며 t7 CACHE-REST-REPAIR 경로도 영향 없다.
        
        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        # 🆕 SSoT: 현재가 주입 (Strategy/A그룹 공용)
        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        indicators['current_price'] = float(df['close'].iloc[-1]) if len(df) > 0 else 0.0
        # 🐟 직전 확정봉 close (가자미근 방향성 체크용: signal.price > prev_candle_close)
        # - df['close'].iloc[-1] = 현재봉 intrabar close (signal.price와 동일 봉)
        # - df['close'].iloc[-2] = 직전 확정 3분봉 close ← Gajami-Direction-Block 소비
        indicators['prev_candle_close'] = float(df['close'].iloc[-2]) if len(df) >= 2 else 0.0
        # 🔒 직전 확정봉 품질 SSoT — 현재 진행봉(iloc[-1]) 완전 제외
        try:
            _pb_open = float(df['open'].iloc[-2])
            _pb_close = float(df['close'].iloc[-2])
            _pb_volume = float(df['volume'].iloc[-2])
            _pb_volume_base = float(df['volume'].iloc[-22:-2].mean())
            _pb_body_pct = nzs(
                ((_pb_close - _pb_open) / _pb_open) * 100.0
                if _pb_open > 0.0 else SENTINEL_FLOAT
            )
            _pb_volume_ratio = nzs(
                _pb_volume / _pb_volume_base
                if _pb_volume_base > 0.0 else SENTINEL_FLOAT
            )
        except Exception:
            _pb_body_pct = SENTINEL_FLOAT
            _pb_volume_ratio = SENTINEL_FLOAT
        indicators['prev_bar_body_pct'] = float(_pb_body_pct)
        indicators['prev_bar_volume_ratio'] = float(_pb_volume_ratio)
        indicators['prev_bar_quality_ready'] = int(
            _pb_body_pct != SENTINEL_FLOAT
        )
        # 👁️ [PREV-BAR-QUALITY-OBS-0813] 직전봉 body/volume은 관측만 유지한다.
        # +0.30% body hard gate는 7일 forward에서 역변별(PASS 17.8% < BLOCK 30.3%)
        # 되어 판정권을 제거했다 — prev_bar_quality_ok_3m 계산 자체를 철거한다.
        # (사용처 0건 재확인 후 제거: 되살아나는 지뢰를 남기지 않는다)
        # (ENTRY-BAR-POS 제거 — 2026-08-02) 목적 불명확 + 오류성으로 재판정하여 철거.
        #  pos=1.00은 완성봉 고가가 아니라 그 시점까지 도착한 틱 중 최고가라, 봉이
        #  진행될수록 같은 가격도 판정이 뒤집힌다. 봉 초반엔 ready=0 fail-open이라
        #  정작 필요한 순간엔 작동하지 않는다. 결과적으로 "골크 직후 ASAP 진입" 원칙과
        #  반대 방향(상승 순간엔 대기, 눌린 뒤엔 허용)으로 작동했다.
        #  현재봉 미확정 GC는 CURRENT_BAR_GC_ENTRY, 오래된 GC는 STALE_GC(dur>4)가
        #  각자 담당하고 있어 이 게이트가 맡아야 할 독립 역할이 없다.
        indicators['current_candle_open'] = float(df['open'].iloc[-1]) if (len(df) >= 1 and 'open' in df.columns) else 0.0
        # 🆕 현재 분석 시점의 마지막 완성 봉 (판결 시점 갱신용)
        indicators['current_candle_time'] = str(df.index[-1])[:16] if len(df) > 0 else ''
        # 감지봉 초기값 (Strategy에서 판결 PASS 시에만 갱신됨)
        indicators['detect_candle_time'] = ''  # 빈 값으로 초기화
        
        # ✅ 선행성 지표만!
        # TF별 ATR 극소값 기준 (3분봉: 0.05%, 그 외: 0.1%)
        _ATR_MIN_PCT = {'1m': 0.03, '3m': 0.05, '5m': 0.08}
        _atr_min = _ATR_MIN_PCT.get(timeframe, 0.1)
        indicators['atr'] = TechnicalIndicators.calculate_atr(df, atr_min_pct=_atr_min)
        indicators['adx'] = TechnicalIndicators.calculate_adx(df)
        
        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        # 🆕 B-3: ATR% (Strategy 소비용 SSoT)
        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        try:
            atr_last = safe_extract_scalar(indicators.get('atr'), 0.0)
            cur = float(indicators.get('current_price', 0.0) or 0.0)
            indicators['atr_pct'] = (atr_last / cur * 100.0) if cur > 0 else 0.0
        except Exception:
            indicators['atr_pct'] = 0.0
        
        # ADX 이전 값 (3봉 전) 저장
        adx_series = TechnicalIndicators.calculate_adx(df)
        if not adx_series.empty and len(adx_series) >= 4:
            indicators['adx_prev'] = adx_series.iloc[-4]  # 3봉 전 (현재=-1, 1봉전=-2, 2봉전=-3, 3봉전=-4)
        else:
            indicators['adx_prev'] = indicators.get('adx', 0)
            
        # MA (추세 확인용) - 네이밍 통일: ma5, ma10, ma15, ma20, ma60, ma120
        if len(df) >= 5:
            indicators['ma5'] = df['close'].rolling(window=5).mean()
        
        if len(df) >= 10:
            indicators['ma10'] = df['close'].rolling(window=10).mean()

            # 🔒 [TLC step2 PhaseA] gap_5_10 확정봉 export — Strategy .iloc[-2] 재계산(계약위반) 대체.
            try:
                _g5s = indicators.get("ma5"); _g10s = indicators.get("ma10")
                if _g5s is not None and _g10s is not None and len(_g5s) >= 2 and len(_g10s) >= 2:
                    _g5c = float(_g5s.iloc[-2]); _g10c = float(_g10s.iloc[-2])
                    indicators["gajami_gap_5_10_closed"] = (abs(_g5c - _g10c) / _g10c) if _g10c > 0 else 1.0
                else:
                    indicators["gajami_gap_5_10_closed"] = 1.0
            except Exception:
                indicators["gajami_gap_5_10_closed"] = 1.0
        
        if len(df) >= VOLUME_MA_PERIOD:  # 20
            indicators['ma20'] = df['close'].rolling(window=VOLUME_MA_PERIOD).mean()
            
            # MA15 = (MA10 + MA20) / 2 (업비트 미제공, 중간 추세)
            if 'ma10' in indicators and indicators['ma10'] is not None:
                indicators['ma15'] = (indicators['ma10'] + indicators['ma20']) / 2
        
        if len(df) >= 60:
            indicators['ma60'] = df['close'].rolling(window=60).mean()
        
        if len(df) >= 120:
            indicators['ma120'] = df['close'].rolling(window=120).mean()
        
        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        # 🆕 B-4: Strategy 호환 별칭 (sma_*) + ma_deviation
        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        # - Strategy는 sma_5 등의 키를 직접 참조하는 경로가 있음
        # - 값은 Series여도 safe_extract_scalar가 마지막 값을 추출하므로 문제 없음
        indicators['sma_5'] = indicators.get('ma5')
        indicators['sma_10'] = indicators.get('ma10')
        indicators['sma_20'] = indicators.get('ma20')
        indicators['sma_60'] = indicators.get('ma60')
        indicators['sma_120'] = indicators.get('ma120')
        
        # ma_deviation: (현재가 - SMA5) / SMA5 (%)
        try:
            sma5_last = safe_extract_scalar(indicators.get('sma_5'), 0.0)
            cur = float(indicators.get('current_price', 0.0) or 0.0)
            indicators['ma_deviation'] = ((cur - sma5_last) / sma5_last * 100.0) if sma5_last > 0 else 0.0
        except Exception:
            indicators['ma_deviation'] = 0.0
        
        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        # 🆕 B-5: MA slope (%), Strategy 소비용 SSoT
        # - 최근 3봉 기준 변화율(%)로 단순화
        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        # 형성 중 봉을 쓰던 미사용 helper는 제거하고 확정봉 helper만 유지한다.
        def _slope_pct_closed(ma_series, lookback=2) -> float:
            try:
                if ma_series is None or len(ma_series) < lookback + 2:
                    return 0.0
                now = float(ma_series.iloc[-2])
                prev = float(ma_series.iloc[-2 - lookback])
                if lookback >= 2:
                    prev_prev = float(ma_series.iloc[-3])
                    prev = min(prev, prev_prev)
                return ((now - prev) / prev * 100.0) if prev > 0 else 0.0
            except Exception:
                return 0.0

        # 🔒 SSoT 정책:
        # - indicators['ma*_slope']는 기울기(% 단위)
        #   → lookback=2: iloc[-1] vs iloc[-3] = 3봉(현재봉 포함)
        # - 가자미근(Soleus) 판정 시 ratio로 변환하여 사용
        # - df 직접 계산은 구조/방향성 보조 판단 전용
        # 🔒 MA별 slope 기준:
        # - MA5 slope: 현재봉 반응 허용 (lookback=2, 3봉, 9분)
        # - MA10 / MA15 / MA20 slope: 확정봉 기준 (lookback=2~3)
        # - MA60 / MA120 slope: STRUCT20 / FALLBACK1 기준 (20봉 구조 slope)
        # - ma60_slope_legacy / ma120_slope_legacy는 감사 로그용 보존값
        # 🔧 MA5 slope 보강:
        # - fetch 경계에서 MA5가 "디플레이션"되며 lookback=2가 우연히 0이 되는 케이스 방지
        # - 상승(+)이면 더 강한 기울기 선택, 하락(-)이면 더 강한 하락(더 음수) 선택
        # - 부호가 갈리면 단기(lookback=2)를 우선(노이즈/반전 과대해석 방지)
        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        # 🔒 MA slope 시간 기준 분리
        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        # - MA5 slope: BIRTH / 가자미근 조기 진입용 현재봉 반응 허용
        # - MA10 slope: 현재봉 기준 (iloc[-1] vs iloc[-4], 3봉 차이 lookback)
        # - MA15 이상 / Regime 방향 판단: 확정봉 기준
        # - 이유: MA5는 조기 진입, MA10 이상은 추세 생존 판정 역할
        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        try:
            # MA5 slope (이전봉 대비)
            ma5_series = indicators.get('ma5')
            if ma5_series is not None and len(ma5_series) >= 3:
                ma5_c0 = float(ma5_series.iloc[-1])   # 현재봉
                ma5_c1 = float(ma5_series.iloc[-2])   # 전봉
                ma5_p1 = float(ma5_series.iloc[-3])   # 전전봉
                _s5_a = ((ma5_c0 - ma5_p1) / ma5_p1 * 100.0) if ma5_p1 > 0 else 0.0
                _s5_b = ((ma5_c0 - ma5_c1) / ma5_c1 * 100.0) if ma5_c1 > 0 else 0.0
                ma5_slope = (_s5_a + _s5_b) / 2.0
                logger.debug(
                    f"[MA5-SLOPE-AUDIT-CALC] {symbol} | "
                    f"ma5_slope={ma5_slope:+.5f}% "
                    f"il-1={ma5_c0:.5f} il-2={ma5_c1:.5f} il-3={ma5_p1:.5f} "
                    f"s5a={_s5_a:+.5f} s5b={_s5_b:+.5f}"
                )
            else:
                ma5_slope = 0.0
                logger.debug(
                    f"[MA5-SLOPE-AUDIT-CALC] {symbol} | "
                    f"ma5_slope=0.0 ma5_series=None_or_len<3"
                )
            
            # MA10 slope (현재봉 반영 — MA5>MA10 골크 비교 대칭, 2026-07-26 헌법 정합)
            ma10_series = indicators.get('ma10')
            if ma10_series is not None and len(ma10_series) >= 3:
                ma10_c0 = float(ma10_series.iloc[-1])   # 현재봉
                ma10_c1 = float(ma10_series.iloc[-2])   # 전봉
                ma10_p1 = float(ma10_series.iloc[-3])   # 전전봉
                _s10_a = ((ma10_c0 - ma10_p1) / ma10_p1 * 100.0) if ma10_p1 > 0 else 0.0
                _s10_b = ((ma10_c0 - ma10_c1) / ma10_c1 * 100.0) if ma10_c1 > 0 else 0.0
                ma10_slope = (_s10_a + _s10_b) / 2.0
            else:
                ma10_slope = 0.0
            
            indicators['ma5_slope'] = ma5_slope
            indicators['ma10_slope'] = ma10_slope
            
        except Exception as e:
            # Fallback: 기존 방식
            logger.warning(f"⚠️ MA slope 계산 실패: {e}, 기존 방식 사용")
        
        # MA15 이상 추세 필터: RT 현재봉 제외, 확정봉 기준
        indicators['ma15_slope'] = _slope_pct_closed(indicators.get('ma15'), 2)
        # 🔒 RT 아티팩트 방지: MA20 slope 확정봉 기준 (iloc[-2] vs iloc[-3])
        _ma20_ser = indicators.get('ma20')
        if _ma20_ser is not None and len(_ma20_ser) >= 4:
            _m20_now  = float(_ma20_ser.iloc[-2])
            _m20_prev = float(_ma20_ser.iloc[-3])
            indicators['ma20_slope'] = ((_m20_now - _m20_prev) / _m20_prev * 100.0) if _m20_prev > 0 else 0.0
        else:
            indicators['ma20_slope'] = 0.0

        # 🟢 MA20 구조 slope (5봉) — MID눌림/TOP_RISE 판단용, 단봉 노이즈 배제
        try:
            _m20_s2 = indicators.get('ma20')
            if _m20_s2 is not None and len(_m20_s2) >= 7:
                _m20_now2 = float(_m20_s2.iloc[-2])
                _m20_p5   = float(_m20_s2.iloc[-7])
                indicators['ma20_slope_struct5'] = ((_m20_now2 - _m20_p5) / _m20_p5 * 100.0) if _m20_p5 > 0 else 0.0
                indicators['ma20_slope_struct_source'] = 'STRUCT5'
            else:
                indicators['ma20_slope_struct5'] = float(indicators.get('ma20_slope', 0.0) or 0.0)
                indicators['ma20_slope_struct_source'] = 'FALLBACK_SHORT'
        except Exception:
            indicators['ma20_slope_struct5'] = float(indicators.get('ma20_slope', 0.0) or 0.0)
            indicators['ma20_slope_struct_source'] = 'FALLBACK_ERR'

        _s60_legacy = _slope_pct_closed(indicators.get('ma60'), 3)
        _ma120_s = indicators.get('ma120')
        _ma120_slope_ready = bool(
            _ma120_s is not None
            and len(_ma120_s) >= 122
            and math.isfinite(float(_ma120_s.iloc[-2]))
            and math.isfinite(float(_ma120_s.iloc[-3]))
            and float(_ma120_s.iloc[-2]) > 0.0
            and float(_ma120_s.iloc[-3]) > 0.0
        )
        _s120_legacy = (
            _slope_pct_closed(_ma120_s, 5)
            if _ma120_slope_ready
            else SENTINEL_FLOAT
        )
        try:
            _ma60_s = indicators.get('ma60')
            _s60_gate = _s60_legacy
            _s120_gate = _s120_legacy

            # 20봉 구조 slope (판단부 slope_ma60/120과 동일 기준)
            _ma60_p20 = (
                float(_ma60_s.iloc[-22])
                if _ma60_s is not None and len(_ma60_s) >= 81
                   and math.isfinite(float(_ma60_s.iloc[-22]))
                   and float(_ma60_s.iloc[-22]) > 0
                else None
            )
            _ma60_p1 = (
                float(_ma60_s.iloc[-3])
                if _ma60_s is not None and len(_ma60_s) >= 3
                else None
            )
            _s60_struct = (
                (float(_ma60_s.iloc[-2]) - _ma60_p20) / _ma60_p20 * 100.0
                if _ma60_p20 is not None
                else (
                    (float(_ma60_s.iloc[-2]) - _ma60_p1) / _ma60_p1 * 100.0
                    if _ma60_p1 is not None and _ma60_p1 > 0
                    else _s60_gate
                )
            )
            _s60_src = "STRUCT20" if _ma60_p20 is not None else "FALLBACK1"

            if not _ma120_slope_ready:
                _s120_struct = SENTINEL_FLOAT
                _s120_src = "UNAVAILABLE"
            else:
                _ma120_p20 = (
                    float(_ma120_s.iloc[-22])
                    if len(_ma120_s) >= 141
                       and math.isfinite(float(_ma120_s.iloc[-22]))
                       and float(_ma120_s.iloc[-22]) > 0
                    else None
                )
                _ma120_p1 = (
                    float(_ma120_s.iloc[-3])
                    if len(_ma120_s) >= 3
                    else None
                )
                _s120_struct = (
                    (float(_ma120_s.iloc[-2]) - _ma120_p20) / _ma120_p20 * 100.0
                    if _ma120_p20 is not None
                    else (
                        (float(_ma120_s.iloc[-2]) - _ma120_p1) / _ma120_p1 * 100.0
                        if _ma120_p1 is not None and _ma120_p1 > 0
                        else _s120_gate
                    )
                )
                _s120_src = "STRUCT20" if _ma120_p20 is not None else "FALLBACK1"

            indicators['ma60_slope'] = float(_s60_struct)
            indicators['ma120_slope'] = float(_s120_struct)
            indicators['ma60_slope_source'] = _s60_src
            indicators['ma120_slope_source'] = _s120_src
            indicators['ma60_slope_legacy'] = float(_s60_legacy)
            indicators['ma120_slope_legacy'] = float(_s120_legacy)
            logger.debug(
                f"[LONG-MA-SLOPE-AUDIT] {symbol} | "
                f"s60={_s60_struct:+.6f} src={_s60_src} legacy={_s60_legacy:+.6f} | "
                f"s120={_s120_struct:+.6f} src={_s120_src} legacy={_s120_legacy:+.6f}"
            )
        except Exception as _long_slope_e:
            indicators['ma60_slope'] = float(_s60_legacy)
            indicators['ma120_slope'] = float(_s120_legacy)
            indicators['ma60_slope_source'] = "LEGACY_FALLBACK"
            indicators['ma120_slope_source'] = "LEGACY_FALLBACK"
            indicators['ma60_slope_legacy'] = float(_s60_legacy)
            indicators['ma120_slope_legacy'] = float(_s120_legacy)
            logger.debug(
                f"[LONG-MA-SLOPE-AUDIT] {symbol} | "
                f"struct_calc_error={str(_long_slope_e)[:80]} -> legacy fallback | "
                f"s60={_s60_legacy:+.6f} src=LEGACY_FALLBACK | "
                f"s120={_s120_legacy:+.6f} src=LEGACY_FALLBACK"
            )
        
        # 🆕 MA5 기울기 히스토리 + MA15-Exit 확정봉 방향 SSoT
        # - exit_closed: iloc[-2] 확정봉 vs iloc[-4], MA5/MA10 동일 시간축
        indicators['ma5_slope_exit_closed'] = SENTINEL_FLOAT
        indicators['ma10_slope_exit_closed'] = SENTINEL_FLOAT
        try:
            ma5_series = indicators.get('ma5')
            ma10_series = indicators.get('ma10')

            if ma5_series is not None and len(ma5_series) >= 6:
                ma5_now_prev1 = float(ma5_series.iloc[-2])
                ma5_old_prev1 = float(ma5_series.iloc[-4])
                indicators['ma5_slope_prev1'] = ((ma5_now_prev1 - ma5_old_prev1) / ma5_old_prev1 * 100.0) if ma5_old_prev1 > 0 else 0.0
                indicators['ma5_slope_exit_closed'] = indicators['ma5_slope_prev1']
            else:
                indicators['ma5_slope_prev1'] = 0.0

            if ma10_series is not None and len(ma10_series) >= 4:
                ma10_now_closed = float(ma10_series.iloc[-2])
                ma10_old_closed = float(ma10_series.iloc[-4])
                if ma10_old_closed > 0:
                    indicators['ma10_slope_exit_closed'] = (
                        (ma10_now_closed - ma10_old_closed)
                        / ma10_old_closed * 100.0
                    )
        except Exception as e:
            logger.debug(f"확정봉 exit slope 계산 실패: {e}")
            indicators['ma5_slope_prev1'] = 0.0            
            indicators['ma5_slope_exit_closed'] = SENTINEL_FLOAT
            indicators['ma10_slope_exit_closed'] = SENTINEL_FLOAT
        
        # 🔒 2봉전 이격도 (이격도 추세 감지용)
        try:
            _ma5_s = indicators.get('ma5')
            if _ma5_s is not None and len(_ma5_s) >= 3 and len(df) >= 3:
                _c_prev2 = float(df['close'].iloc[-3])
                _m5_prev2 = float(_ma5_s.iloc[-3])
                if _m5_prev2 > 0:
                    indicators['price_to_ma5_prev2_pct'] = (_c_prev2 - _m5_prev2) / _m5_prev2 * 100.0
                else:
                    indicators['price_to_ma5_prev2_pct'] = 0.0
            else:
                indicators['price_to_ma5_prev2_pct'] = 0.0
        except Exception:
            indicators['price_to_ma5_prev2_pct'] = 0.0
        
        # 거래량 이동평균 (선행성!)
        if 'volume' in df.columns and len(df) >= VOLUME_MA_PERIOD:
            # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
            # 🔧 ZBT 오류 수정 (2025-11-02)
            # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
            # 1️⃣ 현재 봉 제외하여 평균 계산 (오버샘플링 방지)
            # - 기존: iloc[-1] (현재 봉 포함 → WebSocket 타이밍으로 2배 부풀림)
            # - 수정: iloc[-2] (현재 봉 제외 → 정확한 평균)
            # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
            volume_ma_series = df['volume'].rolling(window=VOLUME_MA_PERIOD).mean()
            if len(df) >= VOLUME_MA_PERIOD + 1:
                volume_ma_value = volume_ma_series.iloc[-2]  # 🔧 수정: iloc[-2]
            else:
                volume_ma_value = volume_ma_series.iloc[-1] if not volume_ma_series.empty else 0
            indicators['volume_ma'] = volume_ma_value
            
            # volume_ratio 계산 (strategy에서 필요) ✅
            current_volume = df['volume'].iloc[-1] if len(df) > 0 else 0
            prev_volume = df['volume'].iloc[-2] if len(df) >= 2 else current_volume
            
            if volume_ma_value > 0:
                raw_volume_ratio = current_volume / volume_ma_value
            else:
                raw_volume_ratio = 0
            
            # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
            # 2️⃣ 단봉 거래량 급등(spike) 필터
            # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
            # 문제: 최근 5봉 감소 추세인데 현재봉만 급등 → 허위 spike
            # 대책: 이전 추세 하락 + 단봉 1.8배 이상이면 신호 무효

            # 거래량 스파이크 검증 (형상 기반)
            knee_exception_hit = False
            knee_exception_spike_ratio = None
            
            if len(df) >= 6:
                recent_vol = df['volume'].tail(6).values
                if len(recent_vol) >= 6:
                    slope_5 = np.polyfit(range(5), recent_vol[:-1], 1)[0]
                    mean_5 = np.mean(recent_vol[:-1])
                    norm_slope = slope_5 / mean_5 if mean_5 > 0 else 0
                    spike_ratio = recent_vol[-1] / mean_5 if mean_5 > 0 else 1.0
                    knee_exception_spike_ratio = spike_ratio
                    
                    if norm_slope < 0 and spike_ratio >= 1.8:
                        knee_exception_hit = True
                        logger.debug(
                            f"⚠️ [거래량스파이크] {symbol} 최근 5봉 감소({norm_slope:.3f}) + "
                            f"단봉 폭등({spike_ratio:.2f}x) → 페널티 적용 (무효화 아님)"
                        )
                        # 🆕 raw_volume_ratio 덮어쓰기 제거 (관측값 보존)
            
            indicators['knee_exception'] = knee_exception_hit
            indicators['knee_exception_spike_ratio'] = knee_exception_spike_ratio
            
            # 🆕 거래량 스파이크 페널티 (점수 감점용)
            if knee_exception_hit:
                indicators['volume_spike_penalty'] = 0.85  # 15% 감점
            else:
                indicators['volume_spike_penalty'] = 1.0   # 페널티 없음
                    
            # 🔧 PATCH: spike_up_pct 추가 (Trading_bot_6_strategy.py 호환)
            # - knee_exception_spike_ratio를 %로 변환
            # - None일 경우 0.0 반환
            if knee_exception_spike_ratio is not None:
                indicators['spike_up_pct'] = (knee_exception_spike_ratio - 1.0) * 100
            else:
                indicators['spike_up_pct'] = 0.0
                        
            indicators['raw_volume_ratio'] = raw_volume_ratio
            
            # 🆕 완벽한 거래량 압력 분석 (Perfect Volume Pressure Analysis)
            # ========================================
            # 핵심 개선:
            # 1. 시간 창 일치: 최근 N개 캔들만 사용
            # 2. 가중 평균: 최신 캔들 강조 (70% 최근 + 30% 전체)
            # 3. 급격한 전환 감지: 매수 → 매도 전환 포착
            # 4. 거래량 가중: 거래량 큰 캔들에 더 높은 영향
            # ========================================
            
            if len(df) >= VOLUME_MA_PERIOD * 2 and 'open' in df.columns:
                # 시간 창 설정
                # raw volume ratio는 실시간 활동량이므로 현재봉 포함을 유지한다.
                recent_window = df.tail(VOLUME_MA_PERIOD)
                previous_window = df.tail(VOLUME_MA_PERIOD * 2).head(VOLUME_MA_PERIOD)
                # 🔒 [RT-BAR-BUYRATIO-EXCLUDE-0809]
                # close>open / close<open 방향 판정은 확정봉만 사용한다.
                _closed_buy_df = df.iloc[:-1]
                _buying_recent_window = _closed_buy_df.tail(VOLUME_MA_PERIOD)
                _buying_previous_window = _closed_buy_df.iloc[
                    -(VOLUME_MA_PERIOD * 2):-VOLUME_MA_PERIOD
                ]
                very_recent = _closed_buy_df.tail(5)
                
                # 거래량 비율 계산 (시간 창 일치!)
                recent_total_volume = recent_window['volume'].sum()
                previous_avg_volume = previous_window['volume'].mean()
                
                if previous_avg_volume > 0:
                    raw_volume_ratio = (recent_total_volume / VOLUME_MA_PERIOD) / previous_avg_volume
                else:
                    raw_volume_ratio = 0
                
                # 거래량 가중 매수 비율 계산 함수
                def calc_weighted_buying_ratio(window):
                    if len(window) == 0:
                        return 0.5
                    bullish = window['close'] > window['open']
                    bearish = window['close'] < window['open']
                    doji = window['close'] == window['open']
                    
                    buying_vol = window.loc[bullish, 'volume'].sum()
                    selling_vol = window.loc[bearish, 'volume'].sum()
                    doji_vol = window.loc[doji, 'volume'].sum()
                    
                    buying_vol += doji_vol / 2
                    selling_vol += doji_vol / 2
                    total = buying_vol + selling_vol
                    
                    return buying_vol / total if total > 0 else 0.5
                
                # 매수 비율 계산 — 방향 판정은 확정봉 SSoT
                buying_ratio_full = calc_weighted_buying_ratio(_buying_recent_window)
                buying_ratio_recent = calc_weighted_buying_ratio(very_recent)
                previous_buying_ratio = calc_weighted_buying_ratio(_buying_previous_window)

                # 🔧 [BUYRATIO-TRADE v2 2026-10-30] 실제 체결 수량 비율은 항상 계산 · 운영 변수 교체는 buying_ratio_source = trade 일 때만
                _br_candle = (buying_ratio_full, buying_ratio_recent, previous_buying_ratio)
                _br_real = None
                _br_source = 'candle'
                try:
                    from Trading_bot_4_data import BuyFlowBook
                    if symbol:
                        _br_values = []
                        for _br_w, _br_n in (
                            (_buying_recent_window, VOLUME_MA_PERIOD),
                            (very_recent, 5),
                            (_buying_previous_window, VOLUME_MA_PERIOD),
                        ):
                            _br_r = BuyFlowBook.window(symbol, list(_br_w.index))
                            if len(_br_w) != _br_n or _br_r is None or _br_r[2] != _br_n:
                                break
                            _br_b, _br_a = float(_br_r[0]), float(_br_r[1])
                            _br_total = _br_b + _br_a
                            if not all(math.isfinite(x) and x >= 0.0 for x in (_br_b, _br_a, _br_total)) or _br_total <= 0.0:
                                break
                            _br_values.append(_br_b / _br_total)
                        if len(_br_values) == 3:
                            _br_real = tuple(_br_values)
                            if BuyFlowBook.enabled:
                                buying_ratio_full, buying_ratio_recent, previous_buying_ratio = _br_real
                                _br_source = 'trade'
                except Exception:
                    buying_ratio_full, buying_ratio_recent, previous_buying_ratio = _br_candle
                    _br_real = None
                    _br_source = 'candle'
                
                # 가중 평균 (최신 강조: 70% 최근 + 30% 전체)
                buying_ratio = buying_ratio_recent * 0.7 + buying_ratio_full * 0.3
                
                # 급격한 전환 감지
                ratio_change = abs(buying_ratio_recent - previous_buying_ratio)
                is_sudden_change = ratio_change > 0.2
                
                is_buying_to_selling = previous_buying_ratio > 0.6 and buying_ratio_recent < 0.4
                is_selling_to_buying = previous_buying_ratio < 0.4 and buying_ratio_recent > 0.6
                
                # 조정 계수 계산
                base_adjustment = buying_ratio * 2  # 0.0 ~ 2.0
                
                if is_buying_to_selling:
                    adjustment_factor = base_adjustment * 0.5  # 급격한 매도 전환: 절반!
                elif is_selling_to_buying:
                    adjustment_factor = base_adjustment * 1.3  # 급격한 매수 전환: 1.3배!
                else:
                    adjustment_factor = base_adjustment
                
                # 조정된 거래량 비율 계산
                adj_ratio = raw_volume_ratio * adjustment_factor
                
                # 🔥 거래량 둔화 우선 반영 (NEAR / NXPC 대응)
                # - 최근 실질 체감 거래량이 0.8x 미만이면, 과도한 상향을 제한
                if volume_ma_value > 0:
                    recent_volume = df['volume'].tail(5).mean() if len(df) >= 5 else df['volume'].mean()
                    recent_vol_ratio = recent_volume / volume_ma_value
                    
                    if recent_vol_ratio < 0.8:
                        # 과대평가 방지: 조정 비율을 1.2x 이상으로 끌어올리지 않음
                        if adj_ratio > 1.2:
                            logger.debug(
                                f"⚠️ [Volume-Cap] {symbol} 최근 거래량 둔화 "
                                f"(recent={recent_vol_ratio:.2f}x) → "
                                f"조정비율 {adj_ratio:.2f}x → 1.20x 상한 적용"
                            )
                            adj_ratio = 1.2
                
                indicators['volume_ratio'] = adj_ratio
                indicators['raw_volume_ratio'] = raw_volume_ratio
                indicators['buying_ratio'] = buying_ratio
                indicators['recent_buying_ratio'] = buying_ratio_recent
                indicators['previous_buying_ratio'] = previous_buying_ratio
                # 🔧 [BUYRATIO-TRADE v2 2026-10-30] 출처 · 대리값 · 실제 체결 수량 비율 병기 (candle 기본에서도 기록)
                indicators['buying_ratio_source'] = _br_source
                indicators['buying_ratio_candle'] = _br_candle[1] * 0.7 + _br_candle[0] * 0.3
                indicators['buying_ratio_trade'] = (
                    _br_real[1] * 0.7 + _br_real[0] * 0.3 if _br_real is not None else None
                )
                indicators['recent_buying_ratio_trade'] = _br_real[1] if _br_real is not None else None
                indicators['previous_buying_ratio_trade'] = _br_real[2] if _br_real is not None else None
                indicators['buying_ratio_trade_valid'] = _br_real is not None
                
                # 압력 방향 판단
                if is_buying_to_selling:
                    indicators['volume_pressure'] = 'SUDDEN_SELLING'
                elif is_selling_to_buying:
                    indicators['volume_pressure'] = 'SUDDEN_BUYING'
                elif buying_ratio >= 0.65:
                    indicators['volume_pressure'] = 'STRONG_BUYING'
                elif buying_ratio >= 0.55:
                    indicators['volume_pressure'] = 'BUYING'
                # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
                # 🔥 PATCH PRESSURE-FIX: 매도압력 분류 완화
                # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
                # 개선: buying_ratio 40~44% → NEUTRAL (기존: SELLING)
                # 근거: 매수 40%는 중립 구간, SELLING 차단/감점 부적절
                # 효과: 억울한 SELLING 차단 제거, 진입 기회 증가
                # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
                elif buying_ratio >= 0.40:
                    indicators['volume_pressure'] = 'NEUTRAL'
                elif buying_ratio >= 0.30:
                    indicators['volume_pressure'] = 'SELLING'
                else:
                    indicators['volume_pressure'] = 'STRONG_SELLING'
                
                # 압력 강도
                if buying_ratio >= 0.5:
                    indicators['pressure_strength'] = (buying_ratio - 0.5) * 2
                else:
                    indicators['pressure_strength'] = (0.5 - buying_ratio) * 2
                
                indicators['is_sudden_change'] = is_sudden_change
                
                # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
                # ✅ SSoT: buying_pressure + volume_signal 주입
                # - buying_pressure: 매수 압력(0~1, 매도는 0 처리)
                # - volume_signal  : Strategy가 그대로 쓰는 거래량 축(0~1)
                # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
                buying_pressure = max(0.0, (buying_ratio - 0.5) * 2.0)  # 0.0 ~ 1.0
                indicators['buying_pressure'] = float(buying_pressure)
                
                vol_ratio_for_signal = float(indicators.get('volume_ratio', 0.0) or 0.0)
                if vol_ratio_for_signal >= 1.0:
                    volume_signal = 1.0
                elif vol_ratio_for_signal >= 0.7:
                    volume_signal = 0.7
                elif vol_ratio_for_signal >= 0.4:
                    volume_signal = 0.4
                else:
                    volume_signal = 0.0
                
                indicators['volume_signal'] = float(volume_signal)
                
            else:
                # 데이터 부족 시 기존 방식
                indicators['volume_ratio'] = raw_volume_ratio
                indicators['raw_volume_ratio'] = raw_volume_ratio
                indicators['buying_ratio'] = 0.5
                indicators['recent_buying_ratio'] = 0.5
                indicators['previous_buying_ratio'] = 0.5
                indicators['volume_pressure'] = 'NEUTRAL'
                indicators['pressure_strength'] = 0
                indicators['is_sudden_change'] = False
        else:
            # 🔒 [VOLUME-DATA-QUALITY-0823]
            # 앞단 density floor가 VOLUME_MA_PERIOD보다 커 길이 부족 fallback은 도달하지 않는다.
            # 여기서는 volume 컬럼 누락/미산출을 정상 0.0x가 아닌 SENTINEL로 전달한다.
            _vol_df_len = len(df) if df is not None else 0
            _vol_has_column = bool(df is not None and 'volume' in df.columns)
            logger.debug(
                f"⚠️ [지표계산] 거래량 계산 불가 - "
                f"volume컬럼: {_vol_has_column}, 캔들: {_vol_df_len}/{VOLUME_MA_PERIOD}"
            )
            indicators['volume_ratio'] = SENTINEL_FLOAT
            indicators['raw_volume_ratio'] = SENTINEL_FLOAT
            indicators['volume_ma'] = SENTINEL_FLOAT
            indicators['buying_ratio'] = 0.5
            indicators['recent_buying_ratio'] = 0.5
            indicators['previous_buying_ratio'] = 0.5
            indicators['volume_pressure'] = 'NEUTRAL'
            indicators['pressure_strength'] = 0
            indicators['is_sudden_change'] = False
        
        # 변동성 (ATR 기반)
        if len(df) >= VOLUME_MA_PERIOD:
            indicators['volatility'] = df['close'].pct_change().rolling(window=VOLUME_MA_PERIOD).std()
        else:
            indicators['volatility'] = DEFAULT_VOLATILITY_VALUE
        
        # 모멘텀 (가격 변화율)
        if len(df) >= DEFAULT_ANALYSIS_WINDOW:
            indicators['momentum'] = (df['close'].iloc[LAST_INDEX] - df['close'].iloc[-DEFAULT_ANALYSIS_WINDOW]) / df['close'].iloc[-DEFAULT_ANALYSIS_WINDOW]
        else:
            indicators['momentum'] = 0
        
        # 노이즈 필터 (최근 변동성)
        if len(df) >= 5:
            recent_changes = df['close'].pct_change().tail(5)
            indicators['noise_filter'] = recent_changes.std()
        else:
            indicators['noise_filter'] = DEFAULT_SCORE
        
        # 🔒 [ENTRY-FIXED-BOX-PURGE-0828-R2]
        # fixed BOX producer는 철거 완료. 위치 SSoT는 Active TLC다.

        # ============================================================
        # 🆕 [INDICATOR-SHADOW] 확정봉 BB(20,2) + RSI(14) + MACD(12,26,9)
        # ============================================================
        # 목적: symbol + bar + px를 재현 가능한 조인키로 남기고,
        #       RSI와 RSI/MACD 다이버전스는 판사2,
        #       BB·KC·Cycle Phase는 판사3 입력으로 제공한다.
        # 원칙: 현재 진행봉(iloc[-1])은 전부 제외한다.
        #       confidence 5신호는 변경하지 않는다.
        # ============================================================
        indicators.setdefault("bollinger_mid", None)
        indicators.setdefault("bollinger_upper", None)
        indicators.setdefault("bollinger_lower", None)
        indicators.setdefault("bollinger_percent_b", None)
        indicators.setdefault("bollinger_percent_b_raw", None)
        indicators.setdefault("bollinger_bandwidth_pct", None)
        indicators.setdefault("bollinger_upper_breakout", False)
        indicators.setdefault("bollinger_lower_breakdown", False)
        indicators.setdefault("keltner_mid", None)
        indicators.setdefault("keltner_upper", None)
        indicators.setdefault("keltner_lower", None)
        indicators.setdefault("keltner_atr20", None)
        indicators.setdefault("keltner_squeeze_ratio", SENTINEL_FLOAT)
        indicators.setdefault("keltner_squeeze_ready", False)
        indicators.setdefault("keltner_squeeze_on", False)
        indicators.setdefault("keltner_had_squeeze", False)
        indicators.setdefault("keltner_squeeze_ttl", 0)
        indicators.setdefault("rsi14", None)
        indicators.setdefault("macd_line", None)
        indicators.setdefault("macd_signal", None)
        indicators.setdefault("macd_hist", None)
        indicators.setdefault("macd_line_pct", None)
        indicators.setdefault("macd_hist_pct", None)
        indicators.setdefault("macd_zero_side", None)
        indicators.setdefault("indicator_shadow_bar", None)
        indicators.setdefault("indicator_shadow_px", None)

        try:
            _bb_win = 20
            _bb_mult = 2.0
            _kc_win = 20
            _kc_mult = 1.5
            _rsi_period = 14
            _macd_fast = 12
            _macd_slow = 26
            _macd_signal_period = 9
            _shadow_min_bars = _macd_slow + _macd_signal_period

            if df is None or len(df) < (_shadow_min_bars + 1):
                raise ValueError(
                    f"CLOSED_DATA_SHORT({0 if df is None else len(df)}"
                    f"<{_shadow_min_bars + 1})"
                )

            # 현재 진행봉 iloc[-1] 제외.
            # 모든 지표·bar·px를 직전 확정봉 iloc[-2]에 고정한다.
            _shadow_close = (
                df["close"]
                .astype(float)
                .iloc[:-1]
                .replace([np.inf, -np.inf], np.nan)
                .dropna()
            )

            if len(_shadow_close) < _shadow_min_bars:
                raise ValueError(
                    f"CLOSED_VALID_SHORT({len(_shadow_close)}"
                    f"<{_shadow_min_bars})"
                )

            _shadow_px = float(df["close"].astype(float).iloc[-2])

            if not math.isfinite(_shadow_px) or _shadow_px <= 0.0:
                raise ValueError(f"CLOSED_PX_INVALID({_shadow_px})")

            try:
                _shadow_bar = df.index[-2].isoformat()
            except Exception:
                _shadow_bar = str(df.index[-2])

            # 1) Bollinger Bands(20,2) — 확정봉 종가 기준
            _bb_close = _shadow_close.tail(_bb_win)
            _bb_ma = float(_bb_close.mean())
            _bb_sd = float(_bb_close.std(ddof=0))

            if not (
                math.isfinite(_bb_ma)
                and math.isfinite(_bb_sd)
                and _bb_ma > 0.0
                and _bb_sd > 0.0
            ):
                raise ValueError(
                    f"BB_INVALID(ma={_bb_ma},sd={_bb_sd})"
                )

            _bb_support = _bb_ma - (_bb_mult * _bb_sd)
            _bb_resistance = _bb_ma + (_bb_mult * _bb_sd)

            if not (
                _bb_support > 0.0
                and _bb_resistance > _bb_support
            ):
                raise ValueError(
                    f"BB_RANGE_INVALID("
                    f"lo={_bb_support},hi={_bb_resistance})"
                )

            _bb_raw_pos = (
                _shadow_px - _bb_support
            ) / (
                _bb_resistance - _bb_support
            )

            _bb_clamped_pos = max(
                0.0,
                min(1.0, _bb_raw_pos),
            )

            _bb_bandwidth_pct = (
                (_bb_resistance - _bb_support)
                / _bb_ma
                * 100.0
            )

            # 2) Keltner Channel(EMA20, ATR20×1.5) — BB 안쪽 수렴 판정
            # KC 실패가 기존 BB·RSI·MACD 판사를 죽이지 않도록 독립 fail-open 처리한다.
            _kc_mid = SENTINEL_FLOAT
            _kc_upper = SENTINEL_FLOAT
            _kc_lower = SENTINEL_FLOAT
            _kc_atr20 = SENTINEL_FLOAT
            _kc_squeeze_ratio = SENTINEL_FLOAT
            _kc_squeeze_ready = False
            _kc_squeeze_on = False
            try:
                _shadow_high = (
                    df["high"].astype(float).iloc[:-1].reindex(_shadow_close.index)
                )
                _shadow_low = (
                    df["low"].astype(float).iloc[:-1].reindex(_shadow_close.index)
                )
                _shadow_prev_close = _shadow_close.shift(1)
                _kc_tr = pd.concat(
                    [
                        _shadow_high - _shadow_low,
                        (_shadow_high - _shadow_prev_close).abs(),
                        (_shadow_low - _shadow_prev_close).abs(),
                    ],
                    axis=1,
                ).max(axis=1)
                _kc_atr_series = _kc_tr.rolling(
                    window=_kc_win,
                    min_periods=_kc_win,
                ).mean()
                _kc_mid_series = _shadow_close.ewm(
                    span=_kc_win,
                    adjust=False,
                    min_periods=_kc_win,
                ).mean()
                _kc_mid = float(_kc_mid_series.iloc[-1])
                _kc_atr20 = float(_kc_atr_series.iloc[-1])
                _kc_upper = _kc_mid + (_kc_mult * _kc_atr20)
                _kc_lower = _kc_mid - (_kc_mult * _kc_atr20)
                _kc_width = _kc_upper - _kc_lower
                _bb_width = _bb_resistance - _bb_support

                _kc_squeeze_ready = bool(
                    all(
                        math.isfinite(_v)
                        for _v in (
                            _kc_mid,
                            _kc_atr20,
                            _kc_upper,
                            _kc_lower,
                            _kc_width,
                        )
                    )
                    and _kc_mid > 0.0
                    and _kc_atr20 > 0.0
                    and _kc_lower > 0.0
                    and _kc_width > 0.0
                )
                if not _kc_squeeze_ready:
                    raise ValueError(
                        f"KC_INVALID(mid={_kc_mid},atr20={_kc_atr20},"
                        f"lo={_kc_lower},hi={_kc_upper})"
                    )

                _kc_squeeze_ratio = _bb_width / _kc_width
                _kc_squeeze_on = bool(
                    _bb_resistance <= _kc_upper
                    and _bb_support >= _kc_lower
                )

            except Exception as _kc_e:
                _kc_squeeze_ready = False
                _kc_squeeze_on = False
                _kc_squeeze_ratio = SENTINEL_FLOAT
                logger.debug(
                    f"⚠️ [KC-SQUEEZE] {symbol} 계산 실패 → coil 비활성 | "
                    f"reason={_kc_e}"
                )

            # 3) RSI(14) — Wilder 초기 SMA + 재귀 평활
            _rsi_delta = _shadow_close.diff().dropna()
            _rsi_gain = (
                _rsi_delta
                .clip(lower=0.0)
                .to_numpy(dtype=float)
            )
            _rsi_loss = (
                -_rsi_delta
                .clip(upper=0.0)
            ).to_numpy(dtype=float)

            if len(_rsi_gain) < _rsi_period:
                raise ValueError(
                    f"RSI_DATA_SHORT("
                    f"{len(_rsi_gain)}<{_rsi_period})"
                )

            _avg_gain = float(
                np.mean(_rsi_gain[:_rsi_period])
            )
            _avg_loss = float(
                np.mean(_rsi_loss[:_rsi_period])
            )

            # 다이버전스 피벗 비교용 RSI 전체 확정봉 시계열.
            # 기존 최종 RSI와 동일한 Wilder seed/재귀를 그대로 사용한다.
            _rsi_values = np.full(len(_shadow_close), np.nan, dtype=float)

            def _wilder_rsi_value(_gain, _loss):
                if _loss <= EPSILON:
                    return 100.0 if _gain > EPSILON else 50.0
                _rs = _gain / _loss
                return 100.0 - (100.0 / (1.0 + _rs))

            _rsi_values[_rsi_period] = _wilder_rsi_value(
                _avg_gain,
                _avg_loss,
            )

            for _rsi_i in range(
                _rsi_period,
                len(_rsi_gain),
            ):
                _avg_gain = (
                    (
                        _avg_gain
                        * (_rsi_period - 1)
                    )
                    + float(_rsi_gain[_rsi_i])
                ) / _rsi_period

                _avg_loss = (
                    (
                        _avg_loss
                        * (_rsi_period - 1)
                    )
                    + float(_rsi_loss[_rsi_i])
                ) / _rsi_period
                _rsi_values[_rsi_i + 1] = _wilder_rsi_value(
                    _avg_gain,
                    _avg_loss,
                )

            _rsi_series = pd.Series(
                _rsi_values,
                index=_shadow_close.index,
                dtype=float,
            )
            _rsi14 = float(_rsi_series.iloc[-1])

            # 4) MACD(12,26,9) — 확정봉 종가 EMA
            _ema_fast = _shadow_close.ewm(
                span=_macd_fast,
                adjust=False,
                min_periods=_macd_fast,
            ).mean()

            _ema_slow = _shadow_close.ewm(
                span=_macd_slow,
                adjust=False,
                min_periods=_macd_slow,
            ).mean()

            _macd_series = _ema_fast - _ema_slow

            _macd_signal_series = _macd_series.ewm(
                span=_macd_signal_period,
                adjust=False,
                min_periods=_macd_signal_period,
            ).mean()

            _macd_hist_series = (
                _macd_series
                - _macd_signal_series
            )

            _macd_line = float(
                _macd_series.iloc[-1]
            )
            _macd_signal = float(
                _macd_signal_series.iloc[-1]
            )
            _macd_hist = float(
                _macd_hist_series.iloc[-1]
            )
            # 🔒 [MACD-ABCD-0822] 확정봉 MACD 결합 4방법.
            # A만 J2 판정에 사용하고 B/C/D는 관측 전용이다.
            _macd_prev = float("nan")
            _macd_signal_prev = float("nan")
            _macd_hist_prev = float("nan")
            if len(_macd_hist_series) >= 2:
                _macd_prev = float(_macd_series.iloc[-2])
                _macd_signal_prev = float(_macd_signal_series.iloc[-2])
                _macd_hist_prev = float(_macd_hist_series.iloc[-2])
            # A: MACD 0선 위.
            _macd_a_above_zero = bool(
                _macd_line > 0.0
            )
            # B: 음(-) histogram 상태가 유지된 채 절대값이 축소되는가.
            # ★ prev_hist<0 필수 — 양→음 전환을 "음봉 수축"으로 오분류하지 않는다.
            _macd_b_hist_contracting = bool(
                math.isfinite(_macd_hist_prev)
                and _macd_hist_prev < 0.0
                and _macd_hist < 0.0
                and abs(_macd_hist) < abs(_macd_hist_prev)
            )
            # C/D: MACD 상향 signal 교차 == histogram 음→양 전환.
            _macd_cd_cross_up = bool(
                math.isfinite(_macd_prev)
                and math.isfinite(_macd_signal_prev)
                and _macd_line > _macd_signal
                and _macd_prev <= _macd_signal_prev
            )

            if not all(
                math.isfinite(_v)
                for _v in (
                    _rsi14,
                    _macd_line,
                    _macd_signal,
                    _macd_hist,
                )
            ):
                raise ValueError(
                    f"INDICATOR_INVALID("
                    f"rsi={_rsi14},"
                    f"macd={_macd_line},"
                    f"signal={_macd_signal},"
                    f"hist={_macd_hist})"
                )

            _macd_line_pct = (
                _macd_line
                / _shadow_px
                * 100.0
            )
            _macd_hist_pct = (
                _macd_hist
                / _shadow_px
                * 100.0
            )

            _macd_zero_side = (
                "ABOVE"
                if _macd_line > 0.0
                else "BELOW"
                if _macd_line < 0.0
                else "ZERO"
            )

            indicators["bollinger_mid"] = _bb_ma
            indicators["bollinger_upper"] = _bb_resistance
            indicators["bollinger_lower"] = _bb_support
            indicators["bollinger_percent_b"] = _bb_clamped_pos
            indicators["bollinger_percent_b_raw"] = _bb_raw_pos
            indicators["bollinger_bandwidth_pct"] = _bb_bandwidth_pct
            indicators["bollinger_upper_breakout"] = bool(
                _bb_raw_pos > 1.0
            )
            indicators["bollinger_lower_breakdown"] = bool(
                _bb_raw_pos < 0.0
            )
            indicators["keltner_mid"] = _kc_mid
            indicators["keltner_upper"] = _kc_upper
            indicators["keltner_lower"] = _kc_lower
            indicators["keltner_atr20"] = _kc_atr20
            indicators["keltner_squeeze_ratio"] = float(_kc_squeeze_ratio)
            indicators["keltner_squeeze_ready"] = bool(_kc_squeeze_ready)
            indicators["keltner_squeeze_on"] = bool(_kc_squeeze_on)

            # KC squeeze had_squeeze — 목격 시 확정봉 5개(TTL) 동안 유효.
            # 확정봉키(_shadow_bar)로 스캔 중복(동일봉 재호출) 시 카운트다운
            # 이중 소모를 방지한다. calculate_coil_setup 자체는 무변경.
            _KELTNER_HAD_SQUEEZE_TTL_BARS = 5
            _had_sq_tf = (
                timeframe.value
                if hasattr(timeframe, "value")
                else str(timeframe)
            )
            _had_sq_key = (_had_sq_tf, symbol)
            _had_sq_state = TechnicalIndicators._keltner_squeeze_ttl_state
            _prev_had_sq = _had_sq_state.get(_had_sq_key, {})
            _prev_had_sq_ttl = int(_prev_had_sq.get("ttl", 0) or 0)
            if _prev_had_sq.get("bar") != _shadow_bar:
                _had_sq_ttl = (
                    _KELTNER_HAD_SQUEEZE_TTL_BARS
                    if (_kc_squeeze_ready and _kc_squeeze_on)
                    else max(0, _prev_had_sq_ttl - 1)
                )
                _had_sq_state[_had_sq_key] = {
                    "bar": _shadow_bar,
                    "ttl": _had_sq_ttl,
                }
            else:
                _had_sq_ttl = _prev_had_sq_ttl
            indicators["keltner_had_squeeze"] = bool(_had_sq_ttl > 0)
            indicators["keltner_squeeze_ttl"] = int(_had_sq_ttl)

            indicators["rsi14"] = float(_rsi14)
            indicators["macd_line"] = _macd_line
            indicators["macd_signal"] = _macd_signal
            indicators["macd_hist"] = _macd_hist
            indicators["macd_line_pct"] = _macd_line_pct
            indicators["macd_hist_pct"] = _macd_hist_pct
            indicators["macd_zero_side"] = _macd_zero_side
            # 🔒 [MACD-ABCD-0822] A만 J2 판정 소비, B/C/D는 관측 전용.
            indicators["macd_a_above_zero"] = _macd_a_above_zero
            indicators["macd_b_hist_contracting"] = _macd_b_hist_contracting
            indicators["macd_c_golden_cross"] = _macd_cd_cross_up
            indicators["macd_d_hist_flip_up"] = _macd_cd_cross_up
            indicators["macd_prev"] = _macd_prev
            indicators["macd_signal_prev"] = _macd_signal_prev
            indicators["macd_hist_prev"] = _macd_hist_prev
            indicators["indicator_shadow_bar"] = _shadow_bar
            indicators["indicator_shadow_px"] = _shadow_px

            # 같은 심볼·TF·확정봉은 한 번만 INFO 로그로 적재한다.
            _shadow_tf = (
                timeframe.value
                if hasattr(timeframe, "value")
                else str(timeframe)
            )
            _shadow_log_key = (
                _shadow_tf,
                symbol,
            )
            _shadow_seen = getattr(
                TechnicalIndicators,
                "_indicator_shadow_logged_bar",
                {},
            )

            if _shadow_seen.get(
                _shadow_log_key
            ) != _shadow_bar:
                _shadow_seen[
                    _shadow_log_key
                ] = _shadow_bar

                setattr(
                    TechnicalIndicators,
                    "_indicator_shadow_logged_bar",
                    _shadow_seen,
                )

        except Exception as e:
            logger.info(
                f"⚠️ [INDICATOR-SHADOW] "
                f"{symbol} 계산 실패 | "
                f"tf="
                f"{timeframe.value if hasattr(timeframe, 'value') else timeframe} | "
                f"reason={e}"
            )

        # ============================================================
        # 🔬 [DIVERGENCE-SHADOW] 확정봉 다이버전스 피벗 계산
        # ============================================================
        # fractal k=2·최근 35봉 · 현재봉 제외.
        # hidden bullish → J2 / regular bullish → J4 /
        # regular bearish → J2 veto 문맥에서 소비한다.
        indicators.setdefault("divergence_rsi_bear_reg", False)
        indicators.setdefault("divergence_macd_bear_reg", False)
        indicators.setdefault("divergence_rsi_bull_hid", False)
        indicators.setdefault("divergence_macd_bull_hid", False)
        indicators.setdefault("divergence_rsi_bull_reg", False)
        indicators.setdefault("divergence_macd_bull_reg", False)
        indicators.setdefault("div_regular_bull", False)
        indicators.setdefault("div_regular_bull_coherent", False)
        indicators.setdefault("div_low_rsi", float("nan"))
        # 🔒 [J4-REG-PIVOT-OBSERVE-0902-R1] ★485-R1
        # J4 regular raw 근거·신선도 관측 전용. 판정 소비 0곳.
        indicators.setdefault("divergence_reg_price1", SENTINEL_FLOAT)
        indicators.setdefault("divergence_reg_price2", SENTINEL_FLOAT)
        indicators.setdefault("divergence_reg_rsi1", SENTINEL_FLOAT)
        indicators.setdefault("divergence_reg_rsi2", SENTINEL_FLOAT)
        indicators.setdefault("divergence_reg_macd1", SENTINEL_FLOAT)
        indicators.setdefault("divergence_reg_macd2", SENTINEL_FLOAT)
        indicators.setdefault("divergence_reg_pivot_distance", -1)
        indicators.setdefault("divergence_reg_pivot_age", -1)
        indicators.setdefault("divergence_reg_signal_age", -1)
        indicators.setdefault("divergence_reg_pivot_bar", "NA")
        indicators.setdefault("divergence_reg_confirm_bar", "NA")
        indicators.setdefault("div_j2_hidden_coherent", False)
        indicators.setdefault("divergence_hidden_bull", False)
        indicators.setdefault("divergence_bear_reg_valid", False)
        indicators.setdefault("divergence_price_delta_pct", 0.0)
        indicators.setdefault("divergence_pivot_distance", 0)

        # 🔒 [J2-PIVOT-AGE-OBSERVE-0828]
        # J2 hidden 저점쌍 관측 SSoT. 판정 소비 0곳.
        indicators.setdefault("divergence_pivot_age", -1)
        indicators.setdefault("divergence_signal_age", -1)
        indicators.setdefault("divergence_pivot_bar", "NA")
        indicators.setdefault("divergence_confirm_bar", "NA")
        indicators.setdefault(
            "divergence_hidden_price1",
            SENTINEL_FLOAT,
        )
        indicators.setdefault(
            "divergence_hidden_price2",
            SENTINEL_FLOAT,
        )
        indicators.setdefault(
            "divergence_hidden_rsi1",
            SENTINEL_FLOAT,
        )
        indicators.setdefault(
            "divergence_hidden_rsi2",
            SENTINEL_FLOAT,
        )
        indicators.setdefault(
            "divergence_hidden_macd1",
            SENTINEL_FLOAT,
        )
        indicators.setdefault(
            "divergence_hidden_macd2",
            SENTINEL_FLOAT,
        )

        indicators.setdefault("_divergence_shadow_ready", False)

        try:
            _div_k = 2
            _div_lookback = 35
            if (
                df is None
                or not {"high", "low", "close"}.issubset(df.columns)
                or "_shadow_close" not in locals()
                or "_rsi_series" not in locals()
                or "_macd_series" not in locals()
            ):
                raise ValueError("DIVERGENCE_INPUT_MISSING")

            _div_close = _shadow_close.astype(float)
            _div_bb_mid = _div_close.rolling(_bb_win).mean()
            _div_bb_sd = _div_close.rolling(_bb_win).std(ddof=0)
            _div_bb_raw = (
                (_div_close - (_div_bb_mid - (_bb_mult * _div_bb_sd)))
                / ((_bb_mult * 2.0 * _div_bb_sd).replace(0.0, np.nan))
            )
            _div_frame = pd.DataFrame(
                {
                    "high": (
                        df["high"]
                        .astype(float)
                        .iloc[:-1]
                        .reindex(_div_close.index)
                    ),
                    "low": (
                        df["low"]
                        .astype(float)
                        .iloc[:-1]
                        .reindex(_div_close.index)
                    ),
                    "rsi": _rsi_series.reindex(_div_close.index),
                    # MACD line을 가격 대비 %로 정규화해 종목 간 크기 차이를 제거한다.
                    "macd": (
                        _macd_series.reindex(_div_close.index)
                        / _div_close
                        * 100.0
                    ),
                    "bb_raw": _div_bb_raw,
                }
            ).replace([np.inf, -np.inf], np.nan).tail(_div_lookback)

            def _fractal_positions(_column, _mode):
                return _confirmed_fractal_positions(
                    _div_frame[_column].to_numpy(dtype=float),
                    _mode,
                    _div_k,
                )

            # 🔒 [DIV-QUALITY-ATR-FLOOR-0829-R1] ★415
            # 하한이 거래소 호가단위(1틱)뿐이라 고가 종목(BTC 틱비율
            # 0.0009%)은 저가 종목(FOLD 0.82%)보다 882배 느슨했다(실측
            # 08-29 114종목 5,230회: 틱/가격 5분위별 reg=1 발생률 10.5%→
            # 2.3% 완전 단조 4.6배, 호가계단 자연실험 z=5.17 유의).
            # 🔒 [R1 정정] _price_quality()는 _bear_quality(regular bearish,
            # J2 VETO 소비)와 _hidden_quality(J2 hidden) 공용 함수다.
            # 여기에 하한을 직접 걸면 bearish divergence가 사라지는 쪽으로도
            # 작동해 not _j2_bear_raw가 True로 뒤집혀 J2/J4가 새로 PASS할
            # 수 있다(ChatGPT 지적) — "강화만"이 시스템 수준에서 깨진다.
            # _price_quality 자체는 원복하고, hidden/regular(J4) 두 소비처
            # 에만 개별적으로 ATR 하한을 추가 검사한다. bearish는 무영향.
            _DIV_PIVOT_ATR_FLOOR_MULT = 0.20
            # 🔒 [DIV-ATR-FLOOR-PIVOT-ANCHOR-0829] ★419-R1
            # divergence 확정창과 같은 최근 최대 35개 확정 ATR의 중앙값을 사용한다.
            # 5개 이상이면 중앙값, 그 미만에서만 기존 직전 확정봉 ATR로 폴백한다.
            try:
                _div_atr_series = indicators.get("atr")
                _div_atr_win = (
                    _div_atr_series.iloc[:-1].tail(_div_lookback)
                    if hasattr(_div_atr_series, "iloc")
                    and len(_div_atr_series) >= 2
                    else None
                )
                if _div_atr_win is not None and len(_div_atr_win) >= 5:
                    _div_atr_raw = float(_div_atr_win.median())
                elif (
                    hasattr(_div_atr_series, "iloc")
                    and len(_div_atr_series) >= 2
                ):
                    _div_atr_raw = float(_div_atr_series.iloc[-2])
                else:
                    _div_atr_raw = 0.0
            except Exception:
                _div_atr_raw = 0.0
            _div_atr_floor = float(_div_atr_raw or 0.0) * _DIV_PIVOT_ATR_FLOOR_MULT
            indicators["divergence_atr_floor"] = _div_atr_floor

            def _price_quality(_price1, _price2, _distance):
                _tick = max(
                    float(get_tick_size(_price1) or 0.0),
                    float(get_tick_size(_price2) or 0.0),
                )
                return bool(
                    _price2 > _price1
                    and (_price2 - _price1) >= _tick
                    and _distance >= (_div_k + 1)
                )

            def _pivot_gap_atr_ok(_price1, _price2):
                # hidden/regular(J4) 전용 추가 검사. bearish는 이 함수를
                # 소비하지 않으므로 완전히 무관하다.
                return bool(abs(_price2 - _price1) >= _div_atr_floor)

            _rsi_bear_reg = _macd_bear_reg = False
            _rsi_bull_hid = _macd_bull_hid = False
            _rsi_bull_reg = _macd_bull_reg = False
            _j2_hidden_coherent = False
            _div_low_rsi = float("nan")
            _bear_valid_raw = _hidden_raw = False
            _bear_pd = _hidden_pd = 0.0
            _bear_dist = _hidden_dist = 0
            # 🔒 [J2-PIVOT-AGE-OBSERVE-0828] 저점쌍 미형성 시 기본값
            _hidden_pivot_age = -1
            _hidden_signal_age = -1
            _hidden_pivot_bar = "NA"
            _hidden_confirm_bar = "NA"

            _highs = _fractal_positions("high", "HIGH")
            if len(_highs) >= 2:
                _h1, _h2 = _highs[-2], _highs[-1]
                _ph1 = float(_div_frame["high"].iloc[_h1])
                _ph2 = float(_div_frame["high"].iloc[_h2])
                _rh1 = float(_div_frame["rsi"].iloc[_h1])
                _rh2 = float(_div_frame["rsi"].iloc[_h2])
                _mh1 = float(_div_frame["macd"].iloc[_h1])
                _mh2 = float(_div_frame["macd"].iloc[_h2])
                _bh1 = float(_div_frame["bb_raw"].iloc[_h1])
                _bh2 = float(_div_frame["bb_raw"].iloc[_h2])
                if (
                    all(
                        math.isfinite(_v)
                        for _v in (
                            _ph1,
                            _ph2,
                            _rh1,
                            _rh2,
                            _mh1,
                            _mh2,
                        )
                    )
                    and _ph1 > 0.0
                ):
                    _bear_dist = int(_h2 - _h1)
                    _bear_pd = ((_ph2 - _ph1) / _ph1) * 100.0
                    _bear_quality = _price_quality(
                        _ph1,
                        _ph2,
                        _bear_dist,
                    )
                    _rsi_bear_reg = bool(
                        _bear_quality and _rh2 < _rh1
                    )
                    _macd_bear_reg = bool(
                        _bear_quality and _mh2 < _mh1
                    )
                    # 일반 약세 다이버전스는 상단 근접 과열권에서만 valid.
                    # BB분기는 자체 유한성 가드 — _bh1/_bh2 NaN이어도 RSI 분기는 살아있다.
                    _bear_extreme = bool(
                        max(_rh1, _rh2) >= 70.0
                        or (
                            math.isfinite(_bh1)
                            and math.isfinite(_bh2)
                            and max(_bh1, _bh2) >= 0.95
                        )
                    )
                    _bear_valid_raw = bool(
                        (_rsi_bear_reg or _macd_bear_reg)
                        and _bear_extreme
                    )

            _lows = _fractal_positions("low", "LOW")
            if len(_lows) >= 2:
                _l1, _l2 = _lows[-2], _lows[-1]
                _pl1 = float(_div_frame["low"].iloc[_l1])
                _pl2 = float(_div_frame["low"].iloc[_l2])
                _rl1 = float(_div_frame["rsi"].iloc[_l1])
                _rl2 = float(_div_frame["rsi"].iloc[_l2])
                _ml1 = float(_div_frame["macd"].iloc[_l1])
                _ml2 = float(_div_frame["macd"].iloc[_l2])
                if (
                    all(
                        math.isfinite(_v)
                        for _v in (_pl1, _pl2, _rl1, _rl2, _ml1, _ml2)
                    )
                    and _pl1 > 0.0
                ):
                    _hidden_dist = int(_l2 - _l1)

                    # 🔒 [J2-PIVOT-AGE-OBSERVE-0828]
                    # pivot_age  : 저점2 → 현재 확정봉 경과봉수.
                    # signal_age : k=2 우측확정이 끝난 뒤
                    #              실제 J2가 이 pivot을 알 수 있게 된 후 경과봉수.
                    # 매 체크마다 재계산하므로 TTL이 아니라
                    # "현재 저점쌍 신호의 경과"를 관측한다.
                    _hidden_pivot_age = int(
                        len(_div_frame) - 1 - _l2
                    )
                    _hidden_signal_age = max(
                        0,
                        _hidden_pivot_age - _div_k,
                    )

                    try:
                        _hidden_pivot_bar = str(
                            _div_frame.index[_l2]
                        )[:16]

                        _hidden_confirm_i = _l2 + _div_k
                        _hidden_confirm_bar = (
                            str(
                                _div_frame.index[_hidden_confirm_i]
                            )[:16]
                            if _hidden_confirm_i < len(_div_frame)
                            else "NA"
                        )
                    except Exception:
                        _hidden_pivot_bar = "NA"
                        _hidden_confirm_bar = "NA"

                    _hidden_pd = (
                        ((_pl2 - _pl1) / _pl1) * 100.0
                    )
                    _hidden_quality = bool(
                        _price_quality(
                            _pl1,
                            _pl2,
                            _hidden_dist,
                        )
                        and _pivot_gap_atr_ok(_pl1, _pl2)
                    )
                    _rsi_bull_hid = bool(
                        _hidden_quality and _rl2 < _rl1
                    )
                    _macd_bull_hid = bool(
                        _hidden_quality and _ml2 < _ml1
                    )
                    _hidden_raw = bool(
                        _rsi_bull_hid or _macd_bull_hid
                    )
                    # 🔒 [J2-HIDDEN-OSC-COHERENCE-0904] ★504-R1
                    # hidden bullish = price HL + oscillator LL.
                    # 한 oscillator가 hidden이어도 다른 쪽이 반대로 상승하면
                    # 선명한 hidden으로 보지 않는다. 동률은 허용한다.
                    _j2_hidden_coherent = bool(
                        (_rsi_bull_hid and _ml2 <= _ml1)
                        or (_macd_bull_hid and _rl2 <= _rl1)
                    )
                    # 🔒 [JUDGE4-REVERSAL-0822] regular bullish divergence.
                    # hidden과 같은 확정 저점쌍/RSI/MACD/거리만 재사용하고
                    # 가격 방향과 oscillator 방향만 반대로 판정한다.
                    #
                    # hidden  : price higher-low + oscillator lower-low
                    # regular : price lower-low  + oscillator higher-low
                    _reg_tick = max(
                        float(get_tick_size(_pl1) or 0.0),
                        float(get_tick_size(_pl2) or 0.0),
                    )
                    # 🔒 [DIV-QUALITY-ATR-FLOOR-0829-R1] ★415 — J4 regular 전용.
                    # bearish는 건드리지 않고 hidden과 같은 _pivot_gap_atr_ok
                    # 헬퍼를 공유한다(정의 1곳: L4226 부근, 소비 2곳: hidden+regular).
                    indicators["divergence_reg_gap_abs"] = float(abs(_pl1 - _pl2))
                    _reg_quality = bool(
                        _pl2 < _pl1
                        and (_pl1 - _pl2) >= _reg_tick
                        and _hidden_dist >= (_div_k + 1)
                        and _pivot_gap_atr_ok(_pl1, _pl2)
                    )
                    _rsi_bull_reg = bool(
                        _reg_quality and _rl2 > _rl1
                    )
                    _macd_bull_reg = bool(
                        _reg_quality and _ml2 > _ml1
                    )
                    _div_low_rsi = float(
                        min(_rl1, _rl2)
                    )

            # 🔒 [JUDGE4-REVERSAL-0822] regular 상승 다이버전스 J4 SSoT.
            indicators["divergence_rsi_bull_reg"] = _rsi_bull_reg
            indicators["divergence_macd_bull_reg"] = _macd_bull_reg
            # 🔒 [J4-REG-RAW-SSOT-0906] ★526
            # 다이버전스 "존재" SSoT는 사용자 계약대로 RSI OR MACD다.
            # ★495-R1의 oscillator coherence는 J4 품질조건으로 별도 키에 보존한다.
            # 판정 집합은 변경하지 않는다.
            indicators["div_regular_bull"] = bool(
                _rsi_bull_reg or _macd_bull_reg
            )
            indicators["div_regular_bull_coherent"] = bool(
                (_rsi_bull_reg and _ml2 >= _ml1)
                or (_macd_bull_reg and _rl2 >= _rl1)
            )
            indicators["div_low_rsi"] = _div_low_rsi
            indicators["divergence_rsi_bear_reg"] = _rsi_bear_reg
            indicators["divergence_macd_bear_reg"] = _macd_bear_reg
            indicators["divergence_rsi_bull_hid"] = _rsi_bull_hid
            indicators["divergence_macd_bull_hid"] = _macd_bull_hid
            # 🔒 [J2-HIDDEN-OSC-COHERENCE-0904] ★504-R1
            # raw RSI/MACD hidden과 double-hidden은 관측값으로 보존한다.
            # 메인 J2는 두 oscillator가 서로 반대 방향이 아닌 결합값만 소비한다.
            # 신규 threshold 없음; double-hidden 강제도 아니다.
            indicators["div_rsi_hid"] = _rsi_bull_hid
            indicators["div_macd_hid"] = _macd_bull_hid
            indicators["div_double_hidden"] = bool(
                _rsi_bull_hid and _macd_bull_hid
            )
            indicators["div_j2_hidden_coherent"] = bool(
                _j2_hidden_coherent
            )
            # TLC가 아직 계산 전이므로 결합값은 FINAL 블록에서만 확정한다.
            indicators["divergence_hidden_bull"] = False
            indicators["divergence_bear_reg_valid"] = _bear_valid_raw
            # 🔒 [DIV-CLASS-GATE-1109] ① W / M ② W 일 때 기울기로 유형 (J2 / J4 / J4-1 / J4-2) · 표 교체는 6_strategy J2 입구
            try:
                _dc_closed = df.iloc[:-1].tail(70)
                _dc_rsi = _rsi_series.reindex(_dc_closed.index).to_numpy(dtype=float)
                _dc_macd = (
                    _macd_series.reindex(_dc_closed.index)
                    / _dc_closed["close"].astype(float) * 100.0
                ).to_numpy(dtype=float)
                _dc_s, _dc_t, _dc_p = _div_class_mw(
                    _dc_closed["high"].to_numpy(dtype=float),
                    _dc_closed["low"].to_numpy(dtype=float),
                    _dc_closed["close"].to_numpy(dtype=float),
                    _dc_rsi, _dc_macd, float(_div_atr_raw or 0.0), get_tick_size,
                )
            except Exception:
                _dc_s, _dc_t, _dc_p = "ERR", "ERR", float("nan")
            indicators["div_class_shape"] = _dc_s
            indicators["div_class_type"] = _dc_t
            indicators["div_class_pos"] = _dc_p

            # 동시 발생 시 가자미근 후보인 히든상승 품질값을 우선 기록한다.
            if _hidden_raw:
                indicators["divergence_price_delta_pct"] = _hidden_pd
                indicators["divergence_pivot_distance"] = _hidden_dist

                # 🔒 [J2-PIVOT-AGE-OBSERVE-0828]
                # 계산 → 로그 전달. 판정 소비 0곳.
                indicators["divergence_pivot_age"] = _hidden_pivot_age
                indicators["divergence_signal_age"] = _hidden_signal_age
                indicators["divergence_pivot_bar"] = _hidden_pivot_bar
                indicators["divergence_confirm_bar"] = _hidden_confirm_bar
                indicators["divergence_hidden_price1"] = _pl1
                indicators["divergence_hidden_price2"] = _pl2
                indicators["divergence_hidden_rsi1"] = _rl1
                indicators["divergence_hidden_rsi2"] = _rl2
                indicators["divergence_hidden_macd1"] = _ml1
                indicators["divergence_hidden_macd2"] = _ml2
            elif _rsi_bear_reg or _macd_bear_reg:
                indicators["divergence_price_delta_pct"] = _bear_pd
                indicators["divergence_pivot_distance"] = _bear_dist

            # 🔒 [J4-REG-PIVOT-OBSERVE-0902-R1] ★485-R1
            # J4 regular와 J2 hidden은 동일한 확정 price-low 쌍을 사용한다.
            # 판정에 이미 사용한 raw 값과 기존 age metadata만 별도 export한다.
            # 새 계산·threshold·판정 변화 없음.
            if _rsi_bull_reg or _macd_bull_reg:
                indicators["divergence_reg_price1"] = _pl1
                indicators["divergence_reg_price2"] = _pl2
                indicators["divergence_reg_rsi1"] = _rl1
                indicators["divergence_reg_rsi2"] = _rl2
                indicators["divergence_reg_macd1"] = _ml1
                indicators["divergence_reg_macd2"] = _ml2
                indicators["divergence_reg_pivot_distance"] = _hidden_dist
                indicators["divergence_reg_pivot_age"] = _hidden_pivot_age
                indicators["divergence_reg_signal_age"] = _hidden_signal_age
                indicators["divergence_reg_pivot_bar"] = _hidden_pivot_bar
                indicators["divergence_reg_confirm_bar"] = _hidden_confirm_bar

            indicators["_divergence_shadow_ready"] = True

        except Exception as _div_e:
            logger.info(
                f"⚠️ [DIVERGENCE-SHADOW] {symbol} 계산 실패 | "
                f"tf={timeframe.value if hasattr(timeframe, 'value') else timeframe} | "
                f"bar={indicators.get('indicator_shadow_bar')} | "
                f"reason={_div_e}"
            )

        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        # 🧩 거래량 추세 보정 (volume_slope) - 타임프레임별 차등 적용
        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        # 
        # 목적: 고점 피크 후 거래량 둔화 구간에서 진입 억제
        # 원리: 최근 5개 봉 거래량의 추세선 기울기를 정규화하여 평가
        #       - 증가 추세: 신호 유지 (1.0)
        #       - 중립: 약간 감산 (0.9)
        #       - 감소 추세: 명확히 감산 (0.75~0.9, 타임프레임별)
        # 
        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        if indicators.get('volume_ratio') is not None and len(df) >= 5 and 'volume' in df.columns:
            recent_volumes = df['volume'].tail(5).values
            
            # 거래량 절대 기울기 계산
            volume_slope = np.polyfit(range(5), recent_volumes, 1)[0]
            
            # ✅ 상대 기울기로 정규화 (거래량 규모 독립적)
            avg_volume = recent_volumes.mean()
            if avg_volume > 0:
                # 평균 거래량 대비 기울기 비율 (%)
                normalized_slope = volume_slope / avg_volume
            else:
                normalized_slope = 0
            
            # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
            # 타임프레임별 민감도 테이블
            # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
            # (상승임계, 하락임계, 하락감산, 중립감산)
            slope_thresholds = {
                '1m':  (0.04, -0.04, 0.75, 0.9),
                '3m':  (0.04, -0.04, 0.75, 0.9),
                '5m':  (0.05, -0.05, 0.8, 0.9),
                '10m': (0.05, -0.05, 0.8, 0.9),
                '15m': (0.06, -0.06, 0.85, 0.9),
                '30m': (0.07, -0.07, 0.85, 0.9),
                '1h':  (0.08, -0.08, 0.85, 0.9),
                '4h':  (0.10, -0.10, 0.9,  0.95),
                '1d':  (0.10, -0.10, 0.9,  0.95),
            }
            up_thr, down_thr, down_mult, neutral_mult = slope_thresholds.get(
                timeframe, (0.05, -0.05, 0.8, 0.9)
            )
            
            # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
            # 단계적 보정
            # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
            if normalized_slope > up_thr:
                # 거래량 증가 추세 → 신호 유지
                trend_multiplier = 1.0
            elif normalized_slope < down_thr:
                # 거래량 감소 추세 → 고점 피크 후 둔화
                trend_multiplier = down_mult
            else:
                # 중립 (±임계값 이내) → 약한 감산
                trend_multiplier = neutral_mult
            
            # 최종 추세 보정 적용
            current_volume_ratio = float(indicators.get('volume_ratio', 0.0) or 0.0)
            
            # 1) 기존 방식 유지(추세 보정)
            indicators['volume_ratio'] = current_volume_ratio * trend_multiplier
            
            # 2) 🆕 스무딩 값 추가: 단일 봉 급감/급증의 오탐 제거
            #    - df에서 최근 3개 봉의 거래량/평균(20) 비율을 구해 중앙값(median) 사용
            try:
                if df is not None and len(df) >= 25:
                    v = df['volume'].astype(float)
                    ma20 = v.rolling(20).mean()
                    r = (v / ma20).replace([float('inf'), -float('inf')], 0.0).fillna(0.0)
                    last3 = r.tail(3).tolist()
                    last3_sorted = sorted(last3)
                    median3 = last3_sorted[1]  # 중앙값
                    # 최종 의사결정용: 추세보정값 vs 중앙값 중 더 보수적인 쪽 선택
                    indicators['volume_ratio_smooth'] = float(min(indicators['volume_ratio'], median3))
                else:
                    indicators['volume_ratio_smooth'] = float(indicators['volume_ratio'])
            except Exception:
                indicators['volume_ratio_smooth'] = float(indicators['volume_ratio'])
            
            # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
            # 📌 [최종 거래량 변수 정리]
            # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
            # Strategy 최종 판단 기준: volume_ratio (추세 보정 적용값)
            # 
            # 변수 역할:
            # - raw_volume_ratio: 원시값 (current_volume / MA20)
            #   용도: 디버깅, 로그 출력
            # 
            # - volume_ratio: 압력 분석 + 추세 보정 적용값
            #   용도: Strategy 최종 진입 판단 기준 ⭐
            #   계산: raw → 압력 조정 → 추세 보정
            # 
            # - volume_ratio_smooth: 중앙값 스무딩 적용값
            #   용도: 관측/점수 전용 — Strategy hard 판단 소비 금지
            #   계산: min(volume_ratio, median3)
            #   🔒 [VOL-CONTRACT-FIX-0814] 급증 순간을 직전 중앙값으로 누르므로
            #   최종 진입 판단은 위 SSoT 선언대로 volume_ratio를 사용한다.
            # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
                            
        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        # ✅ rally_info 계산 및 추가 (Strategy 재계산 방지)
        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        # 🔒 [PARENT-RALLY-CLOSED-0823]
        # 부모파동 구조(low/high/pullback)는 같은 확정봉 snapshot에서만 계산한다.
        _rally_df = TechnicalIndicators.drop_incomplete_candle(df, timeframe)
        rally_info = TechnicalIndicators.detect_rally_pattern(_rally_df, symbol)
        indicators['rally_info'] = rally_info
        
        # 🔑 빠른 접근을 위한 핵심 필드 추출
        indicators['has_rally'] = rally_info.get('has_rally', False)
        indicators['rally_pct'] = rally_info.get('rally_pct', 0.0)
        indicators['rally_duration'] = rally_info.get('rally_duration', 0)
        indicators['max_price'] = rally_info.get('max_price', 0.0)
        indicators['pullback_ratio'] = rally_info.get('pullback_ratio', 0.0)
        
        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        # ✅ Strategy가 필요로 하는 추가 지표들 (SSoT 완성)
        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        
        # 최근 저점/고점 (가자미근 계산용)
        if len(df) >= 20:
            indicators['recent_low'] = float(df['low'].tail(20).min())
            indicators['recent_high'] = float(df['high'].tail(20).max())
        elif len(df) >= 5:
            indicators['recent_low'] = float(df['low'].tail(5).min())
            indicators['recent_high'] = float(df['high'].tail(5).max())
        else:
            indicators['recent_low'] = float(df['close'].iloc[-1]) if len(df) > 0 else 0.0
            indicators['recent_high'] = float(df['close'].iloc[-1]) if len(df) > 0 else 0.0

        # 🆕 [CLI 06-30 C-1] 꼭지가드 검증용 closed 20봉 고저점
        # - Strategy는 df를 직접 보지 않으므로 Analysis가 current candle 제외 기준을 export한다.
        # - recent_high/low는 기존 소비처 호환을 위해 현재봉 포함값 그대로 유지한다.
        try:
            if len(df) >= 21:
                _recent_closed_ref = df.iloc[-21:-1]  # 현재봉 제외 직전 20봉
            elif len(df) >= 2:
                _recent_closed_ref = df.iloc[:-1]
            else:
                _recent_closed_ref = df

            indicators['recent_low_closed'] = (
                float(_recent_closed_ref['low'].tail(20).min())
                if len(_recent_closed_ref) > 0 else indicators['recent_low']
            )
            indicators['recent_high_closed'] = (
                float(_recent_closed_ref['high'].tail(20).max())
                if len(_recent_closed_ref) > 0 else indicators['recent_high']
            )
        except Exception:
            indicators['recent_low_closed'] = indicators['recent_low']
            indicators['recent_high_closed'] = indicators['recent_high']

        # 저항선 (BREAKOUT 체제용)
        if len(df) >= 20:
            indicators['resistance_level'] = float(df['high'].tail(20).max())
        else:
            indicators['resistance_level'] = 0.0
        
        # 최근 캔들 정보 (EntryFilter용)
        if len(df) > 0:
            indicators['last_high'] = float(df['high'].iloc[-1])
            indicators['last_low'] = float(df['low'].iloc[-1])
            indicators['last_volume'] = float(df['volume'].iloc[-1])
        else:
            indicators['last_high'] = 0.0
            indicators['last_low'] = 0.0
            indicators['last_volume'] = 0.0
        
        # 거래량 가속도 (반등 초입 감지용)
        try:
            if len(df) >= 8:
                vol_recent = df['volume'].tail(3).mean()
                vol_older = df['volume'].iloc[-8:-3].mean()
                indicators['vol_accel'] = vol_recent / vol_older if vol_older > 0 else 1.0
            elif len(df) >= 3:
                indicators['vol_accel'] = 1.0
            else:
                indicators['vol_accel'] = 1.0
        except Exception:
            indicators['vol_accel'] = 1.0
        
        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        # ✅ MA5 시리즈 제공 (Strategy MA5 추세 체크용)
        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        indicators['ma5_series'] = df['close'].rolling(5).mean()
        
        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        # 🆕 A그룹: 의사결정 핵심 로직 (Strategy 이관)
        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        
        # A-1. Bearish block (하락장 차단 핵심)
        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        # 🔧 regime는 Analyzer/상위 로직에서 결정됨
        # - 미주입 가능성을 명시적으로 받아들이고 오판 fallback 방지
        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        regime = indicators.get('regime', None)
        
        indicators['bearish_block'] = False
        if regime in (MarketRegime.TRENDING_DOWN, MarketRegime.BREAKDOWN):
            indicators['bearish_block'] = True        
        
        # 🔒 [VOLUME-DATA-QUALITY-0823] 계산 불가를 정상 0.0x로 위조하지 않는다.
        _volume_ratio_norm = safe_extract_scalar(
            indicators.get('volume_ratio', SENTINEL_FLOAT),
            SENTINEL_FLOAT,
        )
        indicators['volume_ratio'] = (
            _volume_ratio_norm
            if np.isfinite(_volume_ratio_norm)
            else SENTINEL_FLOAT
        )
        
        # A-4. Trend strength (추세 강도 핵심)
        indicators['trend_strength'] = None
        ma20_slope = indicators.get('ma20_slope')
        if ma20_slope is not None:
            indicators['trend_strength'] = abs(ma20_slope)
        
        # A-5. Confidence penalty (신뢰도 보정 핵심)
        indicators['confidence_penalty'] = 1.0
        
        # 하락장이면 50% 감점
        if indicators['bearish_block']:
            indicators['confidence_penalty'] *= BEARISH_PENALTY
        
        # 거래량 부족이면 30% 감점
        volume_ratio = indicators.get('volume_ratio', 0)
        if volume_ratio is not None and volume_ratio < MIN_VOLUME_RATIO:
            indicators['confidence_penalty'] *= LOW_VOLUME_PENALTY
            
        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        # B-2. Strategy 소비 안정화 기본 키 (SSoT)
        # - None이 들어오면 비교/포맷에서 터지므로 기본값 보장
        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        indicators.setdefault('global_knee_volume_relaxed', False)
        indicators.setdefault('regime_early', False)
        indicators.setdefault('regime_turn_candidate', False)
        indicators.setdefault('volume_ratio_smooth', float(indicators.get('volume_ratio', 0.0) or 0.0))
        
        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        # 🆕 B-8-1: Knee 완화 플래그 (전역/예외)
        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        try:
            global_bull = bool(indicators.get('global_bull', False))
            knee_exc = bool(indicators.get('knee_exception', False))
            if global_bull or knee_exc:
                indicators['global_knee_volume_relaxed'] = True
        except Exception:
            pass
        indicators['global_knee_volume_relaxed'] = False
        
        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        # 🆕 B-8-2: 급등 과열 페널티 (연대기 필터)
        # - Strategy는 penalty 값만 소비
        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        over_rally_penalty = 1.0
        try:
            has_rally = bool(indicators.get('has_rally', False))
            rally_pct = float(indicators.get('rally_pct', 0.0) or 0.0)
            pullback_ratio = float(indicators.get('pullback_ratio', 0.0) or 0.0)
            rally_duration = int(indicators.get('rally_duration', 0) or 0)

            # 8%↑ 급등 + (되밀림 작음 OR 시간오래+고점권) → 강한 과열
            if has_rally and rally_pct >= 0.08 and (
                pullback_ratio <= 0.20 or (rally_duration >= 60 and pullback_ratio <= 0.40)
            ):
                over_rally_penalty = 0.75
            # 5%↑ 급등 + 되밀림 작음 → 중간 과열
            elif has_rally and rally_pct >= 0.05 and pullback_ratio <= 0.20:
                over_rally_penalty = 0.85
        except Exception:
            over_rally_penalty = 1.0
        
        if over_rally_penalty < 1.0:
            logger.debug(
                f"[OverRally] {symbol} | "
                f"rally={rally_pct*100:.1f}% dur={rally_duration}분 "
                f"pullback={pullback_ratio:.2f} → penalty={over_rally_penalty:.2f}"
            )
        indicators['over_rally_penalty'] = over_rally_penalty

        # 🔒 [OVERRALLY-PB-REVERT-0810] P-E(0.02<pb<=0.20) 91분 실측 되돌림.
        #   발화율 9.4% → ★68.2% (예상 19%의 3.6배) · 종목 9개에서 상시 발화
        #   ★부작용: over_rally_hard_block 을 입력으로 쓰는 bypass 2개가 켜짐
        #     TD-RG-FRESH-RECLAIM 3건 · TD-RG-FRESH-ABS-THRUST 3건
        #   → 차단을 늘리려다 우회로를 연 셈. 원복한다.
        #   ★F1(_parent_wave_top, t5 L10552)이 이미 C국면 origin 면제를 끊으므로
        #     P-E 는 F1 의 필수조건이 아니다 (ChatGPT 지적 · 소스 확인 완료).
        _over_rally_hard_block = bool(
            has_rally
            and rally_pct >= 0.05
            and rally_duration >= 60
            and pullback_ratio > 0.0
            and pullback_ratio <= 0.10
        )
        indicators['over_rally_hard_block'] = int(_over_rally_hard_block)
        if _over_rally_hard_block:
            logger.info(
                f"⚠️ [OverRally-CANDIDATE] {symbol} | "
                f"rally={rally_pct*100:.1f}% "
                f"dur={rally_duration}분 "
                f"pullback={pullback_ratio:.2f} "
                f"→ TD/RG 소비처에서 hard block 판정"
            )

        # ━━━ B-8-3: 매도압력 무시 플래그
        ignore_selling_pressure = False
        try:
            if bool(indicators.get('global_bull', False)) and bool(indicators.get('is_sudden_change', False)):
                ignore_selling_pressure = True
        except Exception:
            ignore_selling_pressure = False
        
        indicators['ignore_selling_pressure'] = ignore_selling_pressure
        
        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        # 🆕 B-8-4: Strategy 키 누락 방지 (보조)
        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        indicators.setdefault('candidate_hold_count', 0)
        indicators.setdefault('candidate_persistent', False)
        indicators.setdefault('entry_volume_checked', False)
        indicators.setdefault('fast_entry', False)
        indicators.setdefault('early_entry_flag', False)
        
        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        # ❌ B-9-1 제거됨
        # SoleusZone 확정은 calculate_ma_analysis()에서만 수행 (SSoT 단일화)
        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        
        # (knee_mode / td_knee_mode export 완전 삭제 — Strategy 소비처 없음, 가자미근 단일 Gate)
        
        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        # 🆕 B-9-3: Strategy 소비 안정화 기본키
        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        indicators.setdefault('soleus_upper_price', None)
        indicators.setdefault('knee_exception', False)
        
        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        # ✅ 전환 초입(Early Turn) 감지
        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        # - 하락 이후 반등 초입을 빠르게 포착
        # - 07:00 같은 "가자미근 적기"를 살리기 위한 가산
        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        try:
            # 필수 지표
            ma5_slope = nzs(indicators.get('ma5_slope'))
            ma10_slope = nzs(indicators.get('ma10_slope'))
            ma5_slope_prev1 = nzs(indicators.get('ma5_slope_prev1'))

            # 🔒 [EARLY-TURN-EVENT-0823]
            # early_turn은 "양수 상태"가 아니라 MA5 하락/평탄→상승의 1회성 전환 이벤트다.
            early_turn = (
                ma5_slope > 0
                and ma5_slope_prev1 <= 0
                and ma10_slope >= 0
            )

            indicators['early_turn'] = float(early_turn)

            # 🔒 정석: early_turn은 "관측"만 유지 (점수 보정/가산 금지)
            indicators['early_turn_bonus'] = 0.0

        except Exception:
            indicators['early_turn'] = 0.0
            indicators['early_turn_bonus'] = 0.0
        
        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        # ✅ 5봉 완전 꺾임(모멘텀 소진) 감지
        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        # - 최근 5개 종가가 연속 하락이면 모멘텀 소진으로 판정
        # - Strategy에서 체제 무관 HARD-BLOCK 용도로 사용
        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        try:
            close5 = df['close'].tail(5).astype(float).values
            indicators['close_down5'] = float(
                all(close5[i] < close5[i - 1] for i in range(1, len(close5)))
            )
        except Exception:
            indicators['close_down5'] = 0.0
        
        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        # ✅ 피뢰침(위꼬리) 매도 압력 감지
        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        # - 급등 후 즉시 매도 의사 표시 캔들 차단용
        # - 위꼬리가 몸통의 2배 이상 = 피뢰침
        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        try:
            last = df.iloc[-1]
            o = float(last['open'])
            c = float(last['close'])
            h = float(last['high'])

            body = abs(c - o)
            upper_shadow = h - max(c, o)

            # 위꼬리가 몸통의 2배 이상 → 피뢰침
            indicators['upper_shadow_alert'] = float(
                body > 0 and (upper_shadow / body) >= 2.0
            )
        except Exception:
            indicators['upper_shadow_alert'] = 0.0
        
        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        # ✅ PULLBACK_BOUNCE (메이저 눌림 반등형)
        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        # - ETH/BTC 같은 메이저에서만 의미 있음
        # - "눌림 후 반등 신호가 명확할 때만" 진입 사유 생성
        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        try:
            # 핵심 지표
            ma5_slope = float(indicators.get('ma5_slope', 0.0))
            price = float(df.iloc[-1]['close'])
            ma5 = float(indicators.get('sma_5', 0.0))
            
            close_down5 = int(indicators.get('close_down5', 0.0))
            upper_shadow = int(indicators.get('upper_shadow_alert', 0.0))
            
            # 메이저 코인만 허용 (BTC/ETH)
            is_major = symbol in ['KRW-BTC', 'KRW-ETH']
            
            pullback_bounce = (
                is_major and
                ma5_slope >= 0.0 and        # 반등 시작
                price >= ma5 and            # MA5 재탈환
                close_down5 == 0 and        # 모멘텀 소진 아님
                upper_shadow == 0           # 매도 압력 없음
            )
            
            indicators['pullback_bounce'] = float(pullback_bounce)
            
            if pullback_bounce:
                # 눌림 반등 가산 (보수적)
                indicators['pullback_bonus'] = 0.05
            else:
                indicators['pullback_bonus'] = 0.0
                
        except Exception:
            indicators['pullback_bounce'] = 0.0
            indicators['pullback_bonus'] = 0.0
        
        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        # ✅ Entry Type 분리 (SSoT)
        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        # - REVERSAL: 하락 → 반등 초입 (IP형)
        # - TREND: 횡보 → 정렬 → 추세 시작 (ZBT형)
        # - PULLBACK: 메이저 눌림 반등 (ETH형)
        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        entry_type = "NONE"
        _entry_ma5 = float(safe_extract_scalar(indicators.get('ma5'), 0.0))
        _entry_ma10 = float(safe_extract_scalar(indicators.get('ma10'), 0.0))
        _entry_s20 = float(safe_extract_scalar(indicators.get('ma20_slope'), 0.0))
        _entry_clean = bool(
            indicators.get('close_down5', 0.0) == 0.0
            and indicators.get('upper_shadow_alert', 0.0) == 0.0
        )
        # PULLBACK_BOUNCE (메이저 눌림 반등형 - ETH)
        if indicators.get('pullback_bounce', 0.0) == 1.0:
            entry_type = "PULLBACK"
        # REVERSAL = 실제 첫 고개 + 확정 중기(MA20) 하락이 남아 있는 반전형.
        # MA20가 이미 생존한 strict GC 상승은 REVERSAL로 오분류하지 않는다.
        elif (
            indicators.get('early_turn', 0.0) == 1.0
            and _entry_clean
            and _entry_s20 < 0.0
        ):
            entry_type = "REVERSAL"
        # TREND = strict GC가 성립하고 매도압력/5봉소진이 없는 상승 경로.
        elif _entry_clean and _entry_ma5 > _entry_ma10 > 0.0:
            entry_type = "TREND"
        indicators['entry_type'] = entry_type
        
        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        # 🔒 NUMERIC SENTINEL 정규화 (SSoT)
        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        # Strategy 모듈로 전달되는 모든 숫자 값의 None 제거
        # 이 한 줄로 NoneType 오류 영구 차단
        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        for k, v in indicators.items():
            # 🔒 bool은 상태값이므로 숫자 SENTINEL 정규화 대상에서 제외
            if isinstance(v, bool):
                continue
            if isinstance(v, (int, float, type(None))):
                indicators[k] = nzs(v)
        
        # SSoT: Strategy에서 사용할 키 생성
        # - 최종 가자미근 후보는 calculate_ma_analysis().is_gajami_candidate / gajami_meta가 진실
        # - indicators에는 가자미근 판정을 두지 않음 (ma_data가 SSoT)

        return indicators
            
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    # 🆕 [MA 간격 기반 조기 진입 함수] 2025-11-12
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    @staticmethod
    def calculate_ma_gap_score(
        ma5: float, 
        ma10: float, 
        ma15: float, 
        ma20: float, 
        ma60: float,
        current_price: float,
        symbol: str = ""
    ) -> tuple:
        """
        MA 간격 기반 조기 진입 점수 계산        
        핵심 원칙:
        1. 간격이 좁을수록 = 골든크로스 초기 = 높은 점수
        2. 가격이 MA5 근처일수록 = 좋은 진입점 = 높은 점수
        3. 역배열 또는 혼합 상태 = 차단        
        Args:
            ma5, ma10, ma15, ma20, ma60: 이동평균 값
            current_price: 현재가
            symbol: 종목명 (로깅용)        
        Returns:
            tuple: (점수 0~1, 메타데이터 dict)
        """
        
        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        # 1단계: 골든크로스 및 준골든크로스 판정
        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        gap_ma5_ma10_pct = (ma5 - ma10) / ma10 * 100
        
        # Case 1: 준골든크로스 구간 (-0.15% ~ 0%)
        if -0.15 <= gap_ma5_ma10_pct < 0:
            # 필수: 현재가가 두 MA보다 위에 있어야 함
            if current_price > ma5 and current_price > ma10:
                # 준골든크로스 점수: 0.5 ~ 0.7
                pre_cross_score = 0.5 + abs(gap_ma5_ma10_pct) * 1.33
                pre_cross_score = min(0.7, max(0.5, pre_cross_score))
                
                # 가격 위치 점수
                price_above_ma5_pct = (current_price - ma5) / ma5 * 100
                if price_above_ma5_pct <= 0.5:
                    price_score = 1.0
                elif price_above_ma5_pct <= 1.0:
                    price_score = 0.8
                elif price_above_ma5_pct <= 1.5:
                    price_score = 0.6
                else:  # 과열 차단
                    return 0.0, {
                        'reason': f'가격 과열 (MA5 대비 +{price_above_ma5_pct:.2f}%)',
                        'gap_5_10': gap_ma5_ma10_pct
                    }
                
                # 최종 점수 (간격 60% + 가격 40%)
                final_score = pre_cross_score * 0.6 + price_score * 0.4
                final_score = min(0.7, max(0.5, final_score))
                
                metadata = {
                    'gap_5_10': gap_ma5_ma10_pct,
                    'gap_10_15': (ma10 - ma15) / ma15 * 100,
                    'gap_15_20': (ma15 - ma20) / ma20 * 100,
                    'price_above_ma5': price_above_ma5_pct,
                    'gap_score': pre_cross_score,
                    'price_score': price_score,
                    'acceleration': 0.0,
                    'stage': '준골든_크로스직전',
                    'optimal': True,
                    'reason': f'준골든크로스 (gap={gap_ma5_ma10_pct:.3f}%)'
                }
                
                logger.debug(
                    f"🎯 [준골든크로스] {symbol} | "
                    f"현재가={current_price:.2f} > MA5={ma5:.2f} > MA10={ma10:.2f} | "
                    f"gap={gap_ma5_ma10_pct:.3f}% | 점수={final_score:.2f}"
                )
                
                return final_score, metadata
        
        # Case 2: 골든크로스 완전 미발생 (gap < -0.15%)
        if gap_ma5_ma10_pct < -0.15:
            return 0.0, {
                'reason': f'골든크로스 미발생 (gap={gap_ma5_ma10_pct:.2f}%)',
                'ma5': ma5, 'ma10': ma10
            }
        
        # Case 3: 골든크로스 완료 (gap >= 0)
        # → 아래 기존 로직 계속 진행
        
        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        # 🔒 가자미근 간격 점수 3단계 로직 (완성 + 진행 + 초입)
        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        # 철학:
        # 1. 완전 정배열(완성): 기존 계산 유지
        # 2. 진행 중(5>10, 5>20): 중간 점수 부여
        # 3. 초입(5>10만): 낮은 점수 부여
        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        
        # MA 간격 계산
        gap_5_10 = (ma5 - ma10) / ma10 * 100
        gap_10_15 = (ma10 - ma15) / ma15 * 100
        gap_15_20 = (ma15 - ma20) / ma20 * 100
        gap_10_20 = (ma10 - ma20) / ma20 * 100
        
        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        # 🎯 단계별 간격 점수 계산 (위치 기반 재정의)
        # - 철학: "무릎에서 사서 목덜미에서 판다"
        # - 초입(무릎) = 높은 점수 / 완성(목덜미) = 낮은 점수
        # - 기준: 0.50 = 100점 (이상적 진입)
        # - 조건 순서: 구체적 → 포괄적 (겹침 제거)
        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        gap_score = 0.0
        stage_label = ""
        
        # ─────────────────────────────────────────────
        # 1️⃣ 완성: 완전 정배열 (목덜미, 늦은 진입)
        # ─────────────────────────────────────────────
        if ma5 > ma10 > ma20:
            stage_label = "완성"
            
            if gap_5_10 <= 0.05:    # 매우 좁은 간격 (최근 골크)
                gap_score = 0.30
            elif gap_5_10 <= 0.15:  # 좁은 간격
                gap_score = 0.25
            elif gap_5_10 <= 0.30:  # 보통 간격
                gap_score = 0.20
            else:                   # 넓은 간격 (이미 많이 상승)
                gap_score = 0.15
        
        # ─────────────────────────────────────────────
        # 2️⃣ 진행: ma5가 ma10/ma20 모두 넘었지만
        #    ma10은 아직 ma20을 못 넘은 상태 (중반)
        # ─────────────────────────────────────────────
        elif (ma5 > ma10) and (ma5 > ma20) and not (ma10 > ma20):
            stage_label = "진행"
            
            if gap_5_10 >= 0.002:   # 0.2% 이상
                gap_score = 0.45
            elif gap_5_10 >= 0.001: # 0.1% 이상
                gap_score = 0.40
            else:
                gap_score = 0.35
        
        # ─────────────────────────────────────────────
        # 3️⃣ 중기회복: ma10이 ma20을 넘었지만
        #    ma5는 아직 ma10을 못 넘은 상태 (무릎 직전)
        # ─────────────────────────────────────────────
        elif (ma10 > ma20) and not (ma5 > ma10):
            stage_label = "중기회복"
            
            if gap_10_20 >= 0.002:  # 0.2% 이상
                gap_score = 0.50
            elif gap_10_20 >= 0.001:# 0.1% 이상
                gap_score = 0.45
            else:
                gap_score = 0.40
        
        # ─────────────────────────────────────────────
        # 4️⃣ 초입: ma5가 ma10을 넘었지만
        #    위 세 조건에 해당하지 않는 경우 (가장 이상적!)
        # ─────────────────────────────────────────────
        elif ma5 > ma10:
            stage_label = "초입"
            
            if gap_5_10 >= 0.002:   # 0.2% 이상 (강한 초입)
                gap_score = 0.55
            elif gap_5_10 >= 0.001: # 0.1% 이상 (일반 초입)
                gap_score = 0.50
            else:                   # 미세한 간격
                gap_score = 0.45
        
        # ─────────────────────────────────────────────
        # 5️⃣ 없음: 골든크로스 미발생
        # ─────────────────────────────────────────────
        else:
            stage_label = "없음"
            gap_score = 0.0
        
        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        # 최종 점수 적용
        gap_score = float(np.clip(gap_score, 0.0, 1.0))
        
        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        # 가격 위치 점수 (MA5 대비)
        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        price_above_ma5_pct = (current_price - ma5) / ma5 * 100
        
        if price_above_ma5_pct < -0.2:
            price_score = 0.3
        elif price_above_ma5_pct <= 0.5:
            price_score = 1.0
        elif price_above_ma5_pct <= 1.0:
            price_score = 0.85
        elif price_above_ma5_pct <= 1.5:
            price_score = 0.7
        elif price_above_ma5_pct <= 2.0:
            price_score = 0.5
        else:
            price_score = 0.1
        
        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        # 추세 가속도 보너스 (완성 단계에서만 적용)
        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        acceleration = 0.0
        if stage_label == "완성":
            if gap_5_10 < gap_10_15 and gap_10_15 < gap_15_20:
                acceleration = 0.1
            elif gap_5_10 > gap_10_15 * 2:
                acceleration = -0.1
        
        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        # 최종 점수 계산
        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        if stage_label == "완성":
            # 완성: 간격 70% + 가격 30% + 가속도
            base_score = gap_score * 0.7 + price_score * 0.3
            final_score = base_score + acceleration
        else:
            # 진행/초입: 간격 점수만 사용 (단순화)
            final_score = gap_score
        final_score = max(0.0, min(1.0, final_score))
        
        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        # 7단계: 단계 판정
        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        if gap_5_10 <= 0.05:
            stage = "직후_안정화대기"
        elif gap_5_10 <= 0.15:
            stage = "초기_최적"      # ⭐ 최적 진입 구간
        elif gap_5_10 <= 0.3:
            stage = "초기"
        elif gap_5_10 <= 0.5:
            stage = "진행중"
        elif gap_5_10 <= 0.8:
            stage = "중기"
        elif gap_5_10 <= 1.2:
            stage = "후기"
        else:
            stage = "완료_늦음"
        
        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        # 8단계: 메타데이터 생성
        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        metadata = {
            'gap_5_10': gap_5_10,
            'gap_10_15': gap_10_15,
            'gap_15_20': gap_15_20,
            'price_above_ma5': price_above_ma5_pct,
            'gap_score': gap_score,
            'price_score': price_score,
            'acceleration': acceleration,
            'stage': stage,
            'optimal': gap_5_10 <= 0.3 and price_above_ma5_pct <= 0.5
        }
        
        return final_score, metadata
    
    @staticmethod
    def drop_incomplete_candle(df: pd.DataFrame, timeframe: str) -> pd.DataFrame:
        """
        미완성 캔들 제거 - 확정 구조 판단용 snapshot

        🔒 헌법:
        - 시장 체제(Regime)·부모파동 구조(rally/pullback): 확정봉 ONLY
        - MA5/BIRTH 조기 트리거: intrabar(30초) 유지
        구조와 조기 트리거의 시간축을 분리한다.

        Args:
            df: OHLCV 데이터프레임
            timeframe: 타임프레임 (1m, 3m, 5m 등)
        Returns:
            완성된 캔들만 포함된 데이터프레임
        """
        import time
        
        if len(df) < 2:
            return df
        
        # 타임프레임을 초 단위로 변환
        timeframe_map = {
            '1m': 60, '3m': 180, '5m': 300, '10m': 600,
            '15m': 900, '30m': 1800, '1h': 3600, '4h': 14400, '1d': 86400
        }
        timeframe_sec = timeframe_map.get(timeframe, 180)  # 기본 3m
        
        # 마지막 캔들의 타임스탬프
        last_ts = df.index[-1]
        now_ts = time.time()
        
        # 인덱스가 datetime-like가 아니면 미완성 판정 불가 → 그대로 사용
        if not hasattr(last_ts, "timestamp"):
            return df
        
        # ============================================================
        # 🔧 개선: 캔들 종료 시각 기준 판정 (95% 진행률 방식 폐기)
        # ============================================================
        # - 캔들 종료 시각 = 캔들 시작 시각 + timeframe
        # - 현재 시각 < 종료 시각 → 미완성
        # - 예: 05:27:00 시작 3분봉 → 05:30:00까지 미완성
        # ============================================================
        
        candle_end_ts = last_ts.timestamp() + timeframe_sec
        
        # 현재 시각이 캔들 종료 시각보다 이전이면 미완성
        if now_ts < candle_end_ts:
            # 미완성 캔들 제외
            return df.iloc[:-1]
        
        return df
    @staticmethod
    def calculate_ma_analysis(
        df: pd.DataFrame, 
        timeframe: str, 
        symbol: str = "",
        ma_exit_applicable_timeframes: List[str] = None,
        regime: MarketRegime = None,
        config: Any = None,
        indicators: Dict[str, Any] = None,
        ticker_price: float = None  # 🔒 가격 괴리 검증용
    ) -> Dict[str, Any]:
        """MA 분석 - 골든크로스/데드크로스, 정배열/역배열, 이격도
        Args:
            df: OHLCV 데이터프레임
            timeframe: 타임프레임 (1m, 3m, 5m 등)
            symbol: 종목 코드 (로깅용)
            ma_exit_applicable_timeframes: MA15 이탈 적용 타임프레임 리스트 (config 기반)
            regime: 시장 체제 (이격도 임계값 조정용)
        """
        
        # ══════════════════════════════════════════════════════
        # 🔒 timeframe 정규화 (SSoT: 문자열)
        # ══════════════════════════════════════════════════════
        # timeframe이 Enum/문자열 혼재되므로 함수 초입에서 단 한 번 정규화
        # ══════════════════════════════════════════════════════
        timeframe = timeframe.value if hasattr(timeframe, "value") else str(timeframe)
        
        # ✅ SSoT: Bot이 계산한 indicators를 그대로 사용 (빈 dict 초기화 폐기)
        indicators = indicators or {}
        indicators['ma20_far_above'] = 0
        indicators['ma20_dev_closed'] = 0.0
        indicators['ma20_dev_med160'] = 0.0
        # 🔒 [JURY-FAIL-CLOSED-0817] 판사 키 매 호출 0 시작.
        for _jury_key in (
            "judge1_gajami_pass", "judge1_gajami_veto",
            "judge2_osc_pass", "judge2_osc_veto", "judge2_meanrev_pass",
            "judge3_cycle_pass", "judge3_cycle_veto",
            "judge3_exhaustion_turn_pass",
            "judge4_reversal_pass",
            "judge_lane_authorized",
        ):
            indicators[_jury_key] = 0
        # 🔒 [J2-PRODUCER-CONSUMER-INTEGRITY-0902] ★478-R2
        # J2 provenance도 매 호출 fail-closed.
        # 이번 호출에서 J2 Producer가 실제 export되기 전에는
        # 이전 호출의 봉인값을 절대 재사용하지 않는다.
        indicators["judge2_prov_ready"] = 0
        indicators["judge2_prov_pass"] = 0
        indicators["judge2_prov_verdict"] = "NEUTRAL"
        indicators["judge2_prov_bar"] = ""
        indicators["judge2_prov_hidgo"] = 0

        # 🔒 [J3-PRODUCER-CONSUMER-INTEGRITY-0902] ★489
        # J3 provenance도 매 호출 fail-closed.
        indicators["judge3_prov_ready"] = 0
        indicators["judge3_prov_pass"] = 0
        indicators["judge3_prov_verdict"] = "NEUTRAL"
        indicators["judge3_prov_bar"] = ""
        indicators["judge3_prov_det"] = ""
        indicators["judge3_prov_fake_top"] = 0

        # 🔒 [JUDGE-TLC-CONTEXT-RESET-0823]
        # Active TLC가 이번 호출에서 미산출/예외여도 이전 호출의 phase/detail/위치/구조 문맥을 재사용하지 않는다.
        indicators["cycle_active_pos"] = 0.5
        indicators["cycle_active_pos_closed"] = 0.5
        # 🔒 [J2-HIDDEN-STRUCT-STALE-RESET-0905] ★518-R1
        # cycle_active_structure는 Active TLC ready 이후에만 export된다.
        # indicators는 외부 주입·재사용되므로 TLC 계산 실패 시 이전 구조가 남지 않게
        # 매 호출 UNCONFIRMED로 되돌린다. UNCONFIRMED는 ★518에서 fail-open이다.
        indicators["cycle_active_structure"] = "UNCONFIRMED"

        # 🔒 [J1-WINDOW-POS-RESET-0828]
        # cycle_window_pos는 Active-TLC 성공 시에만 생산된다.
        # indicators dict는 외부 주입·재사용되므로 이번 호출에서 TLC가 실패하면
        # 이전 호출의 window_pos가 남아 J1 0.40~0.70 VETO를 오염시킬 수 있다.
        # SENTINEL = 미산출 → J1 window 법은 fail-open.
        indicators["cycle_window_pos"] = SENTINEL_FLOAT
        indicators["cycle_window_hi_recent"] = SENTINEL_FLOAT
        indicators["cycle_window_lo_recent"] = SENTINEL_FLOAT
        indicators["cycle_window_width_pct"] = SENTINEL_FLOAT
        # 🔒 [ACTIVE-TLC-PARENT-RANGE-SSOT-0828] stale parent 문맥 금지.
        indicators["cycle_parent_ready"] = 0
        indicators["cycle_parent_pos"] = SENTINEL_FLOAT
        indicators["cycle_parent_pos_closed"] = SENTINEL_FLOAT
        indicators["cycle_parent_low"] = SENTINEL_FLOAT
        indicators["cycle_parent_high"] = SENTINEL_FLOAT
        indicators["cycle_parent_wave_id"] = "NONE"

        # 🔒 [J1-LOW-WINDOW-OPEN-0829-R2]
        # 이번 호출의 LOW_OPEN은 반드시 이번 parent 판정으로 새로 계산한다.
        indicators["j1_low_window_open"] = 0
        # 🔒 [J1-PARENT-WINDOW-STATE-0829]
        # parent state export도 매 호출 중립값에서 시작한다.
        # 실제 값은 Jury에서 현재 cycle_parent_wave_id 기준으로 복원한다.
        indicators["j1_wave_state"] = "UNSEEN"

        indicators["cycle_phase_shadow"] = "COMPRESSION"
        indicators["cycle_detail_shadow"] = "UNKNOWN"
        indicators["cycle_phase_age_shadow"] = -1
        indicators["tlc_context_shadow"] = {}
        # 🔒 [J4-GC-ORIGIN-SSOT-0829] ★414-R1 — J4 직접 입력 stale 차단.
        indicators["cycle_reversal_candle_bull"] = False
        indicators["cycle_reversal_candle_kind"] = "-"
        indicators["gajami_fake_late_reset_gc"] = 0
        
        result = {
            'signal': 0,
            'trend': 0,
            'is_actionable': False,
            'ma_deviation': {'state': 'neutral', 'value': 0},
            'is_bullish': False,
            'is_bearish': False,
            'is_sideways': False,
            'cross_signals': [],
            'alignment_level': 0
        }
        
        if not is_valid_df(df):
            result['analysis_invalid_reason'] = 'ANALYSIS_INVALID_NO_DATA'
            result['analysis_invalid_detail'] = 'Invalid DataFrame'
            return result
        
        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        # 🔒 PATCH A: 가격 괴리 하드 블록 (입력 데이터 신뢰성 검증)
        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        if ticker_price is not None and ticker_price > 0:
            df_price = float(df['close'].iloc[-1])
            gap_pct = abs(df_price - ticker_price) / ticker_price * 100.0
            
            if gap_pct >= 2.0:
                logger.error(
                    f"❌ {symbol} 가격 괴리 심각! "
                    f"DataFrame: {df_price:.2f}원 vs 티커: {ticker_price:.2f}원 "
                    f"(차이: {gap_pct:.4f}%)"
                )
                result['analysis_invalid_reason'] = 'PRICE_DIVERGENCE'
                result['analysis_invalid_detail'] = f'Gap={gap_pct:.2f}%'
                return result
        
        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        # 🔧 PATCH B 제거 (2026-02-07): intrabar MA 계산 복원
        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        # 설계 원칙:
        #   - 시장 체제: 확정봉 기준 (analyze_market_regime에서 캔들 캐시로 보장)
        #   - MA/slope: 30초 단위 intrabar 업데이트 (진입 타이밍 핵심)
        # 기존 PATCH B가 미완성 캔들을 제거하면서:
        #   - indicators['ma5_slope'] (intrabar) vs ma5 (확정봉) 시점 불일치 발생
        #   - 30초 단위 MA 갱신이 무효화되어 진입 타이밍 1봉(3분) 지연
        # df = TechnicalIndicators.drop_incomplete_candle(df, timeframe)  # 삭제
        
        # 🔒 MA120 강제 요구 게이트 제거.
        # - MA60 STRUCT20(20봉 구조 slope) 성립 최소 봉수부터 분석 시작.
        # - MA120은 미생성 상태면 계산 대상에서 제외(억지 rolling(120) 금지), df가
        #   120봉을 채운 뒤 자연 합류. Density 게이트와 동일 상수로 정합.
        _required_ma_bars = SoleusGajamiConstants.MA60_STRUCT20_REQUIRED_BARS
        if len(df) < _required_ma_bars:
            result['analysis_invalid_reason'] = 'ANALYSIS_INVALID_NO_DATA'
            result['analysis_invalid_detail'] = f'Insufficient candles ({len(df)}/{_required_ma_bars})'
            logger.debug(
                f"📊 [ANALYSIS-INVALID] {symbol} | "
                f"reason=NO_DATA | candles={len(df)}/{_required_ma_bars}"
            )
            return result

        ma_periods = [5, 10, 20, 60] + ([120] if len(df) >= 120 else [])
        # MA 계산
        mas = {}
        for period in ma_periods:
            mas[f'ma{period}'] = df['close'].rolling(window=period).mean()
        
        current_price = df['close'].iloc[LAST_INDEX]
        current_mas = {}

        # ER(Efficiency Ratio) — 확정봉 기준 / 로그수익 정규화 / 0~1 클램프
        # 가격이 단순히 올랐느냐가 아니라, **'얼마나 효율적으로(직선에 가깝게) 올랐느냐'**를 측정합니다.
        # 아이디어: 최근 5봉 동안의 가격 변화량(절대치)을 각 봉의 변동폭 합계로 나눕니다.
        # 작동 원리: * 찐 반등: 노이즈 없이 매수세가 집중되어 직선으로 뻗어 나갑니다 (ER 지수 높음).
        # 가짜 반등: 위아래로 심하게 흔들리며 억지로 골든크로스를 만듭니다 (ER 지수 낮음).
        # 적용: ER > 0.6 이상인 구간에서만 MA 골든크로스를 승인합니다.
        try:
            _er_period = 5  # 10->5로 단축, 10봉은 평평해짐
            if len(df) >= _er_period + 2:
                _p = df['close'].iloc[-(_er_period+2):-1].astype(float)  # 확정봉 기준
                _rets = [math.log(_p.iloc[i] / _p.iloc[i-1])
                         for i in range(1, len(_p)) if _p.iloc[i-1] > 0]
                _direction = abs(math.log(_p.iloc[-1] / _p.iloc[0])) if _p.iloc[0] > 0 else 0.0
                _noise = sum(abs(r) for r in _rets)
                _er = (_direction / _noise) if _noise > 0 else 0.0
                indicators['er_10'] = round(max(0.0, min(1.0, _er)), 4)
            else:
                indicators['er_10'] = 0.0
        except Exception:
            indicators['er_10'] = 0.0
            
        prev_mas = {}
        
        for key, ma_series in mas.items():
            if not ma_series.empty and len(ma_series) >= 2:
                current_mas[key] = ma_series.iloc[LAST_INDEX]
                prev_mas[key] = ma_series.iloc[SECOND_LAST_INDEX]
        # 묶음 v5 : 완성봉 MA 를 6_strategy MA HardCut 이 읽는 키로
        for _p5k in (5, 10, 20):
            _v5k = prev_mas.get(f'ma{_p5k}')
            if _v5k is not None and np.isfinite(float(_v5k)) and float(_v5k) > 0:
                indicators[f'sma_{_p5k}_closed'] = float(_v5k)
        # 묶음 v5 : 완성봉 종가 vs 완성봉 MA5 이격 %
        try:
            _c_cl5 = float(df['close'].iloc[SECOND_LAST_INDEX]); _m5_cl5 = float(prev_mas.get('ma5', 0) or 0)
            if np.isfinite(_c_cl5) and np.isfinite(_m5_cl5) and _m5_cl5 > 0:
                indicators['ma5_gap_pct'] = (_c_cl5 / _m5_cl5 - 1.0) * 100.0
        except Exception:
            pass
        # [MA20-FAR-ABOVE] 완성봉과 그 앞 160봉(유효 120봉 이상)만 사용
        try:
            _m20_ser = mas.get('ma20')
            if timeframe == '3m' and _m20_ser is not None and len(_m20_ser) == len(df):
                _dev20 = (df['close'].astype(float) / _m20_ser.astype(float) - 1.0).abs()
                _dev_hist = _dev20.iloc[:SECOND_LAST_INDEX].tail(160)
                _dev_hist = _dev_hist[np.isfinite(_dev_hist)]
                _dev_now = float(_dev20.iloc[SECOND_LAST_INDEX])
                _m20_now = float(_m20_ser.iloc[SECOND_LAST_INDEX])
                _low_now = float(df['low'].iloc[SECOND_LAST_INDEX])
                if (
                    len(_dev_hist) >= 120
                    and np.isfinite(_dev_now)
                    and np.isfinite(_m20_now)
                    and np.isfinite(_low_now)
                    and _m20_now > 0
                ):
                    _dev_med = float(_dev_hist.median())
                    indicators['ma20_dev_closed'] = _dev_now
                    indicators['ma20_dev_med160'] = _dev_med
                    indicators['ma20_far_above'] = int(_low_now > _m20_now and _dev_now > _dev_med)
        except Exception:
            indicators['ma20_far_above'] = 0
        
        # 🔧 MA15 계산: (MA10 + MA20) / 2 (중간 추세) ---> 절대 삭제 금지
        if 'ma10' in current_mas and 'ma20' in current_mas:
            ma10_val = current_mas['ma10']
            ma20_val = current_mas['ma20']
            # None 체크: 둘 다 유효한 값일 때만 계산
            if ma10_val is not None and ma20_val is not None:
                current_mas['ma15'] = (ma10_val + ma20_val) / 2
        
        if 'ma10' in prev_mas and 'ma20' in prev_mas:
            ma10_val = prev_mas['ma10']
            ma20_val = prev_mas['ma20']
            # None 체크: 둘 다 유효한 값일 때만 계산
            if ma10_val is not None and ma20_val is not None:
                prev_mas['ma15'] = (ma10_val + ma20_val) / 2
        
        if not current_mas:
            return result
        
        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        # 🆕 Golden Evidence (MA5/10/20 근접 + 양 기울기) 감지
        #    - 원래 Strategy 모듈의 cond1/cond2/cond3 를 Analysis로 이관
        #    - 역할: 골든크로스 직전 "상승 초입 2~3봉" 증거를 MA 점수 시스템에 통합
        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        evidence_pre_golden = False

        # 🔒 SSoT: slope는 calculate_all_indicators()의 indicators만 사용
        try:
            _ma5_s = mas.get("ma5")
            if _ma5_s is not None and len(_ma5_s) >= 3:
                _c0 = float(_ma5_s.iloc[-1])
                _c1 = float(_ma5_s.iloc[-2])
                _p1 = float(_ma5_s.iloc[-3])
                _a = ((_c0 - _p1) / _p1 * 100.0) if _p1 > 0 else 0.0
                _b = ((_c0 - _c1) / _c1 * 100.0) if _c1 > 0 else 0.0
                logger.debug(
                    f"[MA5-SLOPE-AUDIT-CONSUME] {symbol} | "
                    f"has_ind_s5={int('ma5_slope' in indicators)} "
                    f"ind_s5={float(indicators.get('ma5_slope', 0.0) or 0.0):+.5f}% "
                    f"mas_calc={( (_a + _b) / 2.0 ):+.5f}% "
                    f"il-1={_c0:.5f} il-2={_c1:.5f} il-3={_p1:.5f} "
                    f"s5a={_a:+.5f} s5b={_b:+.5f}"
                )
            else:
                logger.debug(
                    f"[MA5-SLOPE-AUDIT-CONSUME] {symbol} | "
                    f"has_ind_s5={int('ma5_slope' in indicators)} "
                    f"ind_s5={float(indicators.get('ma5_slope', 0.0) or 0.0):+.5f}% "
                    f"mas_ma5=None_or_len<3"
                )
        except Exception:
            pass
        ma5_slope_short = float(indicators.get("ma5_slope", 0.0) or 0.0) / 100.0
        ma10_slope_short = float(indicators.get("ma10_slope", 0.0) or 0.0) / 100.0

        gap_ma5_ma10_pct_local = None

        try:
            ma5_now = current_mas.get("ma5")
            ma10_now = current_mas.get("ma10")
            ma20_now = current_mas.get("ma20")

            if ma5_now is not None and ma10_now is not None and float(ma10_now) > 0:
                gap_ma5_ma10_pct_local = (float(ma5_now) - float(ma10_now)) / float(ma10_now) * 100.0

            # 🔥 Golden Evidence 조건 (기존 Strategy cond1/2/3 재현)
            cond1 = (
                ma5_now is not None and ma10_now is not None and
                float(ma5_now) >= float(ma10_now) * 0.9993 and
                ma5_slope_short > 0.0001
            )
            cond2 = (
                ma5_now is not None and ma20_now is not None and
                float(ma5_now) >= float(ma20_now) * 0.9985 and
                ma5_slope_short > 0.0001
            )
            cond3 = (
                ma10_now is not None and ma20_now is not None and
                float(ma10_now) >= float(ma20_now) * 0.9990 and
                ma10_slope_short > 0.0001
            )

            # 🔒 MA120 전환도 SSoT로 통일 (indicators ma120_slope는 STRUCT20/FALLBACK1)
            ma120_slope_ratio = float(indicators.get("ma120_slope", 0.0) or 0.0) / 100.0
            ma120_turning_up = (ma120_slope_ratio > -0.001)

            # 체제 필터: 약세/붕괴 체제에서는 Evidence 무효 (상승 초입만 인정)
            _ma20_slope_short = float(indicators.get('ma20_slope', 0.0) or 0.0) / 100.0
            regime_allowed_for_evidence = (
                regime in [MarketRegime.TRENDING_UP, MarketRegime.RANGING, MarketRegime.BREAKOUT]
                or (
                    regime == MarketRegime.TRENDING_DOWN
                    and ma120_turning_up
                    and ma5_slope_short > 0
                    and ma10_slope_short > 0
                    and _ma20_slope_short >= 0
                )
            )

            if regime_allowed_for_evidence and (cond1 or cond2 or cond3):
                evidence_pre_golden = True

        except Exception as e:
            logger.debug(f"[GoldenEvidence] {symbol} 계산 오류: {e}")
            evidence_pre_golden = False

        # 디버깅 / Strategy 보조 참고용
        result['evidence_pre_golden'] = evidence_pre_golden
        
        # ========================================
        # 🔥 MA15 체크 여부 (config.ini 기반!)
        # ========================================
        ma15 = None
        MA15_SAFETY_BUFFER = 0.025  # 2.5%
        
        # 기본값: 빈 리스트 (체크 안 함)
        if ma_exit_applicable_timeframes is None:
            ma_exit_applicable_timeframes = []
        
        should_check_ma15 = timeframe in ma_exit_applicable_timeframes  # ✅ config 기반!
        
        if should_check_ma15 and len(df) >= 15:
            ma15_series = df['close'].rolling(window=15).mean()
            if not ma15_series.empty:
                ma15 = ma15_series.iloc[LAST_INDEX]
        
        # 1. 크로스 체크
        cross_groups = {
            'primary': [
                (5, 10, 0.30),   # 단기 시동
                (5, 20, 0.35)    # 단기→중기 돌파
            ],
            'secondary': [
                (10, 20, 0.40),  # 중기 전환
                (10, 60, 0.45)   # 체제 전환
            ],
            'tertiary': [
                (20, 60, 0.50)   # 중기 추세 확정
            ]
        }
        
        active_groups = []
        if timeframe in ['1m', '3m', '5m']:
            active_groups = ['primary', 'secondary', 'tertiary']
        elif timeframe in ['10m', '15m', '30m', '1h']:
            active_groups = ['primary', 'secondary']
        else:
            active_groups = ['primary']
        
        cross_score = 0
        cross_signals = []
        golden_count = 0
        dead_count = 0
        golden_event_bonus = 0  # 🆕 교차 이벤트 보너스

        # ============================================================
        # 🔥 골든크로스 지속성 SSoT (조합별 10개 독립 상태)
        # ============================================================
        import time
        now_ts = time.time()
        # tf_key: 정규화된 timeframe 문자열 (가독성용 alias)
        tf_key = timeframe

        if not hasattr(TechnicalIndicators, "_golden_cross_state"):
            TechnicalIndicators._golden_cross_state = {}

        tf_bucket = TechnicalIndicators._golden_cross_state.setdefault(tf_key, {})
        sym_bucket = tf_bucket.setdefault(symbol, {})

        golden_duration_by_key = {}            # e.g. {"5x10": 120.0, ...}
        golden_count_by_group = {"primary": 0, "secondary": 0, "tertiary": 0}

        primary_stable_60 = 0                 # primary 조합 중 duration>=60초 개수
        primary_active_durations = []         # primary 활성 조합 duration 리스트 (min/max용)

        # ============================================================
        # 🆕 PATCH 3: 재시작(메모리 초기화) 시 200봉 기반 지속시간 1회 복원
        # - 목적: in-memory 타이머 리셋으로 인한 duration=0 문제 제거
        # - 원칙: "현재도 active인 조합"만 과거로 연속 유지된 구간을 스캔해 since를 복원
        # - 실행: 프로세스 생애주기 동안 (timeframe, symbol) 1회만
        # ============================================================
        try:
            if not hasattr(TechnicalIndicators, "_gc_bootstrap_done"):
                TechnicalIndicators._gc_bootstrap_done = {}

            boot_tf = TechnicalIndicators._gc_bootstrap_done.setdefault(tf_key, set())

            if symbol not in boot_tf:
                # 타임프레임 초(Upbit 캔들 시작시각 기준)
                tf_map = {
                    '1m': 60, '3m': 180, '5m': 300, '10m': 600,
                    '15m': 900, '30m': 1800, '1h': 3600, '4h': 14400, '1d': 86400
                }
                tf_sec = tf_map.get(tf_key, 180)

                # 활성 그룹(기존 로직과 동일)을 그대로 사용
                all_pairs = []
                for g in active_groups:
                    for sp, lp, _w in cross_groups.get(g, []):
                        all_pairs.append((sp, lp))

                # rolling MA 시리즈는 이미 mas에 존재
                # 최신부터 거꾸로 "연속 유지" 바(봉) 수를 카운트
                def _count_consecutive_true(ma_s, ma_l) -> int:
                    if ma_s is None or ma_l is None:
                        return 0
                    try:
                        n = min(len(ma_s), len(ma_l))
                        if n <= 2:
                            return 0
                        cnt = 0
                        for idx in range(n - 1, -1, -1):
                            a = ma_s.iloc[idx]
                            b = ma_l.iloc[idx]
                            if a is None or b is None:
                                break
                            # NaN 방어
                            try:
                                if float(a) > float(b):
                                    cnt += 1
                                else:
                                    break
                            except Exception:
                                break
                        return cnt
                    except Exception:
                        return 0

                reconstructed = 0

                for sp, lp in all_pairs:
                    ma_s_key = f"ma{sp}"
                    ma_l_key = f"ma{lp}"
                    gc_key = f"{sp}x{lp}"

                    ma_s_series = mas.get(ma_s_key)
                    ma_l_series = mas.get(ma_l_key)

                    # 현재도 active인 조합만 복원
                    cur_s = current_mas.get(ma_s_key, None)
                    cur_l = current_mas.get(ma_l_key, None)
                    if cur_s is None or cur_l is None:
                        continue

                    if float(cur_s) > float(cur_l):
                        bars = _count_consecutive_true(ma_s_series, ma_l_series)
                        if bars > 0:
                            dur = bars * tf_sec
                            # sym_bucket에는 "since timestamp"만 저장 (기존 로직과 정합)
                            sym_bucket[gc_key] = now_ts - dur
                            reconstructed += 1

                logger.debug(
                    f"🔄 [GC-Bootstrap] {symbol} TF={tf_key} | "
                    f"reconstructed_pairs={reconstructed}/{len(all_pairs)} | "
                    f"candles={len(df)}"
                )

                boot_tf.add(symbol)
        except Exception as e:
            logger.debug(f"⚠️ [GC-Bootstrap] {symbol} TF={tf_key} 실패(무시): {e}")

        for group_name in active_groups:
            group_crosses = cross_groups[group_name]
            
            for short_period, long_period, weight in group_crosses:
                ma_short = f'ma{short_period}'
                ma_long = f'ma{long_period}'
                
                if (ma_short in current_mas and ma_long in current_mas and 
                    ma_short in prev_mas and ma_long in prev_mas):
                    
                    # ========================================
                    # 🔥 정배열 상태 체크 (State-based)
                    # ========================================
                    is_golden_state = current_mas[ma_short] > current_mas[ma_long]
                    is_dead_state = current_mas[ma_short] < current_mas[ma_long]
                    
                    # ─────────────────────────────────────────────
                    # ✅ 조합별 지속시간 업데이트 (active=유지, inactive=리셋)
                    # ─────────────────────────────────────────────
                    gc_key = f"{short_period}x{long_period}"

                    if is_golden_state:
                        if gc_key not in sym_bucket:
                            sym_bucket[gc_key] = now_ts
                            dur = 0.0  # ✅ 골든 발생 직후 명시
                        else:
                            dur = now_ts - sym_bucket[gc_key]
                        golden_duration_by_key[gc_key] = float(dur)
                        golden_count_by_group[group_name] = golden_count_by_group.get(group_name, 0) + 1

                        if group_name == "primary":
                            primary_active_durations.append(float(dur))
                            if dur >= 60:
                                primary_stable_60 += 1
                    else:
                        # 종료 → 리셋
                        if gc_key in sym_bucket:
                            del sym_bucket[gc_key]
                        golden_duration_by_key[gc_key] = 0.0
                    
                    # 🆕 교차 이벤트 체크 (Event-based, 보너스용)
                    # - 동율(==)은 골크/데크 아님 (이벤트로 취급 금지)
                    is_golden_event = (prev_mas[ma_short] < prev_mas[ma_long] and 
                                      current_mas[ma_short] > current_mas[ma_long])
                    is_dead_event = (prev_mas[ma_short] > prev_mas[ma_long] and 
                                    current_mas[ma_short] < current_mas[ma_long])
                    
                    # ========================================
                    # 골든크로스 (정배열 상태 기반)
                    # ========================================
                    if is_golden_state:
                        # 🆕 교차 이벤트 발생 시 Fake Cross 검증
                        is_fake_cross = False
                        if is_golden_event:
                            # 2봉 전 데이터 확인 (3봉 이상 있을 때만)
                            if len(df) >= 3:
                                ma_short_series = mas[ma_short]
                                ma_long_series = mas[ma_long]
                                
                                if len(ma_short_series) >= 3 and len(ma_long_series) >= 3:
                                    ma_short_prev2 = ma_short_series.iloc[-3]
                                    ma_long_prev2 = ma_long_series.iloc[-3]
                                    
                                    # Fake Cross 패턴 감지
                                    if ma_short_prev2 > ma_long_prev2:
                                        is_fake_cross = True
                                        logger.debug(
                                            f"⚠️ [FakeCross] {symbol} {short_period}x{long_period} "
                                            f"2봉 내 재교차 감지 → 이벤트 보너스 제외"
                                        )
                        
                        # 기본 가중치 (정배열 상태)
                        adjusted_weight = weight
                        if timeframe in ['1m', '3m', '5m']:
                            adjusted_weight *= 1.2
                        elif timeframe in ['4h', '1d']:
                            adjusted_weight *= 0.8
                        
                        # 정배열 상태 점수 부여
                        cross_score += adjusted_weight
                        golden_count += 1
                        cross_signals.append(f"GS_{short_period}x{long_period}")  # GS = Golden State
                        cross_signals.append(f"GC_{short_period}x{long_period}")  # GC = Legacy (GC-Score 호환)
                        
                        # 🆕 교차 이벤트 보너스 제거: 인위적 가산 금지
                        # (상태 유지/기록은 하되 점수에는 반영하지 않음)
                        if is_golden_event and not is_fake_cross:
                            # event_bonus = adjusted_weight * 0.3  # ❌ 제거: 인위적 보너스
                            # cross_score += event_bonus            # ❌ 제거: 점수 반영
                            golden_event_bonus += 1
                            cross_signals.append(f"GE_{short_period}x{long_period}")  # GE = Golden Event
                            
                            if group_name == 'primary':
                                ma15_info = f", MA15:{ma15:.0f}" if ma15 else ""
                                logger.debug(
                                    f"[MA] {symbol} 🔥 골든크로스 이벤트 {short_period}x{long_period} "
                                    f"(상태: +{adjusted_weight:.2f}, "
                                    f"가격:{current_price:.0f}{ma15_info}) ✅"
                                )
                        else:
                            # 정배열 상태 유지 중 (이벤트 없음)
                            if group_name == 'primary':
                                logger.debug(
                                    f"[MA] {symbol} 정배열 유지 {short_period}x{long_period} "
                                    f"(점수: +{adjusted_weight:.2f})"
                                )
                    
                    # ========================================
                    # 데드크로스 (역배열 상태 기반)
                    # ========================================
                    elif is_dead_state:
                        adjusted_weight = weight
                        if timeframe in ['1m', '3m', '5m']:
                            adjusted_weight *= 1.2
                        elif timeframe in ['4h', '1d']:
                            adjusted_weight *= 0.8
                        
                        # 역배열 상태 점수 차감
                        cross_score -= adjusted_weight
                        dead_count += 1
                        cross_signals.append(f"DS_{short_period}x{long_period}")  # DS = Dead State
                        
                        # 데드크로스 이벤트 발생 시 추가 페널티
                        if is_dead_event:
                            event_penalty = adjusted_weight * 0.3
                            cross_score -= event_penalty
                            cross_signals.append(f"DE_{short_period}x{long_period}")  # DE = Dead Event
                            
                            if group_name == 'primary':
                                logger.info(
                                    f"[MA] {symbol} 🔴 데드크로스 이벤트 {short_period}x{long_period} "
                                    f"(상태: -{adjusted_weight:.2f}, 이벤트: -{event_penalty:.2f}, "
                                    f"가격:{current_price:.0f}) ❌"
                                )
                        else:
                            if group_name == 'primary':
                                logger.debug(
                                    f"[MA] {symbol} 역배열 유지 {short_period}x{long_period} "
                                    f"(점수: -{adjusted_weight:.2f})"
                                )
        
        result['cross_signals'] = cross_signals
        
        result['golden_count'] = golden_count
        indicators['golden_count'] = golden_count
        
        # ============================================================
        # ✅ 골든 지속성 SSoT 출력 (Strategy 소비용)
        # ============================================================
        result["golden_duration_by_key_sec"] = golden_duration_by_key
        result["golden_count_by_group"] = golden_count_by_group

        result["golden_primary_count"] = int(golden_count_by_group.get("primary", 0))
        result["golden_primary_stable_60"] = int(primary_stable_60)

        # 🔍 MEASURE ONLY: 판정 영향 없음 (로그/분석용)
        if primary_active_durations:
            result["golden_primary_min_duration_sec"] = float(min(primary_active_durations))
            result["golden_primary_max_duration_sec"] = float(max(primary_active_durations))
        else:
            result["golden_primary_min_duration_sec"] = 0.0
            result["golden_primary_max_duration_sec"] = 0.0

        # 레거시 호환(전체 max duration) - MEASURE ONLY
        result["golden_duration_sec"] = float(max(golden_duration_by_key.values())) if golden_duration_by_key else 0.0
        result['golden_event_bonus'] = golden_event_bonus  # MEASURE ONLY: 교차 이벤트 보너스 수
        
        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        # 🔥 골든크로스 조합별 난이도 점수 시스템 (2025-12-07 전면 개선)
        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        # 철학:
        #   1. 간격이 벌어진 골크만 인정 (횡보 중 붙어있는 골크 무시)
        #   2. 중장기선 골크일수록 난이도 높음 → 높은 점수
        #   3. 조합 패턴(특히 중장기 선돌파)에 추가 보너스
        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        
        # 골든크로스 감지 (간격 필터 적용)
        has_5x10_gc = "GC_5x10" in cross_signals
        has_5x20_gc = "GC_5x20" in cross_signals
        has_10x20_gc = "GC_10x20" in cross_signals
        has_10x60_gc = "GC_10x60" in cross_signals
        has_20x60_gc = "GC_20x60" in cross_signals
        
        # 🎯 단계 1: 개별 골크 난이도 점수 (기본 점수)
        gc_individual_score = 0.0
        gc_details = []
        
        if has_5x10_gc:
            gc_individual_score += 0.05
            gc_details.append("5>10(+5%)")
        if has_5x20_gc:
            gc_individual_score += 0.08
            gc_details.append("5>20(+8%)")
        if has_10x20_gc:
            gc_individual_score += 0.06
            gc_details.append("10>20(+6%)")
        if has_10x60_gc:
            gc_individual_score += 0.10
            gc_details.append("10>60(+10%)")
        if has_20x60_gc:
            gc_individual_score += 0.08
            gc_details.append("20>60(+8%)")
        
        # 🎯 단계 2: 특수 조합 패턴 보너스 → ❌ 점수 반영 금지 (로그 참고용만)
        pattern_bonus = 0.0  # ✅ 항상 0으로 고정
        pattern_name = ""
        
        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        # ❌ 패턴 감지 로직 전체 제거 (로그도 불필요)
        # - 중장기 선돌파 패턴 (0.35)
        # - 3중 골크 패턴 (0.25)  
        # - 중장기 골크 패턴 (0.20)
        # - 2중 골크 패턴 (0.15)
        # - 부분 골크 패턴 (0.10)
        # 
        # 제거 이유:
        # - 완성도가 높을수록 보너스 증가 → 머리 꼭대기 진입 유도
        # - 인위적 가산으로 신뢰도 착시 발생
        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        
        # 🎯 단계 3: 최종 골크 점수 적용 (개별 점수만 반영)
        if gc_individual_score > 0:
            old_score = cross_score
            total_gc_bonus = gc_individual_score  # ✅ 패턴 보너스 제외
            cross_score *= (1 + total_gc_bonus)
            
            logger.info(
                f"💎 [GC-Score] {symbol} 골든크로스 점수 | "
                f"개별:{gc_individual_score*100:.1f}% ({', '.join(gc_details)}) | "
                f"총:{total_gc_bonus*100:.1f}% | "
                f"{old_score:.3f} → {cross_score:.3f}"
            )
        
        # ============================================================
        # ✅ [레거시 제거] 단일 골든 타이머(_last_golden_cross) 사용 금지
        # - 조합별 지속성 SSoT(result["golden_duration_by_key_sec"])가 진실
        # - 필요한 경우 레거시 값은 result["golden_duration_sec"]로 대체
        # ============================================================
        golden_duration = float(result.get("golden_duration_sec", 0.0) or 0.0)

        # 기존 신선도 페널티는 4h/1d에서만 유지 (원래 의도 유지)
        golden_hours = golden_duration / 3600.0
        tf_value = timeframe  
        mid_long_tfs = ["4h", "1d"]

        if tf_value in mid_long_tfs:
            max_valid_hours_by_tf = {"4h": 36.0, "1d": 72.0}
            max_valid_hours = max_valid_hours_by_tf.get(tf_value, 48.0)

            if golden_hours > max_valid_hours:
                stale_penalty = 0.90
                old_score = cross_score
                cross_score *= stale_penalty
                logger.warning(
                    f"⚠️ [MA-Stale] {symbol} 골든크로스 신선도 저하 ({tf_value} 모드) | "
                    f"경과:{golden_hours:.1f}H > 기준:{max_valid_hours:.1f}H | "
                    f"점수: {old_score:.3f} → {cross_score:.3f} (×{stale_penalty})"
                )
        
        # 크로스 일관성 보너스/패널티
        # - golden_count는 "현재 프레임에서 골든 상태 조합 수(스냅샷)"
        # - 과도한 골든(8개 이상)은 꼭지/후반 가능성 → 보너스 제거
        GC_OVERHEAT_CAP = 5  # 최대 5개 쌍 기준
        
        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        # ✅ 골든크로스 개수 스테이지 점수 (역U자: 초입↑, 과열↓)
        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        # 목적: 1~2개 초입을 통과시키고, 8~9개 과열을 감점/차단 방향으로 유도
        # 철학: "가자미근 탄생" 시점이 최고점, "완성" 시점은 최저점
        gc_stage_score = 0.0
        if golden_count == 1:
            gc_stage_score = 0.40  # 시작 단계
        elif golden_count == 2:
            gc_stage_score = 0.50  # 최적 진입! ⭐
        elif golden_count == 3:
            gc_stage_score = 0.45  # 초중반
        elif golden_count == 4:
            gc_stage_score = 0.40  # strong early/mid
        elif golden_count >= 5:
            gc_stage_score = 0.30  # mid/late
        # (제거: max 5개 기준으로 7~9 조건 도달 불가)
        
        result["gc_stage_score"] = gc_stage_score  # 로그/검증용
        
        # cross_score에 스테이지 반영 (피크=0.50을 1.0으로 정규화)
        if gc_stage_score > 0:
            stage_multiplier = gc_stage_score / 0.50
            old_cross = cross_score
            cross_score *= stage_multiplier
            logger.info(
                f"📊 [GC-Stage] {symbol} 골든 {golden_count}개 → "
                f"stage_score={gc_stage_score:.2f} (×{stage_multiplier:.2f}) | "
                f"{old_cross:.3f} → {cross_score:.3f}"
            )
        
        # 기존 일관성 보너스는 "초입 보호(3~7)"에만 제한적으로 유지
        if golden_count >= 3 and dead_count == 0:
            if golden_count >= GC_OVERHEAT_CAP:
                # 과열 구간: 스테이지 감점 이미 적용됨 (추가 보너스 없음)
                logger.debug(
                    "[MA] %s 과도 골든 상태 %d개(>=%d) → 과열구간(스테이지 감점 적용)",
                    symbol, golden_count, GC_OVERHEAT_CAP
                )
            else:
                # 3~7개 구간: 일관성 보너스 유지 (초입 보호)
                cross_score *= 1.3
                logger.debug("[MA] %s 일관된 골든크로스 %d개! (보너스 30%%)", symbol, golden_count)
        elif dead_count >= 3 and golden_count == 0:
            cross_score *= 1.3
            logger.debug("[MA] %s 일관된 데드크로스 %d개! (강화 30%%)", symbol, dead_count)
        elif golden_count > 0 and dead_count > 0:
            cross_score *= 0.7
            logger.debug("[MA] %s 상충 신호: 골든 %d개 vs 데드 %d개 (약화 30%%)", symbol, golden_count, dead_count)
        
        # 2. 정배열/역배열
        alignment_score = 0
        
        # 🆕 역배열 플래그 초기화
        result['is_bearish_alignment'] = False
        result['is_partial_bearish'] = False
        
        if 'ma5' in current_mas and 'ma10' in current_mas and 'ma20' in current_mas:
            if current_price > current_mas['ma5'] > current_mas['ma10'] > current_mas['ma20']:
                alignment_score = 0.3
                result['alignment_level'] = 3
                result['is_bullish'] = True
                
                if 'ma60' in current_mas and current_mas['ma20'] > current_mas['ma60']:
                    alignment_score = 0.5
                    result['alignment_level'] = 4
                    
                    if 'ma120' in current_mas and current_mas['ma60'] > current_mas['ma120']:
                        alignment_score = 0.7
                        result['alignment_level'] = 5
            
            elif current_price < current_mas['ma5'] < current_mas['ma10'] < current_mas['ma20']:
                alignment_score = -0.3
                result['alignment_level'] = -3
                result['is_bearish'] = True
                
                if 'ma60' in current_mas and current_mas['ma20'] < current_mas['ma60']:
                    alignment_score = -0.5
                    result['alignment_level'] = -4
                    result['is_bearish_alignment'] = True  # ✅ 완전 역배열!
                    
                    if 'ma120' in current_mas and current_mas['ma60'] < current_mas['ma120']:
                        alignment_score = -0.7
                        result['is_bearish_alignment'] = True  # ✅ 더 강한 완전 역배열!
                        result['alignment_level'] = -5
            
            # 🟡 부분 역배열 체크 (현재가 < MA20 & 단기 MA 역배열)
            elif current_price < current_mas['ma20'] and current_mas['ma5'] < current_mas['ma10'] < current_mas['ma20']:
                result['is_partial_bearish'] = True  # ✅ 부분 역배열!
        
        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        # 🆕 정배열 전환 감지 (패치 A)
        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        result['is_alignment_transition'] = False
        result['transition_strength'] = 0.0
        
        if len(df) >= 2 and 'ma5' in prev_mas and 'ma10' in prev_mas and 'ma20' in prev_mas:
            try:
                # 이전 봉 alignment_level 계산 (간소화 버전)
                prev_price = df['close'].iloc[SECOND_LAST_INDEX]
                prev_alignment = 0
                
                # 이전 봉이 정배열이었는지 체크
                if 'ma60' in prev_mas:
                    if prev_price > prev_mas['ma5'] > prev_mas['ma10'] > prev_mas['ma20'] > prev_mas['ma60']:
                        prev_alignment = 4
                    elif prev_price > prev_mas['ma5'] > prev_mas['ma10'] > prev_mas['ma20']:
                        prev_alignment = 3
                elif prev_price > prev_mas['ma5'] > prev_mas['ma10'] > prev_mas['ma20']:
                    prev_alignment = 3
                
                curr_alignment = result.get('alignment_level', 0)
                
                # 전환 감지: 미정배열(0~3) → 강정배열(4~5)
                if prev_alignment < 4 and curr_alignment >= 4:
                    result['is_alignment_transition'] = True
                    result['transition_strength'] = 1.0 if curr_alignment >= 5 else 0.8
                    
                    logger.debug(
                        f"🔄 [정배열전환] {symbol} {prev_alignment} → {curr_alignment} "
                        f"(강도: {result['transition_strength']:.1f})"
                    )
            
            except Exception as e:
                logger.debug(f"정배열 전환 감지 오류: {e}")
        
        # 3. 이격도
        ma_short = current_mas.get('ma5', current_mas.get('ma10', 0))
        if ma_short > 0:
            ma_deviation = ((current_price - ma_short) / ma_short) * PERCENT_CONVERSION
            result['ma_deviation']['value'] = ma_deviation
            
            # 🔥 체제별 임계값 가져오기
            if regime:
                regime_key = regime.value  # 'TRENDING_UP', 'RANGING', etc.
                regime_thresholds = MA_DEVIATION_THRESHOLDS.get(
                    regime_key, 
                    MA_DEVIATION_THRESHOLDS['RANGING']
                )
                
                tf_thresholds = regime_thresholds.get(
                    timeframe, 
                    regime_thresholds['default']
                )
                
                logger.debug(
                    f"[MA이격도] {symbol} {regime_key} | "
                    f"현재:{ma_deviation:.2f}% | "
                    f"허용범위:{tf_thresholds['oversold']:.1f}~{tf_thresholds['overbought']:.1f}%"
                )
            else:
                # 체제 정보 없으면 기존 방식 (하위 호환성)
                tf_thresholds = MA_ANALYSIS_THRESHOLDS.get(timeframe, MA_ANALYSIS_DEFAULT)
            
            # 과매수/과매도 판단
            if ma_deviation < tf_thresholds['oversold']:
                result['ma_deviation']['state'] = 'oversold'
                logger.debug("[MA이격도] %s 과매도 (%.2f%%)", symbol, ma_deviation)
            elif ma_deviation > tf_thresholds['overbought']:
                result['ma_deviation']['state'] = 'overbought'
                logger.debug("[MA이격도] %s 과매수 (%.2f%%)", symbol, ma_deviation)
            else:
                result['ma_deviation']['state'] = 'neutral'
        
        # 4. 추세
        if 'ma20' in current_mas:
            ma20_series = mas['ma20']  # ✅ 이미 계산된 시리즈 재사용
            
            # 형성 중 봉을 제외한 확정봉 4봉 스팬.
            if len(ma20_series) >= 6 and float(ma20_series.iloc[-6]) != 0.0:
                ma20_slope = (
                    ma20_series.iloc[-2] - ma20_series.iloc[-6]
                ) / ma20_series.iloc[-6]
                result['trend'] = ma20_slope
            else:
                result['trend'] = 0
        
        # ========================================
        # 🆕 5. 개별 MA 이격도 (Deviation) - 각 MA별 현재가 이격
        # ========================================
        ma_deviations = {}
        for ma_period in [5, 10, 15, 20]:
            ma_key = f'ma{ma_period}'
            if ma_key in current_mas and current_mas[ma_key] > 0:
                deviation_pct = ((current_price - current_mas[ma_key]) / current_mas[ma_key]) * 100
                ma_deviations[ma_key] = deviation_pct
        
        result['ma_deviations'] = ma_deviations
        
        # 평균 이격도 계산
        if ma_deviations:
            result['avg_deviation'] = sum(ma_deviations.values()) / len(ma_deviations)
            logger.debug(
                f"📏 [개별MA이격도] {symbol} | "
                f"MA5: {ma_deviations.get('ma5', 0):.2f}% | "
                f"MA10: {ma_deviations.get('ma10', 0):.2f}% | "
                f"MA15: {ma_deviations.get('ma15', 0):.2f}% | "
                f"MA20: {ma_deviations.get('ma20', 0):.2f}% | "
                f"평균: {result['avg_deviation']:.2f}%"
            )
        else:
            result['avg_deviation'] = 0
        
        # ========================================
        # 🆕 6. MA 간격 (Spacing) - MA끼리 얼마나 벌어져 있는지
        # ========================================
        ma_spacings = {}
        ma_pairs = [
            ('ma5', 'ma10', '5-10'),
            ('ma10', 'ma15', '10-15'),
            ('ma15', 'ma20', '15-20'),
            ('ma5', 'ma20', '5-20')  # 전체 벌어짐
        ]
        
        for ma1_key, ma2_key, label in ma_pairs:
            if ma1_key in current_mas and ma2_key in current_mas:
                if current_mas[ma2_key] > 0:
                    spacing_pct = ((current_mas[ma1_key] - current_mas[ma2_key]) / current_mas[ma2_key]) * 100
                    ma_spacings[label] = spacing_pct
        
        result['ma_spacings'] = ma_spacings
        
        # 평균 간격 계산
        if ma_spacings:
            result['avg_spacing'] = round(sum(ma_spacings.values()) / len(ma_spacings), 3)
            logger.debug(
                f"📐 [MA간격] {symbol} | "
                f"5-10: {ma_spacings.get('5-10', 0):.2f}% | "
                f"10-15: {ma_spacings.get('10-15', 0):.2f}% | "
                f"15-20: {ma_spacings.get('15-20', 0):.2f}% | "
                f"5-20: {ma_spacings.get('5-20', 0):.2f}% | "
                f"평균: {result['avg_spacing']:.3f}%"
            )
        else:
            result['avg_spacing'] = 0
        
        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        # 🆕 [MA 간격 기반 조기 진입 점수] 2025-11-12
        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        ma_gap_score = 0.0
        ma_gap_meta = {}
        
        # MA 간격 조기 진입 활성화 여부 확인
        enable_ma_gap = getattr(TechnicalIndicators, 'enable_ma_gap_early_entry', True)
        
        # ═══════════════════════════════════════════════════════
        # 🆕 E-GC 체제 필터: PRE_UP, TRENDING_UP만 허용
        # ═══════════════════════════════════════════════════════
        e_gc_regime_allowed = False
        if regime is not None:
            # MarketRegime enum 값 확인 (실제 enum 이름에 따라 조정 필요)
            allowed_regimes = [
                MarketRegime.TRENDING_UP,
                # PRE_UP이 정의되어 있다면 추가
                # MarketRegime.PRE_UP,
            ]
            e_gc_regime_allowed = regime in allowed_regimes
        
        if enable_ma_gap and e_gc_regime_allowed and (
            golden_count > 0                      # 1) 기존 골든크로스
            or evidence_pre_golden               # 2) 🔥 Golden Evidence (cond1~3)
            or (current_mas.get('ma5', 0) > current_mas.get('ma10', 0) >= current_mas.get('ma15', 0))  # 3) 정배열 초입
        ):
            ma5_val = current_mas.get('ma5', 0)
            ma10_val = current_mas.get('ma10', 0)
            ma15_val = current_mas.get('ma15', 0)
            ma20_val = current_mas.get('ma20', 0)
            ma60_val = current_mas.get('ma60', 0)
            
            if all([ma5_val > 0, ma10_val > 0, ma15_val > 0, ma20_val > 0]):
                # 🔥 @staticmethod 추가 필요 (라인 508에서) 또는 클래스명 명시
                ma_gap_score, ma_gap_meta = TechnicalIndicators.calculate_ma_gap_score(
                    ma5_val, ma10_val, ma15_val, ma20_val, ma60_val,
                    current_price, symbol
                )
                
                # ═══════════════════════════════════════════════════════
                # 🆕 문턱 리프트 (+0.02): ATR + Volume 조건 확인
                # ═══════════════════════════════════════════════════════
                if ma_gap_score > 0.75:  # 기존: 유의미한 점수일 때만 로그
                    logger.info(
                        f"[Early GC] {symbol} | "
                        f"간격:{ma_gap_meta['gap_5_10']:.3f}% | "
                        f"단계:{ma_gap_meta['stage']} | "
                        f"MA간격점수:{ma_gap_score:.2f} | "
                        f"최적:{ma_gap_meta['optimal']}"
                    )
                    
                    # 🔥 문턱 리프트 조건 체크
                    # ATR: 0.45~3.5% 범위 확인 (별도 변수 필요 - indicators에서 전달)
                    # Volume: ≥0.8x 확인
                    # 이 조건들은 strategy.py에서 최종 진입 시 적용
                    ma_gap_meta['threshold_lift_eligible'] = True  # 플래그 전달
        
        # MA 간격 점수를 result에 추가
        result['ma_gap_score'] = ma_gap_score
        result['ma_gap_meta'] = ma_gap_meta

        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        # 🆕 MA20–MA60 이격도(%) + 변화율(수렴/확산) (횡보 예측 SSoT)
        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        ma20_val = float(current_mas.get('ma20', 0) or 0)
        ma60_val = float(current_mas.get('ma60', 0) or 0)

        if ma20_val > 0 and ma60_val > 0:
            ma20_ma60_gap_pct = abs(ma20_val - ma60_val) / ma60_val * 100.0

            # 최근 10봉 전 대비 이격 변화 (음수 = 수렴 시작)
            try:
                if len(df) >= 10 and mas.get('ma20') is not None and mas.get('ma60') is not None:
                    ma20_prev = float(mas['ma20'].iloc[-10])
                    ma60_prev = float(mas['ma60'].iloc[-10])
                    if ma60_prev > 0:
                        gap_prev = abs(ma20_prev - ma60_prev) / ma60_prev * 100.0
                        ma20_ma60_gap_delta = ma20_ma60_gap_pct - gap_prev
                    else:
                        ma20_ma60_gap_delta = 0.0
                else:
                    ma20_ma60_gap_delta = 0.0
            except Exception:
                ma20_ma60_gap_delta = 0.0
        else:
            ma20_ma60_gap_pct = 0.0
            ma20_ma60_gap_delta = 0.0

        result['ma20_ma60_gap_pct'] = float(ma20_ma60_gap_pct)
        result['ma20_ma60_gap_delta'] = float(ma20_ma60_gap_delta)
        
        # ========================================
        # 7. 최종 신호 - 🔥 하이브리드 보정 (골든크로스 여부에 따라 차등 적용)
        # ========================================
        # 기본 신호
        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        # 🎯 MA 간격 점수를 SSoT로 사용 (위치 기반 우선)
        # - 철학: MA는 완성도가 아니라 진입 위치를 측정
        # - cross_score는 ma_gap_score가 없을 때만 fallback
        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        
        if ma_gap_score > 0:
            # 위치 기반 점수를 기본 신호로 사용
            base_signal = ma_gap_score
            
            logger.debug(
                f"📊 [MA간격기준] {symbol} | "
                f"ma_gap_score={ma_gap_score:.2f} 적용"
            )
        
        else:
            # MA 간격 점수가 없을 때만 fallback
            base_signal = cross_score + alignment_score * 0.8
            
            logger.debug(
                f"📊 [Fallback-Cross] {symbol} | "
                f"cross_score={cross_score:.2f} "
                f"alignment={alignment_score:.2f}"
            )
        
        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        # 🎯 핵심: 골든크로스가 있으면 보정 완화/면제 (단, 하락 구조에서는 권한 박탈)
        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        alignment_level = int(result.get('alignment_level', 0) or 0)
        
        # ✅ "권한용 골든" 분리: 강한 하락(단기 역배열)에서는 골든 권한 = 0
        # 철학: "골든/데드 + 정배열/역배열 + 간격/기울기" 세트 판단 복귀
        golden_count_effective = golden_count
        if alignment_level <= -3:
            golden_count_effective = 0
        
        result['golden_count_effective'] = golden_count_effective  # 디버그/검증용
        
        has_golden_cross = golden_count_effective > 0
        has_strong_golden_cross = golden_count_effective >= 2  # 2개 이상
        
        # 보정 계수 초기화
        spacing_factor = 1.0
        deviation_factor = 1.0
        
        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        # 🆕 공통: 절대적 기준 먼저 계산 (모든 골든크로스 상태에서 사용)
        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        # 극도로 좁은 간격은 골든크로스와 무관하게 위험
        if abs(result['avg_spacing']) <= 0.05:  # 0.05% 미만 (극도로 평행)
            absolute_spacing_factor = 0.6  # 40% 감소
        elif abs(result['avg_spacing']) <= 0.1:  # 0.1% 미만
            absolute_spacing_factor = 0.7  # 30% 감소
        else:
            absolute_spacing_factor = 1.0  # 보정 없음
        
        # 🧩 골든크로스 직후 회복 구간 완화 (무릎 구간 진입 허용)
        if has_golden_cross:
            # 골든크로스가 있으면 절대 기준 완화 → 최소 0.85 보장
            original_absolute = absolute_spacing_factor
            absolute_spacing_factor = max(absolute_spacing_factor, 0.85)
            if original_absolute < 0.85:
                logger.debug(
                    f"🧩 [무릎구간] {symbol} 골든크로스 감지 → "
                    f"절대기준 완화 ({original_absolute:.2f} → 0.85)"
                )
        
        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        # 이제 골든크로스 상태별 분기
        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        if has_strong_golden_cross:
            # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
            # 강한 골든크로스 (2개 이상): 보정 면제 ⭐
            # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
            # "무릎에서 사기" 핵심!
            spacing_factor = 1.0
            deviation_factor = 1.0
            
            # 🧩 선택적 보완: 극단적 괴리는 최소 보정 (5% 이상)
            # 강한 골든크로스여도 가격이 너무 멀면 "어깨에서 사는" 위험
            if abs(result['avg_deviation']) > 5.0:  # 5% 초과
                deviation_factor = 0.95  # 5% 감소 (최소 보정)
                logger.debug(
                    f"🧩 [괴리보정] {symbol} 강한 골든크로스 {golden_count}개이나 "
                    f"극단적 괴리({result['avg_deviation']:.2f}%) → 최소 보정 적용"
                )
            else:
                logger.debug(f"✅ [신호보정] {symbol} 강한 골든크로스 {golden_count}개 → 보정 면제")
            
        elif has_golden_cross:
            # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
            # 약한 골든크로스 (1개): 최소 보정만
            # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
            # 극단적인 경우만 약간의 페널티
            
            # 1. MA 간격 보정 (완화 버전)
            if abs(result['avg_spacing']) <= 0.05:  # 0.05% 미만 (극도로 평행)
                weak_spacing_factor = 0.9  # 10% 감소 (기존 40% → 10%)
            elif abs(result['avg_spacing']) <= 0.15:  # 0.15% 미만 (매우 가까움)
                weak_spacing_factor = 0.95  # 5% 감소 (기존 20% → 5%)
            else:
                weak_spacing_factor = 1.0  # 보정 없음
            
            # 🆕 절대 기준과 비교 (더 엄격한 것 선택)
            spacing_factor = min(absolute_spacing_factor, weak_spacing_factor)
            
            # 2. 가격 이격도 보정 (🆕 개선 버전 - 부드러운 단계)
            if abs(result['avg_deviation']) < 0.1:  # 0.1% 미만 (극도로 붙어있음)
                deviation_factor = 0.9  # 10% 감소
            elif abs(result['avg_deviation']) < 0.3:  # 0.3% 미만 (가까움)
                deviation_factor = 0.95  # 5% 감소
            elif abs(result['avg_deviation']) > 5.0:  # 5.0% 초과 (너무 멀리)
                deviation_factor = 0.9  # 10% 감소
            else:
                deviation_factor = 1.0  # 보정 없음 (0.3~5.0% 구간은 정상)
            
            if spacing_factor < 1.0 or deviation_factor < 1.0:
                logger.debug(
                    f"📊 [신호보정] {symbol} 약한 골든크로스 → 최소 보정 "
                    f"(절대:{absolute_spacing_factor:.2f}x, "
                    f"약한:{weak_spacing_factor:.2f}x, "
                    f"최종간격:{spacing_factor:.2f}x, 이격:{deviation_factor:.2f}x)"
                )
        
        else:
            # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
            # 골든크로스 없음: 체제별 + 타임프레임별 보정 적용
            # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
            
            # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
            # 🆕 1. 체제별 임계값 설정
            # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
            # 체제별로 MA 간격 판단 기준을 다르게 설정
            if regime == MarketRegime.RANGING:
                spacing_limit = 0.15  # 횡보: 완화 (0.125 → 0.15)
            elif regime in [MarketRegime.TRENDING_UP, MarketRegime.BREAKOUT]:
                spacing_limit = 0.10  # 상승/돌파: 더 완화 (차단 대신 감산만)
            elif regime in [MarketRegime.BREAKDOWN, MarketRegime.TRENDING_DOWN]:
                spacing_limit = 0.125  # 하락: 기존 유지 (보수적)
            else:
                spacing_limit = 0.125  # 기본값 (regime이 None이거나 미정의)
            
            # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
            # 2. MA 간격 보정 (체제별 + 타임프레임별)
            # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
            
            # 2-1. 타임프레임별 기준 계산 (기존 로직)
            if timeframe   in ['1m', '3m']:
                if abs(result['avg_spacing']) <= 0.15:
                    tf_spacing_factor = 0.7
                elif abs(result['avg_spacing']) <= 0.4:
                    tf_spacing_factor = 0.85
                elif abs(result['avg_spacing']) <= 0.6:
                    tf_spacing_factor = 0.95
                else:
                    tf_spacing_factor = 1.0
            
            elif timeframe   in ['5m', '10m', '15m']:
                if abs(result['avg_spacing']) <= 0.1:
                    tf_spacing_factor = 0.6
                elif abs(result['avg_spacing']) <= 0.3:
                    tf_spacing_factor = 0.8
                elif abs(result['avg_spacing']) <= 0.5:
                    tf_spacing_factor = 0.9
                else:
                    tf_spacing_factor = 1.0
            
            else:  # 장기
                if abs(result['avg_spacing']) <= 0.08:
                    tf_spacing_factor = 0.5
                elif abs(result['avg_spacing']) <= 0.25:
                    tf_spacing_factor = 0.7
                elif abs(result['avg_spacing']) <= 0.4:
                    tf_spacing_factor = 0.85
                else:
                    tf_spacing_factor = 1.0
            
            # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
            # 🆕 2-2. 체제별 조정 (핵심 개선!)
            # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
            
            # 🟢 상승/돌파 체제: 과도한 감산 방지
            if regime in [MarketRegime.TRENDING_UP, MarketRegime.BREAKOUT]:
                if abs(result['avg_spacing']) < spacing_limit:
                    # 🎯 튜닝 포인트 1: penalty 계수
                    # - 0.4: 공격적 (강세장)
                    # - 0.5: 균형 (기본값) ✅
                    # - 0.6: 보수적 (약세장)
                    confidence_penalty = (spacing_limit - abs(result['avg_spacing'])) * 0.5
                    
                    # 🎯 튜닝 포인트 2: 최소 보장선
                    # - 0.80: 보수적 (20% 감산 허용)
                    # - 0.85: 균형 (15% 감산 허용) ✅
                    # - 0.90: 공격적 (10% 감산만)
                    regime_spacing_factor = max(0.85, 1.0 - confidence_penalty)
                    
                    logger.debug(
                        f"⚙️ [상승체제완화] {symbol} 체제:{regime.value} | "
                        f"spacing {result['avg_spacing']:.3f}% "
                        f"→ 신뢰도 {confidence_penalty*100:.1f}% 감산 "
                        f"(factor: {regime_spacing_factor:.3f}, 최소보장: 0.85)"
                    )
                else:
                    regime_spacing_factor = 1.0
                
                # 타임프레임 기준과 체제 기준 중 덜 엄격한 것 선택
                spacing_factor = max(tf_spacing_factor, regime_spacing_factor)
            
            # 🟡 횡보 체제: 완화된 기준 적용
            elif regime == MarketRegime.RANGING:
                if abs(result['avg_spacing']) < spacing_limit:
                    # 🎯 튜닝 포인트 3: RANGING 감산 폭
                    # - 0.85: 15% 감소 (거래 빈도 ↑)
                    # - 0.80: 20% 감소 (균형) ✅
                    # - 0.75: 25% 감소 (거래 빈도 ↓)
                    regime_spacing_factor = 0.80
                    
                    logger.debug(
                        f"⚙️ [횡보완화] {symbol} spacing {result['avg_spacing']:.3f}% "
                        f"< {spacing_limit:.3f}% → 감산 20% (factor: 0.80)"
                    )
                else:
                    regime_spacing_factor = 1.0
                
                # 타임프레임 기준과 비교하여 더 엄격한 것 선택
                spacing_factor = min(tf_spacing_factor, regime_spacing_factor)
            
            # 🔴 하락 체제: 기존 로직 유지 (엄격)
            elif regime in [MarketRegime.BREAKDOWN, MarketRegime.TRENDING_DOWN]:
                spacing_factor = min(absolute_spacing_factor, tf_spacing_factor)
                
                if spacing_factor < 1.0:
                    logger.debug(
                        f"⚠️ [하락체제엄격] {symbol} 체제:{regime.value} | "
                        f"spacing {result['avg_spacing']:.3f}% "
                        f"→ 엄격 필터 적용 (factor: {spacing_factor:.2f})"
                    )
            
            # ⚪ 체제 미정의 (None): 기존 타임프레임 로직 사용
            else:
                spacing_factor = min(absolute_spacing_factor, tf_spacing_factor)
                
                if spacing_factor < 1.0 and regime is not None:
                    logger.debug(
                        f"⚙️ [기본필터] {symbol} 체제:{regime.value if regime else 'None'} | "
                        f"spacing {result['avg_spacing']:.3f}% "
                        f"→ 기본 필터 적용 (factor: {spacing_factor:.2f})"
                    )
            
            # 2-3. 절대 기준과 최종 비교 (안전장치)
            # 극단적으로 좁은 간격은 체제 관계없이 감산
            spacing_factor = min(spacing_factor, absolute_spacing_factor)
            
            # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
            # 3. 가격 이격도 보정 (기존 로직 유지)
            # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
            if abs(result['avg_deviation']) < 0.2:
                deviation_factor = 0.7
            elif abs(result['avg_deviation']) < 0.5:
                deviation_factor = 0.85
            elif abs(result['avg_deviation']) < 1.0:
                deviation_factor = 0.95
            elif abs(result['avg_deviation']) > 4.0:
                deviation_factor = 0.8
            else:
                deviation_factor = 1.0
            
            if spacing_factor < 1.0 or deviation_factor < 1.0:
                logger.debug(
                    f"📊 [신호보정] {symbol} 체제:{regime.value if regime else 'None'} | "
                    f"TF:{timeframe} | 절대:{absolute_spacing_factor:.2f}x | "
                    f"TF:{tf_spacing_factor:.2f}x | 최종간격:{spacing_factor:.2f}x | "
                    f"이격:{deviation_factor:.2f}x"
                )
        
        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        # 최종 신호 계산
        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        final_factor = spacing_factor * deviation_factor
        
        # ✅ 최소 보장: 골든크로스 여부에 따라 차등 보장
        if has_golden_cross:
            MIN_SIGNAL_RETENTION = 0.80  # 골든크로스: 80% 보장 ⭐
        else:
            # 체제별 동적 임계값 조정
            if regime == MarketRegime.RANGING:
                # 🎯 튜닝 포인트 4: RANGING 임계값
                # - 0.40: 공격적 (거래 빈도 ↑↑)
                # - 0.45: 균형 (추천) ✅
                # - 0.50: 보수적 (거래 빈도 ↓)
                MIN_SIGNAL_RETENTION = 0.45  # 횡보: 완화
                
            elif regime == MarketRegime.TRENDING_UP:
                # 🎯 튜닝 포인트 5: TRENDING_UP 임계값
                # - 0.42: 공격적 (반등 기회 ↑)
                # - 0.47: 균형 (추천) ✅
                # - 0.52: 보수적 (선별력 ↑)
                MIN_SIGNAL_RETENTION = 0.47  # 상승: 균형
                
            elif regime in [MarketRegime.TRENDING_DOWN, MarketRegime.BREAKDOWN]:
                # 🎯 튜닝 포인트 6: 하락장 임계값
                # - 0.45: 공격적 (위험!)
                # - 0.50: 균형 (추천) ✅
                # - 0.55: 보수적 (안전)
                MIN_SIGNAL_RETENTION = 0.50  # 하락: 보수
                
            else:
                # BREAKOUT 또는 regime이 None인 경우
                MIN_SIGNAL_RETENTION = 0.47  # 기본값
        
        # 최소값 보장 적용
        if final_factor < MIN_SIGNAL_RETENTION:
            final_factor = MIN_SIGNAL_RETENTION
            
            # 보정 적용 시 로그
            logger.debug(
                f"🔒 [최소보장] {symbol} 체제:{regime.value if regime else 'None'} | "
                f"final_factor {spacing_factor * deviation_factor:.3f} "
                f"→ {MIN_SIGNAL_RETENTION:.3f} 보장 적용"
            )
            
        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        # 🔧 STEP 1: 신호 역할 분리 (85% 구조 핵심)
        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        
        # 1️⃣ 방향/강도 (기존 base_signal 그대로)
        result['signal_strength'] = base_signal          # -1.0 ~ +1.0
        
        # 2️⃣ 패널티 계수 (기존 final_factor 그대로)
        result['penalty_factor'] = final_factor          # 0.7 ~ 1.0
        
        # 3️⃣ 최종 실행 신호 (기존 signal 의미)
        result['final_signal'] = (
            result['signal_strength'] * result['penalty_factor']
        )
        
        # 4️⃣ 하위 호환용 (기존 Strategy 깨지지 않게 유지)
        result['signal'] = result['final_signal']
        
        logger.debug(
            f"✅ [SignalSplit] {symbol} | "
            f"strength={result['signal_strength']:.3f}, "
            f"penalty={result['penalty_factor']:.3f}, "
            f"final={result['final_signal']:.3f}"
        )

        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        # 🆕 정배열 전환 보정 + ATR 기반 활성도 체크
        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        transition_boost = 1.0
        volatility_gate = 1.0
        
        # 1) 정배열 전환 직후 + 골든크로스 동반 → 20% 강화
        if result.get('is_alignment_transition', False) and result.get('golden_count_effective', 0) >= 1:
            transition_boost = 1.2
            logger.debug(
                f"⚡ [전환강화] {symbol} 정배열전환+GC → 1.2x "
                f"(강도: {result.get('transition_strength', 0):.1f})"
            )
        
        # 2) ATR 기반 활성도 체크 (움직임 없는 정배열 차단)
        if len(df) >= 14:
            # ATR 계산 (간소화 버전)
            high_low = df['high'].tail(14) - df['low'].tail(14)
            atr_value = high_low.mean()
            
            current_price = df['close'].iloc[LAST_INDEX]
            if current_price > 0 and atr_value > 0:
                atr_pct = (atr_value / current_price) * 100
                
                # ATR < 0.1%: 움직임 없음 → 신호 감산
                if atr_pct < 0.10:
                    volatility_gate = max(0.5, atr_pct / 0.10)  # 0.5~1.0
                    logger.debug(
                        f"📉 [활성도차단] {symbol} ATR {atr_pct:.3f}% < 0.10% → {volatility_gate:.2f}x"
                    )
                
                result['atr_pct'] = atr_pct
        
        # 최종 적용 (통합 계산)
        result['signal'] = result['signal'] * transition_boost * volatility_gate
        
        # 디버그 로그 (보정 발생 시에만)
        if final_factor < 1.0:
            logger.debug(
                f"🔧 [신호보정] {symbol} | "
                f"기본: {base_signal:.3f} → 최종: {result['signal']:.3f} "
                f"(간격: {spacing_factor:.2f}x, 이격: {deviation_factor:.2f}x, 최종: {final_factor:.2f}x) | "
                f"골든: {golden_count}개 | "
                f"MA간격: {result['avg_spacing']:.2f}% | "
                f"가격이격: {result['avg_deviation']:.2f}%"
            )
        
        # ========================================
        # 🚨 PATCH I: MA5 < MA10 역배열 시 신호 제한 (2026-01-07)
        # ========================================
        # 목적: 역배열(하락 추세) 구간에서 골든크로스 과대평가 방지
        # 원칙: MA5 < MA10 = 단기 하락 추세 우선
        # 효과: 역배열 시 signal 상한을 0.30(장기 정배열) 또는 0.00(전면 하락)로 제한
        ma5_now = current_mas.get('ma5')
        ma10_now = current_mas.get('ma10')
        
        # 🔧 준가자미근 보호: MA5/MA10 미세 차이는 신호 절단 금지
        ma5_lt_ma10 = False
        ma_gap_pct = 0.0
        
        if ma5_now is not None and ma10_now is not None:
            ma5_val = float(ma5_now)
            ma10_val = float(ma10_now)
            ma_gap_pct = abs(ma5_val - ma10_val) / max(ma10_val, 1e-9)
            
            # 0.1% 이상 차이날 때만 역배열로 판단 (준골든크로스 보호)
            ma5_lt_ma10 = (ma5_val < ma10_val) and (ma_gap_pct >= 0.001)
        
        long_bull_ok = False  # default
        
        if ma5_lt_ma10:
            ma20_now = current_mas.get('ma20')
            ma60_now = current_mas.get('ma60')
            ma120_now = current_mas.get('ma120')
            
            # 장기 정배열(상승 기반)일 때만 0.30까지 허용, 아니면 0.00
            if ma20_now is not None and ma60_now is not None and float(ma20_now) > float(ma60_now):
                if ma120_now is None or float(ma60_now) > float(ma120_now):
                    long_bull_ok = True
            
            cap = 0.30 if long_bull_ok else 0.00
            if result.get('signal', 0.0) > cap:
                logger.info(
                    f"🚫 [MA5<MA10] {symbol} 역배열 감지 → 신호 제한 | "
                    f"원본:{result.get('signal', 0.0):.3f} → {cap:.2f} | "
                    f"gap={ma_gap_pct:.4%} (long_bull_ok={long_bull_ok})"
                )
                result['signal'] = cap
        else:
            # 준골든크로스 보호 로그
            if ma5_now is not None and ma10_now is not None:
                ma5_val = float(ma5_now)
                ma10_val = float(ma10_now)
                if ma5_val < ma10_val:
                    logger.debug(
                        f"🟡 [준골든크로스] {symbol} MA5~MA10 미세차이 보호 | "
                        f"gap={ma_gap_pct:.4%} < 0.1% | 신호 유지"
                    )
        
        # 🔒 [2026-08-08 SSoT-P4/P5] 고정 최소보장·배열 계단 max() 제거.
        # 위에서 계산된 Producer signal을 그대로 유지한다.
        
        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        # 🆕 MA 기울기 보너스 (2025-11-09)
        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        # 목적: MA 후행성 보완, 추세 전환 조기 포착
        # 구조:
        #  - MA20 기울기: 중기 추세
        #  - MA5 기울기: 단기 방향성
        #  - 상승 기울기(둘 다 +) → +0.05 보너스
        #  - 하락 기울기(둘 다 -) → -0.05 감산
        # 효과:
        #  - 골든크로스 직전 상승 전환 감지
        #  - 데드크로스 직전 하락 전환 감지
        #  - MA 신호 지연 1-2봉 단축
        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        slope_bonus = 0.0
        result['ma_slope_bonus'] = 0.0  # 기본값 초기화
        
        # 🔒 SSoT: MA slope는 indicators(3봉, %) 하나로 통일
        try:
            slope5 = float(indicators.get("ma5_slope", 0.0) or 0.0) / 100.0
            slope20 = float(indicators.get("ma20_slope", 0.0) or 0.0) / 100.0

            slope_bonus = 0.0
            if slope5 > 0 and slope20 > 0:
                slope_bonus = 0.05
            elif slope5 < 0 and slope20 < 0:
                slope_bonus = -0.05

            result['signal'] = result['signal'] + slope_bonus
            result['ma_slope_bonus'] = slope_bonus

            # Strategy/로그 호환 키는 유지하되 SSoT 값으로만 주입
            result['slope_ma5'] = slope5
            result['slope_ma20'] = slope20

            logger.debug(
                f"📈 [MA기울기] {symbol} | MA5={slope5:.5f}, MA20={slope20:.5f} | bonus={slope_bonus:+.2f}"
            )
        except Exception as e:
            result['ma_slope_bonus'] = 0.0
        
        result['signal'] = np.clip(result['signal'], SIGNAL_STRENGTH_MIN, SIGNAL_STRENGTH_MAX)
        
        # 8. actionable 판단
        result['is_actionable'] = (
            len(cross_signals) > 0 or
            result['alignment_level'] != 0 or
            abs(result['signal']) >= 0.2 or
            result['ma_deviation']['state'] != 'neutral'
        )
        
        # 9. 횡보 판단
        if abs(result['signal']) < 0.1 and result['alignment_level'] == 0:
            result['is_sideways'] = True
        
        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        # 10. RANGING 서브 체제 판단 (Strategy에 전달)
        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        ranging_subtype = None
        if regime == MarketRegime.RANGING and df is not None and len(df) >= 3:
            try:
                # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
                # ADX fallback (중립값)
                # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
                # - ADX 미산출/결측 시 None 전파 방지용
                # - 20.0은 추세/비추세 경계값 (Neutral)
                # - regime 판단 시 계산된 값이 있으면 나중에 개선 가능
                # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
                adx_val = 20.0  # fallback (중립)
                
                # 거래량 비율 계산
                if 'volume' in df.columns and len(df) >= 20:
                    volume_ma = df['volume'].rolling(20).mean()
                    if not volume_ma.empty:
                        vol_ratio = float(df['volume'].iloc[-1] / volume_ma.iloc[-1])
                    else:
                        vol_ratio = 1.0
                else:
                    vol_ratio = 1.0
                
                # MA 기울기는 위에서 이미 계산됨 (ma5_slope_short, ma10_slope_short)
                # classify_ranging_subtype는 @staticmethod가 아니므로 호출 방법 확인 필요
                
                # EXPANSION 조건
                if (15 <= adx_val <= 25 and 
                    ma5_slope_short > 0 and ma10_slope_short > 0 and 
                    vol_ratio >= 0.7):
                    ranging_subtype = "RANGING_EXPANSION"
                # TOP_HEAVY 조건 (간략화)
                else:
                    recent = df.tail(3)
                    upper_wicks = recent["high"] - recent[["open", "close"]].max(axis=1)
                    body_sizes = (recent["close"] - recent["open"]).abs()
                    upper_wick_ratio = (upper_wicks / (body_sizes + 1e-9)).mean()
                    
                    if len(recent) >= 2:
                        current_high = float(recent["high"].iloc[-1])
                        prev_highs_max = float(recent["high"].iloc[:-1].max())
                        high_failed = (current_high <= prev_highs_max)
                    else:
                        high_failed = False
                    
                    if upper_wick_ratio >= 1.5 and high_failed:
                        ranging_subtype = "RANGING_TOP_HEAVY"
                    else:
                        ranging_subtype = "RANGING_WEAK"
                        
            except Exception as e:
                logger.debug(f"[RANGING서브체제] {symbol} 판단 실패: {e}")
                ranging_subtype = "RANGING_WEAK"
        
        result['ranging_subtype'] = ranging_subtype
        
        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        # 11. 가자미근 판정 (vFinal Hard Gate)
        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        
        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        # 🔥 기본값 초기화 (필수 - 모든 경로에서 정의 보장)
        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        # [가자미근 판정 초기화]
        gajami_meta = {
            "reason": "not_checked",
            "is_candidate": False  # ✅ 기본값 명시
        }
        is_gajami_candidate = False
        is_gajami_knee = False
        
        # 🔒 SSoT: soleus_gajami만 사용
        # alias(is_gajami_candidate, is_gajami_knee, is_soleus_zone)는
        # 최종 alias 구간에서만 매핑
        
        # - 가자미근은 형상(soleus_gajami) 단일 판정
        tier = 3  # 기본값: Tier3 (low_area_threshold 계산용 유지)
        
        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        # Soleus Zone 판정 (SSoT: Single Source of Truth)
        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        # 철학: Analysis에서 확정 판정까지 완료
        #   - 횡보 감지 (3조건: 응축 + 저가권 + 고점근접 차단)
        #   - 저점 리셋 (Stale Low 방지)
        #   - Soleus Zone 최종 판정 (발목 위~무릎 아래 범위)
        #   - gajami_meta로 모든 정보 전달
        # 
        # Strategy 역할: 
        #   - Analysis 판정 결과를 소비 (재판정은 허용하되 권장하지 않음)
        #   - MA/Volume 등 추가 조건으로 최종 진입 결정
        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        
        # regime이 있고, df가 충분하면 가자미근 판정 수행
        if regime and df is not None and len(df) >= 60:
            try:
                recent = df.tail(60)
                cur_price = float(recent["close"].iloc[-1])
                
                # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
                # 🔥 Tier 판별 (즉시 실행 - 이후 모든 로직에서 사용)
                # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
                # Tier1: BTC, ETH (변동성 작음, 초입 0.25~2.0%)
                # Tier2: 고가 코인 (노이즈 적음, 초입 0.40~2.0%)
                # Tier3: 일반 알트 (변동성 큼, 초입 0.60~2.0%)
                # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
                if config is not None:
                    try:
                        # Tier1 종목 확인
                        tier1_str = config.get('VOLUME', 'exclude_tier1', fallback='')
                        tier1_symbols = set(tier1_str.replace(' ', '').split(',')) if tier1_str else set()
                        
                        # Tier2 기준가 확인
                        high_price_threshold = config.getfloat('VOLUME', 'high_price_threshold', fallback=80000)
                        
                        # - tier만 유지 (low_area_threshold 계산용)
                        if symbol in tier1_symbols:
                            tier = 1
                        elif cur_price >= high_price_threshold:
                            tier = 2
                        else:
                            tier = 3

                        logger.debug(
                            f"[Tier] {symbol} → Tier{tier} (가격: {cur_price:,.0f}원)"
                        )
                    except Exception as e:
                        logger.warning(f"⚠️ [Tier] {symbol} 판별 실패: {e} (Tier3 기본값 사용)")
                        tier = 3
                
                # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
                # 횡보 감지 → 저점 리셋 (Stale Low 방지)
                # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
                recent_5 = recent.tail(5)
                low_40 = float(recent["low"].min())
                low_5 = float(recent_5["low"].min())
                
                # 🆕 파동 기준점(동적): 최근 12봉(36분) 저점
                recent_12 = recent.tail(12)
                low_12 = float(recent_12["low"].min())
                
                # 🔧 FIX: high_20, retrace_from_high 정의 추가
                recent_20 = recent.tail(20)
                high_20 = float(recent_20["high"].max()) if len(recent_20) >= 20 else float(recent["high"].max())
                retrace_from_high = ((high_20 - cur_price) / high_20) if high_20 > 0 else 0.0
                
                # 횡보 조건: 가격 범위만 사용 (ATR 의존성 제거)
                # 철학: 횡보는 "가격 응축"이 본질, ATR은 부차적
                price_range_pct = ((recent_5["high"].max() - recent_5["low"].min()) / cur_price) if cur_price > 0 else 999
                
                # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
                # 횡보 리셋 조건 강화 (꼭지 오판 방지)
                # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
                
                # ✅ 조건 1: 저가권 조건 (TIER별 차등)
                # 철학: 진짜 횡보는 저점 근처에서 일어남
                low_area_ratio = ((low_5 - low_40) / low_40) if low_40 > 0 else 999
                # TIER2는 변동폭이 작아서 저점 판정 기준 완화
                low_area_threshold = 0.025 if tier == 2 else 0.015  # TIER2: 2.5%, 기타: 1.5%
                is_low_area = (low_area_ratio <= low_area_threshold)
                indicators['is_low_area'] = is_low_area
                
                # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
                # 🔒 위치 판정: 고점 근접 여부 (형상과 완전 독립)
                # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
                # 철학: 체제 라벨 대신 원시 slope로 성격 분기
                #   - MA20 slope = 추세 방향 (관성 제거는 MA10 부호로)
                #   - MA10 부호 = 현재 모멘텀 생사 (크기는 soleus가 담당)
                #   상승 성격: 계단 돌파가 수익원 → 짧게 보고 완화
                #   하락 성격: 반등은 데드캣 → 길게 보고 엄격
                #   횡보 성격: 상단 재도전 위험 → 중간 + 상단 강제 차단
                # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
                
                # TF별 명시적 파라미터 테이블 (수학적 보정 없음)
                # 운영 전략: 짧은 TF=공격적, 긴 TF=보수적
                # 횡보 판정용 고점
                high_40 = float(recent["high"].max())
                dist_from_high_40 = ((high_40 - cur_price) / high_40) if high_40 > 0 else 0.0
                
                # 최종 횡보 판정: 3가지 조건 모두 충족
                is_sideways = (
                    price_range_pct < 0.006 and  # 5봉 범위 < 0.6% (응축)
                    is_low_area and               # 저가권 (저점 근처)
                    (dist_from_high_40 >= 0.01)   # 40봉 고점에서 1% 이상 떨어짐 (횡보용)
                )
                
                # ─────────────────────────────────────────────
                # 🆕 reference_low 선택 로직 (파동 추적)
                # 철학:
                # - 횡보: low_5 (기존 유지)
                # - 비횡보: low_12를 우선 사용하되,
                #           파동이 무의미하면 low_40로 폴백
                # ─────────────────────────────────────────────
                # ─────────────────────────────────────────────
                # reference_low 기준점 통일 (단순화)
                # ─────────────────────────────────────────────
                # 1순위: 기준 저점 선택
                # 🔧 PATCH LOW-EXPAND: 3분봉 저점 기준 확장 (36분 → 120분)
                # - 근거: KRW-IP 09:00 폭등 사례 (36분 lookback으로는 국지 저점 포착 불가)
                # - 효과: "저점 형성 → 반등" 패턴 인식 가능
                # - TRENDING_DOWN: low_40 (120분) 유지
                # - 그 외: low_40 (120분) 확장 ← 변경!
                # 🔧 PATCH 2-FINAL: reference_low 기준 단일화
                # - 3분봉 기준 국지 저점 대응
                # - 모든 체제(TD/RANGING/TU)에서 low_40 (120분) 사용
                # - 근거: KRW-IP 09:00 폭등 사례 (36분 lookback 부족)
                # - 효과: "저점 형성 → 반등" 패턴 인식 가능
                reference_low = low_40

                # 2순위: 횡보 시 5봉 저점으로 교체 (기존 유지)
                if is_sideways and low_5 is not None:
                    reference_low = low_5
                
                # 안전장치: reference_low가 없거나 0이면 폴백
                if reference_low is None or reference_low <= 0:
                    reference_low = low_40 if low_40 > 0 else cur_price
                
                # 🔧 PATCH ADAPTIVE-LOW: 적응형 저점 갱신 (국지 박스 대응)
                # 철학: "큰 파동 후 새로운 저점 형성" 인식
                # - 현재가가 기준 저점 대비 15% 이상 상승했으면
                # - 최근 12봉 저점을 새로운 기준점으로 갱신
                # - 효과: 급등 후 조정 구간의 "국지 저점" 포착 가능
                try:
                    if reference_low > 0 and cur_price > reference_low * 1.15:
                        # 15% 이상 상승 → 국지 저점 재설정
                        if low_12 is not None and low_12 > 0:
                            old_reference_low = reference_low  # 기존값 저장
                            reference_low = low_12
                            logger.debug(
                                f"[ADAPTIVE-LOW] {symbol} 저점 갱신 | "
                                f"기존: {old_reference_low:.0f}원 → 신규: {low_12:.0f}원 "
                                f"(상승률: {((cur_price/old_reference_low-1)*100):.1f}%)"
                            )
                except Exception:
                    pass

                # - 가자미근은 형상(soleus_gajami) 단일 판정
                # - 거리 기반 판정 금지
                
                # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━   
                # ❌ 기존 가자미근 로직 제거 완료
                # - sideways_gajami (횡보형)
                # - trend_pullback_gajami (눌림형)
                # - td_knee_gajami (골든크로스형)
                # → soleus_gajami (Fan-out Pattern) 단일화
                # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
                # 🦵 Soleus Gajami (Fan-out Pattern)
                # "5-10 골든크로스 + 이격 확장 + MA20 평행/상승"
                # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
                # 🔒 변수 초기화 (스코프 오류 방지)
                soleus_gajami = False
                # ma20_ok: MA20 평행/상방 여부 (soleus_gajami AND 게이트에 사용됨)
                ma20_ok = False
                _gajami_constitution_ok = False
                _gajami_constitution_effective_ok = False
                _v2_ctn_fail = "ANALYSIS_INVALID_ERROR"
                _regime_gate_ok = False
                gajami_lifecycle = {"state": "DEAD", "birth_price": None, "rise_pct": None, "death_reason": "INIT", "dur": 999}
                
                try:
                    if regime in [MarketRegime.TRENDING_UP, MarketRegime.BREAKOUT, MarketRegime.RANGING, MarketRegime.TRENDING_DOWN]:
                        ma5  = float(current_mas.get("ma5", 0.0) or 0.0) if len(recent) >= 5 else 0.0
                        ma10 = float(current_mas.get("ma10", 0.0) or 0.0) if len(recent) >= 10 else 0.0
                        ma20 = float(recent["close"].rolling(20).mean().iloc[-2]) if len(recent) >= 21 else 0.0

                        # 🔒 TYPE-SAFE: MA 스칼라 확정 (Series ambiguous 방지)
                        ma5 = safe_extract_scalar(ma5, 0.0)
                        ma10 = safe_extract_scalar(ma10, 0.0)
                        ma20 = safe_extract_scalar(ma20, 0.0)

                        prev_prev_ma5  = float(recent["close"].rolling(5).mean().iloc[-3]) if len(recent) >= 7 else ma5
                        prev_ma5       = float(recent["close"].rolling(5).mean().iloc[-2]) if len(recent) >= 6 else ma5
                        prev_prev_ma10 = float(recent["close"].rolling(10).mean().iloc[-3]) if len(recent) >= 12 else ma10
                        prev_ma10      = float(recent["close"].rolling(10).mean().iloc[-2]) if len(recent) >= 11 else ma10

                        # =========================================================
                        # ✅ 가자미근(SOLEUS) = "상향 형상" 정의 (정석)
                        # =========================================================
                        # ① MA5 > MA10 골든크로스 + 간격이 "벌어지는 중"
                        # ② MA20은 하락 중단(평행 이상)이면 충분
                        # ③ MA5·10·20 전체 방향이 "위로 접힘" (모두 상향)
                        # =========================================================

                        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
                        # 🔒 MA20 방향성 단일 기준 (df 기반) - 혼용 차단
                        # =========================================================
                        # 🔍 MEASURE ONLY (판정 영향 없음)
                        # =========================================================
                        ma20_slope_pct_ind = float(indicators.get("ma20_slope") or 0.0)  # % 단위 (로그/판정 공용)

                        # 2) MA5-MA10 간격: "벌어지는 중"이어야 함
                        spread_now = ((ma5 - ma10) / ma10 * 100.0) if ma10 > 0 else 0.0
                        spread_prev = ((prev_prev_ma5 - prev_prev_ma10) / prev_prev_ma10 * 100.0) if prev_prev_ma10 > 0 else spread_now
                        spread_expand = spread_now - spread_prev
                        indicators['spread_expand'] = spread_expand

                        # 🔒 SSoT: 슬로프는 indicators에서 가져오기 (중복 계산 제거)
                        # - 출처: indicators['ma5_slope'], indicators['ma10_slope'] (3봉 기준, % 단위)
                        # - 사용: ratio로 변환 (/ 100.0)
                        
                        ma5_slope = float(indicators.get("ma5_slope", 0.0) or 0.0) / 100.0
                        ma10_slope = float(indicators.get("ma10_slope", 0.0) or 0.0) / 100.0

                        # 🆕 MA10 1봉 슬로프 (감속/정지 감지 전용)
                        # - 3봉은 9분 지연 → V자 반등 초입에서 "이미 멈춘 MA10"을 하락으로 오판
                        # - 1봉은 "지금 멈췄는가?"만 확인 (단독 판정 금지, 
                        # 보조 전용)
                        
                        # 재가속 감지용 (세그먼트)
                        ma5_slope_seg_now = ma5_slope
                        ma5_slope_seg_prev = float(indicators.get("ma5_slope_prev1", 0.0) or 0.0) / 100.0

                        #  =========================================================
                        # 🔒 SOLEUS GATE START (판정 영역)
                        # =========================================================
                        
                        # =========================================================
                        # 🔒 가자미근 초입 로직 v3 (정석)
                        # =========================================================
                        # 핵심 철학: 초입은 '완성된 부채꼴'이 아니라
                        #           '곧 위로 접힐 부채꼴의 시작점'
                        # =========================================================
                        
                        # 🔒 [GC-CLOSED-STRICT-SSOT-0822-R2] canonical MA5×MA10 GC는
                        # 확정봉 strict `>`만 사용한다. EQ·현재봉은 GC 자격이 아니라 관측/trigger 축이다.
                        _closed_gc_ma5 = float(prev_mas.get('ma5', 0.0) or 0.0)
                        _closed_gc_ma10 = float(prev_mas.get('ma10', 0.0) or 0.0)
                        gc_5_10 = bool(
                            _closed_gc_ma5 > 0.0
                            and _closed_gc_ma10 > 0.0
                            and _closed_gc_ma5 > _closed_gc_ma10
                        )
                        indicators["closed_gc_5x10_relation"] = int(gc_5_10)
                        result["closed_gc_5x10_relation"] = int(gc_5_10)
                        
                        # ② 간격 확대 정의 강화
                        # - spread_expand > 0 만으로는 '수렴→0'도 확대라고 오판 가능
                        # - '정배열 상태에서' 실제 양(+) 간격이 생기며 벌어질 때만 확대 인정
                        gap_5_10 = (ma5 - ma10) / ma10 if ma10 > 0 else 0.0
                        
                        # ✅ 정석: 수렴(거의 동일)에서 "첫 확장"도 확장으로 인정
                        is_converged = (abs(gap_5_10) <= 0.00001)
                        
                        # ══════════════════════════════════════════════════════
                        # spacing_expanding 정석 정의 (SSoT)
                        # ══════════════════════════════════════════════════════
                        # 1) 수렴 초입: MA5가 MA10보다 먼저 들리는 순간
                        # 2) 정상 확장: 이미 정배열이고 간격이 유지/확대
                        # ══════════════════════════════════════════════════════
                        SPREAD_EXPAND_EPS = 0.00002  # 0.002%p 수준 노이즈 허용
                        MIN_GAP_5_10 = 0.0025        # 🔧 0.25% 최소 간격 (TIER3 기준)

                        # 의미 분리 (동작 동일, 가독성 개선)
                        converge_lift = (is_converged and ma5_slope > ma10_slope)

                        normal_expand = (
                            ma5 > ma10
                            and gap_5_10 > MIN_GAP_5_10     # 🔧 최소 간격 강제
                            and spread_expand > 0
                        )

                        spacing_expanding = (converge_lift or normal_expand)
                        
                        # ③ 방향성 조건 (선행/후행 구조 반영)
                        
                        # 🔧 PATCH: MA slope 합산 기반 판정 (정석)
                        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
                        # 철학:
                        # - 개별 slope: 최소 안전장치만 (완화)
                        # - 합산 slope: 진짜 판정 기준 (에너지 충족 여부)
                        # - "예쁜 가자미근만 인정" 문제 해결
                        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
                        
                        # 1️⃣ 개별 최소 조건 (안전장치)
                        MA5_SLOPE_MIN = -0.01         # MA5: 미세 음수 허용
                        MA10_SLOPE_MIN_STRONG = SoleusGajamiConstants.MA10_SLOPE_DEFAULT  # SSoT: 기존 0.0003 → 상수화
                        
                        ma5_ok = (ma5_slope >= MA5_SLOPE_MIN)                        
                        individual_ok = (ma10_slope >= MA10_SLOPE_MIN_STRONG)
                        
                        # 4️⃣ 최종 방향성 판정
                        # direction_energy_ok:
                        # - 진입 자격 ❌
                        # - 포지션 크기 / 익절 전략 참고용 ⭕
                        direction_energy_ok = (
                            ma5_ok and
                            individual_ok
                        )
                        
                        # 🔒 MA20 방향성 판정은 is_gajami_shape()가 SSoT
                        # 여기서는 혼동 방지를 위해 MEASURE ONLY로 유지
                        ma20_ok = True
                        
                        # =========================================================
                        # 🔒 장기 구조 차단 (SSoT)
                        # =========================================================
                        # - long_ma_bearish: MA60/MA120 둘 다 비상향 OR TD/RG 강하향(≤-0.30%)
                        # - 헌법: (MA60 상향 OR MA120 상향) 이 False면 진입 불가
                        # - ❌ mid_ma_bearish 완전 삭제 (2026-02-06)
                        #   사유: MA20<MA60은 가자미근(하락→반등 초입)의 구조적 필연
                        #         이 조건으로 차단하면 가자미근 진입이 원천 불가
                        #         중기 리스크는 포지션 사이즈/손절에서 관리
                        # - 핵심: df(전체) 기준 계산 (recent가 아님)
                        # =========================================================
                        long_ma_bearish = False
                        long_ma_strong_down_veto = False
                        td_ma60_down_raw = False
                        td_ma60_down_observe = False
                        td_ma60_down_veto = False
                        long_ma_veto = False
                        slope_ma60 = 0.0
                        slope_ma120 = SENTINEL_FLOAT
                        ma60_val = 0.0
                        ma120_val = SENTINEL_FLOAT
                        
                        # 🔒 slope_ma20_df는 가자미근 SSoT에 사용되므로
                        # len(df)>=60 구간에서도 반드시 계산한다
                        slope_ma20_df = None
                        
                        if len(df) >= 22:
                            try:
                                ma20_prev_df = float(df["close"].rolling(20).mean().iloc[-2])
                                # 🔒 RT 아티팩트 방지: iloc[-1](RT봉) 제외, 확정봉 전용
                                ma20_df = ma20_prev_df  # ma20_df: 현재 미사용 (long_bearish_align 제거됨)
                                if len(df) >= 23:
                                    ma20_prev2_df = float(df["close"].rolling(20).mean().iloc[-3])
                                    slope_ma20_df = (ma20_prev_df - ma20_prev2_df) / ma20_prev2_df if ma20_prev2_df > 0 else 0.0
                                else:
                                    slope_ma20_df = 0.0
                                # 둔화/가속 판정용 이전 봉 slope
                                if len(df) >= 24:
                                    ma20_prev3_df = float(df["close"].rolling(20).mean().iloc[-4])
                                    slope_ma20_df_prev = (ma20_prev2_df - ma20_prev3_df) / ma20_prev3_df if ma20_prev3_df > 0 else 0.0
                                else:
                                    slope_ma20_df_prev = slope_ma20_df
                            except Exception:
                                slope_ma20_df = 0.0
                                slope_ma20_df_prev = 0.0
                                ma20_df = 0.0  # fallback
                        
                        # slope_ma60 / long_ma_bearish 계산 (df 기준 SSoT)
                        # ⚠️ [단일로직-동기] MA slope 변경 시 미니 _struct20_slope_from_closes/_mini_long_gate_ok(122)도 동일하게.
                        # - MA15 이상 추세 판단은 RT 현재봉 제외
                        # - [-2]=확정봉 현재값, [-22]=20봉 구조 slope(len>=81), fallback=[-3]
                        if len(df) >= 62:
                            ma60_series = df["close"].rolling(60).mean()
                            ma60_val = float(ma60_series.iloc[-2])
                            _ma60_prev20 = (
                                float(ma60_series.iloc[-22])
                                if len(ma60_series) >= 81
                                else float("nan")
                            )
                            _ma60_prev1 = float(ma60_series.iloc[-3])
                            ma60_prev = (
                                _ma60_prev20
                                if math.isfinite(_ma60_prev20) and _ma60_prev20 > 0.0
                                else _ma60_prev1
                            )
                            slope_ma60 = (ma60_val - ma60_prev) / ma60_prev if ma60_prev > 0 else 0.0

                        # 장기 구조(60<120) 계산: 확정봉 기준
                        if len(df) >= 122:
                            ma60_series = df["close"].rolling(60).mean()
                            ma120_series = df["close"].rolling(120).mean()

                            ma60_val = float(ma60_series.iloc[-2])
                            _ma60_prev20 = (
                                float(ma60_series.iloc[-22])
                                if len(ma60_series) >= 81
                                else float("nan")
                            )
                            _ma60_prev1 = float(ma60_series.iloc[-3])
                            ma60_prev = (
                                _ma60_prev20
                                if math.isfinite(_ma60_prev20) and _ma60_prev20 > 0.0
                                else _ma60_prev1
                            )
                            slope_ma60 = (ma60_val - ma60_prev) / ma60_prev if ma60_prev > 0 else 0.0

                            ma120_val = float(ma120_series.iloc[-2])
                            _ma120_prev20 = (
                                float(ma120_series.iloc[-22])
                                if len(ma120_series) >= 141
                                else float("nan")
                            )
                            _ma120_prev1 = float(ma120_series.iloc[-3])
                            ma120_prev = (
                                _ma120_prev20
                                if math.isfinite(_ma120_prev20) and _ma120_prev20 > 0.0
                                else _ma120_prev1
                            )
                            slope_ma120 = (ma120_val - ma120_prev) / ma120_prev if ma120_prev > 0 else 0.0

                            # 🔒 [PATCH1-C] 장기선 veto 판정은 dur 확정(L~6903) 이후로 이동.
                            # 여기서는 slope_ma60/120·ma60_val/120_val만 계측하고
                            # long_ma_* 변수는 아래 DUR-SSOT 확정 직후 1회만 계산한다.
                            pass
                        
                        # =========================================================
                        # 🔒 가자미근 형상 SSoT (초입 허용 + Window 종료)
                        # =========================================================
                        # 핵심:
                        #   1) 초입 허용: 5>10 + MA5↑ + MA10 flat 이상(미세 음수 허용)
                        #   2) Window 종료: MA5-MA10 간격 상한 초과 시 "이미 지남" 차단
                        # =========================================================

                        # ─────────────────────────────────────────
                        # 2) aligned_gate = 가자미근 형상 SSoT
                        # ─────────────────────────────────────────
                        # 🔒 SSoT: 정렬 판정은 is_gajami_shape()로 통일
                        # - 중복 정의 제거 (이중 개념 방지)
                        # - 🔧 FIX: gajami_shape_result는 Line 4949에서 생성됨
                        #   여기서 참조하면 NameError → except → soleus 전멸
                        aligned_gate = False  # is_gajami_shape() 결과로 아래에서 재설정
                        
                        # 3) 가자미근 진입 Window 제한 (초입만 허용)
                        # ─────────────────────────────────────────────────────
                        # 철학:
                        # - 가자미근은 "상향으로 접히기 시작하는 순간"을 잡는다
                        # - MA5-MA10 간격이 너무 벌어지면 "이미 많이 올라간 상태"
                        # - 늦은 진입(추격 매수) 방지 → 리스크 최소화
                        # 
                        # 기준: MA5-MA10 간격 ≤ 0.4%
                        # - 0.4% 이내: 초입 구간 (골든크로스 직후~초기 정배열)
                        # - 0.4% 초과: Window 종료 (이미 상승이 진행됨)
                        # 
                        # 예시:
                        # - 10:03 (간격 0.2%) → ✅ 초입, 진입 가능
                        # - 10:15 (간격 0.6%) → ❌ 늦은 정배열, 진입 불가
                        # ─────────────────────────────────────────────────────
                        # MA5-MA10 GAP: 과열만 차단 (정석 정의)
                        # - 붙어 있어야 한다는 조건 ❌
                        # - 벌어지는 것은 전환형 상승에서 정상
                        # - 과도하게 벌어진 경우(과열)만 차단
                        
                        ma5_ma10_gap = abs(ma5 - ma10) / ma10 if ma10 > 0 else 0.0
                        
                        # ✅ 정석: "오래된 골크"는 가자미근을 강화하지 않는다.
                        # - gap 허용 폭은 골든 개수가 아니라, "현재 5/10의 상승 형상 강도"로만 결정한다.
                        
                        # 기본: 엄격 (3m 기준, 늦은 진입 차단)
                        gap_threshold = 0.010  # 1.0%
                        
                        # 5MA가 '확실히' 고개를 든 경우에만 아주 소폭 완화 (형상 강한 전환형만)
                        # (direction_energy_ok는 여기서 추가로 "확실한 고개"를 요구)
                        if ma5_slope >= SoleusGajamiConstants.MA5_SLOPE_QUANT_RATIO and ma10_slope > 0.0:
                            gap_threshold = 0.014  # 1.4% (상한)
                        
                        gajami_window_open = (ma5_ma10_gap <= gap_threshold)
                        
                        if gc_5_10 and not gajami_window_open:
                            logger.debug(
                                f"🚫 [GAJAMI-OVEREXTENDED] {symbol} | "
                                f"MA5-MA10 gap={ma5_ma10_gap*100:.2f}% > {gap_threshold*100:.2f}% "
                                f"(slope5={ma5_slope:.4f}, slope10={ma10_slope:.4f})"
                            )

                        # 4) 최종 SSoT (정식 가자미근 형상 Gate)
                        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
                        # 🔒 과거 버그 재발 방지: early_entry_ok / short_ma_aligned 절대 사용 금지
                        # - aligned_gate가 최종 정렬 기준 (full_aligned는 참고용)
                        # - MA10 유지성 필수 (평행→꺾임 구간 차단)
                        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
                        # 🔒 STEP 1: 가자미근 형상 판정 (SSoT)
                        # - aligned_gate가 최종 정렬 기준
                        # - full_aligned는 참고용이며 직접 사용하지 않음
                        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
                        # =========================================================
                        # 🦶 GAJAMI (SOLEUS) — 형상 기반 하드코딩 판정
                        # =========================================================
                        
                        # ── 1) 타임프레임별 최근성 (하드코딩)
                        if timeframe == "1m":
                            RECENT_GC_MAX_BARS = 5
                        
                        elif timeframe == "3m":
                            # 🔒 정석: 기울기(lookback=2, 3봉) 각도에 따른 유효기간 (상수 lookup)
                            ma5_slope_ratio = abs(float(indicators.get("ma5_slope", 0.0) or 0.0) / 100.0)
                            
                            # 🔒 [2026-08-02] default를 하드코딩 대신 테이블에서 직접 읽어,
                            #  테이블 값이 바뀌어도 default가 자동 동기화되게 한다.
                            RECENT_GC_MAX_BARS = int(
                                SoleusGajamiConstants.RECENT_GC_BARS_3M[-1][1]
                            )
                            for threshold, bars in SoleusGajamiConstants.RECENT_GC_BARS_3M:
                                if ma5_slope_ratio >= threshold:
                                    RECENT_GC_MAX_BARS = bars
                                    break
                        
                        elif timeframe in ("5m", "10m"):
                            RECENT_GC_MAX_BARS = 2
                        
                        else:
                            RECENT_GC_MAX_BARS = 1  # 15m 이상
                        
                        # ══════════════════════════════════════════════════════
                        # 🔒 정석 DURATION 계산 (SSoT: 차트 직접 계산)
                        # ══════════════════════════════════════════════════════
                        # 정의: "False → True 전환이 몇 봉 전에 발생했는가?"
                        #      ≠ "True 상태가 얼마나 지속되었는가?"
                        # ══════════════════════════════════════════════════════
                        
                        def _calc_cross_duration_bars(
                            fast_series, slow_series,
                            lookback=20, start_idx=1,
                            allow_converge_birth=False,
                            converge_max_bars=2,
                            meta=None,
                        ):
                            """
                            마지막 골든크로스 발생 시점 기준 경과 봉 수 계산

                            allow_converge_birth:
                                True일 때만 직전봉 수렴(기차길)을 DC에 준해 인정.
                                5x10 전용으로만 사용한다.

                            converge_max_bars:
                                수렴형 birth는 최근 N봉만 플래그로 인정.
                                indicators 오기록 방지용.

                            반환:
                                int  : GC 성립이 확인된 확정봉까지의 경과 (1 = 가장 최근 확정봉에서 GC 성립)
                                None : lookback 범위 내 전환 없음 또는 계산 불가
                            """
                            try:
                                if fast_series is None or slow_series is None:
                                    return None

                                n = min(len(fast_series), len(slow_series))
                                if n < 3:
                                    return None

                                fast = fast_series.values
                                slow = slow_series.values

                                # 현재 GC가 아니면 살아 있는 크로스가 없음
                                if not (fast[-1] > slow[-1]):
                                    return None

                                for i in range(start_idx, min(lookback + 1, n)):
                                    # 비GC(DC 또는 동치) → GC 전환
                                    _prev_dc = fast[-i-1] <= slow[-i-1]
                                    _is_converge = False

                                    if allow_converge_birth and i <= converge_max_bars and not _prev_dc:
                                        try:
                                            _ref = abs(float(slow[-i-1])) if float(slow[-i-1]) != 0 else abs(float(slow[-i]))
                                            _pct_margin = _ref * 0.0005  # 0.05%
                                            try:
                                                _tick_unit = get_tick_size(_ref)
                                                _tick_margin = _tick_unit * 0.5
                                                # 🔒 [2026-08-02] 하한 max(..., _tick_unit) 제거.
                                                #  min()으로 좁힌 마진을 다시 1틱까지 끌어올려,
                                                #  굵은 틱 종목에서 '수렴' 인정폭이 가격의 0.39%까지 부풀었다.
                                                #  실측 KRW-OPEN 08-02 08:21: _pct_margin 0.128원이 1.0원으로 8배 확대
                                                #  → 직전봉 MA 간격 0.7원이 '수렴'으로 인정 → 열십자 교차 없이 dur=1
                                                #  → 실제 골크(07:51) 7봉 뒤인데 "신선 탄생"으로 통과, +0.78% 고점 진입.
                                                #  저가코인 sub-tick 인정은 _pct_margin(0.05%)이 이미 담당한다
                                                #  (XEC 0.0097원: _pct_margin 0.00000485 ≈ 0.5틱, 원 의도 유지됨).
                                                _eq_margin_dur = min(_pct_margin, _tick_margin)
                                            except Exception:
                                                _eq_margin_dur = _pct_margin
                                            _is_converge = (
                                                abs(float(fast[-i-1]) - float(slow[-i-1]))
                                                <= _eq_margin_dur
                                            )
                                        except Exception:
                                            pass

                                    # 전환 조건: 이전 봉 역배열 또는 수렴 → 현재 봉 정배열
                                    if fast[-i] > slow[-i] and (_prev_dc or _is_converge):
                                        if meta is not None and _is_converge and not _prev_dc:
                                            meta["converge_birth_detected"] = True
                                            meta["converge_birth_dur"]  = int(i)
                                            meta["converge_prev_gap"]   = float(fast[-i-1]) - float(slow[-i-1])
                                            meta["converge_cur_gap"]    = float(fast[-i])   - float(slow[-i])
                                            meta["converge_margin"]     = float(_eq_margin_dur)
                                        return i

                                return None
                            except Exception:
                                return None
                        
                        # 🔒 [DUR-CLOSED-SSOT-0817] dur은 확정봉 MA 기준으로만 계산한다.
                        # 반환 규약: 1 = 가장 최근 확정봉에서 GC 성립(현재 미확정봉은 제외).
                        # dur_5x10_bars는 가자미근 intrabar 원칙(L5008 헌법)의 예외:
                        # duration(교차 지속 개수)은 확정 사실만 세어야 하므로 확정봉 고정.
                        _gc5x10_meta = {"converge_birth_detected": False}

                        _ma5_src = mas.get('ma5')
                        _ma10_src = mas.get('ma10')
                        _ma20_src = mas.get('ma20')
                        # 🔒 [GC-CLOSED-BAR-SSOT-0904-R1] ★506-R1
                        # 확정봉을 위치(iloc[:-1])가 아니라 실제 캔들 종료시각으로 정한다.
                        # 무거래로 df 마지막 행이 이미 종료된 봉이면 위치 고정은
                        # 그 확정봉을 한 개 더 버려 GC/MA20 인식을 지연시킨다.
                        # 실측 STEEM 09-04: 11:18봉(11:21 종료)을 다음 봉 수신까지 버림.
                        _gc_drop_n = len(df) - len(
                            TechnicalIndicators.drop_incomplete_candle(df, timeframe)
                        )
                        _ma5_closed = (
                            (
                                _ma5_src.iloc[:-_gc_drop_n]
                                if _gc_drop_n > 0
                                else _ma5_src
                            )
                            if (_ma5_src is not None and len(_ma5_src) >= 4)
                            else None
                        )
                        _ma10_closed = (
                            (
                                _ma10_src.iloc[:-_gc_drop_n]
                                if _gc_drop_n > 0
                                else _ma10_src
                            )
                            if (_ma10_src is not None and len(_ma10_src) >= 4)
                            else None
                        )
                        _ma20_closed = (
                            (
                                _ma20_src.iloc[:-_gc_drop_n]
                                if _gc_drop_n > 0
                                else _ma20_src
                            )
                            if (_ma20_src is not None and len(_ma20_src) >= 13)
                            else None
                        )

                        # 🔒 CONVERGE-BIRTH 조건부 복원
                        # - AERGO/SLX식 엉킨 리셋GC는 차단 유지
                        # - 정배열 + gap + s5>s10 + MA20 비악화면 birth 인정.
                        # - TD 여부는 blanket veto가 아니라 위 품질조건으로 관리한다.
                        # - TOP 여부는 birth 차단자가 아니라 soft risk로 기록.
                        try:
                            _converge_gap_ratio = (
                                (float(ma5) - float(ma10)) / float(ma10)
                                if float(ma10) > 0 else -999.0
                            )
                        except Exception:
                            _converge_gap_ratio = -999.0

                        # 🔒 [MA20-ALIVE-OR-RECOVERING-0902] ★486-R2
                        # 확정 MA20 구조기울기의 방향/회복 SSoT.
                        # cur >= 0 : MA20 자체가 아직 상승 중이므로 생존.
                        # cur < 0  : 이전 구조기울기보다 덜 나빠질 때만 회복으로 인정.
                        # iloc[-2/-7/-8/-13]만 사용하므로 현재 미확정봉은 사용하지 않는다.
                        # 실증: KRW-AXL 09-02 13:01 cur=+0.2658/prev=+0.4652(둘다 양수,
                        #   상승 중 단순 감속)인데 기존 cur>=prev 식이 DECEL_END로 오판·차단.
                        # 소비처 2곳(SSoT, 의도적으로 값을 갈라놓지 않음):
                        #   ① 바로 아래 _allow_converge_birth(수렴탄생 인정 조건)
                        #   ② t6 TD/RG hard veto(gajami_ma20_not_decel 그대로 소비)
                        _ma20_not_decel = True
                        _ma20_not_decel_cur = float(
                            indicators.get("ma20_slope_struct5", indicators.get("ma20_slope", 0.0)) or 0.0
                        )
                        _ma20_not_decel_prev = _ma20_not_decel_cur
                        try:
                            # 🔒 [MA20-CLOSED-BAR-PARITY-0904] ★506-R1
                            # MA5/MA10과 동일한 확정봉 시계(_ma20_closed)를 소비한다.
                            # 정상 intrabar에서는 값이 완전히 동일하다.
                            _ma20_nd_s = _ma20_closed
                            if _ma20_nd_s is not None and len(_ma20_nd_s) >= 12:
                                _m20_c0 = float(_ma20_nd_s.iloc[-1])
                                _m20_c5 = float(_ma20_nd_s.iloc[-6])
                                _m20_p0 = float(_ma20_nd_s.iloc[-7])
                                _m20_p5 = float(_ma20_nd_s.iloc[-12])
                                if (
                                    math.isfinite(_m20_c0) and math.isfinite(_m20_c5)
                                    and math.isfinite(_m20_p0) and math.isfinite(_m20_p5)
                                    and _m20_c5 > 0.0 and _m20_p5 > 0.0
                                ):
                                    _ma20_not_decel_cur  = (_m20_c0 - _m20_c5) / _m20_c5 * 100.0
                                    _ma20_not_decel_prev = (_m20_p0 - _m20_p5) / _m20_p5 * 100.0
                                    _ma20_not_decel = bool(
                                        _ma20_not_decel_cur
                                        >= min(_ma20_not_decel_prev, 0.0)
                                    )
                        except Exception:
                            _ma20_not_decel = True
                        indicators["gajami_ma20_not_decel"] = int(_ma20_not_decel)
                        indicators["gajami_ma20_tlc_cur"]   = float(_ma20_not_decel_cur)
                        indicators["gajami_ma20_tlc_prev"]  = float(_ma20_not_decel_prev)

                        indicators["gajami_converge_birth_top_soft_risk"] = 0
                        _allow_converge_birth = bool(
                            ma20 > 0.0
                            and ma5 > ma10 > ma20
                            and _converge_gap_ratio >= 0.001
                            and ma5_slope > ma10_slope
                            and _ma20_not_decel
                        )
                        indicators["gajami_converge_birth_filtered_ok"] = int(
                            _allow_converge_birth
                        )

                        dur_5x10_bars = (
                            _calc_cross_duration_bars(
                                _ma5_closed,
                                _ma10_closed,
                                lookback=20,
                                allow_converge_birth=_allow_converge_birth,
                                converge_max_bars=2 if _allow_converge_birth else 0,
                                meta=_gc5x10_meta,
                            )
                            if _ma5_closed is not None and _ma10_closed is not None
                            else None
                        )

                        _converge_birth_detected = bool(
                            _allow_converge_birth
                            and _gc5x10_meta.get("converge_birth_detected", False)
                        )

                        # 🔒 진짜 탄생봉 검증: DC→GC 구조 필수
                        # 직전봉 DC 없이 dur=1 발생 = 가짜 리셋 탄생봉 → 차단
                        _is_true_gc_birth = False
                        _prev_dc = False            # 🆕 scope 보장
                        _prev2_dc = False           # 🆕 scope 보장
                        _prev_already_gc = False    # 🆕 scope 보장
                        _gap_prev = 0.0
                        _gap_prev2 = 0.0
                        _prev_near_dc = False
                        indicators["subtick_body_birth"] = 0
                        try:
                            _s5 = mas.get('ma5')
                            _s10 = mas.get('ma10')

                            # 🔒 SSoT: tick-size 양자화 마진
                            # MA10-MA5 차이가 0.5틱 이상일 때만 진짜 DC로 인정
                            # 저가 코인(MA5=MA10 동치)을 DC로 오인하는 노이즈 차단
                            try:
                                _tick_ref = float(_s5.iloc[-1]) if (_s5 is not None and len(_s5) > 0) else 0.0
                                _tick = get_tick_size(_tick_ref)
                            except Exception:
                                _tick = 1.0

                            # 🔒 [2026-08-02 단위 혼용 수정] MA 교차 상태를 '주문 호가단위'로
                            #  판정하고 있었다. 가격대별 실효 문턱이 400배 차이난다
                            #  (250원 1틱=0.200% ↔ 1억 1틱=0.0005%).
                            #  실측 KRW-OPEN: 거래소 골크 07:51인데 직전봉 MA 간격 0.1원 < 0.5원
                            #  → prev_dc=0 → true_gc=0 → birth 미등록.
                            #  규모: ENTRY-FORENSIC 26,645건 중 true_gc=0 이 91.0%,
                            #        1틱>=0.30% 종목은 96%가 true_gc=0, 진입 손실의 85%가 여기.
                            _dc_margin = min(
                                float(_tick) * 0.5,
                                float(_tick_ref) * SoleusGajamiConstants.DC_MARGIN_MAX_RATIO
                            )
                            _eq_margin = min(
                                float(_tick) * 0.3,
                                float(_tick_ref) * SoleusGajamiConstants.DC_MARGIN_MAX_RATIO * 0.6
                            )
                            _FLOAT_EPS = 1e-9  # 🆕 IEEE 754 정밀도 흡수
                            _subtick_equilibrium = False
                            _cur_body_strong = False

                            if (_s5 is not None and _s10 is not None
                                    and len(_s5) >= 3 and len(_s10) >= 3):
                                _gap_prev  = float(_s10.iloc[-2]) - float(_s5.iloc[-2])
                                _gap_prev2 = float(_s10.iloc[-3]) - float(_s5.iloc[-3])

                                _prev_dc  = _gap_prev  >= _dc_margin - _FLOAT_EPS
                                _prev2_dc = _gap_prev2 >= _dc_margin - _FLOAT_EPS
                                # 🔒 BIRTH-NEAR-DC: 0~0.5틱 DC 수렴은 전전봉 확실 DC가 있으면 탄생 DC로 인정
                                _prev_near_dc = _gap_prev >= _eq_margin - _FLOAT_EPS

                                _subtick_equilibrium = bool(
                                    abs(_gap_prev)  <= _eq_margin + _FLOAT_EPS
                                    and abs(_gap_prev2) <= _eq_margin + _FLOAT_EPS
                                )
                                # 직전봉 수렴만 보는 분리 플래그 (전전봉 조건과 충돌 방지)
                                _prev_subtick_equilibrium = bool(
                                    abs(_gap_prev) <= _eq_margin + _FLOAT_EPS
                                )
                                _ma5_cur  = float(_s5.iloc[-1])
                                _ma10_cur = float(_s10.iloc[-1])
                                _cur_body_strong = (_ma5_cur - _ma10_cur) >= _eq_margin - _FLOAT_EPS  # 🆕

                                logger.debug(
                                    f"[PREV-DC-TICK] {symbol} tick={_tick:.4f} "
                                    f"dc_m={_dc_margin:.4f} eq_m={_eq_margin:.4f} "
                                    f"gap_prev={_gap_prev:+.4f} gap_prev2={_gap_prev2:+.4f} "
                                    f"prev_dc={int(_prev_dc)} prev_near={int(_prev_near_dc)} "
                                    f"prev2_dc={int(_prev2_dc)} "
                                    f"subtick_eq={int(_subtick_equilibrium)} "
                                    f"cur_body={int(_cur_body_strong)}"
                                )

                            if (dur_5x10_bars is not None and int(dur_5x10_bars) <= SoleusGajamiConstants.NEWBORN_MAX_DUR
                                    and _s5 is not None and _s10 is not None
                                    and len(_s5) >= 3 and len(_s10) >= 3):

                                # 중기선 생존 확인 (1봉 DC 허용 조건)
                                # 시간축 통일: _prev_dc[-2], _prev2_dc[-3]과 동일하게 확정봉[-2] 기준
                                _s20 = mas.get('ma20')
                                _ma10_v = float(_s10.iloc[-2])
                                _ma20_v = float(_s20.iloc[-2]) if (
                                    _s20 is not None and len(_s20) >= 2
                                ) else 0.0
                                _gc_gap_1020 = (
                                    (_ma10_v - _ma20_v) / _ma20_v * 100
                                    if _ma20_v > 0 else 0.0
                                )

                                # 기존: 0.3~0.5틱 DC흔들림 + 전전봉DC
                                # 신규: 0~0.5틱 DC 수렴 중(weak) + 전전봉 확실DC
                                # 케이스: gap_prev=+0.2(dc_margin 미달) + gap_prev2=+0.7(확실DC) → 정상 birth
                                _prev_weak_dc = bool(
                                    _gap_prev >= -_FLOAT_EPS             # 직전봉이 DC였거나 동치
                                    and _gap_prev < _dc_margin - _FLOAT_EPS  # dc_margin(0.5) 미달 수렴
                                )
                                _subtick_dc_converge_birth = bool(
                                    _prev_weak_dc
                                    and _prev_subtick_equilibrium   # 직전봉만 수렴
                                    and _prev2_dc                   # 전전봉 진짜 DC 필수
                                )
                                indicators["gajami_subtick_dc_converge_birth"] = int(
                                    _subtick_dc_converge_birth
                                )

                                _near_dc_birth = bool(
                                    (_prev_near_dc and _prev2_dc)
                                    or (_prev_weak_dc and _prev2_dc)
                                    or _subtick_dc_converge_birth
                                )

                                if _subtick_dc_converge_birth:
                                    logger.info(
                                        f"✅ [SUBTICK-DC-CONVERGE-BIRTH] {symbol} "
                                        f"미세 DC 수렴→GC 탄생 인정 | "
                                        f"gap_prev={_gap_prev:+.4f} gap_prev2={_gap_prev2:+.4f} "
                                        f"dc_ok={int(_prev2_dc)} "
                                        f"eq_m={_eq_margin:.4f} dc_m={_dc_margin:.4f} "
                                        f"gap10_20={_gc_gap_1020:.3f}%"
                                    )
                                _is_true_gc_birth = (
                                    (_prev_dc or _near_dc_birth) and (
                                        _prev2_dc
                                        or _gc_gap_1020 > 0.0
                                    )
                                ) or (
                                    _subtick_equilibrium and _cur_body_strong
                                    and (
                                        # 🔒 B와 동일 기준 — SSoT 정합
                                        # gap < 0(GC 유지)을 DC로 오인하는 경로 제거
                              
                                        _prev2_dc
                                    )
                                )
                                # 🆕 MA5 리드 몸통 탄생 인정
                                # subtick_equilibrium(평행→GC)과 별도 경로:
                                _cur_body_gc = (_ma5_cur - _ma10_cur) >= (_eq_margin - _FLOAT_EPS)

                                # 🔒 잔존 미아 1번 폐쇄 (ETH 21:54 헌법 위반 통로)
                                # 직전봉 이미 GC 유지(gap_prev 음수)를 새 birth로 오인
                                # 대체 통로: _true_dc_body_birth 유지
                                _true_lead_body_birth = False

                                # 🆕 DC→강한몸통 정석 GC 탄생 인정 (SSoT 재사용)
                                # 기존 _is_true_gc_birth(진짜 DC→GC 탄생 구조)를 그대로 소비
                                # + 현재봉 몸통(_cur_body_gc) 확인 → 새 판정 신설/중복 없음
                                _true_dc_body_birth = bool(
                                    _is_true_gc_birth
                                    and _cur_body_gc
                                )

                                if _true_dc_body_birth and not _true_lead_body_birth and not (_subtick_equilibrium and _cur_body_strong):
                                    logger.info(
                                        f"✅ [SUBTICK-BODY-BIRTH-DC] {symbol} "
                                        f"DC→강한몸통 정석GC 탄생 인정(SSoT) | "
                                        f"is_true_gc_birth={int(_is_true_gc_birth)} "
                                        f"cur_body={_ma5_cur - _ma10_cur:+.4f} "
                                        f"eq_margin={_eq_margin:.4f} "
                                        f"prev_dc={int(_prev_dc)} prev2_dc={int(_prev2_dc)}"
                                    )

                                indicators["subtick_body_birth"] = int(
                                    (_subtick_equilibrium and _cur_body_strong)
                                    or _true_lead_body_birth
                                    or _true_dc_body_birth
                                )
                                indicators["subtick_lead_body_birth"] = int(_true_lead_body_birth)
                                indicators["subtick_dc_body_birth"] = int(_true_dc_body_birth)

                                if _is_true_gc_birth and not _prev2_dc:
                                    logger.info(
                                        f"✅ [SHORT-DC-PASS] {symbol} "
                                        f"상승장 1봉눌림→재GC 정상인정 | "
                                        f"gap10-20={_gc_gap_1020:+.3f}%"
                                    )
                                elif _is_true_gc_birth and _near_dc_birth and not _prev_dc:
                                    logger.info(
                                        f"✅ [NEAR-DC-BIRTH-PASS] {symbol} "
                                        f"0~0.5틱 DC 수렴 + 전전봉확실DC → 진짜 GC 탄생 인정 | "
                                        f"gap10-20={_gc_gap_1020:+.3f}%"
                                    )
                                # 직전 확정봉 GC 유지 여부 — downstream SUBTICK·SHAPE-V2 휩소 방어 SSoT
                                _prev_already_gc = (
                                    float(_s5.iloc[-2]) > float(_s10.iloc[-2])
                                )
                        except Exception:
                            pass

                        # 진짜 DC→GC 탄생 여부를 Strategy가 동일 SSoT로 소비한다.
                        indicators["gajami_true_gc_birth"] = int(
                            _is_true_gc_birth
                        )
                        indicators["gajami_prev_already_gc"] = int(_prev_already_gc)

                        dur_5x20_bars = _calc_cross_duration_bars(mas.get('ma5'), mas.get('ma20'), lookback=20)
                        dur_10x20_bars = _calc_cross_duration_bars(mas.get('ma10'), mas.get('ma20'), lookback=20)
                        # 5x20 역할 분리:
                        # - 5x10 생애주기/DUR 자체에는 흡수하지 않는다.
                        # - 직접 GC 자격(recent_cross_ok)은 5x10 확정 DUR이 담당한다.
                        # - 5x20은 GoldenEvidence freshness와 reset-GC 위험 판별에 별도 소비한다.
                        
                        # ══════════════════════════════════════════════════════
                        # 🔒 최근성 판정
                        # ══════════════════════════════════════════════════════
                        # - 직접 GC 최근성 자격은 확정봉 MA5×MA10 DUR 하나가 SSoT.
                        # - MA5×MA20은 GoldenEvidence freshness와 reset-GC 위험 판별에만 소비.
                        # - MA10×MA20 오래됨은 비RANGING의 중기 신선도 보조 veto로 별도 소비.
                        # - NOTE: 과도 골든은 Analysis에서 차단하지 않음
                        #         (꼭지 리스크는 Strategy/Risk/Exit에서 관리)
                        # ══════════════════════════════════════════════════════
                        
                        # ✅ 형상 절대형(B안): recent_cross OR 구조 차단
                        # 단, "정답지 보호" 예외 1개만 허용:
                        # - 5x10 골든크로스 직후 2봉(≤2) 이내는 초입으로 인정 (발목~종아리 진입 보장)
                        # 🔒 dur_5x10_bars 누락 시 복원용: closed MA5x10을 lookback=60으로 재계산
                        _raw_dur_actual_5x10 = _calc_cross_duration_bars(
                            _ma5_closed,
                            _ma10_closed,
                            lookback=60,
                            allow_converge_birth=_allow_converge_birth,
                            converge_max_bars=2 if _allow_converge_birth else 0,
                        )
                        _dur_fill_candidate = (
                            int(dur_5x10_bars)
                            if dur_5x10_bars is not None
                            else _raw_dur_actual_5x10
                        )

                        # 🔧 [DUR-MISSING-FILL]
                        # 목적:
                        # - dur_5x10_bars=None 때문에 Strategy에서 dur=999로 떨어지는 missing 케이스 보정
                        # - 이미 같은 블록에서 계산한 _raw_dur_actual_5x10만 사용
                        # - dur 임계 완화가 아니라 누락된 실제 dur 복원만 수행
                        if dur_5x10_bars is None and _dur_fill_candidate is not None:
                            try:
                                dur_5x10_bars = int(_dur_fill_candidate)
                                logger.info(
                                    f"🟢 [DUR-MISSING-FILL] {symbol} "
                                    f"dur_5x10_bars None → actual5x10={dur_5x10_bars} 복원 | "
                                    f"source=closed_ma_5x10"
                                )
                            except Exception as _dur_fill_e:
                                logger.debug(
                                    f"⚠️ [DUR-MISSING-FILL-ERR] {symbol} {_dur_fill_e}"
                                )

                        # 🔒 DUR-SSOT 최종 export — 상태 캐시/old-wave 상속 금지
                        # raw 계산 + 기존 SHALLOW/FLICKER 전처리 결과만 단일 정본으로 사용한다.
                        _dur_actual_5x10 = (
                            int(dur_5x10_bars)
                            if dur_5x10_bars is not None
                            else None
                        )
                        indicators["dur_5x10_bars"] = _dur_actual_5x10
                        result["dur_5x10_bars"] = _dur_actual_5x10

                        # 🔬 [ACTIVE-TLC-P5-DUR-CLOSED-0815]
                        # Active TLC 부모와 같은 확정봉 시간축의 MA5>MA10 GC 나이.
                        # 기존 RT dur은 진입 SSoT로 그대로 두고 TLC 결합용만 분리한다.
                        # 🔒 [TLC-DUR-CLOSED-BAR-PARITY-0904] ★506-R1
                        # canonical dur과 동일한 확정봉 MA 시리즈(_ma5_closed/_ma10_closed)를 소비한다.
                        _tlc_dur_5x10_closed = (
                            _calc_cross_duration_bars(
                                _ma5_closed,
                                _ma10_closed,
                                lookback=60,
                                allow_converge_birth=False,
                            )
                            if (
                                _ma5_closed is not None
                                and _ma10_closed is not None
                                and len(_ma5_closed) >= 3
                                and len(_ma10_closed) >= 3
                            )
                            else None
                        )
                        indicators["tlc_dur_5x10_closed"] = (
                            int(_tlc_dur_5x10_closed)
                            if _tlc_dur_5x10_closed is not None
                            else -1
                        )

                        # 🔒 [PATCH1-C] 장기선 판정은 dur 확정 후 1회만 계산한다.
                        # (구 위치 L6342는 dur 미확정 상태라 override가 dur를 볼 수 없었음)
                        _long_gate_ok, _long_gate_fail = _long_ma_alive_verdict(
                            slope_ma60,
                            slope_ma120,
                            regime,
                            ma5_slope,
                            _dur_actual_5x10,
                        )
                        long_ma_strong_down_veto = bool(
                            _long_gate_fail == "LONG_STRONG_DOWN"
                        )
                        long_ma_bearish = bool(not _long_gate_ok)
                        td_ma60_down_raw = bool(
                            regime == MarketRegime.TRENDING_DOWN
                            and slope_ma60 < 0.0
                        )
                        td_ma60_down_observe = bool(
                            td_ma60_down_raw
                            and not long_ma_bearish
                        )
                        td_ma60_down_veto = bool(
                            regime == MarketRegime.TRENDING_DOWN
                            and long_ma_bearish
                        )
                        long_ma_veto = bool(long_ma_bearish)
                        logger.debug(
                            f"[LONG-MA-BEARISH-CHECK] {symbol} | "
                            f"bearish={int(long_ma_bearish)} "
                            f"dur={_dur_actual_5x10} "
                            f"raw_td_ma60_down={int(td_ma60_down_raw)} "
                            f"observe={int(td_ma60_down_observe)} "
                            f"strong_down={int(long_ma_strong_down_veto)} "
                            f"veto={int(long_ma_veto)} "
                            f"ma60={ma60_val:.2f} ma120={ma120_val:.2f} "
                            f"s60={slope_ma60:.6f}(<0) "
                            f"s120={slope_ma120:.6f}(<=0)"
                        )
                        if td_ma60_down_observe:
                            logger.info(
                                f"👁️ [TD-MA60-DOWN-OBSERVE] {symbol} "
                                f"TD MA60 단독 하락 관측만 | "
                                f"dur={_dur_actual_5x10} "
                                f"ma60={ma60_val:.2f} ma120={ma120_val:.2f} "
                                f"s60={slope_ma60:+.6f} s120={slope_ma120:+.6f}"
                            )

                        _closed_gc_5x10_ok = bool(
                            _dur_actual_5x10 is not None
                            and _dur_actual_5x10 <= RECENT_GC_MAX_BARS
                        )
                        indicators["closed_gc_5x10_ok"] = int(_closed_gc_5x10_ok)
                        result["closed_gc_5x10_ok"] = int(_closed_gc_5x10_ok)

                        if (
                            dur_5x10_bars is not None
                            and _raw_dur_actual_5x10 is not None
                            and int(_raw_dur_actual_5x10) > int(dur_5x10_bars) + 1
                        ):
                            logger.debug(
                                f"🔒 [DUR-ACTUAL-LATCH] {symbol} "
                                f"actual5x10 {_raw_dur_actual_5x10}→{dur_5x10_bars}"
                            )
                        _dur_actual_5x20 = _calc_cross_duration_bars(
                            mas.get('ma5'), mas.get('ma20'), lookback=60
                        )
                        _dur_actual = min(
                            _dur_actual_5x10 if _dur_actual_5x10 is not None else 999,
                            _dur_actual_5x20 if _dur_actual_5x20 is not None else 999
                        )
                        _dur_actual = None if _dur_actual == 999 else _dur_actual
                        # 추세 생존(MA5/MA10 동시 상향 + 정배열) 시 유효기간 1.5배 연장
                        # 다종목 검증 후 배수 재조정 필요
                        _bars_limit = RECENT_GC_MAX_BARS
                        if ma5_slope > 0 and ma10_slope > 0 and ma5 > ma10:
                            _bars_limit = int(RECENT_GC_MAX_BARS * 1.5)
                        # 🆕 TREND CONTINUING SSoT (Fix2/Fix3 공용 단일 정의)
                        # 정배열(ma5>ma10>ma20) + 쌍방상향 = 추세 지속 상태
                        _local_trend_birth = bool(
                            dur_5x10_bars is not None
                            and int(dur_5x10_bars) <= SoleusGajamiConstants.NEWBORN_MAX_DUR
                            and ma5 > ma10 > ma20
                            and ma5_slope > 0
                            and ma10_slope >= SoleusGajamiConstants.MA10_SLOPE_NEWBORN_MIN
                            and slope_ma20_df is not None
                            and float(slope_ma20_df) >= 0.0
                            and not long_ma_veto
                        )
                        indicators["gajami_local_trend_birth"] = int(_local_trend_birth)

                        _trend_continuing = bool(
                            (
                                regime == MarketRegime.TRENDING_UP
                                or _local_trend_birth
                            )
                            and ma5 > ma10 > ma20
                            and ma5_slope > 0
                            and (ma10_slope >= 0 or _local_trend_birth)
                            and not long_ma_veto
                        )
                        
                        logger.debug(
                            f"[TREND-CONTINUING] {symbol} | ok={_trend_continuing} "
                            f"local_birth={int(_local_trend_birth)} "
                            f"regime={regime.value if hasattr(regime,'value') else regime} "
                            f"align={'ma5>ma10>ma20' if ma5>ma10>ma20 else 'NOT'} "
                            f"s5={ma5_slope:.5f} s10={ma10_slope:.5f} "
                            f"s20={float(slope_ma20_df or 0.0):.5f} "
                            f"long_veto={int(long_ma_veto)}"
                        )

                        # 🔒 [MIDTREND-RESCUE-REMOVE-0817] dur=None은 진입 자격으로 구제하지 않는다.
                        # 실제 GC age는 DUR-MISSING-FILL에서 closed MA5x10 lookback=60으로 먼저 복원한다.

                        _is_ma10_birth_newborn = bool(
                            dur_5x10_bars is not None
                            and int(dur_5x10_bars) <= SoleusGajamiConstants.NEWBORN_MAX_DUR
                            and gc_5_10
                            and ma5 >= ma10
                            and ma5_slope > 0.0
                            and (
                                ma10_slope >= SoleusGajamiConstants.MA10_SLOPE_NEWBORN_MIN
                                or (slope_ma20_df is not None and float(slope_ma20_df) >= 0.0)
                            )
                        )
                        MA5_REACCEL_EPS = 0.00005
                        ma5_reaccelerating = (
                            ma5_slope_seg_now > (ma5_slope_seg_prev + MA5_REACCEL_EPS)
                            and ma5_slope_seg_now > 0.0
                        )
                        # CORE-GATE(6_strategy)가 momentum 품질을 소비하도록 indicators 노출
                        indicators["gajami_spacing_expanding"] = int(bool(spacing_expanding))
                        indicators["gajami_ma5_reaccelerating"] = int(bool(ma5_reaccelerating))

                        # 🔒 [PATCH1-C] 위에서 dur 확정 후 계산한 장기선 판정을 그대로 재사용
                        # (중복 계산 제거 — dur 없이 재계산하는 구멍 원천 봉쇄)
                        _long_alive_verdict_ok = _long_gate_ok
                        _long_alive_verdict_fail = _long_gate_fail
                        _long_alive_ok = bool(_long_alive_verdict_ok)

                        # 🔒 CONVERGE-BIRTH origin SSoT
                        # - GC-5x10-CONVERGE-BIRTH를 이미 탄생으로 인정한 봉은
                        #   dur_5x20이 오래됐다는 이유만으로 FAKE_LATE_RESET_GC 처리하지 않는다.
                        # - _is_birth_traintrack은 downstream(recent/trigger 소비)이므로 여기서 쓰지 않는다.
                        _birth_prev_dc = bool(
                            _prev_dc if int(dur_5x10_bars or 0) == 1
                            else _prev2_dc if int(dur_5x10_bars or 0) == 2
                            else False
                        )
                        _fake_late_reset_gc_candidate = bool(
                            dur_5x10_bars is not None
                            and int(dur_5x10_bars) <= SoleusGajamiConstants.NEWBORN_MAX_DUR
                            and dur_5x20_bars is not None
                            and int(dur_5x20_bars) > RECENT_GC_MAX_BARS
                            and not _is_true_gc_birth
                            and not _birth_prev_dc
                        )

                        # 🔒 [RESET-GC-ORIGIN-SSOT-0827] origin 면제 권한 철거.
                        # 수렴(convergence)은 형상 증거일 뿐 출생 증명이 아니므로
                        # fake-late-reset 판정을 세탁하는 권한을 주지 않는다.
                        # 하루 1회(ONDO 11:04) 발화 = 오진입 직접 원인이었다.
                        _converge_birth_origin_ok = False
                        indicators["gajami_converge_birth_origin_ok"] = 0

                        _fake_late_reset_gc_base = bool(
                            _fake_late_reset_gc_candidate
                        )

                        _strong_momentum_reset_gc_ok = bool(
                            _fake_late_reset_gc_base
                            and spacing_expanding
                            and ma5_reaccelerating
                            and _long_alive_ok
                            and ma5 > ma10 > ma20
                            and ma5_slope > 0.0
                            and ma10_slope > 0.0
                            and float(slope_ma20_df or 0.0) > 0.0
                        )
                        indicators["gajami_strong_momentum_reset_gc_ok"] = int(
                            _strong_momentum_reset_gc_ok
                        )

                        _fake_late_reset_gc = bool(
                            _fake_late_reset_gc_base
                            and not _strong_momentum_reset_gc_ok
                        )
                        indicators["gajami_fake_late_reset_gc"] = int(
                            _fake_late_reset_gc
                        )

                        recent_cross_ok = bool(
                            dur_5x10_bars is not None
                            and int(indicators.get("closed_gc_5x10_ok", 0)) == 1
                            and dur_5x10_bars <= _bars_limit
                            and not (
                                dur_10x20_bars is not None
                                and dur_10x20_bars > RECENT_GC_MAX_BARS
                                and regime != MarketRegime.RANGING
                                and not _is_ma10_birth_newborn
                            )
                            and not _fake_late_reset_gc
                        )

                        # 🔒 GC 최근성 역할 분리:
                        # - recent_cross_ok의 직접 GC 자격은 확정봉 5x10 DUR만 사용한다.
                        # - 5x20은 여기서 OR하지 않는다.
                        # - 5x20 freshness는 GoldenEvidence(_dur_actual), 오래된 reset 판별은
                        #   FAKE_LATE_RESET_GC에서 각각 별도 소비한다.

                        # recent_cross_reason: 차단 사유 로그 전용 (판정 영향 없음)
                        recent_cross_reason = ""
                        if _fake_late_reset_gc:
                            recent_cross_reason = (
                                f"FAKE_LATE_RESET_GC("
                                f"5x10:{dur_5x10_bars},5x20:{dur_5x20_bars},"
                                f"true_gc:{int(_is_true_gc_birth)},prev_dc:{int(_prev_dc)},"
                                f"birth_dc:{int(_birth_prev_dc)})"
                            )
                        elif _strong_momentum_reset_gc_ok:
                            recent_cross_reason = "MOMENTUM_RESET_GC"
                            logger.info(
                                f"✅ [MOMENTUM-RESET-GC] {symbol} "
                                f"강모멘텀 reset-GC 면제 | "
                                f"expand={int(spacing_expanding)} "
                                f"reacc={int(ma5_reaccelerating)} "
                                f"long={int(_long_alive_ok)} "
                                f"s5={ma5_slope:+.5f} s10={ma10_slope:+.5f} "
                                f"s20={float(slope_ma20_df or 0.0):+.5f} "
                                f"s60={float(slope_ma60 or 0.0):+.6f} "
                                f"s120={float(slope_ma120 or 0.0):+.6f}"
                            )
                        elif _converge_birth_detected:
                            recent_cross_reason = "CONVERGE_BIRTH_FILTERED"
                        
                        # 🔒 [ALL-OLD-DEAD-PURGE-0823]
                        # recent_cross_ok는 앞에서 closed_gc_5x10_ok==1을 요구한다.
                        # closed_gc_5x10_ok는 dur_5x10<=RECENT_GC_MAX_BARS일 때만 True이므로,
                        # 뒤의 all_old(dur_5x10>RECENT_GC_MAX_BARS)는
                        # recent_cross_ok=True 상태에서는 구조적으로 도달 불가하다.
                        # 따라서 이 블록은 True→False 판정권이 없고,
                        # 이미 False인 건의 recent_cross_reason만 "모두오래됨"으로 덮어쓴다.
                        
                        # ══════════════════════════════════════════════════════
                        # NOTE: 과도 골든 차단 정책 (현재 철학)
                        # ══════════════════════════════════════════════════════
                        # 과거에는 "과도 골든(8개 이상)"을 Analysis에서 차단했으나,
                        # 현재는 자동매매 철학에 맞게 자격 차단을 하지 않는다.
                        # 
                        # 현재 정책:
                        # - golden_count=8~9는 강한 추세/가자미근 초입 가능
                        # - 자동매매는 사전 공포 차단을 하지 않는다
                        # - 꼭지 리스크는 Strategy/Risk/Exit에서 관리
                        # ══════════════════════════════════════════════════════
                        
                        # ══════════════════════════════════════════════════════
                        # 🔒 3m 단타 정석 보강:
                        # 전환 이벤트 이후 구조가 강화된 경우 보호
                        # ══════════════════════════════════════════════════════
                        # 철학:
                        # - 전환 이벤트는 "계기"
                        # - 진입 판단은 "현재 형상"
                        # - 3m 단타에서는 "지금 모양이 무릎이냐"가 더 중요
                        # ══════════════════════════════════════════════════════
                        # (STRUCTURE DISABLED 블록 제거 — recent_cross_ok SSoT 단일화)
                        
                        # ══════════════════════════════════════════════════════
                        # NOTE: 계측 로그 철학
                        # ══════════════════════════════════════════════════════
                        # 아래 MEASURE 로그들은 진입/판정에 영향 없는 순수 계측용이다.
                        # 자동매매의 기계성을 해치지 않으며, 문제 분석 종료 후에도
                        # debug로 유지한다.
                        # ══════════════════════════════════════════════════════
                        
                        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
                        # 🔍 [MEASURE ONLY] recent_cross 계측 (판정 완료 후 기록)
                        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
                        try:
                            logger.debug(
                                f"[RECENT-CROSS] {symbol} | "
                                f"dur_5x10={dur_5x10_bars} "
                                f"dur_5x20={dur_5x20_bars} "
                                f"dur_10x20={dur_10x20_bars} | "
                                f"MAX={RECENT_GC_MAX_BARS} | "
                                f"ok={int(recent_cross_ok)} "
                                f"ma10_birth={int(_is_ma10_birth_newborn)} "
                                f"reason={recent_cross_reason if recent_cross_reason else 'N/A'}"
                            )
                        except Exception:
                            pass
                        
                        # ── 3) 가자미근 형상 (핵심 철학)
                        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
                        # 🆕 PATCH-SOLEUS-CONVERGE: 수렴 초입 인식 (2026-01-27)
                        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
                        # 문제:
                        # - 완벽한 가자미근 형성 (MA5/10/20 수렴)
                        # - slope ≈ 0 이라고 차단 (s5:0.0000 < 0.0010)
                        # - "완전 수렴 = 가자미근 시작점"인데 인식 못함
                        #
                        # 해결:
                        # - MA 수렴 감지: abs(ma5-ma10)/ma10 <= 0.01%
                        # - 수렴 + 강한 정배열(golden >= 8) → slope 극도 완화
                        # - 기존 → slope 기준 유지
                        #
                        # 철학:
                        # - 가자미근 = "MA 완전 수렴 후 위로 접히기 시작"
                        # - 이 순간에는 slope가 0 또는 미세 음수 가능
                        # - 단, 강한 정배열일 때만 허용 (구조 안전장치)
                        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
                        
                        # 1) MA 수렴 감지 (0.01% 이내)
                        is_ma_converged = False
                        try:
                            if ma10 > 0 and ma20 > 0:
                                gap_5_10 = abs(ma5 - ma10) / ma10
                                gap_10_20 = abs(ma10 - ma20) / ma20
                                is_ma_converged = (gap_5_10 <= 0.0001 and gap_10_20 <= 0.0001)
                        except Exception:
                            is_ma_converged = False
                        
                        # 2) indicators에서 골든크로스 개수 추출
                        golden_cnt = int(indicators.get("golden_count", 0))
                        
                        # 3) 수렴 + 강한 정배열 → 초입 허용 (정석 확장)
                        # - 3m 초입에서는 golden_cnt 6~7에서도 "가격=MA5 수렴 시작점"이 충분히 발생한다.
                        # - 단, MA10 우상향은 유지 (가짜 수렴 제거)
                        is_price_on_ma5 = False
                        try:
                            if ma5 > 0 and cur_price > 0:
                                # 가격이 MA5에 붙어있는가? (≤0.10%)
                                is_price_on_ma5 = (abs(cur_price - ma5) / ma5) <= 0.001
                        except Exception:
                            is_price_on_ma5 = False
                        
                        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
                        # 🎯 가자미근 형상 판정 (체제 독립, 코드 상수 사용)
                        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
                        
                        # 1. 수렴 후보 판정 (gap 기반, 강화)
                        gap_5_10 = abs(ma5 - ma10) / ma10 if ma10 > 0 else 999
                        
                        converge_candidate = bool(
                            is_ma_converged                              # 완전 수렴 (0.01%)
                            or is_price_on_ma5                           # 가격=MA5
                            or (gap_5_10 <= SoleusGajamiConstants.GAP_5_10_TIGHT)  # 초근접 (0.1%, GC 불문)
                            or (gap_5_10 <= SoleusGajamiConstants.GAP_5_10_EARLY and 
                                golden_cnt >= SoleusGajamiConstants.GC_COUNT_EARLY)  # 조기 수렴 (0.5% + GC≥2)
                        )
                        
                        # 2. slope 요구치 (형상 전용, 실측 기반 완화)
                        ma5_slope_req = SoleusGajamiConstants.MA5_SLOPE_DEFAULT
                        ma10_slope_req = SoleusGajamiConstants.MA10_SLOPE_DEFAULT
                        
                        # 수렴 구간은 완화
                        # - 압축형 초입(gap 초근접)은 골든카운트 누락/지연이 있어도 수렴 완화가 걸려야 정석
                        if converge_candidate and (
                            is_price_on_ma5
                            or (gap_5_10 <= SoleusGajamiConstants.GAP_5_10_TIGHT)
                            or (golden_cnt >= SoleusGajamiConstants.GC_CONVERGE_MIN)
                        ) and gap_5_10 >= 0.0001:
                            ma5_slope_req = (
                                SoleusGajamiConstants.MA5_SLOPE_PRICE_ON_MA5
                                if is_price_on_ma5
                                else SoleusGajamiConstants.MA5_SLOPE_CONVERGE
                            )
                            ma10_slope_req = SoleusGajamiConstants.MA10_SLOPE_CONVERGE
                        
                        # 🔒 정상 모드: 형상만 판단
                        normal_ok = (
                            ma5_slope >= ma5_slope_req and
                            ma10_slope >= ma10_slope_req
                        )
                        # GC 탄생봉(dur<=2): MA10은 구조적으로 0 이하 → is_gajami_shape CORE GATE와 동일 기준
                        # 근거: GC 직후 MA10이 아직 하락/횡보 = 구조적 필연, MA5 상승 확인으로 대체
                        _newborn_upfold_ok = (
                            dur_5x10_bars is not None
                            and int(dur_5x10_bars) <= SoleusGajamiConstants.NEWBORN_MAX_DUR
                            and ma5_slope >= ma5_slope_req
                            and ma10_slope >= ma10_slope_req
                        )
                        shape_upfold_ok = bool(normal_ok or _newborn_upfold_ok)
                        
                        # 수렴 초입 진입 로그
                        if (
                            shape_upfold_ok
                            and converge_candidate
                            and golden_cnt >= SoleusGajamiConstants.GC_CONVERGE_MIN
                        ):
                            converge_type = "MA수렴" if is_ma_converged else "가격=MA5"
                            logger.debug(
                                f"✅ [CONVERGE-ENTRY] {symbol} {converge_type} + {golden_cnt}골든 "
                                f"→ 가자미근 초입 인정 | s5:{ma5_slope:.4f} "
                                f"s10:{ma10_slope:.4f} s20_pct:{ma20_slope_pct_ind:.4f} "
                                f"ma20_not_decel={int(bool(indicators.get('gajami_ma20_not_decel', 1)))} "
                                f"ma20_tlc={float(indicators.get('gajami_ma20_tlc_cur', 0.0)):+.4f}/"
                                f"{float(indicators.get('gajami_ma20_tlc_prev', 0.0)):+.4f}"
                            )
                        
                        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
                        # 🔍 [MEASURE ONLY] shape_upfold 계측 (판정 완료 후 기록)
                        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
                        try:
                            converge_status = "N/A"
                            if 'is_ma_converged' in locals() and 'is_price_on_ma5' in locals():
                                if is_ma_converged:
                                    converge_status = "MA수렴"
                                elif is_price_on_ma5:
                                    converge_status = "가격=MA5"
                                else:
                                    converge_status = "수렴없음"
                            
                            logger.debug(
                                f"[SHAPE-UPFOLD] {symbol} | "
                                f"s5={ma5_slope:.5f} "
                                f"s10={ma10_slope:.5f} "
                                f"s20_pct={ma20_slope_pct_ind:.5f} | "
                                f"ok={int(shape_upfold_ok)} "
                                f"converged={int(is_ma_converged) if 'is_ma_converged' in locals() else -1} "
                                f"price_on_ma5={int(is_price_on_ma5) if 'is_price_on_ma5' in locals() else -1} "
                                f"status={converge_status}"
                            )
                        except Exception:
                            pass
                        
                        # ======================================================
                        # 🔒 PATCH-B6: 가자미근 물리적 불가능 종목 차단
                        # ======================================================
                        # 목적:
                        # - ATR이 너무 작아 1틱 + 수수료조차 회수 불가능한 종목 제거
                        # - SHIB, DOGE류 초미세 가격 종목 자동 제외
                        # 철학:
                        # - "가자미근은 공간이 있어야 존재한다"
                        # - SHIB 예외 처리 ❌ / 전략 수학적 전제 필터 ⭕
                        # ======================================================
                        
                        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
                        # 🔒 Upbit 호가 단위 반영 (TIER3 동적 DIR_MIN 계산에 사용됨)
                        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
                        # 🔧 canonical 호가단위 (≥10000→10 오매핑 버그 폐기)
                        tick_size = get_tick_size(cur_price)
                        
                        # ATR 절대값
                        # ✅ FIX: indicators의 ATR SSoT 사용 (Series/None 안전)
                        atr_price = safe_extract_scalar(indicators.get("atr"), 0.0)
                        if atr_price < 0:
                            atr_price = 0.0
                        
                        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
                        # ✅ 정석: ATR은 형상 판정에서 제거
                        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
                        # - ATR은 포지션 크기, 손절 폭, 목표 수익 계산에만 사용
                        # - 가자미근 = 형상이므로 변동성과 무관
                        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
                        # 🔒 ACTIVE: 물리적 불가능 종목 차단 (PATCH-B6)
                        # - ATR < (tick_size * 2.0) → 수수료·호가 극복 불가
                        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
                        min_required_move = tick_size * 2.0
                        gajami_physically_possible = True

                        # 🔒 FALSE-BIRTH-OVERAGED-GC:
                        # dur_5x10=1 리셋이지만 dur_5x20이 이미 오래된 경우
                        # → 장기 GC 일시접촉 가짜 탄생 차단
                        # 사전계산: 확정봉 종가 MA5 상향돌파 (FALSE-BIRTH 면제용)
                        _ma5_cross_up_pre = False
                        try:
                            if len(df) >= 2 and hasattr(mas, 'get'):
                                _s5_pre = mas.get('ma5', None)
                                if _s5_pre is not None and len(_s5_pre) >= 2:
                                    _pc = float(df["close"].iloc[-2])
                                    _pm5 = float(_s5_pre.iloc[-2])
                                    _cc = float(df["close"].iloc[-1])
                                    _cm5 = float(_s5_pre.iloc[-1])
                                    if _cm5 > 0 and _pm5 > 0:
                                        _ma5_cross_up_pre = bool(
                                            (_pc < _pm5) and (_cc >= _cm5 * 0.999)
                                        )
                        except Exception:
                            pass

                        if (
                            dur_5x10_bars is not None and int(dur_5x10_bars) <= SoleusGajamiConstants.NEWBORN_MAX_DUR
                            and dur_5x20_bars is not None and int(dur_5x20_bars) > RECENT_GC_MAX_BARS
                            and bool(indicators.get("gajami_subtick_dc_converge_birth", 0))
                            and not _prev_dc
                            and not _ma5_cross_up_pre
                            and not bool(indicators.get("subtick_dc_body_birth", 0))
                            # 🔧 SUBTICK-BODY-BIRTH-DC(_true_dc_body_birth) 인정 탄생봉 면제
                            # true birth ↔ fake birth 충돌 제거. dict.get으로 스코프 안전
                        ):
                            gajami_physically_possible = False
                            indicators["gajami_false_birth_overaged_gc"] = int(True)
                            logger.info(
                                f"🚫 [FALSE-BIRTH-OVERAGED-GC] {symbol} "
                                f"장기GC 일시접촉 가짜탄생 차단 | "
                                f"dur_5x10={dur_5x10_bars} "
                                f"dur_5x20={dur_5x20_bars} "
                                f"MAX={RECENT_GC_MAX_BARS} "
                                f"converge_birth=1 prev_dc=0"
                            )
                        else:
                            indicators["gajami_false_birth_overaged_gc"] = int(False)
                        
                        # ✅ 저가(초미세 호가) 종목만 물리성 차단 적용
                        # - 목적: SHIB류 자동 제외
                        # - 개선: 일반 가격대 종목은 형상 판정에서 ATR로 탈락시키지 않음 (리스크/포지션에서 처리)
                        if cur_price < 1 and tick_size > 0 and atr_price > 0 and atr_price < min_required_move:
                            gajami_physically_possible = False
                            logger.info(
                                f"🚫 [BLOCK-LOW-ATR] {symbol} "
                                f"ATR={atr_price:.8f} < 최소요구={min_required_move:.8f} "
                                f"(tick={tick_size:.8f}) → 가자미근 진입 차단"
                            )
                        
                        # 🔒 SUBTICK 공통 SSoT: MA10 추세 생존 판단 (HIGH/MID 양쪽 소비)    
                        # - 본체 _tlc_ma10_alive와 동치식, 정의 시점 불일치로 인라인 계산
                        # - isfinite + 분모>0 명시 가드 + try/except 이중 방어
                        # - 데이터 부족(len<13)/nan/inf/분모0/iloc오류 → 모든 경로 False 보장
                        _ma10_alive_subtick = False
                        try:
                            _ma10_s_st = mas.get('ma10')
                            if _ma10_s_st is not None and len(_ma10_s_st) >= 13:
                                _m10_cur_0 = float(_ma10_s_st.iloc[-2])
                                _m10_cur_1 = float(_ma10_s_st.iloc[-7])
                                _m10_prev_0 = float(_ma10_s_st.iloc[-8])
                                _m10_prev_1 = float(_ma10_s_st.iloc[-13])
                                if (
                                    math.isfinite(_m10_cur_0)
                                    and math.isfinite(_m10_cur_1)
                                    and math.isfinite(_m10_prev_0)
                                    and math.isfinite(_m10_prev_1)
                                    and _m10_cur_1 > 0.0
                                    and _m10_prev_1 > 0.0
                                ):
                                    _ma10_cur_st = (_m10_cur_0 - _m10_cur_1) / _m10_cur_1 * 100.0
                                    _ma10_prev_st = (_m10_prev_0 - _m10_prev_1) / _m10_prev_1 * 100.0
                                    _MA10_FLOOR_ST = SoleusGajamiConstants.MA10_SLOPE_HALT * 100.0
                                    _MA10_POSITIVE_ABS_FLOOR_ST = SoleusGajamiConstants.MA10_POSITIVE_ABS_FLOOR * 100.0
                                    _ma10_alive_subtick = bool(
                                        _ma10_cur_st >= _MA10_FLOOR_ST
                                        and (
                                            _ma10_prev_st <= 0.0
                                            or _ma10_cur_st >= _ma10_prev_st * 0.5
                                            or _ma10_cur_st >= _MA10_POSITIVE_ABS_FLOOR_ST
                                        )
                                    )
                        except Exception:
                            _ma10_alive_subtick = False

                        # 🔒 SUBTICK-MA-BODY: MA5-MA10 몸통 < 1틱 → 관측 로그만 남김
                        # - 대상: 100원 이상 (tick=1원 이상 구간)
                        # - DOGE/TRON: cur_price<1 이미 위에서 차단
                        # - 100원+ 구간도 SUBTICK-BODY-OBSERVE로 PHYS hard 차단 금지
                        if cur_price >= 100 and tick_size >= 1 and ma10 > 0:
                            _ma_body_abs = abs(ma5 - ma10)
                            if _ma_body_abs < tick_size:
                                _is_birth_subtick_high = bool(
                                    _is_ma10_birth_newborn
                                    and recent_cross_ok
                                    and gc_5_10
                                    and ma5 >= ma10
                                    and (_is_true_gc_birth or _prev_already_gc)
                                )
                                _is_birth_subtick_low_tick = bool(
                                    _is_ma10_birth_newborn
                                    and _ma10_alive_subtick
                                    and recent_cross_ok
                                    and gc_5_10
                                    and ma5 >= ma10
                                )
                                if not (_is_birth_subtick_high or _is_birth_subtick_low_tick):
                                    logger.info(
                                        f"👁️ [SUBTICK-BODY-OBSERVE] {symbol} "
                                        f"MA몸통={_ma_body_abs:.4f} < tick={tick_size:.1f} "
                                        f"→ MA몸통은 관측값만 기록, PHYS hard 차단 금지 | "
                                        f"true_birth={int(_is_true_gc_birth)} "
                                        f"ma10_alive_st={int(_ma10_alive_subtick)}"
                                    )
                                elif _is_birth_subtick_low_tick and not _is_birth_subtick_high:
                                    logger.info(
                                        f"✅ [SUBTICK-BODY-EXEMPT-MA10ALIVE] {symbol} "
                                        f"MA몸통={_ma_body_abs:.4f} < tick={tick_size:.1f} "
                                        f"dur={dur_5x10_bars} ma10_alive=1 → 양자화 우회 면제"
                                    )

                        # 🔒 SUBTICK-MA-BODY-MID: 10~100원 구간 (tick=0.1원)
                        # MA몸통 0.3틱(0.03원) 미만은 관측 로그만 남김
                        # - 100원+ 구간도 SUBTICK-BODY-OBSERVE로 PHYS hard 차단 금지
                        # - SIGN류(10~100원): 별도 기준 (tick*0.3)
                        # - DEEP류(10~100원): 0.03원 이상 → 통과 유지
                        if 1 <= cur_price < 100 and tick_size > 0 and ma10 > 0:
                            _ma_body_mid = abs(ma5 - ma10)
                            _is_birth_subtick_mid_true = bool(
                                _is_ma10_birth_newborn
                                and recent_cross_ok
                                and gc_5_10
                                and ma5 >= ma10
                                and (_is_true_gc_birth or _prev_already_gc)
                            )
                            _is_birth_subtick_mid_alive = bool(
                                _is_ma10_birth_newborn
                                and _ma10_alive_subtick
                                and recent_cross_ok
                                and gc_5_10
                                and ma5 >= ma10
                            )
                            if _ma_body_mid < tick_size * 0.3 and not (
                                _is_birth_subtick_mid_true or _is_birth_subtick_mid_alive
                            ):
                                logger.info(
                                    f"👁️ [SUBTICK-MID-OBSERVE] {symbol} "
                                    f"MA몸통={_ma_body_mid:.4f} < tick*0.3={tick_size*0.3:.3f} "
                                    f"→ MA몸통은 관측값만 기록, PHYS hard 차단 금지 | "
                                    f"true_birth={int(_is_true_gc_birth)} "
                                    f"ma10_alive_st={int(_ma10_alive_subtick)}"
                                )
                            elif _is_birth_subtick_mid_alive and not _is_birth_subtick_mid_true:
                                logger.info(
                                    f"✅ [SUBTICK-MID-EXEMPT-MA10ALIVE] {symbol} "
                                    f"MA몸통={_ma_body_mid:.4f} < tick*0.3={tick_size*0.3:.3f} "
                                    f"dur={dur_5x10_bars} ma10_alive=1 → 중간가격대 양자화 우회 면제"
                                )

                        # ── 4) 최종 soleus 판정
                        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
                        # 🔍 [MEASURE ONLY] soleus 8게이트 계측 (판정 완료 후 기록)
                        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
                        try:
                            logger.debug(
                                f"[SOLEUS-MEASURE] {symbol} | t={df.index[-1] if hasattr(df, 'index') else 'NA'} | "
                                f"regime={regime.value if hasattr(regime, 'value') else regime} | "
                                f"shape={int(shape_upfold_ok)} "
                                f"recent={int(recent_cross_ok)} "
                                f"dir={int(direction_energy_ok)} "
                                f"ma20={int(ma20_ok)} "
                                f"window={int(gajami_window_open)} "
                                f"expand={int(spacing_expanding)} "
                                f"align={int(aligned_gate)} "
                                f"phys={int(gajami_physically_possible)} | "
                                f"GC={golden_cnt} "
                                f"s5={ma5_slope:.5f} "
                                f"s10={ma10_slope:.5f} "
                                f"s20_pct={ma20_slope_pct_ind:.5f} | "
                                f"gap5_10={ma5_ma10_gap:.4f} "
                                f"spread={spread_expand:.5f}"
                            )
                        except Exception:
                            pass
                        
                        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
                        # 🔒 SSoT: 가자미근 최종 정의 (이 블록만 진짜 판정, 과거 로직 제거됨)
                        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
                        # 철학:
                        #   가자미근 = 무릎 형상 (진입 시점 판단)
                        #   direction_energy_ok = 에너지 (진입 후 관리용)
                        #
                        # 필수 조건 (AND):
                        #   - shape_upfold_ok   : 형상 (5·10·20 상향 접힘)
                        #   - ma20_ok           : MA20 하락 중단 (평행 이상)
                        #   - aligned_gate      : 5·10·20 정배열
                        #
                        # 타입 구분 (OR):
                        #   - recent_cross_ok                   : 초입형 (골든 직후 무릎)
                        #   - (spacing_expanding and window_ok) : 펼쳐진형 (과열 아닐 때)
                        #
                        # ❌ direction_energy_ok 제외:
                        #   - direction_energy_ok는 진입 조건이 아님
                        #   - 무릎에서는 에너지가 약해도 진입
                        #   - 에너지는 포지션 사이즈/익절 전략에 사용
                        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
                        
                        # 🔒 None-safe 처리
                        window_ok = bool(gajami_window_open) if gajami_window_open is not None else False
                        
                        # 🔒 SOL 보강(정석): "펼쳐진형"은 재가속 증거가 필요
                        # - 초입형(recent_cross_ok)은 그대로(정답지 보호)
                        # - 휴식/평행 구간의 가짜 확장(spacing_expanding 착시) 차단
                        # MA5_REACCEL_EPS / ma5_reaccelerating → L5425 블록으로 이동
                        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
                        # 🎯 캔들 내부 MA5 돌파 감지 (Intrabar Cross)
                        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
                        # 철학:
                        # - 3분봉 내부에서 MA5 아래 → 위로 돌파 순간 포착
                        # - 미래 예측 X, 현재 봉 내부 사실만 사용
                        # - 종가 확정 대기 X, 돌파 발생 즉시 O
                        #
                        # 조건:
                        # - 저가 < MA5 (봉 시작 시 MA5 아래 있었음)
                        # - 고가 > MA5 (봉 내부에서 MA5 돌파함)
                        # - 종가 >= MA5 (돌파 유지 중)
                        #
                        # "판단은 느리게, 트리거는 빠르게"
                        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
                        
                        ma5_intrabar_cross_up = False
                        _had_pullback = False
                        _pb_sn = _pb_sp1 = _pb_sp2 = 0.0
                        try:
                            _ma5_s2 = mas.get('ma5', None)
                            if _ma5_s2 is not None and len(_ma5_s2) >= 4:
                                _pb_sn = float(_ma5_s2.iloc[-1] - _ma5_s2.iloc[-2])
                                _pb_sp1 = float(_ma5_s2.iloc[-2] - _ma5_s2.iloc[-3])
                                _pb_sp2 = float(_ma5_s2.iloc[-3] - _ma5_s2.iloc[-4])
                                _mpb = float(_ma5_s2.iloc[-1]) * 0.0003
                                _had_pullback = (
                                    (_pb_sp1 < -_mpb and abs(_pb_sp1) > abs(_pb_sn) * 0.5) or
                                    (_pb_sp2 < -_mpb and abs(_pb_sp2) > abs(_pb_sn) * 0.5)
                                )
                        except Exception:
                            pass
                        logger.debug(
                            f"[PULLBACK-CHECK] {symbol} | "
                            f"pb={_had_pullback} "
                            f"sn={_pb_sn:.4f} sp1={_pb_sp1:.4f} sp2={_pb_sp2:.4f}"
                        )
                        
                        try:
                            # 🔒 PATCH: 진짜 크로스 조건으로 수정
                            # - "근접(low<MA5<high)"이 아니라 "돌파(아래→위)"
                            # - 직전 봉 종가가 직전 MA5 아래 → 현재 봉 종가가 현재 MA5 이상
                            
                            if len(df) >= 2:
                                current_close = float(df["close"].iloc[-1])
                                prev_close = float(df["close"].iloc[-2])
                                
                                # 현재 MA5
                                ma5_val = float(current_mas.get('ma5', 0))
                                
                                # 직전 MA5 (안전하게 한 번만 가져오기)
                                ma5_series = mas.get('ma5', None) if hasattr(mas, 'get') else None
                                if ma5_series is not None and len(ma5_series) >= 2:
                                    prev_ma5 = float(ma5_series.iloc[-2])
                                else:
                                    prev_ma5 = ma5_val
                                
                                if ma5_val > 0 and prev_ma5 > 0:
                                    # 진짜 크로스: 직전 아래 → 현재 이상
                                    ma5_intrabar_cross_up = bool(
                                        (prev_close < prev_ma5)
                                        and (current_close >= ma5_val * 0.999)
                                    )
                                    
                                    if ma5_intrabar_cross_up:
                                        logger.info(
                                            f"⚡ [MA5-CROSS] {symbol} MA5 상향 돌파(종가 기준)! | "
                                            f"직전종가:{prev_close:.1f} < 직전MA5:{prev_ma5:.1f} | "
                                            f"현재종가:{current_close:.1f} >= 현재MA5:{ma5_val:.1f}"
                                        )
                                    indicators["ma5_intrabar_cross_up"] = int(ma5_intrabar_cross_up)
                        except Exception as e:
                            logger.info(
                                f"⚠️ [MA5-INTRABAR-CROSS] {symbol} 계산 오류: {e}"
                            )
                        
                        # ✅ INTRABAR-CROSS → RECENT 갱신 (GC 유지 조건 필수)
                        # - Dead-cat 방지: GC 유지중일 때만 최근성 인정
                        # - dur_* 값 미변조 (override만 수행)
                        # 🔒 PATCH: INTRABAR는 최근성 판정을 뒤집지 못한다
                        #   - 원칙: "신호 감지 보조"이지 "최근성 재판정 수단"이 아니다
                        #   - GC가 감지되었고(dur is not None), 1봉 이내일 때만 보조
                        #   - 오래된 GC(dur>1)는 절대 뒤집지 못함
                        
                        # 타입 안전성: dur을 int로 정규화 (런타임 에러 0% 보장)
                        try:
                            dur_5x10_i = int(dur_5x10_bars)
                        except (TypeError, ValueError):
                            dur_5x10_i = None
                        
                        intrabar_applicable = (
                            recent_cross_ok is False
                            and dur_5x10_i is not None
                            and dur_5x10_i <= 1
                            and not _fake_late_reset_gc
                        )
                        
                        if (
                            ma5_intrabar_cross_up
                            and intrabar_applicable
                            and gc_5_10
                            and ma5 > ma10
                        ):
                            recent_cross_ok = True
                            logger.debug(
                                f"✅ [RECENT_GC-INTRABAR] {symbol} "
                                f"봉내 MA5 돌파 + GC 유지 → 최근성 인정 | "
                                f"dur5x10={dur_5x10_bars} "
                                f"dur5x20={dur_5x20_bars} "
                                f"dur10x20={dur_10x20_bars}"
                            )
                        
                        if (
                            not recent_cross_ok
                            and gc_5_10
                            and not _is_true_gc_birth
                            and not _prev_dc          # 🆕 실제 DC 차단 면제
                        ):
                            logger.debug(
                                f"🚫 [GC-Rebound-Block] {symbol} "
                                f"DC없는 리셋GC → Rebound 차단 | dur={dur_5x10_bars}"
                            )

                        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
                        # 🔒 MA20 방향성 단일 Gate (df 기반)
                        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
                        # 철학:
                        
                        # - MA20 "방향성"은 df 기반 구조적 기울기로만 판단
                        # - recent 기반 국지적 계산은 방향 판정에 사용 금지
                        # - 가자미근 형상(shape)과 방향성(direction) 완전 분리
                        #
                        # 기준:
                        # - 3m: -0.0003 (평행~미세 하방까지만 허용)
                        # - 5m: -0.0005
                        # - 15m: -0.001
                        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
                        MA20_DIRECTION_MIN = SoleusGajamiConstants.MA20_SLOPE_MIN  # SSoT: -0.03%
                        
                        # df 기반 MA20 기울기가 이미 계산되어 있는지 확인
                        if 'slope_ma20_df' in locals() and slope_ma20_df is not None:
                            _ma20_prev = locals().get('slope_ma20_df_prev', slope_ma20_df)
                            _ma20_decelerating = (slope_ma20_df > _ma20_prev)  # 이전보다 덜 음수 = 둔화
                            ma20_direction_ok = (
                                slope_ma20_df >= MA20_DIRECTION_MIN
                                or (
                                    _ma20_decelerating
                                    and slope_ma20_df >= SoleusGajamiConstants.MA20_SLOPE_DECEL_LIMIT
                                )
                                or (
                                    dur_5x10_bars is not None and int(dur_5x10_bars) <= SoleusGajamiConstants.NEWBORN_MAX_DUR
                                    and slope_ma20_df >= SoleusGajamiConstants.MA20_SLOPE_DECEL_LIMIT
                                )
                            )
                            if not ma20_direction_ok:
                                logger.debug(
                                    f"🚫 [MA20-DIRECTION] {symbol} df기반 MA20 하락 "
                                    f"| s20_df={slope_ma20_df:.6f} prev={_ma20_prev:.6f} "
                                    f"decel={_ma20_decelerating}"
                                )
                        else:
                            ma20_direction_ok = False
                            logger.warning(
                                f"⚠️ [MA20-DIRECTION] {symbol} slope_ma20_df 미계산 → 차단"
                            )
                        
                        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
                        # 🔒 진입 트리거 (Entry Trigger) 분리
                        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
                        # 철학:
                        # - 형성 판단: 3분봉 확정값 (안정성) → gajami_shape_ok + gajami_physically_possible + window_ok
                        # - 진입 트리거: 캔들 내부 이벤트 (타이밍) → entry_trigger_ok
                        #
                        # "판단은 느리게, 트리거는 빠르게"
                        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
                        
                        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
                        # 🔒 A안: 초입 보호 + 중간 흔들림 차단
                        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
                        # 변경 의도:
                        #   - recent_cross_ok 단독 트리거 금지
                        #   - 현재 상태(수렴 유효/확장 진행) 확인 필수
                        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
                        
                        # 1) 현재 recent-cross gap 상태
                        # 🔒 [GAP-BAND-SSOT-0807] 이름 정정: 기존 _is_newborn_local은
                        #   RECENT_GC_MAX_BARS 기준이라 "newborn"이 아니었다.
                        #   COMPRESS_MIN_LOCAL은 정의 후 사용처 0회(죽은 코드) → 삭제.
                        #   _is_birth_local(dur<=5)은 gap_band()의 _birth(dur<=2)와 기준이 다른
                        #   별개 축이었다 — gap_band()로 흡수, 거짓 주석("L814 SSoT 일치") 삭제.
                        _is_recent_gc_local = (
                            dur_5x10_bars is not None
                            and int(dur_5x10_bars) <= RECENT_GC_MAX_BARS
                        )
                        _, _recent_gap_max = SoleusGajamiConstants.gap_band(dur_5x10_bars)

                        # 2) 간격 확장/유지 여부 (축소(음수)로 꺾이면 차단)
                        # - 기존: 간격 범위만 체크 → 수렴 상태도 통과
                        # - 개선: gap>=0(정배열) + spread_expand>0(벌어지는 중) 동시 요구

                        # ✅ 현재 MA5-MA10 간격(%)은 spread_now가 SSoT
                        gap_5_10_pct = spread_now

                        # 3) recent_cross 유효성: 현재 간격이 허용 범위 (형상 SSoT와 동일)
                        _recent_gap_floor = -0.10 if _is_recent_gc_local else 0.0
                        recent_still_valid = bool(
                            _recent_gap_floor <= gap_5_10_pct <= _recent_gap_max
                            and (
                                spread_expand > 0
                                or (dur_5x10_bars is not None and int(dur_5x10_bars) <= SoleusGajamiConstants.NEWBORN_MAX_DUR)
                            )
                        )
                        
                        # 4) recent_cross 기반 트리거는 "초입 유효"일 때만
                        recent_trigger = bool(recent_cross_ok and recent_still_valid)
                        
                        # 🔒 intrabar 단독 우회 차단
                        # intrabar는 '타이밍 트리거'일 뿐, 가자미근 타입 자격이 없으면 단독 진입 금지
                        # 🔧 FIX(2/11): 펼쳐진형 경로에 gap 상한 추가
                        # - 근거: KRW-IN 23:12 gap=0.77% 스파이크 캔들로 오진입
                        # - recent_still_valid(0.50%)와 window_ok(1.0%) 사이 갭이 넓어 추격 허용됨
                        # - 확장형이라도 gap > 0.60% 시 INTRABAR 단독 트리거 차단
                        INTRABAR_EXPAND_GAP_MAX = 0.60   # 0.60% (gap_5_10_pct와 동일 단위)
                        # intrabar_trigger = bool(
                        #     ma5_intrabar_cross_up and
                        #     (
                        #         recent_still_valid or                  # 초입 유효 (gap ≤ 0.50%)
                        #         (spacing_expanding and window_ok       # 펼쳐진형 (과열 아님)
                        #          and gap_5_10_pct <= INTRABAR_EXPAND_GAP_MAX)  # 🆕 0.60% 상한
                        #     )
                        # )
                        intrabar_trigger = bool(
                            ma5_intrabar_cross_up and
                            recent_cross_ok and                        # 탄생 아닌 재골크 차단
                            (
                                recent_still_valid or                  # 초입 유효 (gap ≤ 0.50%)
                                (spacing_expanding and window_ok       # 펼쳐진형 (과열 아님)
                                 and gap_5_10_pct <= INTRABAR_EXPAND_GAP_MAX)  # 0.60% 상한
                            )
                        )
                        if ma5_intrabar_cross_up and not intrabar_trigger:
                            logger.debug(
                                f"🚫 [INTRABAR-GAP-BLOCK] {symbol} "
                                f"봉내 돌파 감지했으나 차단 | "
                                f"recent_ok={recent_cross_ok} gap={gap_5_10_pct:.2f}% | "
                                f"still_valid={recent_still_valid} expand={spacing_expanding}"
                            )
                        
                        # ✅ GoldenEvidence 트리거(초입) 편입 (정석)
                        # - GoldenEvidence는 "무릎 증거"이므로 독립 트리거로 작동한다.
                        # - GC 수명(RECENT_GC_MAX_BARS) 초과 시 차단 (목덜미 우회 방지)
                        # 🔒 GC 수명 검증: 오래된 GC(목덜미)에서 evidence_trigger 차단
                        # evidence_trigger = bool(
                        #     evidence_pre_golden
                        #     and recent_still_valid
                        #     and (_dur_actual is None or _dur_actual <= RECENT_GC_MAX_BARS)
                        # )
                        evidence_trigger = bool(
                            evidence_pre_golden
                            and recent_still_valid
                            and _dur_actual is not None
                            and _dur_actual <= RECENT_GC_MAX_BARS
                        )
                        
                        # 🆕 준골든크로스 트리거 (임박 골든)
                        # - 완전 GC_5x10은 아니지만, "수렴 중 + MA5가 MA10을 추월 중"이면 초입 트리거 인정
                        # - evidence_pre_golden과 독립 경로 (evidence cond2/cond3 미충족 케이스 커버)
                        # - 조건: gap ∈ [-0.15%, 0%), MA5 slope > MA10 slope, MA5 상승 중
                        near_golden_trigger = bool(
                            (not gc_5_10) and
                            (-0.15 <= gap_5_10_pct < 0.0) and
                            (ma5_slope > ma10_slope) and
                            (ma5_slope > 0.0) and
                            recent_still_valid
                        )

                        # 5) 최종 진입 트리거 (정석)
                        # - recent/intrabar/evidence/near_golden + 펼쳐진형
                        # - 3분봉 확정 기다리지 않고 형성 순간 인정
                        entry_trigger_ok = bool(
                            not _fake_late_reset_gc
                            and (
                                recent_trigger
                                or intrabar_trigger
                                or evidence_trigger
                                or near_golden_trigger
                                or (recent_cross_ok and spacing_expanding and window_ok and (
                                    ma5_reaccelerating
                                    or (gc_5_10 and _had_pullback and ma5_slope > 0 and ma10_slope > 0)
                                ))
                            )
                        )
                        logger.debug(
                            f"[TRIGGER-DETAIL] {symbol} | "
                            f"recent={recent_trigger} intrabar={intrabar_trigger} "
                            f"evidence={evidence_trigger} near_gc={near_golden_trigger} | "
                            f"expand={spacing_expanding} window={window_ok} "
                            f"reacc={ma5_reaccelerating} pb={_had_pullback} "
                            f"s5={ma5_slope:.5f} s10={ma10_slope:.5f} | "
                            f"result={entry_trigger_ok}"
                        )
                        if intrabar_trigger and spacing_expanding and not recent_still_valid:
                            logger.info(
                                f"⚠️ [INTRABAR-EXPAND-WATCH] {symbol} "
                                f"intrabar EXPAND 경로 진입 | "
                                f"reacc={ma5_reaccelerating} pb={_had_pullback} "
                                f"gap={gap_5_10_pct:.2f}% window={window_ok}"
                                f" → ENTRY={entry_trigger_ok}"
                            )

                        # 🔬 [GAP-MAX-TRACE-0817] recent gap 무효를 INFO에서 틱 단위로 관측.
                        # 판정값/임계값은 변경하지 않는다.
                        if recent_cross_ok and not recent_still_valid:
                            try:
                                _ri_px = float(ma10)
                                _ri_tk = float(get_tick_size(_ri_px) or 0.0)
                                _ri_gap_tick = (
                                    (gap_5_10_pct / 100.0) * _ri_px / _ri_tk
                                    if _ri_tk > 0.0 else 0.0
                                )
                                _ri_max_tick = (
                                    (_recent_gap_max / 100.0) * _ri_px / _ri_tk
                                    if _ri_tk > 0.0 else 0.0
                                )
                            except Exception:
                                _ri_px = 0.0
                                _ri_tk = 0.0
                                _ri_gap_tick = 0.0
                                _ri_max_tick = 0.0

                            logger.info(
                                f"🚫 [RECENT-INVALID] {symbol} recent_cross 있으나 현재 무효 | "
                                f"gap={gap_5_10_pct:.3f}%({_ri_gap_tick:+.2f}틱) "
                                f"범위 {_recent_gap_floor}~{_recent_gap_max}%"
                                f"(상한 {_ri_max_tick:.2f}틱) | "
                                f"px={_ri_px:.6g} tick={_ri_tk:.6g} "
                                f"expand={spread_expand:.4f} dur={dur_5x10_bars}"
                            )
                                                
                        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
                        # 🕐 현재 3분봉 식별자 계산: 데이터 봉 시간 우선 (정석)
                        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
                        # 철학:
                        # - 실거래: 데이터 최신 봉 = 현재 시간 (일치)
                        # - 시뮬: 데이터 과거 봉 ≠ 현재 시간 (불일치) → 데이터 우선!
                        # - fallback: 벽시계 시간 (예외 상황)
                        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
                        try:
                            # 데이터 마지막 봉 시간 추출 (우선)
                            _ts = int(df.index[-1].timestamp())  # pandas datetime index
                        except Exception:
                            # fallback: 벽시계 시간 (데이터 접근 실패 시)
                            import time as _time
                            _ts = int(_time.time())
                        
                        candle_start_ts = (_ts // 180) * 180  # 3분 = 180초
                        
                        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
                        # 🔒 봉 내부 중복 진입 차단 (메타 기록만)
                        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
                        # 주의:
                        # - Analysis 단계에서 신호를 3분봉 1회로 강제 잠그면,
                        #   Strategy/Execution의 다른 Gate에서 막혔을 때 같은 봉에서 재시도 자체가 불가해져
                        #   '정답 타점 미진입 → 늦은 진입'을 유발한다.
                        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
                        # 🎯 soleus_gajami 최종 정의 (형성 + 트리거 통합)
                        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
                        
                        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
                        # 🎯 가자미근 형상 판정 (SSoT 함수 호출)
                        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
                        # 🆕 TIER별 MA5 DIR_MIN (CONFIG SSoT → tier 변수 사용)
                        _dir_min_map = {
                            1: SoleusGajamiConstants.MA5_DIR_MIN_TIER1,
                            2: SoleusGajamiConstants.MA5_DIR_MIN_TIER2,
                            3: SoleusGajamiConstants.MA5_DIR_MIN_TIER3,  # fallback
                        }
                        # 🆕 TIER3 틱 기반 동적 계산 (양자화 보정)
                        # 🔧 동적 계산 = 가격·틱 실질 기준 / tier3_base = 상한선
                        if tier == 3 and cur_price > 0 and tick_size > 0:
                            # 분모를 prev_ma5로 통일 (ma5_slope 계산과 동일 기준)
                            _ref = prev_ma5 if prev_ma5 > 0 else cur_price
                            _dynamic = (tick_size / (5.0 * _ref)) * SoleusGajamiConstants.MA5_DIR_TICK_MULTIPLIER
                            _ma5_dir_min = min(_dynamic, SoleusGajamiConstants.MA5_DIR_MIN_TIER3)
                        else:
                            _ma5_dir_min = _dir_min_map.get(tier, SoleusGajamiConstants.MA5_DIR_MIN_TIER3)
                        
                        _is_birth_bottom = bool(
                            dur_5x10_bars is not None
                            and int(dur_5x10_bars) <= SoleusGajamiConstants.NEWBORN_MAX_DUR
                            and is_low_area
                        )
                        if _is_birth_bottom:
                            _ma5_dir_min = 0.00001

                        # 🔧 GC 탄생봉 보호 컨텍스트
                        # - _newborn_ctx는 기존 traintrack 판정 유지
                        # - turn1은 2봉 블렌드와 분리한 현재봉 MA5 lift 판정 전용
                        _newborn_ma5_turn_up = bool(
                            dur_5x10_bars is not None
                            and int(dur_5x10_bars) <= SoleusGajamiConstants.NEWBORN_MAX_DUR
                            and float(ma5 or 0.0) > float(ma10 or 0.0) > 0.0
                            and float(prev_ma5 or 0.0) > 0.0
                            and float(ma5 or 0.0) > float(prev_ma5 or 0.0)
                        )
                        _newborn_ctx = _gc_newborn_context(
                            dur_5x10_bars, ma5_slope, ma10_slope
                        )
                        logger.info(
                            f"[NEWBORN-CTX] {symbol} | ok={_newborn_ctx} "
                            f"turn1={int(_newborn_ma5_turn_up)} "
                            f"dur={dur_5x10_bars} s5={ma5_slope:.5f} "
                            f"s10={(ma10_slope if ma10_slope is not None else 0.0):.5f}"
                        )

                        # 🔒 현재봉 MA pair는 shape/trigger 관측용으로 유지한다.
                        # canonical GC 자격은 위 gc_5_10(확정봉 strict `>`)만 소비한다.
                        _closed_ma5  = float(prev_mas.get('ma5',  0.0) or 0.0)
                        _closed_ma10 = float(prev_mas.get('ma10', 0.0) or 0.0)

                        _cur_ma5  = float(ma5 or 0.0)
                        _cur_ma10 = float(ma10 or 0.0)

                        _entry_ma5_gt_ma10 = bool(_cur_ma5 > _cur_ma10)
                        _entry_ma5_lt_ma10 = bool(_cur_ma5 < _cur_ma10)
                        _entry_ma5_eq_ma10 = bool(_cur_ma5 == _cur_ma10)

                        _cur_gap_pct = abs(_cur_ma5 - _cur_ma10) / _cur_ma10 * 100 if _cur_ma10 > 0 else 999.0

                        indicators["entry_ma5_gt_ma10"] = int(_entry_ma5_gt_ma10)
                        indicators["entry_ma5_lt_ma10"] = int(_entry_ma5_lt_ma10)
                        indicators["entry_ma5_eq_ma10"] = int(_entry_ma5_eq_ma10)

                        indicators["entry_ma5_rt"] = _cur_ma5
                        indicators["entry_ma10_rt"] = _cur_ma10
                        indicators["entry_ma5_closed"] = _closed_ma5
                        indicators["entry_ma10_closed"] = _closed_ma10
                        _birth_traintrack_area_ok = True
                        _birth_traintrack = bool(
                            _newborn_ctx
                            and recent_cross_ok
                            and entry_trigger_ok
                            and ma5_slope > ma10_slope
                            and ma10_slope >= 0.0
                            and (slope_ma20_df is not None and float(slope_ma20_df) >= 0.0)
                            and _cur_gap_pct <= 0.05
                            and _birth_traintrack_area_ok
                        )

                        # 🔒 [REACC-VALID-GC-REMOVE-0817] False 고정 잔여 회선 물리 철거.

                        # 🟢 MA60/MA120 구조상승 + MA20 구조급락 아님 → 눌림 잔상 허용
                        # is_gajami_shape() MA20 게이트보다 먼저 계산
                        _s20_struct5 = float(
                            indicators.get('ma20_slope_struct5', indicators.get('ma20_slope', 0.0)) or 0.0
                        ) / 100.0
                        _long_backed_mid_pullback = bool(
                            dur_5x10_bars is not None
                            and int(dur_5x10_bars) <= SoleusGajamiConstants.NEWBORN_SHAPE_MAX_DUR
                            and float(_cur_ma5) > float(_cur_ma10) > 0.0
                            and float(ma5_slope or 0.0) > 0.0
                            and float(slope_ma60 or 0.0) > 0.0
                            and float(slope_ma120 or 0.0) > 0.0
                            and _s20_struct5 >= SoleusGajamiConstants.MA20_SLOPE_DECEL_LIMIT
                        )
                        indicators["gajami_long_backed_mid_pullback"] = int(_long_backed_mid_pullback)
                        if _long_backed_mid_pullback:
                            logger.info(
                                f"🟢 [LONG-BACKED-MID-PULLBACK] {symbol} "
                                f"MA60/MA120 구조상승 + MA20 구조급락아님 → MID 눌림 잔상 허용 | "
                                f"dur={dur_5x10_bars} "
                                f"s10={ma10_slope:+.5f} s20_struct5={_s20_struct5:+.5f} "
                                f"s60={float(slope_ma60 or 0.0):+.5f} s120={float(slope_ma120 or 0.0):+.5f}"
                            )

                        gajami_shape_result = is_gajami_shape(
                            ma5=ma5,
                            ma10=ma10,
                            ma20=ma20,
                            ma5_slope=ma5_slope,
                            ma10_slope=ma10_slope,
                            ma20_slope=float(slope_ma20_df) if slope_ma20_df is not None else 0.0,
                            ma60_slope=float(slope_ma60) if 'slope_ma60' in locals() and slope_ma60 is not None else 0.0,
                            ma120_slope=float(slope_ma120) if 'slope_ma120' in locals() and slope_ma120 is not None else 0.0,
                            gap_5_10_pct=spread_now,
                            ma5_prev_prev=prev_prev_ma5,
                            ma5_prev=prev_ma5,
                            ma10_prev=prev_ma10,
                            ma5_dir_min=_ma5_dir_min,
                            current_price=cur_price,
                            regime=regime,
                            dur_5x10_bars=dur_5x10_bars,
                            newborn_max_bars=RECENT_GC_MAX_BARS,
                            ma5_series=mas.get('ma5'),
                            ma10_series=mas.get('ma10'),
                            is_birth_bottom=_is_birth_bottom,
                            intrabar_gc=ma5_intrabar_cross_up,
                            df=df,
                            symbol=symbol,
                            birth_traintrack_ok=_birth_traintrack,
                            subtick_body_birth=bool(indicators.get("subtick_body_birth", False)),
                            long_backed_mid_pullback_ok=_long_backed_mid_pullback,
                            newborn_ma5_turn_up=_newborn_ma5_turn_up,
                            gc_5x10_confirmed=gc_5_10,
                        )
                        
                        _core_shape_ok  = gajami_shape_result['gajami_ok']
                        gajami_shape_ok = _core_shape_ok
                        indicators["gajami_convergence_birth_core"] = int(
                            gajami_shape_result.get("convergence_birth", 0)
                        )

                        # 📊 SHAPE-V2-MEASURE-RAW
                        try:
                            _ma5_s = mas.get('ma5')
                            _ma10_s = mas.get('ma10')
                            if (
                                _ma5_s is not None and _ma10_s is not None
                                and len(_ma5_s) >= 6 and len(_ma10_s) >= 6
                            ):
                                _spread = [
                                    float(_ma5_s.iloc[-i]) - float(_ma10_s.iloc[-i])
                                    for i in range(6, 0, -1)
                                ]
                                _past_body   = sum(_spread[:3]) / 3.0
                                _recent_body = sum(_spread[3:]) / 3.0
                                _body_current = float(_spread[-1])

                                # [2026-07-25 철거] JANGDAE-BIRTH 판정권만 차단, 관측은 유지:
                                #   원 조건식은 그대로 계산해 로그에 observe=로 남기되(진단용),
                                #   실제 진입권 반영(_body_birth_current·indicators)은 강제 0.
                                #   true_gc·box_pos 없이 dur+rise_pct만으로 "탄생 강한몸통" 인정하던
                                #   구조가 소진형(천장 마지막 발악) 장대양봉을 걸러내지 못함(phantom GC 구멍).
                                #   실전익절 forward -0.354%(7건 0/7 전멸).
                                _body_birth_current = False

                                # 🔒 [JANGDAE-PURGE-0819] 死 export·로그 전량 철거.
                                #    소비처 전부 死항이었다. ENTRY-PREM-BLOCK-0819는
                                #    prev_candle_close/atr_pct(기존 SSoT)만 소비해 이 값들이 불필요.
                                # 🔒 [BODY-GROWTH-SIGN-0816] 우회로 2개 제거 — 몸통 양수 요구만 유지.
                                #  COW/ENSO 둘 다 이 우회로로 통과해 손절(20:39/20:42, 08-16).
                                _body_growth = bool(
                                    _recent_body > _past_body
                                    and _recent_body > 0.0
                                )

                                indicators["shape_v2_gajami_ok"]         = bool(gajami_shape_ok)
                                indicators["shape_v2_source"]             = "CORE"
                                indicators["shape_v2_body_growth"]        = bool(_body_growth)
                                indicators["shape_v2_body_past"]          = float(_past_body)
                                indicators["shape_v2_body_recent"]        = float(_recent_body)
                                indicators["shape_v2_body_current"]       = float(_body_current)
                                indicators["shape_v2_body_birth_current"] = bool(_body_birth_current)
                                indicators["shape_v2_head_ok"]            = bool(
                                    SoleusGajamiConstants.ma5_alive_ratio(ma5_slope)
                                )
                                indicators["shape_v2_lead_ok"]            = bool(ma5_slope > ma10_slope)
                                indicators["shape_v2_align_ok"]           = bool(
                                    gajami_shape_result.get("aligned_ok", False)
                                )

                                logger.info(
                                    f"📊 [SHAPE-V2-MEASURE-RAW] {symbol} | "
                                    f"gajami_ok={gajami_shape_ok} | "
                                    f"body_growth={_body_growth} | "
                                    f"body_recent={_recent_body:.5f} body_past={_past_body:.5f} | "
                                    f"body_cur={_body_current:+.5f} birth_cur={int(_body_birth_current)} | "
                                    f"head={int(SoleusGajamiConstants.ma5_alive_ratio(ma5_slope))} "
                                    f"floor={-SoleusGajamiConstants.MA5_SLOPE_ALIVE_DIP_RATIO:+.5f} | "
                                    f"lead={int(ma5_slope > ma10_slope)} | "
                                    f"align={int(gajami_shape_result.get('aligned_ok', False))} | "
                                    f"train_area={int(_birth_traintrack_area_ok)}"
                                )
                        except Exception:
                            pass

                        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
                        # 🔬 CORE 통과 계측 (진입 위치/과열 디버깅)
                        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
                        if _core_shape_ok:
                            try:
                                sym = str(indicators.get("symbol", "UNKNOWN"))
                                cur = float(indicators.get("current_price", 0.0) or 0.0)
                                atrp = float(indicators.get("atr_pct", 0.0) or 0.0)
                                ma5_val = float(indicators.get("ma5", 0.0) or 0.0)
                                ma10_val = float(indicators.get("ma10", 0.0) or 0.0)
                                gap_pct = ((ma5_val - ma10_val) / ma10_val * 100.0) if ma10_val > 0 else 0.0
                                
                                logger.info(
                                    f"[CORE-PASS] {sym} | "
                                    f"s5={ma5_slope:.5f} s10={ma10_slope:.5f} "
                                    f"gap={gap_pct:.2f}% "
                                    f"px={cur:.2f} atr%={atrp:.2f}"
                                )
                            except Exception:
                                pass
                        
                        # ⛔ V_BOUNCE / PULLBACK 제거 — 헌법 오염 원천 차단
                        # 가자미근 = CORE(is_gajami_shape) 단일 기준
                        
                        aligned_gate = bool(gajami_shape_result.get('aligned_ok', False))  # 🔧 MEASURE ONLY 정합성 복구
                        
                        # 🔍 디버그: SSoT 함수 결과 로그
                        if _core_shape_ok:
                            logger.debug(
                                f"✅ [GAJAMI-SSoT] {symbol} | "
                                f"shape={gajami_shape_result['shape_ok']} "
                                f"ma20={gajami_shape_result['ma20_ok']} "
                                f"align={gajami_shape_result['aligned_ok']}"
                            )
                        
                        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
                        # 🔒 MA20 하락 지속 차단 + 전환 초입 허용 Gate (Patch 7)
                        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
                        # 원칙:
                        # - df slope: 빠르지만 과민 (급락 후 일시 개선 가능)
                        # - 봉 확정 slope: 안정적 (indicators['ma20_slope'], % 단위)
                        # - 급락 후 일시 개선(XRP) vs 안정적 전환 초입(VANA) 구분
                        # 단위: indicators['ma20_slope']는 % → /100으로 ratio 변환 (SSoT)
                        
                        # 봉 확정 MA20 slope (ratio)
                        _raw_bar = indicators.get("ma20_slope")
                        if _raw_bar is None:
                            _ma20_slope_bar = -1.0          # 데이터 누락 → 보수적 차단
                        else:
                            _ma20_slope_bar = float(_raw_bar) / 100.0   # % → ratio (SSoT)
                        
                        _PULLBACK_MIN = SoleusGajamiConstants.MA20_SLOPE_PULLBACK_MIN  # -0.00025
                        
                        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
                        # 🔒 is_breakout_early: 전환 초입 판정 (SSoT)
                        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
                        # 정석: 전환 초입에서는 MA20이 '평탄~미세 하방'일 수 있다.
                        # - 골든크로스 + 간격 확장 + MA5/MA10 양전환 + MA20 하락 둔화
                        # - ma20_trend_ok 완화에서 재사용
                        # - TRENDING_DOWN이면 첫 조건(regime != TD)에서 항상 False
                        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
                        is_breakout_early = bool(
                            regime != MarketRegime.TRENDING_DOWN
                            and gc_5_10
                            and spacing_expanding
                            and (ma5_slope > 0.0)
                            and (ma10_slope >= 0.0)
                            and (slope_ma20_df is not None and slope_ma20_df >= MA20_DIRECTION_MIN)
                        )
                        
                        ma20_trend_ok = (
                            ma20_direction_ok                       # 1차: df >= -0.0003
                            or (
                                regime != MarketRegime.TRENDING_DOWN
                                and _ma20_slope_bar >= _PULLBACK_MIN
                                and (ma5_slope >= SoleusGajamiConstants.MA5_SLOPE_PULLBACK_RATIO or ma10_slope >= 0.00020)
                                and (ma5 > ma10)
                                and (slope_ma20_df is not None and slope_ma20_df >= MA20_DIRECTION_MIN)
                            )
                            or is_breakout_early                    # 3차: 전환 초입 완화
                        )
                        
                        if not ma20_trend_ok:
                            logger.debug(
                                f"🚫 [MA20-TREND-GATE] {symbol} 하락 지속 차단 | "
                                f"dir_ok={ma20_direction_ok} "
                                f"s20_df={float(slope_ma20_df or 0.0):.6f} "
                                f"s20_bar={_ma20_slope_bar:.6f} "
                                f"s5={ma5_slope:.5f} s10={ma10_slope:.5f}"
                            )
                        
                        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
                        # 🔒 Soleus 최종 판정 (SSoT): 형상 + 물리성 + 윈도우 + 트리거 + MA20 Gate
                        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
                        # - evidence_trigger는 entry_trigger_ok에 포함되어 있으므로 여기서 우회 OR 금지
                        #
                        # ┌────────────────────────────────────────────────────────┐
                        # │  📜 가자미근 헌법 — 2대 공식 + 절대 거부권            │
                        # │                                                        │
                        # │  ① MA5 확실한 우상향                                │
                        # │     정상 : ma5_slope >= _ma5_dir_min (Tier별)       │
                        # │     완화 : 탄생(dur≤2) + 확장 중 → slope > 0 허용  │
                        # │     (탄생봉은 slope가 물리적으로 작을 수밖에 없음)   │
                        # │                                                        │
                        # │  ② 골든크로스(MA5>MA10, 확정봉) + 탄생 근거             │
                        # │     gc_5_10=True AND                                    │
                        # │     (spacing_expanding OR 탄생봉 OR 최근GC)             │
                        # │     Strategy 헌법 TD-LATE-ENTRY-BLOCK(dur>2)과 통일 │
                        # │                                                        │
                        # │  📜 헌법 제3조: TOP + 수렴 = 절대 거부권            │
                        # │     box_pos≥0.95 AND not spacing_expanding           │
                        # │     → 하락 수렴 = 가자미근 반대 방향, 예외 없음     │
                        # │                                                        │
                        # │  ⚠️ newborn 미사용: soleus_gajami → is_soleus_zone  │
                        # │     → newborn 순환 참조 → dur_5x10_bars 직접 사용  │
                        # └────────────────────────────────────────────────────────┘

                        # 탄생봉 여부 (순환 참조 방지: newborn 대신 dur 직접 사용)
                        _is_birth_traintrack = bool(_birth_traintrack)
                        _verified_birth_traintrack = bool(
                            _is_birth_traintrack
                            and dur_5x10_bars is not None
                            and int(dur_5x10_bars) <= SoleusGajamiConstants.NEWBORN_MAX_DUR
                            and recent_cross_ok
                            and entry_trigger_ok
                            and ma5_slope > ma10_slope
                            and ma10_slope >= 0.0
                            and ma20_trend_ok
                        )

                        _is_newborn_dur = (
                            dur_5x10_bars is not None
                            and int(dur_5x10_bars) <= RECENT_GC_MAX_BARS
                            and ma10_slope >= SoleusGajamiConstants.MA10_SLOPE_NEWBORN_MIN
                        )

                        # 📜 헌법 ① : MA5 확실한 우상향
                        # 탄생봉(dur≤2)은 slope>0만으로 완화 (spacing_expanding 불필요)
                        # 근거: 탄생 순간 gap≈0 → spacing_expanding이 물리적으로 False
                        #       수렴/TOP 위험은 헌법③(TOP+수렴)·헌법④(RANGING위치)가 차단
                        _law1_birth_relax = _is_newborn_dur
                        _gajami_law1_ok = (
                            (ma5_slope >= _ma5_dir_min)          # 정상: Tier별 최솟값
                            or _law1_birth_relax
                        )

                        # 📜 헌법 ② : 골든크로스 + 이격 벌어짐 (탄생봉/눌림반등 유예)
                        _gajami_law2_ok = bool(
                            gc_5_10
                            and (spacing_expanding or _is_newborn_dur or recent_cross_ok)
                        )

                        # 📜 가자미근 기본 헌법 — 우회 불가 hard gate
                        # GC 자격은 확정봉 strict `>` 한 곳만 소비한다.
                        _basic_gc_ok = bool(gc_5_10)

                        # 🔧 [BASIC-HEAD-SSoT] 헌법 0조와 동일 MA5 생존 기준
                        _basic_ma5_head_ok = bool(
                            SoleusGajamiConstants.ma5_alive_ratio(ma5_slope)
                        )

                        _birth_mid_shallow_alive_ok = bool(
                            dur_5x10_bars is not None
                            and int(dur_5x10_bars) <= SoleusGajamiConstants.NEWBORN_MAX_DUR
                            and recent_cross_ok
                            and entry_trigger_ok
                            and _entry_ma5_gt_ma10
                            and ma5_slope > 0.0
                            and ma10_slope >= SoleusGajamiConstants.MA10_SLOPE_NEWBORN_MIN
                            and (
                                slope_ma20_df is None
                                or float(slope_ma20_df) >= SoleusGajamiConstants.MA20_SLOPE_DECEL_LIMIT
                            )
                        )

                        # _long_alive_ok(L5898)와 동일 계산 → SSoT 재사용
                        _basic_long_alive_ok = _long_alive_ok

                        _basic_mid_alive_ok = bool(
                            ma10_slope > 0.0
                            or (slope_ma20_df is not None and float(slope_ma20_df) >= 0.0)
                            or _birth_mid_shallow_alive_ok
                            or _long_backed_mid_pullback
                            or (
                                dur_5x10_bars is not None
                                and int(dur_5x10_bars) <= SoleusGajamiConstants.NEWBORN_MAX_DUR
                                and bool(gajami_shape_result.get("aligned_ok", False))
                            )
                        )

                        # 🔒 헌법 0조: TD/RG + MA20 하향 차단
                        _basic_td_rg_ma20_down_block = bool(
                            regime in (MarketRegime.TRENDING_DOWN, MarketRegime.RANGING)
                            and slope_ma20_df is not None
                            and float(slope_ma20_df) < SoleusGajamiConstants.MA20_SLOPE_DECEL_LIMIT
                            and not _birth_mid_shallow_alive_ok
                            and not _long_backed_mid_pullback
                        )

                        _gajami_basic_ok = bool(
                            _basic_gc_ok
                            and _basic_ma5_head_ok
                            and _basic_mid_alive_ok
                            and _basic_long_alive_ok
                            and not _basic_td_rg_ma20_down_block
                        )
                        indicators["gajami_basic_ok"] = int(_gajami_basic_ok)

                        logger.debug(
                            f"[GAJAMI-BASIC] {symbol} = {_gajami_basic_ok} | "
                            f"gc={int(_basic_gc_ok)} cur_ma5={_cur_ma5:.4f} cur_ma10={_cur_ma10:.4f} "
                            f"head={int(_basic_ma5_head_ok)} s5={ma5_slope:.5f} "
                            f"mid={int(_basic_mid_alive_ok)} s10={ma10_slope:.5f} "
                            f"s20={float(slope_ma20_df or 0.0):.6f} "
                            f"long={int(_basic_long_alive_ok)} s60={float(slope_ma60 or 0.0):.6f} "
                            f"s120={float(slope_ma120 or 0.0):.6f} "
                            f"td_rg_ma20_down={int(_basic_td_rg_ma20_down_block)} "
                            f"(thr={SoleusGajamiConstants.MA20_SLOPE_DECEL_LIMIT:+.5f})"
                        )

                        # 양자화 BIRTH 품질 SSoT
                        # 직전봉 GC유지 + SHAPE-V2 5중 AND → 휩소(CPOOL/NEAR: _prev_already_gc=False) 차단 유지
                        _quantized_shape_v2_ok = bool(
                            _is_ma10_birth_newborn
                            and _prev_already_gc
                            and bool(indicators.get("shape_v2_body_growth", False))
                            and bool(indicators.get("shape_v2_head_ok", False))
                            and bool(indicators.get("shape_v2_lead_ok", False))
                            and bool(indicators.get("shape_v2_align_ok", False))
                        )
                        
                        _trend_birth_base_without_swing = bool(
                            _trend_continuing
                            and entry_trigger_ok
                            and _birth_mid_shallow_alive_ok
                            and cur_price >= ma5
                        )

                        _veto3_trend_birth_exempt = bool(
                            _trend_birth_base_without_swing
                        )
                        indicators["gajami_veto3_trend_birth_exempt"] = int(_veto3_trend_birth_exempt)

                        _is_birth_veto_exempt = bool(
                            dur_5x10_bars is not None
                            and int(dur_5x10_bars) <= SoleusGajamiConstants.NEWBORN_MAX_DUR
                            and ma5_slope > 0
                            and (ma5 > ma10 or _verified_birth_traintrack)
                        )
                        _is_birth_veto_exempt = False
                        # 🔧 [TU-STRUCTURAL] hysteresis stable=TD 지연으로
                        #   TU면제(regime!=TU) 미작동 → MA slope 구조로 직접 인정
                        _structural_tu_breakout = bool(
                            bool(indicators.get("subtick_dc_body_birth", 0))
                            and ma5 > ma10
                            and ma5_slope > 0
                            and (ma10_slope or 0) > 0
                            and float(slope_ma20_df or 0.0) > 0
                        )
                        indicators["gajami_structural_tu_breakout"] = int(_structural_tu_breakout)

                        # BOX 위치·높이는 관측값만 유지하고 헌법 거부권 회수.
                        _gajami_constitution_veto = False                        
                        _atr_pct_val = float(indicators.get("atr_pct", 0) or 0)
                        # 📜 헌법④ TLC(Trend Life Cycle) 기반 — MA10/MA20 추세 방향
                        # 구조: 단일 기울기(3봉) → 현재/이전 4봉 2구간 비교 (상대 추세 판단)
                        # - 현재 구간: iloc[-2] ~ iloc[-6]  (4봉 차이)
                        # - 이전 구간: iloc[-7] ~ iloc[-11] (동일 길이 앞 구간)
                        # - ratio = 현재/이전절댓값 → 로그용 (실제 판단: _tlc_ratio_ok)
                        _tlc_ma10_prev  = 0.0   # fallback (except 대비)
                        _tlc_ma20_prev  = 0.0   # fallback (except 대비)
                        _tlc_ma20_ratio = None  # 데이터 부족 시 None 유지
                        _tlc_ma10_slope_w7 = 0.0
                        _tlc_ma10_prev_w7  = 0.0
                        _tlc_ma20_slope_w7 = 0.0
                        try:
                            _ma5_s  = mas.get('ma5')
                            _ma10_s = mas.get('ma10')
                            _ma20_s = mas.get('ma20')
                            # ── MA5 현재 구간: iloc[-2] ~ iloc[-7] (5봉 차이=15분) ──
                            _tlc_ma5_slope = (
                                (float(_ma5_s.iloc[-2]) - float(_ma5_s.iloc[-7])) / float(_ma5_s.iloc[-7]) * 100.0
                                if _ma5_s is not None and len(_ma5_s) >= 7
                                else float(indicators.get("ma5_slope", 0.0) or 0.0)
                            )
                            # ── 현재 구간: iloc[-2] ~ iloc[-7] (5봉 차이=15분) ──
                            _tlc_ma10_slope = (
                                (float(_ma10_s.iloc[-2]) - float(_ma10_s.iloc[-7])) / float(_ma10_s.iloc[-7]) * 100.0
                                if _ma10_s is not None and len(_ma10_s) >= 7
                                else float(indicators.get("ma10_slope", 0.0) or 0.0)
                            )
                            # ── 이전 구간: iloc[-8] ~ iloc[-13] (5봉 차이=15분) ──
                            _tlc_ma10_prev = (
                                (float(_ma10_s.iloc[-8]) - float(_ma10_s.iloc[-13])) / float(_ma10_s.iloc[-13]) * 100.0
                                if _ma10_s is not None and len(_ma10_s) >= 13
                                else _tlc_ma10_slope
                            )
                            # ── 현재 구간: iloc[-2] ~ iloc[-7] (5봉 차이=15분) ──
                            _tlc_ma20_slope = (
                                (float(_ma20_s.iloc[-2]) - float(_ma20_s.iloc[-7])) / float(_ma20_s.iloc[-7]) * 100.0
                                if _ma20_s is not None and len(_ma20_s) >= 7
                                else (float(slope_ma20_df * 100.0) if slope_ma20_df is not None else float(indicators.get("ma20_slope", 0.0) or 0.0))
                            )
                            # ── 이전 구간: iloc[-8] ~ iloc[-13] (5봉 차이=15분) ──
                            _tlc_ma20_prev = (
                                (float(_ma20_s.iloc[-8]) - float(_ma20_s.iloc[-13])) / float(_ma20_s.iloc[-13]) * 100.0
                                if _ma20_s is not None and len(_ma20_s) >= 13
                                else _tlc_ma20_slope
                            )
                            # ── 가속도 비율: 현재/이전 절댓값 (부호 유지) ──
                            _tlc_ma20_ratio = (
                                _tlc_ma20_slope / abs(_tlc_ma20_prev)
                                if _tlc_ma20_prev != 0.0 else None
                            )
                            # 🆕 [SHADOW] W7 구간 (span만 W5와 다름, -2 시작=확정봉 동일)
                            _tlc_ma10_slope_w7 = (
                                (float(_ma10_s.iloc[-2]) - float(_ma10_s.iloc[-9])) / float(_ma10_s.iloc[-9]) * 100.0
                                if _ma10_s is not None and len(_ma10_s) >= 9 else _tlc_ma10_slope
                            )
                            _tlc_ma10_prev_w7 = (
                                (float(_ma10_s.iloc[-10]) - float(_ma10_s.iloc[-17])) / float(_ma10_s.iloc[-17]) * 100.0
                                if _ma10_s is not None and len(_ma10_s) >= 17 else _tlc_ma10_slope_w7
                            )
                            _tlc_ma20_slope_w7 = (
                                (float(_ma20_s.iloc[-2]) - float(_ma20_s.iloc[-9])) / float(_ma20_s.iloc[-9]) * 100.0
                                if _ma20_s is not None and len(_ma20_s) >= 9 else _tlc_ma20_slope
                            )
                        except Exception:
                            _tlc_ma5_slope  = float(indicators.get("ma5_slope", 0.0) or 0.0)
                            _tlc_ma10_slope = float(indicators.get("ma10_slope", 0.0) or 0.0)
                            _tlc_ma20_slope = (
                                float(slope_ma20_df * 100.0) if slope_ma20_df is not None
                                else float(indicators.get("ma20_slope", 0.0) or 0.0)
                            )
                            _tlc_ma10_slope_w7 = _tlc_ma10_slope
                            _tlc_ma10_prev_w7  = _tlc_ma10_slope_w7
                            _tlc_ma20_slope_w7 = _tlc_ma20_slope
                        # ── 측정 로그 (threshold 결정 전 데이터 수집용) ──
                        logger.debug(
                            f"📐 [TLC-RATIO] {symbol} | "
                            f"ma5_cur={_tlc_ma5_slope:+.4f}% "
                            f"ma20_cur={_tlc_ma20_slope:+.4f}% "
                            f"ma20_prev={_tlc_ma20_prev:+.4f}% "
                            f"ratio={f'{_tlc_ma20_ratio:.3f}' if _tlc_ma20_ratio is not None else 'N/A'} | "
                            f"ma10_cur={_tlc_ma10_slope:+.4f}% "
                            f"ma10_prev={_tlc_ma10_prev:+.4f}%"
                        )

                        # 🐟 true DC→GC birth body momentum
                        # - ma5_reaccelerating은 V회복에서 1주기 늦다.
                        # - true birth + structural TU + TLC20 생존이면 reacc 대체 momentum으로 소비.
                        _birth_body_momentum_ok = bool(
                            dur_5x10_bars is not None
                            and int(dur_5x10_bars) <= 1
                            and _is_true_gc_birth
                            and entry_trigger_ok
                            and _structural_tu_breakout
                            and float(_tlc_ma20_slope or 0.0) > 0.0
                        )
                        indicators["gajami_birth_body_momentum_ok"] = int(
                            _birth_body_momentum_ok
                        )
                        if _birth_body_momentum_ok and not (
                            ma5_reaccelerating or _had_pullback or intrabar_trigger
                        ):
                            logger.info(
                                f"✅ [BODY-MOMENTUM-OPEN] {symbol} "
                                f"true DC→GC 현재봉 모멘텀 → reacc 지각 대체 | "
                                f"dur={dur_5x10_bars} "
                                f"true_gc={int(_is_true_gc_birth)} "
                                f"dc_body={int(bool(indicators.get('subtick_dc_body_birth', 0)))} "
                                f"trigger={int(entry_trigger_ok)} "
                                f"struct_tu={int(_structural_tu_breakout)} "
                                f"s20_df={float(slope_ma20_df or 0.0):+.5f} "
                                f"tlc20={float(_tlc_ma20_slope or 0.0):+.4f}% "
                                f"reacc={int(ma5_reaccelerating)} "
                                f"pb={int(_had_pullback)} "
                                f"intra={int(intrabar_trigger)} "
                                f"candle={indicators.get('current_candle_time', '')}"
                            )

                       # TOP존 추세생존: ma10 비후퇴 OR (확장 AND 비TOP)
                        # - spacing_expanding 단독은 이미 오른 자리 재가속 우회가 됨
                        # - pos>=0.90 TOP존에서는 ma10 비후퇴만 인정
                        # 🟢 박스→TLC: 정배열 신고가 연속은 box_pos=TOP이어도 꼭대기 아님
                        try:
                            _sc5   = float(mas['ma5'].iloc[-2])  if mas.get('ma5')  is not None and len(mas['ma5'])  >= 2 else 0.0
                            _sc10  = float(mas['ma10'].iloc[-2]) if mas.get('ma10') is not None and len(mas['ma10']) >= 2 else 0.0
                            _sc20  = float(mas['ma20'].iloc[-2]) if mas.get('ma20') is not None and len(mas['ma20']) >= 2 else 0.0
                            _sc5p2 = float(mas['ma5'].iloc[-3])  if mas.get('ma5')  is not None and len(mas['ma5'])  >= 3 else 0.0
                            _sc10p2= float(mas['ma10'].iloc[-3]) if mas.get('ma10') is not None and len(mas['ma10']) >= 3 else 0.0
                        except Exception:
                            _sc5 = _sc10 = _sc20 = _sc5p2 = _sc10p2 = 0.0
                        _structural_uptrend_continuation = bool(
                            dur_5x10_bars is not None and int(dur_5x10_bars) <= SoleusGajamiConstants.NEWBORN_MAX_DUR
                            and _sc5 > _sc10 > _sc20 > 0.0
                            and _sc5p2 > _sc10p2                # 전전봉도 정배열 — 연속성 확인
                            and _basic_long_alive_ok
                            and float(indicators.get("ma5_slope", 0.0) or 0.0) > 0.0
                        )
                        indicators["gajami_struct_uptrend_cont"] = int(_structural_uptrend_continuation)
                        if _structural_uptrend_continuation:
                            logger.info(
                                f"🟢 [TOP-ZONE-MA-ALIGN] {symbol} "
                                f"정배열 신고가 연속 관측 | "
                                f"dur={dur_5x10_bars} long_alive={int(_basic_long_alive_ok)}"
                            )

                        _top_zone_trend_alive = bool(
                            _tlc_ma10_slope >= 0.0
                            or spacing_expanding
                            or _structural_uptrend_continuation
                        )

                        _ranging_top_body_momentum_raw_ok = bool(
                            _birth_body_momentum_ok
                        )
                        _ranging_top_body_momentum_ok = bool(
                            _ranging_top_body_momentum_raw_ok
                        )

                        indicators["gajami_ranging_top_body_momentum_raw_ok"] = int(
                            _ranging_top_body_momentum_raw_ok
                        )
                        indicators["gajami_ranging_top_body_momentum_ok"] = int(
                            _ranging_top_body_momentum_ok
                        )
                        indicators["gajami_top_body_momentum_jangdae_block"] = 0
                        indicators["gajami_top_zone_trend_alive"] = int(
                            _top_zone_trend_alive
                        )
                        
                        _gajami_birth_recognized = bool(
                            (
                                indicators.get("gajami_convergence_birth_core", 0)
                                or indicators.get("subtick_body_birth", 0)
                            )
                            and _basic_long_alive_ok
                        )
                        indicators["gajami_birth_recognized"] = int(_gajami_birth_recognized)

                        _birth_impulse_ok = bool(
                            ma5_reaccelerating
                            or _had_pullback
                            or intrabar_trigger
                            or _birth_body_momentum_ok
                            or _gajami_birth_recognized
                        )

                        _quantized_with_ma5_pos_ok = bool(
                            _quantized_shape_v2_ok
                            and ma5 > 0
                            and current_price is not None
                            and current_price >= ma5 * 0.997
                        )
                        _quantized_td_rg_rescue_ok = bool(
                            _quantized_with_ma5_pos_ok
                            and _birth_impulse_ok
                        )
                        _quantized_birth_rescue_ok = bool(
                            _quantized_td_rg_rescue_ok
                            if (regime in (MarketRegime.TRENDING_DOWN, MarketRegime.RANGING)
                                and dur_5x10_bars is not None
                                and int(dur_5x10_bars) <= SoleusGajamiConstants.NEWBORN_MAX_DUR)
                            else _quantized_shape_v2_ok
                        )
                        _quantized_birth_rescue_ok = False
                        _quantized_td_rg_rescue_ok = False

                        _MID_TLC_FLOOR = SoleusGajamiConstants.MA20_SLOPE_DECEL_LIMIT * 100.0  # -0.15%
                        _tlc_alive_at_top = bool(
                            _basic_long_alive_ok
                            and float(_tlc_ma20_slope or 0.0) >= _MID_TLC_FLOOR
                            and _top_zone_trend_alive   # Diff1 SSoT 재사용 — 가속 중복 제거
                        )
                        indicators["gajami_tlc_alive_at_top"] = int(_tlc_alive_at_top)

                        # TLC 상태 기반 판단 (MA10/MA20 각각)
                        # 가자미근: 단기↓ + 중기↑ → 진입
                        _gajami_tlc = (
                            _tlc_ma10_slope < 0.0
                            and _tlc_ma20_slope > 0.0
                            and _tlc_ma10_slope >= _tlc_ma10_prev
                        )
                        # 상승 끝물: MA10 양수 급감속(50%미만) + MA20 가속 → 차단
                        _peak_block = (
                            _tlc_ma10_prev > 0
                            and _tlc_ma10_slope > 0
                            and abs(_tlc_ma10_prev) > 1e-6
                            and (_tlc_ma10_slope / _tlc_ma10_prev) < 0.30
                            and _tlc_ma20_slope > _tlc_ma20_prev
                        )
                        # 완전 하락 차단: 둘다 음수
                        _both_down = (
                            _tlc_ma10_slope < 0 and _tlc_ma20_slope < 0
                        )
                        # MA5 추세 생존: 가속 or 진짜 탄생봉
                        _ma5_trend_alive = (
                            ma5_reaccelerating
                            or (recent_cross_ok and int(dur_5x10_bars or 0) <= RECENT_GC_MAX_BARS)
                        )
                        # 중기쌍 floor: 한쪽 상향이어도 다른 쪽 급락이면 BIRTH 회복 아님
                        _birth_mid_pair_slope_ok = bool(
                            (_tlc_ma10_slope > 0.0 or _tlc_ma20_slope > 0.0)
                            and _tlc_ma10_slope >= _MID_TLC_FLOOR
                            and _tlc_ma20_slope >= _MID_TLC_FLOOR
                        )
                        _tlc_ratio_ok = (
                            _tlc_ma5_slope > 0
                            and _birth_mid_pair_slope_ok
                            and not _peak_block
                        )

                        # MA10 전환점: prev양수→cur음수 = MA5가 MA10 막 추월하는 탄생 구조
                        # mid_recover 제거 후 단독 전환 신호
                        _birth_mid_ma10_turning_ok = bool(
                            _tlc_ma10_prev > 0.0
                            and _tlc_ma10_slope < 0.0
                            and _tlc_ma20_slope > 0.0
                            and dur_5x10_bars is not None
                            and int(dur_5x10_bars) <= SoleusGajamiConstants.NEWBORN_MAX_DUR
                            and ma5_intrabar_cross_up
                        )
                        _birth_mid_alive = bool(
                            _birth_mid_pair_slope_ok
                            or _birth_mid_shallow_alive_ok
                            or _birth_mid_ma10_turning_ok
                        )
                        logger.info(
                            f"[TLC-STATE] {symbol} | "
                            f"gajami={_gajami_tlc} peak={_peak_block} down={_both_down} "
                            f"ratio_ok={_tlc_ratio_ok} ma5_alive={_ma5_trend_alive} "
                            f"mid_pair={_birth_mid_pair_slope_ok} turning={_birth_mid_ma10_turning_ok} "
                            f"floor={_MID_TLC_FLOOR:+.3f} | "
                            f"ma10={_tlc_ma10_slope:+.3f}/prev={_tlc_ma10_prev:+.3f} "
                            f"ma20={_tlc_ma20_slope:+.3f}/prev={_tlc_ma20_prev:+.3f}"
                        )

                        if (
                            not _gajami_basic_ok
                            or not _gajami_law1_ok
                            or not _gajami_law2_ok
                            or _gajami_constitution_veto
                        ):
                            _veto_parts = []
                            if not _gajami_basic_ok:
                                _veto_parts.append(
                                f"BASIC(gc={int(_basic_gc_ok)},head={int(_basic_ma5_head_ok)},"
                                f"mid={int(_basic_mid_alive_ok)},long={int(_basic_long_alive_ok)},"
                                f"td_rg_ma20={int(_basic_td_rg_ma20_down_block)}, "
                                f"thr={SoleusGajamiConstants.MA20_SLOPE_DECEL_LIMIT:+.5f})"
                            )
                            if not _gajami_law1_ok:
                                _veto_parts.append(
                                    f"헌법①(s5:{ma5_slope:.5f}<{_ma5_dir_min:.5f}"
                                    f",birth_relax={_law1_birth_relax})"
                                )
                            if not _gajami_law2_ok:
                                _veto_parts.append(
                                    f"헌법②(closed_gc={gc_5_10},rt_gc={_entry_ma5_gt_ma10}"
                                    f",rt_eq={_entry_ma5_eq_ma10}"
                                    f",closed_ma5={_closed_ma5:.4f},closed_ma10={_closed_ma10:.4f}"
                                    f",rt_ma5={_cur_ma5:.4f},rt_ma10={_cur_ma10:.4f}"
                                    f",expand={spacing_expanding},dur={dur_5x10_bars})"
                                )
                            logger.info(
                                f"📜 [가자미근-헌법] {symbol} 거부권 | {' | '.join(_veto_parts)}"
                            )

                        # 🔒 TD 체제에서는 recent_cross 만료 시 soleus 차단
                        # (만료된 골든 기반 늦은 반등 진입 방지)
                        _td_recent_gate = (
                            regime != MarketRegime.TRENDING_DOWN
                            or recent_cross_ok
                        )
                        # 탄생봉(dur<=2): 정배열 미완성은 구조적 정상 → cross_score 음수 허용
                        _is_parallel_gc = bool(
                            int(indicators.get("closed_gc_5x10_ok", 0)) == 1
                            and 0.0 <= spread_now <= 0.1
                            and ma5_slope > 0
                        )
                        _gc_quality_ok = (
                            (cross_score >= 0)
                            or (_is_newborn_dur and cross_score >= -1.5)
                            or _is_parallel_gc
                        )
                        logger.info(
                            f"[GC-QUALITY] {symbol} cross_score={cross_score:.2f} "
                            f"→ gc_quality_ok={_gc_quality_ok}"
                            + (f" [기차길 gap={spread_now:.3f}% s5={ma5_slope:.5f}]" if _is_parallel_gc else "")
                        )
                        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
                        # 🏛️ Phase 2-A: 헌법/개별법 구조분해 — 판정 불변
                        # 목적: legacy 혼합식을 변수 차원에서 분리. 매매 결과 변경 없음.
                        # 주의: _gajami_constitution_ok는 legacy_final을 그대로 따라간다.
                        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
                        _core_shape_raw_ok = bool(_core_shape_ok)
                        _legacy_shape_law_raw_ok = bool(
                            _gajami_basic_ok
                            and _core_shape_raw_ok
                            and gajami_physically_possible
                            and _gajami_law1_ok
                            and _gajami_law2_ok
                        )
                        _legacy_individual_laws_ok = bool(
                            not _gajami_constitution_veto
                        )
                        _gajami_constitution_legacy_ok = bool(
                            _legacy_shape_law_raw_ok
                            and _legacy_individual_laws_ok
                        )

                        # V2 헌법은 이 위치에서 1회만 계산한다.
                        # 계산 실패는 시장 DEAD가 아니라 분석 무효로 분리하고 진입은 fail-closed 한다.
                        _v2_ctn_error = None
                        try:
                            # 🔒 [MA5-TICK-UP-0816] 확정봉 1봉 변화를 틱으로 재계산.
                            _ma5_tick_up_v2 = SoleusGajamiConstants.ma5_tick_up(
                                indicators.get("ma5")
                            )
                            _v2_ctn_ok, _v2_ctn_fail = calculate_gajami_constitution(
                                ma5=float(_cur_ma5 or 0.0),
                                ma10=float(_cur_ma10 or 0.0),
                                ma5_slope=float(ma5_slope or 0.0),
                                ma10_slope=float(ma10_slope or 0.0),
                                ma20_slope=float(slope_ma20_df or 0.0),
                                ma60_slope=float(slope_ma60 or 0.0),
                                ma120_slope=float(slope_ma120 or 0.0),
                                regime=regime,
                                dur_5x10_bars=dur_5x10_bars,
                                ma5_tick_up=_ma5_tick_up_v2,
                                gc_5x10_confirmed=gc_5_10,
                            )
                        except Exception as _v2_ctn_e:
                            _v2_ctn_ok = False
                            _v2_ctn_fail = "ANALYSIS_INVALID_ERROR"
                            _v2_ctn_error = (
                                f"{type(_v2_ctn_e).__name__}:"
                                f"{str(_v2_ctn_e)[:120]}"
                            )
                            indicators["gajami_constitution_v2_error"] = 1
                            indicators["gajami_constitution_v2_error_detail"] = (
                                _v2_ctn_error
                            )
                            logger.exception(
                                f"🛑 [GAJAMI-CONSTITUTION-ERR] {symbol} | "
                                f"{_v2_ctn_error} → ANALYSIS_INVALID_ERROR"
                            )
                        _shape_fail_reason = str(gajami_shape_result.get("reason", "") or "")
                        _shape_slope_fail = _shape_fail_reason.startswith((
                            "CORE",
                            "HARD-CORE-GUARD",
                            "MA5_NOT_LEADING",
                            "MA5_SLOPE_LOW",     # L663-670 분리 정합 — slope_min 미달도 동일 slope veto (soft 격하 대상)
                            "리드부족",          # L1030 reason 정합 — MA5 선행 veto 동일 개념
                        ))
                        _is_birth_cand = bool(
                            dur_5x10_bars is not None
                            and int(dur_5x10_bars) <= SoleusGajamiConstants.NEWBORN_MAX_DUR
                            and recent_cross_ok
                        )

                        # 🆕 [핀셋#1 SSoT] 추세생존 — 계산 1곳, Analysis/Strategy 동일 소비
                        # strict: ma5 > ma10 > ma20 + MA10 slope 생존 + (MA20 slope>0 OR MA60 살아있음)
                        # 🆕 [MA60사면 06-29] 박스돌파 초기 MA20(중기선) 하강잔상이어도 장기선(MA60)>0면 생존 인정.
                        #   SLX 11:42(MA20↓-0.27%, s60+0.2%, fwd+5%) 구제 / GAS형 카운터트렌드(s60 strong↓)는 차단유지.
                        #   모집단 4종목 정배열차단 167봉: s60>0 forward 62%승 + 봇 손절-1%타이트/SLX형+5%승 = 비대칭+EV.
                        # 🔧 [중기-현재봉금지 정합 06-29] 배열의 MA10 값을 closed로(헌법: MA10/MA20 확정봉). _cur_ma10→_closed_ma10(L7135 SSoT).
                        #   intrabar flicker 정배열 제거 → MA60사면이 '확정 정배열'만 구제(TD/RG 까다롭게 부합). MA5는 헌법상 현재봉 허용 유지·slope는 이미 확정봉.
                        # 🔒 [TREND-ALIVE-CLOSED-MA10] trend_alive는 TOP_RISE 5% 사면으로 소비되므로
                        #   중기 추세 판단이다. MA10 배열 기준은 확정봉(_closed_ma10)만 사용한다.
                        _trend_alive_ma10 = float(_closed_ma10 or 0.0)

                        _trend_alive = bool(
                            _cur_ma5 > _trend_alive_ma10 > ma20
                            and _trend_alive_ma10 > 0.0
                            and _tlc_ma10_slope > 0.0
                            and (
                                _tlc_ma20_slope > 0.0
                                or float(slope_ma60 or 0.0) > 0.0
                            )   # 🔧 [TREND-ALIVE-MA60] MA20 래깅시 MA60 살아있으면 추세생존 인정.
                        )
                        indicators["gajami_trend_alive"] = int(_trend_alive)

                        # 🔧 VALID 초기 가자미근(dur>2)도 soft격하 — 상태법 정합
                        _shape_slope_state_ok = bool(
                            _is_birth_cand
                            or _trend_alive
                            or (
                                recent_cross_ok
                                and entry_trigger_ok
                                and window_ok
                                and _v2_ctn_ok
                                and _tlc_ratio_ok
                                and _ma5_trend_alive
                                and _birth_mid_pair_slope_ok
                                and bool(indicators.get("shape_v2_body_growth", False))
                                and (_had_pullback or is_price_on_ma5)
                            )
                        )
                        # slope-soft 경로(early-return)에서 aligned_ok는 미계산 default=False
                        # → gajami_shape_result 대신 gap_5_10_pct 직접 체크(역배열만 차단)
                        _shape_align_ok = bool(gap_5_10_pct >= -0.15)
                        _shape_slope_soft = False
                        indicators["gajami_shape_slope_soft_penalty"] = 0

                        _legacy_shape_law_ok = bool(
                            _gajami_basic_ok
                            and _core_shape_ok
                            and gajami_physically_possible
                            and _gajami_law1_ok
                            and _gajami_law2_ok
                        )
                        _gajami_constitution_ok = bool(
                            _legacy_shape_law_ok
                            and _legacy_individual_laws_ok
                        )

                        # 🔒 [TRECLAIM-CONSTITUTION-DEDUP-0827]
                        # TRECLAIM은 구조 origin만 판정한다.
                        # 헌법은 최종 soleus 경로의 _const_breakout_raw /
                        # _soleus_original_ok에서 다시 강제되므로 여기서 중복 판정하지 않는다.
                        _trend_reclaim_origin_ok = bool(
                            _prev2_dc
                            and dur_5x10_bars is not None
                            and int(dur_5x10_bars) <= SoleusGajamiConstants.NEWBORN_MAX_DUR
                            and entry_trigger_ok
                            and _tlc_ma20_slope >= 0.0
                            and not _both_down
                        )

                        # 🔍 [TRECLAIM-ARMS-OBSERVE-0827] ★298
                        # TRECLAIM 판단 시점 값을 고정한다.
                        # 후단에서 dur가 재정규화되므로 여기서 snapshot 해야 한다.
                        try:
                            _treclaim_durok = int(
                                dur_5x10_bars is not None
                                and int(dur_5x10_bars)
                                <= SoleusGajamiConstants.NEWBORN_MAX_DUR
                            )
                            _treclaim_arms_obs = (
                                f"prev2dc={int(bool(_prev2_dc))} "
                                f"dur={dur_5x10_bars} "
                                f"durok={_treclaim_durok} "
                                f"const={int(bool(_gajami_constitution_ok))} "
                                f"trig={int(bool(entry_trigger_ok))} "
                                f"ma20s={float(_tlc_ma20_slope):+.4f}%"
                                f"({int(float(_tlc_ma20_slope) >= 0.0)}) "
                                f"notbothdn={int(not bool(_both_down))} "
                                f"ctx_peak={int(bool(_peak_block))}"
                            )
                        except Exception:
                            _treclaim_arms_obs = "obs_err"

                        if _trend_reclaim_origin_ok and _peak_block:
                            logger.info(
                                f"✅ [TREND-RECLAIM-ORIGIN] {symbol} "
                                f"진짜 DC→GC + MA20 생존 → peak newborn origin 인정 | "
                                f"const={int(bool(_gajami_constitution_ok))} final_override=0 | "
                                f"dur={dur_5x10_bars} prev2_dc={int(_prev2_dc)} "
                                f"ma20s={_tlc_ma20_slope:+.4f}%"
                            )
                        _newborn_tlc_alive_ok = bool(
                            dur_5x10_bars is not None
                            and int(dur_5x10_bars) <= SoleusGajamiConstants.NEWBORN_MAX_DUR
                            and _gajami_constitution_ok
                            and entry_trigger_ok
                            and _tlc_ma20_slope > 0.0
                            and ma10_slope >= SoleusGajamiConstants.NEWBORN_TLC_MA10_SLOPE_MIN_RATIO
                            and _birth_mid_pair_slope_ok
                            and not _both_down
                            and (
                                not _peak_block
                                or _trend_reclaim_origin_ok
                            )
                        )
                        indicators["gajami_newborn_tlc_alive_ok"] = int(_newborn_tlc_alive_ok)
                        if _newborn_tlc_alive_ok and not _tlc_ratio_ok:
                            logger.info(
                                f"✅ [NEWBORN-TLC-ALIVE] {symbol} "
                                f"dur={dur_5x10_bars} "
                                f"ma5s={_tlc_ma5_slope:+.4f}% "
                                f"ma10s={ma10_slope:+.5f} "
                                f"ma20s={_tlc_ma20_slope:+.4f}% "
                                f"→ newborn TLC alive"
                            )
                        _log_fn = logger.info if not _gajami_constitution_ok else logger.debug
                        _log_fn(
                            f"[GAJAMI-CONSTITUTION] {symbol} = {_gajami_constitution_ok} "
                            f"| shape={gajami_shape_ok} phys={gajami_physically_possible} "
                            f"law1={_gajami_law1_ok} law2={_gajami_law2_ok} "
                            f"closed_gc={gc_5_10} rt_gc={_entry_ma5_gt_ma10} "
                            f"rt_eq={_entry_ma5_eq_ma10} "
                            f"closed_ma5={_closed_ma5:.4f} closed_ma10={_closed_ma10:.4f} "
                            f"rt_ma5={_cur_ma5:.4f} rt_ma10={_cur_ma10:.4f} "
                            f"newborn={_is_newborn_dur}(train={_is_birth_traintrack},verified_train={_verified_birth_traintrack},dur={dur_5x10_bars},s10={ma10_slope:.5f},gap={_cur_gap_pct:.3f}%) "
                            f"veto3={_gajami_constitution_veto}"
                            f"(birth_exempt={int(_is_birth_veto_exempt)},"
                            f"trend_birth_exempt={int(_veto3_trend_birth_exempt)},"
                            f"normal_expand={int(normal_expand)},"
                            f"struct_tu={int(_structural_tu_breakout)}) "
                            f"body_mom_top={int(_ranging_top_body_momentum_ok)} "
                            f"tlc_top={int(_tlc_alive_at_top)}"
                        )

                        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
                        # 🏛️ V2 헌법 소비 flag 계산
                        # - gajami_constitution_v2_ok/fail
                        # - gajami_shape_constitution_v2_ok
                        # downstream 소비: BIRTH-HEIGHT 면제 / TD-RG newborn life / Strategy _rt_v3_ok
                        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
                        # 위에서 계산한 V2 헌법 SSoT를 재사용한다. 동일 함수 중복 호출 금지.
                        _ctn_ok, _ctn_fail = _v2_ctn_ok, _v2_ctn_fail
                        indicators["gajami_constitution_v2_ok"] = int(_ctn_ok)
                        indicators["gajami_constitution_v2_fail"] = _ctn_fail
                        indicators["gajami_constitution_v2_error"] = int(
                            _v2_ctn_error is not None
                        )
                        indicators["gajami_constitution_v2_error_detail"] = (
                            _v2_ctn_error or ""
                        )

                        # 🏛️ Shape constitution flag: 고개듦 + 복어형 과확장 금지
                        _head_lift_v2_ok = bool(ma5_slope > ma10_slope)
                        _puffer_gap_5_10 = (
                            abs(float(_cur_ma5 or 0) - float(_cur_ma10 or 0))
                            / float(_cur_ma10 or 1)
                        )
                        _puffer_gap_10_20 = (
                            abs(float(_cur_ma10 or 0) - float(ma20 or 0))
                            / float(ma20 or 1)
                        )
                        _is_newborn_puffer = bool(
                            dur_5x10_bars is not None
                            and int(dur_5x10_bars)
                            <= SoleusGajamiConstants.NEWBORN_MAX_DUR
                        )
                        _puffer_shape_v2 = bool(
                            _puffer_gap_5_10 > 0.030
                            and _puffer_gap_10_20 > 0.030
                            and not _is_newborn_puffer
                        )
                        _shape_constitution_v2_ok = bool(
                            _ctn_ok
                            and _head_lift_v2_ok
                            and not _puffer_shape_v2
                        )
                        indicators["gajami_shape_constitution_v2_ok"] = int(
                            _shape_constitution_v2_ok
                        )
                        # gajami_decision_v2 객체와 V3 shadow 로그는 제거 완료

                        _ma20_reclaim = (
                            len(df) >= 2
                            and float(df['close'].iloc[SECOND_LAST_INDEX]) <= float(prev_mas.get('ma20', 0.0) or 0.0)
                            and cur_price >= ma20
                        )
                        logger.info(
                            f"[MA20-RECLAIM] {symbol} | "
                            f"prev_close={float(df['close'].iloc[SECOND_LAST_INDEX]):.1f} "
                            f"prev_ma20={float(prev_mas.get('ma20', 0.0) or 0.0):.1f} "
                            f"cur={cur_price:.1f} ma20={ma20:.1f} "
                            f"reclaim={_ma20_reclaim} dur={dur_5x10_bars}"
                        )

                        # 🆕 TD newborn 강회복 예외 SSoT
                        # Analysis 4곳 + Strategy 3곳 동시 소비 (indicators 전달)
                        # WLD 15:36: dur=2 새골크 s5=0.427% → 면제
                        # IN 06:09: quantized_shape_v2_ok=True이면 ma10_alive/s5 임계 우회
                        # 휩소 방어: _prev_already_gc + SHAPE-V2 5중 + dc_conflict=0로 분리
                        # 🔒 TD newborn strong recovery 면제는 MA20 회복 없이 불가
                        # MID-TREND 기준: _tlc_ma20_slope = 확정봉 기반 5봉 회귀값 (헌법: 현재봉 금지)
                        _td_newborn_ma20_ok = bool(_tlc_ma20_slope >= 0.0)

                        _td_newborn_strong_recovery_base = bool(
                            regime == MarketRegime.TRENDING_DOWN
                            and dur_5x10_bars is not None
                            and int(dur_5x10_bars) <= 1
                            and _is_ma10_birth_newborn
                            and (_ma10_alive_subtick or _quantized_shape_v2_ok)
                            and (ma5_slope >= SoleusGajamiConstants.MA5_SLOPE_QUANT_RATIO or _quantized_shape_v2_ok)
                            and ma10_slope >= 0.0
                            and _gajami_constitution_ok
                            and entry_trigger_ok
                            and not _peak_block
                        )
                        _over_rally_hard_block = bool(indicators.get('over_rally_hard_block', 0))
                        if _td_newborn_strong_recovery_base and _over_rally_hard_block:
                            logger.info(
                                f"🚫 [TD-NEWBORN-OVERRALLY-BLOCK] {symbol} "
                                f"TD 강회복 면제 차단 | over_rally_block=1"
                            )
                        _td_newborn_strong_recovery_exempt = bool(
                            _td_newborn_strong_recovery_base
                            and _td_newborn_ma20_ok
                            and not _over_rally_hard_block
                        )
                        _td_newborn_strong_recovery_exempt = False
                        indicators["gajami_td_newborn_strong_recovery_exempt"] = int(_td_newborn_strong_recovery_exempt)

                        if _td_newborn_strong_recovery_base and not _td_newborn_ma20_ok:
                            logger.info(
                                f"🚫 [TD-NEWBORN-MA20-BLOCK] {symbol} "
                                f"TD 강회복 면제 차단 | "
                                f"dur={dur_5x10_bars} s5={ma5_slope:.5f} s10={ma10_slope:.5f} "
                                f"tlc20={_tlc_ma20_slope:+.3f}% reclaim={int(_ma20_reclaim)}"
                            )
                        if _td_newborn_strong_recovery_exempt:
                            logger.info(
                                f"✅ [TD-NEWBORN-STRONG-RECOVERY-EXEMPT] {symbol} | "
                                f"dur={dur_5x10_bars} s5={ma5_slope:.5f} s10={ma10_slope:.5f} "
                                f"ma10_alive_st={int(_ma10_alive_subtick)} "
                                f"m20_ok={int(_td_newborn_ma20_ok)} "
                                f"tlc20={_tlc_ma20_slope:+.3f}% → TD 4곳 면제"
                            )

                        # TD 전환 초입 탄생봉 MA20 회복형 중복 veto 제거:
                        # 헌법/생애주기 PASS + dur<=2 + 회복분기(expand / lowbox / decel)이면
                        # TD 전용 MA20 반등 hard veto로 다시 죽이지 않는다.
                        # mid_shallow/decel은 단독 면제권이 아니라 recovery predicate 내부에서만 소비한다.
                        _td_ma20_newborn_expand_ok = bool(
                            spacing_expanding
                            and bool(indicators.get("shape_v2_body_growth", False))
                            and entry_trigger_ok
                        )
                        _td_ma20_newborn_lowbox_ok = bool(
                            regime == MarketRegime.TRENDING_DOWN
                            and dur_5x10_bars is not None
                            and int(dur_5x10_bars) <= SoleusGajamiConstants.NEWBORN_MAX_DUR
                            and _gajami_constitution_ok
                            and bool(indicators.get("subtick_dc_body_birth", 0))
                            and bool(indicators.get("shape_v2_body_growth", False))
                            and entry_trigger_ok
                            and is_low_area
                            and (_had_pullback or ma5_reaccelerating)
                        )

                        _TD_NEWBORN_DECEL_MA20_FLOOR_PCT = -0.35
                        _td_ma20_newborn_decel_ok = bool(
                            bool(indicators.get("shape_v2_body_growth", False))
                            and entry_trigger_ok
                            and _tlc_ma20_slope > _tlc_ma20_prev
                            and _tlc_ma20_slope >= _TD_NEWBORN_DECEL_MA20_FLOOR_PCT
                        )

                        _td_ma20_newborn_recovery_ok = bool(
                            regime == MarketRegime.TRENDING_DOWN
                            and dur_5x10_bars is not None
                            and int(dur_5x10_bars) <= SoleusGajamiConstants.NEWBORN_MAX_DUR
                            and _gajami_constitution_ok
                            and (
                                _td_ma20_newborn_expand_ok
                                or _td_ma20_newborn_lowbox_ok
                                or _td_ma20_newborn_decel_ok
                            )
                        )
                        indicators["gajami_td_ma20_newborn_recovery_ok"] = int(_td_ma20_newborn_recovery_ok)

                        # [TD_DOWNHALT 우회도로 폐기 2026-07-10]
                        # TD 반등도 가자미근 헌법과 기존 개별법을 그대로 통과해야 한다.

                        _td_ma20_rebound_ok = bool(
                            regime != MarketRegime.TRENDING_DOWN
                            or _tlc_ma20_slope > 0.0
                            or (
                                _ma20_reclaim
                                and _tlc_ma20_slope >= 0.0
                            )
                            or (
                                _is_birth_veto_exempt
                                and _tlc_ma20_slope >= 0.0
                            )
                            or _td_newborn_strong_recovery_exempt
                            or _td_ma20_newborn_recovery_ok
                        )
                        indicators["gajami_td_ma20_rebound_block"] = int(not _td_ma20_rebound_ok)

                        if _td_ma20_newborn_recovery_ok and _tlc_ma20_slope <= 0.0:
                            logger.info(
                                f"✅ [TD-MA20-NEWBORN-RECOVERY-ALLOW] {symbol} "
                                f"탄생봉 MA20 회복형 veto 제거 | "
                                f"dur={dur_5x10_bars} "
                                f"mid_shallow={int(_birth_mid_shallow_alive_ok)} "
                                f"expand_ok={int(_td_ma20_newborn_expand_ok)} "
                                f"lowbox_ok={int(_td_ma20_newborn_lowbox_ok)} "
                                f"decel_ok={int(_td_ma20_newborn_decel_ok)} "
                                f"constitution={int(_gajami_constitution_ok)} "
                                f"tlc20={_tlc_ma20_slope:+.3f}% "
                                f"prev={_tlc_ma20_prev:+.3f}% "
                                f"reclaim={int(_ma20_reclaim)}"
                            )

                        if not _td_ma20_rebound_ok:
                            logger.info(
                                f"🚫 [TD-MA20-REBOUND-BLOCK] {symbol} TD MA20 반등 미확인 | "
                                f"tlc20={_tlc_ma20_slope:+.3f}% prev={_tlc_ma20_prev:+.3f}% "
                                f"reclaim={int(_ma20_reclaim)}"
                            )

                        # 🔒 [MA20-COLLAPSE-BIRTH-ORIGIN-SSOT-0829-R1]
                        # RANGING MA20 급락 면제의 탄생 origin을 검증된 수렴형 탄생까지 정합화한다.
                        # _gajami_birth_recognized 전체는 subtick_body_birth를 포함하므로 raw-origin으로 쓰지 않는다.
                        _ranging_ma20_alive_ok = bool(
                            regime != MarketRegime.RANGING
                            or _tlc_ma20_slope >= _MID_TLC_FLOOR
                            or (
                                (
                                    _is_true_gc_birth
                                    or bool(indicators.get("gajami_convergence_birth_core", 0))
                                )
                                and _gajami_constitution_ok
                                and entry_trigger_ok
                                and _birth_impulse_ok
                            )
                        )
                        indicators["gajami_ranging_ma20_collapse_block"] = int(not _ranging_ma20_alive_ok)

                        if not _ranging_ma20_alive_ok:
                            logger.info(
                                f"🚫 [RANGING-MA20-COLLAPSE-BLOCK] {symbol} RANGING MA20 급락 | "
                                f"tlc20={_tlc_ma20_slope:+.3f}% floor={_MID_TLC_FLOOR:+.3f}%"
                            )

                        _birth_old_midcycle_reborn_block = False
                        _birth_top_late_newborn_block = False

                        _strong_impulse_birth_ok = False
                        indicators["gajami_strong_impulse_birth_ok"] = 0

                        _birth_top_ma10_ok = True
                        _birth_top_reset_ok = True
                        _birth_traintrack_life_ok = bool(
                            not _is_birth_traintrack
                            or _is_birth_bottom
                            or _trend_continuing
                            or (
                                _verified_birth_traintrack
                                and (
                                    regime != MarketRegime.TRENDING_DOWN
                                    or _tlc_ma20_slope > 0.0
                                )
                                and (
                                    regime != MarketRegime.RANGING
                                    or _tlc_ratio_ok
                                )
                            )
                            or (
                                _tlc_ratio_ok
                                and _tlc_ma10_slope > 0.0
                            )
                        )
                        indicators["gajami_birth_traintrack_life_ok"] = bool(_birth_traintrack_life_ok)

                        _birth_micro_box_ok = bool(
                            dur_5x10_bars is not None
                            and int(dur_5x10_bars) <= SoleusGajamiConstants.NEWBORN_MAX_DUR
                            and _birth_mid_alive
                            and _ma5_trend_alive
                            and entry_trigger_ok
                            and not _both_down
                        )

                        _birth_top_lifecycle_ok = True
                        _birth_tlc_height_ok = True

                        _birth_tlc_ok = bool(
                            _gajami_constitution_ok
                            and entry_trigger_ok
                            and not _peak_block
                            and _ma5_trend_alive
                            and not _both_down
                            and dur_5x10_bars is not None
                            and int(dur_5x10_bars) <= SoleusGajamiConstants.NEWBORN_MAX_DUR
                            and _birth_tlc_height_ok
                            and _birth_top_ma10_ok
                            and _birth_top_reset_ok
                            and _birth_traintrack_life_ok
                            and _birth_top_lifecycle_ok
                            and (_birth_mid_alive or _td_ma20_newborn_lowbox_ok)
                            and _td_ma20_rebound_ok
                            and _ranging_ma20_alive_ok
                        )

                        # TLC 진입자격 하드게이트 해제
                        # TLC 역할 재배치: 진입 자격 → 추세 품질(포지션크기/손절민감도)
                        # 탄생 초입(dur<=2) + MA20 생존이면 TLC 미완성이어도 진입 허용
                        # 찐/가짜 구분은 MA20 생존(reclaim) + TLC-alive 플래그가 담당
                        # (box_h/box_pos는 상승의 후행 결과라 변별력 약함 — 2026-06-19 검증,
                        #  _newborn_no_reclaim_block가 실제 판정)
                        _newborn_no_reclaim_block = bool(
                            dur_5x10_bars is not None
                            and int(dur_5x10_bars) <= SoleusGajamiConstants.NEWBORN_MAX_DUR
                            and not _ma20_reclaim
                            and not _birth_tlc_ok
                            and not _td_newborn_strong_recovery_exempt
                            and not _birth_mid_shallow_alive_ok
                            and not _newborn_tlc_alive_ok
                        )

                        # 🔒 [REGIME-GATE-LEGACY-TLC-AUTH-REMOVE-0823]
                        # TD/RG의 recent/MA20/GC 안전조건은 유지한다.
                        # W5/W7 고정창 TLC(_tlc_ratio_ok, _tlc_ma10/20_slope)는 관측만 남기고
                        # 능동형 Active TLC와 중복되는 독립 hard-veto 권한은 제거한다.
                        _regime_gate_ok = bool(
                            window_ok
                            and entry_trigger_ok
                            and (
                                ma20_trend_ok
                                or _birth_tlc_ok
                                or _td_newborn_strong_recovery_exempt
                            )
                            and (
                                _gc_quality_ok
                                or _birth_tlc_ok
                                or _td_newborn_strong_recovery_exempt
                            )
                            and (
                                regime not in (
                                    MarketRegime.TRENDING_DOWN,
                                    MarketRegime.RANGING,
                                )
                                or (
                                    _td_recent_gate
                                    and _td_ma20_rebound_ok
                                    and _ranging_ma20_alive_ok
                                )
                            )
                            and not (
                                indicators.get("tu_suppressed", False)
                                and not (_is_birth_bottom or _birth_tlc_ok)
                            )
                            and not _newborn_no_reclaim_block
                        )
                        _log_fn = logger.info if not _regime_gate_ok else logger.debug
                        _log_fn(
                            f"[REGIME-GATE] {symbol} = {_regime_gate_ok} "
                            f"td_rg_apply={int(regime in (MarketRegime.TRENDING_DOWN, MarketRegime.RANGING))} "
                            f"window={int(window_ok)} "
                            f"td_recent={int(_td_recent_gate)} "
                            f"td_recent_raw={int(_td_recent_gate)} "
                            f"td_m20_rebound={int(_td_ma20_rebound_ok)} "
                            f"rg_m20_alive={int(_ranging_ma20_alive_ok)} "
                            f"ma20_gate={int(ma20_trend_ok or _birth_tlc_ok or _td_newborn_strong_recovery_exempt)} "
                            f"ratio_obs={int(_tlc_ratio_ok)} "
                            f"legacy_both_down_obs={int(_tlc_ma10_slope <= 0 and _tlc_ma20_slope <= 0)} "
                            f"td_m20_recovery={int(_td_ma20_newborn_recovery_ok)} "
                            f"gc_gate={int(_gc_quality_ok or _birth_tlc_ok or _td_newborn_strong_recovery_exempt)} "
                            f"birth_tlc={_birth_tlc_ok} "
                            f"both_down={int(_both_down)} "
                            f"mid_shallow={int(_birth_mid_shallow_alive_ok)} "
                            f"td_newborn_exempt={int(_td_newborn_strong_recovery_exempt)} "
                            f"tlc_obs=({_tlc_ma10_slope:.5f},{_tlc_ma20_slope:.5f})"
                        )

                        # _gajami_birth_recognized / _birth_impulse_ok는 위 계산값을 재사용한다.
                        
                        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
                        # 🏰 GAJAMI LIFECYCLE (Stateless, dur_5x10 기반)
                        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
                        gajami_lifecycle = None
                        _life_calc_error = None
                        try:
                            # 🔒 TLC SSoT 계산값 선초기화
                            _birth_price = None
                            _rise_pct = 0.0
                            _rise_atr_ratio = 0.0
                            _height_anchor_price = None
                            _height_anchor_bars = 0
                            _height_anchor_ready = False
                            _height_rise_pct = 0.0
                            _height_max_rise = 0.0
                            _ma5_now = 0.0
                            _ma10_now = 0.0
                            _ma5_slope_ratio = 0.0
                            _ma10_slope_ratio = 0.0
                            structure_ok = False
                            _newborn_strong_rise_exempt = False
                            _birth_height_ok = False
                            structure_confirmed = False
                            _v2_newborn_exempt = False
                            _life_ma10_dead_gate_pre = False
                            _life_height_exceeded_pre = False
                            _life_trend_alive_pre = False
                            _life_pre_height_state = "DEAD"
                            _life_pre_height_reason = "INIT"
                            _life_pre_height_valid = False
                            _life_height_too_low_pre = False
                            _life_pre_td_state = "DEAD"
                            _life_pre_td_reason = "INIT"
                            _life_pre_td_valid = False
                            # 🔒 TYPE-SAFE: lifecycle 입력 스칼라 확정 (Series ambiguous 방지)
                            cur_price = safe_extract_scalar(cur_price, 0.0)
                            _raw_dur = safe_extract_scalar(dur_5x10_bars, -1)
                            dur_5x10_bars = int(_raw_dur) if _raw_dur >= 0 else None
                            is_sideways = bool(is_sideways) if not hasattr(is_sideways, 'iloc') else bool(is_sideways.iloc[-1] if len(is_sideways) > 0 else False)
                            
                            # ✅ 체제별 가자미근 HEIGHT 상한
                            # 🆕 [CLI 06-30 C-3] 사면블록(p2/trend_alive) dead-weight 제거.
                            #   06-30 검증: MAX_RISE는 regime 단독 분기. p2/trend_alive는 MAX_RISE 결정에 영향 없음(읽는 곳 0).
                            if regime in (MarketRegime.TRENDING_UP, MarketRegime.BREAKOUT, MarketRegime.RANGING):
                                MAX_RISE = 0.050
                            else:
                                MAX_RISE = 0.015
                            _height_max_rise = MAX_RISE
                            
                            MIN_RISE = 0.0001
                            MA10_SLOPE_DEAD = -0.0005

                            def _safe_last_float(val):
                                if val is None:
                                    return 0.0
                                if hasattr(val, "iloc"):
                                    if len(val) == 0:
                                        return 0.0
                                    return float(val.iloc[-1])
                                try:
                                    return float(val)
                                except Exception:
                                    return 0.0
                            
                            _ma5_now = _safe_last_float(ma5)
                            _ma10_now = _safe_last_float(ma10)
                            # (5x20 선행 GC 흡수는 5x20_absorb_ok 블록에서 처리됨)
                            _ma10_slope_raw = indicators.get('ma10_slope', 0.0)
                            _ma10_slope_ratio = float(_ma10_slope_raw or 0.0) / 100.0
                            _ma5_slope_raw = indicators.get('ma5_slope', 0.0)
                            _ma5_slope_ratio = float(_ma5_slope_raw or 0.0) / 100.0

                            _is_newborn_here = (dur_5x10_bars is not None and int(dur_5x10_bars) <= SoleusGajamiConstants.NEWBORN_MAX_DUR)
                            structure_ok = False   # 🔧 dur=None/0 신생아 분기 미할당 방지 — 8403 UnboundLocalError→soleus abort 차단 (5685회/일)
                            if _ma5_now > 0 and _ma10_now > 0 and _ma5_now < _ma10_now and not _is_newborn_here:
                                pass  # 최종 state는 _calc_lifecycle_state()에서 판정
                            elif dur_5x10_bars is None or dur_5x10_bars < 1:
                                # 🔒 [NO-DUR-VALID-REMOVE-0817] GC age 불명은 새 birth로 재작성하지 않는다.
                                _birth_price = 0.0
                                _rise_pct = 0.0
                                indicators["gajami_rise_atr_ratio"] = 0.0
                                indicators["gajami_rise_cur_pct"] = 0.0
                            else:
                                if df is not None and len(df) > dur_5x10_bars + 1:
                                    golden_idx = len(df) - dur_5x10_bars - 1
                                    # 🔧 2026-08-01 birth_price SSoT 통일 (dur 무관 = GC 발생 캔들 종가)
                                    #  - 기존: dur>2에서만 golden_idx 이전 5봉 최저 저점으로 정의가 바뀌어,
                                    #    같은 GC인데 dur 2→3이 되는 순간 birth가 과거로 점프했다
                                    #    (MEGA 16:45 TLC birth=56.6 vs lifecycle birth=55.60 동시 존재).
                                    #  - 결과: rise_pct가 부풀어 Late-Entry-Guard가 정답봉 직후 초입(dur=3)을
                                    #    "추격"(rise 1.62%/4.57x)으로 오판해 차단 → 45분 뒤 늦은 자리만 통과하는 역선택.
                                    #  - 원 주석의 원칙("이전 하락 저점 배제")을 전 구간에 동일 적용.
                                    _birth_price = float(df['close'].iloc[golden_idx])
                                else:
                                    _birth_price = float(cur_price)

                                # 🔒 [2026-08-07] TLC rise(_rise_pct)만 확정봉으로 단일화 (SSoT).
                                #   기존: dur==1만 확정봉, dur>1은 현재가 → 같은 지표가 dur에 따라 기준이 바뀜
                                #   실측 8월 전수: 같은 bar 안에서 rise_atr 변동 249건(51.7%),
                                #                 1.00x 위→아래 하락 53건(과열게이트 무력화)
                                #   소비처 확인: _rise_pct → TLC-RISE-ATR(t6 L9994~9997)·MIN_RISE 게이트
                                #   ⚠️ _rise_price는 HEIGHT 계열(-> _newborn_strong_rise_exempt L10089
                                #   -> _const_breakout_raw/_soleus_original_ok 실제 PASS 경로)까지 재사용되므로
                                #   원래의 dur==1 분기(확정봉/현재가)를 그대로 보존한다 — TLC 축만 분리.
                                _rise_pct = ((float(df['close'].iloc[-2]) - _birth_price) / _birth_price) if _birth_price > 0 else 0.0
                                if dur_5x10_bars is not None and int(dur_5x10_bars) == 1:
                                    _rise_price = float(df['close'].iloc[-2])
                                else:
                                    _rise_price = float(cur_price)
                                # [BIRTH-RISE-AUDIT] 게이트 시점 실측 현재봉 상승률 기록 (행위무변)
                                _rise_cur = ((cur_price - _birth_price) / _birth_price) if _birth_price > 0 else 0.0
                                # 🌊 HEIGHT 전용 능동형 앵커
                                # - 구조 birth_price는 GC 기준 그대로 유지한다.
                                # - HEIGHT만 최근 24시간 내 ATR 1.0x 반등이 확인된
                                #   마지막 확정 스윙저점을 기준으로 계산한다.
                                _height_anchor_price = float(_birth_price)
                                _height_anchor_bars = 0
                                _height_anchor_ready = False
                                _height_rise_pct = float(_rise_price - _birth_price) / _birth_price if _birth_price > 0 else 0.0
                                if df is not None and len(df) >= 4:
                                    _anc_end = len(df) - 2
                                    _anc_start = max(0, _anc_end - 479)
                                    _anc_post_high = float(df["high"].iloc[_anc_end])
                                    _anc_th_pct = max(0.30, float(_atr_pct_val or 0.0))
                                    for _k in range(_anc_end - 1, _anc_start - 1, -1):
                                        _anc_low = float(df["low"].iloc[_k])
                                        _anc_high = float(df["high"].iloc[_k])
                                        if not (
                                            math.isfinite(_anc_low)
                                            and math.isfinite(_anc_high)
                                            and _anc_low > 0.0
                                        ):
                                            continue
                                        _anc_rebound_pct = (
                                            (_anc_post_high - _anc_low)
                                            / _anc_low * 100.0
                                        )
                                        if _anc_rebound_pct >= _anc_th_pct:
                                            _height_anchor_price = _anc_low
                                            _height_anchor_bars = _anc_end - _k
                                            _height_anchor_ready = True
                                            break
                                        _anc_post_high = max(
                                            _anc_post_high,
                                            _anc_high,
                                        )
                                if _height_anchor_ready and _height_anchor_price > 0.0:
                                    _height_rise_pct = (
                                        (_rise_price - _height_anchor_price)
                                        / _height_anchor_price
                                    )
                                indicators["tlc_height_anchor_price"] = float(
                                    _height_anchor_price or 0.0
                                )
                                indicators["tlc_height_anchor_bars"] = int(
                                    _height_anchor_bars
                                )
                                indicators["tlc_height_anchor_ready"] = int(
                                    _height_anchor_ready
                                )
                                indicators["tlc_height_rise_pct"] = float(
                                    _height_rise_pct
                                )
                                indicators["tlc_height_max_rise"] = float(
                                    _height_max_rise
                                )
                                # 🎯 ① 단기 눌림목 (실제 저점구조·MA60↑·fresh·rise<0 = +EV)
                                try:
                                    _dur_ev = int(dur_5x10_bars) if dur_5x10_bars is not None else 999
                                except Exception:
                                    _dur_ev = 999

                                _ma60_up_ev = bool(float(slope_ma60 or 0.0) > 0.0)

                                _pullback_ev_entry = bool(
                                    regime == MarketRegime.RANGING
                                    and _dur_ev <= SoleusGajamiConstants.NEWBORN_MAX_DUR
                                    and is_low_area
                                    and _ma60_up_ev
                                    and float(_rise_cur or 0.0) < 0.0
                                )

                                # 🎯 ② 파동 중눌림: 이전20봉(현재봉 제외) 고점 대비 −3~−6%·MA60↑
                                # indicators['recent_high']는 현재봉 포함이라 사용 금지 → df 직접 계산
                                try:
                                    _recent_high_v = (
                                        float(df["high"].iloc[-21:-1].max())
                                        if (
                                            df is not None
                                            and len(df) >= 21
                                            and "high" in df.columns
                                        )
                                        else 0.0
                                    )
                                except Exception:
                                    _recent_high_v = 0.0

                                _recovery_pct = (
                                    (cur_price - _recent_high_v) / _recent_high_v * 100.0
                                    if _recent_high_v > 0 else 0.0
                                )

                                _wave_pullback_entry = bool(
                                    -6.0 <= _recovery_pct <= -3.0
                                    and _ma60_up_ev
                                )

                                # 🔒 [NEG-RISE-FLOOR-0809] F1(부모파동 C차단)과 한 쌍.
                                # 실측 NEG_RISE 차단 n=120: 막힌쪽보다 나머지가 더 나쁨
                                # (역방향 시간대 4/4). 단독 완화 금지 — F1이 C국면
                                # 재열림을 걸러낸다는 전제로만 적용.
                                _NEG_RISE_ATR_BLOCK_FLOOR = -2.00

                                _rise_atr_ratio = (_rise_cur / (_atr_pct_val / 100.0)) if _atr_pct_val > 0 else 0.0
                                
                                _neg_rise_raw = bool(_rise_cur < 0.0)
                                _neg_rise_atr_valid = bool(_atr_pct_val > 0.0)

                                # BOX TOP은 관측만 유지하고 NEG_RISE 판결에는 관여하지 않는다.
                                _neg_rise_top_block = False

                                # 🎯 NEG_RISE EV 후보 raw: 모집단 기반 눌림 후보
                                _neg_rise_ev_candidate_raw = bool(
                                    _neg_rise_raw
                                    and _neg_rise_atr_valid
                                    and (_pullback_ev_entry or _wave_pullback_entry)
                                )

                                # 🔒 확정봉 MA5>MA10 정배열 필수 (역배열 눌림 오살림 제거)
                                _neg_rise_ev_closed_gc_ok = bool(
                                    int(indicators.get("closed_gc_5x10_ok", 0) or 0) == 1
                                    and float(_closed_ma5 or 0.0) > float(_closed_ma10 or 0.0)
                                )

                                # 🔒 [NEG-RISE-PREM-EXEMPT-0820]
                                # NEG_RISE 중 직전 확정봉 대비 -0.70% 이상 눌린 구간만 hard 차단 면제.
                                # prem 축 자체 실측: n=328, win=50.8%, EV=+0.3933%, 부호=10/10.
                                # ATR 미산출 차단은 보존하고, 최종 T6 Core와 같은 closed-GC 안전망을 유지한다.
                                _prem_ne_base = float(
                                    indicators.get("prev_candle_close", 0.0) or 0.0
                                )
                                _prem_ne = (
                                    ((cur_price - _prem_ne_base) / _prem_ne_base) * 100.0
                                    if _prem_ne_base > 0.0 and cur_price > 0.0
                                    else 0.0
                                )
                                _neg_rise_ev_exempt_ok = bool(
                                    _neg_rise_raw
                                    and _neg_rise_atr_valid
                                    and _neg_rise_ev_closed_gc_ok
                                    and _prem_ne_base > 0.0
                                    and _prem_ne <= -0.70
                                )
                                
                                # ATR 미산출이면 보수적으로 기존 차단 유지
                                # 박스 꼭지 음봉은 얕은 눌림으로 보지 않고 hard block
                                _neg_rise_effective_block = bool(
                                    _neg_rise_raw
                                    and (
                                        _neg_rise_top_block
                                        or (
                                            (
                                                not _neg_rise_atr_valid
                                                or _rise_atr_ratio < _NEG_RISE_ATR_BLOCK_FLOOR
                                            )
                                            and not _neg_rise_ev_exempt_ok
                                        )
                                    )
                                )
                                _neg_rise_atr_soft_ok = bool(
                                    _neg_rise_raw
                                    and _neg_rise_atr_valid
                                    and not _neg_rise_top_block
                                    and not _neg_rise_effective_block
                                    and not _neg_rise_ev_exempt_ok
                                )

                                indicators["gajami_rise_atr_ratio"] = float(_rise_atr_ratio)
                                indicators["gajami_rise_cur_pct"] = float(_rise_cur * 100.0)
                                indicators["gajami_neg_rise_raw"] = int(_neg_rise_raw)
                                indicators["gajami_neg_rise_block"] = int(_neg_rise_effective_block)
                                indicators["gajami_neg_rise_top_block"] = int(_neg_rise_top_block)
                                indicators["gajami_neg_rise_atr_soft_ok"] = int(_neg_rise_atr_soft_ok)
                                indicators["gajami_neg_rise_atr_floor"] = float(_NEG_RISE_ATR_BLOCK_FLOOR)
                                indicators["gajami_pullback_ev_entry"] = int(_pullback_ev_entry)
                                indicators["gajami_wave_pullback_entry"] = int(_wave_pullback_entry)
                                indicators["gajami_wave_recovery_pct"] = float(_recovery_pct)
                                indicators["gajami_neg_rise_ev_candidate_raw"] = int(_neg_rise_ev_candidate_raw)
                                indicators["gajami_neg_rise_ev_closed_gc_ok"] = int(_neg_rise_ev_closed_gc_ok)
                                indicators["gajami_neg_rise_ev_exempt_ok"] = int(_neg_rise_ev_exempt_ok)

                                if _neg_rise_effective_block:
                                    _neg_rise_block_reason = (
                                        "BOX_TOP_NEG_RISE"
                                        if _neg_rise_top_block
                                        else (
                                            "ATR_MISSING"
                                            if not _neg_rise_atr_valid
                                            else "ATR_DEEP_NEG_RISE"
                                        )
                                    )
                                    logger.info(
                                        f"🚫 [NEG-RISE-BLOCK] {symbol} "
                                        f"NEG_RISE hard 차단 | "
                                        f"reason={_neg_rise_block_reason} "
                                        f"rise_atr={_rise_atr_ratio:.2f}x "
                                        f"(<{_NEG_RISE_ATR_BLOCK_FLOOR:.2f}x) "
                                        f"rise_cur={_rise_cur*100.0:.3f}%"
                                    )
                                elif _neg_rise_ev_exempt_ok:
                                    logger.info(
                                        f"🟢 [NEG-RISE-PREM-EXEMPT] {symbol} "
                                        f"직전확정봉 대비 깊은 눌림 → NEG_RISE hard 차단 면제 | "
                                        f"prem={_prem_ne:+.3f}% (<=-0.700%) "
                                        f"dur={_dur_ev} "
                                        f"closed_gc={int(_neg_rise_ev_closed_gc_ok)} "
                                        f"legacy_pullback_obs={int(_pullback_ev_entry)} "
                                        f"legacy_wave_obs={int(_wave_pullback_entry)} "
                                        f"rise_cur={_rise_cur*100.0:.3f}% "
                                        f"rise_atr={_rise_atr_ratio:.2f}x"
                                    )                                    
                                elif _neg_rise_atr_soft_ok:
                                    logger.info(
                                        f"✅ [NEG-RISE-ATR-SOFT] {symbol} "
                                        f"birth 노이즈 눌림 → NEG_RISE hard 차단 격하 | "
                                        f"rise_atr={_rise_atr_ratio:.2f}x "
                                        f"floor={_NEG_RISE_ATR_BLOCK_FLOOR:.2f}x "
                                        f"rise_cur={_rise_cur*100.0:.3f}%"
                                    )
                                try:
                                    logger.info(
                                        f"🧾 [ENTRY-FORENSIC] {symbol} | "
                                        f"regime={regime.value if hasattr(regime,'value') else regime} "
                                        f"dur={dur_5x10_bars} "
                                        f"| rise_atr={_rise_atr_ratio:.2f}x(src=rise_cur={_rise_cur*100.0:.3f}%) "
                                        f"true_gc={int(_is_true_gc_birth)} "
                                        f"prev_dc={int(_prev_dc)} prev2_dc={int(_prev2_dc)} "
                                        f"ER={float(indicators.get('er_10',0) or 0):.3f} "
                                        f"ma10={_tlc_ma10_slope:+.3f}<{_tlc_ma10_prev:+.3f} "
                                        f"expand={int(spacing_expanding)}"
                                    )
                                except Exception as _entry_forensic_e:
                                    logger.debug(f"[ENTRY-FORENSIC-ERR] {symbol} {_entry_forensic_e}")
                                
                                newborn = (
                                    dur_5x10_bars is not None
                                    and int(dur_5x10_bars) <= SoleusGajamiConstants.NEWBORN_MAX_DUR
                                )

                                structure_ok = True
                                if df is not None and len(df) >= 1 and _birth_price > 0:
                                    current_low = float(df['low'].iloc[-1])
                                    # 🔧 canonical 호가단위 (≥10000→10 오매핑 버그 폐기)
                                    birth_tick_size = get_tick_size(_birth_price)
                                    
                                    STRUCTURE_BREAK_TICKS = 2
                                    STRUCTURE_BREAK_PCT = 0.005
                                    tick_allowance = birth_tick_size * STRUCTURE_BREAK_TICKS
                                    pct_allowance = _birth_price * STRUCTURE_BREAK_PCT
                                    _atr_raw = float(safe_extract_scalar(indicators.get('atr'), 0.0) or 0.0)
                                    _atr_cap = _atr_raw if (newborn and _atr_raw > 0.0) else 0.0
                                    pct_cap = max(birth_tick_size * 5, _atr_cap)
                                    allowance = max(tick_allowance, min(pct_allowance, pct_cap))
                                    threshold_raw = _birth_price - allowance
                                    if birth_tick_size > 0:
                                        threshold = math.floor(threshold_raw / birth_tick_size) * birth_tick_size
                                    else:
                                        threshold = threshold_raw
                                    
                                    current_close = float(df['close'].iloc[-1])
                                    if current_close < threshold:
                                        # ✨ BREAK_STRUCTURE_LOW 정석 처리 (계산/로그/판단 동시 수정):
                                        # 저점 wick이 아니라 종가가 BIRTH 구조선을 이탈했을 때만 구조 붕괴로 판단.
                                        #
                                        # 판단 분기 8대 조건:
                                        #   1) current_close < threshold (종가 이탈 - 발동 트리거)
                                        #   2) newborn (dur≤2, 탄생 직후)
                                        #   3) _gajami_constitution_ok (헌법 통과)
                                        #   4) entry_trigger_ok (트리거 통과 = MA5 재탈환 포함)
                                        #   5) _regime_gate_ok (체제 게이트 통과)
                                        #   6) cur_price >= _birth_price (BIRTH 가격 회복; threshold 아님)
                                        #   7) _birth_reclaim_area_ok
                                        #      - 박스/위치가 BIRTH 회복 가능 구간이어야 함
                                        #   8) _birth_reclaim_depth_ok (깊은 붕괴 reclaim 금지)
                                        #
                                        # 후속 처리: structure_ok=True 유지만 함. VALID 직접 박지 않음.
                                        # → Master 후속 로직(MA10_SLOPE_DEAD/TOP_RISE/TREND-ALIVE/TREND-DEAD)이 순차 판단.
                                        # 참고: 같은 GAJAMI-LIFECYCLE 함수 내 [NEWBORN-INTRABAR-IGNORE] 블록과 동일한 newborn-aware 정신.
                                        # BOX TOP 품질권 회수: 실제 회복 구조만 후단에서 판정한다.
                                        _birth_reclaim_top_quality_ok = True

                                        _birth_reclaim_area_ok = True
                                        try:
                                            _birth_reclaim_atr_ratio = max(0.0, float(_atr_pct_val or 0.0) / 100.0)
                                        except Exception:
                                            _birth_reclaim_atr_ratio = 0.0
                                        _birth_reclaim_depth_ratio = min(
                                            max(_birth_reclaim_atr_ratio * 0.35, 0.00035),
                                            0.00060
                                        )
                                        _birth_reclaim_depth_allowance = max(
                                            allowance * 2,
                                            _birth_price * _birth_reclaim_depth_ratio
                                        )
                                        _birth_reclaim_depth_floor = threshold - _birth_reclaim_depth_allowance
                                        _birth_reclaim_depth_ok = bool(
                                            current_low >= _birth_reclaim_depth_floor
                                        )
                                        # impulse_exempt 제거:
                                        # - 2026-07-05 회귀검증상 impulse area 면제 경로 0발동
                                        # - BREAK_STRUCTURE_LOW가 종가 기준으로 교정되면서 wick 오살 우회용 면제 경로 불필요
                                        # - sweep/rescue live 경로는 유지
                                        _birth_reclaim_td_rg_thrust_ok = bool(
                                            _birth_impulse_ok
                                            or _tlc_ratio_ok
                                        )
                                        _birth_reclaim_price_ok = bool(
                                            cur_price >= _birth_price
                                            or (
                                                newborn
                                                and _ma5_now > 0
                                                and cur_price >= _ma5_now
                                                and _ma5_trend_alive
                                                and _birth_mid_alive
                                                and _birth_reclaim_td_rg_thrust_ok
                                            )
                                        )

                                        _birth_reclaim_sweep_ok = bool(
                                            newborn
                                            and _gajami_constitution_ok
                                            and entry_trigger_ok
                                            and _regime_gate_ok
                                            and _birth_reclaim_price_ok
                                            and _birth_reclaim_area_ok
                                            and _birth_reclaim_depth_ok
                                        )
                                        if _birth_reclaim_sweep_ok:
                                            logger.info(
                                                f"🔍 [BIRTH-RECLAIM-SWEEP-DETECTED] {symbol} "
                                                f"저점 스윕 후 BIRTH 회복 감지 → uptrend rescue 판정 대기 | "
                                                f"regime={regime.value if hasattr(regime,'value') else regime} "
                                                f"dur={dur_5x10_bars} | "
                                                f"birth={_birth_price:.2f} cur={cur_price:.2f} "
                                                f"cur_low={current_low:.2f} threshold={threshold:.2f} "
                                                f"price_ok={int(_birth_reclaim_price_ok)} "
                                                f"cur≥birth={cur_price >= _birth_price} "
                                                f"impulse={int(_birth_impulse_ok)} "
                                                f"tlc_ratio={int(_tlc_ratio_ok)} "
                                                f"area_ok={_birth_reclaim_area_ok} "
                                                f"depth_ok={_birth_reclaim_depth_ok} "
                                                f"true_gc={int(_is_true_gc_birth)} "
                                                f"ma60s={float(slope_ma60 or 0.0):+.6f} "
                                                f"long_veto={int(long_ma_veto)}"
                                            )
                                        # 🔧 [UPTREND-RECLAIM-RESCUE] 상승추세 얕은눌림 reclaim 면제
                                        # sweep_ok(헌법+trigger+regime+price+area+depth 전통과) ∧ true_gc ∧ MA60↑ → BREAK_STRUCTURE_LOW 면제
                                        _uptrend_reclaim_rescue_ok = bool(
                                            _birth_reclaim_sweep_ok
                                            # true_gc=0 V회복도 MA60↑면 reclaim 허용
                                            and float(slope_ma60 or 0.0) > 0.0
                                            and not long_ma_veto
                                        )
                                        _uptrend_reclaim_rescue_ok = False
                                        indicators["gajami_uptrend_reclaim_rescue_ok"] = int(
                                            _uptrend_reclaim_rescue_ok
                                        )

                                        if _uptrend_reclaim_rescue_ok:
                                            structure_ok = True
                                            logger.info(
                                                f"🟢 [UPTREND-RECLAIM-RESCUE] {symbol} "
                                                f"상승추세 얕은눌림 reclaim → BREAK_STRUCTURE_LOW 면제 | "
                                                f"dur={dur_5x10_bars} "
                                                f"ma60s={float(slope_ma60 or 0.0):+.6f} "
                                                f"birth={_birth_price:.2f} "
                                                f"cur={cur_price:.2f} "
                                                f"low={current_low:.2f} "
                                                f"sweep_ok={int(_birth_reclaim_sweep_ok)} "
                                                f"true_gc={int(_is_true_gc_birth)}"
                                            )
                                        else:
                                            structure_ok = False
                                            breach_abs = current_close - threshold
                                            breach_pct = (breach_abs / threshold * 100) if threshold > 0 else 0.0
                                            tick_range = f"{STRUCTURE_BREAK_TICKS}틱~{int(pct_cap/birth_tick_size)}틱"
                                            logger.info(
                                                f"🚫 [GAJAMI-LIFECYCLE] {symbol} BREAK_STRUCTURE_LOW | "
                                                f"regime={regime.value if hasattr(regime,'value') else regime} dur={dur_5x10_bars} | "
                                                f"birth={_birth_price:.2f} threshold={threshold:.2f} cur_low={current_low:.2f} close={current_close:.2f} cur={cur_price:.2f} | "
                                                f"close_breach={breach_abs:.2f}원 ({breach_pct:.2f}%) rise={_rise_pct:.2%} | "
                                                f"reclaim_sweep={int(_birth_reclaim_sweep_ok)} "
                                                f"price_ok={int(_birth_reclaim_price_ok)} cur≥birth={cur_price >= _birth_price} "
                                                f"impulse={int(_birth_impulse_ok)} tlc_ratio={int(_tlc_ratio_ok)} "
                                                f"top_q={int(_birth_reclaim_top_quality_ok)} "
                                                f"area_ok={_birth_reclaim_area_ok} depth_ok={_birth_reclaim_depth_ok} "
                                                f"depth_floor={_birth_reclaim_depth_floor:.2f} | "
                                                f"allowance={allowance:.2f}원({tick_range}) tick={birth_tick_size:.4f} pct_cap={pct_cap:.2f}"
                                            )

                                if structure_ok:
                                    _is_early_cross = (dur_5x10_bars <= 4)
                                    _newborn_strong_rise_exempt = bool(
                                        dur_5x10_bars is not None
                                        and int(dur_5x10_bars) <= SoleusGajamiConstants.NEWBORN_MAX_DUR
                                        and bool(indicators.get("gajami_basic_ok", False))
                                        and entry_trigger_ok
                                        and _birth_impulse_ok
                                        and _tlc_ratio_ok
                                        and _tlc_ma10_slope >= 0.50
                                        and _tlc_ma20_slope >= 0.50
                                        and _height_rise_pct <= max(
                                            _height_max_rise * 2.5,
                                            (_atr_pct_val / 100.0) * 1.5,
                                        )
                                    )
                                    _newborn_strong_rise_exempt = False
                                    indicators["gajami_newborn_strong_rise_exempt"] = int(_newborn_strong_rise_exempt)
                                    _life_ma10_dead_gate_pre = bool(
                                        structure_ok
                                        and dur_5x10_bars is not None
                                        and int(dur_5x10_bars) > 4
                                        and _ma10_slope_ratio <= MA10_SLOPE_DEAD
                                    )
                                    _life_height_exceeded_pre = bool(
                                        structure_ok
                                        and _height_rise_pct > _height_max_rise
                                        and not (
                                            _newborn_ctx
                                            and _is_true_gc_birth
                                            and _height_rise_pct <= (_atr_pct_val / 100.0)
                                        )
                                        and not (
                                            gc_5_10
                                            and _had_pullback
                                            and ma5_slope > 0
                                            and ma10_slope > 0
                                            and spacing_expanding
                                        )
                                        and not _top_zone_trend_alive
                                        and not _newborn_strong_rise_exempt
                                        and not _ranging_top_body_momentum_ok
                                        and not bool(
                                            indicators.get(
                                                "gajami_long_backed_mid_pullback",
                                                0,
                                            )
                                        )
                                    )
                                    _life_trend_alive_pre = bool(
                                        (
                                            _ma5_slope_ratio
                                            >= -SoleusGajamiConstants.MA5_SLOPE_ALIVE_DIP_RATIO
                                            or _is_newborn_here
                                        )
                                        and (
                                            cross_score >= -1.0
                                            or _is_newborn_here
                                        )
                                    )

                                    if _life_ma10_dead_gate_pre:
                                        if regime == MarketRegime.TRENDING_DOWN:
                                            logger.info(
                                                f"🚫 [GAJAMI-LIFECYCLE] {symbol} state=DEAD "
                                                f"reason=MA10_SLOPE_DEAD "
                                                f"rise={_rise_pct:.2%} "
                                                f"dur={dur_5x10_bars} "
                                                f"s10={_ma10_slope_ratio:+.5f} "
                                                f"thr={MA10_SLOPE_DEAD:+.5f} "
                                                f"regime={regime.value if hasattr(regime, 'value') else regime}"
                                            )
                                        else:
                                            logger.debug(
                                                f"🟡 [MA10-SLOPE-RELEASE] {symbol} | "
                                                f"dur={dur_5x10_bars} rise={_rise_pct:.2%} "
                                                f"s10={_ma10_slope_ratio:+.5f} "
                                                f"dead_thr={MA10_SLOPE_DEAD:+.5f} "
                                                f"regime={regime.value if hasattr(regime, 'value') else regime} "
                                                f"→ 비TD birth 진행"
                                            )
                                    elif _life_height_exceeded_pre:
                                        logger.info(
                                            f"🔬 [TOP-RISE-OBS] {symbol} rise초과(관측전용·판정無) dur={dur_5x10_bars} "
                                            f"height={_height_rise_pct:.2%} "
                                            f"> MAX={_height_max_rise:.2%} "
                                            f"anchor={float(_height_anchor_price or 0.0):.4f} "
                                            f"bars={_height_anchor_bars} "
                                            f"exempt={int(_newborn_strong_rise_exempt)}"
                                        )
                                    elif _life_trend_alive_pre:
                                        logger.info(
                                            f"✅ [TREND-ALIVE] {symbol} 추세 생존 | "
                                            f"cross={cross_score:.2f} s5={_ma5_slope_ratio:.5f} "
                                            f"floor={-SoleusGajamiConstants.MA5_SLOPE_ALIVE_DIP_RATIO:.5f} "
                                            f"dur={dur_5x10_bars} rise={_rise_pct:.2%}"
                                        )
                                    else:
                                        logger.info(
                                            f"🔴 [TREND-DEAD] {symbol} 추세 소멸 | "
                                            f"cross={cross_score:.2f} s5={_ma5_slope_ratio:.5f} "
                                            f"floor={-SoleusGajamiConstants.MA5_SLOPE_ALIVE_DIP_RATIO:.5f} "
                                            f"dur={dur_5x10_bars} rise={_rise_pct:.2%}"
                                        )

                                    (
                                        _life_pre_height_state,
                                        _life_pre_height_reason,
                                    ) = _calc_lifecycle_state(
                                        ma5_now=_ma5_now,
                                        ma10_now=_ma10_now,
                                        dur_5x10_bars=dur_5x10_bars,
                                        structure_ok=structure_ok,
                                        ma10_dead_gate=(
                                            _life_ma10_dead_gate_pre
                                        ),
                                        regime_is_td=(
                                            regime == MarketRegime.TRENDING_DOWN
                                        ),
                                        trend_alive=(
                                            _life_trend_alive_pre
                                        ),
                                    )
                                    _life_pre_height_valid = (
                                        _life_pre_height_state == "VALID"
                                    )

                                # 🔒 MA15-ENTRY-FRAGILE: MA10·MA20 압착 + MA5 버퍼 부족
                                # → MA15_EXIT 즉시 발동 위험 구조 차단
                                # MA15 = (MA10+MA20)/2 (MA15_EXIT SSoT 동일)
                                # threshold=0.10% = fast_breach(-0.20%) 기준의 절반
                                _ma20_now = _safe_last_float(ma20)
                                _MA15_FRAGILE_THR = 0.0010  # 0.10%
                                _ma15_now = (
                                    (_ma10_now + _ma20_now) / 2.0
                                    if _ma10_now > 0 and _ma20_now > 0 else 0.0
                                )
                                _mid_stack_width = (
                                    abs(_ma10_now - _ma20_now) / _ma20_now
                                    if _ma10_now > 0 and _ma20_now > 0 else 1.0
                                )
                                _ma5_ma15_buffer = (
                                    (_ma5_now - _ma15_now) / _ma15_now
                                    if _ma5_now > 0 and _ma15_now > 0 else 1.0
                                )
                                _ma5_ma20_buffer = (
                                    (_ma5_now - _ma20_now) / _ma20_now
                                    if _ma5_now > 0 and _ma20_now > 0 else 1.0
                                )
                                _ma15_entry_fragile = bool(
                                    structure_ok
                                    and _life_pre_height_valid
                                    and regime in (MarketRegime.TRENDING_DOWN, MarketRegime.RANGING)
                                    and dur_5x10_bars is not None
                                    and int(dur_5x10_bars) <= SoleusGajamiConstants.NEWBORN_MAX_DUR
                                    and entry_trigger_ok
                                    and _regime_gate_ok
                                    and _mid_stack_width <= _MA15_FRAGILE_THR
                                    and _ma5_ma15_buffer <= _MA15_FRAGILE_THR
                                    and _ma5_ma20_buffer <= _MA15_FRAGILE_THR
                                    and not spacing_expanding
                                )
                                indicators["gajami_ma15_entry_fragile"] = int(_ma15_entry_fragile)
                                logger.debug(
                                    f"[MA15-ENTRY-FRAGILE-AUDIT] {symbol} | "
                                    f"fragile={int(_ma15_entry_fragile)} "
                                    f"mid_stack={_mid_stack_width:.3%} "
                                    f"ma5_ma15_buf={_ma5_ma15_buffer:.3%} "
                                    f"ma5_ma20_buf={_ma5_ma20_buffer:.3%} "
                                    f"expand={int(spacing_expanding)} "
                                    f"life={_life_pre_height_state}"
                                )

                                _dur_now = int(dur_5x10_bars) if dur_5x10_bars is not None else 999
                                _birth_height_ok = bool(
                                    structure_ok
                                    and _newborn_ctx
                                    and _birth_tlc_ok
                                    and _gajami_constitution_ok
                                    and entry_trigger_ok
                                    and _regime_gate_ok
                                    and bool(indicators.get("shape_v2_body_growth", False))
                                    and _birth_impulse_ok
                                    and _life_pre_height_valid
                                )
                                indicators["gajami_birth_height_ok"] = bool(_birth_height_ok)

                                if (structure_ok
                                        and _rise_pct < MIN_RISE
                                        and not _birth_height_ok
                                        and not (
                                            (
                                                _dur_now <= SoleusGajamiConstants.NEWBORN_MAX_DUR
                                                and _rise_pct >= 0.0
                                            )
                                            or _verified_birth_traintrack
                                        )
                                        and _life_pre_height_valid):
                                    structure_confirmed = False
                                    if _ma10_now > 0:
                                        ma_gap_ratio = abs(_ma5_now - _ma10_now) / _ma10_now
                                        momentum_confirmed = (
                                            _ma5_slope_ratio > SoleusGajamiConstants.MA5_SLOPE_LIFT_RATIO and
                                            _ma5_slope_ratio > abs(_ma10_slope_ratio)
                                        )
                                        ma10_3bar_rising = False
                                        try:
                                            if df is not None and len(df) >= 14:
                                                ma10_series = df['close'].rolling(10).mean()
                                                ma10_0 = float(ma10_series.iloc[-1])
                                                ma10_1 = float(ma10_series.iloc[-2])
                                                ma10_2 = float(ma10_series.iloc[-3])
                                                ma10_3 = float(ma10_series.iloc[-4])
                                                ma10_3bar_rising = (
                                                    ma10_0 >= ma10_1 and
                                                    ma10_1 >= ma10_2 and
                                                    ma10_2 >= ma10_3
                                                )
                                        except Exception:
                                            ma10_3bar_rising = False
                                        structure_confirmed = (
                                            _gajami_constitution_ok and is_low_area and
                                            ma_gap_ratio < 0.0015 and
                                            momentum_confirmed and
                                            ma10_3bar_rising
                                        )
                                                                                # V2 헌법 PASS 탄생봉(dur≤2) + trigger → TOO_EARLY 면제
                                        _v2_newborn_exempt = bool(
                                            (
                                                bool(indicators.get("gajami_constitution_v2_ok", 0))
                                                and dur_5x10_bars is not None
                                                and int(dur_5x10_bars) <= SoleusGajamiConstants.NEWBORN_MAX_DUR
                                                and entry_trigger_ok
                                            )
                                            or (
                                                ma5_reaccelerating
                                                and dur_5x10_bars is not None
                                                and int(dur_5x10_bars) <= 1
                                                and float(slope_ma120 or 0.0) >= 0.0
                                            )
                                        )                                        
                                _v2_newborn_exempt = False                                        
                                _life_height_too_low_pre = bool(
                                    structure_ok
                                    and _rise_pct < MIN_RISE
                                    and not _birth_height_ok
                                    and not (
                                        (
                                            _dur_now <= SoleusGajamiConstants.NEWBORN_MAX_DUR
                                            and _rise_pct >= 0.0
                                        )
                                        or _verified_birth_traintrack
                                    )
                                    and regime == MarketRegime.TRENDING_DOWN
                                    and not structure_confirmed
                                    and not _quantized_shape_v2_ok
                                    and not _v2_newborn_exempt
                                )

                                if (
                                    structure_ok
                                    and _life_pre_height_valid
                                    and not _life_height_too_low_pre
                                    and int(indicators.get('rally_info', {}).get('rally_duration', 0) or 0) >= 60
                                    and dur_5x10_bars <= SoleusGajamiConstants.NEWBORN_SHAPE_MAX_DUR
                                    and float(indicators.get('pullback_ratio', 0) or 0) < 0.30
                                    and (int(dur_5x10_bars) if dur_5x10_bars is not None else 999) > SoleusGajamiConstants.NEWBORN_MAX_DUR
                                ):
                                    logger.info(
                                        f"🟡 [RALLY-REBORN-GC-OBSERVE] {symbol} Rally 장기 재GC 관측 | "
                                        f"rally={int(indicators.get('rally_info',{}).get('rally_duration',0))}분 "
                                        f"dur={dur_5x10_bars} rise={_rise_pct:.2%} "
                                        f"pullback={float(indicators.get('pullback_ratio', 0) or 0):.2f} | "
                                        f"legacy_writer_removed=1"
                                    )
                        except Exception as e:
                            _life_err = f"{type(e).__name__}:{str(e)[:120]}"
                            _life_calc_error = _life_err
                            logger.exception(
                                f"⚠️ [GAJAMI-LIFECYCLE-ERROR] {symbol} 계산 실패 | "
                                f"err={_life_err} "
                                f"dur={dur_5x10_bars} "
                                f"regime={regime.value if hasattr(regime, 'value') else regime} "
                                f"shape={int(bool(locals().get('_core_shape_ok', False)))} "
                                f"basic={int(bool(locals().get('_gajami_basic_ok', False)))} "
                                f"trig={int(bool(locals().get('entry_trigger_ok', False)))}"
                            )

                        # 🔒 TLC LIFECYCLE SSoT PRE-TD — 중간 소비용 state만 계산
                        try:
                            _life_dur = dur_5x10_bars
                            _life_ma5_now = float(
                                _ma5_now or 0.0
                            )
                            _life_ma10_now = float(
                                _ma10_now or 0.0
                            )
                            _life_s5 = float(
                                _ma5_slope_ratio or 0.0
                            )
                            _life_s10 = float(
                                _ma10_slope_ratio or 0.0
                            )
                            _life_structure_ok = bool(
                                structure_ok
                            )
                            _life_rise = float(
                                _rise_pct or 0.0
                            )
                            (
                                _life_pre_td_state,
                                _life_pre_td_reason,
                            ) = _calc_lifecycle_state(
                                ma5_now=_life_ma5_now,
                                ma10_now=_life_ma10_now,
                                dur_5x10_bars=_life_dur,
                                structure_ok=(
                                    _life_structure_ok
                                ),
                                ma10_dead_gate=(
                                    _life_ma10_dead_gate_pre
                                ),
                                regime_is_td=(
                                    regime
                                    == MarketRegime.TRENDING_DOWN
                                ),
                                trend_alive=(
                                    _life_trend_alive_pre
                                ),
                            )
                            _life_pre_td_valid = (
                                _life_pre_td_state == "VALID"
                            )
                        except Exception as _life_pre_e:
                            _life_pre_error = (
                                f"{type(_life_pre_e).__name__}:"
                                f"{str(_life_pre_e)[:120]}"
                            )
                            _life_pre_td_state = "ANALYSIS_INVALID_ERROR"
                            _life_pre_td_reason = f"ERROR:{_life_pre_error}"
                            _life_pre_td_valid = False
                            indicators["gajami_lifecycle_pre_error"] = (
                                _life_pre_error
                            )
                            logger.exception(
                                f"🛑 [LIFECYCLE-SSOT-PRE-ERROR] {symbol} | "
                                f"{_life_pre_error} → ANALYSIS_INVALID_ERROR"
                            )

                        # ━━ 최종 판정 ━━
                        # 🔒 SSoT: newborn(dur≤2) 진입 시 MA10 생존/비후퇴 필수
                        # _tlc_ma10_slope / _tlc_ma10_prev 는 percent 단위 (로그: -0.098%)
                        # MA10_SLOPE_HALT(-0.00030) * 100.0 = -0.03%
                        _MA10_NEWBORN_DECAY_FLOOR = SoleusGajamiConstants.MA10_SLOPE_HALT * 100.0
                        _MA10_NEWBORN_RETAIN_RATIO = 0.5
                        _MA10_POSITIVE_ABS_FLOOR_ST = SoleusGajamiConstants.MA10_POSITIVE_ABS_FLOOR * 100.0

                        _tlc_ma10_alive = bool(
                            _tlc_ma10_slope >= _MA10_NEWBORN_DECAY_FLOOR
                            and (
                                _tlc_ma10_prev <= 0.0
                                or _tlc_ma10_slope >= _tlc_ma10_prev * _MA10_NEWBORN_RETAIN_RATIO
                                or _tlc_ma10_slope >= _MA10_POSITIVE_ABS_FLOOR_ST
                            )
                        )

                        _is_td_rg_regime = bool(
                            regime in (MarketRegime.TRENDING_DOWN, MarketRegime.RANGING)
                        )
                        _is_newborn_life = bool(
                            dur_5x10_bars is not None
                            and int(dur_5x10_bars) <= SoleusGajamiConstants.NEWBORN_MAX_DUR
                        )
                        # _quantized_with_ma5_pos_ok / _quantized_td_rg_rescue_ok /
                        # _quantized_birth_rescue_ok는 위에서 계산된 값을 재사용한다.

                        # 🆕 GC직후 reacc+ma10_alive 신생 복합 origin (FRESH_REACC_NEWBORN)
                        _fresh_reacc_newborn_ok = bool(
                            ma5_reaccelerating
                            and bool(spacing_expanding)
                            and _birth_impulse_ok
                            and _tlc_ma10_alive
                            and _tlc_ratio_ok
                            and _rise_atr_ratio >= 0.90
                            and dur_5x10_bars is not None
                            and int(dur_5x10_bars) <= 1
                            and float(slope_ma120 or 0.0) >= 0.0
                        )
                        indicators["gajami_fresh_reacc_newborn_ok"] = int(_fresh_reacc_newborn_ok)

                        # TD/RG newborn birth_state 받침(origin) 게이트
                        # 일반 ma5_reaccelerating 단독은 origin 아님.
                        # 단, fresh_reacc_newborn_ok는 로그 검증된 reacc+impulse+TLC+rise_atr 조합 origin으로 인정.
                        # 🔒 [2026-08-03] 파생 birth_recog 의 raw-origin 자격 제거.
                        #  _gajami_birth_recognized(L8893) = (conv_birth_core OR subtick_body_birth)
                        #  인데, subtick_body_birth 는 진짜 DC 없이도 "MA 초밀착+현재봉 몸통"만으로 1이 된다.
                        #  게다가 이 값이 _birth_impulse_ok(L8902)와 여기(origin) 양쪽에 들어가
                        #  아래 BIRTH-STATE-ORIGIN-BLOCK 차단이 구조적으로 절대 발동하지 못했다(자기 무력화).
                        #  실증 ADA 08-03 07:45: true_gc=0 prev_dc=0 prev2_dc=0 pullback=0 인데
                        #   subtick_body_birth=1 → BIRTH 승인 → score 0.940 → 매수 → -0.47%.
                        #   매수 같은 초(07:45:13)에 TD-RG-LATE-VALID-ORIGIN-BLOCK 이 찍혔다.
                        #  ※ conv_birth_core 는 검증된 수렴형 탄생이므로 origin 자격 유지.
                        _birth_state_origin_ok = bool(
                            _is_true_gc_birth
                            or _had_pullback
                            or intrabar_trigger
                            or _verified_birth_traintrack
                            or _trend_reclaim_origin_ok
                            or _strong_momentum_reset_gc_ok
                            or _trend_continuing
                            or bool(indicators.get("gajami_convergence_birth_core", 0))
                            or _fresh_reacc_newborn_ok      # 🆕 GC직후 reacc+ma10_alive 신생
                        )

                        if (
                            _is_td_rg_regime
                            and _is_newborn_life
                            and _birth_impulse_ok
                            and not _birth_state_origin_ok
                        ):
                            logger.info(
                                f"🚫 [BIRTH-STATE-ORIGIN-BLOCK] {symbol} "
                                f"TD/RG newborn 받침 없는 birth_state 차단 | "
                                f"true_gc={int(_is_true_gc_birth)} pullback={int(_had_pullback)} "
                                f"intrabar={int(intrabar_trigger)} verified_train={int(_verified_birth_traintrack)} "
                                f"reacc={int(ma5_reaccelerating)} body_mom={int(_birth_body_momentum_ok)} "
                                f"dur={dur_5x10_bars} "
                                f"ma10_alive={int(_tlc_ma10_alive)} quant_rescue={int(_quantized_birth_rescue_ok)} "
                                f"fresh_reacc={int(_fresh_reacc_newborn_ok)} "
                                f"rise_atr={_rise_atr_ratio:.2f}x "
                                f"momentum_reset={int(_strong_momentum_reset_gc_ok)} trend_cont={int(_trend_continuing)} "
                                # 🔍 [TRECLAIM-ARMS-OBSERVE-0827] ★298
                                f"| treclaim={int(_trend_reclaim_origin_ok)} "
                                f"[{_treclaim_arms_obs}]"
                            )

                        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
                        # 🆕 [PINSET-MEGA] 눌림 실측 SSoT — 가짜 꼭지 newborn 차단
                        # TD/RG 과열 후 무눌림 꼭지 재GC(dur≤2) = 탄생 아님
                        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
                        try:
                            _rise_cur_pct_for_fake_top = float(
                                indicators.get(
                                    "gajami_rise_cur_pct",
                                    float(_rise_cur or 0.0) * 100.0
                                ) or 0.0
                            )
                        except Exception:
                            _rise_cur_pct_for_fake_top = 0.0

                        _over_rallied = bool(
                            float(indicators.get("over_rally_penalty", 1.0) or 1.0) < 1.0
                        )
                        # 🔒 [FAKE-TOP-OVERRALLY-GATE-0808]
                        #   DC→GC가 "진짜"라는 사실과 그 GC가 "새 부모파동의 탄생"이라는
                        #   사실은 다르다. TD/RG 체제에서 이미 과열(over_rallied)된
                        #   부모파동 위에서도 진짜 DC→GC는 얼마든지 재발생하므로,
                        #   _is_true_gc_birth 단독 면제는 그 재발생을 무조건 살려준다.
                        #   실증 PIEVERSE 08-08: 18:03(되밀림 약31%, 과열아님)은 유지,
                        #   18:21/18:24(되밀림 13.5~18.9%, TD/RG 과열)는 면제 취소.
                        #   PATCH 1(rally 180분 상한 제거)이 선행되어야 over_rally_penalty
                        #   자체가 장기 부모파동에서도 계산되므로 이 게이트가 실효를 가진다.
                        # 🔒 [SUBTICK-ORIGIN-DEDUP-0809] subtick_dc_converge_birth=1은
                        # 정의상 _prev2_dc=True를 필수로 하고(L6622), 그 값이 _near_dc_birth
                        # 경로로 이미 _is_true_gc_birth를 True로 만든다(L6643). 즉 이 OR항은
                        # 정상 탄생을 새로 살리는 효과 없이, 바로 위 TD/RG 과열 취소
                        # (not (_is_td_rg_regime and _over_rallied))만 되돌리는 중복 우회로였다.
                        # 🔒 [PARENT-WAVE-ORIGIN-SSOT-0809]
                        # MA5xMA10 재교차(cross_event)는 새 부모파동의 origin이 아니다.
                        # 부모파동 고점권(C국면, 0.02<pullback<=0.20)에서는 true_gc 단독
                        # BIRTH 면제를 금지한다. F1이 이미 계산·저장한 pullback_ratio를
                        # 그대로 재사용 — 신규 지표 없음.
                        # 실증 GRVT 17:33: true_gc=1이지만 17:31 FAKE-TOP-BIRTH-BLOCK이
                        # 잡았던 동일 부모파동(pullback=0.118) 안에서 재교차했을 뿐인데
                        # true_gc 하나로 origin_ok=True가 되어 BLOCK이 풀렸다.
                        _pullback_parent = float(
                            indicators.get('pullback_ratio', 0.0) or 0.0
                        )
                        _parent_wave_top = bool(
                            0.02 < _pullback_parent <= 0.20
                        )
                        _fake_top_origin_ok = bool(
                            (
                                _is_true_gc_birth
                                and not _parent_wave_top
                                and not (_is_td_rg_regime and _over_rallied)
                            )
                            or _verified_birth_traintrack
                        )
                        # 🔒 [LATE-VALID-ORIGIN-STRUCTURAL-ONLY-0809]
                        # dur>2 재진입은 구조 origin만 인정, 즉시성 origin
                        # (trend_continuing/fresh_reacc)·_had_pullback(F1서 폐기)은 제외.
                        _fake_top_birth = bool(
                            _parent_wave_top
                            and _rise_cur_pct_for_fake_top < 0.60
                            and not _fake_top_origin_ok
                        )
                        # 🔧 Phase B: 확정봉 TLC slope-turn 기반 꼭지 확정
                        # MA10 롤오버(+→−) + MA20 비상승만 진짜 꼭지로 분류
                        # 상승 둔화는 관측용만, hard veto 아님
                        _tlc_top_confirmed = bool(
                            _tlc_ma10_prev > 0.0
                            and _tlc_ma10_slope < 0.0
                            and _tlc_ma20_slope <= 0.0
                        )
                        _tlc_slope_decel = bool(
                            _tlc_ma10_slope > 0.0
                            and _tlc_ma10_slope < _tlc_ma10_prev
                        )

                        _ma20_dir = 'UP' if _tlc_ma20_slope > 0 else ('DN' if _tlc_ma20_slope < 0 else 'FLAT')
                        _tlc_decel_ratio = (_tlc_ma10_slope / _tlc_ma10_prev) if abs(_tlc_ma10_prev) > 1e-6 else 0.0

                        _tlc_atr_c = float(indicators.get("atr_pct", 0.0) or 0.0)
                        _tlc_norm7 = (_tlc_ma10_slope_w7 / _tlc_atr_c) if _tlc_atr_c > 1e-9 else 0.0
                        _tlc_norm5 = (_tlc_ma10_slope / _tlc_atr_c) if _tlc_atr_c > 1e-9 else 0.0
                        try:
                            _tlc_ma10_s3 = mas.get("ma10")
                            if _tlc_ma10_s3 is not None and len(_tlc_ma10_s3) >= 5:
                                _tlc_m10_c3 = float(_tlc_ma10_s3.iloc[-2])
                                _tlc_m10_p3 = float(_tlc_ma10_s3.iloc[-5])
                                _tlc_slope3 = ((_tlc_m10_c3 - _tlc_m10_p3) / _tlc_m10_p3 * 100.0) if _tlc_m10_p3 > 0 else 0.0
                            else:
                                _tlc_slope3 = 0.0
                        except Exception:
                            _tlc_slope3 = 0.0
                        _tlc_norm3 = (_tlc_slope3 / _tlc_atr_c) if _tlc_atr_c > 1e-9 else 0.0
                        # 🔒 [2026-08-08] 부모파동(W7) 반영 — 자식파동(W3) 단독 GROWTH 승격 금지.
                        #   판정 로그는 GROWTH/NEUTRAL 확정 시점(phase 분기 안)에서만 찍는다 —
                        #   여기서 fast/trend만으로 로그를 찍으면 ratio_ok=True인 경우
                        #   실제로는 GROWTH인데 "차단했다"고 잘못 기록되는 정합 오류가 있었다.
                        _tlc_trend_fast = 1 if _tlc_norm3 > 0.2 else (-1 if _tlc_norm3 < -0.1 else 0)
                        _tlc_trend_slow = 1 if _tlc_norm7 > 0.2 else (-1 if _tlc_norm7 < -0.1 else 0)
                        _tlc_trend = (
                            -1 if (_tlc_trend_fast < 0 or _tlc_trend_slow < 0)
                            else (1 if _tlc_trend_fast > 0 else 0)
                        )

                        # 🔒 [TLC-ACTIVE-BIRTH-0809 / GRADED-0810]
                        # 초판(tslow>=0 AND OR4)은 TD/RG newborn의 65.9%를 차단했고
                        # 차단군(-0.242%)이 통과군(-0.254%)보다 좋았다
                        #   (08-08 +0.022%p / 08-09 +0.001%p, 역방향 2/2)
                        # 차단력의 100%가 _tlc_trend_slow>=0 단독에서 나왔다 — 빼면 65.9%→0.9%.
                        # 그 축은 두 날 모두 거꾸로였다: W7 하락중 -0.242% vs 비하락 -0.255%.
                        # 두 날 모두 정당한 유일 조합만 남긴다 — 차단 3.6% · Δ-0.034%p
                        # forward 9,281셀 · 다음봉 open 진입 · TP+0.6/SL-0.3/20봉 · 비용 0.2548% 차감
                        # ⚠️ trend_slow(W7 부모파동)는 진입 조건으로 되살리지 말 것 —
                        #   AND 지배항이라 게이트 전체를 뒤집는다.
                        _tlc_birth_active = bool(
                            regime not in (MarketRegime.TRENDING_DOWN, MarketRegime.RANGING)
                            or _tlc_ratio_ok
                            or bool(indicators.get("shape_v2_body_growth", False))
                        )

                        _tlc_rg_whipsaw = bool(
                            regime == MarketRegime.RANGING
                            and 0.0 <= _tlc_norm3 < 0.4
                            and not is_low_area
                        )
                        _tlc_deep_down = bool(_tlc_norm3 < -0.4)
                        indicators["tlc_rg_whipsaw"] = int(_tlc_rg_whipsaw)
                        indicators["tlc_deep_down"] = int(_tlc_deep_down)

                        logger.info(
                            f"🧭 [TLC-DIAG] {symbol} | regime={regime.value if hasattr(regime,'value') else regime} ma20={_ma20_dir} "
                            f"| W5 m10={_tlc_ma10_slope:+.4f}/p{_tlc_ma10_prev:+.4f} m20={_tlc_ma20_slope:+.4f} r={_tlc_decel_ratio:+.2f} "
                            f"| W7 m10={_tlc_ma10_slope_w7:+.4f}/p{_tlc_ma10_prev_w7:+.4f} m20={_tlc_ma20_slope_w7:+.4f} "
                            f"ratiok={int(_tlc_ratio_ok)} topconf={int(_tlc_top_confirmed)} decel={int(_tlc_slope_decel)} "
                            f"| norm7={_tlc_norm7:+.2f} norm3={_tlc_norm3:+.2f} trend={_tlc_trend} "
                            f"whip={int(_tlc_rg_whipsaw)} deep={int(_tlc_deep_down)}"
                        )

                        # [TLC-REMOVE-0821] 구 TLC phase(BIRTH/GROWTH/MATURE/AGING) 생산부 철거.
                        # 능동형 TLC(cycle_phase_shadow)로 완전 분리 대체.
                        # 🆕 좁은박스 꼭지 + JANGDAE ok=0 가짜 birth 차단
                        # 🔧 box_pos/converge_top 추측 → 확정봉 TLC 롤오버로 판별축 교체
                        # 🔒 [2026-08-08] 가짜 꼭지 newborn 차단기 死코드 복구.
                        # 이미 계산 중인 rally/pullback/origin/TLC 값만 소비한다.
                        # 🔒 [PARENT-WAVE-TOP-0809] over_rallied(곱셈 penalty 기반) +
                        # not _had_pullback(국소 MA5 3봉 눌림, L7523)은 부모 파동
                        # 되밀림과 무관해 방향이 거꾸로였다. detect_rally_pattern()이
                        # 이미 계산·저장한 pullback_ratio(L1872, 저점→고점 구조)를
                        # BIRTH 판정에 처음 연결한다. 신규 지표 없음.                        
                        indicators["gajami_fake_top_birth"] = int(_fake_top_birth)
                        indicators["gajami_fake_top_birth_observe"] = int(_fake_top_birth)
                        indicators["gajami_fake_top_birth_rise_cur_pct"] = float(_rise_cur_pct_for_fake_top)
                        if _fake_top_birth:
                            logger.info(
                                f"👁️ [FAKE-TOP-BIRTH-OBSERVE] {symbol} "
                                f"부모파동 고점권 재교차 관측 | "
                                f"pullback={_pullback_parent:.3f} "
                                f"rally_parent_pos={1.0-_pullback_parent:.3f} "
                                f"rise_cur={_rise_cur_pct_for_fake_top:+.3f}% "
                                f"tlc_top_confirmed={int(_tlc_top_confirmed)} "
                                f"tlc_decel={int(_tlc_slope_decel)} "
                                f"ma10s={_tlc_ma10_slope:+.4f}/prev{_tlc_ma10_prev:+.4f} "
                                f"ma20s={_tlc_ma20_slope:+.4f} "
                                f"true_gc={int(_is_true_gc_birth)} "
                                f"verified_train={int(_verified_birth_traintrack)} "
                                f"converge_dc={int(bool(indicators.get('gajami_subtick_dc_converge_birth', 0)))} "
                                f"regime={regime.value if hasattr(regime, 'value') else regime}"
                            )

                        _birth_state_ok = bool(
                            _birth_tlc_ok
                            and _life_pre_td_valid
                            and dur_5x10_bars is not None
                            and int(dur_5x10_bars) <= SoleusGajamiConstants.NEWBORN_MAX_DUR
                            and _birth_impulse_ok
                            and (
                                _tlc_ma10_alive
                                or (float(_tlc_ma20_slope or 0.0) > 0.0)
                                or _quantized_birth_rescue_ok
                            )
                            and not _fake_top_birth   # 🆕 가짜꼭지 차단
                        )
                        indicators["gajami_birth_state_ok"] = bool(_birth_state_ok)

                        _is_birth_ok = bool(
                            _birth_state_ok
                            or (
                                _birth_tlc_ok
                                and _tlc_ratio_ok
                                and _life_pre_td_valid
                                and _ma20_reclaim
                                and (regime != MarketRegime.RANGING or _gajami_tlc)
                                and _tlc_ma10_alive
                            )
                        )
                        # 🔧 [BIRTH-OK-ACTIVE-FIELDS-0814] 로그 줄 추가 금지 원칙에 맞춰
                        # 상시 True 상수 4개(top_ma10_ok/top_reset_ok/top_lifecycle_ok/
                        # tlc_height_ok, t5 L9394-9428 확인)와 다른 INFO 로그에 이미
                        # 있는 필드(regime/tlc_gajami/s10·s20)를 빼고, _birth_tlc_ok·
                        # _birth_state_ok를 실제로 결정하는 활성 항목으로 교체한다.
                        # ext60atr은 birth_state 구성항은 아니지만 82% 고점권 편중의
                        # 위치축을 같은 봉에서 조인하기 위해 포함한다.
                        # logger.debug → logger.info.
                        logger.info(
                            f"[BIRTH-OK] {symbol} | dur={dur_5x10_bars} "
                            f"birth_ok={int(_is_birth_ok)} "
                            f"birth_state={int(_birth_state_ok)} "
                            f"tlc_ok={int(_birth_tlc_ok)} "
                            f"const={int(_gajami_constitution_ok)} "
                            f"trig={int(entry_trigger_ok)} "
                            f"peak={int(_peak_block)} "
                            f"ma5alive={int(_ma5_trend_alive)} "
                            f"both_down={int(_both_down)} "
                            f"mid_alive={int(_birth_mid_alive)} "
                            f"td_lowbox={int(_td_ma20_newborn_lowbox_ok)} "
                            f"train_life={int(_birth_traintrack_life_ok)} "
                            f"rebound={int(_td_ma20_rebound_ok)} "
                            f"rg_ma20={int(_ranging_ma20_alive_ok)} "
                            f"life_pre={int(_life_pre_td_valid)} "
                            f"impulse={int(_birth_impulse_ok)} "
                            f"ma10alive={int(_tlc_ma10_alive)} "
                            f"ma20pos={int(float(_tlc_ma20_slope or 0.0) > 0.0)} "
                            f"quant={int(_quantized_birth_rescue_ok)} "
                            f"faketop={int(_fake_top_birth)}"
                        )
                        _newborn_strong_impulse_ok = bool(
                            indicators.get("gajami_newborn_strong_rise_exempt", False)
                        )

                        _cur_v = float(current_price) if current_price is not None else 0.0
                        _ma5_v = float(ma5) if ma5 is not None else 0.0

                        # 🔒 TD/RG TOP newborn은 가격이 MA5 위/동일인 것만으로는 부족
                        # 박스 TOP에서는 MA5가 명확히 고개를 들어야 reclaim 인정
                        _TD_RG_TOP_RECLAIM_MA5_SLOPE_MIN = 0.001

                        _td_rg_top_newborn_zone = False
                        _td_rg_top_reclaim_slope_ok = bool(
                            not _td_rg_top_newborn_zone
                            or ma5_slope >= _TD_RG_TOP_RECLAIM_MA5_SLOPE_MIN
                        )

                        _td_rg_top_price_ok = bool(
                            not _td_rg_top_newborn_zone
                            or (
                                _ma5_v > 0
                                and _cur_v >= _ma5_v
                                and _td_rg_top_reclaim_slope_ok
                            )
                        )

                        # 🆕 TD/RG BIRTH RECLAIM: impulse 없이도 형상·구조가 충분한 탄생봉 구제
                        # 조건: TD/RG + dur<=2 + 형상OK + quantized + cur>=MA5 + MA20 회복
                        _td_rg_birth_reclaim_ok = bool(
                            _is_td_rg_regime
                            and _is_newborn_life
                            and _td_rg_top_price_ok
                            and _quantized_shape_v2_ok
                            and _quantized_with_ma5_pos_ok
                            and entry_trigger_ok
                            and _gajami_constitution_ok
                            and _tlc_ma20_slope >= 0.0
                        )

                        _over_rally_hard_block = bool(indicators.get('over_rally_hard_block', 0))

                        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
                        # 🟢 TD/RG FRESH-RECLAIM OVERRALLY PINSET
                        # - over_rally 제거 아님
                        # - expand=1 + fresh(dur<=2) + birth_reclaim=1 탄생봉만 격하
                        # - WLD 17:33형 구제, INJ/SOL expand=0류 차단 유지
                        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
                        _td_rg_fresh_reclaim_over_rally_ok = bool(
                            _over_rally_hard_block
                            and _is_td_rg_regime
                            and _is_newborn_life
                            and dur_5x10_bars is not None
                            and int(dur_5x10_bars) <= SoleusGajamiConstants.NEWBORN_MAX_DUR
                            and bool(spacing_expanding)
                            and _td_rg_birth_reclaim_ok
                            and _gajami_constitution_ok
                            and entry_trigger_ok
                            and _life_pre_td_valid
                        )
                        _td_rg_fresh_abs_thrust_birth_ok = bool(
                            _over_rally_hard_block
                            and _is_td_rg_regime
                            and _is_newborn_life
                            and dur_5x10_bars is not None
                            and int(dur_5x10_bars) <= SoleusGajamiConstants.NEWBORN_MAX_DUR
                            and _gajami_constitution_ok
                            and _life_pre_td_valid
                            and _rise_cur >= 0.006
                            and ma5_slope >= SoleusGajamiConstants.MA5_SLOPE_CORE_MIN
                            and float(slope_ma20_df or 0.0) >= 0.0
                            and float(ma5 or 0.0) > float(ma10 or 0.0) > 0.0
                        )
                        indicators["gajami_td_rg_fresh_abs_thrust_birth_ok"] = int(
                            _td_rg_fresh_abs_thrust_birth_ok
                        )

                        _td_rg_over_rally_bypass_ok = bool(
                            (
                                _td_rg_fresh_reclaim_over_rally_ok
                                or _td_rg_fresh_abs_thrust_birth_ok
                            )
                            and not _fake_top_birth   # 🆕 가짜꼭지는 bypass도 차단
                        )
                        _td_rg_over_rally_bypass_ok = False

                        if _td_rg_fresh_abs_thrust_birth_ok:
                            logger.info(
                                f"🟢 [TD-RG-FRESH-ABS-THRUST-BIRTH-ALLOW] {symbol} | "
                                f"dur={dur_5x10_bars} "
                                f"rise_cur={_rise_cur*100.0:.3f}% "
                                f"expand={int(bool(spacing_expanding))} "
                                f"over_rally={int(_over_rally_hard_block)} "
                                f"constitution={int(_gajami_constitution_ok)} "
                                f"life={_life_pre_td_state} "
                                f"s5={ma5_slope:+.5f} "
                                f"s20={float(slope_ma20_df or 0.0):+.5f}"
                            )

                        if _td_rg_fresh_reclaim_over_rally_ok:
                            logger.info(
                                f"🟢 [TD-RG-FRESH-RECLAIM-OVERRALLY-ALLOW] {symbol} | "
                                f"dur={dur_5x10_bars} "
                                f"expand={int(bool(spacing_expanding))} "
                                f"birth_reclaim={int(_td_rg_birth_reclaim_ok)} "
                                f"over_rally={int(_over_rally_hard_block)} "
                                f"constitution={int(_gajami_constitution_ok)} "
                                f"trigger={int(entry_trigger_ok)} "
                                f"life={_life_pre_td_state} "
                                f"tlc=({_tlc_ma10_slope:+.3f},{_tlc_ma20_slope:+.3f})"
                            )

                        _td_rg_mid_shallow_alive_ok = bool(
                            (_birth_mid_shallow_alive_ok and _tlc_ratio_ok)
                            and (
                                not _over_rally_hard_block
                                or _td_rg_over_rally_bypass_ok
                            )
                        )
                        _convergence_birth_state_ok = bool(
                            bool(indicators.get("gajami_convergence_birth_core", 0))
                            and _birth_state_ok
                            and _birth_impulse_ok
                        )
                        _td_rg_newborn_life_quality_ok = bool(
                            _is_td_rg_regime
                            and _is_newborn_life
                            and (
                                not _over_rally_hard_block
                                or _td_rg_over_rally_bypass_ok
                            )
                            and _td_rg_top_price_ok
                            and (
                                _birth_state_ok
                                or _newborn_tlc_alive_ok
                                or (_birth_impulse_ok and _tlc_ma10_alive and _tlc_ratio_ok)
                                or _quantized_td_rg_rescue_ok
                                or _newborn_strong_impulse_ok
                                or _td_rg_birth_reclaim_ok
                                or _td_rg_fresh_reclaim_over_rally_ok
                                or _td_rg_fresh_abs_thrust_birth_ok
                                or (
                                    bool(indicators.get("gajami_constitution_v2_ok", 0))
                                    and _birth_impulse_ok
                                    and _tlc_ma10_alive
                                    and _is_newborn_life
                                )
                            )
                        )
                        indicators["gajami_td_rg_newborn_life_quality_ok"] = int(_td_rg_newborn_life_quality_ok)

                        _cur_below_ma5_strict = int(_cur_v < _ma5_v) if _ma5_v > 0 and current_price is not None else -1

                        if _is_td_rg_regime and _is_newborn_life:
                            logger.info(
                                f"🧬 [TD-RG-NEWBORN-LIFE] {symbol} | "
                                f"ok={int(_td_rg_newborn_life_quality_ok)} "
                                f"regime={regime.value if hasattr(regime, 'value') else regime} "
                                f"dur={dur_5x10_bars} "
                                f"birth_state={int(_birth_state_ok)} "
                                f"impulse={int(_birth_impulse_ok)} "
                                f"ma10_alive={int(_tlc_ma10_alive)} "
                                f"tlc_ratio_ok={int(_tlc_ratio_ok)} "
                                f"conv_state={int(_convergence_birth_state_ok)} "
                                f"newborn_tlc_alive={int(_newborn_tlc_alive_ok)} "
                                f"tlc_ma5_slope={_tlc_ma5_slope:+.4f}% "
                                f"newborn_strong={int(_newborn_strong_impulse_ok)} "
                                f"quantized={int(_quantized_shape_v2_ok)} "
                                f"quant_ma5_pos={int(_quantized_with_ma5_pos_ok)} "
                                f"quant_rescue={int(_quantized_td_rg_rescue_ok)} "
                                f"rise={_rise_pct:.2%} "
                                f"top_price_ok={int(_td_rg_top_price_ok)} "
                                f"top_slope_ok={int(_td_rg_top_reclaim_slope_ok)} "
                                f"cur_below_ma5_strict={_cur_below_ma5_strict} "
                                f"birth_reclaim={int(_td_rg_birth_reclaim_ok)} "
                                f"over_rally_block={int(_over_rally_hard_block)} "
                                f"cur={_cur_v:.4f} ma5={_ma5_v:.4f}"
                            )

                        _newborn_birth_state_observe = bool(
                            not bool(indicators.get("gajami_struct_uptrend_cont", 0))
                            and (
                                (
                                    not (_is_td_rg_regime and _is_newborn_life)
                                    and dur_5x10_bars is not None
                                    and int(dur_5x10_bars) <= SoleusGajamiConstants.NEWBORN_MAX_DUR
                                    and not _is_birth_ok
                                    and not (
                                        _tlc_ma10_alive
                                        or _quantized_shape_v2_ok
                                        or _birth_mid_shallow_alive_ok
                                    )
                                )
                                or (
                                    _is_td_rg_regime
                                    and _is_newborn_life
                                    and not _td_rg_newborn_life_quality_ok
                                    and not _td_rg_mid_shallow_alive_ok
                                )
                            )
                        )
                        
                        # 🔒 TLC LIFECYCLE SSoT FINAL — 최종 객체 1회 생성
                        try:
                            # 🔒 [2026-08-04 E9~E11] L10693 결과 재사용 — 중복 호출 제거.
                            #  L10693 과 인자 7개가 전부 동일 변수이고 그 사이 재대입이 0건이며
                            #  _calc_lifecycle_state(L501)는 순수 함수다.
                            #  실측 대조 n=7,015 일치율 100.00% (08-03~04 로그).
                            #  L10693 의 except 경로(ANALYSIS_INVALID_ERROR)도 그대로 전파된다
                            #  — 해당 경로는 3일간 0건.
                            _life_ssot_state = _life_pre_td_state
                            _life_ssot_reason = _life_pre_td_reason

                            if _life_calc_error is not None:
                                _life_ssot_state = "ANALYSIS_INVALID_ERROR"
                                _life_ssot_reason = f"ERROR:{_life_calc_error}"
                                indicators["gajami_lifecycle_calc_error"] = (
                                    _life_calc_error
                                )
                                logger.error(
                                    f"🛑 [LIFECYCLE-CALC-INVALID] {symbol} | "
                                    f"{_life_calc_error} "
                                    f"→ ANALYSIS_INVALID_ERROR"
                                )
                            # 능동형 TLC 관측값은 lifecycle state를 재판정하지 않는다.

                            _life_null_output = bool(
                                _life_calc_error is not None
                                or _life_ssot_reason in (
                                    "DEAD_CROSS",
                                    "NO_GOLDEN",
                                    "INIT",
                                )
                            )
                            _life_birth_price_out = (
                                None
                                if _life_null_output
                                else _birth_price
                            )
                            _life_rise_pct_out = (
                                None
                                if _life_null_output
                                else _rise_pct
                            )
                            _life_height_anchor_price_out = (
                                None
                                if _life_null_output
                                else _height_anchor_price
                            )
                            _life_height_rise_pct_out = (
                                None
                                if _life_null_output
                                else _height_rise_pct
                            )
                            _life_height_max_rise_out = (
                                None
                                if _life_null_output
                                else _height_max_rise
                            )
                            try:
                                _life_dur_out = (
                                    int(_life_dur)
                                    if _life_dur is not None
                                    else 0
                                    if _life_ssot_state == "VALID"
                                    else 999
                                )
                            except (TypeError, ValueError):
                                _life_dur_out = 999
                            gajami_lifecycle = {
                                "state": _life_ssot_state,
                                "birth_price": _life_birth_price_out,
                                "rise_pct": _life_rise_pct_out,
                                "height_anchor_price": _life_height_anchor_price_out,
                                "height_anchor_bars": int(_height_anchor_bars),
                                "height_rise_pct": _life_height_rise_pct_out,
                                "height_max_rise": _life_height_max_rise_out,
                                "death_reason": (
                                    None
                                    if _life_ssot_reason == "NONE"
                                    else _life_ssot_reason
                                ),
                                "dur": _life_dur_out,
                            }

                            indicators[
                                "gajami_lifecycle_ssot_shadow_state"
                            ] = _life_ssot_state
                            indicators[
                                "gajami_lifecycle_ssot_shadow_reason"
                            ] = _life_ssot_reason
                            indicators[
                                "gajami_lifecycle_ssot_shadow_match"
                            ] = -1
                            indicators[
                                "gajami_lifecycle_ssot_mode"
                            ] = "PRIMARY"

                            _life_final_bar = str(
                                indicators.get(
                                    "current_candle_time"
                                )
                                or (
                                    df.index[-1]
                                    if (
                                        df is not None
                                        and len(df) > 0
                                    )
                                    else "NA"
                                )
                            )[:16]
                            _life_final_key = (
                                timeframe,
                                symbol,
                            )
                            _life_final_sig = (
                                _life_final_bar,
                                _life_ssot_state,
                                _life_ssot_reason,
                            )
                            _life_final_seen = getattr(
                                TechnicalIndicators,
                                "_lifecycle_ssot_final_seen",
                                {},
                            )
                            if (
                                _life_final_seen.get(
                                    _life_final_key
                                )
                                != _life_final_sig
                            ):
                                _life_final_seen[
                                    _life_final_key
                                ] = _life_final_sig
                                setattr(
                                    TechnicalIndicators,
                                    "_lifecycle_ssot_final_seen",
                                    _life_final_seen,
                                )
                                logger.info(
                                    f"🔒 [LIFECYCLE-SSOT-FINAL] {symbol} | "
                                    f"tf={timeframe} "
                                    f"bar={_life_final_bar} "
                                    f"regime={regime.value if hasattr(regime, 'value') else regime} "
                                    f"state={_life_ssot_state}/{_life_ssot_reason} | "
                                    f"dur={_life_dur_out} "
                                    f"structure={int(_life_structure_ok)} "
                                    f"ma10_dead={int(_life_ma10_dead_gate_pre)} "
                                    f"height_hi={int(_life_height_exceeded_pre)} "
                                    # 🔒 [TLC-HEIGHT-VALUE-OBSERVE-0830] ★439
                                    # height_hi는 판정 비트만 남기고 값이 없어
                                    # "통과분이 몇 %였나"를 잴 수 없었다(★426과 동일 계열).
                                    # 실측: [TOP-RISE-OBS] 발화가 2일 1회뿐이라
                                    # MAX=5.0% 밴드의 분포가 전혀 관측되지 않는다.
                                    # indicators 경유 — L10503~10516에서 이미 export된 SSoT.
                                    f"hrise={safe_extract_scalar(indicators.get('tlc_height_rise_pct'), -9.0):+.4f} "
                                    f"hmax={safe_extract_scalar(indicators.get('tlc_height_max_rise'), -1.0):.4f} "
                                    f"hanc={safe_extract_scalar(indicators.get('tlc_height_anchor_price'), 0.0):.6g} "
                                    f"hbars={int(safe_extract_scalar(indicators.get('tlc_height_anchor_bars'), -1))} "
                                    f"hready={int(safe_extract_scalar(indicators.get('tlc_height_anchor_ready'), 0))} "
                                    f"height_lo_obs={int(_life_height_too_low_pre)} "
                                    f"trend_alive_obs={int(_life_trend_alive_pre)} "
                                    f"trend_alive_role=OBS_ONLY"
                                )
                        except NameError as _life_apply_e:
                            _life_apply_error = (
                                f"NameError:"
                                f"{getattr(_life_apply_e, 'name', None) or str(_life_apply_e)[:80]}"
                            )
                            indicators["gajami_lifecycle_apply_error"] = (
                                _life_apply_error
                            )
                            logger.error(
                                f"🛑 [LIFECYCLE-SSOT-APPLY-ERROR] {symbol} | "
                                f"tf={timeframe} "
                                f"bar={str(indicators.get('current_candle_time') or 'NA')[:16]} | "
                                f"{_life_apply_error} "
                                f"→ ANALYSIS_INVALID_ERROR"
                            )
                            gajami_lifecycle = {
                                "state": "ANALYSIS_INVALID_ERROR",
                                "birth_price": None,
                                "rise_pct": None,
                                "death_reason": f"ERROR:{_life_apply_error}",
                                "dur": 999,
                            }
                        except Exception as _life_apply_e:
                            _life_apply_error = (
                                f"{type(_life_apply_e).__name__}:"
                                f"{str(_life_apply_e)[:120]}"
                            )
                            indicators["gajami_lifecycle_apply_error"] = (
                                _life_apply_error
                            )
                            logger.exception(
                                f"🛑 [LIFECYCLE-SSOT-APPLY-ERROR] {symbol} | "
                                f"{_life_apply_error} "
                                f"→ ANALYSIS_INVALID_ERROR"
                            )
                            gajami_lifecycle = {
                                "state": "ANALYSIS_INVALID_ERROR",
                                "birth_price": None,
                                "rise_pct": None,
                                "death_reason": f"ERROR:{_life_apply_error}",
                                "dur": 999,
                            }

                        # [TLC-REMOVE-0821] 구 TLC phase 소비회선 철거.
                        # 소비처는 능동형 cycle_phase_shadow로 교체됨.

                        # 🔒 [AUTH-REMOVE-0817] BIRTH 양성 판결권(AUTH) 회선 철거.
                        #    AUTH-OFF-0816 이후 발화 0건. 근거: BYPASS-28-VERDICT-0817.md §3-A1
                        indicators["gajami_fake_top_birth_block"] = int(_fake_top_birth)
                        indicators["gajami_newborn_birth_state_observe"] = int(
                            _newborn_birth_state_observe
                        )

                        if _newborn_birth_state_observe:
                            logger.info(
                                f"👁️ [NEWBORN-BIRTH-STATE-OBSERVE] {symbol} | "
                                f"regime={regime.value if hasattr(regime, 'value') else regime} "
                                f"dur={dur_5x10_bars} birth_ok={_is_birth_ok} "
                                f"birth_state={_birth_state_ok} impulse={_birth_impulse_ok} "
                                f"body_mom={int(_birth_body_momentum_ok)} "
                                f"mid_shallow={int(_td_rg_mid_shallow_alive_ok)} "
                                f"mid_ma_slope=({_tlc_ma10_slope:.3f},{_tlc_ma20_slope:.3f})"
                            )

                        # 🔒 [LEGACY-TLC-AUTHORITY-SPLIT-0827]
                        # 아래 두 값은 Active TLC(parent/leg/phase/det14)가 아니라
                        # 확정봉 MA 기반 legacy 축이다. 08-27에는 hard 권한을 격하했으나,
                        # 08-28 [TLC-HARDVETO-RESTORE-0828]에서 _tlc_top_dead의
                        # LIFECYCLE_ENTRY 차단권을 명시적으로 복원했다.
                        # 현재 계약: raw/export는 관측용으로 보존하고, _tlc_top_dead만
                        # 아래 _lifecycle_entry_ok에서 직접 소비한다.
                        _tlc_top_dead = bool(
                            _tlc_ma10_slope < 0.0 and _tlc_ma20_slope <= 0.0
                        )
                        _tlc_newborn_inactive = bool(
                            _is_td_rg_regime
                            and _is_newborn_life
                            and not _tlc_birth_active
                        )
                        # 🔒 [TLC-SSOT-DEMOTE-0827] §186-O의 08-27 처방 귀속 기록.
                        # "_lifecycle_entry_ok 이중소비 제거"는 08-28 RESTORE로 철회된
                        # 과거 계약이며 현재 동작을 설명하지 않는다.
                        _legacy_tlc_observe_raw = bool(
                            _tlc_top_dead or _tlc_newborn_inactive
                        )
                        # 하위 호환용 raw key는 과거 산식(FAKE_TOP 포함)을 그대로 보존한다.
                        # raw key 자체는 현재 판정 소비처가 없고, 복원된 직접 소비는
                        # 아래 _lifecycle_entry_ok의 `and not _tlc_top_dead` 한 곳이다.
                        _tlc_hard_veto_raw = bool(
                            _legacy_tlc_observe_raw or _fake_top_birth
                        )
                        indicators["gajami_tlc_hard_veto_raw"] = int(_tlc_hard_veto_raw)
                        # 🔒 [TLC-HARDVETO-RESTORE-0828]
                        # [TLC-SSOT-DEMOTE-0827]은 사장님 지시("J2/J3/J4를 막는 J1을
                        # 중립으로")를 ★판사 무관 전역 해제로 잘못 구현했다.
                        # 결과 : 08-28 J1 단독 진입 23건(전체 70%) 승률 ★13.0%
                        #        평균 −0.640% · J2 50%(+0.177%) · J3 50%(+0.527%)
                        # 이 지점(L11869)은 판사 계산(L13400대)보다 앞이라
                        # J2/J3/J4 조건을 쓸 수 없다 → ★원상복구가 맞다.
                        # 판사 면제는 t6 [J1-GATE-JUDGE-NEUTRAL-0826]에서 별도 처리한다.
                        indicators["gajami_tlc_hard_veto"] = int(_tlc_hard_veto_raw)
                        indicators["gajami_legacy_tlc_observe_raw"] = int(
                            _legacy_tlc_observe_raw
                        )
                        indicators["gajami_tlc_top_dead_observe"] = int(_tlc_top_dead)
                        indicators["gajami_tlc_newborn_inactive"] = int(_tlc_newborn_inactive)
                        _lifecycle_entry_ok = (
                            _regime_gate_ok
                            and gajami_lifecycle.get("state") == "VALID"
                            # 🔒 [TLC-HARDVETO-RESTORE-0828] 08-27 제거분 복원
                            and not _tlc_top_dead
                            and dur_5x10_bars is not None
                            and (
                                int(dur_5x10_bars) <= RECENT_GC_MAX_BARS
                                or _trend_continuing
                            )
                        )

                        # 🔒 [CONSTITUTION-NO-OVERRIDE-0817]
                        # 헌법 False는 BIRTH/TLC/AUTH 어떤 긍정 조건도 뒤집지 않는다.
                        _gajami_constitution_effective_ok = bool(
                            _gajami_constitution_ok
                        )

                        # 🔒 TD/RG newborn 받침(origin) 필수 — 내부 OR-우회 전부를 외곽 AND 1곳으로 덮음
                        # origin = DC구조/눌림/현재봉트리거/검증된기차길/검증된 fresh_reacc_newborn
                        # mid_shallow·일반 reacc 단독은 origin 아님
                        _td_rg_newborn_origin_required_ok = bool(
                            not (_is_td_rg_regime and _is_newborn_life)
                            or _birth_state_origin_ok
                            or _td_rg_over_rally_bypass_ok
                        )

                        # 🔒 [TOP-FLAT-RISEATR-PURGE-0828]
                        # TOP_FLAT의 0<=rise_atr<0.20 단독 veto를 제거한다.
                        # 08-28 실측 1,984건 중 1,817건(91.6%)이 이 항으로 발화해
                        # 막 반등한 TD/RG newborn까지 TOP으로 오분류했다.
                        # `_parent_wave_top`은 Active-TLC 부모 위치가 아니라 기존
                        # pullback_ratio(0.02, 0.20] 기반 legacy top flag이며 그대로 유지한다.
                        _top_flat_birth_raw = bool(
                            _is_newborn_life
                            and not spacing_expanding
                            and not _neg_rise_effective_block
                            and not bool(indicators.get("gajami_struct_uptrend_cont", 0))
                            and not _ranging_top_body_momentum_ok
                            and not _is_birth_veto_exempt
                            and _is_td_rg_regime
                            and _parent_wave_top
                            and _neg_rise_atr_soft_ok
                        )

                        # 🔒 [TOP-FLAT-BIRTH-FRESH-CROSS-OWNER-0829] ★413
                        # 전전 확정봉이 DC인 실제 fresh DC→GC birth는 TOP_FLAT의
                        # "오래된 부모파동 고점 재교차" 소유 대상이 아니다.
                        # PASS 우회가 아니라 TOP_FLAT 단독 오분류 권한만 회수한다.
                        _top_flat_fresh_cross = bool(
                            _top_flat_birth_raw
                            and _prev2_dc
                        )
                        _top_flat_birth = bool(
                            _top_flat_birth_raw
                            and not _top_flat_fresh_cross
                        )

                        indicators["gajami_top_flat_birth_raw"] = int(
                            _top_flat_birth_raw
                        )
                        indicators["gajami_top_flat_fresh_cross"] = int(
                            _top_flat_fresh_cross
                        )
                        indicators["gajami_top_flat_birth_block"] = int(_top_flat_birth)

                        if _top_flat_fresh_cross:
                            logger.info(
                                f"🟢 [TOP-FLAT-FRESH-CROSS-RELEASE] {symbol} "
                                f"fresh DC→GC는 TOP_FLAT 비대상 | "
                                f"regime={regime.value if hasattr(regime, 'value') else regime} "
                                f"newborn={int(_is_newborn_life)} "
                                f"prev2_dc={int(_prev2_dc)} "
                                f"parent_top={int(_parent_wave_top)}"
                            )

                        if _top_flat_birth:
                            logger.info(
                                f"🚫 [TOP-FLAT-BIRTH-BLOCK] {symbol} flat/low-rise newborn 차단 | "
                                f"regime={regime.value if hasattr(regime, 'value') else regime} "
                                f"newborn={int(_is_newborn_life)} "
                                f"spacing_expanding={int(spacing_expanding)} "
                                f"rise_atr={_rise_atr_ratio:.2f}x "
                                f"neg_rise_block={int(_neg_rise_effective_block)}"
                            )
                        # ma10꺾임 + lower-high(strict<) + ER BAD
                        # 06:16/06:18: swing_ref=0 → whipsaw=False (보호)
                        # 09:22: 794<795 + ER0.04 → whipsaw=True (차단)
                        # 09:52: swing_equal + ER0.57 → whipsaw=False (장기 보호)
                        _er10 = float(indicators.get("er_10", 0.0) or 0.0)
                        _td_rg_top_whipsaw = False
                        indicators["gajami_td_rg_top_whipsaw"] = int(_td_rg_top_whipsaw)

                        _td_rg_top_newborn_rise_cur_pct = float(
                            indicators.get("gajami_rise_cur_pct", 0.0) or 0.0
                        )
                        _TD_RG_TOP_NEWBORN_RISE_CUR_FLOOR_PCT = 0.30

                        _td_rg_top_newborn_followthrough_gate_ok = bool(
                            not _td_rg_top_newborn_zone
                            or _is_birth_veto_exempt
                            or (
                                _td_rg_top_newborn_rise_cur_pct
                                >= _TD_RG_TOP_NEWBORN_RISE_CUR_FLOOR_PCT
                            )
                            or bool(indicators.get("gajami_neg_rise_atr_soft_ok", 0))
                            or bool(indicators.get("gajami_tlc_alive_at_top", 0))  # struct→TLC(좋은V회복 통과·SEI차단)
                        )
                        indicators["gajami_td_rg_top_newborn_followthrough_gate_ok"] = int(
                            _td_rg_top_newborn_followthrough_gate_ok
                        )
                        indicators["gajami_td_rg_top_newborn_followthrough_gate_block"] = int(
                            not _td_rg_top_newborn_followthrough_gate_ok
                        )
                        if (
                            _td_rg_top_newborn_zone
                            and bool(indicators.get("gajami_neg_rise_atr_soft_ok", 0))
                        ):
                            logger.info(
                                f"✅ [TD-RG-TOP-NEWBORN-ATR-SOFT] {symbol} "
                                f"follow-through 부족이나 ATR 노이즈 눌림 → hard 차단 격하 | "
                                f"rise_cur={_td_rg_top_newborn_rise_cur_pct:+.3f}% "
                                f"rise_atr={float(indicators.get('gajami_rise_atr_ratio', 0.0)):+.2f}x "
                                f"floor={float(indicators.get('gajami_neg_rise_atr_floor', -2.00)):+.2f}x"
                            )

                        if not _td_rg_top_newborn_followthrough_gate_ok:
                            logger.info(
                                f"🚫 [TD-RG-TOP-NEWBORN-FOLLOWTHROUGH-BLOCK] {symbol} "
                                f"TD/RG TOP newborn 현재봉 follow-through 부족 → soleus 차단 | "
                                f"regime={regime.value if hasattr(regime, 'value') else regime} "
                                f"dur={dur_5x10_bars} "
                                f"rise_cur={_td_rg_top_newborn_rise_cur_pct:+.3f}% "
                                f"floor={_TD_RG_TOP_NEWBORN_RISE_CUR_FLOOR_PCT:.2f}% "
                                f"ma10_alive={int(_tlc_ma10_alive)} "
                                f"tlc=({_tlc_ma10_slope:+.3f},{_tlc_ma20_slope:+.3f}) "
                                f"mid_shallow={int(_td_rg_mid_shallow_alive_ok)} "
                                f"quality={int(_td_rg_newborn_life_quality_ok)}"
                            )

                        try:
                            _td_rg_late_valid_age = (
                                int(dur_5x10_bars)
                                if dur_5x10_bars is not None
                                else None
                            )
                        except Exception:
                            _td_rg_late_valid_age = None

                        # 🔒 [LATE-VALID-INTRABAR-DROP-0809]
                        # intrabar_trigger는 현재봉 순간 돌파 트리거일 뿐 구조 origin이
                        # 아니다. recent_cross_ok가 dur<=6까지 열려 late-valid(dur>2)
                        # 구간과 겹쳐 있었다.
                        _td_rg_late_valid_raw_origin_ok = bool(
                            _is_true_gc_birth
                            or _verified_birth_traintrack
                            or _trend_reclaim_origin_ok
                            or _strong_momentum_reset_gc_ok
                            or bool(indicators.get("gajami_convergence_birth_core", 0))
                            or _is_birth_ok
                            or _newborn_strong_impulse_ok
                        )

                        # 🔒 [LATE-VALID-ORIGIN-REGIME-DROP-0809]
                        # regime in (TD, RANGING) 전제 때문에 TRENDING_UP에서는
                        # 이 늦은-VALID 차단이 원천적으로 무력했다. 실증 BORA:
                        # 17:21:56 RANGING·dur=3·raw_origin 4항 전부 0 → 정상 차단.
                        # 17:23:51 TU 전환·dur=4 → 동일 조건인데 regime 라벨만
                        # 바뀌어 보호 소멸, 9초 뒤 진입. 같은 파동 나이를 체제
                        # 전환으로 세탁하지 못하게 TD/RG 한정을 제거한다.
                        # 🔒 [GC-AGE-SSOT-DUR4-0814] 08-02 「DUR4 통일」에서 누락된 곳.
                        # NEWBORN_MAX_DUR(2, 탄생축)를 GC신선도 판사(RECENT_GC_MAX_BARS=4) 자리에
                        # 잘못 사용 — dur3~4가 fresh VALID인데 이 판사만 late로 전부 차단했다.
                        _td_rg_late_valid_origin_ok = bool(
                            not (
                                _td_rg_late_valid_age is not None
                                and _td_rg_late_valid_age > 4
                                and _lifecycle_entry_ok
                                and not _td_rg_late_valid_raw_origin_ok
                            )
                        )
                        indicators["gajami_td_rg_late_valid_origin_ok"] = int(
                            _td_rg_late_valid_origin_ok
                        )
                        indicators["gajami_td_rg_late_valid_origin_block"] = int(
                            not _td_rg_late_valid_origin_ok
                        )

                        if not _td_rg_late_valid_origin_ok:
                            logger.info(
                                f"🚫 [LATE-VALID-ORIGIN-BLOCK] {symbol} "
                                f"dur>4 structural origin 없음 → 동일파동 추격 차단 | "
                                f"regime={regime.value if hasattr(regime,'value') else regime} "
                                f"dur={dur_5x10_bars} "
                                f"life={gajami_lifecycle.get('state')} "
                                f"raw_origin={int(_td_rg_late_valid_raw_origin_ok)} "
                                f"true_gc={int(_is_true_gc_birth)} "
                                f"verified_train={int(_verified_birth_traintrack)} "
                                f"trend_reclaim={int(_trend_reclaim_origin_ok)}"
                            )

                        _td_rg_ma20_not_decel_ok = bool(
                            not _is_td_rg_regime
                            or bool(int(indicators.get("gajami_ma20_not_decel", 1)))
                        )
                        # 🔒 [2026-08-03] CONST breakout 경로의 origin 우회 제거.
                        #  _soleus_original_ok(아래)에는 _td_rg_newborn_origin_required_ok 가 있는데
                        #  이 경로에만 없어, origin 없는 TD/RG newborn 이 CONST 로 다시 soleus=True 가 됐다.
                        #  동일 판정이 경로별로 달라지는 전형적 우회도로.
                        _const_breakout_raw = bool(
                            _gajami_constitution_ok
                            and _td_rg_ma20_not_decel_ok
                            and _td_rg_newborn_origin_required_ok
                            and _td_rg_late_valid_origin_ok
                            and (
                                _is_birth_ok
                                or _lifecycle_entry_ok
                                or _newborn_strong_impulse_ok
                            )
                            and not _tlc_deep_down
                            and not bool(indicators.get("gajami_neg_rise_block", 0))
                        )
                       # 헌법·생애주기 통과 돌파를 BOX 위치로 재차단하지 않는다.
                        _const_breakout_top_block = False
                        _const_breakout_ok = bool(_const_breakout_raw)

                        _soleus_original_ok = (
                            _gajami_constitution_effective_ok
                            and not bool(indicators.get("gajami_neg_rise_block", 0))
                            and not _td_rg_top_whipsaw
                            # and not _tlc_rg_whipsaw   # 07-20 격하: CORE-PASS 탄생 오차단(KRW-AI). 계산·라벨·CORE-PASS-BUT-REJECT 로그 존치=관측. sim 실체결로 whipsaw EV 학습.
                            and not _tlc_deep_down
                            and not _top_flat_birth
                            and _td_rg_ma20_not_decel_ok
                            and _td_rg_top_newborn_followthrough_gate_ok
                            and _td_rg_late_valid_origin_ok
                            and (
                                _is_birth_ok
                                or _lifecycle_entry_ok
                                or _newborn_strong_impulse_ok
                            )
                            and _td_rg_newborn_origin_required_ok
                        )
                        # 🔒 lifecycle state는 TLC phase와 분리한다.
                        # 구조적 VALID 판정만 최종 Decision이 소비한다.
                        soleus_gajami = bool(
                            (_const_breakout_ok or _soleus_original_ok)
                            and gajami_lifecycle.get("state") == "VALID"
                            # 🔒 [LEGACY-TLC-AUTHORITY-SPLIT-0827]
                            # FAKE_TOP은 legacy MA/TLC와 분리한 독립 Hard Veto다.
                            # OR 바깥에서 CONST/original 두 경로에 동일 적용한다.
                            and not _fake_top_birth
                            # 🔒 [CONST-TOPFLAT-BYPASS-REMOVE-0827]
                            # original 경로에만 있던 TOP_FLAT 계약을
                            # OR 바깥 공통 계약으로 올려 CONST 단독 우회를 제거한다.
                            and not _top_flat_birth
                        )

                        indicators["gajami_const_breakout_open"] = int(_const_breakout_ok)
                        indicators["gajami_const_breakout_top_block"] = int(_const_breakout_top_block)

                        if _const_breakout_ok and not _soleus_original_ok:
                            logger.info(
                                f"🟢 [CONST-BREAKOUT-OPEN] {symbol} "
                                f"헌법PASS+생존birth → anti-chase 묶음 우회 | "
                                f"origin={int(_td_rg_late_valid_origin_ok)} "
                                f"birth={int(_is_birth_ok)} "
                                f"life={int(_lifecycle_entry_ok)} "
                                f"newborn_imp={int(_newborn_strong_impulse_ok)} "
                                f"deep_down={int(_tlc_deep_down)} "
                                f"neg_rise={int(bool(indicators.get('gajami_neg_rise_block', 0)))} "
                                # 🔒 [CONST-TOPFLAT-BYPASS-REMOVE-0827] 판단·로그 정합
                                f"top_flat={int(_top_flat_birth)} "
                                f"bypass={'BLOCKED' if _top_flat_birth else 'OPEN'}"
                            )

                        if (
                            _is_td_rg_regime
                            and _is_newborn_life
                            and _birth_impulse_ok
                            and not _td_rg_newborn_origin_required_ok
                        ):
                            logger.info(
                                f"🚫 [SOLEUS-GAJAMI-ORIGIN-BLOCK] {symbol} "
                                f"TD/RG newborn 받침 없는 가자미 차단 | "
                                f"true_gc={int(_is_true_gc_birth)} pullback={int(_had_pullback)} "
                                f"intrabar={int(intrabar_trigger)} verified_train={int(_verified_birth_traintrack)} "
                                f"reacc={int(ma5_reaccelerating)} mid_shallow={int(_birth_mid_shallow_alive_ok)} "
                                f"quant_rescue={int(_quantized_birth_rescue_ok)} dur={dur_5x10_bars} "
                                f"momentum_reset={int(_strong_momentum_reset_gc_ok)} trend_cont={int(_trend_continuing)}"
                            )

                        indicators['gajami_long_ma_bearish'] = int(long_ma_bearish)
                        indicators['gajami_td_ma60_down_veto'] = int(td_ma60_down_veto)
                        indicators['gajami_long_ma_strong_down_veto'] = int(long_ma_strong_down_veto)
                        if soleus_gajami and long_ma_veto:
                            soleus_gajami = False
                            if long_ma_strong_down_veto:
                                _veto_label = "LONG-MA-STRONG-DOWN-VETO"
                            elif td_ma60_down_veto:
                                _veto_label = "TD-MA60-DOWN-VETO"
                            else:
                                _veto_label = "LONG-MA-BEARISH-VETO"
                            logger.info(
                                f"🚫 [{_veto_label}] {symbol} 장기선/TD MA60 하락 → 가자미근 차단 | "
                                f"regime={regime.value if hasattr(regime, 'value') else regime} | "
                                f"src=closed[-2/-3] "
                                f"ma60={ma60_val:.2f} ma120={ma120_val:.2f} "
                                f"s60={slope_ma60:.6f} s120={slope_ma120:.6f}"
                            )

                        _fail_reason = "PASS"

                        if not soleus_gajami:
                            _fail_reason = None
                            if indicators.get("gajami_neg_rise_block", 0):
                                _fail_reason = "NEG_RISE"
                            elif not _core_shape_ok:
                                _fail_reason = f"SHAPE({gajami_shape_result.get('reason', '?')})"
                            elif not gajami_physically_possible:
                                _fail_reason = "PHYS"
                            elif not _gajami_basic_ok:
                                _fail_reason = "BASIC_LAW"
                            elif not _gajami_law1_ok:
                                _fail_reason = "LAW1"
                            elif not _gajami_law2_ok:
                                _fail_reason = "LAW2"
                            elif _gajami_constitution_veto:
                                _fail_reason = "VETO3"
                            elif indicators.get("gajami_top_signature_block", 0):
                                _fail_reason = "TOP_SIGNATURE"
                            elif not entry_trigger_ok:
                                _fail_reason = "TRIGGER"
                            elif indicators.get("gajami_td_ma20_rebound_block", 0):
                                _fail_reason = "TD_MA20_REBOUND"
                            elif indicators.get("gajami_ranging_ma20_collapse_block", 0):
                                _fail_reason = "RANGING_MA20_COLLAPSE"
                            elif not (ma20_trend_ok or _birth_tlc_ok or _td_newborn_strong_recovery_exempt):
                                _fail_reason = "MA20"
                            elif indicators.get("gajami_top_flat_birth_block", 0):
                                _fail_reason = "TOP_FLAT_BIRTH"
                            elif not window_ok:
                                _fail_reason = "WINDOW"
                            elif not _td_rg_ma20_not_decel_ok:
                                _fail_reason = "TD_RG_MA20_NEG_WORSEN"
                            elif indicators.get("gajami_td_rg_top_newborn_followthrough_gate_block", 0):
                                _fail_reason = "TD_RG_TOP_NEWBORN_FOLLOWTHROUGH"
                            elif not _td_rg_late_valid_origin_ok:
                                _fail_reason = "LATE_VALID_ORIGIN"
                            # legacy MA/TLC 관측값은 FINAL reason을 선점하지 않는다.
                            elif gajami_lifecycle.get("state") not in (None, "VALID"):
                                _fail_reason = f"LIFECYCLE({gajami_lifecycle.get('death_reason','?')})"
                            elif not _regime_gate_ok:
                                _fail_reason = "REGIME_GATE"
                            elif indicators.get('gajami_long_ma_strong_down_veto', 0):
                                _fail_reason = "LONG_STRONG_DOWN"
                            elif indicators.get('gajami_td_ma60_down_veto', 0):
                                _fail_reason = "TD_MA60_DOWN"
                            elif indicators.get('gajami_long_ma_bearish', 0):
                                _fail_reason = "LONG_MA_BEARISH"
                            elif indicators.get("tlc_rg_whipsaw", 0):
                                _fail_reason = "TLC_RG_WHIPSAW"
                            elif indicators.get("tlc_deep_down", 0):
                                _fail_reason = "TLC_DEEP_DOWN"
                            elif not _td_rg_newborn_origin_required_ok:
                                _fail_reason = "TD_RG_NEWBORN_ORIGIN"
                            elif indicators.get("gajami_fake_top_birth", 0):
                                _fail_reason = "FAKE_TOP_BIRTH"
                            elif not _lifecycle_entry_ok:
                                _fail_reason = "LIFECYCLE_ENTRY"
                            else:
                                _fail_reason = "UNKNOWN"
                            logger.info(f"🚫 [FINAL-FAIL] {symbol} reason={_fail_reason}")

                        # 🪜 [GATE-LADDER] 전 게이트 1줄 전수 — RESULT=PASS=Analysis 통과 / 그외=차단사유
                        try:
                            logger.info(
                                f"🪜 [GATE-LADDER] {symbol} | "
                                f"stage=ANALYSIS RESULT={_fail_reason} | "
                                f"regime={regime.value if hasattr(regime,'value') else regime} "
                                f"dur={dur_5x10_bars} "
                                f"rise={(gajami_lifecycle.get('rise_pct') or 0.0):+.3%} | "
                                f"shape={int(_core_shape_ok)} "
                                f"phys={int(gajami_physically_possible)} "
                                f"basic={int(_gajami_basic_ok)} "
                                f"ctn_raw={int(_gajami_constitution_ok)} "
                                f"ctn_eff={int(_gajami_constitution_effective_ok)} "
                                f"law1={int(_gajami_law1_ok)} "
                                f"law2={int(_gajami_law2_ok)} "
                                f"veto3={int(_gajami_constitution_veto)} "
                                f"trig={int(entry_trigger_ok)} "
                                f"ma20_gate={int(ma20_trend_ok or _birth_tlc_ok or _td_newborn_strong_recovery_exempt)} "
                                f"window={int(window_ok)} "
                                f"top_alive={int(bool(indicators.get('gajami_tlc_alive_at_top', 0)))} "
                                f"closed_gc={int(indicators.get('closed_gc_5x10_ok', 0) or 0)} "
                                f"life={gajami_lifecycle.get('state')} "
                                f"life_reason={gajami_lifecycle.get('death_reason')} "
                                f"life_entry={int(_lifecycle_entry_ok)} "
                                f"regime_gate={int(_regime_gate_ok)} "
                                f"origin={int(_td_rg_newborn_origin_required_ok)} "
                                f"late_valid={int(_td_rg_late_valid_origin_ok)} "
                                f"birth_recog={int(bool(indicators.get('gajami_birth_recognized',0)))} "
                                f"conv_birth={int(bool(indicators.get('gajami_convergence_birth_core',0)))} "
                                f"subtick_birth={int(bool(indicators.get('subtick_body_birth',0)))} | "
                                f"neg_rise={int(bool(indicators.get('gajami_neg_rise_block',0)))} "
                                f"ma15_frag={int(bool(indicators.get('gajami_ma15_entry_fragile',0)))} "
                                f"td_m20reb={int(bool(indicators.get('gajami_td_ma20_rebound_block',0)))} "
                                f"rg_m20col={int(bool(indicators.get('gajami_ranging_ma20_collapse_block',0)))} "
                                f"top_follow={int(bool(indicators.get('gajami_td_rg_top_newborn_followthrough_gate_block',0)))} "
                                f"top_flat={int(bool(indicators.get('gajami_top_flat_birth_block',0)))} "
                                f"top_whip={int(bool(indicators.get('gajami_td_rg_top_whipsaw',0)))} "
                                f"tlc_whip={int(bool(indicators.get('tlc_rg_whipsaw',0)))} "
                                f"tlc_deep={int(bool(indicators.get('tlc_deep_down',0)))} "
                                f"fake_top={int(bool(indicators.get('gajami_fake_top_birth',0)))} "
                                f"legacy_ma_down={int(bool(indicators.get('gajami_tlc_top_dead_observe',0)))} "
                                f"legacy_nb_inactive={int(bool(indicators.get('gajami_tlc_newborn_inactive',0)))} "
                                f"legacy_tlc_raw={int(bool(indicators.get('gajami_legacy_tlc_observe_raw',0)))} "
                                f"nb_birth_obs={int(bool(indicators.get('gajami_newborn_birth_state_observe',0)))} "
                                f"long_strong_dn={int(bool(indicators.get('gajami_long_ma_strong_down_veto',0)))} "
                                f"td_ma60_dn={int(bool(indicators.get('gajami_td_ma60_down_veto',0)))} "
                                f"long_bear={int(bool(indicators.get('gajami_long_ma_bearish',0)))} "
                                f"ma20_nd={int(bool(indicators.get('gajami_ma20_not_decel',1)))} "
                                f"ma20_tlc={float(indicators.get('gajami_ma20_tlc_cur',0.0)):+.4f}/"
                                f"{float(indicators.get('gajami_ma20_tlc_prev',0.0)):+.4f}"
                            )
                        except Exception:
                            pass

                        # ━━ CHAIN TRACE ━━
                        try:
                            _chain = (
                                f"shape={int(_core_shape_ok)} → "
                                f"trigger={int(entry_trigger_ok)}("
                                f"recent={int(recent_trigger)} "
                                f"intra={int(intrabar_trigger)} "
                                f"expand={int(spacing_expanding)} "
                                f"reacc={int(ma5_reaccelerating)} "
                                f"pb={int(_had_pullback)}) → "
                                f"lifecycle={gajami_lifecycle.get('state','?')} → "
                                f"regime={int(_regime_gate_ok)}"
                            )
                        except Exception:
                            _chain = "chain_build_fail"

                        # ━━ FINAL DECISION ━━
                        # 🔒 [ENTRY-DECISION-FIRST-BLOCK-SSOT-0829]
                        # FINAL-FAIL / GATE-LADDER에서 이미 확정한 first blocker를
                        # 다시 별도 if/elif 체인으로 재판정하지 않는다.
                        _final_reason = _fail_reason
                        logger.info(
                            f"[ENTRY-DECISION] {symbol} | "
                            f"{_chain} | "
                            f"final={'PASS' if soleus_gajami else 'BLOCK'} | "
                            f"reason={_final_reason}"
                        )

                        # 🔬 [DIVERGENCE-SHADOW] FINAL 문맥 결합 — 관측 전용
                        try:
                            if not bool(
                                indicators.get("_divergence_shadow_ready", False)
                            ):
                                raise ValueError("DIVERGENCE_RAW_NOT_READY")

                            _div_hidden = bool(
                                indicators.get(
                                    "divergence_rsi_bull_hid",
                                    False,
                                )
                                or indicators.get(
                                    "divergence_macd_bull_hid",
                                    False,
                                )
                            )
                            # 같은 봉에서 충돌하면 상승 생애의 히든상승을 우선한다.
                            _div_bear = bool(
                                indicators.get(
                                    "divergence_bear_reg_valid",
                                    False,
                                )
                                and not _div_hidden
                            )
                            indicators["divergence_hidden_bull"] = _div_hidden
                            indicators["divergence_bear_reg_valid"] = _div_bear

                            _div_bar = indicators.get("indicator_shadow_bar")
                            _div_tf = (
                                timeframe.value
                                if hasattr(timeframe, "value")
                                else str(timeframe)
                            )
                            _div_key = (_div_tf, symbol)
                            _div_seen = getattr(
                                TechnicalIndicators,
                                "_divergence_shadow_logged_bar",
                                {},
                            )
                            if (
                                _div_bar
                                and _div_seen.get(_div_key) != _div_bar
                            ):
                                _div_seen[_div_key] = _div_bar
                                setattr(
                                    TechnicalIndicators,
                                    "_divergence_shadow_logged_bar",
                                    _div_seen,
                                )
                        except Exception as _div_final_e:
                            logger.info(
                                f"⚠️ [DIVERGENCE-SHADOW] {symbol} "
                                f"FINAL 결합 실패 | "
                                f"bar={indicators.get('indicator_shadow_bar')} | "
                                f"reason={_div_final_e}"
                            )

                        # ============================================================
                        # 🔥 [CYCLE-PHASE-SHADOW] 확정봉 변동성 생애 관측 —
                        #    bandwidth추이 + MA정배열 + 거래량 + 반전캔들
                        #    + ADX + 다이버전스 → 수렴/발화/확산/고갈 판독
                        # ============================================================
                        # 목적: TLC를 GC나이 순간판정이 아니라 변동성 생애
                        #       (COMPRESSION→IGNITION→EXPANSION→EXHAUSTION)로
                        #       보기 위한 관측 축. 판정 미사용, 매매 영향0.
                        # 규칙(사장님 확정, 미결 L123~124):
                        #   EXHAUSTION = bw축소 AND (약세div신뢰 OR 정배열깨짐)
                        #   EXPANSION  = bw확장 AND MA정배열 AND ADX>=30 AND ADX확산
                        #   IGNITION   = bw확장 AND MA정배열 AND (거래량 OR 반전캔들)
                        #   COMPRESSION= bw축소 AND ADX<=20
                        #   그 외 = TRANSITION
                        #   약세div신뢰 = 강한상승확산(EXPANSION 조건) 중엔 무시.
                        #   과열권 판단은 divergence_bear_reg_valid에 이미 반영됨.
                        # 원칙: 현재 진행봉(iloc[-1]) 전부 제외, 확정봉만 사용.
                        #       DIVERGENCE-SHADOW 기존 플래그(_divergence_shadow_ready)
                        #       를 그대로 재사용 — 신규 플래그·기존 코드 수정 없음.
                        #       거래량은 volume_ratio 재사용 않고 확정봉만 독립
                        #       재계산한다(기존 키는 라이브봉 포함 가능성).
                        #       bandwidth 추이는 직전 확정봉 대비 변곡을 관측한다.
                        # ============================================================
                        indicators["_cycle_phase_shadow_ready"] = False
                        _kc_release = False
                        try:
                            # J3는 J2 다이버전스 준비 여부와 독립적으로 계산한다.
                            if (
                                df is None
                                or len(df) < 32
                                or not {
                                    "open", "high", "low",
                                    "close", "volume",
                                }.issubset(df.columns)
                            ):
                                raise ValueError(
                                    "CYCLE_INPUT_SHORT_OR_MISSING"
                                )

                            # 현재 진행봉 제외 — 확정봉 시계열로 고정.
                            _cycle_close = (
                                df["close"].astype(float).iloc[:-1]
                            )
                            _cycle_volume = (
                                df["volume"].astype(float).iloc[:-1]
                            )

                            # 1) bandwidth 추이 — BB(20,2) 확정봉, 직전 1봉 대비
                            _cycle_bb_mid = _cycle_close.rolling(20).mean()
                            _cycle_bw = (
                                (
                                    _cycle_close.rolling(20).std(ddof=0)
                                    * 4.0
                                )
                                / _cycle_bb_mid.replace(0.0, np.nan)
                                * 100.0
                            ).replace(
                                [np.inf, -np.inf], np.nan
                            ).dropna()

                            if len(_cycle_bw) < 2:
                                raise ValueError("CYCLE_BW_SHORT")

                            # 🔬 [ACTIVE-TLC-P8-BW-TREND-0816]
                            # 1봉 부호가 아니라 최근 BW 자체의 노이즈를 기준으로
                            # 의미 있는 수축/확장만 판정한다.
                            _bw_now = float(_cycle_bw.iloc[-1])
                            _bw_prev3 = _cycle_bw.iloc[-4:-1].astype(float)
                            _bw_ref = (
                                float(_bw_prev3.median())
                                if len(_bw_prev3) >= 2
                                else float(_cycle_bw.iloc[-2])
                            )
                            _bw_delta_pct = (
                                ((_bw_now - _bw_ref) / _bw_ref) * 100.0
                                if _bw_ref > 0.0 else 0.0
                            )
                            _bw_step_pct = (
                                _cycle_bw.pct_change()
                                .replace([np.inf, -np.inf], np.nan)
                                .abs()
                                .dropna()
                                .tail(20)
                                * 100.0
                            )
                            _bw_deadband_pct = (
                                float(_bw_step_pct.median())
                                if len(_bw_step_pct) > 0
                                else 0.0
                            )
                            _bw_expanding = bool(
                                _bw_delta_pct > _bw_deadband_pct
                            )
                            _bw_contracting = bool(
                                _bw_delta_pct < -_bw_deadband_pct
                            )
                            # 절대 폭 위치 SHADOW — 최근 60개 확정 BW 중 현재 위치.
                            _bw_rank_60 = _cycle_bw.tail(60)
                            _bw_percentile_60 = (
                                float((_bw_rank_60 <= _bw_now).mean() * 100.0)
                                if len(_bw_rank_60) >= 20 else -1.0
                            )
                            # 판사3 SHADOW — 절대 저변동 수렴극 기억.
                            # 최근 60개 BW 하위 20%를 deep compression으로 보고,
                            # 이후 3봉 동안만 발화 자격의 선행증거로 유지한다.
                            _JUDGE3_DEEP_COMP_PCT60_MAX = 20.0
                            _JUDGE3_DEEP_COMP_TTL_BARS = 3
                            _deep_comp_now = bool(
                                0.0 <= _bw_percentile_60
                                <= _JUDGE3_DEEP_COMP_PCT60_MAX
                            )

                            # 2) MA 정배열(ma5>ma10>ma20>0) — 붕괴 감지 위해
                            #    이전 확정봉도 함께 판정.
                            _cycle_ma5 = _cycle_close.rolling(5).mean()
                            _cycle_ma10 = _cycle_close.rolling(10).mean()
                            _cycle_ma20 = _cycle_close.rolling(20).mean()

                            _ma_aligned_prev = bool(
                                float(_cycle_ma5.iloc[-2])
                                > float(_cycle_ma10.iloc[-2])
                                > float(_cycle_ma20.iloc[-2])
                                > 0.0
                            )
                            _ma_aligned = bool(
                                float(_cycle_ma5.iloc[-1])
                                > float(_cycle_ma10.iloc[-1])
                                > float(_cycle_ma20.iloc[-1])
                                > 0.0
                            )
                            _ma_alignment_broken = bool(
                                _ma_aligned_prev and not _ma_aligned
                            )
                            # 🆕 바닥발화 보조축(2026-07-17 정의보정) — MA5 상승 관측.
                            # 정배열 완성 전의 능동형 TLC 전환 초입도 별도 계측하며,
                            # 정배열은 발화의 결과이므로 선행 필수조건으로 사용하지 않는다.
                            _ma5_rising = bool(
                                float(_cycle_ma5.iloc[-1])
                                > float(_cycle_ma5.iloc[-2])
                            )
                            
                            # 3) 거래량 — 확정봉만으로 독립 재계산(라이브봉 배제)
                            # 🔬 [ACTIVE-TLC-P8-VOL-MEDIAN-0816]
                            # 폭발봉에 끌리는 평균 대신 최근 20개 확정봉의
                            # 전형적 거래량을 중앙값으로 잡는다.
                            _vol_base = float(
                                _cycle_volume.iloc[-21:-1].median()
                            )
                            _volume_ratio_closed = (
                                float(_cycle_volume.iloc[-1]) / _vol_base
                                if math.isfinite(_vol_base)
                                and _vol_base > 0.0
                                else 0.0
                            )

                            # 4) 반전캔들 — 직전 확정봉 2개(iloc[-3],iloc[-2])
                            _po = float(df["open"].iloc[-3])
                            _pc = float(df["close"].iloc[-3])
                            _o = float(df["open"].iloc[-2])
                            _h = float(df["high"].iloc[-2])
                            _l = float(df["low"].iloc[-2])
                            _c = float(df["close"].iloc[-2])

                            _bull_engulf = bool(
                                _pc < _po
                                and _c > _o
                                and _o <= _pc
                                and _c >= _po
                            )
                            _bear_engulf = bool(
                                _pc > _po
                                and _c < _o
                                and _o >= _pc
                                and _c <= _po
                            )
                            _body = abs(_c - _o)
                            _lower_wick = min(_o, _c) - _l
                            _upper_wick = _h - max(_o, _c)
                            _hammer = bool(
                                _body > 0.0
                                and _c > _o
                                and _lower_wick >= (2.0 * _body)
                                and _upper_wick <= _body
                            )
                            _reversal_candle_bull = bool(_bull_engulf or _hammer)
                            # 🔒 [REVERSAL-CANDLE-KIND-OBSERVE-0829] ★417
                            # 판정은 그대로 두고 어느 패턴(장악형/망치형)이 FIRE를
                            # 만들었는지만 관측한다. 거래소 원본 1,592봉 실측:
                            # 적색 망치(_c<=_o) 배제로 47/82건(57%) 소실 — 봇 정의가
                            # 교과서 정의보다 REVERSAL을 덜 잡는 정도를 측정하기 위함.
                            # 몸통이 크면서 아래꼬리도 몸통의 2배 이상인 장대양봉은
                            # 두 조건을 동시에 만족할 수 있어(실측 가능성 확인) 임의
                            # 우선순위로 하나를 감추지 않고 "BE+HM"으로 함께 남긴다.
                            _reversal_candle_kind = (
                                "BE+HM" if (_bull_engulf and _hammer)
                                else "BE" if _bull_engulf
                                else "HM" if _hammer
                                else "-"
                            )

                            # 5) ADX — 기존 계산값(indicators['adx']) 재사용,
                            #    확정봉 2개(iloc[-3],iloc[-2])만 사용.
                            _adx_series = indicators.get("adx")
                            if (
                                not hasattr(_adx_series, "iloc")
                                or len(_adx_series) < 3
                            ):
                                raise ValueError("CYCLE_ADX_MISSING")

                            _adx_prev = float(_adx_series.iloc[-3])
                            _adx_closed = float(_adx_series.iloc[-2])

                            if not (
                                math.isfinite(_adx_prev)
                                and math.isfinite(_adx_closed)
                            ):
                                raise ValueError("CYCLE_ADX_INVALID")

                            _adx_expanding = bool(_adx_closed > _adx_prev)
                            # 🆕 ADX 고갈 — 확산정점 후 꺾임(2026-07-17 정의보정)
                            # adx>=30(강한추세였음) + not expanding(꺾임) = 고갈.
                            # 기존 EXHAUSTION은 이걸 안 봐서 오답봉이 TRANSITION
                            # 으로 무판별됐음.
                            _adx_exhaustion = bool(
                                _adx_closed >= 30.0 and not _adx_expanding
                            )

                            # 6) 다이버전스 — DIVERGENCE-SHADOW FINAL 결과 재사용
                            _bear_div = bool(
                                indicators.get(
                                    "divergence_bear_reg_valid", False
                                )
                            )
                            _hidden_bull = bool(
                                indicators.get(
                                    "divergence_hidden_bull", False
                                )
                            )

                            # 강한 상승 확산 중 약세div는 오답무시(사장님 원칙)
                            _strong_up_expansion = bool(
                                _bw_expanding
                                and _ma_aligned
                                and _adx_closed >= 30.0
                                and _adx_expanding
                            )
                            _bear_div_reliable = bool(
                                _bear_div and not _strong_up_expansion
                            )

                            # 완만 발화 종합 SHADOW — 2026-07-18 실측 211건 반예측(6%),
                            # 소비처 0건 확인 후 제거(discrimination-miner harness).

                            # KC는 J3 전용. ready(계산가능)가 아니라 최근수렴(had_squeeze) 후
                            # 해제(now not on)만 소비. 첫봉성은 아래 _phase_age==1이 보장.
                            _kc_release = bool(
                                indicators.get("keltner_had_squeeze", False)
                                and not bool(
                                    indicators.get("keltner_squeeze_on", False)
                                )
                            )

                            # 7) ACTIVE PRICE STRUCTURE → TLC 부모 생애주기 SSoT
                            _active_tlc = _active_tlc_price_structure(
                                df,
                                indicators.get("atr"),
                                timeframe=timeframe,
                            )
                            if not bool(_active_tlc.get("ready", False)):
                                raise ValueError(
                                    "ACTIVE_TLC_STRUCTURE_NOT_READY"
                                )
                            # 🔒 [ACTIVE-TLC-LIVE-PHASE-SSOT-0829]
                            # _active_tlc["phase"]는 현재 자식구조 기준으로 산출된다.
                            # parent_structure는 부모 문맥 관측용으로만 보존한다.
                            _cycle_phase = str(_active_tlc["phase"])
                            _cycle_parent_structure = str(
                                _active_tlc.get("parent_structure", "UNCONFIRMED")
                            )
                            _cycle_live_structure = str(
                                _active_tlc.get("structure", "UNCONFIRMED")
                            )
                            indicators["cycle_phase_shadow"] = _cycle_phase
                            indicators["cycle_active_leg"] = str(
                                _active_tlc["leg"]
                            )
                            indicators["cycle_active_structure"] = _cycle_live_structure
                            indicators["cycle_active_pos"] = float(
                                _active_tlc["active_pos"]
                            )
                            indicators["cycle_active_pos_closed"] = float(
                                _active_tlc.get("active_pos_closed", 0.5)
                            )
                            indicators["cycle_active_retrace"] = float(
                                _active_tlc["retrace"]
                            )
                            indicators["cycle_active_progress"] = float(
                                _active_tlc["progress"]
                            )
                            indicators["cycle_active_leg_bars"] = int(
                                _active_tlc["leg_bars"]
                            )
                            indicators["cycle_prev_leg_bars"] = int(
                                _active_tlc["prev_leg_bars"]
                            )
                            indicators["cycle_leg_time_ratio"] = float(
                                _active_tlc["leg_time_ratio"]
                            )
                            indicators["cycle_active_efficiency"] = float(
                                _active_tlc["efficiency"]
                            )
                            indicators["cycle_active_low"] = float(
                                _active_tlc["active_low"]
                            )
                            indicators["cycle_active_high"] = float(
                                _active_tlc["active_high"]
                            )
                            indicators["cycle_active_anchor"] = float(
                                _active_tlc["anchor_price"]
                            )
                            # ★[2026-08-21] parent_pos → window_pos : 부모 구간이 아니라
                            #  df 전체 창의 argmax 위치(L841)라 이름이 사실과 달랐다.
                            indicators["cycle_window_pos"] = float(
                                _active_tlc["window_pos"]
                            )
                            indicators["cycle_window_hi_recent"] = float(
                                _active_tlc["window_hi_recent"]
                            )
                            indicators["cycle_window_lo_recent"] = float(
                                _active_tlc["window_lo_recent"]
                            )
                            indicators["cycle_window_width_pct"] = float(
                                _active_tlc["window_width_pct"]
                            )
                            # 🔒 [ACTIVE-TLC-PARENT-RANGE-SSOT-0828]
                            # 마지막 두 확정 opposite DC pivot 기반 실제 부모 swing 위치.
                            indicators["cycle_parent_ready"] = int(
                                bool(_active_tlc.get("parent_ready", False))
                            )
                            indicators["cycle_parent_pos"] = float(
                                _active_tlc.get("parent_pos", SENTINEL_FLOAT)
                            )
                            indicators["cycle_parent_pos_closed"] = float(
                                _active_tlc.get("parent_pos_closed", SENTINEL_FLOAT)
                            )
                            indicators["cycle_parent_low"] = float(
                                _active_tlc.get("parent_low", SENTINEL_FLOAT)
                            )
                            indicators["cycle_parent_high"] = float(
                                _active_tlc.get("parent_high", SENTINEL_FLOAT)
                            )
                            indicators["cycle_parent_wave_id"] = str(
                                _active_tlc.get("parent_wave_id", "NONE")
                            )
                            indicators["cycle_bw_now_pct"] = round(
                                _bw_now, 4
                            )
                            indicators["cycle_bw_ref_pct"] = round(
                                _bw_ref, 4
                            )
                            indicators["cycle_bw_delta_pct"] = round(
                                _bw_delta_pct, 4
                            )
                            indicators["cycle_bw_deadband_pct"] = round(
                                _bw_deadband_pct, 4
                            )
                            indicators["cycle_ma_aligned"] = _ma_aligned
                            indicators[
                                "cycle_ma_alignment_broken"
                            ] = _ma_alignment_broken
                            indicators[
                                "cycle_volume_ratio_closed"
                            ] = round(_volume_ratio_closed, 4)
                            indicators[
                                "cycle_reversal_candle_bull"
                            ] = _reversal_candle_bull
                            indicators[
                                "cycle_reversal_candle_kind"
                            ] = _reversal_candle_kind
                            indicators[
                                "cycle_reversal_candle_bear"
                            ] = _bear_engulf
                            indicators["cycle_adx_closed"] = round(
                                _adx_closed, 4
                            )
                            indicators[
                                "cycle_adx_expanding"
                            ] = _adx_expanding
                            indicators[
                                "cycle_adx_exhaustion"
                            ] = _adx_exhaustion
                            indicators[
                                "cycle_ma5_rising"
                            ] = _ma5_rising                            
                            indicators[
                                "cycle_bear_div_reliable"
                            ] = _bear_div_reliable
                            indicators["_cycle_phase_shadow_ready"] = True

                            # 🔒 [ACTIVE-TLC-AGE-0815]
                            # Active TLC는 현재 진행봉 provisional high/low까지 소비한다.
                            # 따라서 상태키도 직전 확정봉이 아니라 현재봉이어야 한다.
                            _cyc_bar = (
                                indicators.get("current_candle_time")
                                or indicators.get("indicator_shadow_bar")
                            )
                            _cyc_tf = (
                                timeframe.value
                                if hasattr(timeframe, "value")
                                else str(timeframe)
                            )
                            _cyc_key = (_cyc_tf, symbol)

                            if not hasattr(
                                TechnicalIndicators,
                                "_cycle_phase_shadow_state",
                            ):
                                setattr(
                                    TechnicalIndicators,
                                    "_cycle_phase_shadow_state",
                                    TechnicalIndicators._load_cycle_phase_state_cache(),
                                )
                            _cycle_state = getattr(
                                TechnicalIndicators,
                                "_cycle_phase_shadow_state",
                                {},
                            )
                            # 🔒 [J1-PARENT-WINDOW-STATE-0829]
                            # 원본 record는 보존한다.
                            # 아래 60분 stale reset은 phase 생애만 fresh로 만들며,
                            # 같은 cycle_parent_wave_id의 J1 소유권 상태는 지우지 않는다.
                            _prev_state_raw = _cycle_state.get(_cyc_key, {})
                            _prev_state = dict(_prev_state_raw)
                            _prev_deep_comp_ttl = int(
                                _prev_state.get("deep_comp_ttl", 0) or 0
                            )

                            _tf_map = {
                                '1m': 60, '3m': 180, '5m': 300,
                                '10m': 600, '15m': 900, '30m': 1800,
                                '1h': 3600, '4h': 14400, '1d': 86400,
                            }
                            _tf_sec = _tf_map.get(_cyc_tf, 180)

                            # 🔒 [ACTIVE-TLC-P7-STALE-0816]
                            # 재시작 캐시는 짧은 중단만 이어 붙인다.
                            # 60분 초과 공백이나 시간축 역행은 같은 phase 생애로
                            # 연결하지 않고 fresh state로 재시작한다.
                            _prev_bar = _prev_state.get("bar")
                            if _cyc_bar and _prev_bar:
                                try:
                                    _resume_gap_sec = (
                                        pd.to_datetime(_cyc_bar)
                                        - pd.to_datetime(_prev_bar)
                                    ).total_seconds()
                                    if (
                                        _resume_gap_sec < 0.0
                                        or _resume_gap_sec > 3600.0
                                    ):
                                        _prev_state = {}
                                        _prev_deep_comp_ttl = 0
                                except Exception:
                                    pass

                            if _cyc_bar and _prev_state.get("bar") != _cyc_bar:
                                # 새 봉: age/TTL은 실제 경과 봉 수만큼 진행.
                                # 스캔이 봉을 건너뛰면 _bar_step > 1이 되며,
                                # age/TTL 모두 그만큼 반영해야 시간축이 맞는다.
                                _prev_phase = str(
                                    _prev_state.get("phase") or "NONE"
                                )
                                try:
                                    _span = (
                                        pd.to_datetime(_cyc_bar)
                                        - pd.to_datetime(
                                            _prev_state.get("bar")
                                        )
                                    ).total_seconds()
                                    _bar_step = max(
                                        1, int(round(_span / _tf_sec))
                                    )
                                except Exception:
                                    _bar_step = 1

                                if _prev_phase == _cycle_phase:
                                    _phase_start_bar = _prev_state.get(
                                        "phase_start_bar"
                                    )
                                    try:
                                        _span2 = (
                                            pd.to_datetime(_cyc_bar)
                                            - pd.to_datetime(
                                                _phase_start_bar
                                            )
                                        ).total_seconds()
                                        _phase_age = max(
                                            1,
                                            int(round(_span2 / _tf_sec))
                                            + 1,
                                        )
                                    except Exception:
                                        _phase_age = (
                                            int(
                                                _prev_state.get("age", 0)
                                                or 0
                                            )
                                            + _bar_step
                                        )
                                else:
                                    _phase_age = 1
                                    _phase_start_bar = _cyc_bar

                                # DeepComp TTL도 실제 경과 봉 수만큼 소모.
                                # armed 판정은 "현재봉 직전"까지 살아있던
                                # TTL 기준이므로 _bar_step - 1을 먼저 뺀다.
                                _ttl_before_current = max(
                                    0,
                                    _prev_deep_comp_ttl - max(
                                        0, _bar_step - 1
                                    ),
                                )
                                _deep_comp_armed = bool(
                                    _ttl_before_current > 0
                                )
                                _deep_comp_ttl = (
                                    _JUDGE3_DEEP_COMP_TTL_BARS
                                    if _deep_comp_now
                                    else max(
                                        0,
                                        _prev_deep_comp_ttl - _bar_step,
                                    )
                                )

                            else:
                                # 같은 봉: phase가 바뀌면 즉시 새 생애 age=1.
                                # TTL은 봉이 바뀐 것이 아니므로 재차감하지 않는다.
                                _stored_phase = str(
                                    _prev_state.get("phase") or "NONE"
                                )

                                if _stored_phase == _cycle_phase:
                                    _prev_phase = str(
                                        _prev_state.get("prev") or "NONE"
                                    )
                                    _phase_age = int(
                                        _prev_state.get("age", 0) or 0
                                    )
                                    _phase_start_bar = (
                                        _prev_state.get("phase_start_bar")
                                        or _cyc_bar
                                    )
                                else:
                                    _prev_phase = _stored_phase
                                    _phase_age = 1
                                    _phase_start_bar = _cyc_bar

                                _deep_comp_ttl = _prev_deep_comp_ttl
                                _deep_comp_armed = bool(
                                    int(
                                        _prev_state.get(
                                            "deep_comp_armed", 0
                                        ) or 0
                                    )
                                )

                            # 🔒 [J1-PARENT-WINDOW-STATE-0829]
                            # phase 갱신 때 record를 통째로 교체하지 않는다.
                            # j1_wave/j1_wst 같은 parent 소유권 키를 보존한 채
                            # phase 필드만 갱신한다.
                            _cycle_record = dict(_prev_state_raw)
                            _cycle_record.update({
                                "bar": _cyc_bar,
                                "phase": _cycle_phase,
                                "prev": _prev_phase,
                                "phase_start_bar": _phase_start_bar,
                                "age": _phase_age,
                                "deep_comp_ttl": _deep_comp_ttl,
                                "deep_comp_armed": int(_deep_comp_armed),
                            })
                            _cycle_state[_cyc_key] = _cycle_record
                            setattr(
                                TechnicalIndicators,
                                "_cycle_phase_shadow_state",
                                _cycle_state,
                            )
                            TechnicalIndicators._save_cycle_phase_state_cache()
                            # 🔬 [ACTIVE-TLC-14-0815]
                            # 가격 구조 = 부모 / BB·ADX·Volume·KC = 세부 국면
                            if _cycle_phase == "EXHAUSTION":
                                # 🔬 [ACTIVE-TLC-P8-EXH-NAME-0816]
                                # 이 분기는 ADX가 아니라 BW 수축 여부의 2분류다.
                                # 거짓 이름(EXH_ADX)을 실제 산식과 일치시킨다.
                                _cycle_detail = (
                                    "EXH_BW_CONTRACT"
                                    if _bw_contracting
                                    else "EXH_NONCONTRACT"
                                )
                            elif _cycle_phase == "EXPANSION":
                                _cycle_detail = (
                                    "EARLY_EXPANSION"
                                    if _phase_age <= 1
                                    else "MATURE"
                                )
                            elif _cycle_phase == "IGNITION":
                                _cycle_detail = (
                                    "EARLY_IGNITION"
                                    if _phase_age <= 1
                                    else "IGNITION_LATE"
                                )
                            else:  # COMPRESSION
                                _det_a = bool(
                                    _bw_expanding or _kc_release
                                )
                                _det_v = float(
                                    _volume_ratio_closed or 0.0
                                )
                                if _det_a:
                                    _cycle_detail = (
                                        "VOL_EXP_NEAR"
                                        if _det_v >= 0.7
                                        else "VOL_EXP_MID"
                                        if _det_v >= 0.3
                                        else "VOL_EXP_FAR"
                                    )
                                elif (
                                    _bw_contracting
                                    and _adx_closed <= 20.0
                                ):
                                    _cycle_detail = (
                                        "COMP_FRESH"
                                        if _phase_age <= 2
                                        else "COMP_DEEP"
                                    )
                                elif (
                                    _bw_contracting
                                    and _adx_closed <= 30.0
                                ):
                                    _cycle_detail = "COILING"
                                elif _bw_contracting:
                                    _cycle_detail = "COOLING"
                                else:
                                    _cycle_detail = "FLAT"
                            indicators["cycle_detail_shadow"] = _cycle_detail
                            # 🔒 [ACTIVE-TLC-14-REMATCH-0816] 재매칭 2줄 철회.
                            # 가격구조 부모 phase/age는 변경하지 않는다.
                            # 실측(P8창 n=2,649, 기준 -0.1215%): VOL_EXP_NEAR(+0.0198%)를
                            # IGNITION(-0.1787)으로, EXH_NONCONTRACT(-0.0337%)를 EXPANSION
                            # (-0.1855)으로 보냈다 — 원래 phase가 더 가깝다. 스프레드
                            # 0.1358%p → 0.6868%p (5.1배). VOL_EXP_FAR/MID·COOLING은 유지.
                            _cycle_detail_family = {
                                "VOL_EXP_FAR": "IGNITION_IMMINENT",
                                "VOL_EXP_MID": "IGNITION_IMMINENT",
                                "COOLING": "EXHAUSTION",
                            }.get(_cycle_detail, _cycle_phase)
                            indicators["cycle_detail_family_shadow"] = _cycle_detail_family

                            # 🔬 [ACTIVE-TLC-P4P5-CONTEXT-0815]
                            # 🔒 [ACTIVE-TLC-LIVE-PHASE-SSOT-0829] ★418
                            # P4: 현재 자식구조→phase→14detail→family를 한 계약으로 묶는다.
                            #     부모구조는 parent_structure 문맥으로만 별도 보존한다.
                            # P5: RT DUR과 확정봉 TLC DUR을 같은 계약에 연결한다.
                            # 판정권은 부여하지 않는다 — Strategy는 SHADOW로만 소비한다.
                            _tlc_dur_rt_ctx = (
                                int(dur_5x10_bars)
                                if dur_5x10_bars is not None
                                else -1
                            )
                            indicators["tlc_context_shadow"] = {
                                "structure": _cycle_live_structure,
                                "parent_structure": _cycle_parent_structure,
                                "leg": str(_active_tlc.get("leg", "SIDE")),
                                "phase": _cycle_phase,
                                "detail": _cycle_detail,
                                "family": _cycle_detail_family,
                                "phase_age": int(_phase_age),
                                "parent_ready": int(
                                    bool(_active_tlc.get("parent_ready", False))
                                ),
                                "parent_pos": float(
                                    _active_tlc.get("parent_pos", SENTINEL_FLOAT)
                                ),
                                "parent_pos_closed": float(
                                    _active_tlc.get("parent_pos_closed", SENTINEL_FLOAT)
                                ),
                                "parent_low": float(
                                    _active_tlc.get("parent_low", SENTINEL_FLOAT)
                                ),
                                "parent_high": float(
                                    _active_tlc.get("parent_high", SENTINEL_FLOAT)
                                ),
                                "parent_wave_id": str(
                                    _active_tlc.get("parent_wave_id", "NONE")
                                ),
                                "dur_rt": _tlc_dur_rt_ctx,
                                "dur_closed": int(
                                    indicators.get("tlc_dur_5x10_closed", -1)
                                ),
                                "hhfirst": int(_active_tlc.get("obs_first_high_break_bars", -1)),
                                "hhage": int(_active_tlc.get("obs_first_high_break_age", -1)),
                            }

                            # 🔒 [J3-CONT-DUR-RANGE-REMOVE-0827]
                            # J3 CONT는 Active TLC의 현재 continuation을 판정한다.
                            # UP_LEG + det14 + 확정봉 위치(aposc) + NEG_RISE hard veto를 유지하고,
                            # GC 나이(dur)를 J3 내부의 추가 veto로 다시 사용하지 않는다.
                            # 공통 CLOSED_GC 및 Strategy의 절대 Hard Veto 계약은 변경하지 않는다.
                            # _tlc_dur_rt_ctx와 JUDGE3-CYCLE dur 로그는 관측값으로 계속 유지한다.
                            _j3_aposc = float(
                                _active_tlc.get("active_pos_closed", 0.5)
                            )
                            try:
                                _j3_durc = int(
                                    indicators.get("tlc_dur_5x10_closed", -1)
                                )
                            except (TypeError, ValueError):
                                _j3_durc = -1
                            # 🔒 [J3-CONT-UPLEG-0825]
                            # CONT는 현재 진행 상승 leg의 continuation만 담당한다.
                            # DOWN_LEG 반전 RG_LOW lane은 [J3-RGLOW-REMOVE-0826]으로 PASS 판정권을 회수하고 관측(OBS)만 유지한다.
                            # PASS 판정권을 회수하고 관측(OBS)만 유지한다.
                            # WET 18:40 오진입: leg=DOWN_LEG + det=VOL_EXP_NEAR + lane=CONT → J3 단독 PASS.
                            # 0825 당시에는 leg SSoT만 정합했고 위치 문턱은 기존값을 유지했다.
                            _j3_continuation_pass = bool(
                                str(
                                    _active_tlc.get("leg", "SIDE")
                                ) == "UP_LEG"
                                # 🔒 [J3-CONT-TRUEGC-OVERCONSTRAINT-REVERT-0906] ★520
                                # DET14 producer 계약:
                                #   VOL_EXP_NEAR  ⇒ COMPRESSION 내부의 BW/KC 확장 + 거래량 근접 셀
                                #   IGNITION_LATE ⇒ IGNITION age>1 셀
                                #
                                # ★475는 VOL_EXP_NEAR에 gajami_true_gc_birth를 추가했지만,
                                # true_gc는 fresh-GC window가 아니라 정확한 DC→GC 탄생 event bit다.
                                # J3 진입의 GC 최근성은 공통 closed_gc_5x10_ok와 Strategy hard gate가
                                # 이미 소유한다. CONT가 true_gc를 다시 요구하면 DUR2~4의 fresh GC도
                                # exact-birth event가 아니라는 이유로 중복 차단할 수 있다.
                                #
                                # 최신 정본 §242도 과거 "919건 중 2건" 주석은 epoch가 달라 stale이며
                                # 현재 근거로 쓰지 말라고 정정했다. 따라서 ★520은 true_gc 결합만 회수한다.
                                # IGNITION_LATE는 과거/최근 실측 방향이 충돌하므로 이번 패치에서 삭제하지 않는다.
                                # 아래 UP_LEG·aposc·MA5·NEG_RISE·FAKE_TOP 안전장은 그대로 유지한다.
                                # 새 변수·상수·threshold 0개.
                                and _cycle_detail in (
                                    "VOL_EXP_NEAR",
                                    "IGNITION_LATE",
                                )
                                # 🔒 [J3-CONT-APOSC-REVERT-0830] ★456 — ★445 철회·원복
                                # ★445의 SAND 근거는 오조인으로 무효였고,
                                # 배포 후 신규 허용영역(aposc>=0.9)에서
                                # QUID·ATH·BARD·BIGTIME·WET J3 손실 5건이 연속 발생했다.
                                # aposc는 확정종가의 active range 내 위치값일 뿐
                                # 자체적으로 품질의 양/음 방향을 뜻하지 않는다.
                                # 따라서 ★445 이전의 검증 계약으로 원복한다.
                                and _j3_aposc < 0.8
                                # 🔒 [J3-CONT-MA5-ALIVE-0831] ★469
                                # J3의 EXH_TURN lane도 `_ma5_rising`을 소비하므로
                                # CONT lane 역시 같은 확정봉 MA5 방향가드를 유지한다.
                                # 같은 판사 안의 방향성 계약을 맞춘다.
                                #   J3 EXH_TURN : _ma5_rising(확정봉 MA5[-1]>MA5[-2])을 본다
                                #   Analysis    : MA5_SLOPE_LOW · MA5_NOT_UP(s5=DEAD)로 차단한다
                                #     (단, 이 헌법 판정의 _ma5_alive는 slope비율+tick_up 결합
                                #      SSoT로 _ma5_rising과 동일 계산식은 아니다 — 방향성만 정합)
                                #   J3 CONT   : 그 둘을 모두 뚫는다  ← 이 줄로 교정
                                # KRW-ONDO 08-31 02:06:18 : ma5r=0 인데 J3=1 PASS →
                                #   02:07:08 매수 → 02:24 -0.90%. Analysis는 02:02·02:05
                                #   두 번 MA5_SLOPE_LOW/MA5_NOT_UP으로 막았었다.
                                # 실측: ma5r=0의 TP-first가 두 측정에서 -6.3%p / -6.7%p.
                                # 새 변수·상수 0개(L12632 `_ma5_rising`, 확정봉 iloc[-1]>iloc[-2]).
                                and _ma5_rising
                                # 🔒 [J3-NEG-RISE-HARD-VETO-0827]
                                # Analysis가 이미 NEG_RISE hard block으로 확정한 자리는
                                # J3 CONT가 독립 PASS로 다시 열지 않는다.
                                and not bool(
                                    int(
                                        indicators.get(
                                            "gajami_neg_rise_block", 0
                                        ) or 0
                                    )
                                )
                                # 🔒 [J3-CONT-FAKE-TOP-CONSUME-0902] ★493
                                # 부모파동 고점권 재교차로 판단된 자리를
                                # J3 CONT가 독립 PASS로 다시 열지 않는다.
                                and not bool(
                                    int(
                                        indicators.get(
                                            "gajami_fake_top_birth", 0
                                        ) or 0
                                    )
                                )
                            )
                            # 🔒 [J3-EXHAUSTION-TURN-0905] ★515
                            # 과거 RG_LOW의 넓은 RANGING+parent-structure 조건은 제거한다.
                            # J3의 "추세전환" 권한은 TLC 14단계의 EXHAUSTION family가
                            # 확인되고, DOWN_LEG 저점권에서 fresh closed GC가 생긴
                            # 경우에만 복구한다. 새 숫자 0개(0.5·NEWBORN_MAX_DUR 재사용).
                            _j3_exhaustion_turn_pass = bool(
                                regime in (
                                    MarketRegime.TRENDING_DOWN,
                                    MarketRegime.RANGING,
                                )
                                and _cycle_phase == "EXHAUSTION"
                                and _cycle_detail_family == "EXHAUSTION"
                                and str(
                                    _active_tlc.get("leg", "SIDE")
                                ) == "DOWN_LEG"
                                and _j3_aposc < 0.5
                                and bool(
                                    int(
                                        indicators.get(
                                            "closed_gc_5x10_ok", 0
                                        ) or 0
                                    )
                                )
                                and 1 <= _j3_durc <= (
                                    SoleusGajamiConstants.NEWBORN_MAX_DUR
                                )
                                and _ma5_rising
                                and not bool(
                                    int(
                                        indicators.get(
                                            "gajami_neg_rise_block", 0
                                        ) or 0
                                    )
                                )
                                and not bool(
                                    int(
                                        indicators.get(
                                            "gajami_fake_top_birth", 0
                                        ) or 0
                                    )
                                )
                            )
                            # 🔒 [J3-EXHAUSTION-TURN-OBSERVE-0905] ★515-O
                            # ★515-R1의 EXH_TURN 판정 연결만 회수한다.
                            # 계산·export·lane 로그는 그대로 남겨 관측을 계속한다.
                            _judge3_cycle_pass = bool(_j3_continuation_pass)
                            _judge3_cycle_veto = False
                            _judge3_cycle_verdict = (
                                "VETO" if _judge3_cycle_veto
                                else "PASS" if _judge3_cycle_pass
                                else "NEUTRAL"
                            )
                            indicators["cycle_bw_percentile_60"] = round(
                                _bw_percentile_60, 2
                            )
                            # 🔒 [2026-08-08 SSoT-P1/P4] BW signal Producer.
                            # t6의 기존 보간식/부호를 그대로 이관한다.
                            _bwp = safe_extract_scalar(
                                indicators.get(
                                    "cycle_bw_percentile_60",
                                    SENTINEL_FLOAT,
                                ),
                                SENTINEL_FLOAT,
                            )
                            _bw_signal = 0.0
                            if np.isfinite(_bwp) and 0.0 <= _bwp <= 100.0:
                                # 🔒 [BW-PEAK-REALIGN-0823] 실측 정점은 백분위 30~40
                                #   (n=56 · 3창 EV +1,208/+1,308/+1,398 · 부호 2/2).
                                #   백분위 0~21(구 고득점권; 0~18=1.0, 이후 선형하락)은
                                #   n=126 · 3창 전부 최악
                                #   (−798/−1,573/−798) → 저백분위 과대평가를 낮춘다.
                                #   소비처 2곳 : conf(w=0.1995) ·
                                #   RangingHybrid quality(계수 0.20 → 임계·보너스·차단선).
                                _bw_signal = float(np.interp(
                                    _bwp,
                                    (0.0, 18.0, 30.0, 40.0, 50.0, 65.0, 100.0),
                                    (0.6, 0.7, 1.0, 1.0, 0.0, -0.5, -1.0),
                                ))
                            indicators["bw_signal"] = _bw_signal
                            indicators["cycle_prev_phase_shadow"] = _prev_phase
                            indicators["cycle_phase_age_shadow"] = _phase_age
                            indicators["cycle_deep_comp_now_shadow"] = int(
                                _deep_comp_now
                            )
                            indicators["cycle_deep_comp_ttl_shadow"] = int(
                                _deep_comp_ttl
                            )
                            indicators["cycle_deep_comp_armed_shadow"] = int(
                                _deep_comp_armed
                            )
                            indicators["judge3_cycle_pass"] = int(
                                _judge3_cycle_pass
                            )
                            indicators["judge3_exhaustion_turn_pass"] = int(
                                _j3_exhaustion_turn_pass
                            )
                            indicators["judge3_cycle_veto"] = int(
                                _judge3_cycle_veto
                            )
                            indicators[
                                "judge3_cycle_verdict"
                            ] = _judge3_cycle_verdict

                            # 🔒 [J3-PRODUCER-CONSUMER-INTEGRITY-0902] ★489
                            # 현재 Producer 판결을 별도 키에 봉인한다.
                            indicators["judge3_prov_ready"] = 1
                            indicators["judge3_prov_pass"] = int(
                                _judge3_cycle_pass
                            )
                            indicators["judge3_prov_verdict"] = str(
                                _judge3_cycle_verdict
                            )
                            indicators["judge3_prov_bar"] = str(
                                _cyc_bar or ""
                            )
                            indicators["judge3_prov_det"] = str(
                                _cycle_detail or ""
                            )
                            indicators["judge3_prov_fake_top"] = int(
                                bool(
                                    indicators.get(
                                        "gajami_fake_top_birth", 0
                                    )
                                )
                            )
                            _cyc_seen = getattr(
                                TechnicalIndicators,
                                "_cycle_phase_shadow_logged_bar",
                                {},
                            )

                            try:
                                _cyc_dur = (
                                    int(dur_5x10_bars)
                                    if dur_5x10_bars is not None
                                    else -1
                                )
                            except (TypeError, ValueError):
                                _cyc_dur = -1

                            _cyc_gc = int(
                                bool(
                                    indicators.get(
                                        "entry_ma5_gt_ma10", 0
                                    )
                                )
                            )

                            # 같은 봉이라도
                            # ① Active phase/detail 변화
                            # ② MA5>MA10 GC 발생
                            # ③ dur 변화
                            # ④ J3 PASS/NEUTRAL/VETO 변화
                            # ⑤ EXHAUSTION 전환 lane 변화
                            # 가 있으면 다시 적재한다.
                            _cyc_log_state = (
                                _cyc_bar,
                                _cycle_phase,
                                _cycle_detail,
                                _cyc_gc,
                                _cyc_dur,
                                int(_judge3_cycle_pass),
                                int(_j3_exhaustion_turn_pass),
                                _judge3_cycle_verdict,
                            )

                            if (
                                _cyc_bar
                                and _cyc_seen.get(_cyc_key)
                                != _cyc_log_state
                            ):
                                _cyc_seen[_cyc_key] = _cyc_log_state
                                setattr(
                                    TechnicalIndicators,
                                    "_cycle_phase_shadow_logged_bar",
                                    _cyc_seen,
                                )

                                _cyc_jbar = str(
                                    _cyc_bar
                                ).replace("T", " ")[:16]

                                logger.info(
                                    f"⚖️ [JUDGE3-CYCLE] {symbol} | "
                                    f"tf={_cyc_tf} "
                                    f"bar={_cyc_bar} jbar={_cyc_jbar} "
                                    f"J3={int(_judge3_cycle_pass)} "
                                    # 🔒 [J3-EXH-TURN-OBS-LABEL-0906] ★520
                                    # ★515-O에서 EXH_TURN의 PASS 권한은 회수됐고 계산만 관측용으로 남았다.
                                    # active lane으로 오인하지 않도록 OBS를 명시한다.
                                    f"lane={'CONT' if _j3_continuation_pass else 'EXH_TURN(OBS)' if _j3_exhaustion_turn_pass else 'NONE'} "
                                    f"verdict={_judge3_cycle_verdict} | "
                                    f"phase={_cycle_phase} "
                                    f"det={_cycle_detail} family={_cycle_detail_family} "
                                    f"prev={_prev_phase} age={_phase_age} "
                                    f"dur={_cyc_dur} "
                                    f"durc={int(indicators.get('tlc_dur_5x10_closed', -1))} "
                                    f"gc={_cyc_gc} "
                                    # 🔒 [J3-NEG-RISE-HARD-VETO-0827] 판단 원재료 로그 정합
                                    f"neg={int(bool(indicators.get('gajami_neg_rise_block', 0)))} "
                                    # 🔒 [J3-TRUEGC-OBSERVE-0906] ★520
                                    # ★520 이후 true_gc는 J3 CONT 판정변수가 아니라 origin 관측값이다.
                                    # restored VOL_EXP_NEAR의 true_gc=0/1 분포를 분리 계측하기 위해 유지한다.
                                    f"true_gc={int(bool(indicators.get('gajami_true_gc_birth', 0)))} "
                                    # 🔒 [J3-FAKE-TOP-DECISION-LOG-0902]
                                    # J3 CONT가 실제 소비하는 FAKE_TOP도 같은 로그에서 재현한다.
                                    f"fake_top={int(bool(indicators.get('gajami_fake_top_birth', 0)))} | "
                                    f"leg={indicators.get('cycle_active_leg', '?')} "
                                    f"struct={indicators.get('cycle_active_structure', '?')} "
                                    f"pstruct={_cycle_parent_structure} "
                                    f"ubrk={int(_active_tlc.get('obs_down_upbreak', 0) or 0)} "
                                    # 🔬 [TLC-BREAK-DECISION-OBSERVE-0830] ★440
                                    # phase 번역이 실제 소비하는 break bit를 동일 로그에 직접 노출한다.
                                    # bhi/blo는 _break_high/_break_low의 복제 관측값이며 판정 변화 0.
                                    f"bhi={int(_active_tlc.get('break_high', -1))} "
                                    f"blo={int(_active_tlc.get('break_low', -1))} "
                                    f"fixls={_active_tlc.get('obs_fix_low_state', 'NA')} "
                                    f"fixlag={int(_active_tlc.get('obs_fix_anchor_bars', -1))} "
                                    f"dbrk={int(_active_tlc.get('obs_up_downbreak', 0) or 0)} "
                                    f"fixhs={_active_tlc.get('obs_fix_high_state', 'NA')} "
                                    f"fixhlag={int(_active_tlc.get('obs_fix_high_anchor_bars', -1))} "
                                    f"hhfirst={int(_active_tlc.get('obs_first_high_break_bars', -1))} "
                                    f"hhage={int(_active_tlc.get('obs_first_high_break_age', -1))} "
                                    f"swsrc={_active_tlc.get('swing_source', 'NONE')} "
                                    f"swrev={float(_active_tlc.get('swing_reversal', SENTINEL_FLOAT)):.6f} "
                                    f"swatr={float(_active_tlc.get('swing_wave_atr', SENTINEL_FLOAT)):.2f} "
                                    f"apos={safe_extract_scalar(indicators.get('cycle_active_pos'), 0.5):.6f} "
                                    f"aposc={safe_extract_scalar(indicators.get('cycle_active_pos_closed'), 0.5):.6f} "
                                    # 🔒 [J3-APOSC-PASSBIT-OBSERVE-0829] ★426
                                    # 🔒 [J3-CONT-APOSC-REVERT-0830] ★456 — ★445 철회
                                    # J3 CONT가 실제 소비하는 `_j3_aposc < 0.8` 판정을
                                    # 수치 재판정 없이 동일한 PASS 비트로 로그에 기록한다.
                                    # aposc 6자리 출력만으로 0.800000 경계의 원판정을
                                    # 복원할 수 없는 문제를 해소한다. 관측 전용 · 판정 변화 0.
                                    f"apok={int(_j3_aposc < 0.8)} "
                                    f"ret={float(indicators.get('cycle_active_retrace', 0.0) or 0.0):.3f} "
                                    f"prog={float(indicators.get('cycle_active_progress', 0.0) or 0.0):.3f} "
                                    f"eff={float(indicators.get('cycle_active_efficiency', 0.0) or 0.0):.3f} "
                                    f"lbar={int(indicators.get('cycle_active_leg_bars', 0) or 0)} "
                                    f"lratio={float(indicators.get('cycle_leg_time_ratio', SENTINEL_FLOAT) or SENTINEL_FLOAT):.3f} "
                                    f"wpos={safe_extract_scalar(indicators.get('cycle_window_pos'), SENTINEL_FLOAT):.3f} "
                                    f"ppos={safe_extract_scalar(indicators.get('cycle_parent_pos'), SENTINEL_FLOAT):.3f} "
                                    f"pposc={safe_extract_scalar(indicators.get('cycle_parent_pos_closed'), SENTINEL_FLOAT):.3f} "
                                    f"pready={int(indicators.get('cycle_parent_ready', 0) or 0)} | "
                                    f"deep_now={int(_deep_comp_now)} "
                                    f"deep_ttl={_deep_comp_ttl} "
                                    f"armed={int(_deep_comp_armed)} "
                                    f"bw_pct60={_bw_percentile_60:.1f} | "
                                    f"bear_div_ctx={int(_bear_div_reliable)} "
                                    f"adx_exh={int(_adx_exhaustion)} | "
                                    f"bwc={int(_bw_contracting)} bwe={int(_bw_expanding)} "
                                    f"bwnow={_bw_now:.4f} bwref={_bw_ref:.4f} "
                                    f"bwd={_bw_delta_pct:+.4f} "
                                    f"bwdb={_bw_deadband_pct:.4f} | "
                                    f"align={int(_ma_aligned)} "
                                    f"brk={int(_ma_alignment_broken)} "
                                    f"ma5r={int(_ma5_rising)} "
                                    f"adx={_adx_closed:.1f} "
                                    f"adxe={int(_adx_expanding)} | "
                                    f"vol={_volume_ratio_closed:.2f} "
                                    f"rev_candle={int(_reversal_candle_bull)} "
                                    f"revb_candle={int(_bear_engulf)} "
                                    f"kc={int(_kc_release)} "
                                    f"sue={int(_strong_up_expansion)}"
                                )
                                # 🔒 [J3-FIDELITY-COMPACT-0915] ★676-R1
                                # ★§675 : SHIB·ANIME·KAT 의 J3 lane 이 갈린 입력이
                                #  이 줄에만 있는데 200s 스로틀에 잘린다. 전체 면제는
                                #  791B/행(필드 60여 개)이라 과하므로 lane 판정 입력만
                                #  compact 로 복제한다.
                                # ★lane 은 L13614 와 같은 두 bool 을 그대로 찍는다
                                #  (라벨로 가공하면 재현식이 달라진다).
                                # ★§678 BLOCKER-2 : bar/jbar/dur 는 ★causal join key 다.
                                #  ★행동 변화 없음 — 로그만 추가한다.
                                logger.info(
                                    f"⚖️ [J3-FID] {symbol} "
                                    f"J3={int(_judge3_cycle_pass)} "
                                    f"v={_judge3_cycle_verdict} "
                                    f"bar={_cyc_bar} "
                                    f"jbar={_cyc_jbar} "
                                    f"dur={_cyc_dur} "
                                    f"cont={int(_j3_continuation_pass)} "
                                    f"exh={int(_j3_exhaustion_turn_pass)} "
                                    f"gc={_cyc_gc} "
                                    f"dbrk={int(_active_tlc.get('obs_up_downbreak', 0) or 0)} "
                                    f"ph={_cycle_phase} "
                                    f"age={_phase_age} "
                                    f"tgc={int(bool(indicators.get('gajami_true_gc_birth', 0)))}"
                                )

                        except Exception as _cyc_e:
                            logger.info(
                                f"⚠️ [CYCLE-PHASE-SHADOW] {symbol} "
                                f"계산 실패 | "
                                f"tf={timeframe.value if hasattr(timeframe, 'value') else timeframe} | "
                                f"bar={indicators.get('indicator_shadow_bar')} | "
                                f"reason={_cyc_e}"
                            )

                        # ============================================================
                        # ⚖️ [ENTRY-JURY] 판사1 가자미 + 판사2 오실·다이버
                        # ============================================================
                        # JUDGE3와 동일한 PASS/NEUTRAL/VETO 스키마.
                        # Strategy Core의 약한 J1 lane에서 J2·J3 VETO로 소비한다.
                        try:
                            _jury_tf = (
                                timeframe.value
                                if hasattr(timeframe, "value")
                                else str(timeframe)
                            )
                            _jury_bar = str(
                                indicators.get("current_candle_time")
                                or indicators.get("indicator_shadow_bar")
                                or ""
                            )
                            _jury_ctx_bar = indicators.get("indicator_shadow_bar")
                            _jury_key = (_jury_tf, symbol)

                            # ── J1: MA 헌법 + 구조 생애 + Active TLC 소유권 ──
                            # TLC는 J1 부모/현재 구조 문맥으로 소비한다:
                            # 성숙 꼭지 또는 UP_LEG+LH_LL 하락구조 반등은 J1 PASS에서 제외한다.
                            _j1_gc = bool(
                                int(indicators.get("closed_gc_5x10_relation", 0) or 0) == 1
                            )
                            _j1_golden = int(
                                indicators.get("golden_count", 0) or 0
                            )
                            try:
                                _j1_dur_src = (
                                    gajami_lifecycle.get("dur", None)
                                    if isinstance(gajami_lifecycle, dict)
                                    else None
                                )
                                _j1_dur = int(
                                    _j1_dur_src
                                    if _j1_dur_src is not None
                                    else indicators.get("dur_5x10_bars", -1)
                                )
                            except (TypeError, ValueError):
                                _j1_dur = -1

                            _j1_s5 = float(ma5_slope or 0.0)
                            _j1_s10 = float(ma10_slope or 0.0)
                            _j1_s20_raw = slope_ma20_df
                            _j1_s20 = float(_j1_s20_raw or 0.0)

                            # 🔒 [J1-READY-REAL-0817] 실제 GC dur만 J1 판정 대상으로 인정.
                            _j1_ready = bool(
                                1 <= _j1_dur < 999
                                and _j1_s20_raw is not None
                                and all(
                                    math.isfinite(_v)
                                    for _v in (
                                        _j1_s5,
                                        _j1_s10,
                                        _j1_s20,
                                    )
                                )
                            )
                            _j1_trend_dead = bool(
                                _j1_ready
                                and _j1_s10 < 0.0
                                and _j1_s20 < 0.0
                            )
                            # 🔒 [J1-PARENT-TLC-QUALIFIER-0823]
                            # local GC dur은 교차 나이일 뿐 부모파동 탄생을 보장하지 않는다.
                            # Active TLC가 EXPANSION/MATURE의 확정 고점이면 re-GC를 새 BIRTH로 승인하지 않는다.
                            # IGNITION/EARLY 계열은 aposc=1.0이어도 차단하지 않아 진짜 초입을 보존한다.
                            _j1_tlc_ctx = indicators.get("tlc_context_shadow") or {}
                            _j1_tlc_phase = str(_j1_tlc_ctx.get("phase", "") or "")
                            _j1_tlc_detail = str(_j1_tlc_ctx.get("detail", "") or "")

                            # 🔒 [J1-TLC-OBSERVE-SAFE-0827]
                            # 관측 패치가 Cycle 로컬 변수(_cycle_parent_structure 등)의
                            # 생존 여부에 의존하지 않도록 이미 export된 SSoT만 소비한다.
                            _j1_obs_leg = str(
                                _j1_tlc_ctx.get(
                                    "leg",
                                    indicators.get("cycle_active_leg", "?"),
                                )
                                or "?"
                            )
                            _j1_obs_pstruct = str(
                                _j1_tlc_ctx.get("parent_structure", "?")
                                or "?"
                            )
                            _j1_obs_true_gc = int(
                                indicators.get("gajami_true_gc_birth", 0) or 0
                            )
                            _j1_obs_reset_mom = int(
                                indicators.get(
                                    "gajami_strong_momentum_reset_gc_ok", 0
                                ) or 0
                            )
                            _j1_obs_fake_reset = int(
                                indicators.get(
                                    "gajami_fake_late_reset_gc", 0
                                ) or 0
                            )
                            _j1_aposc = safe_extract_scalar(
                                indicators.get("cycle_active_pos_closed"),
                                0.5,
                            )
                            # 🔒 [J1-ACTIVE-TLC-TOP-QUALIFIER-0904] ★509-R2
                            # 0.9995는 "고점권 문턱"이 아니라 Active TLC의
                            # 사실상 1.000 포화점만 잡는 기존 최소 기준이다.
                            # parent 위치만으로 차단하지 않고, UP_LEG에서
                            # active/parent가 동시에 완전 상단이며 실제 국면도
                            # 성숙 또는 냉각일 때만 J1 재-GC를 꼭지로 본다.
                            _j1_tlc_family = str(
                                _j1_tlc_ctx.get("family", "") or ""
                            )
                            _j1_live_structure = str(
                                _j1_tlc_ctx.get("structure", "?") or "?"
                            )
                            _j1_parent_pos_closed_top = safe_extract_scalar(
                                _j1_tlc_ctx.get("parent_pos_closed"),
                                SENTINEL_FLOAT,
                            )
                            _j1_mature_top = bool(
                                _j1_obs_leg == "UP_LEG"
                                and _j1_aposc >= 0.9995
                                and _j1_parent_pos_closed_top >= 0.9995
                                and (
                                    (
                                        _j1_tlc_phase == "EXPANSION"
                                        and _j1_tlc_detail == "MATURE"
                                    )
                                    or (
                                        _j1_tlc_phase == "COMPRESSION"
                                        and _j1_tlc_detail == "COOLING"
                                        and _j1_tlc_family == "EXHAUSTION"
                                    )
                                )
                            )
                            # 🔒 [J1-ACTIVE-STRUCTURE-OWNER-0905] ★514
                            # UP_LEG인데 live structure가 LH_LL이면
                            # 상승 추세의 가자미근이 아니라 하락 구조 내부 반등이다.
                            # Active TLC가 이미 산출한 structure를 J1이 소비하지 않아
                            # 계산·판단이 어긋나 있던 것을 맞춘다(새 threshold 0).
                            # 실측 12일: UP_LEG∧LH_LL 전체 66건 24.2%인데
                            #   J1 단독 21건만 9.5% — 같은 자리도 J1이 소유하면 최악이다.
                            # 🔒 [J1-STRUCT-LL-PARITY-0906] ★521
                            # ★514는 LH_LL만 봤는데 J2 ★518-R1은 endswith("_LL")로 HH_LL까지 본다.
                            # _live_structure는 UP_LEG일 때 H축만 갱신하고 L축은 부모 구조를
                            # 유지하므로 HH_LL도 "저점축 미확정" 압축 국면이다
                            # (t5 L1162가 LH_LL과 HH_LL을 같은 COMPRESSION으로 분류한다).
                            # 같은 판정을 두 판사가 다르게 쓰는 로직 불일치를 J2 계약에 맞춘다.
                            # 실측: UP_LEG∧LH_LL 0건(baseline 0) ·
                            #   UP_LEG∧HH_LL 4건 승률 0.00% 평균 -0.728%(수익 진입 0건).
                            #   ★509-R2 위치3조건 3건도 전부 mature_top=0·전부 HH_LL이라 같이 닫힌다.
                            # XLM 09-06 00:31: struct=HH_LL aposc=1.000 pposc=1.000 → ctreb=1.
                            #   정답봉 후보 00:05/00:08은 struct=LH_HL이라 훼손 0.
                            # 새 숫자·변수·조건 0개(연산자 1개 정합화).
                            _j1_countertrend_rebound = bool(
                                _j1_obs_leg == "UP_LEG"
                                and _j1_live_structure.endswith("_LL")
                            )
                            _j1_tlc_bad_timing = bool(
                                _j1_mature_top
                                or _j1_countertrend_rebound
                            )
                            # 🔒 [J1-BODY-GROWTH-OWNER-0828]
                            # body_growth는 J1 가자미 형상 법률이다. 누락 시 기존 Strategy
                            # 계약(default=True)을 보존하고, J2/J3/J4 판결에는 관여하지 않는다.
                            _j1_body_growth_ok = bool(
                                indicators.get("shape_v2_body_growth", True)
                            )
                            # ══════════════════════════════════════════════════
                            # 🔒 [J1-LOW-WINDOW-OPEN-0829-R2]
                            # 5차-1 : J1 저점 진입창
                            # ══════════════════════════════════════════════════
                            # OG3 계산은 현 위치에 유지하되 관측 전용이다.
                            # LOW_OPEN/J1 PASS의 필수 AND에는 편입하지 않는다.
                            # iloc[-1]은 진행봉이므로 c=-2, p=-3, pp=-4, ppp=-5.
                            # 주의: ①은 candle-open V가 아니라 역사표본과 동일한
                            # '전전 확정종가 → 직전봉 저점 → 직전종가 회복' V-proxy다.
                            _og3_ready = 0
                            _og3_k = -1
                            _og3_c1 = _og3_c2 = _og3_c3 = 0
                            _og3_va = _og3_vb = 0
                            try:
                                if df is not None and len(df) >= 5:
                                    _og3_cl = df["close"].astype(float).to_numpy()
                                    _og3_lo = df["low"].astype(float).to_numpy()
                                    _og3_cc = float(_og3_cl[-2])
                                    _og3_pc = float(_og3_cl[-3])
                                    _og3_ppc = float(_og3_cl[-4])
                                    _og3_pppc = float(_og3_cl[-5])
                                    _og3_pl = float(_og3_lo[-3])
                                    _og3_ppl = float(_og3_lo[-4])
                                    if min(
                                        _og3_cc,
                                        _og3_pc,
                                        _og3_ppc,
                                        _og3_pppc,
                                        _og3_pl,
                                        _og3_ppl,
                                    ) > 0.0:
                                        _og3_ready = 1
                                        # ① 이전봉 V-proxy
                                        _og3_c1 = int(
                                            (_og3_ppc - _og3_pl)
                                            / _og3_ppc * 100.0 >= 0.15
                                            and _og3_pc >= _og3_ppc * 0.9995
                                        )
                                        # ② 현재 확정봉 급등
                                        _og3_c2 = int(
                                            (_og3_cc - _og3_pc)
                                            / _og3_pc * 100.0 >= 0.10
                                        )
                                        # ③ 전전봉 추세 유지
                                        _og3_c3 = int(
                                            _og3_ppc >= _og3_pppc * 0.9995
                                        )
                                        # 변A: 전전봉 V-proxy → 이전봉 완만 → 현재봉 상승
                                        _og3_va = int(
                                            (_og3_pppc - _og3_ppl)
                                            / _og3_pppc * 100.0 >= 0.15
                                            and abs(_og3_pc - _og3_ppc)
                                            / _og3_ppc * 100.0 <= 0.15
                                            and _og3_cc > _og3_pc
                                        )
                                        # 변B: 이전봉 살짝 눌림 → 현재봉 상승
                                        _og3_pull = (
                                            (_og3_ppc - _og3_pc)
                                            / _og3_ppc * 100.0
                                        )
                                        _og3_vb = int(
                                            0.0 < _og3_pull <= 0.30
                                            and _og3_cc > _og3_pc
                                        )
                                        _og3_k = (
                                            _og3_c1
                                            + _og3_c2
                                            + _og3_c3
                                        )
                            except Exception:
                                _og3_ready = 0
                                _og3_k = -1

                            _og3_own = int(
                                bool(
                                    _og3_ready
                                    and (
                                        _og3_k == 3
                                        or _og3_va
                                        or _og3_vb
                                    )
                                )
                            )
                            indicators["owner_gajami_ready"] = _og3_ready
                            indicators["owner_gajami_v_prev"] = _og3_c1
                            indicators["owner_gajami_surge_cur"] = _og3_c2
                            indicators["owner_gajami_trend_hold"] = _og3_c3
                            indicators["owner_gajami_varA"] = _og3_va
                            indicators["owner_gajami_varB"] = _og3_vb
                            indicators["owner_gajami_count"] = _og3_k
                            indicators["owner_gajami_ok"] = _og3_own

                            # ── 부모 swing 위치 취득 ──
                            # 4차가 만든 tlc_context_shadow 하나만 소비한다.
                            _j1_parent_ready = int(
                                _j1_tlc_ctx.get("parent_ready", 0) or 0
                            )
                            _j1_parent_pos = safe_extract_scalar(
                                _j1_tlc_ctx.get("parent_pos"),
                                SENTINEL_FLOAT,
                            )
                            _j1_parent_pos_closed = safe_extract_scalar(
                                _j1_tlc_ctx.get("parent_pos_closed"),
                                SENTINEL_FLOAT,
                            )
                            _j1_parent_wave_id = str(
                                _j1_tlc_ctx.get("parent_wave_id", "NONE")
                                or "NONE"
                            )

                            # 문지기 4봉 / J1 3봉의 기존 2단 DUR 계약은 그대로 유지한다.
                            _j1_dur_max = (
                                RECENT_GC_MAX_BARS - 1
                                if timeframe == "3m"
                                else RECENT_GC_MAX_BARS
                            )

                            # ── J1 저점 진입창 OPEN ──
                            # pposc = 현재 판정 시점의 "직전 확정봉" parent 위치.
                            # ppos  = 현재 live 가격의 parent 위치.
                            #
                            # 중요:
                            # pposc는 "과거에 어디서 출발했는가"를 저장한 값이 아니다.
                            # LOW_OPEN을 실제로 거쳤는지의 이력은 5-2에서
                            # cycle_parent_wave_id별 상태로 별도 기록한다.
                            #
                            # 0.40 / 0.70은 5차 구조의 최초 경계.
                            # 6차 실매매 누적 후 판사별 독립 튜닝한다.
                            _j1_low_window_open = bool(
                                _j1_parent_ready == 1
                                and _j1_parent_wave_id != "NONE"
                                # DUR은 현행 1-base READY 계약을 그대로 소비한다.
                                and _j1_ready
                                # LOW_OPEN 자체도 GC 자격을 완결적으로 가진다.
                                and _j1_gc
                                and _j1_dur <= _j1_dur_max
                                # 확정 구조가 부모 하위 40%, live도 70% 미만일 때만 OPEN
                                and 0.0 <= _j1_parent_pos_closed < 0.40
                                and 0.0 <= _j1_parent_pos < 0.70
                            )
                            indicators["j1_low_window_open"] = int(
                                _j1_low_window_open
                            )

                            # 🔒 [J1-PARENT-WINDOW-STATE-0829]
                            # 5-2/5-3: 같은 cycle_parent_wave_id에서
                            # J1 저점 기회는 한 번만 소유한다.
                            #
                            # UNSEEN -> OPEN:
                            #   이 parent에서 첫 LOW_OPEN을 관측.
                            #
                            # OPEN -> MISSED:
                            #   그 OPEN을 만든 확정 GC 진입 생애가 종료.
                            #
                            # MISSED는 같은 parent에서 terminal.
                            # 이후 local GC가 dur=1로 다시 태어나도 J1을 재개방하지 않는다.
                            #
                            # 위치 상단(ppos/pposc)으로 terminal 처리하지 않는다.
                            # 그것은 ★411과 같은 과교정을 재현하므로 제외한다.
                            _j1_wave_state = "UNSEEN"
                            _j1_wave_state_changed = False
                            if (
                                _j1_parent_ready == 1
                                and _j1_parent_wave_id != "NONE"
                            ):
                                _j1_cycle_state = getattr(
                                    TechnicalIndicators,
                                    "_cycle_phase_shadow_state",
                                    {},
                                ) or {}
                                _j1_record = dict(
                                    _j1_cycle_state.get(_jury_key, {}) or {}
                                )
                                _j1_saved_wave = str(
                                    _j1_record.get(
                                        "j1_wave",
                                        "NONE",
                                    ) or "NONE"
                                )
                                _j1_wave_state = str(
                                    _j1_record.get(
                                        "j1_wst",
                                        "UNSEEN",
                                    ) or "UNSEEN"
                                )
                                # 부모파동 자체가 바뀌었을 때만 새 J1 생애를 연다.
                                if _j1_saved_wave != _j1_parent_wave_id:
                                    _j1_saved_wave = _j1_parent_wave_id
                                    _j1_wave_state = "UNSEEN"
                                    _j1_wave_state_changed = True
                                # 이 parent의 첫 저점 진입창.
                                if (
                                    _j1_wave_state == "UNSEEN"
                                    and _j1_low_window_open
                                ):
                                    _j1_wave_state = "OPEN"
                                    _j1_wave_state_changed = True
                                # OPEN했던 확정 GC 생애가 끝난 뒤에는
                                # 같은 parent의 이후 local GC 재탄생을 J1에 주지 않는다.
                                elif (
                                    _j1_wave_state == "OPEN"
                                    and not (
                                        _j1_gc
                                        and 1 <= _j1_dur <= _j1_dur_max
                                    )
                                ):
                                    _j1_wave_state = "MISSED"
                                    _j1_wave_state_changed = True
                                _j1_record["j1_wave"] = _j1_saved_wave
                                _j1_record["j1_wst"] = _j1_wave_state
                                _j1_cycle_state[_jury_key] = _j1_record
                                setattr(
                                    TechnicalIndicators,
                                    "_cycle_phase_shadow_state",
                                    _j1_cycle_state,
                                )
                                # 상태전이 직후 60초 내 재시작돼도
                                # OPEN/MISSED를 잃지 않도록 즉시 영속화한다.
                                if _j1_wave_state_changed:
                                    TechnicalIndicators._save_cycle_phase_state_cache(
                                        force=True
                                    )
                            indicators["j1_wave_state"] = _j1_wave_state
                            # 🔒 [J1-WINDOW-MID-OWNER-0828] OBSERVE ONLY.
                            # cycle_window_pos는 df 전체 window 위치다.
                            # 5-1부터 J1 위치 판정권은 parent swing(ppos/pposc)로 이관한다.
                            _j1_window_pos = safe_extract_scalar(
                                indicators.get("cycle_window_pos"),
                                SENTINEL_FLOAT,
                            )
                            _j1_window_mid_block = bool(
                                0.40 <= _j1_window_pos < 0.70
                            )

                            # 🔒 [J1-DUR-SHIFT-0817]
                            # 3m 진입권은 기존 dur1~3 계약 유지.
                            _judge1_pass = bool(
                                _j1_ready
                                and _j1_gc
                                and _j1_dur <= _j1_dur_max
                                # 🔒 [J1-PARENT-WINDOW-STATE-0829]
                                # 같은 parent에서 첫 저점 GC 기회를 놓친 뒤(MISSED),
                                # 또는 실제 J1 BUY 체결로 소유권을 사용한 뒤(USED)는
                                # 같은 parent의 local GC 재탄생으로 J1을 다시 열지 않는다.
                                # J2/J3/J4와 _judge1_veto에는 관여하지 않는다.
                                and _j1_wave_state not in ("MISSED", "USED")
                                and SoleusGajamiConstants.ma5_alive_ratio(_j1_s5)
                                and _j1_body_growth_ok
                                # 🔒 [J1-LOW-WINDOW-HARD-ELIGIBILITY-ROLLBACK-0829] ★411
                                # LOW_WINDOW 정의·계산·관측(pready/pwave/ppos/pposc/lowopen)은
                                # 그대로 둔다. 철회하는 것은 이 한 줄, 즉 LOW_OPEN을 J1 PASS의
                                # 즉시 hard AND로 쓰던 적용 방식뿐이다.
                                # 5-1 배포(01:35) 직후 J1 PASS가 2.08%→0.28%로 급감했고
                                # ★380-R2(07:09)·★393(07:40)은 그보다 늦어 원인이 아니다.
                                # LOW_OPEN은 순간 게이트라 시간에 걸친 상태를 추적하지
                                # 않는데, 그 정의를 5-2(창 이력 기록)·5-3(late/high 종료)
                                # 없이 곧바로 판정에 편입한 것이 설계 오류였다.
                                # 관측 4곳(계산/지표export/jury_sig/로그)은 표본이 쌓일
                                # 때까지 그대로 남기고, 근거가 서면 재도입한다.
                                and bool(soleus_gajami)
                                and not _j1_tlc_bad_timing
                            )
                            _judge1_veto = bool(
                                _j1_ready
                                and (
                                    not SoleusGajamiConstants.ma5_alive_ratio(_j1_s5)
                                    or not _j1_body_growth_ok
                                )
                            )
                            _judge1_verdict = (
                                "VETO"
                                if _judge1_veto
                                else "PASS"
                                if _judge1_pass
                                else "NEUTRAL"
                            )

                            # ── J2: RSI + RSI/MACD 다이버전스 ──
                            # BB·KC·Cycle Phase·DET14는 J2 판정에 사용하지 않는다.
                            # 단 Active TLC live *_LL은 local hidden의 수학적 반대조건이 아니라
                            # 상위 가격구조 위험문맥으로만 소비한다(★518-R1, 2026-09-05).
                            _j2_div_ready = bool(
                                indicators.get(
                                    "_divergence_shadow_ready",
                                    False,
                                )
                            )
                            # 🔒 [J2-HIDDEN-OSC-COHERENCE-0904] ★504-R1
                            # raw OR는 진단/legacy shadow용으로 보존하고,
                            # 메인 J2 판정만 oscillator coherence SSoT를 소비한다.
                            _j2_hidden_raw = bool(
                                _j2_div_ready
                                and (
                                    indicators.get(
                                        "divergence_rsi_bull_hid",
                                        False,
                                    )
                                    or indicators.get(
                                        "divergence_macd_bull_hid",
                                        False,
                                    )
                                )
                            )
                            _j2_hidden = bool(
                                _j2_div_ready
                                and indicators.get(
                                    "div_j2_hidden_coherent",
                                    False,
                                )
                            )
                            _j2_bear_raw = bool(
                                _j2_div_ready
                                and (
                                    indicators.get(
                                        "divergence_rsi_bear_reg",
                                        False,
                                    )
                                    or indicators.get(
                                        "divergence_macd_bear_reg",
                                        False,
                                    )
                                )
                            )
                            _j2_rsi = safe_extract_scalar(
                                indicators.get("rsi14"),
                                SENTINEL_FLOAT,
                            )

                            _j2_ready = bool(
                                indicators.get("indicator_shadow_bar")
                                and math.isfinite(_j2_rsi)
                                and 0.0 <= _j2_rsi <= 100.0
                            )
                            _j2_overheat = bool(
                                _j2_ready
                                and _j2_rsi >= 70.0
                            )
                            _j2_rsi_oversold = bool(
                                _j2_ready
                                and _j2_rsi < 40.0
                            )
                            # 🔒 [J2-TEXTBOOK-0817] 교과서적 평균회귀 조합만 PASS.
                            # 🔒 [J2-MEANREV-RAW-PRESERVE-0904]
                            # legacy meanrev는 비교 SHADOW이므로 기존 raw OR hidden을 유지한다.
                            _j2_meanrev_go = bool(
                                _j2_rsi_oversold
                                and _j2_hidden_raw
                            )
                            _j2_bear_context = bool(
                                _j2_ready
                                and _j2_bear_raw
                                and _j2_overheat
                            )

                            # 일반 약세 다이버전스는 RSI 과열과 결합될 때만 VETO한다.
                            _judge2_veto = bool(_j2_bear_context)
                            # 🔒 [J2-HIDDEN-MACD-0822] hidden bullish divergence를 J2 본축으로 유지.
                            # 🔒 [J2-MACD-BELOW-FLIP-0827] MACD 0선 방향만 실측 근거로 BELOW 교정.
                            # 2026-08-22~27 6일 인과라벨: macdA=1 n=1,490 44.1%(-0.48%p 3/6)
                            # vs macdA=0 n=702 49.4%(+6.33%p 6/6, 일자별 부호 6/6).
                            _j2_macd_above_zero = bool(
                                indicators.get("macd_a_above_zero", False)
                            )
                            # 🔒 [J2-OVERHEAT-BLOCK-0822] RSI>=70 꼭대기 hidden은 기존대로 제외.
                            # 🔒 [J2-HIDDEN-RANGING-MA20-CONTEXT-0830] ★450
                            # Hidden continuation은 기존 RANGING MA20 collapse 판정을
                            # 뒤집지 않는다. t5 L10216이 이미 산출한 값을 소비만 한다.
                            # 근거: BORA 11:28은 같은 시점 시스템이 이미
                            # RANGING-MA20-COLLAPSE를 산출했는데 J2가 단독 PASS했다.
                            # 🔒 [J2-HIDDEN-STRUCT-CONTEXT-0905] ★518-R1
                            # hidden bullish는 local fractal price HL + oscillator LL이다.
                            # Active TLC live *_LL은 더 넓은 DC swing 문맥이므로 local hidden의
                            # 수학적 부정은 아니다. 다만 상위 가격구조가 저점을 갱신 중이라는
                            # 위험 문맥이므로 J2 단독 continuation PASS만 context guard로 차단한다.
                            # 다른 판사(J1/J3/J4)의 독립 PASS는 그대로 유지한다.
                            # DOWN_LEG/phase/DET14 자체는 veto하지 않는다.
                            _j2_hidden_structure = str(
                                indicators.get(
                                    "cycle_active_structure",
                                    "UNCONFIRMED",
                                )
                                or "UNCONFIRMED"
                            )
                            _j2_hidden_structure_ok = bool(
                                not _j2_hidden_structure.endswith("_LL")
                            )
                            _j2_hidden_go = bool(
                                _j2_ready
                                and _j2_div_ready
                                and _j2_hidden
                                and _j2_hidden_structure_ok
                                and not _j2_macd_above_zero
                                and not _j2_bear_raw
                                and not _j2_overheat
                                and not bool(int(indicators.get(
                                    "gajami_ranging_ma20_collapse_block", 0) or 0))
                            )
                            _j2_momentum_go = bool(
                                _j2_ready
                                and _j2_div_ready
                                and 65.0 <= _j2_rsi < 70.0
                                and not _j2_bear_raw
                            )
                            _judge2_pass = bool(_j2_hidden_go)
                            _judge2_verdict = (
                                "VETO"
                                if _judge2_veto
                                else "PASS"
                                if _judge2_pass
                                else "NEUTRAL"
                            )
                            # 🔒 [J4-GC-ORIGIN-SSOT-0829] ★414-R1
                            # ARM=regular bullish∧MACD<=0, FIRE=확정봉 반전캔들.
                            # GC origin SSoT가 fake reset이면 J4도 승인하지 않는다.
                            _j4_macd_below_zero = not bool(
                                indicators.get("macd_a_above_zero", False)
                            )
                            # 🔒 [J4-REG-RAW-SSOT-0906] ★526
                            # divergence 존재(raw OR)와 J4 품질(coherence)을 분리한다.
                            _j4_regular_raw = bool(
                                indicators.get("div_regular_bull", False)
                            )
                            _j4_regular = bool(
                                indicators.get("div_regular_bull_coherent", False)
                            )
                            _j4_reversal_candle_event = bool(
                                indicators.get("cycle_reversal_candle_bull", False)
                            )
                            _judge4_reversal_pass = bool(
                                _j2_ready
                                and _j2_div_ready
                                and _j4_regular
                                and _j4_macd_below_zero
                                and _j4_reversal_candle_event
                                and not bool(int(indicators.get("gajami_fake_late_reset_gc", 0) or 0))
                                and not _j2_bear_raw
                            )
                            indicators["judge4_reversal_pass"] = int(
                                _judge4_reversal_pass
                            )
                            # 🔒 [DIV-CLASS-GATE-1109] 분류판 J4 = 위 J4 와 같은 조건에서 「일반 다이버전스」 자리만 「W ∧ (J4 일반 · J4-1 과장 · J4-2 트리플)」
                            # 실제 표 교체 = 6_strategy J2 입구 (config [DIVERGENCE] div_class_gate · 기본 켬)
                            _dcg_j4 = bool(
                                str(indicators.get("div_class_shape", "")) == "W"
                                and str(indicators.get("div_class_type", "")) in ("J4_REGULAR", "J4_1_EXAG", "J4_2_TRIPLE")
                            )
                            _judge4_class_pass = bool(
                                _j2_ready
                                and _j2_div_ready
                                and _dcg_j4
                                and _j4_macd_below_zero
                                and _j4_reversal_candle_event
                                and not bool(int(indicators.get("gajami_fake_late_reset_gc", 0) or 0))
                                and not _j2_bear_raw
                            )
                            indicators["judge4_class_pass"] = int(_judge4_class_pass)
                            # ── KC setup SHADOW + J3×J2 활성 진입 SSoT ──
                            # raw KC setup 값은 로그·indicators 관측만 유지하고, 진입 승인에는 사용하지 않는다.
                            _coil_s5_closed_pct = safe_extract_scalar(
                                indicators.get("ma5_slope_exit_closed"),
                                SENTINEL_FLOAT,
                            )
                            _coil_setup_ready, _coil_setup_ok, _coil_squeeze_ratio = (
                                calculate_coil_setup(
                                    squeeze_ready=bool(
                                        indicators.get("keltner_squeeze_ready", False)
                                    ),
                                    squeeze_on=bool(
                                        indicators.get("keltner_had_squeeze", False)
                                    ),
                                    squeeze_ratio=safe_extract_scalar(
                                        indicators.get("keltner_squeeze_ratio"),
                                        SENTINEL_FLOAT,
                                    ),
                                    ma5_slope_ratio=(
                                        _coil_s5_closed_pct / 100.0
                                        if _coil_s5_closed_pct != SENTINEL_FLOAT
                                        else SENTINEL_FLOAT
                                    ),
                                )
                            )
                            # KC/coil 판정을 J3(IGNITION+수렴선행)로 이관.
                            # raw KC×J2 혼합 철거 → J3 정식판결 × J2.
                            # 키 이름은 T6의 기존 소비 계약을 위해 유지한다.
                            # J3 계산 실패·미산출 시 0으로 처리하여 승인하지 않는다(fail-closed).
                            # [2026-07-23 우회 shadow화] coil_j2가 헌법·STALE·soleus를
                            # 모두 뒤집어 RE(06:00)·TREE(12:12) dur6·const0 오진입 통과시킴.
                            # J2/J3 계산·로그는 유지(판사 관측), 진입 승인만 회수.
                            # Analysis BLOCK은 어떤 lane도 PASS로 못 뒤집는다(개헌).
                            _coil_j2_entry_pass = False

                            # ── J2 legacy mean-reversion SHADOW ──
                            # 메인 J2 PASS는 J2-HIDDEN-MACD-0822가 담당한다.
                            # meanrev는 비교 로그용으로만 보존하며 judge_lane_authorized에는 편입하지 않는다.
                            _judge2_meanrev_ready = bool(_j2_ready)
                            # 원래 meanrev 조건식은 관측용으로 독립 계산한다.
                            _judge2_meanrev_pass = bool(
                                _j2_ready
                                and _j2_meanrev_go
                                and not _j2_overheat
                                and not _judge2_veto
                            )
                            _judge2_meanrev_verdict = (
                                "PASS" if _judge2_meanrev_pass else "NEUTRAL"
                            )

                            # 🔒 [OWNER-GAJAMI-3COND-OBSERVE-0828]
                            # OG3는 관측 전용이다. LOW_OPEN/J1 판정에는 사용하지 않는다.
                            # R1에서 이미 앞으로 이동된 계산 블록은 불필요한 왕복 수술을 피하기 위해 현 위치를 유지한다.

                            # 🔒 [JUDGE-POSITIVE-OR-0817] 판사 승인 SSoT.
                            # 절대 부정은 헌법/Core가 담당하고 여기서는 긍정표만 합친다.
                            _judge_lane_authorized = bool(
                                _judge1_pass
                                or _judge2_pass
                                or _judge3_cycle_pass
                                or _judge4_reversal_pass
                            )

                            indicators[
                                "judge1_gajami_pass"
                            ] = int(_judge1_pass)
                            indicators[
                                "judge1_gajami_veto"
                            ] = int(_judge1_veto)
                            indicators[
                                "judge1_gajami_verdict"
                            ] = _judge1_verdict

                            indicators[
                                "judge2_osc_pass"
                            ] = int(_judge2_pass)
                            indicators[
                                "judge2_osc_veto"
                            ] = int(_judge2_veto)
                            indicators[
                                "judge2_osc_verdict"
                            ] = _judge2_verdict
                            # 🔒 [J2-PRODUCER-CONSUMER-INTEGRITY-0902] ★478-R2
                            # 기존 judge2_osc_* scalar는 호환용으로 유지한다.
                            # 이 순간의 Producer 판결을 별도 키에 봉인하여
                            # 이후 handoff/mutation이 scalar만 바꾸면 T6에서 검출한다.
                            # 판정식·threshold·다이버전스 계산은 변경하지 않는다.
                            indicators["judge2_prov_ready"] = 1
                            indicators["judge2_prov_pass"] = int(
                                _judge2_pass
                            )
                            indicators["judge2_prov_verdict"] = str(
                                _judge2_verdict
                            )
                            indicators["judge2_prov_bar"] = str(
                                _jury_ctx_bar or ""
                            )
                            indicators["judge2_prov_hidgo"] = int(
                                _j2_hidden_go
                            )

                            indicators[
                                "judge2_meanrev_ready"
                            ] = int(_judge2_meanrev_ready)
                            indicators[
                                "judge2_meanrev_pass"
                            ] = int(_judge2_meanrev_pass)
                            indicators[
                                "judge2_meanrev_verdict"
                            ] = _judge2_meanrev_verdict
                            indicators[
                                "judge2_meanrev_bar"
                            ] = str(_jury_ctx_bar or "")

                            indicators["coil_setup_ready"] = int(_coil_setup_ready)
                            indicators["coil_setup_ok"] = int(_coil_setup_ok)
                            indicators["coil_squeeze_ratio"] = float(
                                _coil_squeeze_ratio
                            )
                            indicators["coil_j2_entry_pass"] = int(
                                _coil_j2_entry_pass
                            )
                            indicators["coil_entry_bar"] = str(_jury_ctx_bar or "")
                            indicators["judge_lane_authorized"] = int(
                                _judge_lane_authorized
                            )

                            # 같은 현재봉에서는 판결 조합이 바뀔 때만 다시 기록한다.
                            _jury_sig = (
                                _jury_bar,
                                _judge1_verdict,
                                int(_j1_body_growth_ok),
                                str(indicators.get("cycle_phase_shadow", "?")),
                                _judge2_verdict,
                                str(indicators.get("judge3_cycle_verdict", "NEUTRAL")),
                                int(_judge4_reversal_pass),
                                _judge2_meanrev_verdict,
                                int(_coil_j2_entry_pass),
                                int(_judge_lane_authorized),
                                _j1_obs_leg,
                                _j1_obs_pstruct,
                                _j1_obs_true_gc,
                                _j1_obs_reset_mom,
                                _j1_obs_fake_reset,
                                int(_og3_own),
                                int(_j1_low_window_open),
                                _j1_parent_wave_id,
                                int(_j1_mature_top),
                                _j1_live_structure,
                                int(_j1_countertrend_rebound),
                                # 🔒 [J1-PARENT-WST-JURY-SIG-0829]
                                # 상태전이 자체도 JUDGE1 dedup signature에 포함한다.
                                # UNSEEN/OPEN/MISSED 전환이 다른 판정값 변화 없이
                                # 발생해도 wst 로그를 반드시 남긴다.
                                _j1_wave_state,
                            )
                            _jury_seen = getattr(
                                TechnicalIndicators,
                                "_entry_jury_shadow_state",
                                {},
                            )

                            if (
                                _jury_bar
                                and _jury_seen.get(_jury_key)
                                != _jury_sig
                            ):
                                _jury_seen[_jury_key] = _jury_sig
                                setattr(
                                    TechnicalIndicators,
                                    "_entry_jury_shadow_state",
                                    _jury_seen,
                                )

                                logger.info(
                                    f"⚖️ [JUDGE1-GAJAMI] {symbol} | "
                                    f"tf={_jury_tf} "
                                    f"bar={_jury_bar} "
                                    f"ctx={_jury_ctx_bar} "
                                    f"J1={int(_judge1_pass)} "
                                    f"verdict={_judge1_verdict} | "
                                    f"ready={int(_j1_ready)} "
                                    f"gc={int(_j1_gc)} "
                                    f"golden={_j1_golden} "
                                    f"dur={_j1_dur} "
                                    # 🔒 [JURY-LOG-DECISION-VAR-PARITY-0902] ★480
                                    # 판정은 `_j1_dur <= _j1_dur_max`인데
                                    # dur만 찍히고 상한이 없어 문턱 재현이 불가능했다.
                                    # 문지기 4봉 / J1 3봉의 2단 DUR 계약을 그대로 관측한다.
                                    f"durmax={_j1_dur_max} "
                                    f"s5={_j1_s5:+.6f} "
                                    f"alive_floor="
                                    f"{-SoleusGajamiConstants.MA5_SLOPE_ALIVE_DIP_RATIO:.6f} "
                                    f"s10={_j1_s10:+.6f} "
                                    f"s20={_j1_s20:+.6f} "
                                    f"body={int(_j1_body_growth_ok)} "
                                    f"wpos={_j1_window_pos:.3f} "
                                    f"pready={int(_j1_parent_ready)} "
                                    f"ppos={_j1_parent_pos:.3f} "
                                    f"pposc={_j1_parent_pos_closed:.3f} "
                                    f"lowopen={int(_j1_low_window_open)} "
                                    f"pwave={_j1_parent_wave_id} "
                                    f"wst={_j1_wave_state} "
                                    f"wmid={int(_j1_window_mid_block)} "
                                    f"trend_dead={int(_j1_trend_dead)} "
                                    f"tlc={indicators.get('cycle_phase_shadow', '?')} "
                                    f"det={indicators.get('cycle_detail_shadow', '?')} "
                                    f"family={_j1_tlc_family or '?'} "
                                    f"tage={indicators.get('cycle_phase_age_shadow', -1)} "
                                    f"apos={safe_extract_scalar(indicators.get('cycle_active_pos'), 0.5):.6f} "
                                    f"aposc={safe_extract_scalar(indicators.get('cycle_active_pos_closed'), 0.5):.6f} "
                                    f"mature_top={int(_j1_mature_top)} "
                                    f"ctreb={int(_j1_countertrend_rebound)} "
                                    f"tlc_bad={int(_j1_tlc_bad_timing)} "
                                    f"struct={_j1_live_structure} "
                                    f"atrp={safe_extract_scalar(indicators.get('atr_pct'), SENTINEL_FLOAT):.6f} "
                                    f"soleus="
                                    f"{'PASS' if soleus_gajami else 'BLOCK'} "
                                    f"reason={_final_reason} | "
                                    # 🔒 [J1-TLC-OBSERVE-0827] 관측 전용, 판정 영향 없음
                                    f"leg={_j1_obs_leg} "
                                    f"pstruct={_j1_obs_pstruct} "
                                    f"true_gc={_j1_obs_true_gc} "
                                    f"reset_mom={_j1_obs_reset_mom} "
                                    f"fake_reset={_j1_obs_fake_reset} "
                                    f"down_leg={int(_j1_obs_leg == 'DOWN_LEG')} | "
                                    # 🔒 [OWNER-GAJAMI-3COND-OBSERVE-0828] OG3는 관측 전용 — LOW_OPEN 판정 미사용
                                    f"og3_ready={_og3_ready} "
                                    f"og3_k={_og3_k} "
                                    f"og3_v={_og3_c1} "
                                    f"og3_surge={_og3_c2} "
                                    f"og3_hold={_og3_c3} "
                                    f"og3_vA={_og3_va} "
                                    f"og3_vB={_og3_vb} "
                                    f"og3_own={_og3_own} "
                                    f"(Strategy Core 약한 J1 lane 보완)"
                                )
                                # 🔒 [J1-FIDELITY-COMPACT-0915] ★676-R1
                                # ★§675 : Gate① miss 의 J1 축 최초 divergence 입력이
                                #  이 줄에만 있는데 200s 스로틀에 잘린다. 전체 면제는
                                #  803B/행이라 과하므로 판정 입력만 compact 로 복제한다.
                                # ★§678 BLOCKER-2 : bar/ctx/dur 는 ★causal join key 다.
                                #  벽시계 timestamp 만으로 instance 를 맞추지 않는다
                                #  (KAT 은 dur 도 divergence 였다).
                                #  ★행동 변화 없음 — 로그만 추가한다.
                                logger.info(
                                    f"⚖️ [J1-FID] {symbol} "
                                    f"J1={int(_judge1_pass)} "
                                    f"v={_judge1_verdict} "
                                    f"bar={_jury_bar} "
                                    f"ctx={_jury_ctx_bar} "
                                    f"dur={_j1_dur} "
                                    f"rdy={int(_j1_ready)} "
                                    f"gld={_j1_golden} "
                                    f"s5={_j1_s5:+.6f} "
                                    f"s10={_j1_s10:+.6f}"
                                )

                                logger.info(
                                    f"⚖️ [JUDGE2-OSC] {symbol} | "
                                    f"tf={_jury_tf} "
                                    f"bar={_jury_bar} "
                                    f"ctx={_jury_ctx_bar} "
                                    f"J2={int(_judge2_pass)} "
                                    f"verdict={_judge2_verdict} | "
                                    f"ready={int(_j2_ready)} "
                                    f"div_ready={int(_j2_div_ready)} "
                                    f"hidden={int(_j2_hidden)} "
                                    f"hidraw={int(_j2_hidden_raw)} "
                                    f"macdA={int(_j2_macd_above_zero)} "
                                    f"macdB={int(indicators.get('macd_b_hist_contracting', False))} "
                                    f"macdC={int(indicators.get('macd_c_golden_cross', False))} "
                                    f"macdD={int(indicators.get('macd_d_hist_flip_up', False))} "
                                    f"rsihid={int(indicators.get('div_rsi_hid', False))} "
                                    f"macdhid={int(indicators.get('div_macd_hid', False))} "
                                    f"dblhid={int(indicators.get('div_double_hidden', False))} "
                                    # 🔒 [J2-PIVOT-AGE-OBSERVE-0828] 관측 전용
                                    f"divage={int(indicators.get('divergence_pivot_age', -1))} "
                                    f"sigage={int(indicators.get('divergence_signal_age', -1))} "
                                    f"divdist={int(indicators.get('divergence_pivot_distance', -1) or -1)} "
                                    f"divbar={indicators.get('divergence_pivot_bar', 'NA')} "
                                    f"divconfirm={indicators.get('divergence_confirm_bar', 'NA')} "
                                    f"p1={safe_extract_scalar(indicators.get('divergence_hidden_price1'), SENTINEL_FLOAT):.6g} "
                                    f"p2={safe_extract_scalar(indicators.get('divergence_hidden_price2'), SENTINEL_FLOAT):.6g} "
                                    f"r1={safe_extract_scalar(indicators.get('divergence_hidden_rsi1'), SENTINEL_FLOAT):.2f} "
                                    f"r2={safe_extract_scalar(indicators.get('divergence_hidden_rsi2'), SENTINEL_FLOAT):.2f} "
                                    f"m1={safe_extract_scalar(indicators.get('divergence_hidden_macd1'), SENTINEL_FLOAT):+.5f} "
                                    f"m2={safe_extract_scalar(indicators.get('divergence_hidden_macd2'), SENTINEL_FLOAT):+.5f} "
                                    f"hidgo={int(_j2_hidden_go)} momgo={int(_j2_momentum_go)} "
                                    # 🔒 [J2-HIDDEN-STRUCT-CONTEXT-0905] ★518-R1 로그 정합
                                    f"struct={_j2_hidden_structure} "
                                    f"strok={int(_j2_hidden_structure_ok)} "
                                    # 🔒 [J2-HIDDEN-RANGING-MA20-CONTEXT-0830] ★450 로그 정합
                                    f"rgm20col={int(bool(indicators.get('gajami_ranging_ma20_collapse_block', 0)))} "
                                    f"j4={int(_judge4_reversal_pass)} "
                                    f"reg={int(_j4_regular)} "
                                    f"lowrsi={safe_extract_scalar(indicators.get('div_low_rsi'), float('nan')):.1f} "
                                    f"rsi_os={int(_j2_rsi_oversold)} "
                                    f"meanrev={int(_j2_meanrev_go)} | "
                                    f"rsi={_j2_rsi:.2f} "
                                    f"bear_raw={int(_j2_bear_raw)} "
                                    f"bear_ctx={int(_j2_bear_context)} "
                                    f"overheat={int(_j2_overheat)} "
                                    f"soleus="
                                    f"{'PASS' if soleus_gajami else 'BLOCK'} "
                                    f"reason={_final_reason} "
                                    f"(Strategy Core 약한 J1 lane 보완)"
                                )

                                # 🔒 [J4-GC-ORIGIN-SSOT-0829] 판단·TLC·origin을 같은 Jury 로그로 기록.
                                logger.info(
                                    f"⚖️ [JUDGE4-REVERSAL] {symbol} | "
                                    f"tf={_jury_tf} "
                                    f"bar={_jury_bar} "
                                    f"ctx={_jury_ctx_bar} "
                                    f"J4={int(_judge4_reversal_pass)} "
                                    f"verdict={'PASS' if _judge4_reversal_pass else 'NEUTRAL'} | "
                                    f"ready={int(_j2_ready)} "
                                    f"div_ready={int(_j2_div_ready)} "
                                    f"regraw={int(_j4_regular_raw)} "
                                    f"reg={int(_j4_regular)} "
                                    f"reg_rsi={int(indicators.get('divergence_rsi_bull_reg', False))} "
                                    f"reg_macd={int(indicators.get('divergence_macd_bull_reg', False))} "
                                    f"macd_below={int(_j4_macd_below_zero)} "
                                    f"candle_event={int(_j4_reversal_candle_event)}"
                                    f"/{indicators.get('cycle_reversal_candle_kind', '-')} "
                                    f"divgap={safe_extract_scalar(indicators.get('divergence_reg_gap_abs'), -1.0):.6g} "
                                    f"divfloor={safe_extract_scalar(indicators.get('divergence_atr_floor'), -1.0):.6g} "
                                    # 🔒 [J4-REG-PIVOT-OBSERVE-0902-R1] ★485-R1
                                    # regular bullish의 실제 price pivot 쌍과 그 시점의
                                    # RSI/MACD 대응값·신선도를 한 줄에서 검증한다.
                                    f"rp1={safe_extract_scalar(indicators.get('divergence_reg_price1'), SENTINEL_FLOAT):.8g} "
                                    f"rp2={safe_extract_scalar(indicators.get('divergence_reg_price2'), SENTINEL_FLOAT):.8g} "
                                    f"rr1={safe_extract_scalar(indicators.get('divergence_reg_rsi1'), SENTINEL_FLOAT):.4f} "
                                    f"rr2={safe_extract_scalar(indicators.get('divergence_reg_rsi2'), SENTINEL_FLOAT):.4f} "
                                    f"rm1={safe_extract_scalar(indicators.get('divergence_reg_macd1'), SENTINEL_FLOAT):+.8f} "
                                    f"rm2={safe_extract_scalar(indicators.get('divergence_reg_macd2'), SENTINEL_FLOAT):+.8f} "
                                    f"rdist={int(indicators.get('divergence_reg_pivot_distance', -1))} "
                                    f"rage={int(indicators.get('divergence_reg_pivot_age', -1))} "
                                    f"rsigage={int(indicators.get('divergence_reg_signal_age', -1))} "
                                    f"rbar={str(indicators.get('divergence_reg_pivot_bar', 'NA')).replace(' ', 'T')} "
                                    f"rconfirm={str(indicators.get('divergence_reg_confirm_bar', 'NA')).replace(' ', 'T')} "
                                    f"fake_reset={int(bool(indicators.get('gajami_fake_late_reset_gc', 0)))} "
                                    f"true_gc={_j1_obs_true_gc} reset_mom={_j1_obs_reset_mom} | "
                                    f"tlc={_j1_tlc_ctx.get('phase', '?')} det={_j1_tlc_ctx.get('detail', '?')} "
                                    f"leg={_j1_obs_leg} struct={_j1_tlc_ctx.get('structure', '?')} pstruct={_j1_obs_pstruct} "
                                    f"aposc={safe_extract_scalar(indicators.get('cycle_active_pos_closed'), 0.5):.6f} "
                                    f"lbar={int(indicators.get('cycle_active_leg_bars', 0) or 0)} "
                                    f"longsd={int(bool(indicators.get('gajami_long_ma_strong_down_veto', 0)))} | "
                                    f"lowrsi={safe_extract_scalar(indicators.get('div_low_rsi'), float('nan')):.1f} "
                                    f"rsi={_j2_rsi:.2f} "
                                    f"bear_raw={int(_j2_bear_raw)} "
                                    f"soleus={'PASS' if soleus_gajami else 'BLOCK'} "
                                    f"reason={_final_reason}"
                                )

                                logger.info(
                                    f"⚖️ [JUDGE2-MEANREV] {symbol} | "
                                    f"tf={_jury_tf} "
                                    f"bar={_jury_ctx_bar} "
                                    f"ready={int(_judge2_meanrev_ready)} "
                                    f"J2={_judge2_verdict} "
                                    f"lane={int(_judge2_meanrev_pass)} "
                                    f"verdict={_judge2_meanrev_verdict} | "
                                    f"rsi_os={int(_j2_rsi_oversold)} "
                                    # 🔒 [J2-MEANREV-RAW-PRESERVE-0904]
                                    # legacy meanrev SHADOW는 메인 J2 coherence와 분리해
                                    # 기존 raw OR hidden 관측을 그대로 유지한다.
                                    f"hidden={int(_j2_hidden_raw)} "
                                    f"bear={int(_j2_bear_raw)} "
                                    # 🔒 [JURY-LOG-DECISION-VAR-PARITY-0902] ★480
                                    # _judge2_meanrev_pass 4조건 중 overheat·veto가 없어
                                    # 이 판사만 로그로 판정 재현이 불가능했다.
                                    f"overheat={int(_j2_overheat)} "
                                    f"veto={int(_judge2_veto)}"
                                )

                                logger.info(
                                    f"🔥 [J3-J2-ENTRY] {symbol} | "
                                    f"tf={_jury_tf} bar={_jury_ctx_bar} "
                                    f"phase={indicators.get('cycle_phase_shadow', 'NA')} "
                                    f"J3={indicators.get('judge3_cycle_verdict', 'NEUTRAL')} "
                                    f"kc_release={int(_kc_release)} "
                                    f"J2={_judge2_verdict} "
                                    f"active={int(_coil_j2_entry_pass)} "
                                    f"authorized={int(_judge_lane_authorized)} "
                                    f"constitution={int(bool(_gajami_constitution_ok))} | "
                                    f"coil_shadow_ready={int(_coil_setup_ready)} "
                                    f"setup={int(_coil_setup_ok)} "
                                    f"ratio={_coil_squeeze_ratio:.4f} "
                                    f"dur={_j1_dur}"
                                )

                        except Exception as _jury_e:
                            logger.info(
                                f"⚠️ [ENTRY-JURY] "
                                f"{symbol} 계산 실패 | "
                                f"tf="
                                f"{timeframe.value if hasattr(timeframe, 'value') else timeframe} | "
                                f"bar={indicators.get('current_candle_time')} "
                                f"ctx={indicators.get('indicator_shadow_bar')} | "
                                f"reason={_jury_e}"
                            )

                        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
                        # 🔬 진단: CORE 통과했는데 최종 탈락 케이스 (억울한 차단 검증)
                        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
                        if _core_shape_ok and not soleus_gajami:
                            try:
                                sym = str(indicators.get("symbol", "UNKNOWN"))
                                
                                # 어느 조건이 차단했는지 확인
                                reject_reason = []
                                if not gajami_physically_possible:
                                    reject_reason.append("PHYS")
                                if not _gajami_basic_ok:
                                    reject_reason.append("BASIC_LAW")
                                if not window_ok:
                                    reject_reason.append("WINDOW")
                                if indicators.get("gajami_neg_rise_block", 0):
                                    reject_reason.append("NEG_RISE")
                                if indicators.get("gajami_td_rg_top_newborn_followthrough_gate_block", 0):
                                    reject_reason.append("TD_RG_TOP_NEWBORN_FOLLOWTHROUGH")
                                if not entry_trigger_ok:
                                    reject_reason.append("TRIGGER")
                                if indicators.get("gajami_td_ma20_rebound_block", 0):
                                    reject_reason.append("TD_MA20_REBOUND")
                                if indicators.get("gajami_ranging_ma20_collapse_block", 0):
                                    reject_reason.append("RANGING_MA20_COLLAPSE")
                                if not (ma20_trend_ok or _birth_tlc_ok or _td_newborn_strong_recovery_exempt):
                                    reject_reason.append("MA20")
                                if indicators.get("gajami_long_ma_strong_down_veto", 0):
                                    reject_reason.append("LONG_STRONG_DOWN")
                                if indicators.get("gajami_td_ma60_down_veto", 0):
                                    reject_reason.append("TD_MA60_DOWN")
                                if not _td_rg_newborn_origin_required_ok:
                                    reject_reason.append("TD_RG_NEWBORN_ORIGIN")
                                if indicators.get("gajami_td_rg_top_whipsaw", 0):
                                    reject_reason.append("TD_RG_TOP_WHIPSAW")
                                if indicators.get("tlc_rg_whipsaw", 0):
                                    reject_reason.append("TLC_RG_WHIPSAW")
                                if indicators.get("tlc_deep_down", 0):
                                    reject_reason.append("TLC_DEEP_DOWN")
                                if indicators.get("gajami_top_flat_birth_block", 0):
                                    reject_reason.append("TOP_FLAT_BIRTH")
                                if indicators.get("gajami_fake_top_birth", 0):
                                    reject_reason.append("FAKE_TOP_BIRTH")
                                if indicators.get("gajami_top_signature_block", 0):
                                    reject_reason.append("TOP_SIGNATURE")

                                if gajami_lifecycle.get("state") == "DEAD":
                                    reject_reason.append(f"LIFECYCLE_{gajami_lifecycle.get('state')}")
                                reject_str = "+".join(reject_reason) if reject_reason else str(_fail_reason)
                                
                                logger.debug(
                                    f"[CORE-PASS-BUT-REJECT] {sym} | "
                                    f"reject={reject_str} | "
                                    f"phys={gajami_physically_possible} "
                                    f"win={window_ok} "
                                    f"trig={entry_trigger_ok} "
                                    f"m20={ma20_trend_ok} | "
                                    f"lifecycle={gajami_lifecycle.get('state','?')} "
                                    f"rise={gajami_lifecycle.get('rise_pct') or 0:.2%} "
                                    f"pb={_had_pullback}"
                                )
                            except Exception:
                                pass
                        
                        # soleus_candidate BOX 의존 제거 (lifecycle로 대체)
                        # lifecycle state (BIRTH/ALIVE)가 이미 초입 판단함
                        
                        # 디버깅 로그: 형성 완료 후 트리거 대기
                        if (gajami_shape_ok and gajami_physically_possible and window_ok) and not entry_trigger_ok:
                            logger.debug(
                                f"⏳ [SOLEUS-WAIT] {symbol} 형성 완료, 트리거 대기 중 | "
                                f"recent_cross={recent_cross_ok} intrabar={ma5_intrabar_cross_up}"
                            )
                        
                        # 디버깅: soleus 차단 사유 추적 (SSoT 결과 사용)
                        if not _core_shape_ok:
                            logger.debug(
                                f"[GAJAMI-SHAPE-FAIL] {symbol} | {gajami_shape_result.get('reason', 'NO_REASON')}"
                            )
                        if gc_5_10 and not soleus_gajami:
                            reject_reasons = []

                            if not _gajami_basic_ok:
                                reject_reasons.append("BASIC_LAW")

                            # ① 최근 골든크로스 체크
                            if not recent_cross_ok:
                                if recent_cross_reason:
                                    reject_reasons.append(recent_cross_reason)
                                else:
                                    reject_reasons.append(f"오래된골크(5x10:{dur_5x10_bars}봉)")
                            
                            # ②③④ 가자미근 형상 체크 (SSoT 함수 결과 사용)
                            if not _core_shape_ok:
                                # SSoT 함수의 차단 사유를 그대로 사용
                                reject_reasons.append(gajami_shape_result['reason'])
                            
                            # 물리성 체크
                            if not gajami_physically_possible:
                                reject_reasons.append("PHYS필터차단")
                            
                            # 윈도우 체크
                            if not window_ok:
                                reject_reasons.append("늦은진입차단")
                            
                            # 트리거 체크 (형성 완료 후 트리거 미발생)
                            if (gajami_shape_ok and gajami_physically_possible and window_ok) and not entry_trigger_ok:
                                reject_reasons.append("트리거미발생")

                            if indicators.get("gajami_fake_top_birth", 0):
                                reject_reasons.append("FAKE_TOP_BIRTH")
                            if indicators.get("gajami_top_signature_block", 0):
                                reject_reasons.append("TOP_SIGNATURE")

                            if reject_reasons:
                                logger.info(
                                    f"🚫 [SOLEUS-BLOCK][MA-ANALYSIS] {symbol} | {' | '.join(reject_reasons)}"
                                )
                        
                except Exception as e:
                    _soleus_analysis_error = (
                        f"{type(e).__name__}:{str(e)[:120]}"
                    )
                    indicators["gajami_analysis_error"] = (
                        _soleus_analysis_error
                    )
                    soleus_gajami = False
                    # 🔒 [JUDGE-ERROR-FAIL-CLOSED-0817] Analysis 오류 시 stale 승인 제거.
                    indicators["judge1_gajami_pass"] = 0
                    indicators["judge_lane_authorized"] = 0
                    gajami_lifecycle = {
                        "state": "ANALYSIS_INVALID_ERROR",
                        "birth_price": None,
                        "rise_pct": None,
                        "death_reason": f"ERROR:{_soleus_analysis_error}",
                        "dur": 999,
                    }
                    logger.exception(
                        f"🛑 [SOLEUS_GAJAMI] 계산 실패: "
                        f"{_soleus_analysis_error} "
                        f"→ ANALYSIS_INVALID_ERROR"
                    )
                    # ma20_ok은 독립 계산값이므로 예외로 덮어쓰지 않는다.

                # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
                # Soleus Zone 적용 가능 여부 (체제별 분리)
                # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
                # 철학: BREAKOUT/BULL은 돌파/고점 추종 전략 (저점 무관)
                #       RANGING/TD/TU는 저점 반등 전략 (Soleus 핵심)
                # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
                # Soleus 적용 가능 체제 판정
                # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
                # 철학:
                #   가자미근(Soleus) = 저점 반등 전략
                #   → 저점(reference_low) 기준이 유효한 체제에서만 성립
                #
                # 🟢 Soleus 성립 체제: RANGING, TRENDING_UP, TRENDING_DOWN
                #    → 저점 횡보/회복/시험 후 반등 전략
                #
                # 🔴 Soleus 비성립 체제: BREAKOUT, BULL_MARKET
                #    → 돌파/고점 추종 체제 (저점 기준 무의미)
                #    → 눌림/추격/모멘텀 전략 사용
                # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
                
                # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
                # Soleus Zone (가자미근 구간) 최종 판정
                # 철학: 발목 위–무릎 아래 범위만 판정 (체제는 가중치로만)
                # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
                # 🔒 HARD BLOCK: 역배열에서는 Soleus Zone 비활성화 (스칼라 기반)
                ma5_val = 0.0
                ma10_val = 0.0
                
                try:
                    s5 = mas.get("ma5")
                    s10 = mas.get("ma10")
                    if s5 is not None and len(s5) > 0:
                        ma5_val = float(s5.iloc[-1])
                    if s10 is not None and len(s10) > 0:
                        ma10_val = float(s10.iloc[-1])
                except Exception:
                    ma5_val = 0.0
                    ma10_val = 0.0
                
                # ✅ MA 배열 판정
                is_ma_aligned = (ma5_val > 0 and ma10_val > 0 and ma5_val >= ma10_val)
                
                # ✅ PATCH-TD: TD 가자미근 허용 범위 확대 (0.996 → 0.995)
                # - 목적: TD 저점 반등 초입 포착 (골든 근처/회복 허용)
                # - 효과: ma5가 ma10보다 0.5%까지 낮아도 Soleus Zone 성립
                # - 기존 PATCH-B (횡보-시동형) 통합: 0.995로 단일화
                is_ma_near_cross = (
                    ma5_val > 0
                    and ma10_val > 0
                    and ma5_val >= ma10_val * 0.995
                    and regime != MarketRegime.TRENDING_DOWN
                )
                                
                regime_str = regime.value if hasattr(regime, 'value') else str(regime)
                logger.debug(
                    f"[SOLEUS] {symbol} | tier={tier} regime={regime_str} "
                    f"low_area={is_low_area} "
                    f"soleus={soleus_gajami}"
                )
                
                # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
                # ✅ SSoT 최종: soleus_gajami 단일 판정
                # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
                # 철학: 형상(soleus_gajami) 확정 = 진입 자격 확정
                #       STEP 2(long_ma_bearish)에서 체제 불문 하드 차단 처리 완료
                # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
                
                # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
                # 🔒 Single Source of Truth: soleus_gajami
                # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
                # 아래는 모두 Strategy/로그 호환용 alias (판정 영향 없음)
                # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
                # 🔒 SSoT: soleus_gajami 단일 판정
                is_soleus_zone = soleus_gajami        # alias (호환/로그용)
                is_gajami_candidate = soleus_gajami   # alias (Strategy 호환)
                is_gajami_knee = soleus_gajami        # alias (디버그용)
                if not isinstance(gajami_lifecycle, dict):
                    gajami_lifecycle = {"state": "DEAD", "birth_price": None, "rise_pct": None, "death_reason": "INIT", "dur": 999}

                gajami_meta = {
                    "reason": (
                        "SOLEUS_PASS"
                        if soleus_gajami
                        else "SOLEUS_BLOCK"
                    ),
                    "soleus_zone": bool(soleus_gajami),
                    "is_candidate": bool(soleus_gajami),
                    "gajami_lifecycle": gajami_lifecycle or {},
                    "gajami_constitution_ok": _gajami_constitution_ok,
                    "gajami_constitution_effective_ok": _gajami_constitution_effective_ok,
                    "gajami_constitution_fail": str(_v2_ctn_fail or ""),
                    "regime_gate_ok": _regime_gate_ok,
                    "tier": tier,
                    "regime": regime.value if regime else "UNKNOWN",
                    "entry_state": None,
                }
                
                # (변수 초기화 제거 - gajami_meta에 직접 False 입력)

                # meta에 유형 기록 (Strategy에서 디버깅/정합성 판단용)
                
                # ✅ PATCH-C: 횡보-시동형(고개듦) 타입 기록
                # - sideways_detected: 횡보 구간
                # - is_ma_near_cross: MA5가 MA10 근접 (골크 직전/직후)
                # - ma5_slope > 0: MA5 상승 전환
                head_lift_trigger = False
                try:
                    ma5_slope_val = nzs(indicators.get("ma5_slope"))
                    head_lift_trigger = (
                        bool(is_sideways) and
                        bool(is_ma_near_cross) and
                        (ma5_slope_val > 0)
                    )
                except Exception:
                    head_lift_trigger = False

                gajami_meta["head_lift_trigger"] = bool(head_lift_trigger)
                # gajami_meta 최종 저장 — V2 diff/decision_v2/state_gc_fresh 장식값은 제거 완료
                indicators["gajami_meta"] = gajami_meta

            except Exception as e:
                # 🔧 indicators 스코프 버그 수정 후 이 블록은 도달하지 않아야 함
                logger.error(
                    f"❌ [{symbol}] 가자미근 계산 오류 (예상치 못한 예외) | "
                    f"error={str(e)[:100]} | Tier{tier}"
                )
                
                # 방어적으로 차단
                gajami_meta = {
                    "reason": f"calc_error:{str(e)[:100]}",
                    "soleus_zone": False,
                    "is_candidate": False,
                }
                is_gajami_candidate = False
                is_gajami_knee = False

        result['gajami_meta'] = gajami_meta
        # 🔒 alias는 이미 위에서 정의됨 (단일 지점)
        result['is_gajami_knee'] = is_gajami_knee  # alias → result 전달
        result['is_gajami_candidate'] = is_gajami_candidate  # alias → result 전달
        
        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        # SSoT: 가자미근 점수는 Strategy 전담
        # - Analysis는 자격(soleus_gajami) 판정만 담당
        # - 품질 점수(shape_bonus)는 Strategy가 계산
        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        
        # 🔧 Strategy MA15 Exit를 위해 current_mas/prev_mas 전달
        result['current_mas'] = current_mas
        result['prev_mas'] = prev_mas       # 🆕 완성봉 기준 MA Exit용
        
        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        # 🔒 SSOT-RECOVER 폐기 (2025-01-21)
        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        # 철학 수정:
        # - soleus_zone = "형상 판정" (필요조건)
        # - is_gajami_candidate = "최종 진입 자격" (충분조건)
        # - 차단 로직(ANIME-BLOCK 등)의 결정을 존중
        # - 복구하면 모든 안전장치가 무력화됨
        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        
        # 🔒 [DEPRECATED][COMPAT-ONLY]
        # fixed BOX 계산·판정은 철거 완료.
        # 외부 소비자의 KeyError 방지용 invalid stub만 유지한다.
        result.setdefault("ranging_predicted", False)
        result.setdefault("trimmed_box", {"valid": False})

        # 🔧 PATCH FINAL: indicators 반환 (Strategy/Bot에서 사용)
        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        # 🔒 SOLEUS SSoT BRIDGE (Analysis → Strategy 동기화)
        # - 동일 봉에서 Strategy가 soleus 판단을 놓치지 않도록 indicators에도 플래그 주입
        # - 예외 처리 아님: Analysis 확정 판정을 그대로 전달하는 정석 연결
        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        try:
            _meta = result.get('gajami_meta') or {}
            _soleus = bool(_meta.get('soleus_zone', False))
            
            # ✅ Producer(Analysis) SSoT: 소비자가 그대로 쓰는 단일 플래그/사유
            indicators['soleus_pass'] = 1.0 if _soleus else 0.0
            indicators.setdefault('soleus_zone_reason', "ANALYSIS_FAIL")
            
            if _soleus:
                indicators['soleus_final_locked'] = 1.0
                indicators['hard_gajami_pass'] = 1.0
                indicators['soleus_zone_reason'] = "ANALYSIS_PASS"
                logger.debug(f"✅ [SOLEUS-BRIDGE] {symbol} | Analysis → indicators 동기화 완료")
        except Exception as e:
            logger.warning(f"⚠️ [SOLEUS-BRIDGE-ERR] {symbol} | {e}")
        
        result['indicators'] = indicators

        return result

class MarketAnalyzer:
    """시장 분석 클래스 - KRW 마켓 전용"""
    
    def __init__(self, config):
        self.config = config
        self.data_service = MarketDataService()
        self.market_data_service = MarketDataService()
        self.timeframe = config.get_cached('timeframe')
        
        self.top_n = config.get_cached('top_markets_count')
        tf_config = TIMEFRAME_CONFIGS.get(self.timeframe, TIMEFRAME_CONFIGS[DEFAULT_TIMEFRAME])
        self.min_volume = tf_config.get('min_volume', DEFAULT_MIN_VOLUME_KRW)
        
        # 🆕 MA 이탈 적용 타임프레임 (config 기반!)
        from Trading_bot_1_core import TradingSettings
        settings = TradingSettings.from_config(config)
        self.ma_exit_applicable_timeframes = (
            settings.ma_exit_applicable_timeframes 
            if hasattr(settings, 'ma_exit_applicable_timeframes') 
            else []
        )
        
        # 캐시
        self.market_regime = MarketRegime.RANGING
        self.top_markets_cache = []
        self._gc_birth_watchlist: dict = {}  # {symbol: 남은봉수}
        self.last_analysis_time = datetime.min
        # 🆕 전 종목 미니 MA 감시층 (관측권 복구용)
        self._mini_candle_cache: dict = {}
        self._pre_gc_pool:  set = set()
        self._fresh_gc_pool: set = set()
        self._load_mini_gc_cache()

        # 🔒 REST closed candle → miniGC sealed sync 연결
        # - Data 모듈이 Analysis를 import하지 않도록 callback 방식 사용
        try:
            if hasattr(self.data_service, "add_raw_candle_callback"):
                self.data_service.add_raw_candle_callback(self._on_rest_candles_for_mini_gc)
        except Exception as _mini_cb_e:
            logger.debug(f"[MINI-REST-SYNC] callback 등록 실패: {_mini_cb_e}")
        
        self.integrated_cache = None
        
        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        # 🆕 불장 모드 관련 상태 변수 (2025-11-26)
        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        self.global_bull_mode = False          # 글로벌 불장 여부
        self.bull_entry_counter = 0            # 불장 진입 카운터
        self.bull_exit_counter = 0             # 불장 해제 카운터
        self.market_heat_score = 0.0           # 시장 열기 스코어 (0~1)
        self.btc_dominance_24h_change = 0.0    # BTC 도미넌스 24h 변화율
        self.alt_liquidity_ratio = 1.0         # 알트 유동성 비율 (오늘/어제)
        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        self.integrated_cache_time = datetime.min
        
        # 🔒 CONFIG 기반 TTL 계산 (12m = 720초)
        market_period_str = self.config.get(
            'MARKET_SELECTION',
            'market_selection_period',
            fallback='12m'
        )
        
        ttl_seconds = 720  # 기본 12분
        
        try:
            if market_period_str.endswith('m'):
                ttl_seconds = int(market_period_str[:-1]) * 60
            elif market_period_str.endswith('h'):
                ttl_seconds = int(market_period_str[:-1]) * 3600
            else:
                ttl_seconds = int(market_period_str)
        except Exception:
            ttl_seconds = 720  # 파싱 실패 시 안전 기본값
        
        self.integrated_cache_ttl = ttl_seconds
        
        # 체제 전환 안정화 상태 관리
        self.regime_history = []              # [(datetime, MarketRegime), ...]
        self.regime_last_change_time = None   # 마지막 체제 전환 시각
        self.regime_cooldown_seconds = self._get_regime_cooldown()
        
        # ========================================
        # 🆕 업비트 API 구조 모니터링
        # ========================================
        self.known_caution_flags = {
            'PRICE_FLUCTUATIONS',
            'TRADING_VOLUME_SOARING', 
            'DEPOSIT_AMOUNT_SOARING',
            'CONCENTRATION_OF_SMALL_ACCOUNTS',
            'GLOBAL_PRICE_DIFFERENCES'  # 우리는 무시하지만 알려진 플래그
        }
        self.known_warning_flags = {
            'DELISTING'
        }
        self.api_structure_checked = False  # 세션당 1회만 체크
    
    def _check_api_structure_changes(self, all_markets: List[Dict]) -> None:
        """
        업비트 API 구조 변경 감지 (세션당 1회)
        
        새로운 플래그나 구조 변경을 감지하고 눈에 띄는 경고 출력
        """
        if self.api_structure_checked or not all_markets:
            return
        
        self.api_structure_checked = True
        
        new_caution_flags = set()
        new_warning_flags = set()
        unknown_event_keys = set()
        
        # 전체 마켓 스캔 (샘플링으로 충분)
        sample_size = min(50, len(all_markets))
        
        for market in all_markets[:sample_size]:
            market_event = market.get('market_event', {})
            
            if not market_event:
                continue
            
            # 1. market_event의 알려지지 않은 키 체크
            known_event_keys = {'caution', 'warning', 'delisting'}
            for key in market_event.keys():
                if key not in known_event_keys:
                    unknown_event_keys.add(key)
            
            # 2. caution 플래그 체크
            caution = market_event.get('caution', {})
            if isinstance(caution, dict):
                for flag in caution.keys():
                    if flag not in self.known_caution_flags:
                        new_caution_flags.add(flag)
            
            # 3. warning 플래그 체크
            warning = market_event.get('warning', {})
            if isinstance(warning, dict):
                for flag in warning.keys():
                    if flag not in self.known_warning_flags:
                        new_warning_flags.add(flag)
        
        # ========================================
        # 🚨 변경 사항 발견 시 눈에 띄는 경고!
        # ========================================
        if new_caution_flags or new_warning_flags or unknown_event_keys:
            logger.warning("=" * 70)
            logger.warning("🚨 업비트 API 구조 변경 감지! 🚨")
            logger.warning("=" * 70)
            
            if new_caution_flags:
                logger.warning(f"⚠️ 새로운 CAUTION 플래그 발견: {', '.join(sorted(new_caution_flags))}")
                logger.warning(f"   → Trading_bot_5_analysis.py의 dangerous_flags에 추가 필요!")
            
            if new_warning_flags:
                logger.warning(f"⚠️ 새로운 WARNING 플래그 발견: {', '.join(sorted(new_warning_flags))}")
                logger.warning(f"   → 필터링 로직 검토 필요!")
            
            if unknown_event_keys:
                logger.warning(f"⚠️ 새로운 market_event 키 발견: {', '.join(sorted(unknown_event_keys))}")
                logger.warning(f"   → API 구조 변경 확인 필요!")
            
            logger.warning("=" * 70)
            logger.warning("📌 조치사항:")
            logger.warning("   1. 업비트 공지사항 확인 (https://upbit.com/service_center/notice)")
            logger.warning("   2. 새 플래그의 의미 파악")
            logger.warning("   3. 필터링 로직 업데이트")
            logger.warning("   4. 코드 수정 후 재시작 권장")
            logger.warning("=" * 70)
            
            # ========================================
            # 🆕 파일로도 저장 (나중에 확인 가능)
            # ========================================
            from datetime import datetime
            from pathlib import Path
            
            alert_dir = Path("outputs") / "api_alerts"
            alert_dir.mkdir(parents=True, exist_ok=True)
            
            alert_file = alert_dir / f"api_change_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
            
            with open(alert_file, 'w', encoding='utf-8') as f:
                f.write("=" * 70 + "\n")
                f.write("업비트 API 구조 변경 감지 리포트\n")
                f.write(f"감지 시각: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
                f.write("=" * 70 + "\n\n")
                
                if new_caution_flags:
                    f.write("🆕 새로운 CAUTION 플래그:\n")
                    for flag in sorted(new_caution_flags):
                        f.write(f"  - {flag}\n")
                    f.write("\n")
                
                if new_warning_flags:
                    f.write("🆕 새로운 WARNING 플래그:\n")
                    for flag in sorted(new_warning_flags):
                        f.write(f"  - {flag}\n")
                    f.write("\n")
                
                if unknown_event_keys:
                    f.write("🆕 새로운 market_event 키:\n")
                    for key in sorted(unknown_event_keys):
                        f.write(f"  - {key}\n")
                    f.write("\n")
                
                f.write("=" * 70 + "\n")
                f.write("조치 필요:\n")
                f.write("1. 업비트 공지사항 확인\n")
                f.write("2. Trading_bot_5_analysis.py 업데이트\n")
                f.write("3. 필터링 로직 검토\n")
                f.write("=" * 70 + "\n")
            
            logger.warning(f"💾 상세 리포트 저장: {alert_file}")
    
    async def _get_filtered_markets_fast(self) -> List[str]:
        """KRW 마켓 필터링 - EXCLUSION 섹션 참조"""
        all_markets = await self.market_data_service.get_markets()

        # ========================================
        # 🆕 강제 포함 종목 확인 (최우선) ← 추가
        # ========================================
        included_markets = set()
        if hasattr(self, 'config'):
            config_included = self.config.get_included_markets()
            if config_included:
                included_markets = set(config_included)
                logger.info(
                    f"📌 강제 포함 종목: {len(included_markets)}개 "
                    f"({', '.join(list(included_markets))})"
                )
                
        # ========================================
        # 🆕 API 구조 변경 감지 (세션당 1회)
        # ========================================
        self._check_api_structure_changes(all_markets)
        
        excluded = set(self.config.get_excluded_markets())
        excluded_keywords = self.config.get_excluded_keywords()
        
        exclude_caution = self.config.getboolean(
            'EXCLUSION',
            'exclude_caution_markets', 
            fallback=True
        )
        
        exclude_warning = self.config.getboolean(
            'EXCLUSION',
            'exclude_warning_markets',
            fallback=True
        )
        
        exclude_deposit_soaring = self.config.getboolean(
            'EXCLUSION',
            'exclude_deposit_amount_soaring',
            fallback=True
        )
        
        logger.info(
            f"🔍 [필터설정] "
            f"유의종목:{exclude_caution} | "
            f"경고종목:{exclude_warning} | "
            f"입금급등:{exclude_deposit_soaring}"
        )
        
        filtered = []
        excluded_count = 0
        
        for market in all_markets:
            market_code = market.get('market', '')
            
            # KRW 마켓만
            if not market_code.startswith('KRW-'):
                continue
            
            # 제외 목록
            if market_code in excluded:
                # 🆕 강제 포함이면 제외 목록도 우회
                if market_code not in included_markets:
                    excluded_count += 1
                    logger.debug(f"🚫 제외목록: {market_code}")
                    continue
                else:
                    logger.debug(f"⭐ {market_code} 제외목록이지만 강제 포함으로 우회!")
            
            # 키워드 필터
            if excluded_keywords:
                # 🛡️ 타입 안전: market_code가 dict일 수도 있음
                market_lower = str(market_code).lower() if market_code else ""
                if any(str(keyword).lower() in market_lower for keyword in excluded_keywords):
                    excluded_count += 1
                    logger.debug(f"🚫 키워드제외: {market_code}")
                    continue
            
            # ========================================
            # 유의종목 필터 (명확한 순서)
            # ========================================
            should_exclude = False
            reasons = []
            market_event = market.get('market_event', {})

            # ========================================
            # 0. 상폐 예정 체크 (최우선) ✅ 수정
            # ========================================
            if market_event:
                # 패턴 1: delisting 필드 직접 체크
                if market_event.get('delisting'):
                    reasons.append("⚠️상폐예정")
                    should_exclude = True
                
                # 패턴 2: warning 딕셔너리 내 DELISTING
                warning_value = market_event.get('warning')
                if isinstance(warning_value, dict):
                    if warning_value.get('DELISTING'):
                        reasons.append("⚠️상폐예정")
                        should_exclude = True

            # 상폐 예정이면 바로 제외 (다른 체크 건너뜀)
            if should_exclude:
                excluded_count += 1
                logger.warning(f"🚫 {market_code} 제외 - {'+'.join(reasons)}")
                continue

            # ========================================
            # 1. 투자유의종목 체크 (실제 위험만)
            # ========================================
            if exclude_caution and market_event:
                caution_dict = market_event.get('caution')
                
                if isinstance(caution_dict, dict) and caution_dict:
                    # 위험 항목 매핑 (김치프리미엄 제외!)
                    dangerous_flags = {
                        # 'PRICE_FLUCTUATIONS': '가격급등락',  # ✅ 제거: 변동성 = 기회
                        # 'TRADING_VOLUME_SOARING': '거래량급등', # 일단 제외
                        # 'DEPOSIT_AMOUNT_SOARING': '입금액급등', # 일단 제외
                        'CONCENTRATION_OF_SMALL_ACCOUNTS': '소수계정독점'  # 실제 위험만 유지
                    }
                    
                    triggered = []
                    for flag, label in dangerous_flags.items():
                        if caution_dict.get(flag, False):
                            triggered.append(label)
                    
                    if triggered:
                        reasons.append(f"투자유의({','.join(triggered)})")
                        should_exclude = True

            # ========================================
            # 2. 주의종목 (자동 지정) - 불리언 체크
            # ========================================
            if exclude_warning and market_event:
                warning_value = market_event.get('warning')
                
                # 불리언 True: 경고 (유의 자동지정)
                if warning_value is True:
                    reasons.append("경고(유의·자동지정)")  # 🔄 변경
                    should_exclude = True
                
                # 딕셔너리: 구체적 이유 (DELISTING 제외, 위에서 처리했음)
                elif isinstance(warning_value, dict):
                    warning_types = [k for k, v in warning_value.items() 
                                    if v and k != 'DELISTING']
                    if warning_types:
                        reasons.append(f"경고({','.join(warning_types)})")  # 🔄 변경
                        should_exclude = True

            # ========================================
            # 3. 입금액 급등 (2봉 안정성 검증 추가)
            # ========================================
            if exclude_deposit_soaring:
                if market.get('deposit_amount_soaring', False):
                    try:
                        # 🆕 2봉 안정성 확인 (최근 3개 캔들)
                        candles = await self.data_service.get_candles(
                            market_code, '3m', 3
                        )
                        
                        if candles and len(candles) >= 3:
                            recent_close = candles[-1]['trade_price']
                            prev_close_2 = candles[-3]['trade_price']
                            price_stability = abs(recent_close - prev_close_2) / prev_close_2
                            
                            if price_stability < 0.02:  # ±2% 이내 안정
                                logger.info(
                                    f"⚖️ {market_code} 입금급등이지만 2봉 안정화 → 허용 "
                                    f"(변동={price_stability*100:.2f}%)"
                                )
                                # ✅ 안정화 시 제외하지 않음 (should_exclude 변경 없음)
                            else:
                                # ❌ 불안정: 제외
                                reasons.append("입금액급등")
                                should_exclude = True
                        else:
                            # 캔들 데이터 부족: 보수적으로 제외
                            reasons.append("입금액급등")
                            should_exclude = True
                            
                    except Exception as e:
                        # 오류 발생 시 보수적으로 제외
                        logger.warning(
                            f"⚠️ {market_code} 입금급등 안정성 확인 실패: {e}"
                        )
                        reasons.append("입금액급등")
                        should_exclude = True

            # 최종 제외 처리
            if should_exclude:
                # 🆕 강제 포함이면 모든 필터 우회!
                if market_code in included_markets:
                    logger.debug(f"⭐ {market_code} {'+'.join(reasons)} 하지만 강제 포함!")
                    filtered.append(market_code)
                    continue
                
                exclusion_reason = "+".join(reasons)
                excluded_count += 1
                logger.debug(f"🚫 {market_code} 제외 - {exclusion_reason}")
                continue

            # 통과!
            filtered.append(market_code)
        
        logger.info(f"✅ 필터링 완료: {len(filtered)}개 종목 선정 (제외: {excluded_count}개)")
        return filtered
    
    async def calculate_bull_mode(
        self,
        top_markets_4h: List[str],
        regimes_4h: Dict[str, MarketRegime],
        tickers: List[dict] = None
    ) -> Tuple[bool, float]:
        """
        글로벌 불장 모드 판단        
        Args:
            top_markets_4h: 상위 50개 마켓 리스트
            regimes_4h: 각 마켓의 4H 체제 딕셔너리        
        Returns:
            Tuple[bool, float]: (불장 여부, 시장 열기 스코어)
        """
        # 🔒 ticker_dict 안전 초기화 (전역 스코프)
        ticker_dict = {}
        
        try:
            # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
            # 1. BTC 4H 추세 (가중치 30%)
            # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
            btc_regime_4h = regimes_4h.get("KRW-BTC", MarketRegime.RANGING)
            btc_bull = 1.0 if btc_regime_4h in [
                MarketRegime.TRENDING_UP, 
                MarketRegime.BREAKOUT
            ] else 0.0
            
            # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
            # 2. 알트 4H 추세 비율 (가중치 30%)
            # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
            bull_symbols = [
                m for m in top_markets_4h 
                if regimes_4h.get(m) in [
                    MarketRegime.TRENDING_UP, 
                    MarketRegime.BREAKOUT
                ]
            ]
            bull_ratio = len(bull_symbols) / max(1, len(top_markets_4h))
            
            # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
            # 3. BTC vs 알트 상대 성과 (가중치 10%)
            # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
            dominance_factor = 0.5  # 기본값 (중립)
            
            try:
                # ✅ 티커 데이터로 24h 변화율 계산 (1d 캔들 조회 제거)
                if not tickers:
                    raise ValueError("티커 데이터 없음")
                
                # 티커를 딕셔너리로 변환
                ticker_dict = {t['market']: t for t in tickers}
                
                # 1) BTC 24h 변화율
                btc_ticker = ticker_dict.get("KRW-BTC")
                btc_change_24h = 0.0
                if btc_ticker:
                    btc_change_24h = btc_ticker.get('signed_change_rate', 0.0) * 100.0
                
                # 2) 알트코인 평균 24h 변화율 (상위 30개)
                alt_changes = []
                for market in top_markets_4h[:30]:
                    if market == "KRW-BTC":
                        continue
                    ticker = ticker_dict.get(market)
                    if ticker:
                        change = ticker.get('signed_change_rate', 0.0) * 100.0
                        alt_changes.append(change)
                
                alt_avg_change_24h = np.mean(alt_changes) if alt_changes else 0.0
                
                # 3) BTC 대비 알트 상대 성과 계산
                relative_performance = alt_avg_change_24h - btc_change_24h
                self.btc_dominance_24h_change = -relative_performance  # 음수 = 알트 우세
                
                # 4) 도미넌스 팩터 계산
                # 알트가 BTC보다 5%p 이상 아웃퍼폼 → 알트 불장
                # BTC가 알트보다 5%p 이상 강세 → BTC 불장
                if relative_performance >= 5.0:
                    dominance_factor = 1.0  # 강한 알트 불장
                elif relative_performance <= -5.0:
                    dominance_factor = 0.0  # BTC 불장
                else:
                    # 선형 보간: -5 ~ +5 구간
                    dominance_factor = 0.5 + (relative_performance / 10.0)
                    dominance_factor = np.clip(dominance_factor, 0.0, 1.0)
                
                logger.debug(
                    f"   - 상대성과: BTC={btc_change_24h:.1f}% | "
                    f"알트평균={alt_avg_change_24h:.1f}% | "
                    f"상대={relative_performance:+.1f}%p | "
                    f"factor={dominance_factor:.2f}"
                )
                        
            except Exception as e:
                logger.debug(f"상대 성과 계산 실패 (기본값 사용): {e}")
            
            # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
            # 4. 알트 유동성 증가율 (가중치 10%)
            # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
            alt_liquidity_factor = 0.5  # 기본값
            
            try:
                # ✅ 티커 데이터로 24h 거래대금 활용 (1d 캔들 조회 제거)
                # 상위 10개 알트코인의 24h 거래대금 합산
                sample_alts = [m for m in top_markets_4h[:10] if m != "KRW-BTC"]
                
                vol_recent = 0.0  # 24h 거래대금 합산
                
                for market in sample_alts:
                    ticker = ticker_dict.get(market)
                    if ticker:
                        vol_recent += ticker.get('acc_trade_price_24h', 0)
                
                # ✅ 단순화: 거래대금 절대값 기준 (50조 이상 = 높은 유동성)
                # 티커는 24h 데이터만 제공하므로 절대값 기준 판단
                if vol_recent > 0:
                    # 거래대금 기준 (단위: 원)
                    # 50조 이상 = 1.0 (매우 높음)
                    # 20조 = 0.5 (중립)
                    # 10조 이하 = 0.0 (낮음)
                    threshold_high = 50_000_000_000_000  # 50조
                    threshold_low = 10_000_000_000_000   # 10조
                    
                    if vol_recent >= threshold_high:
                        alt_liquidity_factor = 1.0
                    elif vol_recent <= threshold_low:
                        alt_liquidity_factor = 0.0
                    else:
                        # 선형 보간
                        alt_liquidity_factor = (vol_recent - threshold_low) / (threshold_high - threshold_low)
                        alt_liquidity_factor = np.clip(alt_liquidity_factor, 0.0, 1.0)
                    
                    self.alt_liquidity_ratio = vol_recent / threshold_low  # 기준 대비 배수
                    
                    logger.debug(
                        f"   - 유동성: 최근={vol_recent/1e12:.1f}조 | "
                        f"기준=10~50조 | "
                        f"증가율={self.alt_liquidity_ratio:.2f}x | "
                        f"factor={alt_liquidity_factor:.2f}"
                    )
                    
            except Exception as e:
                logger.debug(f"알트 유동성 계산 실패 (기본값 사용): {e}")
            
            # 5. 시장 열기 스코어 계산 (알트 중심)
            # 🔥 BTC는 참고용만, 판단에서 완전 제외
            # 🔥 dominance도 제외 (알트 독립성 확보)
            market_heat_score = (
                0.7 * bull_ratio +              # 알트 추세 70%
                0.3 * alt_liquidity_factor      # 알트 유동성 30%
            )
            
            self.market_heat_score = market_heat_score
            
            logger.debug(
                f"   - 시장열기(알트 중심): "
                f"추세(70%)={bull_ratio:.1%} | "
                f"유동성(30%)={alt_liquidity_factor:.2f} | "
                f"최종={market_heat_score:.3f} | "
                f"💡 BTC={btc_bull:.1f}(참고), dominance={dominance_factor:.2f}(참고)"
            )
            
            # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
            # 6. 히스테리시스 적용 (빠른 진입 / 느린 해제)
            # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
            
            # 강제 OFF 조건 체크
            if (btc_regime_4h == MarketRegime.BREAKDOWN or 
                bull_ratio < BULL_FORCE_EXIT_RATIO):
                self.global_bull_mode = False
                self.bull_entry_counter = 0
                self.bull_exit_counter = 0
                
                logger.info(
                    f"🚨 [불장 강제 종료] "
                    f"BTC={btc_regime_4h.name}, bull_ratio={bull_ratio:.2%}"
                )
                return False, market_heat_score
            
            # 진입 조건 체크
            if market_heat_score >= BULL_ENTRY_THRESHOLD:
                self.bull_entry_counter += 1
                self.bull_exit_counter = 0  # 리셋
                
                if self.bull_entry_counter >= BULL_ENTRY_COUNT:
                    if not self.global_bull_mode:
                        logger.info(
                            f"🔥 [불장 진입] "
                            f"heat={market_heat_score:.2f}, "
                            f"BTC={btc_bull:.0f}, ALT={bull_ratio:.2%}, "
                            f"DOM={dominance_factor:.2f}, LIQ={alt_liquidity_factor:.2f}"
                        )
                    self.global_bull_mode = True
            
            # 해제 조건 체크
            elif market_heat_score <= BULL_EXIT_THRESHOLD:
                if self.global_bull_mode:
                    self.bull_exit_counter += 1
                    
                    if self.bull_exit_counter >= BULL_EXIT_COUNT:
                        logger.info(
                            f"❄️ [불장 해제] "
                            f"heat={market_heat_score:.2f} (5회 연속 하락)"
                        )
                        self.global_bull_mode = False
                        self.bull_entry_counter = 0
                        self.bull_exit_counter = 0
                else:
                    self.bull_entry_counter = max(0, self.bull_entry_counter - 1)
            
            # 중간 구간 (0.35 ~ 0.50): 현재 상태 유지
            else:
                # 카운터 자연 감소
                if not self.global_bull_mode:
                    self.bull_entry_counter = max(0, self.bull_entry_counter - 1)
            
            return self.global_bull_mode, market_heat_score
            
        except Exception as e:
            logger.error(f"불장 모드 계산 실패: {e}")
            return False, 0.0


    def calculate_symbol_bull_level(
        self,
        symbol: str,
        regime_4h: MarketRegime,
        adx_4h: float,
        atr_pct_4h: float,
        change_24h_pct: float
    ) -> int:
        """
        심볼별 로컬 불장 레벨 계산 (0~3)
        
        Args:
            symbol: 종목 심볼
            regime_4h: 4H 체제
            adx_4h: 4H ADX
            atr_pct_4h: 4H ATR (%)
            change_24h_pct: 24h 변화율 (%)
        
        Returns:
            int: 불장 레벨 (0=없음, 1=약함, 2=보통, 3=강함)
        """
        try:
            # Level 3: 강한 심볼 불장
            if (regime_4h == MarketRegime.BREAKOUT and
                adx_4h >= SYMBOL_BULL_LEVEL_3['adx_min'] and
                atr_pct_4h >= SYMBOL_BULL_LEVEL_3['atr_min'] and
                change_24h_pct >= SYMBOL_BULL_LEVEL_3['change_24h_min']):
                return 3
            
            # Level 2: 보통 심볼 불장
            if (regime_4h in [MarketRegime.TRENDING_UP, MarketRegime.BREAKOUT] and
                adx_4h >= SYMBOL_BULL_LEVEL_2['adx_min'] and
                change_24h_pct >= SYMBOL_BULL_LEVEL_2['change_24h_min']):
                return 2
            
            # Level 1: 약한 심볼 불장
            if (regime_4h == MarketRegime.TRENDING_UP and
                adx_4h >= SYMBOL_BULL_LEVEL_1['adx_min'] and
                change_24h_pct >= SYMBOL_BULL_LEVEL_1['change_24h_min']):
                return 1
            
            # Level 0: 불장 아님
            return 0
            
        except Exception as e:
            logger.debug(f"{symbol} 심볼 불장 레벨 계산 실패: {e}")
            return 0
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    
    def get_bull_market_context(self) -> Dict[str, Any]:
        """
        불장 모드 컨텍스트 반환        
        봇이 indicators에 포함시킬 수 있도록 불장 정보 제공        
        Returns:
            Dict: 불장 관련 정보
                - global_bull: 글로벌 불장 여부 (bool)
                - market_heat_score: 시장 열기 스코어 (float, 0~1)
                - bull_entry_counter: 진입 카운터 (int)
                - bull_exit_counter: 해제 카운터 (int)
                - btc_dominance_24h_change: BTC 도미넌스 변화율 (%)
                - alt_liquidity_ratio: 알트 유동성 비율        
        사용 예시:
            bull_context = analyzer.get_bull_market_context()
            indicators.update(bull_context)
        """
        return {
            'global_bull': self.global_bull_mode,
            'market_heat_score': self.market_heat_score,
            'bull_entry_counter': self.bull_entry_counter,
            'bull_exit_counter': self.bull_exit_counter,
            'btc_dominance_24h_change': self.btc_dominance_24h_change,
            'alt_liquidity_ratio': self.alt_liquidity_ratio,
        }
    
    async def perform_integrated_analysis(self) -> List[str]:
        """통합 마켓 분석 - ATR 순위 기반        
        흐름:
        0. 시장 체제 기반 동적 설정 조정 (신규!)
        1. 기본 필터링 (유의/경고 제외)
        2. 강제 포함 종목 확인
        3. 거래대금 상위 50개 선정
        4. 50개 모두 ATR 계산
        5. ATR 높은 순 정렬
        6. 상위 30개 선정        
        Returns:
            List[str]: 선정된 마켓 심볼 리스트
        """
        try:
            logger.info("통합 시장 분석 시작")
            
            # 🆕 Track2 모멘텀 후보 수집용 리스트
            momentum_candidates = []
            _surge_markets_for_trace = set()
            track2_birth_watch_markets = []
            reversal_birth_watch_markets = []
            _mini_priority = []
            _mini_core_priority = []
            _mini_slow_priority = []
            _mini_set = set()
            
            # ========================================
            # 🔒 마켓 설정 (체제별 동적 조정 제거 - SSoT 유지)
            # ========================================
            # 철학:
            #   - 체제 판단은 점수/가중치 보정에만 사용 (연성 변수)
            #   - 마켓 선정 개수/ATR 하한은 고정값 유지 (경성 변수 금지)
            #   - 체제 오판이 스캔 대상 자체를 왜곡하지 않도록 함
            #   - "예상 진입 시간대 로그 없음" 현상 방지
            
            top_markets_count = self.config.getint(
                'MARKET_SELECTION', 'top_markets_count', fallback=35
            )
            min_atr_percent = self.config.getfloat(
                'MARKET_SELECTION', 'min_atr_percent', fallback=0.10
            )

            # market_selection_debug_symbols는 get_current_top_markets()의
            # MARKET-TRACE-CACHE 로그에서만 사용. perform_integrated_analysis에서는 불용.
            
            regime = self.market_regime  # 현재 체제 (로깅용)
            logger.info(
                f"⚙️ [마켓설정] 종목 {top_markets_count}개, ATR≥{min_atr_percent}% | "
                f"체제={regime.value if regime else 'UNKNOWN'} (선정에 미반영)"
            )
                
            # ========================================
            # 1. 기본 필터링 (유의/경고/상폐 제외)
            # ========================================
            valid_markets = await self._get_filtered_markets_fast()
            
            if not valid_markets:
                logger.warning("필터링된 마켓이 없습니다")
                return []
            
            # ========================================
            # 3. 🆕 하이브리드: 24h 거래대금 → 4h 거래대금 재계산
            # ========================================
            candidate_count = self.config.getint(
                'MARKET_SELECTION',
                'volatility_candidate_count',
                fallback=30
            )
            
            # 🆕 하이브리드 1차 필터링 개수 (더 많이 가져옴)
            first_pass_count = self.config.getint(
                'MARKET_SELECTION',
                'hybrid_first_pass_count',
                fallback=150
            )
            
            # 🆕 24h ticker liquidity proxy 재계산 활성화 여부
            use_4h_recheck = self.config.getboolean(
                'MARKET_SELECTION',
                'hybrid_use_4h_recheck',
                fallback=True
            )
            
            # ========================================
            # 3-1. 24시간 거래대금으로 1차 필터링 (빠름)
            # ========================================
            logger.info(f"1차: 24시간 거래대금 조회 중... (빠른 선별)")
            
            tickers = await self.data_service.get_ticker(valid_markets)
            
            if not tickers:
                logger.warning("티커 조회 실패")
                return []
            
            # 24시간 거래대금 기준 정렬
            sorted_by_24h = sorted(
                tickers,
                key=lambda x: x.get('acc_trade_price_24h', 0),
                reverse=True
            )
            
            # 🆕 1원 미만 종목 필터링 (시스템 안정성 - 호가 계산 불가)
            filtered_by_price = [
                t for t in sorted_by_24h 
                if safe_price(t.get('trade_price')) >= MIN_TRADE_PRICE
            ]
            
            excluded_sub_1won = len(sorted_by_24h) - len(filtered_by_price)
            if excluded_sub_1won > 0:
                excluded_markets = [
                    t.get('market', 'UNKNOWN') 
                    for t in sorted_by_24h 
                    if safe_price(t.get('trade_price')) < MIN_TRADE_PRICE
                ]
                logger.info(
                    f"🚫 1원 미만 종목 {excluded_sub_1won}개 제거 "
                    f"({', '.join(excluded_markets[:5])}{'...' if excluded_sub_1won > 5 else ''}) "
                    f"- 호가 계산 불가"
                )
            
            # 1차 선정: 제외/가격필터 통과 후 24시간 거래대금 상위 N개
            # 이후 변동률 후보도 반드시 이 1차 풀 안에서만 선발한다.
            top_markets_1st = [t['market'] for t in filtered_by_price[:first_pass_count]]

            _vol_rank_map = {t['market']: i+1 for i, t in enumerate(sorted_by_24h)}
            _chg_sorted = sorted(
                filtered_by_price,
                key=lambda x: float(x.get('signed_change_rate', 0)),
                reverse=True
            )
            _chg_rank_map = {t['market']: i+1 for i, t in enumerate(_chg_sorted)}

            # 🔒 [CHG-LANE-BINDING] 감사→선정 완성 (관측권 복구, 진입완화 아님)
            _CHG_BIND_TOP = 15
            _tp1_set0 = set(top_markets_1st)
            _chg_bound = []

            for _ct in _chg_sorted[:_CHG_BIND_TOP]:
                try:
                    _cm = _ct.get('market')
                    _signed_rate = float(_ct.get('signed_change_rate', 0) or 0)
                    if _cm and _cm not in _tp1_set0 and _signed_rate > 0.0:
                        top_markets_1st.append(_cm)
                        _tp1_set0.add(_cm)
                        _chg_bound.append(_cm)
                except Exception:
                    pass

            if _chg_bound:
                logger.info(
                    f"⚡ [CHG-LANE-BINDING] {len(_chg_bound)}개 편입 | "
                    f"sample={_chg_bound[:8]}"
                )

            _first_pass_set = set(top_markets_1st)
            # 🆕 [CHG-LANE-AUDIT] 변동률 상위 무버의 풀/부호/편입 가시화 — binding 후 상태 확인용
            #   ① in_first_pass=0 → 24h 거래대금 150위 밖이라 미관측
            #   ② in_first_pass=1 ∧ signed>0 → 본류 surge OR 누수 의심(stale/data_miss/slot컷)
            #   ③ in_first_pass=1 ∧ signed<0 → Reversal-BirthWatch 경로
            for _audit_t in _chg_sorted[:20]:
                _am = _audit_t.get('market')
                logger.info(
                    f"📈 [CHG-LANE-AUDIT] {_am} | "
                    f"chg_rank={_chg_rank_map.get(_am)} "
                    f"signed={float(_audit_t.get('signed_change_rate', 0) or 0) * 100.0:.2f}% "
                    f"vol_rank={_vol_rank_map.get(_am, 9999)} "
                    f"in_first_pass={int(_am in _first_pass_set)} "
                    f"acc24h={int(_audit_t.get('acc_trade_price_24h', 0) or 0)}"
                )

            logger.info(
                f"✅ 1차 선정: 24시간 거래대금 상위 {len(top_markets_1st)}개 "
                f"(SURGE는 acc24h floor 기반 별도 풀)"
            )
                        
            if use_4h_recheck:
                logger.info("2차: 24h ticker 기반 거래대금 재계산 (API 0회)")

                volume_data_24h_proxy = []
                ticker_dict = {t['market']: t for t in tickers}

                for market in top_markets_1st:
                    try:
                        t = ticker_dict.get(market, {})
                        acc_24h = float(t.get('acc_trade_price_24h', 0))
                        volume_krw = acc_24h / 24.0

                        volume_data_24h_proxy.append({'market': market, 'volume_krw': volume_krw})

                        # Track2: 모멘텀 관측 후보 (ticker 기반)
                        try:
                            change_24h_pct = float(t.get('signed_change_rate', 0)) * 100.0
                            last_close    = float(t.get('trade_price', 0))
                            if (
                                WATCHLIST_CHANGE_15M_MIN <= change_24h_pct <= WATCHLIST_CHANGE_30M_MAX
                                and last_close >= WATCHLIST_MIN_PRICE
                            ):
                                momentum_candidates.append({
                                    "market":     market,
                                    "change_24h": change_24h_pct,
                                    "volume_krw": volume_krw
                                })
                        except Exception:
                            pass

                    except Exception as e:
                        logger.debug(f"{market} ticker 거래대금 조회 실패: {e}")
                                    
                # 24h liquidity proxy 데이터가 충분히 수집되었는지 확인
                if len(volume_data_24h_proxy) >= candidate_count:
                    # 24h liquidity proxy 순 정렬
                    sorted_by_4h = sorted(
                        volume_data_24h_proxy,
                        key=lambda x: x['volume_krw'],
                        reverse=True
                    )
                    
                    # 상위 N개 후보
                    _vol_count   = self.config.getint('MARKET_SELECTION', 'volume_group_count', fallback=7)
                    _surge_count = self.config.getint('MARKET_SELECTION', 'atr_group_count', fallback=28)

                    # ① 거래대금 TOP _vol_count개 (고정)
                    _vol_markets = [item['market'] for item in sorted_by_4h[:_vol_count]]

                    # ② 변동률 SURGE 후보 풀:
                    #    1차 거래대금 순위와 분리하고 전체 가격필터 통과 종목에서 관측한다.
                    #    이후 ticker3분 방향·signed 양수·surge_count·최종 편입 수가 제한하며,
                    #    Bot7 GC필터와 Analysis/Strategy 헌법은 그대로 심사한다.
                    _first_pass_set = set(top_markets_1st)

                    # ③ SURGE 레인 최소 거래대금 floor
                    try:
                        _surge_min_acc24h = self.config.getfloat(
                            'MARKET_SELECTION',
                            'surge_min_acc_trade_price_24h',
                            fallback=340_000_000.0
                        )
                    except Exception:
                        _surge_min_acc24h = 340_000_000.0

                    _surge_pool_set = set(
                        t['market'] for t in filtered_by_price
                        if float(
                            t.get('acc_trade_price_24h', 0) or 0
                        ) >= _surge_min_acc24h
                    )

                    # ② ticker 3분 방향 공식 (REST 캔들 0콜)
                    # 현재봉은 rt_delta>0만 허용하고, 상승 강도와 이전봉 방향으로 Tier1~4 분류
                    # 주의: REST 3분봉 아님, ticker 3분 snapshot 기준
                    import time as _time
                    _snap      = getattr(self, '_ticker_3m_close_cache', {})
                    _now_ts    = _time.time()
                    _STALE_SEC = 420  # 7분 초과 시 stale

                    _surge_scored  = []
                    _surge_rt_nonpos_set = set()
                    _or_total = _or_pass = _or_data_miss = _or_down = _or_stale = 0
                    # 🔒 [SURGE-RT-NONPOS-CLOSED-OBSERVE-0831] ★465 관측 전용
                    _or_nonpos_closed_up = 0

                    for t in filtered_by_price:
                        if not (
                            (t.get('market') in _surge_pool_set)
                            and (t.get('market') not in _vol_markets)
                            and float(t.get('signed_change_rate', 0) or 0) > 0
                        ):
                            continue
                        _or_total += 1
                        _m  = t.get('market')
                        _px = float(t.get('trade_price', 0) or 0)
                        _c  = _snap.get(_m, {})
                        _last    = float(_c.get('last_close', 0.0) or 0.0)
                        _prev    = float(_c.get('prev_close', 0.0) or 0.0)
                        _last_ts = float(_c.get('last_ts',   0.0) or 0.0)
                        _prev_ts = float(_c.get('prev_ts',   0.0) or 0.0)

                        if _last <= 0 or _prev <= 0 or _px <= 0:
                            _or_data_miss += 1
                            continue
                        if _last_ts <= 0 or (_now_ts - _last_ts) > _STALE_SEC:
                            _or_stale += 1
                            continue

                        _closed_delta = (_last - _prev) / _prev
                        _rt_delta     = (_px   - _last) / _last
                        _FLAT_THR    = 0.00001   # 보합 판정 임계값 (0.001%)
                        _closed_up   = _closed_delta >  _FLAT_THR
                        _closed_flat = abs(_closed_delta) <= _FLAT_THR
                        _rt_up       = _rt_delta > _FLAT_THR
                        _rt_flat     = 0.0 < _rt_delta <= _FLAT_THR

                        if _rt_delta <= 0.0:
                            _surge_rt_nonpos_set.add(_m)
                            # 🔒 [SURGE-RT-NONPOS-CLOSED-OBSERVE-0831] ★465 관측 전용·판정 변화 0
                            # rt로 탈락했지만 확정봉(_closed_delta)은 상승이던 건을 센다.
                            # 후단 score는 max(_closed_delta, _rt_delta)를 소비하지만
                            # 현재 관문은 rt 하나로 탈락시키는 규모를 측정한다.
                            # ORBS 정답창에서는 후보 대부분이 rt_nonpos로 진입평가 전에 소멸했다.
                            if _closed_delta > _FLAT_THR:
                                _or_nonpos_closed_up += 1
                            _or_down += 1
                            continue

                        if _closed_up and _rt_up:
                            _tier = 1   # 둘 다 상승
                        elif _rt_up:
                            _tier = 2   # 현재봉↑ (V자 #2)
                        elif _closed_up:
                            _tier = 3   # 이전봉↑ + 현재봉 미세상승
                        elif _closed_flat and _rt_flat:
                            _tier = 4   # 이전봉 보합 + 현재봉 미세상승
                        else:
                            _or_down += 1
                            continue

                        _or_pass += 1
                        _surge_scored.append((
                            _tier,
                            max(_closed_delta, _rt_delta),
                            t, _closed_delta, _rt_delta,
                            _closed_up, _rt_up, _prev_ts, _last_ts,
                        ))

                    # tier(막상승전환 먼저) → +24h_signed 오름차순(덜오른 초입먼저) → ticker3m score → acc24h
                    # 🔧 [BIRTH-PROXIMITY] "많이 오른 순"→"가자미근 탄생직전(덜오른 초입)" 우선 정렬
                    #   완화 아님: 풀 동일, 관측 우선순위만 변경. CHG-LANE-AUDIT으로 측정.
                    _surge_scored = sorted(
                        _surge_scored,
                        key=lambda x: (
                            x[0],
                            +float(x[2].get('signed_change_rate', 0) or 0),
                            -float(x[1] or -999.0),
                            -float(x[2].get('acc_trade_price_24h',  0) or 0),
                        ),
                    )
                    _surge_pool   = [t for _ti, _s, t, *_ in _surge_scored]
                    _vol_sup_mkts = [t['market'] for t in _surge_pool[:_surge_count]]
                    _surge_markets_for_trace = set(_vol_sup_mkts)

                    # tier 카운트
                    _tier_counts = {1:0, 2:0, 3:0, 4:0}
                    for _ti, *_ in _surge_scored:
                        _tier_counts[_ti] = _tier_counts.get(_ti, 0) + 1

                    # Tier6 fallback: 28개 부족 시 변동률+거래대금 보충 (OR통과와 분리)
                    _t6_filled = 0
                    if len(_vol_sup_mkts) < _surge_count:
                        _already = set(_vol_markets) | set(_vol_sup_mkts)
                        _t6 = sorted(
                            [
                                t for t in filtered_by_price
                                if (t.get('market') in _surge_pool_set)
                                and (t.get('market') not in _already)
                                and (t.get('market') not in _surge_rt_nonpos_set)
                                and float(t.get('signed_change_rate', 0) or 0) > 0
                            ],
                            # 🆕 [BIRTH-PROXIMITY 정합 06-29] 메인 surge 정렬과 동일 철학 — fallback도 "많이 오른 순" 폐기.
                            #   24h_signed 오름차순(덜 오른 초입 먼저) → acc24h. (tier6=OR 미통과 fill이라 tier/3m 없음)
                            key=lambda x: (
                                +float(x.get('signed_change_rate', 0) or 0),   # 🔧 [BIRTH-PROXIMITY-T6] 메인정렬 정합: 덜오른 초입 우선
                                -float(x.get('acc_trade_price_24h',  0) or 0),
                            ),
                        )
                        _fill_t6 = _surge_count - len(_vol_sup_mkts)
                        _vol_sup_mkts += [t['market'] for t in _t6[:_fill_t6]]
                        _t6_filled = min(_fill_t6, len(_t6))
                        _surge_markets_for_trace = set(_vol_sup_mkts)

                    # 🟢 Reversal BirthWatch — 관측 전용
                    # 🔒 [REVERSAL-ADMISSION-REVOKE-0903] ★499-R1
                    # 24h 음수 + 3m 미세 회복 후보의 producer/audit은 반사실 관측용으로 유지한다.
                    # result 특별편입 권한은 후단에서 회수하며, 여기서는 후보만 산출한다.
                    try:
                        _rev_scored = []
                        _REV_FLAT_THR = 0.00001

                        for t in filtered_by_price:
                            _m = t.get('market')
                            if not (
                                (_m in _surge_pool_set)
                                and (_m not in _vol_markets)
                            ):
                                continue

                            _signed_pct = float(t.get('signed_change_rate', 0) or 0) * 100.0
                            if _signed_pct >= 0.0:
                                continue

                            _rank = int(_vol_rank_map.get(_m, 9999) or 9999)
                            # 🔧 [REVERSAL-WATCH-FIRSTPASS]
                            # 24h signed 음수 후보의 3m 회복 여부를 관측한다.
                            # acc24h SURGE 풀 + first-pass rank 범위는 관측 모집단 계약으로 유지.
                            # ★499-R1 이후 result 특별편입 권한은 없다.
                            if _rank > first_pass_count:
                                continue

                            _px = float(t.get('trade_price', 0) or 0)
                            _c = _snap.get(_m, {})
                            _last = float(_c.get('last_close', 0.0) or 0.0)
                            _prev = float(_c.get('prev_close', 0.0) or 0.0)
                            _last_ts = float(_c.get('last_ts', 0.0) or 0.0)

                            if _last <= 0 or _prev <= 0 or _px <= 0:
                                continue
                            if _last_ts <= 0 or (_now_ts - _last_ts) > _STALE_SEC:
                                continue

                            _closed_delta = (_last - _prev) / _prev
                            _rt_delta = (_px - _last) / _last

                            if not (_closed_delta > _REV_FLAT_THR or _rt_delta > _REV_FLAT_THR):
                                continue

                            _rev_score = max(_closed_delta, _rt_delta)
                            _rev_scored.append((_rank, -_rev_score, _m, _signed_pct))

                        reversal_birth_watch_markets = [
                            _m for _rank, _neg_score, _m, _signed_pct in sorted(_rev_scored)
                        ][:max(3, min(8, MAX_MOMENTUM_WATCHLIST))]

                        if reversal_birth_watch_markets:
                            logger.info(
                                f"🟢 [Reversal-BirthWatch-Candidate] "
                                f"후보 {len(reversal_birth_watch_markets)}개 관측 | admission=0 | "
                                f"sample={reversal_birth_watch_markets[:5]}"
                            )
                        # 🔧 [REVERSAL-BIRTHWATCH-AUDIT] 후보 생산은 유지, 특별편입은 0으로 고정.
                        logger.info(
                            f"[REVERSAL-BIRTHWATCH-AUDIT] "
                            f"total_rev_candidates={len(_rev_scored)} "
                            f"rev_watch={len(reversal_birth_watch_markets)} "
                            f"rev_admitted=0 "
                            f"detail={[(m, round(s,2), int(r)) for r,ns,m,s in sorted(_rev_scored)[:8]]}"
                        )
                    except Exception:
                        reversal_birth_watch_markets = []

                    _surge_selected_detail = [
                        (
                            _ti,
                            t.get('market'),
                            round(_s * 100.0, 3),
                            round(_cd * 100.0, 3),
                            round(_rd * 100.0, 3),
                            int(_cu), int(_ru),
                            int(_now_ts - _lts) if _lts else 9999,
                            round(float(t.get('signed_change_rate', 0) or 0) * 100.0, 2),
                        )
                        for _ti, _s, t, _cd, _rd, _cu, _ru, _pts, _lts in _surge_scored[:10]
                    ]

                    logger.info(
                        f"🔎 [SURGE-OR-TICKER3M] 후보 {_or_total}개 → 방향통과 {_or_pass}개 "
                        f"(①24h양수∧②현재봉rt>0∧4tier[둘다↑/현재↑/이전↑+현재미세↑/이전flat+현재미세↑]"
                        f"∧③acc24h≥{_surge_min_acc24h:.0f}원, "
                        f"sort=tier→signed_asc→ticker3m_score→acc24h, "
                        f"tier={_tier_counts}, "
                        f"tier6_fill={_t6_filled}, "
                        f"data_miss={_or_data_miss} stale={_or_stale} "
                        f"dir_fail={_or_down} rt_nonpos={len(_surge_rt_nonpos_set)} "
                        f"nonpos_closed_up={_or_nonpos_closed_up}) "
                        f"→ 상위 {len(_vol_sup_mkts)}개 | "
                        f"selected={_vol_sup_mkts[:10]} | "
                        f"detail=(tier,market,score%,closed%,rt%,cu,ru,age_sec,signed%)={_surge_selected_detail}"
                    )

                    # 골크 임박/직후 중심 선정 (적응형, 상한 없음)
                    # 1순위: 최근봉 GC → 1봉후 GC예정 → 1/2/3봉전 GC
                    # 2순위: vol 안전망(적응)  3순위: surge 잔여
                    _target_count = top_markets_count   # 최대 35
                    _valid_mini_set = set(
                        t.get("market") for t in filtered_by_price if t.get("market")
                    )

                    def _mini_gc_rank(s: str) -> tuple:
                        try:
                            entries, _now_bucket = self._get_mini_closed_window(
                                s,
                                self._MINI_CACHE_BARS
                            )
                            if len(entries) < self._MINI_CACHE_BARS:
                                return (99, 999.0, s)

                            buckets = [int(e["bucket"]) for e in entries]

                            # 최신 미니캔들이 15분 이상 지연된 경우에만 순위 대상에서 제외한다.
                            if _now_bucket - buckets[-1] > self._MINI_TF_SECS * 5:
                                return (99, 999.0, s)

                            c = [float(e["close"]) for e in entries]
                            _mini_all = self._mini_candle_cache.get(s, [])
                            _mini_cur = next(
                                (float(e["close"]) for e in reversed(_mini_all)
                                 if isinstance(e, dict)
                                 and int(e.get("bucket", 0) or 0) == _now_bucket),
                                None,
                            )
                            ma5 = (
                                (sum(c[-4:]) + _mini_cur) / 5
                                if _mini_cur is not None and _mini_cur > 0.0
                                else sum(c[-5:]) / 5
                            )
                            ma10 = (
                                (sum(c[-9:]) + _mini_cur) / 10
                                if _mini_cur is not None and _mini_cur > 0.0
                                else sum(c[-10:]) / 10
                            )
                            ma5p1  = sum(c[-6:-1])  / 5
                            ma10p1 = sum(c[-11:-1]) / 10
                            ma5p2  = sum(c[-7:-2])  / 5
                            ma10p2 = sum(c[-12:-2]) / 10
                            ma5p3  = sum(c[-8:-3])  / 5
                            ma10p3 = sum(c[-13:-3]) / 10
                            ma5p4  = sum(c[-9:-4])  / 5
                            ma10p4 = sum(c[-14:-4]) / 10

                            signed_gap = ((ma5 - ma10) / ma10 * 100.0) if ma10 > 0 else 0.0
                            gap_now = abs(signed_gap) if ma10 > 0 else 999.0
                            ma5_slope  = ma5  - ma5p1
                            ma10_slope = ma10 - ma10p1
                            slope_diff = ma5_slope - ma10_slope
                            _fresh_quality_ok = bool(
                                ma5 >= ma10
                            )
                            _fresh_age = self._mini_gc_age_from_history(
                                ma5p1, ma10p1, ma5p2, ma10p2,
                                ma5p3, ma10p3, ma5p4, ma10p4,
                            )

                            # 0순위: 최근봉 GC
                            if _fresh_quality_ok and _fresh_age == 0:
                                return (0, gap_now, s)

                            # 1순위: 1봉후 GC 예정
                            if (
                                ma5 < ma10
                                and gap_now <= self._MINI_GAP_MAX
                                and ma5_slope > 0.0
                                and slope_diff > 0.0
                            ):
                                eta = (ma10 - ma5) / slope_diff if slope_diff > 0 else 999.0
                                if eta <= 1.5:
                                    return (1, eta, s)
                                return (6, eta, s)

                            # 2~4순위: 1~3봉전 GC
                            if (
                                _fresh_quality_ok
                                and 1 <= _fresh_age <= self._MINI_FRESH_MAX_AGE
                            ):
                                return (_fresh_age + 1, gap_now, s)

                            return (99, gap_now, s)

                        except Exception:
                            return (99, 999.0, s)

                    _fresh_all = [
                        s for s in getattr(self, "_fresh_gc_pool", set())
                        if s in _valid_mini_set
                    ]
                    _pre_all = [
                        s for s in getattr(self, "_pre_gc_pool", set())
                        if s in _valid_mini_set
                    ]
                    _mini_priority = sorted(
                        set(_fresh_all) | set(_pre_all),
                        key=_mini_gc_rank
                    )

                    # MINI 핵심과 PRE_GC_SLOW 분리
                    # - 핵심: GC_0B / PRE_GC_ETA / GC_1B / GC_2B / GC_3B
                    # - SLOW: 대상선정 제외. 다음 스캔에서 ETA 승격 시 재편입
                    _mini_core_priority = []
                    _mini_slow_priority = []

                    for _s in _mini_priority:
                        try:
                            _rank_no = int(_mini_gc_rank(_s)[0])
                        except Exception:
                            _rank_no = 99

                        if _rank_no in (0, 1, 2, 3, 4):
                            _mini_core_priority.append(_s)
                        elif _rank_no == 6:
                            _mini_slow_priority.append(_s)

                    _mini_set = set(_mini_core_priority)

                    # vol/surge: mini core 제외 후 남은 자리에 적응형 할당
                    _remaining  = max(0, _target_count - len(_mini_core_priority))
                    _VOL_SAFETY = 3
                    _vol_take   = min(
                        len([s for s in _vol_markets  if s not in _mini_set]),
                        max(min(_VOL_SAFETY, _remaining), _remaining // 10)
                    )
                    _surge_take = max(0, _remaining - _vol_take)
                    _vol_fill   = [s for s in _vol_markets  if s not in _mini_set and self._mini_constitution_pre_ok(s)[0]][:_vol_take]
                    # 🔒 [SURGE-FILL-PRUNE-0903] ★496-R2
                    # SURGE_FILL은 MINI+VOL 뒤의 잔여 슬롯 채움용 filler다.
                    # 35개 고정 충원을 요구하지 않으므로 편입 권한만 회수한다.
                    # 성적(18.2%)은 p=0.4542로 유의하지 않으므로 제거 근거로 쓰지 않는다.
                    # _surge_take / _vol_sup_mkts / 선정 로그는 관측용으로 유지한다.
                    _surge_fill = []

                    candidates_markets = list(dict.fromkeys(
                        _mini_core_priority
                        + _vol_fill
                        + _surge_fill
                    ))[:_target_count]

                    _mini_selected = sum(1 for s in candidates_markets if s in _mini_set)
                    _vol_selected = sum(1 for s in candidates_markets if s in set(_vol_fill))
                    _surge_selected = sum(1 for s in candidates_markets if s in set(_surge_fill))

                    logger.info(
                        f"✅ 2차 선정: "
                        f"미니GC {_mini_selected}개(pool={len(_mini_priority)}, fresh={len(_fresh_all)}/pre={len(_pre_all)}) "
                        f"+ vol안전망 {_vol_selected}개 "
                        f"+ surge_fill {_surge_selected}개 "
                        f"+ rt비양수컷 {len(_surge_rt_nonpos_set)}개 "
                        f"= {len(candidates_markets)}개 "
                        f"| first_pass={len(top_markets_1st)} "
                        f"surge_pool={len(_surge_pool)}"
                    )
                    
                    # 원본 ticker 데이터에서 해당 종목만 추출
                    candidates = [
                        t for t in sorted_by_24h 
                        if t['market'] in candidates_markets
                    ]
                    
                    # 24h liquidity proxy 순서대로 정렬
                    candidates_dict = {c['market']: c for c in candidates}
                    candidates = [
                        candidates_dict[market] 
                        for market in candidates_markets 
                        if market in candidates_dict
                    ]
                    
                    # 🔍 상위 5개 출력 (디버깅용)
                    for i, item in enumerate(sorted_by_4h[:5], 1):
                        market = item['market']
                        volume_krw = item['volume_krw']
                        logger.info(f"   {i}. {market}: {volume_krw:,.0f}원 (24h proxy)")
                    
                    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
                    # 🌱 Track2: Birth-Watch 후보 수집
                    # - candidates 직접 추가 금지 유지
                    # - result 생성 후 별도 주입 → Bot7 GC필터/GAJAMI/CORE-GATE 심사
                    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
                    try:
                        if momentum_candidates:
                            momentum_candidates.sort(
                                key=lambda x: (x["change_24h"], x["volume_krw"]),
                                reverse=True
                            )
                            track2_birth_watch_markets = [
                                m["market"] for m in momentum_candidates[:MAX_MOMENTUM_WATCHLIST]
                            ]
                            logger.info(
                                f"🟡 [Track2-BirthWatch-Candidate] "
                                f"모멘텀 후보 {len(track2_birth_watch_markets)}개 수집 | "
                                f"sample={track2_birth_watch_markets[:5]}"
                            )
                    except Exception:
                        track2_birth_watch_markets = []
                    
                else:
                    # 폴백: 2시간 데이터 부족 시 24시간 사용
                    logger.warning(
                        f"⚠️ 24h liquidity proxy 데이터 부족 "
                        f"({len(volume_data_24h_proxy)}개 < {candidate_count}개) "
                        f"→ 1차 선정 50개 내에서 24시간 기준 사용"
                    )
                    # 🔥 1차 50개만 필터링
                    first_pass_tickers = [t for t in sorted_by_24h if t["market"] in top_markets_1st]
                    # 🔥 그 중 상위 30개 선택
                    candidates = first_pass_tickers[:candidate_count]
                    logger.info(
                        f"✅ 24시간 거래대금 상위 {len(candidates)}개 후보 선정 (폴백)"
                    )
            else:
                # liquidity recheck 비활성화 시: 24시간 기준 사용
                # 🔥 1차 50개만 필터링
                first_pass_tickers = [t for t in sorted_by_24h if t["market"] in top_markets_1st]
                # 🔥 그 중 상위 30개 선택
                candidates = first_pass_tickers[:candidate_count]
                logger.info(
                    f"✅ 24시간 거래대금 상위 {len(candidates)}개 후보 선정"
                )
            
            # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
            # 정석 구조: MINI 우선 확정 → 남은 슬롯만 보조 lane 보충
            # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
            _track2_injected = []

            try:
                # 🔒 [REVERSAL-ADMISSION-REVOKE-0903] ★499-R1
                # ★496-R2는 Reversal의 선순위만 철회하고 잔여슬롯 편입권을 남겼다.
                # 09-03 실제 REVERSAL 선정 BUY 4건은 전부 J1 단독(1000)으로 집행되어 4/4 손실.
                # J4와는 무관한 selection lane이므로 producer/audit은 관측용으로 남기고
                # result 특별편입 권한만 회수한다. 종목 자체를 금지하지 않으므로
                # MINI/Track2/normal 후보 자격이 있으면 기존 경로로는 그대로 들어올 수 있다.
                # 새 상수·threshold 0개 — admission 권한만 회수.
                result = list(dict.fromkeys(
                    _mini_core_priority
                ))[:top_markets_count]

                # 2) 남은 슬롯만 기타 보조 lane으로 보충
                _remaining_slots = top_markets_count - len(result)
                if _remaining_slots > 0:
                    _mini_slow_watch = list(dict.fromkeys(_mini_slow_priority))[
                        :max(3, min(8, MAX_MOMENTUM_WATCHLIST))
                    ]

                    # 🔒 [MOMENTUM-KEEPALIVE-KILL-0819]
                    # 🔒 [REVERSAL-ADMISSION-REVOKE-0903] ★499-R1
                    # Track2/PRE_GC_SLOW만 잔여슬롯 보조 편입을 담당한다.
                    # Reversal BirthWatch는 후보 관측만 유지하고 result에는 특별편입하지 않는다.
                    # downstream Strategy의 J1~J4/ATR 공통 게이트는 그대로 적용된다.
                    _aux_candidates = list(dict.fromkeys(
                        track2_birth_watch_markets
                        + _mini_slow_watch
                    ))

                    for _m in _aux_candidates:
                        if _remaining_slots <= 0:
                            break
                        if _m and _m not in result and self._mini_constitution_pre_ok(_m)[0]:
                            result.append(_m)
                            _remaining_slots -= 1
                            if _m in track2_birth_watch_markets:
                                _track2_injected.append(_m)

                # 3) PRE_GC_SLOW는 fill보다 먼저 제한 편입한다.
                # - rank6은 아직 ETA가 느린 후보지만, VOL/SURGE보다 birth proximity가 높다.
                # - 매수 PASS 아님. Bot7 GC/GAJAMI/CORE-GATE 최종 심사는 그대로 유지.

                # 4) 남은 슬롯에 기존 candidates(VOL/SURGE)를 제한 편입
                _remaining_slots = top_markets_count - len(result)
                if _remaining_slots > 0:
                    _result_set = set(result)
                    for _m in [c['market'] for c in candidates]:
                        if _remaining_slots <= 0:
                            break
                        if _m and _m not in _result_set and self._mini_constitution_pre_ok(_m)[0]:
                            result.append(_m)
                            _result_set.add(_m)
                            _remaining_slots -= 1

                # 5) 최종 하드캡
                result = result[:top_markets_count]

                # 로그
                if _track2_injected:
                    logger.info(
                        f"🌱 [Track2-BirthWatch-Inject] "
                        f"result 주입 {len(_track2_injected)}개 | "
                        f"injected={_track2_injected[:5]} | "
                        f"final_count={len(result)}"
                    )

            except Exception as e:
                logger.debug(f"⚠️ [Inject-통합] 실패: {e}")
                result = [
                    c['market'] for c in candidates
                    if c.get('market') and self._mini_constitution_pre_ok(c['market'])[0]
                ][:top_markets_count]
            # 🔒 [INCLUDE-FINAL-RESTORE-0822]
            # included_markets = 거래량/레인 순위와 무관하게 최종 MainPool에 강제 포함.
            # 단, EXCLUSION_LIST 명시 제외는 항상 우선한다.
            # 스캔 대상만 강제한다. GC/헌법/판사/Strategy 진입조건은 절대 우회하지 않는다.
            try:
                _inc_final = []
                if self.config.get_cached('priority_include'):
                    _inc_valid = set(valid_markets or [])
                    _inc_excluded = set(
                        self.config.get_excluded_markets() or []
                    )
                    for _im in (self.config.get_included_markets() or []):
                        _im = str(_im).strip()
                        if (
                            _im.startswith("KRW-")
                            and _im in _inc_valid
                            and _im not in _inc_excluded
                            and _im not in _inc_final
                        ):
                            _inc_final.append(_im)
                if _inc_final:
                    _inc_before = set(result or [])
                    _inc_added = [
                        _im for _im in _inc_final
                        if _im not in _inc_before
                    ]
                    result = list(
                        dict.fromkeys(
                            _inc_final + list(result or [])
                        )
                    )[:top_markets_count]
                    logger.info(
                        f"⭐ [INCLUDE-FINAL] configured={len(_inc_final)} "
                        f"added={len(_inc_added)} | "
                        f"added={_inc_added} | "
                        f"final_included="
                        f"{[_im for _im in _inc_final if _im in result]}"
                    )
            except Exception as _inc_e:
                logger.warning(
                    f"[INCLUDE-FINAL] 스킵: "
                    f"{type(_inc_e).__name__}:{str(_inc_e)[:80]}"
                )

            try:
                _prev_cache_set = set(self.top_markets_cache or [])
                _new_entries = [m for m in result if m not in _prev_cache_set]

                if _prev_cache_set and _new_entries:
                    _rank24_map = {
                        t.get('market'): i + 1
                        for i, t in enumerate(filtered_by_price)
                    }
                    _ticker_map = {t.get('market'): t for t in filtered_by_price}
                    _surge_markets = set(_surge_markets_for_trace)
                    _candidate_markets = [c.get('market') for c in candidates]

                    try:
                        _prev_cache_age = (
                            datetime.now() - self.last_analysis_time
                        ).total_seconds() if self.last_analysis_time else -1.0
                    except Exception:
                        _prev_cache_age = -1.0

                    for _sym in _new_entries[:10]:
                        _t = _ticker_map.get(_sym, {})
                        _rank24 = _rank24_map.get(_sym)
                        logger.info(
                            f"🔍 [MARKET-NEW-ENTRY-TRACE] {_sym} | "
                            f"prev_cache=0 new_entry=1 "
                            f"prev_cache_age={_prev_cache_age:.1f}s "
                            f"ttl={self.integrated_cache_ttl}s "
                            f"rank24h={_rank24 if _rank24 is not None else 'OUT'} "
                            f"in_first_pass={int(_rank24 is not None and _rank24 <= first_pass_count)} "
                            f"in_surge={int(_sym in _surge_markets)} "
                            f"candidate_rank={(_candidate_markets.index(_sym) + 1) if _sym in _candidate_markets else 'OUT'} "
                            f"result_rank={(result.index(_sym) + 1) if _sym in result else 'OUT'} "
                            f"top_markets_count={top_markets_count} "
                            f"acc24h={float(_t.get('acc_trade_price_24h', 0)):,.0f} "
                            f"signed={float(_t.get('signed_change_rate', 0)) * 100:+.2f}% "
                            f"price={float(_t.get('trade_price', 0)):.8f}"
                        )
            except Exception as e:
                logger.debug(f"⚠️ [MARKET-NEW-ENTRY-TRACE] 실패: {e}")
            
            # ========================================
            # 6. 결과 리포트 (간소화)
            # ========================================
            logger.info("=" * 70)
            logger.info(f"✅ 최종 선정 결과:")
            logger.info(
                f"   - 1차: {len(top_markets_1st)}개 "
                f"(제외/가격필터 후 24h 거래대금 상위)"
            )
            logger.info(
                f"   - 2차: {len(result)}개 "
                f"(미니GC 우선 + vol안전망 + 보조 lane · SURGE_FILL 미사용)"
            )
            logger.info(f"   - 3차(ATR): PASS")
            logger.info(f"   - 4차(거래대금): PASS")
            logger.info(
                f"   - 진입 필터: 확정 GC 최근성 + Strategy/Bot 공통 최종 심사"
            )
            
            logger.info(f"\n📊 선정 종목 전체 ({len(result)}개):")
            try:
                _ms_locals = locals()

                _mst_mini   = set(_ms_locals.get("_mini_set", set()))
                _mst_vol    = set(_ms_locals.get("_vol_fill", []))
                _mst_surge  = set(_ms_locals.get("_surge_fill", []))
                _mst_track2 = set(_ms_locals.get("_track2_injected", []))

                _mini_rank_fn       = _ms_locals.get("_mini_gc_rank")
                _mini_priority_list = list(_ms_locals.get("_mini_priority", []))

                _ticker_map_log = {
                    t.get("market"): t
                    for t in _ms_locals.get("filtered_by_price", [])
                    if isinstance(t, dict) and t.get("market")
                }

                def _mst_mini_label(m: str) -> str:
                    try:
                        if not callable(_mini_rank_fn):
                            return "MINI_UNKNOWN"
                        _rank_tuple = _mini_rank_fn(m)
                        _rank_no  = int(_rank_tuple[0])
                        _rank_val = float(_rank_tuple[1])
                        if   _rank_no == 0: _label = "MINI_GC_0B"
                        elif _rank_no == 1: _label = "MINI_PRE_GC_ETA"
                        elif _rank_no == 2: _label = "MINI_GC_1B"
                        elif _rank_no == 3: _label = "MINI_GC_2B"
                        elif _rank_no == 4: _label = "MINI_GC_3B"
                        elif _rank_no == 6: _label = "MINI_PRE_GC_SLOW"
                        elif _rank_no == 9: _label = "MINI_OTHER"
                        else:               _label = "MINI_UNKNOWN"
                        _order = _mini_priority_list.index(m) + 1 if m in _mini_priority_list else 9999
                        return f"{_label}(order={_order},rank={_rank_no},v={_rank_val:.3f})"
                    except Exception:
                        return "MINI_UNKNOWN"

                for i, market in enumerate(result, 1):
                    if   market in _mst_track2: _source = "TRACK2_BIRTH_WATCH"
                    # 🔒 [MOMENTUM-KEEPALIVE-KILL-0819] 死 레인 제거: 671건/일 편입, 실거래 0건.
                    elif market in _mst_mini:   _source = _mst_mini_label(market)
                    elif market in _mst_vol:    _source = "VOL_SAFETY"
                    elif market in _mst_surge:  _source = "SURGE_FILL"
                    else:                       _source = "BASE_OR_FALLBACK"
                    _t      = _ticker_map_log.get(market, {})
                    _rank24 = int(_ms_locals.get("_vol_rank_map", {}).get(market, 9999) or 9999)
                    _chg_rk = int(_ms_locals.get("_chg_rank_map", {}).get(market, 9999) or 9999)
                    _signed = float((_t.get("signed_change_rate", 0.0) or 0.0) * 100.0)
                    _acc24h = float(_t.get("acc_trade_price_24h", _t.get("volume_krw", 0.0)) or 0.0)
                    logger.info(
                        f"   {i:2d}. {market} | source={_source} | "
                        f"rank24h={_rank24} chg_rank={_chg_rk} "
                        f"signed={_signed:+.2f}% acc24h={_acc24h:,.0f}"
                    )
            except Exception as _mst_e:
                logger.debug(f"⚠️ [MARKET-SELECTION-TRACE] 실패: {_mst_e}")
                for i, market in enumerate(result, 1):
                    logger.info(f"   {i:2d}. {market}")
            
            logger.info("=" * 70)
            logger.info(f"🔥 [PASS] ATR 계산+선정 주석 처리 → 2차 {len(result)}개 반환 (API 절약)")
            logger.info("=" * 70)
            
            # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
            # 🆕 불장 모드 판단 (4시간 경계에서만 실행)
            # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
            # Phase 2: 4시간봉은 4시간 경계(00, 04, 08, 12, 16, 20시)에서만 분석
            from datetime import datetime
            now = datetime.now()
            
            # 4시간 경계 체크 (시작 직후 3분 이내)
            is_4h_boundary = (now.hour % 4 == 0) and (now.minute < 3)
            
            # 초기화 시 강제 실행 플래그
            if not hasattr(self, '_bull_analysis_initialized'):
                self._bull_analysis_initialized = False
            
            # 4시간 경계이거나 초기화 필요 시에만 실행
            if is_4h_boundary or not self._bull_analysis_initialized:
                try:
                    if not self._bull_analysis_initialized:
                        logger.info("🔥 [불장분석] 초기화 실행...")
                    else:
                        logger.info(f"🔥 [불장분석] 4시간 경계 ({now.hour:02d}:00) → 실행")
                    
                    # 상위 30개 마켓 (불장 분석 경량화)
                    top50_for_bull = top_markets_1st[:30] if len(top_markets_1st) >= 30 else top_markets_1st
                    
                    # ticker 전일대비 등락률 기반 regime 근사 (API 호출 0회)
                    _tdct = {t['market']: t for t in (sorted_by_24h or [])}
                    regimes_4h = {}
                    for market in top50_for_bull:
                        _chg = float(_tdct.get(market, {}).get('signed_change_rate', 0)) * 100
                        if _chg > 1.0:
                            regimes_4h[market] = MarketRegime.BREAKOUT
                        elif _chg > 0.0:
                            regimes_4h[market] = MarketRegime.TRENDING_UP
                        elif _chg < -1.0:
                            regimes_4h[market] = MarketRegime.BREAKDOWN
                        elif _chg < 0.0:
                            regimes_4h[market] = MarketRegime.TRENDING_DOWN
                        else:
                            regimes_4h[market] = MarketRegime.RANGING
                    logger.info(f"   - 4H 체제 수집 완료(ticker기반): {len(regimes_4h)}개")
                    
                    # 불장 판단 실행
                    bull_mode, heat_score = await self.calculate_bull_mode(top50_for_bull, regimes_4h, sorted_by_24h)
                    
                    # 상승 종목 수 계산
                    bull_count = len([
                        r for r in regimes_4h.values() 
                        if r in [MarketRegime.TRENDING_UP, MarketRegime.BREAKOUT]
                    ])
                    
                    logger.info("=" * 70)
                    logger.info(f"📊 [시장 상태 분석 결과]")
                    logger.info(f"   - 🌡️  시장 열기: {heat_score:.3f}")
                    logger.info("   - 📈 상승 종목: %d/%d (%.1f%%)", bull_count, len(regimes_4h), bull_count/max(1,len(regimes_4h))*100)
                    logger.info(f"   - 🔥 불장 모드: {'🟢 ON' if bull_mode else '⚪ OFF'}")
                    
                    if bull_mode:
                        logger.info(f"   - 💡 진입 완화: 무릎상한↑ | 거래량↓ | 패턴완화 | Threshold↓")
                    else:
                        logger.info(f"   - 🛡️  보수 모드: 기존 엄격 기준 유지")
                    
                    logger.info("=" * 70)
                    
                    # 초기화 완료 표시 (성공 시)
                    self._bull_analysis_initialized = True
                    
                except Exception as e:
                    logger.warning(f"⚠️ 불장 판단 실패 (기본값 사용): {e}")
                    self.global_bull_mode = False
                    self.market_heat_score = 0.0
                    
                    # 에러 발생해도 초기화 완료 표시 (무한 재시도 방지)
                    self._bull_analysis_initialized = True
            else:
                # 4시간 경계 아님 - 기존 값 유지
                logger.debug(
                    f"📦 [불장분석] 스킵 (다음 경계: {((now.hour // 4 + 1) * 4) % 24:02d}:00)"
                )
            # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
            
            # 🔒 캐시 업데이트 (12분 주기 유지)
            self.top_markets_cache = result
            self.last_analysis_time = datetime.now()
            logger.debug(f"📦 [캐시] 종목 선정 결과 캐시 저장 ({len(result)}개) | TTL: {self.integrated_cache_ttl}초")
            
            return result
            
        except Exception as e:
            logger.error(f"통합 분석 오류: {e}")
            import traceback
            logger.error(f"상세:\n{traceback.format_exc()}")
            return []
    
    # ─────────────────────────────────────────────────────
    # 🆕 전 종목 미니 MA 감시층 — 관측권 복구
    # ─────────────────────────────────────────────────────
    _MINI_CACHE_BARS      = 21          # 미니GC 단기 판정 window
    _MINI_LONG_CACHE_BARS = 141         # 저장/복원 cap: 122 게이트 + 여유분
    _MINI_LONG_GATE_BARS  = 122         # 가자미 MA120 guard(len(df)>=122)와 동일
    _MINI_TF_SECS         = 180
    _MINI_GAP_MAX         = 0.15
    _MINI_STALE_SEC       = 1800
    _MINI_FRESH_MAX_AGE   = 3

    @staticmethod
    def _mini_gc_age_from_history(
        ma5p1, ma10p1, ma5p2, ma10p2, ma5p3, ma10p3, ma5p4, ma10p4
    ) -> int:
        """미니GC 최근성 SSoT: 현재 GC 기준 0~3봉, 그 외 99."""
        return (
            0 if ma5p1 <= ma10p1 else
            1 if ma5p2 <= ma10p2 else
            2 if ma5p3 <= ma10p3 else
            3 if ma5p4 <= ma10p4 else
            99
        )

    def _get_mini_gc_cache_path(self) -> str:
        from pathlib import Path          # ← #1 인라인 import
        return str(Path("outputs") / "cache" / "mini_gc_cache_3m.json")

    def _load_mini_gc_cache(self) -> None:
        import json, time as _t
        try:
            path = self._get_mini_gc_cache_path()
            with open(path, "r", encoding="utf-8") as f:
                data = json.load(f)
            if data.get("version") != 1 or data.get("timeframe_secs") != self._MINI_TF_SECS:
                logger.info("[MINI-GC-CACHE] 버전 불일치 → 폐기")
                return
            # 30분 통째폐기 삭제 — 미니캐시는 재시작 넘어 141봉 누적 보존
            n = 0
            for sym, entries in data.get("markets", {}).items():
                if isinstance(entries, list) and entries:
                    _norm = self._normalize_mini_cache_entries(entries)
                    if _norm:
                        self._mini_candle_cache[sym] = _norm
                        n += 1
            logger.info(f"[MINI-GC-CACHE] 복원 {n}개 종목")
            self._recalc_mini_gc_pools()
        except FileNotFoundError:
            logger.info("[MINI-GC-CACHE] 파일 없음 → 워밍업 시작")
        except Exception as e:
            logger.warning(f"[MINI-GC-CACHE] 복원 실패({e}) → 워밍업")
            self._mini_candle_cache = {}

    def _save_mini_gc_cache(self) -> None:
        import json, os, time as _t
        from pathlib import Path
        try:
            path = self._get_mini_gc_cache_path()
            tmp  = path + ".tmp"
            Path(path).parent.mkdir(parents=True, exist_ok=True)
            _now = _t.time()
            _dead_sec = 12 * 3600   # 12h 이상 미갱신 = 죽은/빠진 종목 → 저장 제외
            data = {
                "version": 1,
                "timeframe_secs": self._MINI_TF_SECS,
                "saved_at": _now,
                "markets": {
                    sym: entries[-(self._MINI_LONG_CACHE_BARS + 2):]
                    for sym, entries in self._mini_candle_cache.items()
                    if entries
                    and (_now - float(entries[-1].get("bucket", 0) or 0)) <= _dead_sec
                },
            }
            with open(tmp, "w", encoding="utf-8") as f:
                json.dump(data, f)
            os.replace(tmp, path)
        except Exception as e:
            logger.debug(f"[MINI-GC-CACHE] 저장 실패: {e}")

    def _upsert_mini_cache_bar(
        self,
        sym: str,
        bucket,
        close,
        src_ts,
        now_ts,
        sealed: bool = False,
    ) -> str:
        """미니GC cache bar 단일 upsert SSoT.

        반환:
            written | updated | old_skip | sealed_skip | invalid

        정책:
            - ticker(sealed=False): forming bucket 임시값
            - REST(sealed=True): closed bucket 권위값
            - 같은 bucket 충돌 시 REST sealed 우선, ticker는 버림
        """
        try:
            if not sym:
                return "invalid"

            _bucket = int(bucket or 0)
            _close = float(close or 0.0)
            _src_ts = float(src_ts or 0.0)
            _rx_ts = float(now_ts or 0.0)
            _sealed = int(bool(sealed))

            if _bucket <= 0 or _close <= 0.0 or not math.isfinite(_close):
                return "invalid"

            _bar = {
                "bucket": _bucket,
                "close": _close,
                "src_ts": _src_ts,
                "rx_ts": _rx_ts,
            }
            if _sealed:
                _bar["sealed"] = 1

            _entries = self._mini_candle_cache.setdefault(sym, [])

            if _entries:
                _last_bucket = int(_entries[-1].get("bucket", 0) or 0)

                if _last_bucket == _bucket:
                    _old_src_ts = float(_entries[-1].get("src_ts", 0.0) or 0.0)
                    _old_sealed = int(_entries[-1].get("sealed", 0) or 0)

                    # 🔒 REST 권위 closed봉은 ticker가 다시 덮지 못한다.
                    if _old_sealed and not _sealed:
                        return "sealed_skip"

                    if _sealed or _src_ts >= _old_src_ts:
                        _entries[-1].update(_bar)
                        return "updated"

                    return "old_skip"

                if _last_bucket > _bucket:
                    if _sealed:
                        _entries.append(_bar)
                        return "written"
                    return "old_skip"

            _entries.append(_bar)
            return "written"

        except Exception:
            return "invalid"

    def _normalize_mini_cache_entries(self, entries):
        """미니GC cache 정규화: bucket 중복/역순/불량 close 제거. sealed REST 우선."""
        try:
            _by_bucket = {}

            for _seq, e in enumerate(entries or []):
                if not isinstance(e, dict):
                    continue

                try:
                    _bucket = int(e.get("bucket", 0) or 0)
                    _close = float(e.get("close", 0.0) or 0.0)
                    _src_ts = float(e.get("src_ts", e.get("last_ts", 0.0)) or 0.0)
                    _rx_ts = float(e.get("rx_ts", 0.0) or 0.0)
                    _sealed = int(e.get("sealed", 0) or 0)
                except Exception:
                    continue

                if _bucket <= 0 or _close <= 0.0:
                    continue

                if not math.isfinite(_close):
                    continue

                _cur = {
                    "bucket": _bucket,
                    "close": _close,
                    "src_ts": _src_ts,
                    "rx_ts": _rx_ts,
                    "_seq": _seq,
                }
                if _sealed:
                    _cur["sealed"] = 1

                _prev = _by_bucket.get(_bucket)
                if _prev is None:
                    _by_bucket[_bucket] = _cur
                    continue

                _prev_src = float(_prev.get("src_ts", 0.0) or 0.0)
                _prev_seq = int(_prev.get("_seq", -1) or -1)
                _prev_sealed = int(_prev.get("sealed", 0) or 0)

                if _sealed != _prev_sealed:
                    _replace = bool(_sealed > _prev_sealed)
                else:
                    _replace = bool(
                        (_src_ts > _prev_src)
                        or (_src_ts == _prev_src and _seq > _prev_seq)
                    )

                if _replace:
                    _by_bucket[_bucket] = _cur

            _out = []
            for _bucket in sorted(_by_bucket.keys()):
                _e = dict(_by_bucket[_bucket])
                _e.pop("_seq", None)
                _out.append(_e)

            # 오래된 데이터는 종목별/구간별로 삭제한다.
            # 파일 전체 saved_at 폐기는 금지: 짧은 재시작의 141봉 누적을 보존해야 한다.
            import time as _t
            _gap_limit = int(self._MINI_TF_SECS) * 4
            _keep_n = int(self._MINI_LONG_CACHE_BARS) + 2
            _now_bucket = int(_t.time() // self._MINI_TF_SECS) * self._MINI_TF_SECS

            if _out:
                _last_bucket = int(_out[-1].get("bucket", 0) or 0)
                if _last_bucket <= 0 or (_now_bucket - _last_bucket) > _gap_limit:
                    return []

                _cut_idx = 0
                for _i in range(len(_out) - 1):
                    _gap = (
                        int(_out[_i + 1].get("bucket", 0) or 0)
                        - int(_out[_i].get("bucket", 0) or 0)
                    )
                    if _gap > _gap_limit:
                        _cut_idx = _i + 1

                if _cut_idx > 0:
                    _out = _out[_cut_idx:]

            return _out[-_keep_n:]

        except Exception as e:
            logger.debug(f"[MINI-GC-CACHE] normalize 실패: {e}")
            return []

    def _on_rest_candles_for_mini_gc(self, market: str, timeframe: str, candles):
        """MarketDataService raw REST candle callback adapter."""
        try:
            if str(timeframe or "").strip().lower() != "3m":
                return {}
            return self.sync_mini_gc_cache_from_rest_candles(market, candles)
        except Exception as e:
            logger.debug(f"[MINI-REST-SYNC] callback adapter 실패: {market} {timeframe} | {e}")
            return {}

    def sync_mini_gc_cache_from_rest_candles(
        self,
        symbol: str,
        candles,
        now_ts: float = None,
    ) -> dict:
        """REST 3m closed candles → miniGC cache sealed sync.

        입력:
            candles: Upbit 원본 candle dict list.
                     반드시 candle["timestamp"] ms UTC를 사용한다.
                     df.index.timestamp() 사용 금지.
        """
        import time as _time

        _stats = {"written": 0, "updated": 0, "sealed_skip": 0, "old_skip": 0, "invalid": 0}

        try:
            if not symbol or not candles:
                return _stats

            _now_ts = float(now_ts or _time.time())
            _now_bucket = int(_now_ts // self._MINI_TF_SECS) * self._MINI_TF_SECS

            for _c in candles:
                if not isinstance(_c, dict):
                    _stats["invalid"] += 1
                    continue

                try:
                    _ts_raw = _c.get("timestamp")
                    _close = _c.get("trade_price", _c.get("close"))

                    if _ts_raw is None or _close is None:
                        _stats["invalid"] += 1
                        continue

                    _ts = float(_ts_raw)
                    if _ts > 10_000_000_000:
                        _ts = _ts / 1000.0

                    _bkt = int(_ts // self._MINI_TF_SECS) * self._MINI_TF_SECS

                    if _bkt >= _now_bucket:
                        continue

                    _st = self._upsert_mini_cache_bar(
                        symbol, _bkt, float(_close), float(_bkt), _now_ts, sealed=True,
                    )

                    if _st in _stats:
                        _stats[_st] += 1
                    else:
                        _stats["invalid"] += 1

                except Exception:
                    _stats["invalid"] += 1
                    continue

            _entries = self._mini_candle_cache.get(symbol, [])
            if _entries:
                _entries[:] = self._normalize_mini_cache_entries(_entries)

            if _stats["written"] or _stats["updated"]:
                self._update_mini_gc_one(symbol)
                logger.debug(
                    f"[MINI-REST-SYNC] {symbol} | "
                    f"sealed_written={_stats['written']} "
                    f"sealed_updated={_stats['updated']} "
                    f"sealed_skip={_stats['sealed_skip']} "
                    f"old_skip={_stats['old_skip']} "
                    f"invalid={_stats['invalid']}"
                )

            return _stats

        except Exception as e:
            logger.debug(f"[MINI-REST-SYNC] {symbol} 실패: {e}")
            return _stats

    def _mini_ticker_source_ts(self, ticker: dict, fallback_ts: float) -> float:
        """티커 payload의 체결/서버 timestamp를 초 단위로 정규화한다. 없으면 수신시각 사용."""
        try:
            for _k in ("trade_timestamp", "timestamp", "acc_trade_timestamp"):
                _v = ticker.get(_k)
                if _v is None:
                    continue

                _ts = float(_v)
                if _ts > 10_000_000_000:
                    _ts = _ts / 1000.0

                if 946684800 <= _ts <= 4102444800:
                    return _ts
        except Exception:
            pass

        return float(fallback_ts)

    def _get_mini_closed_window(self, sym: str, n: int = None):
        import time as _t

        n = int(n or self._MINI_CACHE_BARS)
        _now_bucket = int(_t.time() // self._MINI_TF_SECS) * self._MINI_TF_SECS

        entries = self._mini_candle_cache.get(sym, [])
        _closed = [
            e for e in entries
            if isinstance(e, dict)
            and int(e.get("bucket", 0) or 0) < _now_bucket
        ]

        if len(_closed) < n:
            return [], _now_bucket

        return _closed[-n:], _now_bucket

    def _mini_long_gate_ok(self, sym: str) -> tuple:
        """미니GC 장기MA gate.
        반환: (ok, trusted, sealed_cnt, s60, s120, fail)
        정책: 티커 base 122봉으로 장기MA 판정. REST sealed는 품질보정/계측만.
        """
        try:
            _long_n = int(self._MINI_LONG_GATE_BARS)
            _entries, _now_bucket = self._get_mini_closed_window(sym, _long_n)

            if len(_entries) < _long_n:
                return True, False, 0, 0.0, 0.0, "INSUFFICIENT"
            _buckets = [int(e.get("bucket", 0) or 0) for e in _entries]
            if _now_bucket - _buckets[-1] > self._MINI_TF_SECS * 3:
                return True, False, 0, 0.0, 0.0, "STALE"
            _sealed_cnt = sum(
                1 for e in _entries
                if int(e.get("sealed", 0) or 0)
            )

            # sealed% 게이트 삭제 — 티커 base로 장기MA 판정, REST는 정확도만 개선
            _c = [float(e.get("close", 0.0) or 0.0) for e in _entries]
            _s60  = _struct20_slope_from_closes(_c, 60)
            _s120 = _struct20_slope_from_closes(_c, 120)
            _ok, _fail = _long_ma_alive_verdict(_s60, _s120, None)
            return bool(_ok), True, _sealed_cnt, float(_s60), float(_s120), (_fail or "PASS")
        except Exception as e:
            return True, False, 0, 0.0, 0.0, f"ERR:{str(e)[:40]}"

    def _mini_constitution_pre_ok(self, sym: str) -> tuple:
        """편입 pre-check. 헌법 무조건 필요조건만 확신 거부 → false-reject 0.
        반환: (ok, fail_reason)
        """
        try:
            N = self._MINI_CACHE_BARS
            _closed, _nb = self._get_mini_closed_window(sym, N)
            if len(_closed) < N:
                return True, "INSUFFICIENT"
            _bk = [int(e.get("bucket", 0) or 0) for e in _closed]
            if _nb - _bk[-1] > self._MINI_TF_SECS * 3:
                return True, "STALE"
            c = [float(e.get("close", 0.0) or 0.0) for e in _closed]
            ma5       = sum(c[-5:]) / 5
            ma10      = sum(c[-10:]) / 10
            ma5_slope = ma5 - (sum(c[-6:-1]) / 5)
            if ma5 <= ma10:
                logger.debug(f"🚫 [SELECT-PRECHECK] {sym} fail=PRE_GC_FAIL (ma5<=ma10)")
                return False, "PRE_GC_FAIL"
            if ma5_slope <= 0.0:
                logger.debug(f"🚫 [SELECT-PRECHECK] {sym} fail=PRE_MA5_NOT_UP")
                return False, "PRE_MA5_NOT_UP"
            _lok, _t, _sc, _s60, _s120, _lf = self._mini_long_gate_ok(sym)
            if not _lok and str(_lf).startswith("LONG_STRONG_DOWN"):
                logger.info(f"🚫 [SELECT-PRECHECK] {sym} fail=PRE_{_lf} s60={_s60:.4f} s120={_s120:.4f}")
                return False, f"PRE_{_lf}"
            if not _lok:
                logger.info(f"🟡 [SELECT-PRECHECK-LONG-SLACK] {sym} fail=PRE_{_lf} s60={_s60:.4f} s120={_s120:.4f} → 대상유지")
            return True, "PASS"
        except Exception as e:
            return True, f"ERR:{str(e)[:30]}"

    def _update_mini_gc_one(self, sym: str) -> None:
        N = self._MINI_CACHE_BARS
        _closed, _now_bucket = self._get_mini_closed_window(sym, N)

        if len(_closed) < N:
            self._pre_gc_pool.discard(sym)
            self._fresh_gc_pool.discard(sym)
            return

        buckets = [int(e["bucket"]) for e in _closed]

        # 🔧 [GATEKEEPER-STALE-정합] 문지기(미니GC)는 판사(_is_candle_dense_enough
        # /데이터지연 4×TF=12분)보다 약하거나 같아야 한다. 기존 3×TF(9분)는
        # 판사보다 엄격해 GC 직후 종목을 조기 탈락시켰다. 5×TF(15분)로 완화.
        if _now_bucket - buckets[-1] > self._MINI_TF_SECS * 5:
            self._pre_gc_pool.discard(sym)
            self._fresh_gc_pool.discard(sym)
            return

        c = [float(e["close"]) for e in _closed]
        _mini_all = self._mini_candle_cache.get(sym, [])
        _mini_cur = next(
            (float(e["close"]) for e in reversed(_mini_all)
             if isinstance(e, dict)
             and int(e.get("bucket", 0) or 0) == _now_bucket),
            None,
        )
        _mini_ma5_rt = bool(_mini_cur is not None and _mini_cur > 0.0)
        ma5 = (
            (sum(c[-4:]) + _mini_cur) / 5
            if _mini_ma5_rt
            else sum(c[-5:]) / 5
        )
        ma10 = (
            (sum(c[-9:]) + _mini_cur) / 10
            if _mini_ma5_rt
            else sum(c[-10:]) / 10
        )
        ma5p1  = sum(c[-6:-1])  / 5
        ma10p1 = sum(c[-11:-1]) / 10
        ma5p2  = sum(c[-7:-2])  / 5
        ma10p2 = sum(c[-12:-2]) / 10
        ma5p3  = sum(c[-8:-3])  / 5
        ma10p3 = sum(c[-13:-3]) / 10
        ma5p4  = sum(c[-9:-4])  / 5
        ma10p4 = sum(c[-14:-4]) / 10
        ma20   = sum(c[-20:])   / 20
        ma20p1 = sum(c[-21:-1]) / 20
        # ← #3 MA slope (가격 틱이 아닌 MA 변화량)
        ma5_slope  = ma5  - ma5p1
        ma10_slope = ma10 - ma10p1
        ma20_slope = ma20 - ma20p1
        signed_gap = ((ma5 - ma10) / ma10 * 100.0) if ma10 > 0 else 0.0
        # MA10/MA20 후행 상태 — 대상선정 관측용
        # - 미니GC 후보선정에는 사용하지 않음
        # - 실제 dead-cat 차단은 후단 가자미근/생애주기 진입체크에 위임
        _mini_mid_alive = bool(
            ma10_slope > 0.0
            or ma20_slope >= 0.0
        )        
        gap_abs = abs(signed_gap)
        gap_pct = gap_abs
        bucket_age = int(_now_bucket - buckets[-1])
        # FRESH_GC 최근성은 단일 SSoT 계산을 로그·편입·rank가 함께 소비한다.
        _mini_gc_age_hint = self._mini_gc_age_from_history(
            ma5p1, ma10p1, ma5p2, ma10p2,
            ma5p3, ma10p3, ma5p4, ma10p4,
        )

        if (ma5 < ma10 and gap_pct <= self._MINI_GAP_MAX
                and ma5_slope > 0 and ma5_slope > ma10_slope):
            _was_pre = sym in self._pre_gc_pool
            self._pre_gc_pool.add(sym)
            self._fresh_gc_pool.discard(sym)
            if not _was_pre:
                logger.info(
                    f"🟡 [MINI-GC-PRE] {sym} "
                    f"signed_gap={signed_gap:+.4f}% gap_abs={gap_abs:.4f}% "
                    f"ma5={ma5:.6f} ma10={ma10:.6f} "
                    f"s5={ma5_slope:+.6f} s10={ma10_slope:+.6f} "
                    f"closed_age={bucket_age}s ma5_rt={int(_mini_ma5_rt)} mid_closed=1"
                )
        # FRESH_GC: 대상선정 문지기
        # - 현재 MA5>=MA10 + 최근 0~3봉 GC만 편입
        # - 순간 MA5 slope·lift·중장기 품질은 후단 진입 판사에 위임
        elif (
            ma5 >= ma10
            and _mini_gc_age_hint <= self._MINI_FRESH_MAX_AGE
        ):
            _was_fresh = sym in self._fresh_gc_pool
            self._fresh_gc_pool.add(sym)
            self._pre_gc_pool.discard(sym)
            if not _was_fresh:
                logger.info(
                    f"🟢 [MINI-GC-FRESH] {sym} "
                    f"age={_mini_gc_age_hint} "
                    f"mid_alive={int(_mini_mid_alive)} "
                    f"ma10_ok={int(ma10_slope > 0.0)} "
                    f"ma20_ok={int(ma20_slope >= 0.0)} "
                    f"signed_gap={signed_gap:+.4f}% gap_abs={gap_abs:.4f}% "
                    f"ma5={ma5:.6f} ma10={ma10:.6f} ma20={ma20:.6f} "
                    f"s5={ma5_slope:+.6f} s10={ma10_slope:+.6f} s20={ma20_slope:+.6f} "
                    f"closed_age={bucket_age}s ma5_rt={int(_mini_ma5_rt)} mid_closed=1"
                )
        else:
            self._pre_gc_pool.discard(sym)
            self._fresh_gc_pool.discard(sym)

    def _recalc_mini_gc_pools(self) -> None:
        self._pre_gc_pool.clear()
        self._fresh_gc_pool.clear()
        for sym in list(self._mini_candle_cache.keys()):
            self._update_mini_gc_one(sym)
        logger.info(
            f"[MINI-GC-POOL] 재계산 완료 | "
            f"pre={len(self._pre_gc_pool)} fresh={len(self._fresh_gc_pool)}"
        )

    async def refresh_ticker_3m_close_cache(self) -> None:
        """3분마다 ticker 현재가 snapshot 저장 (OR 공식용, REST 캔들 0콜)"""
        try:
            import time as _time
            # 🔒 [MINI-CACHE-ALL-0821] 캐시 적재 universe는 전 KRW 종목.
            # 진입 자격은 perform_integrated_analysis()의
            # filtered_by_price / _valid_mini_set에서 기존대로 별도 제한한다.
            _all_mk = await self.market_data_service.get_markets()
            cache_markets = [
                m.get("market") for m in (_all_mk or [])
                if str(m.get("market", "")).startswith("KRW-")
            ]
            if not cache_markets:
                cache_markets = await self._get_filtered_markets_fast()
            if not cache_markets:
                logger.debug("[TICKER-3M-SNAP] cache_markets 없음")
                return
            tickers = await self.data_service.get_ticker(cache_markets)
            if not tickers:
                logger.debug("[TICKER-3M-SNAP] ticker 응답 없음")
                return
            old = getattr(self, '_ticker_3m_close_cache', {})
            now_ts = _time.time()
            new = {}
            _wall_bucket = int(now_ts // self._MINI_TF_SECS) * self._MINI_TF_SECS

            _mini_written = 0
            _mini_sealed_skip = 0
            _mini_old_skip = 0

            for t in tickers:
                _m  = t.get('market')
                _px = float(t.get('trade_price', 0) or 0)
                if not _m or _px <= 0:
                    continue

                _src_ts = self._mini_ticker_source_ts(t, now_ts)

                if _src_ts > now_ts + 5.0:
                    _src_ts = now_ts

                _src_bucket = int(_src_ts // self._MINI_TF_SECS) * self._MINI_TF_SECS

                _prev_last = float(old.get(_m, {}).get('last_close', 0.0) or 0.0)
                _prev_ts   = float(old.get(_m, {}).get('last_ts',   0.0) or 0.0)
                new[_m] = {
                    'prev_close': _prev_last,
                    'last_close': _px,
                    'prev_ts':    _prev_ts,
                    'last_ts':    _src_ts,
                    'last_bucket': _src_bucket,
                }

                # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
                # 미니GC cache upsert SSoT
                # - ticker: 현재 wall bucket만 갱신
                # - 닫힌 bucket은 이후 ticker로 수정 금지
                # - REST sealed closed봉은 ticker보다 우선
                # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
                if _src_bucket < _wall_bucket:
                    _mini_sealed_skip += 1
                    continue

                if _src_bucket > _wall_bucket:
                    _src_bucket = _wall_bucket

                _st = self._upsert_mini_cache_bar(
                    _m,
                    _src_bucket,
                    _px,
                    _src_ts,
                    now_ts,
                    sealed=False,
                )

                if _st in ("written", "updated"):
                    _mini_written += 1
                    _entries = self._mini_candle_cache.get(_m, [])
                    _entries[:] = self._normalize_mini_cache_entries(_entries)
                    self._update_mini_gc_one(_m)

                elif _st == "sealed_skip":
                    # REST sealed closed봉과 같은 bucket이면 ticker는 버린다.
                    _mini_sealed_skip += 1

                else:
                    _mini_old_skip += 1

            self._ticker_3m_close_cache = new
            _warmup = sum(1 for v in new.values() if float(v.get('prev_close', 0.0) or 0.0) <= 0)
            # 🔒 [MINI-GC-POOL-SYMS-OBSERVE-0830] ★455
            # MINI-GC-PRE/FRESH 는 풀 전이 시에만 로그된다(L16884 `if not _was_pre`).
            # 그래서 「그 시각 그 종목이 풀에 있었는가」를 사후에 확인할 수 없고,
            # ★446(BORA 08:12 상류 미심사) 관문⑤가 관측 부재로 닫히지 않는다.
            # 3분마다 1줄이므로 풀 스냅샷을 남긴다. 관측 전용 · 판정 변화 0.
            logger.info(
                f"📊 [TICKER-3M-SNAP] 저장 {len(new)}개 | warmup={_warmup} ts={now_ts:.0f} "
                f"| mini_pre={len(self._pre_gc_pool)} fresh={len(self._fresh_gc_pool)} "
                f"| pre_syms={sorted(self._pre_gc_pool)} "
                f"| fresh_syms={sorted(self._fresh_gc_pool)} "
                f"| mini_write={_mini_written} sealed_skip={_mini_sealed_skip} old_skip={_mini_old_skip}"
            )
            self._save_mini_gc_cache()
        except Exception as _e:
            logger.debug(f"[TICKER-3M-SNAP] 실패: {_e}")

    async def get_current_top_markets(self) -> List[str]:
        """현재 상위 마켓"""
        _market_debug_symbols_raw = self.config.get(
            'MARKET_SELECTION', 'market_selection_debug_symbols', fallback=''
        )
        _market_debug_symbols = [
            s.strip()
            for s in str(_market_debug_symbols_raw or '').split(',')
            if s.strip()
        ]

        if self.top_markets_cache:
            cache_age = (datetime.now() - self.last_analysis_time).total_seconds()
            cache_remain = max(0.0, float(self.integrated_cache_ttl) - cache_age)

            # 🔒 WLD 16:06 방어: universe cache는 360s 그대로 믿지 않는다.
            # - 매수 게이트 완화 아님
            # - 급등/골크 후보를 보기 위한 시장선정 fast refresh
            try:
                _fast_universe_ttl = int(
                    self.config.get(
                        'MARKET_SELECTION',
                        'momentum_universe_ttl_sec',
                        fallback='60'
                    )
                )
            except Exception:
                _fast_universe_ttl = 60

            _fast_universe_ttl = max(
                30,
                min(int(self.integrated_cache_ttl), _fast_universe_ttl)
            )

            if cache_age < self.integrated_cache_ttl:
                if cache_age < _fast_universe_ttl:
                    logger.debug(
                        f"📦 [MARKET-CACHE-HIT] "
                        f"age={cache_age:.1f}s "
                        f"ttl={self.integrated_cache_ttl}s "
                        f"fast_ttl={_fast_universe_ttl}s "
                        f"remain={cache_remain:.1f}s "
                        f"cache_count={len(self.top_markets_cache)}"
                    )
                    if _market_debug_symbols:
                        _cache_set = set(self.top_markets_cache)
                        for _sym in _market_debug_symbols:
                            logger.info(
                                f"🔍 [MARKET-TRACE-CACHE] {_sym} | "
                                f"cache_hit=1 age={cache_age:.1f}s "
                                f"ttl={self.integrated_cache_ttl}s "
                                f"fast_ttl={_fast_universe_ttl}s "
                                f"remain={cache_remain:.1f}s "
                                f"in_cache={int(_sym in _cache_set)} "
                                f"cache_count={len(self.top_markets_cache)}"
                            )
                    return self.top_markets_cache

                logger.info(
                    f"⚡ [MARKET-CACHE-FAST-REFRESH] "
                    f"age={cache_age:.1f}s >= fast_ttl={_fast_universe_ttl}s "
                    f"ttl={self.integrated_cache_ttl}s "
                    f"→ 모멘텀/급등 후보 재선정"
                )

        logger.debug(
            f"📦 [MARKET-CACHE-MISS] cache_hit=0 → 통합 시장 분석 재실행"
        )

        markets = await self.perform_integrated_analysis()
        return markets

    def register_gc_birth(self, symbol: str) -> None:
        self._gc_birth_watchlist[symbol] = 3
        cache_key = symbol if symbol else "GLOBAL"

        # 🔒 [REGIME-STATE-PRESERVE]
        # GC birth는 같은 봉 캐시만 무효화한다.
        # stable regime은 hysteresis/cooldown 기준 상태이므로 삭제 금지.
        _invalidated_regime_bucket = getattr(
            self, "_regime_bucket_by_symbol", {}
        ).pop(cache_key, None)
        logger.info(
            f"[GC-BIRTH-WATCHLIST] {symbol} 등록 (3봉) | "
            f"regime_bucket_invalidated="
            f"{int(_invalidated_regime_bucket is not None)} "
            f"old_bucket={_invalidated_regime_bucket}"
        )

    def tick_gc_birth_watchlist(self, symbol: str) -> None:
        if symbol in self._gc_birth_watchlist:
            self._gc_birth_watchlist[symbol] -= 1
            if self._gc_birth_watchlist[symbol] <= 0:
                del self._gc_birth_watchlist[symbol]
                logger.info(f"[GC-BIRTH-WATCHLIST] {symbol} 만료")

    def _calculate_atr_signal(
        self,
        indicators: dict,
        regime: MarketRegime,
        symbol: str = "",
    ) -> Tuple[float, Dict]:
        """ATR signal Producer — Strategy의 기존 계산을 Analysis로 그대로 이관한다."""
        indicators = indicators or {}

        if not indicators.get("_valid", True):
            return 0.0, {
                "reason": "indicators invalid",
                "atr": 0.0,
                "atr_pct": 0.0,
                "volatility_level": "INVALID",
                "signal_raw": 0.0,
            }

        current_price = safe_extract_scalar(
            indicators.get("current_price", 0.0),
            0.0,
        )
        atr_pct_direct = safe_extract_scalar(
            indicators.get("atr_pct", 0.0),
            0.0,
        )

        if atr_pct_direct > 0.0:
            atr = (
                (atr_pct_direct / 100.0) * current_price
                if current_price > 0.0
                else 0.0
            )
            atr_pct = atr_pct_direct
        else:
            atr = safe_extract_scalar(
                indicators.get("atr", 0.0),
                0.0,
            )

            if current_price <= 0.0 or atr <= 0.0:
                return 0.0, {
                    "reason": "ATR 데이터 없음",
                    "atr": 0.0,
                    "atr_pct": 0.0,
                    "volatility_level": "UNKNOWN",
                    "signal_raw": 0.0,
                }

            atr_pct = round(
                (atr / current_price) * PERCENT_CONVERSION,
                4 if symbol == "KRW-BTC" else 2,
            )

        _tf = (
            self.timeframe.value
            if hasattr(self.timeframe, "value")
            else str(self.timeframe)
        )
        _atr_min_default = float(
            self.config.get_cached("atr_min_threshold", 0.5)
        )
        _atr_max_default = float(
            self.config.get_cached("atr_max_threshold", 5.0)
        )

        atr_min_threshold = float(self.config.getfloat(
            "ATR_SETTINGS",
            f"atr_min_threshold_{_tf}",
            fallback=_atr_min_default,
        ))
        atr_max_threshold = float(self.config.getfloat(
            "ATR_SETTINGS",
            f"atr_max_threshold_{_tf}",
            fallback=_atr_max_default,
        ))

        if atr_pct <= 0.0:
            return 0.0, {
                "reason": "atr_pct invalid",
                "atr": float(atr),
                "atr_pct": float(atr_pct),
                "volatility_level": "UNKNOWN",
                "signal_raw": 0.0,
            }

        if atr_pct < atr_min_threshold:
            signal_strength = 1.0
        elif atr_pct >= atr_max_threshold:
            signal_strength = 0.0
        else:
            signal_strength = 1.0 - (
                (atr_pct - atr_min_threshold)
                / (atr_max_threshold - atr_min_threshold)
            )

        signal_strength = float(
            np.clip(signal_strength, 0.0, 1.0)
        )
        signal_raw = signal_strength

        adjustment = 1.0
        adjustment_reason = ""

        if regime == MarketRegime.RANGING:
            if atr_pct < atr_min_threshold:
                adjustment = 1.15
                adjustment_reason = " (횡보 완화)"

        elif regime == MarketRegime.BREAKDOWN:
            adjustment = 0.70
            adjustment_reason = " (붕괴 억제)"

        elif regime == MarketRegime.TRENDING_UP:
            if atr_pct < atr_min_threshold:
                adjustment = 0.85
                adjustment_reason = " (상승 저변동 억제)"

        signal_strength *= adjustment
        signal_strength = float(
            np.clip(signal_strength, 0.0, 1.0)
        )

        if atr_pct < atr_min_threshold:
            volatility_level = "VERY_LOW"
        elif atr_pct >= atr_max_threshold:
            volatility_level = "EXTREME"
        else:
            volatility_level = "NORMAL"

        if signal_strength >= 0.8:
            reason = (
                f"✅ 최적 변동성 ({atr_pct:.2f}%)"
                f"{adjustment_reason}"
            )
        elif signal_strength >= 0.5:
            reason = (
                f"🟢 적정 변동성 ({atr_pct:.2f}%)"
                f"{adjustment_reason}"
            )
        elif signal_strength >= 0.2:
            reason = (
                f"🟡 낮은 신호 ({atr_pct:.2f}%)"
                f"{adjustment_reason}"
            )
        else:
            reason = (
                f"🔴 부적합 변동성 ({atr_pct:.2f}%)"
                f"{adjustment_reason}"
            )

        return signal_strength, {
            "reason": reason,
            "atr": float(atr),
            "atr_pct": float(atr_pct),
            "volatility_level": volatility_level,
            "signal_raw": float(signal_raw),
            "adjustment": float(adjustment),
            "threshold_min": float(atr_min_threshold),
            "threshold_max": float(atr_max_threshold),
            "timeframe": _tf,
        }

    def _save_and_return_regime(
        self,
        regime: MarketRegime,
        indicators: dict,
        df: pd.DataFrame,
        symbol: str,
    ) -> MarketRegime:
        """regime을 indicators에 저장하고 return (모든 경로 공통)"""
        if indicators is not None:
            indicators["mr_regime"] = regime
            indicators["mr_candle_time"] = (
                str(df.index[-1])[:16]
                if df is not None and len(df) > 0
                else ""
            )

            # 🔒 [2026-08-08 SSoT-P1]
            # 최종 regime까지 포함한 ATR signal을 Analysis에서 완성한다.
            _atr_signal, _atr_signal_meta = self._calculate_atr_signal(
                indicators,
                regime,
                symbol,
            )
            indicators["atr_signal"] = float(_atr_signal)
            indicators["atr_signal_meta"] = _atr_signal_meta

            logger.debug(
                f"✅ [MR-REGIME-SAVE] {symbol} | "
                f"mr_regime={regime.value} | "
                f"mr_candle={indicators.get('mr_candle_time', 'N/A')}"
            )

        self.market_regime = regime
        if hasattr(self, '_stable_regime_by_symbol') and symbol:
            cache_key = symbol if symbol else "GLOBAL"
            _prev_cached_regime = self._stable_regime_by_symbol.get(cache_key)
            if (
                _prev_cached_regime is not None
                and _prev_cached_regime != regime
            ):
                logger.info(
                    f"🗄️ [REGIME-CACHE-WRITE] {symbol} "
                    f"{_prev_cached_regime.value}→{regime.value} | "
                    f"candle={str(df.index[-1])[:16] if df is not None and len(df) > 0 else 'NA'}"
                )
            self._stable_regime_by_symbol[cache_key] = regime

        return regime
    
    async def analyze_market_regime(self, df: pd.DataFrame = None, indicators: dict = None, symbol: str = None) -> MarketRegime:
        """
        시장 체제 분석 - Score 기반 완전 재구축
        
        철학:
        - 단일 지표(ADX<20)로 체제를 강제하지 않는다
        - MA/ADX/Volume/Momentum/Position 신호를 모두 점수화
        - 각 체제의 합계 점수를 비교하여 최종 결정
        - 이전 체제에 관성 부여로 플리핑 방지
        """
        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        # 0. SSoT 강제: symbol 필수 검증
        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        # 🔒 SSoT 최종 방어: 보조 스캔/관측 경로에서의 무의미한 호출 차단
        # - indicators=None 또는 symbol=None은 체제 판단 불가
        # - 거래/진입 로직과 무관 → 조용히 스킵
        # 🔒 MA120 잡음 임계 (RANGING-Guard·DeadZone 공유 SSoT 상수)
        # 🔒 [REGIME-FALLBACK-0816] self.market_regime(다른 종목 값) 대신
        #  이 종목 자신의 마지막 확정 체제만 폴백으로 사용.
        _symbol_regime_fallback = getattr(
            self, "_stable_regime_by_symbol", {}
        ).get(symbol, MarketRegime.RANGING)
        _MR_SLOPE_NOISE_THR = 0.03  # % 단위
        if not indicators or not symbol or symbol == "UNKNOWN":
            logger.debug(
                f"⏭️ [SSoT-SKIP] analyze_market_regime() skipped | "
                f"symbol={symbol} indicators={bool(indicators)}"
            )
            
            return self._save_and_return_regime(_symbol_regime_fallback, indicators, df, symbol)
        
        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        # 1. 입력 유효성 (캐시·쿨다운보다 먼저)
        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        if df is None or not is_valid_df(df):
            return self._save_and_return_regime(_symbol_regime_fallback, indicators, df, symbol)

        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        # 🔒 SYSTEM CONSTITUTION [헌법 1조]
        # Regime 판단은 "3분 확정봉" ONLY
        # - intrabar(미완성 봉 포함) 입력이 와도 여기서 확정봉으로 강제
        # - MA/슬로프/가자미근은 다른 함수에서 intrabar 유지 (헌법 2조)
        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        try:
            tf_key = self.timeframe.value if hasattr(self.timeframe, "value") else str(self.timeframe)
            df_regime = TechnicalIndicators.drop_incomplete_candle(df, tf_key)
            if df_regime is not None and is_valid_df(df_regime) and len(df_regime) >= 2:
                df = df_regime
        except Exception:
            pass

        if len(df) < 60:
            return self._save_and_return_regime(MarketRegime.RANGING, indicators, df, symbol)

        # ============================================================
        # 🧠 2. 캔들 캐시 (종목별, 최우선)
        # - 같은 봉이면 체제 재계산 금지
        # - 글로벌 쿨다운보다 앞에서 처리
        # ============================================================
        cache_key = symbol if symbol else "GLOBAL"

        try:
            regime_bucket = str(df.index[-1])
        except Exception:
            import time as _time
            _tf_map = {'1m': 60, '3m': 180, '5m': 300, '10m': 600,
                       '15m': 900, '30m': 1800, '1h': 3600, '4h': 14400, '1d': 86400}
            tf_key = self.timeframe.value if hasattr(self.timeframe, "value") else str(self.timeframe)
            tf_sec = _tf_map.get(tf_key, 180)
            regime_bucket = int(_time.time() // tf_sec)

        if not hasattr(self, "_regime_bucket_by_symbol"):
            self._regime_bucket_by_symbol = {}

        if not hasattr(self, "_stable_regime_by_symbol"):
            self._stable_regime_by_symbol = {}

        if not hasattr(self, "_regime_last_change_by_symbol"):
            self._regime_last_change_by_symbol = {}

        # 🔒 [REGIME-FALLBACK-0816] 신규 편입 종목이 글로벌 체제를 상속하지 않도록,
        #  이력 없으면 RANGING 중립에서 시작.
        _prev_stable_regime_for_symbol = self._stable_regime_by_symbol.get(
            cache_key,
            MarketRegime.RANGING
        )
        _regime_last_change_for_symbol = self._regime_last_change_by_symbol.get(
            cache_key
        )

        prev_bucket = self._regime_bucket_by_symbol.get(cache_key)

        if (
            cache_key in self._stable_regime_by_symbol
            and prev_bucket == regime_bucket
        ):
            cached_regime = self._stable_regime_by_symbol[cache_key]
            return self._save_and_return_regime(cached_regime, indicators, df, symbol)

        self._regime_bucket_by_symbol[cache_key] = regime_bucket

        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        # 3. 심볼별 쿨다운 (캐시 미스 시에만)
        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        if not self._is_regime_change_allowed(_regime_last_change_for_symbol, per_symbol=True):
            if cache_key in self._stable_regime_by_symbol:
                cached_regime = self._stable_regime_by_symbol[cache_key]
                return self._save_and_return_regime(cached_regime, indicators, df, symbol)
            # 신규 봉 + 캐시 없음 → 계산 허용

        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        # 2. 기본 지표 계산
        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        LAST_INDEX = -1
        # 🔒 [REGIME-CLOSED-BASIS-SSOT-0809]
        # 위 drop_incomplete_candle()에서 미완성봉 제거 완료.
        # 따라서 -1이 최신 확정봉이며 Regime의 현재 기준이다.
        REGIME_INDEX = LAST_INDEX

        try:
            current_price = float(df['close'].iloc[LAST_INDEX])
            price_prev = float(df['close'].iloc[-2]) if len(df) >= 2 else current_price
        except Exception:
            return self._save_and_return_regime(_symbol_regime_fallback, indicators, df, symbol)

         # MA 계산 (regime용: 확정봉 기준 REGIME_INDEX / L85 헌법 준수)
        ma5   = float(df['close'].rolling(5).mean().iloc[REGIME_INDEX])   if len(df) >= 6   else 0.0
        ma10  = float(df['close'].rolling(10).mean().iloc[REGIME_INDEX])  if len(df) >= 11  else 0.0
        ma20  = float(df['close'].rolling(20).mean().iloc[REGIME_INDEX])  if len(df) >= 21  else 0.0
        ma60  = float(df['close'].rolling(60).mean().iloc[REGIME_INDEX])  if len(df) >= 61  else 0.0
        ma120 = float(df['close'].rolling(120).mean().iloc[REGIME_INDEX]) if len(df) >= 121 else 0.0
        
        # 🔒 TYPE-SAFE: MA 스칼라 확정 (Series ambiguous 방지)
        # - MA 정의 직후 즉시 스칼라 확정 (이후 모든 비교 안전화)
        ma5 = safe_extract_scalar(ma5, 0.0)
        ma10 = safe_extract_scalar(ma10, 0.0)
        ma20 = safe_extract_scalar(ma20, 0.0)
        ma60 = safe_extract_scalar(ma60, 0.0)
        ma120 = safe_extract_scalar(ma120, 0.0)

        # ADX 계산
        adx_series = TechnicalIndicators.calculate_adx(df)
        current_adx = float(adx_series.iloc[-1]) if not adx_series.empty else 20.0

        # ATR 계산
        # TF별 ATR 기준 (config 우선, 없으면 0.1)
        _atr_min = self.config.getfloat('ATR_SETTINGS', 'atr_min_pct', fallback=0.1)
        atr_series = TechnicalIndicators.calculate_atr(df, atr_min_pct=_atr_min)
        atr_val = float(atr_series.iloc[-1]) if not atr_series.empty else 0.0
        atr_pct = (atr_val / current_price * 100.0) if current_price > 0 else 0.0

        # 모멘텀 계산
        if len(df) >= 6:
            momentum_5 = (current_price - float(df['close'].iloc[-6])) / float(df['close'].iloc[-6]) * 100.0
        else:
            momentum_5 = 0.0

        if len(df) >= 21:
            momentum_20 = (current_price - float(df['close'].iloc[-21])) / float(df['close'].iloc[-21]) * 100.0
        else:
            momentum_20 = 0.0

        # 박스 위치 계산 (0~1)
        if len(df) >= 20:
            recent_20 = df.tail(20)
            hi20 = float(recent_20['high'].max())
            lo20 = float(recent_20['low'].min())
            range_width_pct = ((hi20 - lo20) / lo20 * 100.0) if lo20 > 0 else 0.0
            pos_in_range = (current_price - lo20) / (hi20 - lo20) if hi20 > lo20 else 0.5
        else:
            range_width_pct = 0.0
            pos_in_range = 0.5

        # MA 이격도
        div_ma20 = ((current_price - ma20) / ma20 * 100.0) if ma20 > 0 else 0.0

        # 거래량 비율
        if len(df) >= 20:
            vol_ma = float(df['volume'].rolling(20).mean().iloc[LAST_INDEX])
            vol_now = float(df['volume'].iloc[LAST_INDEX])
            vol_ratio = (vol_now / vol_ma) if vol_ma > 0 else 1.0
        else:
            vol_ratio = 1.0

        # MA 구조 플래그
        is_full_bullish = (current_price > ma5 > ma10 > ma20 > ma60) if ma60 > 0 else False
        # 부분 정배열 감지 (강화: MA5 > MA10 명확히 확인, 0.2% 이상 차이 필요)
        is_partial_bullish = (
            (ma5 > ma10 > ma20 > 0) and
            ma10 > 0 and (ma5 - ma10) / ma10 > 0.002  # 최소 0.2% 차이
        )
        is_full_bearish = (current_price < ma5 < ma10 < ma20 < ma60) if ma60 > 0 else False
        is_partial_bearish = (ma5 < ma10 < ma20 > 0)
        
        # 🆕 MA5 < MA10 일때 즉시 TD 판정 (데드캣바운스 차단)
        is_ma5_below_ma10 = (ma5 > 0 and ma10 > 0 and ma5 < ma10)

        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        # 🆕 MA 기울기 계산 (추세 방향 참고)
        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        # 🔒 시장 체제 판단용 MA 기울기 (mr_ prefix)
        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        mr_ma20_slope = 0.0
        if len(df) >= 25 and ma20 > 0:
            ma20_prev = float(df['close'].rolling(20).mean().iloc[REGIME_INDEX - 3]) if len(df) >= 5 else ma20
            if ma20_prev > 0:
                mr_ma20_slope = (ma20 - ma20_prev) / ma20_prev * 100.0
               
        # MA5 기울기(%) - 단기 추세 방향
        mr_slope_ma5 = 0.0
        if len(df) >= 7 and ma5 > 0:
            ma5_prev = float(df['close'].rolling(5).mean().iloc[REGIME_INDEX - 3]) if len(df) >= 5 else ma5
            if ma5_prev > 0:
                mr_slope_ma5 = (ma5 - ma5_prev) / ma5_prev * 100.0
        
        # MA10 기울기(%) - 중기 추세 방향
        mr_slope_ma10 = 0.0
        if len(df) >= 15 and ma10 > 0:
            ma10_prev = float(df['close'].rolling(10).mean().iloc[REGIME_INDEX - 3]) if len(df) >= 5 else ma10
            if ma10_prev > 0:
                mr_slope_ma10 = (ma10 - ma10_prev) / ma10_prev * 100.0
        
        # 🆕 MA120 기울기(%) - 장기 추세 방향
        mr_slope_ma120 = SENTINEL_FLOAT
        if len(df) >= 125 and ma120 > 0:
            ma120_prev = float(df['close'].rolling(120).mean().iloc[REGIME_INDEX - 3]) if len(df) >= 5 else ma120
            if ma120_prev > 0:
                mr_slope_ma120 = (ma120 - ma120_prev) / ma120_prev * 100.0
        _mr_ma120_available = (mr_slope_ma120 != SENTINEL_FLOAT)
        
        # 🆕 MA60 기울기(%) - 체제 판단용 (정석)
        mr_slope_ma60 = 0.0
        if len(df) >= 63 and ma60 > 0:
            ma60_prev = float(df['close'].rolling(60).mean().iloc[REGIME_INDEX - 3]) if len(df) >= 5 else ma60
            if ma60_prev > 0:
                mr_slope_ma60 = (ma60 - ma60_prev) / ma60_prev * 100.0

        # ✅ 구조 중립(횡보 가능) 판정: slope가 충분히 작을 때만 ADX를 RANGING 핵심 근거로 사용
        ma_slope_thr_pct = float(MA_SLOPE_TREND_THRESHOLD) * 100.0
        structure_neutral = (abs(mr_slope_ma5) <= ma_slope_thr_pct and abs(mr_ma20_slope) <= ma_slope_thr_pct)

        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        # 🎯 정석 체제 판단 (MA10, MA20 기반)
        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        # 철학:
        # - 3분봉 체제축: MA10(≈30분) + MA20(≈60분)
        # - MA60은 컨텍스트(ctx) 참고용, 체제 판정 불참여
        # - 체제 판단과 진입 패턴(가자미근)을 완전 분리
        #
        # 규칙:
        # - MA10 & MA20 둘 다 상승 → TRENDING_UP
        # - MA10 & MA20 둘 다 하락 → TRENDING_DOWN
        # - 그 외 → RANGING
        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        
        # 체제 판단 임계값 (ratio 단위)
        # 🔧 [2026-08-06] 체제 MA와 같은 확정봉 ATR로 동적화하고 상승/하락을 비대칭화한다.
        _reg_atr_val = (
            float(atr_series.iloc[REGIME_INDEX])
            if not atr_series.empty and len(atr_series) >= 2
            else 0.0
        )
        _reg_atr_px = float(df['close'].iloc[REGIME_INDEX])
        _reg_atr_pct = (
            (_reg_atr_val / _reg_atr_px * 100.0)
            if _reg_atr_val > 0.0 and _reg_atr_px > 0.0
            else float(atr_pct or 0.0)
        )
        _REG_ATR_R = _reg_atr_pct / 100.0
        REGIME_SLOPE_UP_FAST = max(0.00020, _REG_ATR_R * 0.30)
        REGIME_SLOPE_UP_SLOW = max(0.00012, _REG_ATR_R * 0.18)
        REGIME_SLOPE_DN_FAST = -max(0.00020, _REG_ATR_R * 0.50)
        REGIME_SLOPE_DN_SLOW = -max(0.00012, _REG_ATR_R * 0.30)

        # ✅ 3분봉 체제축: MA10(30분) + MA20(60분)
        ma10_slope_ratio = mr_slope_ma10 / 100.0
        ma20_slope_ratio = mr_ma20_slope / 100.0

        ma10_direction = "UP" if ma10_slope_ratio > REGIME_SLOPE_UP_FAST else \
                         "DOWN" if ma10_slope_ratio < REGIME_SLOPE_DN_FAST else "FLAT"
        ma20_direction = "UP" if ma20_slope_ratio > REGIME_SLOPE_UP_SLOW else \
                         "DOWN" if ma20_slope_ratio < REGIME_SLOPE_DN_SLOW else "FLAT"

        # (참고) MA60은 컨텍스트만 유지
        ma60_slope_ratio = mr_slope_ma60 / 100.0
        ma60_direction = "UP" if ma60_slope_ratio > REGIME_SLOPE_UP_SLOW else \
                         "DOWN" if ma60_slope_ratio < REGIME_SLOPE_DN_SLOW else "FLAT"

        if ma10_direction == "UP" and ma20_direction == "UP":
            # 🆕 [Patch-A] MA20-MA60 수렴 체크: 이격 부족 시 RANGING 유지
            # 근거: 기울기가 UP이어도 MA20≈MA60 수렴 상태는 실질 추세 미확립
            # 임계값: 0.2% — 급등 직후 MA60 반응 지연 허용
            _tu_gap = ((ma20 - ma60) / ma60) if ma60 > 0 else 0.0
            _TU_MA60_GAP_MIN = 0.002  # 0.2% (급등 직후 MA60 반응 지연 허용)
            # 🔒 추세 꺾임 체크: GC 직전 역배열 지속 → RANGING
            _dc_recent = 0
            if len(df) >= 6:
                _ma5_ser = df['close'].rolling(5).mean()
                _ma10_ser = df['close'].rolling(10).mean()
                for _ki in range(2, 5):
                    if float(_ma5_ser.iloc[-_ki]) <= float(_ma10_ser.iloc[-_ki]):
                        _dc_recent += 1

            if _tu_gap >= _TU_MA60_GAP_MIN and _dc_recent < 3 and (ma5 >= ma20):
                primary_regime = MarketRegime.TRENDING_UP
            else:
                primary_regime = MarketRegime.RANGING
                indicators["tu_suppressed"] = True
                logger.info(
                    f"⚠️ [TU억제] {symbol} MA20-MA60 이격부족 or 역배열이력 or MA5<MA20 → RANGING | "
                    f"gap={_tu_gap*100:.2f}% dc={_dc_recent}봉 ma5_vs_ma20={'OK' if ma5>=ma20 else 'NG'} "
                    f"(MA5={ma5:.2f} MA20={ma20:.2f} MA60={ma60:.2f})"
                )
        elif ma10_direction == "DOWN" and ma20_direction == "DOWN":
            # 🛡️ REGIME-BULL-GUARD: 정배열 + 미세 음수 → TD 전환 보류
            _bull_align = (ma5 > 0 and ma10 > 0 and ma20 > 0 and ma5 > ma10 > ma20)
            _td_hard_thr = -0.05  # -0.05% 이상 확실한 하락만 TD 인정
            if (
                _bull_align
                and mr_slope_ma10 > _td_hard_thr
                and mr_ma20_slope > _td_hard_thr
            ):
                primary_regime = MarketRegime.RANGING
                logger.info(
                    f"🛡️ [REGIME-BULL-GUARD] {symbol} | "
                    f"정배열 + 미세음수 → TD 보류 → RANGING | "
                    f"MA10:{mr_slope_ma10:.3f}% MA20:{mr_ma20_slope:.3f}%"
                )
            else:
                primary_regime = MarketRegime.TRENDING_DOWN
        elif ma10_direction == "FLAT" and ma20_direction == "UP":
            # MA10이 상승 임계 아래로 꺾이면 현재 성질은 RANGING이다.
            # 기존 TU 상속은 후단 상태기계가 담당하며 primary에서 재승격하지 않는다.
            primary_regime = MarketRegime.RANGING
        else:
            primary_regime = MarketRegime.RANGING

        # ━━ REGIME-SUPPRESS CORRECTION (확장·판정변경·계측동반) ━━
        # ① RG_UNDER_TU : RANGING인데 MA5>MA10 + MA10/MA20 실제 양수 방향
        #                 + MA20>MA60 + MA60 STRUCT20 상승이면 TU 보정
        # ② TD_OVER_VREC: TD인데 fresh GC V회복+MA60 구조 안죽음
        _sh_s60_struct = 0.0
        try:
            # fresh GC 감지 (최근 2~4봉 내 DC→GC 전환)
            _sh_fresh_gc = 0
            if df is not None and len(df) >= 8:
                _m5s = df['close'].rolling(5).mean()
                _m10s = df['close'].rolling(10).mean()
                if float(_m5s.iloc[-1]) > float(_m10s.iloc[-1]):
                    for _k in range(2, 5):
                        if float(_m5s.iloc[-_k]) <= float(_m10s.iloc[-_k]):
                            _sh_fresh_gc = 1
                            break
            # STRUCT20 s60 (Regime 최신 확정봉 기준 — 20봉 구조기울기)
            try:
                if df is not None and len(df) >= 81:
                    _m60 = df['close'].rolling(60).mean()
                    _p60 = float(_m60.iloc[REGIME_INDEX - 20])
                    _n60 = float(_m60.iloc[REGIME_INDEX])
                    if _p60 > 0:
                        _sh_s60_struct = (_n60 - _p60) / _p60 * 100.0
            except Exception:
                _sh_s60_struct = 0.0
            # 🔒 [REGIME-RG-UNDER-TU-DEADLOCK-FIX-0829-R2] ★412(1/2)
            # RANGING을 만든 동적 UP 문턱을 구제 조건에서 다시 요구하지 않는다.
            # ma10>ma20 지연도 제거하되, 실제 MA10·MA20 양수 방향과
            # ma20>ma60 장기 구조 안전장은 유지한다.
            _sh_rg_under_tu = bool(
                primary_regime == MarketRegime.RANGING
                and ma5 > ma10
                and ma20 > ma60 and ma60 > 0
                and mr_slope_ma10 > 0.0
                and mr_ma20_slope > 0.0
            )
            _sh_td_over_vrec = bool(
                primary_regime == MarketRegime.TRENDING_DOWN
                and ma5 > ma10 > 0
                and _sh_fresh_gc == 1
                and _sh_s60_struct >= -0.20
            )
            if _sh_rg_under_tu or _sh_td_over_vrec:
                _sh_type = "RG_UNDER_TU" if _sh_rg_under_tu else "TD_OVER_VREC"
                _sh_tu_gap = ((ma20 - ma60) / ma60 * 100.0) if ma60 > 0 else 0.0
                _sh_px = float(df['close'].iloc[-1]) if (df is not None and len(df) > 0) else 0.0
                _sh_candle = str(df.index[-1]) if (df is not None and len(df) > 0) else "?"
                _sh_tu_suppressed = int(bool(indicators.get("tu_suppressed", False)))
                _sh_ma5_vs_ma20 = int(ma5 >= ma20)
                logger.info(
                    f"🟡 [REGIME-SUPPRESS-CORRECTION] {symbol} | type={_sh_type} regime={primary_regime.value} | "
                    f"px={_sh_px:.4f} candle={_sh_candle} | "
                    f"s10={mr_slope_ma10:+.4f}% s20={mr_ma20_slope:+.4f}% "
                    f"s60_regime={mr_slope_ma60:+.4f}% s60_struct={_sh_s60_struct:+.4f}% | "
                    f"ma60_up={int(_sh_s60_struct > 0.0)} fresh_gc={_sh_fresh_gc} "
                    f"fullstack={int(ma5 > ma10 > ma20 > ma60 > 0)} ma5_vs_ma20={_sh_ma5_vs_ma20} "
                    f"tu_gap={_sh_tu_gap:+.3f}% tu_suppressed={_sh_tu_suppressed} "
                    f"dc_recent={locals().get('_dc_recent', -1)} | "
                    f"dir10={ma10_direction} dir20={ma20_direction} dir60={ma60_direction}"
                )
                if _sh_rg_under_tu and _sh_s60_struct > 0.0:
                    primary_regime = MarketRegime.TRENDING_UP
                    indicators["tu_suppressed"] = False
                    indicators["regime_corrected"] = "RG_UNDER_TU->TU"
                    logger.info(
                        f"🔧 [REGIME-CORRECT] {symbol} RG_UNDER_TU→TU 보정 | "
                        f"mid_up s10={mr_slope_ma10:+.3f}% s20={mr_ma20_slope:+.3f}% "
                        f"fullstack={int(ma5 > ma10 > ma20 > ma60 > 0)} "
                        f"ma20={ma20:.2f}>ma60={ma60:.2f} s60_struct={_sh_s60_struct:+.3f}% "
                        f"tu_suppressed=0"
                    )
        except Exception:
            pass

        # 🔒 TU decay는 단순 음수(<0)가 아니라 현재 체제 UP 임계 이탈을 소비한다.
        _primary_tu_decay = bool(
            primary_regime == MarketRegime.RANGING
            and ma10_direction != "UP"
        )

        logger.info(
            f"🎯 [정석체제] {symbol} | {primary_regime.value} | "
            f"bucket={regime_bucket} | "
            f"MA10:{ma10_direction}({mr_slope_ma10:+.3f}% "
            f"up>{REGIME_SLOPE_UP_FAST*100:.3f} dn<{REGIME_SLOPE_DN_FAST*100:.3f}) "
            f"MA20:{ma20_direction}({mr_ma20_slope:+.3f}% "
            f"up>{REGIME_SLOPE_UP_SLOW*100:.3f} dn<{REGIME_SLOPE_DN_SLOW*100:.3f}) "
            f"MA60(ctx):{ma60_direction}({mr_slope_ma60:+.3f}%) "
            f"atr={_reg_atr_pct:.3f}% tu_decay={int(_primary_tu_decay)}"
        )
        
        # 📋 보조 정보 로깅 (체제 변경 없음, 참고용)
        logger.debug(
            f"📋 [보조정보] {symbol} | "
            f"MA5:{ma5:.1f}(slope:{mr_slope_ma5:.3f}%) "
            f"MA10:{ma10:.1f}(slope:{mr_slope_ma10:.3f}%) | "
            f"full_bull={is_full_bullish} full_bear={is_full_bearish} | "
            f"partial_bull={is_partial_bullish} partial_bear={is_partial_bearish} | "
            f"ma5_below_ma10={is_ma5_below_ma10}"
        )

        # 연속 상승/하락 체크
        consecutive_rises = 0
        consecutive_drops = 0
        for i in range(1, min(6, len(df))):
            if df['close'].iloc[-i] > df['close'].iloc[-i-1]:
                consecutive_rises += 1
            elif df['close'].iloc[-i] < df['close'].iloc[-i-1]:
                consecutive_drops += 1

        # 2. Regime Score 초기화
        # ⚠️ DIAGNOSTIC PATH (진단 전용)
        # - Score 경쟁 결과(final_regime)는 raw_regime으로만 저장
        # - 실제 체제는 primary_regime(MA10/MA20)이 직결 결정
        # - 향후 Score 재활성화 대비 로직 보존
        scores = {
            MarketRegime.TRENDING_UP: 0.0,
            MarketRegime.TRENDING_DOWN: 0.0,
            MarketRegime.RANGING: 0.0,
            MarketRegime.BREAKOUT: 0.0,
            MarketRegime.BREAKDOWN: 0.0,
        }

        # 🆕 TD 전환 감지 부스트: 고점 후 조정(peak→drop)
        # 목적: POL 같은 "고점 형성 → 조정" 구간을 TU로 끌고 가지 않도록 TD 점수 강화
        try:
            lookback = 30 if len(df) >= 30 else min(20, len(df))
            recent_high = float(df["high"].tail(lookback).max())
            if recent_high > 0:
                peak_drop_pct = (recent_high - current_price) / recent_high * 100.0

                # 조건: 최근 고점 대비 5% 이상 하락 + 최근 하락 연속봉(2+) → TD 점수 가산
                if peak_drop_pct >= 5.0 and consecutive_drops >= 2:
                    scores[MarketRegime.TRENDING_DOWN] += 1.5
                    scores[MarketRegime.TRENDING_UP] -= 0.5
                    logger.debug(
                        f"🔻 [TD-PeakDrop] {symbol} 고점후조정 감지 | "
                        f"lookback={lookback} | high={recent_high:.2f} | "
                        f"cur={current_price:.2f} | drop={peak_drop_pct:.2f}% | "
                        f"drops={consecutive_drops} → TD+1.5, TU-0.5"
                    )
        except Exception as e:
            logger.debug(f"TD-PeakDrop 계산 실패: {e}")

        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        # 3. TRENDING_UP 점수 계산
        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        
        # MA 구조 (핵심 지표)
        if is_full_bullish:
            scores[MarketRegime.TRENDING_UP] += 3.0  # 완전 정배열
        elif is_partial_bullish:
            scores[MarketRegime.TRENDING_UP] += 2.0  # 부분 정배열
            if current_adx >= 20:
                scores[MarketRegime.TRENDING_UP] += 0.5  # 🆕 상승초입 보너스
        
        # 중기 추세
        if ma20 > 0 and ma20 > ma60:
            scores[MarketRegime.TRENDING_UP] += 0.5
        
        # ADX (추세 강도)
        if current_adx >= 30:
            scores[MarketRegime.TRENDING_UP] += 2.0  # 강한 추세
        elif current_adx >= 25:
            scores[MarketRegime.TRENDING_UP] += 1.5
        elif current_adx >= 20:
            scores[MarketRegime.TRENDING_UP] += 1.0
        else:
            # ADX < 20: 추세 강도 부족 → TRENDING_UP에 약한 패널티
            scores[MarketRegime.TRENDING_UP] -= 0.5
        
        # 모멘텀
        if momentum_5 > 2.0:
            scores[MarketRegime.TRENDING_UP] += 1.5
        elif momentum_5 > 1.0:
            scores[MarketRegime.TRENDING_UP] += 1.0
        elif momentum_5 > 0.0:
            scores[MarketRegime.TRENDING_UP] += 0.5
        
        if momentum_20 > 3.0:
            scores[MarketRegime.TRENDING_UP] += 1.0
        elif momentum_20 > 0.5:
            scores[MarketRegime.TRENDING_UP] += 0.5
        
        # 거래량
        if vol_ratio >= 1.5:
            scores[MarketRegime.TRENDING_UP] += 1.0
        elif vol_ratio >= 1.2:
            scores[MarketRegime.TRENDING_UP] += 0.5
        
        # MA 이격도 (과매수 필터)
        if 0.0 <= div_ma20 <= 5.0:
            scores[MarketRegime.TRENDING_UP] += 1.0  # 적정 이격
        elif div_ma20 > 10.0:
            scores[MarketRegime.TRENDING_UP] -= 1.0  # 과매수
        
        # 박스 위치
        if 0.3 <= pos_in_range <= 0.7:
            scores[MarketRegime.TRENDING_UP] += 0.5  # 적정 위치
        
        # 연속 상승
        if consecutive_rises >= 3:
            scores[MarketRegime.TRENDING_UP] += 0.5

        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        # 4. TRENDING_DOWN 점수 계산
        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        
        # MA 구조
        if is_full_bearish:
            scores[MarketRegime.TRENDING_DOWN] += 3.0
        elif is_partial_bearish:
            scores[MarketRegime.TRENDING_DOWN] += 2.0
        
        # ADX
        if current_adx >= 30:
            scores[MarketRegime.TRENDING_DOWN] += 2.0
        elif current_adx >= 25:
            scores[MarketRegime.TRENDING_DOWN] += 1.5
        elif current_adx >= 20:
            scores[MarketRegime.TRENDING_DOWN] += 1.0
        
        # 모멘텀
        if momentum_5 < -2.0:
            scores[MarketRegime.TRENDING_DOWN] += 1.5
        elif momentum_5 < -1.0:
            scores[MarketRegime.TRENDING_DOWN] += 1.0
        elif momentum_5 < 0.0:
            scores[MarketRegime.TRENDING_DOWN] += 0.5
        
        if momentum_20 < -3.0:
            scores[MarketRegime.TRENDING_DOWN] += 1.0
        elif momentum_20 < -0.5:
            scores[MarketRegime.TRENDING_DOWN] += 0.5
        
        # 거래량
        if vol_ratio >= 1.5:
            scores[MarketRegime.TRENDING_DOWN] += 1.0
        elif vol_ratio >= 1.2:
            scores[MarketRegime.TRENDING_DOWN] += 0.5
        
        # MA 이격도
        if div_ma20 < -5.0:
            scores[MarketRegime.TRENDING_DOWN] += 1.0
        elif div_ma20 < -2.0:
            scores[MarketRegime.TRENDING_DOWN] += 0.5
        
        # 연속 하락
        if consecutive_drops >= 4:
            scores[MarketRegime.TRENDING_DOWN] += 1.0
        elif consecutive_drops >= 3:
            scores[MarketRegime.TRENDING_DOWN] += 0.5
        
        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        # 🆕 하락장 조기 감지 부스트 (2025-12-10)
        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        # 목적: TRENDING_UP → TRENDING_DOWN 전환 1~2봉 앞당김
        # 조건: 역배열 + 단기모멘텀 하락 + 추세강도 + 거래량
        # 효과: 0G 같은 고점 진입 방지 (체제 전환 민감도 ↑)
        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        strong_td_signal = (
            (is_full_bearish or is_partial_bearish) and
            momentum_5 <= -0.8 and
            current_adx >= 20 and
            vol_ratio >= 1.2
        )
        if strong_td_signal:
            scores[MarketRegime.TRENDING_DOWN] += 1.0
            logger.debug(
                f"🔻 [TD-EarlyDetect] {symbol} 강한 하락 신호 감지 | "
                f"역배열:{is_full_bearish or is_partial_bearish} | "
                f"mom5:{momentum_5:.2f} | ADX:{current_adx:.1f} | "
                f"vol:{vol_ratio:.2f}x → TD점수 +1.0"
            )

        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        # 5. RANGING 점수 계산
        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        
        # ADX (핵심 지표) - ✅ 구조 중립일 때만 RANGING 근거로 사용
        # 목적: 하락 진행(구조) + ADX 약화(강도) 조합이 RANGING 오판으로 이어지는 것 차단
        if structure_neutral:
            if current_adx < 15:
                scores[MarketRegime.RANGING] += 2.0
            elif current_adx < 18:
                scores[MarketRegime.RANGING] += 1.0
            elif current_adx < 20:
                scores[MarketRegime.RANGING] += 0.5
        
        # MA 이격도
        if abs(div_ma20) <= 1.0:
            scores[MarketRegime.RANGING] += 1.0  # MA20 근처
        
        # 박스 폭
        if range_width_pct <= 2.0:
            if is_partial_bullish:
                scores[MarketRegime.RANGING] += 0.5  # 🆕 상승초입 시 감소
            else:
                scores[MarketRegime.RANGING] += 1.5
        elif range_width_pct <= 3.0:
            if is_partial_bullish:
                scores[MarketRegime.RANGING] += 0.3  # 🆕 상승초입 시 감소
            else:
                scores[MarketRegime.RANGING] += 1.0
        elif range_width_pct <= 5.0:
            scores[MarketRegime.RANGING] += 0.5
        
        # 모멘텀 부재
        if abs(momentum_5) < 0.5 and abs(momentum_20) < 1.0:
            scores[MarketRegime.RANGING] += 1.5
        elif abs(momentum_5) < 1.0 and abs(momentum_20) < 2.0:
            scores[MarketRegime.RANGING] += 1.0
        
        # MA 구조 불명확
        if not is_full_bullish and not is_partial_bullish and not is_full_bearish and not is_partial_bearish:
            scores[MarketRegime.RANGING] += 1.0
        
        # 거래량 감소
        if vol_ratio < 0.8:
            scores[MarketRegime.RANGING] += 0.5

        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        # 6. BREAKOUT 점수 계산
        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        if len(df) >= 20:
            recent_sr = df.tail(20)
            recent_hi = float(recent_sr['high'].max())
            
            if recent_hi > 0:
                dist_hi_pct = (current_price - recent_hi) / recent_hi * 100.0
                
                # 상단 돌파
                if dist_hi_pct > 1.5:
                    scores[MarketRegime.BREAKOUT] += 2.0
                elif dist_hi_pct > 1.0:
                    scores[MarketRegime.BREAKOUT] += 1.5
                elif dist_hi_pct > 0.5:
                    scores[MarketRegime.BREAKOUT] += 1.0
                
                # 추가 확인
                if dist_hi_pct > 0.5:
                    # ADX
                    if current_adx >= 25:
                        scores[MarketRegime.BREAKOUT] += 1.5
                    
                    # 거래량
                    if vol_ratio >= 1.5:
                        scores[MarketRegime.BREAKOUT] += 1.5
                    elif vol_ratio >= 1.3:
                        scores[MarketRegime.BREAKOUT] += 1.0
                    
                    # 모멘텀
                    if momentum_5 > 1.0:
                        scores[MarketRegime.BREAKOUT] += 1.0
                    
                    # TRENDING_UP 점수도 보너스
                    scores[MarketRegime.TRENDING_UP] += 0.5

        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        # 7. BREAKDOWN 점수 계산
        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        if len(df) >= 20:
            recent_sr = df.tail(20)
            recent_lo = float(recent_sr['low'].min())
            
            if recent_lo > 0:
                dist_lo_pct = (current_price - recent_lo) / recent_lo * 100.0
                
                # 하단 이탈
                if dist_lo_pct < -1.5:
                    scores[MarketRegime.BREAKDOWN] += 2.0
                elif dist_lo_pct < -1.0:
                    scores[MarketRegime.BREAKDOWN] += 1.5
                elif dist_lo_pct < -0.5:
                    scores[MarketRegime.BREAKDOWN] += 1.0
                
                # 추가 확인
                if dist_lo_pct < -0.5:
                    # ADX
                    if current_adx >= 25:
                        scores[MarketRegime.BREAKDOWN] += 1.5
                    
                    # 거래량
                    if vol_ratio >= 1.5:
                        scores[MarketRegime.BREAKDOWN] += 1.5
                    elif vol_ratio >= 1.3:
                        scores[MarketRegime.BREAKDOWN] += 1.0
                    
                    # 모멘텀
                    if momentum_5 < -1.0:
                        scores[MarketRegime.BREAKDOWN] += 1.0
                    
                    # TRENDING_DOWN 점수도 보너스
                    scores[MarketRegime.TRENDING_DOWN] += 0.5

        # 8. 이전 체제 + 1차 MA 체제에 "보너스" 부여 (플리핑 방지)
        if _prev_stable_regime_for_symbol in scores:
            scores[_prev_stable_regime_for_symbol] += 0.5

        # ✅ 1차 MA 체제(primary_regime)는 "유효 투표권" 부여 (독재 금지)
        # - MA10/MA20이 방향 일치할 때만 이미 primary_regime이 결정됨
        # - 따라서 +1.0은 "전환 초입을 살리는 표" 역할
        if primary_regime in scores:
            scores[primary_regime] += 1.0

        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        # 9. 최종 체제 결정 (Hysteresis 적용)
        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        # 🆕 이전 체제 기반 전환 가중치 조정 (2025-12-10)
        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        # 목적: 체제 전환 민감도 강화 (관성 축소)
        # 방법: 이전 체제 + 현재 지표 조합으로 전환 신호 증폭
        # 효과: TU↔TD 전환이 1~2봉 빨라짐
        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        prev_regime = _prev_stable_regime_for_symbol
        
        # 체제별 점수 추출 (안전하게)
        tu = MarketRegime.TRENDING_UP
        td = MarketRegime.TRENDING_DOWN
        rg = MarketRegime.RANGING
        
        tu_score = float(scores.get(tu, 0.0))
        td_score = float(scores.get(td, 0.0))
        rg_score = float(scores.get(rg, 0.0))
        
        # (A) TRENDING_UP → TRENDING_DOWN 전환 가속
        # 조건: MA5 < MA20 + 모멘텀 음수 + ADX 유지
        if prev_regime == tu:
            if ma5 < ma20 and momentum_5 < -0.5 and current_adx >= 20:
                boost = 0.8
                penalty = 0.3
                td_score += boost
                tu_score -= penalty
                logger.debug(
                    f"🔄 [Regime-TU→TD] {symbol} 하락 전환 가속 | "
                    f"MA5<MA20 | mom5:{momentum_5:.2f} | ADX:{current_adx:.1f} | "
                    f"TD점수 +{boost:.1f}, TU점수 -{penalty:.1f}"
                )
        
        # (B) TRENDING_DOWN → TRENDING_UP 전환 가속
        # 조건: MA5 > MA20 + 모멘텀 양수 + ADX 유지
        elif prev_regime == td:
            if ma5 > ma20 and momentum_5 > 0.8 and current_adx >= 20:
                boost = 0.8
                penalty = 0.3
                tu_score += boost
                td_score -= penalty
                logger.debug(
                    f"🔄 [Regime-TD→TU] {symbol} 상승 전환 가속 | "
                    f"MA5>MA20 | mom5:{momentum_5:.2f} | ADX:{current_adx:.1f} | "
                    f"TU점수 +{boost:.1f}, TD점수 -{penalty:.1f}"
                )
        
        # (C) RANGING → 강한 추세 전환 가속
        # 조건: 완전 정/역배열 + 강한 ADX + 강한 모멘텀
        elif prev_regime == rg:
            # 상승 추세 전환
            if is_full_bullish and current_adx >= 22 and momentum_5 > 1.5:
                boost = 0.7
                tu_score += boost
                logger.debug(
                    f"🔄 [Regime-RG→TU] {symbol} 상승 추세 전환 | "
                    f"완전정배열 | mom5:{momentum_5:.2f} | ADX:{current_adx:.1f} | "
                    f"TU점수 +{boost:.1f}"
                )
            # 하락 추세 전환
            elif is_full_bearish and current_adx >= 22 and momentum_5 < -1.5:
                boost = 0.7
                td_score += boost
                logger.debug(
                    f"🔄 [Regime-RG→TD] {symbol} 하락 추세 전환 | "
                    f"완전역배열 | mom5:{momentum_5:.2f} | ADX:{current_adx:.1f} | "
                    f"TD점수 +{boost:.1f}"
                )
        
        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        # 점수 안전장치 (음수 방지 + 상한 제한)
        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        tu_score = max(0.0, min(tu_score, 15.0))  # 0~15 범위
        td_score = max(0.0, min(td_score, 15.0))
        rg_score = max(0.0, min(rg_score, 15.0))
        
        # 보정된 점수 재적용
        scores[tu] = tu_score
        scores[td] = td_score
        scores[rg] = rg_score
        
        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        # 🆕 하락 구조 확인: 하락 진행 중이면 RANGING 전환 금지 (2025-12-23)
        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        # 목적: 하락 중간의 일시적 변동성 축소를 횡보로 오판하는 것 방지
        # 조건: MA역배열 + MA5 하락 기울기 + 음수 모멘텀 = 하락 구조 유지
        # 효과: DOOD 04:41 같은 "하락 중 RANGING 오전환" 차단
        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        if ma5 < ma20 and mr_slope_ma5 < 0 and momentum_5 < 0:
            # 하락 구조 확정 → RANGING 점수는 TD보다 항상 낮게 캡(최소 여유 0.5)
            rg_before = rg_score
            rg_cap = max(0.0, td_score - 0.5)
            rg_score = min(rg_score, rg_cap)
            scores[rg] = rg_score

            if rg_score < rg_before:
                logger.info(
                    f"🚫 [하락구조유지] {symbol} RANGING 전환 차단 | "
                    f"MA5<MA20 ({ma5:.2f}<{ma20:.2f}) | slope_ma5={mr_slope_ma5:.3f}% | mom5={momentum_5:.2f}% | "
                    f"RG점수 {rg_before:.2f}→{rg_score:.2f} (cap={rg_cap:.2f})"
                )
        
        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        # 최종 체제 결정 (Score는 참고용, 정석체제는 SSoT로 확정)
        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        best_regime_by_score = max(scores.items(), key=lambda kv: kv[1])[0]
        best_score_by_score = float(scores[best_regime_by_score])
        
        # hysteresis 입력은 기존 변수(best_regime/best_score)로 유지 (구조 최소 변경)
        best_regime = best_regime_by_score
        best_score = best_score_by_score
        
        current_regime = _prev_stable_regime_for_symbol
        current_score = float(scores.get(current_regime, 0.0))
        
        # 히스테리시스 임계값 로드
        cur_thresh = REGIME_HYSTERESIS_THRESHOLDS.get(
            current_regime.value, {"entry": 2.5, "exit": 1.5}
        )
        nxt_thresh = REGIME_HYSTERESIS_THRESHOLDS.get(
            best_regime_by_score.value, {"entry": 2.5, "exit": 1.5}
        )

        # ✅ TD→RANGING 추가쿨다운은 Score/Hysteresis 참고값에만 적용
        # primary_regime SSoT를 조기 return으로 덮지 않는다.
        if (current_regime == MarketRegime.TRENDING_DOWN and best_regime_by_score == MarketRegime.RANGING):
            if _regime_last_change_for_symbol is not None:
                elapsed = (datetime.now() - _regime_last_change_for_symbol).total_seconds()
                _gc_full = is_full_bullish or (ma5 > ma10)  # GC 상태면 쿨다운 면제
                if elapsed < (self.regime_cooldown_seconds * 1.5) and not _gc_full:
                    _td_rg_score_candidate = best_regime_by_score
                    _td_rg_score_candidate_score = best_score_by_score
                    logger.info(
                        f"⏸️ [TD→RANGING-추가쿨다운] {symbol} Score/Hysteresis 전환만 보류 | "
                        f"elapsed={elapsed:.0f}s < {self.regime_cooldown_seconds*1.5:.0f}s | "
                        f"primary={primary_regime.value} current={current_regime.value} "
                        f"score_candidate={_td_rg_score_candidate.value}({_td_rg_score_candidate_score:.2f}) "
                        f"→ primary_regime SSoT는 계속 진행"
                    )
                    best_regime_by_score = current_regime
                    best_score_by_score = current_score
                    nxt_thresh = cur_thresh
        
        # 🔧 수정: 새 체제 진입 조건 먼저 체크 (강한 신호 우선)
        # 1) 새 체제가 entry 이상 + 현재 체제가 exit 미만 → 전환
        if best_score_by_score >= nxt_thresh["entry"] and current_score < cur_thresh["exit"]:
            final_regime = best_regime_by_score
            logger.info(
                f"🔄 [Hysteresis전환] {current_regime.value}→{best_regime_by_score.value} | "
                f"현재={current_score:.2f}<exit={cur_thresh['exit']} | "
                f"새점수={best_score_by_score:.2f}≥entry={nxt_thresh['entry']}"
            )
        # 2) 새 체제가 압도적으로 높으면 강제 전환 (현재 체제 2배 초과)
        elif best_score_by_score >= nxt_thresh["entry"] and best_score_by_score > current_score * 2.0:
            final_regime = best_regime_by_score
            logger.info(
                f"🔄 [Hysteresis강제전환] {current_regime.value}→{best_regime_by_score.value} | "
                f"새점수={best_score_by_score:.2f} > 현재점수={current_score:.2f}×2 (압도적)"
            )
        
        elif current_score >= cur_thresh["exit"]:
            final_regime = current_regime
            logger.debug(
                f"⏸️ [Hysteresis유지] {current_regime.value} | "
                f"현재점수={current_score:.2f} ≥ exit={cur_thresh['exit']} | "
                f"새점수={best_score_by_score:.2f}"
            )
        
        else:
            final_regime = current_regime
            logger.debug(
                f"⏸️ [Hysteresis대기] {current_regime.value} | "
                f"새점수={best_score_by_score:.2f} < entry={nxt_thresh['entry']}"
            )
        
        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        # 🔒 [TU-확정게이트]
        # - primary_regime은 1차 후보값이다.
        # - Hysteresis/Score가 아직 TU를 확정하지 않았으면
        #   순간 TU를 최종 체제로 덮어쓰지 않는다.
        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        _hyst_regime = final_regime

        final_regime = primary_regime

        if (
            primary_regime == MarketRegime.TRENDING_UP
            and _hyst_regime != MarketRegime.TRENDING_UP
        ):
            logger.info(
                f"🔒 [TU-확정게이트] {symbol} primary=TU 미확정 → "
                f"{_hyst_regime.value} 사용 | "
                f"score={best_regime_by_score.value} current={current_regime.value}"
            )
            final_regime = _hyst_regime

        best_regime = final_regime  # 기존 로직 호환성 유지

        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        # 🆕 MA 기반 1차 체제와 Score/Hysteresis 최종체제 관계 정리
        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        reconcile_reason = ""

        if final_regime == best_regime_by_score:
            reconcile_reason = f"최종체제=Score일치 → {final_regime.value}"
        elif final_regime != primary_regime:
            reconcile_reason = (
                f"TU-확정게이트 적용 | "
                f"primary={primary_regime.value} → final={final_regime.value} | "
                f"Score참고={best_regime_by_score.value}"
            )
        else:
            reconcile_reason = (
                f"정석체제={primary_regime.value} | "
                f"Score참고={best_regime_by_score.value}"
            )
        
        # 상세 로그
        logger.debug(
            f"📊 [RegimeScore] "
            f"ADX={current_adx:.1f} | "
            f"mom5={momentum_5:.2f}% | mom20={momentum_20:.2f}% | "
            f"vol={vol_ratio:.2f}x | range={range_width_pct:.2f}% | "
            f"scores=[UP:{scores[MarketRegime.TRENDING_UP]:.2f} "
            f"DOWN:{scores[MarketRegime.TRENDING_DOWN]:.2f} "
            f"RANG:{scores[MarketRegime.RANGING]:.2f} "
            f"BO:{scores[MarketRegime.BREAKOUT]:.2f} "
            f"BD:{scores[MarketRegime.BREAKDOWN]:.2f}] → {best_regime_by_score.value}"
        )
        
        if reconcile_reason:
            logger.info(f"🔧 [체제조정] {reconcile_reason}")

        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        # 🆕 불장 모드 적용 - 엄격한 조건 (BULL_MARKET = 무손실 체제)
        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        # 철학:
        # - global_bull_mode는 "시장 컨텍스트"
        # - BULL_MARKET은 "개별 종목이 정말 강할 때만" 허용
        # - 손절 발생 = 체제 오판 → 승격 요건을 강하게 잠근다
        if self.global_bull_mode and final_regime == MarketRegime.TRENDING_UP:
            bull_conditions = {
                "full_bullish": bool(is_full_bullish),                       # 완전 정배열 필수
                "strong_adx": (current_adx >= 25),                           # 강한 추세
                "strong_momentum": (momentum_5 > 1.0 and momentum_20 > 1.5), # 지속 상승
                "volume_support": (vol_ratio >= 1.2),                        # 거래량 지지
                "ma20_rising": (mr_ma20_slope >= 0.10),                      # MA20 명확한 상승(0.10%+)
            }

            if all(bull_conditions.values()):
                final_regime = MarketRegime.BULL_MARKET
                logger.info(
                    f"🔥 [BULL_MARKET] TU→BULL 승격 | "
                    f"full={bull_conditions['full_bullish']} | "
                    f"ADX={current_adx:.1f} | mom5={momentum_5:.2f}% | mom20={momentum_20:.2f}% | "
                    f"vol={vol_ratio:.2f}x | ma20_slope={mr_ma20_slope:.2f}%"
                )
            else:
                failed = [k for k, v in bull_conditions.items() if not v]
                logger.info(
                    f"⚠️ [BULL_MARKET-REJECT] TU 유지 | "
                    f"미달={','.join(failed)} | global_bull_mode=ON"
                )
                
        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        # 🆕 RANGING 판정 완충: 반등 에너지 감지 시 이전 체제 유지 (2025-12-19)
        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        # 목적: RANGING 오판으로 인한 손실 방지
        # 근거: 리포트 데이터상 RANGING 50% 손절률 vs 다른 체제 100% 승률
        # 방법: RANGING 확정 직전, 반등 징후가 있으면 이전 체제 유지
        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        # 🆕 상향전환(TD→RANGING)+신선GC SSoT 플래그 — 후단 되돌림 4곳 공통 소비
        # 하향/진짜하락 보호 불변, 상향+GC만 관성 면제 (1차: TD→RANGING 한정)
        _primary_gc_upshift_ok = bool(
            prev_regime == MarketRegime.TRENDING_DOWN
            and primary_regime == MarketRegime.RANGING
            and ma5 > 0
            and ma10 > 0
            and ma20 > 0
            and ma5 > ma10
            and mr_slope_ma5 > 0
            and mr_ma20_slope > -0.15
        )
        if _primary_gc_upshift_ok:
            logger.info(
                f"🔓 [Regime-GC-Upshift-Exempt] {symbol} "
                f"TD→{primary_regime.value} 신선GC 상향 → 후단 관성 면제 | "
                f"mr_ma20={mr_ma20_slope:+.3f}%"
            )

        if final_regime == MarketRegime.RANGING and prev_regime != MarketRegime.RANGING:
            candle_body_pct = safe_extract_scalar(df['candle_body_pct'].iloc[-1]) if 'candle_body_pct' in df.columns else 0.0
            
            # 🔒 RANGING 오판 방지 하한선: "중기 하락 추세"면 RANGING 확정을 막는다
            # ✅ 3분봉 정석: MA20(60분)이 중기 대표 (MA60은 너무 둔감)
            # - 로컬 변수 ma20_slope(L6951), slope_ma120(L6980) 재사용 (재계산/섀도잉 금지)
            # 🔒 MA120 DeadZone(L10216)과 SSoT 통일: |slope|<0.03%는 잡음 → 하락 근거 금지
            # OR→AND: 한쪽만 잡음 음수여도 RANGING이 TD로 뒤집히던 SAND 05:26 차단
            is_mid_downtrend = (
                _mr_ma120_available
                and (mr_ma20_slope < -_MR_SLOPE_NOISE_THR)
                and (mr_slope_ma120 < -_MR_SLOPE_NOISE_THR)
            )
            
            # 반등 에너지: 양봉(0.3% 이상) + 거래량 형성(0.8x 이상)
            has_bounce_energy = (
                candle_body_pct >= 0.3 and       # 양봉 (음봉 제외)
                vol_ratio >= 0.8                 # 거래량 형성 중
            )
            
            if has_bounce_energy and not _primary_gc_upshift_ok:
                final_regime = prev_regime  # 이전 체제 유지 (기존 완충 유지)
                logger.info(
                    f"🟡 [RANGING-완충] {symbol} 반등 에너지 감지 → 이전 체제 유지 | "
                    f"candle={candle_body_pct:.2f}%, vol={vol_ratio:.2f}x | "
                    f"RANGING 확정 유보 → {prev_regime.value} 유지"
                )
            
            # 🔥 추가: 완충 이후에도 "중기 하락 추세"면 RANGING 확정 금지 → TRENDING_DOWN로 되돌림
            if final_regime == MarketRegime.RANGING and is_mid_downtrend:
                final_regime = MarketRegime.TRENDING_DOWN
                logger.info(
                    f"🔒 [RANGING-Guard] {symbol} 중기 하락추세 감지(MA20 AND MA120 < -{_MR_SLOPE_NOISE_THR:.2f}%) → RANGING 금지 | "
                    f"ma20_slope={mr_ma20_slope:.4f}%, ma120_slope={mr_slope_ma120:.4f}%"
                )

        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        # ✅ 하락-횡보(RANGING_DOWN) 절대 차단 (WCT 16:06 케이스 방지)
        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        # 목적: 하락 추세인데 단기 반등으로 MA5>=MA10 순간 RANGING 오판 방지
        # 조건: 연속 하락 + 모멘텀 음수 + MA20 기울기 음수 → RANGING 금지
        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        try:
            if final_regime == MarketRegime.RANGING:
                downtrend_like = (
                    (mr_ma20_slope < 0.0) and
                    (momentum_5 < 0.0 or momentum_20 < 0.0) and
                    (consecutive_drops >= 3)
                )
                
                if downtrend_like:
                    logger.info(
                        f"🚫 [RANGING→TD-Guard] {symbol} 하락-횡보 감지 → TRENDING_DOWN 강제 | "
                        f"ma20_slope={mr_ma20_slope:.3f}% | mom5={momentum_5:.2f}% | mom20={momentum_20:.2f}% | "
                        f"drops={consecutive_drops}"
                    )
                    final_regime = MarketRegime.TRENDING_DOWN
        except Exception as e:
            logger.debug(f"⚠️ [RANGING→TD-Guard] {symbol} 체크 실패: {e}")

        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        # 🔒 PATCH 12-A: MA120 slope Dead Zone (후보 생성 억제)
        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        # 목적: MA120 slope 미세 잡음으로 인한 체제 후보 남발 차단
        # 원칙: 확정 지연이 아니라 "후보 생성 품질" 개선
        # 효과: ±0.03% 이내 잡음은 중립 처리 → 정석 primary 우선 유지
        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        try:
            # Dead Zone: MA120 slope 미세 잡음 억제
            # ✅ 3분봉 정석: MA10/MA20(primary_regime)가 방향 일치하면 DeadZone 면제
            # - MA120(6시간)은 3분봉에서 너무 느려서 전환 초입을 죽임
            # - primary_regime이 명확한 방향이면 "잡음"이 아니라 "전환 초입"
            adx_for_deadzone = float(current_adx) if current_adx is not None else 0.0
            primary_has_direction = (primary_regime != MarketRegime.RANGING)
            
            if _mr_ma120_available and abs(mr_slope_ma120) < _MR_SLOPE_NOISE_THR and adx_for_deadzone < 35.0 and not primary_has_direction:
                # 🔒 가드상 primary는 항상 RANGING → stale self.market_regime(과거 TD/TU) 대신 정석 RANGING 유지
                _deadzone_keep = (
                    primary_regime
                    if primary_regime == MarketRegime.RANGING
                    else _prev_stable_regime_for_symbol
                )
                final_regime = _deadzone_keep
                logger.debug(
                    f"⏸️ [Regime-DeadZone] {symbol} MA120 slope 잡음 "
                    f"({mr_slope_ma120:.5f}%) → 체제 후보 생성 억제 "
                    f"(keep={_deadzone_keep.value}, prev={_prev_stable_regime_for_symbol.value}, primary={primary_regime.value}) | "
                    f"adx={adx_for_deadzone:.1f}"
                )
        except Exception as e:
            logger.debug(f"⚠️ [Regime-DeadZone] {symbol} 체크 실패: {e}")

        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        # 🔒 PATCH 12-B: RANGING → TRENDING_UP 후보 질 필터 (선택)
        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        # 목적: 횡보 말기 미세 반등 오판 방지
        # 조건: 최소 시동 조건(momentum_5 > 0.5 또는 adx >= 20) 요구
        # 효과: "진짜 시동"만 TRENDING_UP 후보로 승격
        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        try:
            if final_regime == MarketRegime.TRENDING_UP and _prev_stable_regime_for_symbol == MarketRegime.RANGING:
                # 최소 시동 조건 1개만 요구
                if not (momentum_5 > 0.5 or current_adx >= 20):
                    final_regime = _prev_stable_regime_for_symbol
                    logger.debug(
                        f"⏸️ [Regime-Candidate-Guard] {symbol} RANGING→UP 후보 보류 | "
                        f"momentum5={momentum_5:.2f}%, adx={current_adx:.1f} (최소 조건 미달)"
                    )
        except Exception as e:
            logger.debug(f"⚠️ [Regime-Candidate-Guard] {symbol} 체크 실패: {e}")

        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        # 🔒 글로벌 표준 후보(정석)로 최종 후보를 단일화
        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        # - 입력: MA60/MA120(EMA50/200 대응) + ADX만 사용
        # - Volume/Momentum/Position/Slope 등 "진입용 미시 신호"는 체제 후보에서 배제
        # - 이후 5봉 persistence가 작동하게 해서 1분 플리핑 제거
        # ============================================================
        # 🔍 PATCH 3: 글로벌 벤치마크는 관찰용 센서 (덮어쓰기 금지)
        # ============================================================
        adx_g = float(current_adx) if current_adx is not None else 0.0

        # 글로벌 벤치마크를 별도 변수로 계산 (final_regime 덮어쓰기 금지)
        global_regime = final_regime
        if adx_g < 15:
            global_regime = MarketRegime.RANGING
        else:
            if ma60 > 0 and ma120 > 0:
                # 🔒 [GLOBAL-TD-S60-STRUCT-0809]
                # MA60<MA120 레벨 지연만으로 TD를 잠그지 않는다.
                # STRUCT20 MA60까지 실제 하락 중일 때만 global TD 확정.
                if ma60 > ma120 and adx_g >= 20:
                    global_regime = MarketRegime.TRENDING_UP
                elif (
                    ma60 < ma120
                    and adx_g >= 20
                    and _sh_s60_struct <= 0.0
                ):
                    global_regime = MarketRegime.TRENDING_DOWN
                else:
                    global_regime = MarketRegime.RANGING
            else:
                global_regime = MarketRegime.RANGING

        # BENCHMARK-OBSERVE 로그 제거 (SUPPRESS-SHADOW 대체·bloat 14k/일)
        # global_regime 계산 유지 — GLOBAL-TD-GUARD(L14366)·CLEAR-UPTREND-TU(L14398) 소비

        # ✅ RAW 캡처: Inertia/Unified-Inertia 적용 전 로컬 판결
        raw_regime = final_regime

        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        # 🔒 PATCH 12: 체제 전환 2봉 확정 (초미니 - 최우선 적용)
        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        # 목적: MA120 slope 미세 전환으로 인한 체제 플리핑 방지
        # 원칙: 동일 후보가 2회 연속일 때만 전환 확정
        # 역할: 전처리 필터 (Regime Inertia/Unified Inertia는 후처리로 유지)
        # 위치: raw_regime 직후, 기존 Inertia보다 우선 적용
        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        if not hasattr(self, "_regime_2candle_confirm"):
            self._regime_2candle_confirm = {}
        
        prev_regime = _prev_stable_regime_for_symbol or final_regime  # 초기값 보호
        candidate = final_regime
        
        # ─────────────────────────────────────────────────────────────
        # 🔒 PATCH 12-MINI-A: RANGING → TRENDING_UP 후보 질 필터 (이중 방어)
        # ─────────────────────────────────────────────────────────────
        # 목적: 조용한 MA 잡음으로 candidate 생성되는 것 방지
        # 위치: PATCH 12 내부 (raw_regime 이후 재검증)
        # 효과: final_regime 필터(12-B) 통과한 것도 재차 검증
        # ─────────────────────────────────────────────────────────────
        try:
            if (
                _prev_stable_regime_for_symbol == MarketRegime.RANGING and
                candidate == MarketRegime.TRENDING_UP
            ):
                # 최소 시동 조건 (1개만 충족해도 OK)
                has_min_start_signal = (
                    momentum_5 >= 0.5 or        # 가격 모멘텀
                    current_adx >= 20 or        # 추세 강도
                    vol_ratio >= 1.2            # 거래량 시동
                )
                
                if not has_min_start_signal:
                    logger.debug(
                        f"⏸️ [Regime-Candidate-Guard-2nd] {symbol} "
                        f"RANGING→UP candidate 보류 (2차 검증) | "
                        f"mom5={momentum_5:.2f}%, ADX={current_adx:.1f}, vol={vol_ratio:.2f}x"
                    )
                    candidate = _prev_stable_regime_for_symbol
        except Exception:
            pass
        
        # 심볼별 확정 카운터
        confirm_state = self._regime_2candle_confirm.get(symbol, {
            "prev_candidate": prev_regime,
            "counter": 0
        })
        
        # 동일 후보가 연속인지 체크
        if candidate != confirm_state["prev_candidate"]:
            # 🔧 PATCH 12-D: RANGING ↔ TRENDING_UP 사이 흔들림은 완전 리셋 금지
            # RANGING <-> TRENDING_UP 사이에서는 counter - 1 (감속), 완전 리셋 방지
            if {candidate, confirm_state["prev_candidate"]} == {
                MarketRegime.RANGING, MarketRegime.TRENDING_UP
            }:
                # 미세 잡음으로 인한 영구 대기 방지: 감속만 적용
                confirm_state["counter"] = max(1, confirm_state["counter"] - 1)
                confirm_state["prev_candidate"] = candidate
                logger.debug(
                    f"🔧 [Counter-감속] {symbol} RANGING↔UP 흔들림 | "
                    f"counter {confirm_state['counter']+1}→{confirm_state['counter']} (완전 리셋 방지)"
                )
            else:
                # 다른 체제 전환은 기존대로 완전 리셋
                confirm_state["prev_candidate"] = candidate
                confirm_state["counter"] = 1
        else:
            # 동일 후보 연속 → 카운터 증가
            confirm_state["counter"] += 1
        
        # ─────────────────────────────────────────────────────────────
        # 🔧 PATCH 12-C′: 장기 대기 후보 정리 (초미니)
        # ─────────────────────────────────────────────────────────────
        # 목적: 의미 없는 후보 누적으로 인한 체제 정체 방지
        # 조건: counter ≥ 3 이지만 아직 확정되지 않은 경우
        # 단, TRENDING_UP 유지 중인 경우는 보호 (PATCH 12-C 일관성)
        # 효과: 2봉 목표인데 3봉 넘어가면 잡음으로 판단 → 후보 폐기
        # ─────────────────────────────────────────────────────────────
        # ✅ 옵션 B: RANGING→TRENDING_UP 전환은 2~3봉 확인으로 확정(정석)
        # - 기본은 5봉(보수)
        # - 단, "골든크로스+강한 ADX"가 확인되면 3봉
        # - 더 강하면(ADX>=50 + 거래량/모멘텀) 2봉
        PERSIST_BARS = 5

        adx_b = float(current_adx) if current_adx is not None else 0.0
        is_ru = (prev_regime == MarketRegime.RANGING and candidate == MarketRegime.TRENDING_UP)
        has_gc = (ma5 > 0 and ma10 > 0 and ma5 > ma10)

        if is_ru and has_gc and adx_b >= 45.0:
            PERSIST_BARS = 3
            if adx_b >= 50.0 and (vol_ratio >= 1.2 or momentum_5 >= 0.8):
                PERSIST_BARS = 2

        # ─────────────────────────────────────────────────────────────
        # 🔧 PATCH 12-C′: 장기 대기 후보 정리 (무한 반복 방지)
        # ─────────────────────────────────────────────────────────────
        # 기존 'counter>=3 즉시 폐기'는 옵션 B(2~3봉 확정)과 충돌하므로 폐기 임계값을 상향한다.
        # - 확정(PERSIST_BARS)보다 충분히 오래 대기할 때만 후보 폐기
        cleanup_threshold = max(PERSIST_BARS + 2, 6)

        if (
            confirm_state["counter"] >= cleanup_threshold and
            candidate != prev_regime and
            not (
                prev_regime == MarketRegime.TRENDING_UP and
                candidate == MarketRegime.TRENDING_UP
            )
        ):
            logger.debug(
                f"🧹 [Regime-Candidate-Cleanup] {symbol} "
                f"candidate={candidate.value} counter={confirm_state['counter']} (th={cleanup_threshold}) → 후보 폐기 (잡음 판정)"
            )
            candidate = prev_regime
            confirm_state["counter"] = 0

        if confirm_state["counter"] >= PERSIST_BARS:
            final_regime = candidate
            logger.debug(
                f"✅ [체제확정] {symbol} {prev_regime.value}→{candidate.value} | "
                f"연속={confirm_state['counter']}/{PERSIST_BARS}봉 | adx={adx_b:.1f}"
            )
        else:
            if _primary_gc_upshift_ok:
                final_regime = primary_regime
                logger.debug(
                    f"🔓 [GC-Upshift-PATCH12] {symbol} 신선GC → SSoT 즉시 확정 | "
                    f"{prev_regime.value}→{primary_regime.value}"
                )
            elif (
                prev_regime == MarketRegime.TRENDING_UP
                and _primary_tu_decay
            ):
                # 🔒 stale TU 즉시 해제 — 현재 동적 UP 임계 이탈을 그대로 소비한다.
                final_regime = primary_regime
                logger.info(
                    f"🔓 [STALE-TU-RELEASE] {symbol} TU→RANGING 즉시 확정 | "
                    f"s10={mr_slope_ma10:+.3f}% dir10={ma10_direction} "
                    f"up>{REGIME_SLOPE_UP_FAST*100:.3f}% | "
                    f"counter={confirm_state['counter']}/{PERSIST_BARS}"
                )
            else:
                final_regime = prev_regime
                logger.info(
                    f"⏸️ [{PERSIST_BARS}봉대기] {symbol} candidate={candidate.value}→유지={prev_regime.value} | "
                    f"연속={confirm_state['counter']}/{PERSIST_BARS}봉 | adx={adx_b:.1f}"
                )
        
        # 상태 저장
        self._regime_2candle_confirm[symbol] = confirm_state

        # ============================================================
        # 🧲 Regime Inertia (플리핑 억제) — Analysis 전용 SSoT
        # ============================================================
        # 설정값 (보수적 기본)
        # ✅ 충돌 제거 원칙:
        # - RANGING→TU 업시프트는 "PERSIST_BARS"가 이미 판사 역할을 했으므로
        #   Inertia에서 추가로 5봉을 또 요구하지 않는다.
        # - 그 외(하향/기타 전환)는 기존 5봉 보수 유지
        REGIME_INERTIA_MIN_BARS = 5      # 최소 5봉 연속 합의
        REGIME_INERTIA_MAX_BARS = 8      # 상한(안정화)
        REGIME_HEAD_LIFT_HOLD_BARS = 5   # HEAD_LIFT도 동일하게 5봉 보류
        
        # 상태 저장 (심볼별)
        if not hasattr(self, "_regime_inertia"):
            self._regime_inertia = {}
        
        state = self._regime_inertia.get(symbol, {
            "last_regime": final_regime,
            "same_count": 0,
            "hold_down": 0
        })
        
        # HEAD_LIFT(횡보-시동형) 감지 시 하향 전환 보류 카운트 설정
        # indicators가 없을 수 있으므로 None-safe로 현재 봉 gajami_meta만 조회
        _gmeta = (indicators or {}).get("gajami_meta") or {}
        head_lift = bool(_gmeta.get("head_lift_trigger", False))
        
        if head_lift:
            state["hold_down"] = REGIME_HEAD_LIFT_HOLD_BARS
        
        # ✅ Inertia need 결정
        inertia_need = REGIME_INERTIA_MIN_BARS
        try:
            # 업시프트(R→TU)는 기존 완화 유지
            if (state["last_regime"] == MarketRegime.RANGING and 
                final_regime == MarketRegime.TRENDING_UP):
                inertia_need = min(REGIME_INERTIA_MIN_BARS, PERSIST_BARS)
            # 🔒 stale TU 해제: 현재 후보가 RANGING/TD면 오래된 TU 관성 유지 금지
            elif (state["last_regime"] == MarketRegime.TRENDING_UP and
                  final_regime in (MarketRegime.RANGING, MarketRegime.TRENDING_DOWN) and
                  (state.get("hold_down", 0) <= 0 or _primary_tu_decay)):
                inertia_need = 1
                logger.debug(f"🧲 [Regime-Reversal] {symbol} TU→{final_regime.value} → 관성 즉시 해제")
        except Exception:
            inertia_need = REGIME_INERTIA_MIN_BARS
        
        # 후보 체제와 이전 체제 비교
        if final_regime == state["last_regime"]:
            state["same_count"] = min(state["same_count"] + 1, REGIME_INERTIA_MAX_BARS)
        else:
            # 하향 전환(TU/RANGING → TD) 보류 조건
            is_downshift = (
                state["last_regime"] in (MarketRegime.TRENDING_UP, MarketRegime.RANGING)
                and final_regime == MarketRegime.TRENDING_DOWN
            )
            
            if is_downshift and state.get("hold_down", 0) > 0:
                # 보류: 이전 체제 유지
                state["hold_down"] -= 1
                final_regime = state["last_regime"]
                state["same_count"] = 1
                logger.info(
                    f"🧲 [Regime-Inertia] {symbol} 하향 전환 보류 | "
                    f"유지={final_regime.value} "
                    f"HEAD_LIFT={head_lift} hold={state['hold_down']}"
                )
            else:
                # 합의 봉 수가 부족하면 전환 보류
                if state["same_count"] + 1 < inertia_need and not _primary_gc_upshift_ok:
                    final_regime = state["last_regime"]
                    state["same_count"] += 1
                    logger.info(
                        f"🧲 [Regime-Inertia] {symbol} 전환 보류 | "
                        f"candidate→유지={final_regime.value} "
                        f"need={inertia_need} have={state['same_count']}"
                    )
                else:
                    # 전환 확정
                    state["last_regime"] = final_regime
                    state["same_count"] = 1
        
        # 상태 저장
        self._regime_inertia[symbol] = state

        # ============================================================
        # 🧲 Unified Inertia — GAJAMI / BOX / REGIME (Analysis SSoT)
        # ============================================================
        # 공통 설정
        INERTIA_MIN_BARS = 5
        INERTIA_MAX_BARS = 8
        
        if not hasattr(self, "_analysis_inertia"):
            self._analysis_inertia = {}
        
        st = self._analysis_inertia.get(symbol, {
            "regime": final_regime,  # indicators 대신 final_regime 사용
            "box_state": None,
            "cnt": 0
        })
        
        # 현재 후보
        cand_regime = final_regime
        cand_box = None  # analyze_market_regime에서는 box_state 접근 불가
        
        # HEAD_LIFT가 있으면 하향(BOX/REGIME) 전환을 보류
        # indicators가 없을 수 있으므로 None-safe로 현재 봉 gajami_meta만 조회
        _gmeta = (indicators or {}).get("gajami_meta") or {}
        head_lift = bool(_gmeta.get("head_lift_trigger", False))
        
        # ✅ Unified-Inertia need 결정
        unified_need = INERTIA_MIN_BARS
        try:
            # 업시프트(R→TU)는 기존 완화 유지
            if (st["regime"] == MarketRegime.RANGING and 
                cand_regime == MarketRegime.TRENDING_UP):
                unified_need = min(INERTIA_MIN_BARS, PERSIST_BARS)
            # 🔒 stale TU 해제: 현재 후보가 RANGING/TD면 오래된 TU 관성 유지 금지
            elif (st["regime"] == MarketRegime.TRENDING_UP and
                  cand_regime in (MarketRegime.RANGING, MarketRegime.TRENDING_DOWN) and
                  (not head_lift or _primary_tu_decay)):
                unified_need = 1
                logger.debug(f"🧲 [Unified-Reversal] {symbol} TU→{cand_regime.value} → 관성 즉시 해제")
        except Exception:
            unified_need = INERTIA_MIN_BARS
        
        def _same(a, b):
            return (a == b) or (a is None and b is None)
        
        # 합의 카운트
        if _same(st["regime"], cand_regime) and _same(st["box_state"], cand_box):
            st["cnt"] = min(st["cnt"] + 1, INERTIA_MAX_BARS)
        else:
            # HEAD_LIFT 중에는 하향만 보류
            downshift = (
                (st["regime"] in (MarketRegime.TRENDING_UP, MarketRegime.RANGING) and cand_regime == MarketRegime.TRENDING_DOWN)
                or (st["box_state"] == "BOTTOM" and cand_box == "TOP")
            )
            if head_lift and downshift and st["cnt"] < unified_need:
                # 보류: 기존 상태 유지
                cand_regime = st["regime"]
                cand_box = st["box_state"]
                st["cnt"] += 1
                logger.info(
                    f"🧲 [Unified-Inertia] {symbol} HEAD_LIFT 보류 | "
                    f"candidate={final_regime.value}→유지={cand_regime.value} "
                    f"cnt={st['cnt']}"
                )
            else:
                # 합의 부족 시 보류
                if st["cnt"] + 1 < unified_need and not _primary_gc_upshift_ok:
                    cand_regime = st["regime"]
                    cand_box = st["box_state"]
                    st["cnt"] += 1
                    logger.info(
                        f"🧲 [Unified-Inertia] {symbol} 합의부족 보류 | "
                        f"candidate={final_regime.value}→유지={cand_regime.value} "
                        f"cnt={st['cnt']} need={unified_need}"
                    )
                else:
                    # 확정
                    st["regime"] = cand_regime
                    st["box_state"] = cand_box
                    st["cnt"] = 1
        
        # 확정 반영 (SSoT)
        final_regime = cand_regime
        # Note: indicators는 analyze_market_regime에서 접근 불가
        # regime만 반영, box_state는 calculate_all_indicators에서 처리
        
        self._analysis_inertia[symbol] = st

        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        # 10. 체제 전환 처리
        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        # 🧲 Raw vs Stable Regime 분리 (Analysis SSoT)
        # - raw_regime    : Score/Hysteresis/Inertia 경로의 관측값(진단용)
        # - stable_regime : 5봉대기/Inertia까지 통과한 최종 확정 체제
        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        stable_regime = final_regime

        # 🔒 정석: 중기 하락 구조(MA10/MA20 기반)에서는 TU 확정을 보수화
        # - 3분봉: MA10(30분)+MA20(60분)이 체제축 (df 컬럼 대신 로컬 변수 사용)
        # - 로컬 변수: ma10(L6888), ma20(L6889), slope_ma10(L6973), ma20_slope(L6951)
        # 🔒 TD 체제인데 정배열 + MA20 약하락 → RANGING 완화
        if stable_regime == MarketRegime.TRENDING_DOWN:
            if (
                ma5 > ma10 and ma10 >= ma20
                and mr_ma20_slope > -0.15
            ):
                stable_regime = MarketRegime.RANGING
                logger.debug(
                    f"🧯 [Regime-TD-Relax] {symbol} TD+정배열+MA20약하락 → RANGING | "
                    f"ma20_slope={mr_ma20_slope:.4f}%"
                )

        if stable_regime == MarketRegime.TRENDING_UP:
            if global_regime == MarketRegime.TRENDING_DOWN:
                stable_regime = MarketRegime.RANGING
                logger.info(f"🛡️ [GLOBAL-TD-GUARD] {symbol} global=TD + stable=TU → RANGING 강등")
            elif (
                (ma10 > 0 and ma20 > 0)
                and (ma10 < ma20)
                and (mr_slope_ma10 < 0.0)
                and (mr_ma20_slope < 0.0 or primary_regime != MarketRegime.TRENDING_UP)
            ):
                stable_regime = MarketRegime.RANGING
                logger.info(
                    f"🧯 [Regime-TU-Guard] {symbol} 중기 하락 구조(MA10/MA20) → TU 보수화(RANGING) | "
                    f"ma10_slope={mr_slope_ma10:.4f}% ma20_slope={mr_ma20_slope:.4f}%"
                )
            
        # 🔧 [CLEAR-UPTREND-TU]
        # 기존 FULLSTACK 교정 경로는 그대로 보존한다.
        # 🔒 [REGIME-RG-UNDER-TU-DEADLOCK-FIX-0829-R2] ★412(2/2)
        # 앞단 RG_UNDER_TU가 이미 primary=TU로 구조 보정한 경우에는
        # 동적 UP 문턱/ma10>ma20을 여기서 다시 요구하지 않는다.
        # 단, MA10·MA20 실제 양수 방향 + ma20>ma60 + global!=TD는 유지한다.
        _clear_fullstack_up = bool(
            ma5 > ma10 > ma20 > ma60 > 0
            and mr_slope_ma10 > REGIME_SLOPE_UP_FAST * 100.0
            and mr_ma20_slope > REGIME_SLOPE_UP_SLOW * 100.0
        )
        _clear_primary_tu = bool(
            primary_regime == MarketRegime.TRENDING_UP
            and ma5 > ma10
            and ma20 > ma60 and ma60 > 0
            and mr_slope_ma10 > 0.0
            and mr_ma20_slope > 0.0
        )
        if (
            (_clear_fullstack_up or _clear_primary_tu)
            and global_regime != MarketRegime.TRENDING_DOWN
            and stable_regime != MarketRegime.TRENDING_UP
        ):
            logger.info(
                f"🔧 [CLEAR-UPTREND-TU] {symbol} "
                f"path={'FULLSTACK' if _clear_fullstack_up else 'PRIMARY_TU'} → TU 확정"
                f"(hysteresis/guard 우회) | prev={stable_regime.value} "
                f"primary={primary_regime.value} "
                f"ma20={ma20:.2f}>ma60={ma60:.2f} "
                f"s10={mr_slope_ma10:.4f}% s20={mr_ma20_slope:.4f}%"
            )
            stable_regime = MarketRegime.TRENDING_UP

        # 관측 저장 (디버깅/검증 전용)
        if not hasattr(self, "_raw_regime_by_symbol"):
            self._raw_regime_by_symbol = {}
        if not hasattr(self, "_stable_regime_by_symbol"):
            self._stable_regime_by_symbol = {}

        self._raw_regime_by_symbol[symbol] = raw_regime
        self._stable_regime_by_symbol[symbol] = stable_regime

        if raw_regime != stable_regime:
            logger.debug(
                f"🧲 [Regime-Stable] {symbol} "
                f"raw={getattr(raw_regime,'value',raw_regime)} "
                f"stable={getattr(stable_regime,'value',stable_regime)}"
            )

        regime_changed = (stable_regime != _prev_stable_regime_for_symbol)

        if regime_changed:
            now = datetime.now()
            self.regime_last_change_time = now  # legacy mirror
            self._regime_last_change_by_symbol[cache_key] = now
            self.regime_history.append((now, stable_regime))
            
            # 히스토리 최대 10개만 유지
            if len(self.regime_history) > 10:
                self.regime_history = self.regime_history[-10:]

            # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
            # 🆕 체제 전환 시 상세 로그 (MA·거래량·박스 위치 포함)
            # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
            logger.info(
                f"🔄 [체제전환] {_prev_stable_regime_for_symbol.value} → {stable_regime.value} | "
                f"정석체제(SSoT): {primary_regime.value} | "
                f"Score참고: {best_regime_by_score.value} ({best_score_by_score:.2f}점) | "
                f"ADX: {current_adx:.1f} | "
                f"모멘텀: 5={momentum_5:.1f}% 20={momentum_20:.1f}% | "
                f"MA정배열: full_bull={is_full_bullish} partial_bull={is_partial_bullish} | "
                f"MA역배열: full_bear={is_full_bearish} partial_bear={is_partial_bearish} | "
                f"MA20기울기: {mr_ma20_slope:.2f}% | "
                f"Range폭: {range_width_pct:.2f}% 위치: {pos_in_range*100:.1f}% | "
                f"거래량: {vol_ratio:.2f}x | "
                f"쿨다운: {self.regime_cooldown_seconds}초"
            )
        else:
            # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
            # 🆕 체제 유지 시 상세 로그
            # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
            logger.debug(
                f"[체제유지] {stable_regime.value} | "
                f"정석={primary_regime.value} | Score참고={best_regime_by_score.value}({best_score_by_score:.2f}) | "
                f"ADX: {current_adx:.1f} | "
                f"모멘텀: 5={momentum_5:.1f}% 20={momentum_20:.1f}% | "
                f"MA정배열: full_bull={is_full_bullish} partial_bull={is_partial_bullish} | "
                f"MA역배열: full_bear={is_full_bearish} partial_bear={is_partial_bearish} | "
                f"MA20기울기: {mr_ma20_slope:.2f}% | "
                f"Range폭: {range_width_pct:.2f}% 위치: {pos_in_range*100:.1f}% | "
                f"거래량: {vol_ratio:.2f}x"
            )

        # ============================================================
        # 🧠 최종 체제 확정 (SSoT)
        # ============================================================
        # ============================================================
        # 🆕 MR-Regime Snapshot (SSoT)
        # - 체제전환/청산 로직은 mr_regime만 사용하도록 강제
        # - df는 이미 확정봉 (라인 7390에서 drop_incomplete_candle 적용됨)
        # ============================================================
        # 🔬 [REGIME-DIVERGE] 근본(primary↔stable 괴리) 측정 전용 — 판정 미변경.
        try:
            _rd_p = primary_regime.value if hasattr(primary_regime, "value") else str(primary_regime)
            _rd_f = final_regime.value if hasattr(final_regime, "value") else str(final_regime)
            _rd_s = stable_regime.value if hasattr(stable_regime, "value") else str(stable_regime)
            if _rd_p != _rd_s:
                _rd_gap = ((ma20 - ma60) / ma60 * 100.0) if ma60 > 0 else 0.0
                logger.info(
                    f"🔬 [REGIME-DIVERGE] {symbol} primary={_rd_p} final={_rd_f} stable={_rd_s} "
                    f"global={global_regime.value if hasattr(global_regime, 'value') else global_regime} "
                    f"s60_struct={_sh_s60_struct:+.4f}% "
                    f"| corrected={indicators.get('regime_corrected', '-')} "
                    f"tu_suppressed={int(bool(indicators.get('tu_suppressed', False)))} "
                    f"ma20={ma20:.4f} ma60={ma60:.4f} gap={_rd_gap:+.3f}% "
                    f"s10={mr_slope_ma10:+.4f}% s20={mr_ma20_slope:+.4f}%"
                )
        except Exception:
            pass

        # 🔧 [TD-STRUCT-OVERRIDE] primary=TD + stable=RANGING + 확신하락 → TD 강제(박스 하단 매수 방지). conviction=0은 유지.
        _regime_out = stable_regime
        try:
            if primary_regime == MarketRegime.TRENDING_DOWN and stable_regime == MarketRegime.RANGING:
                _td_conv = bool(
                    _mr_ma120_available
                    and mr_ma20_slope < -0.10
                    and mr_slope_ma120 < -0.03
                )
                if _td_conv:
                    _regime_out = MarketRegime.TRENDING_DOWN
                    logger.info(f"🔧 [TD-STRUCT-OVERRIDE] {symbol} 확신하락 오라벨 → RANGING을 TD로 강제 | ma20_slope={mr_ma20_slope:+.4f} ma120_slope={mr_slope_ma120:+.4f}")
                else:
                    logger.info(f"🔬 [TD-STRUCT-OVERRIDE-CONV0] {symbol} conviction=0 → RANGING 유지 | ma20_slope={mr_ma20_slope:+.4f} ma120_slope={mr_slope_ma120:+.4f}")
        except Exception:
            pass
        # 🔧 [REGIME-K2G5-v2] 세 갈래 체제 = 닫힌 봉 정배열 ∧ |MA5 − MA20| ≥ 5 틱 → TRENDING_UP / TRENDING_DOWN / RANGING
        #  근거 : REGIME_R5_THREE_CLASS_PREREG_v1 · REGIME_FORWARD_LABEL_PREREG_v1 · 비교 = 종가 Decimal 정수 비교 (R114 동률 계약 · float MA 비교 아님)
        #  스위치 : config [REGIME] k2g5_enabled (기본 false = 계산 · 계수만 · 라벨 불변) · BULL_MARKET · BREAKOUT · BREAKDOWN 은 덮지 않음
        _k2 = None
        _k2_gap_t = None
        try:
            _k2_on = bool(self.config.getboolean('REGIME', 'k2g5_enabled', fallback=False)) if self.config is not None else False
        except Exception:
            _k2_on = False
        try:
            if _regime_out in (MarketRegime.TRENDING_UP, MarketRegime.TRENDING_DOWN, MarketRegime.RANGING) and len(df) >= 21:
                from decimal import Decimal
                _cl = [Decimal(str(float(x))) for x in df['close'].iloc[-20:].tolist()]
                _px = float(current_price)
                _tk = float(get_tick_size(_px) or 0.0)
                if len(_cl) == 20 and all(x.is_finite() and x > 0 for x in _cl) and math.isfinite(_px) and _tk > 0 and math.isfinite(_tk):
                    _s5, _s10, _s20 = sum(_cl[-5:]), sum(_cl[-10:]), sum(_cl)
                    _d510 = 2 * _s5 - _s10          # 부호 = MA5 − MA10 (× 10 정수배 · 정확)
                    _d1020 = 2 * _s10 - _s20        # 부호 = MA10 − MA20 (× 20)
                    _d520 = 4 * _s5 - _s20          # = 20 × (MA5 − MA20)
                    _thr = Decimal(str(_tk)) * 100  # 20 × 5 틱
                    if _d510 > 0 and _d1020 > 0 and _d520 >= _thr:
                        _k2 = MarketRegime.TRENDING_UP
                    elif _d510 < 0 and _d1020 < 0 and -_d520 >= _thr:
                        _k2 = MarketRegime.TRENDING_DOWN
                    else:
                        _k2 = MarketRegime.RANGING   # 동률 (차 = 0) = 정배열 아님
                    _k2_gap_t = float(abs(_d520) / 20 / Decimal(str(_tk)))
        except Exception:
            _k2 = None
        _k2_prev = _regime_out
        if _k2 is not None:
            self._k2g5_counts = getattr(self, '_k2g5_counts', {'eval': 0, 'diff': 0, 'applied': 0})
            self._k2g5_counts['eval'] += 1
            if _k2 != _regime_out:
                self._k2g5_counts['diff'] += 1
                if _k2_on:
                    _regime_out = _k2                     # 대입 = 로그 · 지표 기록과 분리 (로그 예외가 대입을 취소하지 않음)
                    self._k2g5_counts['applied'] += 1
        try:
            if indicators is not None and _k2 is not None:
                indicators['regime_k2g5'] = _k2.value
                indicators['regime_k2g5_prev'] = _k2_prev.value
                indicators['regime_k2g5_gap_ticks'] = round(_k2_gap_t, 2) if _k2_gap_t is not None else None
                indicators['regime_k2g5_on'] = int(_k2_on)
            if _k2 is not None and _k2 != _k2_prev:
                logger.info(f"🔧 [REGIME-K2G5] {symbol} {_k2_prev.value}→{_k2.value} on={int(_k2_on)} gap={_k2_gap_t:.1f}틱 | 계수 eval={self._k2g5_counts['eval']} diff={self._k2g5_counts['diff']} applied={self._k2g5_counts['applied']}")
        except Exception:
            pass
        logger.info(
            f"🧭 [REGIME-FINAL] {symbol} | "
            f"primary={primary_regime.value} final={final_regime.value} "
            f"stable={stable_regime.value} out={_regime_out.value} | "
            f"s10={mr_slope_ma10:+.3f}% s20={mr_ma20_slope:+.3f}% "
            f"up10>{REGIME_SLOPE_UP_FAST*100:.3f}% up20>{REGIME_SLOPE_UP_SLOW*100:.3f}% "
            f"dn10<{REGIME_SLOPE_DN_FAST*100:.3f}% dn20<{REGIME_SLOPE_DN_SLOW*100:.3f}% "
            f"atr={_reg_atr_pct:.3f}% tu_decay={int(_primary_tu_decay)}"
        )
        return self._save_and_return_regime(_regime_out, indicators, df, symbol)
    
    def classify_ranging_subtype(
        self,
        df: pd.DataFrame,
        adx: float,
        ma5_slope: float,
        ma10_slope: float,
        volume_ratio: float,
        lookback: int = 3
    ) -> str:
        """
        RANGING 서브 체제 분류
        
        철학:
        - RANGING은 단일 체제가 아니라 3가지 하위 상태를 가짐
        - 익절 전략이 서브 체제에 따라 완전히 달라짐
        - partial_sell_triggers보다 take_profit이 먼저 발동하는 문제 해결
        
        Args:
            df: OHLCV 데이터프레임 (최소 lookback+1 봉 필요)
            adx: ADX 값 (추세 강도)
            ma5_slope: MA5 기울기 (양수=상승, 음수=하락)
            ma10_slope: MA10 기울기
            volume_ratio: 거래량 비율 (현재 / MA20)
            lookback: 판단 기준 봉 수 (기본 3봉)
        
        Returns:
            "RANGING_WEAK"       - 순수 박스, 힘 없음 (빠른 익절 OK)
            "RANGING_EXPANSION"  - 박스 상단 확장 시도 (partial 우선)
            "RANGING_TOP_HEAVY"  - 고점 소모, 분배 구간 (조기 정리)
        
        설계 원칙:
        - EXPANSION: partial(2.0%)이 먼저 발동하도록 take_profit=2.2%
        - TOP_HEAVY: 빠른 회수를 위해 take_profit=1.0%
        - WEAK: 안전 운용 take_profit=1.8%
        """
        
        if df is None or len(df) < lookback + 1:
            logger.debug(
                f"[classify_ranging] 데이터 부족 | "
                f"len={len(df) if df is not None else 0} < {lookback+1} → WEAK"
            )
            return "RANGING_WEAK"
        
        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        # 1. EXPANSION: 박스 상단 확장 시도
        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        # 조건:
        # - ADX 15~25 (추세는 아니지만 힘은 있음)
        # - MA5, MA10 기울기 모두 양(+)
        # - 거래량 유지 (0.7배 이상)
        #
        # 의미: "횡보 상단을 밀어올릴 가능성"
        # 예시: PEPE 케이스
        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        if (
            15 <= adx <= 25 and
            ma5_slope > 0 and
            ma10_slope > 0 and
            volume_ratio >= 0.7
        ):
            logger.debug(
                f"🟢 [SubRegime] EXPANSION | "
                f"ADX={adx:.1f} MA5↗{ma5_slope:.5f} MA10↗{ma10_slope:.5f} V={volume_ratio:.2f}x"
            )
            return "RANGING_EXPANSION"
        
        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        # 2. TOP_HEAVY: 고점 소모, 분배 구간
        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        # 조건:
        # - 최근 lookback 봉에서 윗꼬리 비율 1.5배 이상
        # - 고점 갱신 실패 (현재 고점 ≤ 직전 고점들)
        #
        # 의미: "고점에서 매물 출회, 더 들고 있으면 손절"
        # 예시: MNT 말기 구간
        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        try:
            recent = df.tail(lookback)
            
            # 윗꼬리 비율 계산
            # 윗꼬리 = high - max(open, close)
            upper_wicks = (
                recent["high"] - recent[["open", "close"]].max(axis=1)
            )
            
            # 캔들 실체 = abs(close - open)
            body_sizes = (recent["close"] - recent["open"]).abs()
            
            # 0으로 나누기 방지
            upper_wick_ratio = (upper_wicks / (body_sizes + 1e-9)).mean()
            
            # 고점 갱신 실패 확인
            # 현재 고점이 직전 고점들(lookback-1개) 최대값보다 낮거나 같으면 실패
            if len(recent) >= 2:
                current_high = float(recent["high"].iloc[-1])
                prev_highs_max = float(recent["high"].iloc[:-1].max())
                high_failed = (current_high <= prev_highs_max)
            else:
                high_failed = False
            
            # TOP_HEAVY 판정
            if upper_wick_ratio >= 1.5 and high_failed:
                logger.debug(
                    f"🔴 [SubRegime] TOP_HEAVY | "
                    f"윗꼬리비율={upper_wick_ratio:.2f} 고점실패={high_failed}"
                )
                return "RANGING_TOP_HEAVY"
                
        except Exception as e:
            logger.debug(f"⚠️ [classify_ranging] TOP_HEAVY 판정 실패: {e}")
            # 예외 발생 시 안전하게 WEAK 반환
        
        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        # 3. WEAK: 기본 (순수 횡보)
        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        logger.debug(f"🟡 [SubRegime] WEAK (기본 횡보)")
        return "RANGING_WEAK"
        
    def analyze_volume_pattern(self, df: pd.DataFrame) -> Dict[str, Any]:
        """거래량 패턴 분석 (선행성 지표!)"""
        
        if not is_valid_df(df) or len(df) < VOLUME_MA_PERIOD:
            logger.debug(f"거래량 분석 불가: 데이터 부족 (len={len(df) if df is not None else 0})")  # ✅ debug
            return {'surge': False, 'accumulation': False, 'distribution': False, 'volume_ratio': 0}
        
        if 'volume' not in df.columns:
            logger.error(f"❌ 'volume' 컬럼 없음! 사용 가능 컬럼: {df.columns.tolist()}")
            return {'surge': False, 'accumulation': False, 'distribution': False, 'volume_ratio': 0}
        
        # 타임프레임별 거래량 MA 기간 적용 (3m은 12봉, 5m은 15봉 등)
        timeframe_str = self.config.get('TRADING', 'timeframe', fallback='3m')
        volume_ma_period = VOLUME_MA_PERIODS.get(timeframe_str, VOLUME_MA_PERIOD)
        volume_ma = df['volume'].rolling(window=volume_ma_period).mean()
        current_volume = df['volume'].iloc[LAST_INDEX]
        
        # 0 나누기 방지 (warning 없이 처리)
        volume_ma_value = volume_ma.iloc[LAST_INDEX] if not volume_ma.empty else 0
        if pd.isna(volume_ma_value) or volume_ma_value == 0:
            logger.debug(f"volume_ma 계산 불가 (NaN or 0), 기본값 사용")  # ✅ debug
            volume_ma_value = 1  # 0으로 나누기 방지

        volume_surge = current_volume > volume_ma_value * 2
        
        recent = df.tail(DEFAULT_ANALYSIS_WINDOW)
        price_change = (recent['close'].iloc[LAST_INDEX] - recent['close'].iloc[FIRST_INDEX]) / recent['close'].iloc[FIRST_INDEX]
        volume_change = recent['volume'].mean() / df['volume'].tail(WEBSOCKET_PING_INTERVAL).mean()
        
        accumulation = price_change > 0 and volume_change > 1.2
        distribution = price_change < 0 and volume_change > 1.2
        
        # ========================================
        # 🆕 디버깅: volume_ratio 계산 확인
        # ========================================
        if volume_ma.empty or volume_ma.iloc[LAST_INDEX] == 0:
            logger.warning(f"⚠️ volume_ma 계산 실패 또는 0 (MA={volume_ma.iloc[LAST_INDEX] if not volume_ma.empty else 'empty'})")
            volume_ratio = 0
        else:
            volume_ratio = current_volume / volume_ma.iloc[LAST_INDEX]
        
        # ========================================
        # 🆕 디버깅 로그 (상세)
        # ========================================
        ma_val = float(volume_ma.iloc[LAST_INDEX]) if (volume_ma is not None and not volume_ma.empty) else 0.0
        
        logger.debug(
            f"📊 [거래량분석] 현재={current_volume:,.0f} | "
            f"MA={ma_val:,.0f} | "
            f"비율={volume_ratio:.2f}x"
        )
        
        return {
            'surge': volume_surge,
            'accumulation': accumulation,
            'distribution': distribution,
            'volume_ratio': volume_ratio
        }
    
    def calculate_momentum(self, df: pd.DataFrame) -> float:
        """모멘텀 계산"""
        if not is_valid_df(df) or len(df) < DEFAULT_ANALYSIS_WINDOW:
            return 0.0
        
        momentum = (df['close'].iloc[LAST_INDEX] - df['close'].iloc[-DEFAULT_ANALYSIS_WINDOW]) / df['close'].iloc[-DEFAULT_ANALYSIS_WINDOW]
        return momentum
    
    def identify_support_resistance(self, df: pd.DataFrame, lookback: int = None) -> Dict[str, List[float]]:
        """지지/저항선 식별 (SSoT: config 기반)"""
        # config에서 lookback 로드 (fallback: 기존 상수)
        if lookback is None:
            lookback = self.config.getint(
                'SUPPORT_RESISTANCE',
                'lookback_period',
                fallback=SUPPORT_RESISTANCE_LOOKBACK
            )
        
        if not is_valid_df(df) or len(df) < lookback:
            return {'support': [], 'resistance': []}
        
        recent = df.tail(lookback)
        
        highs = []
        lows = []
        
        for i in range(2, len(recent) - 2):
            if (recent['high'].iloc[i] > recent['high'].iloc[i-1] and 
                recent['high'].iloc[i] > recent['high'].iloc[i-2] and
                recent['high'].iloc[i] > recent['high'].iloc[i+1] and 
                recent['high'].iloc[i] > recent['high'].iloc[i+2]):
                highs.append(recent['high'].iloc[i])
            
            if (recent['low'].iloc[i] < recent['low'].iloc[i-1] and 
                recent['low'].iloc[i] < recent['low'].iloc[i-2] and
                recent['low'].iloc[i] < recent['low'].iloc[i+1] and 
                recent['low'].iloc[i] < recent['low'].iloc[i+2]):
                lows.append(recent['low'].iloc[i])
        
        def cluster_levels(levels, threshold=0.01):
            if not levels:
                return []
            
            sorted_levels = sorted(levels)
            clusters = []
            current_cluster = [sorted_levels[0]]
            
            for level in sorted_levels[1:]:
                if level <= current_cluster[-1] * (1 + threshold):
                    current_cluster.append(level)
                else:
                    clusters.append(sum(current_cluster) / len(current_cluster))
                    current_cluster = [level]
            
            clusters.append(sum(current_cluster) / len(current_cluster))
            return clusters
        
        support_levels = cluster_levels(lows)
        resistance_levels = cluster_levels(highs)
        
        current_price = df['close'].iloc[LAST_INDEX]
        
        support = [s for s in support_levels if s < current_price * 1.02][-3:]
        resistance = [r for r in resistance_levels if r > current_price * 0.98][:3]
        
        return {'support': support, 'resistance': resistance}
    
    def _get_regime_cooldown(self) -> int:
        """타임프레임별 체제 전환 최소 간격(초) 반환"""
        tf = None
        try:
            tf = self.timeframe.value if hasattr(self.timeframe, "value") else self.timeframe
        except Exception:
            tf = DEFAULT_TIMEFRAME

        # 타임프레임별 설정 우선
        try:
            cooldown_map = REGIME_STABILIZATION_BY_TIMEFRAME
        except NameError:
            cooldown_map = {}

        if isinstance(tf, str) and tf in cooldown_map:
            v = int(cooldown_map[tf])
            # 3m도 180초 그대로 사용 (60초 상한 제거)
            return v

        # 폴백: 전역 기본값
        try:
            v = int(REGIME_STABILIZATION_SECONDS)
            return v
        except NameError:
            return 600
        
    def _is_regime_change_allowed(self, last_change_time=None, per_symbol: bool = False) -> bool:
        """체제 전환 쿨다운 체크"""
        if per_symbol:
            if last_change_time is None:
                return True
            elapsed = (datetime.now() - last_change_time).total_seconds()
            return elapsed >= self.regime_cooldown_seconds

        if self.regime_last_change_time is None:
            return True

        elapsed = (datetime.now() - self.regime_last_change_time).total_seconds()
        return elapsed >= self.regime_cooldown_seconds
    
    async def get_market_summary(self) -> Dict[str, Any]:
        """시장 요약 정보 - KRW 마켓 전용"""
        try:
            filtered_markets = await self._get_filtered_markets_fast()
            tickers = await self.market_data_service.get_tickers(filtered_markets[:100])
            
            if not tickers:
                return {}
            
            total_volume = sum(float(t.get('acc_trade_price_24h', 0)) for t in tickers)
            avg_change = sum(float(t.get('signed_change_rate', 0)) for t in tickers) / len(tickers)
            
            rising = sum(1 for t in tickers if float(t.get('signed_change_rate', 0)) > 0)
            falling = sum(1 for t in tickers if float(t.get('signed_change_rate', 0)) < 0)
            
            return {
                'timestamp': datetime.now().isoformat(),
                'total_markets': len(filtered_markets),
                'analyzed_markets': len(tickers),
                'total_volume_krw': total_volume,
                'average_change': avg_change,
                'rising_count': rising,
                'falling_count': falling,
                'rising_ratio': rising / len(tickers) if tickers else 0,
                'market_regime': self.market_regime.value,
                'top_markets': self.top_markets_cache[:5] if self.top_markets_cache else []
            }
            
        except Exception as e:
            logger.error(f"시장 요약 오류: {e}")
            return {}
        
# **END**        