#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
업비트 자동매매 봇 v1.0 - Simulation Reality Engine Module
========================================
책임: 시뮬레이션 현실성 구현, 시드 제어, 부분체결 관리, API 장애 모사
- 슬리피지 모델링 (유동성/ATR 기반 동적 스케일링)
- 부분 체결 시뮬레이션 및 미체결 잔량 추적
- 재시도/취소 정책 구현 (IMMEDIATE_RETRY/WAIT_TTL/CANCEL)
- 체결 지연 모델
- 시드 기반 재현성 보장
- 시나리오 프로파일 적용
- API 장애 시뮬레이션

제공 인터페이스:
  - process_order_with_atr(): ATR 기반 개선된 주문 처리
  - handle_partial_fill(): 부분체결 처리 및 재시도/취소 결정
  - simulate_api_failure(): API 장애 시뮬레이션
  - track_order_state(): 주문 상태 추적
  - get_unfilled_orders(): 미체결 주문 조회
  - cancel_order(): 주문 취소
  - get_performance_impact(): 리얼리티 영향 통계
  - set_seed(): 시드 설정
  - apply_scenario(): 시나리오 프로파일 적용
  
금지사항:
  - 호가 단위 조정 직접 구현 (core.snap_to_upbit_tick 사용)
  - 직접적인 거래 실행 (bot 모듈에서 처리)
  - 실제 API 호출 (data 모듈에서 처리)
  - 성과 기록 (performance 모듈에서 처리)
  - 전략 결정 (strategy 모듈에서 처리)
  - 지표 계산 (analysis 모듈에서 처리)
  - 매매 신호 생성 (strategy 모듈에서 처리)
  
데이터 흐름:
  Bot → SimulationRealityEngine (주문 시뮬레이션) → Bot (결과 반환)
  Bot → SimulationRealityEngine (부분체결 처리) → Bot (재시도/취소 결정)
  Bot → SimulationRealityEngine (API 장애) → Bot (장애 처리)
  
개선사항:
  - configparser 메서드 fallback 키워드 인자 오류 수정
  - API 장애 시뮬레이션 완전 구현
  - 장애 유형별 가중치 기반 발생
  - 점검 시간 시뮬레이션
  - 재시도 메커니즘 검증
========================================
"""

import random
import asyncio
import time
import uuid
from datetime import datetime, timedelta
from typing import Dict, Optional, List, Tuple, Any
from dataclasses import dataclass, field
from collections import deque
from enum import Enum
import numpy as np

from Trading_bot_1_core import (
    logger, singleton,
    snap_to_upbit_tick  # core 모듈의 공통 유틸리티 사용
)
from global_constants import *

# ================== Enums ==================
class PartialFillPolicy(Enum):
    """부분체결 정책"""
    IMMEDIATE_RETRY = "immediate_retry"
    WAIT_TTL = "wait_ttl"
    CANCEL = "cancel"
    SPLIT_ORDER = "split_order"
    ADAPTIVE = "adaptive"

class OrderStatus(Enum):
    """주문 상태"""
    PENDING = "pending"
    PARTIAL = "partial"
    FILLED = "filled"
    CANCELLED = "cancelled"
    FAILED = "failed"
    RETRYING = "retrying"

class APIFailureType(Enum):
    """API 장애 유형"""
    CONNECTION_TIMEOUT = "connection_timeout"
    RATE_LIMIT_429 = "rate_limit"  
    SERVER_ERROR_500 = "server_error"
    INVALID_RESPONSE = "invalid_response"
    NETWORK_UNREACHABLE = "network_unreachable"
    MAINTENANCE = "maintenance"

# ================== Data Classes ==================
@dataclass
class OrderState:
    """주문 상태 추적"""
    order_id: str
    symbol: str
    side: str
    original_volume: float
    filled_volume: float = 0.0
    remaining_volume: float = 0.0
    original_price: float = 0.0
    avg_filled_price: float = 0.0
    status: OrderStatus = OrderStatus.PENDING
    retry_count: int = 0
    max_retries: int = PARTIAL_FILL_RETRY_MAX
    created_at: datetime = field(default_factory=datetime.now)
    updated_at: datetime = field(default_factory=datetime.now)
    ttl_seconds: int = 30
    partial_fills: List[Dict] = field(default_factory=list)
    total_slippage: float = 0.0
    total_fees: float = 0.0
    api_failures: List[Dict] = field(default_factory=list)
    
    def __post_init__(self):
        self.remaining_volume = self.original_volume

@dataclass
class SimulationStats:
    """시뮬레이션 통계"""
    total_orders: int = 0
    total_slippage: float = 0.0
    total_partial_fills: int = 0
    total_failed_fills: int = 0
    total_cancelled_orders: int = 0
    total_retries: int = 0
    successful_retries: int = 0
    avg_fill_ratio: float = 100.0
    avg_latency_ms: float = 0.0
    slippage_history: deque = field(default_factory=lambda: deque(maxlen=100))
    fill_ratio_history: deque = field(default_factory=lambda: deque(maxlen=100))
    latency_history: deque = field(default_factory=lambda: deque(maxlen=100))
    partial_fill_outcomes: Dict[str, int] = field(default_factory=dict)
    # API 장애 통계
    api_failures_total: int = 0
    api_failure_types: Dict[str, int] = field(default_factory=dict)
    api_recovery_success: int = 0
    api_recovery_failed: int = 0

@singleton
class SimulationRealityEngine:
    """시뮬레이션 리얼리티 엔진 - API 장애 포함 완전 구현"""
    
    def __init__(self, config):  # 인덴테이션 수정 (클래스 아래 1단계)
        self.config = config
        self.enabled = config.getboolean('SIMULATION_REALITY', 'enable_reality', fallback=False)
        
        # 필수 속성 초기화 (enabled 여부와 관계없이)
        self.seed_value = DEFAULT_SEED_VALUE
        self.stats = SimulationStats()  # 항상 초기화
        self.active_orders = {}
        self.order_history = deque(maxlen=MEMORY_BOUNDS['trade_history'])
        self.order_count = 0
        self.is_maintenance = False
        self.maintenance_end_time = None
        
        # 기본값 설정
        self.preset = REALITY_PRESET_MEDIUM
        self.scenario = SCENARIO_NORMAL
        self.partial_fill_policy = PartialFillPolicy.ADAPTIVE
        self.use_dynamic_slippage = True
        self.slippage_atr_weight = 0.7
        self.slippage_liquidity_weight = 0.3
        self.api_failure_enabled = False
        self.api_failure_rate = 0.001
        self.failure_weights = {}
        self.maintenance_simulation = False
        self.maintenance_start_hour = 3
        self.maintenance_duration = 30
        
        # 프리셋 기본값
        self.slippage_enabled = False
        self.slippage_base = 0.05
        self.partial_fill_enabled = False
        self.fill_ratio_base = 95
        self.latency_enabled = False
        self.latency_min = 50
        self.latency_max = 200
        self.apply_tick_size = True
        
        # 시나리오 배수 기본값
        self.scenario_multipliers = SCENARIO_MULTIPLIERS.get('normal')
        
        if not self.enabled:
            logger.info("시뮬레이션 리얼리티 비활성화")
            return
        
        # enabled=True일 때만 설정값 로드
        self._initialize_seed()
        
        # 프리셋 및 시나리오
        self.preset = config.get('SIMULATION_REALITY', 'reality_preset', fallback=REALITY_PRESET_MEDIUM)
        self._load_preset()
        self.scenario = config.get('SIMULATION_REALITY', 'scenario_profile', fallback=SCENARIO_NORMAL)
        self._load_scenario()
        
        # 설정값 로드
        self._load_configurations()
        
        # 부분체결 정책
        policy_name = config.get('SIMULATION_REALITY', 'partial_fill_policy', fallback='ADAPTIVE')
        self.partial_fill_policy = PartialFillPolicy[policy_name]
        
        # 동적 슬리피지
        self.use_dynamic_slippage = config.getboolean('SIMULATION_REALITY', 'use_dynamic_slippage', fallback=True)
        self.slippage_atr_weight = config.getfloat('SIMULATION_REALITY', 'slippage_atr_weight', fallback=0.7)
        self.slippage_liquidity_weight = config.getfloat('SIMULATION_REALITY', 'slippage_liquidity_weight', fallback=0.3)
        
        # API 장애 시뮬레이션 설정
        self._load_api_failure_config()
        
        logger.info(f"리얼리티 엔진 초기화 (preset: {self.preset}, seed: {self.seed_value}, API 장애: {self.api_failure_enabled})")
    
    def _initialize_seed(self):  # 이 메서드도 클래스 아래 1단계 인덴테이션
        """시드 초기화"""
        if not self.config.getboolean('SIMULATION_REALITY', 'use_seed', fallback=True):
            self.seed_value = None
            return
        
        seed_mode = self.config.get('SIMULATION_REALITY', 'seed_mode', fallback=SEED_MODE_FIXED)
        seed_value = self.config.getint('SIMULATION_REALITY', 'seed_value', fallback=DEFAULT_SEED_VALUE)
        
        if seed_mode == SEED_MODE_FIXED:
            self.seed_value = seed_value if seed_value > 0 else DEFAULT_SEED_VALUE
        elif seed_mode == SEED_MODE_DAILY:
            self.seed_value = int(datetime.now().strftime(SEED_DATE_FORMAT))
        else:  # RANDOM
            self.seed_value = int(time.time() * 1000) % 2**32
        
        random.seed(self.seed_value)
        np.random.seed(self.seed_value)
    
    def _load_api_failure_config(self):
        """API 장애 시뮬레이션 설정 로드 - 개선사항: fallback 키워드 인자"""
        self.api_failure_enabled = self.config.getboolean('API_FAILURE_SIMULATION', 'enabled', fallback=False)
        self.api_failure_rate = self.config.getfloat('API_FAILURE_SIMULATION', 'failure_rate', fallback=0.001)
        
        # 장애 유형별 가중치
        self.failure_weights = {
            APIFailureType.CONNECTION_TIMEOUT: self.config.getfloat('API_FAILURE_SIMULATION', 
                'connection_timeout_weight', fallback=0.3),
            APIFailureType.RATE_LIMIT_429: self.config.getfloat('API_FAILURE_SIMULATION',
                'rate_limit_weight', fallback=0.3),
            APIFailureType.SERVER_ERROR_500: self.config.getfloat('API_FAILURE_SIMULATION',
                'server_error_weight', fallback=0.2),
            APIFailureType.INVALID_RESPONSE: self.config.getfloat('API_FAILURE_SIMULATION',
                'invalid_response_weight', fallback=0.1),
            APIFailureType.NETWORK_UNREACHABLE: self.config.getfloat('API_FAILURE_SIMULATION',
                'network_unreachable_weight', fallback=0.05),
            APIFailureType.MAINTENANCE: self.config.getfloat('API_FAILURE_SIMULATION',
                'maintenance_weight', fallback=0.05)
        }
        
        # 점검 시간 설정
        self.maintenance_simulation = self.config.getboolean('API_FAILURE_SIMULATION', 
            'maintenance_window_simulation', fallback=False)
        self.maintenance_start_hour = self.config.getint('API_FAILURE_SIMULATION',
            'maintenance_start_hour', fallback=3)
        self.maintenance_duration = self.config.getint('API_FAILURE_SIMULATION',
            'maintenance_duration_minutes', fallback=30)
    
    def _load_scenario(self):
        """시나리오 프로파일 로드"""
        self.scenario_multipliers = SCENARIO_MULTIPLIERS.get(
            self.scenario, SCENARIO_MULTIPLIERS['normal']
        )
    
    def _load_preset(self):
        """프리셋 적용"""
        presets = {
            REALITY_PRESET_NONE: {'slippage': False, 'partial': False, 'latency': False},
            REALITY_PRESET_LOW: {'slippage': True, 'partial': False, 'latency': False, 
                                'slippage_base': 0.03, 'fill_ratio': 98, 'latency_range': (30, 100)},
            REALITY_PRESET_MEDIUM: {'slippage': True, 'partial': True, 'latency': True,
                                   'slippage_base': 0.05, 'fill_ratio': 95, 'latency_range': (50, 200)},
            REALITY_PRESET_HIGH: {'slippage': True, 'partial': True, 'latency': True,
                                 'slippage_base': 0.08, 'fill_ratio': 90, 'latency_range': (100, 500)},
            REALITY_PRESET_EXTREME: {'slippage': True, 'partial': True, 'latency': True,
                                    'slippage_base': 0.15, 'fill_ratio': 80, 'latency_range': (200, 1000)}
        }
        self.preset_config = presets.get(self.preset, presets[REALITY_PRESET_MEDIUM])
    
    def _load_configurations(self):
        """설정값 로드 - 개선사항: fallback 키워드 인자"""
        self.slippage_enabled = self.preset_config.get('slippage', True)
        self.slippage_base = self.preset_config.get('slippage_base', 0.05)
        self.partial_fill_enabled = self.preset_config.get('partial', True)
        self.fill_ratio_base = self.preset_config.get('fill_ratio', 95)
        self.latency_enabled = self.preset_config.get('latency', True)
        latency_range = self.preset_config.get('latency_range', (50, 200))
        self.latency_min, self.latency_max = latency_range
        self.apply_tick_size = self.config.getboolean('SIMULATION_REALITY', 'apply_tick_size', fallback=True)
    
    async def simulate_api_failure(self) -> Optional[Dict]:
        """API 장애 시뮬레이션"""
        if not self.api_failure_enabled:
            return None
        
        # 점검 시간 체크
        if self.maintenance_simulation:
            now = datetime.now()
            if now.hour == self.maintenance_start_hour and not self.is_maintenance:
                self.is_maintenance = True
                self.maintenance_end_time = now + timedelta(minutes=self.maintenance_duration)
                logger.warning(f"API 점검 시작 - {self.maintenance_duration}분간 지속")
                return {
                    'failure_type': APIFailureType.MAINTENANCE,
                    'retry_after': self.maintenance_duration * SECONDS_PER_MINUTE,
                    'message': "System maintenance in progress"
                }
            
            if self.is_maintenance:
                if datetime.now() < self.maintenance_end_time:
                    remaining = (self.maintenance_end_time - datetime.now()).seconds
                    return {
                        'failure_type': APIFailureType.MAINTENANCE,
                        'retry_after': remaining,
                        'message': f"Maintenance ends in {remaining}s"
                    }
                else:
                    self.is_maintenance = False
                    logger.info("API 점검 종료")
        
        # 확률적 장애 발생
        if random.random() > self.api_failure_rate:
            return None
        
        # 가중치 기반 장애 유형 선택
        failure_types = list(self.failure_weights.keys())
        weights = list(self.failure_weights.values())
        failure_type = random.choices(failure_types, weights=weights)[0]
        
        self.stats.api_failures_total += 1
        self.stats.api_failure_types[failure_type.value] = \
            self.stats.api_failure_types.get(failure_type.value, 0) + 1
        
        logger.debug(f"API 장애 시뮬레이션: {failure_type.value}")
        
        # 장애 유형별 응답
        if failure_type == APIFailureType.CONNECTION_TIMEOUT:
            await asyncio.sleep(30)  # 30초 타임아웃
            raise asyncio.TimeoutError("API connection timeout")
            
        elif failure_type == APIFailureType.RATE_LIMIT_429:
            return {
                'failure_type': failure_type,
                'status': HTTP_TOO_MANY_REQUESTS,
                'retry_after': random.randint(30, 120),
                'message': "Rate limit exceeded"
            }
            
        elif failure_type == APIFailureType.SERVER_ERROR_500:
            return {
                'failure_type': failure_type,
                'status': HTTP_SERVER_ERRORS[0],
                'message': "Internal server error",
                'should_retry': True
            }
            
        elif failure_type == APIFailureType.INVALID_RESPONSE:
            return {
                'failure_type': failure_type,
                'status': HTTP_SUCCESS,
                'data': {'corrupted': True},
                'message': "Invalid response format"
            }
            
        elif failure_type == APIFailureType.NETWORK_UNREACHABLE:
            raise ConnectionError("Network unreachable")
            
        return None
    
    async def process_order_with_atr(self, symbol: str, side: str, price: float, volume: float,
                                    volume_rank: int = 50, atr: float = 0, current_price: float = None,
                                    market_regime: str = "RANGING", timeframe: str = "5m",
                                    order_id: str = None) -> Dict:
        """ATR 기반 주문 처리 - API 장애 포함"""
        
        # 디버깅: 입력 값 타입 확인 - 임시 로직
        logger.debug(f"ATR 타입: {type(atr)}, 값: {atr}")
        
        if not self.enabled:
            return self._create_simple_result(price, volume)
        
        # ATR이 Series인 경우 스칼라로 변환
        if hasattr(atr, 'iloc'):  # pandas Series 체크
            atr = float(atr.iloc[-1]) if len(atr) > 0 else 0.0
        elif hasattr(atr, '__iter__') and not isinstance(atr, (str, bytes)):
            # 리스트나 배열인 경우
            atr = float(atr[-1]) if len(atr) > 0 else 0.0
        else:
            # 스칼라 값인 경우
            atr = float(atr) if atr else 0.0
        
        # API 장애 체크
        try:
            api_failure = await self.simulate_api_failure()
            if api_failure:
                if api_failure.get('failure_type') == APIFailureType.MAINTENANCE:
                    return {
                        'status': 'failed',
                        'filled_price': 0,
                        'filled_volume': 0,
                        'remaining_volume': volume,
                        'slippage': 0,
                        'fill_ratio': 0,
                        'latency_ms': 0,
                        'api_failure': api_failure
                    }
                elif api_failure.get('status') == HTTP_TOO_MANY_REQUESTS:
                    # Rate limit - 재시도 필요
                    await asyncio.sleep(api_failure['retry_after'])
                    self.stats.api_recovery_success += 1
        except (asyncio.TimeoutError, ConnectionError) as e:
            self.stats.api_recovery_failed += 1
            return {
                'status': 'failed',
                'filled_price': 0,
                'filled_volume': 0,
                'remaining_volume': volume,
                'slippage': 0,
                'fill_ratio': 0,
                'latency_ms': 0,
                'error': str(e)
            }
        
        # 주문 ID 생성
        if not order_id:
            order_id = f"{side}_{symbol}_{int(time.time()*1000)}_{uuid.uuid4().hex[:8]}"
        
        # 주문 상태 생성/조회
        if order_id not in self.active_orders:
            order_state = OrderState(
                order_id=order_id,
                symbol=symbol,
                side=side,
                original_volume=volume,
                original_price=price,
                ttl_seconds=30 if timeframe in ['1m', '3m', '5m'] else 60
            )
            self.active_orders[order_id] = order_state
        else:
            order_state = self.active_orders[order_id]
        
        # 주문 처리
        result = await self._process_single_fill(
            order_state, price, order_state.remaining_volume,
            volume_rank, atr, current_price or price, market_regime, timeframe
        )
        
        # 부분체결 처리
        if result['status'] == 'partial':
            result = await self.handle_partial_fill(order_state, result, market_regime)
        
        # 완료된 주문 정리
        if order_state.status in [OrderStatus.FILLED, OrderStatus.CANCELLED, OrderStatus.FAILED]:
            self.order_history.append(order_state)
            del self.active_orders[order_id]
        
        return result
    
    async def _process_single_fill(self, order_state: OrderState, price: float, volume: float,
                                  volume_rank: int, atr: float, current_price: float,
                                  market_regime: str, timeframe: str) -> Dict:
        """단일 체결 시도 (개선 버전)"""
        self.order_count += 1
        
        # ============================================================
        # 🧮 현실적 슬리피지 모델 (2025-11-09)
        # ============================================================
        # 체결 지연
        latency = await self._simulate_latency(timeframe)
        
        # 참여율 기반 가격 임팩트 (유동성 대비 주문 비중)
        # 실제 호가창이 없으므로 volume_rank로 추정
        # 🔒 [단위 버그 수정 2026-08-02] volume 은 '코인 개수'인데 KRW 스케일 추정치로
        #  나누고 있었다. 저가 코인(XEC 0.0097원 = 1건에 1억 개)은 participation 이
        #  항상 1.0 으로 포화되어 impact 0.12%(최대)가 무조건 붙었다.
        #  실측: XEC 08-02 09:21 진입 슬리피지 0.181% / 09:22 청산 0.178% (왕복≈0.36%)
        #        vs OPEN 08:21 진입 0.042% / 09:02 청산 0.072% (왕복 0.114%)
        #        — 저가코인일수록 슬리피지 확대 패턴 로그 실측.
        #  같은 파일 L674 는 order_value = price * volume 으로 이미 원 환산을 한다(내부 불일치).
        order_value_krw = price * volume
        # 🔒 [2026-08-02] 유동성 추정 기준 100만원 → 1억원.
        #  봇 주문이 회당 약 100만원인데 기준이 100만원이라
        #  liquidity_estimate(1만~50만원)보다 항상 커서 participation 이 전 종목 1.0 포화,
        #  impact 0.12%(최대)가 모든 체결에 상시 부과됐다(왕복 0.24%).
        #  검산: XEC 24h 거래대금 70억 → 3분봉당 약 1,460만원, 100만원 주문 = 6.8%.
        #        1억 기준이 주는 0.13과 같은 자릿수. 100만원 기준의 1.0은 비현실적.
        #  ※ 정석은 acc24h(실거래대금)를 직접 쓰는 것이고 이건 그때까지의 근사.
        liquidity_estimate = 100000000 / (volume_rank + 1)
        participation = min(order_value_krw / max(1e-9, liquidity_estimate), 1.0)
        
        # 슬리피지 계산
        if self.use_dynamic_slippage and atr > 0:
            base_slippage = self._calculate_dynamic_slippage(
                price, volume, volume_rank, atr, current_price, order_state.side
            )
            
            # 마이크로 임팩트 추가 (±0.02% 랜덤)
            import random
            micro = 0.0002 * (1 if random.random() > 0.5 else -1)
            
            # 유동성 임팩트 (최대 0.12%)
            impact = 0.0012 * participation
            
            # 최종 슬리피지
            slippage_pct = base_slippage + (micro + impact) * 100
        else:
            slippage_pct = self._calculate_slippage(price, volume, volume_rank, order_state.side)
        
        # 체결가 계산
        if order_state.side == 'buy':
            filled_price = price * (1 + slippage_pct / PERCENT_CONVERSION)
        else:
            filled_price = price * (1 - slippage_pct / PERCENT_CONVERSION)
        
        if self.apply_tick_size:
            # 🔧 2026-08-01: 구 legacy 유틸은 호가 스냅이 아닌 단순 6자리 반올림이었다.
            #  → 실제 업비트 호가 단위로 스냅해야 진입가가 봉 고가 위로 뜨지 않는다.
            filled_price = snap_to_upbit_tick(filled_price, "round")
        
        # 체결률 계산
        fill_ratio = self._calculate_fill_ratio(volume, volume_rank, market_regime)
        filled_volume = volume if fill_ratio == PERCENT_CONVERSION else volume * fill_ratio / PERCENT_CONVERSION  # [SIMDUST] 100% 체결은 원래 수량 유지
        remaining = volume - filled_volume
        
        # 주문 상태 업데이트
        order_state.filled_volume += filled_volume
        order_state.remaining_volume = order_state.original_volume - order_state.filled_volume
        order_state.avg_filled_price = ((order_state.avg_filled_price * (order_state.filled_volume - filled_volume) + 
                                        filled_price * filled_volume) / order_state.filled_volume 
                                       if order_state.filled_volume > 0 else filled_price)
        order_state.total_slippage += slippage_pct
        order_state.updated_at = datetime.now()
        
        # 체결 기록
        order_state.partial_fills.append({
            'timestamp': datetime.now(),
            'price': filled_price,
            'volume': filled_volume,
            'slippage': slippage_pct,
            'fill_ratio': fill_ratio
        })
        
        # 상태 결정
        if fill_ratio < PARTIAL_FILL_MIN_RATIO:
            status = 'failed'
            order_state.status = OrderStatus.FAILED
        elif order_state.remaining_volume < order_state.original_volume * PERCENT_TO_RATIO:  # 99% 체결
            status = 'filled'
            order_state.status = OrderStatus.FILLED
        else:
            status = 'partial'
            order_state.status = OrderStatus.PARTIAL
        
        # 통계 업데이트
        self._update_statistics(slippage_pct, fill_ratio, latency)
        
        return {
            'status': status,
            'filled_price': order_state.avg_filled_price,
            'filled_volume': order_state.filled_volume,
            'remaining_volume': order_state.remaining_volume,
            'slippage': slippage_pct,
            'fill_ratio': fill_ratio,
            'latency_ms': latency,
            'order_state': order_state,
            'retry_needed': status == 'partial'
        }
    
    async def handle_partial_fill(self, order_state: OrderState, result: Dict, 
                                 market_regime: str) -> Dict:
        """부분체결 처리"""
        # TTL 체크
        if (datetime.now() - order_state.created_at).seconds > order_state.ttl_seconds:
            return await self.cancel_order(order_state, "TTL_EXPIRED")
        
        # 재시도 한계 체크
        if order_state.retry_count >= order_state.max_retries:
            return await self.cancel_order(order_state, "MAX_RETRIES_EXCEEDED")
        
        # 정책별 처리
        if self.partial_fill_policy == PartialFillPolicy.IMMEDIATE_RETRY:
            return await self._retry_immediately(order_state, result)
        elif self.partial_fill_policy == PartialFillPolicy.WAIT_TTL:
            return await self._wait_and_retry(order_state, result)
        elif self.partial_fill_policy == PartialFillPolicy.CANCEL:
            if order_state.filled_volume / order_state.original_volume < 0.9:
                return await self.cancel_order(order_state, "PARTIAL_FILL_POLICY")
            return result
        elif self.partial_fill_policy == PartialFillPolicy.SPLIT_ORDER:
            return await self._split_order(order_state, result)
        else:  # ADAPTIVE
            return await self._adaptive_policy(order_state, result, market_regime)
    
    async def _retry_immediately(self, order_state: OrderState, result: Dict) -> Dict:
        """즉시 재시도"""
        order_state.retry_count += 1
        order_state.status = OrderStatus.RETRYING
        self.stats.total_retries += 1
        
        retry_price = order_state.original_price * (1.001 if order_state.side == 'buy' else 0.999)
        
        retry_result = await self._process_single_fill(
            order_state, retry_price, order_state.remaining_volume,
            50, 0, retry_price, "RANGING", "5m"
        )
        
        if retry_result['status'] == 'filled':
            self.stats.successful_retries += 1
        
        return retry_result
    
    async def _wait_and_retry(self, order_state: OrderState, result: Dict) -> Dict:
        """대기 후 재시도"""
        await asyncio.sleep(PARTIAL_FILL_RETRY_INTERVAL)
        return await self._retry_immediately(order_state, result)
    
    async def _split_order(self, order_state: OrderState, result: Dict) -> Dict:
        """분할 주문"""
        if order_state.remaining_volume > order_state.original_volume * 0.1:
            split_volume = order_state.remaining_volume / 3
            order_state.remaining_volume = split_volume
            result['status'] = 'partial'
            result['retry_needed'] = True
        return result
    
    async def _adaptive_policy(self, order_state: OrderState, result: Dict, 
                              market_regime: str) -> Dict:
        """적응형 정책"""
        fill_pct = order_state.filled_volume / order_state.original_volume
        
        if fill_pct >= 0.9:
            return await self.cancel_order(order_state, "90PCT_FILLED")
        
        if market_regime in ["TRENDING_UP", "TRENDING_DOWN"]:
            return await self._retry_immediately(order_state, result)
        elif fill_pct < MAJORITY_THRESHOLD:
            return await self._split_order(order_state, result)
        else:
            return await self._wait_and_retry(order_state, result)
    
    async def cancel_order(self, order_state: OrderState, reason: str) -> Dict:
        """주문 취소"""
        order_state.status = OrderStatus.CANCELLED
        self.stats.total_cancelled_orders += 1
        
        outcome_key = f"{order_state.filled_volume/order_state.original_volume:.1%}_cancelled"
        self.stats.partial_fill_outcomes[outcome_key] = \
            self.stats.partial_fill_outcomes.get(outcome_key, 0) + 1
        
        return {
            'status': 'cancelled',
            'filled_price': order_state.avg_filled_price if order_state.filled_volume > 0 else 0,
            'filled_volume': order_state.filled_volume,
            'remaining_volume': order_state.remaining_volume,
            'slippage': order_state.total_slippage / len(order_state.partial_fills) if order_state.partial_fills else 0,
            'fill_ratio': order_state.filled_volume / order_state.original_volume * PERCENT_CONVERSION,
            'latency_ms': 0,
            'order_state': order_state,
            'cancel_reason': reason
        }
    
    def _calculate_dynamic_slippage(self, price: float, volume: float, volume_rank: int,
                                   atr: float, current_price: float, side: str) -> float:
        """동적 슬리피지 계산"""
        if not self.slippage_enabled:
            return 0.0
        
        base = self.slippage_base
        
        # 유동성 팩터
        liquidity_factor = 1 + (volume_rank - 1) / 100
        liquidity_factor = min(liquidity_factor, 2.5)
        
        # ATR 변동성 팩터
        if current_price > 0 and atr > 0:
            atr_ratio = atr / current_price
        else:
            atr_ratio = PERCENT_TO_RATIO
        
        volatility_factor = 1 + (atr_ratio ** 0.7) * 10
        volatility_factor = min(volatility_factor, self.config.getfloat('SIMULATION_REALITY', 
            'slippage_max_multiplier', fallback=3.0))
        
        # 주문 영향도
        order_value = price * volume
        order_impact = 1.5 if order_value > SLIPPAGE_LARGE_ORDER_THRESHOLD * 5 else \
                      1.2 if order_value > SLIPPAGE_LARGE_ORDER_THRESHOLD else 1.0
        
        # 방향성 조정
        direction_mult = 1.1 if side == 'buy' else 0.9
        
        # 가중 평균
        weighted_factor = (liquidity_factor * self.slippage_liquidity_weight +
                         volatility_factor * self.slippage_atr_weight)
        
        # 최종 계산
        slippage = base * weighted_factor * order_impact * direction_mult * \
                  self.scenario_multipliers['slippage']
        slippage *= random.uniform(0.9, 1.1)
        
        return min(max(slippage, SLIPPAGE_MIN_PCT), SLIPPAGE_MAX_PCT)
    
    def _calculate_slippage(self, price: float, volume: float, volume_rank: int, side: str) -> float:
        """기본 슬리피지 계산"""
        if not self.slippage_enabled:
            return 0.0
        
        base = self.slippage_base
        volume_mult = 2.0 if volume_rank > 50 else 1.5 if volume_rank > 30 else 1.0 if volume_rank > 10 else 0.5
        order_mult = 1.5 if price * volume > SLIPPAGE_LARGE_ORDER_THRESHOLD else 1.0
        
        return min(max(base * volume_mult * order_mult * self.scenario_multipliers['slippage'] * 
                      random.uniform(0.8, 1.2), SLIPPAGE_MIN_PCT), SLIPPAGE_MAX_PCT)
    
    def _calculate_fill_ratio(self, volume: float, volume_rank: int, market_regime: str) -> float:
        """체결률 계산"""
        if not self.partial_fill_enabled:
            return PARTIAL_FILL_MAX_RATIO
        
        base_ratio = 98 if volume_rank <= 10 else 95 if volume_rank <= 30 else \
                    90 if volume_rank <= 50 else 85
        
        if market_regime == "TRENDING_UP":
            base_ratio += 5
        elif market_regime == "TRENDING_DOWN":
            base_ratio -= 5
        
        base_ratio *= self.scenario_multipliers['fill_ratio']
        
        return min(max(base_ratio + random.uniform(-5, 5), PARTIAL_FILL_MIN_RATIO), PARTIAL_FILL_MAX_RATIO)
    
    async def _simulate_latency(self, timeframe: str) -> float:
        """체결 지연 시뮬레이션"""
        if not self.latency_enabled:
            return 0.0
        
        base_latency = random.uniform(self.latency_min, self.latency_max)
        importance = 2.0 if timeframe in ['1m', '3m'] else 1.5 if timeframe in ['5m', '10m', '15m'] else 1.0
        latency = base_latency * importance * self.scenario_multipliers['latency']
        
        if latency > 0:
            await asyncio.sleep(latency / 1000)
        
        return min(latency, LATENCY_MAX_MS)
    
    def _update_statistics(self, slippage: float, fill_ratio: float, latency: float):
        """통계 업데이트"""
        self.stats.total_orders += 1
        self.stats.slippage_history.append(slippage)
        self.stats.fill_ratio_history.append(fill_ratio)
        self.stats.latency_history.append(latency)
        
        if slippage > 0:
            self.stats.total_slippage += slippage
        if fill_ratio < PARTIAL_FILL_MAX_RATIO:
            self.stats.total_partial_fills += 1
        if fill_ratio < PARTIAL_FILL_MIN_RATIO:
            self.stats.total_failed_fills += 1
        
        if self.stats.fill_ratio_history:
            self.stats.avg_fill_ratio = sum(self.stats.fill_ratio_history) / len(self.stats.fill_ratio_history)
        if self.stats.latency_history:
            self.stats.avg_latency_ms = sum(self.stats.latency_history) / len(self.stats.latency_history)
    
    def _create_simple_result(self, price: float, volume: float) -> Dict:
        """단순 결과 생성 (리얼리티 비활성화시)"""
        return {
            'status': 'filled',
            'filled_price': price,
            'filled_volume': volume,
            'remaining_volume': 0,
            'slippage': 0,
            'fill_ratio': 100,
            'latency_ms': 0
        }
    
    def get_performance_impact(self) -> Dict:
        """리얼리티 영향 통계"""
        if self.stats.total_orders == 0:
            return {
                'enabled': self.enabled,
                'preset': self.preset,
                'scenario': self.scenario,
                'seed': self.seed_value,
                'total_orders': 0,
                'avg_slippage_pct': 0,  # 추가
                'avg_fill_ratio': 100,   # 추가
                'partial_fill_rate': 0,  # 추가
                'failed_fill_rate': 0,   # 추가
                'cancelled_rate': 0,     # 추가
                'retry_success_rate': 0, # 추가
                'avg_latency_ms': 0      # 추가
            }
           
        avg_slippage = sum(self.stats.slippage_history) / len(self.stats.slippage_history) \
                      if self.stats.slippage_history else 0
        
        return {
            'enabled': self.enabled,
            'preset': self.preset,
            'scenario': self.scenario,
            'seed': self.seed_value,
            'partial_fill_policy': self.partial_fill_policy.value,
            'dynamic_slippage': self.use_dynamic_slippage,
            'total_orders': self.stats.total_orders,
            'avg_slippage_pct': avg_slippage,
            'avg_fill_ratio': self.stats.avg_fill_ratio,
            'partial_fill_rate': (self.stats.total_partial_fills / self.stats.total_orders * PERCENT_CONVERSION),
            'failed_fill_rate': (self.stats.total_failed_fills / self.stats.total_orders * PERCENT_CONVERSION),
            'cancelled_rate': (self.stats.total_cancelled_orders / self.stats.total_orders * PERCENT_CONVERSION) \
                            if self.stats.total_orders > 0 else 0,
            'retry_success_rate': (self.stats.successful_retries / self.stats.total_retries * PERCENT_CONVERSION) \
                                if self.stats.total_retries > 0 else 0,
            'avg_latency_ms': self.stats.avg_latency_ms,
            'partial_fill_outcomes': dict(self.stats.partial_fill_outcomes),
            'api_failures': {
                'total': self.stats.api_failures_total,
                'types': dict(self.stats.api_failure_types),
                'recovery_rate': (self.stats.api_recovery_success / 
                                 (self.stats.api_recovery_success + self.stats.api_recovery_failed) * PERCENT_CONVERSION) \
                               if (self.stats.api_recovery_success + self.stats.api_recovery_failed) > 0 else 0
            }
        }
    
    def export_statistics(self) -> Dict:
        """상세 통계 내보내기"""
        return {
            'summary': self.get_performance_impact(),
            'active_orders': len(self.active_orders),
            'unfilled_volume': sum(o.remaining_volume for o in self.active_orders.values()),
            'detailed': {
                'slippage_distribution': list(self.stats.slippage_history),
                'fill_ratio_distribution': list(self.stats.fill_ratio_history),
                'latency_distribution': list(self.stats.latency_history),
                'partial_fill_outcomes': dict(self.stats.partial_fill_outcomes),
                'api_failure_types': dict(self.stats.api_failure_types)
            }
        }