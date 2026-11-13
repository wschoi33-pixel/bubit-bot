#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
업비트 자동매매 봇 v1.0 - Bot Module (통합 실행 엔진)
========================================
책임: 거래 사이클 관리, 주문 조정, 실행 엔진 활용, 원자적 청산
- 거래 사이클 오케스트레이션
- 실행 엔진을 통한 모드 무관 주문 처리
- 포지션 라이프사이클 관리
- 원자적 킬스위치 청산 보장
- 15분봉 MA 이탈 실시간 체크 (단기 타임프레임 전용)
- 일일 요약 텍스트 리포트 생성

제공 인터페이스:
  - run(): 메인 실행 루프
  - execute_trade(): 통합 거래 실행
  - emergency_liquidation(): 원자적 긴급 청산
  - graceful_shutdown(): 정상 종료
  
금지사항:
  - 직접적인 API 호출 (ExecutionEngine 통해서만)
  - 지표 계산 (analysis 모듈에서 처리)
  - 신호 생성 (strategy 모듈에서 처리)
  - 성과 분석 직접 수행 (performance 모듈에서 처리)
  - 데이터 직접 조회 (MarketDataService 통해서만)
  
데이터 흐름:
  Bot → ExecutionEngine → API/Simulation → Bot
  Bot → MarketDataService → Data
  
개선사항:
  - 초기화 단계 간소화
  - 중복 로직 제거
  - 과도한 로깅 정리
  - 명확한 책임 분리
========================================
"""

import asyncio
import os
import pandas as pd
import time
import psutil  # 🆕 메모리 모니터링 추가

from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, Optional, Set
from collections import defaultdict
from Trading_bot_1_core import (
    logger, Position, TradingSignal, ExecutionResult,
    MarketRegime, TimeFrame, TradingSettings,
    TimerManager, singleton, is_valid_df,
    DataPackage, format_price_krw
)
from Trading_bot_2_config import ConfigManager
from Trading_bot_3_performance import PerformanceManager
from Trading_bot_4_data import (
    UpbitAPIClient, WebSocketManager, DataManager, MarketDataService
)
from Trading_bot_5_analysis import MarketAnalyzer, TechnicalIndicators, SoleusGajamiConstants
from Trading_bot_6_strategy import SignalAnalyzer, StrategyType, safe_extract_atr
from Trading_bot_9_risk import RiskManager
from Trading_bot_10_portfolio_risk import PortfolioRiskManager
from Trading_bot_11_execution_engine import (
    ExecutionEngineFactory, OrderRequest
)
from global_constants import *

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 📌 기본 상수 정의
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
DEFAULT_TIMEOUT_HOURS = 24.0  # 포지션 타임아웃 기본값: 24시간

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 🛡️ SafeLogger 보호: WinError 233 완전 차단 (진입체크 로그 보호)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# ✅ logger는 Analysis에서 이미 SafeLogger로 래핑됨
# Bot에서는 래핑된 logger를 그대로 사용 (이중 래핑 방지)
# (Trading_bot_5_analysis에서 logger가 SafeLogger로 래핑되어 import됨)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def _real_remaining_q8(held, executed):
    """[보완 v3_4 G6] REAL 잔량 = 보유 − 체결 을 거래소 수량 단위 (1e-8 · 반올림 짝수) 로 · 전량 판정 · 잔량 · 0 판정 공통
    한계 : float 로 이미 합쳐진 값 (예 1.3e8 단위 수량의 1e-8 차이) 은 복구 불가 → 양쪽이 같은 값으로 보이면 0 (봇 수량 = 거래소 대조는 BALANCE-MISMATCH 경보가 감시)"""
    from decimal import Decimal, ROUND_HALF_EVEN
    return (Decimal(str(float(held))) - Decimal(str(float(executed)))).quantize(Decimal('1e-8'), rounding=ROUND_HALF_EVEN)


def _q8(x, rounding="down"):
    """[보완 v3_4] REAL 수량을 거래소 단위 (1e-8) Decimal 로 · down = 전송량 (내림) · even = 체결 합산 복원 (반올림 짝수)"""
    from decimal import Decimal, ROUND_DOWN, ROUND_HALF_EVEN
    return Decimal(str(float(x))).quantize(Decimal('1e-8'), rounding=ROUND_DOWN if rounding == "down" else ROUND_HALF_EVEN)


def _tp_target_q8(original_volume, ratio):
    """[보완 v3_6 ⑤] TP 단계 목표 = 원수량 × 비율 을 Decimal 로 곱한 뒤 8 자리 내림 (float 곱 3 × 0.3 = 0.8999999999999999 → 0.89999999 방지) · 정상 · 복구 공통"""
    from decimal import Decimal, ROUND_DOWN
    return (Decimal(str(float(original_volume))) * Decimal(str(float(ratio)))).quantize(Decimal('1e-8'), rounding=ROUND_DOWN)


@singleton
class UnifiedTradingBot:
    """통합 거래 봇 - 싱글톤"""
    
    def __init__(self, config: ConfigManager):
        """초기화"""
        # ========================================
        # 1. 기본 설정 (가장 먼저!)
        # ========================================
        self.config = config
        self.mode = config.get_cached('mode')
        self.timeframe = TimeFrame.from_string(config.get_cached('timeframe'))
        self.is_running = False
        self.positions = {}
        self._positions_lock = asyncio.Lock()
        
        # ========================================
        # 2. 통계 변수
        # ========================================
        self._cycle_count = 0
        self.daily_trades = []
        self.daily_net_pnl = 0
        self.daily_reset_time = datetime.now().replace(hour=0, minute=0, second=0)
        self.consecutive_errors = 0
        self.top_markets = []
        self._force_entry_check = False  # 즉시 체크 플래그
        self._priority_recheck = {}       # 🆕 우선 재체크 큐: {symbol: remaining_checks}
        self._bar_open_recheck_due: Dict[str, float] = {}
        self._gc_birth_ttl_last_candle = {}  # 🌱 WS 미구독 GC-BIRTH TTL 1봉 1회 소비용: {symbol: candle_key}
        self._late_entry_decay_lock = {}  # 🔒 Late-Entry 차단 후 하락 재진입 금지 만료시각
        self._max_ma_cache = 100  # 최대 100개 종목만 캐싱
        self.ma15_cache_time = {}
        self.ma15_cache = {}  # MA15 캐시 (일일 리셋용)
        self._max_strategy_perf = 20  # 최대 20개만 추적
        self._api_throttle_count = 0  # API 429 에러 카운터 (체크 주기 자동 조정용)
        
        # ========================================
        # 🆕 과소 수량 쿨다운 (운영 보호)
        # ========================================
        self.min_qty_rejection_time = {}  # {symbol: timestamp}
        self.min_qty_rejection_reason = {}  # {symbol: reason}
        
        # ========================================
        # 🆕 WebSocket 캔들 확정 하이브리드 모드 (Polling 보조)
        # ========================================
        self._candle_close_event: Optional[asyncio.Event] = None
        self._ws_candle_task: Optional[asyncio.Task] = None
        self._realtime_hardstop_task: Optional[asyncio.Task] = None
        
        # ========================================
        # 🆕 Intrabar OHLC 반영용 DF refresh 상태 (타임프레임 무관)
        # - force_refresh=True를 매번 쓰면 WS intrabar 보정이 무의미해짐
        # - 심볼별로 "캔들 경계"에서만 refresh, 그 외는 캐시/인플레이스 보정
        # ========================================
        self._df_refresh_ts_by_symbol: Dict[str, float] = {}
        # 🔒 [CACHE-MAX-GAP-REST-0822-R3] REST가 동일 gap을 확인한 종목은
        #    5×TF TTL 동안 MAX_GAP 재확인만 억제한다. freshness stale은 hold 대상이 아니다.
        self._rest_gap_confirmed_until: Dict[str, float] = {}
        # 🔥 WS 봉 닫힘 종목별 관리 (완전 복원 2단계)
        self._new_candle_symbols: Set[str] = set()
        
        # ========================================
        # 🆕 실시간 ticker 델타 계산용 (30초 폴링 간 변화량)
        # 헌법: "체제=봉단위, 나머지=30초 갱신"
        # ========================================
        self._prev_ticker: Dict[str, Dict] = {}
        self._last_ticker_data: Dict[str, Dict] = {}  # 심볼별 마지막 ticker 원본
        
        # ========================================
        # 3. 타이머
        # ========================================
        # 마켓 갱신 주기를 config에서 계산
        selection_period = config.get('MARKET_SELECTION', 'market_selection_period', fallback='1m')

        # 지원 주기: 1m, 3m, 6m, 10m, 12m, 15m, 30m, 1h, 2h, 4h, 1d
        period_map = {
            '1m': 60,        # 1분
            '3m': 180,       # 3분 (ticker_3m_snap과 동기화)
            '6m': 360,       # 6분
            '10m': 600,      # 10분
            '12m': 720,      # 12분
            '15m': 900,      # 15분
            '30m': 1800,     # 30분
            '1h': 3600,      # 1시간
            '2h': 7200,      # 2시간
            '4h': 14400,     # 4시간
            '1d': 86400      # 1일
        }

        market_update_interval = period_map.get(selection_period, 60)
        self._market_update_interval_sec = market_update_interval
        
        # 🆕 거래 종목 수 설정 추가
        self.top_markets_count = config.getint(
            'MARKET_SELECTION',
            'top_markets_count',
            fallback=20
        )

        # 디버그 로그 추가 (선택사항)
        logger.info(f"[설정] 마켓 선정 주기: {selection_period} ({market_update_interval}초)")
        logger.info(f"[설정] 거래 종목 수: {self.top_markets_count}개")  # 🆕 추가

        self._market_update_task = None    # 백그라운드 시장 갱신 태스크
        self._prefetch_task = None         # DataFrame Prefetch 태스크 (단일 유지)
        self._ticker_3m_snap_task = None   # ticker 3분 snapshot 태스크 (OR 공식용)
        self.timers = TimerManager({
            'metrics': 60,
            'performance': 300,
            'strategy_review': 1800,
            'report_cleanup': 86400,
            'market_update': market_update_interval,
            'cleanup': 3600,
            'websocket_check': 60,
            'memory_check': 3600,
            'ticker_3m_snap': 180,   # 3분 ticker snapshot (OR 공식용, 선정 주기 아님)
        })
        
        # ========================================
        # 4. 리스크 설정
        # ========================================
        self.risk_profile = config.get_cached('risk_profile') or 'stable'
        self.max_positions = config.get_cached('max_positions') or 8
        self.risk_per_trade = config.get_cached('risk_per_trade') or 0.008
        
        # ========================================
        # 5. 초기 잔고
        # ========================================
        self.initial_balance = config.get_cached('initial_balance') if self.mode == 'sim' else 0
        self.current_balance = self.initial_balance
        
        # ========================================
        # 6. 리포트 디렉토리
        # ========================================
        self.report_dir = Path("outputs") / "summaries"
        self.report_dir.mkdir(parents=True, exist_ok=True)
        
        # 로그 정리 설정
        self.log_dir = Path("outputs") / "logs"
        self.log_dir.mkdir(parents=True, exist_ok=True)
        self.log_retention_days = config.getint('LOGGING', 'retention_days', fallback=30)
        self.report_retention_days = config.getint('LOGGING', 'report_retention_days', fallback=90)
        self.db_backup_retention_days = config.getint('LOGGING', 'db_backup_retention_days', fallback=90)
        self.killswitch_retention_days = config.getint('LOGGING', 'killswitch_retention_days', fallback=30)
        # ========================================
        # 6.5 📒 Decision Ledger (관측 전용 · 기본 off)
        # ========================================
        # 지표 계산 이후 종목별 진입 판단마다 1행을 별도 JSONL 로 남긴다.
        # 매매 판정·주문·포지션에는 관여하지 않는다. 초기화가 실패해도 봇은 그대로 뜬다.
        self._decision_ledger = None
        try:
            if config.getboolean('LOGGING', 'decision_ledger_enabled', fallback=False):
                from Trading_bot_z_decision_ledger import DecisionLedger
                _dl_tf = self.timeframe.value
                _dl_obj = DecisionLedger(
                    out_dir=config.get('LOGGING', 'decision_ledger_dir', fallback='outputs/decision_ledger'),
                    mode=self.mode,
                    timeframe=_dl_tf,
                    tf_seconds=int(self.timeframe.minutes) * 60,
                    level=config.get('LOGGING', 'decision_ledger_level', fallback='full'),
                    queue_max=config.getint('LOGGING', 'decision_ledger_queue_max', fallback=5000),
                    flush_sec=config.getfloat('LOGGING', 'decision_ledger_flush_sec', fallback=2.0),
                    # provenance: 소스는 이 모듈 파일이 있는 폴더(절대경로) 기준,
                    # config 는 ConfigManager 가 실제로 읽은 경로(--config 반영).
                    # 필수 SHA 를 하나라도 못 얻으면 start() 가 실패한다(fail-close).
                    source_dir=Path(__file__).resolve().parent,
                    config_files={
                        'base': Path(config.base_config_path).resolve(),
                        'timeframe': (
                            Path(config.timeframe_config_dir)
                            / f"config_{config.timeframe}.ini"
                        ).resolve(),
                    },
                    session_start=datetime.now(),
                )
                # start() 는 필수 provenance SHA·폴더·당일 파일·header 기록 성공을 1초 안에 확인하고, 실패면 예외를 낸다.
                _dl_obj.start(1.0)
                self._decision_ledger = _dl_obj
                logger.info(
                    f"📒 [DECISION-LEDGER] 기록 시작 | run={_dl_obj.run_id} "
                    f"level={config.get('LOGGING', 'decision_ledger_level', fallback='full')}"
                )
        except Exception as _dl_e:
            self._decision_ledger = None
            logger.warning(
                f"⚠️ [DECISION-LEDGER] 초기화 실패 → 기록 없이 진행 | "
                f"{type(_dl_e).__name__}: {str(_dl_e)[:120]}"
            )
        
        # ========================================
        # 7. 마켓 필터
        # ========================================
        self.EXCLUDED_MARKETS = set(config.get_excluded_markets())
        self.EXCLUDED_NAME_KEYWORDS = set(config.get_excluded_keywords())
        self.INCLUDED_MARKETS = set(config.get_included_markets())
        
        # ========================================
        # 8. 재매수 방지 쿨다운
        # ========================================
        self.sell_cooldown = {}
        self.cooldown_minutes = config.getint('POSITION_MANAGEMENT', 'cooldown_after_loss_minutes', fallback=30)
        
        # ========================================
        # 9. 긴급청산 쿨다운
        # ========================================
        self.emergency_sell_cooldown = {}
        self.emergency_cooldown_seconds = 120
        
        # ========================================
        # 10. 연속손실 추적
        # ========================================
        self.consecutive_losses = defaultdict(int)
        self._cooldown = {}  # 🆕 PRE 실패 시 쿨다운 관리
        
        # ========================================
        # 11. 회복 거래 추적
        # ========================================
        self.recovery_trades = {}
        
        # ========================================
        # 12. ATR 필터 설정
        # ========================================
        self.atr_filter_enabled = config.getboolean('ATR_SETTINGS', 'enable_atr_entry_filter', fallback=True)
        filter_levels_str = config.get('ATR_SETTINGS', 'atr_filter_levels', fallback='VERY_LOW,EXTREME')
        self.atr_filter_levels = [level.strip() for level in filter_levels_str.split(',')]
        
        # ========================================
        # 13. 캐시 보존 설정
        # ========================================
        self.cash_preservation_override = config.getfloat(
            'RISK_MANAGEMENT',
            'cash_preservation_override_confidence',
            fallback=0.85
        )
        
        # ========================================
        # 14. API 키 로드 (실거래 전용)
        # ========================================
        if self.mode == 'real':
            from dotenv import load_dotenv
            import os
            load_dotenv()
            self.access_key = os.getenv('UPBIT_ACCESS_KEY')
            self.secret_key = os.getenv('UPBIT_SECRET_KEY')
        else:
            self.access_key = None
            self.secret_key = None
        
        # ========================================
        # 15. 핵심 컴포넌트 초기화 (순서 중요!)
        # ========================================
        
        # 15.1 API 클라이언트
        from Trading_bot_4_data import UpbitAPIClient
        self.api_client = UpbitAPIClient(config, self.access_key, self.secret_key)
        
        # 15.2 데이터 서비스
        from Trading_bot_4_data import MarketDataService
        self.data_service = MarketDataService()
        
        # 15.3 성과 관리자 (Performance Manager) ← 핵심!
        from Trading_bot_3_performance import PerformanceManager
        self.performance = PerformanceManager(config)
        
        # 15.4 리스크 관리자
        from Trading_bot_9_risk import RiskManager
        self.risk_manager = RiskManager(config)
        
        # 15.5 마켓 분석기
        from Trading_bot_5_analysis import MarketAnalyzer
        self.analyzer = MarketAnalyzer(config)
        
        # 15.6 WebSocket 매니저 (옵션)
        try:
            from Trading_bot_4_data import WebSocketManager
            self.ws_manager = WebSocketManager(config, self.data_service)
        except Exception as e:
            logger.warning(f"WebSocket 초기화 실패: {e}")
            self.ws_manager = None
        
        # 15.7 현재 전략 타입 (기본값)
        from Trading_bot_6_strategy import StrategyType
        self.current_strategy = StrategyType.BALANCED
               
        # ========================================
        # 16. 초기화 완료 로그
        # ========================================
        logger.info("=" * 60)
        logger.info(f"🤖 업비트 자동매매 봇 v1.0 초기화")
        logger.info("=" * 60)
        logger.info(f"모드: {self.mode}")
        logger.info(f"타임프레임: {self.timeframe.value}")
        logger.info(f"리스크 프로필: {self.risk_profile}")
        logger.info(f"최대 포지션: {self.max_positions}개")
        logger.info(f"재매수 쿨다운: {self.cooldown_minutes}분")
        logger.info(f"ATR 필터: {'활성' if self.atr_filter_enabled else '비활성'} ({', '.join(self.atr_filter_levels) if self.atr_filter_enabled else 'N/A'})")
        logger.info(f"캐시보존 우회: {self.cash_preservation_override:.2f}+")
        
        if self.mode == 'sim':
            logger.info(f"초기 자산: {self.initial_balance:,.0f}원")
        
        logger.info("=" * 60)

    def _get_ws_subscription_markets(self) -> list:
        """보유 포지션을 우선 보존한 WebSocket 구독 목록 SSoT."""
        return list(dict.fromkeys(
            list(self.positions.keys())
            + list((self.top_markets or [])[:self.top_markets_count])
        ))

    async def initialize(self) -> bool:
        """컴포넌트 초기화"""
        try:
            logger.info("=" * 70)
            logger.info("🚀 업비트 자동매매 봇 초기화 시작")
            logger.info("=" * 70)
            
            # ========================================
            # 1. API 키 확인 (실거래 모드만)
            # ========================================
            if self.mode == 'real':
                if not self.access_key or not self.secret_key:
                    logger.error("❌ API 키가 설정되지 않았습니다 (.env 파일 확인)")
                    return False
                logger.info("✅ API 키 확인 완료")
            
            # ========================================
            # 2. Reality Engine 초기화 (시뮬레이션만)
            # ========================================
            self.reality_engine = None
            if self.mode == 'sim':
                try:
                    from Trading_bot_simulation_reality_engine import SimulationRealityEngine
                    self.reality_engine = SimulationRealityEngine(self.config)
                    logger.info(f"✅ Reality Engine 초기화 (enabled: {self.reality_engine.enabled})")
                except ImportError as e:
                    logger.warning(f"⚠️ Reality Engine 모듈 없음 - 기본 시뮬레이션 사용: {e}")
                    self.reality_engine = None
                except Exception as e:
                    logger.error(f"⚠️ Reality Engine 초기화 실패 - 기본 시뮬레이션 사용: {e}")
                    self.reality_engine = None
            
            # ========================================
            # 3. 실행 엔진 초기화 (잔고 동기화!)
            # ========================================
            if self.mode == 'sim':
                current_balance = self.initial_balance  # 항상 설정값 사용
                
                # 안전장치: 음수면 초기값 사용
                if current_balance <= 0:
                    logger.warning(f"⚠️ 잔고 0 이하 감지 - 초기값 사용")
                    current_balance = self.initial_balance
                
                logger.info(f"💰 시뮬레이션 초기 잔고: {current_balance:,.0f}원")
                
                # 실행 엔진 생성
                from Trading_bot_11_execution_engine import ExecutionEngineFactory
                self.engine = ExecutionEngineFactory.create_engine(
                    self.config,
                    api_client=self.api_client,
                    reality_engine=self.reality_engine,
                    initial_balance=current_balance
                )
            else:
                # 실거래: API 클라이언트 사용
                from Trading_bot_11_execution_engine import ExecutionEngineFactory
                self.engine = ExecutionEngineFactory.create_engine(
                    self.config,
                    api_client=self.api_client
                )
            
            # 엔진 초기화
            if not await self.engine.initialize():
                logger.error("❌ 실행 엔진 초기화 실패")
                return False
            
            logger.info(f"✅ 실행 엔진 초기화 완료 (모드: {self.mode})")
            
            # ========================================
            # 4. 신호 분석기 초기화
            # ========================================
            from Trading_bot_6_strategy import SignalAnalyzer
            self.signal_analyzer = SignalAnalyzer(self.config, self.timeframe)
            logger.info("✅ 신호 분석기 초기화 완료")
            
            # ========================================
            # 5. 포트폴리오 리스크 관리자 초기화
            # ========================================
            from Trading_bot_10_portfolio_risk import PortfolioRiskManager
            self.portfolio_risk = PortfolioRiskManager(self.config)
            logger.info("✅ 포트폴리오 리스크 관리자 초기화 완료")
            
            # ========================================
            # 6. 데이터 매니저 확인
            # ========================================
            if not hasattr(self, 'data_manager') or self.data_manager is None:
                from Trading_bot_4_data import DataManager
                self.data_manager = DataManager()
                logger.info("✅ 데이터 매니저 초기화 완료")
            else:
                logger.info("✅ 데이터 매니저 이미 초기화됨")
            
            # ========================================
            # 7. 거래 설정 로드
            # ========================================
            from Trading_bot_1_core import TradingSettings
            self.settings = TradingSettings.from_config(self.config)
            logger.info(
                f"✅ 거래설정 | "
                f"손절:{self.settings.stop_loss_pct*100:.2f}% "
                f"익절:{self.settings.take_profit_pct*100:.2f}% "
                f"추적:{self.settings.trailing_stop_pct*100:.2f}%"
            )
            logger.info(
                f"✅ 거래설정 | "
                f"타임아웃:{self.settings.timeout_hours}h "
                f"최대포지션:{self.settings.max_positions}개"
            )
            
            # ========================================
            # 8. 타이머 초기화 (self.timers: L182)
            # ========================================
            logger.info("✅ 타이머 초기화 완료")
            
            # ========================================
            # 9. WebSocket 시작
            # ========================================
            if hasattr(self, 'ws_manager') and self.ws_manager:
                try:
                    logger.info("🔌 WebSocket 시작 중...")
                    await self.ws_manager.start()
                    await asyncio.sleep(2)  # 연결 안정화 대기
                    
                    # 초기 마켓 설정
                    if hasattr(self, 'top_markets') and self.top_markets:
                        # 🔧 수정: config 변수 사용
                        subscribe_count = min(
                            self.top_markets_count,
                            len(self.top_markets)
                        )
                        self.ws_manager.update_markets(
                            self.top_markets[:subscribe_count]
                        )
                        logger.info(
                            f"✅ WebSocket 초기 구독: {subscribe_count}개 종목 "
                            f"(설정: {self.top_markets_count})"
                        )
                    else:
                        # top_markets 없으면 기본 종목
                        default_markets = ['KRW-BTC', 'KRW-ETH', 'KRW-XRP']
                        self.ws_manager.update_markets(default_markets)
                        logger.info(f"✅ WebSocket 기본 구독: {len(default_markets)}개 종목")
                    
                    # DataManager + MarketDataService에 WebSocket 연결
                    if hasattr(self, 'data_manager'):
                        self.data_manager.set_websocket_manager(self.ws_manager)
                    if hasattr(self, 'data_service'):
                        self.data_service.set_websocket_manager(self.ws_manager)
                    logger.info("✅ DataManager/MarketDataService ↔ WebSocket 연결 완료")
                    
                    # 상태 확인
                    ws_status = self.ws_manager.get_status()
                    logger.info(
                        f"✅ WebSocket 초기화 완료 | "
                        f"연결: {ws_status['connected']} | "
                        f"건강: {ws_status['healthy']} | "
                        f"구독: {len(ws_status['markets'])}개"
                    )
                    
                    # 🔥 봉 닫힘 이벤트 콜백 등록 (완전 복원)
                    self.ws_manager.add_candle_close_callback(self._on_candle_close)
                    logger.info("✅ WebSocket 봉 닫힘 콜백 등록 완료")
                    
                except Exception as e:
                    logger.warning(f"[WS-에러] WebSocket 시작 실패 - REST 모드로 전환: {e}")
            else:
                logger.info("[REST-모드] WebSocket Manager 없음 - REST 전용 모드")
            
            # ========================================
            # 10. 최종 상태 확인
            # ========================================
            logger.info("=" * 70)
            logger.info("🎉 모든 컴포넌트 초기화 완료!")
            logger.info("=" * 70)

            # 세션 시작 잔고 저장
            balance = await self.engine.get_balance()
            self.session_start_balance = balance
            logger.info(f"💰 현재 잔고: {balance:,.0f}원")
            logger.info(f"📊 세션 시작 기준 잔고: {self.session_start_balance:,.0f}원")

            # REAL 전용: 재시작 후 거래소 보유 포지션 관리 재개
            if self.mode == 'real':
                await self._recover_positions_from_exchange()
                await self._reconcile_real_orders()

            # 🆕 초기 메모리 사용량 로깅
            self._log_memory_usage("초기화 완료")
            
            # ========================================
            # 11. Phase 2: WatchList 완전 제거
            # ========================================
            # 분석 결과:
            # - 1분봉 API: 1,379회 (208개 종목)
            # - 성과: Promote 0건 (3시간)
            # - 효율: 0%
            # 결론: 정리 단계 - MainPool 전용
            logger.info("📋 WatchList 비활성화 (MainPool 전용)")
            
            # ========================================
            # 12. 초기화 완료
            # ========================================
            logger.info("🎉 모든 컴포넌트 초기화 완료!")
            
            return True
        
        except Exception as e:
            logger.error(f"❌ 초기화 실패: {e}")
            import traceback
            logger.error(f"상세 오류:\n{traceback.format_exc()}")
            return False
    
    async def _load_markets(self):
        """마켓 목록 로드"""
        try:
            self.markets = await self.data_service.get_markets()
            if not self.markets:
                self.markets = [
                    {'market': 'KRW-BTC', 'korean_name': '비트코인'},
                    {'market': 'KRW-ETH', 'korean_name': '이더리움'}
                ]
            logger.info(f"마켓 목록: {len(self.markets)}개")
        except Exception as e:
            logger.error(f"마켓 로드 실패: {e}")
            self.markets = []
    
    async def _ws_candle_close_listener(self):
        """
        WebSocket 기반 캔들 확정 감시 루프 (Polling 하이브리드 모드)        
        목적:
          - 캔들 확정 시점에 이벤트를 set()하여 메인 루프가 즉시 스캔하도록 트리거
          - 기존 30초 Polling은 유지하고, 추가로 캔들 확정 시 1회 더 실행        
        장점:
          - Polling 안정성 유지 (WebSocket 장애 시에도 정상 동작)
          - 캔들 확정 시점 정확도 100% (0초 지연)
          - 전략 로직 변경 없음 (스케줄링만 개선)
        """
        if self._candle_close_event is None:
            self._candle_close_event = asyncio.Event()
        
        logger.info("🔄 WebSocket 캔들 확정 리스너 초기화 중...")
        
        # DataManager 확인
        if not hasattr(self, 'data_manager') or self.data_manager is None:
            logger.warning("⚠️ DataManager 미초기화 - 캔들 리스너 비활성화 (Polling만 사용)")
            return
        
        # WebSocketManager 확인
        if not hasattr(self.data_manager, 'ws_manager') or self.data_manager.ws_manager is None:
            logger.warning("⚠️ WebSocketManager 미초기화 - 캔들 리스너 비활성화 (Polling만 사용)")
            return
        
        ws_manager = self.data_manager.ws_manager
        
        logger.info(
            f"✅ WebSocket 캔들 확정 리스너 시작 완료\n"
            f"   - 타임프레임: {self.timeframe.value}\n"
            f"   - 모드: Hybrid (Polling 30초 + 캔들 확정 이벤트)\n"
            f"   - 효과: 캔들 확정 시 즉시 추가 스캔 (지연 0초)"
        )
        
        # 캔들 확정 콜백
        def on_candle_close_callback(symbol: str, timeframe_str: str):
            """WebSocket에서 캔들 확정 시 호출되는 콜백"""
            if timeframe_str != self.timeframe.value:
                return
            if not self.is_running:
                return
            # 🔒 [CANDLE-CLOSE-PRIORITY-GUARD-REMOVE-0827]
            # "즉시 스캔 트리거" 로그는 조건 없이 항상 찍히는데
            # 우선재체크 등록만 top_markets 소속 여부로 걸러지던
            # 로그-동작 계약 불일치를 제거한다.
            self._priority_recheck[symbol] = max(
                self._priority_recheck.get(symbol, 0), 1
            )
            # __PRIPROV609__ ★priority producer ★관측 전용 (★판정 없음)
            try:
                import json as _ppj
                self._pri_prov_seq = int(getattr(self, "_pri_prov_seq", 0)) + 1
                logger.info("[PRI-PROV-SET] " + _ppj.dumps({
                    "seq": int(self._pri_prov_seq),
                    "cycle": int(getattr(self, "_cycle_count", 0)),
                    "sym": str(symbol),
                    "source": "WS_CANDLE_CLOSE",
                    "ttl": 1,
                    "after": int(self._priority_recheck.get(symbol, 0)),
                }, ensure_ascii=False, separators=(",", ":")))
            except Exception as _ppe:
                logger.warning("[PRI-PROV-ERR] %s" % type(_ppe).__name__)
            self.analyzer.tick_gc_birth_watchlist(symbol)
            # GC 탄생 등록은 _check_entry_signals()의 GC freshness filter에서만 수행
            # 콜백: 캔들 확정 이벤트 전달 + watchlist 수명 감소만 담당
            if self._candle_close_event is not None:
                self._candle_close_event.set()
                logger.info(
                    f"🕒 [WS-CandleClose] {symbol} {timeframe_str} 캔들 확정 → 즉시 스캔 트리거"
                )
        
        # WebSocket Manager에 콜백 등록 및 캔들 구독
        try:
            # ========================================
            # 🔥 실제 WebSocketManager 연동 (Upbit 캔들 WebSocket)
            # ========================================
            
            # 1. 콜백 등록
            if hasattr(ws_manager, 'add_candle_close_callback'):
                ws_manager.add_candle_close_callback(on_candle_close_callback)
                logger.info("[WS-초기화] 캔들 콜백 등록 완료")
            else:
                logger.warning(
                    "⚠️ WebSocketManager에 add_candle_close_callback 없음 - "
                    "Polling만 사용"
                )
                return
            
            # 2. Upbit 캔들 WebSocket 실제 구독
            if hasattr(ws_manager, '_subscribe_candles'):
                # 🔥 타임프레임 → 분(minutes) 변환 (h/d 지원)
                def _tf_to_minutes(tf: str) -> int:
                    t = (tf or "").lower().strip()
                    try:
                        if t.endswith("m"):
                            return int(t[:-1])
                        if t.endswith("h"):
                            return int(t[:-1]) * 60
                        if t.endswith("d"):
                            return int(t[:-1]) * 1440
                    except Exception:
                        pass
                    return 3  # fallback

                interval_min = _tf_to_minutes(self.timeframe.value)
                interval = str(interval_min)
                
                # 🔥 top_markets 준비될 때까지 대기
                markets_to_subscribe = []
                if self.top_markets:
                    markets_to_subscribe = list(self.top_markets[:self.top_markets_count])
                
                if not markets_to_subscribe:
                    logger.info("[WS-대기] Candle 구독 대기 중 (top_markets 준비 후)")
                    return
                
                await ws_manager._subscribe_candles(
                    markets=markets_to_subscribe,
                    interval=interval
                )
                # 🔥 상태 저장 후 실제 구독 전송
                await ws_manager._subscribe()
                logger.info(f"[WS-초기화] Candle 구독 완료: {interval}분봉, {len(markets_to_subscribe)}개 종목")
            else:
                logger.warning("[WS-에러] _subscribe_candles 메서드 없음")
                return
                
        except Exception as e:
            logger.warning(f"[WS-에러] Candle 연동 실패: {e} - Polling만 사용")
            import traceback
            logger.debug(f"상세:\n{traceback.format_exc()}")
            return
        
        logger.info("✅ WebSocket 하이브리드 모드 활성화 완료 (0초 지연)")

    async def _recover_positions_from_exchange(self):
        """[REAL 전용] 기동 시 거래소 보유분으로 self.positions 재구성"""
        if self.mode != 'real':
            return
        try:
            accounts = await self.api_client.get_balance()
            if not isinstance(accounts, list):
                logger.warning(
                    f"🔁 [포지션복구] accounts 타입 오류({type(accounts).__name__}) → 복구 스킵"
                )
                return
        except Exception as e:
            logger.error(f"🔁 [포지션복구] accounts 조회 실패: {e}")
            return
        try:
            min_krw = float(self.config.get_cached('min_order_amount') or 5000)
        except Exception:
            min_krw = 5000.0

        now = datetime.now()
        recovered = 0

        for acc in accounts or []:
            cur = str(acc.get('currency') or '').strip()
            if not cur or cur == 'KRW':
                continue
            symbol = f"KRW-{cur}"
            if symbol in self.positions:
                continue
            if symbol in getattr(self, 'EXCLUDED_MARKETS', set()):
                continue
            try:
                volume    = float(acc.get('balance', 0) or 0) + float(acc.get('locked', 0) or 0)
                avg_price = float(acc.get('avg_buy_price', 0) or 0)
            except (TypeError, ValueError):
                continue
            if volume <= 0 or avg_price <= 0 or volume * avg_price < min_krw:
                continue
            try:
                tickers   = await self.data_service.get_ticker([symbol])
                cur_price = float((tickers or [{}])[0].get('trade_price', avg_price))
            except Exception:
                cur_price = avg_price

            s = self.settings
            pos = Position(
                symbol=symbol,
                volume=volume,
                avg_price=avg_price,
                entry_time=now,
                entry_reason="RECOVERED_ON_RESTART",
                stop_loss=avg_price * (1.0 - s.stop_loss_pct),
                take_profit=avg_price * (1.0 + s.take_profit_pct),
                peak_price=max(avg_price, cur_price),
                trailing_stop_distance=s.trailing_stop_pct,
                entry_atr=0.0,
                entry_regime=None,
                original_volume=volume,
                metadata={
                    'recovered': True,
                    'recovery_time': now.isoformat(),
                    'source': 'ACCOUNTS',
                },
            )
            async with self._positions_lock:
                self.positions[symbol] = pos
            self._restore_position_state(pos)  # [보완 v3_4 ④]
            recovered += 1
            logger.warning(
                f"🔁 [포지션복구] {symbol} | "
                f"vol={volume:.8f} avg={avg_price:,.2f} "
                f"현재가={cur_price:,.2f} "
                f"손절={pos.stop_loss:,.2f} → 관리 재개"
            )
        logger.warning(f"🔁 [포지션복구] 완료: {recovered}건")

    async def run(self) -> int:
        """메인 실행 루프"""
        if not await self.initialize():
            return EXIT_CODE_FAILURE
        
        self.is_running = True
        
        # 🆕 세션 시작 시간 기록
        self.session_start_time = datetime.now()
        
        logger.info("거래 시작")
        
        # ========================================
        # 🆕 WebSocket 헬스체크 태스크 시작
        # ========================================
        health_check_task = asyncio.create_task(
            self._monitor_websocket_health()
        )
        logger.info("✅ WebSocket 헬스체크 태스크 시작")
        
        # ========================================
        # 🆕 WebSocket 캔들 확정 리스너 태스크 시작 (하이브리드 모드)
        # ========================================
        if self._ws_candle_task is None:
            self._ws_candle_task = asyncio.create_task(
                self._ws_candle_close_listener()
            )
            logger.info("✅ WebSocket 캔들 확정 리스너 태스크 시작 (하이브리드 모드)")
        
        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        # 🔧 초기 종목 선정 (동기 1회)
        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        logger.info("📊 [TICKER-3M-SNAP] 초기 seed 시작")
        await self.analyzer.refresh_ticker_3m_close_cache()
        logger.info("📊 [TICKER-3M-SNAP] 초기 seed 완료 (첫 선정은 data_miss 일부 정상)")

        logger.info("📋 초기 종목 선정 시작...")
        await self._update_top_markets()
        logger.info(f"✅ 초기 종목 선정 완료: {len(self.top_markets)}개")
        
        # 🔥 STALE 초기 오탐 방지: refresh 시각 선세팅
        now_ts = time.time()
        for symbol in self.top_markets[:self.top_markets_count]:
            self._df_refresh_ts_by_symbol[symbol] = now_ts
        logger.debug(f"✅ DF refresh 시각 초기화: {len(self.top_markets[:self.top_markets_count])}개")
        
        try:
            if self._realtime_hardstop_task is None:
                self._realtime_hardstop_task = asyncio.create_task(
                    self._realtime_hardstop_monitor()
                )
                logger.info(
                    "✅ 실시간 하드스톱 모니터 시작 (WS ticker 0.2초)"
                )
            while self.is_running:
                cycle_start = datetime.now()
                self._cycle_count += 1

                # 🔒 [ENTRY-CYCLE-PRE-0909] ★382-R · 관측 전용 (판정 무변경)
                # replay가 live main-cycle을 같은 외생 입력으로 한 번만 실행하기 위한 SSoT.
                # BAR-OPEN due 승격 · _check_positions · risk/kill/POSITION_FULL ·
                # _check_entries 내부 priority/FGA/GC/cap보다 반드시 먼저 기록한다.
                # [진입체크대상]은 이미 GC까지 지난 post-filter 목록이므로 입력으로 쓰지 않는다.
                # _check_entries가 호출되지 않는 cycle도 기록해야 treatment 반사실을 재현할 수 있다.
                try:
                    _ecp = list(self.top_markets or [])
                    logger.info(
                        f"[ENTRY-CYCLE-PRE] "
                        f"cycle={self._cycle_count} "
                        f"start={cycle_start:%Y-%m-%d %H:%M:%S.%f} "
                        f"count={len(_ecp)} "
                        f"cap={int(self.top_markets_count)} | "
                        f"{_ecp}"
                    )
                except Exception as _ecp_e:
                    logger.error(
                        f"🛑 [ENTRY-CYCLE-PRE-ERR] "
                        f"cycle={self._cycle_count} | "
                        f"{type(_ecp_e).__name__}: "
                        f"{str(_ecp_e)[:120]}"
                    )

                # 🔒 [BAR-OPEN-FRESH-RECHECK-0824]
                # 만기된 예약을 기존 priority queue로 올린 뒤 이번 사이클에서
                # 가격/DF/MA/TLC/J1~J4를 전부 새로 계산한다.
                # stale signal은 재사용하지 않는다.
                _bar_now = time.time()
                _bar_due_symbols = [
                    _sym
                    for _sym, _due in list(
                        self._bar_open_recheck_due.items()
                    )
                    if float(_due or 0.0) <= _bar_now
                ]

                if _bar_due_symbols:
                    for _sym in _bar_due_symbols:
                        self._bar_open_recheck_due.pop(
                            _sym, None
                        )
                        self._priority_recheck[_sym] = max(
                            self._priority_recheck.get(_sym, 0),
                            1,
                        )
                        # __PRIPROV609__ ★priority producer ★관측 전용 (★판정 없음)
                        try:
                            import json as _ppj
                            self._pri_prov_seq = int(getattr(self, "_pri_prov_seq", 0)) + 1
                            logger.info("[PRI-PROV-SET] " + _ppj.dumps({
                                "seq": int(self._pri_prov_seq),
                                "cycle": int(getattr(self, "_cycle_count", 0)),
                                "sym": str(_sym),
                                "source": "BAR_OPEN_RECHECK",
                                "ttl": 1,
                                "after": int(self._priority_recheck.get(_sym, 0)),
                            }, ensure_ascii=False, separators=(",", ":")))
                        except Exception as _ppe:
                            logger.warning("[PRI-PROV-ERR] %s" % type(_ppe).__name__)

                    logger.info(
                        f"⏱️ [BAR-OPEN-RECHECK-DUE] "
                        f"{len(_bar_due_symbols)}개 → fresh 재평가 | "
                        f"sample={_bar_due_symbols[:5]}"
                    )
                
                try:
                    # 매도 (1회)
                    await self._check_positions()
                    
                    # 리스크 체크
                    risk_metrics = await self._get_risk_metrics()
                    can_trade = await self.risk_manager.check_risk_conditions(risk_metrics)
                    
                    # 킬스위치 / 리스크 / 포지션 FULL 최상위 진입 차단 로그
                    if self.risk_manager.kill_switch_active:
                        logger.info(
                            f"🚫 [BOT-ENTRY-BLOCK] phase=KILL_SWITCH | "
                            f"level={self.risk_manager.current_level} "
                            f"→ 신규 매수 차단"
                        )
                    elif not can_trade:
                        logger.info(
                            f"🚫 [BOT-ENTRY-BLOCK] phase=RISK_CONDITION | "
                            f"level={self.risk_manager.current_level} "
                            f"metrics={risk_metrics} "
                            f"→ 신규 매수 차단"
                        )
                    else:
                        async with self._positions_lock:
                            _position_count_for_entry = len(self.positions)

                        if _position_count_for_entry >= self.settings.max_positions:
                            logger.info(
                                f"🚫 [BOT-ENTRY-BLOCK] phase=POSITION_FULL_RUN | "
                                f"positions={_position_count_for_entry}/{self.settings.max_positions} "
                                f"→ _check_entries 호출 전 차단"
                            )
                        else:
                            preserve_cash = self._should_preserve_cash(risk_metrics)
                            
                            if preserve_cash:
                                logger.debug(
                                    f"💰 캐시보존모드 - "
                                    f"신뢰도 {self.cash_preservation_override:.2f}+ 만 진입"
                                )
                            
                            # 🔥 갱신 직후 즉시 체크 플래그 처리
                            if self._force_entry_check:
                                logger.info("🔥 [즉시진입체크] 종목 갱신 완료 → 즉시 실행")
                                self._force_entry_check = False
                            
                            # 진입 체크 실행 (항상 1회)
                            await self._check_entries(preserve_cash=preserve_cash)
                    
                    # 📊 ticker 3분 snapshot + market_update 순서 보장
                    _ticker_snap_due   = self.timers.check_and_reset('ticker_3m_snap')
                    _market_update_due = self.timers.check_and_reset('market_update')

                    # market_update가 걸리는 사이클 → snapshot 먼저 완료 보장
                    if _market_update_due:
                        if self._ticker_3m_snap_task and not self._ticker_3m_snap_task.done():
                            logger.info("📊 [TICKER-3M-SNAP] market_update 전 기존 snapshot 태스크 대기")
                            await self._ticker_3m_snap_task
                        # market_update 직전에는 ticker_snap_due 무관하게 최신 MINI_GC 캐시 보장
                        logger.info("📊 [TICKER-3M-SNAP] market_update 직전 동기 갱신 시작")
                        await self.analyzer.refresh_ticker_3m_close_cache()
                        logger.info("📊 [TICKER-3M-SNAP] market_update 직전 동기 갱신 완료")
                    elif _ticker_snap_due:
                        if not self._ticker_3m_snap_task or self._ticker_3m_snap_task.done():
                            self._ticker_3m_snap_task = asyncio.create_task(
                                self.analyzer.refresh_ticker_3m_close_cache()
                            )
                        else:
                            logger.debug("📊 [TICKER-3M-SNAP] 기존 태스크 실행 중 → 스킵")

                    # 🔥 상위 종목 갱신 (직렬 실행 - 중복 API 차단)
                    if _market_update_due:
                        if not self._market_update_task or self._market_update_task.done():
                            self._market_update_task = asyncio.create_task(self._update_top_markets())
                            self._market_update_task.add_done_callback(self._on_market_update_done)
                            logger.info("📋 [마켓갱신] 백그라운드 실행 시작")
                    
                    # 일일 리셋
                    now = datetime.now()
                    if now.date() > self.daily_reset_time.date():
                        await self._daily_reset()
                    
                    # 주기적 작업
                    await self._handle_periodic_tasks()
                    
                    # 오류 리셋
                    self.consecutive_errors = 0
                    
                except Exception as e:
                    import traceback
                    logger.error(f"사이클 {self._cycle_count} 오류: {e}")
                    logger.error(f"오류 위치: {traceback.format_exc()}")
                    self.consecutive_errors += 1
                    if self.consecutive_errors > self.config.get_cached('max_consecutive_errors'):
                        logger.critical("최대 오류 초과")
                        break
                
                # 이벤트 기반 대기: WS-CandleClose 발생 시 즉시 깨어남
                # clear() 선행: wait() 직전 초기화로 레이스 조건 방지 (clear-first 패턴)
                # timeout fallback: 폴링 안전망 유지
                elapsed = (datetime.now() - cycle_start).total_seconds()
                tf_wait = max(5, self.timeframe.trading_interval - elapsed)
                market_wait = max(5, float(getattr(self, "_market_update_interval_sec", tf_wait)))
                wait = min(tf_wait, market_wait)

                # 봉초 가드 예약이 있으면 가장 가까운 fresh 재평가 시각에 맞춰 깨운다.
                if self._bar_open_recheck_due:
                    _next_bar_due = min(
                        self._bar_open_recheck_due.values()
                    )
                    _bar_recheck_wait = max(
                        0.05,
                        _next_bar_due - time.time(),
                    )
                    wait = min(wait, _bar_recheck_wait)

                if self._candle_close_event is not None:
                    try:
                        await asyncio.wait_for(
                            self._candle_close_event.wait(), timeout=wait
                        )
                    except asyncio.TimeoutError:
                        pass
                    finally:
                        self._candle_close_event.clear()
                else:
                    await asyncio.sleep(wait)
                        
            logger.info(f"✅ 정상 종료 (총 사이클: {self._cycle_count})")
                
        except KeyboardInterrupt:
            logger.info("🛑 사용자 중단 (Ctrl+C)")
        
        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        # ⚙️ asyncio.CancelledError 별도 처리 (정상 종료)
        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        # 목적: 시그널 핸들러에 의한 정상 종료를 에러로 표시하지 않음
        # 효과: 로그 가독성 향상, 정상/비정상 종료 명확히 구분
        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        except asyncio.CancelledError:
            logger.info("✅ 비동기 작업 취소 (정상 종료)")
        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        
        except Exception as e:
            logger.error(f"❌ 실행 오류: {e}")
        
        finally:
            # ========================================
            # 🆕 헬스체크 태스크 종료
            # ========================================
            self.is_running = False

            if 'health_check_task' in locals() and not health_check_task.done():
                health_check_task.cancel()
                try:
                    await health_check_task
                except asyncio.CancelledError:
                    pass
                logger.info("✅ WebSocket 헬스체크 태스크 종료")

            # ========================================
            # 실시간 하드스톱 태스크 종료
            # ========================================
            if (
                self._realtime_hardstop_task is not None
                and not self._realtime_hardstop_task.done()
            ):
                self._realtime_hardstop_task.cancel()
                try:
                    await self._realtime_hardstop_task
                except asyncio.CancelledError:
                    pass
                except Exception as e:
                    logger.warning(
                        f"⚠️ 실시간 하드스톱 종료 중 예외: {e}"
                    )
                finally:
                    self._realtime_hardstop_task = None
                logger.info("✅ 실시간 하드스톱 모니터 종료")

            # ========================================
            # 🆕 WebSocket 캔들 리스너 태스크 종료
            # ========================================
            if self._ws_candle_task is not None and not self._ws_candle_task.done():
                self._ws_candle_task.cancel()
                try:
                    await self._ws_candle_task
                except asyncio.CancelledError:
                    pass
                except Exception as e:
                    logger.warning(f"⚠️ WebSocket 캔들 리스너 종료 중 예외: {e}")
                finally:
                    self._ws_candle_task = None
                logger.info("✅ WebSocket 캔들 리스너 태스크 종료")
            
            await self.graceful_shutdown()
        
        return EXIT_CODE_SUCCESS if not self.risk_manager.kill_switch_active else EXIT_CODE_FAILURE
    
    async def _update_top_markets(self):
        """상위 마켓 업데이트 (MainPool 전용)"""
        try:
            # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
            # Phase 2: MainPool 전용 (WatchList 제거)
            # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
            # 🔒 SSoT 강제: Producer 단계에서 symbol 없는 결과 제거
            # 🔒 캐시 기반 선정 (TTL 준수)
            raw_symbols = await self.analyzer.get_current_top_markets()

            # perform_integrated_analysis() 결과 정화
            current = []
            for sym in raw_symbols or []:
                if not sym or sym == "UNKNOWN":
                    logger.error(
                        f"❌ [SSoT-BREAK][Producer] invalid symbol from analyzer → drop | sym={sym}"
                    )
                    continue
                current.append(sym)

            if not current:
                logger.warning(
                    "⚠️ [Producer] 유효한 symbol 없음 → market update 스킵"
                )
                return

            logger.info(
                f"📋 [MAINPOOL] {len(current)}개 | "
                f"{', '.join([s.replace('KRW-', '') for s in current[:5]])} "
                f"{'...' if len(current) > 5 else ''}"
            )
        
            old_market_list = list(self.top_markets or [])
            old_markets = set(old_market_list)

            if current != old_market_list:
                self.top_markets = current

                # 🆕 신규 급등 편입 종목 → 즉시 우선 스캔 (우선재체크만)
                new_entries = [sym for sym in current if sym not in old_markets]
                for sym in new_entries:
                    self._priority_recheck[sym] = max(
                        self._priority_recheck.get(sym, 0), 3
                    )
                    # __PRIPROV609__ ★priority producer ★관측 전용 (★판정 없음)
                    try:
                        import json as _ppj
                        self._pri_prov_seq = int(getattr(self, "_pri_prov_seq", 0)) + 1
                        logger.info("[PRI-PROV-SET] " + _ppj.dumps({
                            "seq": int(self._pri_prov_seq),
                            "cycle": int(getattr(self, "_cycle_count", 0)),
                            "sym": str(sym),
                            "source": "NEW_ENTRY",
                            "ttl": 3,
                            "after": int(self._priority_recheck.get(sym, 0)),
                        }, ensure_ascii=False, separators=(",", ":")))
                    except Exception as _ppe:
                        logger.warning("[PRI-PROV-ERR] %s" % type(_ppe).__name__)
                    self._gc_birth_ttl_last_candle.pop(sym, None)
                    logger.info(f"🚀 [NewEntry-Priority] {sym} 신규 편입 → 즉시 우선 스캔 등록")

                if new_entries:
                    self._force_entry_check = True
                    if self._candle_close_event is not None:
                        self._candle_close_event.set()
                    logger.info(
                        f"⚡ [NewEntry-ImmediateWake] 신규편입 {len(new_entries)}개 → "
                        f"진입체크 루프 즉시 깨움 | sample={new_entries[:5]}"
                    )

                # 🔒 WS 구독 갱신: 보유 포지션을 최우선으로 항상 유지
                ws_markets = self._get_ws_subscription_markets()
                try:
                    if ws_markets and hasattr(self.data_manager, "ws_manager") and self.data_manager.ws_manager:
                        ws_manager = self.data_manager.ws_manager

                        def _tf_to_minutes(tf: str) -> int:
                            t = (tf or "").lower().strip()
                            try:
                                if t.endswith("m"):
                                    return int(t[:-1])
                                if t.endswith("h"):
                                    return int(t[:-1]) * 60
                                if t.endswith("d"):
                                    return int(t[:-1]) * 1440
                            except Exception:
                                pass
                            return 3

                        interval = str(_tf_to_minutes(self.timeframe.value))
                        await self.data_manager.update_websocket_markets(
                            ws_markets, auto_subscribe=False
                        )
                        if hasattr(ws_manager, "_subscribe_candles"):
                            await ws_manager._subscribe_candles(
                                markets=ws_markets,
                                interval=interval
                            )
                        await ws_manager._subscribe()
                        logger.debug(f"[WS-마켓갱신] 통합구독 1회: {len(ws_markets)}개")
                except Exception as e:
                    logger.warning(f"[WS-에러] 통합 재구독 실패: {e}")
                    
        except Exception as e:
            logger.error(f"마켓 업데이트 오류: {e}")
    
    def _on_market_update_done(self, task: asyncio.Task):
        """백그라운드 마켓 갱신 완료/실패 콜백"""
        try:
            exc = task.exception()
            if exc:
                logger.error(f"❌ [마켓갱신] 백그라운드 태스크 실패: {exc}")
            else:
                logger.info("📋 [마켓갱신] 백그라운드 태스크 완료")
                
                # 🔧 중복 방지: 이미 True면 덮어쓰지 않음
                if not self._force_entry_check:
                    self._force_entry_check = True
                if self._candle_close_event is not None:
                    self._candle_close_event.set()
        except asyncio.CancelledError:
            logger.debug("📋 [마켓갱신] 태스크 취소됨")
    
    async def _realtime_hardstop_monitor(self):
        """WS ticker_cache 기반 손절+부분익절 실시간 모니터."""
        _base_cap = abs(float(self.settings.stop_loss_pct or 0.005))
        while self.is_running:
            try:
                wm = self.ws_manager
                ticker_cache = getattr(wm, "ticker_cache", None) if wm else None
                if not isinstance(ticker_cache, dict):
                    await asyncio.sleep(1.0)
                    continue
                for symbol, position in list(self.positions.items()):
                    if getattr(position, "_closing", False):
                        continue
                    ticker = ticker_cache.get(symbol) or {}
                    price = float(ticker.get("trade_price", 0.0) or 0.0)
                    timestamp_ms = float(ticker.get("timestamp", 0.0) or 0.0)
                    age_sec = (
                        time.time() * 1000.0 - timestamp_ms
                    ) / 1000.0
                    if (
                        price <= 0.0
                        or timestamp_ms <= 0.0
                        or age_sec > 3.0
                        or position.avg_price <= 0.0
                    ):
                        continue
                    _stop_price = float(
                        getattr(position, "stop_loss", 0.0) or 0.0
                    )
                    hard_cap = (
                        (position.avg_price - _stop_price)
                        / position.avg_price
                        if 0.0 < _stop_price < position.avg_price
                        else _base_cap
                    )
                    loss = (
                        price - position.avg_price
                    ) / position.avg_price
                    if loss <= -hard_cap:
                        logger.warning(
                            f"🛑 [RT-HARDSTOP] {symbol} | "
                            f"loss={loss * 100:.2f}% "
                            f"limit={-hard_cap * 100:.2f}% "
                            f"ticker_age={age_sec:.2f}s"
                        )
                        signal = TradingSignal(
                            symbol=symbol,
                            action="SELL",
                            price=price,
                            reason="실시간손절",
                            confidence=1.0,
                            sell_ratio=1.0,
                            market_regime=(
                                getattr(position, "entry_regime", None)
                                or MarketRegime.RANGING
                            ),
                        )
                        await self.execute_trade(position, signal)
                    else:
                        _tp_regime = (
                            getattr(position, "entry_regime", None)
                            or MarketRegime.RANGING
                        )
                        _tp_entry_conf = (
                            (getattr(position, "metadata", None) or {})
                            .get("entry_confidence")
                        )
                        _tp_signal = self.signal_analyzer._check_partial_sell(
                            position,
                            price,
                            _tp_regime,
                            entry_conf=_tp_entry_conf,
                        )
                        if _tp_signal is not None:
                            await self.execute_trade(position, _tp_signal)
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.debug(f"[RT-HARDSTOP-ERR] {e}")
            await asyncio.sleep(0.2)

    async def _check_positions(self):
        """포지션 체크"""
        async with self._positions_lock:
            if not self.positions:
                return
            
            # 복사본으로 반복 (안전)
            positions_snapshot = dict(self.positions)
        
        # Lock 밖에서 처리 (긴 작업은 Lock 밖에서!)
        for symbol, position in positions_snapshot.items():
            try:
                # ============================================================
                # 🔥 청산 신선가 조회 (매도 분석용)
                # - MarketDataService.get_current_price(): WS 신선도 검증 + REST 폴백 SSoT
                # ============================================================
                current_price_1m = await self.data_service.get_current_price(symbol)
                
                # ============================================================
                # 🔥 매도 분석용 current_price는 1m 기준 사용
                # ============================================================
                current_price = current_price_1m  # ← 🔥 핵심: 1m 현재가로 통일
                if current_price is None or current_price <= 0:
                    logger.debug(
                        f"⏭️ [{symbol}] 현재가 조회 실패 → 포지션 체크 스킵"
                    )
                    continue
                
                # ========================================
                # 긴급청산 쿨다운 체크 (기존과 동일)
                # ========================================
                if symbol in self.emergency_sell_cooldown:
                    elapsed = (datetime.now() - self.emergency_sell_cooldown[symbol]).total_seconds()
                    if elapsed < self.emergency_cooldown_seconds:
                        logger.debug(f"⏸️ {symbol} 긴급청산 쿨다운 중 ({self.emergency_cooldown_seconds - elapsed:.0f}초 남음)")
                        continue
                
                # ========================================
                # 포지션 수량 검증 (기존과 동일)
                # ========================================
                # 🔥 부분매도 진행 중이면 최소수량 체크 스킵 (저가 코인 보호)
                partial_cnt = int(getattr(position, "partial_sell_count", 0) or 0)
                
                if partial_cnt <= 0 and position.volume < MIN_ORDER_AMOUNT / current_price:
                    logger.warning(f"⚠️ {symbol} 잔여 수량 과소 - 강제 정리: {position.volume:.8f}개")
                    async with self._positions_lock:
                        self.positions.pop(symbol, None)
                    
                    # 🆕 Strategy에 포지션 종료 이벤트 통지 (recovery backfill 6봉)
                    try:
                        if hasattr(self, "signal_analyzer") and self.signal_analyzer:
                            self.signal_analyzer.register_recovery_backfill(symbol, 6)
                    except Exception as e:
                        logger.warning(f"[RECOVERY-REGISTER-FAIL] {symbol}: {e}")
                    
                    continue
                
                # ========================================
                # 🔥 비상 손절만 유지 (일반 손절은 strategy MA15 보호에 위임)
                # ========================================
                if current_price > 0:
                    
                    # 2️⃣ 비상 손절 체크 (-6% 급락 대비)
                    current_loss = (current_price - position.avg_price) / position.avg_price
                    
                    if current_loss < -0.06:  # -6%
                        logger.critical(
                            f"🚨🚨🚨 {symbol} 비상 손절 발동! "
                            f"손실: {current_loss*100:.2f}% < -6%"
                        )
                        
                        emergency_stop = TradingSignal(
                            symbol=symbol,
                            action="SELL",
                            price=current_price,
                            reason=f"비상손절 ({current_loss*100:.2f}%)",
                            confidence=1.0,
                            sell_ratio=1.0,
                            market_regime=MarketRegime.RANGING
                        )
                        
                        await self.execute_trade(position, emergency_stop)
                        continue
                
                # DataFrame 조회 (intrabar: 캐시 우선, 캔들 경계에서만 refresh)
                now = time.time()
                tf_sec = self._timeframe_to_seconds(self.timeframe.value)
                
                last = self._df_refresh_ts_by_symbol.get(symbol, 0.0)
                force_refresh = (now - last) >= tf_sec
                
                df = await self.data_service.get_dataframe(
                    symbol, self.timeframe.value, count=200, force_refresh=force_refresh
                )
                
                if force_refresh:
                    self._df_refresh_ts_by_symbol[symbol] = now
                
                # (제거) df_id 검증 로그 — 운영 환경에서 분석 가치 없음
                # 🔍 검증 로그: DataFrame 객체 동일성 확인
                # if df is not None and not df.empty:
                #     logger.debug(
                #         f"[BOT-DF-ID] {symbol} df_id={id(df)} "
                #         f"high={df.iloc[-1]['high']:.2f} low={df.iloc[-1]['low']:.2f} "
                #         f"refresh={force_refresh}"
                #     )
                
                if not is_valid_df(df):
                    logger.debug(f"⏭️ [DATA-SKIP] {symbol} 데이터프레임 무효 → 진입 스킵")
                    continue
                
                # ✅ 30초 폴링마다: 최신 봉을 현재가로 동기화 (MA/가자미근 즉시 반영)
                # - close: 즉시 동기화
                # - high/low: 캔들 내부 극값을 즉시 반영 (머리/장대 대응)
                self._apply_intrabar_price_to_df(df, current_price)
                
                # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
                # 🆕 STALE 체크 (운영 안정 표준형)
                # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
                
                data_stale = False
                stale_latest_time = None
                delay_seconds = 0
                
                try:
                    # ───────────────────────────────
                    # ① Config 안전 접근
                    # ───────────────────────────────
                    config = getattr(self, "config", None)
                    
                    if config:
                        try:
                            timeframe_str = config.get("TRADING", "timeframe", fallback="3m").strip().lower()
                            
                            # 안전한 timeframe 파싱
                            if timeframe_str.endswith("m"):
                                timeframe_minutes = int(timeframe_str[:-1])
                            elif timeframe_str.endswith("h"):
                                timeframe_minutes = int(timeframe_str[:-1]) * 60
                            elif timeframe_str.endswith("d"):
                                timeframe_minutes = int(timeframe_str[:-1]) * 1440
                            else:
                                raise ValueError(f"Unknown timeframe format: {timeframe_str}")
                            
                            stale_multiplier = float(
                                config.get("GAJAMI_LIFECYCLE", "stale_threshold_multiplier", fallback="2.0")
                            )
                            
                        except Exception as e:
                            logger.warning(f"[STALE-CONFIG] {symbol} Config 파싱 실패: {e}, 기본값 사용")
                            timeframe_minutes = 3
                            stale_multiplier = 2.0
                    else:
                        logger.warning(f"[STALE-CONFIG] {symbol} Config 없음, 기본값 사용")
                        timeframe_minutes = 3
                        stale_multiplier = 2.0
                    
                    stale_threshold_seconds = timeframe_minutes * 60 * stale_multiplier
                    
                    # ───────────────────────────────
                    # ② 시간 계산 (UTC 통일 + 음수 보호)
                    # ───────────────────────────────
                    from datetime import timezone
                    
                    latest_candle_time = df.index[-1]
                    now = datetime.now()
                    
                    if latest_candle_time.tzinfo:
                        latest_utc = latest_candle_time.astimezone(timezone.utc).replace(tzinfo=None)
                    else:
                        latest_utc = latest_candle_time
                    
                    if now.tzinfo:
                        now_utc = now.astimezone(timezone.utc).replace(tzinfo=None)
                    else:
                        now_utc = now
                    
                    delay_seconds = (now_utc - latest_utc).total_seconds()
                    delay_seconds = max(delay_seconds, 0)  # 음수 방지
                    
                    # ───────────────────────────────
                    # ③ STALE 판정
                    # ───────────────────────────────
                    if delay_seconds > stale_threshold_seconds:
                        data_stale = True
                        stale_latest_time = latest_candle_time
                        
                        logger.warning(
                            f"⚠️ [DATA-STALE] {symbol} "
                            f"| candle_time={latest_candle_time} "
                            f"| delay={delay_seconds:.1f}s ({delay_seconds/60:.1f}분) "
                            f"| threshold={stale_threshold_seconds}s "
                            f"→ 평가는 진행, 실주문만 차단"
                        )
                        
                except Exception as e:
                    logger.error(f"[STALE-CHECK] {symbol} STALE 체크 오류: {e}")
                
                # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
                
                # 데이터 패키지 생성
                try:
                    indicators = TechnicalIndicators.calculate_all_indicators(
                        df, symbol=symbol, timeframe=self.timeframe.value
                    )
                    if not indicators:
                        logger.info(
                            f"⏭️ [POSITION-CHECK-SKIP] {symbol} phase=INDICATOR_HARD_BLOCK | "
                            f"reason=calculate_all_indicators_returned_empty"
                        )
                        continue

                    indicators["_valid"] = True
                    
                    # ④ indicators 메타 정보 저장
                    if "_meta" not in indicators:
                        indicators["_meta"] = {}
                    
                    indicators["_meta"]["data_stale"] = data_stale
                    indicators["_meta"]["stale_latest_time"] = stale_latest_time
                    indicators["_meta"]["delay_seconds"] = delay_seconds
                    
                    # 이전 봉 MA 제공 (CrossState용)
                    if len(df) >= 2:
                        _ma5_s = indicators.get("ma5")
                        _ma10_s = indicators.get("ma10")

                        indicators["ma5_prev"] = (
                            float(_ma5_s.iloc[-2])
                            if _ma5_s is not None and hasattr(_ma5_s, "iloc") and len(_ma5_s) >= 2
                            else 0.0
                        )
                        indicators["ma10_prev"] = (
                            float(_ma10_s.iloc[-2])
                            if _ma10_s is not None and hasattr(_ma10_s, "iloc") and len(_ma10_s) >= 2
                            else 0.0
                        )
                    else:
                        indicators["ma5_prev"] = 0.0
                        indicators["ma10_prev"] = 0.0
                    
                    # 현재 봉 정보
                    indicators["candle_time"] = df.index[-1]
                    indicators["close"] = float(df["close"].iloc[-1])
                    
                    # BACKFILL용 최소 슬라이싱 (메모리 안정성)
                    max_backfill = int(
                        config.get("GAJAMI_LIFECYCLE", "max_backfill_bars", fallback="4")
                    ) if config else 4
                    
                    indicators["df"] = df.tail(max_backfill + 3)
                except Exception as e:
                    logger.warning(f"⚠️ [{symbol}] 지표 계산 실패: {e}")
                    # 🔒 PATCH D: 빈 dict 전파 차단
                    indicators = {
                        "_valid": False,
                        "_calc_failed": True
                    }
                
                # 체제 분석
                regime = await self.analyzer.analyze_market_regime(df, indicators, symbol=symbol)
                
                # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
                # 🆕 불장 모드 정보 추가 (2025-11-26)
                # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
                try:
                    bull_context = self.analyzer.get_bull_market_context()
                    if indicators:  # 🔒 안전 체크
                        indicators.update(bull_context)
                    else:
                        indicators = bull_context
                except Exception as e:
                    logger.warning(f"⚠️ [{symbol}] 불장 정보 추가 실패: {e}")
                
                # ✅ ma_data 항상 계산 (MA 분석 + ma15, ma20 포함)
                ma_data = TechnicalIndicators.calculate_ma_analysis(
                    df=df,
                    timeframe=self.timeframe.value,
                    symbol=symbol,
                    ma_exit_applicable_timeframes=self.signal_analyzer.ma_exit_applicable_timeframes if hasattr(self.signal_analyzer, 'ma_exit_applicable_timeframes') else [],
                    regime=regime,
                    config=self.config,
                    indicators=indicators,
                    ticker_price=current_price  # 🔒 PATCH A: 가격 괴리 검증용
                )

                # 📒 [DECISION-LEDGER · B3] 보유 중 체제 기록 (관측 전용 · 닫힌 봉마다 1 행 · 결정값 읽기만)
                _dlh = getattr(self, "_decision_ledger", None)
                if _dlh is not None:
                    try:
                        _dlh.capture_hold(symbol=symbol, df=df, regime=regime, indicators=indicators, ma_data=ma_data, current_price=current_price, position=position)
                    except Exception:
                        pass

                from Trading_bot_1_core import DataPackage

                data_package = DataPackage(
                    df=df,
                    market=symbol,
                    current_price=current_price,  # ← 🔥 핵심: 이제 611원(1m)으로 전달됨!
                    regime=regime,
                    indicators=indicators,
                    ma_data=ma_data,
                    pattern_data={},
                    timeframe=self.timeframe.value
                )
                
                # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
                # 🔒 매도도 한 사이클에서 단일 체제 판정 사용 (Entry와 동일 원칙)
                # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
                # 설계 원칙:
                # - 체제는 환경(State), 매도는 행동(Action)
                # - Entry에서 확정된 regime을 Exit까지 유지
                # - 보정은 환경(체제)이 아닌 행동(손절/익절/TP/TS)으로 대응
                # - data_package.regime (1039라인)을 Single Source of Truth로 사용
                # 
                # 제거 이유:
                # - 체제 촐삭(flickering) 방지: "57초 만에 TD→RG 전환" 같은 불안 매도 차단
                # - 3분 고정 설계 완전 관철: 포지션 보유 중 30초마다 체제 재판정 금지
                # - 일관성 확립: 동일 df/시각에서 Entry-Exit 간 체제 불일치 구조적 차단
                # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
                
                # ========================================
                # 🔗 P4d: 수익률 버퍼 주입
                # ========================================
                try:
                    if len(df) >= 2 and hasattr(self.performance, 'p4d') and self.performance.p4d:
                        prev_close = float(df['close'].iloc[-2])
                        if prev_close > 0:
                            ret = (current_price - prev_close) / prev_close
                            self.performance.p4d.ingest_tick_returns(symbol, ret)
                except Exception as e:
                    logger.debug(f"[P4d] {symbol} 수익률 주입 실패: {e}")
                
                # ========================================
                # 🔥 수정: 동적 가중치 사용!
                # ========================================
                # 매도 신호
                risk_metrics = await self._get_risk_metrics()
                signal = await self.signal_analyzer.generate_signal(
                    data_package, position,
                    self.performance.get_adaptive_weights(
                        regime=regime.value if regime else None,
                        symbol=position.symbol
                    ) if self.performance else {},
                    self.performance,
                    risk_metrics
                )
                
                # 15 캔들 MA 이탈 체크
                if not signal and ma_data:
                    signal = self._check_ma_breach(position, symbol, current_price, ma_data)
                
                if signal and signal.action == "SELL":
                    # ========================================
                    # 긴급청산이면 쿨다운 설정 (신규!)
                    # ========================================
                    if "긴급청산" in signal.reason or "emergency" in str(signal.reason).lower() if signal.reason else "":
                        self.emergency_sell_cooldown[symbol] = datetime.now()
                        logger.info(f"🕒 {symbol} 긴급청산 쿨다운 시작: {self.emergency_cooldown_seconds}초")
                    
                    await self.execute_trade(position, signal)
                    
            except Exception as e:
                logger.error(f"포지션 체크 오류 {symbol}: {e}")
    
    def _calc_realtime_indicators(self, symbol: str) -> dict:
        """
        30초 폴링 간 ticker 변화량으로 실시간 매수비·거래량 계산
        
        헌법: "체제 판단은 봉단위, 나머지는 30초 갱신"
        - acc_bid_volume: 당일 누적 매수 체결량
        - acc_ask_volume: 당일 누적 매도 체결량
        - 델타(Δ) = 현재 누적 - 이전 폴링 누적 = 30초간 체결량
        """
        import time as _time
        
        result = {
            'rt_volume_delta': 0.0,
            'rt_valid': False,
        }
        
        ticker_data = self._last_ticker_data.get(symbol)
        if not ticker_data:
            return result
        
        try:
            acc_bid = float(ticker_data.get('acc_bid_volume', 0))
            acc_ask = float(ticker_data.get('acc_ask_volume', 0))
            acc_vol = float(ticker_data.get('acc_trade_volume', 0))
            ask_bid = ticker_data.get('ask_bid', 'NEUTRAL')
            
            prev = self._prev_ticker.get(symbol)
            
            if prev is not None:
                d_bid = max(acc_bid - prev['acc_bid_volume'], 0)
                d_ask = max(acc_ask - prev['acc_ask_volume'], 0)
                d_vol = max(acc_vol - prev['acc_trade_volume'], 0)
                
                result['rt_volume_delta'] = d_vol
                result['rt_valid'] = True
                            
            # 현재값 저장 (다음 폴링 비교용)
            self._prev_ticker[symbol] = {
                'acc_bid_volume': acc_bid,
                'acc_ask_volume': acc_ask,
                'acc_trade_volume': acc_vol,
                'ts': _time.time(),
            }
            
        except Exception as e:
            logger.debug(f"[RT-Ticker-ERR] {symbol} | {e}")
        
        return result
    
    async def _validate_entry_price(self, symbol: str, df: pd.DataFrame) -> Optional[float]:
        """진입 가격 검증 - DataFrame 신선도 + 티커 동기화
        Args:
            symbol: 종목 코드
            df: DataFrame
        Returns:
            float: 검증된 현재가 (실패 시 None)
        """
        try:
            # 1. DataFrame 신선도 검증
            if hasattr(df, 'index') and len(df.index) > 0:
                if hasattr(df.index[-1], 'strftime'):
                    latest_time = df.index[-1]
                elif 'timestamp' in df.columns:
                    latest_time = pd.to_datetime(df['timestamp'].iloc[-1])
                else:
                    latest_time = datetime.now()
            else:
                latest_time = datetime.now()
            
            # 데이터 나이 계산
            data_age_hours = (datetime.now() - latest_time).total_seconds() / 3600
            
            # 타임프레임별 허용 나이
            max_age_hours = {
                '1m': 0.1, '3m': 0.2, '5m': 0.33, '10m': 0.5,
                '15m': 0.75, '30m': 1.5, '1h': 3.0, '4h': 10.0, '1d': 60.0
            }
            
            timeframe = self.timeframe.value
            max_age = max_age_hours.get(timeframe, 2.0)
            
            if data_age_hours > max_age:
                logger.warning(
                    f"⚠️ {symbol} 데이터 지연: "
                    f"최신시각: {latest_time.strftime('%Y-%m-%d %H:%M:%S')} "
                    f"({data_age_hours:.2f}h = {data_age_hours*60:.1f}분 > 허용: {max_age:.1f}h) | "
                    f"스킵"
                )
                return None
            
            # 2. 티커로 실시간 가격 조회 (3회 재시도)
            current_price = None
            
            for attempt in range(3):
                try:
                    tickers = await self.data_service.get_ticker([symbol])
                    
                    if tickers and len(tickers) > 0:
                        # 🆕 ticker 전체 보존 (실시간 매수비·거래량 계산용)
                        self._last_ticker_data[symbol] = tickers[0]
                        current_price = float(tickers[0].get('trade_price', 0))
                        
                        if current_price > 0:
                            break
                    
                    if attempt < 2:
                        await asyncio.sleep(0.3)
                
                except Exception as e:
                    logger.debug(f"티커 조회 실패 ({attempt+1}/3): {e}")
                    if attempt < 2:
                        await asyncio.sleep(0.3)
            
            # 티커 조회 실패
            if current_price is None or current_price <= 0:
                logger.error(f"❌ {symbol} 티커 조회 실패")
                return None
            
            # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
            # 🔒 가격 괴리 체크 제거 (SSoT 위반)
            # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
            # - df['close'][-1] (과거 확정 봉) vs ticker (실시간)는 다를 수밖에 없음
            # - 변동성 코인에서 5~10% 차이는 정상
            # - indicators['current_price']가 이미 SSoT로 존재
            # - "판단은 느리게, 트리거는 빠르게" 철학과 일치
            # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
            
            return current_price
            
        except Exception as e:
            logger.error(f"❌ {symbol} 가격 검증 실패: {e}")
            return None

    # 🔭 [1M-TRIGGER-SHADOW] 졸업판정 2026-08-03. 판정법 = 로그의 would_break(0/1)를
    # 진입 MFE30 라벨(.claude/harness/judge_join.py)과 교차 → would_break=1의 정답률이
    # 기준선(35.7%)을 유의하게 넘으면 승격, 아니면 이 함수째 삭제.
    # ⚠️ 비용: 3분봉 매수 성공 건마다 1분봉 REST 1회(주문 완료 후). 졸업 미달 시 비용만 남으므로 반드시 판정할 것.
    async def _shadow_entry_trigger_1m(
        self,
        signal: TradingSignal,
    ) -> None:
        """주문판정 시점의 1분봉 돌파 여부를 사후 기록한다. 진입 판단에는 사용하지 않는다."""
        symbol = signal.symbol
        setup_bar = str(
            signal.indicators.get("current_candle_time")
            or signal.indicators.get("detect_candle_time", "")
        )
        _signal_ts = getattr(signal, "timestamp", None)
        signal_time = pd.Timestamp(_signal_ts or datetime.now())
        if signal_time.tzinfo is not None:
            signal_time = (
                signal_time.tz_convert("Asia/Seoul")
                .tz_localize(None)
            )
        try:
            df_1m = await self.data_service.get_entry_trigger_1m(
                symbol,
                count=3,
            )
            ticker = self._last_ticker_data.get(symbol) or {}
            trigger_price = float(signal.price or 0.0)
            ticker_ts_ms = float(
                ticker.get("timestamp", 0.0) or 0.0
            )

            if ticker_ts_ms > 0.0:
                ticker_time = (
                    pd.to_datetime(
                        ticker_ts_ms,
                        unit="ms",
                        utc=True,
                    )
                    .tz_convert("Asia/Seoul")
                    .tz_localize(None)
                )
                ticker_age_sec = max(
                    0.0,
                    (signal_time - ticker_time).total_seconds(),
                )
            else:
                ticker_age_sec = -1.0
            if df_1m is None or df_1m.empty:
                signal.indicators["entry_trigger_1m_ready"] = 0
                logger.info(
                    f"🔭 [1M-TRIGGER-SHADOW] {symbol} | "
                    f"setup_bar={setup_bar} "
                    f"ready=0 reason=NO_1M_DATA "
                    f"(SHADOW·진입 무영향)"
                )
                return
            eligible = df_1m[
                (
                    df_1m.index
                    + pd.Timedelta(minutes=1)
                ) <= signal_time
            ]
            if eligible.empty:
                signal.indicators["entry_trigger_1m_ready"] = 0
                logger.info(
                    f"🔭 [1M-TRIGGER-SHADOW] {symbol} | "
                    f"setup_bar={setup_bar} "
                    f"ready=0 reason=NO_1M_AT_SIGNAL "
                    f"(SHADOW·진입 무영향)"
                )
                return

            last_closed = eligible.iloc[-1]
            last_closed_time = pd.Timestamp(
                eligible.index[-1]
            )
            now_kst = signal_time
            bar_age_sec = max(
                0.0,
                (
                    now_kst
                    - (
                        last_closed_time
                        + pd.Timedelta(minutes=1)
                    )
                ).total_seconds(),
            )
            prev_high = float(last_closed["high"])
            prev_close = float(last_closed["close"])
            ready = bool(
                trigger_price > 0.0
                and prev_high > 0.0
                and 0.0 <= ticker_age_sec <= 10.0
                and bar_age_sec <= 120.0
            )
            would_break = int(
                ready
                and trigger_price > prev_high
            )
            signal.indicators["entry_trigger_1m_ready"] = int(
                ready
            )
            signal.indicators["entry_trigger_1m_would_break"] = (
                would_break
            )
            signal.indicators["entry_trigger_1m_setup_bar"] = (
                setup_bar
            )
            signal.indicators["entry_trigger_1m_bar"] = str(
                last_closed_time
            )
            signal.indicators["entry_trigger_1m_price"] = (
                trigger_price
            )
            signal.indicators["entry_trigger_1m_prev_high"] = (
                prev_high
            )
            signal.indicators["entry_trigger_1m_prev_close"] = (
                prev_close
            )
            signal.indicators["entry_trigger_1m_bar_age_sec"] = (
                bar_age_sec
            )
            signal.indicators["entry_trigger_1m_ticker_age_sec"] = (
                ticker_age_sec
            )
            logger.info(
                f"🔭 [1M-TRIGGER-SHADOW] {symbol} | "
                f"setup_bar={setup_bar} "
                f"ready={int(ready)} "
                f"would_break={would_break} "
                f"setup_px={format_price_krw(float(signal.price or 0.0))} "
                f"trigger_px={format_price_krw(trigger_price)} "
                f"prev_1m_high={format_price_krw(prev_high)} "
                f"prev_1m_close={format_price_krw(prev_close)} "
                f"bar={last_closed_time} "
                f"bar_age={bar_age_sec:.0f}s "
                f"ticker_age={ticker_age_sec:.1f}s "
                f"(SHADOW·진입 무영향)"
            )
        except Exception as e:
            signal.indicators["entry_trigger_1m_ready"] = 0
            logger.info(
                f"🔭 [1M-TRIGGER-SHADOW] {symbol} | "
                f"setup_bar={setup_bar} "
                f"ready=0 reason=ERR:{str(e)[:80]} "
                f"(SHADOW·진입 무영향)"
            )

    async def _check_entries(self, preserve_cash: bool = False):
        """진입 기회 체크 및 실행 - 간소화 버전
        Args:
            preserve_cash: True이면 캐시 보존 모드 (고신뢰도만 허용)
        """
        # 🔥 실행 시간 측정 시작
        entry_start = datetime.now()
        
        # 🔧 안전 초기화 (try 블록 밖에서)
        indicators = {}
        symbol = "UNKNOWN"
        
        try:
            # 포지션 여유 체크
            async with self._positions_lock:
                current_position_count = len(self.positions)
            
            if current_position_count >= self.settings.max_positions:
                logger.info(
                    f"🚫 [BOT-ENTRY-BLOCK] phase=POSITION_FULL | "
                    f"positions={current_position_count}/{self.settings.max_positions} "
                    f"→ 진입체크 전체 스킵"
                )
                return
            
            # 리스크 레벨이 너무 높으면 무조건 중단
            if self.risk_manager.current_level in ['emergency', 'shutdown']:
                logger.debug(
                    f"[진입체크] 리스크 레벨 심각 "
                    f"({self.risk_manager.current_level}) - 진입 완전 차단"
                )
                return
            
            # 체크할 종목 목록
            if not self.top_markets:
                logger.info(
                    f"🚫 [BOT-ENTRY-BLOCK] phase=NO_TOP_MARKETS | "
                    f"top_markets=0 → 진입 체크 건너뜀"
                )
                return
            
            # 🆕 우선 재체크 큐 소비
            priority_symbols = []
            for sym in list(self._priority_recheck.keys()):
                if self._priority_recheck[sym] > 0:
                    priority_symbols.append(sym)
                    self._priority_recheck[sym] -= 1
                    logger.debug(f"[우선재체크] {sym} (남은횟수: {self._priority_recheck[sym]})")
                else:
                    del self._priority_recheck[sym]
            
            # 🔒 [EXCLUDED-MARKETS-DATA-LAYER-0828]
            # 전 KRW 캐시 저장은 유지한다.
            # 명시적 제외종목은 진입 전용 후보·확정 REST 경로에서만 제거한다.
            _entry_excluded = set(
                getattr(self, "EXCLUDED_MARKETS", set()) or set()
            )
            # __PRICONSUME613__ ★priority ★소비 seam — ★관측 전용 (★판정 없음)
            #   ★소비 loop(L1956~1962) ★직후 · ★exclusion filter ★직전
            #   ★raw_pri = ★필터 전 ordered · ★queue_after 는 ★0 키도 ★그대로
            #   ★T7 은 ★1→0 키를 ★이번엔 남기고 ★다음 소비에서 삭제한다
            try:
                import json as _pcj
                self._pri_consume_seq = int(getattr(self, "_pri_consume_seq", 0)) + 1
                logger.info("[PRI-PROV-CONSUME] " + _pcj.dumps({
                    "consume_seq": int(self._pri_consume_seq),
                    "cycle": int(self._cycle_count),
                    "raw_pri": list(priority_symbols),
                    "excluded": sorted(_entry_excluded),
                    "queue_after": [[k, int(v)] for k, v
                                    in self._priority_recheck.items()],
                }, ensure_ascii=False, separators=(",", ":")))
            except Exception as _pce:
                logger.warning("[PRI-CONSUME-ERR] %s" % type(_pce).__name__)
            if _entry_excluded and priority_symbols:
                priority_symbols = [
                    s
                    for s in priority_symbols
                    if s not in _entry_excluded
                ]
            # 일반 선정 풀은 먼저 확정한다.
            _normal_pool = [
                s
                for s in self.top_markets
                if s not in _entry_excluded
            ][:self.top_markets_count]
            _already_scanned = set(_normal_pool) | set(priority_symbols)
            # 🌱 [FRESH-GC-ADMIT] 티커 미니GC 전 유니버스 감지
            # → REST 확정봉 갱신 → 아래 A-1에서 같은 사이클 신규GC 검증.
            # 동일 종목은 3분봉당 1회만 갱신해 REST 중복과 형성봉 오염을 막는다.
            _fga_targets = []
            try:
                if self.timeframe.value == "3m":
                    _fga_tf_sec = max(
                        1,
                        self._timeframe_to_seconds(
                            self.timeframe.value
                        ),
                    )
                    _fga_bucket = int(
                        time.time() // _fga_tf_sec
                    )
                    _fga_seen = getattr(
                        self,
                        "_fresh_gc_admit_bucket",
                        None,
                    )
                    if not isinstance(_fga_seen, dict):
                        self._fresh_gc_admit_bucket = {}
                        _fga_seen = self._fresh_gc_admit_bucket
                    _fga_targets = sorted(
                        s
                        for s in (
                            getattr(
                                self.analyzer,
                                "_fresh_gc_pool",
                                set(),
                            )
                            or set()
                        )
                        if s
                        and s not in _entry_excluded
                        and s not in _already_scanned
                        and self.analyzer._gc_birth_watchlist.get(
                            s, 0
                        ) <= 0
                        and _fga_seen.get(s) != _fga_bucket
                    )[:20]
                    _fga_rest_ok = 0
                    for _fga_sym in _fga_targets:
                        # 성공·실패와 무관하게 같은 3분봉에서는 1회만 시도한다.
                        # 실패 종목의 앞 20칸 반복 점유로 후순위 fresh GC가
                        # 굶는 것을 방지한다.
                        _fga_seen[_fga_sym] = _fga_bucket
                        try:
                            _fga_df = (
                                await self.data_service.get_dataframe(
                                    _fga_sym,
                                    self.timeframe.value,
                                    count=200,
                                    force_refresh=True,
                                )
                            )
                            if (
                                is_valid_df(_fga_df)
                                and len(_fga_df) >= 20
                            ):
                                _fga_rest_ok += 1
                        except Exception:
                            continue
                    if _fga_targets:
                        # 🔒 [FRESH-GC-ADMIT-SYMBOL-OBSERVE-0830] ★449
                        # _fga_targets(L1981)는 sorted(key없음)[:20] = 알파벳순 컷이다.
                        # fresh 풀이 20 초과인 시각이 52.1%(중앙 21·max 58)인데
                        # 실제 target은 1~2뿐이고 어떤 종목이 잘리는지 로그에 없어
                        # 편향 여부를 판정할 수 없었다. 관측 전용 · 판정 변화 0 · 신규 상수 0.
                        _fga_pool_n = len(
                            getattr(self.analyzer, "_fresh_gc_pool", set())
                            or set()
                        )
                        logger.info(
                            f"🌱 [FRESH-GC-ADMIT] "
                            f"target={len(_fga_targets)} "
                            f"rest_ok={_fga_rest_ok} "
                            f"bucket={_fga_bucket} "
                            f"pool={_fga_pool_n} "
                            f"syms={','.join(_fga_targets)} "
                            f"→ A-1 확정봉 GC 검증"
                        )
            except Exception as _fga_e:
                logger.warning(
                    f"⚠️ [FRESH-GC-ADMIT-ERR] "
                    f"{type(_fga_e).__name__}:"
                    f"{str(_fga_e)[:120]}"
                )
            # 🌱 [GC-BIRTH-CACHE-SCAN]
            # 선정 풀 밖 캐시 종목만 확정봉 신규 GC를 검사한다.
            try:
                _gc_scan_cache = getattr(
                    self.data_service, "_dataframe_cache", None
                )
                _gc_scan_hits = 0
                _tf_sec = self._timeframe_to_seconds(self.timeframe.value)

                if isinstance(_gc_scan_cache, dict):
                    for _uc_sym, _uc_df in list(_gc_scan_cache.items()):
                        try:
                            if (
                                not _uc_sym
                                or _uc_sym in _entry_excluded
                                or _uc_sym in _already_scanned
                                or self.analyzer._gc_birth_watchlist.get(_uc_sym, 0) > 0
                                or not is_valid_df(_uc_df)
                                or len(_uc_df) < 20
                            ):
                                continue

                            _last_ts = pd.Timestamp(_uc_df.index[-1])
                            _now_ts = (
                                pd.Timestamp.now(tz=_last_ts.tz)
                                if _last_ts.tz is not None
                                else pd.Timestamp.now()
                            )
                            _cache_age_sec = max(
                                0.0, (_now_ts - _last_ts).total_seconds()
                            )
                            if _cache_age_sec > (_tf_sec * 2):
                                continue
                            _last_is_open = _cache_age_sec < _tf_sec
                            _prev_i, _closed_i = (
                                (-3, -2) if _last_is_open else (-2, -1)
                            )

                            _close = _uc_df["close"]
                            _ma5 = _close.rolling(5).mean()
                            _ma10 = _close.rolling(10).mean()

                            _prev5 = float(_ma5.iloc[_prev_i])
                            _prev10 = float(_ma10.iloc[_prev_i])
                            _closed5 = float(_ma5.iloc[_closed_i])
                            _closed10 = float(_ma10.iloc[_closed_i])

                            if any(
                                _v != _v
                                for _v in (
                                    _prev5, _prev10, _closed5, _closed10
                                )
                            ):
                                continue

                            if (
                                _prev5 <= _prev10
                                and _closed5 > _closed10
                            ):
                                self.analyzer.register_gc_birth(_uc_sym)
                                self._gc_birth_ttl_last_candle[_uc_sym] = (
                                    _uc_df.index[-1]
                                )
                                _gc_scan_hits += 1

                        except Exception:
                            continue

                if _gc_scan_hits > 0:
                    logger.info(
                        f"🌱 [GC-BIRTH-CACHE-SCAN] "
                        f"선정 밖 확정봉 신규GC={_gc_scan_hits} | "
                        f"cache={len(_gc_scan_cache)}"
                    )

            except Exception as _gc_scan_e:
                logger.error(
                    f"🛑 [GC-BIRTH-CACHE-SCAN-ERR] "
                    f"{type(_gc_scan_e).__name__}:"
                    f"{str(_gc_scan_e)[:120]}"
                )

            # 전체 후보 목록
            # 🔒 [EXCLUDED-MARKETS-DATA-LAYER-0828]
            # 제외종목의 기존 GC watchlist 상태가 남아 있어도
            # 진입 후보/REST 우선풀에는 편입하지 않는다.
            # 🔒 [FGA-SAME-CYCLE-PRIORITY-0902] ★492
            # 방금 FRESH-GC-ADMIT으로 확정한 age=0 GC를
            # 오래된 watchlist보다 앞에 두어 같은 사이클 hard-cap에 보존한다.
            # 새 종목을 만드는 것이 아니라 기존 확정종목의 순서만 교정한다.
            _fga_confirmed = [
                s
                for s in _fga_targets
                if (
                    self.analyzer._gc_birth_watchlist.get(s, 0) > 0
                    and s not in _entry_excluded
                )
            ]

            _gc_force = list(dict.fromkeys(
                _fga_confirmed
                + [
                    s
                    for s, n in self.analyzer._gc_birth_watchlist.items()
                    if n > 0 and s not in _entry_excluded
                ]
            ))

            # GC-BIRTH-WATCHLIST/우선재체크 → 현재 top_markets 순서로 편입
            _priority_pool = list(
                dict.fromkeys(_gc_force + priority_symbols)
            )

            # 2) 합집합 → 중복 제거 → top_markets_count 하드캡
            all_markets = list(dict.fromkeys(
                _priority_pool + _normal_pool
            ))[:self.top_markets_count]
            
            if not hasattr(self, "_entry_cycle_count"):
                self._entry_cycle_count = 0
            
            total = len(all_markets)
            
            # 🔥 전체 종목 매번 체크 (1사이클 완전 스캔)
            BATCH_SIZE = total
            
            if all_markets:
                start = (self._entry_cycle_count * BATCH_SIZE) % total
                end = start + BATCH_SIZE
                
                # 순환 처리
                if end <= total:
                    markets_to_check = all_markets[start:end]
                else:
                    markets_to_check = all_markets[start:] + all_markets[:end - total]
                
                # 🆕 우선 재체크 종목 추가
                for sym in priority_symbols:
                    if sym not in markets_to_check:
                        markets_to_check.insert(0, sym)
                
                # 🔒 배치 상한 유지 (API 폭증 방지)
                if len(markets_to_check) > BATCH_SIZE:
                    markets_to_check = markets_to_check[:BATCH_SIZE]
            else:
                markets_to_check = []
            
            self._entry_cycle_count += 1
            
            # __SELPROV601__ ★선정 provenance — ★관측 전용 (★판정 없음)
            #   ★cycle    = self._cycle_count (★ENTRY-CYCLE-PRE canonical · L828)
            #   ★rr_cycle = self._entry_cycle_count ([라운드로빈] local)
            #   ★force_fga / force_watch 로 ★F 의 ★출처를 ★분리 보존
            #   ★pre_cap 은 ★같은 입력으로 ★재계산만 (★L2185~2187 원식 ★불변)
            try:
                import json as _spj
                _sp_pre = list(dict.fromkeys(_priority_pool + _normal_pool))
                _sp_fw = [s for s, n in self.analyzer._gc_birth_watchlist.items()
                          if n > 0 and s not in _entry_excluded]
                logger.info("[SEL-PROV] " + _spj.dumps({
                    "cycle": int(self._cycle_count),
                    "rr_cycle": int(self._entry_cycle_count),
                    "cap": int(self.top_markets_count),
                    "force": list(_gc_force),
                    "force_fga": list(_fga_confirmed),
                    "force_watch": _sp_fw,
                    "pri": list(priority_symbols),
                    "norm": list(_normal_pool),
                    "priority_pool": list(_priority_pool),
                    "pre_cap": _sp_pre,
                    "cap35": list(all_markets),
                    "mtc": list(markets_to_check),
                }, ensure_ascii=False, separators=(",", ":")))
            except Exception as _spe:
                logger.warning("[SEL-PROV-ERR] %s" % type(_spe).__name__)
            
            logger.info(
                f"[라운드로빈] "
                f"사이클#{self._entry_cycle_count} | "
                f"{len(markets_to_check)}개 선정 "
                f"(우선:{len(priority_symbols)})"
            )
                        
            # ========================================
            # 🆕 DF 로딩: 데이터 기반 판단 (WS 우선 / STALE→REST)
            # - 캐시 최신 → 사용
            # - 캐시 없음/STALE → REST (병렬)
            # ========================================
            
            df_map = {}
            rest_symbols = []  # REST 필요한 종목 수집
            
            # 타임프레임 초 계산
            tf = str(self.timeframe.value or "3m").lower()
            tf_map = {
                "1m": 60, "3m": 180, "5m": 300, "10m": 600,
                "15m": 900, "30m": 1800, "1h": 3600, "4h": 14400, "1d": 86400,
            }
            timeframe_seconds = tf_map.get(tf, 180)
            # 🔒 [2026-08-03] 2배(=1봉 누락 허용) → 1배로 축소.
            #  last_bar 은 '진행 중인 봉'이라 정상이면 last_age ∈ [0, timeframe).
            #  180s 를 넘으면 다음 봉이 이미 마감됐는데 df 에 없다는 뜻이고,
            #  기존 360s 임계는 그 1봉 누락을 정확히 허용했다.
            #  실측(2일 FRESH-MOVER-DIAG 22,549건):
            #    last_age 중앙 160s / 75% 302s / 90% 573s / 최대 9749s(54봉)
            #    180~360s 구간 5,054건 = 22.4%  ← MA 창이 1봉 밀린 채 판정
            #  확정봉 49건 대조에서 봇 GC/DC 판정이 거래소와 20.4% 상반한 것과 규모 일치.
            #  MA10 창이 통째로 과거로 밀려 dur·TLC·soleus·birth_state 가 전부 오염된다.
            #  ⚠️ REST 재구축 2.1배 증가 예상(4,760 → 9,814건/2일). API 429 감시 필수.
            stale_threshold = timeframe_seconds  # 1봉. 봉 마감 즉시 재구축
            
            # 1단계: 캐시 체크 + REST 필요 종목 수집
            for symbol in markets_to_check:
                use_rest = False
                
                try:
                    cached = self.data_service._dataframe_cache.get(symbol)
                    
                    if cached is None or cached.empty:
                        # 캐시 없음 → REST
                        use_rest = True
                    else:
                        # STALE 체크 (2배 여유)
                        try:
                            last_ts = cached.index[-1]
                            now = pd.Timestamp.now(tz=last_ts.tz) if last_ts.tz else pd.Timestamp.now()
                            age = (now - last_ts).total_seconds()
                            # 🔧 내부 갭 캐시(마지막 봉만 최신·중간 결손) → REST 무조건 재구축.
                            #    age만 보던 판정이 05:15→05:42 갭 DF를 '최신' 오인해 정답봉(05:44 GC)
                            #    심사 누락시킴. 기존 MAX_GAP 재사용(새 threshold 0). get_dataframe REST성공=
                            #    확정봉 SSoT 무조건 업서트(정책 일치, 원복된 조건부 갭필 아님).
                            _dense_ok, _dense_reason = TechnicalIndicators._is_candle_dense_enough(
                                cached, self.timeframe.value, symbol
                            )

                            # 🔒 REST가 같은 MAX_GAP을 실제 거래소 정본에서도 확인했다면
                            # 5×TF TTL 동안 MAX_GAP 재확인만 생략한다.
                            # freshness는 독립 축: age > stale_threshold면 아래 REST repair로 흐른다.
                            _gap_hold_until = float(
                                self._rest_gap_confirmed_until.get(symbol, 0.0) or 0.0
                            )
                            if (
                                str(_dense_reason).startswith("MAX_GAP_")
                                and time.time() < _gap_hold_until
                                and age <= stale_threshold
                            ):
                                df_map[symbol] = cached
                                logger.debug(
                                    f"⏸️ [REST-GAP-HOLD] {symbol} | "
                                    f"reason={_dense_reason} "
                                    f"left={max(0.0, _gap_hold_until - time.time()):.0f}s "
                                    f"→ REST-confirmed CACHE 사용·MAX_GAP 재확인만 생략"
                                )
                                continue

                            # [2026-07-31] _dense_ok를 계산만 하고 버리던 것을 판단에 연결.
                            # MAX_GAP_ 외 밀도 결함(DF_LEN_/RECENT60_COUNT_)도 REST 재구축 대상.
                            # 미연결 시 WS로 마지막 봉만 들어와 age가 짧으면 8/20 결손 캐시를 그대로 사용 →
                            # MA10 최대 0.53% 오차 → DC 소실 → dur 오염 → 정답봉 STALE 오차단
                            # (UNI 07-31 13:57 dur=19 실측).
                            if age > stale_threshold or not _dense_ok:
                                use_rest = True
                                logger.info(
                                    f"🔄 [CACHE-REST-REPAIR] {symbol} | "
                                    f"age={age:.0f}s reason={_dense_reason} → REST 재구축"
                                )
                            else:
                                # 캐시 최신 → 사용
                                df_map[symbol] = cached
                        except Exception:
                            use_rest = True
                    
                    if use_rest:
                        rest_symbols.append(symbol)
                
                except Exception as e:
                    logger.debug(f"[Cache-Check] {symbol} 실패: {e}")
                    rest_symbols.append(symbol)
            
            # 2단계: REST 필요 종목 병렬 실행
            rest_count = 0
            
            if rest_symbols:
                logger.info(f"[REST-Start] {len(rest_symbols)}개 종목 병렬 조회 시작")
                
                DF_CONCURRENCY_LIMIT = 5
                semaphore = asyncio.Semaphore(DF_CONCURRENCY_LIMIT)
                
                async def fetch_df_rest(symbol):
                    """STALE/내부-gap 복구용 REST 조회 (병렬)."""
                    try:
                        async with semaphore:
                            # t4 계약: REST 실패 시 이 cached 객체를 그대로 반환한다.
                            _cached_before = self.data_service._dataframe_cache.get(symbol)
                            df = await self.data_service.get_dataframe(
                                symbol,
                                self.timeframe.value,
                                count=200,
                                force_refresh=True
                            )
                            _cache_fallback = bool(
                                df is not None
                                and _cached_before is not None
                                and df is _cached_before
                            )
                            return symbol, df, _cache_fallback
                    except Exception as e:
                        logger.debug(f"[REST-Fetch] {symbol} 실패: {e}")
                        return symbol, None, False
                
                # 병렬 실행
                rest_start = time.time()
                rest_tasks = [
                    asyncio.create_task(fetch_df_rest(symbol))
                    for symbol in rest_symbols
                ]
                rest_results = await asyncio.gather(*rest_tasks, return_exceptions=False)
                
                # 결과 병합
                for symbol, df, _cache_fallback in rest_results:
                    if df is None or isinstance(df, Exception) or getattr(df, "empty", False):
                        continue
                    # 🔒 [CACHE-MAX-GAP-REST-0822-R4]
                    # REST 실패 시 t4는 기존 cached 객체를 그대로 반환한다.
                    # 이 경우만 복구 실패로 본다. 실제 REST 성공 DF의 gap은 거래소 정본이다.
                    if _cache_fallback:
                        logger.warning(
                            f"🚫 [REST-FALLBACK-CACHE] {symbol} | "
                            f"REST 실패 → 기존 CACHE 반환 "
                            f"→ 이번 사이클 진입 DF에서 제외"
                        )
                        continue
                    _post_dense_ok, _post_dense_reason = (
                        TechnicalIndicators._is_candle_dense_enough(
                            df, self.timeframe.value, symbol
                        )
                    )
                    if str(_post_dense_reason).startswith("MAX_GAP_"):
                        # 거래소 REST 자체에도 gap이 있으면 실제 무거래 구간일 수 있다.
                        # 5×TF TTL 동안 MAX_GAP 재확인만 억제한다.
                        # freshness stale(age > stale_threshold)은 hold 대상이 아니다.
                        self._rest_gap_confirmed_until[symbol] = (
                            time.time() + timeframe_seconds * 5
                        )
                        logger.info(
                            f"🟡 [REST-GAP-CONFIRMED] {symbol} | "
                            f"reason={_post_dense_reason} "
                            f"hold={timeframe_seconds * 5}s "
                            f"→ REST 정본 사용·5×TF MAX_GAP 재확인만 억제"
                        )
                    else:
                        self._rest_gap_confirmed_until.pop(symbol, None)
                    df_map[symbol] = df
                    rest_count += 1
                
                rest_elapsed = time.time() - rest_start
                logger.info(
                    f"[REST-Done] {rest_count}/{len(rest_symbols)}개 완료 | "
                    f"{rest_elapsed:.2f}초"
                )
            
            # 최종 결과
            ws_count = len(df_map) - rest_count  # 🔥 WS 데이터 개수 계산
            logger.info(
                f"[사이클#{self._entry_cycle_count} DF-Load] 완료 {len(df_map)}개 | "
                f"WS={ws_count}, REST={rest_count}개"
            )
            
            # 🔥 큐 로직 제거: DF 로딩은 오직 데이터만 판단
            # - 큐는 이벤트 기반 스캔 트리거용으로만 사용
            # - DF 로딩과 큐를 섞으면 경계 파괴됨
            
            # ========================================
            # 🔥 골든크로스 신선도 필터 (ATR 로직 대체)
            # ========================================
            use_gc_filter = self.config.getboolean(
                'MARKET_SELECTION',
                'use_gc_freshness_filter',
                fallback=True
            )
            
            if use_gc_filter:
                gc_max_candles = self.config.getint(
                    'MARKET_SELECTION',
                    'gc_max_candles_since_cross',
                    fallback=3
                )
                
                gc_passed = []
                gc_birth_priority = []  # ⚡ GC 탄생/Watchlist 종목 최우선 처리용
                gc_rebound_symbols = set()  # 빈 set 유지 (entry_type 태깅용)
                gc_stats = {'total': 0, 'passed': 0, 'no_gc': 0, 'stale': 0, 'error': 0}
                # 🔒 [EXCLUSION-ENTRY-0822] 캐시는 전 KRW 종목을 유지하되,
                #    명시적 제외 종목은 미니GC 판정부터 진입 경로에서 제외한다.
                _excluded_set = set(getattr(self, "EXCLUDED_MARKETS", set()) or set())
                # 🌱 [G6] WS 미구독 종목 GC-BIRTH-WATCHLIST TTL SSoT 보강
                # WS 구독 종목: on_candle_close_callback에서 tick (기존 유지)
                # WS 미구독 priority-only 종목: df.index[-1] candle_key 기준 1봉 1회 tick
                _ws_subscribed_set = set(self.top_markets[:self.top_markets_count])

                def _tick_gc_birth_ttl_if_needed(_sym, _df):
                    if _sym in _ws_subscribed_set:
                        return
                    if self.analyzer._gc_birth_watchlist.get(_sym, 0) <= 0:
                        self._gc_birth_ttl_last_candle.pop(_sym, None)
                        return
                    try:
                        _candle_key = _df.index[-1]
                    except Exception:
                        _candle_key = None
                    if _candle_key is None:
                        return
                    if self._gc_birth_ttl_last_candle.get(_sym) == _candle_key:
                        return
                    self._gc_birth_ttl_last_candle[_sym] = _candle_key
                    self.analyzer.tick_gc_birth_watchlist(_sym)
                        
                for sym in markets_to_check:
                    if sym in _excluded_set:
                        continue
                    gc_stats['total'] += 1

                    df = df_map.get(sym)
                    _diag_source = "REST" if sym in rest_symbols else "CACHE"
                    _diag_target = sym in _priority_pool

                    if df is None or isinstance(df, Exception):
                        if _diag_target:
                            logger.info(
                                f"🔍 [FRESH-MOVER-DIAG] {sym} | "
                                f"source={_diag_source} "
                                f"priority={int(sym in priority_symbols)} "
                                f"gc_force={int(sym in _gc_force)} "
                                f"result=DF_MISSING"
                            )
                        gc_stats['error'] += 1
                        continue

                    if not is_valid_df(df) or len(df) < 20:
                        if _diag_target:
                            logger.info(
                                f"🔍 [FRESH-MOVER-DIAG] {sym} | "
                                f"source={_diag_source} "
                                f"priority={int(sym in priority_symbols)} "
                                f"gc_force={int(sym in _gc_force)} "
                                f"df_len={len(df) if df is not None else 0} "
                                f"result=DF_INVALID_OR_SHORT"
                            )
                        gc_stats['error'] += 1
                        continue

                    # MA 계산 (GC 유무 확인용)
                    _rt_price = self.data_manager.get_realtime_price(sym)
                    if _rt_price and _rt_price > 0:
                        self._apply_intrabar_price_to_df(df, _rt_price)
                    tail = df.tail(20)
                    ma5  = tail["close"].rolling(5).mean()
                    ma10 = tail["close"].rolling(10).mean()

                    # GC 유지 여부만 확인 (선별)
                    # crossed_age/GC-Rebound/slope/pullback 판단은 bot5(SSoT) 위임
                    # GC gap/MA = iloc[-1] RT, slope만 iloc[-2]
                    # 🔒 확정봉(iloc[-2]) GC 생존 시 intrabar RT flicker로 NO_GC 떨구지 않음
                    #    중기 MA에 현재봉 사용 금지 — 확정봉 골크 판정은 bot5(SSoT) 위임
                    _m5_c       = float(ma5.iloc[-1])
                    _m10_c      = float(ma10.iloc[-1])
                    _m5_closed  = float(ma5.iloc[-2])   # len(df)<20 체크로 보장
                    _m10_closed = float(ma10.iloc[-2])
                    _closed_gc  = bool(_m5_closed > _m10_closed)
                    _gap_pct    = (
                        abs(_m5_c - _m10_c) / _m10_c * 100.0
                        if _m10_c > 0 else 999.0
                    )
                    # elif 공유: 여기서 산출해 중복 제거
                    _m5_slope = (
                        float(ma5.iloc[-2]) - float(ma5.iloc[-3])
                        if len(ma5) >= 3 else 0.0
                    )
                    _SLOPE_NOISE_EPS = 1e-4

                    if (
                        _m5_c < _m10_c
                        and _gap_pct > 0.1
                        and not _closed_gc
                    ):
                        _diag_verdict = "NO_GC_HARD"
                    elif (
                        _m5_c < _m10_c
                        and not _closed_gc
                        and _m5_slope < -_SLOPE_NOISE_EPS
                    ):
                        _diag_verdict = "NO_GC_TRAINROAD_DOWN"
                    else:
                        _diag_verdict = "PASS"

                    # 🆕 [FRESH-MOVER-DIAG] 관측전용 — 판단·DF·REST 변경 없음
                    if _diag_target:
                        try:
                            _last_ts  = pd.Timestamp(df.index[-1])
                            _closed_ts = pd.Timestamp(df.index[-2])
                            _tz = _last_ts.tz
                            _now_ts = (
                                pd.Timestamp.now(tz=_tz)
                                if _tz is not None
                                else pd.Timestamp.now()
                            )
                            _last_age_sec = max(
                                0.0, (_now_ts - _last_ts).total_seconds()
                            )
                            _closed_end_ts = _closed_ts + pd.Timedelta(
                                seconds=timeframe_seconds
                            )
                            _closed_age_sec = max(
                                0.0, (_now_ts - _closed_end_ts).total_seconds()
                            )
                            _tail_idx = pd.DatetimeIndex(df.index[-20:])
                            _tail20_span_min = max(
                                0.0,
                                (_tail_idx[-1] - _tail_idx[0]).total_seconds() / 60.0
                            )
                            _tail_gaps = (
                                _tail_idx.to_series().diff().dropna()
                                .dt.total_seconds() / 60.0
                            )
                            _tail20_max_gap_min = (
                                float(_tail_gaps.max()) if len(_tail_gaps) > 0 else 0.0
                            )
                            _expected_span_min = 19.0 * timeframe_seconds / 60.0
                            _density20 = (
                                min(1.0, _expected_span_min / _tail20_span_min)
                                if _tail20_span_min > 0 else 0.0
                            )
                            logger.info(
                                f"🔍 [FRESH-MOVER-DIAG] {sym} | "
                                f"source={_diag_source} "
                                f"priority={int(sym in priority_symbols)} "
                                f"gc_force={int(sym in _gc_force)} | "
                                f"df_len={len(df)} "
                                f"last_bar={str(_last_ts)[:16]} "
                                f"last_age={_last_age_sec:.0f}s "
                                f"closed_bar={str(_closed_ts)[:16]} "
                                f"closed_end_age={_closed_age_sec:.0f}s | "
                                f"tail20_span={_tail20_span_min:.0f}m "
                                f"expected={_expected_span_min:.0f}m "
                                f"density20={_density20:.2f} "
                                f"max_gap={_tail20_max_gap_min:.1f}m | "
                                f"rt_ma5={_m5_c:.4f} rt_ma10={_m10_c:.4f} "
                                f"gap={_gap_pct:.3f}% "
                                f"closed_ma5={_m5_closed:.4f} "
                                f"closed_ma10={_m10_closed:.4f} "
                                f"closed_gc={int(_closed_gc)} "
                                f"slope5={_m5_slope:+.5f} | "
                                f"verdict={_diag_verdict}"
                            )
                        except Exception as _diag_e:
                            logger.debug(f"[FRESH-MOVER-DIAG-ERR] {sym} {_diag_e}")

                    if _m5_c < _m10_c and _gap_pct > 0.1 and not _closed_gc:
                        gc_stats['no_gc'] += 1
                        logger.debug(f"[GC-FILTER] {sym} NO_GC [bot7 선계산]")
                        _tick_gc_birth_ttl_if_needed(sym, df)
                        if self.analyzer._gc_birth_watchlist.get(sym, 0) > 0:
                            gc_passed.append(sym)
                            gc_birth_priority.append(sym)  # ⚡ Watchlist 종목 최우선 큐 등록
                            gc_stats['passed'] += 1
                            logger.info(
                                f"[GC-BIRTH-WATCHLIST] {sym} NO_GC → 강제편입 "
                                f"(잔여={self.analyzer._gc_birth_watchlist.get(sym, 0)}봉)"
                            )
                        continue
                    elif _m5_c < _m10_c and not _closed_gc:  # gap≤0.1% 기차길 → 방향성 확인

                        if _m5_slope < -_SLOPE_NOISE_EPS:
                            gc_stats['no_gc'] += 1
                            logger.debug(
                                f"[GC-FILTER] {sym} 기차길 but MA5 하향 → NO_GC | "
                                f"gap={_gap_pct:.3f}% slope={_m5_slope:.5f} eps={_SLOPE_NOISE_EPS:g}"
                            )
                            _tick_gc_birth_ttl_if_needed(sym, df)
                            if self.analyzer._gc_birth_watchlist.get(sym, 0) > 0:
                                gc_passed.append(sym)
                                gc_birth_priority.append(sym)
                                gc_stats['passed'] += 1
                                logger.info(
                                    f"[GC-BIRTH-WATCHLIST] {sym} TRAINROAD-DOWN → 강제편입 "
                                    f"(잔여={self.analyzer._gc_birth_watchlist.get(sym, 0)}봉)"
                                )
                            continue

                        logger.info(
                            f"[GC-FILTER] {sym} 기차길 GC 인정(평탄/상향) | "
                            f"gap={_gap_pct:.3f}% slope={_m5_slope:.5f} eps={_SLOPE_NOISE_EPS:g}"
                        )

                    # 선정 종목 RT 신규 GC 감지:
                    # 직전 확정봉 non-GC(DC/EQ) + 현재 형성봉 strict GC를 확인한다.
                    _is_new_gc_birth = False
                    if (
                        len(ma5) >= 3
                        and _m5_closed <= _m10_closed
                        and _m5_c > _m10_c
                        and self.analyzer._gc_birth_watchlist.get(sym, 0) <= 0
                    ):
                        self.analyzer.register_gc_birth(sym)
                        # 🌱 [G6] WS 미구독 신규 GC: 등록봉을 기준점으로 저장 → 다음 봉부터 TTL 소비
                        if sym not in _ws_subscribed_set:
                            try:
                                self._gc_birth_ttl_last_candle[sym] = df.index[-1]
                            except Exception:
                                pass
                        _is_new_gc_birth = True
                    else:
                        _tick_gc_birth_ttl_if_needed(sym, df)

                    gc_passed.append(sym)
                    # ⚡ 신규 GC 탄생 OR Watchlist 등록 종목은 최우선 큐에 등록
                    if _is_new_gc_birth or self.analyzer._gc_birth_watchlist.get(sym, 0) > 0:
                        gc_birth_priority.append(sym)

                    gc_stats['passed'] += 1
                    logger.debug(
                        f"[B7-HANDOFF] {sym} | "
                        f"rt_ma5={_m5_c:.2f} rt_ma10={_m10_c:.2f} "
                        f"closed_ma5={_m5_closed:.2f} closed_ma10={_m10_closed:.2f} "
                        f"rt_dc={int(_m5_c < _m10_c)} "
                        f"closed_gc={int(_closed_gc)} "
                        f"gap={_gap_pct:.3f}% "
                        f"[bot7 선계산]"
                    )
                
                # ⚡ [GC-PRIORITY-REORDER] GC 탄생/Watchlist 종목을 markets_to_check 맨 앞으로 승격
                # 정석 처리: threshold/조건 변경 없음, 처리 순서만 정정
                # dict.fromkeys()로 dedup + 삽입순서 보존 (Watchlist+신규GC 동시 트리거 케이스 안전 처리)
                if gc_birth_priority:
                    _priority_symbols = list(dict.fromkeys([s for s in gc_birth_priority if s in gc_passed]))
                    _priority_set = set(_priority_symbols)
                    _normal_symbols = [s for s in gc_passed if s not in _priority_set]
                    markets_to_check = _priority_symbols + _normal_symbols
                    logger.info(
                        f"⚡ [GC-PRIORITY-REORDER] fresh={len(_priority_symbols)} "
                        f"normal={len(_normal_symbols)} | front={_priority_symbols[:5]}"
                    )
                else:
                    markets_to_check = gc_passed
                
                # 결과 로그
                if not gc_passed:
                    logger.warning(
                        f"⚠️ [골크필터] 통과 종목 없음 [{gc_max_candles}봉] | "
                        f"대상={gc_stats['total']}, 골크없음={gc_stats['no_gc']}, "
                        f"오래됨={gc_stats['stale']}, 오류={gc_stats['error']}"
                    )
                else:
                    logger.info(
                        f"🔥 [골크필터] {gc_stats['total']}개 → {len(gc_passed)}개 [{gc_max_candles}봉] | "
                        f"골크없음={gc_stats['no_gc']}, 오래됨={gc_stats['stale']}, 오류={gc_stats['error']}"
                    )
            
            # 🔥 실제 진입 체크 대상 (골크 필터 후)
            logger.info(
                f"[진입체크대상] {len(markets_to_check)}개 | {markets_to_check}"
            )
            
            # ========================================
            # 🔥 Rate Limiting 방지 루프 (순차 유지)
            # ========================================
            for idx, symbol in enumerate(markets_to_check):
                # 🔒 예외 경로 100% 안전 보장
                indicators = {}
                # 📒 [DECISION-LEDGER] 관측 전용 — 이전 종목의 값이 기록에 섞이지 않게 초기화한다.
                #  아래 7개 이름은 이 회전 안에서 읽히기 전에 반드시 다시 대입된다
                #  (position · df · current_price · regime · ma_data · signal · success)
                #  → 판정에 쓰이는 값은 달라지지 않는다.
                position = None
                df = None
                current_price = None
                regime = None
                ma_data = {}
                signal = None
                success = None
                _dl_stage = "PRE"

                # ⚡ GC 탄생봉 fast-lane: sleep 면제
                _is_gc_birth_fast = False
                try:
                    _cached_ma = self.data_service._dataframe_cache.get(symbol)
                    if _cached_ma is not None and not _cached_ma.empty and len(_cached_ma) >= 10:
                        _m5  = float(_cached_ma['close'].rolling(5).mean().iloc[-1])
                        _m10 = float(_cached_ma['close'].rolling(10).mean().iloc[-1])
                        _m5p = float(_cached_ma['close'].rolling(5).mean().iloc[-2])
                        _m10p= float(_cached_ma['close'].rolling(10).mean().iloc[-2])
                        _is_gc_birth_fast = (_m5 > _m10) and (_m5p <= _m10p)
                except Exception:
                    pass

                if not _is_gc_birth_fast:
                    # 🔥 10개마다 0.5초 대기
                    if idx > 0 and idx % 10 == 0:
                        logger.debug(
                            f"[⏱️진입제한] {idx}개 체크 완료, 0.5초 대기... "
                            f"({idx}/{len(markets_to_check)})"
                        )
                        await asyncio.sleep(0.5)
                else:
                    logger.debug(f"⚡ [GC-FastLane] {symbol} GC 탄생봉 → sleep 면제")
                
                try:                                       
                    # 재매수 쿨다운 체크
                    if symbol in self.sell_cooldown:
                        cooldown_time = self.sell_cooldown[symbol]
                        elapsed = (datetime.now() - cooldown_time).total_seconds() / 60
                        
                        if elapsed < self.cooldown_minutes:
                            remaining = self.cooldown_minutes - elapsed
                            logger.info(
                                f"🚫 [BOT-ENTRY-BLOCK] {symbol} phase=SELL_COOLDOWN | "
                                f"elapsed={elapsed:.1f}m "
                                f"remain={remaining:.1f}m "
                                f"cooldown={self.cooldown_minutes}m "
                                f"→ 재매수 차단"
                            )
                            _dl_stage = "SELL_COOLDOWN"  # 📒 [DECISION-LEDGER] 관측 전용
                            continue
                        else:
                            del self.sell_cooldown[symbol]
                    
                    # 🆕 포지션 체크 (PRE는 예외)
                    position = None
                    async with self._positions_lock:
                        if symbol in self.positions:
                            position = self.positions[symbol]
                            meta = position.metadata or {}

                            if meta.get('entry_stage') == 'PRE' and not meta.get('addon_done', False):
                                partial_cnt = int(getattr(position, "partial_sell_count", 0) or 0)
                                if partial_cnt > 0:
                                    logger.info(
                                        f"⚠️ [Addon차단] {symbol} | "
                                        f"부분매도 진행 중(partial_sell_count={partial_cnt}) → ADDON 차단"
                                    )
                                    _dl_stage = "ADDON_PARTIAL_BLOCK"  # 📒 [DECISION-LEDGER] 관측 전용
                                    continue

                                logger.debug(f"🔄 [진입체크] {symbol} PRE 포지션 → STD 추가매수 평가")
                            else:
                                logger.info(
                                    f"🚫 [BOT-ENTRY-BLOCK] {symbol} phase=ALREADY_HOLDING | "
                                    f"entry_stage={meta.get('entry_stage', 'STD')} "
                                    f"addon_done={int(bool(meta.get('addon_done', False)))} "
                                    f"→ 신규 진입 스킵"
                                )
                                _dl_stage = "ALREADY_HOLDING"  # 📒 [DECISION-LEDGER] 관측 전용
                                continue
                    
                    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
                    # 🆕 Prefetch된 DataFrame 가져오기
                    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
                    df = df_map.get(symbol)
                    
                    if isinstance(df, Exception) or df is None:
                        logger.info(
                            f"🚫 [BOT-ENTRY-BLOCK] {symbol} phase=DF_MISSING | "
                            f"reason=NO_DATAFRAME"
                        )
                        _dl_stage = "DF_MISSING"  # 📒 [DECISION-LEDGER] 관측 전용
                        continue
                    
                    if not is_valid_df(df):
                        logger.info(
                            f"🚫 [BOT-ENTRY-BLOCK] {symbol} phase=DF_INVALID | "
                            f"reason=INVALID_DATAFRAME"
                        )
                        _dl_stage = "DF_INVALID"  # 📒 [DECISION-LEDGER] 관측 전용
                        continue
                    
                    # ========================================
                    # 🆕 통합 가격 검증 (한 번만!)
                    # ========================================
                    current_price = await self._validate_entry_price(symbol, df)
                    
                    if current_price is None:
                        logger.info(
                            f"🚫 [BOT-ENTRY-BLOCK] {symbol} phase=PRICE_VALIDATE | "
                            f"reason=NO_VALID_CURRENT_PRICE"
                        )
                        _dl_stage = "PRICE_VALIDATE"  # 📒 [DECISION-LEDGER] 관측 전용
                        continue
                    
                    # ✅ DataFrame 최신봉을 검증된 현재가로 동기화 (close + high/low)
                    self._apply_intrabar_price_to_df(df, current_price)
                    
                    logger.info(f"[진입체크] {symbol} ━━━━━━━━━━━━━━")
                    
                    # 지표 계산
                    try:
                        indicators = TechnicalIndicators.calculate_all_indicators(
                            df, symbol=symbol, timeframe=self.timeframe.value
                        )
                        if not indicators:
                            logger.info(
                                f"🚫 [BOT-ENTRY-BLOCK] {symbol} phase=INDICATOR_HARD_BLOCK | "
                                f"reason=calculate_all_indicators_returned_empty"
                            )
                            _dl_stage = "INDICATOR_HARD_BLOCK"  # 📒 [DECISION-LEDGER] 관측 전용
                            continue

                        indicators["_valid"] = True
                        
                        # 🆕 실시간 ticker 지표 주입 (30초 헌법 복원)
                        rt_ind = self._calc_realtime_indicators(symbol)
                        indicators.update(rt_ind)
                    except Exception as e:
                        logger.warning(f"⚠️ [{symbol}] 지표 계산 실패: {e}")
                        # 🔒 PATCH D: 빈 dict 전파 차단
                        indicators = {
                            "_valid": False,
                            "_calc_failed": True
                        }
                    
                    # Phase 2: WatchList 로직 제거됨
                    
                    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
                    # 🆕 불장 모드 정보 추가 (2025-11-26)
                    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
                    try:
                        bull_context = self.analyzer.get_bull_market_context()
                        if indicators:
                            indicators.update(bull_context)
                        else:
                            indicators = bull_context
                    except Exception as e:
                        logger.warning(f"⚠️ [{symbol}] 불장 정보 추가 실패: {e}")
                    
                    # 체제 분석
                    regime = await self.analyzer.analyze_market_regime(df, indicators, symbol=symbol) if hasattr(self, 'analyzer') else None
                    
                    # ✅ ma_data 계산
                    ma_data = {}
                    try:
                        # ma_exit_applicable_timeframes 안전하게 가져오기
                        ma_exit_timeframes = []
                        if hasattr(self, 'signal_analyzer') and hasattr(self.signal_analyzer, 'ma_exit_applicable_timeframes'):
                            ma_exit_timeframes = self.signal_analyzer.ma_exit_applicable_timeframes
                        
                        ma_data = TechnicalIndicators.calculate_ma_analysis(
                            df,
                            self.timeframe.value,
                            symbol,
                            ma_exit_applicable_timeframes=ma_exit_timeframes,
                            regime=regime,
                            config=self.config,
                            indicators=indicators,
                            ticker_price=current_price  # 🔒 PATCH A: 가격 괴리 검증용
                        )
                    except Exception as e:
                        logger.warning(f"[{symbol}] ma_data 계산 실패: {e}")
                        ma_data = {}
                    
                    # DataPackage 생성
                    data_package = DataPackage(
                        df=df,
                        market=symbol,
                        current_price=current_price,
                        regime=regime,
                        indicators=indicators,
                        ma_data=ma_data,
                        pattern_data={},
                        timeframe=self.timeframe.value
                    )
                    
                    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
                    # 🔒 체제는 한 사이클에서 단일 판정만 사용 (3분 고정 설계 준수)
                    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
                    # 배경: 
                    # - 진입 직전 재검증은 3분 고정 캐시를 무효화시킴
                    # - 하락 유사 가드가 단발로 걸려 RANGING → TD로 오판 유발
                    # - data_package.regime (1430라인)을 Single Source of Truth로 사용
                    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
                    
                    # ========================================
                    # 🔗 P4d: 수익률 버퍼 주입
                    # ========================================
                    try:
                        if len(df) >= 2 and hasattr(self.performance, 'p4d') and self.performance.p4d:
                            prev_close = float(df['close'].iloc[-2])
                            if prev_close > 0:
                                ret = (current_price - prev_close) / prev_close
                                self.performance.p4d.ingest_tick_returns(symbol, ret)
                    except Exception as e:
                        logger.debug(f"[P4d] {symbol} 수익률 주입 실패: {e}")
                    
                    # 신호 생성
                    risk_metrics = await self._get_risk_metrics()
                    signal = await self.signal_analyzer.generate_signal(
                        data_package,
                        position,  # 🆕 PRE 포지션 전달
                        self.performance.get_adaptive_weights(
                            regime=regime.value if regime else None,
                            symbol=symbol
                        ) if self.performance else {},
                        self.performance,
                        risk_metrics
                    )
                    _dl_stage = "STRATEGY_DONE"  # 📒 [DECISION-LEDGER] 관측 전용
                    
                    if signal is None:
                        reason = getattr(self.signal_analyzer, "last_rejection_reason", None) or "조건 미충족"
                        logger.info(f"[진입체크] {symbol} ━━ 신호 없음 (이유: {reason}) scalp_min={getattr(self.signal_analyzer,'last_scalp_min',0)}")
                        
                        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
                        # 🆕 우선 재체크 등록 트리거 (스캔 공백 보완)
                        # 조건: '형성 중' 신호가 보이지만 아직 최종 통과 전
                        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
                        try:
                            golden_count = ma_data.get('golden_count', 0) if ma_data else 0
                            ma5_slope_ratio = float(ma_data.get('slope_ma5', 0.0) or 0.0) if ma_data else 0.0
                            vol_ratio = indicators.get('volume_ratio', 0.0) if indicators else 0.0
                            soleus_zone = indicators.get('soleus_zone', False) if indicators else False

                            partial_signal = (
                                golden_count >= 1
                                and ma5_slope_ratio >= SoleusGajamiConstants.MA5_SLOPE_PRECHECK_RATIO
                                and vol_ratio >= 0.3
                                and not soleus_zone
                            )

                            if partial_signal:
                                # 최대 2회만 우선 재체크
                                self._priority_recheck[symbol] = max(
                                    self._priority_recheck.get(symbol, 0),
                                    2
                                )
                                # __PRIPROV609__ ★priority producer ★관측 전용 (★판정 없음)
                                try:
                                    import json as _ppj
                                    self._pri_prov_seq = int(getattr(self, "_pri_prov_seq", 0)) + 1
                                    logger.info("[PRI-PROV-SET] " + _ppj.dumps({
                                        "seq": int(self._pri_prov_seq),
                                        "cycle": int(getattr(self, "_cycle_count", 0)),
                                        "sym": str(symbol),
                                        "source": "PARTIAL_SIGNAL",
                                        "ttl": 2,
                                        "after": int(self._priority_recheck.get(symbol, 0)),
                                    }, ensure_ascii=False, separators=(",", ":")))
                                except Exception as _ppe:
                                    logger.warning("[PRI-PROV-ERR] %s" % type(_ppe).__name__)
                                logger.debug(f"[우선재체크] {symbol} 부분 가자미근 형성 중 (2회 등록)")
                        except Exception:
                            pass
                        
                        continue
                    
                    # 안전 장치: 타입 검증
                    if not isinstance(signal, TradingSignal):
                        logger.error(f"[진입체크] {symbol} 잘못된 신호 타입: {type(signal)}")
                        _dl_stage = "SIGNAL_TYPE_INVALID"  # 📒 [DECISION-LEDGER] 관측 전용
                        continue
                    
                    if not hasattr(signal, 'action'):
                        logger.error(f"[진입체크] {symbol} 신호에 action 속성 없음")
                        _dl_stage = "SIGNAL_NO_ACTION"  # 📒 [DECISION-LEDGER] 관측 전용
                        continue
                    
                    # ========================================
                    # 매수 신호 처리
                    # ========================================
                    if signal.action == "BUY":
                        confidence = signal.confidence
                        
                        logger.info(
                            f"[진입체크] {symbol} 매수신호 발생 - "
                            f"신뢰도: {confidence:.3f}"
                        )
                        
                        # 🔒 [BOT-JUDGE-HANDOFF-0828]
                        # Strategy가 확정한 route/판사표를 그대로 소비한다.
                        # Bot에서 entry_type을 STANDARD로 덮어쓰지 않는다.
                        if not hasattr(signal, 'indicators') or signal.indicators is None:
                            signal.indicators = {}

                        entry_type = str(
                            signal.indicators.get(
                                "entry_type", "STANDARD"
                            )
                            or "STANDARD"
                        )

                        _j1_vote = bool(
                            int(
                                signal.indicators.get(
                                    "judge1_gajami_pass", 0
                                ) or 0
                            )
                        )
                        _j2_vote = bool(
                            int(
                                signal.indicators.get(
                                    "judge2_osc_pass", 0
                                ) or 0
                            )
                        )
                        _j3_vote = bool(
                            int(
                                signal.indicators.get(
                                    "judge3_cycle_pass", 0
                                ) or 0
                            )
                        )
                        _j4_vote = bool(
                            int(
                                signal.indicators.get(
                                    "judge4_reversal_pass", 0
                                ) or 0
                            )
                        )
                        _j234_vote = bool(
                            _j2_vote or _j3_vote or _j4_vote
                        )

                        signal.indicators["strategy_judges"] = str(
                            signal.indicators.get(
                                "judges",
                                f"{int(_j1_vote)}{int(_j2_vote)}"
                                f"{int(_j3_vote)}{int(_j4_vote)}",
                            )
                            or ""
                        )

                        signal.indicators["execution_judges"] = (
                            f"{int(_j1_vote)}{int(_j2_vote)}"
                            f"{int(_j3_vote)}{int(_j4_vote)}"
                        )
                        signal.indicators["bot_j1_lane_denied"] = 0
                        signal.indicators[
                            "bot_j1_lane_deny_reason"
                        ] = ""

                        # ★383-R1 계약의 방어선:
                        # J234 승인 신호는 공통 JUDGE route다.
                        if _j234_vote and entry_type != "JUDGE":
                            signal.indicators.setdefault(
                                "judge_entry_source_type",
                                entry_type,
                            )
                            entry_type = "JUDGE"
                            signal.indicators["entry_type"] = entry_type

                            logger.info(
                                f"🟢 [BOT-JUDGE-ROUTE-NORMALIZE] {symbol} | "
                                f"execution_judges="
                                f"{signal.indicators['execution_judges']} "
                                f"→ JUDGE"
                            )
                                                                        
                        # ATR 필터
                        atr_value = safe_extract_atr(signal.indicators.get('atr', 0))

                        atr_pct = (atr_value / signal.price * 100) if signal.price > 0 else 0
                        
                        # 🔒 정석: 가자미근 컨텍스트면 FastEntry ATR 상한 사용
                        is_gajami_ctx = bool(signal.indicators.get("gajami_entry", False))
                        
                        atr_limit_default = 2.5
                        atr_limit_soleus = float(self.config.get_cached("fastentry_atr_max") or atr_limit_default)
                        atr_limit = atr_limit_soleus if is_gajami_ctx else atr_limit_default

                        if atr_pct > atr_limit:
                            logger.info(
                                f"🚫 {symbol} ATR 과다 진입 차단! "
                                f"ATR: {atr_pct:.2f}% > 한도 {atr_limit:.2f}% "
                                f"({'가자미근' if is_gajami_ctx else '일반'})"
                            )
                            _dl_stage = "ATR_OVER_LIMIT"  # 📒 [DECISION-LEDGER] 관측 전용
                            continue
                        
                        # ========================================
                        # 🔒 [BOT-J1-EXEC-LANE-SPLIT-0828]
                        # Direction / DecayLock / Late-Entry는 J1 가자미 실행법이다.
                        # J1 실패가 J2/J3/J4 독립 승인을 전역 continue로 죽이지 못하게 한다.
                        # 실제 추격 임계는 아래 코드값(1.5/2.0%, 3.50x)이 SSoT다.
                        # ========================================
                        _bot_j1_deny_reason = None
                        _bot_j1_deny_detail = ""

                        if is_gajami_ctx and _j1_vote:
                            # ① 내려오는 중 J1 진입 금지
                            _prev_close = float(
                                signal.indicators.get('prev_candle_close', 0) or 0
                            )
                            if _prev_close > 0 and signal.price <= _prev_close:
                                if not _j234_vote:
                                    logger.info(
                                        f"🚫 [Gajami-Direction-Block] {symbol} | "
                                        f"하락/정체 중 진입 금지 | "
                                        f"현재가={signal.price:.2f} ≤ 직전봉close={_prev_close:.2f}"
                                    )
                                    _dl_stage = "GAJAMI_DIRECTION"  # 📒 [DECISION-LEDGER] 관측 전용
                                    continue

                                _bot_j1_deny_reason = "GAJAMI_DIRECTION"
                                _bot_j1_deny_detail = (
                                    f"현재가={signal.price:.2f} "
                                    f"≤ 직전봉close={_prev_close:.2f}"
                                )

                            # ② 이전 J1 고점차단 파동 재진입 금지
                            _lifecycle = signal.indicators.get(
                                'gajami_lifecycle', {}
                            ) or {}
                            _decay_until = float(
                                self._late_entry_decay_lock.get(symbol, 0) or 0
                            )

                            if (
                                _bot_j1_deny_reason is None
                                and _decay_until > time.time()
                            ):
                                if not _j234_vote:
                                    logger.info(
                                        f"🚫 [Late-Entry-DecayLock] {symbol} | "
                                        f"고점 차단 후 재진입 금지 | "
                                        f"잔여={(_decay_until - time.time()):.0f}s"
                                    )
                                    _dl_stage = "LATE_ENTRY_DECAY_LOCK"  # 📒 [DECISION-LEDGER] 관측 전용
                                    continue

                                _bot_j1_deny_reason = "LATE_ENTRY_DECAY_LOCK"
                                _bot_j1_deny_detail = (
                                    f"잔여={(_decay_until - time.time()):.0f}s"
                                )

                            # ③ 실제 진입예정가 기준 J1 추격 차단
                            if _bot_j1_deny_reason is None:
                                _birth_price_lg = _lifecycle.get('birth_price')
                                _rise_pct = _lifecycle.get('rise_pct', 0) or 0

                                try:
                                    if (
                                        _birth_price_lg is not None
                                        and signal.price > 0
                                        and float(_birth_price_lg) > 0
                                    ):
                                        _rise_pct = (
                                            float(signal.price)
                                            - float(_birth_price_lg)
                                        ) / float(_birth_price_lg)
                                except Exception:
                                    pass

                                _guard_atr = (
                                    atr_pct
                                    if atr_pct > 0
                                    else float(
                                        signal.indicators.get('atr_pct', 0) or 0
                                    )
                                )

                                if _rise_pct > 0:
                                    _regime_v = getattr(
                                        signal, 'market_regime', None
                                    )
                                    _regime_name = (
                                        _regime_v.value
                                        if hasattr(_regime_v, 'value')
                                        else str(_regime_v or '')
                                    )
                                    _strict_chase = _regime_name in (
                                        'TRENDING_DOWN', 'RANGING'
                                    )
                                    CHASE_MAX_PCT = (
                                        0.015 if _strict_chase else 0.020
                                    )
                                    CHASE_MAX_ATR_RATIO = 3.50

                                    _rise_atr_ratio = None
                                    _chase_pct_fail = (
                                        _rise_pct > CHASE_MAX_PCT
                                    )
                                    _chase_atr_fail = False

                                    if _guard_atr > 0:
                                        _atr_frac = _guard_atr / 100.0
                                        _rise_atr_ratio = (
                                            _rise_pct / _atr_frac
                                        )
                                        _chase_atr_fail = (
                                            _rise_atr_ratio
                                            > CHASE_MAX_ATR_RATIO
                                        )

                                    if _chase_pct_fail or _chase_atr_fail:
                                        _rise_atr_text = (
                                            f"{_rise_atr_ratio:.2f}x"
                                            if _rise_atr_ratio is not None
                                            else "N/A"
                                        )

                                        # J1 동일 파동 재진입 잠금은 유지.
                                        self._late_entry_decay_lock[symbol] = (
                                            time.time() + 360
                                        )

                                        if not _j234_vote:
                                            logger.info(
                                                f"🚫 [Late-Entry-Guard] {symbol} "
                                                f"진입가 기준 추격 차단 | "
                                                f"birth={format_price_krw(_birth_price_lg)}원 "
                                                f"entry={format_price_krw(signal.price)}원 "
                                                f"rise={_rise_pct:.2%} "
                                                f"Rise/ATR={_rise_atr_text} "
                                                f"limit={CHASE_MAX_PCT:.2%}/"
                                                f"{CHASE_MAX_ATR_RATIO:.2f}x "
                                                f"regime={_regime_name}"
                                            )
                                            _dl_stage = "LATE_ENTRY_GUARD"  # 📒 [DECISION-LEDGER] 관측 전용
                                            continue

                                        _bot_j1_deny_reason = (
                                            "LATE_ENTRY_GUARD"
                                        )
                                        _bot_j1_deny_detail = (
                                            f"birth={format_price_krw(_birth_price_lg)}원 "
                                            f"entry={format_price_krw(signal.price)}원 "
                                            f"rise={_rise_pct:.2%} "
                                            f"Rise/ATR={_rise_atr_text} "
                                            f"limit={CHASE_MAX_PCT:.2%}/"
                                            f"{CHASE_MAX_ATR_RATIO:.2f}x "
                                            f"regime={_regime_name}"
                                        )

                        # J1 실행법 실패 + J234 승인 → J1만 탈락.
                        if _bot_j1_deny_reason is not None:
                            signal.indicators["bot_j1_lane_denied"] = 1
                            signal.indicators[
                                "bot_j1_lane_deny_reason"
                            ] = _bot_j1_deny_reason

                            signal.indicators["execution_judges"] = (
                                f"0{int(_j2_vote)}"
                                f"{int(_j3_vote)}{int(_j4_vote)}"
                            )
                            # 🔒 [EXECUTION-JUDGES-SSoT-0828]
                            # strategy_judges는 Strategy 원판결을 보존하고,
                            # legacy judges는 실제 주문 집행 판사표로 갱신한다.
                            signal.indicators["judges"] = (
                                signal.indicators["execution_judges"]
                            )

                            if entry_type != "JUDGE":
                                signal.indicators.setdefault(
                                    "judge_entry_source_type",
                                    entry_type,
                                )
                                entry_type = "JUDGE"
                                signal.indicators["entry_type"] = entry_type

                            logger.info(
                                f"🟡 [BOT-J1-LANE-DENY] {symbol} | "
                                f"reason={_bot_j1_deny_reason} | "
                                f"{_bot_j1_deny_detail} → "
                                f"J1 실행 lane만 탈락 · "
                                f"J2/J3/J4 집행 유지 | "
                                f"execution_judges="
                                f"{signal.indicators['execution_judges']}"
                            )
                        
                        # ========================================
                        # 캐시보존 우회 판단
                        # ========================================
                        is_recovery_trade = False
                        override_reason = None
                        
                        if preserve_cash:
                            if confidence >= self.cash_preservation_override:
                                is_recovery_trade = True
                                override_reason = "cash_preservation"
                                logger.info(
                                    f"✅ {symbol} 고신뢰도 진입 승인! "
                                    f"신뢰도 {confidence:.3f} ≥ {self.cash_preservation_override:.2f} "
                                    f"(캐시보존모드 우회)"
                                )
                            else:
                                logger.info(
                                    f"🚫 {symbol} 신뢰도 부족 - 진입 차단 "
                                    f"(신뢰도 {confidence:.3f} < {self.cash_preservation_override:.2f})"
                                )
                                _dl_stage = "CASH_PRESERVE_CONF_LOW"  # 📒 [DECISION-LEDGER] 관측 전용
                                continue
                        
                        # 정상모드 confidence 하한·판사 예외는
                        # RiskCalculator.calculate_position_size()에서 단일 판정한다.
                        
                        # 진입 실행
                        logger.info(f"[진입체크] {symbol} execute_trade 호출 시도")
                        success = await self.execute_trade(position, signal)  # 🆕 position 전달
                        
                        if not success:
                            logger.info(
                                f"🚫 [BOT-BUY-BLOCK] {symbol} phase=EXECUTE_TRADE | "
                                f"result=False | "
                                f"conf={confidence:.3f} "
                                f"price={format_price_krw(signal.price)} "
                                f"gajami={int(bool(signal.indicators.get('gajami_entry', False)))} "
                                f"reason={getattr(self.signal_analyzer, 'last_rejection_reason', None) or 'EXECUTE_TRADE_FALSE'}"
                            )
                            continue
                        
                        if success:
                            # 회복 거래 플래그 설정
                            if is_recovery_trade:
                                async with self._positions_lock:
                                    if symbol in self.positions:
                                        self.positions[symbol].metadata = self.positions[symbol].metadata or {}
                                        self.positions[symbol].metadata['recovery_trade'] = True
                                        self.positions[symbol].metadata['override_reason'] = override_reason
                                
                                self.recovery_trades[symbol] = {
                                    'entry_time': datetime.now(),
                                    'confidence': confidence,
                                    'override_reason': override_reason
                                }
                                
                                logger.info(
                                    f"🌟 {symbol} 회복 거래 진입! "
                                    f"신뢰도 {confidence:.3f} - "
                                    f"수익 시 캐시보존모드 해제 예정"
                                )
                            
                            # EEE 신뢰도 추출 (있으면 표시)
                            eee_conf = signal.indicators.get('eee_confidence') if hasattr(signal, 'indicators') else None
                            eee_display = f"{eee_conf:.3f}" if eee_conf is not None else "N/A"
                            
                            logger.info(
                                f"✅ {symbol} 매수 성공 "
                                f"(정책신뢰도: {confidence:.3f}, EEE: {eee_display}, "
                                f"모드: {'캐시보존우회' if preserve_cash else '정상'})"
                            )
                            # 🔭 주문 완료 후 1분봉 실행 트리거 SHADOW
                            # - REST 관측이 실제 주문 시점을 지연시키지 않음
                            if (
                                self.timeframe.value == "3m"
                                and self.config.getboolean(
                                    "ENTRY_TRIGGER",
                                    "shadow_1m_trigger",
                                    fallback=False,
                                )
                            ):
                                await self._shadow_entry_trigger_1m(signal)
                            break  # 한 번에 하나씩만
                        
                except OSError as e:
                    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
                    # 🛡️ WinError 233 완전 차단 (로깅도 생략)
                    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
                    winerr = getattr(e, "winerror", None)
                    error_msg = str(e)
                    
                    # WinError 233이면 조용히 무시 (로그 없음)
                    if winerr == 233 or "233" in error_msg:
                        _dl_stage = "EXC_OSERROR_233"  # 📒 [DECISION-LEDGER] 관측 전용
                        continue  # 조용히 다음 종목으로
                    
                    # 나머지 실제 OSError만 로깅
                    logger.warning(f"🔵 [OSError블록] {symbol} 다른 OSError: {e}")
                    import traceback
                    logger.debug(f"스택 트레이스:\n{traceback.format_exc()}")
                    _dl_stage = "EXC_OSERROR"  # 📒 [DECISION-LEDGER] 관측 전용
                    continue
                    
                except Exception as e:
                    # 🔒 안전장치: symbol, indicators
                    symbol = symbol if 'symbol' in locals() else 'UNKNOWN'
                    indicators = indicators if 'indicators' in locals() else {}
                    
                    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
                    # 🛡️ WinError 233 완전 차단 (로깅도 생략)
                    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
                    winerr = getattr(e, "winerror", None)
                    error_msg = str(e)
                    
                    # WinError 233이면 조용히 무시 (로그 없음)
                    if winerr == 233 or "233" in error_msg or "pipe" in error_msg.lower():
                        _dl_stage = "EXC_PIPE_233"  # 📒 [DECISION-LEDGER] 관측 전용
                        continue  # 조용히 다음 종목으로
                    
                    # 나머지 실제 예외만 로깅
                    logger.error(f"🔴 [Exception블록] {symbol} 처리 오류: {e}")
                    import traceback
                    logger.error(f"스택 트레이스:\n{traceback.format_exc()}")
                    _dl_stage = "EXC_EXCEPTION"  # 📒 [DECISION-LEDGER] 관측 전용
                    continue

                finally:
                    # 📒 [DECISION-LEDGER] 관측 전용 — 종목 1건 진입 판단 종료 시 1행 기록.
                    #  continue / break / 예외 처리 어느 경로로 나가도 여기를 지난다.
                    #  판정·주문·상태를 바꾸지 않는다. 예외는 여기서 삼킨다(매매 경로 전파 0).
                    if self._decision_ledger is not None:
                        try:
                            self._decision_ledger.capture_entry(
                                cycle=self._cycle_count,
                                rr_cycle=self._entry_cycle_count,
                                seq=idx,
                                symbol=symbol,
                                stage=_dl_stage,
                                df=df,
                                current_price=current_price,
                                regime=regime,
                                indicators=indicators,
                                ma_data=ma_data,
                                signal=signal,
                                success=success,
                                position=position,
                                preserve_cash=preserve_cash,
                                reject_reason=getattr(
                                    self.signal_analyzer, "last_rejection_reason", ""
                                ),
                                last_ticker=self._last_ticker_data.get(symbol),
                                sell_cooldown_active=(symbol in self.sell_cooldown),
                                decay_lock_until=self._late_entry_decay_lock.get(symbol, 0),
                            )
                        except Exception:
                            pass
                    
            # 🔥 실행 시간 측정 및 로그
            entry_elapsed = (datetime.now() - entry_start).total_seconds()
            # 📒 [DECISION-LEDGER] 관측 전용 — 사이클 1행 (대상 수·소요)
            if self._decision_ledger is not None:
                try:
                    self._decision_ledger.capture_cycle(
                        cycle=self._cycle_count,
                        rr_cycle=self._entry_cycle_count,
                        n_targets=len(markets_to_check) if 'markets_to_check' in locals() else 0,
                        elapsed_sec=entry_elapsed,
                        preserve_cash=preserve_cash,
                    )
                except Exception:
                    pass
            logger.info(
                f"⏱️ [진입체크소요] {entry_elapsed:.2f}초 | "
                f"{len(markets_to_check) if 'markets_to_check' in locals() else 0}개 처리"
            )
            
            if entry_elapsed > 25:
                logger.warning(
                    f"⚠️ [진입체크지연] {entry_elapsed:.0f}초 > 25초 | "
                    f"병목 가능성"
                )
        
        except Exception as e:
            # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
            # 🛡️ WinError 233 완전 차단 (로깅도 생략)
            # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
            winerr = getattr(e, "winerror", None)
            error_msg = str(e)
            
            # WinError 233이면 조용히 무시 (로그 없음)
            if winerr == 233 or "233" in error_msg:
                return  # 조용히 종료
            
            # 나머지 실제 오류만 로깅
            logger.error(f"진입 체크 오류: {e}")
            import traceback
            logger.error(f"스택 트레이스:\n{traceback.format_exc()}")
    
    def _dl_note(self, signal, code: str) -> None:
        """📒 [DECISION-LEDGER] execute_trade / _create_position 내부 사유 표지 (관측 전용 · BUY 만)"""
        _dl = getattr(self, "_decision_ledger", None)
        if _dl is None:
            return
        try:
            if getattr(signal, "action", None) == "BUY":
                _dl.note(getattr(signal, "symbol", ""), code)
        except Exception:
            pass

    async def execute_trade(self, position: Optional[Position], signal: TradingSignal) -> bool:
        """거래 실행"""
        if not signal or not signal.symbol or signal.price <= 0:
            logger.warning(f"[거래실행] 잘못된 신호")
            return False
        if self.mode == 'real':
            from math import isfinite
            if not isfinite(float(signal.price)) or signal.action not in ("BUY", "SELL"):
                return False
            reason = self.engine.order_block_reason(signal.symbol, signal.action)
            if reason:
                logger.critical(f"[REAL-ORDER-BLOCK] {signal.symbol} {reason}")
                return False
        sell_claimed = False
        sell_order_succeeded = False
        sell_state_synced = False

        try:
            if signal.action == "SELL":
                if not position:
                    logger.warning(
                        f"[매도실행] {signal.symbol} 포지션 없음"
                    )
                    return False
                if getattr(position, "_closing", False):
                    logger.debug(
                        f"[SELL-CLAIM-SKIP] "
                        f"{signal.symbol} 청산 진행 중"
                    )
                    return False
                position._closing = True
                sell_claimed = True
            # ========================================
            # 0. 신호 신선도 검증 (STALE 체크)
            # ========================================
            if hasattr(signal, 'timestamp') and signal.timestamp:
                from datetime import datetime
                signal_age = (datetime.now() - signal.timestamp).total_seconds()
                
                # 타임프레임별 허용 시간 (초)
                max_age_map = {
                    '1m': 2.0,   # 1분봉: 2초
                    '3m': 3.0,   # 3분봉: 3초
                    '5m': 5.0,   # 5분봉: 5초
                    '10m': 8.0,  # 10분봉: 8초
                    '15m': 10.0, # 15분봉: 10초
                    '30m': 15.0, # 30분봉: 15초
                    '1h': 20.0,  # 1시간봉: 20초
                    '4h': 30.0,  # 4시간봉: 30초
                    '1d': 60.0   # 1일봉: 60초
                }
                max_age = max_age_map.get(self.timeframe.value, 5.0)
                
                if signal_age > max_age:
                    logger.warning(
                        f"⏰ [신호폐기] {signal.symbol} | "
                        f"신호 경과: {signal_age:.1f}초 > 허용: {max_age:.1f}초 | "
                        f"STALE 신호 - 주문 취소"
                    )
                    self._dl_note(signal, "STALE_SIGNAL")  # 📒 [DECISION-LEDGER] 관측 전용
                    return False
            
            # ========================================
            # 🔒 과소 수량 쿨다운 체크 (운영 보호)
            # ========================================
            cooldown_sec = 180  # 3분
            last_reason = self.min_qty_rejection_reason.get(signal.symbol)
            last_time = self.min_qty_rejection_time.get(signal.symbol, 0)
            
            if last_reason == "MIN_ORDER_QTY":
                elapsed = time.time() - last_time
                if elapsed < cooldown_sec:
                    remaining = int(cooldown_sec - elapsed)
                    logger.debug(
                        f"⏳ [쿨다운] {signal.symbol} 과소수량 쿨다운 중 → 재시도 스킵 | "
                        f"남은시간: {remaining}초"
                    )
                    self._dl_note(signal, "MIN_QTY_COOLDOWN")  # 📒 [DECISION-LEDGER] 관측 전용
                    return False
            
            # ========================================
            # 1. ATR 값 안전하게 추출 (한 번만!)
            # ========================================
            atr_value = safe_extract_atr(signal.indicators.get('atr', 0))

            # 🔒 ATR=0 복구 (position_size 과대 배분 방지)
            if atr_value <= 0 and signal.price > 0:
                _atr_pct_fb = float(signal.indicators.get('atr_pct', 0) or 0)
                if _atr_pct_fb > 0:
                    atr_value = signal.price * (_atr_pct_fb / 100.0)
                else:
                    _atr_min = float(self.config.get_cached('atr_min_threshold', 0.5) or 0.5)
                    atr_value = signal.price * (_atr_min / 100.0)

            # ATR 비율 계산 (%)
            atr_pct = (atr_value / signal.price * 100) if signal.price > 0 else 0
            
            # Reality Engine 메타데이터 (공통)
            metadata = {
                'atr': atr_value,
                'atr_pct': atr_pct,
                'volume_rank': 50,
                'market_regime': signal.market_regime.value if signal.market_regime else 'RANGING',
                'regime': signal.market_regime.value if signal.market_regime else 'UNKNOWN',
                'timeframe': self.timeframe.value,
                'current_price': signal.price   # ← Sim체결 API 재조회 제거 (3초 지연 해소)
            }
            
            # ========================================
            # 2. 주문 요청 생성
            # ========================================
            if signal.action == "BUY":
                # 🔒 [BAR-OPEN-FRESH-RECHECK-0824]
                # 3m 최초 BUY만 봉 시작 직후 체결을 보류한다.
                # 기존 signal을 보관하지 않고 설정된 guard 경계에서 종목을 fresh 재평가한다.
                # PRE→STD ADDON(position != None)은 이 표본의 대상이 아니므로 제외한다.
                if position is None and self.timeframe.value == "3m":
                    _bar_guard_sec = self.config.getint(
                        "POSITION_MANAGEMENT",
                        "bar_open_guard_seconds",
                        fallback=30,
                    )
                    _bar_guard_sec = max(
                        0, min(int(_bar_guard_sec), 60)
                    )

                    if _bar_guard_sec > 0:
                        _now_local = datetime.now()
                        _since_midnight = (
                            _now_local.hour * 3600
                            + _now_local.minute * 60
                            + _now_local.second
                            + _now_local.microsecond / 1_000_000.0
                        )
                        _in_bar = _since_midnight % 180.0

                        if _in_bar < _bar_guard_sec:
                            _now_epoch = time.time()
                            _recheck_due = (
                                _now_epoch
                                + (_bar_guard_sec - _in_bar)
                            )
                            _prev_due = float(
                                self._bar_open_recheck_due.get(
                                    signal.symbol, 0.0
                                ) or 0.0
                            )

                            if (
                                _prev_due <= _now_epoch
                                or _recheck_due < _prev_due
                            ):
                                self._bar_open_recheck_due[
                                    signal.symbol
                                ] = _recheck_due

                            logger.info(
                                f"⏳ [BAR-OPEN-GUARD] {signal.symbol} | "
                                f"봉 시작 {_in_bar:.1f}s < {_bar_guard_sec}s "
                                f"→ 주문 보류 | "
                                f"fresh_recheck_in="
                                f"{max(0.0, _recheck_due - time.time()):.1f}s"
                            )
                            self._dl_note(signal, "BAR_OPEN_GUARD")  # 📒 [DECISION-LEDGER] 관측 전용
                            return False

                # 🔒 PRE 실패 쿨다운 체크
                cooldown_time = self._cooldown.get(signal.symbol)
                if cooldown_time:
                    cooldown_minutes = self.config.getint('POSITION_MANAGEMENT', 'cooldown_after_loss_minutes', fallback=30)
                    elapsed = (datetime.now() - cooldown_time).total_seconds() / 60.0
                    
                    if elapsed < cooldown_minutes:
                        remaining = cooldown_minutes - elapsed
                        logger.info(
                            f"⏳ [Cooldown] {signal.symbol} | "
                            f"PRE 실패 쿨다운 중 ({remaining:.1f}분 남음) → 진입 차단"
                        )
                        self._dl_note(signal, "PRE_FAIL_COOLDOWN")  # 📒 [DECISION-LEDGER] 관측 전용
                        return False
                    else:
                        # 쿨다운 만료 - 제거
                        self._cooldown.pop(signal.symbol, None)
                        logger.debug(f"✅ [Cooldown] {signal.symbol} 쿨다운 해제")
                
                # 🔒 전량 매도 후 재매수 쿨다운 (2차 방어)
                if signal.symbol in self.sell_cooldown:
                    elapsed = (datetime.now() - self.sell_cooldown[signal.symbol]).total_seconds() / 60.0
                    if elapsed < self.cooldown_minutes:
                        remaining = self.cooldown_minutes - elapsed
                        logger.info(
                            f"⏳ [재매수대기] {signal.symbol} | "
                            f"매도 후 {elapsed:.1f}분 경과 (< {self.cooldown_minutes}분) | "
                            f"{remaining:.1f}분 남음 → 진입 차단"
                        )
                        self._dl_note(signal, "SELL_COOLDOWN_2ND")  # 📒 [DECISION-LEDGER] 관측 전용
                        return False
                    else:
                        # 쿨다운 만료
                        del self.sell_cooldown[signal.symbol]
                        logger.info(f"✅ [재매수허용] {signal.symbol} 쿨다운 해제")
                
                # 잔고 조회
                balance = await self.engine.get_balance()
                
                # 포지션 크기 계산 (ATR 반영)
                position_size = self.signal_analyzer.risk_calculator.calculate_position_size(
                    balance,
                    signal.price,
                    signal.confidence,
                    # await self._get_risk_metrics(),
                    await self._get_risk_metrics(balance=balance),
                    self.positions,
                    atr=atr_value,
                    atr_pct=atr_pct,
                    regime=signal.market_regime,
                    confidence_threshold=float(
                        (signal.indicators or {}).get(
                            "effective_conf_threshold",
                            self.signal_analyzer.min_confidence,
                        )
                        or self.signal_analyzer.min_confidence
                    )
                )

                # 🔒 [STRATEGY-SIZE-RATIO-CONTRACT-0824]
                # Strategy가 선언한 축소 비율을 실제 주문금액에 반영한다.
                # RiskCalculator 산출 뒤에 축소만 허용하며 확대는 금지한다.
                _strategy_size_ratio_raw = (signal.indicators or {}).get(
                    "position_size_ratio", 1.0
                )
                try:
                    _strategy_size_ratio = float(_strategy_size_ratio_raw)
                except (TypeError, ValueError):
                    _strategy_size_ratio = 1.0

                if _strategy_size_ratio != _strategy_size_ratio:  # NaN
                    _strategy_size_ratio = 1.0

                _strategy_size_ratio = max(
                    0.0, min(_strategy_size_ratio, 1.0)
                )
                _strategy_ratio_applied = _strategy_size_ratio < 1.0

                if _strategy_ratio_applied:
                    _position_size_before_ratio = float(position_size)
                    position_size *= _strategy_size_ratio
                    logger.info(
                        f"🛡️ [STRATEGY-SIZE-RATIO] {signal.symbol} | "
                        f"{_position_size_before_ratio:,.0f}원 × {_strategy_size_ratio:.2f} "
                        f"→ {position_size:,.0f}원"
                    )

                # 🔒 0원 보호 (진입 불가 의미값 → 즉시 차단)
                if position_size <= 0:
                    logger.warning(
                        f"🚫 [매수차단] {signal.symbol} position_size=0 "
                        f"| conf={float(signal.confidence or 0):.3f} "
                        f"| 진입 불가 신호"
                    )
                    self._dl_note(signal, "POSITION_SIZE_ZERO")  # 📒 [DECISION-LEDGER] 관측 전용
                    return False

                # Strategy 축소 계약을 최소주문 보정이 다시 확대하지 못하게 한다.
                if _strategy_ratio_applied and position_size < MIN_ORDER_AMOUNT:
                    logger.info(
                        f"🚫 [STRATEGY-SIZE-RATIO-MIN] {signal.symbol} | "
                        f"축소후={position_size:,.0f}원 < 최소={MIN_ORDER_AMOUNT:,}원 "
                        f"→ 재확대 금지·진입 스킵"
                    )
                    self._dl_note(signal, "STRATEGY_SIZE_RATIO_MIN")  # 📒 [DECISION-LEDGER] 관측 전용
                    return False

                # 🆕 ADDON 배수 제한 (PRE → STD 폭증 방지)
                async with self._positions_lock:
                    existing_pos = self.positions.get(signal.symbol)

                if existing_pos:
                    meta = existing_pos.metadata or {}
                    if meta.get('entry_stage') == 'PRE' and not meta.get('addon_done', False):
                        current_amount = float(existing_pos.avg_price * existing_pos.volume)
                        max_target_amount = current_amount * 3.0  # 기존 포지션의 3배 이내

                        if position_size > max_target_amount:
                            logger.warning(
                                f"⚠️ [Addon제한] {signal.symbol} | "
                                f"계산값 {position_size:,.0f}원 → 제한값 {max_target_amount:,.0f}원 (기존 3배)"
                            )
                            position_size = max_target_amount

                # 최소 주문 금액 체크 및 조정
                if position_size < MIN_ORDER_AMOUNT:
                    if balance >= MIN_ORDER_AMOUNT:
                        logger.info(
                            f"[매수실행] {signal.symbol} 포지션 크기 조정: "
                            f"{position_size:,.0f} → {MIN_ORDER_AMOUNT:,}원 (최소금액)"
                        )
                        position_size = MIN_ORDER_AMOUNT
                    else:
                        logger.warning(
                            f"[매수실행] {signal.symbol} 잔고 부족으로 스킵: "
                            f"잔고 {balance:,.0f} < 최소금액 {MIN_ORDER_AMOUNT:,}원"
                        )
                        self._dl_note(signal, "BALANCE_BELOW_MIN")  # 📒 [DECISION-LEDGER] 관측 전용
                        return False
                
                # 포트폴리오 리스크 체크
                check = await self.portfolio_risk.check_portfolio_limits(
                    signal.symbol, position_size, self.positions, balance
                )
                
                if not check['can_trade']:
                    logger.warning(f"[매수실행] {signal.symbol} 포트폴리오 제한: {check.get('reason', 'unknown')}")
                    self._dl_note(signal, "PORTFOLIO_LIMIT")  # 📒 [DECISION-LEDGER] 관측 전용
                    return False
                
                # 주문 수량 계산 및 검증
                adjusted_size = check['adjusted_size']
                
                # 🔧 타입 안전성: adjusted_size를 float로 명시적 변환
                try:
                    adjusted_size = float(adjusted_size)
                except (ValueError, TypeError) as e:
                    logger.error(
                        f"❌ [매수계산] {signal.symbol} adjusted_size 타입 오류! "
                        f"값: {adjusted_size} (타입: {type(adjusted_size).__name__})"
                    )
                    self._dl_note(signal, "ADJUSTED_SIZE_TYPE")  # 📒 [DECISION-LEDGER] 관측 전용
                    return False
                    
                # 🔒 [B6 REAL-MIN-BUY] (REAL 한정) 실제 전송 금액 (엔진 = floor(krw_amount = adjusted_size)) 이 하한 미만이면 진입 생략 · 설정 오류 = 차단
                if self.mode == 'real':
                    try:
                        _b6_min = float(self.config.getfloat('POSITION_MANAGEMENT', 'real_min_buy_krw', fallback=0.0))
                        if not (0.0 <= _b6_min < float('inf')):
                            raise ValueError("real_min_buy_krw must be finite and non-negative")
                    except Exception as _b6_error:
                        logger.error(f"[REAL-MIN-BUY-CONFIG-ERROR] {signal.symbol} | {_b6_error}")
                        self._dl_note(signal, "REAL_MIN_BUY_CONFIG_ERROR")  # 📒 [DECISION-LEDGER] 관측 전용
                        return False
                    from math import floor as _b6_floor, isfinite as _b6_isfinite  # 이 파일은 math 를 모듈에서 import 하지 않음 (L3684 · L4582 · L6544 지역 import 관례)
                    _b6_send = float(_b6_floor(adjusted_size)) if _b6_isfinite(adjusted_size) else -1.0
                    if _b6_min > 0 and _b6_send < _b6_min:
                        logger.info(f"⏭️ [REAL-MIN-BUY-SKIP] {signal.symbol} | 전송금액={_b6_send:,.0f}원 < 하한={_b6_min:,.0f}원 → 진입 생략")
                        self._dl_note(signal, "REAL_MIN_BUY")  # 📒 [DECISION-LEDGER] 관측 전용
                        return False

                volume = adjusted_size / signal.price
                
                # 상세 로그
                logger.info(
                    f"[매수계산] {signal.symbol} | "
                    f"포지션크기: {adjusted_size:,.0f}원 ÷ "
                    f"가격: {signal.price:,.2f}원 = "
                    f"수량: {volume:.8f}개"
                )
                
                # 수량 검증 (비정상 감지)
                max_position_ratio = float(self.config.get_cached('max_position_size') or 0.20)
                max_reasonable_volume = balance * max_position_ratio * 1.5 / signal.price
                min_reasonable_volume = MIN_ORDER_AMOUNT / signal.price  # 최소 주문 금액
                
                if volume > max_reasonable_volume:
                    logger.warning(
                        f"⚠️ [매수계산] {signal.symbol} 최대수량 초과 → 진입 스킵 | "
                        f"qty={volume:.8f} > max={max_reasonable_volume:.8f} | "
                        f"잔고: {balance:,.0f}원"
                    )
                    self._dl_note(signal, "MAX_VOLUME_EXCEEDED")  # 📒 [DECISION-LEDGER] 관측 전용
                    return False
                
                if volume < min_reasonable_volume:
                    logger.warning(
                        f"⚠️ [매수계산] {signal.symbol} 최소수량 미달 → 진입 스킵 | "
                        f"qty={volume:.8f} < min={min_reasonable_volume:.8f} | "
                        f"가격: {signal.price:,.0f}원 | 금액: {adjusted_size:,.0f}원"
                    )
                    
                    # 🔒 과소 수량 쿨다운 기록
                    self.min_qty_rejection_reason[signal.symbol] = "MIN_ORDER_QTY"
                    self.min_qty_rejection_time[signal.symbol] = time.time()
                    
                    self._dl_note(signal, "MIN_VOLUME")  # 📒 [DECISION-LEDGER] 관측 전용
                    return False
                
                # 역산 검증 (이중 체크)
                calculated_cost = volume * signal.price
                if abs(calculated_cost - adjusted_size) > adjusted_size * 0.01:  # 1% 오차 허용
                    logger.error(
                        f"❌ [매수계산] {signal.symbol} 금액 불일치! "
                        f"수량×가격: {calculated_cost:,.0f}원 ≠ "
                        f"포지션크기: {adjusted_size:,.0f}원"
                    )
                    self._dl_note(signal, "COST_MISMATCH")  # 📒 [DECISION-LEDGER] 관측 전용
                    return False
                
                logger.debug(f"[매수실행] {signal.symbol} 주문 수량: {volume:.8f}개 (검증 통과)")
                
                # 주문 요청 생성
                request = OrderRequest(
                    symbol=signal.symbol,
                    side='buy',
                    volume=volume,
                    price=signal.price,
                    metadata=metadata
                )
            
            else:  # SELL
                # 포지션 체크
                if not position:
                    logger.warning(f"[매도실행] {signal.symbol} 포지션 없음")
                    return False
                
                # 실제 보유량 확인 (동기화)
                try:
                    actual_holdings = await self.engine.get_holdings(signal.symbol)
                except Exception as e:
                    logger.warning(
                        f"[매도실행] {signal.symbol} 보유량 조회 실패({e}) "
                        f"→ 메모리 포지션 보존 후 이번 사이클 매도 보류"
                    )
                    return False

                if actual_holdings is None:
                    logger.warning(
                        f"[매도실행] {signal.symbol} 보유량 응답 불명확(None) "
                        f"→ 메모리 포지션 보존 후 이번 사이클 매도 보류"
                    )
                    return False

                if actual_holdings <= 0:
                    logger.warning(f"[매도실행] {signal.symbol} 실제 보유량 없음 - 포지션 정리")
                    self.positions.pop(signal.symbol, None)
                    return False
                
                # Position과 실제 보유량 동기화 (거래소 실제 잔고 = 절대적 진실)
                if position.volume != actual_holdings:
                    logger.warning(
                        f"⚠️ [매도실행] {signal.symbol} 수량 불일치 감지! "
                        f"Position: {position.volume:.8f}개 vs "
                        f"실제: {actual_holdings:.8f}개 → 동기화"
                    )
                    position.volume = actual_holdings
                
                # 매도 수량 계산
                # 🆕 original_volume 기준 절대 비율 계산 (유령 포지션 방지!)
                if hasattr(position, 'original_volume') and position.original_volume > 0:
                    # 🆕 3차 매도 또는 마지막 매도는 무조건 잔량 100%
                    stage = 0
                    if hasattr(signal, 'metadata') and signal.metadata:
                        stage = signal.metadata.get('partial_sell_stage', 0)
                    
                    if stage == 3 or position.is_partial_sell_complete():
                        # 3차 매도는 남은 전량 처리!
                        volume = position.volume
                        logger.info(
                            f"[매도실행] {signal.symbol} {stage}차 최종 매도 | "
                            f"남은 전량 처리: {volume:.8f}개 (100%)"
                        )
                    else:
                        # 1차, 2차는 original_volume 기준 절대 비율
                        target_volume = position.original_volume * signal.sell_ratio
                        if self.mode == 'real':
                            # [보완 v3_4 G3] 단계 목표 = 전송 단위 (8 자리 내림) · 누적 체결 (8 자리 반올림 짝수) 과 정확 비교 · 상대 오차 없음
                            _md = position.metadata if isinstance(position.metadata, dict) else {}
                            _st = (_md.get('tp_stage_filled') or {}).get(str(stage)) or {}
                            _done = float(_st.get('v', 0.0))
                            _tgt_q = _tp_target_q8(position.original_volume, signal.sell_ratio)  # [보완 v3_6 ⑤] Decimal 곱
                            _rem_q = _tgt_q - _q8(_done, "even")
                            if _tgt_q > 0 and _rem_q <= 0:
                                position.record_partial_sell(stage=stage, price=(float(_st.get('krw', 0.0)) / _done) if _done > 0 else float(signal.price), volume=_done, ratio=signal.sell_ratio)
                                (_md.get('tp_stage_filled') or {}).pop(str(stage), None)
                                logger.warning(f"[PARTIAL-ORDER-STAGE-DONE] {signal.symbol} {stage}차 누적 체결 {_done:.8f} = 목표 {_tgt_q} → 단계 완료 처리 · 이번 주문 없음")
                                self._persist_position(signal.symbol)
                                return False
                            if _tgt_q > 0:
                                target_volume = float(_rem_q)
                        elif stage > 0:
                            # SIM은 실제 체결 수량을 보존하고, 같은 단계의 남은 목표만 요청한다.
                            _md = position.metadata if isinstance(position.metadata, dict) else {}
                            _st = (_md.get('tp_stage_filled') or {}).get(str(stage)) or {}
                            _done = float(_st.get('v', 0.0))
                            if target_volume > 0 and _done >= target_volume:
                                position.record_partial_sell(stage=stage, price=float(_st['krw']) / _done, volume=_done, ratio=signal.sell_ratio)
                                (_md.get('tp_stage_filled') or {}).pop(str(stage), None)
                                self._persist_position(signal.symbol)
                                return False
                            target_volume -= _done
                        volume = min(target_volume, position.volume)
                        
                else:
                    # 하위 호환: 상대 비율 (기존 방식)
                    volume = position.volume * signal.sell_ratio
                    logger.warning(
                        f"⚠️ {signal.symbol} original_volume 없음 - 상대 비율 사용"
                    )
                
                # 최종 안전장치
                if volume > actual_holdings:
                    logger.warning(
                        f"⚠️ [매도실행] {signal.symbol} 수량 초과 조정: "
                        f"{volume:.8f} → {actual_holdings:.8f}개"
                    )
                    volume = actual_holdings
                                
                # 🔥 최소 주문 금액 체크 (실거래 거절 방지)
                sell_amount = float(volume) * float(signal.price)

                # 🔒 부분매도 금액 부족 → 전량 승격 (정밀안)
                # 핵심: "실제 매도금액"이 최소금액 미달일 때만 승격한다
                if signal.sell_ratio < 0.999 and sell_amount < MIN_ORDER_AMOUNT:
                    total_position_value = float(actual_holdings) * float(signal.price)
                    logger.info(
                        f"🔁 [부분→전량승격] {signal.symbol} "
                        f"| 부분금액={sell_amount:,.0f}원 < 최소={MIN_ORDER_AMOUNT:,}원 "
                        f"| 포지션={total_position_value:,.0f}원 → 전량 매도 전환"
                    )
                    volume = actual_holdings
                    signal.sell_ratio = 1.0
                    sell_amount = float(volume) * float(signal.price)

                # 🔒 [B4 REMAINDER-GUARD] (REAL 한정) 부분 매도 뒤 남는 금액이 최소 주문 금액 × 여유 배수 미만이면 이 단계에서 전량
                #  (남은 물량이 최소 주문 금액 밑으로 내려가면 손절 · 익절 매도 자체가 막힘 = L4163 매도 차단) · 배수 = config (없으면 0 = 꺼짐)
                if self.mode == 'real' and signal.sell_ratio < 0.999:
                    try:
                        _b4_m = float(self.config.getfloat('PARTIAL_SELL', 'remainder_min_margin', fallback=0.0))
                    except Exception:
                        _b4_m = 0.0
                    _b4_rv = float(actual_holdings) - float(volume)
                    _b4_rk = _b4_rv * float(signal.price)
                    if _b4_m > 0 and _b4_rv > 0 and _b4_rk < MIN_ORDER_AMOUNT * _b4_m:
                        logger.info(
                            f"🔁 [잔량보호→전량] {signal.symbol} "
                            f"| 남는금액={_b4_rk:,.0f}원 < 최소×{_b4_m:.2f}={MIN_ORDER_AMOUNT * _b4_m:,.0f}원 → 전량 매도 전환"
                        )
                        volume = actual_holdings
                        signal.sell_ratio = 1.0
                        sell_amount = float(volume) * float(signal.price)

                if sell_amount < MIN_ORDER_AMOUNT:

                    # 🔒 로그 폭주 방지 (120초 쿨다운)
                    meta = position.metadata or {}
                    last_block_ts = meta.get("sell_min_blocked_ts")
                    now_ts = time.time()

                    should_log = True
                    if last_block_ts is not None:
                        if now_ts - float(last_block_ts) < 120:
                            should_log = False

                    if should_log:
                        logger.warning(
                            f"⚠️ [매도차단] {signal.symbol} 매도금액 부족 | "
                            f"금액={sell_amount:,.0f}원 < 최소={MIN_ORDER_AMOUNT:,}원 | "
                            f"수량={volume:.8f}개 @ {signal.price:.2f}원 | 포지션 유지"
                        )
                        position.metadata = meta
                        position.metadata["sell_min_blocked_ts"] = now_ts

                    return False
                
                # 주문 요청 생성
                request = OrderRequest(
                    symbol=position.symbol,
                    side='sell',
                    volume=volume,
                    price=signal.price,
                    metadata=metadata
                )
            
            # ========================================
            # 3. 진입가 검증 (매수만 적용)
            # ========================================
            if signal.action == "BUY":                
                # 🔒 REST 재조회 제거: _validate_entry_price()에서 저장한 캐시 사용
                # - _last_ticker_data는 L1395에서 방금 저장된 실시간 가격 (동일 사이클)
                # - REST API 재호출(~300ms) 제거 → 진입 지연 단축
                # - tolerance 검증 로직은 그대로 유지 (급락 보호 안전장치 유지)
                try:
                    cached = self._last_ticker_data.get(signal.symbol)
                    current_price = float(cached.get('trade_price', 0)) if cached else signal.price

                    if current_price > 0:
                        price_diff_pct = abs(current_price - signal.price) / signal.price

                        tolerance_map = {
                            '1m': 0.0020,   # 0.20%
                            '3m': 0.0015,   # 0.15%
                            '5m': 0.0012,   # 0.12%
                            '10m': 0.0010,  # 0.10%
                            '15m': 0.0008,  # 0.08%
                            '30m': 0.0008,  # 0.08%
                            '1h': 0.0010,   # 0.10%
                            '4h': 0.0015,   # 0.15%
                            '1d': 0.0020    # 0.20%
                        }
                        tolerance = tolerance_map.get(self.timeframe.value, 0.0015)

                        if price_diff_pct > tolerance:
                            price_change_direction = "상승" if current_price > signal.price else "하락"
                            logger.warning(
                                f"🚫 [가격검증실패] {signal.symbol} | "
                                f"산정가: {signal.price:,.0f}원 → "
                                f"현재가: {current_price:,.0f}원 ({price_change_direction}) | "
                                f"이탈률: {price_diff_pct*100:.2f}% > 허용: {tolerance*100:.2f}% | "
                                f"주문 취소 (고가진입/급락 방지)"
                            )
                            self._dl_note(signal, "PRICE_DRIFT")  # 📒 [DECISION-LEDGER] 관측 전용
                            return False

                        logger.debug(
                            f"✅ [가격검증통과] {signal.symbol} | "
                            f"이탈률: {price_diff_pct*100:.2f}% ≤ {tolerance*100:.2f}% (캐시)"
                        )
                    else:
                        logger.warning(f"⚠️ [가격검증] {signal.symbol} 캐시 가격 무효 - 검증 스킵")

                except Exception as e:
                    logger.warning(f"⚠️ [가격검증] {signal.symbol} 오류 ({e}) - 검증 스킵")
            
            # ========================================
            # 4.0 주문 실행
            # ========================================
            # 🔧 감지봉 시간 + 진입 상승률 추출 (매수만)
            _det_t = ''
            _rise_info = ''
            if hasattr(signal, 'indicators') and signal.indicators:
                _det_t = signal.indicators.get('detect_candle_time', '')
                
                # 🆕 매수일 때만: 가자미근 생애주기 + ATR 정보 추출
                if signal.action == "BUY":
                    _lifecycle = signal.indicators.get('gajami_lifecycle', {})
                    if isinstance(_lifecycle, dict):
                        _birth_price = _lifecycle.get('birth_price')
                        _rise_pct = _lifecycle.get('rise_pct')
                        # 🔒 SSoT: 로그용 진입상승률도 실제 주문가 기준으로 동적 계산
                        try:
                            if _birth_price is not None and request.price is not None and float(_birth_price) > 0:
                                _rise_pct = (float(request.price) - float(_birth_price)) / float(_birth_price)
                        except Exception:
                            pass
                        
                        # ✅ ATR 단위 정규화 (percent/fraction 혼선 방지)
                        _atr_pct_raw = signal.indicators.get('atr_pct')
                        _atr_frac = None
                        
                        try:
                            if _atr_pct_raw is None:
                                # execute_trade() 상단에서 계산한 atr_pct(%)를 fallback으로 사용
                                _atr_frac = (float(atr_pct) / 100.0) if float(atr_pct) > 0 else None
                            else:
                                _atr_pct_raw = float(_atr_pct_raw)
                                _atr_frac = _atr_pct_raw / 100.0  # atr_pct는 항상 % 단위
                        except Exception:
                            _atr_frac = None
                        
                        if _birth_price is not None and _rise_pct is not None:
                            _rise_info = (
                                f" | 탄생가={format_price_krw(_birth_price)}원"
                                f" | 진입상승률={_rise_pct*100:.3f}%"
                                f" | 진입가={format_price_krw(request.price)}원"
                            )
                            
                            # ATR 대비 비율 추가 (핵심 지표!)
                            if _atr_frac is not None and _atr_frac > 0:
                                _rise_ratio = _rise_pct / _atr_frac
                                _rise_info += (
                                    f" | ATR={_atr_frac*100:.2f}%"
                                    f" | Rise/ATR={_rise_ratio:.2f}x"
                                )

                            # 🔒 [RISE-DECISION-VAR-PARITY-0903] ★498-R1 관측 전용 — 판정 0줄
                            #  - 위 진입상승률/Rise/ATR 은 t7 이 주문가(request.price)로 재계산한 값.
                            #  - 게이트 SSoT 는 t5 cur_price 기준 판정변수다.
                            #  - §218 통계는 로그값으로 냈으므로 괴리를 실측하기 전에는
                            #    rise==0 표적을 게이트로 옮기지 않는다.
                            #  - exact-zero 는 반올림 문자열이 아니라 원시 float 로 판정한다.
                            try:
                                _rc_raw = signal.indicators.get("gajami_rise_cur_pct")
                                _rc_v = float(_rc_raw) if _rc_raw is not None else None
                            except Exception:
                                _rc_v = None
                            try:
                                _r5_raw = signal.indicators.get("gajami_rise_atr_ratio")
                                _r5_v = float(_r5_raw) if _r5_raw is not None else None
                            except Exception:
                                _r5_v = None
                            try:
                                _ord0 = int(float(_rise_pct) == 0.0) if float(_birth_price) > 0.0 else -1
                            except Exception:
                                _ord0 = -1
                            _dec0 = int(_rc_v == 0.0) if (_ord0 >= 0 and _rc_v is not None) else -1
                            _rise_info += (
                                f" | risecur={_rc_v:.6f}%"
                                if _rc_v is not None
                                else " | risecur=na"
                            )
                            _rise_info += (
                                f" | ratio5={_r5_v:.4f}x"
                                if _r5_v is not None
                                else " | ratio5=na"
                            )
                            _rise_info += f" | rise0pair={_ord0}/{_dec0}"
            
            action_label = "매수실행" if signal.action == "BUY" else "매도실행"
            logger.info(
                f"[{action_label}] {signal.symbol} | "
                f"감지시각={_det_t}{_rise_info} | "
                f"{request.volume:.8f}개 @ {format_price_krw(request.price)}원"
            )
            
            # 📒 [DECISION-LEDGER] 관측 전용 — Ledger 가 켜져 있고 BUY 일 때만 시각·주문가를 읽는다.
            #  off 이거나 SELL 이면 아래 두 줄(비교·초기화)만 실행된다.
            _dl_on = self._decision_ledger is not None and signal.action == "BUY"
            _dl_order_t = 0.0
            if _dl_on:
                try:
                    _dl_order_t = time.time()
                    self._decision_ledger.capture_submit(
                        symbol=signal.symbol,
                        order_time=_dl_order_t,
                        order_price=request.price,
                        request=request,
                    )
                except Exception:
                    pass
            if self.mode == 'real':
                _before = self.positions.get(signal.symbol)
                _real_before_volume = float(_before.volume) if _before else 0.0
                if request.side == 'buy':
                    request.metadata['krw_amount'] = adjusted_size
                else:
                    _sm = signal.metadata if isinstance(getattr(signal, 'metadata', None), dict) else {}
                    request.metadata['tp_stage'] = int(_sm.get('partial_sell_stage', 0) or 0)  # [보완 v3_4 ①] 늦은 매도 체결의 단계 가산
                    request.metadata['tp_ratio'] = float(getattr(signal, 'sell_ratio', 0) or 0)
            response = await self.engine.execute_order(request)
            if _dl_on:
                try:
                    self._decision_ledger.capture_order(
                        symbol=signal.symbol,
                        resp_time=time.time(),
                        response=response,
                    )
                except Exception:
                    pass
            
            # 실행 결과 확인: 미확정도 원장에 남으며 실패로 재주문하지 않는다.
            if self.mode == 'real' and response.metadata.get('order_unknown'):
                logger.critical(
                    f"[ORDER-UNKNOWN] {signal.symbol} | "
                    f"id={response.metadata.get('identifier')} uuid={response.order_id} "
                    f"확인체결={response.executed_volume} 잔량={response.remaining_volume}"
                )
            if not response.success:
                error_msg = getattr(response, 'error', None) or getattr(response, 'message', '원인 불명')
                if self.mode == 'real':
                    error_msg = response.metadata.get('error') or error_msg
                logger.warning(f"[주문실행] {signal.symbol} 주문 실패: {error_msg}")
                return False

            if signal.action == "SELL":
                sell_order_succeeded = True

            # 🔒 [J1-PARENT-USED-FEEDBACK-0829] 5-5
            # USED는 signal 생성/주문 접수가 아니라 실제 BUY 체결량 > 0 뒤에만 확정한다.
            # 부분체결도 시장 노출이 생겼으므로 USED이며, 체결량 0은 parent를 소비하지 않는다.
            if signal.action == "BUY":
                _j1_exec_ind = (
                    signal.indicators
                    if isinstance(signal.indicators, dict)
                    else {}
                )
                _j1_wst_at_signal = str(
                    _j1_exec_ind.get("j1_wave_state", "UNSEEN")
                    or "UNSEEN"
                )
                _j1_exec_bits = str(
                    _j1_exec_ind.get(
                        "execution_judges",
                        _j1_exec_ind.get("judges", "0000"),
                    )
                    or "0000"
                )
                try:
                    _j1_exec_fill_ok = bool(
                        float(
                            getattr(response, "executed_volume", 0.0)
                            or 0.0
                        )
                        > 0.0
                    )
                except (TypeError, ValueError):
                    _j1_exec_fill_ok = False
                _j1_exec_used = bool(
                    _j1_exec_fill_ok
                    and len(_j1_exec_bits) >= 1
                    and _j1_exec_bits[0] == "1"
                )
                _j1_parent_used_marked = False
                if _j1_exec_used:
                    try:
                        _j1_parent_used_marked = bool(
                            TechnicalIndicators.mark_j1_parent_used(
                                self.timeframe,
                                signal.symbol,
                                _j1_exec_ind.get(
                                    "cycle_parent_wave_id",
                                    "NONE",
                                ),
                            )
                        )
                    except Exception as _j1_used_e:
                        logger.warning(
                            f"⚠️ [J1-PARENT-USED-FEEDBACK] {signal.symbol} "
                            f"실체결 상태기록 실패 | {_j1_used_e}"
                        )
                _j1_exec_ind["j1_wave_state_at_signal"] = (
                    _j1_wst_at_signal
                )
                _j1_exec_ind["j1_parent_used_marked"] = int(
                    _j1_parent_used_marked
                )
                if _j1_parent_used_marked:
                    _j1_exec_ind["j1_wave_state"] = "USED"
                signal.indicators = _j1_exec_ind

            action_label = "매수실행" if signal.action == "BUY" else "매도실행"
            # 🔒 [OWNER-GAJAMI-3COND-BUYLOG-0828]
            # 실제 주문 성공 BUY의 og3 스냅샷을 기록한다.
            # [JUDGE1-GAJAMI]는 jury 조합 변화 시에만 기록되어
            # 실제 BUY 확정봉과 같은 경우가 43.1%뿐이므로
            # 이후 실거래 검증은 이 BUY 성공 로그를 SSoT로 사용한다.
            # 관측 전용 — 주문/판정/수량/가격에는 영향 없음.
            _og3_tag = ""
            if signal.action == "BUY":
                _og3_ind = signal.indicators or {}
                _og3_tag = (
                    f" | og3_ctx={_og3_ind.get('indicator_shadow_bar', 'NA')}"
                    f" og3_ready={_og3_ind.get('owner_gajami_ready', -1)}"
                    f" og3_k={_og3_ind.get('owner_gajami_count', -1)}"
                    f" og3_vpc={_og3_ind.get('owner_gajami_v_prev', -1)}"
                    f" og3_surge={_og3_ind.get('owner_gajami_surge_cur', -1)}"
                    f" og3_hold={_og3_ind.get('owner_gajami_trend_hold', -1)}"
                    f" og3_vA={_og3_ind.get('owner_gajami_varA', -1)}"
                    f" og3_vB={_og3_ind.get('owner_gajami_varB', -1)}"
                    f" og3_own={_og3_ind.get('owner_gajami_ok', -1)}"
                    # 🔒 [TLC-POS-BUYLOG-0828-R1]
                    # wpos=전체 window / ppos=실제 parent swing / aposc=현재 active leg.
                    f" wpos={_og3_ind.get('cycle_window_pos', -1)}"
                    f" ppos={_og3_ind.get('cycle_parent_pos', -1)}"
                    f" pposc={_og3_ind.get('cycle_parent_pos_closed', -1)}"
                    f" pready={_og3_ind.get('cycle_parent_ready', 0)}"
                    f" pwave={_og3_ind.get('cycle_parent_wave_id', 'NONE')}"
                    f" lowopen={_og3_ind.get('j1_low_window_open', -1)}"
                    f" wst0={_og3_ind.get('j1_wave_state_at_signal', _og3_ind.get('j1_wave_state', 'UNSEEN'))}"
                    f" wst={_og3_ind.get('j1_wave_state', 'UNSEEN')}"
                    f" used={_og3_ind.get('j1_parent_used_marked', 0)}"
                    f" aposc={_og3_ind.get('cycle_active_pos_closed', -1)}"
                    # 🔒 [J234-DIRECTION-OBSERVE-0830] ★461 — 관측 전용 · 판정 변화 0
                    # J1 Bot 실행법의 `price <= prev_candle_close` 방향성 검사는
                    # J1 vote가 있을 때만 실행되므로 J2/J3/J4 단독 진입에는 값이 남지 않는다.
                    # 모든 실제 BUY에 동일 축을 기록해 판사별 방향성 분포를 후속 검증한다.
                    f" prevc={_og3_ind.get('prev_candle_close', -1)}"
                    f" dirok={int(float(_og3_ind.get('prev_candle_close', 0) or 0) > 0 and float(getattr(signal, 'price', 0) or 0) > float(_og3_ind.get('prev_candle_close', 0) or 0))}"
                    f" prog={_og3_ind.get('cycle_active_progress', -1)}"
                    f" lbar={_og3_ind.get('cycle_active_leg_bars', -1)}"
                    # 🔒 [JUDGE-EXECUTION-HANDOFF-0828] 실제 체결 판사/route SSoT.
                    f" judges={_og3_ind.get('execution_judges', _og3_ind.get('judges', 'NA'))}"
                    # 🔒 [J3-PRODUCER-CONSUMER-INTEGRITY-0902] ★489
                    f" j3prov={_og3_ind.get('judge3_prov_ready', -1)}/"
                    f"{_og3_ind.get('judge3_prov_pass', -1)}"
                    f"@{_og3_ind.get('judge3_prov_bar', 'NA')}:"
                    f"{_og3_ind.get('judge3_prov_det', 'NA')}"
                    f"/ft={_og3_ind.get('judge3_prov_fake_top', -1)}"
                    f" route={_og3_ind.get('entry_type', 'NA')}"
                )
            logger.info(
                f"[{action_label}] {signal.symbol} 성공 | "
                f"감지시각={_det_t}{_rise_info} | "
                f"체결: {response.executed_volume:.8f}개 @ {format_price_krw(response.executed_price)}원"
                f"{_og3_tag}"
            )
        
            # ========================================
            # 4. 포지션 관리
            # ========================================
            if signal.action == "BUY":
                if position is not None:
                    # 🆕 Add-on 시도
                    logger.debug(
                        f"🔄 [포지션관리] {signal.symbol} | "
                        f"기존 포지션 존재 → Add-on 로직 실행"
                    )
                    await self._add_to_position(position, signal, response)
                    # 📒 [DECISION-LEDGER] 관측 전용 — 추가매수 주문 뒤 표지(생성 여부는 판정하지 않음)
                    if self._decision_ledger is not None:
                        try:
                            self._decision_ledger.capture_position(
                                symbol=signal.symbol, created=None, kind="ADDON"
                            )
                        except Exception:
                            pass
                else:
                    # 신규 포지션 생성
                    await self._create_position(signal, response)
                    # 📒 [DECISION-LEDGER] 관측 전용 — 신규 포지션이 실제로 만들어졌는지 읽는다.
                    #  (_create_position 은 실패해도 예외 없이 return 한다: 체결 0 · 비정상 체결액 롤백 · 손익비 검증 실패)
                    if self._decision_ledger is not None:
                        try:
                            self._decision_ledger.capture_position(
                                symbol=signal.symbol,
                                created=(signal.symbol in self.positions),
                                kind="NEW",
                            )
                        except Exception:
                            pass
            else:
                await self._update_position(position, signal, response)
                sell_state_synced = True

            # ========================================
            # 5. 거래 기록 (실패해도 거래 성공은 유지)
            # ========================================
            try:
                await self._record_trade(position, signal, request, response)
            except Exception as e:
                logger.exception(f"⚠️ 거래 기록 실패 (거래는 성공): {e}")
                if self.mode == 'real':
                    response.metadata['_record_failed'] = True

            if self.mode == 'real':
                from math import isclose
                
                _after = self.positions.get(signal.symbol)
                _actual = float(_after.volume) if _after else 0.0
                _expected = (
                    _real_before_volume + response.executed_volume
                    if signal.action == "BUY"
                    else max(0.0, _real_before_volume - response.executed_volume)
                )
                if not isclose(_actual, _expected, rel_tol=1e-10, abs_tol=1e-12):
                    logger.critical(
                        f"[ORDER-APPLY-HOLD] {signal.symbol} "
                        f"기대수량={_expected} 반영수량={_actual} | 원장 차단 유지"
                    )
                    try:
                        self.engine.mark_apply_hold(response.metadata.get('identifier'), f"기대 {_expected} · 반영 {_actual}")  # [보완 v3_4 ①] L3 · L5
                    except Exception as _ah:
                        logger.critical(f"[ORDER-APPLY-HOLD] 표시 실패 {signal.symbol}: {_ah}")
                    return False
                _ident = response.metadata.get('identifier')
                _rf = bool(response.metadata.get('_record_failed', False))
                _holder = self.positions.get(signal.symbol) or position  # [보완 v3_5] 전량 청산이면 직전 포지션 객체 = 청산 표지
                if _holder is not None and _ident:
                    if not isinstance(_holder.metadata, dict):
                        _holder.metadata = {}
                    _holder.metadata.setdefault('order_applied', {})[_ident] = dict(  # [보완 v3_5] 정상 · 복구 공통 주문별 반영 기록
                        v=float(response.metadata.get('executed_volume_q') or response.executed_volume),
                        f=float(response.executed_price) * float(response.executed_volume), c=float(response.fee), acc=not _rf,
                        side=('sell' if signal.action == "SELL" else 'buy'),  # [보완 v3_6 ②] 복구가 RECORD_RETRY 부채를 만들 근거
                        ap=float(position.avg_price) if position is not None else 0.0,
                        efp=self._entry_fee_share(position, response.executed_volume))
                self._persist_position(signal.symbol, fallback=_holder)  # 반영 기록 · 포지션 · TP 진행 영속 → 그 뒤 반영 확인
                _debt = None
                if _rf and signal.action == "SELL" and position is not None:
                    _debt = dict(kind="sell_record", symbol=signal.symbol, ident=_ident, avg_price=float(position.avg_price),
                                 target_v=float(response.metadata.get('executed_volume_q') or response.executed_volume),
                                 target_f=float(response.executed_price) * float(response.executed_volume), target_c=float(response.fee),
                                 reason=str(getattr(signal, 'reason', '') or ''),  # [보완 v3_7] 원 신호 사유 (리포트 분류 유지) · 사건 식별 = event_id
                                 efp=self._entry_fee_share(position, response.executed_volume),
                                 efp_total=float(getattr(position, 'entry_fee_paid', 0.0) or 0.0), ov=float(getattr(position, 'original_volume', 0.0) or 0.0))  # [보완 v3_10 K3]
                self.engine.acknowledge_order(response, accounting_ok=not _rf, debt=_debt)
            logger.info(f"✅ 거래 성공: {signal.action} {signal.symbol}")
            return True
            
        except Exception as e:
            logger.exception(f"거래 실행 오류: {e}")
            self._dl_note(signal, "EXECUTE_EXCEPTION")  # 📒 [DECISION-LEDGER] 관측 전용
            return False
        finally:
            if (
                sell_claimed
                and position is not None
                and self.positions.get(signal.symbol) is position
                and (not sell_order_succeeded or sell_state_synced)
            ):
                position._closing = False
    
    # ========================================
    # 🔒 SSoT-META: 손절/익절 비율 최종 검증
    # 책임: Bot (최종 의사결정자)
    # 위치: 유일한 검증 지점
    # ========================================
    
    def _validate_stop_take_pct(
        self,
        *,
        symbol: str,
        stop_loss_pct: float,
        take_profit_pct: float | None = None,
        min_stop_pct: float | None = None,
        max_stop_pct: float = -0.10,    # -10% (★극단 안전망 · t6 MAX_STOP_LOSS_PCT 와 같은 성격)
        min_take_pct: float = 0.005,    # +0.5%
        max_take_pct: float = 0.50,     # +50%
        eps: float = 1e-6,
    ) -> None:
        """
        손절/익절 비율 최종 검증 (SSoT)
        
        Args:
            symbol: 종목 심볼
            stop_loss_pct: 손절 비율 (음수)
            take_profit_pct: 익절 비율 (양수, 선택)
            min_stop_pct: 최소 손절 비율 (기본 -0.5%)
            max_stop_pct: 최대 손절 비율 (기본 -10%)
            min_take_pct: 최소 익절 비율 (기본 +0.5%)
            max_take_pct: 최대 익절 비율 (기본 +50%)
            eps: 부동소수점 여유 (기본 1e-6)
            
        Raises:
            ValueError: 비율이 허용 범위를 벗어날 때
        """
        # 손절 검증 (경계값 포함)
        min_stop_pct = -abs(float(self.settings.stop_loss_pct)) if min_stop_pct is None else min_stop_pct
        if stop_loss_pct > (min_stop_pct + eps) or stop_loss_pct < max_stop_pct:
            raise ValueError(
                f"[STOP_LOSS_INVALID] {symbol} "
                f"손절 비율 {stop_loss_pct*100:.3f}% "
                f"(허용: {max_stop_pct*100:.1f}% ~ {min_stop_pct*100:.1f}%)"
            )
        
        # 익절 검증 (선택)
        if take_profit_pct is not None:
            if take_profit_pct < (min_take_pct - eps) or take_profit_pct > max_take_pct:
                raise ValueError(
                    f"[TAKE_PROFIT_INVALID] {symbol} "
                    f"익절 비율 {take_profit_pct*100:.3f}% "
                    f"(허용: {min_take_pct*100:.1f}% ~ {max_take_pct*100:.1f}%)"
                )
    
    async def _create_position(self, signal, response):
        """
        포지션 생성 - 안전장치 3중 검증
        Args:
            signal: 매수 신호
            response: 주문 체결 응답
        안전장치:
            1. 체결 수량 검증
            2. ATR 기반 손익 계산 (Fallback 포함)
            3. 손절가 최종 검증 (3단계)
        """
        # ========================================
        # 1. 체결 수량 검증
        # ========================================
        if response.executed_volume <= 0:
            logger.error(
                f"❌ [포지션생성] {signal.symbol} 체결 수량 0 - 생성 중단"
            )
            self._dl_note(signal, "POS_FILL_ZERO")  # 📒 [DECISION-LEDGER] 관측 전용
            return
        
        # 금액 역산 검증 (슬리피지 + 수수료 여유 반영)
        total_cost = response.executed_volume * response.executed_price
        balance = await self.engine.get_balance()
        max_pos_ratio = float(self.config.get_cached('max_position_size') or 0.20)
        # 1.2 → 1.35로 증가 (슬리피지 3% + 수수료 0.05% + 여유 10%)
        expected_max_cost = balance * max_pos_ratio * 1.35
        
        if total_cost > expected_max_cost:
            logger.error(
                f"❌ [포지션생성] {signal.symbol} 비정상 체결액! "
                f"{total_cost:,.0f}원 > 예상최대 {expected_max_cost:,.0f}원"
            )
            # 롤백 시도
            rollback_success = False
            try:
                rollback_request = OrderRequest(
                    symbol=signal.symbol,
                    side='sell',
                    volume=response.executed_volume,
                    price=response.executed_price,
                    metadata={
                        'rollback': True,
                        'regime': signal.market_regime.value if signal.market_regime else 'ROLLBACK'
                    }
                )
                rollback_response = await self.engine.execute_order(rollback_request)
                rollback_success = bool(getattr(rollback_response, 'success', False))

                if rollback_success:
                    logger.warning(f"⚠️ [포지션생성] {signal.symbol} 롤백 완료")
                else:
                    logger.critical(
                        f"🚨 [롤백실패] {signal.symbol} 롤백 응답 success=False "
                        f"→ 거래소 잔존 의심. 잔고 동기화 필요."
                    )

            except Exception as e:
                logger.critical(
                    f"🚨 [롤백실패] {signal.symbol} 롤백 예외: {e} "
                    f"→ 거래소 잔존 의심. 잔고 동기화 필요."
                )
            self._dl_note(signal, "POS_ABNORMAL_COST_ROLLBACK")  # 📒 [DECISION-LEDGER] 관측 전용
            return
        
        # ========================================
        # 2. ATR 안전하게 추출
        # ========================================
        atr_raw = signal.indicators.get('atr', 0)
        atr = safe_extract_atr(atr_raw)

        # 🔒 ATR=0 복구: 재시작/히스토리 불완전 시 position_size 과대 배분 방지
        # 참고: SL/TP는 config 기반(ATR 불참)이므로 손익 계산에는 무영향
        if atr <= 0 and response.executed_price > 0:
            _atr_pct_fb = float(signal.indicators.get('atr_pct', 0) or 0)
            if _atr_pct_fb > 0:
                atr = response.executed_price * (_atr_pct_fb / 100.0)
                logger.warning(
                    f"⚠️ [ATR-RECOVER] {signal.symbol} atr=0 → "
                    f"atr_pct({_atr_pct_fb:.3f}%)로 환산 복구 | atr={format_price_krw(atr)}"
                )
            else:
                _atr_min = float(self.config.get_cached('atr_min_threshold', 0.5) or 0.5)
                atr = response.executed_price * (_atr_min / 100.0)
                logger.warning(
                    f"⚠️ [ATR-FLOOR] {signal.symbol} atr=0 & atr_pct 없음 → "
                    f"ini 하한({_atr_min:.3f}%) 적용 | atr={format_price_krw(atr)}"
                )

        # ATR 비율 계산
        atr_pct = (atr / response.executed_price * 100) if response.executed_price > 0 else 0

        logger.debug(
            f"[포지션생성] {signal.symbol} ATR: {format_price_krw(atr)}원 ({atr_pct:.4f}%)"
        )

        # ========================================
        # 3. 손익 계산 (ATR 기반)
        # ========================================

        stop_loss, take_profit, trailing = self.signal_analyzer.risk_calculator.calculate_stops(
            price=response.executed_price,
            atr=atr,
            timeframe=self.timeframe.value,
            strategy_type=None,
            regime=signal.market_regime,
            symbol=signal.symbol
        )
        
        # ========================================
        # 4. 손절가 1차 검증 (>= 진입가 체크)
        # ========================================
        if stop_loss >= response.executed_price:
            logger.error(
                f"❌ [포지션생성] {signal.symbol} 손절가 오류 (1차)! "
                f"손절:{format_price_krw(stop_loss)} >= 진입:{format_price_krw(response.executed_price)}"
            )
            # 강제 조정 -2%
            stop_loss = response.executed_price * 0.98
            logger.warning(
                f"⚠️ [포지션생성] {signal.symbol} 손절가 강제 조정: "
                f"{format_price_krw(stop_loss)}원 (-2%)"
            )
        
        # ========================================
        # 5. 손절/익절 비율 최종 검증 (SSoT)
        # ========================================
        stop_loss_pct = (stop_loss - response.executed_price) / response.executed_price
        take_profit_pct = (take_profit - response.executed_price) / response.executed_price
        
        try:
            # 🔒 SSoT: 유일한 검증 지점
            from Trading_bot_1_core import get_tick_size
            _tick_frac = (get_tick_size(response.executed_price) / response.executed_price) if response.executed_price > 0 else 0.0
            self._validate_stop_take_pct(
                symbol=signal.symbol,
                stop_loss_pct=stop_loss_pct,
                take_profit_pct=take_profit_pct,
                max_stop_pct=-0.10 - _tick_frac,
            )
        except ValueError as e:
            # 검증 실패 시 포지션 생성 중단
            logger.error(f"❌ [포지션생성] {e}")
            self._dl_note(signal, "POS_SLTP_INVALID")  # 📒 [DECISION-LEDGER] 관측 전용
            return None
        
        # ========================================
        # 7. 최종 로그 (손익 확정값)
        # ========================================
        logger.info(
            f"[포지션생성] {signal.symbol} | "
            f"수량: {response.executed_volume:.8f}개 × "
            f"가격: {format_price_krw(response.executed_price)}원 = "
            f"총액: {total_cost:,.0f}원 | "
            f"손절: {format_price_krw(stop_loss)}원 ({stop_loss_pct*100:.2f}%) / "
            f"익절: {format_price_krw(take_profit)}원 ({take_profit_pct*100:.2f}%)"
        )
        
        # ========================================
        # 8. 타임프레임별 타임아웃 설정
        # ========================================
        timeout_hours = (
            self.config.get_cached('timeout_hours') or
            self.timeframe.config.get('timeout_hours') or
            DEFAULT_TIMEOUT_HOURS
        )
        
        # ========================================
        # 9. 중복 매수 차단 (불타기/물타기 방지) ✅
        # ========================================
        # 🆕 Add-on 로직 추가로 인해 이 검증은 execute_trade에서 처리
        # (_create_position은 신규 포지션만 생성하도록 보장됨)
        
        # ========================================
        # 9.5. Strategy에서 선언한 entry_stage 받기
        # ========================================
        sig_indicators = getattr(signal, 'indicators', None)
        if sig_indicators and isinstance(sig_indicators, dict):
            entry_stage = sig_indicators.get('entry_stage', 'STD')
            position_ratio = float(sig_indicators.get('position_size_ratio', 1.0))
            soleus_upper = float(sig_indicators.get('soleus_upper_price', 0.0))
        else:
            entry_stage = 'STD'
            position_ratio = 1.0
            soleus_upper = 0.0
        
        # ========================================
        # 🔒 진입 신뢰도 저장 (리포트/DB용 SSoT)
        # ========================================
        # 철학: 매도 시 강제손절 신호(confidence=1.0)가 DB에 저장되는 것을 방지
        #       진입 신뢰도를 Position.metadata에 저장하여 매도 시 복원
        # ========================================
        entry_confidence = float(getattr(signal, "confidence", 0.0) or 0.0)
        
        # ========================================
        # 10. 신규 포지션 생성 (기존 로직)
        # ========================================
        new_position = Position(
            symbol=signal.symbol,
            volume=response.executed_volume,
            avg_price=response.executed_price,
            entry_time=datetime.now(),
            entry_signals={
                **(signal.signal_components or {}),
                "dur_5x10_bars": float(
                    (signal.indicators or {}).get("dur_5x10_bars", -1)
                ),
            },
            entry_reason=signal.reason,
            stop_loss=stop_loss,
            take_profit=take_profit,
            peak_price=response.executed_price,
            trailing_stop_distance=trailing,
            entry_fee_paid=response.fee,
            entry_atr=atr,
            entry_regime=signal.market_regime,
            original_volume=response.executed_volume,
            metadata={
                'timeout_hours': timeout_hours,
                'timeframe': self.timeframe.value,
                'atr_pct': atr_pct,
                'created_at': datetime.now().isoformat(),
                # 🆕 Strategy가 선언한 진입 단계 + Add-on 1회 제한
                'entry_stage': entry_stage,
                'entry_type': signal.indicators.get('entry_type', 'STANDARD'),  # 🏷️ Strategy/Bot 최종 실행 route
                # 🔒 [JUDGE-EXECUTION-HANDOFF-0828]
                # Strategy 판사표와 Bot 실행시점 판사표를 함께 보존한다.
                'strategy_judges': signal.indicators.get(
                    'strategy_judges',
                    signal.indicators.get('judges', ''),
                ),
                'execution_judges': signal.indicators.get(
                    'execution_judges',
                    signal.indicators.get('judges', ''),
                ),
                'entry_authorization_route': signal.indicators.get(
                    'entry_authorization_route', ''
                ),
                # 🔒 [J1-PARENT-USED-FEEDBACK-0829] 5-5 감사 메타.
                'j1_parent_wave_id': signal.indicators.get(
                    'cycle_parent_wave_id', 'NONE'
                ),
                'j1_wave_state_at_signal': signal.indicators.get(
                    'j1_wave_state_at_signal',
                    signal.indicators.get('j1_wave_state', 'UNSEEN'),
                ),
                'j1_parent_used_marked': int(
                    signal.indicators.get('j1_parent_used_marked', 0) or 0
                ),
                'bot_j1_lane_denied': int(
                    signal.indicators.get('bot_j1_lane_denied', 0) or 0
                ),
                'bot_j1_lane_deny_reason': signal.indicators.get(
                    'bot_j1_lane_deny_reason', ''
                ),
                'addon_done': False,
                'position_size_ratio': position_ratio,  # 디버깅용
                'soleus_upper_price': soleus_upper,  # 🆕 추격 방지용
                'entry_confidence': entry_confidence  # 🔒 진입 신뢰도 (리포트/DB용 SSoT)
            }
        )
        
        # ========================================
        # 11. Lock으로 보호하여 저장
        # ========================================
        async with self._positions_lock:
            self.positions[signal.symbol] = new_position
               
        # ========================================
        # 12. 연속손실 리셋
        # ========================================
        self.consecutive_losses[signal.symbol] = 0
        
        logger.info(
            f"✅ [포지션생성] {signal.symbol} 포지션 생성 완료 "
            f"(타임아웃: {timeout_hours:.1f}시간)"
        )

        try:
            await self.performance.record_buy_open(
                new_position,
                response,
                signal,
                timeframe=self.timeframe.value,
            )
        except Exception as e:
            if self.mode == 'real':
                response.metadata['_record_failed'] = True
            logger.warning(
                f"[B2] {signal.symbol} BUY leg 기록 호출 실패: {e}"
            )

    async def _add_to_position(self, position, signal, response):
        """
        🔥 Add-on(추가매수) 규칙
        - 1회만 허용 (addon_done=True면 금지)
        - 기존 포지션이 PRE일 때만 허용
        - 이번 BUY 신호가 STD일 때만 허용 (Strategy가 선언)
        """
        try:
            # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
            # 1. 기본 검증
            # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
            if response.executed_volume <= 0 or response.executed_price <= 0:
                logger.warning(
                    f"⚠️ [AddOn] {position.symbol} 체결값 이상 "
                    f"(vol={response.executed_volume:.8f}, price={response.executed_price:.0f}) → 스킵"
                )
                return

            # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
            # 2. metadata 안전 추출
            # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
            meta = position.metadata
            if meta is None:
                logger.warning(f"⚠️ [AddOn] {position.symbol} metadata 없음 → 스킵")
                return

            # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
            # 3. Add-on 1회 제한 검증
            # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
            if meta.get('addon_done', False):
                logger.info(
                    f"🚫 [AddOn] {position.symbol} | "
                    f"이미 1회 완료 → 추가진입 차단"
                )
                return
            # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
            # 4. PRE 단계 검증 (기존 포지션)
            # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
            existing_stage = meta.get('entry_stage', 'STD')
            if existing_stage != 'PRE':
                logger.info(
                    f"🚫 [AddOn] {position.symbol} | "
                    f"PRE 포지션 아님 (stage={existing_stage}) → 추가매수 차단"
                )
                return

            # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
            # 5. STD 신호 검증 (이번 신호)
            # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
            sig_ind = getattr(signal, 'indicators', None)
            if sig_ind is None or not isinstance(sig_ind, dict):
                logger.debug(
                    f"⚠️ [AddOn] {position.symbol} indicators 없음 → 스킵"
                )
                return
            
            signal_stage = sig_ind.get('entry_stage', 'STD')
            if signal_stage != 'STD':
                logger.info(
                    f"🚫 [AddOn] {position.symbol} | "
                    f"이번 신호가 STD 아님 (stage={signal_stage}) → 추가매수 차단"
                )
                return

            # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
            # 6. 평균단가 계산
            # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
            prev_vol = float(position.volume)
            prev_avg = float(position.avg_price)

            new_vol = (float(_q8(prev_vol, "even") + _q8(response.executed_volume, "even")) if self.mode == 'real'
                       else prev_vol + float(response.executed_volume))  # [보완 v3_6 ⑤] REAL = 8 자리 재양자화 합 (SIM 불변)
            if new_vol <= 0:
                logger.warning(
                    f"⚠️ [AddOn] {position.symbol} new_vol={new_vol:.8f} <= 0 → 스킵"
                )
                return

            new_avg = (
                prev_avg * prev_vol + 
                float(response.executed_price) * float(response.executed_volume)
            ) / new_vol

            # 🔒 평균단가 상한: 가자미근 상단(soleus_upper_price) 초과 금지
            upper = float(meta.get('soleus_upper_price', new_avg))
            if new_avg > upper:
                logger.info(
                    f"🚫 [AddOn-Block] {position.symbol} 평균단가 초과 "
                    f"({new_avg:.0f}원 > {upper:.0f}원) → 추가진입 차단"
                )
                return
            # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
            # 7. Lock으로 보호하여 원자적 업데이트
            # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
            async with self._positions_lock:
                # 🆕 재검증 (Race Condition 방지)
                current_meta = position.metadata or {}
                if current_meta.get('addon_done', False):
                    logger.warning(
                        f"⚠️ [AddOn] {position.symbol} "
                        f"Race Condition 감지 - 이미 Add-on 완료"
                    )
                    return

                # 포지션 업데이트
                position.volume = new_vol
                position.avg_price = new_avg
                position.peak_price = max(
                    float(position.peak_price), 
                    float(response.executed_price)
                )
                position.entry_fee_paid = (
                    float(getattr(position, 'entry_fee_paid', 0.0)) + 
                    float(getattr(response, 'fee', 0.0))
                )

                # 🆕 original_volume 갱신 (중요!)
                position.original_volume = new_vol

                # metadata 업데이트
                position.metadata['addon_done'] = True
                position.metadata['entry_stage'] = 'STD'
                position.metadata['addon_time'] = datetime.now().isoformat()
                position.metadata['addon_volume'] = float(response.executed_volume)
                position.metadata['addon_price'] = float(response.executed_price)

            # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
            # 8. 성공 로그
            # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
            logger.info(
                f"✅ [AddOn] {position.symbol} PRE→STD 추가매수 완료 | "
                f"평균단가: {prev_avg:,.0f}원 → {new_avg:,.0f}원 "
                f"({((new_avg/prev_avg - 1)*100):+.2f}%) | "
                f"수량: {prev_vol:.8f} → {new_vol:.8f} "
                f"(+{response.executed_volume:.8f}) | "
                f"추가금액: {response.executed_price * response.executed_volume:,.0f}원"
            )

        except Exception as e:
            logger.error(
                f"❌ [AddOn] {getattr(position, 'symbol', 'UNKNOWN')} "
                f"오류: {e}", 
                exc_info=True
            )
    
    async def _update_position(self, position, signal, response):
        """포지션 업데이트 - 부분 매도 기록 추가"""
        
        # ========================================
        # 1. 성과 기록 (기존 코드 유지)
        # ========================================
        result = ExecutionResult(
            success=True,
            symbol=position.symbol,
            side="sell",
            executed_volume=response.executed_volume,
            avg_price=response.executed_price,
            total_fee=response.fee,
            slippage_pct=getattr(response, "slippage_pct", 0.0) or 0.0   # [SELLSLIP 11-14] 매도 미끄러짐 DB 전달 (전엔 빠져 0 기록)
        )
        
        # ========================================
        # 🔒 매도 시 진입 신뢰도 복원 전달 (강제손절 1.0 오염 방지)
        # ========================================
        # 철학:
        # - signal.confidence = 매도 신호 신뢰도 (강제손절=1.0, 타임아웃=1.0)
        # - position.metadata['entry_confidence'] = 진입 신뢰도 (실제 값)
        # - DB/리포트에는 진입 신뢰도만 저장되어야 함
        # ========================================
        metadata_for_record = {}
        meta = getattr(position, "metadata", None) or {}
        
        # 진입 신뢰도 (안전한 추출)
        entry_conf = meta.get("entry_confidence")
        if entry_conf is not None:
            try:
                if isinstance(entry_conf, dict):
                    # dict에서 값 추출 시도
                    entry_conf = (
                        entry_conf.get("entry_confidence")
                        or entry_conf.get("confidence")
                        or entry_conf.get("final_conf")
                        or entry_conf.get("value")
                        or entry_conf.get("conf")
                        or 0.0
                    )
                metadata_for_record["entry_confidence"] = float(entry_conf)
            except (ValueError, TypeError) as e:
                logger.warning(
                    f"⚠️ [{position.symbol}] entry_confidence 변환 실패: {e} → 0.0 대체"
                )
                metadata_for_record["entry_confidence"] = 0.0
        
        # 🆕 진입 체제 추가
        entry_regime = getattr(position, "entry_regime", None)
        if entry_regime:
            # MarketRegime enum → string 변환
            if hasattr(entry_regime, 'value'):
                metadata_for_record["entry_regime"] = entry_regime.value
            else:
                metadata_for_record["entry_regime"] = str(entry_regime)
        
        _md_r = getattr(response, 'metadata', None) or {}
        _ev_ident = _md_r.get('identifier') if self.mode == 'real' else None  # [보완 v3_8] REAL = 정상 · 복구 · 재기록 같은 함수 (SIM = 예전 그대로)
        try:
            if _ev_ident:
                _rec_ok = await self._record_sell_shortfall(
                    _ev_ident, position.symbol, float(_q8(_md_r.get('executed_volume_q') or response.executed_volume, 'even')),
                    float(response.executed_price) * float(_q8(_md_r.get('executed_volume_q') or response.executed_volume, 'even')), float(response.fee),  # [보완 v3_10 K8] 목표 수량과 같은 수량
                    position, str(getattr(signal, 'reason', '') or ''), md=metadata_for_record, sig=signal)
            else:
                _rec_ok = await self.performance.record_trade_complete(
                    position, result, signal, metadata_for_record
                )
            if self.mode == 'real' and _rec_ok is not True:  # [보완 v3_6] None 등 = 미기록
                response.metadata['_record_failed'] = True  # [보완 v3_5] 성과 함수가 미기록을 알려 줌 (예외는 함수 안에서 처리됨)
        except Exception as _rec_e:
            if self.mode == 'real':
                response.metadata['_record_failed'] = True
            logger.exception(
                f"[RECORD-FAIL] {position.symbol} "
                f"거래기록 실패(거래·상태는 유지): {_rec_e}"
            )
        
        # ========================================
        # 2. 손익 계산 (기존 코드 유지)
        # ========================================
        from Trading_bot_1_core import round_money, is_positive

        entry_volume_base = float(
            getattr(position, "original_volume", 0.0)
            or position.volume
            or 0.0
        )
        entry_fee_alloc = 0.0
        if entry_volume_base > 0.0:
            entry_fee_ratio = min(
                1.0,
                max(
                    0.0,
                    float(response.executed_volume)
                    / entry_volume_base,
                ),
            )
            entry_fee_alloc = (
                float(getattr(position, "entry_fee_paid", 0.0) or 0.0)
                * entry_fee_ratio
            )

        pnl = (
            response.executed_price - position.avg_price
        ) * response.executed_volume
        pnl -= response.fee
        pnl -= entry_fee_alloc
        pnl = round_money(pnl)
        
        # ========================================
        # 🆕 3. 부분 매도 기록
        # ========================================
        is_partial_sell = False
        stage = 0
        # 묶음 v4 : sim 부분 체결 = 응답 status 'partial' 또는 잔량 > 0 → 단계 완료로 보지 않음
        _sim_partial = self.mode != 'real' and (
            str(getattr(response, 'status', 'filled') or 'filled').lower() == 'partial'
            or float(getattr(response, 'remaining_volume', 0) or 0) > 0
        )
        _full_exit = (
            _real_remaining_q8(position.volume, response.executed_volume) <= 0  # [보완 v3_4 G6] 거래소 수량 단위 1e-8 · 잔량 계산 · 0 판정과 같은 함수
            if self.mode == 'real' else round(position.volume - float(response.executed_volume), 8) <= 0  # [SIMDUST] SIM 1e-8 잔량은 보존
        )
        
        if hasattr(signal, 'metadata') and signal.metadata:
            stage = signal.metadata.get('partial_sell_stage', 0)
            
            if stage > 0:
                is_partial_sell = True
                
                # REAL 미종결/부분 체결은 TP 단계 완료로 올리지 않는다.
                if ((self.mode != 'real' and not _sim_partial) or (
                        self.mode == 'real'
                        and response.metadata.get('terminal')
                        and response.metadata.get('request_satisfied')
                        and not response.metadata.get('order_unknown'))):
                    _pv, _pp = response.executed_volume, response.executed_price
                    if isinstance(position.metadata, dict):  # 묶음 v4 : REAL · sim 공통
                        # [보완 v3_4 G3] 앞선 부분 체결 합산 (20 @ 100 + 20 @ 120 → 40 @ 110) · 누적 정리
                        _prev = (position.metadata.get('tp_stage_filled') or {}).pop(str(stage), None)
                        if _prev and float(_prev.get('v', 0.0)) > 0:
                            _pv = (float(_q8(_prev['v'], "even") + _q8(response.executed_volume, "even"))
                                   if self.mode == 'real' else float(_prev['v']) + float(response.executed_volume))
                            _pp = (float(_prev['krw']) + float(response.executed_volume) * float(response.executed_price)) / _pv
                    position.record_partial_sell(
                        stage=stage,
                        price=_pp,
                        volume=_pv,
                        ratio=signal.sell_ratio
                    )
                    return_pct = (response.executed_price - position.avg_price) / position.avg_price * 100
                    logger.info(
                        f"✅ [부분매도] {position.symbol} {stage}차 완료 | "
                        f"{response.executed_volume:.8f}개 @ {format_price_krw(response.executed_price)}원 | "
                        f"수익률: {return_pct:+.2f}%"
                    )
                else:
                    # [보완 v3_4 G3] 같은 단계 누적 (수량 · 금액) · 목표 (전송 단위) 와 정확 비교 · 도달 = 단계 완료 (로그 분리)
                    _md = position.metadata if isinstance(position.metadata, dict) else {}
                    _all = _md.setdefault('tp_stage_filled', {})
                    _st = _all.setdefault(str(stage), {'v': 0.0, 'krw': 0.0})
                    _st['v'] = (float(_q8(_st['v'], "even") + _q8(response.executed_volume, "even"))
                                if self.mode == 'real' else float(_st['v']) + float(response.executed_volume))
                    _st['krw'] = float(_st['krw']) + float(response.executed_volume) * float(response.executed_price)
                    position.metadata = _md
                    _tgt_q = (_tp_target_q8(getattr(position, 'original_volume', 0.0) or 0.0, signal.sell_ratio)
                              if self.mode == 'real' else float(getattr(position, 'original_volume', 0.0) or 0.0) * float(signal.sell_ratio))
                    _stage_done = ((_tgt_q > 0 and _q8(_st['v'], "even") >= _tgt_q)
                                   if self.mode == 'real' else (
                                       _full_exit if stage == 3 or signal.sell_ratio >= 1.0
                                       else _tgt_q > 0 and _st['v'] >= _tgt_q))
                    if _stage_done:
                        position.record_partial_sell(stage=stage, price=_st['krw'] / _st['v'], volume=_st['v'], ratio=signal.sell_ratio)
                        _all.pop(str(stage), None)
                        logger.warning(f"[PARTIAL-ORDER-STAGE-DONE] {position.symbol} {stage}차 누적 체결 = 목표 {_tgt_q} → 단계 완료")
                    else:
                        logger.critical(
                            f"[PARTIAL-ORDER-HOLD] {position.symbol} "
                            f"{stage}차 미완료 | 확인된 체결량만 차감·원장 대조 유지 | 단계 누적 {_st['v']:.8f} / 목표 {_tgt_q}"
                        )
        
        # ========================================
        # 4. 손익 로그 (기존 코드 개선)
        # ========================================
        # 🔒 전량 매도 쿨다운은 손익과 무관하게 무조건 설정 (즉시 재매수 방지)
        if not is_partial_sell and _full_exit:
            self.sell_cooldown[position.symbol] = datetime.now()
            logger.info(
                f"⏰ [쿨다운설정] {position.symbol} | "
                f"전량 매도 완료 → {self.cooldown_minutes}분 재매수 차단 시작"
            )
        
        if is_positive(pnl):  # 수익
            # 🔒 전량익절만 연속손실 리셋 (부분익절은 리스크 유지)
            if not is_partial_sell:
                self.consecutive_losses[position.symbol] = 0
            
            # 회복 거래 성공 처리
            is_recovery_trade = (
                hasattr(position, 'metadata') and
                position.metadata and
                position.metadata.get('recovery_trade', False)
            )
            
            if is_recovery_trade:
                override_reason = position.metadata.get('override_reason', 'unknown')
                logger.info(
                    f"🌟 {position.symbol} 회복 거래 성공! "
                    f"수익률 {(pnl / (position.avg_price * response.executed_volume) * 100):.2f}% | "
                    f"손익 {pnl:,.0f}원 | 모드: {override_reason}"
                )
                self.recovery_trades.pop(position.symbol, None)
                
                # 기존 킬스위치/캐시보존 해제 로직이 있다면 여기 유지
            
            # 일반 전량 수익 매도완료 로그는 Performance.record_trade_complete()가 SSoT.
            # 7_bot에서는 중복 "매도 완료: 수익률" 로그를 남기지 않는다.
        
        else:  # 손실
            # 손실이면 연속손실 증가
            self.consecutive_losses[position.symbol] += 1
            loss_count = self.consecutive_losses[position.symbol]
            
            # 회복 거래 실패
            is_recovery_trade = (
                hasattr(position, 'metadata') and
                position.metadata and
                position.metadata.get('recovery_trade', False)
            )
            
            if is_recovery_trade:
                logger.warning(
                    f"💔 {position.symbol} 회복 거래 실패 "
                    f"수익률 {(pnl / (position.avg_price * response.executed_volume) * 100):.2f}% | "
                    f"손익 {pnl:,.0f}원 | "
                    f"캐시보존모드 유지 (연속손실 {loss_count}회)"
                )
                self.recovery_trades.pop(position.symbol, None)
            
            # 일반 전량 손실 매도완료 로그는 Performance.record_trade_complete()가 SSoT.
            # 7_bot에서는 중복 "매도 완료: 수익률" 로그를 남기지 않는다.
            
            # 연속손실 3회 이상이면 남은 포지션 강제 청산
            if loss_count >= 3 and signal.sell_ratio < 1.0:
                logger.critical(f"💀 {position.symbol} 연속손실 {loss_count}회 - 남은 포지션 강제 청산")
                # ... 강제 청산 로직 ...
        
        # ========================================
        # 5. 포지션 정리 (Lock 안에서!)
        # ========================================
        if _full_exit:
            # 완전 청산
            # ... 추적 종료 로그 ...            
            # 🔒 PRE 탐색 실패 시 동일 파동 쿨다운
            meta = position.metadata or {}
            if meta.get('entry_stage') == 'PRE' and not meta.get('addon_done', False):
                self._cooldown[position.symbol] = datetime.now()
                logger.info(f"⏳ [Cooldown] {position.symbol} PRE 실패 → 동일 파동 재진입 차단")
            
            async with self._positions_lock:
                self.positions.pop(position.symbol, None)
            
            # 🆕 Strategy에 포지션 종료 이벤트 통지 (recovery backfill 6봉)
            try:
                if hasattr(self, "signal_analyzer") and self.signal_analyzer:
                    self.signal_analyzer.register_recovery_backfill(position.symbol, 6)
            except Exception as e:
                logger.warning(f"[RECOVERY-REGISTER-FAIL] {position.symbol}: {e}")
            
            self.consecutive_losses.pop(position.symbol, None)
            self.recovery_trades.pop(position.symbol, None)
            
            # ... 부분 매도 요약 로그 ...
            logger.info(f"✅ {position.symbol} 포지션 완전 청산")
        
        else:
            # 부분 청산
            new_volume = (float(_real_remaining_q8(position.volume, response.executed_volume)) if self.mode == 'real'
                          else position.volume - response.executed_volume)  # [보완 v3_4 G6]
            min_volume = round_money(MIN_ORDER_AMOUNT / response.executed_price, 8)
            
            # 🔥 중복매수 직접 원인 제거:
            # - 부분매도(is_partial_sell) 중에는 잔여가 최소주문 미만이어도 포지션 유지
            # - 저가코인에서 부분매도 후 포지션 삭제 → 중복매수 발생 방지
            if self.mode != 'real' and new_volume == 0:
                logger.info(
                    f"✅ {position.symbol} 잔여 수량 0 - 포지션 정리"
                )
                
                # 🔒 PRE 탐색 실패 시 동일 파동 쿨다운
                meta = position.metadata or {}
                if meta.get('entry_stage') == 'PRE' and not meta.get('addon_done', False):
                    self._cooldown[position.symbol] = datetime.now()
                    logger.info(f"⏳ [Cooldown] {position.symbol} PRE 실패 → 동일 파동 재진입 차단")
                
                async with self._positions_lock:
                    self.positions.pop(position.symbol, None)
                
                # 🆕 Strategy에 포지션 종료 이벤트 통지 (recovery backfill 6봉)
                try:
                    if hasattr(self, "signal_analyzer") and self.signal_analyzer:
                        self.signal_analyzer.register_recovery_backfill(position.symbol, 6)
                except Exception as e:
                    logger.warning(f"[RECOVERY-REGISTER-FAIL] {position.symbol}: {e}")
                
                self.consecutive_losses.pop(position.symbol, None)
                self.recovery_trades.pop(position.symbol, None)
            
            else:
                async with self._positions_lock:
                    position.volume = new_volume
                
                # 🆕 포지션 volume이 0이면 명시적으로 삭제 (유령 포지션 방지!)
                if (_real_remaining_q8(position.volume, 0.0) <= 0) if self.mode == 'real' else (round(position.volume, 8) <= 0):  # [SIMDUST] SIM 소수점 8자리에서 0인 먼지만 정리
                    logger.info(f"✅ {position.symbol} 포지션 완전 청산 (volume=0) - 삭제")
                    
                    # 🔒 PRE 탐색 실패 시 동일 파동 쿨다운
                    meta = position.metadata or {}
                    if meta.get('entry_stage') == 'PRE' and not meta.get('addon_done', False):
                        self._cooldown[position.symbol] = datetime.now()
                        logger.info(f"⏳ [Cooldown] {position.symbol} PRE 실패 → 동일 파동 재진입 차단")
                    
                    async with self._positions_lock:
                        self.positions.pop(position.symbol, None)
                    
                    # 🆕 Strategy에 포지션 종료 이벤트 통지 (recovery backfill 6봉)
                    try:
                        if hasattr(self, "signal_analyzer") and self.signal_analyzer:
                            self.signal_analyzer.register_recovery_backfill(position.symbol, 6)
                    except Exception as e:
                        logger.warning(f"[RECOVERY-REGISTER-FAIL] {position.symbol}: {e}")
                    
                    self.consecutive_losses.pop(position.symbol, None)
                    self.recovery_trades.pop(position.symbol, None)
                    return
                
                # 🆕 부분 청산 로그 개선
                if is_partial_sell:
                    logger.info(
                        f"📊 {position.symbol} {stage}차 부분 청산 완료 | "
                        f"남은 수량: {new_volume:.8f}개 ({(1-signal.sell_ratio)*100:.0f}%) | "
                        f"진행: {position.partial_sell_count}/3단계 | "
                        f"다음 목표: {stage+1}차"
                    )
                else:
                    logger.debug(
                        f"📊 {position.symbol} 부분 청산: "
                        f"남은 수량 {new_volume:.8f}개 (비율 {(1-signal.sell_ratio)*100:.0f}%)"
                    )
    
    async def _record_trade(self, position, signal, request, response):
        """거래 기록 - 매수금액, 매도금액 포함"""
        from Trading_bot_1_core import round_money
        
        pnl = 0
        buy_amount = 0   # 매수금액
        sell_amount = 0  # 매도금액
        
        if request.side == 'sell' and position:
            buy_amount = position.avg_price * response.executed_volume
            sell_amount = response.executed_price * response.executed_volume
            
            pnl = sell_amount - buy_amount - response.fee
            pnl = round_money(pnl)
        
        self.daily_trades.append({
            'time': datetime.now(),
            'symbol': request.symbol,
            'side': request.side,
            'buy_amount': buy_amount,
            'sell_amount': sell_amount,
            'pnl': pnl,
            'reason': signal.reason if signal else ''
        })
        
        if request.side == 'sell':
            self.daily_net_pnl = round_money(self.daily_net_pnl + pnl)
    
    async def emergency_liquidation(self, event_id: str = None):
        """긴급 청산 - 원자적 처리"""
        logger.warning("긴급 청산 시작")
        
        # 스냅샷 생성 (Lock 안에서)
        async with self._positions_lock:
            positions_to_close = list(self.positions.items())
        
        # Lock 밖에서 청산 처리
        for symbol, position in positions_to_close:
            for attempt in range(3):
                try:
                    current_price = await self.data_service.get_current_price(symbol)
                    if current_price is None or current_price <= 0:
                        logger.warning(
                            f"⚠️ [EMERGENCY-PRICE-FRESH-FAIL] {symbol} | "
                            f"get_current_price 실패 → avg_price fallback | "
                            f"avg={position.avg_price:.2f}"
                        )
                        current_price = position.avg_price

                    emergency_signal = TradingSignal(
                        symbol=symbol,
                        action="SELL",
                        price=current_price,
                        reason=f"긴급청산 ({event_id or 'SYSTEM'})",
                        confidence=1.0,
                        sell_ratio=1.0,
                        market_regime=(
                            getattr(position, "entry_regime", None)
                            or MarketRegime.RANGING
                        ),
                    )

                    if await self.execute_trade(position, emergency_signal):
                        break
                    if symbol not in self.positions:
                        break
                    if attempt < 2:
                        await asyncio.sleep(1)

                except Exception as e:
                    logger.error(f"{symbol} 청산 실패: {e}")
                    if attempt < 2:
                        await asyncio.sleep(1)

        # 성공한 종목은 execute_trade() -> _update_position()에서 개별 pop / cooldown 처리 완료.
        # 실패한 종목은 거래소 잔존 가능성이 있으므로 메모리 포지션 보존.
        # 다음 사이클에서 재시도 또는 수동 점검 대상으로 남긴다.
        logger.info("긴급 청산 완료")
    
    def _should_check_ma15(self) -> bool:
        """15분 MA 체크 여부 - 설정 기반"""
        if not self.settings.enable_15min_ma_exit:
            return False
        
        # 현재 타임프레임이 적용 대상인지 확인
        current_tf = self.timeframe.value  # '1m', '5m', etc.
        
        # settings의 ma_exit_applicable_timeframes 체크
        if hasattr(self.settings, 'ma_exit_applicable_timeframes'):
            return current_tf in self.settings.ma_exit_applicable_timeframes
        
        # 없으면 기존 방식 (호환성, 실제로는 발생 안 함)
        return self.timeframe.minutes == 1  # ← 1분봉만!

    def _timeframe_to_seconds(self, tf_value: str) -> int:
        """타임프레임 문자열을 초로 변환 (타임프레임 무관)"""
        try:
            tf = str(tf_value).strip().lower()
            if tf.endswith("m"):
                return int(tf[:-1]) * 60
            if tf.endswith("h"):
                return int(tf[:-1]) * 3600
            if tf.endswith("d"):
                return int(tf[:-1]) * 86400
        except Exception:
            pass
        # 기본 3m (fallback)
        return 180

    def _should_force_refresh_df(self, symbol: str, tf_value: str) -> bool:
        """
        WS 우선, 데이터 없으면 REST
        """
        # 1. WS 상태 확인
        ws_ok = False
        try:
            if self.ws_manager:
                ws_ok = self.ws_manager.is_healthy()
        except:
            ws_ok = False

        # 2. DF 존재 확인
        try:
            df_cache = self.data_service.market_data_service._dataframe_cache
            df = df_cache.get(symbol)
        except:
            return True

        # 3. WS 정상인데 DF가 아직 없으면 → 잠시 유예
        if ws_ok and (df is None or df.empty):
            return False  # 🔥 REST 쏘지 말고 WS 기다림

        if df is None or df.empty:
            return True  # WS 비정상 + 데이터 없음 → REST

        # 4. 최신성 확인
        try:
            last_ts = df.index[-1]
            now = pd.Timestamp.now(tz=last_ts.tz) if last_ts.tz else pd.Timestamp.now()

            if (now - last_ts).total_seconds() > 180:
                return True
        except:
            return True

        # 5. WS 정상 + 데이터 최신 → REST 금지
        if ws_ok:
            return False

        return True

    def _on_candle_close(self, market: str, timeframe: str):
        """
        WebSocket 봉 닫힘 이벤트 핸들러 (완전 복원 2단계)
        - 종목별 봉 닫힘 관리 (정밀 제어)
        - 해당 종목만 REST 동기화 트리거
        - sync 함수로 안정성 보장
        """
        self._new_candle_symbols.add(market)
        
        # 🔥 메모리 누수 방지 (상한 방어)
        if len(self._new_candle_symbols) > 200:
            logger.warning(
                f"⚠️ [CandleQueue] 큐 과다 누적({len(self._new_candle_symbols)}개) → 초기화"
            )
            self._new_candle_symbols.clear()
        
    def _apply_intrabar_price_to_df(self, df, current_price: float) -> None:
        """
        30초 폴링마다 마지막 봉(미확정 포함)을 현재가로 동기화한다.
        목적:
        - MA(rolling close) 즉시 반영 → 가자미근 진입 타이밍 지연 제거
        - intrabar(봉 내부) 고가/저가도 최소한 반영
        주의:
        - volume은 왜곡 위험이 커서 동기화하지 않는다.
        """
        try:
            if df is None or df.empty:
                return
            if current_price is None:
                return
            cp = float(current_price)
            if cp <= 0:
                return

            idx = df.index[-1]

            # 확정봉은 REST 권위. ticker는 현재 형성봉에만 적용한다.
            try:
                _tf_sec = self._timeframe_to_seconds(self.timeframe.value)
                _bar_ts = int((idx.tz_localize("Asia/Seoul") if getattr(idx, "tzinfo", None) is None else idx).timestamp()) if hasattr(idx, "timestamp") else None
                if _bar_ts is None:
                    return
                if (int(time.time()) - _bar_ts) >= _tf_sec:
                    return
            except Exception:
                return

            # close 동기화 (현재 형성봉만)
            df.at[idx, "close"] = cp

            # high/low 동기화 (봉 내부 돌파/꼬리 감지 정확도 개선)
            if "high" in df.columns:
                try:
                    df.at[idx, "high"] = max(float(df.at[idx, "high"]), cp)
                except Exception:
                    df.at[idx, "high"] = cp

            if "low" in df.columns:
                try:
                    df.at[idx, "low"] = min(float(df.at[idx, "low"]), cp)
                except Exception:
                    df.at[idx, "low"] = cp

        except Exception:
            # 동기화 실패는 거래 로직을 멈추지 않는다 (관측/보정 레벨)
            return
    
    async def _get_ma_data(self, market: str):
        """MA 데이터 조회 - 간소화 버전
        Args:
            market: 종목 코드
        Returns:
            dict: MA 데이터 또는 None
        """
        cache_key = f"{market}_{self.timeframe.value}"
        
        # 캐시 확인 (폴링 주기와 정합: 1m/3m은 25초, 그 외 60초)
        cache_ttl = 25 if self.timeframe.value in ("1m", "3m") else 60
        
        if hasattr(self, '_ma_cache') and cache_key in self._ma_cache:
            cached_time, data = self._ma_cache[cache_key]
            if (datetime.now() - cached_time).total_seconds() < cache_ttl:
                return data
        
        try:
            df = await self.data_service.get_dataframe(
                market, 
                self.timeframe.value,
                150
            )
            
            if df is not None and not df.empty:
                result = {
                    'ma5': float(df['close'].rolling(5).mean().iloc[-1]) if len(df) >= 5 else 0,
                    'ma10': float(df['close'].rolling(10).mean().iloc[-1]) if len(df) >= 10 else 0,
                    'ma15': float(df['close'].rolling(15).mean().iloc[-1]) if len(df) >= 15 else 0,
                    'ma20': float(df['close'].rolling(20).mean().iloc[-1]) if len(df) >= 20 else 0,
                    'ma60': float(df['close'].rolling(60).mean().iloc[-1]) if len(df) >= 60 else 0,
                    'ma120': float(df['close'].rolling(120).mean().iloc[-1]) if len(df) >= 120 else 0,
                    'timeframe': self.timeframe.value
                }
                
                # 캐시 초기화
                if not hasattr(self, '_ma_cache'):
                    self._ma_cache = {}
                
                # 크기 제한 (50개 초과 시 가장 오래된 것 제거)
                if len(self._ma_cache) >= 50:
                    oldest_key = min(self._ma_cache.keys(), 
                                key=lambda k: self._ma_cache[k][0])
                    del self._ma_cache[oldest_key]
                
                # 캐시 저장
                self._ma_cache[cache_key] = (datetime.now(), result)
                
                return result
                
        except Exception as e:
            logger.debug(f"[MA계산] {market} 조회 실패: {e}")
            pass
        
        return None
    
    def _check_ma_breach(self, position, symbol, current_price, ma_data):
        """MA 이탈 체크 - 타임프레임별 동적        
        원칙:
        1. 진입가가 MA 위에 있어야만 의미있음 (핵심!)
        2. 타임프레임별로 적절한 MA 자동 선택
        3. 현재가가 MA 아래로 이탈하면 매도        
        타임프레임별 MA 선택:
        - 1m, 3m, 5m: 15MA (단기 그룹, 글로벌 표준)
        - 10m, 15m, 30m: 20MA (중기)
        - 1h+: 60-120MA (장기)
        """
        if not ma_data:
            return None
        
        # 현재 타임프레임
        timeframe = self.timeframe.value
        
        # ========================================
        # 단기 (1-5분): 15MA 사용
        # ========================================
        if timeframe in ['1m', '3m', '5m']:
            # 15MA 직접 사용 또는 10-20 가중평균
            if ma_data.get('ma15', 0) > 0:
                reference_ma = ma_data['ma15']
                ma_name = '15MA'
            elif ma_data.get('ma10', 0) > 0 and ma_data.get('ma20', 0) > 0:
                # 15MA 없으면 가중평균으로 근사
                reference_ma = ma_data['ma10'] * 0.4 + ma_data['ma20'] * 0.6
                ma_name = '15MA(근사)'
            else:
                return None
            
            # 시간 환산
            ma_minutes = {
                '1m': 15,
                '3m': 45,
                '5m': 75
            }
            ma_period_text = f"{ma_minutes[timeframe]}분"
        
        # ========================================
        # 중기 (10-30분): 20MA
        # ========================================
        elif timeframe in ['10m', '15m', '30m']:
            if ma_data.get('ma20', 0) > 0:
                reference_ma = ma_data['ma20']
                ma_name = '20MA'
                ma_minutes = int(timeframe[:-1]) * 20
                ma_period_text = f"{ma_minutes}분"
            else:
                return None
        
        # ========================================
        # 장기 (1시간+): 60MA
        # ========================================
        else:  # 1h, 4h, 1d
            if ma_data.get('ma60', 0) > 0:
                reference_ma = ma_data['ma60']
                ma_name = '60MA'
                ma_period_text = "장기추세"
            elif ma_data.get('ma120', 0) > 0:
                reference_ma = ma_data['ma120']
                ma_name = '120MA'
                ma_period_text = "장기추세"
            else:
                return None
        
        # ========================================
        # 핵심: 진입가가 MA 위에 있어야만 의미있음!
        # "절대 손해 안 본다"의 전제 조건
        # ========================================
        entry_above_ma = position.avg_price > reference_ma * 1.0005  # 0.05% 버퍼
        
        if not entry_above_ma:
            # 진입부터 MA 아래였으면 이탈 매도 의미없음
            logger.debug(
                f"[MA이탈] {symbol} 진입가 MA 아래 → 체크 스킵 "
                f"(진입:{position.avg_price:.0f} vs MA:{reference_ma:.0f})"
            )
            return None
        
        # 현재가와 MA 이격도 계산
        divergence = ((current_price - reference_ma) / reference_ma * 100) if reference_ma > 0 else 0
        
        # MA 아래로 이탈했는가?
        if current_price < reference_ma:
            breach_ratio = (reference_ma - current_price) / reference_ma
            
            # 임계값 이상 이탈 (기본 0.3%)
            if breach_ratio > self.settings.ma_breach_threshold:
                logger.info(
                    f"⚠️ [MA이탈] {symbol} {timeframe} {ma_name} 이탈 감지! "
                    f"현재가:{current_price:.0f} < MA:{reference_ma:.0f} "
                    f"(이격도:{divergence:.1f}%)"
                )
                
                return TradingSignal(
                    symbol=symbol,
                    action="SELL",
                    price=current_price,
                    reason=f"{timeframe} {ma_name}이탈 ({ma_period_text}, 이격도:{divergence:.1f}%, 진입가 MA위)",
                    confidence=0.95,
                    sell_ratio=1.0,
                    market_regime=MarketRegime.RANGING,
                    primary_signal="ma_exit"
                )
        
        return None
    
    def _should_preserve_cash(self, metrics):
        """캐시 보존 판단 - WARNING 레벨 기반
        
        WARNING 레벨 발동 조건:
        1. 연속손실 3회 이상 (max_consecutive_losses의 60%)
        2. 일일손실 80% 이상
        
        Returns:
            bool: True이면 캐시 보존 모드 (고신뢰도만 허용)
        """
        from Trading_bot_9_risk import RiskLevel
        
        # 타입 안전한 비교
        if not hasattr(self.risk_manager, 'risk_level'):
            return False
        
        # WARNING 레벨이면 캐시 보존 모드
        is_warning = (
            self.risk_manager.risk_level == RiskLevel.WARNING or
            str(self.risk_manager.current_level).lower() == 'warning'
        )
        
        if is_warning:
            consecutive = metrics.get('consecutive_losses', 0)
            daily_loss = metrics.get('daily_loss', 0)
            
            logger.info(
                f"💰 [캐시보존모드] 발동! "
                f"연속손실: {consecutive}회, "
                f"일일손실: {daily_loss:+.1f}원"
            )
        
        return is_warning
    
    # async def _get_risk_metrics(self):
    async def _get_risk_metrics(self, balance=None):
        """리스크 메트릭"""
        if not self.performance:
            return {
                'consecutive_losses': 0,
                'daily_loss_amount': 0,
                'daily_profit_sum': 0,
                'daily_net_pnl': 0,
                'max_drawdown': 0,
                'win_rate': 0,
                'market_regime': 'RANGING'  # 🆕 기본값 추가
            }
        
        # Performance 계산값을 그대로 사용
        metrics = self.performance.get_risk_metrics()
        
        # 🆕 현재 시장 체제 추가
        if hasattr(self, 'analyzer') and self.analyzer:
            try:
                # analyzer.market_regime이 MarketRegime Enum이면 .value 사용
                if hasattr(self.analyzer.market_regime, 'value'):
                    metrics['market_regime'] = self.analyzer.market_regime.value
                else:
                    metrics['market_regime'] = str(self.analyzer.market_regime)
                
                logger.debug(
                    f"[리스크메트릭] 현재 체제: {metrics['market_regime']}"
                )
            except Exception as e:
                logger.debug(f"체제 조회 실패: {e}")
                metrics['market_regime'] = 'RANGING'
        else:
            metrics['market_regime'] = 'RANGING'
        
        # 🆕 실거래 모드: 실시간 API 잔고로 교체
        # if self.mode == 'real' and hasattr(self, 'engine'):
        #     try:
        #         real_balance = await self.engine.get_balance()
        #         metrics['current_balance'] = real_balance
        #         logger.debug(f"[실거래잔고] API 조회: {real_balance:,.0f}원")
        if self.mode == 'real' and hasattr(self, 'engine'):
            try:
                real_balance = balance if balance is not None else await self.engine.get_balance()
                metrics['current_balance'] = real_balance
                logger.debug(f"[실거래잔고] {'캐시' if balance is not None else 'API조회'}: {real_balance:,.0f}원")        
            except Exception as e:
                logger.warning(f"실거래 잔고 조회 실패, 계산값 사용: {e}")
        
        return metrics
                  
    async def _daily_reset(self):
        """일일 리셋"""
        if self.daily_trades:
            await self._save_daily_summary()
        
        self.daily_net_pnl = 0
        self.daily_trades = []
        self.daily_reset_time = datetime.now().replace(hour=0, minute=0, second=0)
        self.ma15_cache.clear()
        
        logger.info("일일 리셋 완료")
    
    async def _save_daily_summary(self):
        """일일 요약 저장 - 종목별 + 매도 사유별 통계"""
        try:
            filename = self.report_dir / f"daily_summary_{datetime.now().strftime('%Y-%m-%d')}.txt"
            
            balance = await self.engine.get_balance() if self.engine else self.current_balance
            total_value = balance + sum(p.volume * p.avg_price for p in self.positions.values())
            returns = ((total_value - self.initial_balance) / self.initial_balance * 100) if self.initial_balance > 0 else 0
            
            content = [
                f"{'='*60}\n",
                f"업비트 자동매매 봇 v1.0 - 일일 거래 요약\n",
                f"날짜: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n",
                f"{'='*60}\n\n",
                f"모드: {self.mode} | 타임프레임: {self.timeframe.value}\n",
                f"현재 잔고: {balance:,.0f}원\n",
                f"총 자산: {total_value:,.0f}원 ({returns:+.1f}%)\n\n"
            ]
            
            # 전체 통계
            if self.daily_trades:
                sell_trades = [t for t in self.daily_trades if t.get('side') == 'sell']
                buy_trades = [t for t in self.daily_trades if t.get('side') == 'buy']
                wins = sum(1 for t in sell_trades if t.get('pnl', 0) > 0)
                losses = sum(1 for t in sell_trades if t.get('pnl', 0) <= 0)
                win_rate = (wins / (wins + losses) * 100) if (wins + losses) > 0 else 0
                
                content.append(f"{'='*60}\n")
                content.append(f"📊 전체 통계\n")
                content.append(f"{'='*60}\n")
                content.append(f"거래: {len(self.daily_trades)}건 (매수 {len(buy_trades)}, 매도 {len(sell_trades)})\n")
                content.append(f"승률: {win_rate:.1f}% ({wins}승 {losses}패)\n")
                content.append(f"순손익: {self.daily_net_pnl:+,.0f}원\n\n")
                
                # ========================================
                # 매도 사유별 통계
                # ========================================
                from collections import defaultdict
                
                sell_reasons = defaultdict(lambda: {
                    'count': 0,
                    'wins': 0,
                    'losses': 0,
                    'total_buy_amount': 0,
                    'total_sell_amount': 0,
                    'total_pnl': 0
                })
                
                for trade in self.daily_trades:
                    if trade.get('side') != 'sell':
                        continue
                    
                    reason = trade.get('reason', '알 수 없음')
                    reason_key = self._extract_reason_keyword(reason, is_buy=False)
                    
                    pnl = trade.get('pnl', 0)
                    buy_amount = trade.get('buy_amount', 0)
                    sell_amount = trade.get('sell_amount', 0)
                    
                    sell_reasons[reason_key]['count'] += 1
                    sell_reasons[reason_key]['total_buy_amount'] += buy_amount
                    sell_reasons[reason_key]['total_sell_amount'] += sell_amount
                    sell_reasons[reason_key]['total_pnl'] += pnl
                    
                    if pnl > 0:
                        sell_reasons[reason_key]['wins'] += 1
                    else:
                        sell_reasons[reason_key]['losses'] += 1
                
                # 매도 사유별 출력
                if sell_reasons:
                    sorted_sell_reasons = sorted(
                        sell_reasons.items(),
                        key=lambda x: x[1]['count'],
                        reverse=True
                    )
                    
                    content.append(f"{'='*60}\n")
                    content.append(f"🎯 매도 사유별 통계\n")
                    content.append(f"{'='*60}\n")
                    content.append(f"{'사유':<20} {'횟수':<6} {'승률':<8} {'평균손익':<15} {'총손익':<15} {'수익률':<10}\n")
                    content.append(f"{'-'*80}\n")
                    
                    for reason, stats in sorted_sell_reasons:
                        count = stats['count']
                        wins = stats['wins']
                        total_pnl = stats['total_pnl']
                        total_buy = stats['total_buy_amount']
                        
                        # 집계 후 계산
                        win_rate = (wins / count * 100) if count > 0 else 0
                        avg_pnl = total_pnl / count if count > 0 else 0
                        return_pct = (total_pnl / total_buy * 100) if total_buy > 0 else 0
                        
                        content.append(
                            f"{reason:<20} "
                            f"{count:<6} "
                            f"{win_rate:>6.1f}% "
                            f"{avg_pnl:>+13,.0f}원 "
                            f"{total_pnl:>+13,.0f}원 "
                            f"{return_pct:>+8.2f}%\n"
                        )
                    
                    content.append(f"\n")
                
                # ========================================
                # 종목별 통계
                # ========================================
                symbol_stats = defaultdict(lambda: {
                    'trades': 0,
                    'wins': 0,
                    'losses': 0,
                    'total_buy_amount': 0,
                    'total_sell_amount': 0,
                    'total_pnl': 0
                })
                
                for trade in sell_trades:
                    symbol = trade.get('symbol', 'UNKNOWN')
                    pnl = trade.get('pnl', 0)
                    buy_amount = trade.get('buy_amount', 0)
                    sell_amount = trade.get('sell_amount', 0)
                    
                    symbol_stats[symbol]['trades'] += 1
                    symbol_stats[symbol]['total_buy_amount'] += buy_amount
                    symbol_stats[symbol]['total_sell_amount'] += sell_amount
                    symbol_stats[symbol]['total_pnl'] += pnl
                    
                    if pnl > 0:
                        symbol_stats[symbol]['wins'] += 1
                    else:
                        symbol_stats[symbol]['losses'] += 1
                
                sorted_symbols = sorted(
                    symbol_stats.items(), 
                    key=lambda x: x[1]['trades'], 
                    reverse=True
                )
                
                if sorted_symbols:
                    content.append(f"{'='*60}\n")
                    content.append(f"📈 종목별 통계 (총 {len(sorted_symbols)}개 종목)\n")
                    content.append(f"{'='*60}\n")
                    content.append(f"{'종목':<12} {'거래':<6} {'승률':<8} {'총손익':<15} {'수익률':<10}\n")
                    content.append(f"{'-'*60}\n")
                    
                    for symbol, stats in sorted_symbols:
                        trades = stats['trades']
                        wins = stats['wins']
                        total_pnl = stats['total_pnl']
                        total_buy = stats['total_buy_amount']
                        
                        # 집계 후 계산
                        win_rate = (wins / trades * 100) if trades > 0 else 0
                        return_pct = (total_pnl / total_buy * 100) if total_buy > 0 else 0
                        
                        content.append(
                            f"{symbol:<12} "
                            f"{trades:<6} "
                            f"{win_rate:>6.1f}% "
                            f"{total_pnl:>+13,.0f}원 "
                            f"{return_pct:>+8.2f}%\n"
                        )
                    
                    content.append(f"\n")
            
            # 현재 포지션
            if self.positions:
                content.append(f"{'='*60}\n")
                content.append(f"💼 보유 포지션 ({len(self.positions)}개)\n")
                content.append(f"{'='*60}\n")
                
                for symbol, pos in self.positions.items():
                    content.append(
                        f"{symbol:<12} "
                        f"{pos.volume:.4f}개 @ {format_price_krw(pos.avg_price)}원 | "
                        f"사유: {pos.entry_reason[:30] if pos.entry_reason else 'N/A'}...\n"
                    )
                content.append(f"\n")
            
            content.append(f"{'='*60}\n")
            
            with open(filename, 'w', encoding='utf-8') as f:
                f.writelines(content)
                
            logger.info(f"✅ 일일 요약 저장: {filename}")
                
        except Exception as e:
            logger.error(f"요약 저장 오류: {e}")

    def _extract_reason_keyword(self, reason: str, is_buy: bool = True) -> str:
        """사유에서 핵심 키워드 추출
        Args:
            reason: 거래 사유 전체 문자열
            is_buy: True면 매수, False면 매도 
        Returns:
            str: 분류된 사유 키워드
        """
        if not reason:
            return "알 수 없음"
        
        # 🛡️ 타입 안전: reason이 dict일 수 있음
        reason_lower = str(reason).lower() if reason else ""
        
        if is_buy:
            # 매수 사유 분류
            if "골든크로스" in reason or "golden" in reason_lower or "gc_" in reason_lower:
                return "골든크로스"
            elif "정배열" in reason or "alignment" in reason_lower:
                return "정배열"
            elif "돌파" in reason or "breakout" in reason_lower:
                return "돌파"
            elif "atr" in reason_lower or "변동성" in reason:
                return "ATR확대"
            elif "거래량" in reason or "volume" in reason_lower:
                return "거래량급증"
            elif "추세" in reason or "trend" in reason_lower:
                return "추세강화"
            else:
                return "기타매수"
        else:
            # 매도 사유 분류
            if "익절" in reason or "take profit" in reason_lower or "+%" in reason:
                return "익절"
            elif "추적" in reason or "trailing" in reason_lower:
                return "추적손절"
            elif "ma" in reason_lower or "이평" in reason or "이탈" in reason:
                return "MA이탈"
            elif "손절" in reason or "stop loss" in reason_lower or "-%" in reason:
                return "손절"
            elif "타임아웃" in reason or "timeout" in reason_lower:
                return "타임아웃"
            elif "데드크로스" in reason or "dead" in reason_lower or "dc_" in reason_lower:
                return "데드크로스"
            elif "역배열" in reason:
                return "역배열"
            elif "긴급" in reason or "emergency" in reason_lower:
                return "긴급청산"
            else:
                return "기타매도"
    
    async def _recover_late_fills(self):
        """[보완 v3_4 ①] 늦게 확인된 체결 복구 — REAL_LATE_FILL_RECOVERY_DESIGN_v2
        근거 = 거래소 조회 snapshot − applied 만 · 포지션 반영 1 회 (원장 write-ahead + 포지션 마커) · 성과 기록 최대 1 회 (중단 이력 = 재기록 안 함 · 미종결 유지)
        자동 = L1 (불명 뒤 체결) · L2 (부분 체결 잔량) · 수수료만 증가 / 금지 = apply_hold (L3 · L5) · 재시작으로 거래소 복구된 포지션 = 이미 반영으로 간주"""
        if self.mode != 'real':
            return
        for rec in self.engine.late_fills():
            ident, sym, side = rec['identifier'], rec['symbol'], rec['side']
            try:
                it = self.engine.begin_recovery(ident)
                dv, df, dc, mk = float(it['dv']), float(it['df']), float(it['dc']), it['marker']
                pos = self.positions.get(sym)
                _ts = None
                if pos is None:
                    try:
                        _ts = self.engine.load_position_state(sym)  # [보완 v3_5] 청산 표지
                    except Exception:
                        _ts = None
                _pm = pos.metadata if (pos is not None and isinstance(pos.metadata, dict)) else (_ts or {})
                _oa = (_pm.get('order_applied') or {}).get(ident)  # [보완 v3_5] 정상 경로 반영 기록
                if _oa is not None:
                    _mv = float(_oa.get('v', 0.0))
                    dv = max(0.0, float(it['target_v']) - _mv); df = float(it['target_f']) - float(_oa.get('f', 0.0)); dc = float(it['target_c']) - float(_oa.get('c', 0.0))
                if _oa is not None and _oa.get('mk') == mk:
                    dv, df, dc = float(_oa['mdv']), float(_oa['mdf']), float(_oa['mdc'])  # [보완 v3_6 ①] 이 복구 건은 이미 반영 · 반영 때 고정한 증분 (영속 뒤 중단 → 재개해도 회계 증분 보존)
                _acc_fail = _oa is not None and _oa.get('acc') is False  # [보완 v3_6 ②] 정상 경로 성과 미기록 표지
                _closed_holder = None  # [보완 v3_6 ③] 늦은 체결 전량 청산 = 청산 표지 근거
                _buy_fee_hold = False  # [보완 v3_11 L1]
                if it['step'] == 0 and side == 'buy' and dv <= 1e-12 and dc > 1e-12 and mk not in _pm.get('late_fill_markers', []):
                    _ovb = float(getattr(pos, 'original_volume', 0.0) or 0.0) if pos is not None else 0.0
                    if pos is not None and abs(float(pos.volume) - _ovb) <= 1e-8 and int(getattr(pos, 'partial_sell_count', 0) or 0) == 0 and not (_pm.get('recovered') and not _pm.get('restored_state')):  # 영속 상태 없는 재시작 복구 = 근거 없음 (v3_4 ④ 와 같은 규칙)
                        async with self._positions_lock:  # 부분 매도 없음 = 늘어난 수수료 전부 남은 물량 몫 → 정확히 1 회 (마커)
                            pos.entry_fee_paid = float(getattr(pos, 'entry_fee_paid', 0.0) or 0.0) + float(dc)
                            if not isinstance(pos.metadata, dict):
                                pos.metadata = {}
                            pos.metadata.setdefault('late_fill_markers', []).append(mk)
                            _oab = dict(_oa or {}); _oab.update(v=float(it['target_v']), f=float(it['target_f']), c=float(it['target_c']), mk=mk, mdv=0.0, mdf=float(df), mdc=float(dc))
                            _oab.setdefault('acc', True); _oab.setdefault('side', side); pos.metadata.setdefault('order_applied', {})[ident] = _oab
                        logger.critical(f"[LATE-FILL-BUY-FEE] {sym} id={ident} 매수 수수료 +{dc:.6f} 반영 (수량 불변 · 부분 매도 없음)")
                    else:
                        _buy_fee_hold = True  # 부분 매도 뒤 · 청산 뒤 = 실현 손익 · 남은 물량 배분 근거 없음 → 부채
                if it['step'] == 0:
                    if mk in _pm.get('late_fill_markers', []) or (_oa is not None and dv <= 1e-12):
                        logger.warning(f"[LATE-FILL] {sym} id={ident} 포지션 마커 있음 = 이미 반영 (재반영 금지)")
                    elif _pm.get('recovered') and not _pm.get('restored_state') and dv > 0:
                        # [보완 v3_4 ④] 영속 상태 없는 재시작 복구 포지션 = 반영 여부 증거 없음 → 자동 복구 금지 (거래소 잔고로 추정하지 않음)
                        self.engine.mark_apply_hold(ident, "재시작 · 영속 포지션 상태 없음")
                        logger.critical(f"[LATE-FILL-HOLD] {sym} id={ident} 영속 포지션 상태 없음 → APPLY-HOLD")
                        continue
                    elif dv > 0:
                        acc = await self.api_client.get_balance(strict=True)
                        _cur = sym.replace('KRW-', '')
                        exch = sum(float(a.get('balance', 0) or 0) + float(a.get('locked', 0) or 0) for a in acc if a.get('currency') == _cur)
                        held = float(pos.volume) if pos is not None else 0.0
                        if side == 'buy' and held + dv > exch + 1e-8:
                            logger.critical(f"[LATE-FILL-HOLD] {sym} id={ident} 복구 뒤 봇 {held + dv:.8f} > 거래소 {exch:.8f} (balance + locked) → 중단")
                            continue
                        if side == 'sell' and (pos is None or held + 1e-8 < dv):
                            logger.critical(f"[LATE-FILL-HOLD] {sym} id={ident} 매도 증분 {dv:.8f} > 봇 보유 {held:.8f} → 중단")
                            continue
                        px = df / dv  # 추가분 가격 = ΔF / ΔV (누적 평균 아님)
                        async with self._positions_lock:
                            if side == 'buy' and pos is None:
                                s = self.settings
                                pos = Position(symbol=sym, volume=dv, avg_price=px, entry_time=datetime.now(), entry_reason="LATE_FILL_RECOVERED",
                                               stop_loss=px * (1.0 - s.stop_loss_pct), take_profit=px * (1.0 + s.take_profit_pct), peak_price=px,
                                               trailing_stop_distance=s.trailing_stop_pct, entry_atr=0.0, entry_regime=None, original_volume=dv,
                                               metadata={'late_fill_markers': [], 'source': 'LATE_FILL'})
                                pos.entry_fee_paid = float(max(0.0, dc))  # [보완 v3_10 K3] 늦은 매수 체결 수수료 = 매수 수수료 근거
                                self.positions[sym] = pos
                            elif side == 'buy':
                                _v0b = float(pos.volume); _ov0b = float(getattr(pos, 'original_volume', 0.0) or 0.0) or _v0b
                                _unalloc = float(getattr(pos, 'entry_fee_paid', 0.0) or 0.0) * (min(1.0, _v0b / _ov0b) if _ov0b > 0 else 1.0)  # [보완 v3_11 L2] 남은 물량 몫
                                _nv = float(_q8(pos.volume, "even") + _q8(dv, "even"))  # [보완 v3_6 ⑤]
                                _part = int(getattr(pos, 'partial_sell_count', 0) or 0) > 0 or _v0b < _ov0b - 1e-8  # [보완 v3_12 M1] 부분 매도 있었나
                                if _part and _nv > _ov0b + 1e-8:
                                    self.engine.mark_apply_hold(ident, "부분 매도 뒤 늦은 매수 · 새 보유 > 원수량 (배분 근거 없음)")
                                    logger.critical(f"[LATE-FILL-HOLD] {sym} id={ident} 부분 매도 뒤 새 보유 {_nv:.8f} > 원수량 {_ov0b:.8f} → APPLY-HOLD")
                                    continue
                                pos.avg_price = (float(pos.avg_price) * float(pos.volume) + df) / _nv
                                pos.volume = _nv
                                if _part:
                                    pos.entry_fee_paid = (_unalloc + float(max(0.0, dc))) * _ov0b / _nv  # [보완 v3_12 M1] TP 기준 (원수량) 보존 · 남은 물량 전부 매도 시 배분 합 = 미배분 + 새 수수료
                                else:
                                    pos.original_volume = _nv
                                    pos.entry_fee_paid = _unalloc + float(max(0.0, dc))  # [보완 v3_10 K3 · v3_11 L2] 이미 배분한 몫은 다시 배분하지 않음
                            else:
                                pos.volume = float(_real_remaining_q8(pos.volume, dv))
                            if not isinstance(pos.metadata, dict):
                                pos.metadata = {}
                            pos.metadata.setdefault('late_fill_markers', []).append(mk)
                            _oa_new = dict(_oa or {})
                            if _oa is not None:
                                _oa_new.setdefault('v0', _oa.get('v', 0.0)); _oa_new.setdefault('f0', _oa.get('f', 0.0)); _oa_new.setdefault('c0', _oa.get('c', 0.0))  # 정상 경로 원값 (RECORD_RETRY 근거)
                            _oa_new.update(v=float(it['target_v']), f=float(it['target_f']), c=float(it['target_c']), mk=mk, mdv=dv, mdf=df, mdc=dc)  # [보완 v3_6 ①] 반영 기록 = 목표 · 이 복구 건 증분 고정 (같은 잠금 · 같은 영속)
                            _oa_new.setdefault('acc', True); _oa_new.setdefault('side', side)
                            pos.metadata.setdefault('order_applied', {})[ident] = _oa_new
                        if side == 'sell' and int(rec.get('tp_stage', 0) or 0) > 0:
                            _st = str(int(rec['tp_stage'])); _a = pos.metadata.setdefault('tp_stage_filled', {}).setdefault(_st, {'v': 0.0, 'krw': 0.0})
                            _a['v'] = float(_q8(_a['v'], "even") + _q8(dv, "even")); _a['krw'] = float(_a['krw']) + df  # [보완 v3_5] 재양자화 누적
                            _tgt_q = _tp_target_q8(getattr(pos, 'original_volume', 0.0) or 0.0, rec.get('tp_ratio', 0) or 0)  # [보완 v3_6 ⑤] 정상 경로와 같은 함수
                            if _tgt_q > 0 and _q8(_a['v'], "even") >= _tgt_q:
                                pos.record_partial_sell(stage=int(_st), price=_a['krw'] / _a['v'], volume=_a['v'], ratio=float(rec.get('tp_ratio', 0) or 0))
                                pos.metadata['tp_stage_filled'].pop(_st, None)
                        if side == 'sell' and _real_remaining_q8(pos.volume, 0.0) <= 0:
                            async with self._positions_lock:
                                self.positions.pop(sym, None)
                            _closed_holder = pos  # [보완 v3_6 ③]
                    if pos is None and _ts is not None:
                        pass  # [보완 v3_6 ③] 청산 표지만 있는 종목 · 바뀐 것 없음 = 표지 보존 (fallback 없는 저장은 표지를 지움)
                    else:
                        self._persist_position(sym, fallback=_closed_holder)  # [보완 v3_4 ④] 반영 → 영속 → step 1 · [보완 v3_6 ③] 전량 청산이면 청산 표지
                    self.engine.recovery_step(ident, 1)
                    it['step'] = 1
                acc_ok = True; _debt = None; _debts = []; _cdiff = False  # [보완 v3_6] 부채 여러 건 · [보완 v3_9] 내용 불일치 표지 (주문마다 초기화)
                if it['step'] == 1:
                    self.engine.recovery_step(ident, 2)
                    it['step'] = 2
                _rs = "LATE_FILL_RECOVERY"  # [보완 v3_7] reason 에 id 표식 없음 (사건 식별 = event_id · 리포트 LIKE 분류 오염 방지)
                if side == 'sell':
                    _src = pos if pos is not None else type("ClosedPos", (), dict(symbol=sym, avg_price=float(_pm.get('avg_price', 0.0) or 0.0), volume=0.0,
                                                                                original_volume=float(_pm.get('original_volume', 0.0) or 0.0), entry_fee_paid=float(_pm.get('entry_fee_paid', 0.0) or 0.0),
                                                                                entry_time=datetime.now(), metadata={'efp_unknown': True} if 'entry_fee_paid' not in _pm else {}, entry_regime=None))()  # [보완 v3_10 K3]
                    if not rec.get('ev'):  # [보완 v3_8] 사건 식별 이전 예약 주문 = 성과 DB 에 NULL 사건 id 로 기록됐을 수 있음 → 자동 기록 금지
                        _ok = None
                        acc_ok = False
                        _debt = dict(kind="record_manual", symbol=sym, side=side, ident=ident, reason="LEGACY_ORDER_NO_EVENT_ID")
                        logger.critical(f"[LATE-FILL-ACCOUNTING-HOLD] {sym} id={ident} 사건 식별 이전 주문 = 성과 자동 기록 안 함 · 미종결 유지")
                    else:
                        _ok = await self._record_sell_shortfall(ident, sym, it['target_v'], it['target_f'], it['target_c'], _src, _rs)  # [보완 v3_7] 회계 진실 = 성과 DB
                    if _ok == "efp_unknown":  # [보완 v3_10 K3]
                        _ok = "efp_hold"; acc_ok = False  # 아래 sell_record 조건이 이 부채를 덮지 않게 (CLI 첫 구현 결함 · F2 가 잡음)
                        _debt = dict(kind="record_manual", symbol=sym, side=side, ident=ident, reason="EFP_UNKNOWN")
                    _cdiff = (_ok == "diff")  # [보완 v3_9]
                    if _cdiff:
                        _ok = True  # 아래 금액 · 수수료 보정 부채 판정으로 넘김 · 없으면 J2 끝에서 내용 불일치 부채
                    if _ok is False or (_ok not in (True, "efp_hold") and rec.get('ev')):  # [보완 v3_10 K3]
                        acc_ok = False
                        _debt = dict(kind="sell_record", symbol=sym, ident=ident, avg_price=float(getattr(_src, 'avg_price', 0.0) or 0.0), target_v=float(it['target_v']),
                                     target_f=float(it['target_f']), target_c=float(it['target_c']), efp=0.0, reason=_rs,
                                     efp_total=float(getattr(_src, 'entry_fee_paid', 0.0) or 0.0), ov=float(getattr(_src, 'original_volume', 0.0) or 0.0))  # [보완 v3_10 K3]
                        logger.critical(f"[LATE-FILL-ACCOUNTING-FAIL] {sym} id={ident} 성과 기록 부족분 미기록 → 부채 · 재시도")
                    elif dv <= 1e-12 and dc > 1e-12:
                        acc_ok = False
                        _debt = dict(kind="fee_adjust", symbol=sym, fee=dc, reason=_rs)
                        logger.critical(f"[LATE-FILL-ACCOUNTING-HOLD] {sym} id={ident} 매도 수수료만 {dc} 증가 · 성과 보정 API 없음 → 미종결 유지")
                if _debt:
                    _debts.append(_debt)
                if _buy_fee_hold:  # [보완 v3_11 L1]
                    acc_ok = False
                    _debts.append(dict(kind="buy_fee_adjust", symbol=sym, ident=ident, fee=float(dc), reason="BUY_FEE_NO_ALLOCATION_BASIS"))
                    logger.critical(f"[LATE-FILL-ACCOUNTING-HOLD] {sym} id={ident} 매수 수수료만 {dc:.6f} 증가 · 부분 매도 · 청산 뒤 = 배분 근거 없음 → 미종결")
                if side == 'sell' and _cdiff and not (_debt and _debt.get('kind') in ('fee_adjust',)) and not (dv <= 1e-12 and abs(df) > 1e-6):
                    acc_ok = False  # [보완 v3_9] 같은 사건 · 다른 내용 · 금액 / 수수료 보정 부채가 따로 없음
                    _debts.append(dict(kind="event_content_diff", symbol=sym, ident=ident, reason="EVENT_CONTENT_DIFF"))
                if _acc_fail and _oa.get('side') != 'sell':  # [보완 v3_7] 매도 = 위 부족분 기록이 회계 진실로 처리 · 매수 기록 실패만 부채
                    acc_ok = False
                    _v0 = float(_oa.get('v0', _oa.get('v', 0.0))); _f0 = float(_oa.get('f0', _oa.get('f', 0.0))); _c0 = float(_oa.get('c0', _oa.get('c', 0.0)))  # 정상 경로 원값만 (늦은 증분은 자기 기록)
                    if _oa.get('side') == 'sell' and _v0 > 0 and 'ap' in _oa:
                        _debts.append(dict(kind="sell_record", symbol=sym, avg_price=float(_oa['ap']), volume=_v0, price=_f0 / _v0,
                                           fee=_c0, efp=float(_oa.get('efp', 0.0)), reason=f"RECORD_RETRY:{ident}"))  # [보완 v3_6 ②]
                    else:
                        _debts.append(dict(kind="record_manual", symbol=sym, side=_oa.get('side', side), reason=f"RECORD_RETRY:{ident}"))  # [보완 v3_6 ②] 근거 부족 = 자동 해소 불가
                    logger.critical(f"[LATE-FILL-ACCOUNTING-FAIL] {sym} id={ident} 정상 경로 성과 미기록 표지 (acc=False) → 부채")
                if dv <= 1e-12 and abs(df) > 1e-6:
                    acc_ok = False
                    _debts.append(dict(kind="funds_adjust", symbol=sym, side=side, df=df, reason=_rs))  # [보완 v3_6 ④] 금액만 변화 = 평단 · 손익 보정 근거 없음 → 미종결
                    logger.critical(f"[LATE-FILL-ACCOUNTING-HOLD] {sym} id={ident} 체결 금액만 {df:,.6f} 변화 · 수량 불변 → 미종결 유지")
                self.engine.finish_recovery(ident, acc_ok, debt=_debts)
                logger.critical(f"[LATE-FILL-RECOVERED] {sym} {side} id={ident} ΔV={dv:.8f} ΔF={df:,.2f} ΔC={dc:,.4f} 회계={acc_ok}")
            except Exception as e:
                logger.critical(f"[LATE-FILL-HOLD] {sym} id={ident} {type(e).__name__}: {e}")

    async def _record_sell_shortfall(self, ident, sym, target_v, target_f, target_c, src, reason, md=None, sig=None):
        """[보완 v3_7 · v3_8] 회계 진실 = 성과 DB : 이 주문으로 이미 기록된 (수량 · 금액 · 수수료) 를 빼고 모자란 만큼만 기록 · 사건 id = ord:{ident}:{target_v}
        [v3_8] 정상 · 복구 · 재기록이 모두 이 함수 · 조회 → 계산 → INSERT 를 한 잠금 안에서 (같은 프로세스 안 경쟁 제거)
        반환 True = 기록했거나 기록할 것 없음 · False = 미기록 (부채) · 초과 기록 (기록 > 확인 체결) = False + critical (자동 해소 없음)"""
        _own = globals().setdefault("_REAL_ACCT_OWNER", {})  # [보완 v3_12 M2] 프로세스 안 회계 쓰기 소유 루프 = 하나 (v3_11 = 루프마다 잠금)
        _cur = asyncio.get_running_loop()
        with globals().setdefault("_REAL_ACCT_OWNER_GUARD", __import__("threading").Lock()):
            _ol = _own.get("loop")
            if _ol is not None and _ol is not _cur and not _ol.is_closed():
                logger.critical(f"[ACCT-LOOP-REFUSE] {sym} id={ident} 다른 이벤트 루프가 회계 쓰기 소유 중 → 미기록 (부채 · 미종결)")
                return False
            if _ol is not _cur:
                _own["loop"] = _cur; _own["lock"] = asyncio.Lock()
            _lk = _own["lock"]
        async with _lk:
            rv, rf, rc = self.performance.recorded_event_totals(f"ord:{ident}:")
            sv = float(_real_remaining_q8(target_v, rv))
            if sv < 0:
                logger.critical(f"[EVENT-OVER] {sym} id={ident} 기록 수량 {rv:.8f} > 확인 체결 {float(target_v):.8f} → 미종결 유지")
                return False
            if sv == 0:
                _ftol = max(1e-6, abs(float(target_f)) * 1e-12)
                if abs(float(target_f) - rf) > _ftol or abs(float(target_c) - rc) > 1e-9:  # [보완 v3_9] 같은 사건 · 다른 내용 = 덮지 않음
                    logger.critical(f"[EVENT-CONTENT-DIFF] {sym} id={ident} 수량 같음 {rv:.8f} · 금액 기록 {rf:,.6f} ↔ 확인 {float(target_f):,.6f} · 수수료 기록 {rc:.6f} ↔ 확인 {float(target_c):.6f} → HOLD")
                    return "diff"
                return True
            _smd = getattr(src, 'metadata', None)
            if isinstance(_smd, dict) and _smd.get('efp_unknown'):  # [보완 v3_10 K3] 매수 수수료 근거 없음 = 0 으로 넣어 완료 처리 금지
                logger.critical(f"[EVENT-EFP-UNKNOWN] {sym} id={ident} 매수 수수료 근거 없는 상태 → 자동 기록 안 함 · HOLD")
                return "efp_unknown"
            sf = float(target_f) - rf; sc = float(target_c) - rc
            if not (sf == sf and sc == sc and abs(sf) != float('inf') and abs(sc) != float('inf')) or sf <= 0 or sc < -1e-9:  # [보완 v3_10 K4] 0 으로 깎지 않음
                logger.critical(f"[EVENT-CONTENT-DIFF] {sym} id={ident} 증분 비정상 (금액 {sf} · 수수료 {sc}) → HOLD")
                return "diff"
            sc = max(0.0, sc)
            _r = ExecutionResult(success=True, symbol=sym, side="sell", executed_volume=sv, avg_price=sf / sv, total_fee=sc)
            _sig = sig if sig is not None else type("ShortfallSignal", (), {"reason": reason, "confidence": 0.0, "market_regime": None, "sell_ratio": 1.0, "metadata": {}})()
            _md = dict(md or {}); _md["event_id"] = f"ord:{ident}:{float(_q8(target_v, 'even')):.8f}"
            _ok = await self.performance.record_trade_complete(src, _r, _sig, _md)
            return _ok is True

    @staticmethod
    def _entry_fee_share(position, executed_volume):
        """[보완 v3_6] 매도 1 건에 배분되는 매수 수수료 = entry_fee_paid × min(1, 체결 / 원수량) (3_performance record_trade_complete 와 같은 식)"""
        try:
            if position is None:
                return 0.0
            base = float(getattr(position, 'original_volume', 0.0) or position.volume or 0.0)
            return float(getattr(position, 'entry_fee_paid', 0.0) or 0.0) * (min(1.0, max(0.0, float(executed_volume) / base)) if base > 0 else 0.0)
        except Exception:
            return 0.0

    def _perf_has_reason(self, reason):
        """[보완 v3_5] 성과 DB trade_details 에 같은 reason (주문 표식) 행이 이미 있는가 · 읽기 전용 · 정확히 1 회 기록 근거"""
        import sqlite3
        p = getattr(self.performance, 'db_path', None)
        if not p:
            raise RuntimeError("성과 DB 경로 없음")
        from pathlib import Path
        con = sqlite3.connect(Path(p).resolve().as_uri() + "?mode=ro", uri=True, timeout=5)  # [보완 v3_6] Windows 경로 · # · 공백 안전 (f-string URI 는 # 뒤를 잘라 다른 파일을 엶 · 실측)
        try:
            return con.execute("SELECT 1 FROM trade_details WHERE reason=? LIMIT 1", (reason,)).fetchone() is not None
        finally:
            con.close()

    async def _retry_accounting_debts(self):
        """[보완 v3_5] 미기록이 확정된 매도 성과 행 재시도 · reason 표식으로 정확히 1 회 · 부채 0 = 원장 종결 재판정 · 수수료 보정 부채 = 미종결 유지"""
        if self.mode != 'real':
            return
        done = set()
        for ident, idx, d in self.engine.pending_debts():
            if ident in done:
                continue
            done.add(ident)
            try:
                if d.get('kind') != 'sell_record':
                    logger.critical(f"[ACCOUNTING-DEBT] {d.get('symbol')} id={ident} {d.get('kind')} = 자동 해소 불가 · 미종결 유지")
                    continue
                if 'target_v' not in d:  # [보완 v3_7] v3_5 / v3_6 형식 (단일 체결 · reason 표식) = 사건 대조 근거 없음 → 자동 해소 안 함
                    logger.critical(f"[ACCOUNTING-DEBT] {d.get('symbol')} id={ident} 옛 형식 매도 부채 = 자동 해소 불가 · 미종결 유지")
                    continue
                _r0 = next((r for r in self.engine.pending_orders() if r.get('identifier') == ident), {})
                if not _r0.get('ev'):  # [보완 v3_8] 사건 식별 이전 주문
                    logger.critical(f"[ACCOUNTING-DEBT] {d.get('symbol')} id={ident} 사건 식별 이전 주문 = 자동 해소 불가 · 미종결 유지")
                    continue
                _sn = _r0.get('snapshot') or {}
                if _sn.get('volume') is not None and _sn.get('funds') is not None:  # [보완 v3_10 K2] 목표 = 그 주문의 최신 확인 체결 (부채의 옛 목표 아님)
                    _tv, _tf, _tc = float(_sn['volume']), float(_sn['funds']), float(_sn.get('fee', 0.0) or 0.0)
                else:
                    _tv, _tf, _tc = float(d['target_v']), float(d['target_f']), float(d['target_c'])
                if 'efp_total' in d:  # [보완 v3_10 K3] 포지션 전체 매수 수수료 · 원수량 (비율은 성과 함수가 계산)
                    _efp, _ov = float(d.get('efp_total') or 0.0), float(d.get('ov') or 0.0) or _tv
                else:
                    _efp, _ov = float(d.get('efp', 0.0) or 0.0), float(d['target_v'])
                _pos = Position(symbol=d['symbol'], volume=_tv, avg_price=float(d['avg_price']), entry_time=datetime.now(), entry_reason="RECORD_RETRY",
                                stop_loss=0.0, take_profit=0.0, peak_price=float(d['avg_price']), trailing_stop_distance=0.0, entry_atr=0.0, entry_regime=None,
                                original_volume=_ov, metadata={}, entry_fee_paid=_efp)
                _ok = await self._record_sell_shortfall(d.get('ident') or ident, d['symbol'], _tv, _tf, _tc, _pos, d.get('reason') or "RECORD_RETRY")
                if _ok is True:
                    self.engine.resolve_debt(ident, idx, "재기록 commit")
                else:
                    logger.critical(f"[ACCOUNTING-DEBT] {d['symbol']} id={ident} 재기록 실패 · 다음 주기 재시도")
            except Exception as e:
                logger.critical(f"[ACCOUNTING-DEBT] id={ident} {type(e).__name__}: {e}")

    def _persist_position(self, symbol, fallback=None):
        """[보완 v3_4 ④] REAL 포지션 · TP 진행 · 늦은 체결 마커를 원장 DB 에 영속 (재시작 복원 근거 · 거래소 잔고 추정 대신) · 실패 = 예외 (호출 경로가 반영 확인을 멈춤)"""
        if self.mode != 'real':
            return
        p = self.positions.get(symbol)
        try:
            if p is None:
                if fallback is not None and any(r['symbol'] == symbol for r in self.engine.pending_orders()):
                    _fm = fallback.metadata if isinstance(fallback.metadata, dict) else {}
                    self.engine.save_position_state(symbol, dict(  # [보완 v3_5] 청산 표지 : 미종결 · 회계가 끝날 때까지 근거 보존
                        closed=True, volume=0.0, avg_price=float(fallback.avg_price), original_volume=float(getattr(fallback, 'original_volume', 0.0) or 0.0),
                        entry_fee_paid=float(getattr(fallback, 'entry_fee_paid', 0.0) or 0.0),  # [보완 v3_10 K3]
                        partial_sell_count=int(getattr(fallback, 'partial_sell_count', 0) or 0), tp_stage_filled=_fm.get('tp_stage_filled', {}),
                        late_fill_markers=list(_fm.get('late_fill_markers', [])), order_applied=_fm.get('order_applied', {}), saved_at=datetime.now().isoformat()))
                else:
                    self.engine.drop_position_state(symbol)
                return
            md = p.metadata if isinstance(p.metadata, dict) else {}
            self.engine.save_position_state(symbol, dict(
                volume=float(p.volume), avg_price=float(p.avg_price), original_volume=float(getattr(p, 'original_volume', 0.0) or 0.0),
                entry_fee_paid=float(getattr(p, 'entry_fee_paid', 0.0) or 0.0),  # [보완 v3_10 K3]
                partial_sell_count=int(getattr(p, 'partial_sell_count', 0) or 0), tp_stage_filled=md.get('tp_stage_filled', {}),
                late_fill_markers=list(md.get('late_fill_markers', [])), order_applied=md.get('order_applied', {}), saved_at=datetime.now().isoformat()))
        except Exception as e:
            logger.critical(f"[POSITION-STATE-HOLD] {symbol} 영속 저장 실패: {e}")
            raise

    def _restore_position_state(self, pos):
        """[보완 v3_4 ④] 재시작 : 거래소 복구 포지션에 영속 상태 복원 · 거래소와 다르면 경보 (자동 편입 없음 · 늦은 체결 복구 / 대조가 판정)"""
        try:
            s = self.engine.load_position_state(pos.symbol)
        except Exception as e:
            logger.critical(f"[POSITION-STATE-HOLD] {pos.symbol} 영속 상태 읽기 실패: {e}")
            return
        if not s:
            return
        if s.get('closed'):
            logger.critical(f"[POSITION-STATE-DIFF] {pos.symbol} 영속 = 청산 표지인데 거래소 보유 {float(pos.volume):.8f} → 늦은 체결 복구 · 대조가 판정 (자동 편입 없음)")
            return
        exch_v = float(pos.volume)
        pos.volume = float(s['volume'])
        pos.avg_price = float(s.get('avg_price') or pos.avg_price)
        pos.original_volume = float(s.get('original_volume') or pos.volume)
        pos.partial_sell_count = int(s.get('partial_sell_count', 0) or 0)
        md = pos.metadata if isinstance(pos.metadata, dict) else {}
        md.update(tp_stage_filled=s.get('tp_stage_filled', {}) or {}, late_fill_markers=list(s.get('late_fill_markers', []) or []),
                  order_applied=s.get('order_applied', {}) or {}, restored_state=True)
        if 'entry_fee_paid' in s:  # [보완 v3_10 K3]
            pos.entry_fee_paid = float(s.get('entry_fee_paid') or 0.0)
        else:
            md['efp_unknown'] = True  # 옛 상태 = 매수 수수료 근거 없음 → 자동 성과 기록 거부
        pos.metadata = md
        if abs(exch_v - pos.volume) > 1e-8:
            logger.critical(f"[POSITION-STATE-DIFF] {pos.symbol} 거래소 {exch_v:.8f} ≠ 영속 {pos.volume:.8f} → 늦은 체결 복구 · 대조가 판정 (자동 편입 없음)")

    async def _reconcile_real_orders(self):
        """REAL 전용 조회·경보. 주문 재전송·임의 잔고 편입·임의 해제 없음."""
        if self.mode != 'real':
            return
        try:
            pending = await self.engine.refresh_pending_orders()
            try:
                await self._recover_late_fills()  # [보완 v3_4 ①]
                await self._retry_accounting_debts()  # [보완 v3_5]
                _open = {r['symbol'] for r in self.engine.pending_orders()}
                for _s, _st in self.engine.list_position_states().items():
                    if _st.get('closed') and _s not in _open:
                        self.engine.drop_position_state(_s)  # [보완 v3_5] 청산 표지 = 미종결 끝나면 정리
                pending = self.engine.pending_orders()
            except Exception as _lf:
                logger.critical(f"[LATE-FILL-HOLD] 복구 단계 실패 (미종결 경보는 계속): {_lf}")
            for rec in pending:
                snap = rec.get('snapshot', {})
                logger.critical(
                    f"[ORDER-PENDING] {rec['symbol']} id={rec['identifier']} "
                    f"uuid={rec.get('uuid', '')} state={snap.get('state', rec.get('state'))} "
                    f"확인체결={snap.get('volume', '미확정')} "
                    f"반영체결={rec.get('applied_volume', 0)} | 자동 해제 금지 · 조회: real_journal_resolve.py list"
                )
        except Exception as e:
            logger.critical(f"[ORDER-JOURNAL-HOLD] {e}")
        try:
            from math import isfinite, isclose
            accounts = await self.api_client.get_balance(strict=True)
            actual = {}
            for item in accounts:
                currency = item.get('currency')
                if not currency or currency == 'KRW':
                    continue
                free = float(item['balance'])
                locked = float(item.get('locked', 0) or 0)
                if not all(isfinite(v) and v >= 0 for v in (free, locked)):
                    raise ValueError(f"{currency} 잔고 숫자 오류")
                if free + locked > 0:
                    actual[f"KRW-{currency}"] = free + locked
            async with self._positions_lock:
                local = {s: float(p.volume) for s, p in self.positions.items()}
            for symbol in set(actual) | set(local):
                exchange_qty, bot_qty = actual.get(symbol, 0.0), local.get(symbol, 0.0)
                if not isclose(exchange_qty, bot_qty, rel_tol=1e-8, abs_tol=1e-12):
                    logger.critical(
                        f"[BALANCE-MISMATCH] {symbol} "
                        f"거래소={exchange_qty} 봇={bot_qty} | 자동 편입/삭제 없음"
                    )
        except Exception as e:
            logger.warning(f"[BALANCE-RECONCILE-FAIL] {e}")

    async def _handle_periodic_tasks(self):
        """주기적 작업"""
        # 🔥 WebSocket 상태 모니터링 (1분마다)
        if self.timers.check_and_reset('websocket_check'):
            await self._check_websocket_health()
        
        # 성과 로그
        if self.timers.check_and_reset('performance'):
            if self.mode == 'real':
                await self._reconcile_real_orders()
            await self._log_performance()
        
        # 🔧 추가: 일일 정리 (기존 report_cleanup 타이머 활용)
        if self.timers.check_and_reset('report_cleanup'):
            logger.info("=" * 60)
            logger.info("🧹 일일 파일 정리 시작")
            logger.info("=" * 60)
            
            self._cleanup_old_logs()
            self._cleanup_old_reports()
            self._cleanup_old_databases()
            
            logger.info("=" * 60)
            logger.info("✅ 일일 파일 정리 완료")
            logger.info("=" * 60)
        
        # 🆕 메모리 사용량 체크 (1시간마다)
        if self.timers.check_and_reset('memory_check'):
            self._log_memory_usage("주기적 체크")
        
        # 전략 검토
        if self.timers.check_and_reset('strategy_review'):
            # 전략 검토 로직 (추후 구현)
            pass
        
        # 일반 캐시 정리
        if self.timers.check_and_reset('cleanup'):
            self._cleanup_cache()
    
    async def _check_websocket_health(self):
        """WebSocket 건강 상태 체크"""
        if not hasattr(self, 'ws_manager') or not self.ws_manager:
            return
        
        try:
            status = self.ws_manager.get_status()
            
            is_healthy = status.get('healthy', False)
            is_connected = status.get('connected', False)
            reconnect_attempts = status.get('reconnect_attempts', 0)
            
            if not is_healthy:
                logger.warning(
                    f"⚠️ WebSocket 비정상 | "
                    f"연결: {is_connected} | "
                    f"재시도: {reconnect_attempts}회 | "
                    f"모드: REST API 백업"
                )
            else:
                logger.debug(
                    f"✅ WebSocket 정상 | "
                    f"구독: {len(status.get('markets', []))}개"
                )
        
        except Exception as e:
            logger.debug(f"WebSocket 상태 체크 실패: {e}")
    
    async def _log_performance(self):
        """성과 로그 (당일 통계 중심)"""
        balance = await self.engine.get_balance() if self.engine else self.current_balance
        total_value = balance + sum(p.volume * p.avg_price for p in self.positions.values())
        
        # 전체 누적 수익률
        total_returns = ((total_value - self.initial_balance) / self.initial_balance * 100) if self.initial_balance > 0 else 0
        
        # 당일 통계 가져오기
        daily_stats = {}
        if hasattr(self, 'performance') and self.performance:
            daily_stats = self.performance.get_daily_stats()
        
        daily_count = daily_stats.get('count', 0)
        daily_pnl = daily_stats.get('pnl', 0)
        daily_win_rate = daily_stats.get('win_rate', 0)
        
        # 당일 수익률 (세션 시작 잔고 대비)
        if hasattr(self, 'session_start_balance') and self.session_start_balance > 0:
            daily_returns_pct = (daily_pnl / self.session_start_balance * 100)
        else:
            daily_returns_pct = 0
        
        # ✨ 최종 개선된 로그
        logger.info(
            f"[성과] 💰 자산: {total_value:,.0f}원 (전체: {total_returns:+.1f}%) | "
            f"📅 오늘: {daily_count}건 {daily_pnl:+,.0f}원 ({daily_returns_pct:+.2f}%) | "
            f"🎯 승률: {daily_win_rate:.0%} | "  # ← .1% → .0% 변경!
            f"📊 포지션: {len(self.positions)}개"
        )
    
    def _log_memory_usage(self, context: str = ""):
        """메모리 사용량 로깅
        
        Args:
            context: 로깅 컨텍스트 (예: "초기화 완료", "주기적 체크")
        """
        try:
            process = psutil.Process(os.getpid())
            mem_info = process.memory_info()
            
            # RSS: 실제 물리 메모리 사용량 (MB)
            rss_mb = mem_info.rss / 1024 / 1024
            
            # VMS: 가상 메모리 사용량 (MB)
            vms_mb = mem_info.vms / 1024 / 1024
            
            # 시스템 전체 메모리
            sys_mem = psutil.virtual_memory()
            sys_total_mb = sys_mem.total / 1024 / 1024
            sys_used_mb = sys_mem.used / 1024 / 1024
            sys_percent = sys_mem.percent
            
            logger.info(f"💾 메모리 사용 [{context}]")
            logger.info(f"   봇 프로세스: {rss_mb:.1f} MB (물리) / {vms_mb:.1f} MB (가상)")
            logger.info("   시스템 전체: %.0f / %.0f MB (%.1f%%)", sys_used_mb, sys_total_mb, sys_percent)
            
            # ⚠️ 경고: 봇이 4GB 이상 사용 시
            if rss_mb > 4096:
                logger.warning(f"⚠️ 메모리 사용량 높음: {rss_mb:.1f} MB - 재시작 권장")
            
            # 🚨 위험: 시스템 메모리 85% 이상 사용 시
            if sys_percent > 85:
                logger.warning("🚨 시스템 메모리 부족: %.1f%% 사용 중", sys_percent)
        
        except Exception as e:
            logger.debug(f"메모리 로깅 실패: {e}")
    
    def _cleanup_cache(self):
        """캐시 정리 - 간소화 버전"""
        try:
            now = datetime.now()
            
            # MA 캐시 정리 (5분 이상 된 것)
            if hasattr(self, '_ma_cache'):
                expired = [k for k, (cached_time, _) in self._ma_cache.items()
                        if (now - cached_time).seconds > 300]
                
                for key in expired:
                    del self._ma_cache[key]
                
                if expired:
                    logger.debug(f"MA 캐시 {len(expired)}개 정리")
            
        except Exception as e:
            logger.error(f"캐시 정리 오류: {e}")
    
    def _cleanup_old_logs(self):
        """오래된 로그 파일 정리 (날짜 기반)"""
        try:
            now = datetime.now()
            cutoff_date = now - timedelta(days=self.log_retention_days)
            
            deleted_count = 0
            deleted_size = 0
            
            # ========================================
            # 1. 날짜 형식 로그 파일 정리 (trading_bot_YYYYMMDD.log)
            # ========================================
            for log_file in self.log_dir.glob("trading_bot_*.log"):
                try:
                    # 파일명에서 날짜 추출 (trading_bot_20251011.log)
                    filename = log_file.stem  # 확장자 제외
                    date_str = filename.split('_')[-1]  # '20251011'
                    
                    # 날짜 파싱
                    try:
                        file_date = datetime.strptime(date_str, '%Y%m%d')
                        
                        if file_date.date() < cutoff_date.date():
                            file_size = log_file.stat().st_size
                            log_file.unlink()
                            deleted_count += 1
                            deleted_size += file_size
                            
                            logger.debug(
                                f"🗑️ 로그 삭제: {log_file.name} "
                                f"(날짜: {file_date.strftime('%Y-%m-%d')}, "
                                f"크기: {file_size/1024:.1f}KB)"
                            )
                    except ValueError:
                        # 날짜 파싱 실패 - 수정 시간으로 판단
                        file_time = datetime.fromtimestamp(log_file.stat().st_mtime)
                        if file_time.date() < cutoff_date.date():
                            file_size = log_file.stat().st_size
                            log_file.unlink()
                            deleted_count += 1
                            deleted_size += file_size
                            
                            logger.debug(
                                f"🗑️ 로그 삭제: {log_file.name} "
                                f"(수정시간: {file_time.strftime('%Y-%m-%d')}, "
                                f"크기: {file_size/1024:.1f}KB)"
                            )
                            
                except Exception as e:
                    logger.error(f"로그 삭제 실패 {log_file}: {e}")
            
            # ========================================
            # 2. 로그 로테이션 파일 정리 (*.log.1, *.log.2, ...)
            # ========================================
            for log_file in self.log_dir.glob("*.log.*"):
                try:
                    # 로테이션 파일은 수정 시간 기준
                    file_time = datetime.fromtimestamp(log_file.stat().st_mtime)
                    
                    if file_time.date() < cutoff_date.date():
                        file_size = log_file.stat().st_size
                        log_file.unlink()
                        deleted_count += 1
                        deleted_size += file_size
                        
                        logger.debug(
                            f"🗑️ 로테이션 로그 삭제: {log_file.name} "
                            f"(수정: {file_time.strftime('%Y-%m-%d')}, "
                            f"크기: {file_size/1024:.1f}KB)"
                        )
                except Exception as e:
                    logger.error(f"로테이션 로그 삭제 실패 {log_file}: {e}")
            
            if deleted_count > 0:
                logger.info(
                    f"✅ 로그 정리 완료: {deleted_count}개 파일 삭제 "
                    f"(총 {deleted_size/1024/1024:.2f}MB, 보관기간 {self.log_retention_days}일)"
                )
            else:
                logger.debug(f"로그 정리: 삭제 대상 없음 (보관기간 {self.log_retention_days}일)")
        
        except Exception as e:
            logger.error(f"로그 정리 오류: {e}")
    
    def _cleanup_old_reports(self):
        """오래된 리포트 파일 정리 (날짜 기반)"""
        try:
            now = datetime.now()
            cutoff_date = now - timedelta(days=self.report_retention_days)
            
            deleted_count = 0
            deleted_size = 0
            
            # ========================================
            # 리포트 파일 정리 (daily_summary_YYYY-MM-DD.txt 또는 .log)
            # ========================================
            for report_file in self.report_dir.glob("daily_summary_*.*"):
                try:
                    # 파일명에서 날짜 추출 (daily_summary_2025-10-11.txt/log)
                    filename = report_file.stem  # 확장자 제외
                    date_str = filename.replace('daily_summary_', '')  # '2025-10-11'
                    
                    # 날짜 파싱
                    try:
                        file_date = datetime.strptime(date_str, '%Y-%m-%d')
                        
                        if file_date.date() < cutoff_date.date():
                            file_size = report_file.stat().st_size
                            report_file.unlink()
                            deleted_count += 1
                            deleted_size += file_size
                            
                            logger.debug(
                                f"🗑️ 리포트 삭제: {report_file.name} "
                                f"(날짜: {file_date.strftime('%Y-%m-%d')}, "
                                f"크기: {file_size/1024:.1f}KB)"
                            )
                    except ValueError:
                        # 날짜 파싱 실패 - 수정 시간으로 판단
                        file_time = datetime.fromtimestamp(report_file.stat().st_mtime)
                        if file_time.date() < cutoff_date.date():
                            file_size = report_file.stat().st_size
                            report_file.unlink()
                            deleted_count += 1
                            deleted_size += file_size
                            
                            logger.debug(
                                f"🗑️ 리포트 삭제: {report_file.name} "
                                f"(수정시간: {file_time.strftime('%Y-%m-%d')}, "
                                f"크기: {file_size/1024:.1f}KB)"
                            )
                            
                except Exception as e:
                    logger.error(f"리포트 삭제 실패 {report_file}: {e}")
            
            if deleted_count > 0:
                logger.info(
                    f"✅ 리포트 정리 완료: {deleted_count}개 파일 삭제 "
                    f"(총 {deleted_size/1024:.1f}KB, 보관기간 {self.report_retention_days}일)"
                )
            else:
                logger.debug(f"리포트 정리: 삭제 대상 없음 (보관기간 {self.report_retention_days}일)")
        
        except Exception as e:
            logger.error(f"리포트 정리 오류: {e}")
    
    def _cleanup_old_databases(self):
        """오래된 DB 백업 및 킬스위치 파일 정리"""
        try:
            now = datetime.now()
            
            # ========================================
            # 🛡️ 보호할 원본 DB 파일 목록 (삭제 금지!)
            # ========================================
            PROTECTED_DB_FILES = {
                'kill_switch_events.db',
                'signal_performance.db',
                'market_data.db',
                'positions.db'
            }
        
            # ========================================
            # 1. DB 백업 파일 정리
            # ========================================
            db_cutoff = now - timedelta(days=self.db_backup_retention_days)
            db_backup_patterns = [
                "*.db.backup",
                "*.db.bak",
                "*_backup_*.db"
            ]
            
            db_deleted_count = 0
            
            for pattern in db_backup_patterns:
                for db_file in Path(".").glob(pattern):
                    try:
                        if db_file.name in PROTECTED_DB_FILES:
                            logger.warning(f"⚠️ 원본 DB 보호: {db_file.name} 삭제 차단")
                            continue
                        
                        if db_file.suffix == '.db' and not any([
                            'backup' in db_file.name.lower(),
                            'bak' in db_file.name.lower(),
                            'old' in db_file.name.lower()
                        ]):
                            logger.warning(f"⚠️ 원본 DB로 의심: {db_file.name} 삭제 차단")
                            continue
                        
                        file_time = datetime.fromtimestamp(db_file.stat().st_mtime)
                        
                        if file_time < db_cutoff:
                            db_file.unlink()
                            db_deleted_count += 1
                            logger.debug(f"🗑️ DB 백업 삭제: {db_file.name}")
                    except Exception as e:
                        logger.error(f"DB 백업 삭제 실패 {db_file}: {e}")
            
            if db_deleted_count > 0:
                logger.info(
                    f"✅ DB 백업 정리 완료: {db_deleted_count}개 파일 삭제 "
                    f"(보관기간 {self.db_backup_retention_days}일)"
                )
            
            # ========================================
            # 2. 킬스위치 이벤트 파일 정리 (신규!)
            # ========================================
            # 킬스위치 파일은 짧게 보관 (30일)
            killswitch_cutoff = now - timedelta(days=self.killswitch_retention_days)
            
            killswitch_patterns = [
                "kill_switch_*.json",
                "kill_switch_*.txt",
                "killswitch_*.json"
            ]
            
            ks_deleted_count = 0
            ks_deleted_size = 0
            
            for pattern in killswitch_patterns:
                for ks_file in Path(".").glob(pattern):
                    try:
                        # 파일명에서 날짜 추출 시도
                        # kill_switch_KILL_20251011_143022.json
                        filename = ks_file.stem
                        
                        # 날짜 파싱 시도 (YYYYMMDD 형식)
                        import re
                        date_match = re.search(r'(\d{8})', filename)
                        
                        if date_match:
                            date_str = date_match.group(1)
                            try:
                                file_date = datetime.strptime(date_str, '%Y%m%d')
                                
                                if file_date < killswitch_cutoff:
                                    file_size = ks_file.stat().st_size
                                    ks_file.unlink()
                                    ks_deleted_count += 1
                                    ks_deleted_size += file_size
                                    
                                    logger.debug(
                                        f"🗑️ 킬스위치 파일 삭제: {ks_file.name} "
                                        f"(날짜: {file_date.strftime('%Y-%m-%d')})"
                                    )
                            except ValueError:
                                # 날짜 파싱 실패 - 수정시간 기준
                                file_time = datetime.fromtimestamp(ks_file.stat().st_mtime)
                                if file_time < killswitch_cutoff:
                                    file_size = ks_file.stat().st_size
                                    ks_file.unlink()
                                    ks_deleted_count += 1
                                    ks_deleted_size += file_size
                        else:
                            # 날짜 없음 - 수정시간 기준
                            file_time = datetime.fromtimestamp(ks_file.stat().st_mtime)
                            if file_time < killswitch_cutoff:
                                file_size = ks_file.stat().st_size
                                ks_file.unlink()
                                ks_deleted_count += 1
                                ks_deleted_size += file_size
                                
                    except Exception as e:
                        logger.error(f"킬스위치 파일 삭제 실패 {ks_file}: {e}")
            
            if ks_deleted_count > 0:
                logger.info(
                    f"✅ 킬스위치 파일 정리 완료: {ks_deleted_count}개 파일 삭제 "
                    f"(총 {ks_deleted_size/1024:.1f}KB, 보관기간 {self.killswitch_retention_days}일)"
                )
            else:
                logger.debug(f"킬스위치 파일 정리: 삭제 대상 없음 (보관기간 {self.killswitch_retention_days}일)")
        
        except Exception as e:
            logger.error(f"파일 정리 오류: {e}")
    
    async def _monitor_websocket_health(self):
        """WebSocket 헬스 모니터링 및 자동 복구        
        주기적으로 WebSocket 상태를 체크하고:
        - 연결 상태 로그
        - 장기 실패 시 강제 재시작
        - 데이터 신선도 체크
        """
        last_check = time.time()
        check_interval = 60  # 1분마다 체크
        last_health_log = time.time()
        health_log_interval = 300  # 5분마다 정상 상태 로그
        
        while self.is_running:
            try:
                await asyncio.sleep(10)  # 10초마다 루프
                
                current_time = time.time()
                
                # ========================================
                # 1분마다 상태 체크
                # ========================================
                if current_time - last_check < check_interval:
                    continue
                
                last_check = current_time
                
                # WebSocket Manager 확인
                if not self.data_manager or not self.data_manager.ws_manager:
                    logger.warning("⚠️ WebSocket Manager 미초기화")
                    continue
                
                ws_manager = self.data_manager.ws_manager
                status = ws_manager.get_status()
                
                # ========================================
                # 상태별 처리
                # ========================================
                if status['healthy']:
                    # 정상 상태 - 5분마다 로그
                    if current_time - last_health_log >= health_log_interval:
                        logger.info(
                            f"[WebSocket 헬스체크] ✅ 정상 | "
                            f"구독: {len(status['markets'])}개 | "
                            f"캐시: {status.get('cached_tickers', 0)}개 | "
                            f"최신: {status.get('last_data_age_sec', 0):.1f}초 전 | "
                            f"수신태스크: {status.get('receive_task', 'unknown')}"
                        )
                        last_health_log = current_time
                    
                    # 데이터 신선도 체크 (30초 이상 오래됨)
                    last_age = status.get('last_data_age_sec')
                    if last_age and last_age > 30:
                        logger.warning(
                            f"⚠️ WebSocket 데이터 오래됨: {last_age:.1f}초 | "
                            f"재연결 필요 가능성"
                        )
                
                else:
                    # 비정상 상태
                    logger.warning(
                        f"[WebSocket 헬스체크] ⚠️ 비정상 | "
                        f"연결: {status['connected']} | "
                        f"건강: {status['healthy']} | "
                        f"재시도: {status['reconnect_attempts']}회 | "
                        f"모드: {status['mode']} | "
                        f"수신태스크: {status.get('receive_task', 'unknown')}"
                    )
                    
                    # ========================================
                    # 🚨 강제 재시작 조건
                    # ========================================
                    # 1. 재연결 시도 15회 이상
                    # 2. 수신 태스크 중지됨
                    should_force_restart = False
                    restart_reason = ""
                    
                    if status['reconnect_attempts'] >= 15:
                        should_force_restart = True
                        restart_reason = f"재연결 실패 {status['reconnect_attempts']}회"
                    
                    elif status.get('receive_task') == 'stopped':
                        should_force_restart = True
                        restart_reason = "수신 태스크 중지"
                    
                    if should_force_restart:
                        logger.critical(
                            f"🚨 WebSocket 강제 재시작 실행! "
                            f"사유: {restart_reason}"
                        )
                        
                        try:
                            # 완전 종료
                            logger.info("1/3: WebSocket 중지 중...")
                            await ws_manager.stop()
                            
                            # 대기
                            logger.info("2/3: 3초 대기 중...")
                            await asyncio.sleep(3)
                            
                            # 재시작
                            logger.info("3/3: WebSocket 재시작 중...")
                            
                            subscribe_markets = self._get_ws_subscription_markets()
                            if subscribe_markets:
                                ws_manager.update_markets(subscribe_markets)
                                logger.info(
                                    f"✅ WebSocket 구독 재설정: {len(subscribe_markets)}개 종목 "
                                    f"(보유포지션 우선 포함)"
                                )
                            
                            await ws_manager.start()
                            
                            # 재시작 후 상태 확인
                            await asyncio.sleep(2)
                            new_status = ws_manager.get_status()
                            
                            if new_status['healthy']:
                                logger.info(
                                    f"✅ WebSocket 강제 재시작 성공! "
                                    f"연결: {new_status['connected']} | "
                                    f"건강: {new_status['healthy']} | "
                                    f"구독: {len(new_status['markets'])}개"
                                )
                            else:
                                logger.error(
                                    f"❌ WebSocket 재시작했으나 비정상: "
                                    f"연결={new_status['connected']}, "
                                    f"건강={new_status['healthy']}, "
                                    f"재시도={new_status['reconnect_attempts']}회"
                                )
                            
                        except Exception as e:
                            logger.error(f"WebSocket 강제 재시작 실패: {e}")
                            import traceback
                            logger.debug(f"상세:\n{traceback.format_exc()}")
            
            except Exception as e:
                logger.error(f"WebSocket 헬스체크 오류: {e}")
                import traceback
                logger.debug(f"상세:\n{traceback.format_exc()}")
                await asyncio.sleep(30)  # 오류 발생 시 30초 대기
    
    async def _update_websocket_markets(self):
        """WebSocket 구독 마켓 업데이트        
        시장 분석 후 상위 종목이 변경되면
        WebSocket 구독 목록도 업데이트
        """
        try:
            if not self.data_manager or not self.data_manager.ws_manager:
                return
            
            # 보유 포지션 + 상위 종목 통합 구독
            if hasattr(self, 'top_markets') and self.top_markets:
                subscribe_markets = self._get_ws_subscription_markets()
                
                # 기존 구독과 비교
                ws_manager = self.data_manager.ws_manager
                current_markets = ws_manager.markets
                
                if set(subscribe_markets) != current_markets:
                    logger.info(
                        f"[WebSocket] 구독 업데이트: "
                        f"{len(subscribe_markets)}개 종목"
                    )
                    ws_manager.update_markets(subscribe_markets)
        
        except Exception as e:
            logger.error(f"WebSocket 구독 업데이트 오류: {e}")
    
    async def graceful_shutdown(self):
        """정상 종료 - 안전한 리소스 정리"""
        logger.info("=" * 60)
        logger.info("봇 종료 시작...")
        logger.info("=" * 60)
        
        self.is_running = False
        
        try:
            # ========================================
            # 1. 일일 요약 저장
            # ========================================
            if self.daily_trades:
                try:
                    await self._save_daily_summary()
                    logger.info("✅ 일일 요약 저장 완료")
                except Exception as e:
                    logger.error(f"일일 요약 저장 오류: {e}")
            
            # ========================================
            # 2. 리스크 관리자 종료 (🆕 추가!)
            # ========================================
            if hasattr(self, 'risk_manager') and self.risk_manager:
                try:
                    if hasattr(self.risk_manager, 'shutdown'):
                        await self.risk_manager.shutdown()
                        logger.info("✅ 리스크 관리자 종료 완료")
                    else:
                        logger.debug("리스크 관리자에 shutdown() 메서드 없음 - 스킵")
                except Exception as e:
                    logger.error(f"리스크 관리자 종료 오류: {e}")
            
            # ========================================
            # 3. 성과 관리자 종료 (🔄 수정!)
            # ========================================
            if hasattr(self, 'performance') and self.performance:
                try:
                    if hasattr(self.performance, 'shutdown'):
                        await self.performance.shutdown()
                        logger.info("✅ 성과 관리자 종료 완료")
                    else:
                        logger.debug("성과 관리자에 shutdown() 메서드 없음 - 스킵")
                except Exception as e:
                    logger.error(f"성과 관리자 종료 오류: {e}")
            
            # ========================================
            # 4. WebSocket 정리
            # ========================================
            if hasattr(self, 'ws_manager') and self.ws_manager:
                try:
                    await self.ws_manager.stop()
                    logger.info("✅ WebSocket 종료 완료")
                except Exception as e:
                    logger.error(f"WebSocket 종료 오류: {e}")
            
            # ========================================
            # 5. API 클라이언트 정리 (aiohttp session)
            # ========================================
            if hasattr(self, 'api_client') and self.api_client:
                try:
                    await self.api_client.close()
                    logger.info("✅ API 클라이언트 종료 완료")
                except Exception as e:
                    logger.error(f"API 클라이언트 종료 오류: {e}")
            
            # ========================================
            # 6. 실행 엔진 정리
            # ========================================
            if hasattr(self, 'engine') and self.engine:
                try:
                    if hasattr(self.engine, 'shutdown'):
                        await self.engine.shutdown()
                        logger.info("✅ 실행 엔진 종료 완료")
                except Exception as e:
                    logger.error(f"실행 엔진 종료 오류: {e}")
            # ========================================
            # 6.5 📒 Decision Ledger 종료 (남은 큐 기록 후 닫기)
            # ========================================
            if getattr(self, '_decision_ledger', None) is not None:
                try:
                    await asyncio.to_thread(self._decision_ledger.stop, 3.0)
                    logger.info(f"✅ Decision Ledger 종료 완료 | {self._decision_ledger.stats()}")
                except Exception as _dl_e:
                    logger.error(f"Decision Ledger 종료 오류: {_dl_e}")
            
            # ========================================
            # 7. 최종 성과 출력
            # ========================================
            try:
                if hasattr(self, 'engine') and self.engine:
                    balance = await self.engine.get_balance()
                    runtime = datetime.now() - self.session_start_time
                    returns = ((balance - self.initial_balance) / self.initial_balance * 100) if self.initial_balance > 0 else 0
                    
                    logger.info("=" * 60)
                    logger.info("📊 최종 성과")
                    logger.info("=" * 60)
                    logger.info(f"초기 자산: {self.initial_balance:,.0f}원")
                    logger.info(f"최종 현금 (보유 평가 제외): {balance:,.0f}원")
                    logger.info("현금 수익률 (보유 평가 제외): %+.2f%%", returns)
                    # [SIMEXIT-DISPLAY] 위 = 현금만 · 아래 = 참고 평가 (보유 × 마지막 WS 시세 · 없으면 매수가 · 시세 나이 검사 없음 · 정산 값 아님) · 매매 동작 불변
                    try:
                        if self.mode == 'real': raise RuntimeError('SIM 전용')   # [v1_2] 참고 평가 = SIM 전용 (REAL 은 거래소 잔고가 정산 기준)
                        _tc = getattr(getattr(getattr(self, 'data_manager', None), 'ws_manager', None), 'ticker_cache', None) or {}
                        _hv = sum(float(p.volume) * float((_tc.get(s) or {}).get('trade_price') or p.avg_price) for s, p in list(self.positions.items()))
                        _tv = balance + _hv
                        logger.info("[참고 평가] 보유 %d 종 %s원 · 현금 + 보유 %s원 · 수익률 %+.2f%% (마지막 시세 · 없으면 매수가)", len(self.positions), f"{_hv:,.0f}", f"{_tv:,.0f}", ((_tv - self.initial_balance) / self.initial_balance * 100) if self.initial_balance > 0 else 0)
                    except Exception as _sx_e:
                        logger.debug(f"[SIMEXIT-DISPLAY] 참고 평가 실패: {_sx_e}")
                    logger.info(f"실행 시간: {runtime}")
                    logger.info(f"총 거래: {len(self.daily_trades)}건")
                    
                    if self.daily_trades:
                        wins = sum(1 for t in self.daily_trades if t.get('side') == 'sell' and t.get('pnl', 0) > 0)
                        losses = sum(1 for t in self.daily_trades if t.get('side') == 'sell' and t.get('pnl', 0) <= 0)
                        win_rate = (wins / (wins + losses) * 100) if (wins + losses) > 0 else 0
                        logger.info("승률: %.1f%% (%d승 %d패)", win_rate, wins, losses)
                    
                    logger.info("=" * 60)
            except Exception as e:
                logger.error(f"최종 성과 출력 오류: {e}")
            
            # ========================================
            # 8. 킬스위치 상태 확인
            # ========================================
            if hasattr(self, 'risk_manager'):
                if self.risk_manager.kill_switch_active:
                    logger.warning("⚠️ 킬스위치 발동으로 종료")
                    if self.risk_manager.active_kill_event:
                        logger.warning(f"사유: {self.risk_manager.active_kill_event.reason}")
        
        except Exception as e:
            logger.error(f"종료 처리 중 오류: {e}")
            import traceback
            logger.error(f"스택 트레이스:\n{traceback.format_exc()}")
        
        finally:
            logger.info("=" * 60)
            logger.info("✅ 봇 종료 완료")
            logger.info("=" * 60)

# **END**