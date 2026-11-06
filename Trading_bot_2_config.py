#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# ============================================================
# CONFIG MERGE RULE (SSoT 규약)
# ============================================================
# 병합 우선순위:
#   1) DEFAULT_CONFIG (폴백)
#   2) config.ini (공통 설정)
#   3) config_{timeframe}.ini (최종 SSoT)
#
# 모든 매매 관련 로직은
# 반드시 본 ConfigManager를 통해 값을 받아야 한다.
# ============================================================
"""
업비트 자동매매 봇 v1.0 - Config Module
========================================
책임: config.ini 파일 읽기 및 설정값 제공만 담당
- config.ini 파일 읽기 (UTF-8)
- 환경 변수 관리
- 설정값 캐싱 (단순 dict)
- 기본값 자동 처리

- 타임프레임별 설정 병합 지원
  동작 방식:
  1. config.ini (공통) 읽기
  2. timeframe 값 추출 (예: "1m")
  3. configs/config_1m.ini 읽기
  4. 병합 (타임프레임 설정 우선)
  5. 캐싱 및 검증

제공 인터페이스:
  - get_cached(): 캐시된 설정값 조회
  - get_api_keys(): API 키 반환
  - get_signal_weights(): 신호 가중치
  - get_excluded_markets(): 제외 마켓
  - get_included_markets(): 포함 마켓
  
금지사항:
  - 성과 관리 (performance 모듈에서 처리)
  - 가중치 조정 (performance 모듈에서 처리) 
  - 거래 실행 (bot 모듈에서 처리)
  - 데이터 처리 (data 모듈에서 처리)
  - 복잡한 검증 로직
  
데이터 흐름:
  Config → 모든 모듈 (설정값만 제공)
  
개선사항:
  - 불필요한 검증 로직 제거
  - 캐싱 로직 단순화
  - 메서드 수 최소화
========================================
"""

import os
import time
import configparser
from pathlib import Path
from typing import Dict, List, Tuple, Optional
from dotenv import load_dotenv
from Trading_bot_1_core import logger, MarketRegime, singleton
from global_constants import *

load_dotenv()

# ========================================
# 기본 설정값 정의 (폴백용)
# ========================================
DEFAULT_CONFIG = {
    'TRADING': {
        'mode': 'sim',
        'timeframe': DEFAULT_TIMEFRAME,
        'max_positions': 8,
        'risk_per_trade': 0.008,
        'daily_max_loss_pct': 0.02
    },
    'RISK_PROFILE': {
        'profile': 'stable'
    },
    'SIMULATION': {
        'initial_balance': 10000000
    },
    'API_ENDPOINTS': {
        'base_url': 'https://api.upbit.com',
        'websocket_url': 'wss://api.upbit.com/websocket/v1'
    },
    'RISK_MANAGEMENT': {
        'stop_loss_pct': 0.025,
        'take_profit_pct': 0.045,
        'trailing_stop_pct': 0.015,
        'max_consecutive_losses': 6,
        'critical_consecutive_losses': 8,
        'max_drawdown_pct': 0.15,
        'enable_timeout_exit': True,
        'timeout_only_if_losing': True,
        'timeout_hours': 4,
        'enable_trailing_stop': True,
        'enable_partial_sell': True,
        'partial_sell_ratios': '0.4,0.4,0.2',
        'partial_sell_triggers': '1.3,1.8,2.5',
        'max_consecutive_errors': 10
    },
    'MARKET_SELECTION': {
        'market_selection_period': '4h',
        'top_markets_count': 20,
        'min_volume_krw': 5000000000,
        'min_atr_percent': 0.15,
        'enable_volatility_filter': True,
        'atr_calculation_timeframe': '15m',
        'atr_lookback_period': 14,
        'volatility_candidate_count': 50,
        'use_gc_freshness_filter': True,
        'gc_max_candles_since_cross': 6,
    },
    'INCLUSION_LIST': {
        'included_markets': 'KRW-BTC,KRW-ETH,KRW-XRP,KRW-SOL',
        'priority_include': True
    },
    'EXCLUSION_LIST': {
        'excluded_markets': 'KRW-HBAR,KRW-ARB,KRW-SHIB,KRW-BTT',
        'excluded_keywords': 'hedera,hbar,arbitrum,arb,shiba,bittorrent'
    },
    'EXCLUSION': {
        'exclude_caution_markets': True
    },
    'MARKET_CONDITION': {
        'volatility_threshold': 0.03,
    },
    'MARKET_REGIME': {
        'adx_strong_trend': 25,
        'adx_medium_trend': 20
    },
    'STRATEGY': {
        'enable_whipsaw_detection': True,
        'min_confidence_threshold': 0.25,
        'check_volume_quality': True,
        'min_volume_surge': 2.0,
        'ma_trend_filter': True,
        'ma_trend_min_strength': 0.20
    },
    'STRATEGY_SELECTION': {
        'enable_adaptive_strategy': True,
        'enable_range_trading': True,
        'enable_cash_preservation': True
    },
    'RANGE_TRADING': {
        'range_min_width': 0.02,
        'bounce_threshold': 0.005,
        'range_lookback': 40
    },
    'WHIPSAW_DETECTION': {
        'lookback_candles': 10,
        'min_direction_changes': 5,
        'volatility_threshold_pct': 2.5,
        'score_penalty': 0.4
    },
    'MA_EXIT': {
        'enable_15min_ma_exit': True,
        'breach_threshold': 0.003,
        'applicable_timeframes': '1m,3m,5m,10m,15m'
    },
    'PERFORMANCE_TRACKING': {
        'enabled': True,
        'auto_adjust_weights': True,
        'min_samples_for_adjustment': 15,
        'adjustment_check_hours': 2.0
    },
    'POSITION_SIZING': {
        'use_kelly_criterion': True,
        'base_position_size': 0.06,
        'max_position_size': 0.12
    },
    'SIGNAL_WEIGHTS': {
        # 🔒 [2026-08-02] config_{tf}.ini 와 동일한 5축만 유지.
        #   noise_filter / breakthrough / accumulation 3개는 components 에 없는 유령 축이라 제거.
        'ma_signal_weight': 0.18,
        'volume_signal_weight': 0.42,
        'atr_signal_weight': 0.20,
        'adx_signal_weight': 0.00,
        'bw_signal_weight': 0.20,
        # CONF-SHADOW defaults — 매매 판단 무변경 (기존 유지)
        'gb_conf_rise_atr_max': 1.20,
        'legacy_conf_threshold': 0.53,
        'gb_conf_threshold': 0.52
    },
    'EMERGENCY_EXIT': {
        'full_exit_loss': -5.0,
        'partial_70_loss': -4.0,
        'partial_50_loss': -2.5,
        'full_exit_ratio': 1.0,
        'partial_70_ratio': 0.7,
        'partial_50_ratio': 0.5
    },
    'API_LIMITS': {
        'calls_per_second': 5,
        'calls_per_minute': 280
    },
    'DATABASE': {
        'performance_db_path': 'signal_performance.db'
    },
    'ADAPTIVE_SYSTEM': {
        'regime_threshold_trending_up': 0.60,
        'regime_threshold_trending_down': 0.85,
        'regime_threshold_ranging': 0.75,
        'regime_threshold_breakout': 0.55,
        'regime_threshold_breakdown': 0.90
    }
}


@singleton
class ConfigManager(configparser.ConfigParser):
    """
    설정 관리자 v2.1 - 타임프레임별 설정 병합 지원
    
    주요 기능:
    - config.ini (공통) + configs/config_*.ini (타임프레임) 병합
    - 타임프레임 설정이 공통 설정보다 우선
    - 기존 인터페이스 100% 호환
    - 자동 검증 및 폴백
    
    사용 예:
        config = ConfigManager()
        value = config.get_cached('stop_loss_pct')  # 타임프레임별 값 자동 반영
    """
    
    def __init__(self, 
                 base_config: str = 'config.ini',
                 timeframe_config_dir: str = 'configs'):
        """
        초기화 및 설정 병합
        
        Args:
            base_config: 공통 설정 파일 경로
            timeframe_config_dir: 타임프레임별 설정 디렉토리
        """
        # ========================================
        # ConfigParser 초기화 (부모 클래스)
        # ========================================
        super().__init__(inline_comment_prefixes=('#',))
        
        self.base_config_path = Path(base_config)
        self.timeframe_config_dir = Path(timeframe_config_dir)
        
        # ========================================
        # 1단계: 공통 설정 읽기
        # ========================================
        self.base_parser = self._load_config_file(
            self.base_config_path, 
            "공통 설정",
            is_required=True
        )
        
        # ========================================
        # 2단계: 타임프레임 추출
        # ========================================
        self.timeframe = self.base_parser.get('TRADING', 'timeframe', fallback=DEFAULT_TIMEFRAME)
        logger.info(f"🎯 선택된 타임프레임: {self.timeframe}")
        
        # ========================================
        # 3단계: 타임프레임별 설정 읽기
        # ========================================
        timeframe_config_path = self.timeframe_config_dir / f"config_{self.timeframe}.ini"
        self.timeframe_parser = self._load_config_file(
            timeframe_config_path,
            f"{self.timeframe} 타임프레임 설정",
            is_required=False  # 없어도 동작 (공통 설정만 사용)
        )
        
        # ========================================
        # 4단계: 병합 및 자기 자신에 복사
        # ========================================
        merged = self._merge_configs()
        
        # 병합된 설정을 자기 자신(ConfigParser)에 복사
        for section in merged.sections():
            if not self.has_section(section):
                self.add_section(section)
            for key, value in merged.items(section):
                self.set(section, key, value)
        
        # ========================================
        # 5단계: 검증 및 캐싱
        # ========================================
        self._validate_merged_config()
        
        # 리스트 설정
        self.EXCLUDED_MARKETS = []
        self.EXCLUDED_NAME_KEYWORDS = []
        self.INCLUDED_MARKETS = []
        
        self._load_list_configs()
        self._cache_essential_configs()
        
        logger.info("✅ 설정 병합 완료")
        
        # 디버그: 핵심 설정 출력
        if logger.level <= 10:  # DEBUG 레벨
            self._print_merged_summary()
    
    # ========================================
    # Private Methods (내부 처리)
    # ========================================
    
    def _load_config_file(self, 
                          filepath: Path, 
                          description: str,
                          is_required: bool = True) -> configparser.ConfigParser:
        """
        설정 파일 읽기 (재시도 로직 포함)
        
        Args:
            filepath: 설정 파일 경로
            description: 파일 설명 (로그용)
            is_required: 필수 파일 여부
        
        Returns:
            ConfigParser 객체
        
        Raises:
            FileNotFoundError: 필수 파일이 없을 때
            ValueError: 파싱 오류
        """
        parser = configparser.ConfigParser(inline_comment_prefixes=('#',))
        
        # 파일 존재 체크
        if not filepath.exists():
            if is_required:
                raise FileNotFoundError(f"❌ {description} 파일 필수: {filepath}")
            else:
                logger.warning(f"⚠️ {description} 파일 없음: {filepath}")
                logger.info("   → 공통 설정만 사용합니다")
                return parser  # 빈 파서 반환
        
        # 재시도 로직
        max_retries = 3
        retry_delay = 0.2
        
        for attempt in range(max_retries):
            try:
                # 파일 크기 체크
                file_size = filepath.stat().st_size
                if file_size == 0:
                    raise ValueError(f"❌ {description} 파일이 비어있음")
                
                # 파일 읽기
                with open(filepath, 'r', encoding='utf-8') as f:
                    parser.read_file(f)
                
                # 성공
                logger.info(f"✅ {description} 로드: {filepath.name} ({file_size:,} bytes, {len(parser.sections())}개 섹션)")
                return parser
                
            except configparser.ParsingError as e:
                if attempt < max_retries - 1:
                    logger.warning(f"⚠️ 파싱 오류 (재시도 {attempt+1}/{max_retries})")
                    time.sleep(retry_delay)
                else:
                    raise ValueError(f"❌ {description} 파싱 실패: {e}")
            
            except Exception as e:
                if attempt < max_retries - 1:
                    logger.warning(f"⚠️ 읽기 오류 (재시도 {attempt+1}/{max_retries})")
                    time.sleep(retry_delay)
                else:
                    if is_required:
                        raise ValueError(f"❌ {description} 읽기 실패: {e}")
                    else:
                        logger.warning(f"⚠️ {description} 읽기 실패: {e} (공통 설정만 사용)")
                        return parser
        
        return parser
    
    def _merge_configs(self) -> configparser.ConfigParser:
        """
        설정 병합 (타임프레임 설정 우선)        
        병합 규칙:
        1. 기본값(DEFAULT_CONFIG)을 베이스로 설정
        2. 공통 설정(config.ini)으로 덮어쓰기
        3. 타임프레임 설정(config_*.ini)으로 최종 덮어쓰기 (최우선)        
        Returns:
            병합된 ConfigParser 객체
        """
        merged = configparser.ConfigParser(inline_comment_prefixes=('#',))
        
        # ========================================
        # 1단계: 기본값 설정
        # ========================================
        for section, options in DEFAULT_CONFIG.items():
            if not merged.has_section(section):
                merged.add_section(section)
            for key, value in options.items():
                merged.set(section, key, str(value))
        
        # ========================================
        # 2단계: 공통 설정 복사 (기본값 덮어쓰기)
        # ========================================
        for section in self.base_parser.sections():
            if not merged.has_section(section):
                merged.add_section(section)
            for key, value in self.base_parser.items(section):
                merged.set(section, key, value)
        
        # ========================================
        # 3단계: 타임프레임 설정 덮어쓰기 (최우선)
        # ========================================
        override_count = 0
        for section in self.timeframe_parser.sections():
            if not merged.has_section(section):
                merged.add_section(section)
            
            for key, value in self.timeframe_parser.items(section):
                old_value = merged.get(section, key, fallback=None) if merged.has_option(section, key) else None
                merged.set(section, key, value)
                
                # 변경 사항 로그 (DEBUG)
                if old_value and old_value != value:
                    logger.debug(f"   [{section}] {key}: {old_value} → {value}")
                    override_count += 1
        
        if override_count > 0:
            logger.info(f"   📝 타임프레임 설정으로 {override_count}개 항목 덮어쓰기")
        
        # ========================================
        # ✅ 4단계: 병합 결과 검증 로그
        # ========================================
        logger.info(
            f"📘 [병합검증] TF:{self.timeframe.value if hasattr(self.timeframe, 'value') else self.timeframe} | "
            f"손절:{merged.get('RISK_MANAGEMENT', 'stop_loss_pct', fallback='N/A')} | "
            f"익절:{merged.get('RISK_MANAGEMENT', 'take_profit_pct', fallback='N/A')} | "
            f"추적:{merged.get('RISK_MANAGEMENT', 'trailing_stop_pct', fallback='N/A')}"
        )
        
        return merged
    
    def _validate_merged_config(self):
        """
        병합된 설정 검증
        
        검증 항목:
        1. 필수 섹션 존재
        2. 타임프레임 일치 (TIMEFRAME_INFO 섹션)
        3. 수치 범위 (손절/익절 등)
        """
        # ========================================
        # 필수 섹션 검증
        # ========================================
        required_sections = [
            'TRADING', 'RISK_MANAGEMENT', 'STRATEGY', 
            'SIGNAL_WEIGHTS', 'POSITION_SIZING'
        ]
        missing = [s for s in required_sections if not self.has_section(s)]
        if missing:
            raise ValueError(f"❌ 필수 섹션 누락: {missing}")
        
        # ========================================
        # 타임프레임 일치 검증
        # ========================================
        if self.has_section('TIMEFRAME_INFO'):
            config_tf = self.get('TIMEFRAME_INFO', 'timeframe', fallback=None)
            if config_tf and config_tf != self.timeframe:
                logger.warning(
                    f"⚠️ 타임프레임 불일치: "
                    f"config.ini({self.timeframe}) != config_{self.timeframe}.ini({config_tf})"
                )
        
        # ========================================
        # 수치 범위 검증
        # ========================================
        self._validate_numeric_ranges()
    
    def _validate_numeric_ranges(self):
        """수치 값 범위 검증"""
        validations = [
            ('RISK_MANAGEMENT', 'stop_loss_pct', 0.0001, 0.10, "손절 비율"),
            ('RISK_MANAGEMENT', 'take_profit_pct', 0.001, 0.50, "익절 비율"),
            ('STRATEGY', 'min_confidence_threshold', 0.0, 1.0, "신뢰도 임계값"),
            ('TRADING', 'max_positions', 1, 20, "최대 포지션 수"),
            ('TRADING', 'risk_per_trade', 0.0001, 0.10, "거래당 리스크"),
        ]
        
        for section, key, min_val, max_val, desc in validations:
            if not self.has_option(section, key):
                continue
            
            try:
                if isinstance(min_val, int):
                    value = self.getint(section, key)
                else:
                    value = self.getfloat(section, key)
                
                if not (min_val <= value <= max_val):
                    logger.warning(
                        f"⚠️ {desc} 범위 초과: [{section}] {key} = {value} "
                        f"(권장: {min_val}~{max_val})"
                    )
            except ValueError as e:
                logger.error(f"❌ {desc} 값 오류: [{section}] {key} - {e}")
    
    def _load_list_configs(self):
        """리스트 형태 설정 로드"""
        included = self.get('INCLUSION_LIST', 'included_markets', fallback='')
        self.INCLUDED_MARKETS = [m.strip() for m in included.split(',') if m.strip()]
        
        excluded = self.get('EXCLUSION_LIST', 'excluded_markets', fallback='')
        self.EXCLUDED_MARKETS = [m.strip() for m in excluded.split(',') if m.strip()]
        
        keywords = self.get('EXCLUSION_LIST', 'excluded_keywords', fallback='')
        self.EXCLUDED_NAME_KEYWORDS = [k.strip().lower() for k in keywords.split(',') if k.strip()]
    
    def _cache_essential_configs(self):
        """필수 설정값 캐싱"""
        # 타임프레임별 설정 (TIMEFRAME_CONFIGS에서 폴백)
        tf_config = TIMEFRAME_CONFIGS.get(self.timeframe, TIMEFRAME_CONFIGS[DEFAULT_TIMEFRAME])
        
        self.cached_values = {
            # 기본
            'mode': self.get('TRADING', 'mode', fallback='sim'),
            'timeframe': self.timeframe,
            # 🔧 [BUYRATIO-TRADE v2 2026-10-30] candle = 기존 대리값 (기본) · trade = 실제 체결 수량 비율로 운영 변수 교체 (config 에 적어야 켜짐)
            'buying_ratio_source': self.get('TRADING', 'buying_ratio_source', fallback='candle'),
            'max_positions': self.getint('TRADING', 'max_positions', fallback=tf_config.get('max_positions', 8)),
            'risk_per_trade': self.getfloat('TRADING', 'risk_per_trade', fallback=tf_config.get('risk_per_trade', 0.008)),
            'daily_max_loss_pct': self.getfloat('TRADING', 'daily_max_loss_pct', fallback=tf_config.get('daily_max_loss_pct', None)),
            'daily_max_loss': self.getfloat('TRADING', 'daily_max_loss', fallback=tf_config.get('daily_max_loss', None)),
            'initial_balance': self.getfloat('SIMULATION', 'initial_balance', fallback=10000000),
            
            # 리스크
            'stop_loss_pct': self.getfloat('RISK_MANAGEMENT', 'stop_loss_pct', fallback=tf_config.get('stop_loss_pct', 0.025)),
            'take_profit_pct': self.getfloat('RISK_MANAGEMENT', 'take_profit_pct', fallback=tf_config.get('take_profit_pct', 0.045)),
            'trailing_stop_pct': self.getfloat('RISK_MANAGEMENT', 'trailing_stop_pct', fallback=tf_config.get('trailing_stop_pct', 0.015)),
            'enable_timeout_exit': self.getboolean('RISK_MANAGEMENT', 'enable_timeout_exit', fallback=True),
            'timeout_hours': self.getfloat('RISK_MANAGEMENT', 'timeout_hours', fallback=4.0),
            'timeout_only_if_losing': self.getboolean('RISK_MANAGEMENT', 'timeout_only_if_losing', fallback=True),
            'enable_trailing_stop': self.getboolean('RISK_MANAGEMENT', 'enable_trailing_stop', fallback=True),
            'max_consecutive_losses': self.getint('RISK_MANAGEMENT', 'max_consecutive_losses', fallback=tf_config.get('max_consecutive_losses', 5)),
            'critical_consecutive_losses': self.getint('RISK_MANAGEMENT', 'critical_consecutive_losses', fallback=tf_config.get('critical_consecutive_losses', 8)),
            'max_errors': self.getint('RISK_MANAGEMENT', 'max_errors', fallback=10),
            'max_drawdown_pct': self.getfloat('RISK_MANAGEMENT', 'max_drawdown_pct', fallback=0.12),
            
            # MA 이탈
            'enable_15min_ma_exit': self.getboolean('MA_EXIT', 'enable_15min_ma_exit', fallback=True),
            'ma_breach_threshold': self.getfloat('MA_EXIT', 'breach_threshold', fallback=0.003),
            
            # 마켓
            'top_markets_count': self.getint('MARKET_SELECTION', 'top_markets_count', fallback=10),
            'min_volume_krw': self.getfloat('MARKET_SELECTION', 'min_volume_krw', fallback=5000000000),
            'exclude_caution_markets': self.getboolean('EXCLUSION', 'exclude_caution_markets', fallback=True),
            'priority_include': self.getboolean('INCLUSION_LIST', 'priority_include', fallback=True),
            
            # 시장
            'volatility_threshold': self.getfloat('MARKET_CONDITION', 'volatility_threshold', fallback=0.03),
            'adx_strong_trend': self.getfloat('MARKET_REGIME', 'adx_strong_trend', fallback=25),
            'adx_medium_trend': self.getfloat('MARKET_REGIME', 'adx_medium_trend', fallback=20),
            
            # 전략
            'min_confidence_threshold': self.getfloat('STRATEGY', 'min_confidence_threshold', fallback=tf_config.get('min_confidence_threshold', 0.25)),
            'enable_whipsaw_detection': self.getboolean('STRATEGY', 'enable_whipsaw_detection', fallback=True),
            'min_volume_surge': self.getfloat('STRATEGY', 'min_volume_surge', fallback=tf_config.get('min_volume_surge', 1.5)),
            
            # 성과
            'performance_enabled': self.getboolean('PERFORMANCE_TRACKING', 'enabled', fallback=True),
            'auto_adjust_weights': self.getboolean('PERFORMANCE_TRACKING', 'auto_adjust_weights', fallback=True),
            'min_samples_for_adjustment': self.getint('PERFORMANCE_TRACKING', 'min_samples_for_adjustment', fallback=15),
            'adjustment_check_hours': self.getfloat('PERFORMANCE_TRACKING', 'adjustment_check_hours', fallback=2.0),
            
            # 포지션
            'use_kelly_criterion': self.getboolean('POSITION_SIZING', 'use_kelly_criterion', fallback=True),
            'base_position_size': self.getfloat('POSITION_SIZING', 'base_position_size', fallback=tf_config.get('base_position_size', 0.06)),
            'max_position_size': self.getfloat('POSITION_SIZING', 'max_position_size', fallback=tf_config.get('max_position_size', 0.12)),
            
            # API
            'api_base_url': self.get('API_ENDPOINTS', 'base_url', fallback='https://api.upbit.com'),
            'calls_per_second': self.getint('API_LIMITS', 'calls_per_second', fallback=5),
            'calls_per_minute': self.getint('API_LIMITS', 'calls_per_minute', fallback=280),
            
            # 연속 손실
            'max_consecutive_errors': self.getint('RISK_MANAGEMENT', 'max_consecutive_errors', fallback=10),
            
            # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
            # 🔒 SSoT: Tier1 메이저 코인 목록 (VOLUME 섹션)
            # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
            'exclude_tier1': [
                m.strip() 
                for m in self.get('VOLUME', 'exclude_tier1', fallback='KRW-BTC,KRW-ETH').split(',') 
                if m.strip()
            ]
        }
    
    def _print_merged_summary(self):
        """병합된 설정 요약 출력 (디버깅용)"""
        logger.info("=" * 60)
        logger.info(f"📊 현재 적용된 설정 ({self.timeframe} 타임프레임)")
        logger.info("=" * 60)
        
        key_configs = [
            ('max_positions', '최대 포지션'),
            ('risk_per_trade', '거래당 리스크'),
            ('stop_loss_pct', '손절 비율'),
            ('take_profit_pct', '익절 비율'),
            ('timeout_hours', '타임아웃'),
            ('min_confidence_threshold', '신뢰도 임계값'),
            ('base_position_size', '기본 포지션 크기'),
        ]
        
        for key, desc in key_configs:
            value = self.cached_values.get(key, 'N/A')
            logger.info(f"  {desc:20s}: {value}")
        
        logger.info("=" * 60)
    
    # ========================================
    # Public Methods (기존 인터페이스 유지)
    # ========================================
    
    def get_cached(self, key: str, default=None):
        """캐시된 설정값 반환"""
        return self.cached_values.get(key, default)
    
    # get, getint, getfloat, getboolean, set 등은 
    # ConfigParser 상속으로 자동 제공됨
    
    def get_api_keys(self) -> Tuple[str, str]:
        """API 키 반환"""
        return os.getenv('UPBIT_ACCESS_KEY'), os.getenv('UPBIT_SECRET_KEY')
    
    def get_excluded_markets(self) -> List[str]:
        """제외 마켓"""
        return self.EXCLUDED_MARKETS.copy()
    
    def get_excluded_keywords(self) -> List[str]:
        """제외 키워드"""
        return self.EXCLUDED_NAME_KEYWORDS.copy()
    
    def get_included_markets(self) -> List[str]:
        """포함 마켓"""
        return self.INCLUDED_MARKETS.copy()
    
    def get_signal_weights(self) -> Dict[str, float]:
        """신호 가중치 (SSoT: config_{tf}.ini [SIGNAL_WEIGHTS])

        🔒 [2026-08-02] 세 가지 오류 교정
         ① 유령 축 3개 제거 — noise_filter / breakthrough / accumulation 은
            components 에 존재하지 않는 신호인데 합계 16.7% 지분을 먹고 있었다.
         ② adx 축 추가 — config_{tf}.ini 에 adx_signal_weight 가 있으면 반영.
         ③ 키 이름을 '_signal' 로 통일 — t3 beta/boost 딕셔너리
            (ma_signal / volume_signal / bw_signal / atr_signal / adx_signal)와
            동일 키를 쓰게 되어 P4 성과튜닝·상관보정이 실제로 걸린다.
        """
        return {
            'ma_signal': self.getfloat('SIGNAL_WEIGHTS', 'ma_signal_weight', fallback=0.18),
            'volume_signal': self.getfloat('SIGNAL_WEIGHTS', 'volume_signal_weight', fallback=0.42),
            'atr_signal': self.getfloat('SIGNAL_WEIGHTS', 'atr_signal_weight', fallback=0.20),
            'adx_signal': self.getfloat('SIGNAL_WEIGHTS', 'adx_signal_weight', fallback=0.00),
            'bw_signal': self.getfloat('SIGNAL_WEIGHTS', 'bw_signal_weight', fallback=0.20),
        }
    
    def get_regime_thresholds(self) -> Dict[MarketRegime, float]:
        """체제별 임계값"""
        base = self.get_cached('min_confidence_threshold')
        if base is None:
            base = 0.25
        
        return {
            MarketRegime.TRENDING_UP: base * 0.85,
            MarketRegime.RANGING: base * 1.0,
            MarketRegime.TRENDING_DOWN: base * 1.15,
            MarketRegime.BREAKOUT: base * 0.80,
            MarketRegime.BREAKDOWN: base * 1.30
        }
    
    def get_emergency_exits(self) -> Dict[float, float]:
        """긴급 청산 설정"""
        return {
            self.getfloat('EMERGENCY_EXIT', 'full_exit_loss', fallback=-5.0): 
                self.getfloat('EMERGENCY_EXIT', 'full_exit_ratio', fallback=1.0),
            self.getfloat('EMERGENCY_EXIT', 'partial_70_loss', fallback=-4.0): 
                self.getfloat('EMERGENCY_EXIT', 'partial_70_ratio', fallback=0.7),
            self.getfloat('EMERGENCY_EXIT', 'partial_50_loss', fallback=-2.5): 
                self.getfloat('EMERGENCY_EXIT', 'partial_50_ratio', fallback=0.5)
        }
    
    def get_whipsaw_config(self) -> Dict:
        """Whipsaw 설정"""
        return {
            'enabled': self.cached_values['enable_whipsaw_detection'],
            'lookback_candles': self.getint('WHIPSAW_DETECTION', 'lookback_candles', fallback=10),
            'min_direction_changes': self.getint('WHIPSAW_DETECTION', 'min_direction_changes', fallback=5),
            'volatility_threshold_pct': self.getfloat('WHIPSAW_DETECTION', 'volatility_threshold_pct', fallback=2.5),
            'score_penalty': self.getfloat('WHIPSAW_DETECTION', 'score_penalty', fallback=0.4)
        }
    
    def get_db_paths(self) -> Dict[str, str]:
        """DB 경로"""
        return {
            'performance': self.get('DATABASE', 'performance_db_path', fallback="signal_performance.db")
        }
    
    def get_ma_exit_timeframes(self) -> List[str]:
        """MA 이탈 적용 타임프레임"""
        timeframes = self.get('MA_EXIT', 'applicable_timeframes', fallback='1m,3m,5m,10m,15m')
        return [t.strip() for t in timeframes.split(',')]
    
    def get_partial_sell_config(self) -> Tuple[List[float], List[float]]:
        """부분 매도 설정"""
        if not self.getboolean('RISK_MANAGEMENT', 'enable_partial_sell', fallback=True):
            return [], []
        
        ratios_str = self.get('RISK_MANAGEMENT', 'partial_sell_ratios', fallback='0.3,0.3,0.4')
        triggers_str = self.get('RISK_MANAGEMENT', 'partial_sell_triggers', fallback='1.0,1.5,2.5')
        
        return ([float(r) for r in ratios_str.split(',')], 
                [float(t) for t in triggers_str.split(',')])
        
# **END**