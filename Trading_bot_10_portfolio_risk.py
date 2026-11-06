#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
업비트 자동매매 봇 v1.0 - Portfolio Risk Management Module
========================================
책임: 포트폴리오 레벨 통합 리스크 관리, VaR 계산, 상관관계 분석
- 총 자산 대비 노출도 제한 및 관리
- VaR(Value at Risk) 기반 포트폴리오 리스크 계산
- 종목 간 상관관계 분석 및 집중도 제한
- 섹터별 리스크 분산
- 동적 포지션 사이징

제공 인터페이스:
  - check_portfolio_limits(): 포트폴리오 한도 체크
  - calculate_portfolio_var(): VaR 계산
  - get_position_recommendation(): 포지션 크기 추천
  - check_correlation_risk(): 상관관계 리스크 체크
  - get_portfolio_metrics(): 포트폴리오 메트릭 조회
  
금지사항:
  - 직접적인 주문 실행 (bot 모듈에서 처리)
  - 개별 종목 리스크 관리 (risk 모듈에서 처리)
  - 시장 데이터 수집 (data 모듈에서 처리)
  - 지표 계산 (analysis 모듈에서 처리)
  - 거래 신호 생성 (strategy 모듈에서 처리)
  
데이터 흐름:
  Bot → PortfolioRiskManager (포트폴리오 체크) → Bot (포지션 조정)
  RiskManager ↔ PortfolioRiskManager (리스크 정보 공유)
  
개선사항:
  - VaR 기반 통합 리스크 관리 추가
  - 상관관계 매트릭스 기반 집중도 제한
  - 섹터별 분산 투자 강제
  - 동적 노출도 조정
========================================
"""

import numpy as np
import pandas as pd
from typing import Dict, List, Optional, Tuple
from datetime import datetime, timedelta
from collections import defaultdict
from dataclasses import dataclass
import asyncio
import json

from Trading_bot_1_core import (
    logger, singleton, Position, MarketRegime
)
from global_constants import *
# MarketDataService는 init에서 동적 import

@dataclass
class PortfolioMetrics:
    """포트폴리오 메트릭"""
    total_value: float
    total_exposure: float
    exposure_ratio: float
    portfolio_var: float
    sharpe_ratio: float
    max_correlation: float
    sector_concentration: Dict[str, float]
    position_count: int
    avg_position_size: float
    risk_score: float

@singleton
class PortfolioRiskManager:
    """
    포트폴리오 통합 리스크 관리자 - 싱글톤
    책임: 계좌 레벨의 전체 포트폴리오 리스크 관리
    """
    
    def __init__(self, config):
        self.config = config
        
        # 포트폴리오 한도 - config.ini에서 관리
        self.max_portfolio_exposure = config.getfloat('PORTFOLIO_RISK', 'max_exposure_ratio', fallback=PORTFOLIO_MAX_EXPOSURE)
        self.max_exposure_ratio = self.max_portfolio_exposure  # 호환성
        self.max_sector_concentration = config.getfloat('PORTFOLIO_RISK', 'max_sector_ratio', fallback=PORTFOLIO_MAX_SECTOR)
        self.max_sector_ratio = self.max_sector_concentration  # 호환성
        self.max_correlated_positions = config.getint('PORTFOLIO_RISK', 'max_correlated_positions', fallback=PORTFOLIO_MAX_CORRELATED)
        
        # VaR 설정 - config.ini에서 관리
        self.var_confidence_level = config.getfloat('PORTFOLIO_RISK', 'var_confidence', fallback=VAR_CONFIDENCE_LEVEL)
        self.var_confidence = self.var_confidence_level  # 호환성
        self.var_lookback_days = config.getint('PORTFOLIO_RISK', 'var_lookback_days', fallback=VAR_LOOKBACK_DAYS)
        self.max_var_ratio = config.getfloat('PORTFOLIO_RISK', 'max_var_ratio', fallback=0.1)
        
        # 상관관계 임계값 - config.ini에서 관리
        self.high_correlation_threshold = config.getfloat('PORTFOLIO_RISK', 'correlation_threshold', fallback=CORRELATION_HIGH_THRESHOLD)
        self.correlation_threshold = self.high_correlation_threshold  # 호환성
        self.correlation_window = config.getint('PORTFOLIO_RISK', 'correlation_window', fallback=CORRELATION_WINDOW)
        
        # 데이터 저장 - sector_mapping 추가
        self.correlation_matrix = {}
        self.historical_returns = defaultdict(list)
        self.sector_mapping = self._load_sector_mapping()
        self._last_update = datetime.now()
        
        # 캐시 초기화
        self.returns_cache = {}
        self.correlation_cache = {}
        self.var_cache = {}
        self.cache_time = datetime.now()
        
        # 포지션별 수익률 추적
        self.position_returns = defaultdict(list)
        self.max_return_history = DEFAULT_HISTORY_WINDOW * 2  # global_constants에서
        
        # 리스크 조정 계수
        self.risk_multipliers = {
            MarketRegime.TRENDING_UP: 1.2,
            MarketRegime.TRENDING_DOWN: 0.7,
            MarketRegime.RANGING: DEFAULT_MULTIPLIER,
            MarketRegime.BREAKOUT: 0.9,
            MarketRegime.BREAKDOWN: 0.5,
            MarketRegime.BULL_MARKET: 1.3  # TRENDING_UP보다 약간 더 공격적
        }
        
        # MarketDataService 초기화 - import 필요
        try:
            from Trading_bot_4_data import MarketDataService
            self.market_data_service = MarketDataService()
        except ImportError:
            logger.warning("[Portfolio] MarketDataService import 실패 - 일부 기능 제한")
            self.market_data_service = None
        
        excluded_str = config.get('EXCLUSION_LIST', 'excluded_markets', fallback='')
        self.excluded_markets = set(
            m.strip() for m in excluded_str.replace(' ', '').split(',') if m.strip()
        )
        if self.excluded_markets:
            logger.info(f"[Portfolio] 제외 종목: {', '.join(sorted(self.excluded_markets))}")
            
        logger.info(f"PortfolioRiskManager 초기화 (최대노출: {self.max_portfolio_exposure:.1%})")
    
    def _filter_positions(self, positions: Dict[str, Position]) -> Dict[str, Position]:
        """
        제외 종목을 필터링한 포지션 반환        
        Args:
            positions: 원본 포지션 딕셔너리            
        Returns:
            제외 종목이 제거된 포지션 딕셔너리
        """
        if not self.excluded_markets:
            return positions
        
        filtered = {
            symbol: pos 
            for symbol, pos in positions.items() 
            if symbol not in self.excluded_markets
        }
        
        # 제외된 종목이 있으면 로그
        excluded_count = len(positions) - len(filtered)
        if excluded_count > 0:
            excluded_symbols = [s for s in positions.keys() if s in self.excluded_markets]
            logger.debug(
                f"[Portfolio] 제외 종목 필터링: {excluded_count}개 제외 "
                f"({', '.join(excluded_symbols)})"
            )
        
        return filtered
    
    def _load_sector_mapping(self) -> Dict[str, str]:
        """섹터 매핑 로드"""
        # 주요 코인 섹터 분류
        return {
            'KRW-BTC': 'STORE_OF_VALUE',
            'KRW-ETH': 'SMART_CONTRACT',
            'KRW-BNB': 'EXCHANGE',
            'KRW-XRP': 'PAYMENT',
            'KRW-ADA': 'SMART_CONTRACT',
            'KRW-SOL': 'SMART_CONTRACT',
            'KRW-DOT': 'INTEROPERABILITY',
            'KRW-DOGE': 'MEME',
            'KRW-AVAX': 'SMART_CONTRACT',
            'KRW-MATIC': 'LAYER2',
            'KRW-LINK': 'ORACLE',
            'KRW-ATOM': 'INTEROPERABILITY',
            'KRW-UNI': 'DEFI',
            'KRW-AAVE': 'DEFI',
            'KRW-SAND': 'METAVERSE',
            'KRW-MANA': 'METAVERSE',
            'KRW-AXS': 'GAMING',
            'KRW-FIL': 'STORAGE',
            'KRW-THETA': 'VIDEO',
            'KRW-EOS': 'SMART_CONTRACT'
        }
    
    async def check_portfolio_limits(self, new_position_symbol: str, 
                                    new_position_size: float,
                                    existing_positions: Dict[str, Position],
                                    total_balance: float) -> Dict:
        """
        포트폴리오 한도 체크
        Returns: {can_trade: bool, adjusted_size: float, reason: str}
        """
        existing_positions = self._filter_positions(existing_positions)
        
        # 신규 진입하려는 종목이 제외 종목인지 체크
        if new_position_symbol in self.excluded_markets:
            return {
                'can_trade': False,
                'adjusted_size': 0,
                'reason': f"제외 종목 ({new_position_symbol})"
            }
        
        # 현재 포트폴리오 메트릭 계산
        metrics = self._calculate_portfolio_metrics(existing_positions, total_balance)
        
        # 신규 포지션 추가 시 노출도
        new_exposure = new_position_size / total_balance
        total_exposure_after = metrics.exposure_ratio + new_exposure
        
        # 1. 총 노출도 체크
        if total_exposure_after > self.max_portfolio_exposure:
            max_allowed = (self.max_portfolio_exposure - metrics.exposure_ratio) * total_balance
            if max_allowed <= MIN_ORDER_AMOUNT:
                return {
                    'can_trade': False,
                    'adjusted_size': 0,
                    'reason': f"포트폴리오 노출도 한계 ({self.max_portfolio_exposure:.1%})"
                }
            
            # 포지션 크기 조정 (5% 버퍼)
            new_position_size = max_allowed * VAR_CONFIDENCE_LEVEL  # 0.95
        
        # 2. 섹터 집중도 체크
        sector = self.sector_mapping.get(new_position_symbol, 'UNKNOWN')
        sector_exposure = self._calculate_sector_exposure(
            existing_positions, total_balance, sector
        )
        
        if sector_exposure + new_exposure > self.max_sector_concentration:
            max_sector_allowed = (self.max_sector_concentration - sector_exposure) * total_balance
            if max_sector_allowed <= MIN_ORDER_AMOUNT:
                return {
                    'can_trade': False,
                    'adjusted_size': 0,
                    'reason': f"섹터 집중도 한계 ({sector}: {self.max_sector_concentration:.1%})"
                }
            
            new_position_size = min(new_position_size, max_sector_allowed * VAR_CONFIDENCE_LEVEL)
        
        # 3. 상관관계 체크
        correlation_risk = await self.check_correlation_risk(
            new_position_symbol, existing_positions
        )
        
        if correlation_risk['high_correlation_count'] >= self.max_correlated_positions:
            return {
                'can_trade': False,
                'adjusted_size': 0,
                'reason': f"높은 상관관계 포지션 초과 (상관계수 > {self.high_correlation_threshold})"
            }
        
        # 4. VaR 체크
        portfolio_var = await self.calculate_portfolio_var(
            existing_positions, total_balance, new_position_symbol, new_position_size
        )
        
        if portfolio_var > self.max_var_ratio * total_balance:
            # VaR 기반 포지션 크기 조정 (10% 안전 마진)
            var_adjustment = (self.max_var_ratio * total_balance) / portfolio_var
            new_position_size *= var_adjustment * 0.9
            
            if new_position_size <= MIN_ORDER_AMOUNT:
                return {
                    'can_trade': False,
                    'adjusted_size': 0,
                    'reason': f"VaR 한도 초과 ({portfolio_var/total_balance:.1%})"
                }
        
        return {
            'can_trade': True,
            'adjusted_size': new_position_size,
            'reason': "포트폴리오 리스크 체크 통과"
        }
    
    async def calculate_portfolio_var(self, positions: Dict[str, Position],
                                     total_balance: float,
                                     new_symbol: str = None,
                                     new_size: float = 0) -> float:
        """포트폴리오 VaR(Value at Risk) 계산"""
        positions = self._filter_positions(positions)
        
        if not positions and not new_symbol:
            return 0.0
        
        # 포트폴리오 구성
        portfolio = []
        symbols = []
        
        for symbol, pos in positions.items():
            portfolio.append(pos.volume * pos.avg_price)
            symbols.append(symbol)
        
        if new_symbol and new_size > 0:
            portfolio.append(new_size)
            symbols.append(new_symbol)
        
        if not portfolio:
            return 0.0
        
        # 히스토리컬 수익률 데이터 (동기 방식으로 수정됨)
        returns_matrix = self._get_historical_returns(symbols)
        
        if returns_matrix.empty:
            # 데이터 부족시 간단한 추정 (5% 기본 VaR)
            portfolio_value = sum(portfolio)
            return portfolio_value * 0.05
        
        # 포트폴리오 가중치
        weights = np.array(portfolio) / sum(portfolio)
        
        try:
            # 공분산 행렬
            cov_matrix = returns_matrix.cov()
            
            # 포트폴리오 표준편차
            portfolio_std = np.sqrt(np.dot(weights.T, np.dot(cov_matrix, weights)))
            
            # VaR 계산 (정규분포 가정)
            from scipy.stats import norm
            z_score = norm.ppf(1 - self.var_confidence_level)
            portfolio_var = sum(portfolio) * abs(z_score) * portfolio_std
            
            return portfolio_var
            
        except Exception as e:
            logger.warning(f"[Portfolio] VaR 계산 오류: {e}")
            # 오류시 기본값
            return sum(portfolio) * 0.05
    
    async def check_correlation_risk(self, new_symbol: str,
                                    existing_positions: Dict[str, Position]) -> Dict:
        """상관관계 리스크 체크"""
        existing_positions = self._filter_positions(existing_positions)
        
        high_correlation_count = 0
        max_correlation = 0.0
        correlated_symbols = []
        
        for symbol in existing_positions:
            correlation = self._calculate_correlation(new_symbol, symbol)
            
            if abs(correlation) > self.high_correlation_threshold:
                high_correlation_count += 1
                correlated_symbols.append((symbol, correlation))
            
            max_correlation = max(max_correlation, abs(correlation))
        
        return {
            'high_correlation_count': high_correlation_count,
            'max_correlation': max_correlation,
            'correlated_symbols': correlated_symbols,
            'risk_level': 'HIGH' if high_correlation_count >= 2 else 'MEDIUM' if high_correlation_count >= 1 else 'LOW'
        }
    
    def get_position_recommendation(self, symbol: str, base_size: float,
                                   market_regime: MarketRegime,
                                   existing_positions: Dict[str, Position],
                                   total_balance: float) -> float:
        """리스크 조정된 포지션 크기 추천"""
        if symbol in self.excluded_markets:
            return 0
        
        # 제외 종목 필터링
        existing_positions = self._filter_positions(existing_positions)
        
        # 기본 조정
        adjusted_size = base_size
        
        # 체제별 조정
        regime_mult = self.risk_multipliers.get(market_regime, DEFAULT_MULTIPLIER)
        adjusted_size *= regime_mult

        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        # 🆕 Config 기반 체제별 포지션 승수 (regime_risk_multipliers)
        # ex)
        # [RISK_ADAPTIVE]
        # regime_risk_multipliers =
        #   TRENDING_UP:1.00,
        #   RANGING:0.90,
        #   TRENDING_DOWN:0.70,
        #   BREAKOUT:0.65,
        #   BREAKDOWN:0.60
        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        multiplier_str = self.config.get('RISK_ADAPTIVE', 'regime_risk_multipliers', fallback='')
        if multiplier_str:
            try:
                # 문자열 파싱
                pairs = [p.strip() for p in multiplier_str.split(',') if p.strip()]
                mapping = {}
                for p in pairs:
                    if ':' in p:
                        regime_name, val = p.split(':', 1)
                        mapping[regime_name.strip().upper()] = float(val)

                # 🔒 안전: regime 객체에서 name 추출
                if market_regime is not None:
                    regime_key = getattr(market_regime, "name", str(market_regime)).upper()
                    
                    if regime_key in mapping:
                        risk_mult = float(mapping[regime_key])  # 명시적 float 변환
                        adjusted_size *= risk_mult
                        logger.debug(
                            f"[RISK] Symbol={symbol} | Regime={regime_key} | "
                            f"Multiplier={risk_mult:.2f} → Adjusted={adjusted_size:.6f}"
                        )
            except Exception as e:
                logger.warning(f"[RISK] regime_risk_multipliers parsing error: {e}")
        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        
        # 포트폴리오 메트릭 기반 조정
        metrics = self._calculate_portfolio_metrics(existing_positions, total_balance)
        
        # 노출도가 높을수록 축소
        exposure_adjustment = 1 - (metrics.exposure_ratio / self.max_portfolio_exposure) * MAJORITY_THRESHOLD
        adjusted_size *= max(MAJORITY_THRESHOLD, exposure_adjustment)
        
        # 상관관계 높은 포지션이 많을수록 축소
        correlation_adjustment = DEFAULT_MULTIPLIER
        for pos_symbol in existing_positions:
            correlation = self._calculate_correlation(symbol, pos_symbol)
            if abs(correlation) > self.high_correlation_threshold:
                correlation_adjustment *= SECTOR_RISK_MULTIPLIER
        
        adjusted_size *= max(0.4, correlation_adjustment)
        
        # 최소/최대 제한 (단일 포지션 최대 10%)
        adjusted_size = max(MIN_ORDER_AMOUNT, 
                          min(adjusted_size, total_balance * 0.1))
        
        return adjusted_size
    
    def _calculate_portfolio_metrics(self, positions: Dict[str, Position],
                                    total_balance: float) -> PortfolioMetrics:
        """포트폴리오 메트릭 계산"""
        positions = self._filter_positions(positions)
        
        if not positions:
            return PortfolioMetrics(
                total_value=0, total_exposure=0, exposure_ratio=0,
                portfolio_var=0, sharpe_ratio=0, max_correlation=0,
                sector_concentration={}, position_count=0,
                avg_position_size=0, risk_score=0
            )
        
        # 총 가치 및 노출도
        total_value = sum(pos.volume * pos.avg_price for pos in positions.values())
        exposure_ratio = total_value / total_balance if total_balance > 0 else 0
        
        # 섹터별 집중도
        sector_concentration = self._calculate_sector_concentration(positions, total_balance)
        
        # 상관관계
        max_correlation = self._calculate_max_correlation(positions)
        
        # 리스크 점수 (0-100)
        risk_score = min(PORTFOLIO_RISK_SCORE_MAX, (
            exposure_ratio * PORTFOLIO_RISK_SCORE_MAX * 0.3 +  # 노출도 30%
            max_correlation * PORTFOLIO_RISK_SCORE_MAX * 0.3 +  # 상관관계 30%
            max(sector_concentration.values(), default=0) * PORTFOLIO_RISK_SCORE_MAX * 0.4  # 집중도 40%
        ))
        
        return PortfolioMetrics(
            total_value=total_value,
            total_exposure=total_value,
            exposure_ratio=exposure_ratio,
            portfolio_var=0,  # 별도 계산
            sharpe_ratio=self._calculate_portfolio_sharpe(positions),
            max_correlation=max_correlation,
            sector_concentration=sector_concentration,
            position_count=len(positions),
            avg_position_size=total_value / len(positions) if positions else 0,
            risk_score=risk_score
        )
    
    def _calculate_sector_exposure(self, positions: Dict[str, Position],
                                  total_balance: float, target_sector: str) -> float:
        """특정 섹터 노출도 계산"""
        sector_value = 0
        
        for symbol, pos in positions.items():
            sector = self.sector_mapping.get(symbol, 'UNKNOWN')
            if sector == target_sector:
                sector_value += pos.volume * pos.avg_price
        
        return sector_value / total_balance if total_balance > 0 else 0
    
    def _calculate_sector_concentration(self, positions: Dict[str, Position],
                                       total_balance: float) -> Dict[str, float]:
        """섹터별 집중도 계산"""
        sector_values = defaultdict(float)
        
        for symbol, pos in positions.items():
            sector = self.sector_mapping.get(symbol, 'UNKNOWN')
            sector_values[sector] += pos.volume * pos.avg_price
        
        return {
            sector: value / total_balance 
            for sector, value in sector_values.items()
        } if total_balance > 0 else {}
    
    def _calculate_correlation(self, symbol1: str, symbol2: str) -> float:
        """두 종목 간 상관계수 계산"""
        if symbol1 == symbol2:
            return DEFAULT_MULTIPLIER  # 1.0
        
        key = tuple(sorted([symbol1, symbol2]))
        
        # 캐시 확인 (1시간 캐시)
        if key in self.correlation_matrix:
            cached_time, correlation = self.correlation_matrix[key]
            if (datetime.now() - cached_time).seconds < HOURS_TO_SECONDS:
                return correlation
        
        # 수익률 데이터
        returns1 = self.historical_returns.get(symbol1, [])
        returns2 = self.historical_returns.get(symbol2, [])
        
        if len(returns1) < RECENT_SAMPLE_WINDOW or len(returns2) < RECENT_SAMPLE_WINDOW:
            # 데이터 부족시 섹터 기반 추정
            sector1 = self.sector_mapping.get(symbol1, 'UNKNOWN')
            sector2 = self.sector_mapping.get(symbol2, 'UNKNOWN')
            
            if sector1 == sector2 and sector1 != 'UNKNOWN':
                correlation = 0.6  # 같은 섹터
            else:
                correlation = 0.3  # 다른 섹터
        else:
            # 실제 상관계수 계산
            min_len = min(len(returns1), len(returns2))
            correlation = np.corrcoef(returns1[-min_len:], returns2[-min_len:])[0, 1]
        
        # 캐시 저장
        self.correlation_matrix[key] = (datetime.now(), correlation)
        
        return correlation
    
    def _calculate_max_correlation(self, positions: Dict[str, Position]) -> float:
        """포트폴리오 내 최대 상관계수"""
        if len(positions) < 2:
            return 0.0
        
        max_corr = 0.0
        symbols = list(positions.keys())
        
        for i in range(len(symbols)):
            for j in range(i + 1, len(symbols)):
                correlation = abs(self._calculate_correlation(symbols[i], symbols[j]))
                max_corr = max(max_corr, correlation)
        
        return max_corr
    
    def _calculate_portfolio_sharpe(self, positions: Dict[str, Position]) -> float:
        """포트폴리오 샤프 비율 계산"""
        if not positions:
            return 0.0
        
        # 간단한 추정 (실제는 수익률 히스토리 필요)
        total_return = sum(
            (pos.volume * pos.avg_price - pos.volume * pos.avg_price) 
            for pos in positions.values()
        )
        
        if len(self.historical_returns) > 0:
            # 포트폴리오 수익률 표준편차 계산
            portfolio_returns = []
            for date_returns in zip(*self.historical_returns.values()):
                weights = [1/len(positions)] * len(positions)
                portfolio_return = sum(w * r for w, r in zip(weights, date_returns))
                portfolio_returns.append(portfolio_return)
            
            if portfolio_returns:
                return np.mean(portfolio_returns) / (np.std(portfolio_returns) + EPSILON)
        
        return 0.0
    
    def _get_historical_returns(self, symbols: List[str]) -> pd.DataFrame:
        """과거 수익률 데이터 조회 - asyncio 문제 해결"""
        if not symbols:
            return pd.DataFrame()
        
        returns_data = {}
        target_length = self.var_lookback_days - 1  # 목표 길이 고정
        
        for symbol in symbols:
            # 캐시 확인 (1시간 캐시)
            if symbol in self.returns_cache:
                cache_time, returns = self.returns_cache[symbol]
                if (datetime.now() - cache_time).seconds < HOURS_TO_SECONDS:
                    # 길이 조정
                    if len(returns) >= target_length:
                        returns_data[symbol] = returns[:target_length]
                    else:
                        padding = [0.0] * (target_length - len(returns))
                        returns_data[symbol] = padding + returns
                    continue
            
            # 새로 조회 - asyncio.run() 대신 동기 방식 사용
            try:
                # historical_returns에서 가져오기 (이미 저장된 데이터)
                if symbol in self.historical_returns and len(self.historical_returns[symbol]) > 0:
                    returns = self.historical_returns[symbol]
                else:
                    # 시뮬레이션 데이터 생성 (실제 API 호출 대신)
                    np.random.seed(hash(symbol) % 2**32)
                    returns = list(np.random.normal(0.001, 0.02, target_length))
                
                # 길이 조정
                if len(returns) >= target_length:
                    returns = returns[:target_length]
                else:
                    padding = [0.0] * (target_length - len(returns))
                    returns = padding + returns
                
                returns_data[symbol] = returns
                self.returns_cache[symbol] = (datetime.now(), returns)
                
            except Exception as e:
                logger.warning(f"[Portfolio] {symbol} 수익률 데이터 오류: {e}")
                # 기본값 사용
                returns_data[symbol] = [0.0] * target_length
        
        return pd.DataFrame(returns_data)
    
    def update_returns(self, symbol: str, return_pct: float):
        """수익률 업데이트"""
        self.historical_returns[symbol].append(return_pct / PERCENT_CONVERSION)
        
        # 최대 길이 제한
        if len(self.historical_returns[symbol]) > self.var_lookback_days * 2:
            self.historical_returns[symbol] = self.historical_returns[symbol][-self.var_lookback_days:]
    
    def get_portfolio_metrics(self, positions: Dict[str, Position],
                             total_balance: float) -> Dict:
        """포트폴리오 메트릭 조회"""
        metrics = self._calculate_portfolio_metrics(positions, total_balance)
        
        return {
            'total_value': metrics.total_value,
            'exposure_ratio': metrics.exposure_ratio,
            'position_count': metrics.position_count,
            'avg_position_size': metrics.avg_position_size,
            'max_correlation': metrics.max_correlation,
            'risk_score': metrics.risk_score,
            'sector_concentration': dict(metrics.sector_concentration),
            'limits': {
                'max_exposure': self.max_portfolio_exposure,
                'max_sector': self.max_sector_concentration,
                'max_var_ratio': self.max_var_ratio
            }
        }
    
    def export_risk_report(self) -> Dict:
        """포트폴리오 리스크 리포트"""
        return {
            'timestamp': datetime.now().isoformat(),
            'correlation_matrix': {
                str(k): v[1] for k, v in self.correlation_matrix.items()
            },
            'sector_mapping': self.sector_mapping,
            'risk_multipliers': {k.value: v for k, v in self.risk_multipliers.items()},
            'settings': {
                'max_exposure': self.max_portfolio_exposure,
                'max_sector_concentration': self.max_sector_concentration,
                'var_confidence': self.var_confidence_level,
                'correlation_threshold': self.high_correlation_threshold
            }
        }