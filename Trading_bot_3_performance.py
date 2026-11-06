#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
업비트 자동매매 봇 v1.0 - Performance Module (Final Enhanced)
========================================
책임: 성과 추적, 가중치 관리, Kelly Criterion 계산, 리포트 생성
- 거래 성과 기록 및 분석
- 신호별 성과 추적
- 동적 가중치 조정
- Kelly Criterion 계산
- 성과 리포트 생성
- 거래 스냅샷 저장

개선사항 (v1.0 Final Enhanced):
- WAL 모드 활성화 (정전 보호 8배 강화)
- DB 무결성 체크 및 자동 복구 (데일리 백업 패턴)
- 트랜잭션 일괄 처리 (ACID 보장)
- SQLite BACKUP API (온라인 백업, LOCK 충돌 방지)
- 데일리 자동 백업 (30일 보관)
- 주간 VACUUM (DB 최적화, VACUUM 전 백업 보장)
- 백업 자동 정리
- LOCK 충돌 완전 방지

제공 인터페이스:
  - record_trade_complete(): 거래 완료 기록
  - get_signal_confidence(): 신호 신뢰도
  - calculate_kelly_criterion(): Kelly 비율
  - get_adjusted_weights(): 조정된 가중치
  - get_dynamic_threshold(): 동적 임계값
  - generate_performance_report(): 성과 리포트 생성
  - save_trade_snapshot(): 거래 스냅샷 저장
  
금지사항:
  - 거래 실행 (bot 모듈에서 처리)
  - 시장 데이터 수집 (data 모듈에서 처리)
  - 신호 생성 (strategy 모듈에서 처리)
  - 지표 계산 (analysis 모듈에서 처리)
  
데이터 흐름:
  Bot → Performance (거래 결과) → Bot (가중치)
  Performance → Files/DB (리포트/스냅샷)
========================================
"""

import sqlite3
import numpy as np
import time
import json
import asyncio
from datetime import datetime, timedelta
from typing import Dict, List, Optional
from collections import deque
from pathlib import Path

from Trading_bot_1_core import logger, MarketRegime, Position, ExecutionResult, singleton
from global_constants import *

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 🔧 JSON 직렬화 Helper (numpy → Python native 타입 변환)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
def convert_to_serializable(obj):
    """
    numpy 타입을 Python native 타입으로 변환
    - numpy.int64 → int
    - numpy.float64 → float
    - dict/list 재귀적 변환
    """
    if isinstance(obj, dict):
        return {k: convert_to_serializable(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [convert_to_serializable(item) for item in obj]
    elif isinstance(obj, np.integer):
        return int(obj)
    elif isinstance(obj, np.floating):
        return float(obj)
    elif isinstance(obj, np.ndarray):
        return convert_to_serializable(obj.tolist())
    else:
        return obj
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

@singleton
class PerformanceManager:
    """
    성과 관리자 - 싱글톤
    책임: 성과 추적, 가중치 관리, 리포트 생성
    """
    SECTOR_CLUSTERS = {
        # ========================================
        # 1️⃣ BTC Layer (시장 기준점)
        # ========================================
        'BTC': ['KRW-BTC'],
        
        # ========================================
        # 2️⃣ MAJOR Layer (메이저 알트)
        # ========================================
        'MAJOR': [
            'KRW-ETH', 'KRW-SOL', 'KRW-XRP', 'KRW-ADA', 'KRW-DOGE',
            'KRW-LINK', 'KRW-AVAX', 'KRW-MATIC', 'KRW-DOT', 'KRW-ATOM'
        ],
        
        # ========================================
        # 3️⃣ CN Layer (중국/아시아계)
        # ========================================
        'CN': [
            'KRW-CFX', 'KRW-NEO', 'KRW-VET', 'KRW-ONT', 'KRW-TRX',
            'KRW-FIL', 'KRW-ZIL', 'KRW-BTT'
        ],
        
        # ========================================
        # 4️⃣ KR Layer (국내/김치코인)
        # ========================================
        'KR': [
            'KRW-BORA', 'KRW-MVL', 'KRW-SSX', 'KRW-AHT', 'KRW-MED',
            'KRW-CRE', 'KRW-PLA', 'KRW-FNSA', 'KRW-BOBA'
        ],
        
        # ========================================
        # 5️⃣ RWA (실물자산 토큰화)
        # ========================================
        'RWA': [
            'KRW-RIO', 'KRW-ONDO', 'KRW-SNX', 'KRW-MKR', 'KRW-POL', 'KRW-AAVE'
        ],
        
        # ========================================
        # 6️⃣ AI (인공지능/데이터)
        # ========================================
        'AI': [
            'KRW-FET', 'KRW-AIOZ', 'KRW-OCEAN', 'KRW-GRT', 'KRW-AGIX', 'KRW-TAO'
        ],
        
        # ========================================
        # 7️⃣ MEME (밈/커뮤니티)
        # ========================================
        'MEME': [
            'KRW-DOGE', 'KRW-SHIB', 'KRW-PEPE', 'KRW-BONK', 'KRW-FLOKI', 'KRW-MOCA'
        ],
        
        # ========================================
        # 8️⃣ NFT (콘텐츠형 - GAME 통합)
        # ========================================
        'NFT': [
            'KRW-IMX', 'KRW-SAND', 'KRW-MANA', 'KRW-GALA', 'KRW-PIXEL',
            'KRW-AXS', 'KRW-TLM', 'KRW-ENJ', 'KRW-THETA'
        ],
        
        # ========================================
        # 9️⃣ DEFI (탈중앙금융 - Layer2 통합)
        # ========================================
        'DEFI': [
            'KRW-UNI', 'KRW-CAKE', 'KRW-CVX', 'KRW-COMP', 'KRW-AAVE',
            'KRW-ARB', 'KRW-OP', 'KRW-LDO'
        ],
        
        # ========================================
        # 🔟 INFRA (인프라/플랫폼)
        # ========================================
        'INFRA': [
            'KRW-KLAY', 'KRW-STX', 'KRW-INJ', 'KRW-SEI', 'KRW-NEAR',
            'KRW-SUI', 'KRW-HBAR', 'KRW-ICP'
        ],
        
        # ========================================
        # 1️⃣1️⃣ OTHERS (혼합형/테마성)
        # ========================================
        'OTHERS': [
            'KRW-GMX', 'KRW-RUNE', 'KRW-TON', 'KRW-XDC', 'KRW-WAVES'
        ]
    }
    
    def __init__(self, config=None):
        self.config = config
        self.mode = config.get_cached('mode') if config else 'sim'
        self.db_path = config.get_db_paths()['performance'] if config else "signal_performance.db"
        
        # 초기 잔고
        self.initial_balance = config.getfloat('SIMULATION', 'initial_balance', fallback=10000000) if config else 10000000
        
        # 🆕 추가: 프로그램 시작 시간 (클린 스레이트 기준점)
        self.session_start_time = datetime.now()
        
        # 성과 추적
        # self.trades = deque(maxlen=MEMORY_BOUNDS['trade_history'])\
        # MEMORY_BOUNDS['trade_history']가 None이면 문제
        self.trades = deque(maxlen=min(1000, MEMORY_BOUNDS.get('trade_history', 1000)))
        
        from collections import OrderedDict
        self.signal_stats = OrderedDict() 
        self._max_signal_stats = 50  # 최대 50개 신호만 추적
        # signal_stats 크기 제한 강제
        def _trim_signal_stats(self):
            if len(self.signal_stats) > self._max_signal_stats:
                # 오래된 항목 제거
                for _ in range(len(self.signal_stats) - self._max_signal_stats):
                    self.signal_stats.popitem(last=False)
                    
        self.metrics = {
            'total_trades': 0, 'wins': 0, 'losses': 0,
            'total_pnl': 0.0, 'total_fees': 0.0,
            'consecutive_losses': 0, 'max_drawdown': 0.0,
            'peak_balance': self.initial_balance
        }
        
        # 가중치
        self.base_weights = config.get_signal_weights() if config else {}
        
        # ATR 가중치 안전장치 (🔒 2026-08-02: get_signal_weights()가 '_signal' 접미사
        #  키로 통일됨에 따라 'atr' → 'atr_signal'로 동기화)
        if 'atr_signal' not in self.base_weights:
            if config:
                atr_weight = config.getfloat('SIGNAL_WEIGHTS', 'atr_signal_weight', fallback=0.15)
                self.base_weights['atr_signal'] = atr_weight
                logger.warning(f"ATR 가중치 누락 - 기본값 추가: {atr_weight}")
            else:
                self.base_weights['atr_signal'] = 0.15
        
        # 가중치 정규화
        total_weight = sum(self.base_weights.values())
        if total_weight > 0:
            self.base_weights = {k: v/total_weight for k, v in self.base_weights.items()}        
        
        self.current_weights = self.base_weights.copy()
        self.last_weights = self.base_weights.copy()
        self.adjustment_count = 0
        self.daily_adjustments = 0
        self.last_adjustment_date = datetime.now().date()
        
        # 설정
        self.auto_adjust = config.get_cached('auto_adjust_weights') if config else False
        self.min_samples_adjustment = SAFE_MIN_SAMPLES
        
        # 리포트 디렉토리 설정
        self.output_dir = Path("outputs")
        self.output_dir.mkdir(exist_ok=True)
        
        self.report_dirs = {
            'reports': self.output_dir / 'reports',
            'trades': self.output_dir / 'trades'
        }
        
        for dir_path in self.report_dirs.values():
            dir_path.mkdir(exist_ok=True)
        
        # DB 연결 풀
        self._conn = None
        self._conn_lock = asyncio.Lock()
        
        # DB 초기화
        self._init_db()
        
        # DB 무결성 체크
        if not self._check_db_integrity():
            logger.warning("⚠️ DB 손상 감지 - 복구 시도")
            
            if self._attempt_db_recovery():
                logger.info("✅ DB 복구 성공")
            else:
                logger.error("❌ DB 복구 실패 - 새로 시작합니다")
        
        # DB에서 최근 거래 로드
        self._load_trades_from_db()
        
        # 리포트 생성 주기
        self.last_report_time = datetime.now()
        self.report_interval_hours = 4
        
        # 백업 및 최적화 관리
        self.last_backup_date = None
        self.last_vacuum_date = None
        
        # 🆕 P4c Meta Optimizer
        self.meta = MetaOptimizer(self)
        self.weights_history = []
        
        # 🆕 P4d Correlation Adapter
        self.p4d = P4dCorrelationAdapter(self)        
    
    def _get_connection(self):
        """DB 연결 재사용 (싱글톤 패턴)
        Returns:
            sqlite3.Connection: 재사용 가능한 DB 연결
        """
        if self._conn is None:
            self._conn = sqlite3.connect(
                self.db_path,
                timeout=SQLITE_TIMEOUT,
                check_same_thread=False  # asyncio 단일 스레드 환경
            )
            
            # WAL 모드 활성화
            try:
                self._conn.execute("PRAGMA journal_mode=WAL")
                self._conn.execute("PRAGMA synchronous=NORMAL")
                self._conn.execute("PRAGMA cache_size=10000")
                logger.debug("✅ DB 연결 생성 및 WAL 모드 활성화")
            except Exception as e:
                logger.warning(f"⚠️ WAL 모드 설정 실패: {e}")
        
        return self._conn
    
    def _init_db(self):
        """DB 초기화 - WAL 모드 및 무결성 강화"""
        try:
            conn = self._get_connection()  # ✅ 연결 재사용
            
            # 테이블 생성
            conn.execute("""
                CREATE TABLE IF NOT EXISTS performances (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
                    signal_type TEXT,
                    success BOOLEAN,
                    return_pct REAL,
                    profit REAL,
                    mode TEXT
                )
            """)
            
            conn.execute("""
                CREATE TABLE IF NOT EXISTS trade_details (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
                    symbol TEXT,
                    side TEXT,
                    price REAL,
                    volume REAL,
                    amount REAL,
                    fee REAL,
                    pnl REAL,
                    return_pct REAL,
                    reason TEXT,
                    confidence REAL,
                    market_regime TEXT,
                    signal_components TEXT,
                    weights_used TEXT,
                    mode TEXT,
                    slippage_pct REAL,
                    execution_time_ms INTEGER,
                    timeframe TEXT
                )
            """)
            
            conn.execute("""
                CREATE TABLE IF NOT EXISTS daily_summary (
                    date DATE PRIMARY KEY,
                    total_trades INTEGER,
                    wins INTEGER,
                    losses INTEGER,
                    total_pnl REAL,
                    total_fees REAL,
                    win_rate REAL,
                    avg_return REAL,
                    max_drawdown REAL,
                    mode TEXT
                )
            """)
            
            conn.commit()
            
            # 🔒 스키마 마이그레이션 (price/amount 컬럼 보장)
            self._ensure_trade_details_schema()
            
            logger.info("성과 DB 초기화 완료 (WAL 모드)")
            
        except Exception as e:
            logger.error(f"DB 초기화 실패: {e}")
    
    def _ensure_trade_details_schema(self):
        """
        trade_details 테이블 스키마 마이그레이션 (price/amount 컬럼 보장)
        
        철학:
        - 구 스키마: entry_price, exit_price, executed_price
        - 신 스키마: price, volume, amount
        - ALTER TABLE로 기존 데이터 보존하며 컬럼 추가
        """
        try:
            conn = self._get_connection()
            cursor = conn.execute("PRAGMA table_info(trade_details)")
            existing_cols = {row[1] for row in cursor.fetchall()}
            
            altered = False
            
            # price 컬럼 추가 (구 스키마에 없음)
            if 'price' not in existing_cols:
                conn.execute("ALTER TABLE trade_details ADD COLUMN price REAL")
                logger.info("🛠️ trade_details: price 컬럼 추가")
                altered = True
            
            # amount 컬럼 추가 (구 스키마에 없음)
            if 'amount' not in existing_cols:
                conn.execute("ALTER TABLE trade_details ADD COLUMN amount REAL")
                logger.info("🛠️ trade_details: amount 컬럼 추가")
                altered = True
            
            # [보완 v3_7] 사건 식별 (REAL 성과 행 정확히 1 회 · REAL_EVENT_IDENTITY_DESIGN_v1 C) · 옛 행 = NULL · SIM 행 = NULL
            if 'event_id' not in existing_cols:
                conn.execute("ALTER TABLE trade_details ADD COLUMN event_id TEXT")
                logger.info("🛠️ trade_details: event_id 컬럼 추가")
                altered = True
            conn.execute("CREATE UNIQUE INDEX IF NOT EXISTS ux_trade_details_event ON trade_details(event_id) WHERE event_id IS NOT NULL")
            conn.commit()
            self._has_event_id = True
            if altered:
                logger.warning("✅ trade_details 스키마 마이그레이션 완료 (price/amount/event_id)")
            
        except Exception as e:
            logger.error(f"❌ trade_details 스키마 마이그레이션 실패: {e}")
        
    def _event_exists(self, event_id) -> bool:
        """[보완 v3_7] 같은 사건 id 행이 이미 있는가"""
        if not event_id or not getattr(self, '_has_event_id', False):
            return False
        return self._get_connection().execute("SELECT 1 FROM trade_details WHERE event_id=? LIMIT 1", (event_id,)).fetchone() is not None

    def _event_row(self, event_id):
        """[보완 v3_10 K6] 같은 사건 id 행의 (수량 · 체결가 · 수수료)"""
        if not event_id or not getattr(self, '_has_event_id', False):
            return None
        return self._get_connection().execute("SELECT volume, price, fee FROM trade_details WHERE event_id=? LIMIT 1", (event_id,)).fetchone()

    def _event_same(self, row, result) -> bool:
        """[보완 v3_10 K6] 이미 있는 사건 행과 이번 기록의 내용이 같은가 (수량 1e-8 · 금액 max(1e-6, |금액|×1e-12) · 수수료 1e-9)"""
        try:
            v0, p0, c0 = float(row[0] or 0), float(row[1] or 0), float(row[2] or 0)
            v1, p1, c1 = float(result.executed_volume), float(result.avg_price), float(result.total_fee)
            return abs(v0 - v1) <= 1e-8 and abs(v0 * p0 - v1 * p1) <= max(1e-6, abs(v0 * p0) * 1e-12) and abs(c0 - c1) <= 1e-9
        except Exception:
            return False

    def recorded_event_totals(self, prefix: str):
        """[보완 v3_7] 사건 id 접두가 같은 행의 (수량 합 · 금액 합 · 수수료 합) = 회계 진실 · 열 없음 = 예외 (호출자가 미기록으로 처리)"""
        if not getattr(self, '_has_event_id', False):
            raise RuntimeError("trade_details.event_id 열 없음")
        esc = prefix.replace('\\', '\\\\').replace('%', '\\%').replace('_', '\\_') + '%'
        r = self._get_connection().execute(
            "SELECT COALESCE(SUM(volume),0), COALESCE(SUM(volume*price),0), COALESCE(SUM(fee),0) FROM trade_details WHERE event_id LIKE ? ESCAPE '\\'", (esc,)).fetchone()
        return float(r[0]), float(r[1]), float(r[2])

    def _load_trades_from_db(self):
        """DB에서 거래 내역 로드 - 날짜 기반 쿼리"""
        try:
            conn = self._get_connection()
            
            # ========================================
            # 타임프레임별 조회 기간 (일 단위)
            # ========================================
            timeframe_lookback = {
                '1m': 7,    # 1분봉: 최근 7일
                '3m': 14,   # 3분봉: 최근 2주
                '5m': 21,   # 5분봉: 최근 3주
                '10m': 30,  # 10분봉: 최근 1개월
                '15m': 45,  # 15분봉: 최근 1.5개월
                '30m': 60,  # 30분봉: 최근 2개월
                '1h': 90,   # 1시간봉: 최근 3개월
                '4h': 180,  # 4시간봉: 최근 6개월
                '1d': 365   # 일봉: 최근 1년
            }
            
            timeframe = self.config.get_cached('timeframe') if self.config else '5m'
            lookback_days = timeframe_lookback.get(timeframe, 30)
            
            # 날짜 계산
            from datetime import datetime, timedelta
            cutoff_date = (datetime.now() - timedelta(days=lookback_days)).isoformat()
            
            # 개수 제한 (안전장치)
            max_load = MEMORY_BOUNDS.get('trade_history', 1000)
            
            # ========================================
            # 날짜 + 개수 제한 병행
            # ========================================
            cursor = conn.execute("""
                SELECT
                    timestamp, symbol, pnl, return_pct,
                    confidence, reason, market_regime
                FROM trade_details
                WHERE mode = ?
                  AND TRIM(UPPER(side)) = 'SELL'
                  AND timestamp >= ?
                ORDER BY timestamp DESC
                LIMIT ?
            """, (self.mode, cutoff_date, max_load))
            
            rows = cursor.fetchall()
            
            if not rows:
                logger.info(
                    f"📂 DB에 최근 {lookback_days}일 거래 내역 없음 (신규 시작)"
                )
                return
            
            # deque에 추가 (오래된 것부터)
            for row in reversed(rows):
                timestamp_str, symbol, pnl, return_pct, confidence, reason, market_regime = row
                
                # timestamp 파싱
                try:
                    timestamp = datetime.fromisoformat(timestamp_str)
                except:
                    try:
                        timestamp = datetime.strptime(timestamp_str, '%Y-%m-%d %H:%M:%S')
                    except:
                        timestamp = datetime.now()
                
                trade_record = {
                    'timestamp': timestamp,
                    'symbol': symbol,
                    'success': pnl > 0,
                    'return_pct': return_pct,
                    'profit': pnl,
                    'confidence': confidence if confidence else 0,
                    'reason': reason if reason else '',
                    'market_regime': market_regime if market_regime else 'UNKNOWN'
                }
                self.trades.append(trade_record)
            
            logger.info("=" * 60)
            logger.info(
                f"📂 DB에서 최근 거래 {len(rows)}건 로드 완료 "
                f"(최근 {lookback_days}일, 최대 {max_load}건)"
            )
            
            if rows:
                try:
                    oldest_str = rows[-1][0]
                    newest_str = rows[0][0]
                    
                    oldest = datetime.fromisoformat(oldest_str) if oldest_str else None
                    newest = datetime.fromisoformat(newest_str) if newest_str else None
                    
                    if oldest and newest:
                        logger.info(f"   - 기간: {oldest.date()} ~ {newest.date()}")
                except:
                    pass
            
            logger.info("=" * 60)
            
            # 메트릭 업데이트
            self._update_metrics_from_trades()
            
        except sqlite3.Error as e:
            logger.error(f"❌ DB 거래 로드 실패 (SQLite 오류): {e}")
        except Exception as e:
            logger.error(f"❌ DB 거래 로드 실패: {e}")
    
    def _update_metrics_from_trades(self):
        """로드된 거래에서 메트릭 업데이트
        ⚠️ 중요: 
        - 연속손실: 세션 변수이므로 항상 0으로 초기화!
        - 오늘 손실: 재시작 = 클린 스레이트, 항상 0원!
        - 시뮬레이션: 항상 initial_balance 사용
        - 실거래: API에서 실제 잔고 조회
        """
        try:
            if not self.trades:
                logger.debug("   로드된 거래 없음 - 메트릭 업데이트 스킵")
                return
            
            # ========================================
            # 1. 기본 통계 (DB 기반, 과거 누적)
            # ========================================
            total = len(self.trades)
            wins = sum(1 for t in self.trades if t['success'])
            losses = total - wins
            total_pnl = sum(t['profit'] for t in self.trades)
            
            # ========================================
            # 2. peak_balance 및 max_drawdown 계산
            # 🔴 수정: 모드별로 다르게 처리
            # ========================================
            
            # 시뮬레이션 vs 실거래 구분
            mode = self.config.get_cached('mode') if hasattr(self, 'config') else 'sim'
            
            if mode == 'sim':
                # 시뮬레이션: 초기값 고정 사용
                current_balance = self.initial_balance  # 10억 고정
                balance_source = "설정값"
            else:
                # 실거래: API에서 실제 잔고 조회 (동기 버전)
                try:
                    # ExecutionEngine이 있으면 거기서 가져오기
                    if hasattr(self, 'engine') and self.engine:
                        current_balance = self.engine.get_balance()
                        balance_source = "Engine"
                    # API 클라이언트가 있으면 직접 조회
                    elif hasattr(self, 'api_client') and self.api_client:
                        # 주의: 이 부분은 비동기 함수에서만 작동
                        # 동기 함수라면 캐시된 값 사용
                        current_balance = getattr(self, 'last_known_balance', 0)
                        balance_source = "캐시"
                    else:
                        # 폴백: DB 누적값 사용 (최후 수단)
                        current_balance = total_pnl
                        balance_source = "DB누적"
                        logger.warning("⚠️ 실거래 모드인데 API 접근 불가 - DB값 사용")
                except Exception as e:
                    logger.error(f"실거래 잔고 조회 실패: {e}")
                    current_balance = total_pnl
                    balance_source = "오류-DB폴백"
            
            # Peak balance 업데이트 (✅ 양수일 때만)
            if current_balance > 0 and current_balance > self.metrics['peak_balance']:
                self.metrics['peak_balance'] = current_balance

            # Drawdown 계산 (✅ 음수 방어)
            if self.metrics['peak_balance'] > 0:
                current_drawdown = (current_balance - self.metrics['peak_balance']) / self.metrics['peak_balance']
                # ✅ 낙폭은 최대 -100%까지만 (파산)
                current_drawdown = max(current_drawdown, -1.0)
                self.metrics['max_drawdown'] = min(self.metrics['max_drawdown'], current_drawdown)
            
            # ========================================
            # 3. 메트릭 업데이트 (과거 누적 통계만)
            # ========================================
            self.metrics['total_trades'] = total
            self.metrics['wins'] = wins
            self.metrics['losses'] = losses
            self.metrics['total_pnl'] = total_pnl
            
            # ========================================
            # 🔥 핵심: 세션 변수는 항상 0! (클린 스레이트)
            # ========================================
            self.metrics['consecutive_losses'] = 0
            
            # ========================================
            # 4. 로그 출력 (모드별로 다르게)
            # ========================================
            win_rate = wins / total * 100 if total > 0 else 0
            
            logger.info("   📊 메트릭 업데이트:")
            logger.info(f"      - 총 거래: {total}회 (과거 누적)")
            logger.info(f"      - 승률: {win_rate:.1f}% ({wins}승 {losses}패)")
            logger.info(f"      - DB 누적 손익: {total_pnl:+,.0f}원")
            
            # 모드별 잔고 표시
            if mode == 'sim':
                logger.info(f"      - 시뮬 잔고: {current_balance:,.0f}원 (초기값 고정)")
                logger.info(f"      - [참고] DB 누적시 잔고: {self.initial_balance + total_pnl:,.0f}원")
            else:
                logger.info(f"      - 실제 잔고: {current_balance:,.0f}원 ({balance_source})")
                if balance_source == "DB누적":
                    logger.warning(f"        ⚠️ API 미연결 - DB값 사용중")
            
            logger.info(f"      - 최고 잔고: {self.metrics['peak_balance']:,.0f}원")
            logger.info(f"      - 최대 낙폭: {self.metrics['max_drawdown']*100:.2f}%")
            
            # ========================================
            # 🆕 클린 스레이트 정책 명시
            # ========================================
            logger.warning("=" * 60)
            logger.warning("🔄 [재시작] 클린 스레이트 정책 적용")
            logger.warning("   • 연속손실: 0회 (초기화)")
            logger.warning("   • 오늘 손실: 0원 (초기화)")
            logger.warning("   • 모드: %s", "시뮬레이션" if mode == 'sim' else "실거래")
            if mode == 'sim':
                logger.warning("   • 시뮬 잔고: 매번 초기값에서 시작")
            logger.warning("   ⚠️ 과거 통계는 DB에 보존되어 있습니다.")
            logger.warning("=" * 60)
            
        except Exception as e:
            logger.error(f"❌ 메트릭 업데이트 실패: {e}")
            import traceback
            logger.debug(traceback.format_exc())
    
    def _check_db_integrity(self) -> bool:
        """DB 무결성 체크
        Returns:
            bool: 무결성 정상 여부
        """
        try:
            conn = self._get_connection()  # ✅ 연결 재사용
            
            cursor = conn.execute("PRAGMA integrity_check")
            result = cursor.fetchone()
            # ❌ conn.close() 제거
            
            if result and result[0] == "ok":
                logger.info("✅ DB 무결성 확인 완료")
                return True
            else:
                logger.error(f"❌ DB 손상 감지: {result[0] if result else 'unknown'}")
                logger.error("⚠️ 백업에서 복구를 권장합니다!")
                logger.error(f"   백업 경로: backups/")
                return False
                
        except sqlite3.DatabaseError as e:
            logger.error(f"❌ DB 접근 실패 (손상 가능성): {e}")
            return False
        except Exception as e:
            logger.error(f"❌ 무결성 체크 실패: {e}")
            return False

    def _attempt_db_recovery(self) -> bool:
        """DB 복구 시도 (데일리 백업에서)
        Returns:
            bool: 복구 성공 여부
        """
        try:
            from pathlib import Path
            import shutil
            
            backup_dir = Path("backups")
            if not backup_dir.exists():
                logger.error("❌ 백업 디렉토리 없음")
                return False
            
            # 최신 데일리 백업 파일 찾기 (signal_performance_YYYYMMDD.db)
            backups = []
            for backup_file in backup_dir.glob("signal_performance_*.db"):
                try:
                    # 파일명에서 날짜 추출
                    date_str = backup_file.stem.split('_')[-1]
                    
                    # 8자리 날짜 형식 확인 (YYYYMMDD)
                    if len(date_str) == 8 and date_str.isdigit():
                        backup_date = datetime.strptime(date_str, '%Y%m%d')
                        backups.append((backup_date, backup_file))
                except:
                    continue
            
            # 날짜 기준 내림차순 정렬 (최신순)
            backups = sorted(backups, key=lambda x: x[0], reverse=True)
            
            if not backups:
                logger.error("❌ 백업 파일 없음")
                return False
            
            latest_backup = backups[0][1]  # (날짜, 파일) 튜플에서 파일만
            backup_date = backups[0][0]    # 날짜
            
            logger.info(f"🔄 최신 백업에서 복구 시도: {latest_backup.name} ({backup_date.strftime('%Y-%m-%d')})")
            
            # 손상된 DB 백업 (혹시 모르니)
            damaged_path = f"{self.db_path}.damaged_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
            shutil.copy2(self.db_path, damaged_path)
            logger.info(f"💾 손상된 DB 보존: {damaged_path}")
            
            # 백업 복원
            shutil.copy2(latest_backup, self.db_path)
            logger.info(f"✅ 백업 복원 완료: {latest_backup.name}")
            
            # 복원된 DB 무결성 체크
            return self._check_db_integrity()
            
        except Exception as e:
            logger.error(f"❌ DB 복구 실패: {e}")
            return False
    
    def _backup_progress(self, status, remaining, total):
        """백업 진행률 콜백 (선택사항)
        Args:
            status: 백업 상태 (0=진행중, 1=완료)
            remaining: 남은 페이지 수
            total: 전체 페이지 수
        """
        if total > 0:
            progress = (total - remaining) / total * 100
            # 25%마다 로그 (너무 빈번한 로그 방지)
            if progress > 0 and int(progress) % 25 == 0 and remaining > 0:
                logger.debug(f"   백업 진행: {progress:.0f}%")
    
    async def _daily_backup(self):
        """데일리 자동 백업 - 화이트리스트 방식 (안전!)"""
        try:
            from pathlib import Path
            
            # 백업 디렉토리 확인/생성
            backup_dir = Path("backups")
            backup_dir.mkdir(exist_ok=True)
            
            # 타임스탬프 (YYYYMMDD 형식)
            timestamp = datetime.now().strftime('%Y%m%d')
            
            # ========================================
            # ✅ 백업 대상 DB 화이트리스트 (명시적!)
            # ========================================
            backup_targets = {
                'signal_performance.db': '성과 DB',
                'kill_switch_events.db': '킬스위치 DB'
            }
            
            # Config에서 추가 DB 경로 가져오기 (선택)
            try:
                if hasattr(self, 'config') and self.config:
                    config_db_paths = self.config.get_db_paths()
                    
                    # performance DB는 이미 포함되어 있으므로 중복 체크
                    perf_db = config_db_paths.get('performance', 'signal_performance.db')
                    if perf_db not in backup_targets:
                        backup_targets[perf_db] = 'Config 성과 DB'
                    
                    # 킬스위치 DB 추가 (있다면)
                    ks_db = config_db_paths.get('killswitch', 'kill_switch_events.db')
                    if ks_db and ks_db not in backup_targets:
                        backup_targets[ks_db] = 'Config 킬스위치 DB'
                        
            except Exception as e:
                logger.debug(f"Config DB 경로 조회 실패 (스킵): {e}")
            
            logger.info("=" * 60)
            logger.info("💾 데일리 백업 시작 (온라인 백업 - LOCK 충돌 방지)")
            logger.info(f"   백업 대상: {len(backup_targets)}개")
            logger.info("=" * 60)
            
            backup_count = 0
            skip_count = 0
            fail_count = 0
            
            # ========================================
            # 각 DB 파일 백업
            # ========================================
            for db_name, db_desc in backup_targets.items():
                db_path = Path(db_name)
                
                # DB 파일 존재 여부 확인
                if not db_path.exists():
                    logger.debug(f"📂 {db_desc} ({db_name}) 존재하지 않음 - 스킵")
                    skip_count += 1
                    continue
                
                # 백업 파일명
                db_stem = db_path.stem
                backup_path = backup_dir / f"{db_stem}_{timestamp}.db"
                
                # 이미 오늘 백업이 있으면 스킵
                if backup_path.exists():
                    logger.debug(f"📦 {backup_path.name} 이미 존재 - 스킵")
                    skip_count += 1
                    continue
                
                # ========================================
                # 백업 실행
                # ========================================
                success = await self._backup_single_db(
                    db_path=str(db_path),
                    backup_path=backup_path,
                    db_name=f"{db_desc} ({db_name})"
                )
                
                if success:
                    backup_count += 1
                else:
                    fail_count += 1
            
            # ========================================
            # 백업 결과 요약
            # ========================================
            logger.info("=" * 60)
            logger.info(
                f"✅ 데일리 백업 완료: "
                f"성공 {backup_count}개, 스킵 {skip_count}개, 실패 {fail_count}개"
            )
            
            # 백업된 파일 목록 출력
            if backup_count > 0:
                logger.info(f"   백업 위치: {backup_dir.absolute()}/")
                for backup_file in sorted(backup_dir.glob(f"*_{timestamp}.db")):
                    size_mb = backup_file.stat().st_size / 1024 / 1024
                    logger.info(f"   - {backup_file.name} ({size_mb:.2f}MB)")
            
            logger.info("=" * 60)
            
            # 오래된 백업 정리 (최근 30일만 보관)
            if backup_count > 0:
                await self._cleanup_old_backups(backup_dir, keep_days=30)
            
        except Exception as e:
            logger.error(f"❌ 데일리 백업 실패: {e}")
            import traceback
            logger.debug(f"상세 오류:\n{traceback.format_exc()}")

    async def _backup_single_db(self, db_path: str, backup_path: Path, db_name: str) -> bool:
        """
        단일 DB 백업 (SQLite BACKUP API + 폴백)
        Args:
            db_path: 원본 DB 경로
            backup_path: 백업 파일 경로
            db_name: DB 이름 (로깅용)
        Returns:
            bool: 백업 성공 여부
        """
        try:
            logger.info(f"💾 백업 중: {db_name} → {backup_path.name}")
            
            # ========================================
            # 방법 1: SQLite BACKUP API (온라인 백업)
            # ========================================
            try:
                # 소스 DB 연결 (읽기 전용)
                source_conn = sqlite3.connect(db_path, timeout=SQLITE_TIMEOUT)
                
                # 백업 대상 DB 연결
                backup_conn = sqlite3.connect(str(backup_path))
                
                # 온라인 백업 실행
                with source_conn:
                    source_conn.backup(
                        backup_conn,
                        pages=100,
                        progress=self._backup_progress
                    )
                
                # 연결 종료
                backup_conn.close()
                source_conn.close()
                
                # 백업 파일 크기
                size_mb = backup_path.stat().st_size / 1024 / 1024
                
                logger.info(f"✅ 백업 성공 (온라인): {backup_path.name} ({size_mb:.2f}MB)")
                return True
                
            except Exception as e:
                logger.warning(f"⚠️ 온라인 백업 실패 ({db_name}): {e}")
                
                # ========================================
                # 방법 2: 폴백 - WAL 체크포인트 + 파일 복사
                # ========================================
                logger.warning(f"⚠️ 폴백: 파일 복사 방식으로 재시도 ({db_name})...")
                
                try:
                    import shutil
                    
                    # WAL 체크포인트
                    conn = sqlite3.connect(db_path, timeout=SQLITE_TIMEOUT)
                    conn.execute("PRAGMA wal_checkpoint(FULL)")
                    conn.close()
                    
                    # 파일 복사
                    shutil.copy2(db_path, backup_path)
                    
                    size_mb = backup_path.stat().st_size / 1024 / 1024
                    logger.info(f"✅ 백업 성공 (파일복사): {backup_path.name} ({size_mb:.2f}MB)")
                    return True
                    
                except Exception as e2:
                    logger.error(f"❌ 백업 완전 실패 ({db_name}): {e2}")
                    
                    # 실패한 백업 파일 정리
                    if backup_path.exists():
                        try:
                            backup_path.unlink()
                        except:
                            pass
                    
                    return False
        
        except Exception as e:
            logger.error(f"❌ 백업 오류 ({db_name}): {e}")
            return False

    async def _cleanup_old_backups(self, backup_dir: Path, keep_days: int = 30):
        """오래된 백업 정리
        Args:
            backup_dir: 백업 디렉토리
            keep_days: 보관 일수 (기본 30일)
        """
        try:
            cutoff_date = datetime.now() - timedelta(days=keep_days)
            
            # signal_performance_YYYYMMDD.db 패턴 찾기
            backups = sorted(backup_dir.glob("signal_performance_*.db"))
            removed_count = 0
            
            for backup_file in backups:
                try:
                    # 파일명에서 날짜 추출: signal_performance_20250109.db
                    date_str = backup_file.stem.split('_')[-1]  # '20250109'
                    
                    # 8자리 날짜 형식 확인
                    if len(date_str) != 8 or not date_str.isdigit():
                        continue
                    
                    backup_date = datetime.strptime(date_str, '%Y%m%d')
                    
                    # 30일 초과시 삭제
                    if backup_date < cutoff_date:
                        backup_file.unlink()
                        removed_count += 1
                        logger.debug(f"🗑️ 오래된 백업 삭제: {backup_file.name}")
                        
                except (ValueError, IndexError) as e:
                    # 날짜 파싱 실패시 스킵
                    logger.debug(f"백업 파일 파싱 실패 (스킵): {backup_file.name}")
                    continue
            
            if removed_count > 0:
                logger.info(f"🗑️ 오래된 백업 {removed_count}개 정리 완료 (30일 초과)")
            
        except Exception as e:
            logger.error(f"백업 정리 실패: {e}")

    async def _check_daily_backup(self):
        """데일리 백업 체크 및 실행"""
        try:
            # 마지막 백업 날짜 확인
            if not hasattr(self, 'last_backup_date'):
                self.last_backup_date = None
            
            today = datetime.now().date()
            
            # 오늘 백업 안 했으면 실행
            if self.last_backup_date != today:
                await self._daily_backup()
                self.last_backup_date = today
                
        except Exception as e:
            logger.error(f"백업 체크 실패: {e}")

    async def _weekly_vacuum(self):
        """주간 DB 최적화 (일요일, VACUUM 전 백업 보장)
        주의: VACUUM 실행 중 DB가 잠기므로 거래가 중단될 수 있습니다.
        따라서 거래가 적은 일요일에만 실행하며, VACUUM 전에 반드시 백업합니다.
        """
        try:
            # 일요일(6)이 아니면 스킵
            if datetime.now().weekday() != 6:
                return
            
            # 마지막 VACUUM 날짜 확인 (하루 1회만)
            if not hasattr(self, 'last_vacuum_date'):
                self.last_vacuum_date = None
            
            today = datetime.now().date()
            if self.last_vacuum_date == today:
                return
            
            logger.info("🔧 주간 DB 최적화 시작 (일요일)")
            
            # ========================================
            # 핵심: VACUUM 전에 반드시 백업 먼저!
            # ========================================
            logger.info("   - 최적화 전 백업 실행 (안전장치)...")
            await self._daily_backup()
            
            # ========================================
            # VACUUM 실행
            # ========================================
            conn = sqlite3.connect(self.db_path, timeout=SQLITE_TIMEOUT)
            
            # 파일 크기 (전)
            file_size_before = Path(self.db_path).stat().st_size / 1024 / 1024
            
            # 1. 통계 업데이트
            conn.execute("ANALYZE")
            logger.debug("   - 통계 업데이트 완료")
            
            # 2. 쿼리 최적화
            conn.execute("PRAGMA optimize")
            logger.debug("   - 쿼리 최적화 완료")
            
            # 3. 파일 압축 (시간 걸림! 수초~수분)
            logger.info("   - VACUUM 실행 중... (DB 잠김, 거래 일시 중단)")
            conn.execute("VACUUM")
            logger.debug("   - VACUUM 완료")
            
            conn.close()
            
            # 파일 크기 (후)
            file_size_after = Path(self.db_path).stat().st_size / 1024 / 1024
            saved_mb = file_size_before - file_size_after
            
            logger.info(f"✅ 주간 DB 최적화 완료: {saved_mb:.2f}MB 절약 ({file_size_before:.2f}MB → {file_size_after:.2f}MB)")
            
            self.last_vacuum_date = today
            
        except Exception as e:
            logger.error(f"❌ 주간 DB 최적화 실패: {e}")

    async def _check_weekly_vacuum(self):
        """주간 VACUUM 체크 및 실행"""
        try:
            await self._weekly_vacuum()
        except Exception as e:
            logger.error(f"VACUUM 체크 실패: {e}")
        
    def _update_signal_stats(self, signal_type: str, success: bool, return_pct: float):
        """신호별 통계 업데이트 - 개수 제한 추가"""
        if signal_type not in self.signal_stats:
            # 최대 개수 체크
            if len(self.signal_stats) >= self._max_signal_stats:
                # 가장 오래된 것 제거 (FIFO)
                self.signal_stats.popitem(last=False)
            
            self.signal_stats[signal_type] = {
                'count': 0, 'wins': 0,
                'returns': deque(maxlen=MEMORY_BOUNDS['signal_history']),
                'ema_success': DEFAULT_SCORE
            }
        
        stats = self.signal_stats[signal_type]
        stats['count'] += 1
        if success:
            stats['wins'] += 1
        stats['returns'].append(return_pct)
        stats['ema_success'] = SAFE_EMA_ALPHA * float(success) + (1 - SAFE_EMA_ALPHA) * stats['ema_success']
    
    async def record_buy_open(
        self,
        position: Position,
        result: ExecutionResult,
        signal,
        timeframe: str = "",
    ) -> None:
        """BUY 체결을 trade_details에 기록한다."""
        try:
            _px = float(
                getattr(result, "executed_price", 0.0)
                or getattr(result, "avg_price", 0.0)
                or position.avg_price
                or 0.0
            )
            _vol = float(
                getattr(result, "executed_volume", 0.0)
                or position.volume
                or 0.0
            )
            _fee = float(
                getattr(result, "fee", 0.0)
                or getattr(result, "total_fee", 0.0)
                or 0.0
            )

            if _px <= 0.0 or _vol <= 0.0:
                logger.warning(
                    f"[B2] {position.symbol} BUY leg 기록 스킵 | "
                    f"price={_px} volume={_vol}"
                )
                return

            _meta = position.metadata or {}
            _signals = dict(position.entry_signals or {})
            _signal_indicators = (
                getattr(signal, "indicators", None) or {}
            )

            # 최신 t7은 이미 entry_signals에 dur을 저장한다.
            # 누락본 호환용으로만 동일 값을 보충한다.
            _signals.setdefault(
                "dur_5x10_bars",
                _signal_indicators.get("dur_5x10_bars"),
            )
            _signals.setdefault(
                "entry_rise_pct",
                _signal_indicators.get("entry_rise_pct"),
            )
            _signals.setdefault(
                "entry_type",
                _meta.get("entry_type", ""),
            )
            _signals.setdefault(
                "judges",
                _signal_indicators.get("judges", ""),
            )
            # 🔒 [REPORT-SL-METRICS-0822-R2]
            # T6 계획값과 T7 체결후 적용값을 같은 BUY 레코드에 분리 저장.
            # 틱 밴드가 바뀌면 단일 tick으로 나누지 않고 BAND로 남긴다.
            from Trading_bot_1_core import get_tick_size
            for _sl_prefix, _sl_entry_raw, _sl_stop_raw in (
                (
                    "planned",
                    getattr(signal, "price", 0.0),
                    _signal_indicators.get("stop_loss"),
                ),
                (
                    "effective",
                    position.avg_price,
                    getattr(position, "stop_loss", 0.0),
                ),
            ):
                _sl_entry = float(_sl_entry_raw or 0.0)
                _sl_stop = float(_sl_stop_raw or 0.0)
                _sl_pct_key = f"{_sl_prefix}_sl_pct"
                _sl_ticks_key = f"{_sl_prefix}_sl_ticks"
                _sl_band_key = f"{_sl_prefix}_sl_tick_band"
                if _sl_entry > 0.0 and 0.0 < _sl_stop < _sl_entry:
                    _sl_entry_tick = float(get_tick_size(_sl_entry) or 0.0)
                    _sl_stop_tick = float(get_tick_size(_sl_stop) or 0.0)
                    _sl_same_tick = bool(
                        _sl_entry_tick > 0.0
                        and _sl_stop_tick > 0.0
                        and abs(_sl_entry_tick - _sl_stop_tick) < 1e-12
                    )
                    _signals[_sl_pct_key] = (
                        (_sl_entry - _sl_stop) / _sl_entry * 100.0
                    )
                    _signals[_sl_ticks_key] = (
                        (_sl_entry - _sl_stop) / _sl_entry_tick
                        if _sl_same_tick else None
                    )
                    _signals[_sl_band_key] = (
                        None
                        if _sl_same_tick
                        else f"{_sl_entry_tick:g}->{_sl_stop_tick:g}"
                    )
                else:
                    _signals[_sl_pct_key] = None
                    _signals[_sl_ticks_key] = None
                    _signals[_sl_band_key] = None

            _regime = position.entry_regime
            _regime_s = (
                getattr(_regime, "value", None)
                or (str(_regime) if _regime else "")
            )
            _confidence = float(
                _meta.get("entry_confidence", 0.0) or 0.0
            )
            _reason = (
                _meta.get("entry_type")
                or position.entry_reason
                or "ENTRY"
            )
            _recorded_at = (
                position.entry_time.isoformat()
                if isinstance(position.entry_time, datetime)
                else datetime.now().isoformat()
            )
            _timeframe = (
                timeframe
                or _meta.get("timeframe")
                or (
                    self.config.get_cached("timeframe")
                    if self.config else "5m"
                )
            )

            async with self._conn_lock:
                conn = self._get_connection()
                conn.execute("BEGIN IMMEDIATE")

                conn.execute("""
                    INSERT INTO trade_details (
                        symbol, side, price, volume, amount,
                        fee, pnl, return_pct, reason, confidence,
                        market_regime, signal_components, weights_used,
                        mode, slippage_pct, timestamp, timeframe
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    position.symbol,
                    "BUY",
                    _px,
                    _vol,
                    _px * _vol,
                    _fee,
                    None,
                    None,
                    _reason,
                    _confidence,
                    _regime_s,
                    json.dumps(
                        convert_to_serializable(_signals)
                    ),
                    json.dumps(
                        convert_to_serializable(self.current_weights)
                    ),
                    self.mode,
                    float(
                        getattr(result, "slippage_pct", 0.0) or 0.0
                    ),
                    _recorded_at,
                    _timeframe,
                ))

                conn.commit()

                try:
                    conn.execute("PRAGMA wal_checkpoint(PASSIVE)")
                except Exception as _ckpt_err:
                    logger.debug(
                        f"[B2-WAL-CHECKPOINT] 스킵: {_ckpt_err}"
                    )

            logger.debug(
                f"[B2] {position.symbol} BUY leg 기록 완료 | "
                f"price={_px} volume={_vol} "
                f"dur={_signals.get('dur_5x10_bars')}"
            )

        except sqlite3.Error as e:
            if self._conn:
                try:
                    self._conn.execute("ROLLBACK")
                except Exception:
                    pass
            logger.warning(
                f"[B2] {getattr(position, 'symbol', '?')} "
                f"BUY leg DB 기록 실패: {e}"
            )
        except Exception as e:
            logger.warning(
                f"[B2] {getattr(position, 'symbol', '?')} "
                f"BUY leg 기록 실패: {e}"
            )

    async def record_trade_complete(self, position: Position, result: ExecutionResult,
                       signal, market_data: Dict) -> None:
        """거래 완료 기록 - 정밀도 보정 + 캐시 무효화
        Args:
            position: 포지션 정보
            result: 실행 결과
            signal: 거래 신호
            market_data: 시장 데이터
        """
        _saved = False  # [보완 v3_5] commit 성공 표지 (호출자가 기록 여부 판정 · 예외는 이 함수 안에서 처리)
        _eid = market_data.get("event_id") if isinstance(market_data, dict) else None  # [보완 v3_7] 사건 id (REAL 만 · SIM = None)
        try:
            _erow = self._event_row(_eid) if _eid else None
            if _erow is not None:  # [보완 v3_7] 메모리 지표 (손익 · 연속 손실) 이중 증가 방지 = DB 쓰기 전 판정
                if not self._event_same(_erow, result):  # [보완 v3_10 K6] 같은 사건 · 다른 내용 = 기록됨으로 넘기지 않음
                    logger.critical(f"[EVENT-CONTENT-DIFF] {position.symbol} {_eid} 기존 행과 내용 다름 → 미기록 (HOLD)")
                    return False
                logger.warning(f"[EVENT-DUP] {position.symbol} {_eid} 이미 기록 → 재기록 안 함")
                return True
            # 금액 계산 후 반올림
            from Trading_bot_1_core import round_money, is_positive
            
            # 🔒 실제 체결 수량 기준 수익 계산 (로그와 100% 일치)
            if hasattr(result, 'executed_volume') and result.executed_volume:
                executed_volume = float(result.executed_volume)
            else:
                executed_volume = float(position.volume * signal.sell_ratio)

            profit = (result.avg_price - position.avg_price) * executed_volume
            profit -= result.total_fee

            # 매수 수수료는 전체 매수수량 기준으로 SELL leg에 한 번만 분배
            entry_volume_base = float(
                getattr(position, "original_volume", 0.0)
                or position.volume
                or 0.0
            )
            if entry_volume_base > 0.0:
                entry_fee_ratio = min(
                    1.0,
                    max(0.0, executed_volume / entry_volume_base),
                )
            else:
                entry_fee_ratio = 0.0

            profit -= (
                float(position.entry_fee_paid or 0.0)
                * entry_fee_ratio
            )
            profit = round_money(profit)
            
            success = is_positive(profit)

            # 🔒 체결 단위 기준 수익률 재계산 (profit 기반 100% 정합)
            entry_price = position.avg_price

            if entry_price > 0 and executed_volume > 0:
                entry_amount = entry_price * executed_volume
                return_pct = (profit / entry_amount) * 100
            else:
                return_pct = 0.0

            # 신호별 성과 업데이트 (최종 방어선)
            entry_signals = position.entry_signals or {}
            for sig_type, value in entry_signals.items():
                if sig_type == "dur_5x10_bars":
                    continue  # 🔒 메타 스냅샷(변별 harness용), 신호 타입 통계 대상 아님
                if value is None:
                    continue
                
                # dict → float 정규화
                if isinstance(value, dict):
                    value = (
                        value.get("confidence")
                        or value.get("value")
                        or value.get("score")
                        or 0.0
                    )
                
                try:
                    value = float(value)
                except (TypeError, ValueError):
                    logger.debug(f"[신호통계] {sig_type} float 변환 실패")
                    continue
                
                if value > COMPONENT_MIN_VALUE:
                    self._update_signal_stats(sig_type, success, return_pct)
            
            # 메트릭 업데이트 (반올림)
            self.metrics['total_trades'] += 1
            self.metrics['total_pnl'] = round_money(self.metrics['total_pnl'] + profit)
            self.metrics['total_fees'] = round_money(self.metrics['total_fees'] + result.total_fee)
            
            if success:
                self.metrics['wins'] += 1
                self.metrics['consecutive_losses'] = 0
            else:
                self.metrics['losses'] += 1
                self.metrics['consecutive_losses'] += 1
            
            # 잔고 및 drawdown 계산 (반올림)
            if self.mode == 'sim':
                current_balance = self.initial_balance
            else:
                current_balance = (
                    self.initial_balance
                    + self.metrics['total_pnl']
                )
            current_balance = round_money(current_balance)
            
            # ✅ 양수일 때만 peak 업데이트
            if current_balance and current_balance > 0 and current_balance > (self.metrics.get('peak_balance') or 0):
                self.metrics['peak_balance'] = round_money(current_balance)

            if (self.metrics.get('peak_balance') or 0) > 0:
                drawdown = (current_balance - self.metrics['peak_balance']) / self.metrics['peak_balance']
                # ✅ 낙폭은 최대 -100%까지만
                drawdown = max(drawdown, -1.0)
                self.metrics['max_drawdown'] = min(self.metrics['max_drawdown'], drawdown)
            
            # ========================================
            # 🔒 SSoT: 진입 신뢰도 & 진입 체제 우선 (최종 방어선)
            # ========================================
            # 철학:
            # - signal.confidence = 매도 신호 신뢰도 (강제손절=1.0)
            # - signal.market_regime = 매도 시점 체제
            # - market_data['entry_confidence'] = 진입 신뢰도 (실제 값)
            # - market_data['entry_regime'] = 진입 시점 체제 (실제 값)
            # - DB/리포트에는 진입 신뢰도 & 진입 체제만 저장
            # ========================================
            entry_conf = None
            entry_regime = None
            
            if market_data and isinstance(market_data, dict):
                entry_conf = market_data.get('entry_confidence')
                entry_regime = market_data.get('entry_regime')
            
            confidence_to_store = (
                float(entry_conf) if entry_conf is not None
                else float(signal.confidence if signal else 0.0)
            )
            
            # 🆕 진입 체제 결정 (우선순위: market_data > position.entry_regime > signal)
            regime_to_store = None
            if entry_regime:
                # Bot에서 전달받은 진입 체제 (최우선)
                regime_to_store = str(entry_regime)
            elif position and hasattr(position, 'entry_regime') and position.entry_regime:
                # Position 객체의 entry_regime (2순위)
                if hasattr(position.entry_regime, 'value'):
                    regime_to_store = position.entry_regime.value
                else:
                    regime_to_store = str(position.entry_regime)
            elif signal and signal.market_regime:
                # Signal의 market_regime (fallback - 매도 시점 체제)
                regime_to_store = signal.market_regime.value
            else:
                regime_to_store = 'UNKNOWN'
            
            # 거래 이력 저장 (반올림된 값)
            trade_record = {
                'timestamp': datetime.now(),
                'symbol': position.symbol,
                'success': success,
                'return_pct': return_pct,
                'profit': profit,
                'confidence': confidence_to_store,  # ✅ 진입 신뢰도
                'reason': signal.reason if signal else '',
                'market_regime': regime_to_store  # ✅ 진입 체제 (매도 시점 체제 ❌)
            }
            self.trades.append(trade_record)
            
            # 🆕 일일 통계 캐시 무효화
            if hasattr(self, '_daily_stats_cache'):
                delattr(self, '_daily_stats_cache')
                logger.debug("일일 통계 캐시 무효화")
            
            # ========================================
            # DB 저장 - 트랜잭션 처리 + Lock
            # ========================================
            try:
                async with self._conn_lock:
                    conn = self._get_connection()
                    
                    # 트랜잭션 시작
                    conn.execute("BEGIN IMMEDIATE")
                    
                    timeframe = self.config.get_cached('timeframe') if self.config else '5m'
                    
                    # ========================================
                    # 🔒 DB 스키마 정합 INSERT (price/amount 사용)
                    # ========================================
                    # 철학: trade_details 스키마는 price, volume, amount 사용
                    #       entry_price, exit_price, executed_price는 존재하지 않음
                    # ========================================
                    trade_price = float(result.avg_price)
                    # 🔒 [TIMESTAMP-FIX 2026-08-02] 동일 매도 트랜잭션의 DB 기록 시각 SSoT
                    #  trade_details·performances 양쪽에 같은 값을 써서
                    #  같은 거래인데 두 테이블 timestamp가 미세하게 어긋나는 것도 방지
                    _recorded_at = datetime.now().isoformat()
                    # 🔒 실제 체결 수량 사용 (로그와 100% 일치)
                    if hasattr(result, 'executed_volume') and result.executed_volume:
                        trade_volume = float(result.executed_volume)
                    else:
                        trade_volume = float(position.volume * signal.sell_ratio)

                    trade_amount = trade_price * trade_volume
                    
                    # 1) trade_details 테이블에 저장
                    _ev_on = bool(getattr(self, '_has_event_id', False))  # [보완 v3_7] 열 없으면 기존 INSERT 그대로 (SIM 무영향)
                    conn.execute("""
                        INSERT INTO trade_details (
                            symbol, side, price, volume, amount,
                            fee, pnl, return_pct, reason, confidence,
                            market_regime, signal_components, weights_used,
                            mode, slippage_pct, timestamp, timeframe""" + (", event_id" if _ev_on else "") + """
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?""" + (", ?" if _ev_on else "") + """)
                    """, (
                        position.symbol,
                        'SELL',
                        trade_price,
                        trade_volume,
                        trade_amount,
                        result.total_fee,
                        profit,
                        return_pct,
                        signal.reason if signal else '',
                        confidence_to_store,
                        regime_to_store,
                        json.dumps(convert_to_serializable(position.entry_signals)) if position.entry_signals else '{}',
                        json.dumps(convert_to_serializable(self.current_weights)),
                        self.mode,
                        getattr(result, 'slippage_pct', 0),
                        _recorded_at,
                        timeframe
                    ) + ((_eid,) if _ev_on else ()))
                    
                    # 2) performances 테이블에 저장 (최종 방어선)
                    entry_signals = position.entry_signals or {}
                    for sig_type, value in entry_signals.items():
                        if sig_type == "dur_5x10_bars":
                            continue  # 🔒 메타 스냅샷, performances 테이블 대상 아님
                        if value is None:
                            continue
                        
                        # dict → float 정규화
                        if isinstance(value, dict):
                            value = (
                                value.get("confidence")
                                or value.get("value")
                                or value.get("score")
                                or 0.0
                            )
                        
                        try:
                            value = float(value)
                        except (TypeError, ValueError):
                            logger.debug(f"[DB저장] {sig_type} float 변환 실패")
                            continue
                        
                        if value > COMPONENT_MIN_VALUE:
                            conn.execute("""
                                INSERT INTO performances (
                                    signal_type, success, return_pct, profit, mode, timestamp
                                ) VALUES (?, ?, ?, ?, ?, ?)
                            """, (
                                sig_type,
                                success,
                                return_pct,
                                profit,
                                self.mode,
                                _recorded_at
                            ))
                    
                    # 커밋
                    conn.commit()
                    _saved = True  # [보완 v3_5] 이 줄 뒤 오류 = 기록은 된 것

                    # 🔧 2026-08-01: WAL → 본체 반영(체크포인트)
                    #  08-01 실측: 본체 db mtime 11:56 / WAL 10.7MB 미반영 상태에서
                    #  재시작이 반복되며 trade_details 08-01 SELL이 82건 → 3건으로 유실.
                    #  PASSIVE는 다른 읽기/쓰기를 막지 않으므로 매 매도 기록마다 호출해도 안전.
                    try:
                        conn.execute("PRAGMA wal_checkpoint(PASSIVE)")
                    except Exception as _ckpt_err:
                        logger.debug(f"[WAL-CHECKPOINT] 스킵: {_ckpt_err}")

                    logger.debug(f"✅ DB 저장 완료 (트랜잭션): {position.symbol}")
                    
            except sqlite3.IntegrityError as e:
                if self._conn:
                    try:
                        self._conn.execute("ROLLBACK")
                    except Exception:
                        pass
                if _eid and "UNIQUE" in str(e).upper():
                    _erow2 = self._event_row(_eid)
                    if _erow2 is not None and self._event_same(_erow2, result):
                        _saved = True  # [보완 v3_7] 같은 사건이 먼저 기록됨 = 기록됨
                        logger.warning(f"[EVENT-DUP] {position.symbol} {_eid} UNIQUE → 이미 기록 (경쟁)")
                    else:
                        logger.critical(f"[EVENT-CONTENT-DIFF] {position.symbol} {_eid} UNIQUE · 기존 행과 내용 다름 → 미기록 (HOLD)")  # [보완 v3_10 K6]
                else:
                    logger.error(f"❌ 거래 상세 DB 저장 실패 (무결성): {e}")

            except sqlite3.Error as e:
                # 롤백
                if self._conn:
                    try:
                        self._conn.execute("ROLLBACK")
                        logger.warning(f"⚠️ DB 저장 실패 - 롤백: {e}")
                    except:
                        pass
                logger.error(f"❌ 거래 상세 DB 저장 실패: {e}")
                
            except Exception as e:
                if self._conn:
                    try:
                        self._conn.execute("ROLLBACK")
                    except:
                        pass
                logger.error(f"❌ DB 저장 오류: {e}")
            
            # 백업 및 최적화
            await self._check_weekly_vacuum()
            
            if datetime.now().weekday() != 6:
                await self._check_daily_backup()
            
            # 주기적 리포트 생성 체크
            if datetime.now() - self.last_report_time > timedelta(hours=self.report_interval_hours):
                await self.generate_performance_report()
                self.last_report_time = datetime.now()
            
            # 성과 로그 출력
            try:
                _hold_min = (datetime.now() - position.entry_time).total_seconds() / 60.0
                _exit_reason = signal.reason if signal else "?"
            except Exception:
                _hold_min = -1.0
                _exit_reason = "?"
            logger.info(f"{'💚' if success else '💔'} {position.symbol} 매도 완료: "
                    f"수익률 {return_pct:.2f}% | 손익 {profit:+,.0f}원 | "
                    f"보유 {_hold_min:.1f}분 | 사유 {_exit_reason} | "
                    f"연속손실 {self.metrics['consecutive_losses']}회")
            return _saved  # [보완 v3_5] True = trade_details commit 됨 · False = 미기록 (rollback · 저장 전 오류)
            
        except Exception as e:
            logger.error(f"거래 기록 실패: {e}")
            return _saved  # [보완 v3_5] commit 뒤 후처리 오류여도 기록 여부 그대로
       
    async def generate_performance_report(self) -> Dict:
        """성과 리포트 생성"""
        try:
            report = {
                'timestamp': datetime.now().isoformat(),
                'mode': self.mode,
                'summary': {
                    'total_trades': self.metrics['total_trades'],
                    'wins': self.metrics['wins'],
                    'losses': self.metrics['losses'],
                    'win_rate': self.metrics['wins'] / max(1, self.metrics['total_trades']) * 100,
                    'total_pnl': self.metrics['total_pnl'],
                    'total_fees': self.metrics['total_fees'],
                    'net_profit': self.metrics['total_pnl'],
                    'max_drawdown': self.metrics['max_drawdown'] * 100,
                    'consecutive_losses': self.metrics['consecutive_losses'],
                    'current_balance': (
                        self.initial_balance
                        + self.metrics['total_pnl']
                    )
                },
                'signal_performance': {},
                'recent_trades': [],
                'weight_adjustments': {
                    'current': self.current_weights.copy(),
                    'base': self.base_weights.copy(),
                    'total_adjustments': self.adjustment_count,
                    'daily_adjustments': self.daily_adjustments
                }
            }
            
            # 신호별 성과
            for sig_type, stats in self.signal_stats.items():
                if stats['count'] >= MIN_SAMPLE_SIZE:
                    returns = list(stats['returns'])
                    report['signal_performance'][sig_type] = {
                        'count': stats['count'],
                        'wins': stats['wins'],
                        'win_rate': stats['wins'] / stats['count'] * 100,
                        'avg_return': np.mean(returns) if returns else 0,
                        'confidence': self.get_signal_confidence(sig_type),
                        'kelly_criterion': self.calculate_kelly_criterion(sig_type)
                    }
            
            # 최근 거래
            recent_trades = list(self.trades)[-20:]
            report['recent_trades'] = [{
                'timestamp': t['timestamp'].isoformat() if isinstance(t['timestamp'], datetime) else t['timestamp'],
                'symbol': t['symbol'],
                'success': t['success'],
                'return_pct': t['return_pct'],
                'profit': t['profit']
            } for t in recent_trades]
            
            # 파일 저장
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            report_file = self.report_dirs['reports'] / f"performance_{timestamp}.json"
            
            with open(report_file, 'w', encoding='utf-8') as f:
                json.dump(report, f, indent=2, default=str, ensure_ascii=False)
            
            # 요약 로그 출력
            logger.info("=" * 60)
            logger.info("📊 성과 리포트 생성")
            logger.info(f"총 거래: {report['summary']['total_trades']}회")
            logger.info(f"승률: {report['summary']['win_rate']:.1f}%")
            logger.info(f"순손익: {report['summary']['net_profit']:+,.0f}원")
            logger.info(f"최대DD: {report['summary']['max_drawdown']:.1f}%")
            logger.info(f"리포트 저장: {report_file}")
            logger.info("=" * 60)
            
            return report
            
        except Exception as e:
            logger.error(f"리포트 생성 실패: {e}")
            return {}
    
    def get_signal_confidence(self, signal_type: str) -> float:
        """신호 신뢰도"""
        if signal_type not in self.signal_stats:
            return DEFAULT_SCORE
        
        stats = self.signal_stats[signal_type]
        if stats['count'] < MIN_SAMPLE_SIZE:
            return DEFAULT_SCORE * (stats['count'] / MIN_SAMPLE_SIZE)
        
        return np.clip(stats['ema_success'], CONFIDENCE_SCORE_MIN, CONFIDENCE_SCORE_MAX)
    
    def calculate_kelly_criterion(self, signal_type: str) -> float:
        """Kelly Criterion 계산"""
        if signal_type not in self.signal_stats or self.signal_stats[signal_type]['count'] < MIN_SAMPLE_SIZE:
            return ZERO_THRESHOLD
        
        stats = self.signal_stats[signal_type]
        returns = list(stats['returns'])[-DEFAULT_ANALYSIS_WINDOW:]
        
        if not returns:
            return ZERO_THRESHOLD
        
        wins = [r for r in returns if r > 0]
        losses = [r for r in returns if r < 0]
        
        if not wins or not losses:
            return ZERO_THRESHOLD
        
        win_rate = len(wins) / len(returns)
        avg_win = np.mean(wins) / PERCENT_CONVERSION
        avg_loss = abs(np.mean(losses)) / PERCENT_CONVERSION
        
        if avg_loss < EPSILON:
            return ZERO_THRESHOLD
        
        win_loss_ratio = avg_win / avg_loss
        kelly = (win_rate * (1 + win_loss_ratio) - 1) / win_loss_ratio
        kelly_adjusted = kelly * DEFAULT_KELLY_FRACTION
        
        return np.clip(kelly_adjusted, ZERO_THRESHOLD, DEFAULT_KELLY_MAX_WEIGHT)
    
    def get_adjusted_weights(self, condition, regime: MarketRegime) -> Dict[str, float]:
        """조정된 가중치 반환"""
        # 일일 조정 횟수 체크
        if datetime.now().date() != self.last_adjustment_date:
            self.daily_adjustments = 0
            self.last_adjustment_date = datetime.now().date()
        
        if self.daily_adjustments >= 3:
            return self.current_weights
        
        # 충분한 데이터 체크
        total_samples = sum(s['count'] for s in self.signal_stats.values())
        if total_samples < SAFE_MIN_SAMPLES:
            return self.base_weights
        
        # 가중치 계산
        weights = {}
        for signal_type, base_weight in self.base_weights.items():
            if signal_type in self.signal_stats:
                stats = self.signal_stats[signal_type]
                if stats['count'] >= MIN_SAMPLE_SIZE:
                    # 성과 기반 조정
                    score = stats['ema_success']
                    adjustment = 0.8 + score * 0.4
                    adjustment = np.clip(adjustment, 1 - SAFE_MAX_WEIGHT_CHANGE, 1 + SAFE_MAX_WEIGHT_CHANGE)
                else:
                    adjustment = 1.0
            else:
                adjustment = 1.0
            
            weights[signal_type] = base_weight * adjustment
        
        # 정규화
        total = sum(weights.values())
        if total > EPSILON:
            weights = {k: v/total for k, v in weights.items()}
        else:
            weights = self.base_weights
        
        # 변경 기록
        if self._weights_changed(weights):
            self.last_weights = self.current_weights.copy()
            self.current_weights = weights
            self.daily_adjustments += 1
            self.adjustment_count += 1
            
            logger.info(f"가중치 조정 #{self.adjustment_count}: {weights}")
        
        return self.current_weights

    def _weights_changed(self, new_weights: Dict[str, float]) -> bool:
        """가중치 변경 여부"""
        for key in self.current_weights:
            if abs(new_weights.get(key, 0) - self.current_weights[key]) > 0.001:
                return True
        return False
    
    def get_dynamic_threshold(self) -> float:
        """동적 임계값 계산 - 고정 버전 (학습 모드 제거)
        원칙:
        1. 처음부터 엄격한 기준 적용
        2. 타임프레임별 고정 임계값
        3. Config 오버라이드 가능
        Returns:
            float: 신호 임계값 (0.30 ~ 0.70)
        """
        # ========================================
        # 1. 타임프레임별 기본 임계값
        # ========================================
        timeframe_thresholds = {
            '1m': 0.55,   # 1분봉: 노이즈 많음 → 높게
            '3m': 0.50,   # 3분봉: 노이즈 많음
            '5m': 0.48,   # 5분봉: 적당
            '10m': 0.45,  # 10분봉
            '15m': 0.42,  # 15분봉
            '30m': 0.40,  # 30분봉
            '1h': 0.38,   # 1시간봉
            '4h': 0.35,   # 4시간봉
            '1d': 0.32    # 일봉: 노이즈 적음
        }
        
        # ========================================
        # 2. 현재 타임프레임의 임계값
        # ========================================
        timeframe = self.config.get_cached('timeframe') if self.config else '5m'
        base_threshold = timeframe_thresholds.get(timeframe, 0.45)
        
        # ========================================
        # 3. Config에서 오버라이드 (선택)
        # ========================================
        if self.config:
            config_value = self.config.get_cached('min_confidence_threshold')
            if config_value is not None:
                base_threshold = float(config_value)
                logger.debug(f"[임계값] Config 오버라이드: {base_threshold:.3f}")
        
        # ========================================
        # 4. 안전 범위 제한
        # ========================================
        final_threshold = max(0.30, min(0.70, base_threshold))
        
        # ========================================
        # 5. 로그 (10거래마다)
        # ========================================
        current_trades = len(self.trades)
        
        if current_trades % 10 == 0 or not hasattr(self, '_last_threshold_log'):
            logger.info(
                f"📊 [임계값] 타임프레임: {timeframe} | "
                f"거래: {current_trades}회 | "
                f"임계값: {final_threshold:.3f} (고정)"
            )
            self._last_threshold_log = current_trades
        
        return final_threshold
        
    def validate_signal_reliability(self, signal_type: str, market_regime: str) -> float:
        """신호 신뢰도 검증"""
        if signal_type not in self.signal_stats:
            return DEFAULT_RELIABILITY_LOW
        
        stats = self.signal_stats[signal_type]
        if stats['count'] < CONFIDENCE_MIN_SAMPLES:
            sample_ratio = stats['count'] / CONFIDENCE_MIN_SAMPLES
            return DEFAULT_RELIABILITY_LOW + (CONFIDENCE_SCORE_MAX - DEFAULT_RELIABILITY_LOW) * sample_ratio * 0.3
        
        reliability = stats['wins'] / stats['count']
        
        if market_regime == "BREAKDOWN":
            reliability *= BREAKDOWN_REGIME_MULT
        
        return np.clip(reliability, CONFIDENCE_SCORE_MIN, CONFIDENCE_SCORE_MAX)
        
    def get_trade_count(self) -> int:
        """총 거래 수 반환
        Returns:
            int: 현재까지 완료된 총 거래 수
        """
        return self.metrics.get('total_trades', 0)

    def get_consecutive_losses(self) -> int:
        """연속 손실 횟수 반환
        Returns:
            int: 현재 연속 손실 횟수
        """
        return self.metrics.get('consecutive_losses', 0)

    def get_win_rate(self) -> float:
        """승률 반환
        Returns:
            float: 승률 (0.0 ~ 1.0)
        """
        total = self.metrics.get('total_trades', 0)
        if total == 0:
            return 0.5  # 거래 없을 때 기본값
        
        wins = self.metrics.get('wins', 0)
        return wins / total

    def get_total_pnl(self) -> float:
        """총 손익 반환
        Returns:
            float: 총 손익 (원)
        """
        return self.metrics.get('total_pnl', 0.0)

    def get_current_balance(self) -> float:
        """현재 잔고 반환
        Returns:
            float: 현재 잔고 (원)
        """
        cumulative = self.metrics.get('total_pnl', 0.0)
        return self.initial_balance + cumulative

    def get_daily_stats(self) -> Dict:
        """오늘의 통계 반환 - 캐싱 버전
        Returns:
            Dict: 오늘의 거래 통계 (캐시 1분)
        """
        today = datetime.now().date()
        
        # 캐시 확인 (1분 유효)
        if hasattr(self, '_daily_stats_cache'):
            cache_date, cache_time, cache_data = self._daily_stats_cache
            
            # 날짜가 같고 1분 이내면 캐시 반환
            if cache_date == today and (datetime.now() - cache_time).seconds < 60:
                logger.debug(f"일일 통계 캐시 사용 (나이: {(datetime.now() - cache_time).seconds}초)")
                return cache_data
        
        # 캐시 없거나 만료됨 - 새로 계산
        today_trades = [t for t in self.trades 
                    if isinstance(t['timestamp'], datetime) and t['timestamp'].date() == today]
        
        if not today_trades:
            result = {
                'count': 0,
                'wins': 0,
                'losses': 0,
                'win_rate': 0.0,
                'pnl': 0.0,
                'loss': 0.0
            }
        else:
            wins = sum(1 for t in today_trades if t['success'])
            losses = len(today_trades) - wins
            total_pnl = sum(t['profit'] for t in today_trades)
            daily_loss = abs(total_pnl) if total_pnl < 0 else 0.0
            
            result = {
                'count': len(today_trades),
                'wins': wins,
                'losses': losses,
                'win_rate': wins / len(today_trades),
                'pnl': total_pnl,
                'loss': daily_loss
            }
        
        # 캐시 저장
        self._daily_stats_cache = (today, datetime.now(), result)
        logger.debug(f"일일 통계 재계산 완료: {result['count']}건")
        
        return result

    def get_recent_performance(self, count: int = 10) -> Dict:
        """최근 N개 거래 성과 반환
        Args:
            count: 조회할 거래 수
        Returns:
            Dict: 최근 거래 통계
        """
        if not self.trades:
            return {
                'count': 0,
                'wins': 0,
                'losses': 0,
                'win_rate': 0.0,
                'avg_return': 0.0
            }
        
        recent = list(self.trades)[-count:]
        wins = sum(1 for t in recent if t['success'])
        losses = len(recent) - wins
        
        returns = [t['return_pct'] for t in recent]
        avg_return = sum(returns) / len(returns) if returns else 0.0
        
        return {
            'count': len(recent),
            'wins': wins,
            'losses': losses,
            'win_rate': wins / len(recent) if recent else 0.0,
            'avg_return': avg_return
        }    
    
    def get_performance_summary(self) -> Dict:
        """성과 요약"""
        metrics = self.get_risk_metrics()
        
        # 신호별 성과
        signal_performance = {}
        for sig_type, stats in self.signal_stats.items():
            if stats['count'] >= MIN_SAMPLE_SIZE:
                signal_performance[sig_type] = {
                    'count': stats['count'],
                    'win_rate': stats['wins'] / stats['count'],
                    'confidence': self.get_signal_confidence(sig_type)
                }
        
        return {
            'overall': metrics,
            'signals': signal_performance,
            'weights': {
                'current': self.current_weights,
                'adjustments': self.adjustment_count
            }
        }
    
    async def update_daily_summary(self):
        """일별 요약 업데이트 - 연결 재사용"""
        try:
            today = datetime.now().date()
            
            today_trades = [t for t in self.trades 
                        if isinstance(t['timestamp'], datetime) and t['timestamp'].date() == today]
            
            if today_trades:
                wins = sum(1 for t in today_trades if t['success'])
                losses = len(today_trades) - wins
                total_pnl = sum(t['profit'] for t in today_trades)
                avg_return = np.mean([t['return_pct'] for t in today_trades])
                
                async with self._conn_lock:  # 🆕 Lock
                    conn = self._get_connection()  # ✅ 연결 재사용
                    
                    conn.execute("""
                        INSERT OR REPLACE INTO daily_summary (
                            date, total_trades, wins, losses, total_pnl, total_fees,
                            win_rate, avg_return, max_drawdown, mode
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, (
                        today, len(today_trades), wins, losses, total_pnl,
                        self.metrics['total_fees'], wins / len(today_trades) * 100,
                        avg_return, self.metrics['max_drawdown'], self.mode
                    ))
                    
                    conn.commit()
                    # ❌ conn.close() 제거
                
                logger.info(f"일별 요약 업데이트: {today} - {len(today_trades)}거래, 승률 {wins/len(today_trades)*100:.1f}%")
                
        except Exception as e:
            logger.error(f"일별 요약 업데이트 실패: {e}")
    
    async def shutdown(self):
        """종료 처리 - DB 연결 정리"""
        try:
            logger.info("[성과관리] 종료 처리 시작")
            
            # 최종 성과 저장
            if self.current_weights:
                logger.debug(f"최종 가중치 저장: {self.current_weights}")
            
            # 🆕 DB 연결 종료
            if self._conn:
                try:
                    # WAL 체크포인트 (최종 동기화)
                    self._conn.execute("PRAGMA wal_checkpoint(FULL)")
                    
                    self._conn.close()
                    self._conn = None
                    logger.info("✅ 성과 DB 연결 종료")
                except Exception as e:
                    logger.error(f"성과 DB 종료 오류: {e}")
            
            logger.info("[성과관리] 종료 처리 완료")
            
        except Exception as e:
            logger.error(f"[성과관리] 종료 처리 오류: {e}")              
    
    # ========================================
    # 🆕 AIL v1.0 메서드들
    # ========================================
    
    def get_adaptive_weights(self, regime: str = None, symbol: str = None) -> Dict[str, float]:
        """AIL v1.0: P4 + P4a + P4b 통합"""
        
        # 🆕 P4c: 메타 업데이트
        try:
            if hasattr(self, 'meta') and self.meta:
                self.meta.suggest()
        except Exception as e:
            logger.debug(f"[P4c] 메타 업데이트 실패: {e}")
        
        # 베이스 가중치 로드
        w_base = self.base_weights.copy() if hasattr(self, 'base_weights') else {
            'ma_signal': 0.35, 'volume_signal': 0.30, 'bw_signal': 0.20,
            'atr_signal': 0.10, 'adx_signal': 0.05
        }
        
        # P4: 성과 기반
        try:
            w_p4 = self._p4_performance_tune(w_base, regime)
        except Exception as e:
            logger.warning(f"[AIL] P4 실패: {e}")
            w_p4 = w_base.copy()
        
        # P4a: 계절 감지
        try:
            season, alpha = self._p4a_detect_season()
            
            # 🆕 P4c: alpha 스케일 적용
            alpha *= self._meta_get('p4a.alpha_scale', 1.0)
            
            if alpha != 1.0:
                w_p4a = {k: v * alpha for k, v in w_p4.items()}
                s = sum(w_p4a.values())
                w_p4a = {k: v / s for k, v in w_p4a.items()} if s > EPSILON else w_p4.copy()
            else:
                w_p4a = w_p4.copy()
        except Exception as e:
            logger.warning(f"[AIL] P4a 실패: {e}")
            season, alpha = "UNKNOWN", 1.0
            w_p4a = w_p4.copy()
        
        # P4b: 섹터 보정
        try:
            if self.config and self.config.getboolean('SECTOR_TUNING', 'enabled', fallback=True):
                w_final = self._p4b_sector_adjust(symbol, w_p4a)
            else:
                w_final = w_p4a.copy()
        except Exception as e:
            logger.warning(f"[AIL] P4b 실패: {e}")
            w_final = w_p4a.copy()
        
        # 🆕 P4d: 형제코인 상관/선행-후행 보정
        try:
            if hasattr(self, 'p4d') and self.p4d and self.p4d.enabled and symbol:
                dboost = self.p4d.suggest_boost(symbol)
                if dboost:
                    w_p4d = w_final.copy()
                    for key, delta in dboost.items():
                        if key in w_p4d:
                            # ✅ 약세장: 양수 보정만 70% 적용 (과도한 진입 방지)
                            adjusted_delta = delta * 0.70 if delta > 0 else delta
                            w_p4d[key] = max(0.0, w_p4d[key] * (1.0 + adjusted_delta))
                    s_p4d = sum(w_p4d.values())
                    w_final = {k: v/s_p4d for k, v in w_p4d.items()} if s_p4d > EPSILON else w_final
                    logger.info(f"🔗 [P4d] {symbol} pair-boost → {dboost}")
        except Exception as e:
            logger.debug(f"[P4d] 적용 실패: {e}")
        
        # 최종 정규화
        s = sum(w_final.values())
        if s > EPSILON:
            w_final = {k: v / s for k, v in w_final.items()}
        else:
            w_final = w_base.copy()
        
        # 로깅 (10% 이상 변화)
        if hasattr(self, 'current_weights'):
            max_change = max(abs(w_final.get(k, 0) - self.current_weights.get(k, 0)) for k in w_base.keys())
            if max_change > 0.10:
                cluster = self._get_symbol_cluster(symbol) if symbol else "N/A"
                logger.info(
                    f"📊 [AIL] season:{season}, α:{alpha:.2f}, cluster:{cluster} | "
                    f"regime:{regime or 'N/A'} | max_Δ:{max_change:.1%}"
                )
        
        self.current_weights = w_final.copy()
        
        # 🆕 P4c: 가중치 히스토리 누적
        try:
            if hasattr(self, 'weights_history'):
                self.weights_history.append(w_final.copy())
                if len(self.weights_history) > 200:
                    self.weights_history = self.weights_history[-200:]
        except Exception:
            pass
        
        return w_final
    
    def _p4_performance_tune(self, base_weights: Dict[str, float], regime: str = None) -> Dict[str, float]:
        """P4: 성과 기반 보정"""
        cfg = self.config
        if not cfg:
            return base_weights.copy()
        
        # 🆕 P4c: 메타 오버라이드
        max_adj = self._meta_get('p4.max_adjust_pct',
                                 cfg.getfloat('PERFORMANCE_TUNING', 'max_adjust_pct', fallback=0.20))
        hl = cfg.getint('PERFORMANCE_TUNING', 'ema_half_life_trades', fallback=20)
        min_samples = cfg.getint('PERFORMANCE_TUNING', 'min_samples', fallback=15)
        
        beta = {
            'ma_signal': cfg.getfloat('PERFORMANCE_TUNING', 'beta_ma', fallback=0.8),
            'volume_signal': cfg.getfloat('PERFORMANCE_TUNING', 'beta_volume', fallback=1.0),
            'bw_signal': cfg.getfloat('PERFORMANCE_TUNING', 'beta_bw', fallback=1.1),
            'atr_signal': cfg.getfloat('PERFORMANCE_TUNING', 'beta_atr', fallback=0.5),
            'adx_signal': cfg.getfloat('PERFORMANCE_TUNING', 'beta_adx', fallback=0.4),
        }
        
        trades = list(self.trades) if hasattr(self, 'trades') else []
        if len(trades) < min_samples:
            return base_weights.copy()
        
        import math
        alpha = 1 - math.exp(math.log(0.5) / hl) if hl > 0 else 1.0
        ema = {k: 0.0 for k in base_weights.keys()}
        
        for t in reversed(trades):
            ret = float(t.get('return_pct', 0.0))
            pnl_sign = 1.0 if ret > 0 else -1.0
            magnitude = abs(ret) / 100.0
            
            comp = t.get('signal_components') or {k: 1.0 for k in base_weights.keys()}
            raw = {k: pnl_sign * magnitude * float(comp.get(k, 0.0)) for k in base_weights.keys()}
            
            for k in base_weights.keys():
                ema[k] = (1 - alpha) * ema[k] + alpha * raw[k]
        
        w_new = {}
        for k in base_weights.keys():
            adj = beta.get(k, 0.5) * ema[k]
            factor = max(1.0 - max_adj, min(1.0 + max_adj, 1.0 + adj))
            w_new[k] = base_weights[k] * factor
        
        s = sum(w_new.values())
        return {k: v / s for k, v in w_new.items()} if s > EPSILON else base_weights.copy()
    
    def _p4a_detect_season(self) -> tuple:
        """P4a: 시장 계절 감지"""
        cfg = self.config
        if not cfg:
            return ("UNKNOWN", 1.0)
        
        try:
            alpha_winter = cfg.getfloat('SEASONAL_TUNING', 'alpha_winter', fallback=0.50)
            alpha_spring = cfg.getfloat('SEASONAL_TUNING', 'alpha_spring', fallback=0.85)
            alpha_summer = cfg.getfloat('SEASONAL_TUNING', 'alpha_summer', fallback=1.15)
            alpha_autumn = cfg.getfloat('SEASONAL_TUNING', 'alpha_autumn', fallback=0.75)
            
            recent_trades = list(self.trades)[-50:] if hasattr(self, 'trades') else []
            if not recent_trades:
                return ("UNKNOWN", 1.0)
            
            returns = [abs(float(t.get('return_pct', 0.0))) for t in recent_trades]
            if not returns:
                return ("UNKNOWN", 1.0)
            
            import statistics
            vol_index = statistics.mean(returns) if len(returns) > 1 else returns[0]
            
            if vol_index < 1.0:
                return ("WINTER", alpha_winter)
            elif vol_index < 2.0:
                return ("SPRING", alpha_spring)
            elif vol_index < 3.5:
                return ("SUMMER", alpha_summer)
            else:
                return ("AUTUMN", alpha_autumn)
        except Exception as e:
            logger.warning(f"[P4a] 계절 감지 실패: {e}")
            return ("UNKNOWN", 1.0)
    
    def _p4b_sector_adjust(self, symbol: str, current_weights: Dict[str, float]) -> Dict[str, float]:
        """P4b: 섹터별 보정"""
        cfg = self.config
        if not cfg or not symbol:
            return current_weights.copy()
        
        try:
            lookback = cfg.getint('SECTOR_TUNING', 'lookback_trades', fallback=20)            
            # 🆕 P4c: 메타 오버라이드
            max_sector_adj = self._meta_get('p4b.max_sector_adjust_pct',
                                            cfg.getfloat('SECTOR_TUNING', 'max_sector_adjust_pct', fallback=0.15))
            sensitivity = self._meta_get('p4b.rotation_sensitivity',
                                        cfg.getfloat('SECTOR_TUNING', 'rotation_sensitivity', fallback=0.70))
            
            cluster = self._get_symbol_cluster(symbol)
            if not cluster:
                return current_weights.copy()
            
            recent_trades = list(self.trades)[-lookback:] if hasattr(self, 'trades') else []
            if not recent_trades:
                return current_weights.copy()
            
            sector_trades = [t for t in recent_trades if t.get('symbol') in self.SECTOR_CLUSTERS.get(cluster, [])]
            if not sector_trades:
                return current_weights.copy()
            
            wins = sum(1 for t in sector_trades if t.get('success', False))
            sector_win_rate = wins / len(sector_trades) if sector_trades else 0.5
            sector_score = (sector_win_rate - 0.5) * sensitivity
            
            w_adjusted = current_weights.copy()
            for k in ['volume_signal', 'bw_signal']:
                if k in w_adjusted:
                    factor = max(1.0 - max_sector_adj, min(1.0 + max_sector_adj, 1.0 + sector_score))
                    w_adjusted[k] *= factor
            
            s = sum(w_adjusted.values())
            return {k: v / s for k, v in w_adjusted.items()} if s > EPSILON else current_weights.copy()
        except Exception as e:
            logger.warning(f"[P4b] 섹터 보정 실패: {e}")
            return current_weights.copy()
    
    def _meta_get(self, key, default):
        """메타 파라미터 우선 반환"""
        try:
            if hasattr(self, 'meta') and self.meta and self.meta.params.get(key) is not None:
                return self.meta.params.get(key)
        except Exception:
            pass
        return default
    
    def _get_symbol_cluster(self, symbol: str) -> str:
        """심볼의 섹터 반환"""
        if not symbol:
            return None
        for cluster, symbols in self.SECTOR_CLUSTERS.items():
            if symbol in symbols:
                return cluster
        return None
    
    def get_risk_metrics(self) -> Dict:
        """리스크 메트릭 - 캐시 활용 버전
        Returns:
            Dict: RiskManager가 필요로 하는 모든 메트릭
        """
        try:
            total = self.metrics['total_trades']
            cumulative = self.metrics['total_pnl']
            
            # ✅ 시뮬레이션 모드에서는 잔고 고정
            if self.mode == 'sim':
                current_balance = self.initial_balance
            else:
                current_balance = self.initial_balance + cumulative
            
            # ========================================
            # 1. 현재 drawdown 계산 (✅ 음수 방어 추가)
            # ========================================
            current_drawdown = 0.0
            if self.metrics['peak_balance'] > 0:
                current_drawdown = (current_balance - self.metrics['peak_balance']) / self.metrics['peak_balance']
                # ✅ 낙폭은 최대 -100%까지만 (파산)
                current_drawdown = max(current_drawdown, -1.0)
            
            # ========================================
            # 2. 🆕 일일 통계는 캐시된 값 사용
            # ========================================
            daily_stats = self.get_daily_stats()  # ← 캐싱된 메서드 호출
            
            # ========================================
            # 3. consecutive_errors
            # ========================================
            consecutive_errors = getattr(self, 'error_count', 0)
            
            # ========================================
            # 4. 메트릭 딕셔너리 생성
            # ========================================
            metrics = {
                'win_rate': self.metrics['wins'] / max(1, total),
                'consecutive_losses': self.metrics['consecutive_losses'],
                'consecutive_errors': consecutive_errors,
                'daily_loss_amount': daily_stats['loss'],       # ← 캐시 활용
                'daily_profit_sum': max(0, daily_stats['pnl']), # ← 캐시 활용
                'daily_net_pnl': daily_stats['pnl'],           # ← 캐시 활용
                'max_drawdown': self.metrics['max_drawdown'],
                'current_drawdown': current_drawdown,
                'total_pnl': self.metrics['total_pnl'],
                'total_trades': total,
                'current_balance': current_balance,
                'peak_balance': self.metrics['peak_balance']
            }
            
            # ========================================
            # 5. 디버깅 로그 (변경 시만)
            # ========================================
            if not hasattr(self, '_last_logged_metrics'):
                self._last_logged_metrics = {}
            
            # 값이 변경되었을 때만 로그
            if (self._last_logged_metrics.get('consecutive_losses') != metrics['consecutive_losses'] or
                self._last_logged_metrics.get('daily_loss_amount') != metrics['daily_loss_amount']):
                
                logger.debug(f"📊 [get_risk_metrics] 전달값:")
                logger.debug(f"   - consecutive_losses: {metrics['consecutive_losses']}")
                logger.debug(f"   - daily_loss_amount: {metrics['daily_loss_amount']:,.0f}원")
                logger.debug(f"   - daily_net_pnl: {metrics['daily_net_pnl']:+,.0f}원")
                logger.debug(f"   - max_drawdown: {metrics['max_drawdown']*100:.2f}%")
                logger.debug(f"   - current_balance: {metrics['current_balance']:,.0f}원")
                
                self._last_logged_metrics = metrics.copy()
            
            return metrics
            
        except Exception as e:
            logger.error(f"❌ 리스크 메트릭 생성 실패: {e}")
            import traceback
            logger.debug(f"상세:\n{traceback.format_exc()}")
            
            # 폴백: 안전한 기본값 반환
            return {
                'win_rate': 0.5,
                'consecutive_losses': 0,
                'consecutive_errors': 0,
                'daily_loss_amount': 0.0,
                'daily_profit_sum': 0.0,
                'daily_net_pnl': 0.0,
                'max_drawdown': 0.0,
                'current_drawdown': 0.0,
                'total_pnl': 0.0,
                'total_trades': 0,
                'current_balance': self.initial_balance,
                'peak_balance': self.initial_balance
            }           

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 🔗 P4d: Correlation Adapter (형제코인 상관·선행-후행 계층)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
import math
import statistics
from collections import deque, defaultdict

class P4dCorrelationAdapter:
    """
    형제코인 상관/선행-후행 기반 가중 보정기
    - rolling correlation(동시) + cross-corr(지연) 간단 조합
    - seed_pairs로 초기 연결 설정 후, 자동 추론로 보완
    - 출력: symbol별 보정 factor dict({'ma_signal':.., 'volume_signal':.., ...})
    """
    def __init__(self, pm):
        self.pm = pm
        self.cfg = pm.config
        self.enabled = self.cfg.getboolean('CORRELATION_TUNING', 'enabled', fallback=False) if self.cfg else False
        
        if not self.enabled:
            return
        
        self.window = self.cfg.getint('CORRELATION_TUNING', 'rolling_window', fallback=120)
        self.max_lag = self.cfg.getint('CORRELATION_TUNING', 'max_lag_minutes', fallback=15)
        self.c_thr = self.cfg.getfloat('CORRELATION_TUNING', 'corr_threshold', fallback=0.70)
        self.l_thr = self.cfg.getfloat('CORRELATION_TUNING', 'leadlag_threshold', fallback=0.55)
        self.cap = self.cfg.getfloat('CORRELATION_TUNING', 'weight_boost_cap', fallback=0.12)
        self.decay_minutes = self.cfg.getint('CORRELATION_TUNING', 'decay_minutes', fallback=30)
        self.min_samples = self.cfg.getint('CORRELATION_TUNING', 'min_samples', fallback=60)
        
        self.boosts = {
            'ma_signal':    self.cfg.getfloat('CORRELATION_TUNING','boost_ma', fallback=0.04),
            'volume_signal':self.cfg.getfloat('CORRELATION_TUNING','boost_volume', fallback=0.06),
            'bw_signal':    self.cfg.getfloat('CORRELATION_TUNING','boost_bw', fallback=0.08),
            'atr_signal':   self.cfg.getfloat('CORRELATION_TUNING','boost_atr', fallback=0.00),
            'adx_signal':   self.cfg.getfloat('CORRELATION_TUNING','boost_adx', fallback=0.02),
        }
        
        # seed pair 파싱
        self.pairs = self._parse_seed_pairs(
            self.cfg.get('CORRELATION_TUNING','seed_pairs', fallback='')
        )
        
        # 리턴 버퍼: symbol -> deque of returns
        self.retbuf = defaultdict(lambda: deque(maxlen=self.window))
        self._last_update_ts = 0
        
        logger.info(f"✅ [P4d] 초기화 완료 | 형제군: {len(self.pairs)}개")

    def _parse_seed_pairs(self, s: str):
        """seed_pairs 문자열 파싱"""
        mapping = defaultdict(set)
        for blk in [x.strip() for x in s.split(';') if x.strip()]:
            try:
                if ':' not in blk:
                    continue
                leader, foll = blk.split(':', 1)
                followers = [y.strip() for y in foll.split(',') if y.strip()]
                for f in followers:
                    mapping[leader.strip()].add(f)
            except Exception as e:
                logger.debug(f"[P4d] seed pair 파싱 오류: {blk} - {e}")
        return mapping

    def ingest_tick_returns(self, symbol: str, ret: float):
        """호출부가 1m 등 주기로 수익률을 넣어줌(±%)"""
        if not self.enabled:
            return
        
        # ✅ 이상치 필터링 (±50% 초과 수익률 제외)
        if abs(ret) > 0.50:
            logger.debug(f"[P4d] 이상치 필터링: {symbol} ret={ret:.2%}")
            return
        
        self.retbuf[symbol].append(float(ret))

    def _xcorr(self, a: deque, b: deque, max_lag: int):
        """단순 교차상관: lag∈[0..max_lag]에서 최대 상관 및 lag 반환"""
        if len(a) < 10 or len(b) < 10:
            return (0.0, 0)
        
        arr_a, arr_b = list(a), list(b)
        n = min(len(arr_a), len(arr_b))
        arr_a, arr_b = arr_a[-n:], arr_b[-n:]
        
        best = (0.0, 0)
        
        for lag in range(0, max_lag + 1):
            if n - lag <= 5:
                break
            
            xa = arr_a[:n-lag]
            xb = arr_b[lag:]
            
            if len(xa) != len(xb) or len(xa) < 5:
                continue
            
            # 피어슨 상관
            try:
                ma = statistics.mean(xa)
                mb = statistics.mean(xb)
                sa = statistics.pstdev(xa) or 1e-9
                sb = statistics.pstdev(xb) or 1e-9
                
                cov = statistics.mean([(xa[i]-ma)*(xb[i]-mb) for i in range(len(xa))])
                r = cov / (sa * sb)
                
                # ✅ 상관계수 검증 (-1 ~ 1 범위)
                if -1.0 <= r <= 1.0 and abs(r) > abs(best[0]):
                    best = (r, lag)
            except Exception as e:
                logger.debug(f"[P4d] xcorr 계산 오류: {e}")
        
        return best

    def suggest_boost(self, symbol: str):
        """
        symbol에 적용할 보정factor dict 반환.
        리더(선행) → follower(후행) 관계를 찾으면 follower 보정.
        """
        if not self.enabled:
            return {}
        
        # ✅ 표본 부족 시 skip
        symbol_buf_len = len(self.retbuf.get(symbol, []))
        if symbol_buf_len < self.min_samples:
            return {}
        
        # 후보 리더 집합
        leaders = set(self.pairs.keys())
        
        total_boost = {}
        
        for leader in leaders:
            if leader not in self.retbuf:
                continue
            
            leader_r = self.retbuf[leader]
            
            # ✅ 리더 버퍼도 충분한지 확인
            if len(leader_r) < self.min_samples:
                continue
            
            for follower in self.pairs[leader]:
                if follower != symbol or follower not in self.retbuf:
                    continue
                
                # (1) 동시 상관 체크
                corr0, _ = self._xcorr(leader_r, self.retbuf[follower], max_lag=0)
                
                # (2) 선행-후행 래그 검사
                corr_lag, lag = self._xcorr(leader_r, self.retbuf[follower], self.max_lag)
                
                ok_simul = abs(corr0) >= self.c_thr
                ok_lead = corr_lag >= self.l_thr and lag > 0
                
                strength = 0.0
                
                if ok_simul:
                    strength = max(strength, min(1.0, abs(corr0)))
                
                if ok_lead:
                    # ✅ 래그 패널티 적용 (래그 클수록 신뢰도 ↓)
                    lag_penalty = 1.0 - lag / (self.max_lag + 1)
                    strength = max(strength, min(1.0, corr_lag * lag_penalty))
                
                if strength > 0:
                    # 보정: 구성요소별 base boost × strength
                    for k, base in self.boosts.items():
                        total_boost[k] = total_boost.get(k, 0.0) + base * strength
        
        # ✅ 캡 적용
        for k in list(total_boost.keys()):
            total_boost[k] = max(-self.cap, min(self.cap, total_boost[k]))
        
        # ✅ 약세장 보정: 과도한 부스팅 억제
        if total_boost:
            # 전체 합계가 캡의 80%를 초과하면 비례 축소
            total_abs = sum(abs(v) for v in total_boost.values())
            if total_abs > self.cap * 0.8:
                scale = (self.cap * 0.8) / total_abs
                total_boost = {k: v * scale for k, v in total_boost.items()}
        
        return total_boost
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 🆕 P4c: Meta Optimizer
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
class MetaOptimizer:
    """P4c: 학습률·감쇠율·민감도 자동 최적화"""
    
    def __init__(self, pm):
        self.pm = pm
        self.cfg = pm.config
        self.enabled = self.cfg.getboolean('META_OPTIMIZER', 'enabled', fallback=True) if self.cfg else False
        self.params = {
            'p4.max_adjust_pct': None,
            'p4a.alpha_scale': 1.0,
            'p4b.max_sector_adjust_pct': None,
            'p4b.rotation_sensitivity': None
        }
        self._last_update_ts = 0
        
        if self.cfg:
            self.params['p4.max_adjust_pct'] = self.cfg.getfloat('PERFORMANCE_TUNING', 'max_adjust_pct', fallback=0.20)
            self.params['p4b.max_sector_adjust_pct'] = self.cfg.getfloat('SECTOR_TUNING', 'max_sector_adjust_pct', fallback=0.15)
            self.params['p4b.rotation_sensitivity'] = self.cfg.getfloat('SECTOR_TUNING', 'rotation_sensitivity', fallback=0.70)
    
    def _clip(self, v, lo, hi):
        return max(lo, min(hi, v))
    
    def _calc_weight_volatility(self, window=30):
        if not hasattr(self.pm, 'weights_history'):
            self.pm.weights_history = []
        hist = self.pm.weights_history[-window:]
        if len(hist) < 2:
            return 0.0
        deltas = []
        for i in range(1, len(hist)):
            prev, cur = hist[i-1], hist[i]
            keys = set(prev.keys()) | set(cur.keys())
            delta = max(abs(cur.get(k, 0) - prev.get(k, 0)) for k in keys)
            deltas.append(delta)
        import statistics
        return statistics.mean(deltas) if deltas else 0.0
    
    def _calc_regime_stability(self, window=40):
        trades = list(self.pm.trades)[-window:]
        if len(trades) < 5:
            return 0.0
        seq = [t.get('market_regime', 'UNKNOWN') for t in trades]
        switches = sum(1 for i in range(1, len(seq)) if seq[i] != seq[i-1])
        return switches / (len(seq) - 1)
    
    def _calc_perf(self):
        total = self.pm.metrics.get('total_trades', 0)
        wins = self.pm.metrics.get('wins', 0)
        winrate = wins / total if total > 0 else 0.5
        dd = abs(self.pm.metrics.get('max_drawdown', 0.0))
        return winrate, dd
    
    def _bands(self, val, bands):
        arr = [float(x) for x in bands.split(',')]
        arr.sort()
        idx = 0
        for b in arr:
            if val < b:
                break
            idx += 1
        return idx
    
    def suggest(self):
        cfg = self.cfg
        if not (self.enabled and cfg):
            return self.params
        
        import time
        upd_minutes = cfg.getint('META_OPTIMIZER', 'update_interval_minutes', fallback=15)
        if time.time() - self._last_update_ts < upd_minutes * 60:
            return self.params
        
        min_samples = cfg.getint('META_OPTIMIZER', 'min_samples', fallback=40)
        total_trades = self.pm.metrics.get('total_trades', 0)
        if total_trades < min_samples:
            self._last_update_ts = time.time()
            return self.params
        
        # 메타 지표
        wvol = self._calc_weight_volatility(cfg.getint('META_OPTIMIZER', 'weight_vol_window', fallback=30))
        rstab = self._calc_regime_stability(cfg.getint('META_OPTIMIZER', 'regime_stability_window', fallback=40))
        winrate, dd = self._calc_perf()
        
        target_max_delta = cfg.getfloat('META_OPTIMIZER', 'target_max_delta', fallback=0.08)
        dd_band = self._bands(dd, cfg.get('META_OPTIMIZER', 'dd_bands', fallback='0.05,0.10,0.20'))
        wr_band = self._bands(winrate, cfg.get('META_OPTIMIZER', 'winrate_bands', fallback='0.45,0.55,0.65'))
        
        p4_max = self.params['p4.max_adjust_pct']
        p4b_cap = self.params['p4b.max_sector_adjust_pct']
        p4b_sens = self.params['p4b.rotation_sensitivity']
        
        lr_p4 = cfg.getfloat('META_OPTIMIZER', 'lr_p4', fallback=0.20)
        lr_p4a = cfg.getfloat('META_OPTIMIZER', 'lr_p4a', fallback=0.15)
        lr_p4b = cfg.getfloat('META_OPTIMIZER', 'lr_p4b', fallback=0.20)
        
        # P4 조정
        delta_dir = -1 if (wvol > target_max_delta or dd_band >= 2) else (+1 if (wr_band >= 2 and wvol < target_max_delta / 2) else 0)
        p4_max_new = p4_max * (1 + lr_p4 * delta_dir)
        p4_max_new = self._clip(p4_max_new,
                                cfg.getfloat('META_OPTIMIZER', 'p4_max_adjust_min', fallback=0.05),
                                cfg.getfloat('META_OPTIMIZER', 'p4_max_adjust_max', fallback=0.30))
        
        # P4a 조정
        alpha_scale = 1.0
        if rstab > 0.5 or wvol > target_max_delta:
            alpha_scale = 1.0 - lr_p4a * 0.5
        elif wr_band >= 2 and dd_band <= 0 and wvol < target_max_delta / 2:
            alpha_scale = 1.0 + lr_p4a * 0.5
        alpha_scale = self._clip(alpha_scale,
                                cfg.getfloat('META_OPTIMIZER', 'season_alpha_floor', fallback=0.80),
                                cfg.getfloat('META_OPTIMIZER', 'season_alpha_ceil', fallback=1.20))
        
        # P4b 조정
        adj_dir_b = -1 if (wvol > target_max_delta or dd_band >= 2) else (+1 if (wr_band >= 2 and wvol < target_max_delta / 2) else 0)
        p4b_cap_new = self._clip(p4b_cap * (1 + lr_p4b * 0.5 * adj_dir_b),
                                 cfg.getfloat('META_OPTIMIZER', 'p4b_sector_cap_min', fallback=0.05),
                                 cfg.getfloat('META_OPTIMIZER', 'p4b_sector_cap_max', fallback=0.20))
        p4b_sens_new = self._clip(p4b_sens * (1 + lr_p4b * 0.5 * adj_dir_b),
                                  cfg.getfloat('META_OPTIMIZER', 'p4b_sensitivity_min', fallback=0.30),
                                  cfg.getfloat('META_OPTIMIZER', 'p4b_sensitivity_max', fallback=0.90))
        
        self.params.update({
            'p4.max_adjust_pct': p4_max_new,
            'p4a.alpha_scale': alpha_scale,
            'p4b.max_sector_adjust_pct': p4b_cap_new,
            'p4b.rotation_sensitivity': p4b_sens_new
        })
        self._last_update_ts = time.time()
        
        logger.info(
            f"🧠 [P4c] meta-update | wvol:{wvol:.3f} rstab:{rstab:.2f} win:{winrate:.2f} dd:{dd:.2%} "
            f"→ P4:±{p4_max_new:.2f} | P4a:αx{alpha_scale:.2f} | P4b:cap±{p4b_cap_new:.2f},sens:{p4b_sens_new:.2f}"
        )
        return self.params     

# **END**