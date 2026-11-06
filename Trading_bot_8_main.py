#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
업비트 자동매매 봇 v1.0 - Main Module
========================================
책임: 프로그램 진입점, 리포트 생성, 명령행 처리
"""

import os

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 🔧 환경 변수 초기화 함수
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
def setup_environment_variables():
    """
    NumPy/OpenBLAS 멀티스레딩 비활성화 및 로그 억제    
    목적:
      - WinError 233 방지 (stdout/stderr 파이프 에러)
      - CPU 사용률 최적화 (단일 스레드 강제)
      - 불필요한 OpenBLAS 내부 로그 억제    
    적용 시점:
      - 프로그램 최초 시작 시 (import 전)
      - 모든 모듈 로드 전에 환경 변수 설정 필요
    """
    env_config = {
        "OPENBLAS_NUM_THREADS": "1",       # OpenBLAS 스레드 1개로 제한
        "MKL_NUM_THREADS": "1",            # Intel MKL 스레드 1개로 제한
        "NUMEXPR_NUM_THREADS": "1",        # NumExpr 스레드 1개로 제한
        "NUMEXPR_MAX_THREADS": "1",        # NumExpr 최대 스레드 1개
        "OMP_NUM_THREADS": "1",            # OpenMP 스레드 1개로 제한
        "OPENBLAS_VERBOSE": "0",           # OpenBLAS 내부 로그 완전 억제 (핵심!)
    }
    
    for key, value in env_config.items():
        os.environ[key] = value

# 즉시 실행 (import 전에 설정 필요)
setup_environment_variables()
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

import warnings
warnings.filterwarnings("ignore", category=RuntimeWarning)
warnings.filterwarnings("ignore", category=FutureWarning)

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 🛡️ WinError 233 완전 차단 (Option B: 콘솔 감지형)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
import os, sys, logging

# 콘솔이 없을 때만 무음 처리
if not sys.stdout or not sys.stdout.isatty():
    try:
        null_stream = open(os.devnull, "w", encoding="utf-8", errors="ignore")
        sys.stdout = null_stream
        sys.stderr = null_stream
    except Exception:
        pass

# 로깅 내부 예외 억제 (안전 보강)
logging.raiseExceptions = False

import asyncio
import argparse
import signal
import json
import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Dict, Optional

from Trading_bot_1_core import logger, setup_logging
from Trading_bot_2_config import ConfigManager
from Trading_bot_7_bot import UnifiedTradingBot
from global_constants import EXIT_CODE_SUCCESS, EXIT_CODE_FAILURE, SEPARATOR_LENGTH

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 🔄 SQLite Lock 재시도 데코레이터
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
import time
from functools import wraps

def retry_on_db_lock(max_retries=3, delay=0.5):
    """
    SQLite 'database is locked' 에러 발생 시 재시도하는 데코레이터    
    Args:
        max_retries: 최대 재시도 횟수 (기본 3회)
        delay: 재시도 간 대기 시간 초 (기본 0.5초)    
    사용 예시:
        @retry_on_db_lock(max_retries=3, delay=0.5)
        def generate_excel_report(db_path, ...):
            # DB 접근 코드
    """
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            last_exception = None
            
            for attempt in range(max_retries):
                try:
                    return func(*args, **kwargs)
                    
                except sqlite3.OperationalError as e:
                    error_msg = str(e).lower()
                    
                    # 'database is locked' 에러만 재시도
                    if "database is locked" in error_msg or "locked" in error_msg:
                        last_exception = e
                        
                        if attempt < max_retries - 1:
                            # 재시도 가능
                            logger.debug(
                                f"🔄 DB Lock 감지 [{func.__name__}], "
                                f"{delay}초 후 재시도 ({attempt + 1}/{max_retries})"
                            )
                            time.sleep(delay)
                        else:
                            # 마지막 시도 실패
                            logger.warning(
                                f"⚠️ DB Lock 재시도 {max_retries}회 실패 [{func.__name__}]: {e}"
                            )
                    else:
                        # Lock 외의 다른 OperationalError는 즉시 raise
                        raise
            
            # 모든 재시도 실패 시 마지막 예외 raise
            raise last_exception
            
        return wrapper
    return decorator
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

# ========================================
# 헬퍼 함수 (메서드 밖에!)
# ========================================
def write_and_print(file_obj, text):
    """파일에 쓰면서 화면에도 출력"""
    file_obj.write(text)
    file_obj.flush()  # ← 버퍼 즉시 비우기 (유실 방지)
    print(text, end='', flush=True)  # 화면 출력 + 즉시 flush

def get_min_signal_threshold(total_trades: int) -> int:
    """거래 수에 따른 신호별 최소 임계값 동적 계산
    Args:
        total_trades: 전체 거래 수
    Returns:
        최소 거래 수 임계값
    전략:
        < 20건: 1회 (초기 학습 - 모든 패턴 표시)
        < 50건: 3회 (패턴 파악)
        < 100건: 5회 (검증)
        >= 100건: 10회 (확신)
    """
    if total_trades < 20:
        return 1
    elif total_trades < 50:
        return 3
    elif total_trades < 100:
        return 5
    else:
        return 10

def build_date_filter(date_from: Optional[str] = None, date_to: Optional[str] = None) -> tuple:
    """날짜 필터 SQL 조건 생성    
    Args:
        date_from: 시작 날짜 (YYYY-MM-DD)
        date_to: 종료 날짜 (YYYY-MM-DD)    
    Returns:
        (where_clause, params) 튜플
    """
    conditions = []
    params = []
    
    if date_from:
        conditions.append("DATE(timestamp) >= ?")
        params.append(date_from)
    
    if date_to:
        conditions.append("DATE(timestamp) <= ?")
        params.append(date_to)
    
    if conditions:
        where_clause = " AND " + " AND ".join(conditions)
    else:
        where_clause = ""
    
    return where_clause, params


def _safe_parse_dt(value):
    """리포트/로그 시간 문자열을 datetime으로 변환"""
    if not value:
        return None
    text = str(value).strip()
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00")).replace(tzinfo=None)
    except Exception:
        pass
    try:
        return datetime.strptime(text[:19], "%Y-%m-%d %H:%M:%S")
    except Exception:
        return None


def _iter_report_log_files(log_dir: Path, date_from=None, date_to=None):
    """리포트 날짜 범위에 해당하는 일자 로그만 반환 (SSoT)."""
    _dfrom = date_from.replace("-", "") if date_from else None
    _dto = date_to.replace("-", "") if date_to else None
    for log_file in sorted(log_dir.glob("trading_bot_*.log")):
        _d = log_file.stem.rsplit("_", 1)[-1]
        if len(_d) != 8 or not _d.isdigit():
            continue
        if _dfrom and _d < _dfrom:
            continue
        if _dto and _d > _dto:
            continue
        yield log_file


def _load_market_selection_sources(log_dir: Path, date_from=None, date_to=None):
    """대상선정 source 로그 파싱"""
    import re
    from collections import defaultdict
    events = defaultdict(list)
    rank_pattern = re.compile(
        r"^(?P<ts>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}).*?"
        r"\s+\d+\.\s+(?P<symbol>KRW-[A-Z0-9]+)\s+\|\s+source=(?P<source>[^|\s]+)"
    )
    for log_file in _iter_report_log_files(log_dir, date_from, date_to):
        try:
            with open(log_file, "r", encoding="utf-8", errors="ignore") as lf:
                for line in lf:
                    m = rank_pattern.search(line)
                    if not m:
                        continue
                    ts = _safe_parse_dt(m.group("ts"))
                    symbol = m.group("symbol")
                    source = m.group("source")
                    if ts and symbol and source:
                        events[symbol].append((ts, source))
        except Exception as e:
            logger.warning(f"대상선정 source 로그 파싱 실패 ({log_file.name}): {e}")
    for symbol in events:
        events[symbol].sort(key=lambda x: x[0])
    return events

def _find_market_selection_source(selection_events, symbol, entry_time, exit_time=None):
    """진입시각 직전 가장 가까운 대상선정 source 반환"""
    base_time = entry_time
    if not symbol:
        return "-"
    if not base_time:
        return "BUY_TIME_MISSING"
    
    rows = selection_events.get(symbol) or []
    chosen = None
    for ts, source in rows:
        if ts <= base_time:
            chosen = (ts, source)
        else:
            break
    if not chosen:
        return "-"

    ts, source = chosen
    age_min = max(0, int((base_time - ts).total_seconds() // 60))

    source_base = str(source).split("(", 1)[0]

    source_label = {
        "SURGE_FILL": "SURGE",
        "REVERSAL_BIRTH_WATCH": "REVERSAL",
        "TRACK2_BIRTH_WATCH": "TRACK2",
        "MOMENTUM_KEEPALIVE": "MOMENTUM",

        "MINI_GC_0B": "편입:GC0B",
        "MINI_GC_1B": "편입:GC1B",
        "MINI_GC_2B": "편입:GC2B",
        "MINI_GC_3B": "편입:GC3B",
        "MINI_PRE_GC_ETA": "편입:PRE_GC",
        "MINI_PRE_GC_SLOW": "편입:PRE_GC_SLOW",
    }.get(source_base, source_base)

    if age_min > 15:
        return f"오래됨:{source_label}(편입후{age_min}m)"

    return f"{source_label}(편입후{age_min}m)"

def _load_buy_execution_times(log_dir: Path, date_from=None, date_to=None):
    """BUY 체결 로그에서 실제 진입시각 복구"""
    import re
    from collections import defaultdict

    events = defaultdict(list)

    patterns = [
        re.compile(
            r"^(?P<ts>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}).*?"
            r"\[매수실행\]\s+(?P<symbol>KRW-[A-Z0-9]+)\s+성공"
        ),
        re.compile(
            r"^(?P<ts>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}).*?"
            r"거래 성공:\s*BUY\s+(?P<symbol>KRW-[A-Z0-9]+)"
        ),
    ]

    for log_file in _iter_report_log_files(log_dir, date_from, date_to):
        try:
            with open(log_file, "r", encoding="utf-8", errors="ignore") as lf:
                for line in lf:
                    for pattern in patterns:
                        m = pattern.search(line)
                        if not m:
                            continue

                        ts = _safe_parse_dt(m.group("ts"))
                        symbol = m.group("symbol")

                        if ts and symbol:
                            events[symbol].append(ts)
                        break

        except Exception as e:
            logger.warning(f"BUY 실행 로그 파싱 실패 ({log_file.name}): {e}")

    for symbol in events:
        events[symbol].sort()

    return events


def _find_buy_execution_time(buy_events, symbol, exit_time):
    """SELL 시각 직전 가장 가까운 BUY 실행시각 반환"""
    if not symbol or not exit_time:
        return None

    rows = buy_events.get(symbol) or []
    chosen = None

    for ts in rows:
        if ts <= exit_time:
            chosen = ts
        else:
            break

    return chosen


# ========================================
# 1. 거래 내역 리포트
# ========================================
@retry_on_db_lock(max_retries=3, delay=0.5)  # ✅ 데코레이터 추가
def generate_trades_report(db_path, output_dir, config: ConfigManager, date_from=None, date_to=None):
    """거래 상세 리포트 생성 (콘솔, DB Lock 자동 재시도)"""
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    report_file = output_dir / f"trades_{timestamp}.txt"
    
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # 테이블 존재 확인
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='trade_details'")
        if not cursor.fetchone():
            logger.warning("⚠️ trade_details 테이블이 없습니다. 거래 데이터가 아직 없는 것 같아요.")
            conn.close()
            return
        
        # 날짜 필터 생성
        date_filter, date_params = build_date_filter(date_from, date_to)
        
        # 컬럼 확인
        cursor.execute("PRAGMA table_info(trade_details)")
        columns = [row[1] for row in cursor.fetchall()]
        
        # 사용 가능한 컬럼으로 쿼리 구성
        select_columns = []
        
        if 'timestamp' in columns:
            select_columns.append('timestamp')
        if 'symbol' in columns:
            select_columns.append('symbol')
        if 'side' in columns:
            select_columns.append('side')
        
        # 가격
        if 'price' in columns:
            select_columns.append('price')
        elif 'executed_price' in columns:
            select_columns.append('executed_price as price')
        elif 'avg_price' in columns:
            select_columns.append('avg_price as price')
        else:
            select_columns.append('0 as price')
        
        # 수량
        if 'volume' in columns:
            select_columns.append('volume')
        elif 'executed_volume' in columns:
            select_columns.append('executed_volume as volume')
        else:
            select_columns.append('0 as volume')
        
        # 손익
        if 'pnl' in columns:
            select_columns.append('pnl')
        else:
            select_columns.append('0 as pnl')
        
        if 'return_pct' in columns:
            select_columns.append('return_pct')
        else:
            select_columns.append('0 as return_pct')
        
        # 추가 정보
        if 'reason' in columns:
            select_columns.append('reason')
        else:
            select_columns.append("'' as reason")
        
        if 'confidence' in columns:
            select_columns.append('confidence')
        else:
            select_columns.append('0 as confidence')
        
        if 'market_regime' in columns:
            select_columns.append('market_regime')
        else:
            select_columns.append("'' as market_regime")
        
        # 타임프레임
        if 'timeframe' in columns:
            select_columns.append('timeframe')
        else:
            select_columns.append("'' as timeframe")
        
        # 옛 행은 price 칸이 비어 entry_price · exit_price 에만 가격 · 열마다 있을 때만 질의 (묶음 v4)
        _legacy_px_cols = ""
        if 'entry_price' in columns:
            _legacy_px_cols += ",\n                s.entry_price AS legacy_entry_price"
        if 'exit_price' in columns:
            _legacy_px_cols += ",\n                s.exit_price AS legacy_exit_price"

        # 전체 거래 조회 (진입 체제 + 진입시각 추가)
        query = f"""
            SELECT 
                s.timestamp,
                s.symbol,
                s.side,
                s.price,
                s.volume,
                s.pnl,
                s.return_pct,
                s.reason,
                s.confidence,
                s.market_regime as exit_regime,
                s.timeframe,
                s.market_regime as entry_regime,
                (
                    SELECT b.timestamp
                    FROM trade_details b
                    WHERE TRIM(UPPER(b.side)) = 'BUY'
                      AND b.symbol = s.symbol
                      AND b.timestamp <= s.timestamp
                    ORDER BY b.timestamp DESC
                    LIMIT 1
                ) as entry_timestamp,
                (
                    SELECT b.signal_components
                    FROM trade_details b
                    WHERE TRIM(UPPER(b.side)) = 'BUY'
                      AND b.symbol = s.symbol
                      AND b.timestamp <= s.timestamp
                    ORDER BY b.timestamp DESC
                    LIMIT 1
                ) as entry_components{_legacy_px_cols},
                s.id AS sell_id
            FROM trade_details s
            WHERE TRIM(UPPER(s.side)) = 'SELL'{date_filter}
            ORDER BY s.timestamp DESC
            LIMIT 100
        """

        cursor.execute(query, date_params)
        trades = cursor.fetchall()
        _trade_cols = [d[0] for d in cursor.description] if cursor.description else []  # 아래 커서 재사용 전에 열 이름 저장 (묶음 v4)
        # REPORT-TRUTH v4.1: 매도 ↔ 매수 연결 = 같은 종목 가장 최근 매수의 진입 신호 지문 (confidence + signal_components 핵심 5 값) 이 매도 행과 같을 때만 (봇이 진입 신호를 매도 행에 복사) · 매수 1 건의 누적 매도 수량 ≤ 매수 수량 · 손익 일치는 연결 근거로 쓰지 않음.
        def _rt_num(_value):
            try:
                _number = float(_value)
                return _number if _number == _number and abs(_number) != float("inf") else None
            except (TypeError, ValueError, OverflowError):
                return None
        cursor.execute(f"SELECT id FROM trade_details WHERE TRIM(UPPER(side)) = 'SELL'{date_filter}", date_params)
        _sell_ids = {_r[0] for _r in cursor.fetchall()}
        _fee_c = 'fee' if 'fee' in columns else 'NULL'
        _conf_c = 'confidence' if 'confidence' in columns else 'NULL'
        _sc_c = 'signal_components' if 'signal_components' in columns else 'NULL'
        cursor.execute(f"SELECT id, symbol, TRIM(UPPER(side)), price, volume, {_fee_c}, pnl, {_conf_c}, {_sc_c} FROM trade_details ORDER BY timestamp, id")
        def _rt_fp(_cf, _sc):
            # 진입 신호 지문 = confidence + signal_components 핵심 5 값 (봇이 매수 신호를 매도 행에 복사 · confidence 만으로는 0.45 등 상한값이 겹침)
            try:
                _d = json.loads(_sc) if _sc else None
            except (TypeError, ValueError):
                _d = None
            if _rt_num(_cf) is None or not isinstance(_d, dict):
                return None
            _k = tuple(_rt_num(_d.get(_n)) for _n in ("current_price", "atr_pct", "dur_5x10_bars", "ma_signal", "volume_signal"))
            return None if any(_v is None for _v in _k[:3]) or _k[0] <= 0 else (_rt_num(_cf),) + _k  # [R894] ma_signal · volume_signal 없는 매수 (J2 직행) 도 None 끼리 연결
        _last_buy = {}
        _sold = {}
        _chk = {}
        _rt_bad_ids = set()
        _rt_link = {}
        for _id, _sym, _sd, _px, _vol, _fe, _pn, _cf, _sc in cursor.fetchall():
            if _sd == 'BUY':
                _last_buy[_sym] = (_id, _rt_fp(_cf, _sc), (_px, _vol, _fe))
            elif _sd == 'SELL':
                _qty = _rt_num(_vol)
                _b = _last_buy.get(_sym)
                _bq = _rt_num(_b[2][1]) if _b else None
                _sell_fp = _rt_fp(_cf, _sc)
                _fp_ok = _b is not None and _b[1] is not None and _sell_fp == _b[1]
                _used = _sold.get(_b[0], 0.0) if _b else 0.0
                _qty_ok = _fp_ok and _qty is not None and _qty > 0 and _bq is not None and _bq > 0 and _used + _qty <= _bq * (1 + 1e-6)
                if _fp_ok:
                    # 지문이 맞은 매도만 그 매수 건 사용 수량에 더함 · 수량 모르는 매도가 끼면 그 매수 건 다음 매도도 미확정
                    _sold[_b[0]] = _used + _qty if (_qty is not None and _qty > 0) else float("inf")
                elif _b is not None and _b[1] is not None and _sell_fp is None:
                    _sold[_b[0]] = float("inf")
                if _id in _sell_ids:
                    _chk[_id] = (_px, _vol, _fe, _pn) + (_b[2] if _qty_ok else (None, None, None))
                    _rt_link[_id] = _b[0] if _qty_ok else None

        selection_events = _load_market_selection_sources(
            Path("outputs/logs"), date_from, date_to
        )
        buy_events = _load_buy_execution_times(
            Path("outputs/logs"), date_from, date_to
        )
        
        # 🔍 BUY 레코드 확인
        buy_check_query = f"""
            SELECT COUNT(*) as buy_count
            FROM trade_details
            WHERE UPPER(side) = 'BUY'{date_filter}
        """
        cursor.execute(buy_check_query, date_params)
        result = cursor.fetchone()
        buy_count = result[0] if result else 0
        
        logger.info(f"📊 [진단] 같은 기간 BUY 레코드: {buy_count}건")
        
        # SELL 레코드 개수도 확인
        sell_check_query = f"""
            SELECT COUNT(*) as sell_count
            FROM trade_details
            WHERE UPPER(side) = 'SELL'{date_filter}
        """
        cursor.execute(sell_check_query, date_params)
        result = cursor.fetchone()
        sell_count = result[0] if result else 0
        
        logger.info(f"📊 [진단] 같은 기간 SELL 레코드: {sell_count}건")
        logger.info(f"📊 [진단] BUY/SELL 비율: {buy_count}/{sell_count}")
        
        # 전체 통계 계산
        stats_query_parts = []
        stats_query_parts.append("COUNT(*) as total_trades")
        
        if 'pnl' in columns:
            stats_query_parts.append("SUM(CASE WHEN pnl > 0 THEN 1 ELSE 0 END) as wins")
            stats_query_parts.append("SUM(pnl) as total_pnl")
        else:
            stats_query_parts.append("0 as wins")
            stats_query_parts.append("0 as total_pnl")
        
        if 'return_pct' in columns:
            stats_query_parts.append("AVG(return_pct) as avg_return")
            stats_query_parts.append("MAX(return_pct) as max_return")
            stats_query_parts.append("MIN(return_pct) as min_return")
        else:
            stats_query_parts.append("0 as avg_return")
            stats_query_parts.append("0 as max_return")
            stats_query_parts.append("0 as min_return")
        
        stats_query = f"""
            SELECT {', '.join(stats_query_parts)}
            FROM trade_details
            WHERE (side = 'SELL' OR side = 'sell'){date_filter}
        """
        
        try:
            cursor.execute(stats_query, date_params)
            stats = cursor.fetchone()
        except sqlite3.OperationalError as e:
            stats = (0, 0, 0, 0, 0, 0)
        
        # ========================================
        # 🔥 수정 1: 매도 사유별 통계 - CASE 순서 변경
        # ========================================
        sell_reason_stats = []
        if 'reason' in columns and 'pnl' in columns:
            try:
                query = f"""
                    SELECT 
                        CASE 
                            WHEN reason LIKE '%익절%' OR reason LIKE '%profit%' OR reason LIKE '%+%' THEN '익절'
                            WHEN reason LIKE '%추적%' OR reason LIKE '%trailing%' THEN '추적손절'
                            WHEN reason LIKE '%MA%' OR reason LIKE '%이평%' OR reason LIKE '%이탈%' THEN 'MA이탈'
                            WHEN reason LIKE '%타임아웃%' OR reason LIKE '%timeout%' THEN '타임아웃'
                            WHEN reason LIKE '%데드크로스%' OR reason LIKE '%dead%' OR reason LIKE '%DC_%' THEN '데드크로스'
                            WHEN reason LIKE '%역배열%' THEN '역배열'
                            WHEN reason LIKE '%긴급%' OR reason LIKE '%emergency%' THEN '긴급청산'
                            WHEN reason LIKE '%손절%' OR reason LIKE '%stop%' THEN '손절'
                            ELSE '기타'
                        END as reason_type,
                        COUNT(*) as count,
                        SUM(CASE WHEN pnl > 0 THEN 1 ELSE 0 END) as wins,
                        AVG(pnl) as avg_pnl,
                        SUM(pnl) as total_pnl,
                        AVG(return_pct) as avg_return_pct
                    FROM trade_details
                    WHERE side = 'SELL' AND reason IS NOT NULL AND reason != ''{date_filter}
                    GROUP BY reason_type
                    HAVING count >= 1
                    ORDER BY count DESC
                """
                cursor.execute(query, date_params)
                sell_reason_stats = cursor.fetchall()
            except sqlite3.OperationalError as e:
                sell_reason_stats = []
        
        # 타임프레임별 통계
        timeframe_stats = []
        if 'timeframe' in columns:
            tf_stats_query_parts = ['timeframe']
            tf_stats_query_parts.append("COUNT(*) as total_trades")
            
            if 'pnl' in columns:
                tf_stats_query_parts.append("SUM(CASE WHEN pnl > 0 THEN 1 ELSE 0 END) as wins")
                tf_stats_query_parts.append("SUM(pnl) as total_pnl")
            else:
                tf_stats_query_parts.append("0 as wins")
                tf_stats_query_parts.append("0 as total_pnl")
            
            if 'return_pct' in columns:
                tf_stats_query_parts.append("AVG(return_pct) as avg_return")
                tf_stats_query_parts.append("MAX(return_pct) as max_return")
                tf_stats_query_parts.append("MIN(return_pct) as min_return")
            else:
                tf_stats_query_parts.append("0 as avg_return")
                tf_stats_query_parts.append("0 as max_return")
                tf_stats_query_parts.append("0 as min_return")
            
            tf_stats_query = f"""
                SELECT {', '.join(tf_stats_query_parts)}
                FROM trade_details
                WHERE (side = 'SELL' OR side = 'sell'){date_filter}
                GROUP BY timeframe
                ORDER BY 
                    CASE timeframe
                        WHEN '1m' THEN 1
                        WHEN '3m' THEN 2
                        WHEN '5m' THEN 3
                        WHEN '10m' THEN 4
                        WHEN '15m' THEN 5
                        WHEN '30m' THEN 6
                        WHEN '1h' THEN 7
                        WHEN '4h' THEN 8
                        WHEN '1d' THEN 9
                        ELSE 99
                    END
            """
            
            try:
                cursor.execute(tf_stats_query, date_params)
                timeframe_stats = cursor.fetchall()
            except sqlite3.OperationalError as e:
                timeframe_stats = []
        
        # ========================================
        # 시장 체제별 통계
        # ========================================
        regime_stats = []
        if 'market_regime' in columns and 'pnl' in columns:
            try:
                # ✅ market_regime 컬럼에 이미 진입 체제 저장됨 (JOIN 불필요)
                regime_query = f"""
                    SELECT 
                        CASE 
                             WHEN market_regime = 'TRENDING' THEN '추세장'
                            WHEN market_regime = 'RANGING' THEN '횡보장'
                            WHEN market_regime = 'BREAKDOWN' THEN '붕괴장'
                            WHEN market_regime = 'VOLATILE' THEN '변동장'
                            WHEN market_regime = 'TRENDING_UP' THEN '상승추세'
                            WHEN market_regime = 'TRENDING_DOWN' THEN '하락추세'
                            WHEN market_regime = 'BREAKOUT' THEN '돌파장'
                            WHEN market_regime IS NULL OR market_regime = '' THEN '미분류'
                            ELSE market_regime
                        END as regime_type,
                        COUNT(*) as total_trades,
                        SUM(CASE WHEN pnl > 0 THEN 1 ELSE 0 END) as wins,
                        SUM(pnl) as total_pnl,
                        AVG(return_pct) as avg_return,
                        MAX(return_pct) as max_return,
                        MIN(return_pct) as min_return
                    FROM trade_details
                    WHERE side = 'SELL'{date_filter}
                    GROUP BY regime_type
                    ORDER BY total_trades DESC
                """
                cursor.execute(regime_query, date_params)
                regime_stats = cursor.fetchall()
            except sqlite3.OperationalError as e:
                regime_stats = []
        
        # ========================================
        # 🔥 수정 2: 체제별 매도 사유 분석 - CASE 순서 변경
        # ========================================
        regime_exit_analysis = []
        if 'market_regime' in columns and 'reason' in columns and 'pnl' in columns:
            try:
                regime_exit_query = f"""
                    SELECT 
                        CASE 
                            WHEN market_regime = 'TRENDING_UP' THEN '상승추세'
                            WHEN market_regime = 'TRENDING_DOWN' THEN '하락추세'
                            WHEN market_regime = 'RANGING' THEN '횡보장'
                            WHEN market_regime = 'BREAKOUT' THEN '돌파장'
                            WHEN market_regime = 'BREAKDOWN' THEN '붕괴장'
                            WHEN market_regime = 'VOLATILE' THEN '변동장'
                            WHEN market_regime = 'TRENDING' THEN '추세장'
                            ELSE COALESCE(NULLIF(market_regime, ''), '미분류')
                        END as regime_type,
                        CASE 
                            WHEN reason LIKE '%익절%' OR reason LIKE '%profit%' OR reason LIKE '%+%' THEN '익절'
                            WHEN reason LIKE '%추적%' OR reason LIKE '%trailing%' THEN '추적손절'
                            WHEN reason LIKE '%MA%' OR reason LIKE '%이평%' OR reason LIKE '%이탈%' THEN 'MA이탈'
                            WHEN reason LIKE '%타임아웃%' OR reason LIKE '%timeout%' THEN '타임아웃'
                            WHEN reason LIKE '%데드크로스%' OR reason LIKE '%dead%' OR reason LIKE '%DC_%' THEN '데드크로스'
                            WHEN reason LIKE '%역배열%' THEN '역배열'
                            WHEN reason LIKE '%긴급%' OR reason LIKE '%emergency%' THEN '긴급청산'
                            WHEN reason LIKE '%손절%' OR reason LIKE '%stop%' THEN '손절'
                            ELSE '기타'
                        END as exit_type,
                        COUNT(*) as count,
                        SUM(pnl) as total_pnl,
                        AVG(pnl) as avg_pnl, 
                        AVG(return_pct) as avg_return,
                        MIN(return_pct) as min_return,
                        MAX(return_pct) as max_return
                    FROM trade_details
                    WHERE side = 'SELL' AND reason IS NOT NULL AND reason != ''{date_filter}
                    GROUP BY regime_type, exit_type
                    HAVING count >= 1
                    ORDER BY regime_type, count DESC
                """
                cursor.execute(regime_exit_query, date_params)
                regime_exit_analysis = cursor.fetchall()
            except sqlite3.OperationalError as e:
                regime_exit_analysis = []
        
        # ========================================
        # 🔥 종목별 통계 (SQL 직접 집계)
        # ========================================
        symbol_stats_columns = []
        symbol_stats_columns.append("COALESCE(symbol, 'NULL') as symbol")
        symbol_stats_columns.append("COUNT(*) as trades")
        
        if 'pnl' in columns:
            symbol_stats_columns.append("SUM(CASE WHEN pnl > 0 THEN 1 ELSE 0 END) as wins")
            symbol_stats_columns.append("SUM(pnl) as total_pnl")
        else:
            symbol_stats_columns.append("0 as wins")
            symbol_stats_columns.append("0 as total_pnl")
        
        if 'return_pct' in columns:
            symbol_stats_columns.append("AVG(return_pct) as avg_return")
        else:
            symbol_stats_columns.append("0 as avg_return")
        
        symbol_stats_query = f"""
            SELECT {', '.join(symbol_stats_columns)}
            FROM trade_details
            WHERE side = 'SELL'{date_filter}
            GROUP BY symbol
            ORDER BY trades DESC
        """
        
        try:
            cursor.execute(symbol_stats_query, date_params)
            symbol_stats_rows = cursor.fetchall()
        except sqlite3.OperationalError as e:
            logger.error("종목별 통계 조회 실패: %s", e)
            symbol_stats_rows = []

        # BUY부터 완전청산까지 포지션 단위 집계.
        # 전체 이력으로 결합한 뒤 완전청산일에 날짜 필터를 적용한다.
        pos_rows = []
        pos_excluded_count = 0

        try:
            pos_query = """
                WITH legs AS (
                    SELECT
                        id,
                        timestamp,
                        symbol,
                        TRIM(UPPER(side)) AS side,
                        price,
                        volume,
                        amount,
                        pnl,
                        market_regime,
                        confidence,
                        SUM(
                            CASE
                                WHEN TRIM(UPPER(side)) = 'BUY'
                                THEN 1 ELSE 0
                            END
                        ) OVER (
                            PARTITION BY symbol
                            ORDER BY id
                            ROWS BETWEEN UNBOUNDED PRECEDING
                                     AND CURRENT ROW
                        ) AS pos_seq
                    FROM trade_details
                )
                SELECT
                    symbol,
                    MAX(
                        CASE WHEN side = 'BUY' THEN price END
                    ) AS entry_px,
                    MAX(
                        CASE WHEN side = 'BUY' THEN market_regime END
                    ) AS regime,
                    MAX(
                        CASE WHEN side = 'BUY' THEN confidence END
                    ) AS conf,
                    SUM(
                        CASE
                            WHEN side = 'SELL'
                            THEN COALESCE(pnl, 0)
                            ELSE 0
                        END
                    ) AS pos_pnl,
                    SUM(
                        CASE
                            WHEN side = 'BUY'
                            THEN COALESCE(volume, 0)
                            ELSE 0
                        END
                    ) AS buy_vol,
                    SUM(
                        CASE
                            WHEN side = 'BUY'
                            THEN COALESCE(amount, price * volume, 0)
                            ELSE 0
                        END
                    ) AS buy_amt,
                    SUM(
                        CASE
                            WHEN side = 'SELL'
                            THEN COALESCE(volume, 0)
                            ELSE 0
                        END
                    ) AS sold_vol,
                    SUM(
                        CASE WHEN side = 'SELL' THEN 1 ELSE 0 END
                    ) AS legs,
                    MIN(
                        CASE WHEN side = 'BUY' THEN timestamp END
                    ) AS entry_ts,
                    MAX(
                        CASE WHEN side = 'SELL' THEN timestamp END
                    ) AS exit_ts,
                    SUM(
                        CASE WHEN side = 'BUY' THEN 1 ELSE 0 END
                    ) AS buy_legs
                FROM legs
                WHERE pos_seq > 0
                GROUP BY symbol, pos_seq
                HAVING buy_legs = 1
                   AND legs > 0
                ORDER BY exit_ts DESC
            """

            cursor.execute(pos_query)
            _pos_candidates = cursor.fetchall()

            _from_date = (
                datetime.strptime(date_from, "%Y-%m-%d").date()
                if date_from else None
            )
            _to_date = (
                datetime.strptime(date_to, "%Y-%m-%d").date()
                if date_to else None
            )

            for _row in _pos_candidates:
                _exit_dt = _safe_parse_dt(_row[10])
                if _exit_dt is None:
                    pos_excluded_count += 1
                    continue

                _exit_date = _exit_dt.date()

                if _from_date and _exit_date < _from_date:
                    continue
                if _to_date and _exit_date > _to_date:
                    continue

                _buy_vol = float(_row[5] or 0.0)
                _sold_vol = float(_row[7] or 0.0)
                _volume_tolerance = max(
                    1e-12,
                    _buy_vol * 1e-6,
                )

                # 부분청산·추가매수 미기록·수량 불일치는
                # 완료 포지션으로 오집계하지 않는다.
                if (
                    _buy_vol <= 0.0
                    or abs(_sold_vol - _buy_vol) > _volume_tolerance
                ):
                    pos_excluded_count += 1
                    continue

                pos_rows.append(_row)

        except sqlite3.Error as e:
            logger.warning(
                f"포지션 단위 통계 조회 실패: {e}"
            )
            pos_rows = []
            pos_excluded_count = 0

        conn.close()
               
        # ========================================
        # 텍스트 리포트 생성
        # ========================================
        with open(report_file, 'w', encoding='utf-8') as f:
            # 초기화 정보 헤더 (콘솔 로그와 동일)
            tf = config.get('TRADING', 'timeframe', fallback='3m')
            mode = config.get('TRADING', 'mode', fallback='sim')
            
            # config 파일 경로 (속성 확인 후 사용)
            if hasattr(config, 'config_path'):
                cfg_path = config.config_path
            elif hasattr(config, 'path'):
                cfg_path = config.path
            else:
                cfg_path = 'config.ini'
            
            write_and_print(f, "=" * 150 + "\n")
            write_and_print(f, "⚙️  초기화 요약\n")
            write_and_print(f, "=" * 150 + "\n")
            write_and_print(f, f"✅ 공통 설정 로드: {cfg_path}\n")
            write_and_print(f, f"🎯 선택된 타임프레임: {tf}\n")
            write_and_print(f, f"🎮 실행 모드: {mode}\n")
            
            # 병합검증 정보 (손절/익절/추적)
            sl = config.getfloat('RISK_MANAGEMENT', 'stop_loss_pct', fallback=0.003)
            tp = config.getfloat('RISK_MANAGEMENT', 'take_profit_pct', fallback=0.007)
            ts = config.getfloat('RISK_MANAGEMENT', 'trailing_stop_pct', fallback=0.0025)
            write_and_print(f, f"📘 [병합검증] TF:{tf} | 손절:{sl:.3f} | 익절:{tp:.3f} | 추적:{ts:.4f}\n")
            write_and_print(f, "✅ 설정 병합 완료\n")
            write_and_print(f, "\n")

            write_and_print(f, "=" * 150 + "\n")
            write_and_print(f, "📊 거래 내역 리포트\n")
            write_and_print(f, "=" * 150 + "\n")
            write_and_print(f, f"생성 시간: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            write_and_print(f, f"데이터베이스: {db_path}\n")
            
            # 날짜 필터 표시
            if date_from or date_to:
                write_and_print(f, f"날짜 필터: ")
                if date_from and date_to:
                    write_and_print(f, f"{date_from} ~ {date_to}\n")
                elif date_from:
                    write_and_print(f, f"{date_from} 이후\n")
                else:
                    write_and_print(f, f"{date_to} 이전\n")
            else:
                write_and_print(f, f"날짜 필터: 전체\n")
            
            write_and_print(f, "\n")
            
            # 전체 통계
            if stats and stats[0] > 0:
                total = stats[0]
                wins = stats[1] if len(stats) > 1 else 0
                total_pnl = stats[2] if len(stats) > 2 else 0
                avg_ret = stats[3] if len(stats) > 3 else 0
                max_ret = stats[4] if len(stats) > 4 else 0
                min_ret = stats[5] if len(stats) > 5 else 0
                
                win_rate = (wins / total * 100) if total > 0 else 0
                
                write_and_print(f, "📈 전체 통계\n")
                write_and_print(f, "-" * 150 + "\n")
                write_and_print(
                    f,
                    f"총 거래 수(매도 leg): {total:>6}회 "
                    f"※ 실제 포지션 수는 아래 통계 참조\n"
                )
                write_and_print(f, f"승리:          {wins:>10}회\n")
                write_and_print(f, f"패배:          {total-wins:>10}회\n")
                write_and_print(f, f"승률:          {win_rate:>10.2f}%\n")
                write_and_print(f, f"총 손익:       {total_pnl:>10,.0f}원\n")
                write_and_print(f, f"평균 수익률:    {avg_ret:>10.2f}%\n")
                write_and_print(f, f"최대 수익률:    {max_ret:>10.2f}%\n")
                write_and_print(f, f"최소 수익률:    {min_ret:>10.2f}%\n")
                write_and_print(f, "\n")
            else:
                write_and_print(f, "📈 전체 통계\n")
                write_and_print(f, "-" * 150 + "\n")
                write_and_print(f, "⚠️  해당 기간에 완료된 거래가 없습니다\n")
                if date_from or date_to:
                    write_and_print(f, "    (날짜 필터를 확인하거나 전체 기간으로 다시 시도해보세요)\n")
                write_and_print(f, "\n")
            # REPORT-TRUTH: 체결 원가 연결이 확인된 행만 재계산한다.
            if _chk:
                _ok = []
                _na = 0
                _na_pnl = 0.0
                for _sid, _values in _chk.items():
                    _sp, _sv, _sf, _pn, _bp, _bv, _bf = (_rt_num(_x) for _x in _values)
                    if any(_x is None for _x in (_sp, _sv, _sf, _pn, _bp, _bv, _bf)) or _sp <= 0 or _sv <= 0 or _bp <= 0 or _bv <= 0 or _sf < 0 or _bf < 0 or _sv > _bv * (1 + 1e-6):
                        _na += 1
                        _na_pnl += _pn if _pn is not None else 0.0
                        continue
                    _recalc = _rt_num(_sp * _sv - _bp * _sv - _bf * (_sv / _bv) - _sf)
                    if _recalc is None:
                        _na += 1
                        _na_pnl += _pn
                        continue
                    _ok.append((_pn, _recalc))
                    # 원가는 연결된 매수 기록 그대로 표시 · 손익만 다르면 표에 ‡ (연결 근거로 쓰지 않음)
                    if abs(_pn - _recalc) > 1.0:
                        _rt_bad_ids.add(_sid)
                _db = sum(a for a, _ in _ok)
                _ind = sum(b for _, b in _ok)
                _bad = sum(1 for a, b in _ok if abs(a - b) > 1.0)
                if _ok:
                    write_and_print(f, f"🔎 독립 검산 : 매도 {len(_chk)}건 중 {len(_ok)}건 재계산 · 검산 대상 DB 손익 합계 {_db:+,.0f}원 · 체결 기록 재계산 {_ind:+,.0f}원 · 차 {_db - _ind:+,.0f}원 · 1원 넘게 다른 건 {_bad}건 · 검산 불가 {_na}건\n")
                else:
                    write_and_print(f, f"🔎 독립 검산 : 매도 {len(_chk)}건 · 재계산 가능한 거래 없음 (검산 불가 {_na}건)\n")
                if _bad or _rt_num(_db) is None or _rt_num(_ind) is None or abs(_db - _ind) > max(1.0, 0.5 * len(_ok)):
                    write_and_print(f, "⚠️  [검산 불일치] DB 손익이 체결 기록 재계산과 다릅니다 (표의 ‡ 행) — 이 리포트의 손익을 믿기 전에 확인 필요\n")
                if _na:
                    write_and_print(f, f"⚠️  [검산 불가] 매도 {_na}건 (DB 손익 합계 {_na_pnl:+,.0f}원) 은 매수 기록과 연결되지 않았거나 (진입 신호 지문 없음 · 수량 비정상) 손익 값이 비정상입니다 — 확인되지 않은 숫자입니다\n")
                write_and_print(f, "\n")
            # 포지션 단위 통계 — BUY부터 완전청산까지 실제 거래 단위
            if pos_rows:
                p_n = len(pos_rows)
                p_pnl = sum(
                    float(row[4] or 0.0)
                    for row in pos_rows
                )
                p_win = sum(
                    1
                    for row in pos_rows
                    if float(row[4] or 0.0) > 0.0
                )
                p_legs = sum(
                    int(row[8] or 0)
                    for row in pos_rows
                )

                # DB pnl은 진입·청산 수수료가 이미 반영된 순손익 SSoT.
                p_returns = [
                    float(row[4] or 0.0)
                    / float(row[6])
                    * 100.0
                    for row in pos_rows
                    if float(row[6] or 0.0) > 0.0
                ]

                p_win_rate = p_win / p_n * 100.0
                p_avg_return = (
                    sum(p_returns) / len(p_returns)
                    if p_returns else 0.0
                )

                p_win_returns = [
                    value
                    for value in p_returns
                    if value > 0.0
                ]
                p_loss_returns = [
                    value
                    for value in p_returns
                    if value <= 0.0
                ]

                p_avg_win = (
                    sum(p_win_returns) / len(p_win_returns)
                    if p_win_returns else 0.0
                )
                p_avg_loss = (
                    sum(p_loss_returns) / len(p_loss_returns)
                    if p_loss_returns else 0.0
                )

                p_rr = (
                    abs(p_avg_win / p_avg_loss)
                    if p_avg_loss else 0.0
                )
                p_break_even = (
                    abs(p_avg_loss)
                    / (p_avg_win + abs(p_avg_loss))
                    * 100.0
                    if p_avg_win + abs(p_avg_loss) > 0.0
                    else 0.0
                )

                write_and_print(
                    f,
                    "🎯 포지션 단위 통계 "
                    "(BUY leg → 완전청산 · 실제 거래 단위)\n"
                )
                write_and_print(f, "-" * 150 + "\n")
                write_and_print(
                    f,
                    f"포지션 수:      {p_n:>10}건 "
                    f"(매도 leg {p_legs}회, "
                    f"배율 {p_legs / p_n:.2f})\n"
                )
                write_and_print(
                    f,
                    f"승리:          {p_win:>10}건\n"
                )
                write_and_print(
                    f,
                    f"패배:          {p_n - p_win:>10}건\n"
                )
                write_and_print(
                    f,
                    f"★포지션 승률:   {p_win_rate:>10.2f}%\n"
                )
                write_and_print(
                    f,
                    f"총 손익:       {p_pnl:>10,.0f}원\n"
                )
                write_and_print(
                    f,
                    f"평균 순수익률:  {p_avg_return:>10.3f}%\n"
                )
                write_and_print(
                    f,
                    f"평균 이익:      {p_avg_win:>10.3f}%\n"
                )
                write_and_print(
                    f,
                    f"평균 손실:      {p_avg_loss:>10.3f}%\n"
                )
                write_and_print(
                    f,
                    f"★손익비:       {p_rr:>10.2f}\n"
                )
                write_and_print(
                    f,
                    f"★손익분기 승률: {p_break_even:>10.2f}% "
                    f"(현재 {p_win_rate:.2f}% / "
                    f"갭 {p_break_even - p_win_rate:+.2f}%p)\n"
                )

                if pos_excluded_count > 0:
                    write_and_print(
                        f,
                        f"제외:           {pos_excluded_count:>10}건 "
                        f"(부분청산·추가매수·수량 불일치)\n"
                    )

                write_and_print(f, "\n")

                from collections import defaultdict as _dd

                _regime_positions = _dd(list)
                _regime_names = {
                    "TRENDING": "추세장",
                    "TRENDING_UP": "상승추세",
                    "TRENDING_DOWN": "하락추세",
                    "RANGING": "횡보장",
                    "BREAKOUT": "돌파장",
                    "BREAKDOWN": "붕괴장",
                    "VOLATILE": "변동장",
                    "": "미분류",
                }

                for row in pos_rows:
                    _raw_regime = str(row[2] or "")
                    _regime_name = _regime_names.get(
                        _raw_regime,
                        _raw_regime or "미분류",
                    )
                    _regime_positions[_regime_name].append(row)

                write_and_print(
                    f,
                    "🌍 시장 체제별 (포지션 단위)\n"
                )
                write_and_print(f, "-" * 150 + "\n")
                write_and_print(
                    f,
                    f"{'체제':<18} {'포지션':<8} "
                    f"{'승률':<10} {'총손익':<14} "
                    f"{'포지션당':<12}\n"
                )
                write_and_print(f, "-" * 150 + "\n")

                for regime_name, rows in sorted(
                    _regime_positions.items(),
                    key=lambda item: -len(item[1]),
                ):
                    regime_pnl = sum(
                        float(row[4] or 0.0)
                        for row in rows
                    )
                    regime_wins = sum(
                        1
                        for row in rows
                        if float(row[4] or 0.0) > 0.0
                    )

                    write_and_print(
                        f,
                        f"{regime_name:<18} "
                        f"{len(rows):>6}건 "
                        f"{(regime_wins / len(rows) * 100):>8.1f}% "
                        f"{regime_pnl:>12,.0f}원 "
                        f"{(regime_pnl / len(rows)):>10,.0f}원\n"
                    )

                write_and_print(f, "\n")

            else:
                write_and_print(
                    f,
                    "🎯 포지션 단위 통계\n"
                )
                write_and_print(f, "-" * 150 + "\n")
                write_and_print(
                    f,
                    "⚠️  해당 기간에 BUY→완전청산으로 "
                    f"결합 가능한 포지션이 없습니다.\n"
                )

                if pos_excluded_count > 0:
                    write_and_print(
                        f,
                        f"    부분청산·추가매수·수량 불일치 "
                        f"{pos_excluded_count}건 제외\n"
                    )

                write_and_print(
                    f,
                    "    아래 기존 통계는 매도 leg 단위입니다.\n\n"
                )
            # 매도 사유별 통계
            if sell_reason_stats:
                write_and_print(f, "🎯 매도 사유별 통계\n")
                write_and_print(f, "-" * 150 + "\n")
                write_and_print(f, f"{'매도 사유':<15} {'거래수':<10} {'승률':<12} {'평균손익':<15} {'총손익':<15} {'평균수익률':<12}\n")
                write_and_print(f, "-" * 150 + "\n")
                
                for reason_type, count, wins, avg_pnl, total_pnl, avg_return_pct in sell_reason_stats:
                    win_rate = (wins / count * 100) if count > 0 else 0
                    
                    write_and_print(
                        f, 
                        f"{reason_type:<15} "
                        f"{count:>8}회 "
                        f"{win_rate:>10.1f}% "
                        f"{avg_pnl or 0:>13,.0f}원 "
                        f"{total_pnl or 0:>13,.0f}원 "
                        f"{avg_return_pct or 0:>10.2f}%\n"
                    )
                
                write_and_print(f, "\n")
            
            # 타임프레임별 통계
            if timeframe_stats:
                write_and_print(f, "⏱️  타임프레임별 통계\n")
                write_and_print(f, "-" * 150 + "\n")
                write_and_print(f, f"{'타임프레임':<12} {'거래수':<10} {'승리':<10} {'패배':<10} {'승률':<10} {'총손익':<15} {'평균수익률':<12} {'최대':<10} {'최소':<10}\n")
                write_and_print(f, "-" * 150 + "\n")
                
                for tf_stat in timeframe_stats:
                    timeframe = tf_stat[0] if tf_stat[0] else 'UNKNOWN'
                    total = tf_stat[1]
                    wins = tf_stat[2] if len(tf_stat) > 2 else 0
                    total_pnl = tf_stat[3] if len(tf_stat) > 3 else 0
                    avg_ret = tf_stat[4] if len(tf_stat) > 4 else 0
                    max_ret = tf_stat[5] if len(tf_stat) > 5 else 0
                    min_ret = tf_stat[6] if len(tf_stat) > 6 else 0
                    
                    losses = total - wins
                    win_rate = (wins / total * 100) if total > 0 else 0
                    
                    write_and_print(f, f"{timeframe:<12} {total:>8}회 {wins:>8}회 {losses:>8}회 {win_rate:>8.2f}% {total_pnl:>13,.0f}원 {avg_ret:>10.2f}% {max_ret:>8.2f}% {min_ret:>8.2f}%\n")
                
                write_and_print(f, "\n")
            
            # 시장 체제별 통계
            if regime_stats:
                write_and_print(f, "🌍 시장 체제별 통계\n")
                write_and_print(f, "-" * 150 + "\n")
                write_and_print(f, f"{'시장체제':<12} {'거래수':<10} {'승리':<10} {'패배':<10} {'승률':<10} {'총손익':<15} {'평균수익률':<12} {'최대':<10} {'최소':<10}\n")
                write_and_print(f, "-" * 150 + "\n")
                
                for regime_stat in regime_stats:
                    regime_type = regime_stat[0]
                    total = regime_stat[1]
                    wins = regime_stat[2]
                    total_pnl = regime_stat[3]
                    avg_ret = regime_stat[4]
                    max_ret = regime_stat[5]
                    min_ret = regime_stat[6]
                    
                    losses = total - wins
                    win_rate = (wins / total * 100) if total > 0 else 0
                    
                    write_and_print(
                        f, 
                        f"{regime_type:<12} "
                        f"{total:>8}회 "
                        f"{wins:>8}회 "
                        f"{losses:>8}회 "
                        f"{win_rate:>8.2f}% "
                        f"{total_pnl:>13,.0f}원 "
                        f"{avg_ret:>10.2f}% "
                        f"{max_ret:>8.2f}% "
                        f"{min_ret:>8.2f}%\n"
                    )
                
                write_and_print(f, "\n")
                
                # 시장 체제별 인사이트
                write_and_print(f, "💡 시장 체제별 인사이트\n")
                write_and_print(f, "-" * 150 + "\n")
                
                most_traded = regime_stats[0]
                most_regime = most_traded[0]
                most_count = most_traded[1]
                most_win_rate = (most_traded[2] / most_traded[1] * 100) if most_traded[1] > 0 else 0
                
                write_and_print(f, f"• 가장 많은 거래: {most_regime} ({most_count}회, 승률 {most_win_rate:.1f}%)\n")
                
                if len(regime_stats) > 1:
                    def _pnl_per_sell_leg(row):
                        _count = int(row[1] or 0)
                        return (
                            float(row[3] or 0.0) / _count
                            if _count > 0
                            else float("-inf")
                        )

                    best_regime = max(
                        regime_stats,
                        key=_pnl_per_sell_leg,
                    )
                    worst_regime = min(
                        regime_stats,
                        key=_pnl_per_sell_leg,
                    )

                    _best_label = (
                        "손실이 가장 적은 체제"
                        if _pnl_per_sell_leg(best_regime) < 0.0
                        else "가장 수익성 높은 체제"
                    )
                    _worst_label = (
                        "손실이 가장 큰 체제"
                        if _pnl_per_sell_leg(worst_regime) < 0.0
                        else "수익이 가장 낮은 체제"
                    )

                    write_and_print(
                        f,
                        f"• {_best_label}: {best_regime[0]} "
                        f"(매도 leg당 "
                        f"{_pnl_per_sell_leg(best_regime):+,.0f}원, "
                        f"총 {float(best_regime[3] or 0):+,.0f}원)\n"
                    )
                    write_and_print(
                        f,
                        f"• {_worst_label}: {worst_regime[0]} "
                        f"(매도 leg당 "
                        f"{_pnl_per_sell_leg(worst_regime):+,.0f}원, "
                        f"총 {float(worst_regime[3] or 0):+,.0f}원)\n"
                    )
                
                for regime_stat in regime_stats:
                    regime_name = regime_stat[0]
                    regime_win_rate = (regime_stat[2] / regime_stat[1] * 100) if regime_stat[1] > 0 else 0
                    
                    if regime_name == '추세장':
                        if regime_win_rate >= 90:
                            write_and_print(f, f"  → 추세장 전략이 효과적입니다 ✅\n")
                        else:
                            write_and_print(f, f"  → 추세장 진입 타이밍 개선 필요\n")
                    
                    elif regime_name == '횡보장':
                        if regime_win_rate < 60:
                            write_and_print(f, f"  ⚠️ 횡보장에서 손실 많음 - 진입 자제 권장\n")
                        else:
                            write_and_print(f, f"  → 횡보장 전략이 안정적입니다 ✅\n")
                    
                    elif regime_name == '붕괴장':
                        if regime_stat[1] > 0:
                            write_and_print(f, f"  ⚠️ 붕괴장 진입 감지 - 필터링 강화 권장\n")
                
                write_and_print(f, "\n")
            
            # 체제별 매도 사유 분석
            if regime_exit_analysis:
                write_and_print(f, "🔬 체제별 매도 사유 상세 분석\n")
                write_and_print(f, "-" * 150 + "\n")
                
                regime_groups = {}
                for row in regime_exit_analysis:
                    regime_type = row[0]
                    if regime_type not in regime_groups:
                        regime_groups[regime_type] = []
                    regime_groups[regime_type].append(row)
                
                for regime_type, exits in regime_groups.items():
                    write_and_print(f, f"\n📍 {regime_type}\n")
                    write_and_print(f, f"{'매도사유':<12} {'횟수':<10} {'총손익':<15} {'평균손익':<15} {'평균수익률':<12} {'최소':<10} {'최대':<10}\n")
                    write_and_print(f, "-" * 150 + "\n")
                    
                    total_exits = sum(e[2] for e in exits)
                    
                    for exit_row in exits:
                        exit_type = exit_row[1]
                        count = exit_row[2]
                        total_pnl = exit_row[3]
                        avg_pnl = exit_row[4]
                        avg_return = exit_row[5]
                        min_return = exit_row[6]
                        max_return = exit_row[7]
                        
                        ratio = (count / total_exits * 100) if total_exits > 0 else 0
                        
                        write_and_print(
                            f,
                            f"{exit_type:<12} "
                            f"{count:>3}회({ratio:>4.1f}%) "
                            f"{total_pnl or 0:>13,.0f}원 "
                            f"{avg_pnl or 0:>13,.0f}원 "
                            f"{avg_return:>10.2f}% "
                            f"{min_return:>8.2f}% "
                            f"{max_return:>8.2f}%\n"
                        )
                
                write_and_print(f, "\n")
                
                # 체제별 패턴 분석
                write_and_print(f, "💡 체제별 패턴 분석\n")
                write_and_print(f, "-" * 150 + "\n")
                
                for regime_type, exits in regime_groups.items():
                    total_exits = sum(e[2] for e in exits)
                    
                    # 체제명 먼저 출력 (손절/익절 여부와 무관)
                    write_and_print(f, f"• {regime_type}:\n")
                    
                    stop_loss_data = [e for e in exits if e[1] == '손절']
                    if stop_loss_data:
                        stop_loss_count = stop_loss_data[0][2]
                        stop_loss_ratio = (stop_loss_count / total_exits * 100)
                        stop_loss_avg = stop_loss_data[0][5]
                        
                        # 위에서 읽은 RISK_MANAGEMENT.stop_loss_pct SSoT
                        _sl_deep_pct = abs(
                            float(sl or 0.005)
                        ) * 100.0

                        if stop_loss_ratio >= 40:
                            write_and_print(
                                f,
                                f"  ⚠️ 손절 비율 높음 "
                                f"({stop_loss_ratio:.1f}%, "
                                f"평균 {stop_loss_avg:.2f}%) "
                                f"- 진입 타이밍 재검토 필요\n"
                            )
                        elif stop_loss_avg < -(_sl_deep_pct * 1.3):
                            write_and_print(
                                f,
                                f"  ⚠️ 평균 손절 깊음 "
                                f"({stop_loss_avg:.2f}% < 설정 "
                                f"−{_sl_deep_pct:.2f}%의 1.3배) "
                                f"- 손절 실행 지연 점검\n"
                            )
                        else:
                            write_and_print(
                                f,
                                f"  ✅ 손절 관리 양호 "
                                f"({stop_loss_ratio:.1f}%, "
                                f"평균 {stop_loss_avg:.2f}%)\n"
                            )
                    
                    take_profit_data = [e for e in exits if e[1] == '익절']
                    if take_profit_data:
                        take_profit_count = take_profit_data[0][2]
                        take_profit_ratio = (take_profit_count / total_exits * 100)
                        take_profit_avg = take_profit_data[0][5]
                        
                        _stop_ratio_here = (
                            stop_loss_data[0][2]
                            / total_exits
                            * 100.0
                            if stop_loss_data and total_exits > 0
                            else 0.0
                        )

                        if take_profit_ratio < 20:
                            if _stop_ratio_here >= 50.0:
                                write_and_print(
                                    f,
                                    f"  → 익절 비율 낮음 "
                                    f"({take_profit_ratio:.1f}%) "
                                    f"+ 손절 {_stop_ratio_here:.1f}% "
                                    f"→ 목표가가 아니라 진입 자리 문제\n"
                                )
                            else:
                                write_and_print(
                                    f,
                                    f"  → 익절 비율 낮음 "
                                    f"({take_profit_ratio:.1f}%) "
                                    f"- 익절 목표 하향 조정 고려\n"
                                )
                        else:
                            write_and_print(f, f"  → 익절 비율 양호 ({take_profit_ratio:.1f}%, 평균 {take_profit_avg:.2f}%)\n")
                
                write_and_print(f, "\n")
                
                # 체제 간 비교
                if len(regime_groups) > 1:
                    write_and_print(f, "📊 체제 간 강제손절(stop_loss) 비교\n")
                    write_and_print(f, "-" * 150 + "\n")
                    
                    regime_stop_loss = []
                    for regime_type, exits in regime_groups.items():
                        stop_loss_data = [e for e in exits if e[1] == '손절']
                        if stop_loss_data:
                            total_exits = sum(e[2] for e in exits)
                            stop_loss_count = stop_loss_data[0][2]
                            stop_loss_ratio = (stop_loss_count / total_exits * 100)
                            stop_loss_avg = stop_loss_data[0][5]
                            regime_stop_loss.append((regime_type, stop_loss_ratio, stop_loss_avg, stop_loss_count))
                    
                    regime_stop_loss.sort(key=lambda x: x[1], reverse=True)
                    
                    write_and_print(f, f"{'체제':<15} {'손절비율':<12} {'평균손실':<12} {'손절횟수':<10}\n")
                    write_and_print(f, "-" * 150 + "\n")
                    
                    for regime_type, ratio, avg_loss, count in regime_stop_loss:
                        # 🔒 [2026-08-02] 상단과 동일한 死조건(>40, <-5)이 이 비교 블록에만 남아있었다.
                        #  _sl_deep_pct 는 위 R8 개별 분석 블록에서 이미 계산한 SSoT 재사용.
                        emoji = (
                            "🔴"
                            if (
                                ratio >= 40
                                or avg_loss < -(_sl_deep_pct * 1.3)
                            )
                            else "🟡"
                            if ratio > 25
                            else "🟢"
                        )
                        write_and_print(
                            f,
                            f"{emoji} {regime_type:<13} "
                            f"{ratio:>9.1f}% "
                            f"{avg_loss:>10.2f}% "
                            f"{count:>8}회\n"
                        )
                    
                    write_and_print(f, "\n")
                    
                    if regime_stop_loss:
                        worst_regime = regime_stop_loss[0]
                        if (
                            worst_regime[1] >= 40
                            or worst_regime[2] < -(_sl_deep_pct * 1.3)
                        ):
                            write_and_print(
                                f,
                                f"⚠️ [{worst_regime[0]}] 체제에서 손실이 심각합니다. "
                                f"이 체제에서의 진입 조건을 강화하거나 진입을 제한하는 것을 권장합니다.\n\n"
                            )
            
            # ========================================
            # 🔥 종목별 통계 (SQL 집계 결과 출력)
            # ========================================
            if symbol_stats_rows:
                write_and_print(f, f"{'='*150}\n")
                write_and_print(f, f"📊 종목별 통계 (총 {len(symbol_stats_rows)}개 종목, 상위 20개 표시)\n")
                write_and_print(f, f"{'='*150}\n")
                write_and_print(f, f"{'종목':<12} {'거래':<10} {'승률':<12} {'손익':<18} {'평균수익률':<12}\n")
                write_and_print(f, f"{'-'*150}\n")
                
                for row in symbol_stats_rows[:20]:
                    symbol = row[0]
                    trades_count = row[1]
                    wins = row[2] if len(row) > 2 else 0
                    total_pnl = row[3] if len(row) > 3 else 0
                    avg_return = row[4] if len(row) > 4 else 0
                    
                    win_rate = (wins / trades_count * 100) if trades_count > 0 else 0
                    
                    write_and_print(
                        f, 
                        f"{symbol:<12} "
                        f"{trades_count:>8}회 "
                        f"{win_rate:>10.1f}% "
                        f"{total_pnl or 0:>+16,.0f}원 "
                        f"{avg_return or 0:>+10.2f}%\n"
                    )
                
                write_and_print(f, "\n")
            
            # ========================================
            # 🆕 진입 차단 분석 (로그 파일 파싱)
            # ========================================
            write_and_print(f, f"{'='*150}\n")
            write_and_print(f, "🚫 진입 차단 분석 (같은 기간)\n")
            write_and_print(f, f"{'='*150}\n")
            
            # 로그 파일에서 차단 정보 추출
            try:
                import re
                from collections import Counter
                
                # 경로 수정
                log_dir = Path('outputs/logs')
                log_files = list(_iter_report_log_files(log_dir, date_from, date_to))
                
                if log_files:
                    blocks = []
                    check_pattern = r'\[진입체크\] (.+?) ━━ 신호 없음 \(이유: (.+)\)(?: scalp_min=\S+)?\s*$'
                    
                    for log_file in log_files:
                        
                        try:
                            with open(log_file, 'r', encoding='utf-8', errors='ignore') as lf:
                                for line in lf:
                                    match = re.search(check_pattern, line)
                                    if match:
                                        reason = match.group(2)
                                        blocks.append(reason)
                        except Exception as e:
                            logger.warning(f"로그 파일 읽기 오류 ({log_file.name}): {e}")
                            continue
                    
                    if blocks:
                        # 차단 원인 분류 (우선순위 순서 중요!)
                        categorized = []
                        for reason in blocks:
                            # 1. 시장 체제 차단 (하락장/붕괴)
                            if 'TRENDING_DOWN' in reason or '하락 추세' in reason or '하락장' in reason:
                                categorized.append('하락장 진입 금지')
                            elif '시장 붕괴' in reason or 'BREAKDOWN' in reason:
                                categorized.append('시장 붕괴 체제')
                            
                            # 2. MA 역배열 차단
                            elif 'MA 완전역배열' in reason or '완전역배열' in reason:
                                categorized.append('MA 완전역배열')
                            elif 'MA 부분역배열' in reason or '부분역배열' in reason or '부분 역배열' in reason:
                                categorized.append('MA 부분역배열')
                            
                            # 3. 변동성 차단
                            elif '변동성 과소' in reason:
                                categorized.append('변동성 과소 (거래 부진)')
                            elif '변동성 과다' in reason or 'ATR 필터 차단' in reason:
                                categorized.append('변동성 과다 (급변동)')
                            
                            # 4. 횡보장 박스권 차단
                            elif '횡보 상단' in reason or '상단 진입 위험' in reason:
                                categorized.append('횡보장 상단 (천장권)')
                            elif '횡보 중간' in reason:
                                categorized.append('횡보장 중간 (대기 권장)')
                            
                            # 5. 이격도 차단
                            elif '이격도 과매수' in reason:
                                categorized.append('이격도 과매수')
                            elif '횡보 정배열' in reason:
                                categorized.append('극도의 횡보 (움직임 없음)')
                            
                            # 6. 거래량 차단
                            elif '거래량 부족' in reason or 'VOL_LOW' in reason:
                                categorized.append('거래량 부족')
                            
                            # 7. 신뢰도 차단 (EEE_LOW, CONF_LOW)
                            elif 'EEE_LOW' in reason:
                                categorized.append('MA+Vol+ATR+ADX+Candle 30점미만')
                            elif 'CONF_LOW' in reason or '신뢰도 부족' in reason:
                                categorized.append('최종 신뢰도 부족')
                            
                            # 8. MA 관련 기타
                            elif 'MA 신호 약함' in reason:
                                categorized.append('MA 신호 약함')
                            elif 'MA15 너무 가까움' in reason or '안전거리 부족' in reason:
                                categorized.append('횡보장 MA15 안전거리 부족')
                            elif 'MA20 이격도 과다' in reason:
                                categorized.append('MA20 이격도 과다')
                            
                            # 9. 매도압력 차단 (비율 통합) ← 새로 추가
                            elif '매도압력 STRONG_SELLING' in reason:
                                categorized.append('매도압력 (STRONG_SELLING)')
                            elif '매도압력 SELLING' in reason:
                                categorized.append('매도압력 (SELLING)')
                            elif '급격한 매도전환' in reason:
                                categorized.append('급격한 매도전환')
                            elif '지지선 테스트 중' in reason:
                                categorized.append('지지선 테스트 중')
                            elif '지지선 돌파 마진 부족' in reason:
                                categorized.append('지지선 돌파 마진 부족')
                            
                            # 10. 기타 차단 (강력한 자동 분류)
                            elif '풀백 부족' in reason:
                                categorized.append('풀백 부족')
                            elif '유효한 신호 없음' in reason:
                                categorized.append('유효한 신호 없음')
                            else:
                                # 🔥 핵심 키워드 추출 (순서 중요!)
                                clean_reason = reason
                                
                                # 1) "기타: " 제거
                                if clean_reason.startswith('기타:'):
                                    clean_reason = clean_reason[4:].strip()
                                
                                # 2) 핵심 키워드별 분류 (우선순위 순서)
                                # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
                                # 🔴 신뢰도 관련 (최우선)
                                # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
                                if '품질 임계 미달' in clean_reason:
                                    categorized.append('품질 임계 미달 (조기 차단)')
                                elif ('신뢰도 낮음' in clean_reason or 
                                      '신뢰도:' in clean_reason or 
                                      '신뢰도 부족' in clean_reason or
                                      'CONF_LOW' in clean_reason):
                                    categorized.append('최종 신뢰도 부족')
                                elif 'EEE_LOW' in clean_reason:
                                    categorized.append('MA+Vol+ATR+ADX+Candle 30점미만')
                                
                                # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
                                # 🔴 거래량 관련
                                # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
                                elif '거래량 부족' in clean_reason or 'VOL_LOW' in clean_reason:
                                    categorized.append('거래량 부족')
                                elif '거래량 둔화' in clean_reason or '가속도' in clean_reason:
                                    categorized.append('거래량 둔화')
                                
                                # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
                                # 🔴 매도압력 관련
                                # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
                                elif '매도압력' in clean_reason:
                                    if 'STRONG_SELLING' in clean_reason:
                                        categorized.append('매도압력 (STRONG_SELLING)')
                                    elif 'SELLING' in clean_reason:
                                        categorized.append('매도압력 (SELLING)')
                                    else:
                                        categorized.append('매도압력')
                                elif '급격한 매도전환' in clean_reason:
                                    categorized.append('급격한 매도전환')
                                
                                # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
                                # 🔴 MA/추세 관련
                                # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
                                elif 'MA 역배열' in clean_reason or 'MA역배열' in clean_reason:
                                    categorized.append('MA 역배열 강한하락')
                                elif 'MA5_REVERSAL_BLOCK' in clean_reason:
                                    categorized.append('MA5 하락전환 차단')
                                elif 'TU-NoMA' in clean_reason:
                                    categorized.append('TU 체제 MA 신호 부족')
                                elif '정배열 하락 전환' in clean_reason or '기울기' in clean_reason:
                                    categorized.append('정배열 하락 전환')
                                
                                # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
                                # 🔴 체제 관련
                                # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
                                elif '횡보 체제 차단' in clean_reason or '횡보 차단' in clean_reason:
                                    categorized.append('횡보 체제 차단')
                                elif '횡보 막차' in clean_reason:
                                    categorized.append('횡보 막차 진입 차단')
                                elif 'RANGING-극단' in clean_reason:
                                    categorized.append('RANGING 체제 극단 상황')
                                elif 'TRENDING_UP-극단' in clean_reason:
                                    categorized.append('TRENDING_UP 체제 극단 상황')
                                
                                # 🆕 TD_GATE / NOT_SOLEUS / SIDEWAYS 정식 분류 (2026-01-19)
                                elif 'TD_GATE_NO_GAJAMI_IN_DOWN' in clean_reason:
                                    categorized.append('하락추세 가자미근 미형성')
                                elif 'TD_GATE' in clean_reason:
                                    categorized.append('하락추세 진입차단')
                                elif 'NOT_SOLEUS' in clean_reason or 'GAJAMI' in clean_reason:
                                    # ✅ 통합: NOT_SOLEUS + GAJAMI 관련 모두 하나로
                                    categorized.append('가자미근 미형성')
                                elif 'SIDEWAYS' in clean_reason:
                                    categorized.append('횡보장 차단')
                                
                                # ▼▼▼ 아래 가자미근 관련 블록 제거 (중복) ▼▼▼
                                # elif 'NOT_GAJAMI' in clean_reason or '가자미근 외부' in clean_reason:
                                #     categorized.append('가자미근 외부')
                                # elif 'GAJAMI' in clean_reason:
                                #     categorized.append('가자미근 조건 미충족')
                                
                                # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
                                # 🔴 위치/추격 관련
                                # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
                                elif '박스상단' in clean_reason or '박스 상단' in clean_reason:
                                    categorized.append('박스 상단')
                                elif '추격매수' in clean_reason:
                                    categorized.append('추격매수 방지')
                                elif 'NEAR_HIGH_BLOCK' in clean_reason:
                                    categorized.append('고점 근처 차단')
                                
                                # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
                                # 🔴 기타 명확한 사유
                                # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
                                elif '오류 발생' in clean_reason or 'OSError' in clean_reason:
                                    categorized.append('오류 발생')
                                elif '지지선 재테스트' in clean_reason:
                                    categorized.append('지지선 재테스트 대기')
                                elif '조건 미충족' in clean_reason:
                                    categorized.append('조건 미충족')
                                
                                # 🆕 ANALYSIS_CORE_INVALID 세분화 (2026-01-03)
                                elif 'ANALYSIS_CORE_INVALID' in clean_reason or 'INVALID' in clean_reason:
                                    # 괄호 안의 세부 사유 추출
                                    paren_start = clean_reason.find('(')
                                    paren_end = clean_reason.find(')')
                                    
                                    if paren_start != -1 and paren_end != -1:
                                        # 괄호 안의 내용 추출 (예: "SENTINEL_CONFIDENCE")
                                        detail = clean_reason[paren_start+1:paren_end].strip()
                                        categorized.append(f'ANALYSIS_CORE_INVALID({detail})')
                                    else:
                                        # 괄호 없으면 원본 그대로
                                        categorized.append(clean_reason)
                                
                                # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
                                # 🔴 진짜 기타 (최후 수단)
                                # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
                                else:
                                    # 🆕 SSoT 가자미근 사유는 "기타:" 없이 표시
                                    # 진짜 기타 - 괄호 있으면 괄호 앞까지, 없으면 전체 표시
                                    paren_pos = clean_reason.find('(')
                                    if paren_pos != -1:
                                        # 괄호 있으면 괄호 앞까지만 (공백 제거)
                                        short = clean_reason[:paren_pos].strip()
                                    else:
                                        # 괄호 없으면 전체 표시 (50자 제한)
                                        short = clean_reason[:50]
                                        if len(clean_reason) > 50:
                                            short += '...'
                                    
                                    # 🆕 ANALYSIS_CORE_INVALID는 건수가 많아 "기타:" 제거 (2026-01-03)
                                    # SSoT 관련도 "기타:" 제거
                                    if 'SSoT' in short or '가자미근' in short or 'ANALYSIS_CORE_INVALID' in short:
                                        categorized.append(short)
                                    else:
                                        categorized.append(f'기타: {short}')
                        
                        total_blocks = len(categorized)
                        counter = Counter(categorized)
                        
                        write_and_print(f, f"총 차단 횟수:        {total_blocks:>5}회\n")
                        write_and_print(f, "\n")
                        
                        write_and_print(f, "주요 차단 원인:\n")
                        write_and_print(f, "-" * 150 + "\n")
                        write_and_print(f, f"{'차단 사유':<40} {'횟수':>10} {'비율':>10}\n")
                        write_and_print(f, "-" * 150 + "\n")
                        
                        for reason, count in counter.most_common():
                            pct = (count / total_blocks * 100)
                            write_and_print(f, f"{reason:<40} {count:>10}회 {pct:>9.1f}%\n")
                        
                        write_and_print(f, "\n")
                        
                        # 튜닝 제안
                        write_and_print(f, "💡 튜닝 제안:\n")
                        write_and_print(f, "-" * 150 + "\n")
                        
                        top_reason, top_count = counter.most_common(1)[0]
                        top_pct = (top_count / total_blocks * 100)
                        
                        if top_pct > 40:
                            if 'ATR' in top_reason:
                                write_and_print(f, f"⚠️  [긴급] ATR 필터가 과도합니다 ({top_pct:.0f}% 차단)\n")
                                write_and_print(f, "   → config.ini 수정: atr_filter_levels = EXTREME\n")
                                write_and_print(f, f"   → 예상 효과: 차단 {top_count}회 → {int(top_count * 0.4)}회로 감소\n")
                            elif 'MA' in top_reason and '신호 약함' in top_reason:
                                write_and_print(f, f"⚠️  [중요] MA 기준이 엄격합니다 ({top_pct:.0f}% 차단)\n")
                                write_and_print(f, "   → 사용 중인 타임프레임 config 파일에서 수정: ma_trend_min_strength 값 낮추기\n")
                            elif '신뢰도' in top_reason:
                                write_and_print(f, f"⚠️  [중요] 신뢰도 임계값이 높습니다 ({top_pct:.0f}% 차단)\n")
                                write_and_print(f, "   → 사용 중인 타임프레임 config 파일에서 수정: min_confidence_threshold 값 낮추기\n")
                        
                        write_and_print(f, "\n")
                    else:
                        write_and_print(f, "📭 해당 기간 차단 기록 없음\n\n")
                else:
                    write_and_print(f, "📭 로그 파일 없음 (outputs/logs/)\n\n")
            
            except Exception as e:
                import traceback
                write_and_print(f, f"⚠️  로그 분석 중 오류: {e}\n")
                write_and_print(f, f"{traceback.format_exc()}\n")
            
            write_and_print(f, f"{'='*150}\n")
            
            # 거래 내역 (최대 100건)
            write_and_print(f, f"📋 거래 내역 (최대 100건)\n")
            write_and_print(f, "-" * 150 + "\n")
            
            _rt_marks = set()
            if trades:
                write_and_print(f, f"{'시간':<20} {'종목':<12} {'방향':<6} {'가격':<12} {'매수금액':<15} {'매도금액':<15} {'손익':<15} {'수익률':<10} {'진입체제':<20} {'선정사유':<24} {'상세':<60}\n")
                write_and_print(f, "-" * 150 + "\n")
                
                # 묶음 v4 v1_5 : 숫자 칸 안전 변환 (변환 불가 · NaN · inf = None)
                import math as _m5
                def _num5(_x):
                    try:
                        _f = float(_x)
                    except Exception:
                        return None
                    return _f if _m5.isfinite(_f) else None
                for trade in trades:
                    timestamp_val = trade[0] if len(trade) > 0 else ''
                    symbol = trade[1] if len(trade) > 1 else ''
                    side = trade[2] if len(trade) > 2 else ''
                    price = (_num5(trade[3]) or 0.0) if len(trade) > 3 else 0.0
                    volume = _num5(trade[4]) if len(trade) > 4 else None
                    pnl = _num5(trade[5]) if len(trade) > 5 else None
                    return_pct = _num5(trade[6]) if len(trade) > 6 else None
                    reason = trade[7] if len(trade) > 7 else ''
                    confidence = (_num5(trade[8]) or 0.0) if len(trade) > 8 else 0.0
                    exit_regime = trade[9] if len(trade) > 9 else ''
                    timeframe_val = trade[10] if len(trade) > 10 else ''
                    entry_regime = trade[11] if len(trade) > 11 else ''
                    entry_timestamp = trade[12] if len(trade) > 12 else ''
                    entry_components = trade[13] if len(trade) > 13 else ''
                    _judges = ''
                    if entry_components:
                        try:
                            _judges = str(
                                json.loads(entry_components).get("judges", "")
                                or ""
                            )
                        except Exception:
                            _judges = ''

                    try:
                        dt = datetime.fromisoformat(timestamp_val).strftime('%Y-%m-%d %H:%M')
                    except:
                        dt = str(timestamp_val)[:19]

                    exit_dt = _safe_parse_dt(timestamp_val)
                    entry_dt = _safe_parse_dt(entry_timestamp)

                    if entry_dt is None:
                        entry_dt = _find_buy_execution_time(
                            buy_events, symbol, exit_dt
                        )

                    selection_source = _find_market_selection_source(
                        selection_events, symbol, entry_dt, exit_dt
                    )
                    
                    # 묶음 v4 : 유한 양수만 가격 · 없으면 「자료없음」 · 계산 불가면 「산출불가」 (0 원 · 음수 · inf 금액 만들지 않음)
                    from Trading_bot_1_core import format_price_krw as _fpk4
                    def _pos4(_x):
                        _f = _num5(_x)
                        return _f if (_f is not None and _f > 0) else 0.0
                    price = _pos4(price)
                    _vol_ok = volume is not None and volume > 0
                    if side.upper() == 'SELL':
                        _row = dict(zip(_trade_cols, trade)) if _trade_cols else {}
                        entry_px = _pos4(_row.get('legacy_entry_price'))
                        exit_px = _pos4(_row.get('legacy_exit_price'))
                        if price <= 0 and exit_px > 0:
                            price = exit_px  # price 칸이 빈 옛 행 (2025-10 ~ 2026-01)

                        # DB에 저장된 pnl 그대로 사용 (이미 수수료 반영됨) · NULL 은 0 으로 바꾸지 않음
                        pnl_val = pnl

                        sell_amount = price * volume if (price > 0 and _vol_ok) else None
                        if sell_amount is not None and not _m5.isfinite(sell_amount):
                            sell_amount = None
                        sell_str = f"{sell_amount:,.0f}원" if sell_amount is not None else ("자료없음" if price <= 0 else "산출불가")
                        # 표시 전용 : 진입가 × 수량 (정확) · 없으면 매도금액 − 손익 (수수료 포함 역산 · *) · 역산 ≤ 0 · 손익 없음 = 산출불가
                        buy_amount = None
                        _lb4 = _chk.get(_row.get('sell_id')) if _row else None
                        _lbp = _pos4(_lb4[4]) if _lb4 else 0.0
                        _lbv = _num5(_lb4[5]) if _lb4 else None
                        if _lbp > 0 and _vol_ok and _lbv is not None and _lbv > 0 and volume <= _lbv * (1 + 1e-6) and _m5.isfinite(_lbp * volume):
                            buy_amount = _lbp * volume
                            buy_str = f"{buy_amount:,.0f}원" + ("‡" if _row.get('sell_id') in _rt_bad_ids else "")
                        elif entry_px > 0 and _vol_ok and _m5.isfinite(entry_px * volume):
                            buy_amount = entry_px * volume
                            buy_str = f"{buy_amount:,.0f}원†"
                        elif sell_amount is not None and pnl_val is not None and _m5.isfinite(sell_amount - pnl_val) and (sell_amount - pnl_val) > 0:
                            buy_amount = sell_amount - pnl_val
                            buy_str = f"{buy_amount:,.0f}원*"
                        elif price <= 0 and entry_px <= 0:
                            buy_str = "자료없음"
                        else:
                            buy_str = "산출불가"
                        pnl_str = f"{pnl_val:+,.0f}원" if pnl_val is not None else "자료없음"
                        ret_str = f"{return_pct:+.2f}%" if return_pct is not None else "-"
                        emoji = "💚" if (pnl_val is not None and pnl_val > 0) else "💔"
                        
                        # 진입/매도 체제 한글화
                        regime_map = {
                            'TRENDING_UP': 'TU',
                            'TRENDING_DOWN': 'TD',
                            'TRENDING': '추세',
                            'RANGING': '횡보',
                            'BREAKDOWN': '붕괴',
                            'BREAKOUT': 'BO',
                            'VOLATILE': '변동'
                        }
                        entry_display = regime_map.get(entry_regime, entry_regime or '-')
                        
                        # 체제 전환으로 인한 매도만 체제 변화 표시
                        if reason and ('체제전환' in reason or 'regime' in reason.lower() or '체제 전환' in reason):
                            # reason에서 실제 매도 체제 파싱
                            # 예: "체제전환 TRENDING_UP→RANGING" 또는 "체제 전환: TU→TD"
                            import re
                            match = re.search(r'[→:]\s*([A-Z_]+)', reason)
                            if match:
                                actual_exit_regime = match.group(1)
                                exit_display = regime_map.get(actual_exit_regime, actual_exit_regime)
                                regime_display = f"{entry_display}→{exit_display}"
                            else:
                                # 파싱 실패 시 reason 그대로 표시
                                regime_display = f"{entry_display}→?"
                        else:
                            # 기본: 진입 체제만 표시
                            regime_display = entry_display
                        if _judges:
                            regime_display = f"{regime_display}/{_judges}"
                        detail_parts = []
                        if timeframe_val:
                            detail_parts.append(f"[{timeframe_val}]")
                        if reason:
                            # 체제전환은 전체 표시, 나머지는 20자로 제한
                            if '체제전환' in reason or '체제 전환' in reason:
                                detail_parts.append(str(reason))
                            else:
                                detail_parts.append(str(reason)[:20])
                        if confidence > 0:
                            detail_parts.append(f"신뢰:{confidence:.2f}")
                        
                        detail_str = " | ".join(detail_parts) if detail_parts else str(reason)[:20]
                    else:
                        buy_amount = price * volume if (price > 0 and _vol_ok and _m5.isfinite(price * volume)) else None
                        buy_str = f"{buy_amount:,.0f}원" if buy_amount is not None else ("자료없음" if price <= 0 else "산출불가")
                        sell_str = "-"
                        pnl_str = "-"
                        ret_str = "-"
                        emoji = "📥"
                        detail_str = f"[{timeframe_val}] 매수" if timeframe_val else "매수"
                    
                    # SELL 레코드만 진입 체제 표시
                    if side.upper() == 'SELL':
                        _rt_marks.add(buy_str[-1:])
                        write_and_print(f, f"{dt:<20} {symbol:<12} {side:<6} {(_fpk4(price) if price > 0 else '자료없음'):>10} {buy_str:>13} {sell_str:>13} {pnl_str:>13} {ret_str:>10} {regime_display:<20} {selection_source:<24} {emoji} {detail_str}\n")
                    else:
                        write_and_print(f, f"{dt:<20} {symbol:<12} {side:<6} {(_fpk4(price) if price > 0 else '자료없음'):>10} {buy_str:>13} {sell_str:>13} {pnl_str:>13} {ret_str:>10} {'-':<20} {'-':<24} {emoji} {detail_str}\n")
            else:
                write_and_print(f, "⚠️  해당 기간에 거래 내역이 없습니다\n")
            _rt_legend = {"‡": "‡ = 체결 기록 재계산과 DB 손익이 1원 넘게 다름", "†": "† = 매수 연결 미확정, 기록된 진입가로 계산", "*": "* = 매수 연결 미확정, 매도금액 − 손익으로 역산 (수수료 포함)"}
            if _rt_marks & set(_rt_legend):
                write_and_print(f, "매수금액 표시 : " + " · ".join(_rt_legend[_m] for _m in ("‡", "†", "*") if _m in _rt_marks) + "\n")  # REPORT-TRUTH v4.3
            
            write_and_print(f, "\n")
            write_and_print(f, "=" * 150 + "\n")
        
        logger.info(f"\n💾 저장 완료: {report_file.absolute()}")
        
    except Exception as e:
        import traceback
        logger.error("❌ 거래 내역 리포트 생성 실패: %s", e)
        print(traceback.format_exc())
        
# ========================================
# 2. 성과 분석 리포트
# ========================================
@retry_on_db_lock(max_retries=3, delay=0.5)  # ✅ 데코레이터 추가
def generate_impact_report(db_path, output_dir, config: ConfigManager, date_from=None, date_to=None):
    """영향 요인 분석 리포트 생성 (DB Lock 자동 재시도)"""
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    report_file = output_dir / f"impact_{timestamp}.txt"
    
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # 날짜 필터 생성
        date_filter, date_params = build_date_filter(date_from, date_to)
        
        # 컬럼 확인
        cursor.execute("PRAGMA table_info(trade_details)")
        columns = [row[1] for row in cursor.fetchall()]
        
        # 전체 거래 수 먼저 조회 (날짜 필터 적용)
        try:
            query = f"SELECT COUNT(*) FROM trade_details WHERE side = 'SELL'{date_filter}"
            cursor.execute(query, date_params)
            total_trades_count = cursor.fetchone()[0] or 0
        except sqlite3.OperationalError:
            total_trades_count = 0
        
        # 동적 임계값 계산
        min_threshold = get_min_signal_threshold(total_trades_count)
        
        # ========================================
        # 🔥 수정 3: 신호 타입별 통계 - CASE 순서 변경
        # ========================================
        try:
            # 1차 시도: performances 테이블
            query = f"""
                SELECT signal_type, 
                       COUNT(*) as count,
                       SUM(CASE WHEN success = 1 THEN 1 ELSE 0 END) as wins,
                       AVG(profit) as avg_profit,
                       SUM(profit) as total_profit,
                       AVG(return_pct) as avg_return_pct
                FROM performances
                WHERE 1=1{date_filter}
                GROUP BY signal_type
                HAVING count >= {min_threshold}
                ORDER BY count DESC
            """
            cursor.execute(query, date_params)
            signal_stats = cursor.fetchall()
            
            # 2차 시도: performances가 비어있으면 trade_details의 reason 활용
            if not signal_stats:
                query = f"""
                    SELECT 
                        CASE 
                            WHEN reason LIKE '%익절%' OR reason LIKE '%profit%' THEN '익절'
                            WHEN reason LIKE '%추적%' OR reason LIKE '%trailing%' THEN '추적손절'
                            WHEN reason LIKE '%MA%' OR reason LIKE '%이탈%' THEN '15분MA이탈'
                            WHEN reason LIKE '%타임아웃%' OR reason LIKE '%timeout%' THEN '타임아웃'
                            WHEN reason LIKE '%부분%' OR reason LIKE '%partial%' THEN '부분매도'
                            WHEN reason LIKE '%손절%' OR reason LIKE '%stop%' THEN '손절'
                            ELSE SUBSTR(reason, 1, 20)
                        END as signal_type,
                        COUNT(*) as count,
                        SUM(CASE WHEN pnl > 0 THEN 1 ELSE 0 END) as wins,
                        AVG(pnl) as avg_profit,
                        SUM(pnl) as total_profit,
                        AVG(return_pct) as avg_return_pct
                    FROM trade_details
                    WHERE side = 'SELL' AND reason IS NOT NULL AND reason != ''{date_filter}
                    GROUP BY signal_type
                    HAVING count >= {min_threshold}
                    ORDER BY count DESC
                    LIMIT 15
                """
                cursor.execute(query, date_params)
                signal_stats = cursor.fetchall()
                
        except sqlite3.OperationalError as e:
            signal_stats = []
        
        # ========================================
        # 🔥 수정 4: 매도 사유별 통계 - CASE 순서 변경
        # ========================================
        sell_reason_stats = []
        try:
            query = f"""
                SELECT 
                    CASE 
                        WHEN reason LIKE '%익절%' OR reason LIKE '%profit%' OR reason LIKE '%+%' THEN '익절'
                        WHEN reason LIKE '%추적%' OR reason LIKE '%trailing%' THEN '추적손절'
                        WHEN reason LIKE '%MA%' OR reason LIKE '%이평%' OR reason LIKE '%이탈%' THEN 'MA이탈'
                        WHEN reason LIKE '%타임아웃%' OR reason LIKE '%timeout%' THEN '타임아웃'
                        WHEN reason LIKE '%데드크로스%' OR reason LIKE '%dead%' OR reason LIKE '%DC_%' THEN '데드크로스'
                        WHEN reason LIKE '%역배열%' THEN '역배열'
                        WHEN reason LIKE '%긴급%' OR reason LIKE '%emergency%' THEN '긴급청산'
                        WHEN reason LIKE '%손절%' OR reason LIKE '%stop%' THEN '손절'
                        ELSE '기타'
                    END as reason_type,
                    COUNT(*) as count,
                    SUM(CASE WHEN pnl > 0 THEN 1 ELSE 0 END) as wins,
                    AVG(pnl) as avg_pnl,
                    SUM(pnl) as total_pnl,
                    AVG(return_pct) as avg_return_pct
                FROM trade_details
                WHERE side = 'SELL' AND reason IS NOT NULL AND reason != ''{date_filter}
                GROUP BY reason_type
                HAVING count >= 1
                ORDER BY count DESC
            """
            cursor.execute(query, date_params)
            sell_reason_stats = cursor.fetchall()
        except sqlite3.OperationalError as e:
            sell_reason_stats = []
        
        # 시장 체제별 통계 (날짜 필터 적용)
        try:
            query = f"""
                SELECT market_regime,
                       COUNT(*) as count,
                       AVG(return_pct) as avg_return,
                       SUM(pnl) as total_pnl
                FROM trade_details
                WHERE side = 'SELL'{date_filter}
                GROUP BY market_regime
                ORDER BY count DESC
            """
            cursor.execute(query, date_params)
            regime_stats = cursor.fetchall()
        except sqlite3.OperationalError:
            regime_stats = []
        
        # Reality Engine 통계 (날짜 필터 적용)
        reality_stats = None
        try:
            query_parts = ["COUNT(*) as total_trades"]
            
            if 'slippage_pct' in columns:
                query_parts.append("AVG(slippage_pct) as avg_slippage")
                query_parts.append("MAX(slippage_pct) as max_slippage")
            
            if 'fee' in columns:
                query_parts.append("SUM(fee) as total_fees")
            
            query = f"SELECT {', '.join(query_parts)} FROM trade_details WHERE 1=1{date_filter}"
            cursor.execute(query, date_params)
            reality_stats = cursor.fetchone()
        except sqlite3.OperationalError:
            pass
        
        # 전체 통계 (날짜 필터 적용)
        try:
            query = f"""
                SELECT 
                    COUNT(*) as total_trades,
                    SUM(CASE WHEN pnl > 0 THEN 1 ELSE 0 END) as wins,
                    SUM(pnl) as total_pnl,
                    AVG(return_pct) as avg_return
                FROM trade_details
                WHERE side = 'SELL'{date_filter}
            """
            cursor.execute(query, date_params)
            overall_stats = cursor.fetchone()
        except sqlite3.OperationalError:
            overall_stats = None
        
        conn.close()
        
        # Reality Engine 설정
        reality_enabled = config.getboolean('SIMULATION_REALITY', 'enable_reality', fallback=True)
        reality_preset = config.get('SIMULATION_REALITY', 'reality_preset', fallback='medium')
        
        # 텍스트 리포트 생성 (파일 + 화면)
        with open(report_file, 'w', encoding='utf-8') as f:
            write_and_print(f, "=" * 80 + "\n")
            write_and_print(f, "🎯 신호별 성과 분석 리포트\n")
            write_and_print(f, "=" * 80 + "\n")
            write_and_print(f, f"생성 시간: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            write_and_print(f, f"모드: {config.get_cached('mode').upper()}\n")
            
            # 날짜 필터 표시
            if date_from or date_to:
                write_and_print(f, f"날짜 필터: ")
                if date_from and date_to:
                    write_and_print(f, f"{date_from} ~ {date_to}\n")
                elif date_from:
                    write_and_print(f, f"{date_from} 이후\n")
                else:
                    write_and_print(f, f"{date_to} 이전\n")
            else:
                write_and_print(f, f"날짜 필터: 전체\n")
            
            write_and_print(f, "\n")
            
            # Reality Engine 설정
            write_and_print(f, "⚙️  Reality Engine 설정\n")
            write_and_print(f, "-" * 80 + "\n")
            write_and_print(f, f"리얼리티 엔진:  {'ON ✅' if reality_enabled else 'OFF ⚠️'}\n")
            write_and_print(f, f"프리셋:        {reality_preset.upper()}\n")
            
            if reality_enabled:
                slippage_range = config.get('SIMULATION_REALITY', f'{reality_preset}_slippage_range', fallback='0.0-0.3')
                fill_rate_range = config.get('SIMULATION_REALITY', f'{reality_preset}_fill_rate_range', fallback='0.95-1.0')
                
                write_and_print(f, f"슬리피지 범위: {slippage_range}%\n")
                write_and_print(f, f"체결률 범위:   {fill_rate_range}\n")
            write_and_print(f, "\n")
            
            # 실행 통계
            if reality_stats:
                write_and_print(f, "📊 실행 통계\n")
                write_and_print(f, "-" * 80 + "\n")
                
                idx = 0
                total_trades = reality_stats[idx]
                write_and_print(
                    f,
                    f"총 실행 leg:    {total_trades or 0:>10}건\n"
                )
                idx += 1
                
                if 'slippage_pct' in columns:
                    avg_slip = reality_stats[idx]
                    max_slip = reality_stats[idx + 1]
                    write_and_print(f, f"평균 슬리피지:  {avg_slip or 0:>10.3f}%\n")
                    write_and_print(f, f"최대 슬리피지:  {max_slip or 0:>10.3f}%\n")
                    idx += 2
                
                write_and_print(f, "\n")
            
            # 누적 영향
            write_and_print(f, "💰 누적 영향 (Reality Engine)\n")
            write_and_print(f, "-" * 80 + "\n")
            
            if reality_stats:
                total_trades = reality_stats[0]
                total_fees = 0
                
                if 'fee' in columns:
                    total_fees = reality_stats[-1] or 0
                
                write_and_print(
                    f,
                    f"총 실행 leg:    {total_trades or 0:>10}건\n"
                )
                write_and_print(
                    f,
                    f"총 수수료(참고): {total_fees:>10,.0f}원\n"
                )

                if overall_stats:
                    total_pnl = overall_stats[2] or 0
                    write_and_print(
                        f,
                        f"순손익:        {total_pnl:>10,.0f}원 "
                        f"(수수료 반영 완료)\n"
                    )
                write_and_print(f, "\n")
            else:
                write_and_print(f, f"총 거래:        {'0':>10}건\n")
                write_and_print(f, f"총 수수료:      {'0':>10}원\n")
                write_and_print(f, "\n")
            
            # 전체 성과
            if overall_stats and overall_stats[0] > 0:
                total_trades_sell, wins, total_pnl, avg_ret = overall_stats
                win_rate = (wins / total_trades_sell * 100) if total_trades_sell > 0 else 0
                
                write_and_print(f, "📈 전체 성과\n")
                write_and_print(f, "-" * 80 + "\n")
                write_and_print(f, f"완료 거래:      {total_trades_sell or 0:>10}회\n")
                write_and_print(f, f"승리:          {wins or 0:>10}회\n")
                write_and_print(f, f"패배:          {(total_trades_sell or 0) - (wins or 0):>10}회\n")
                write_and_print(f, f"승률:          {win_rate:>10.2f}%\n")
                write_and_print(f, f"총 손익:       {total_pnl or 0:>10,.0f}원\n")
                write_and_print(f, f"평균 수익률:    {avg_ret or 0:>10.2f}%\n")
                write_and_print(f, "\n")
            else:
                write_and_print(f, "📈 전체 성과\n")
                write_and_print(f, "-" * 80 + "\n")
                write_and_print(f, "⚠️  해당 기간에 완료된 거래가 없습니다\n")
                write_and_print(f, "\n")
            
            # ========================================
            # 🆕 매도 사유별 성과
            # ========================================
            if sell_reason_stats:
                write_and_print(f, "🎯 매도 사유별 성과 (팩터 조정 참고용)\n")
                write_and_print(f, "-" * 80 + "\n")
                write_and_print(f, f"{'매도 사유':<15} {'거래수':<10} {'승률':<12} {'평균손익':<15} {'총손익':<15} {'평균수익률':<12}\n")
                write_and_print(f, "-" * 80 + "\n")
                
                for reason_type, count, wins, avg_pnl, total_pnl, avg_return_pct in sell_reason_stats:
                    win_rate = (wins / count * 100) if count > 0 else 0
                    
                    write_and_print(
                        f, 
                        f"{reason_type:<15} "
                        f"{count:>8}회 "
                        f"{win_rate:>10.1f}% "
                        f"{avg_pnl or 0:>13,.0f}원 "
                        f"{total_pnl or 0:>13,.0f}원 "
                        f"{avg_return_pct or 0:>10.2f}%\n"
                    )
                
                write_and_print(f, "\n")
                
                # 팩터 조정 힌트
                write_and_print(f, "💡 팩터 조정 힌트\n")
                write_and_print(f, "-" * 80 + "\n")
                
                # 가장 많이 발동된 사유
                most_common = sell_reason_stats[0]
                reason_name = most_common[0]
                reason_count = most_common[1]
                reason_win_rate = (most_common[2] / most_common[1] * 100) if most_common[1] > 0 else 0
                
                write_and_print(f, f"• 가장 많은 매도 사유: {reason_name} ({reason_count}회, 승률 {reason_win_rate:.1f}%)\n")
                
                # 사유별 조정 제안
                if reason_name == '익절':
                    if reason_win_rate >= 95:
                        write_and_print(f, "  → 익절 레벨이 적절합니다 ✅\n")
                    else:
                        write_and_print(f, "  → 익절 레벨 하향 조정 고려 (더 빨리 익절)\n")
                
                elif reason_name == 'MA이탈':
                    if reason_win_rate >= 90:
                        write_and_print(f, "  → MA 이탈 로직이 효과적입니다 ✅\n")
                    else:
                        write_and_print(f, "  → MA 기준 완화 고려 (20MA로 변경?)\n")
                
                elif reason_name == '손절':
                    if overall_stats and overall_stats[0]:
                        if reason_count > overall_stats[0] * 0.3:
                            write_and_print(f, "  ⚠️ 손절이 너무 많습니다! 진입 로직 재검토 필요\n")
                        else:
                            write_and_print(f, "  → 손절 비율이 적절합니다\n")
                
                elif reason_name == '추적손절':
                    write_and_print(f, "  → 추적손절이 안전하게 작동 중입니다 ✅\n")
                
                write_and_print(f, "\n")
            
            # 신호 타입별 성과
            write_and_print(f, f"📊 신호 타입별 성과 (최소 {min_threshold}회 이상)\n")
            write_and_print(f, "-" * 80 + "\n")
            write_and_print(f, f"{'신호 타입':<20} {'거래수':<10} {'승률':<12} {'평균손익':<15} {'총손익':<15}\n")
            write_and_print(f, "-" * 80 + "\n")
            
            if signal_stats:
                for signal_type, count, wins, avg_profit, total_profit, avg_return_pct in signal_stats:
                    win_rate = (wins / count * 100) if count > 0 else 0
                    write_and_print(f, f"{signal_type:<20} {count:>8}회 {win_rate:>10.1f}% {avg_profit or 0:>13,.0f}원 {total_profit or 0:>13,.0f}원\n")
            else:
                write_and_print(f, f"(데이터 없음 - 최소 {min_threshold}회 이상 거래 필요)\n")

            
            write_and_print(f, "\n")
            
            # 시장 체제별 성과
            write_and_print(f, "🌍 시장 체제별 성과\n")
            write_and_print(f, "-" * 80 + "\n")
            write_and_print(f, f"{'시장 체제':<20} {'거래수':<10} {'평균수익률':<15} {'총손익':<15}\n")
            write_and_print(f, "-" * 80 + "\n")
            
            if regime_stats:
                for regime, count, avg_ret, total_pnl in regime_stats:
                    write_and_print(f, f"{regime or 'UNKNOWN':<20} {count:>8}회 {avg_ret or 0:>13.2f}% {total_pnl or 0:>13,.0f}원\n")
            else:
                write_and_print(f, "(데이터 없음)\n")
            
            write_and_print(f, "\n")
            write_and_print(f, "=" * 80 + "\n")
            
            # Reality Engine 주의사항
            if reality_enabled:
                write_and_print(f, "\n")
                write_and_print(f, "💡 Reality Engine 참고사항\n")
                write_and_print(f, "-" * 80 + "\n")
                write_and_print(f, "• Reality Engine이 활성화되어 실제 거래소와 유사한 환경을 시뮬레이션합니다.\n")
                write_and_print(f, "• 슬리피지: 주문 가격과 체결 가격의 차이\n")
                write_and_print(f, "• 체결률: 주문이 완전히 체결되지 않을 수 있음\n")
                write_and_print(f, "• 실제 거래 전 충분한 테스트를 권장합니다.\n")
                write_and_print(f, "\n")
        
        logger.info(f"\n💾 저장 완료: {report_file.absolute()}")
        
    except Exception as e:
        import traceback
        logger.info(f"❌ 신호별 성과 분석 리포트 생성 실패: {e}")
        print(traceback.format_exc())

# ========================================
# 3. Excel 리포트
# ========================================
@retry_on_db_lock(max_retries=3, delay=0.5)  # ✅ 데코레이터 추가
def generate_excel_report(db_path, output_dir, date_from=None, date_to=None):
    """엑셀 리포트 생성 (DB Lock 자동 재시도)"""
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    report_file = output_dir / f"detailed_{timestamp}.xlsx"
    
    try:
        import pandas as pd
        
        conn = sqlite3.connect(db_path)
        
        # 날짜 필터 생성
        date_filter, date_params = build_date_filter(date_from, date_to)
        
        # 거래 상세 (날짜 필터 적용)
        query = f"SELECT * FROM trade_details WHERE 1=1{date_filter} ORDER BY timestamp DESC"
        df_trades = pd.read_sql_query(query, conn, params=date_params)
        
        # 일별 요약 (날짜 필터 적용)
        query = f"""
            SELECT DATE(timestamp) as date,
                   COUNT(*) as trades,
                   SUM(pnl) as daily_pnl,
                   AVG(return_pct) as avg_return
            FROM trade_details
            WHERE side = 'SELL'{date_filter}
            GROUP BY DATE(timestamp)
            ORDER BY date DESC
        """
        df_daily = pd.read_sql_query(query, conn, params=date_params)
        
        # 신호별 성과 (날짜 필터 적용)
        try:
            query = f"""
                SELECT signal_type,
                       COUNT(*) as count,
                       SUM(CASE WHEN success = 1 THEN 1 ELSE 0 END) as wins,
                       AVG(return_pct) as avg_return,
                       SUM(profit) as total_profit
                FROM performances
                WHERE 1=1{date_filter}
                GROUP BY signal_type
                ORDER BY count DESC
            """
            df_signals = pd.read_sql_query(query, conn, params=date_params)
        except:
            df_signals = pd.DataFrame()
        
        # 종목별 분석 (날짜 필터 적용)
        query = f"""
            SELECT symbol,
                   COUNT(*) as trades,
                   SUM(pnl) as total_pnl,
                   AVG(return_pct) as avg_return
            FROM trade_details
            WHERE side = 'SELL'{date_filter}
            GROUP BY symbol
            ORDER BY trades DESC
        """
        df_symbols = pd.read_sql_query(query, conn, params=date_params)
        
        conn.close()
        
        # Excel 저장
        print("📊 Excel 파일 생성 중...")
        with pd.ExcelWriter(report_file, engine='openpyxl') as writer:
            df_trades.to_excel(writer, sheet_name='거래내역', index=False)
            df_daily.to_excel(writer, sheet_name='일별요약', index=False)
            if not df_signals.empty:
                df_signals.to_excel(writer, sheet_name='신호별성과', index=False)
            df_symbols.to_excel(writer, sheet_name='종목별분석', index=False)
        
        print(f"✅ Excel 상세 리포트 생성 완료!")
        print(f"💾 저장 완료: {report_file.absolute()}")
        
        # 날짜 필터 표시
        if date_from or date_to:
            print(f"📅 날짜 필터: ", end='')
            if date_from and date_to:
                print(f"{date_from} ~ {date_to}")
            elif date_from:
                print(f"{date_from} 이후")
            else:
                print(f"{date_to} 이전")
        
    except ImportError:
        print("❌ pandas 또는 openpyxl이 설치되지 않았습니다.")
        print("설치: pip install pandas openpyxl")
    except Exception as e:
        import traceback
        print(f"❌ Excel 리포트 생성 실패: {e}")
        print(traceback.format_exc())


def generate_report(report_type: str, config: ConfigManager, date_from: Optional[str] = None, date_to: Optional[str] = None):
    """리포트 생성 통합 함수"""
    output_dir = Path("outputs") / "trades"
    output_dir.mkdir(parents=True, exist_ok=True)
    
    db_path = config.get_db_paths()['performance']
    
    # 날짜 형식 검증
    if date_from:
        try:
            datetime.strptime(date_from, '%Y-%m-%d')
        except ValueError:
            print(f"❌ 잘못된 날짜 형식: {date_from}")
            print("올바른 형식: YYYY-MM-DD (예: 2025-01-10)")
            return
    
    if date_to:
        try:
            datetime.strptime(date_to, '%Y-%m-%d')
        except ValueError:
            print(f"❌ 잘못된 날짜 형식: {date_to}")
            print("올바른 형식: YYYY-MM-DD (예: 2025-01-10)")
            return
    
    # 날짜 필터 표시
    if date_from or date_to:
        print(f"\n📅 날짜 필터 적용:")
        if date_from and date_to:
            print(f"   {date_from} ~ {date_to}")
        elif date_from:
            print(f"   {date_from} 이후")
        else:
            print(f"   {date_to} 이전")
        print()
    
    if report_type == 'excel':
        generate_excel_report(db_path, output_dir, date_from, date_to)
    elif report_type == 'trades':
        generate_trades_report(db_path, output_dir, config, date_from, date_to)
    elif report_type == 'impact':
        generate_impact_report(db_path, output_dir, config, date_from, date_to)
    else:
        print(f"❌ 알 수 없는 리포트 타입: {report_type}")
        print("📋 사용 가능: excel, trades, impact")


def main():
    """메인 실행 함수"""
    parser = argparse.ArgumentParser(description='업비트 자동매매 봇 v1.0')
    parser.add_argument('--config', default='config.ini', help='설정 파일 경로')
    parser.add_argument('--mode', choices=['sim', 'real'], help='실행 모드')
    parser.add_argument('--debug', action='store_true', help='디버그 모드')
    parser.add_argument('--report', action='store_true', help='리포트 생성')
    parser.add_argument('--report-type', choices=['trades', 'impact', 'excel'], 
                       default='trades', help='리포트 타입')
    
    # ========================================
    # 날짜 필터 인자 추가
    # ========================================
    parser.add_argument('--date-from', type=str, help='시작 날짜 (YYYY-MM-DD, 예: 2025-01-10)')
    parser.add_argument('--date-to', type=str, help='종료 날짜 (YYYY-MM-DD, 예: 2025-01-15)')
    
    args = parser.parse_args()
    
    initial_log_level = "DEBUG" if args.debug else "INFO"
    setup_logging(initial_log_level)
    logger.info("=" * 70)
    logger.info("✅ 로깅 시스템 초기화 완료 (초기화 로그 파일 저장 활성화)")
    logger.info("=" * 70)
    
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    # 🔥 VSCode + 일반 콘솔 DEBUG 완전 차단
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    # 목적: 모든 환경(VSCode/일반콘솔/서버)에서 콘솔 DEBUG 차단
    # 방법 1: StreamHandler 필터 (일반 환경용)
    # 방법 2: 루트 로거 레벨 설정 (VSCode Debug Console용)
    # 효과: 콘솔 INFO만, 파일 DEBUG 전체 저장
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    import logging
    
    class ConsoleInfoFilter(logging.Filter):
        """콘솔 출력을 INFO 이상으로 제한 (일반 환경용)"""
        def filter(self, record):
            return record.levelno >= logging.INFO
    
    # 필터 인스턴스 생성
    console_filter = ConsoleInfoFilter()
    
    # 1️⃣ 일반 핸들러에 필터 적용
    root_logger = logging.getLogger()
    console_count = 0
    file_count = 0
    
    for handler in root_logger.handlers:
        if isinstance(handler, logging.FileHandler):
            # 파일: --debug일 때만 DEBUG, 평소에는 INFO
            handler.setLevel(logging.DEBUG if args.debug else logging.INFO)
            file_count += 1
            
        elif isinstance(handler, logging.StreamHandler):
            # 콘솔: 필터 적용
            handler.filters.clear()
            handler.addFilter(console_filter)
            handler.setLevel(logging.DEBUG)
            console_count += 1
    
    # 2️⃣ VSCode Debug Console 차단 (핵심!)
    logging.getLogger().setLevel(logging.INFO)
    
    # 3️⃣ 결과 로깅
    logger.info("=" * 70)
    logger.info("🔥 콘솔 DEBUG 완전 차단 (VSCode + 일반 콘솔)")
    logger.info(f"   📺 모든 콘솔: INFO, WARNING, ERROR만 표시")
    logger.info(f"   📁 로그 파일: {'DEBUG 포함 전체 저장' if args.debug else 'INFO 이상만 저장'}")
    logger.info(f"   📊 핸들러: StreamHandler {console_count}개 | FileHandler {file_count}개")
    logger.info(f"   🔧 루트 로거 레벨: {logging.getLevelName(root_logger.level)}")
    logger.info("=" * 70)
    
    # 4️⃣ 디버깅용 (파일에만 저장됨)
    logger.debug("🔍 [핸들러 상태]")
    logger.debug(f"   루트 레벨: {logging.getLevelName(root_logger.level)}")
    logger.debug(f"   핸들러 수: {len(root_logger.handlers)}개")
    
    for i, h in enumerate(root_logger.handlers):
        logger.debug(f"   #{i+1}: {type(h).__name__} = {logging.getLevelName(h.level)} | 필터={len(h.filters)}개")
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    # 🔒 VSCode 뒤늦은 핸들러까지 완전 차단 (Monkey Patch)
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    # 목적: VSCode가 실행 중 추가하는 StreamHandler까지 필터 자동 주입
    # 방법: Logger.addHandler()를 패치하여 모든 콘솔 핸들러에 필터 강제 적용
    # 효과: 언제 핸들러가 추가되든 콘솔은 INFO만, 파일은 DEBUG 유지
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    import logging
    
    # 원본 addHandler 백업
    _original_addHandler = logging.Logger.addHandler
    
    class _ConsoleInfoFilter(logging.Filter):
        """콘솔 전용 INFO 필터 (파일 제외)"""
        def filter(self, record):
            return record.levelno >= logging.INFO
    
    def _patched_addHandler(self, handler):
        """
        StreamHandler 추가 시 자동으로 INFO 필터 주입
        FileHandler는 건드리지 않음 (DEBUG 유지)
        """
        try:
            # StreamHandler이면서 FileHandler가 아닌 경우 (콘솔 핸들러)
            if isinstance(handler, logging.StreamHandler) and not isinstance(handler, logging.FileHandler):
                # 중복 필터 방지: 동일 필터가 없으면 주입
                if not any(isinstance(f, _ConsoleInfoFilter) for f in handler.filters):
                    handler.addFilter(_ConsoleInfoFilter())
                    logger.debug(f"🔒 자동 필터 주입: {type(handler).__name__}")
                
                # 핸들러 레벨은 DEBUG로 두고 필터가 INFO 미만 차단
                handler.setLevel(logging.DEBUG)
        finally:
            # 원본 함수 호출 (핸들러 등록)
            return _original_addHandler(self, handler)
    
    # Monkey Patch 적용
    logging.Logger.addHandler = _patched_addHandler
    logger.info("🔒 핸들러 자동 필터 패치 적용 완료 (VSCode 대응)")
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    
    # Config 로드
    config = ConfigManager(args.config)
    
    # 리포트 모드
    if args.report:
        import sys, os
        if hasattr(sys.stdout, 'name') and sys.stdout.name == os.devnull:
            sys.stdout = sys.__stdout__
            sys.stderr = sys.__stderr__

        try:
            sys.stdout.reconfigure(encoding='utf-8', errors='replace')
            sys.stderr.reconfigure(encoding='utf-8', errors='replace')
        except Exception:
            pass

        generate_report(args.report_type, config, args.date_from, args.date_to)
        return EXIT_CODE_SUCCESS
    
    # 정상 실행 모드
    try:
        if args.mode:
            config.set('TRADING', 'mode', args.mode)
            config._cache_essential_configs()
        
        # 🔧 패치: config 로드 후 log_level 재확인 및 조정
        config_log_level = config.get('LOGGING', 'level', fallback='INFO')
        if args.debug:
            final_log_level = "DEBUG"
        else:
            final_log_level = config_log_level
        
        # 로그 레벨만 재설정 (핸들러는 유지)
        if final_log_level != initial_log_level:
            import logging
            logging.getLogger().setLevel(getattr(logging, final_log_level))
            logger.info(f"🔧 로그 레벨 조정: {initial_log_level} → {final_log_level}")
        
        logger.info("=" * 70)
        logger.info("✅ 로깅 시스템 준비 완료")
        logger.info(f"✅ 로그 파일: trading_bot_{datetime.now().strftime('%Y%m%d')}.log")
        logger.info("✅ 콘솔 출력: 활성화")
        logger.info("=" * 70)
                      
        logger.info("=" * SEPARATOR_LENGTH)
        logger.info("업비트 자동매매 봇 v1.0 시작")
        logger.info(f"모드: {config.get_cached('mode')}")
        logger.info(f"타임프레임: {config.get_cached('timeframe')}")
        logger.info("=" * SEPARATOR_LENGTH)
        
        logger.info(
            "[체제별 거래량 기준] "
            "TRENDING_DOWN/BREAKDOWN=0.50x | "
            "RANGING=0.40x | "
            "TRENDING_UP/BREAKOUT=0.80x"
        )
        
        # 봇 초기화 (상세 로깅)
        try:
            logger.info("=" * 70)
            logger.info("🤖 UnifiedTradingBot 초기화 시작...")
            logger.info("=" * 70)
            bot = UnifiedTradingBot(config)
            logger.info("✅ UnifiedTradingBot 초기화 완료")
            # 🆕 Monitoring 연결 상태 확인 로그
            if hasattr(bot, 'monitoring_manager') and bot.monitoring_manager:
                logger.info("🛰️ MonitoringManager 연결 확인 완료 (Track1/Track2 활성)")
            else:
                logger.warning("⚠️ MonitoringManager 미연결 상태 (Fallback: Track1 only)")
        except Exception as e:
            logger.error("❌ UnifiedTradingBot 초기화 실패: %s", e, exc_info=True)
            import traceback
            logger.error("초기화 실패 상세 스택:\n%s", traceback.format_exc())
            raise
        
        # 시그널 핸들러
        def signal_handler(sig, frame):
            logger.info("종료 신호 수신...")
            bot.is_running = False
        
        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)
        
        # 실행
        exit_code = asyncio.run(bot.run())
        return exit_code
        
    except KeyboardInterrupt:
        logger.info("사용자 중단")
        return EXIT_CODE_SUCCESS
        
    except Exception as e:
        logger.error("실행 오류: %s", e, exc_info=True)
        import traceback
        logger.error("전체 스택 트레이스:\n%s", traceback.format_exc())
        return EXIT_CODE_FAILURE


if __name__ == "__main__":
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    # 🔧 멀티프로세싱 가드 (Windows 필수) (2025-11-07)
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    import multiprocessing as mp
    
    # Windows 프로세스 생성 방식 설정
    if sys.platform == 'win32':
        mp.freeze_support()
        try:
            mp.set_start_method("spawn", force=True)
        except RuntimeError:
            pass  # 이미 설정된 경우 무시
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    
    # 기존 main() 실행
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        logger.info("사용자 중단 - 봇 종료")
        sys.exit(0)
    except Exception as e:
        logger.error("예상치 못한 오류: %s", e, exc_info=True)
        import traceback
        logger.error("전체 스택 트레이스:\n%s", traceback.format_exc())
        sys.exit(1)

# **END**