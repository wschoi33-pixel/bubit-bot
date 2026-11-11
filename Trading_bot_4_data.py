#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
업비트 자동매매 봇 v1.0 - Data Module
========================================
책임: 데이터 수집, API 통신, WebSocket 관리, 캐시 관리
- Upbit REST API 클라이언트
- WebSocket 실시간 데이터 스트림
- 캔들 및 티커 데이터 제공
- 캐시 관리 (메모리/디스크)
- DataFrame 표준화

제공 인터페이스:
  - get_markets(): 마켓 목록 조회
  - get_candles(): 캔들 데이터 조회
  - get_dataframe(): DataFrame 변환
  - get_current_price(): 현재가 조회
  - get_ticker(): 티커 조회
  - execute_order(): 주문 실행
  
금지사항:
  - 지표 계산 (analysis 모듈에서 처리)
  - 거래 신호 생성 (strategy 모듈에서 처리)
  - 성과 분석 (performance 모듈에서 처리)
  - 마켓 분석/선정 (analysis 모듈에서 처리)
  - 거래 실행 판단 (bot 모듈에서 처리)
  
데이터 흐름:
  API → Data → (Cache) → Bot/Strategy/Analysis
  WebSocket → Data → Bot (실시간 가격)
  
개선사항:
  - 중복 로직 제거 및 책임 명확화
  - 과도한 로깅 정리
  - WebSocket 연결 관리 간소화
  - 캐시 전략 단순화
========================================
"""

import uuid
import jwt
import json
import hashlib
import asyncio
import inspect  # 🔥 추가: iscoroutinefunction 체크용
import aiohttp
import time
import math
import pandas as pd
import logging
from typing import List, Dict, Optional, Set, Tuple, Any
from urllib.parse import urlencode, unquote
from decimal import Decimal, ROUND_DOWN  # [보완 v3_4 G2]
from datetime import datetime
from collections import OrderedDict, defaultdict
from pathlib import Path

from Trading_bot_1_core import (
    logger, AdaptiveRateLimiter, CircuitBreaker, singleton, 
    TimeFrame, with_timeout, with_error_handling
)
from global_constants import *


def standardize_dataframe(df):
    """DataFrame 컬럼명 표준화"""
    if df is None or df.empty:
        return df
    return df.rename(columns={
        'trade_price': 'close',
        'opening_price': 'open', 
        'high_price': 'high',
        'low_price': 'low',
        'candle_acc_trade_volume': 'volume'
    })


@singleton
class CacheManager:
    """캐시 관리자 - 단순화된 버전"""
    
    def __init__(self):
        self._cache = OrderedDict()
        self._lock = asyncio.Lock()
        self._max_size = MEMORY_BOUNDS['cache_max_size']
        self._websocket_store = {}  # WebSocket 데이터 저장용
        
    def make_key(self, *args) -> str:
        """캐시 키 생성"""
        key_type = args[0]
        slot = datetime.now().minute // CACHE_SLOT_INTERVAL
        
        if key_type == 'candles':
            return f"{args[1]}_{key_type}_{args[2]}_{slot}"
        elif key_type == 'ticker':
            return f"ticker_{slot}"
        elif key_type == 'price':
            return f"price_{args[1]}_{slot}"
        elif key_type == 'markets':
            return f"markets_{slot // 10}"
        else:
            return "_".join(map(str, args))
    
    # async def get(self, key: str):    2025-10-18 17:06
    #     """캐시 조회"""
    #     async with self._lock:
    #         if key in self._cache:
    #             ts, value, ttl = self._cache[key]
    #             if time.time() - ts < ttl:
    #                 self._cache.move_to_end(key)
    #                 return value
    #             del self._cache[key]
        
    #     # WebSocket 데이터 확인
    #     if key.startswith('price_'):
    #         market = key.split('_')[1]
    #         if market in self._websocket_store:
    #             ws_data = self._websocket_store[market]
    #             if 'timestamp' in ws_data:
    #                 age = (time.time() * 1000 - ws_data['timestamp']) / 1000
    #                 if age <= 30:  # 30초 이내 데이터
    #                     return ws_data.get('trade_price')
        
    #     return None
    
    async def get(self, key: str):
        """캐시 조회 - 완전 비활성화"""
        return None
    
    async def set(self, key: str, value, ttl_minutes: int = DEFAULT_CACHE_TTL_MINUTES):
        # """캐시 저장"""
        # async with self._lock:
        #     if len(self._cache) >= self._max_size:
        #         self._cache.popitem(last=False)
            
        #     self._cache[key] = (time.time(), value, ttl_minutes * SECONDS_PER_MINUTE)
        """캐시 저장 - 완전 비활성화"""
        pass
    
    def update_websocket_data(self, market: str, data: dict):
        """WebSocket 데이터 업데이트"""
        if isinstance(data, dict):
            if 'timestamp' not in data:
                data['timestamp'] = time.time() * 1000
            self._websocket_store[market] = data
    
    async def cleanup(self):
        """만료된 캐시 정리"""
        async with self._lock:
            now = time.time()
            expired = [key for key, (ts, _, ttl) in self._cache.items() 
                      if now - ts >= ttl]
            for key in expired:
                del self._cache[key]


@singleton
class MarketDataService:
    """중앙 데이터 서비스"""
    
    def __init__(self):
        self.api = None
        self.cache = CacheManager()
        self._initialized = False
        self._dataframe_cache = {}  # 🔥 PATCH: DataFrame 캐시 추가
        self._df_epoch = 0  # 🔥 PATCH: 동시성 보호 epoch
        self.ws_manager = None

        # 🔒 REST raw candle 콜백
        # - 미니GC cache closed봉 sealed sync용
        # - Data 모듈은 Analysis를 직접 import하지 않는다.
        self._raw_candle_callbacks = []
    
    def initialize(self, api_client):
        """API 클라이언트 연결"""
        if not self._initialized:
            self.api = api_client
            self._initialized = True
            logger.info("MarketDataService 초기화 완료")
    
    def set_websocket_manager(self, ws_manager):
        """WebSocket 매니저 설정"""
        self.ws_manager = ws_manager

    def add_raw_candle_callback(self, callback):
        """REST raw candle 수신 콜백 등록."""
        try:
            if callable(callback) and callback not in self._raw_candle_callbacks:
                self._raw_candle_callbacks.append(callback)
                logger.debug("[REST-CANDLE-CB] raw candle callback 등록")
        except Exception as e:
            logger.debug(f"[REST-CANDLE-CB] 등록 실패: {e}")

    def _seed_mini_from_df(self, market: str, timeframe: str, df) -> None:
        """[seal-wiring-fix] WS 캐시 df closed봉 → 미니캐시 seal 콜백.
        REST 콜백(get_candles)이 WS 캐시 경로에서는 우회되므로 보완.
        df.index=KST-naive이면 명시적으로 Asia/Seoul localize 후 UTC bucket으로 변환한다.
        """
        try:
            if not self._raw_candle_callbacks or df is None or df.empty:
                return
            if str(timeframe or "").strip().lower() != "3m":
                return
            if "close" not in df.columns:
                return

            if not hasattr(self, "_mini_seed_last"):
                self._mini_seed_last = {}

            _tf_sec = 180
            _now_bucket = int(time.time() // _tf_sec) * _tf_sec
            _key = (market, "3m")

            if self._mini_seed_last.get(_key) == _now_bucket:
                return

            _cs = []
            for _idx, _cl in zip(df.index, df["close"]):
                try:
                    _ts = pd.Timestamp(_idx)
                    if _ts.tzinfo is None:
                        _ts = _ts.tz_localize("Asia/Seoul")
                    else:
                        _ts = _ts.tz_convert("Asia/Seoul")

                    _utc_sec = int(_ts.tz_convert("UTC").timestamp())
                    _bucket = (_utc_sec // _tf_sec) * _tf_sec

                    if _bucket >= _now_bucket:
                        continue

                    _price = float(_cl)
                    if not math.isfinite(_price) or _price <= 0:
                        continue

                    _cs.append({
                        "timestamp": _bucket * 1000,
                        "trade_price": _price,
                    })
                except Exception:
                    continue

            if not _cs:
                return

            self._mini_seed_last[_key] = _now_bucket
            asyncio.create_task(self._notify_raw_candles(market, timeframe, _cs))

        except Exception as e:
            logger.debug(f"[MINI-SEED-DF] {market} 실패: {e}")

    async def _notify_raw_candles(self, market: str, interval: str, candles: list):
        """REST raw candle 수신 콜백 통지. 콜백 실패가 REST 반환을 막지 않게 방어."""
        if not self._raw_candle_callbacks:
            return

        for cb in list(self._raw_candle_callbacks):
            try:
                _ret = cb(market, interval, candles)
                if inspect.isawaitable(_ret):
                    await _ret
            except Exception as e:
                logger.debug(f"[REST-CANDLE-CB] callback 실패: {market} {interval} | {e}")

    @with_error_handling([])
    async def get_markets(self) -> List[Dict]:
        """마켓 목록 조회"""
        cache_key = self.cache.make_key('markets')
        
        # 캐시 확인
        cached = await self.cache.get(cache_key)
        if cached:
            return [m for m in cached if m.get('market', '').startswith('KRW-')]
        
        # API 조회
        if self.api:
            result = await self.api.fetch_markets()
            if result:
                await self.cache.set(cache_key, result, 10)
                return result
        
        return []
    
    async def get_candles(self, market: str, interval: str, count: int = 150, force_refresh: bool = False) -> List[Dict]:
        """캔들 데이터 조회 - 캐시 비활성화 버전"""
        try:
            count = min(count, MAX_CANDLES_PER_REQUEST)
            
            # 항상 API 직접 호출 (캐시 완전 제거)
            logger.debug(f"[REST-REQ] {market} {interval} 캔들 조회")
            
            if self.api:
                data = await self.api.fetch_candles(market, interval, count)
                if data:
                    # 🔒 raw REST candle dict 보존 지점
                    # - REST 반환 경로를 miniGC side-effect가 막으면 안 된다.
                    # - callback은 비동기 fire-and-forget으로 분리한다.
                    if self._raw_candle_callbacks:
                        try:
                            asyncio.create_task(
                                self._notify_raw_candles(market, interval, data)
                            )
                        except Exception as _cb_e:
                            logger.debug(
                                f"[REST-CANDLE-CB] task 생성 실패: {market} {interval} | {_cb_e}"
                            )
                    return data
            
            return []
            
        except Exception as e:
            logger.error(f"[REST-ERR] {market} 캔들 조회 오류: {e}")
            return []
    
    async def get_ticker(self, markets: List[str]) -> List[Dict]:
        """티커 데이터 조회 - WebSocket 우선 + 상태 체크"""
        results = []
        uncached = []
        
        # 🔥 WebSocket 상태 체크
        ws_healthy = False
        if self.ws_manager and hasattr(self.ws_manager, 'is_healthy'):
            ws_healthy = self.ws_manager.is_healthy()
        
        # WebSocket 데이터 우선 (건강할 때만!)
        if ws_healthy and hasattr(self.ws_manager, 'ticker_cache'):
            for market in markets:
                ticker = self.ws_manager.ticker_cache.get(market)
                if ticker:
                    # 신선도 체크 (5초 이내만 사용)
                    age = (time.time() * 1000 - ticker.get('timestamp', 0)) / 1000
                    if age <= 5:
                        results.append(ticker)
                        logger.debug(f"[WebSocket] {market} 사용 (나이: {age:.1f}초)")
                        continue
                uncached.append(market)
        else:
            # WebSocket 비정상 → 전체 API 조회
            uncached = markets
            if not ws_healthy and self.ws_manager:
                logger.debug(f"⚠️ WebSocket 비정상 → REST API 백업 ({len(markets)}개)")
        
        # API 백업
        if uncached and self.api:
            data = await self.api.fetch_ticker(uncached)
            if data:
                results.extend(data)
                logger.debug(f"[REST-RES] 마켓 데이터(티커) 수신 {len(data)}개")
        
        return results
    
    async def get_tickers(self, markets: List[str]) -> List[Dict]:
        """티커 데이터 조회 (get_ticker의 별칭)"""
        return await self.get_ticker(markets)    
        
    async def get_current_price(self, market: str) -> float:
        """현재가 조회 - 캐시 비활성화 + WebSocket 우선"""
        
        # WebSocket 우선 (실시간)
        if self.ws_manager and hasattr(self.ws_manager, 'ticker_cache'):
            ticker = self.ws_manager.ticker_cache.get(market)
            if ticker and 'trade_price' in ticker:
                # 신선도 체크 (3초 이내만 사용)
                age = (time.time() * 1000 - ticker.get('timestamp', 0)) / 1000
                if age <= 3:
                    return float(ticker['trade_price'])
        
        # API 직접 조회 (캐시 제거)
        if self.api:
            tickers = await self.api.fetch_ticker([market])
            if tickers and tickers[0]:
                price = float(tickers[0].get('trade_price', 0))
                return price
        
        return 0
    
    async def get_dataframe(self, market: str, timeframe: str, count: int = 150, force_refresh: bool = False) -> Optional[pd.DataFrame]:
        """캔들 데이터를 DataFrame으로 조회"""
        try:
            # ============================================================
            # 🔥 PATCH: 캐시 DF 재사용 (WS intrabar high/low 유지)
            # - force_refresh=False 인 경우, "현재 진행 중인 봉"이면 캐시 DF 반환
            # - 캔들 경계에서만 REST 재조회하여 DF 교체
            # - 타임프레임 하드코딩 금지 (tf_map 기반)
            # ============================================================
            try:
                tf = str(timeframe or "3m").strip().lower()
                tf_map = {
                    "1m": 60, "3m": 180, "5m": 300, "10m": 600, "15m": 900, "30m": 1800,
                    "1h": 3600, "4h": 14400, "1d": 86400,
                }
                candle_sec = int(tf_map.get(tf, 180))
            except Exception:
                candle_sec = 180

            # REST 성공 응답은 메인 DataFrame의 확정봉 SSoT다.
            cached = self._dataframe_cache.get(market) if market else None

            # 🔧 [REST-FIX-A 2026-10-16] 봉 버킷당 REST 1회 (주석 의도 복원)
            # force_refresh=True 는 항상 REST. False 이고 아래 7 조건이 모두 맞을 때만 캐시 반환 · 하나라도 아니면 기존 REST.
            #  (1) 이 종목·TF 성공 기록 == 현재 봉 버킷 (기록은 REST 원응답이 송신 시점 현재봉일 때만 · 패치 ③ 참고)
            #  (2) 캐시 DF 생산 TF == 요청 TF  (3) 캐시 마지막 봉이 진행 중 (0 ≤ 봉 나이 < candle_sec)
            #  (4) 직전 봉 간격 == candle_sec  (5) WS 티커 0 ≤ 나이 ≤ 3 초 · 0 < 가격 < inf (NaN 탈락)
            #  (6) WS 캔들 (candle.{tf}) 0 ≤ 나이 ≤ 10 초 · 현재봉 DF 반영 확인 (패치 ①)  (7) 캐시 길이 ≥ 요청 count (200 상한)
            # 계수 : call = 진입 · ret_cache = 실제 캐시 반환 · m_* = 첫 탈락 사유 · exc = 예외 (→ REST)
            if not force_refresh and market and cached is not None and len(cached) >= 2:
                _st = None
                _use_cache = False
                _tp = 0.0
                try:
                    _st = getattr(self, "_rest_fix_stat", None)
                    if _st is None:
                        _st = {"call": 0, "ret_cache": 0, "m_bucket": 0, "m_tf": 0, "m_age": 0, "m_gap": 0,
                               "m_tk": 0, "m_wsc": 0, "m_len": 0, "exc": 0}
                        self._rest_fix_stat = _st
                    _st["call"] += 1
                except Exception:
                    _st = None
                # 주기 요약 : 적중 여부 무관 600 초마다 · 자체 try (로그 실패가 데이터 경로를 바꾸지 않음)
                try:
                    _lt = time.time()
                    if _st is not None and _lt - getattr(self, "_rest_fix_log_ts", 0.0) >= 600.0:
                        self._rest_fix_log_ts = _lt
                        logger.info(f"[REST-FIX] {_st}")
                except Exception:
                    pass
                if _st is not None:
                    try:
                        _now = time.time()
                        _bucket = int(_now // candle_sec)
                        _rest_ok = getattr(self, "_rest_bucket_ok", {}) or {}
                        _cache_tf = (getattr(self, "_df_cache_tf", {}) or {}).get(market)
                        _tk = (
                            self.ws_manager.ticker_cache.get(market)
                            if self.ws_manager and hasattr(self.ws_manager, "ticker_cache")
                            else None
                        )
                        _tk_age = (_now * 1000 - float(_tk.get("timestamp", 0) or 0)) / 1000.0 if _tk else 1e9
                        _tp = float(_tk.get("trade_price", 0) or 0) if _tk else 0.0
                        _wsc = (getattr(self, "_ws_candle_ts", {}) or {}).get(market)
                        _wsc_ok = (
                            bool(_wsc) and len(_wsc) >= 3
                            and _wsc[1] == f"candle.{tf}"
                            and 0.0 <= (_now - _wsc[0]) <= 10.0
                            and pd.Timestamp(_wsc[2]) == pd.Timestamp(cached.index[-1])
                        )
                        _ck_last = pd.Timestamp(cached.index[-1])
                        _ck_prev = pd.Timestamp(cached.index[-2])
                        if _ck_last.tzinfo is not None:
                            _ck_last = _ck_last.tz_convert("Asia/Seoul").tz_localize(None)
                        if _ck_prev.tzinfo is not None:
                            _ck_prev = _ck_prev.tz_convert("Asia/Seoul").tz_localize(None)
                        _ck_age = float((pd.Timestamp.now(tz="Asia/Seoul").tz_localize(None) - _ck_last).total_seconds())
                        _ck_gap = float((_ck_last - _ck_prev).total_seconds())

                        if _rest_ok.get((market, tf)) != _bucket:
                            _st["m_bucket"] += 1
                        elif _cache_tf != tf:
                            _st["m_tf"] += 1
                        elif not (0.0 <= _ck_age < candle_sec):
                            _st["m_age"] += 1
                        elif _ck_gap != float(candle_sec):
                            _st["m_gap"] += 1
                        elif not (0.0 <= _tk_age <= 3.0 and 0.0 < _tp < float("inf")):
                            _st["m_tk"] += 1
                        elif not _wsc_ok:
                            _st["m_wsc"] += 1
                        elif len(cached) < min(int(count), MAX_CANDLES_PER_REQUEST):
                            _st["m_len"] += 1
                        else:
                            _use_cache = True
                    except Exception:
                        _st["exc"] += 1
                        _use_cache = False
                if _use_cache:
                    try:
                        # 기존 REST 경로와 같은 티커 오버레이 (close/high/low)
                        _lk = cached.index[-1]
                        cached.at[_lk, "high"] = max(float(cached.at[_lk, "high"]), _tp)
                        cached.at[_lk, "low"] = min(float(cached.at[_lk, "low"]), _tp)
                        cached.at[_lk, "close"] = _tp
                    except Exception:
                        _st["exc"] += 1
                        _use_cache = False
                if _use_cache:
                    _st["ret_cache"] += 1
                    # 기존 REST 경로처럼 캐시 객체 자체를 반환 (WS intrabar 갱신 공유) · 더 짧은 count 요청만 tail
                    return cached if len(cached) <= int(count) else cached.tail(count)

            _rf_send_ts = time.time()  # 🔧 [REST-FIX-A] REST 송신 시각 (패치 ③ 성공 기록 기준)
            candles = await self.get_candles(
                market,
                timeframe,
                count,
                force_refresh=True,
            )

            # REST 실패 시에만 기존 WS/티커 캐시를 유지한다.
            if not candles:
                if cached is not None and not cached.empty:
                    self._seed_mini_from_df(market, timeframe, cached)
                    return cached
                return None
            
            df = pd.DataFrame(candles)
            
            # 🔥 핵심: 시간 역순 정렬 (최신이 마지막에 오도록)
            if 'candle_date_time_kst' in df.columns:
                df['timestamp'] = pd.to_datetime(df['candle_date_time_kst'])
                df = df.sort_values('timestamp', ascending=True)  # 오름차순 정렬
                df = df.set_index('timestamp')
            else:
                # 타임스탬프 없으면 역순으로
                df = df.iloc[::-1].reset_index(drop=True)
            
            df = standardize_dataframe(df)
            
            required_columns = ['open', 'high', 'low', 'close', 'volume']
            if not all(col in df.columns for col in required_columns):
                return None
            
            for col in required_columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
            
            # 🔥 초저가 코인 가격 정밀도 보정 (SHIB 등 float 절삭 방지)
            df['open']  = df['open'].round(6)
            df['high']  = df['high'].round(6)
            df['low']   = df['low'].round(6)
            df['close'] = df['close'].round(6)
            
            # 🔥 PATCH: DataFrame 캐시에 저장 (WebSocket 실시간 업데이트용)
            result_df = df[required_columns].copy()

            # REST 응답 후 커밋 직전에 현재 진행봉의 최신 WS 누적값만 병합한다.
            try:
                fresh = self._dataframe_cache.get(market) if market else None
                if fresh is not None and not fresh.empty and not result_df.empty:
                    _fresh_idx = fresh.index[-1]
                    _rest_idx  = result_df.index[-1]
                    _fresh_row = fresh.iloc[-1]

                    _fresh_ts = pd.Timestamp(_fresh_idx)
                    if _fresh_ts.tzinfo is not None:
                        _fresh_ts = _fresh_ts.tz_convert("Asia/Seoul").tz_localize(None)
                    _now_kst = pd.Timestamp.now(tz="Asia/Seoul").tz_localize(None)
                    _fresh_age = float((_now_kst - _fresh_ts).total_seconds())
                    _fresh_is_current = (0.0 <= _fresh_age < candle_sec)

                    if _fresh_is_current and _fresh_idx == _rest_idx:
                        result_df.at[_rest_idx, "high"] = max(
                            float(result_df.at[_rest_idx, "high"]),
                            float(_fresh_row["high"]),
                        )
                        result_df.at[_rest_idx, "low"] = min(
                            float(result_df.at[_rest_idx, "low"]),
                            float(_fresh_row["low"]),
                        )
                        result_df.at[_rest_idx, "volume"] = max(
                            float(result_df.at[_rest_idx, "volume"]),
                            float(_fresh_row["volume"]),
                        )
                    elif _fresh_is_current and _fresh_idx > _rest_idx:
                        result_df = pd.concat([
                            result_df,
                            fresh.iloc[[-1]][required_columns],
                        ])
            except Exception:
                pass

            # 현재 진행봉의 최종 close/high/low는 가장 최신 티커가 우선이다.
            try:
                ticker = (
                    self.ws_manager.ticker_cache.get(market)
                    if self.ws_manager and hasattr(self.ws_manager, "ticker_cache")
                    else None
                )
                if ticker and not result_df.empty:
                    _ticker_age = (
                        time.time() * 1000 - float(ticker.get("timestamp", 0) or 0)
                    ) / 1000.0
                    _last_key  = result_df.index[-1]
                    _last_idx  = pd.Timestamp(_last_key)
                    if _last_idx.tzinfo is not None:
                        _last_idx = _last_idx.tz_convert("Asia/Seoul").tz_localize(None)
                    _now_kst  = pd.Timestamp.now(tz="Asia/Seoul").tz_localize(None)
                    _bar_age  = float((_now_kst - _last_idx).total_seconds())
                    _ticker_price = float(ticker.get("trade_price", 0) or 0)

                    if (
                        _ticker_price > 0.0
                        and _ticker_age <= 3.0
                        and 0.0 <= _bar_age < candle_sec
                    ):
                        result_df.at[_last_key, "high"] = max(
                            float(result_df.at[_last_key, "high"]),
                            _ticker_price,
                        )
                        result_df.at[_last_key, "low"] = min(
                            float(result_df.at[_last_key, "low"]),
                            _ticker_price,
                        )
                        result_df.at[_last_key, "close"] = _ticker_price
            except Exception:
                pass

            result_df = (
                result_df[~result_df.index.duplicated(keep="last")]
                .sort_index()
                .tail(count)
            )

            if market and not result_df.empty:
                self._df_epoch += 1
                self._dataframe_cache[market] = result_df
                # 🔧 [REST-FIX-A 2026-10-16] 캐시 DF 생산 TF 기록 + 성공 기록
                # 성공 기록 = 요청 "송신" 시각 버킷 · REST 원응답 (WS 병합 전) 마지막 봉이 송신 시점 현재봉일 때만
                # (경계 직전 송신 · 지연 응답 · WS 새 봉 선도착이면 기록 안 함 → 다음 호출도 REST)
                try:
                    if not hasattr(self, "_rest_bucket_ok"):
                        self._rest_bucket_ok = {}
                    if not hasattr(self, "_df_cache_tf"):
                        self._df_cache_tf = {}
                    self._df_cache_tf[market] = tf
                    _rf_last = pd.Timestamp(df.index[-1])
                    if _rf_last.tzinfo is not None:
                        _rf_last = _rf_last.tz_convert("Asia/Seoul").tz_localize(None)
                    _rf_send_bar = (
                        pd.Timestamp(_rf_send_ts, unit="s", tz="UTC")
                        .tz_convert("Asia/Seoul").tz_localize(None)
                        .floor(f"{candle_sec}s")
                    )
                    if _rf_last == _rf_send_bar:
                        self._rest_bucket_ok[(market, tf)] = int(_rf_send_ts // candle_sec)
                    else:
                        self._rest_bucket_ok.pop((market, tf), None)
                except Exception:
                    try:
                        self._rest_bucket_ok.pop((market, tf), None)
                    except Exception:
                        pass

            return result_df
        
        except Exception as e:
            logger.error(f"DataFrame 변환 오류 {market}: {e}")
            return None


class UpbitAPIClient:
    """업비트 API 클라이언트"""
    
    def __init__(self, config, access_key=None, secret_key=None):
        self.config = config
        self.access_key = access_key
        self.secret_key = secret_key
        self.session = None
        
        self.rate_limiter = AdaptiveRateLimiter(
            config.get_cached('calls_per_second') or 8,    # 초당 8회로 증가, # 5 → 8
            config.get_cached('calls_per_minute') or 480   # config 정합 (8×60=480)
        )
        
        self.circuit_breaker = CircuitBreaker(
            failure_threshold=10,  # 5 → 10
            timeout=30.0,          # 60 → 30  
            success_threshold=2    # 3 → 2
        )
        
        self.api_base_url = config.get_cached('api_base_url') or 'https://api.upbit.com'
        self.semaphore = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)
        
        MarketDataService().initialize(self)
    
    async def _ensure_session(self):
        """세션 확인"""
        if not self.session or self.session.closed:
            timeout = aiohttp.ClientTimeout(total=30, connect=10, sock_read=20)
            connector = aiohttp.TCPConnector(limit=100, limit_per_host=30)
            self.session = aiohttp.ClientSession(
                timeout=timeout, connector=connector
            )
    
    def _create_jwt_token(self, query_params: dict = None):
        """JWT 토큰 생성"""
        if not self.access_key or not self.secret_key:
            raise ValueError("API 키 미설정")
        
        payload = {
            'access_key': self.access_key,
            'nonce': str(uuid.uuid4())
        }
        
        if query_params:
            query = unquote(urlencode(query_params, doseq=True)).encode('utf-8')
            hash_obj = hashlib.sha512()
            hash_obj.update(query)
            payload['query_hash'] = hash_obj.hexdigest()
            payload['query_hash_alg'] = 'SHA512'
        
        return jwt.encode(payload, self.secret_key, algorithm='HS256')
    
    async def _request(self, method: str, url: str, **kwargs) -> Optional[Dict]:
        """HTTP 요청 (429 자동 재시도 + 지수 백오프)"""
        await self._ensure_session()
        await self.rate_limiter.wait_if_needed()
        
        if not await self.circuit_breaker.can_attempt():        
            logger.warning(f"[API차단] Circuit breaker 활성화 - 요청 거부")
            return None
        
        # 🆕 요청에서 마켓 코드 추출
        market = "UNKNOWN"
        if 'params' in kwargs:
            params = kwargs['params']
            if isinstance(params, dict):
                market = params.get('market', params.get('markets', 'UNKNOWN'))

        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        # 🔥 429 자동 재시도 로직 (지수 백오프)
        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        max_retries = 3
        backoff = 2  # 시작 대기 시간 (2초)
        
        for attempt in range(1, max_retries + 1):
            try:
                if (kwargs.get('headers') or {}).get('Authorization'):
                    _auth_params = kwargs.get('params', kwargs.get('json'))
                    kwargs['headers'] = {
                        **kwargs['headers'],
                        'Authorization': f'Bearer {self._create_jwt_token(_auth_params)}',
                    }
                async with self.session.request(method, url, **kwargs) as response:
                    if response.status == 200:
                        result = await response.json()
                        await self.circuit_breaker.record_success()
                        return result
                    
                    elif response.status == 429:
                        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
                        # 🆕 개선: 5초 대기 → 지수 백오프 재시도
                        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
                        if attempt < max_retries:
                            logger.warning(
                                f"[429 Rate Limit] {market} - {backoff}s 대기 "
                                f"(시도 {attempt}/{max_retries})"
                            )
                            await asyncio.sleep(backoff)
                            backoff *= 2  # 지수 백오프 (2s → 4s → 8s)
                            # 🔧 [REST-FIX-B 2026-10-16] 재시도도 rate_limiter 경유 (함수 진입부와 같은 호출)
                            await self.rate_limiter.wait_if_needed()
                            continue  # ✅ 다음 재시도
                        else:
                            # 최종 실패
                            logger.error(
                                f"[API실패] {market} - 429 반복 발생, "
                                f"{max_retries}회 시도 후 포기"
                            )
                            # ⚠️ 중요: CircuitBreaker에 기록 안 함 (정상 제어)
                            return None
                    
                    elif response.status == 404:
                        # 🆕 404 오류 특별 처리
                        error_text = await response.text()
                        if "Code not found" in error_text:
                            logger.info(
                                f"[종목미존재] {market} - 상장폐지 또는 잘못된 코드"
                            )
                        else:
                            logger.warning(
                                f"[404오류] {method} {url} | 종목: {market} | "
                                f"응답: {error_text[:100]}"
                            )
                        await self.circuit_breaker.record_failure()
                        return None
                    
                    else:
                        # 🆕 기타 오류
                        error_text = await response.text()
                        logger.error(
                            f"[API오류] {method} {url} | 종목: {market} | "
                            f"상태: {response.status} | "
                            f"응답: {error_text[:200]}"
                        )
                        await self.circuit_breaker.record_failure()
                        return None
                        
            except Exception as e:
                # 🆕 예외 처리
                logger.error(
                    f"[API예외] {method} {url} | 종목: {market} | "
                    f"타입: {type(e).__name__} | "
                    f"메시지: {str(e)[:150]}"
                )
                await self.circuit_breaker.record_failure()
                return None
        
        # 모든 재시도 실패 (이론상 도달 불가, 안전장치)
        logger.error(f"[API실패] {market} - 재시도 루프 탈출, None 반환")
        return None
    
    async def fetch_markets(self) -> List[Dict]:
        """마켓 목록 조회"""
        result = await self._request('GET', f"{self.api_base_url}/v1/market/all", 
                                    params={'isDetails': 'true'}) or []
        
        return [m for m in result if m.get('market', '').startswith('KRW-')]
    
    async def fetch_candles(self, market: str, interval: str, count: int) -> List[Dict]:
        """캔들 데이터 조회 - 로깅 강화"""
        timeframe_map = {
            '1m': 'minutes/1', '3m': 'minutes/3', '5m': 'minutes/5',
            '10m': 'minutes/10', '12m': 'minutes/12', '15m': 'minutes/15', '30m': 'minutes/30',
            '1h': 'minutes/60', '4h': 'minutes/240', '1d': 'days'
        }
        
        # 🆕 타임프레임 검증
        if interval not in timeframe_map:
            logger.error(
                f"[REST-ERR] {market} 지원하지 않는 타임프레임: {interval} | "
                f"지원: {list(timeframe_map.keys())}"
            )
            return []
        
        candle_type = timeframe_map[interval]
        url = f"{self.api_base_url}/v1/candles/{candle_type}"
        
        # 🆕 요청 로깅
        logger.debug(
            f"[REST-REQ] {market} {interval} | "
            f"URL: {url} | Count: {count}"
        )
        
        result = await self._request('GET', url, params={'market': market, 'count': count})
        
        # 🆕 응답 검증
        if result is None:
            logger.error(
                f"[REST-ERR] {market} {interval} | "
                f"_request() 실패 (None 반환)"
            )
            return []
        
        if not isinstance(result, list):
            logger.error(
                f"[REST-ERR] {market} {interval} | "
                f"잘못된 응답 타입: {type(result)}"
            )
            return []
        
        if len(result) == 0:
            logger.warning(
                f"[REST-WARN] {market} {interval} | "
                f"빈 응답 (데이터 없음)"
            )
            return []
        
        logger.debug(
            f"[REST-RES] {market} {interval} ✅ | "
            f"수신: {len(result)}개"
        )
        
        return result
    
    async def fetch_ticker(self, markets: List[str]) -> List[Dict]:
        """ REST 티커 조회 - 재시도 로직 추가"""
        if not markets:
            return []
        
        all_tickers = []
        max_retries = 3
        
        for i in range(0, len(markets), 200):
            batch = markets[i:i+200]
            params = {'markets': ','.join(batch)}
            
            # 재시도 로직
            for attempt in range(max_retries):
                try:
                    result = await self._request('GET', f"{self.api_base_url}/v1/ticker", params=params)
                    
                    if result:
                        all_tickers.extend(result)
                        break
                    else:
                        if attempt < max_retries - 1:
                            logger.warning(f"REST 티커 조회 실패 ({attempt+1}/{max_retries}) - 재시도")
                            await asyncio.sleep(0.5)
                
                except Exception as e:
                    logger.error(f"REST 티커 조회 오류 ({attempt+1}/{max_retries}): {e}")
                    if attempt < max_retries - 1:
                        await asyncio.sleep(0.5)
            
            if i + 200 < len(markets):
                await asyncio.sleep(0.1)
        
        # 결과 검증
        if not all_tickers:
            logger.error(f"❌ REST 마켓 데이터(티커) 조회 실패: {len(markets)}개 요청")
        else:
            logger.debug(f"✅ REST 마켓 데이터(티커) 조회 성공: {len(all_tickers)}/{len(markets)}개")
        
        return all_tickers
    
    async def get_balance(self, *, strict: bool = False) -> List[Dict]:
        """strict=True: 조회 실패를 실제 잔고 0으로 바꾸지 않는다."""
        try:
            headers = {'Authorization': f'Bearer {self._create_jwt_token()}'}
            result = await self._request(
                'GET', f"{self.api_base_url}/v1/accounts", headers=headers
            )
            if (not isinstance(result, list) or not result
                    or any(not isinstance(v, dict) or not v.get('currency')
                           or 'balance' not in v for v in result)):
                raise RuntimeError("잔고 조회 응답 미확정")
            return result
        except Exception as e:
            logger.error(f"잔고 조회 실패: {e}")
            if strict:
                raise
            return []

    async def _order_post(self, params: Dict) -> Dict:
        """주문 전용. 429만 재시도하고 전송 후 불명확한 응답은 UNKNOWN."""
        body = dict(params)
        sent = False
        try:
            if not body.get('identifier'):
                raise ValueError("주문 identifier 필수")
            await self._ensure_session()
            if not await self.circuit_breaker.can_attempt():
                raise RuntimeError("Circuit breaker 활성화")
            for attempt in range(3):
                await self.rate_limiter.wait_if_needed()
                headers = {
                    'Authorization': f'Bearer {self._create_jwt_token(body)}',
                    'Content-Type': 'application/json',
                }
                sent = True
                async with self.session.request(
                    'POST', f"{self.api_base_url}/v1/orders",
                    headers=headers, json=body,
                ) as response:
                    status = response.status
                    try:
                        result = await response.json()
                    except Exception:
                        result = {}
                if not isinstance(result, dict):
                    result = {}
                if status in (200, 201) and result.get('uuid'):
                    result['_submit_state'] = 'accepted'
                    return result
                if status == 429:
                    if attempt < 2:
                        await asyncio.sleep(2 ** (attempt + 1))
                        continue
                    return {'_submit_state': 'rejected', 'error': 'HTTP 429'}
                error = result.get('error')
                error_text = str(error).lower()
                if (status in (400, 401, 403, 422) and error
                        and 'identifier' not in error_text
                        and 'duplicate' not in error_text):
                    return {'_submit_state': 'rejected', 'error': error}
                return {
                    '_submit_state': 'unknown',
                    'uuid': result.get('uuid', ''),
                    'error': error or f"HTTP {status}: 주문 접수 여부 미확정",
                }
        except asyncio.CancelledError:
            raise  # 전송 전 원장은 실행 엔진이 보존한다.
        except Exception as e:
            return {
                '_submit_state': 'unknown' if sent else 'rejected',
                'error': f"{type(e).__name__}: {e}",
            }

    async def buy_market_order(
        self, market: str, price: float, *, identifier: str
    ) -> Dict:
        if not math.isfinite(float(price)) or float(price) <= 0:
            raise ValueError("매수 금액은 유한한 양수여야 한다")
        _price = int(math.floor(float(price)))  # [보완 v3_4 G1] 원 단위 정수 (원본 str(int(price)) 와 같은 표기)
        if _price < 1:
            raise ValueError("매수 금액 정수화 후 0")
        return await self._order_post({
            'market': market, 'side': 'bid', 'price': str(_price),
            'ord_type': 'price', 'identifier': identifier,
        })

    async def sell_market_order(
        self, market: str, volume: float, *, identifier: str
    ) -> Dict:
        if not math.isfinite(float(volume)) or float(volume) <= 0:
            raise ValueError("매도 수량은 유한한 양수여야 한다")
        _vol = Decimal(str(volume)).quantize(Decimal('1e-8'), rounding=ROUND_DOWN)  # [보완 v3_4 G2] 8 자리 내림 · '1e-05' 지수 표기 금지
        if _vol <= 0:
            raise ValueError("매도 수량 8 자리 내림 후 0")
        return await self._order_post({
            'market': market, 'side': 'ask', 'volume': format(_vol, 'f'),
            'ord_type': 'market', 'identifier': identifier,
        })

    async def get_order(
        self, order_id: str = None, *, identifier: str = None
    ) -> Optional[Dict]:
        params = {'uuid': order_id} if order_id else {'identifier': identifier}
        if not next(iter(params.values())):
            raise ValueError("uuid 또는 identifier 필수")
        headers = {'Authorization': f'Bearer {self._create_jwt_token(params)}'}
        return await self._request(
            'GET', f"{self.api_base_url}/v1/order", params=params, headers=headers
        )

    async def cancel_order(self, order_id: str) -> Optional[Dict]:
        params = {'uuid': order_id}
        headers = {'Authorization': f'Bearer {self._create_jwt_token(params)}'}
        return await self._request(
            'DELETE', f"{self.api_base_url}/v1/order", params=params, headers=headers
        )
    
    async def _get_session(self):
        """aiohttp 세션 가져오기 (싱글톤 패턴)"""
        if self.session is None or self.session.closed:
            import aiohttp
            
            # 타임아웃 설정
            timeout = aiohttp.ClientTimeout(total=60)
            
            # 커넥터 설정 (연결 풀)
            connector = aiohttp.TCPConnector(
                limit=100,
                limit_per_host=30,
                ttl_dns_cache=300
            )
            
            self.session = aiohttp.ClientSession(
                timeout=timeout,
                connector=connector
            )
            self._session_created = True
            logger.debug("aiohttp 세션 생성")
        
        return self.session
    
    async def close(self):
        """세션 정리 - 메모리 누수 방지"""
        try:
            if self.session and not self.session.closed:
                await self.session.close()
                logger.debug("aiohttp 세션 종료")
                
                # 세션이 완전히 닫힐 때까지 대기
                import asyncio
                await asyncio.sleep(0.25)
                
                self.session = None
                self._session_created = False
        except Exception as e:
            logger.error(f"aiohttp 세션 종료 오류: {e}")


class BuyFlowBook:
    """🔧 [BUYRATIO-TRADE v2 2026-10-30] ticker 누적 매수/매도 체결 수량의 차분을 봉 단위로 기록. 부분 기록 봉은 비율 계산에서 제외.
    - 여러 봉을 건너뛴 누적 차이는 봉별 배분을 확정할 수 없어 공백 전후 봉을 부분 기록으로 처리한다."""
    _bars = {}
    _prev = {}
    candle_sec = 180
    enabled = False
    MAX_BARS = 400
    _log_ts = 0.0

    @classmethod
    def configure(cls, timeframe=None, source=None):
        tf_map = {"1m": 60, "3m": 180, "5m": 300, "10m": 600, "15m": 900,
                  "30m": 1800, "1h": 3600, "4h": 14400, "1d": 86400}
        if timeframe:
            seconds = tf_map.get(str(timeframe).strip().lower(), 180)
            if seconds != cls.candle_sec:
                cls._bars.clear()
                cls._prev.clear()
            cls.candle_sec = seconds
        if source is not None:
            cls.enabled = str(source).strip().lower() == "trade"

    @classmethod
    def forget(cls, market):
        prev = cls._prev.pop(market, None)
        if prev is not None:
            row = cls._bars.get(market, {}).get(prev[3])
            if row is not None:
                row[2] = True

    @classmethod
    def reset_prev(cls):
        for market in list(cls._prev):
            cls.forget(market)

    @classmethod
    def on_ticker(cls, market, data):
        if not market:
            return
        try:
            ab = float(data["acc_bid_volume"])
            aa = float(data["acc_ask_volume"])
            s = float(data["trade_timestamp"]) / 1000.0
            if not all(math.isfinite(x) and x >= 0.0 for x in (ab, aa, s)):
                cls.forget(market)
                return

            prev = cls._prev.get(market)
            if prev is not None:
                pb, pa, ps, pk = prev
                if s < ps:
                    return
                same_day = int(s // 86400) == int(ps // 86400)
                if same_day and (ab < pb or aa < pa):
                    cls.forget(market)
                    return

            key = pd.Timestamp(int(s // cls.candle_sec * cls.candle_sec + 32400), unit="s")
            book = cls._bars.setdefault(market, {})
            row = book.setdefault(key, [0.0, 0.0, False])
            if len(book) > cls.MAX_BARS:
                for old_key in sorted(book)[:len(book) - cls.MAX_BARS]:
                    book.pop(old_key, None)
            cls._prev[market] = (ab, aa, s, key)

            if prev is None:
                row[2] = True
                return
            if key > pk + pd.Timedelta(seconds=cls.candle_sec):
                # 여러 봉을 건너뛴 누적 차이 = 봉별 배분 불가 → 공백 전후 봉 부분 기록
                row[2] = True
                previous_row = book.get(pk)
                if previous_row is not None:
                    previous_row[2] = True
            if same_day:
                db, da = ab - pb, aa - pa
            else:
                db, da = ab, aa
                # 새 날짜 (UTC 0 시 초기화) 첫 봉이 아니거나 초기화 여부 불명이면 한 봉에 배분 불가 → 부분 기록
                day_start = int(s // 86400) * 86400
                if s - day_start >= cls.candle_sec or (ab >= pb and aa >= pa):
                    row[2] = True

            row[0] += db
            row[1] += da
            if not all(math.isfinite(x) and x >= 0.0 for x in row[:2]):
                cls.forget(market)
                return
            if s - cls._log_ts >= 600.0:
                cls._log_ts = s
                n_ok = sum(not r[2] for bk in cls._bars.values() for r in bk.values())
                logger.info(f"[BUYRATIO-TRADE] 기록 종목 {len(cls._bars)} · 유효 봉 {n_ok} · 봉 {cls.candle_sec} 초 · 사용 {cls.enabled}")
        except (KeyError, TypeError, ValueError, OverflowError):
            cls.forget(market)

    @classmethod
    def window(cls, market, keys):
        try:
            book = cls._bars.get(market)
            if not book:
                return None
            b = a = 0.0
            n_ok = 0
            for key in keys:
                key = pd.Timestamp(key)
                if key.tzinfo is not None:
                    key = key.tz_convert("Asia/Seoul").tz_localize(None)
                row = book.get(key)
                if row is None or row[2]:
                    continue
                if not all(math.isfinite(x) and x >= 0.0 for x in row[:2]):
                    continue
                b += row[0]
                a += row[1]
                n_ok += 1
            if not math.isfinite(b + a):
                return None
            return b, a, n_ok
        except (TypeError, ValueError, OverflowError):
            return None


class WebSocketManager:
    """WebSocket 연결 관리"""
    
    def __init__(self, config=None, market_data_service=None):
        self.config = config
        self.market_data_service = market_data_service  # 🔥 PATCH: 의존성 주입
        self.websocket = None
        self.session = None
        self.markets = set()
        self.ticker_cache = {}
        self.running = False
        self.connection_healthy = False
        self.reconnect_attempts = 0
        self.max_reconnect_attempts = 10
        self.ws_url = 'wss://api.upbit.com/websocket/v1'
        
        # 🆕 추가: 구독 종목 수 제한 설정
        self.market_limit = (
            config.get_cached('top_markets_count') 
            if config and hasattr(config, 'get_cached') 
            else 20
        )
        
        self.receive_task = None
        self.monitor_task = None
        self._connect_lock = asyncio.Lock()
        
        # ========================================
        # 🆕 캔들 확정 콜백 저장소
        # ========================================
        self.candle_callbacks = []
        
        # 🆕 Candle 재구독용 상태 저장
        self._last_candle_markets = []
        self._last_candle_interval = None
        try:
            BuyFlowBook.configure(
                config.get_cached('timeframe') if config and hasattr(config, 'get_cached') else None,
                config.get_cached('buying_ratio_source', 'candle') if config and hasattr(config, 'get_cached') else None,
            )
        except Exception:
            BuyFlowBook.enabled = False
    
    def is_connected(self) -> bool:
        """연결 상태 확인"""
        return (self.websocket is not None and 
                not getattr(self.websocket, 'closed', True))
    
    def is_healthy(self) -> bool:
        """건강 상태 확인"""
        return self.is_connected() and self.connection_healthy
    
    def update_markets(self, new_markets: list, auto_subscribe: bool = True):
        """구독 마켓 업데이트"""
        try:
            new_markets_set = set(new_markets[:self.market_limit])
            
            if new_markets_set != self.markets:
                for _market in (new_markets_set ^ self.markets):
                    BuyFlowBook.forget(_market)
                self.markets = new_markets_set
                
                logger.debug(
                    f"🔄 WebSocket 구독 업데이트: {len(self.markets)}개 "
                    f"(제한: {self.market_limit})"
                )
                
                if auto_subscribe and self.is_connected() and self.running:
                    asyncio.create_task(self._subscribe())
                    
        except Exception as e:
            logger.error(f"마켓 업데이트 오류: {e}")
    
    async def connect(self) -> bool:
        """WebSocket 연결"""
        async with self._connect_lock:
            try:
                if self.is_connected():
                    return True
                
                await self._cleanup_connection()
                
                if not self.session or self.session.closed:
                    timeout = aiohttp.ClientTimeout(total=30)
                    self.session = aiohttp.ClientSession(timeout=timeout)
                
                self.websocket = await self.session.ws_connect(
                    self.ws_url,
                    heartbeat=30,
                    timeout=10,
                    autoclose=False,
                    autoping=True
                )
                
                self.reconnect_attempts = 0
                self.connection_healthy = True
                
                logger.info("WebSocket 연결 성공")
                BuyFlowBook.reset_prev()
                return True
                
            except Exception as e:
                logger.error(f"WebSocket 연결 실패: {e}")
                self.reconnect_attempts += 1
                return False
    
    async def _subscribe(self):
        """마켓 구독 - Ticker + Candle 통합"""
        try:
            if not self.markets or not self.websocket:
                return
            
            # 🔥 Upbit WebSocket: 하나의 메시지에 모든 타입 포함
            subscribe_format = [
                {"ticket": "upbit-bot-v1.0"},
                {"type": "ticker", "codes": list(self.markets)}
            ]
            
            # 🔥 Candle 구독도 같이 추가
            if self._last_candle_interval and self._last_candle_markets:
                subscribe_format.append({
                    "type": self._last_candle_interval,
                    "codes": self._last_candle_markets
                })
                logger.info(
                    f"[WS-통합구독] Ticker {len(self.markets)}개 + "
                    f"Candle {self._last_candle_interval} {len(self._last_candle_markets)}개"
                )
            else:
                logger.info(f"[WS-통합구독] Ticker {len(self.markets)}개")
            
            subscribe_format.append({"format": "DEFAULT"})
            
            await self.websocket.send_str(json.dumps(subscribe_format))
            
        except Exception as e:
            logger.error(f"구독 오류: {e}")
    
    # ========================================
    # 🆕 Upbit 캔들 WebSocket 구독 추가
    # ========================================
    async def _subscribe_candles(self, markets: list, interval: str):
        """
        Upbit 캔들 WebSocket 구독 (Upbit 규격 준수)
        Args:
            markets: 구독할 마켓 리스트 (예: ['KRW-BTC', 'KRW-ETH'])
            interval: 캔들 간격 (예: '3' 또는 '3m' 또는 'candle.3m')
        """
        try:
            if not markets or not self.websocket:
                return
            
            # interval 정규화: '3' -> 'candle.3m'
            itv = str(interval).strip().lower()
            if itv.startswith("candle."):
                candle_type = itv
            else:
                if itv.endswith(("m", "s", "h", "d", "w")):
                    candle_type = f"candle.{itv}"
                else:
                    candle_type = f"candle.{itv}m"
            
            # 🔥 중복 구독 방어
            if (self._last_candle_markets == markets and 
                self._last_candle_interval == candle_type):
                logger.debug(f"🔁 Candle 동일 구독 요청 무시 ({candle_type}, {len(markets)}개)")
                return
            
            # 🔥 상태만 저장 (WebSocket 전송은 상위에서 _subscribe() 호출 필요)
            self._last_candle_markets = list(markets)
            self._last_candle_interval = candle_type
            
            logger.debug(f"[WS-상태저장] Candle {candle_type} {len(markets)}개 (구독은 상위에서)")
            
        except Exception as e:
            logger.error(f"[WS-에러] Candle 구독 실패: {e}")
    
    async def _cleanup_connection(self):
        """연결 정리"""
        if self.websocket:
            try:
                await self.websocket.close()
            except:
                pass
            finally:
                self.websocket = None
    
    def _update_intrabar_highlow(self, market: str, trade_price: float) -> None:
        """
        ticker 수신 즉시, 현재 DF 마지막 봉의 close/high/low를 실시간 보정
        - 🔥 WS 1차: close 실시간 반영 (조기 진입 핵심)
        - 캔들 경계 체크로 고스트 high/low 방지
        """
        try:
            # 🔒 동일 인스턴스만 사용 (절대 새 인스턴스 생성 금지!)
            svc = self.market_data_service
            if not svc or not hasattr(svc, "_dataframe_cache"):
                return
            
            df_cache = svc._dataframe_cache
            if not isinstance(df_cache, dict):
                return
            
            # 🔥 epoch 체크 (df 교체 중이면 스킵)
            epoch_before = getattr(svc, "_df_epoch", 0)
            
            df = df_cache.get(market)
            if df is None or df.empty:
                return
            
            # 🔥 df 가져온 후 epoch 재확인
            epoch_after = getattr(svc, "_df_epoch", 0)
            if epoch_before != epoch_after:
                return  # df 교체됨 → 스킵

            idx = df.index[-1]
            
            # 🔥 캔들 경계 체크 (타임프레임 자동, 3m 하드코딩 제거)
            candle_ts = int((idx.tz_localize("Asia/Seoul") if getattr(idx, "tzinfo", None) is None else idx).timestamp())
            now_ts = int(time.time())
            
            # config 기반 timeframe → seconds 매핑
            tf = None
            try:
                tf = self.config.get_cached("timeframe") if self.config and hasattr(self.config, "get_cached") else None
            except Exception:
                tf = None
            tf = str(tf or "3m")
            
            tf_map = {
                "1m": 60, "3m": 180, "5m": 300, "10m": 600, "15m": 900, "30m": 1800,
                "1h": 3600, "4h": 14400, "1d": 86400,
            }
            candle_sec = int(tf_map.get(tf, 180))
            
            # 마지막 봉이 현재 진행 봉이 아니면 intrabar 보정 금지
            if (now_ts - candle_ts) >= candle_sec:
                return

            # 🔥 close/high/low 갱신 (조기 진입 핵심!)
            cp = float(trade_price)
            
            # close 반영
            df.at[idx, "close"] = cp
            
            # high/low 반영
            if "high" in df.columns:
                df.at[idx, "high"] = max(float(df.at[idx, "high"]), cp)

            if "low" in df.columns:
                df.at[idx, "low"] = min(float(df.at[idx, "low"]), cp)
            
            # (선택) 최신성 표시
            if "_ws_last_tick_ts" not in df.attrs:
                df.attrs["_ws_last_tick_ts"] = {}
            df.attrs["_ws_last_tick_ts"][market] = time.time()
            
            # (제거) INTRABAR-DF-ID 검증 로그 — 운영 환경에서 불필요
            # 🔍 검증 로그: DataFrame 객체 동일성 확인
            # if high_changed or low_changed:
            #     logger.debug(
            #         f"[INTRABAR-DF-ID] {market} df_id={id(df)} "
            #         f"high={df.at[idx,'high']:.2f} low={df.at[idx,'low']:.2f} "
            #         f"price={trade_price:.2f} changed={'H' if high_changed else ''}{'L' if low_changed else ''}"
            #     )

        except Exception:
            pass
    
    async def receive_data(self):
        """데이터 수신 - 안정성 강화"""
        ping_interval = 25  # 25초마다 핑
        last_ping = time.time()
        
        try:
            while self.running and self.is_connected():
                try:
                    # 타임아웃 30초 (핑 간격보다 길게)
                    msg = await asyncio.wait_for(
                        self.websocket.receive(), 
                        timeout=30
                    )
                    
                    if msg.type in (aiohttp.WSMsgType.TEXT, aiohttp.WSMsgType.BINARY):
                        if msg.type == aiohttp.WSMsgType.BINARY:
                            data = json.loads(msg.data.decode('utf-8'))
                        else:
                            data = json.loads(msg.data)
                        
                        if data.get('type') == 'ticker':
                            market = data.get('code')
                            if market:
                                data['market'] = market
                                data['timestamp'] = time.time() * 1000
                                self.ticker_cache[market] = data
                                if market in self.markets:
                                    BuyFlowBook.on_ticker(market, data)
                                
                                # 🔥 SSoT: 주입받은 인스턴스 사용
                                if self.market_data_service:
                                    cache = self.market_data_service.cache
                                    cache.update_websocket_data(market, data)
                                    
                                    # 🔥 intrabar high/low 실시간 보정
                                    trade_price = data.get("trade_price")
                                    if trade_price is not None:
                                        self._update_intrabar_highlow(market, float(trade_price))
                        
                        # ========================================
                        # 🆕 캔들 확정 메시지 처리 (Upbit 규격: candle.3m)
                        # ========================================
                        elif str(data.get('type', '')).startswith('candle.'):
                            market = data.get('code')
                            candle_type = data.get('type')
                            
                            # 🔥 1) MarketDataService DF 캐시에 캔들 반영
                            is_new_candle = False  # 🔥 새 봉 플래그 추가
                            try:
                                # 🔒 동일 인스턴스만 사용 (절대 새 인스턴스 생성 금지!)
                                svc = self.market_data_service
                                if svc and hasattr(svc, "_dataframe_cache"):
                                    df_cache = svc._dataframe_cache
                                    if market in df_cache:
                                        df = df_cache[market]
                                        if df is not None and not df.empty:
                                            # Upbit 캔들 메시지 키 표준화
                                            kst = data.get("candle_date_time_kst")
                                            if kst:
                                                ts = pd.to_datetime(kst)
                                                row = {
                                                    "open": float(data.get("opening_price", 0) or 0),
                                  
                                                    "high": float(data.get("high_price", 0) or 0),
                                                    "low": float(data.get("low_price", 0) or 0),
                                                    "close": float(data.get("trade_price", 0) or 0),
                                                    "volume": float(data.get("candle_acc_trade_volume", 0) or 0),
                                                }
                                                
                                                # append / update
                                                if ts > df.index[-1]:
                                                    df.loc[ts] = row
                                                    # 🔒 150개 유지 (MA120 + mini long seal 141 보장)
                                                    if len(df) > 150:
                                                        df_cache[market] = df.tail(150)
                                                    else:
                                                        df_cache[market] = df
                                                    logger.debug(f"[WS→DF] {market} 새 봉 추가 | len={len(df_cache[market])}")
                                                    is_new_candle = True  # 🔥 새 봉 플래그 설정
                                                elif ts == df.index[-1]:
                                                    # 같은 현재봉이면 값 갱신 (실제 변경 시에만)
                                                    last_row = df.iloc[-1]
                                                    
                                                    # 변경 후: OHLCV 완전 체크 ✅
                                                    if (last_row["close"] != row["close"] or
                                                        last_row["high"] != row["high"] or
                                                        last_row["low"] != row["low"] or
                                                        last_row["volume"] != row["volume"]):
                                                        
                                                        df.loc[df.index[-1], ["open", "high", "low", "close", "volume"]] = [
                                                            row["open"], row["high"], row["low"], row["close"], row["volume"]
                                                        ]
                                                        
                                                        # 로그는 CLOSE 변경 시에만
                                                        if last_row["close"] != row["close"]:
                                                            logger.debug(f"[WS→DF] {market} 현재 봉 갱신")
                                                elif ts in df.index:
                                                    # 🔒 [CLOSED-BAR-FIX 2026-08-01]
                                                    # 마감 직후 도착한 확정 메시지는 이미 존재하는 봉만 정정한다.
                                                    # (드물게 중복 인덱스가 남아있을 경우 df.loc[ts]가
                                                    #  DataFrame을 반환할 수 있어 .iloc[-1]로 단일 행 보장)
                                                    _prev_sel = df.loc[ts]
                                                    _prev = (
                                                        _prev_sel.iloc[-1]
                                                        if isinstance(_prev_sel, pd.DataFrame)
                                                        else _prev_sel
                                                    )
                                                    if (_prev["close"] != row["close"] or
                                                        _prev["high"] != row["high"] or
                                                        _prev["low"] != row["low"] or
                                                        _prev["volume"] != row["volume"]):
                                                        df.loc[ts, ["open", "high", "low", "close", "volume"]] = [
                                                            row["open"], row["high"], row["low"], row["close"], row["volume"]
                                                        ]
                                                        logger.debug(f"[WS→DF] {market} 확정봉 정정 {ts}")
                            except Exception as e:
                                logger.debug(f"[WS→DF] {market} 반영 실패: {e}")
                            
                            # 🔧 [REST-FIX-A2 2026-10-16] WS 캔들 신선도 기록 = DF 반영 확인 뒤에만 (봉 시작 시각 포함)
                            # · 캐시 마지막 봉 == 메시지 봉 ∧ close · volume 일치 → 기록 (값이 같아 쓰기 생략된 경우도 일치로 인정)
                            # · 과거 봉 정정 메시지 → 현재봉 신선도 갱신 안 함 (기존 기록 유지)
                            # · 그 밖 (반영 실패 · 캐시 없음 · 파싱 실패) → 기록 해제 → get_dataframe 은 REST
                            try:
                                _svc_ts = self.market_data_service
                                if _svc_ts is not None and market:
                                    if not hasattr(_svc_ts, "_ws_candle_ts"):
                                        _svc_ts._ws_candle_ts = {}
                                    _wc_state = "fail"
                                    _wc_ts = None
                                    try:
                                        _wc_df = getattr(_svc_ts, "_dataframe_cache", {}).get(market)
                                        _wc_kst = data.get("candle_date_time_kst")
                                        if _wc_df is not None and not _wc_df.empty and _wc_kst:
                                            _wc_ts = pd.to_datetime(_wc_kst)
                                            if _wc_ts == _wc_df.index[-1]:
                                                _wc_last = _wc_df.iloc[-1]
                                                if (float(_wc_last["close"]) == float(data.get("trade_price", 0) or 0)
                                                        and float(_wc_last["volume"]) == float(data.get("candle_acc_trade_volume", 0) or 0)):
                                                    _wc_state = "ok"
                                            elif _wc_ts < _wc_df.index[-1]:
                                                _wc_state = "past"
                                    except Exception:
                                        _wc_state = "fail"
                                    if _wc_state == "ok":
                                        _svc_ts._ws_candle_ts[market] = (time.time(), str(candle_type), _wc_ts)
                                    elif _wc_state == "fail":
                                        _svc_ts._ws_candle_ts.pop(market, None)
                            except Exception:
                                pass

                            # 🔥 2) 새 봉 추가 시에만 봉 닫힘 이벤트 전달
                            if is_new_candle:
                                for cb in self.candle_callbacks:
                                    try:
                                        timeframe = self.config.get_cached('timeframe') if self.config else 'UNKNOWN'
                                        if inspect.iscoroutinefunction(cb):
                                            asyncio.create_task(cb(market, timeframe))
                                        else:
                                            cb(market, timeframe)
                                    except Exception as e:
                                        logger.error(f"❌ CandleClose 콜백 오류: {e}")
                        
                        self.connection_healthy = True
                        
                    elif msg.type in (aiohttp.WSMsgType.ERROR, aiohttp.WSMsgType.CLOSED):
                        logger.warning(f"⚠️ WebSocket 메시지 오류: {msg.type}")
                        self.connection_healthy = False
                        break
                    
                    elif msg.type == aiohttp.WSMsgType.PONG:
                        logger.debug("Pong 수신")
                        self.connection_healthy = True
                        
                except asyncio.TimeoutError:
                    # 타임아웃 발생 - 핑 전송
                    current_time = time.time()
                    if current_time - last_ping >= ping_interval:
                        try:
                            await self.websocket.ping()
                            last_ping = current_time
                            logger.debug("Ping 전송")
                        except Exception as e:
                            logger.error(f"Ping 전송 실패: {e}")
                            self.connection_healthy = False
                            break
                    else:
                        # 핑 간격 아직 안 됨 - 계속 대기
                        continue
                        
                except Exception as e:
                    logger.error(f"데이터 수신 오류: {e}")
                    await asyncio.sleep(1)
                    
        except Exception as e:
            logger.error(f"수신 루프 오류: {e}")
            import traceback
            logger.debug(f"상세:\n{traceback.format_exc()}")
        finally:
            logger.warning("⚠️ WebSocket 수신 루프 종료")
            self.connection_healthy = False
            BuyFlowBook.reset_prev()
    
    async def run_with_reconnect(self):
        """자동 재연결 - 개선 버전 (영구 실패 방지)"""
        try:
            # 초기 연결
            initial_success = await self.connect()
            
            if initial_success:
                if self.markets:
                    await asyncio.sleep(0.5)
                    await self._subscribe()
                
                await asyncio.sleep(0.5)
                self.receive_task = asyncio.create_task(self.receive_data())
            else:
                logger.warning("초기 WebSocket 연결 실패 - 재연결 루프 시작")
            
            # ========================================
            # 재연결 관리 변수
            # ========================================
            last_reset_time = time.time()
            reset_interval = 300  # 5분마다 카운터 리셋
            consecutive_failures = 0
            
            while self.running:
                await asyncio.sleep(5)
                
                # ========================================
                # 주기적 카운터 리셋 (5분마다)
                # ========================================
                current_time = time.time()
                if current_time - last_reset_time >= reset_interval:
                    if self.reconnect_attempts > 0 or consecutive_failures > 0:
                        logger.info(
                            f"🔄 재연결 카운터 자동 리셋 "
                            f"(시도: {self.reconnect_attempts}→0, "
                            f"연속실패: {consecutive_failures}→0)"
                        )
                        self.reconnect_attempts = 0
                        consecutive_failures = 0
                    last_reset_time = current_time
                
                # ========================================
                # 수신 태스크 상태 체크
                # ========================================
                if self.receive_task:
                    if self.receive_task.done():
                        logger.warning("⚠️ 수신 태스크 종료 감지")
                        self.receive_task = None
                        self.connection_healthy = False
                
                # ========================================
                # 재연결 로직 (연결 끊김 시에만)
                # ========================================
                if not self.is_connected():
                    # 백오프 계산 (지수 증가, 최대 60초)
                    backoff = min(60, 5 * (2 ** min(consecutive_failures, 4)))
                    
                    logger.warning(
                        f"🔌 WebSocket 재연결 시도 "
                        f"(총: {self.reconnect_attempts + 1}회, "
                        f"연속실패: {consecutive_failures}회) "
                        f"- {backoff}초 후"
                    )
                    
                    await asyncio.sleep(backoff)
                    
                    # 재연결 시도
                    success = await self.connect()
                    
                    if success:
                        logger.info(
                            f"✅ WebSocket 재연결 성공! "
                            f"(총 시도: {self.reconnect_attempts + 1}회)"
                        )
                        
                        # 카운터 리셋
                        self.reconnect_attempts = 0
                        consecutive_failures = 0
                        
                        # 구독 및 수신 재시작
                        
                        # 🔥 기존 receive_task 완전 종료 보장 (레이스 컨디션 방지)
                        if self.receive_task:
                            try:
                                self.receive_task.cancel()
                                await asyncio.sleep(0.1)  # cancel 완료 대기
                            except Exception:
                                pass
                            self.receive_task = None
                        
                        if self.markets:
                            await asyncio.sleep(1)
                            # 🔥 통합 구독 (_subscribe가 Ticker + Candle 모두 처리)
                            await self._subscribe()
                        
                        self.receive_task = asyncio.create_task(self.receive_data())
                        
                    else:
                        # 실패 카운터 증가
                        self.reconnect_attempts += 1
                        consecutive_failures += 1
                        
                        logger.error(
                            f"❌ WebSocket 재연결 실패 "
                            f"(연속: {consecutive_failures}회, "
                            f"다음 시도: {min(60, 5 * (2 ** min(consecutive_failures, 4)))}초 후)"
                        )
                
                else:
                    # 연결 정상 - 연속 실패 카운터만 리셋
                    if consecutive_failures > 0:
                        logger.debug("✅ WebSocket 연결 정상 복귀")
                        consecutive_failures = 0
                            
        except Exception as e:
            logger.error(f"재연결 루프 오류: {e}")
            import traceback
            logger.debug(f"상세:\n{traceback.format_exc()}")
        finally:
            if self.session and not self.session.closed:
                await self.session.close()
                
    async def start(self):
        """WebSocket 시작"""
        if self.running:
            return
        
        self.running = True
        
        if not self.markets:
            self.markets = {'KRW-BTC', 'KRW-ETH', 'KRW-XRP'}
        
        self.monitor_task = asyncio.create_task(self.run_with_reconnect())
        await asyncio.sleep(0.2)
    
    async def stop(self):
        """WebSocket 종료"""
        self.running = False
        
        tasks = []
        if self.receive_task and not self.receive_task.done():
            tasks.append(self.receive_task)
        if self.monitor_task and not self.monitor_task.done():
            tasks.append(self.monitor_task)
        
        for task in tasks:
            task.cancel()
        
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        
        await self._cleanup_connection()
        
        if self.session and not self.session.closed:
            await self.session.close()
        
        logger.info("WebSocket 종료 완료")
    
    # ========================================
    # 🆕 캔들 확정 콜백 등록 (Trading_bot_7 연동)
    # ========================================
    def add_candle_close_callback(self, callback):
        """
        Trading_bot_7_bot.py에서 호출하는 캔들 확정 이벤트 콜백 등록       
        Args:
            callback: async 함수 또는 일반 함수 (market, timeframe 인자 받음)
        """
        if callable(callback):
            self.candle_callbacks.append(callback)
            logger.info("📝 CandleClose 콜백 등록 완료")
        else:
            logger.warning("⚠️ 유효하지 않은 콜백 함수")
    
    def get_status(self) -> Dict[str, Any]:
        """상태 정보 - 상세 버전"""
        connected = self.is_connected()
        healthy = self.is_healthy()
        
        # 🆕 수신 태스크 상태
        receive_task_status = "running"
        if not self.receive_task:
            receive_task_status = "not_started"
        elif self.receive_task.done():
            receive_task_status = "stopped"
        
        # 🆕 마지막 데이터 수신 시간
        last_update_age = None
        if self.ticker_cache:
            latest_timestamp = max(
                (t.get('timestamp', 0) for t in self.ticker_cache.values()),
                default=0
            )
            if latest_timestamp > 0:
                last_update_age = (time.time() * 1000 - latest_timestamp) / 1000
        
        return {
            'connected': connected,
            'healthy': healthy,
            'markets': list(self.markets),
            'reconnect_attempts': self.reconnect_attempts,
            'mode': 'WebSocket' if healthy else 'REST Fallback',
            'receive_task': receive_task_status,  # 🆕
            'last_data_age_sec': last_update_age,  # 🆕
            'cached_tickers': len(self.ticker_cache)  # 🆕
        }


@singleton
class DataManager:
    """데이터 관리 통합 클래스"""
    
    def __init__(self):
        self.ws_manager = None
        self.price_cache = {}
    
    def set_websocket_manager(self, ws_manager):
        """WebSocket Manager 설정"""
        self.ws_manager = ws_manager
    
    def get_realtime_price(self, market: str) -> Optional[float]:
        """실시간 가격 조회"""
        if self.ws_manager and self.ws_manager.is_healthy():
            ticker = self.ws_manager.ticker_cache.get(market)
            if ticker:
                # [STALEPX] 구독에서 빠진 종목의 옛 시세가 캐시에 남음 → 3 초 넘은 시세는 버림 (get_current_price · RT-HARDSTOP 과 같은 기준)
                _age = (time.time() * 1000 - float(ticker.get('timestamp', 0) or 0)) / 1000
                if _age > 3:
                    return None
                return ticker.get('trade_price')
        return None
    
    def get_websocket_status(self) -> dict:
        """WebSocket 상태"""
        if self.ws_manager:
            return self.ws_manager.get_status()
        return {'error': 'WebSocket Manager not initialized'}
    
    async def update_websocket_markets(self, markets: list, auto_subscribe: bool = True):
        """WebSocket 구독 마켓 업데이트"""
        if self.ws_manager:
            self.ws_manager.update_markets(markets, auto_subscribe=auto_subscribe)

# **END**