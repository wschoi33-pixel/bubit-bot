#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
업비트 자동매매 봇 v1.0 - Execution Abstraction Engine Module
========================================
책임: 실행 로직 추상화, 실거래/시뮬레이션 통합, 주입 패턴 구현
- 실행 엔진 추상 클래스 정의
- 실거래/시뮬레이션 엔진 구현체
- 통합 실행 인터페이스 제공
- 모드별 분기 완전 제거

제공 인터페이스:
  - ExecutionEngine (추상 클래스)
  - RealExecutionEngine.execute_order(): 실거래 주문 실행
  - SimulationExecutionEngine.execute_order(): 시뮬레이션 주문 실행
  - get_balance(): 잔고 조회 (모드 무관)
  - get_positions(): 포지션 조회 (모드 무관)
  
금지사항:
  - 신호 생성 (strategy 모듈에서 처리)
  - 지표 계산 (analysis 모듈에서 처리)
  - 리스크 판단 (risk 모듈에서 처리)
  - 성과 기록 (performance 모듈에서 처리)
  - 직접적인 모드 분기 (if mode == "real" 금지)
  
데이터 흐름:
  Bot → ExecutionEngine (추상) → RealEngine/SimEngine (구현체) → 결과 반환
  
개선사항:
  - 완전한 실행 로직 추상화
  - 의존성 주입 패턴 적용
  - 모드별 분기 100% 제거
  - 테스트 가능한 구조
========================================
"""

from abc import ABC, abstractmethod
from typing import Dict, Optional, List, Any
import asyncio
import uuid
import random
from datetime import datetime
from dataclasses import dataclass, field, replace

from Trading_bot_1_core import (
    logger, Position, ExecutionResult, singleton,
    snap_to_upbit_tick, format_price_krw
)
from global_constants import *

import time
import math
import json
import hashlib
import sqlite3
from pathlib import Path
from decimal import Decimal, ROUND_DOWN  # [보완 v3_4 G2 · G5]
from math import floor, ceil

# ============================================================
# 🧭 Execution Helper Functions (슬리피지/체결가 보정용)
# ============================================================
def _now_ms(): 
    return int(time.time() * 1000)

def _cap_slippage_limit(side, ref_price, pct):
    """슬리피지 상한가 계산"""
    if side == "BUY":
        return ref_price * (1.0 + pct/100.0)
    else:
        return ref_price * (1.0 - pct/100.0)

def _round_tick(price, tick, side):
    """호가 단위 조정"""
    if tick <= 0: 
        return price
    return ceil(price/tick)*tick if side=="BUY" else floor(price/tick)*tick

def _vwap_from_book(side, orderbook, levels, qty):
    """호가창 VWAP 계산 (선택사항 - 현재 미사용)"""
    book_side = 'asks' if side=="BUY" else 'bids'
    lvls = orderbook.get(book_side, [])[:max(1, levels)]
    v = q = 0.0
    remain = qty
    for px, sz in lvls:
        take = min(remain, sz)
        v += px * take
        q += take
        remain -= take
        if remain <= 0: 
            break
    return v/q if q>0 else None

@dataclass
class OrderRequest:
    """주문 요청 통합 데이터"""
    symbol: str
    side: str  # 'buy' or 'sell'
    volume: float
    price: float
    order_type: str = 'market'
    idempotency_key: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    def __post_init__(self):
        if not self.idempotency_key:
            self.idempotency_key = f"{self.side}_{self.symbol}_{uuid.uuid4().hex[:16]}"
        if self.metadata is None:
            self.metadata = {}

@dataclass
class OrderResponse:
    """주문 응답 통합 데이터"""
    success: bool
    order_id: str
    status: str  # 'filled', 'partial', 'cancelled', 'failed'
    executed_price: float
    executed_volume: float
    remaining_volume: Optional[float]  # None = 미확정/수량 산출 불가
    fee: float
    slippage_pct: float
    latency_ms: float
    metadata: Dict[str, Any] = field(default_factory=dict)

class ExecutionEngine(ABC):
    """
    실행 엔진 추상 클래스
    책임: 모든 실행 엔진의 공통 인터페이스 정의
    """
    
    @abstractmethod
    async def initialize(self) -> bool:
        """엔진 초기화"""
        pass
    
    @abstractmethod
    async def execute_order(self, request: OrderRequest) -> OrderResponse:
        """주문 실행 - 구현체별 처리"""
        pass
    
    @abstractmethod
    async def cancel_order(self, order_id: str) -> bool:
        """주문 취소"""
        pass
    
    @abstractmethod
    async def get_balance(self) -> float:
        """잔고 조회"""
        pass
    
    @abstractmethod
    async def get_positions(self) -> Dict[str, Position]:
        """포지션 조회"""
        pass
    
    @abstractmethod
    async def get_order_status(self, order_id: str) -> Optional[Dict]:
        """주문 상태 조회"""
        pass
    
    @abstractmethod
    async def shutdown(self) -> None:
        """엔진 종료"""
        pass

class RealExecutionEngine(ExecutionEngine):
    """
    실거래 실행 엔진
    책임: 실제 거래소 API를 통한 주문 실행
    """
    
    def __init__(self, api_client, config):
        self.api = api_client
        self.config = config
        # self.positions = {}  # ← 삭제! (API에서 직접 조회)
        self.order_cache = {}
        self._initialized = False
        self.fee_rate = UPBIT_TAKER_FEE  # global_constants에서
        # [REAL-ORDER-SAFETY-1017] 미확정·미반영 주문의 단일 영속 원장.
        self._journal = None
        self._journal_fault = ""
        try:
            root = Path(__file__).resolve().parent / "cache"
            root.mkdir(parents=True, exist_ok=True)
            legacy = root / "order_unknown.json"
            if legacy.exists():
                old = json.loads(legacy.read_text(encoding="utf-8-sig"))
                if not isinstance(old, dict) or old:
                    raise RuntimeError("구판 order_unknown.json 대조 필요")
            path = root / "real_order_journal.sqlite3"
            # [보완 v3_10 K7] REAL 원장 = 한 프로세스만 쓰기 (같은 프로세스 안 재사용 허용 · 다른 프로세스 = fault)
            _locks = globals().setdefault("_REAL_JOURNAL_LOCKS", {})
            _lk = str((root / "real_order_journal.lock").resolve()).lower()
            if _lk not in _locks:
                _fh = open(_lk, "a+b")
                try:
                    try:
                        import msvcrt
                        _fh.seek(0); msvcrt.locking(_fh.fileno(), msvcrt.LK_NBLCK, 1)
                    except ImportError:
                        import fcntl
                        fcntl.flock(_fh.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                except OSError:
                    _fh.close()
                    raise RuntimeError("다른 프로세스가 주문 원장 사용 중 (REAL 단일 작성자 · 봇 · 카나리아 중복 실행 금지)")
                _locks[_lk] = _fh
            if path.exists() and path.stat().st_size == 0:
                raise RuntimeError("비어 있는 주문 원장")
            self._journal = sqlite3.connect(path, timeout=2, isolation_level=None)
            self._journal.execute("PRAGMA journal_mode=WAL")
            self._journal.execute("PRAGMA synchronous=FULL")
            if self._journal.execute("PRAGMA quick_check").fetchone()[0] != "ok":
                raise RuntimeError("주문 원장 무결성 오류")
            self._journal.execute(
                "CREATE TABLE IF NOT EXISTS orders "
                "(id TEXT PRIMARY KEY, market TEXT NOT NULL, "
                "closed INTEGER NOT NULL, payload TEXT NOT NULL)"
            )
            self._journal.execute(
                "CREATE TABLE IF NOT EXISTS scope (id INTEGER PRIMARY KEY, key_hash TEXT)"
            )
            self._journal.execute(  # [보완 v3_4 ④] REAL 포지션 영속 상태 (재시작 복원 근거)
                "CREATE TABLE IF NOT EXISTS position_state (symbol TEXT PRIMARY KEY, payload TEXT NOT NULL)"
            )
            if not self.api.access_key:
                raise RuntimeError("주문 원장 계정 키 없음")
            fingerprint = hashlib.sha256(self.api.access_key.encode()).hexdigest()
            self._journal.execute(
                "INSERT OR IGNORE INTO scope VALUES (1, ?)", (fingerprint,)
            )
            if self._journal.execute("SELECT key_hash FROM scope WHERE id=1").fetchone()[0] != fingerprint:
                raise RuntimeError("API 키 변경: 기존 주문 원장 대조 필요")
            self.pending_orders()
        except Exception as e:
            self._journal_fault = str(e)
            logger.critical(f"[ORDER-JOURNAL-HOLD] {e}")

    # 미종결 1 건 = 그 종목 전면 차단 · 미종결 ≥ 이 건수 = 전 종목 신규 매수 정지
    GLOBAL_BUY_HALT_PENDING = 1

    def pending_orders(self) -> List[Dict]:
        """조회 실패 시 빈 목록을 반환하지 않는다."""
        if self._journal_fault or self._journal is None:
            raise RuntimeError(self._journal_fault or "주문 원장 없음")
        rows = self._journal.execute(
            "SELECT id, market, payload FROM orders WHERE closed=0"
        ).fetchall()
        result = []
        for ident, market, payload in rows:
            rec = json.loads(payload)
            if (not isinstance(rec, dict) or rec.get('identifier') != ident
                    or rec.get('symbol') != market):
                self._journal_fault = "주문 원장 레코드 불일치"
                raise RuntimeError(self._journal_fault)
            result.append(rec)
        return result

    def order_block_reason(self, symbol: str, side: str) -> str:
        try:
            pending = self.pending_orders()
            if any(r['symbol'] == symbol for r in pending):
                return "해당 종목의 주문 미종결/포지션 반영 미확정 (조회: real_journal_resolve.py list · 강제 종결 금지)"
            if side.lower() == 'buy' and len(pending) >= self.GLOBAL_BUY_HALT_PENDING:
                return f"미확정 주문 {len(pending)} 건: 신규 매수 전체 차단 (조회: real_journal_resolve.py list · 강제 종결 금지)"
            return ""
        except Exception as e:
            return f"주문 원장 확인 실패: {e}"

    def _write_order(self, rec: Dict, *, reserve: bool = False) -> None:
        if self._journal_fault or self._journal is None:
            raise RuntimeError(self._journal_fault or "주문 원장 없음")
        db = self._journal
        try:
            db.execute("BEGIN IMMEDIATE")
            if reserve:
                duplicate = db.execute(
                    "SELECT 1 FROM orders WHERE id=?", (rec['identifier'],)
                ).fetchone()
                pending_markets = [m for (m,) in db.execute(
                    "SELECT market FROM orders WHERE closed=0"
                ).fetchall()]
                blocked = (rec['symbol'] in pending_markets
                           or (rec['side'] == 'buy'
                               and len(pending_markets) >= self.GLOBAL_BUY_HALT_PENDING))
                if duplicate or blocked:
                    raise ValueError("중복 논리 주문 또는 미확정 주문 존재")
                next_rev = 1
                saved = dict(rec, _rev=next_rev)
                db.execute(
                    "INSERT INTO orders VALUES (?, ?, 0, ?)",
                    (rec['identifier'], rec['symbol'],
                     json.dumps(saved, ensure_ascii=False, allow_nan=False)),
                )
            else:
                row = db.execute(
                    "SELECT closed, payload FROM orders WHERE id=?",
                    (rec['identifier'],),
                ).fetchone()
                if row is None:
                    raise RuntimeError("주문 원장 갱신 대상 없음")
                current = json.loads(row[1])
                current_rev = current.get('_rev', 0)  # 구판 레코드는 revision 0
                expected_rev = rec.get('_rev', 0)
                if (type(current_rev) is not int or current_rev < 0
                        or type(expected_rev) is not int or expected_rev < 0):
                    raise RuntimeError("주문 원장 revision 오류")
                if row[0] != 0 or current_rev != expected_rev:
                    raise ValueError("원장 최신판 변경/종결: 오래된 갱신 거부")
                next_rev = current_rev + 1
                saved = dict(rec, _rev=next_rev)
                cur = db.execute(
                    "UPDATE orders SET closed=?, payload=? WHERE id=? AND closed=0",
                    (int(rec.get('closed', False)),
                     json.dumps(saved, ensure_ascii=False, allow_nan=False),
                     rec['identifier']),
                )
                if cur.rowcount != 1:
                    raise RuntimeError("주문 원장 갱신 대상 없음")
            db.execute("COMMIT")
            rec['_rev'] = next_rev
        except Exception as e:
            if db.in_transaction:
                db.execute("ROLLBACK")
            if not isinstance(e, ValueError):
                self._journal_fault = str(e)
            raise

    @staticmethod
    def _fill_snapshot(order: Dict, rec: Dict) -> Dict:
        """식별자·상태·체결 합계를 확인한 값만 포지션에 전달한다."""
        if not isinstance(order, dict):
            raise ValueError("주문 조회 응답 형식 오류")
        oid = order.get('uuid')
        if (not oid or (rec.get('uuid') and oid != rec['uuid'])
                or (not rec.get('uuid') and order.get('identifier') != rec['identifier'])
                or order.get('market') != rec['symbol']
                or order.get('side') != ('bid' if rec['side'] == 'buy' else 'ask')
                or order.get('identifier', rec['identifier']) != rec['identifier']):
            raise ValueError("주문 식별자/종목/방향 불일치")

        def number(value):
            value = float(value)
            if not math.isfinite(value) or value < 0:
                raise ValueError("체결 숫자 비유한/음수")
            return value

        state = order.get('state')
        if state not in ('wait', 'watch', 'done', 'cancel'):
            raise ValueError("알 수 없는 주문 상태")
        trades = order.get('trades')
        if not isinstance(trades, list) or int(order['trades_count']) != len(trades):
            raise ValueError("체결 목록 개수 불일치")
        for t in trades:
            number(t['volume'])
        _vq = sum((Decimal(str(t['volume'])) for t in trades), Decimal(0))  # [보완 v3_5] 거래소 원문 정확 합
        volume = float(_vq)
        funds = sum(number(t['funds']) for t in trades)
        if not math.isclose(volume, number(order['executed_volume']),
                            rel_tol=1e-10, abs_tol=1e-12):
            raise ValueError("체결 합계 불일치")
        fee = number(order['paid_fee'])
        remaining = order.get('remaining_volume')
        remaining = number(remaining) if remaining is not None else None
        if volume > 0 and (funds <= 0 or not math.isfinite(funds / volume)):
            raise ValueError("체결 금액/가격 오류")
        if rec['side'] == 'sell' and volume > rec['volume'] * (1 + 1e-10):
            raise ValueError("요청보다 큰 매도 체결량")
        previous = rec.get('snapshot', {})
        if (volume < previous.get('volume', 0.0)
                or fee < previous.get('fee', 0.0)):
            raise ValueError("체결 누계 역행")
        if state == 'done' and remaining not in (None, 0.0):
            raise ValueError("체결 완료와 잔량 불일치")
        terminal = state in ('done', 'cancel')
        satisfied = rec['side'] == 'buy' or volume >= rec['volume'] * (1 - 1e-10)
        return dict(uuid=oid, state=state, volume=volume, volume_q=str(_vq), funds=funds, fee=fee,
                    remaining=remaining, terminal=terminal, satisfied=satisfied)

    def acknowledge_order(self, response: OrderResponse, *, accounting_ok: bool, debt: Optional[Dict] = None) -> None:
        ident = response.metadata['identifier']
        row = self._journal.execute(
            "SELECT payload FROM orders WHERE id=?", (ident,)
        ).fetchone()
        if not row:
            raise RuntimeError("반영 확인 대상 주문 없음")
        rec = json.loads(row[0])
        snap = rec['snapshot']
        if (response.order_id != snap['uuid']
                or response.executed_volume != snap['volume']
                or response.fee != snap['fee']):
            raise RuntimeError("주문 반영 확인 값 불일치")
        self._add_debts(rec, debt)  # [보완 v3_5] 회계 부채 (사건별 · 해소 전 종결 금지) · [보완 v3_6] 공통 · 중복 금지
        rec.update(applied_volume=response.executed_volume, applied_funds=float(snap.get('funds', 0.0)),  # [보완 v3_4 ①] 증분 ΔF 기준
                   applied_fee=response.fee,
                   accounting_ok=bool(accounting_ok) and bool(rec.get('accounting_ok', True)) and not rec.get('accounting_debt'))
        # [보완 v3_4 G3] 봇이 반영한 수량·수수료 = 거래소 확인 체결 (acknowledge 는 기대수량 == 반영수량 확인 뒤에만 호출) 이면 요청량 미충족 (부분 체결 뒤 취소) 이어도 종결 · 남은 단계 목표는 7_bot tp_stage_filled 가 관리
        rec['closed'] = bool(snap['terminal'] and rec['accounting_ok'])  # [보완 v3_5] 부채 · 과거 실패 포함
        self._write_order(rec)

    # ---------- [보완 v3_4 ①] 늦은 체결 복구 (REAL_LATE_FILL_RECOVERY_DESIGN_v2) ----------
    def _pending_rec(self, ident: str) -> Dict:
        for r in self.pending_orders():
            if r['identifier'] == ident:
                return r
        raise RuntimeError(f"미종결 원장 레코드 없음: {ident}")

    @staticmethod
    def _add_debts(rec: Dict, debt) -> None:
        """[보완 v3_6] 부채 dict 또는 목록 · 같은 kind+reason 미해소 부채가 있으면 다시 넣지 않음"""
        items = debt if isinstance(debt, list) else ([debt] if debt else [])
        cur = rec.setdefault('accounting_debt', [])
        for d in items:
            if d.get('ident'):  # [보완 v3_10 K1] 회계 의무 식별 = kind + 주문 id (reason 은 리포트 분류용)
                same = [x for x in cur if x.get('kind') == d.get('kind') and x.get('ident') == d.get('ident')]
            else:
                same = [x for x in cur if x.get('kind') == d.get('kind') and x.get('reason') == d.get('reason')]
            if same:
                x = same[0]
                if d.get('target_v') is not None and float(d['target_v']) > float(x.get('target_v') or 0.0):
                    x.update({k: d[k] for k in ('target_v', 'target_f', 'target_c', 'efp', 'efp_total', 'ov', 'avg_price') if k in d})
                    x['updated_at'] = datetime.now().isoformat()  # 같은 주문 · 늘어난 목표 = 의무 갱신 (버리지 않음)
                continue
            cur.append(dict(d, at=datetime.now().isoformat()))
        if not cur:
            rec.pop('accounting_debt', None)

    @staticmethod
    def _closable(rec: Dict) -> bool:
        """[보완 v3_6 ④] 종결 = 거래소 종결 ∧ 회계 완료 ∧ 반영 = 확인 체결 (수량 · 수수료 · 금액) ∧ 진행 중 복구 없음 · finish_recovery · resolve_debt 공통"""
        snap = rec.get('snapshot') or {}
        return bool(snap.get('terminal') and rec.get('accounting_ok') and not rec.get('accounting_debt') and not rec.get('recover')
                    and rec.get('applied_volume') == snap.get('volume') and rec.get('applied_fee') == snap.get('fee')
                    and abs(float(rec.get('applied_funds', 0.0) or 0.0) - float(snap.get('funds', 0.0) or 0.0)) <= 1e-8)

    def save_position_state(self, symbol: str, state: Dict) -> None:
        """[보완 v3_4 ④] 포지션 영속 상태 저장 (원장과 같은 DB · 같은 계정 범위)"""
        if self._journal_fault or self._journal is None:
            raise RuntimeError(self._journal_fault or "주문 원장 없음")
        self._journal.execute("INSERT OR REPLACE INTO position_state VALUES (?, ?)", (symbol, json.dumps(state, ensure_ascii=False, allow_nan=False)))

    def load_position_state(self, symbol: str) -> Optional[Dict]:
        if self._journal_fault or self._journal is None:
            raise RuntimeError(self._journal_fault or "주문 원장 없음")
        row = self._journal.execute("SELECT payload FROM position_state WHERE symbol=?", (symbol,)).fetchone()
        return json.loads(row[0]) if row else None

    def drop_position_state(self, symbol: str) -> None:
        if self._journal_fault or self._journal is None:
            raise RuntimeError(self._journal_fault or "주문 원장 없음")
        self._journal.execute("DELETE FROM position_state WHERE symbol=?", (symbol,))

    def mark_apply_hold(self, ident: str, reason: str) -> None:
        """반영 불일치 (L3) · 롤백 연결 (L5) 표시 = 자동 복구 금지 · 증거 대조 대상 (수동 종결 아님)"""
        rec = self._pending_rec(ident)
        if not rec.get('apply_hold'):
            rec['apply_hold'] = str(reason)
            self._write_order(rec)

    def late_fills(self) -> List[Dict]:
        """자동 복구 대상 = 확인 체결 (snapshot) > 반영 (applied) 또는 수수료 증가 또는 진행 중 intent · apply_hold 제외"""
        out = []
        for rec in self.pending_orders():
            snap = rec.get('snapshot') or {}
            if not snap or rec.get('apply_hold'):
                continue
            dv = float(snap.get('volume', 0.0)) - float(rec.get('applied_volume', 0.0) or 0.0)
            dc = float(snap.get('fee', 0.0)) - float(rec.get('applied_fee', 0.0) or 0.0)
            df_ = abs(float(snap.get('funds', 0.0)) - float(rec.get('applied_funds', 0.0) or 0.0))  # [보완 v3_5] 금액만 차이
            if dv > 1e-12 or dc > 1e-12 or df_ > 1e-8 or rec.get('recover'):
                out.append(rec)
        return out

    def begin_recovery(self, ident: str) -> Dict:
        """① write-ahead : 목표 (snapshot) · 증분 (ΔV · ΔF 수수료 제외 · ΔC) · 마커를 반영 전에 원장에 기록 · 이미 있으면 그대로 반환"""
        rec = self._pending_rec(ident)
        if rec.get('recover'):
            return rec['recover']
        snap = rec['snapshot']
        av = float(rec.get('applied_volume', 0.0) or 0.0); af = float(rec.get('applied_funds', 0.0) or 0.0); ac = float(rec.get('applied_fee', 0.0) or 0.0)
        it = dict(base_v=av, base_f=af, base_c=ac,  # [보완 v3_4 ⑥] 복구 사건 고정 기준
                  target_v=float(snap['volume']), target_f=float(snap['funds']), target_c=float(snap['fee']),
                  dv=float(snap['volume']) - av, df=float(snap['funds']) - af, dc=float(snap['fee']) - ac, step=0,
                  marker=f"{ident}@{float(snap['volume']):.8f}@{float(snap['funds']):.8f}@{float(snap['fee']):.8f}")
        rec['recover'] = it
        self._write_order(rec)
        return it

    def recovery_step(self, ident: str, step: int) -> None:
        rec = self._pending_rec(ident)
        rec['recover']['step'] = int(step)
        self._write_order(rec)

    def finish_recovery(self, ident: str, accounting_ok: bool, debt: Optional[Dict] = None) -> None:
        """④ applied = 목표 · intent → recovered 이력 (추가 전용) · 종결 = 기존 규칙 (거래소 종결 ∧ 반영 일치 ∧ 기록 성공)"""
        rec = self._pending_rec(ident)
        it = rec.pop('recover')
        rec.setdefault('recovered', []).append(dict(it, at=datetime.now().isoformat(), accounting_ok=bool(accounting_ok)))
        self._add_debts(rec, debt)  # [보완 v3_6] 목록 · 중복 금지
        rec.update(applied_volume=it['target_v'], applied_funds=it['target_f'], applied_fee=it['target_c'],
                   accounting_ok=bool(accounting_ok) and bool(rec.get('accounting_ok', True)) and not rec.get('accounting_debt'))  # [보완 v3_5] 과거 실패 · 부채 누적
        rec['closed'] = self._closable(rec)  # [보완 v3_6 ④] 공통 종결
        self._write_order(rec)

    def pending_debts(self) -> List:
        """[보완 v3_5] 미종결 주문의 회계 부채 (ident, 위치, 내용)"""
        out = []
        for rec in self.pending_orders():
            for i, d in enumerate(rec.get('accounting_debt') or []):
                out.append((rec['identifier'], i, d))
        return out

    def resolve_debt(self, ident: str, idx: int, evidence: str) -> None:
        """[보완 v3_5] 부채 해소 (증거 기록) · 남은 부채 0 = 회계 완료 → 종결 재판정 (거래소 종결 ∧ 반영 일치)"""
        rec = self._pending_rec(ident)
        debts = list(rec.get('accounting_debt') or [])
        d = debts.pop(idx)
        rec.setdefault('debt_resolved', []).append(dict(d, evidence=str(evidence), resolved_at=datetime.now().isoformat()))
        rec['accounting_debt'] = debts
        if not debts:
            rec['accounting_ok'] = True
            rec['closed'] = self._closable(rec)  # [보완 v3_6 ④] finish_recovery 와 같은 조건 (금액 포함)
        self._write_order(rec)

    def list_position_states(self) -> Dict:
        if self._journal_fault or self._journal is None:
            raise RuntimeError(self._journal_fault or "주문 원장 없음")
        return {s: json.loads(p) for s, p in self._journal.execute("SELECT symbol, payload FROM position_state").fetchall()}

    async def refresh_pending_orders(self) -> List[Dict]:
        """조회만 수행. 오래된 조회가 체결 반영·종결 기록을 덮어쓰지 못한다."""
        for rec in self.pending_orders():
            try:
                order = await self.api.get_order(
                    rec.get('uuid'), identifier=rec['identifier']
                )
                if order:
                    snap = self._fill_snapshot(order, rec)
                    rec['uuid'], rec['snapshot'] = snap['uuid'], snap
                    rec['closed'] = bool(snap['terminal'] and snap['volume'] == 0 and not rec.get('recover')) or self._closable(rec)  # [보완 v3_10 K5] 금액 일치 · 진행 중 복구 없음 포함
                    self._write_order(rec)
            except Exception as e:
                logger.critical(f"[ORDER-RECONCILE] {rec['symbol']} {e}")
        return self.pending_orders()

    async def _execute_tracked(
        self, request: OrderRequest, reference: float,
        quote_age: float, slip_limit: float
    ) -> OrderResponse:
        ident = "tb" + uuid.uuid5(uuid.NAMESPACE_URL, str(request.idempotency_key)).hex
        rec = dict(identifier=ident, symbol=request.symbol, side=request.side.lower(),
                   volume=float(request.volume), uuid="", state="prepared",
                   tp_stage=int(request.metadata.get('tp_stage', 0) or 0), tp_ratio=float(request.metadata.get('tp_ratio', 0) or 0),  # [보완 v3_4 ①]
                   created_at=datetime.now().isoformat(), closed=False, ev=1)  # [보완 v3_8] 사건 식별 (event_id) 이후 예약 = 성과 DB 부족분 자동 기록 자격
        started = time.monotonic()
        reserved = False
        error = ""
        try:
            amount = float(request.metadata.get('krw_amount',
                                               request.volume * request.price))
            if rec['side'] == 'buy' and (not math.isfinite(amount) or amount <= 0):
                raise ValueError("매수 주문금액 오류")
            # [보완 v3_4 G1 · G2 · G5] 전송값 정규화 → 재검증 → 원장 기준량 = 전송량 (예약 전 · 실패 = 전송 없음 · 원장 없음)
            if rec['side'] == 'buy':
                amount = float(math.floor(amount))
                if amount < 1:
                    raise ValueError("매수 주문금액 정수화 후 0")
                if reference is None or not math.isfinite(float(reference)) or float(reference) <= 0:
                    raise ValueError("매수 기준가 없음 / 비유한 / 0 이하 → 전송 전 차단")  # [보완 v3_6] max(nan, 1e-12) = nan → 상한 우회 차단
                if amount / float(reference) >= 8.0e6:
                    raise ValueError("예상 수량 ≥ 8.0e6 : float 로 1e-8 단위 정확 보존 근거 밖 (ulp ≤ 0.5e-8 조건 |x| < 2^24 의 여유) → 신규 매수 전송 전 차단")  # [보완 v3_5] (매도는 막지 않음)
            else:
                rec['volume'] = float(Decimal(str(request.volume)).quantize(Decimal('1e-8'), rounding=ROUND_DOWN))
                if not rec['volume'] > 0:
                    raise ValueError("매도 수량 8 자리 내림 후 0")
                if rec['volume'] >= 8.0e6:
                    logger.warning(f"[PRECISION-WARN] {request.symbol} 매도 수량 {rec['volume']} ≥ 8.0e6 : 1e-8 정확 보존 근거 밖 · 매도는 진행 (자산 보존)")  # [보완 v3_5]
            rec['amount'] = amount if rec['side'] == 'buy' else 0.0
            self._write_order(rec, reserve=True)  # POST 이전에 영속 기록
            reserved = True
            if rec['side'] == 'buy':
                submitted = await self.api.buy_market_order(
                    request.symbol, amount, identifier=ident
                )
            else:
                submitted = await self.api.sell_market_order(
                    request.symbol, rec['volume'], identifier=ident  # [보완 v3_4 G5] 정규화된 전송량
                )
            if submitted.get('_submit_state') == 'rejected':
                rec.update(state='rejected', closed=True, error=str(submitted.get('error')))
                self._write_order(rec)
            else:
                rec.update(uuid=submitted.get('uuid', ''), state='unknown')
                self._write_order(rec)
                for attempt in range(6):
                    order = await self.api.get_order(rec.get('uuid'), identifier=ident)
                    if order:
                        # 이상 응답도 식별자를 잃지 않은 상태로 원장에 남긴다.
                        rec['last_response'] = repr(order)[:8000]
                        self._write_order(rec)
                        snap = self._fill_snapshot(order, rec)
                        rec.update(uuid=snap['uuid'], snapshot=snap, state=snap['state'])
                        rec['closed'] = snap['terminal'] and snap['volume'] == 0
                        self._write_order(rec)
                        if snap['terminal']:
                            break
                    if attempt < 5:
                        await asyncio.sleep(0.25)
        except asyncio.CancelledError:
            # prepared/조회 결과는 이미 영속화되어 있다. 취소는 호출자에게 전달.
            raise
        except Exception as e:
            error = f"{type(e).__name__}: {e}"
            if reserved:
                rec['error'] = error
                try:
                    self._write_order(rec)
                except Exception as save_error:
                    logger.critical(f"[ORDER-JOURNAL-HOLD] {save_error}")

        snap = rec.get('snapshot', {})
        volume, funds = snap.get('volume', 0.0), snap.get('funds', 0.0)
        price = funds / volume if volume > 0 else 0.0
        unknown = bool(reserved and not rec.get('closed') and (
            error or not snap.get('terminal') or not snap.get('satisfied')))
        status = ('partial' if unknown else 'filled') if volume > 0 else (
            'unknown' if unknown else 'cancelled' if snap else 'failed')
        slip = ((price - reference) / reference * 100
                * (1 if rec['side'] == 'buy' else -1)) if volume else 0.0
        return OrderResponse(
            success=volume > 0, order_id=rec.get('uuid', ''), status=status,
            executed_price=price, executed_volume=volume,
            remaining_volume=snap.get('remaining'), fee=snap.get('fee', 0.0),
            slippage_pct=slip, latency_ms=(time.monotonic() - started) * 1000,
            metadata=dict(identifier=ident, order_unknown=unknown, executed_volume_q=snap.get('volume_q'),
                          terminal=bool(snap.get('terminal')),
                          request_satisfied=bool(snap.get('satisfied')),
                          error=error or rec.get('error', ''),
                          quote_age_ms=quote_age, slip_limit=slip_limit),
        )
    
    async def initialize(self) -> bool:
        """실거래 엔진 초기화"""
        try:
            # API 연결 확인
            balance = await self.api.get_balance()
            if balance:
                self._initialized = True
                logger.info("실거래 실행 엔진 초기화 완료")
                return True
            return False
        except Exception as e:
            logger.error(f"실거래 엔진 초기화 실패: {e}")
            return False
    
    async def execute_order(self, request: OrderRequest) -> OrderResponse:
        """
        실거래 주문 실행 (개선 버전 2025-11-09)        
        🆕 개선사항:
        1. 호가 최신성 검증 (4500ms)
        2. 슬리피지 상한 체크 (±0.20%)
        3. 가격 괴리 검증 (±0.4%)
        """
        start_time = datetime.now()
        
        try:
            # ============================================================
            # ⚙️ Execution Parameter Loading (Config 기반)
            # ============================================================
            _defaults = {
                'max_quote_age_ms': 4500, 'price_tick_krw': 1,
                'max_slippage_pct_buy': 0.20, 'max_slippage_pct_sell': 0.20,
                'retry_quote_times': 3, 'retry_quote_sleep_ms': 100,
                'dynamic_sanity': True, 'sanity_base_pct': 0.40,
                'sanity_min_pct': 0.30, 'sanity_max_pct': 0.80,
                'sanity_k_vol': 1.50,
            }
            exconf = None
            try:
                _sec = self.config.get('EXECUTION', {})
                if isinstance(_sec, dict):
                    exconf = {k: _sec.get(k, v) for k, v in _defaults.items()}
            except Exception:
                exconf = None
            if exconf is None:
                # ConfigParser 형 시그니처. 여기서도 실패하면 바깥 except → 주문 전 중단.
                exconf = {
                    k: self.config.get('EXECUTION', k, fallback=v)
                    for k, v in _defaults.items()
                }
            age_limit_ms   = int(exconf.get('max_quote_age_ms', 4500))
            tick           = float(exconf.get('price_tick_krw', 1))
            slip_buy       = float(exconf.get('max_slippage_pct_buy', 0.20))
            slip_sell      = float(exconf.get('max_slippage_pct_sell', 0.20))
            
            if (request.side not in ('buy', 'sell') or request.order_type != 'market'
                    or not isinstance(request.metadata, dict)
                    or not math.isfinite(float(request.volume)) or request.volume <= 0
                    or not math.isfinite(float(request.price)) or request.price <= 0):
                raise ValueError("주문 방향/유형/가격/수량 오류")
            reason = self.order_block_reason(request.symbol, request.side)
            if reason:
                raise RuntimeError(reason)
            side = "BUY" if request.side == 'buy' else "SELL"
            qty = request.volume
            
            # ============================================================
            # 📡 Step 1. 시세 최신성: 다단계 재조회 🆕
            # ============================================================
            from Trading_bot_4_data import MarketDataService
            mds = MarketDataService()
            
            # 🆕 파라미터 추가 로드
            q_retry = int(exconf.get('retry_quote_times', 3))
            q_sleep_ms = int(exconf.get('retry_quote_sleep_ms', 100))
            dyn_sanity = str(exconf.get('dynamic_sanity', True)).lower() in ('true', '1', 'yes', 'on')
            sanity_base = float(exconf.get('sanity_base_pct', 0.40))
            sanity_min = float(exconf.get('sanity_min_pct', 0.30))
            sanity_max = float(exconf.get('sanity_max_pct', 0.80))
            k_vol = float(exconf.get('sanity_k_vol', 1.50))
            
            attempt = 0
            current_price = 0.0
            timestamp = 0
            quote_age = 0
            prev_close = 0.0
            
            # 🆕 다단계 재시도 루프
            while True:
                tickers = await mds.get_ticker([request.symbol])
                
                if not tickers or len(tickers) == 0:
                    attempt += 1
                    if attempt >= q_retry:
                        raise RuntimeError(f"{request.symbol} 시세 조회 실패 (재시도 {attempt}회)")
                    logger.debug(f"[재시도 {attempt}/{q_retry}] {request.symbol} 시세 조회 실패")
                    await asyncio.sleep(q_sleep_ms / 1000.0)
                    continue
                
                tkr = tickers[0]
                timestamp = int(tkr.get("timestamp", _now_ms()))
                quote_age = _now_ms() - timestamp
                
                if quote_age <= age_limit_ms:
                    current_price = float(tkr.get("trade_price", 0.0))
                    # 🆕 prev_closing_price 안전 처리
                    prev_close = float(
                        tkr.get("prev_closing_price") or 
                        tkr.get("opening_price") or 
                        current_price
                    )
                    if prev_close == 0:
                        prev_close = current_price or 1.0
                    break
                
                attempt += 1
                if attempt >= q_retry:
                    raise RuntimeError(
                        f"{request.symbol} 시세 지연: {quote_age}ms > {age_limit_ms}ms (재시도 {attempt}회 후 실패)"
                    )
                logger.warning(f"⚠️ [{request.symbol}] quote_age={quote_age}ms > {age_limit_ms}ms → 재조회 ({attempt}/{q_retry})")
                await asyncio.sleep(q_sleep_ms / 1000.0)
            
            # 🆕 변동성 기반 동적 괴리 임계값 계산
            day_vol_pct = abs(current_price - prev_close) / max(prev_close, 1e-9) * 100.0
            micro_spread_pct = 0.08
            vol_proxy = max(day_vol_pct, micro_spread_pct)
            
            if dyn_sanity:
                eff_sanity = sanity_base * (1.0 + k_vol * (vol_proxy / 100.0))
                eff_sanity = max(sanity_min, min(sanity_max, eff_sanity))
            else:
                eff_sanity = sanity_base
            
            # 🆕 체결 직전 재검증 함수
            async def _precommit_check(ref_px: float) -> float:
                """주문 직전 최신 시세로 괴리 검증"""
                tickers_pre = await mds.get_ticker([request.symbol])
                if not tickers_pre or len(tickers_pre) == 0:
                    logger.warning(f"⚠️ [{request.symbol}] pre-commit 시세 조회 실패, 기존 가격 사용")
                    return ref_px
                
                t_pre = tickers_pre[0]
                px = float(t_pre.get("trade_price", ref_px))
                gap = abs(px - ref_px) / max(px, 1e-9) * 100.0
                
                if gap > eff_sanity:
                    raise RuntimeError(
                        f"{request.symbol} 가격 괴리 {gap:.2f}% > {eff_sanity:.2f}% (pre-commit check)"
                    )
                return px
            
            # 🆕 체결 직전 최종 가격 검증
            current_price = await _precommit_check(current_price)
            if not math.isfinite(current_price) or current_price <= 0:
                raise ValueError("주문 직전 시세 비유한/0 이하")
            
            # 슬리피지 상한 계산
            slip_limit = _cap_slippage_limit(side, current_price, slip_buy if side=="BUY" else slip_sell)
            slip_limit = _round_tick(slip_limit, tick, side)
            
            # ============================================================
            # 🛡️ Step 2. 가격 괴리 검증 (안전장치) - 🆕 동적 임계값 적용
            # ============================================================
            if request.price > 0:  # 지정가 주문인 경우
                sanity = abs(request.price - current_price) / current_price * 100
                if sanity > eff_sanity:  # 🆕 동적 임계값 사용
                    raise RuntimeError(
                        f"{request.symbol} 가격 괴리 {sanity:.2f}% > {eff_sanity:.2f}% (체결 중단)"
                    )
            
            # ============================================================
            # 🧾 Step 3. API 주문 실행
            # ============================================================
            logger.info(
                f"[실거래체결] {request.symbol} {side} | "
                f"현재가: {current_price:,.0f}원 | "
                f"슬리피지상한: {slip_limit:,.0f}원 (±{slip_buy if side=='BUY' else slip_sell}%) | "
                f"괴리임계: {eff_sanity:.2f}%"  # 🆕 추가
            )
            
            return await self._execute_tracked(
                request, current_price, quote_age, slip_limit
            )

        except Exception as e:
            # 여기까지는 주문 전 검증 경로. POST 이후 오류는 _execute_tracked가 보존.
            logger.error(f"❌ [{request.symbol}] 실거래 주문 전 검증 실패: {e}")
            return OrderResponse(
                success=False, order_id="", status='failed',
                executed_price=0, executed_volume=0, remaining_volume=None,
                fee=0, slippage_pct=0,
                latency_ms=(datetime.now() - start_time).total_seconds() * 1000,
                metadata={'error': str(e), 'order_unknown': False},
            )
    
    async def cancel_order(self, order_id: str) -> bool:
        """실거래 주문 취소"""
        try:
            result = await self.api.cancel_order(order_id)
            return result is not None
        except Exception as e:
            logger.error(f"주문 취소 실패: {e}")
            return False
    
    async def get_balance(self) -> float:
        """조회 실패·비유한 잔고와 실제 원화 0을 구분한다."""
        balances = await self.api.get_balance(strict=True)
        for balance in balances:
            if balance['currency'] == 'KRW':
                value = float(balance['balance'])
                if not math.isfinite(value) or value < 0:
                    raise ValueError("원화 잔고 비유한/음수")
                return value
        return 0.0
    
    async def get_holdings(self, symbol: str) -> float:
        """실패는 예외로 전달. 정상 응답에 통화가 없을 때만 0."""
        balances = await self.api.get_balance(strict=True)
        target = symbol.replace('KRW-', '')
        for item in balances:
            if item.get('currency') == target:
                free = float(item['balance'])
                locked = float(item.get('locked', 0) or 0)
                if not all(math.isfinite(v) and v >= 0 for v in (free, locked)):
                    raise ValueError("보유량 비유한/음수")
                if locked > 0:
                    raise RuntimeError("잠긴 보유량 존재: 주문/잔고 대조 필요")
                return free
        return 0.0
           
    async def get_positions(self) -> Dict[str, Position]:
        """실거래 포지션 조회 (호환성 유지용 - 실제로는 Bot이 관리)"""
        # 실거래에서는 Bot의 positions를 사용
        # Engine은 API에서 보유량만 조회
        logger.warning("⚠️ RealEngine.get_positions() 호출됨 - Bot의 positions 사용 권장")
        return {}
    
    async def get_order_status(self, order_id: str) -> Optional[Dict]:
        """진행 중 주문을 오래된 캐시로 확정하지 않는다."""
        return await self.api.get_order(order_id)
    
    async def shutdown(self) -> None:
        """실거래 엔진 종료: 영속 원장을 지우지 않고 연결만 닫는다."""
        logger.info("실거래 실행 엔진 종료")
        self.order_cache.clear()
        if self._journal is not None:
            self._journal.close()
            self._journal = None

class SimulationExecutionEngine(ExecutionEngine):
    """
    시뮬레이션 실행 엔진
    책임: 시뮬레이션 환경에서의 주문 실행
    """
    
    def __init__(self, config, reality_engine=None, initial_balance=None):
        self.config = config
        self.reality_engine = reality_engine
        # config에서 초기 잔고 가져오기
        if initial_balance is None:
            initial_balance = config.get_cached('initial_balance')
        self.balance = initial_balance
        self.initial_balance = initial_balance
        
        # ========================================
        # 변경: positions → holdings (단순 수량만)
        # ========================================
        self.holdings = {}  # {symbol: volume(float)}
        
        self.order_history = []
        self._initialized = False
        self.fee_rate = UPBIT_TAKER_FEE  # global_constants에서
    
    async def initialize(self) -> bool:
        """시뮬레이션 엔진 초기화"""
        self._initialized = True
        logger.info(f"시뮬레이션 실행 엔진 초기화 (잔고: {self.balance:,.0f}원)")
        return True
    
    async def _sim_price_guard(self, request, ref_price, start_time):
        """sim 체결 기준가 확인 (묶음 v4 v1_3) — float = 체결 기준가 · OrderResponse = 거부
        공통 : 현재가 조회 (실패 시 0.5 초 뒤 1 회 재시도) · 2 회 실패 = 거부
        매수 : 감지가 비정상 · 괴리 > 시간봉 허용값 = 거부 / 통과면 현재가로 잔고 재검사
        매도 : 현재가로 체결 (괴리는 경고만)"""
        side = str(getattr(request, 'side', '')).strip().lower().split('.')[-1]
        tf = str((request.metadata or {}).get('timeframe', '3m'))
        _tol_default = {'1m': 0.0020, '3m': 0.0015, '5m': 0.0012, '10m': 0.0010, '15m': 0.0008, '30m': 0.0008, '1h': 0.0010, '4h': 0.0015, '1d': 0.0020}
        try:
            tol = float(self.config.get_cached(f'entry_price_tolerance_{tf}') or _tol_default.get(tf, 0.0015))
        except Exception:
            tol = _tol_default.get(tf, 0.0015)
        if not (math.isfinite(tol) and 0 < tol <= 0.05):
            tol = _tol_default.get(tf, 0.0015)

        async def _fetch():
            try:
                from Trading_bot_4_data import MarketDataService  # @singleton = 봇과 같은 인스턴스 · WS 5 초 이내만 · 아니면 REST
                tk = await MarketDataService().get_ticker([request.symbol])
                px = float(tk[0].get('trade_price', 0) or 0) if tk else 0.0
                return px if (math.isfinite(px) and px > 0) else None
            except Exception as e:
                logger.warning(f"⚠️ [Sim체결] {request.symbol} 현재가 조회 예외: {e}")
                return None

        try:
            ref = float(ref_price)
        except Exception:
            ref = float('nan')
        ref_ok = math.isfinite(ref) and ref > 0
        now_px = await _fetch()
        if now_px is None:
            await asyncio.sleep(0.5)
            now_px = await _fetch()
        if now_px is None:
            logger.error(f"❌ [Sim체결거부] {request.symbol} {side} | 현재가 확인 실패 2 회")
            return self._sim_reject(request, start_time, f"sim {side} 거부 : 현재가 확인 실패 2 회")
        if side == 'buy':
            if not ref_ok:
                return self._sim_reject(request, start_time, f"sim 매수 거부 : 감지가 비정상 ({ref_price})")
            drift = abs(ref - now_px) / now_px
            if not math.isfinite(drift) or drift > tol:
                logger.error(f"❌ [Sim체결거부] {request.symbol} 매수 | 감지가 {ref:,.4f} · 현재가 {now_px:,.4f} · 괴리 {drift*100:.3f}% > {tol*100:.2f}%")
                return self._sim_reject(request, start_time, f"sim 매수 거부 : 감지가 괴리 {drift*100:.3f}% > {tol*100:.2f}%")
            required = float(request.volume) * now_px * (1 + self.fee_rate)
            if not math.isfinite(required) or self.balance < required:
                return self._sim_reject(request, start_time, f"잔고 부족 (현재가 재계산) : {self.balance:.0f} < {required:.0f}")
            return float(now_px)
        if side == 'sell':
            if ref_ok and abs(ref - now_px) / now_px > tol:
                logger.warning(f"⚠️ [Sim체결] {request.symbol} 매도 감지가 괴리 → 현재가 {now_px:,.4f} 로 체결")
            return float(now_px)
        return self._sim_reject(request, start_time, f"sim 거부 : 알 수 없는 side ({getattr(request, 'side', None)})")

    def _sim_reject(self, request, start_time, msg: str):
        try:
            remaining = float(request.volume)
            if not math.isfinite(remaining) or remaining < 0:
                remaining = None
        except (TypeError, ValueError, OverflowError):
            remaining = None
        return OrderResponse(
            success=False, order_id=request.idempotency_key, status='failed', executed_price=0, executed_volume=0,
            remaining_volume=remaining, fee=0, slippage_pct=0,
            latency_ms=(datetime.now() - start_time).total_seconds() * 1000, metadata={'error': msg},
        )

    async def execute_order(self, request: OrderRequest) -> OrderResponse:
        """시뮬레이션 주문 실행 - 90% 체결 개선"""
        start_time = datetime.now()
        
        try:
            # 분기 전에 주문 표기를 통일한다. 호출자가 보유한 요청은 변경하지 않는다.
            def _token(value):
                token = getattr(value, 'value', value)
                if not isinstance(token, str):
                    token = getattr(value, 'name', token)
                return str(token).strip().lower()

            side = _token(request.side)
            order_type = _token(request.order_type)
            if side not in ('buy', 'sell'):
                return self._sim_reject(request, start_time, f"sim 거부 : 알 수 없는 side ({request.side})")
            if order_type not in ('market', 'limit'):
                return self._sim_reject(request, start_time, f"sim 거부 : 알 수 없는 order_type ({request.order_type})")
            volume = float(request.volume)
            if not math.isfinite(volume) or volume <= 0:
                return self._sim_reject(request, start_time, f"sim 거부 : 요청 수량 비정상 ({request.volume})")
            try:
                price = float(request.price)
            except (TypeError, ValueError, OverflowError):
                price = float('nan')
            request = replace(request, side=side, order_type=order_type, volume=volume,
                              price=price, metadata=dict(request.metadata or {}))
            limit_price = request.price
            if order_type == 'limit' and (not math.isfinite(limit_price) or limit_price <= 0):
                return self._sim_reject(request, start_time, "sim 거부 : 지정가 비정상")
            held = self.holdings.get(request.symbol, 0.0)
            if not (math.isfinite(self.balance) and self.balance >= 0
                    and math.isfinite(held) and held >= 0):
                return self._sim_reject(request, start_time, "sim 거부 : 잔고 또는 보유량 비정상")
            logger.debug(
                f"[Sim엔진] 주문 수신: {request.symbol} {request.side} "
                f"{request.volume:.8f}개 @ {request.price:,.0f}원"
            )
            # ========================================
            # 1. 잔고/보유량 체크
            # ========================================
            if request.side == 'buy':
                required = request.volume * request.price * (1 + self.fee_rate)
                
                if self.balance < required:
                    logger.warning(
                        f"[Sim엔진] ❌ 잔고부족 | "
                        f"필요:{required:,.0f}원 > 보유:{self.balance:,.0f}원"
                    )
                    return OrderResponse(
                        success=False,
                        order_id=request.idempotency_key,
                        status='failed',
                        executed_price=0,
                        executed_volume=0,
                        remaining_volume=request.volume,
                        fee=0,
                        slippage_pct=0,
                        latency_ms=(datetime.now() - start_time).total_seconds() * 1000,
                        metadata={'error': f"잔고 부족: {self.balance:.0f} < {required:.0f}"}
                    )
            else:  # sell
                current_holdings = self.holdings.get(request.symbol, 0)
                
                if current_holdings <= 0:
                    logger.warning(f"[Sim엔진] ❌ 보유량 없음: {request.symbol}")
                    return OrderResponse(
                        success=False,
                        order_id=request.idempotency_key,
                        status='failed',
                        executed_price=0,
                        executed_volume=0,
                        remaining_volume=request.volume,
                        fee=0,
                        slippage_pct=0,
                        latency_ms=(datetime.now() - start_time).total_seconds() * 1000,
                        metadata={'error': f"보유량 없음: {request.symbol}"}
                    )
                
                if current_holdings < request.volume:
                    logger.warning(
                        f"[Sim엔진] ❌ 보유량부족 | "
                        f"요청:{request.volume:.8f}개 > 보유:{current_holdings:.8f}개"
                    )
                    return OrderResponse(
                        success=False,
                        order_id=request.idempotency_key,
                        status='failed',
                        executed_price=0,
                        executed_volume=0,
                        remaining_volume=request.volume,
                        fee=0,
                        slippage_pct=0,
                        latency_ms=(datetime.now() - start_time).total_seconds() * 1000,
                        metadata={'error': f"보유량 부족: {current_holdings:.8f} < {request.volume:.8f}"}
                    )
            
            # ========================================
            # 🔒 묶음 v4 v1_3 : 주문 순간 시세 확인 (Reality · 기본 경로 공통 · 지정가 제외)
            # ========================================
            if request.order_type == 'market':
                _ref_px = request.metadata.get('current_price', request.price)
                _guard = await self._sim_price_guard(request, _ref_px, start_time)
                if not isinstance(_guard, float):
                    return _guard
                request.price = _guard

            # ========================================
            # 2. Reality Engine 처리
            # ========================================
            if self.reality_engine and self.reality_engine.enabled:
                logger.debug(f"[Sim엔진] Reality Engine 적용 중...")
                
                atr = request.metadata.get('atr', 0)
                volume_rank = request.metadata.get('volume_rank', 50)
                market_regime = request.metadata.get('market_regime', 'RANGING')
                timeframe = request.metadata.get('timeframe', '5m')

                # ATR 기반 현실적 시뮬레이션
                # ========================================
                # 🆕 여기에 삽입! (라인 288 이전) 2025-10-18 15:07
                # ========================================
                # 🔒 묶음 v4 v1_3 : 감지가 재사용 · 10 % fallback 제거 (위 공통 가드가 체결 기준가를 정함)
                
                result = await self.reality_engine.process_order_with_atr(
                    symbol=request.symbol,
                    side=request.side,
                    price=request.price,
                    volume=request.volume,
                    volume_rank=volume_rank,
                    atr=atr,
                    current_price=request.price,
                    market_regime=market_regime,
                    timeframe=timeframe,
                    order_id=request.idempotency_key
                )
                
                # ========================================
                # 🔥 개선: 90% 체결 처리 로직
                # ========================================
                cancel_reason = result.get('cancel_reason', '')
                
                # 90% 이상 체결은 성공으로 간주!
                if result['status'] == 'cancelled' and cancel_reason == '90PCT_FILLED':
                    status = 'filled'
                    is_success = True
                        
                elif result['status'] == 'filled':
                    status = 'filled'
                    is_success = True
                    
                elif result['status'] == 'partial':
                    # 부분 체결도 일단 성공으로 (수량만 조정)
                    status = 'partial'
                    is_success = True
                    logger.info(
                        f"[Sim엔진] 🟡 부분체결 | "
                        f"{result['filled_volume']:.8f}개({result['fill_ratio']:.0f}%)"
                    )
                    
                else:
                    # cancelled (90% 미만) 또는 failed
                    status = 'failed'
                    is_success = False
                    logger.warning(
                        f"[Sim엔진] ❌ 주문실패 | "
                        f"status:{result['status']} | reason:{cancel_reason}"
                    )
                
                # Reality 결과 + 체결 정보 통합 (성공 시에만)
                if is_success:
                    logger.info(
                        f"[Sim엔진] Reality체결 {result['status'][:4].upper()} | "
                        f"수량:{result['filled_volume']:.8f}개({result['fill_ratio']:.0f}%) | "
                        f"슬리패지:{result['slippage']:.3f}% | "
                        f"{request.symbol} @{format_price_krw(result['filled_price'])}원"
                    )
                
                response = OrderResponse(
                    success=is_success,
                    order_id=request.idempotency_key,
                    status=status,
                    executed_price=result['filled_price'],
                    executed_volume=result['filled_volume'],
                    remaining_volume=result.get('remaining_volume', 0),
                    fee=result['filled_price'] * result['filled_volume'] * self.fee_rate,
                    slippage_pct=result['slippage'],
                    latency_ms=result['latency_ms'],
                    metadata={'reality_result': result}
                )
                
            else:
                # 기본 시뮬레이션
                logger.debug(f"[Sim엔진] 기본 시뮬레이션 모드")
                
                slippage = BASIC_SLIPPAGE if request.metadata.get('use_slippage', True) else 0
                
                if request.side == 'buy':
                    executed_price = request.price * (1 + slippage)
                else:
                    executed_price = request.price * (1 - slippage)
                
                # 🔧 2026-08-01: 구 legacy 유틸은 호가 스냅이 아닌 단순 6자리 반올림이었다.
                #  실제 업비트 호가 정렬은 core.snap_to_upbit_tick으로 통일한다.
                #  → sim 체결가가 107.107 같은 업비트에 없는 가격으로 남아
                #    진입가가 봉 고가를 넘는 사례가 141건 중 43건(30%) 발생
                #    (그 집단 손절률 74.4% · 3분내손절 48.8% · 실현 -0.68% = 통계 오염)
                #  → 실제 호가 단위로 스냅. 실거래 경로에는 영향 없음(sim 전용 분기).
                executed_price = snap_to_upbit_tick(executed_price, "round")
                fee = executed_price * request.volume * self.fee_rate
                
                latency = random.uniform(50, 200) / 1000
                await asyncio.sleep(latency)
                
                response = OrderResponse(
                    success=True,
                    order_id=request.idempotency_key,
                    status='filled',
                    executed_price=executed_price,
                    executed_volume=request.volume,
                    remaining_volume=0,
                    fee=fee,
                    slippage_pct=slippage * 100,
                    latency_ms=latency * 1000
                )
            
            # ========================================
            # 3. 잔고 및 보유량 업데이트
            # ========================================
            if response.success:
                fill_price, fill_volume, fee = map(float, (
                    response.executed_price, response.executed_volume, response.fee))
                # SIM 전량 계산에서 발생한 1 ULP 초과만 보정한다.
                if (all(math.isfinite(v) for v in (fill_price, fill_volume, fee))
                        and fill_price > 0 and fee >= 0
                        and 0.0 < request.volume < fill_volume
                        <= math.nextafter(float(request.volume), math.inf)):
                    fill_volume = float(request.volume)
                    fee = fill_price * fill_volume * self.fee_rate
                if (not all(math.isfinite(v) for v in (fill_price, fill_volume, fee))
                        or fill_price <= 0 or not 0 < fill_volume <= request.volume or fee < 0):
                    return self._sim_reject(request, start_time, "sim 거부 : 체결 가격·수량·수수료 비정상")
                gross = fill_price * fill_volume
                if not math.isfinite(gross) or gross <= 0 or fee > gross:
                    return self._sim_reject(request, start_time, "sim 거부 : 체결 금액 비정상")
                if request.order_type == 'limit' and (
                        (request.side == 'buy' and fill_price > limit_price)
                        or (request.side == 'sell' and fill_price < limit_price)):
                    return self._sim_reject(request, start_time, "sim 거부 : 체결가가 지정가 범위 이탈")
                held = self.holdings.get(request.symbol, 0.0)
                if (not math.isfinite(self.balance) or self.balance < 0
                        or not math.isfinite(held) or held < 0
                        or (request.side == 'sell' and fill_volume > held)):
                    return self._sim_reject(request, start_time, "sim 거부 : 잔고·보유량 또는 체결 수량 불일치")
                if request.side == 'buy' and not math.isfinite(held + fill_volume):
                    return self._sim_reject(request, start_time, "sim 거부 : 체결 후 보유량 비정상")
                response.executed_price, response.executed_volume, response.fee = fill_price, fill_volume, fee
                response.remaining_volume = request.volume - fill_volume
                if request.side == 'buy':
                    total_cost = gross + fee
                    if not math.isfinite(total_cost) or total_cost > self.balance:
                        logger.error(f"❌ [Sim체결거부] {request.symbol} 최종 체결액 {total_cost:,.0f}원 > 잔고 {self.balance:,.0f}원")
                        return self._sim_reject(request, start_time, f"최종 체결액 잔고 초과 : {total_cost:.0f} > {self.balance:.0f}")
                    old_balance = self.balance
                    self.balance -= total_cost
                    
                    if request.symbol not in self.holdings:
                        self.holdings[request.symbol] = 0
                    self.holdings[request.symbol] += response.executed_volume
                    
                    # ========================================
                    # ✅ 정보 100% 유지 (2줄)
                    # ========================================
                    regime = request.metadata.get('regime', 'UNKNOWN')
                    logger.info(
                        f"[Sim엔진] ✅ 매수완료 {request.symbol} | "
                        f"체제:{regime} | "  # ← 추가
                        f"수량:{response.executed_volume:.8f}개 × "
                        f"가격:{format_price_krw(response.executed_price)}원 = "
                        f"총액:{total_cost:,.0f}원(수수료:{response.fee:,.0f}원)"
                    )
                    logger.info(
                        f"[Sim엔진] 잔고변동 | "
                        f"{old_balance:,.0f}원 → {self.balance:,.0f}원 | "
                        f"보유:{self.holdings[request.symbol]:.8f}개"
                    )
                    
                else:  # sell
                    total_receive = gross - fee
                    if not math.isfinite(self.balance + total_receive):
                        return self._sim_reject(request, start_time, "sim 거부 : 체결 후 잔고 비정상")
                    old_balance = self.balance
                    self.balance += total_receive
                    
                    old_holdings = self.holdings[request.symbol]
                    self.holdings[request.symbol] -= response.executed_volume
                    
                    if round(self.holdings[request.symbol], 8) <= 0:  # [SIMDUST] SIM 보유 장부의 소수점 먼지도 정리
                        del self.holdings[request.symbol]
                    
                    # ========================================
                    # ✅ 정보 100% 유지 (2줄)
                    # ========================================
                    regime = request.metadata.get('regime', 'UNKNOWN')
                    logger.info(
                        f"[Sim엔진] ✅ 매도완료 {request.symbol} | "
                        f"체제:{regime} | "  # ← 추가
                        f"수량:{response.executed_volume:.8f}개 × "
                        f"가격:{format_price_krw(response.executed_price)}원 = "
                        f"수익:{total_receive:,.0f}원(수수료:{response.fee:,.0f}원)"
                    )
                    logger.info(
                        f"[Sim엔진] 잔고변동 | "
                        f"{old_balance:,.0f}원 → {self.balance:,.0f}원 | "
                        f"보유:{old_holdings:.8f}개 → {self.holdings.get(request.symbol, 0):.8f}개"
                    )
            else:
                logger.warning(
                    f"[Sim엔진] ❌ 주문실패: {response.metadata.get('error', 'Unknown')}"
                )
            
            # 주문 기록
            self.order_history.append({
                'timestamp': datetime.now(),
                'request': request,
                'response': response
            })
            
            return response
            
        except Exception as e:
            logger.error(f"[Sim엔진] 시뮬레이션 주문 실행 실패: {e}", exc_info=True)
            
            return self._sim_reject(request, start_time, str(e))
    
    async def cancel_order(self, order_id: str) -> bool:
        """시뮬레이션 주문 취소"""
        # 시뮬레이션에서는 즉시 성공
        logger.debug(f"시뮬레이션 주문 취소: {order_id}")
        return True
    
    async def get_balance(self) -> float:
        """시뮬레이션 잔고 조회"""
        return self.balance
    
    async def get_holdings(self, symbol: str) -> float:
        """특정 종목의 실제 보유 수량 반환 (단순화!)"""
        return self.holdings.get(symbol, 0.0)
    
    async def get_positions(self) -> Dict[str, Position]:
        """시뮬레이션 포지션 조회 (호환성 유지)"""
        # Bot의 positions를 반환 (Engine은 보유량만 관리)
        # 이 메서드는 사실상 사용되지 않아야 함
        logger.warning("⚠️ Engine.get_positions() 호출됨 - Bot의 positions 사용 권장")
        return {}
    
    async def get_order_status(self, order_id: str) -> Optional[Dict]:
        """시뮬레이션 주문 상태 조회"""
        for order in self.order_history:
            if order['request'].idempotency_key == order_id:
                return {
                    'order_id': order_id,
                    'status': order['response'].status,
                    'executed_price': order['response'].executed_price,
                    'executed_volume': order['response'].executed_volume
                }
        return None
    
    async def shutdown(self) -> None:
        """시뮬레이션 엔진 종료"""
        # 최종 성과 계산 (holdings의 대략적 가치 계산)
        holdings_value = 0
        # Note: 정확한 현재가가 필요하지만, 여기서는 단순화
        
        total_value = self.balance + holdings_value
        returns = ((total_value - self.initial_balance) / self.initial_balance) * PERCENT_CONVERSION
        
        # [SIMEXIT-DISPLAY] 위 값 = 현금만 (보유 평가 안 함) → 보유 중 종료면 큰 음수로 보임 (11-11 -34.57 %) · 참고 평가는 7_bot 최종 성과 줄
        logger.info("시뮬레이션 종료 (현금 기준 · 보유 %d 종 평가 안 함) - 현금 수익률: %+.2f%%, 현금: %s원", len(self.holdings), returns, f"{total_value:,.0f}")
        
    def reset(self, initial_balance: Optional[float] = None):
        """시뮬레이션 리셋"""
        self.balance = initial_balance or self.initial_balance
        self.holdings.clear()
        self
        logger.info(f"시뮬레이션 리셋 (잔고: {self.balance:,.0f}원)")

class ExecutionEngineFactory:
    """
    실행 엔진 팩토리
    책임: 모드에 따른 적절한 엔진 생성
    """
    
    @staticmethod
    def create_engine(config, api_client=None, reality_engine=None, initial_balance=None) -> ExecutionEngine:
        """
        실행 엔진 생성
        Args:
            config: ConfigManager
            api_client: UpbitAPIClient (실거래 전용)
            reality_engine: SimulationRealityEngine (시뮬레이션 전용)
            initial_balance: 초기 잔고 (시뮬레이션 전용, DB 복원 시 사용)
        Returns:
            ExecutionEngine: 생성된 실행 엔진
        """
        mode = config.get_cached('mode')
        
        if mode == 'real':
            if not api_client:
                raise ValueError("실거래 모드는 API 클라이언트 필요")
            return RealExecutionEngine(api_client, config)
        else:
            # 시뮬레이션: initial_balance 전달
            return SimulationExecutionEngine(config, reality_engine, initial_balance)
    
    @staticmethod
    async def create_and_initialize(config, api_client=None, reality_engine=None, 
                                   initial_balance=None) -> Optional[ExecutionEngine]:
        """엔진 생성 및 초기화
        Args:
            config: ConfigManager
            api_client: UpbitAPIClient (실거래 전용)
            reality_engine: SimulationRealityEngine (시뮬레이션 전용)
            initial_balance: 초기 잔고 (시뮬레이션 전용)
        """
        try:
            engine = ExecutionEngineFactory.create_engine(
                config, api_client, reality_engine, initial_balance
            )
            if await engine.initialize():
                return engine
            logger.error("실행 엔진 초기화 실패")
            return None
        except Exception as e:
            logger.error(f"실행 엔진 생성 실패: {e}")
            return None

# **END**