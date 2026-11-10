# -*- coding: utf-8 -*-
"""
Trading_bot_z_decision_ledger.py — Decision Ledger v1.2 (지표 계산 이후 종목별 진입 판단 기록)

책임:
- 종목 1건의 진입 판단마다 1행을 JSONL 로 기록한다 (관측 전용)
- 신호 → 주문 제출 → 주문 응답 → 포지션 생성의 단계 상태를 구분해 남긴다
- 판단 루프에서는 스칼라 복사 + 큐 적재만 하고, 직렬화·디스크 쓰기는 백그라운드 스레드가 한다

금지사항:
- 전달받은 객체(indicators / ma_data / signal / df / position / request / response) 수정 금지
  — dict.get · getattr · index 읽기만 쓴다 (setdefault · pop · 대입 금지)
- 전달받은 객체의 참조 보관 금지 — 그 자리에서 스칼라로 복사한다
- 예외 전파 금지 — capture_* / note 는 모든 예외를 삼킨다 (start 만 실패를 예외로 알린다)
- 판단 루프 안에서 파일 I/O · 대기(blocking) 금지
- 매매 판정 · 주문 · 포지션 · config 값 변경 금지
"""
import hashlib
import json
import os
import queue
import threading
import time
from datetime import datetime, timedelta
from pathlib import Path

SCHEMA_VERSION = "dl-1.2"

# provenance 필수 키 집합 (고정) — 하나라도 SHA 를 못 얻으면 start() 가 실패한다
REQUIRED_SRC_FILES = (
    "Trading_bot_1_core.py", "Trading_bot_4_data.py", "Trading_bot_5_analysis.py",
    "Trading_bot_6_strategy.py", "Trading_bot_7_bot.py",
    "Trading_bot_11_execution_engine.py", "Trading_bot_z_decision_ledger.py",
)
REQUIRED_CFG_KEYS = ("base", "timeframe")

_STOP = object()
_INF = float("inf")
_BAR_FMT = "%Y-%m-%d %H:%M:%S"

# Strategy 호출 이전 단계의 차단 코드 (표지 S1~S7 이 넣는 값)
_PRE_CODES = frozenset([
    "SELL_COOLDOWN", "ADDON_PARTIAL_BLOCK", "ALREADY_HOLDING",
    "DF_MISSING", "DF_INVALID", "PRICE_VALIDATE", "INDICATOR_HARD_BLOCK",
])


def _num(v):
    """스칼라 float 로 복사 (Series 는 마지막 값) — 실패 · NaN · Inf 는 None"""
    try:
        if v is None:
            return None
        if isinstance(v, bool):
            return float(int(v))
        if hasattr(v, "iloc"):
            if len(v) == 0:
                return None
            v = v.iloc[-1]
        f = float(v)
        if f != f or f == _INF or f == -_INF:
            return None
        return f
    except Exception:
        return None


def _int(v):
    f = _num(v)
    return None if f is None else int(f)


def _s(v, n=160):
    """짧은 문자열로 복사 (Enum 은 value)"""
    try:
        if v is None:
            return None
        if hasattr(v, "value"):
            v = v.value
        return str(v)[:n]
    except Exception:
        return None


def _iso(epoch):
    try:
        return (
            datetime.fromtimestamp(epoch).strftime("%Y-%m-%dT%H:%M:%S.")
            + "%03d" % int((epoch % 1) * 1000)
        )
    except Exception:
        return None


def _dumps(obj, strict=True):
    return json.dumps(
        obj, ensure_ascii=False, separators=(",", ":"), allow_nan=not strict
    )


class DecisionLedger:
    """진입 판단 JSONL 기록기 — 큐 + 백그라운드 writer"""

    def __init__(self, out_dir, mode, timeframe, tf_seconds=180, level="full",
                 queue_max=5000, flush_sec=2.0, source_dir=None,
                 config_files=None, session_start=None):
        self._dir = Path(out_dir)
        self._mode = str(mode)
        self._tf = str(timeframe)
        self._tf_sec = int(tf_seconds)
        self._level = level if level in ("full", "gc_full") else "full"
        self._q = queue.Queue(maxsize=max(100, int(queue_max)))
        self._flush_sec = max(0.2, float(flush_sec))
        # provenance: 소스는 source_dir(절대경로) 아래 고정 파일명, config 는 {"base","timeframe"} 경로.
        # 상대경로는 생성 시점(=봇 초기화 시점)의 cwd 로 한 번만 절대화한다.
        try:
            self._src_dir = Path(source_dir).resolve() if source_dir else None
        except Exception:
            self._src_dir = None
        self._cfg = {}
        try:
            for _k, _v in dict(config_files or {}).items():
                try:
                    self._cfg[str(_k)] = Path(_v).resolve()
                except Exception:
                    self._cfg[str(_k)] = None
        except Exception:
            self._cfg = {}
        self._session_start = _s(session_start)
        self.run_id = "%s-%d" % (datetime.now().strftime("%Y%m%dT%H%M%S"), os.getpid())
        self._pending_exec = {}   # symbol -> 주문 제출/응답 스칼라 dict
        self._pending_pos = {}    # symbol -> {"created": 0/1/None, "kind": str}
        self._notes = {}          # symbol -> (code, epoch)
        self.n_enq = 0
        self.n_drop = 0
        self.n_err = 0
        self.n_written = 0
        self._alive = False
        self._abort = False
        self._thread = None
        # writer 시작 확인 (handshake)
        self._started_event = threading.Event()
        self._start_ok = False
        self._start_error = None

    # ------------------------------------------------------------------
    # 수명주기
    # ------------------------------------------------------------------
    def start(self, timeout=1.0):
        """writer 를 띄우고, 폴더 생성 · 당일 파일 open · header 1행 write+flush 성공을
        timeout 안에 확인한다. 실패하면 예외를 낸다(호출부가 Ledger 를 None 으로 되돌린다).
        성공 조건 = 필수 provenance SHA 전부 유효 ∧ header write ∧ flush."""
        if self._thread is not None:
            return
        self._thread = threading.Thread(
            target=self._run, name="decision-ledger-writer", daemon=True
        )
        self._thread.start()
        if not self._started_event.wait(timeout):
            self._abort = True
            raise TimeoutError("Decision Ledger writer 시작 확인 시간 초과")
        if not self._start_ok:
            self._abort = True
            raise RuntimeError("Decision Ledger writer 시작 실패: %s" % (self._start_error,))
        self._alive = True

    def stop(self, timeout=3.0):
        """남은 큐를 쓰고 종료 — 종료 경로에서만 호출 (대기 발생)"""
        try:
            self._alive = False
            try:
                self._q.put(_STOP, timeout=1.0)
            except queue.Full:
                self._abort = True
            if self._thread is not None:
                self._thread.join(timeout=timeout)
        except Exception:
            pass

    def stats(self):
        return {
            "enq": self.n_enq, "drop": self.n_drop,
            "err": self.n_err, "written": self.n_written,
        }

    # ------------------------------------------------------------------
    # 판단 루프에서 부르는 것 (읽기 전용 · 예외 삼킴 · I/O 없음)
    # ------------------------------------------------------------------
    def note(self, symbol, code):
        """execute_trade / _create_position 내부 사유 표지"""
        if not self._alive:
            return
        try:
            if len(self._notes) > 500:
                self._notes.clear()
            self._notes[str(symbol)] = (str(code)[:48], time.time())
        except Exception:
            self.n_err += 1

    def capture_submit(self, *, symbol, order_time, order_price, request):
        """주문 호출 직전 — 제출 사실 · 시각 · 주문가를 스칼라로 보관"""
        if not self._alive:
            return
        try:
            if len(self._pending_exec) > 200:
                self._pending_exec.clear()
            self._pending_exec[str(symbol)] = {
                "submitted": 1,
                "order_px": _num(order_price),
                "order_vol": _num(getattr(request, "volume", None)),
                "order_t": _num(order_time),
                "resp_t": None, "fill_px": None, "fill_vol": None, "fee": None,
                "slip_pct": None, "latency_ms": None, "status": None,
                "order_id": None, "ok": None, "err": "",
            }
        except Exception:
            self.n_err += 1

    def capture_order(self, *, symbol, resp_time, response):
        """주문 응답 직후 — 응답 값을 스칼라로 복사해 제출 기록에 합친다"""
        if not self._alive:
            return
        try:
            sym = str(symbol)
            ex = self._pending_exec.get(sym)
            if ex is None:
                ex = {"submitted": 1, "order_px": None, "order_vol": None, "order_t": None}
                self._pending_exec[sym] = ex
            meta = getattr(response, "metadata", None)
            err = meta.get("error") if isinstance(meta, dict) else None
            ex["resp_t"] = _num(resp_time)
            ex["fill_px"] = _num(getattr(response, "executed_price", None))
            ex["fill_vol"] = _num(getattr(response, "executed_volume", None))
            ex["fee"] = _num(getattr(response, "fee", None))
            ex["slip_pct"] = _num(getattr(response, "slippage_pct", None))
            ex["latency_ms"] = _num(getattr(response, "latency_ms", None))
            ex["status"] = _s(getattr(response, "status", None), 24)
            ex["order_id"] = _s(getattr(response, "order_id", None), 64)
            ex["ok"] = int(bool(getattr(response, "success", False)))
            ex["err"] = _s(err, 200) or ""
        except Exception:
            self.n_err += 1

    def capture_position(self, *, symbol, created, kind="NEW"):
        """포지션 생성 호출 직후 — 생성 여부(봇 positions 에 종목이 있는가)를 보관"""
        if not self._alive:
            return
        try:
            if len(self._pending_pos) > 200:
                self._pending_pos.clear()
            self._pending_pos[str(symbol)] = {
                "created": None if created is None else int(bool(created)),
                "kind": str(kind)[:8],
            }
        except Exception:
            self.n_err += 1

    def capture_hold(self, *, symbol, df, regime, indicators, ma_data, current_price, position):
        """[B3] 보유 종목 청산 점검 = 닫힌 봉마다 1 행 (관측 전용 · 예외 삼킴 · I/O 없음 · 같은 (종목 · mr_candle_time) 은 1 번만)"""
        if not self._alive:
            return
        try:
            sym = str(symbol)
            ind = indicators if isinstance(indicators, dict) else {}
            md = ma_data if isinstance(ma_data, dict) else {}
            mct = _s(ind.get("mr_candle_time"), 24)
            seen = self.__dict__.setdefault("_hold_seen", {})
            if mct and seen.get(sym) == mct:
                return
            if len(seen) > 500:
                seen.clear()
            seen[sym] = mct
            now = time.time()
            pm = md.get("prev_mas")
            pm = pm if isinstance(pm, dict) else {}
            ap = _num(getattr(position, "avg_price", None))
            px = _num(current_price)
            mr = ind.get("mr_regime")
            self._put({
                "schema": SCHEMA_VERSION, "rec": "hold",
                "id": {"run": self.run_id},
                "t": _iso(now), "t_epoch": round(now, 3),
                "market": sym, "mode": self._mode, "tf": self._tf,
                "bar": self._bar(df, now, ind),
                "regime": _s(getattr(regime, "value", regime), 24),
                "mr_regime": _s(getattr(mr, "value", mr), 24),
                "mr_candle_time": mct,
                "ma": {"p5": _num(pm.get("ma5")), "p10": _num(pm.get("ma10")), "p20": _num(pm.get("ma20"))},
                "px": {"detect": px},
                "pos": {
                    "avg": ap,
                    "pnl_pct": (round((px / ap - 1.0) * 100.0, 4) if (px and ap) else None),
                    "partial": _int(getattr(position, "partial_sell_count", 0)),
                    "vol": _num(getattr(position, "volume", None)),
                },
            })
        except Exception:
            self.n_err += 1

    def capture_cycle(self, *, cycle, rr_cycle, n_targets, elapsed_sec, preserve_cash):
        """_check_entries 의 종목 루프가 끝났을 때 1행"""
        if not self._alive:
            return
        try:
            now = time.time()
            self._put({
                "schema": SCHEMA_VERSION, "rec": "cycle",
                "id": {"run": self.run_id, "cycle": _int(cycle), "rr_cycle": _int(rr_cycle)},
                "t": _iso(now),
                "n_targets": _int(n_targets),
                "elapsed_s": _num(elapsed_sec),
                "preserve_cash": int(bool(preserve_cash)),
                "q": {"enq": self.n_enq, "drop": self.n_drop, "err": self.n_err},
            })
        except Exception:
            self.n_err += 1

    def capture_entry(self, *, cycle, rr_cycle, seq, symbol, stage, df,
                      current_price, regime, indicators, ma_data, signal,
                      success, position, preserve_cash, reject_reason,
                      last_ticker=None, sell_cooldown_active=False,
                      decay_lock_until=0.0):
        """종목 1건 진입 판단 종료 시 1행"""
        if not self._alive:
            return
        try:
            now = time.time()
            sym = str(symbol)
            ind = indicators if isinstance(indicators, dict) else {}
            md = ma_data if isinstance(ma_data, dict) else {}
            # 이전 종목의 신호가 섞이지 않게 종목명을 대조한다
            sig_ok = bool(
                signal is not None and getattr(signal, "symbol", None) == symbol
            )
            sind = getattr(signal, "indicators", None) if sig_ok else None
            if not isinstance(sind, dict):
                sind = {}
            ex = self._pending_exec.pop(sym, None)
            pos = self._pending_pos.pop(sym, None)
            note = self._notes.pop(sym, None)
            if note is not None and now - note[1] > 30.0:
                note = None

            action, st_name, code, reason, inferred, sub = self._classify(
                stage, df, current_price, ind, sig_ok, signal, success,
                position, reject_reason, sell_cooldown_active, ex, pos, note,
            )

            gc_rel = _int(ind.get("closed_gc_5x10_relation"))
            dur = _int(ind.get("dur_5x10_bars"))
            row = {
                "schema": SCHEMA_VERSION, "rec": "entry",
                "id": {
                    "run": self.run_id,
                    "decision": "%s-%s-%s" % (self.run_id, _int(cycle), _int(seq)),
                    "cycle": _int(cycle), "rr_cycle": _int(rr_cycle), "seq": _int(seq),
                },
                "t": _iso(now), "t_epoch": round(now, 3),
                "market": sym, "mode": self._mode, "tf": self._tf,
                "bar": self._bar(df, now, ind),
                "final": {
                    "action": action, "stage": st_name, "code": code,
                    "reason": reason, "inferred": inferred, "sub": sub,
                    "ret": None if success is None else int(bool(success)),
                },
                "exec": ex,
                "position": pos,
            }

            # gc_full 수준: 확정 GC 가 아니고 신호도 없으면 축약 행
            if self._level == "gc_full" and gc_rel != 1 and not sig_ok:
                row["compact"] = 1
                row["gc"] = {"rel": gc_rel, "dur": dur}
                row["regime"] = _s(regime, 24)
                self._put(row)
                return

            dur_strict = _int(ind.get("tlc_dur_5x10_closed"))
            t0_end = self._gc_end(df, dur)
            t0_strict = self._gc_end(df, dur_strict)
            row["gc"] = {
                "rel": gc_rel,
                "ok": _int(ind.get("closed_gc_5x10_ok")),
                "dur": dur,
                "dur_strict": dur_strict,
                "rt_gt": _int(ind.get("entry_ma5_gt_ma10")),
                "ma5_closed": _num(ind.get("entry_ma5_closed")),
                "ma10_closed": _num(ind.get("entry_ma10_closed")),
                "ma5_rt": _num(ind.get("entry_ma5_rt")),
                "ma10_rt": _num(ind.get("entry_ma10_rt")),
                "true_birth": _int(ind.get("gajami_true_gc_birth")),
                "converge_ok": _int(ind.get("gajami_converge_birth_filtered_ok")),
                "fake_late_reset": _int(ind.get("gajami_fake_late_reset_gc")),
                "t0_end": t0_end,
                "event_key": ("%s|%s" % (sym, t0_end)) if t0_end else None,
                "event_key_strict": ("%s|%s" % (sym, t0_strict)) if t0_strict else None,
            }

            cm = md.get("current_mas")
            cm = cm if isinstance(cm, dict) else {}
            pm = md.get("prev_mas")
            pm = pm if isinstance(pm, dict) else {}
            row["ma"] = {
                "ma5": _num(cm.get("ma5")), "ma10": _num(cm.get("ma10")),
                "ma15": _num(cm.get("ma15")), "ma20": _num(cm.get("ma20")),
                "ma60": _num(cm.get("ma60")),
                "p5": _num(pm.get("ma5")), "p10": _num(pm.get("ma10")),
                "p15": _num(pm.get("ma15")), "p20": _num(pm.get("ma20")),
            }
            row["slope"] = {
                "s5": _num(ind.get("ma5_slope")), "s10": _num(ind.get("ma10_slope")),
                "s15": _num(ind.get("ma15_slope")), "s20": _num(ind.get("ma20_slope")),
                "s20_5": _num(ind.get("ma20_slope_struct5")),
                "s60": _num(ind.get("ma60_slope")),
                "s5_p1": _num(ind.get("ma5_slope_prev1")),
            }
            row["regime"] = _s(regime, 24)
            row["atr_pct"] = _num(ind.get("atr_pct"))
            row["vol_ratio"] = _num(ind.get("volume_ratio"))
            row["er"] = _num(ind.get("er_10"))

            row["tlc"] = {
                "family": _s(ind.get("cycle_detail_family_shadow"), 48),
                "phase": _s(ind.get("cycle_phase_shadow"), 48),
                "detail": _s(ind.get("cycle_detail_shadow"), 48),
                "phase_age": _int(ind.get("cycle_phase_age_shadow")),
                "leg": _s(ind.get("cycle_active_leg"), 24),
                "struct": _s(ind.get("cycle_active_structure"), 24),
                "leg_bars": _int(ind.get("cycle_active_leg_bars")),
                "aposc": _num(ind.get("cycle_active_pos_closed")),
                "prog": _num(ind.get("cycle_active_progress")),
                "pwave": _s(ind.get("cycle_parent_wave_id"), 48),
                "pposc": _num(ind.get("cycle_parent_pos_closed")),
                "wpos": _num(ind.get("cycle_window_pos")),
                "wst": _s(ind.get("j1_wave_state"), 16),
            }

            row["judge"] = {
                "j1": _int(ind.get("judge1_gajami_pass")),
                "j1_veto": _int(ind.get("judge1_gajami_veto")),
                "j1_v": _s(ind.get("judge1_gajami_verdict"), 12),
                "j2": _int(ind.get("judge2_osc_pass")),
                "j2_raw": _int(ind.get("judge2_osc_pass_raw")),
                "j2_v": _s(
                    ind.get("judge2_osc_verdict_raw", ind.get("judge2_osc_verdict")), 12
                ),
                "j2_prov": "%s/%s/%s/%s/%s" % (
                    _int(ind.get("judge2_prov_ready")), _int(ind.get("judge2_prov_pass")),
                    _s(ind.get("judge2_prov_verdict"), 12),
                    _s(ind.get("judge2_prov_bar"), 20),
                    _int(ind.get("judge2_prov_hidgo")),
                ),
                "j3": _int(ind.get("judge3_cycle_pass")),
                "j3_raw": _int(ind.get("judge3_cycle_pass_raw")),
                "j3_v": _s(ind.get("judge3_cycle_verdict"), 12),
                "j3_veto": _int(ind.get("judge3_cycle_veto")),
                "j3_prov": "%s/%s/%s/%s/%s/%s" % (
                    _int(ind.get("judge3_prov_ready")), _int(ind.get("judge3_prov_pass")),
                    _s(ind.get("judge3_prov_verdict"), 12),
                    _s(ind.get("judge3_prov_bar"), 20),
                    _s(ind.get("judge3_prov_det"), 32),
                    _int(ind.get("judge3_prov_fake_top")),
                ),
                "j4": _int(ind.get("judge4_reversal_pass")),
                "j4c": _int(ind.get("judge4_class_pass")),  # [DIV-CLASS-GATE-1109] 분류판 J4
                "j2_old": _int(ind.get("judge2_osc_pass_old")),  # [DIV-CLASS-GATE-1109] 교체 전 J2
                "j4_old": _int(ind.get("judge4_reversal_pass_old")),  # [DIV-CLASS-GATE-1109] 교체 전 J4
                "div_cls": "%s/%s/%s/%s" % (_s(ind.get("div_class_shape"), 6), _s(ind.get("div_class_type"), 12), _num(ind.get("div_class_pos")), _int(ind.get("div_class_gate_on"))),  # [DIV-CLASS-GATE-1109] 모양/유형/위치/켬
                "lane": _int(ind.get("judge_lane_authorized")),
                # 아래 4개는 BUY 신호가 있었던 행에만 값이 있다
                "final": _s(sind.get("strategy_judges", sind.get("judges")), 8) if sig_ok else None,
                "exec": _s(sind.get("execution_judges"), 8) if sig_ok else None,
                "j1_lane_denied": _int(sind.get("bot_j1_lane_denied")) if sig_ok else None,
                "j1_lane_reason": _s(sind.get("bot_j1_lane_deny_reason"), 32) if sig_ok else None,
            }

            gm = md.get("gajami_meta")
            if not isinstance(gm, dict):
                gm = ind.get("gajami_meta")
            gm = gm if isinstance(gm, dict) else {}
            lc = gm.get("gajami_lifecycle")
            lc = lc if isinstance(lc, dict) else {}
            im = ind.get("_meta")
            im = im if isinstance(im, dict) else {}
            row["gate"] = {
                "soleus": _int(ind.get("soleus_pass")),
                "soleus_reason": _s(gm.get("reason"), 48),
                "const": _int(gm.get("gajami_constitution_ok")),
                "const_eff": _int(gm.get("gajami_constitution_effective_ok")),
                "const_fail": _s(gm.get("gajami_constitution_fail"), 64),
                "regime_gate": _int(gm.get("regime_gate_ok")),
                "lc_state": _s(lc.get("state"), 32),
                "lc_birth_px": _num(lc.get("birth_price")),
                "lc_rise": _num(lc.get("rise_pct")),
                "lc_death": _s(lc.get("death_reason"), 64),
                "lc_dur": _int(lc.get("dur")),
                "ma20_not_decel": _int(ind.get("gajami_ma20_not_decel")),
                "tlc_hard_veto": _int(ind.get("gajami_tlc_hard_veto")),
                "body_growth": _int(ind.get("shape_v2_body_growth")),
                "entry_type": _s(ind.get("entry_type"), 32),
                "route": _s(ind.get("entry_authorization_route"), 16),
                "conf": _num(getattr(signal, "confidence", None)) if sig_ok else None,
                "conf_thr": _num(sind.get("effective_conf_threshold")) if sig_ok else None,
                "stale": int(bool(im.get("data_stale", False))),
                "preserve_cash": int(bool(preserve_cash)),
                "decay_lock_s": round(max(0.0, (_num(decay_lock_until) or 0.0) - now), 1),
                "calc_failed": int(bool(ind.get("_calc_failed", False))),
            }

            lt = last_ticker if isinstance(last_ticker, dict) else {}
            sig_ts = getattr(signal, "timestamp", None) if sig_ok else None
            row["px"] = {
                "detect": _num(current_price),
                "ticker_ts": _int(lt.get("timestamp")),
                "prev_close": _num(ind.get("prev_candle_close")),
                "detect_time": (
                    sig_ts.strftime("%Y-%m-%dT%H:%M:%S.%f")[:23]
                    if hasattr(sig_ts, "strftime") else None
                ),
                "detect_candle_time": _s(sind.get("detect_candle_time"), 24) if sig_ok else None,
            }
            self._put(row)
        except Exception:
            self.n_err += 1

    # ------------------------------------------------------------------
    # 내부 (판단 루프 쪽 · I/O 없음)
    # ------------------------------------------------------------------
    def _put(self, row):
        try:
            self._q.put_nowait(row)
            self.n_enq += 1
        except queue.Full:
            self.n_drop += 1   # 기다리지 않고 버린다

    def _classify(self, stage, df, current_price, ind, sig_ok, signal, success,
                  position, reject_reason, sell_cooldown_active, ex, pos, note):
        """(action, stage, code, reason, inferred, sub) — 봇 상태를 읽기만 한다.

        단계: BOT_PRE → STRATEGY → SIGNAL_BUY → ORDER_SUBMITTED → ORDER_FAILED / ORDER_OK
              → POSITION_CREATED / POSITION_CREATE_FAILED
        action: BUY(포지션 생성 확인 또는 추가매수 주문 성공) · SKIP · ANOMALY(주문은 나갔는데 포지션 미확인)
        """
        st = str(stage or "PRE")
        ncode = str(note[0]) if note is not None else ""
        stamped = st not in ("PRE", "STRATEGY_DONE")

        # 1) 주문이 제출된 행 — 주문/포지션 기록이 단계를 정한다 (execute_trade 반환값보다 우선)
        if ex is not None:
            sub = ncode or (st if stamped else "")
            ok = ex.get("ok")
            if ok is None:
                return "ANOMALY", "ORDER_SUBMITTED", "ORDER_NO_RESPONSE", "", 0, sub
            if not ok:
                return "SKIP", "ORDER_FAILED", "ORDER_FAILED", (ex.get("err") or ""), 0, sub
            if pos is None:
                return "ANOMALY", "ORDER_OK", "POSITION_CAPTURE_MISSING", "", 0, sub
            if pos.get("kind") == "ADDON":
                return "BUY", "ORDER_OK", "ADDON_ORDER_OK", "", 0, sub
            if pos.get("created"):
                return "BUY", "POSITION_CREATED", "BUY", "", 0, ""
            return (
                "ANOMALY", "POSITION_CREATE_FAILED",
                (ncode or "POSITION_CREATE_FAILED"), "", 0, (st if stamped else ""),
            )

        buy_sig = bool(sig_ok and str(getattr(signal, "action", "")) == "BUY")

        # 2) BUY 신호는 있었으나 주문 전에 끝난 행
        if buy_sig:
            if stamped:
                return "SKIP", "SIGNAL_BUY", st, "", 0, ncode
            if ncode:
                return "SKIP", "SIGNAL_BUY", ncode, "", 0, ""
            if success is False:
                return "SKIP", "SIGNAL_BUY", "EXECUTE_TRADE_FALSE", "", 0, ""
            return "SKIP", "SIGNAL_BUY", "BOT_POST_SIGNAL_BLOCK", "", 1, ""

        # 3) 표지가 찍힌 차단 (정확 코드)
        if stamped:
            if st.startswith("EXC_"):
                grp = "EXCEPTION"
            elif st in _PRE_CODES:
                grp = "BOT_PRE"
            else:
                grp = "BOT_POST"
            return "SKIP", grp, st, "", 0, ncode

        # 4) 표지 없이 Strategy 이전에 끝난 행 — 봇의 조건식을 읽기 전용으로 다시 본다 (추정)
        if st == "PRE":
            if df is None:
                if sell_cooldown_active:
                    code = "SELL_COOLDOWN"
                elif position is not None:
                    meta = getattr(position, "metadata", None) or {}
                    is_pre = (
                        meta.get("entry_stage") == "PRE"
                        and not meta.get("addon_done", False)
                    )
                    if not is_pre:
                        code = "ALREADY_HOLDING"
                    elif int(getattr(position, "partial_sell_count", 0) or 0) > 0:
                        code = "ADDON_PARTIAL_BLOCK"
                    else:
                        code = "DF_MISSING"
                else:
                    code = "DF_MISSING"
            elif current_price is None:
                code = "DF_INVALID_OR_PRICE"
            elif not ind:
                code = "INDICATOR_HARD_BLOCK"
            else:
                code = "PRE_STRATEGY_EXCEPTION"
            return "SKIP", "BOT_PRE", code, "", 1, ncode

        # 5) Strategy 가 불린 뒤
        if not sig_ok:
            return "SKIP", "STRATEGY", "STRATEGY_NONE", (_s(reject_reason, 200) or ""), 0, ncode
        return "SKIP", "BOT_POST", "NON_BUY_SIGNAL", "", 0, ncode

    def _bar(self, df, now, ind):
        out = {
            "last": None, "prev": None, "n": None, "last_age_s": None,
            "shadow": _s(ind.get("indicator_shadow_bar"), 20), "tail": [],
            "ohlcv": None,
        }
        try:
            if df is None:
                return out
            idx = df.index
            n = len(idx)
            out["n"] = n
            if n >= 1:
                out["last"] = str(idx[-1])[:19]
                try:
                    # tz 없는 KST 문자열을 로컬 시각으로 해석 (PC 시간대 = KST 가정)
                    out["last_age_s"] = round(
                        now - datetime.strptime(out["last"], _BAR_FMT).timestamp(), 1
                    )
                except Exception:
                    pass
            if n >= 2:
                out["prev"] = str(idx[-2])[:19]
            out["tail"] = [str(idx[-i])[:19] for i in range(min(8, n), 0, -1)]
            # 🔧 [DL-OHLCV-1110-v2] 지표 계산 때 고정한 6 봉 (5_analysis indicators["_dl_ohlcv6"]) 만 기록 · df 다시 안 읽음 · 관측 전용
            try:
                _dlo = ind.get("_dl_ohlcv6") if isinstance(ind, dict) else None
                if isinstance(_dlo, dict) and _dlo.get("v"):
                    out["ohlcv"] = _dlo
            except Exception:
                pass
        except Exception:
            self.n_err += 1
        return out

    def _gc_end(self, df, d):
        """GC 봉 종료 시각. 가정: 마지막 행 = 형성 중 봉 → dur 1 은 df.index[-2]"""
        try:
            if df is None or d is None or d < 1:
                return None
            idx = df.index
            if len(idx) < d + 1:
                return None
            start = datetime.strptime(str(idx[-(d + 1)])[:19], _BAR_FMT)
            return (start + timedelta(seconds=self._tf_sec)).strftime("%Y-%m-%dT%H:%M:%S")
        except Exception:
            return None

    # ------------------------------------------------------------------
    # 백그라운드 writer 스레드 (디스크 I/O 는 여기서만)
    # ------------------------------------------------------------------
    def _sha(self, path):
        try:
            h = hashlib.sha256()
            with open(path, "rb") as f:
                for chunk in iter(lambda: f.read(1 << 20), b""):
                    h.update(chunk)
            return h.hexdigest()
        except Exception:
            return None

    def _build_header(self):
        """필수 provenance 가 전부 유효할 때만 header 를 만든다 (fail-close).
        하나라도 없으면 예외 → 파일을 열기 전에 start() 가 실패한다."""
        if self._src_dir is None:
            raise RuntimeError("provenance: source_dir 없음")
        if set(self._cfg.keys()) != set(REQUIRED_CFG_KEYS):
            raise RuntimeError(
                "provenance: config 키 집합 불일치 %s" % (sorted(self._cfg.keys()),)
            )
        src = {}
        for name in REQUIRED_SRC_FILES:
            src[name] = self._sha(self._src_dir / name)
        cfg = {}
        for key in REQUIRED_CFG_KEYS:
            p = self._cfg.get(key)
            cfg[key] = {"path": str(p) if p is not None else None,
                        "sha": self._sha(p) if p is not None else None}
        missing = [k for k in REQUIRED_SRC_FILES if not src.get(k)]
        missing += ["cfg:" + k for k in REQUIRED_CFG_KEYS if not cfg[k]["sha"]]
        if missing:
            raise RuntimeError("provenance: SHA 를 얻지 못함 %s" % (missing,))
        return {
            "schema": SCHEMA_VERSION, "rec": "header",
            "id": {"run": self.run_id},
            "mode": self._mode, "tf": self._tf, "level": self._level,
            "pid": os.getpid(), "session_start": self._session_start,
            "src_dir": str(self._src_dir),
            "src_sha": src,
            "cfg_sha": cfg,
        }

    def _open_day(self, day, header, t):
        fh = open(
            self._dir / ("decision_ledger_%s.jsonl" % day),
            "a", encoding="utf-8", newline="\n",
        )
        fh.write(_dumps(dict(header, t=t), strict=False) + "\n")
        fh.flush()
        return fh

    def _run(self):
        fh = None
        cur_day = None
        header = None
        # --- 시작 확인: provenance 전부 유효 → 폴더 · 당일 파일 · header 1행 write+flush ---
        try:
            header = self._build_header()      # 실패하면 폴더·파일을 만들기 전에 끝난다
            self._dir.mkdir(parents=True, exist_ok=True)
            now = time.time()
            cur_day = datetime.fromtimestamp(now).strftime("%Y%m%d")
            fh = self._open_day(cur_day, header, _iso(now))
            self._start_ok = True
        except Exception as e:
            self._start_error = "%s: %s" % (type(e).__name__, str(e)[:160])
            self._start_ok = False
            try:
                if fh is not None:
                    fh.close()
            except Exception:
                pass
            self._started_event.set()
            return
        self._started_event.set()

        last_flush = time.monotonic()
        try:
            while not self._abort:
                try:
                    item = self._q.get(timeout=self._flush_sec)
                except queue.Empty:
                    item = None
                if item is _STOP:
                    break
                if item is not None:
                    try:
                        day = str(item.get("t") or "")[:10].replace("-", "")
                        if len(day) != 8:
                            day = cur_day
                        if day != cur_day:
                            fh.flush()
                            fh.close()
                            fh = self._open_day(day, header, item.get("t"))
                            cur_day = day
                        fh.write(_dumps(item) + "\n")
                        self.n_written += 1
                    except Exception:
                        self.n_err += 1
                now = time.monotonic()
                if now - last_flush >= self._flush_sec:
                    try:
                        fh.flush()
                    except Exception:
                        self.n_err += 1
                    last_flush = now
        finally:
            try:
                if fh is not None:
                    fh.flush()
                    fh.close()
            except Exception:
                pass
