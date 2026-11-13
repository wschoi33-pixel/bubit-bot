"""Exercise production divergence calculations without importing the bot runtime.

Only the pure helpers, ATR method, and actual indicator/divergence shadow blocks
are compiled from the source AST. No copied indicator implementation or bot
initialization is involved.
"""

import ast
import copy
from enum import Enum
import math
from pathlib import Path
from types import SimpleNamespace
import unittest
from unittest.mock import patch

import numpy as np
import pandas as pd


ROOT = Path(__file__).resolve().parents[1]


def _tree(filename):
    return ast.parse((ROOT / filename).read_text(encoding="utf-8-sig"))


def _function(tree, name):
    return next(
        node for node in ast.walk(tree)
        if isinstance(node, ast.FunctionDef) and node.name == name
    )


def _assigns(node, name):
    return any(
        isinstance(item, ast.Name) and isinstance(item.ctx, ast.Store)
        and item.id == name for item in ast.walk(node)
    )


def _sets_indicator(node, key):
    return any(
        isinstance(item, ast.Subscript)
        and isinstance(item.ctx, ast.Store)
        and isinstance(item.value, ast.Name)
        and item.value.id == "indicators"
        and isinstance(item.slice, ast.Constant)
        and item.slice.value == key
        for item in ast.walk(node)
    )


def _constant(tree, name):
    for node in tree.body:
        if isinstance(node, ast.Assign) and any(
            isinstance(target, ast.Name) and target.id == name
            for target in node.targets
        ):
            return ast.literal_eval(node.value)
    raise LookupError(name)


class _Logs:
    def __init__(self):
        self.messages = []

    def info(self, message):
        self.messages.append(message)

    debug = info


def _production_calculations():
    analysis = _tree("Trading_bot_5_analysis.py")
    constants = _tree("global_constants.py")
    logs = _Logs()
    namespace = {
        "np": np, "pd": pd, "math": math, "logger": logs,
        "EPSILON": _constant(constants, "EPSILON"),
        "DEFAULT_ATR_PERIOD": _constant(constants, "DEFAULT_ATR_PERIOD"),
        "ADX_DEFAULT_VALUE": _constant(constants, "ADX_DEFAULT_VALUE"),
        "SENTINEL_FLOAT": _constant(analysis, "SENTINEL_FLOAT"),
    }
    helpers = [copy.deepcopy(_function(analysis, name)) for name in (
        "_confirmed_fractal_positions", "_divergence_closed_selection",
        "_bullish_divergence_pair_context", "_div_class_mw",
    )]
    helpers.append(copy.deepcopy(_function(_tree("Trading_bot_1_core.py"), "get_tick_size")))
    atr = copy.deepcopy(_function(analysis, "calculate_atr"))
    atr.decorator_list = []
    helpers.append(atr)
    adx = copy.deepcopy(_function(analysis, "calculate_adx"))
    adx.decorator_list = []
    helpers.append(adx)
    exec(compile(ast.fix_missing_locations(ast.Module(body=helpers, type_ignores=[])),
                 str(ROOT / "Trading_bot_5_analysis.py"), "exec"), namespace)
    namespace["TechnicalIndicators"] = SimpleNamespace(
        calculate_atr=namespace["calculate_atr"],
        calculate_adx=namespace["calculate_adx"],
        _keltner_squeeze_ttl_state={}, _indicator_shadow_logged_bar={},
    )

    calculate = _function(analysis, "calculate_all_indicators")
    first_try = next(
        i for i, node in enumerate(calculate.body)
        if isinstance(node, ast.Try) and _assigns(node, "_bb_win")
    )
    last_try = next(
        i for i, node in enumerate(calculate.body)
        if isinstance(node, ast.Try)
        and _sets_indicator(node, "_divergence_shadow_ready")
    )
    # Include the public defaults so short/invalid input fails as in production.
    defaults = first_try
    while defaults and isinstance(calculate.body[defaults - 1], ast.Expr):
        node = calculate.body[defaults - 1].value
        if not (isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute)
                and node.func.attr == "setdefault"):
            break
        defaults -= 1
    atr_setup = [node for node in calculate.body if (
        isinstance(node, ast.Assign)
        and (_assigns(node, "_ATR_MIN_PCT") or _assigns(node, "_atr_min")
             or _sets_indicator(node, "atr"))
    )]
    wrapper = ast.parse(
        "def run_shadow(df, timeframe, symbol):\n"
        "    indicators = {}\n"
        "    return indicators\n"
    ).body[0]
    wrapper.body[1:1] = copy.deepcopy(atr_setup + calculate.body[defaults:last_try + 1])
    exec(compile(ast.fix_missing_locations(ast.Module(body=[wrapper], type_ignores=[])),
                 str(ROOT / "Trading_bot_5_analysis.py"), "exec"), namespace)
    cycle_try = min(
        (node for node in ast.walk(analysis) if isinstance(node, ast.Try)
         and any(_assigns(stmt, "_cycle_cut") for stmt in node.body)),
        key=lambda node: node.end_lineno - node.lineno,
    )
    cycle_start = next(i for i, node in enumerate(cycle_try.body)
                       if _assigns(node, "_cycle_cut"))
    cycle_end = next(i for i, node in enumerate(cycle_try.body)
                     if isinstance(node, ast.Assign) and _assigns(node, "_adx_exhaustion"))
    cycle_wrapper = ast.parse(
        "def run_cycle(df, indicators, timeframe):\n"
        "    return locals()\n"
    ).body[0]
    cycle_wrapper.body[:0] = copy.deepcopy(cycle_try.body[cycle_start:cycle_end + 1])
    exec(compile(ast.fix_missing_locations(ast.Module(body=[cycle_wrapper], type_ignores=[])),
                 str(ROOT / "Trading_bot_5_analysis.py"), "exec"), namespace)
    return SimpleNamespace(**namespace), logs


def _candles(count=100):
    index = pd.date_range("2026-11-12 09:00", periods=count, freq="3min")
    if count == 100:
        # Two confirmed lows and a neckline in both the 35- and 70-bar windows.
        x = np.array([0, 20, 30, 45, 55, 70, 78, 86, 99])
        y = np.array([120, 104, 112, 103, 111, 100, 108, 99.5, 104.2])
        close = np.interp(np.arange(count), x, y)
    else:
        x = np.arange(count)
        close = 100 + 1.5 * np.sin(x / 3.0) + 0.03 * x
    opening = close + 0.08 * np.cos(np.arange(count))
    return pd.DataFrame({
        "open": opening, "high": np.maximum(opening, close) + 0.4,
        "low": np.minimum(opening, close) - 0.4,
        "close": close, "volume": 100 + np.arange(count),
    }, index=index)


def _append_forming(closed, *, scale=1.0):
    stamp = closed.index[-1] + pd.Timedelta(minutes=3)
    price = float(closed.close.iloc[-1]) * scale
    row = pd.DataFrame({
        "open": [price * 0.95], "high": [price * 1.2],
        "low": [price * 0.8], "close": [price], "volume": [1e8],
    }, index=[stamp])
    return pd.concat([closed, row])


class _Timeframe(Enum):
    THREE_MINUTES = "3m"


class ClosedSelectionTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.production, cls.logs = _production_calculations()

    def test_exact_close_boundary_for_supported_timeframes(self):
        stamp = pd.Timestamp("2026-11-13 10:00", tz="Asia/Seoul")
        for timeframe, duration in (
            ("1m", "1min"), ("3m", "3min"), ("5m", "5min"),
            ("10m", "10min"), ("15m", "15min"), ("30m", "30min"),
            ("1h", "1h"), ("4h", "4h"), ("1d", "1d"),
        ):
            end = (stamp + pd.Timedelta(duration)).timestamp()
            with self.subTest(timeframe=timeframe):
                select = self.production._divergence_closed_selection
                index = pd.DatetimeIndex([stamp])
                self.assertEqual(select(index, timeframe, end - 0.001), (-1, -2))
                self.assertEqual(select(index, timeframe, end), (None, -1))

    def test_kst_naive_utc_and_enum_have_the_same_boundary(self):
        kst = pd.Timestamp("2026-11-13 10:00", tz="Asia/Seoul")
        end = (kst + pd.Timedelta(minutes=3)).timestamp()
        for stamp in (kst.tz_localize(None), kst, kst.tz_convert("UTC")):
            for timeframe in ("3m", _Timeframe.THREE_MINUTES):
                with self.subTest(stamp=stamp, timeframe=timeframe):
                    self.assertEqual(self.production._divergence_closed_selection(
                        pd.DatetimeIndex([stamp]), timeframe, end,
                    ), (None, -1))

    def test_sparse_quiet_market_preserves_its_latest_closed_row(self):
        index = pd.to_datetime(["2026-11-13 09:00", "2026-11-13 09:12"])
        now = pd.Timestamp("2026-11-13 09:22", tz="Asia/Seoul").timestamp()
        self.assertEqual(self.production._divergence_closed_selection(
            index, "3m", now,
        ), (None, -1))

    def test_unknown_or_invalid_metadata_excludes_the_last_row(self):
        index = pd.date_range("2026-11-13 09:00", periods=2, freq="3min")
        now = pd.Timestamp("2026-11-13 12:00", tz="Asia/Seoul").timestamp()
        for candidate, timeframe, clock in (
            (index, "unknown", now), (pd.RangeIndex(2), "3m", now),
            (pd.DatetimeIndex([]), "3m", now),
            (pd.DatetimeIndex([pd.NaT]), "3m", now),
            (index, "3m", float("nan")),
        ):
            with self.subTest(index=candidate, timeframe=timeframe, clock=clock):
                self.assertEqual(self.production._divergence_closed_selection(
                    candidate, timeframe, clock,
                ), (-1, -2))


class ClosedShadowTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.production, cls.logs = _production_calculations()

    def run_shadow(self, frame, now):
        self.logs.messages.clear()
        state = self.production.TechnicalIndicators
        state._keltner_squeeze_ttl_state.clear()
        state._indicator_shadow_logged_bar.clear()
        with patch("time.time", return_value=now):
            return self.production.run_shadow(frame, "3m", "KRW-TEST")

    def assert_same_closed_outputs(self, left, right):
        keys = {
            key for key in left if key.startswith((
                "bollinger_", "keltner_", "macd_", "div", "_fix_j4_",
            )) or key in ("rsi14", "indicator_shadow_bar", "indicator_shadow_px",
                         "_divergence_shadow_ready")
        }
        self.assertTrue(left["_divergence_shadow_ready"], self.logs.messages)
        self.assertTrue(right["_divergence_shadow_ready"], self.logs.messages)
        self.assertTrue(left["keltner_squeeze_ready"])
        self.assertNotEqual(left["div_class_shape"], "ERR")
        for key in sorted(keys):
            with self.subTest(indicator=key):
                a, b = left[key], right[key]
                if isinstance(a, (float, np.floating)):
                    if math.isnan(a):
                        self.assertTrue(math.isnan(b))
                    else:
                        self.assertAlmostEqual(a, b, places=11)
                else:
                    self.assertEqual(a, b)

    def test_forming_row_and_quiet_market_use_identical_completed_data(self):
        closed = _candles()
        forming = _append_forming(closed)
        now = pd.Timestamp(forming.index[-1], tz="Asia/Seoul").timestamp() + 30
        quiet = self.run_shadow(closed, now)
        busy = self.run_shadow(forming, now)
        self.assertTrue(quiet["shadow_last_row_closed"])
        self.assertFalse(busy["shadow_last_row_closed"])
        self.assertEqual(quiet["div_class_shape"], "W")
        self.assert_same_closed_outputs(quiet, busy)

    def test_forming_ohlc_cannot_change_closed_indicators_or_atr_floor(self):
        closed = _candles()
        ordinary = _append_forming(closed)
        now = pd.Timestamp(ordinary.index[-1], tz="Asia/Seoul").timestamp() + 30
        expected = self.run_shadow(closed, now)
        for scale in (0.01, 1.0, 1e5):
            with self.subTest(forming_price_scale=scale):
                actual = self.run_shadow(_append_forming(closed, scale=scale), now)
                self.assert_same_closed_outputs(expected, actual)
                closed_atr = self.production.calculate_atr(closed, atr_min_pct=0.05)
                self.assertAlmostEqual(actual["divergence_atr_floor"],
                                       closed_atr.tail(35).median() * 0.20)

    def test_thirty_five_closed_bars_are_sufficient_but_forming_is_not_counted(self):
        closed = _candles(35)
        forming = _append_forming(closed)
        now = pd.Timestamp(forming.index[-1], tz="Asia/Seoul").timestamp() + 30
        all_closed = self.run_shadow(closed, now)
        self.assertTrue(all_closed["_divergence_shadow_ready"], self.logs.messages)
        self.assertTrue(math.isfinite(all_closed["rsi14"]))
        self.assertTrue(math.isfinite(all_closed["macd_signal"]))
        self.assert_same_closed_outputs(all_closed, self.run_shadow(forming, now))
        only_thirty_four_closed = forming.iloc[1:]
        insufficient = self.run_shadow(only_thirty_four_closed, now)
        self.assertFalse(insufficient["_divergence_shadow_ready"])
        self.assertIsNone(insufficient["rsi14"])
        self.assertTrue(self.logs.messages)

    def test_cycle_bandwidth_ma_volume_reversal_and_adx_share_closed_bar(self):
        closed = _candles()
        closed["volume"] = 10.0
        closed.loc[closed.index[-1], "volume"] = 30.0
        closed.loc[closed.index[-2], ["open", "high", "low", "close"]] = [
            105.0, 105.2, 103.8, 104.0,
        ]
        closed.loc[closed.index[-1], ["open", "high", "low", "close"]] = [
            103.9, 105.4, 103.7, 105.2,
        ]
        forming = _append_forming(closed)
        now = pd.Timestamp(forming.index[-1], tz="Asia/Seoul").timestamp() + 30

        def calculate(frame):
            adx = self.production.calculate_adx(frame)
            with patch("time.time", return_value=now):
                return self.production.run_cycle(frame, {"adx": adx}, "3m")

        expected = calculate(closed)
        self.assertTrue(expected["_reversal_candle_bull"])
        self.assertEqual(expected["_reversal_candle_kind"], "BE")
        self.assertEqual(expected["_volume_ratio_closed"], 3.0)
        outputs = (
            "_bw_now", "_bw_ref", "_bw_delta_pct", "_bw_deadband_pct",
            "_bw_expanding", "_bw_contracting", "_bw_percentile_60",
            "_ma_aligned_prev", "_ma_aligned", "_ma_alignment_broken",
            "_ma5_rising", "_volume_ratio_closed", "_reversal_candle_bull",
            "_reversal_candle_kind", "_bear_engulf", "_adx_prev",
            "_adx_closed", "_adx_expanding", "_adx_exhaustion",
        )
        for scale in (1.0, 1e5):
            actual = calculate(_append_forming(closed, scale=scale))
            for name in outputs:
                with self.subTest(forming_price_scale=scale, cycle_output=name):
                    if isinstance(expected[name], float):
                        self.assertAlmostEqual(expected[name], actual[name], places=11)
                    else:
                        self.assertEqual(expected[name], actual[name])

    def test_cycle_keeps_indicator_snapshot_when_clock_crosses_close_boundary(self):
        closed = _candles()
        forming = _append_forming(closed, scale=1e5)
        boundary = pd.Timestamp(forming.index[-1], tz="Asia/Seoul").timestamp() + 180
        snapshot = self.run_shadow(forming, boundary - 0.001)
        self.assertTrue(snapshot["_divergence_shadow_ready"], self.logs.messages)
        self.assertFalse(snapshot["shadow_last_row_closed"])
        snapshot["adx"] = self.production.calculate_adx(forming)

        with patch("time.time", return_value=boundary + 0.001):
            after_boundary = self.production.run_cycle(forming, snapshot, "3m")
            expected = self.production.run_cycle(
                closed, {"adx": self.production.calculate_adx(closed)}, "3m",
            )
            without_snapshot = self.production.run_cycle(
                forming, {"adx": self.production.calculate_adx(forming)}, "3m",
            )

        pd.testing.assert_series_equal(
            after_boundary["_cycle_close"], closed.close, check_freq=False,
        )
        pd.testing.assert_series_equal(
            after_boundary["_cycle_volume"], closed.volume.astype(float), check_freq=False,
        )
        for output in (
            "_bw_now", "_bw_delta_pct", "_ma_aligned", "_ma5_rising",
            "_volume_ratio_closed", "_reversal_candle_bull", "_adx_closed",
        ):
            with self.subTest(cycle_output=output):
                self.assertEqual(after_boundary[output], expected[output])
        self.assertNotEqual(without_snapshot["_c"], after_boundary["_c"])


if __name__ == "__main__":
    unittest.main()
