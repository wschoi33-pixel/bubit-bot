"""Regression cases for the divergence pair repair; never import/run the bot."""
import ast
import math
from pathlib import Path
import unittest

import numpy as np


REPO = Path(__file__).resolve().parents[1]
SOURCE = REPO / "Trading_bot_5_analysis.py"


def load_functions(source):
    tree = ast.parse(source)
    names = {"_confirmed_fractal_positions", "_bullish_divergence_pair_context", "_div_class_mw"}
    nodes = [n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name in names]
    scope = {"np": np, "math": math}
    exec(compile(ast.Module(body=nodes, type_ignores=[]), str(SOURCE), "exec"), scope)
    return scope, tree


def judge_gates(tree, indicators):
    names = (
        "_j2_hidden_go", "_j4_reversal_candle_event",
        "_judge4_reversal_pass", "_dcg_j4", "_judge4_class_pass",
    )
    by_name = {}
    for node in ast.walk(tree):
        if isinstance(node, ast.Assign):
            for target in node.targets:
                if isinstance(target, ast.Name) and target.id in names:
                    if target.id in by_name:
                        raise AssertionError("ambiguous gate: " + target.id)
                    by_name[target.id] = node
    scope = dict(
        indicators=indicators, _j2_ready=True, _j2_div_ready=True,
        _j2_hidden=True, _j2_hidden_structure_ok=True,
        _j2_macd_above_zero=False, _j2_bear_raw=False, _j2_overheat=False,
        _j4_regular=True, _j4_macd_below_zero=True,
    )
    exec(compile(ast.Module(body=[by_name[n] for n in names], type_ignores=[]),
                 str(SOURCE), "exec"), scope)
    return scope


class DivergencePairRegression(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.functions, cls.tree = load_functions(SOURCE.read_text(encoding="utf-8-sig"))

    def frame(self):
        # Confirmed lows at 2 and 8; the second is confirmed at 10.
        low = np.array([109, 108, 104, 108, 110, 109, 107, 106,
                        100, 106, 107, 108, 109, 110], dtype=float)
        opened = low + 1.0
        closed = opened.copy()
        high = low + 2.0
        return high, low, closed, opened

    def context(self, values):
        return self.functions["_bullish_divergence_pair_context"](*values)

    def indicators(self, **updates):
        result = dict(
            div_class_shape="W", div_class_type="J4_REGULAR",
            cycle_reversal_candle_bull=True,
            div_invalid=False, div_class_invalid=False,
            _fix_j4_rev_win=False, _fix_j4_class_rev_win=False,
        )
        result.update(updates)
        return result

    def test_closed_break_stays_invalid_after_recovery(self):
        values = self.frame()
        values[1][12] = 99.0
        values[2][12] = 99.5
        values[3][12] = 100.0
        values[2][13] = 103.0
        values[1][13] = 102.0
        self.assertTrue(self.context(values)[0])

    def test_wick_below_without_closed_break_is_not_invalidation(self):
        values = self.frame()
        values[1][-1] = 99.0
        values[2][-1] = values[3][-1] = 101.0
        self.assertFalse(self.context(values)[0])

    def test_pivot_hammer_is_retained_after_confirmation(self):
        values = self.frame()
        values[3][8], values[2][8], values[0][8] = 102.0, 103.0, 103.5
        self.assertEqual(self.context(values), (False, True))
        ind = self.indicators(cycle_reversal_candle_bull=False,
                              _fix_j4_rev_win=True, _fix_j4_class_rev_win=True)
        new = judge_gates(self.tree, ind)
        self.assertTrue(new["_judge4_reversal_pass"])
        self.assertTrue(new["_judge4_class_pass"])

    def test_candle_before_pivot_is_not_a_window_event(self):
        values = self.frame()
        values[3][6], values[2][6] = 110.0, 108.0
        values[3][7], values[2][7], values[0][7] = 107.0, 110.0, 111.0
        self.assertEqual(self.context(values), (False, False))

    def test_invalid_pair_blocks_legacy_j2_j4_and_class_j4(self):
        ind = self.indicators(div_invalid=True, div_class_invalid=True)
        new = judge_gates(self.tree, ind)
        for name in ("_j2_hidden_go", "_judge4_reversal_pass", "_judge4_class_pass"):
            self.assertFalse(new[name], name)

    def test_existing_w_classifier_can_keep_a_broken_pair(self):
        # Last bar has a new unconfirmed low. The old confirmed pair still
        # classifies W/J4, even though a completed close has broken L2.
        low = np.interp(np.arange(25), [0, 5, 10, 15, 20, 22, 23, 24],
                        [118, 112, 100, 108, 99.5, 104, 103, 98.2])
        high = low + 1.0
        closed = low + 0.5
        opened = closed.copy()
        opened[-1], closed[-1], high[-1] = 98.8, 99.0, 99.1
        rsi = np.full(25, 40.0)
        macd = np.full(25, -1.0)
        rsi[10], rsi[20] = 20.0, 30.0
        macd[10], macd[20] = -1.0, -0.8
        classify = self.functions["_div_class_mw"]
        shape, kind, _ = classify(high, low, closed, rsi, macd, 2.0, lambda _: 0.1)
        self.assertEqual((shape, kind), ("W", "J4_REGULAR"))
        invalid, _ = self.context((high, low, closed, opened))
        self.assertTrue(invalid)
        ind = self.indicators(div_class_shape=shape, div_class_type=kind,
                              div_invalid=invalid, div_class_invalid=invalid)
        self.assertFalse(judge_gates(self.tree, ind)["_judge4_class_pass"])

    def test_valid_current_candle_behavior_is_preserved(self):
        ind = self.indicators()
        new = judge_gates(self.tree, ind)
        for name in ("_j2_hidden_go", "_judge4_reversal_pass", "_judge4_class_pass"):
            self.assertTrue(new[name], name)

    def test_pair_window_does_not_leak_between_lanes(self):
        ind = self.indicators(cycle_reversal_candle_bull=False,
                              _fix_j4_rev_win=True, _fix_j4_class_rev_win=False)
        new = judge_gates(self.tree, ind)
        self.assertTrue(new["_judge4_reversal_pass"])
        self.assertFalse(new["_judge4_class_pass"])
        ind.update(_fix_j4_rev_win=False, _fix_j4_class_rev_win=True)
        new = judge_gates(self.tree, ind)
        self.assertFalse(new["_judge4_reversal_pass"])
        self.assertTrue(new["_judge4_class_pass"])

    def test_pair_invalidation_does_not_leak_between_lanes(self):
        ind = self.indicators(div_invalid=False, div_class_invalid=True)
        new = judge_gates(self.tree, ind)
        self.assertTrue(new["_j2_hidden_go"])
        self.assertTrue(new["_judge4_reversal_pass"])
        self.assertFalse(new["_judge4_class_pass"])


if __name__ == "__main__":
    unittest.main(verbosity=2)
