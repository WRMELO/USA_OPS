"""SPC Shewhart para USA_OPS.

Portado de RENDA_OPS/lib/spc.py (SALA D-076). Funcoes de analise de
estabilidade para operational_window.parquet.

Para status US:
- ESTAVEL / ATENCAO / INSTAVEL / SPC_INDISPONIVEL (Regra 1)
- Nelson/WE consultivo (D-079/D-066: BC INCONCLUSIVO no motor US)
"""

from __future__ import annotations

from typing import Any

import numpy as np
import pandas as pd

D4_IMR_N2: float = 3.2665  # fator D4 para MR chart com n=2
D4_N4: float = 2.282  # fator D4 para R chart com n=4


def _safe_float_spc(v: Any, default: float = float("nan")) -> float:
    try:
        out = float(v)
    except Exception:
        return default
    return out if np.isfinite(out) else default


def _derive_center_and_sigma(row: pd.Series) -> tuple[float, float, float]:
    i_ucl = _safe_float_spc(row.get("i_ucl"), float("nan"))
    i_lcl = _safe_float_spc(row.get("i_lcl"), float("nan"))
    mr_ucl = _safe_float_spc(row.get("mr_ucl"), float("nan"))
    if not (np.isfinite(i_ucl) and np.isfinite(i_lcl)):
        return float("nan"), float("nan"), float("nan")
    center_line = float((i_ucl + i_lcl) / 2.0)
    sigma_i = float((i_ucl - center_line) / 3.0)
    mr_bar = float(mr_ucl / D4_IMR_N2) if np.isfinite(mr_ucl) and D4_IMR_N2 > 0 else float("nan")
    return center_line, sigma_i, mr_bar


def _build_runs_flags(df: pd.DataFrame) -> pd.DataFrame:
    """Computa flags Nelson/WE B+C nas 4 cartas SPC de um ticker."""
    df = df.sort_values("date").copy()

    derived = df.apply(_derive_center_and_sigma, axis=1, result_type="expand")
    derived.columns = ["_cl", "_si", "_mr_bar"]
    df = pd.concat([df, derived], axis=1)

    iv = pd.to_numeric(df["i_value"], errors="coerce")
    cl = pd.to_numeric(df["_cl"], errors="coerce")
    si = pd.to_numeric(df["_si"], errors="coerce")

    zb_up = cl + si
    zb_dn = cl - si
    za_up = cl + (2.0 * si)
    za_dn = cl - (2.0 * si)

    above_cl = (iv > cl).astype(int)
    below_cl = (iv < cl).astype(int)
    above_za = (iv > za_up).astype(int)
    below_za = (iv < za_dn).astype(int)
    above_zb = (iv > zb_up).astype(int)
    below_zb = (iv < zb_dn).astype(int)

    w4_up = above_cl.rolling(8, min_periods=8).sum() == 8
    w4_dn = below_cl.rolling(8, min_periods=8).sum() == 8
    w3_up = above_zb.rolling(5, min_periods=5).sum() >= 4
    w3_dn = below_zb.rolling(5, min_periods=5).sum() >= 4
    w2_up = above_za.rolling(3, min_periods=3).sum() >= 2
    w2_dn = below_za.rolling(3, min_periods=3).sum() >= 2

    diff_i = iv.diff()
    n3_up = (diff_i > 0).rolling(5, min_periods=5).sum() == 5
    n3_dn = (diff_i < 0).rolling(5, min_periods=5).sum() == 5
    runs_value = w4_up | w4_dn | w3_up | w3_dn | w2_up | w2_dn | n3_up | n3_dn

    mrv = pd.to_numeric(df["mr_value"], errors="coerce")
    mrb = pd.to_numeric(df["_mr_bar"], errors="coerce")
    above_mrb = (mrv > mrb).astype(int)
    w4_mr = above_mrb.rolling(8, min_periods=8).sum() == 8
    diff_mr = mrv.diff()
    n3_mr = (diff_mr > 0).rolling(5, min_periods=5).sum() == 5
    runs_disp = w4_mr | n3_mr

    _empty = pd.Series(float("nan"), index=df.index, dtype=float)
    i_ucl_s = pd.to_numeric(df["i_ucl"], errors="coerce")
    i_lcl_s = pd.to_numeric(df["i_lcl"], errors="coerce")
    mr_ucl_s = pd.to_numeric(df["mr_ucl"], errors="coerce")
    xb_val = pd.to_numeric(df.get("xbar_value", _empty), errors="coerce")
    xb_ucl_s = pd.to_numeric(df.get("xbar_ucl", _empty), errors="coerce")
    xb_lcl_s = pd.to_numeric(df.get("xbar_lcl", _empty), errors="coerce")
    rv = pd.to_numeric(df.get("r_value", _empty), errors="coerce")
    r_ucl_s = pd.to_numeric(df.get("r_ucl", _empty), errors="coerce")

    xb_cl = (xb_ucl_s + xb_lcl_s) / 2.0
    sigma_xb = (xb_ucl_s - xb_cl) / 3.0
    xb_above_cl = (xb_val > xb_cl).astype(int)
    xb_below_cl = (xb_val < xb_cl).astype(int)
    xb_above_za = (xb_val > xb_cl + 2.0 * sigma_xb).astype(int)
    xb_below_za = (xb_val < xb_cl - 2.0 * sigma_xb).astype(int)
    xb_above_zb = (xb_val > xb_cl + sigma_xb).astype(int)
    xb_below_zb = (xb_val < xb_cl - sigma_xb).astype(int)
    xb_w4_up = xb_above_cl.rolling(8, min_periods=8).sum() == 8
    xb_w4_dn = xb_below_cl.rolling(8, min_periods=8).sum() == 8
    xb_w3_up = xb_above_zb.rolling(5, min_periods=5).sum() >= 4
    xb_w3_dn = xb_below_zb.rolling(5, min_periods=5).sum() >= 4
    xb_w2_up = xb_above_za.rolling(3, min_periods=3).sum() >= 2
    xb_w2_dn = xb_below_za.rolling(3, min_periods=3).sum() >= 2
    diff_xb = xb_val.diff()
    xb_n3_up = (diff_xb > 0).rolling(5, min_periods=5).sum() == 5
    xb_n3_dn = (diff_xb < 0).rolling(5, min_periods=5).sum() == 5
    runs_xbar = (
        xb_w4_up
        | xb_w4_dn
        | xb_w3_up
        | xb_w3_dn
        | xb_w2_up
        | xb_w2_dn
        | xb_n3_up
        | xb_n3_dn
    )

    r_bar_s = r_ucl_s / D4_N4
    sigma_r = (r_ucl_s - r_bar_s) / 3.0
    r_above_cl = (rv > r_bar_s).astype(int)
    r_above_za = (rv > r_bar_s + 2.0 * sigma_r).astype(int)
    r_above_zb = (rv > r_bar_s + sigma_r).astype(int)
    r_w4 = r_above_cl.rolling(8, min_periods=8).sum() == 8
    r_w3 = r_above_zb.rolling(5, min_periods=5).sum() >= 4
    r_w2 = r_above_za.rolling(3, min_periods=3).sum() >= 2
    diff_rv = rv.diff()
    r_n3 = (diff_rv > 0).rolling(5, min_periods=5).sum() == 5
    runs_r = r_w4 | r_w3 | r_w2 | r_n3

    any_rule = (
        (iv > i_ucl_s)
        | (iv < i_lcl_s)
        | (mrv > mr_ucl_s)
        | (rv > r_ucl_s)
        | (xb_val > xb_ucl_s)
        | (xb_val < xb_lcl_s)
    )

    df["_blocked_baseline"] = any_rule.fillna(False).astype(bool)
    df["_runs_value"] = runs_value.fillna(False).astype(bool)
    df["_runs_disp"] = runs_disp.fillna(False).astype(bool)
    df["_runs_xbar"] = runs_xbar.fillna(False).astype(bool)
    df["_runs_r"] = runs_r.fillna(False).astype(bool)
    df["blocked_bc"] = (
        df["_blocked_baseline"]
        | df["_runs_value"]
        | df["_runs_disp"]
        | df["_runs_xbar"]
        | df["_runs_r"]
    )
    return df


def is_spc_bc_blocked(s: pd.DataFrame) -> bool:
    """Retorna True se o ticker estiver bloqueado B+C na ultima linha."""
    if s is None or s.empty:
        return True
    required = {"i_value", "i_ucl", "i_lcl", "mr_value", "mr_ucl"}
    if not required.issubset(set(s.columns)):
        return True
    try:
        enriched = _build_runs_flags(s)
        return bool(enriched.iloc[-1]["blocked_bc"])
    except Exception:
        return True


def spc_status_and_rules(df_ticker: pd.DataFrame) -> tuple[str, list[str], list[str]]:
    """Retorna status SPC + regras Regra 1 + flags Nelson/WE consultivas."""
    if df_ticker is None or df_ticker.empty:
        return "SPC_INDISPONIVEL", [], []

    required = {
        "i_value",
        "i_ucl",
        "i_lcl",
        "mr_value",
        "mr_ucl",
        "xbar_value",
        "xbar_ucl",
        "xbar_lcl",
        "r_value",
        "r_ucl",
    }
    if not required.issubset(set(df_ticker.columns)):
        return "SPC_INDISPONIVEL", [], []

    df = df_ticker.sort_values("date").copy()
    last = df.iloc[-1]

    i_val = _safe_float_spc(last.get("i_value"), float("nan"))
    i_ucl = _safe_float_spc(last.get("i_ucl"), float("nan"))
    i_lcl = _safe_float_spc(last.get("i_lcl"), float("nan"))
    if not (np.isfinite(i_val) and np.isfinite(i_ucl) and np.isfinite(i_lcl)):
        return "SPC_INDISPONIVEL", [], []

    mr_val = _safe_float_spc(last.get("mr_value"), float("nan"))
    mr_ucl = _safe_float_spc(last.get("mr_ucl"), float("nan"))
    xb_val = _safe_float_spc(last.get("xbar_value"), float("nan"))
    xb_ucl = _safe_float_spc(last.get("xbar_ucl"), float("nan"))
    xb_lcl = _safe_float_spc(last.get("xbar_lcl"), float("nan"))
    r_val = _safe_float_spc(last.get("r_value"), float("nan"))
    r_ucl = _safe_float_spc(last.get("r_ucl"), float("nan"))

    rules_fired: list[str] = []
    if np.isfinite(i_val) and np.isfinite(i_ucl) and i_val > i_ucl:
        rules_fired.append("I:R1_UP")
    if np.isfinite(i_val) and np.isfinite(i_lcl) and i_val < i_lcl:
        rules_fired.append("I:R1_DOWN")
    if np.isfinite(mr_val) and np.isfinite(mr_ucl) and mr_val > mr_ucl:
        rules_fired.append("MR:R1")
    if np.isfinite(xb_val) and np.isfinite(xb_ucl) and xb_val > xb_ucl:
        rules_fired.append("X:R1_UP")
    if np.isfinite(xb_val) and np.isfinite(xb_lcl) and xb_val < xb_lcl:
        rules_fired.append("X:R1_DOWN")
    if np.isfinite(r_val) and np.isfinite(r_ucl) and r_val > r_ucl:
        rules_fired.append("R:R1")

    if rules_fired:
        status = "INSTAVEL"
    else:
        band_width = float(abs(i_ucl - i_lcl))
        atencao_zone = 0.1 * band_width
        near_upper = i_val >= (i_ucl - atencao_zone)
        near_lower = i_val <= (i_lcl + atencao_zone)
        status = "ATENCAO" if (band_width > 0 and (near_upper or near_lower)) else "ESTAVEL"

    nelson_we_flags: list[str] = []
    try:
        enriched = _build_runs_flags(df)
        last_flags = enriched.iloc[-1]
        if bool(last_flags.get("_runs_value", False)):
            nelson_we_flags.append("W2/W3/W4/N3-I")
        if bool(last_flags.get("_runs_disp", False)):
            nelson_we_flags.append("W4/N3-MR")
        if bool(last_flags.get("_runs_xbar", False)):
            nelson_we_flags.append("W2/W3/W4/N3-Xbar")
        if bool(last_flags.get("_runs_r", False)):
            nelson_we_flags.append("W4/N3-R")
    except Exception:
        nelson_we_flags = []

    return status, rules_fired, nelson_we_flags

