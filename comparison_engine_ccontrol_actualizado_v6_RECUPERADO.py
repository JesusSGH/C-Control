import re
from collections import Counter
import pandas as pd

EXCLUDED_ANALYSIS_PREFIXES = (
    "570", "572",
    "6", "7",
    "102", "170", "215", "281", "300",
)


def normalize_text(text: str) -> str:
    if text is None:
        return ""
    s = str(text).upper().strip()
    s = re.sub(r"\s+", " ", s)
    s = re.sub(r"[^A-Z0-9 /.\-]", "", s)
    return s


def extract_reference(text: str) -> str:
    s = normalize_text(text)
    nums = re.findall(r"\d{4,}", s)
    if not nums:
        return ""
    nums = sorted(nums, key=len)
    return nums[-1]


def is_excluded_account(code: str) -> bool:
    code = str(code or "")
    return any(code.startswith(prefix) for prefix in EXCLUDED_ANALYSIS_PREFIXES)


def flatten_accounts(accounts, exclude_analysis_accounts=True, unmatched_only=True):
    rows = []
    for acc in accounts:
        code = str(getattr(acc, "codigo", "") or "")
        if exclude_analysis_accounts and is_excluded_account(code):
            continue
        name = str(getattr(acc, "nombre", "") or "")
        for m in getattr(acc, "movements", []) or []:
            if unmatched_only and getattr(m, "match_id", None) is not None:
                continue
            desc = getattr(m, "descripcion", "") or ""
            contra = getattr(m, "contrapartida", "") or ""
            asiento = getattr(m, "asiento", "") or ""
            ref = extract_reference(" ".join([str(desc), str(contra), str(asiento)]))
            amount = round(float(getattr(m, "importe", 0.0) or 0.0), 2)
            side = str(getattr(m, "side", "") or "").lower().strip()
            rows.append({
                "Cuenta": code,
                "Nombre": name,
                "Fecha": getattr(m, "fecha", None),
                "Asiento": asiento,
                "Contrapartida": contra,
                "Descripción": desc,
                "Importe": amount,
                "Lado": side,
                "Referencia": ref,
                "DescripcionNorm": normalize_text(desc),
                "ContrapartidaNorm": normalize_text(contra),
            })
    df = pd.DataFrame(rows)
    if not df.empty:
        df["ImporteCent"] = (df["Importe"].round(2) * 100).round().astype(int)
        df["ClaveComparacion"] = list(zip(
            df["Cuenta"],
            df["Lado"],
            df["ImporteCent"],
            df["Referencia"],
            df["DescripcionNorm"].str[:40],
            df["ContrapartidaNorm"].str[:20],
        ))
    return df


def build_account_summary(accounts, exclude_analysis_accounts=True, unmatched_only=True):
    rows = []
    for acc in accounts:
        code = str(getattr(acc, "codigo", "") or "")
        if exclude_analysis_accounts and is_excluded_account(code):
            continue
        movements = []
        for m in getattr(acc, "movements", []) or []:
            if unmatched_only and getattr(m, "match_id", None) is not None:
                continue
            movements.append(m)
        if not movements:
            continue
        debe = round(sum(float(getattr(m, "importe", 0.0) or 0.0) for m in movements if str(getattr(m, "side", "")).lower() == "debe"), 2)
        haber = round(sum(float(getattr(m, "importe", 0.0) or 0.0) for m in movements if str(getattr(m, "side", "")).lower() == "haber"), 2)
        saldo = round(debe - haber, 2)
        rows.append({
            "Cuenta": code,
            "Nombre": str(getattr(acc, "nombre", "") or ""),
            "Movimientos": len(movements),
            "Debe": debe,
            "Haber": haber,
            "Saldo": saldo,
        })
    return pd.DataFrame(rows)


def detect_duplicates(df: pd.DataFrame):
    cols = ["Cuenta", "Nombre", "Fecha", "Asiento", "Contrapartida", "Descripción", "Importe", "Lado", "Tipo Duplicado", "Motivo"]
    if df.empty:
        return pd.DataFrame(columns=cols)
    work = df.copy()
    exact_key = list(zip(
        work["Cuenta"],
        work["Fecha"].astype(str),
        work["Lado"],
        work["ImporteCent"],
        work["ContrapartidaNorm"],
        work["DescripcionNorm"],
        work["Asiento"].astype(str),
    ))
    work["ExactKey"] = exact_key
    exact_counts = work["ExactKey"].value_counts()
    exact_dups = work[work["ExactKey"].map(exact_counts) > 1].copy()
    exact_dups["Tipo Duplicado"] = "Duplicado exacto"
    exact_dups["Motivo"] = "Misma huella contable sobre pendiente no conciliado"
    if exact_dups.empty:
        return pd.DataFrame(columns=cols)
    return exact_dups[cols].sort_values(["Cuenta", "Fecha", "Importe"], ascending=[True, True, False]).reset_index(drop=True)


def detect_grouped_payments(df: pd.DataFrame):
    cols = ["Cuenta", "Nombre", "Referencia", "Importe Grupo Debe", "Importe Grupo Haber", "N Debe", "N Haber", "Tipo Agrupación", "Observación"]
    if df.empty:
        return pd.DataFrame(columns=cols)
    work = df.copy()
    work = work[work["Referencia"].astype(str) != ""]
    if work.empty:
        return pd.DataFrame(columns=cols)

    rows = []
    for (cuenta, nombre, ref), g in work.groupby(["Cuenta", "Nombre", "Referencia"], dropna=False):
        debe = g[g["Lado"] == "debe"]
        haber = g[g["Lado"] == "haber"]
        if debe.empty or haber.empty:
            continue
        sdebe = round(float(debe["Importe"].sum()), 2)
        shaber = round(float(haber["Importe"].sum()), 2)
        if abs(sdebe - shaber) <= 0.01:
            n_debe = len(debe)
            n_haber = len(haber)
            if n_debe == 1 and n_haber == 1:
                tipo = "Directa 1:1"
            elif n_debe == 1 and n_haber > 1:
                tipo = "Pago agrupado 1:N"
            elif n_debe > 1 and n_haber == 1:
                tipo = "Pago agrupado N:1"
            else:
                tipo = "Agrupado N:N"
            rows.append({
                "Cuenta": cuenta,
                "Nombre": nombre,
                "Referencia": ref,
                "Importe Grupo Debe": sdebe,
                "Importe Grupo Haber": shaber,
                "N Debe": n_debe,
                "N Haber": n_haber,
                "Tipo Agrupación": tipo,
                "Observación": "Referencia detectada y suma equilibrada sobre partidas pendientes",
            })
    if not rows:
        return pd.DataFrame(columns=cols)
    return pd.DataFrame(rows).sort_values(["Cuenta", "Referencia"]).reset_index(drop=True)


def _safe_sum(df: pd.DataFrame, col: str) -> float:
    if df.empty or col not in df.columns:
        return 0.0
    return round(float(df[col].sum()), 2)


def build_state_impact_summary(persistent_df, corrected_df, new_df):
    imp_persist = _safe_sum(persistent_df, "Importe")
    imp_corr = _safe_sum(corrected_df, "Importe")
    imp_new = _safe_sum(new_df, "Importe")
    return {
        "impacto_persistentes": imp_persist,
        "impacto_corregidos": imp_corr,
        "impacto_nuevos": imp_new,
        "impacto_total_pendiente": round(imp_persist + imp_new, 2),
    }


def _aggregate_counts_and_amounts(df: pd.DataFrame, count_name: str, amount_name: str) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(columns=["Cuenta", count_name, amount_name])
    return df.groupby("Cuenta", dropna=False).agg(**{
        count_name: ("Cuenta", "size"),
        amount_name: ("Importe", lambda s: round(float(s.sum()), 2)),
    }).reset_index()


def enrich_accounts_with_phase1_metrics(account_compare, persistent_df, corrected_df, new_df):
    df = account_compare.copy()
    for extra in (
        _aggregate_counts_and_amounts(persistent_df, "Persistentes", "Impacto Persistentes"),
        _aggregate_counts_and_amounts(corrected_df, "Corregidos", "Impacto Corregidos"),
        _aggregate_counts_and_amounts(new_df, "Nuevos", "Impacto Nuevos"),
    ):
        df = df.merge(extra, on="Cuenta", how="left")

    fill_zero_cols = [
        "Persistentes", "Impacto Persistentes",
        "Corregidos", "Impacto Corregidos",
        "Nuevos", "Impacto Nuevos",
    ]
    for col in fill_zero_cols:
        if col not in df.columns:
            df[col] = 0
    df[fill_zero_cols] = df[fill_zero_cols].fillna(0)

    df["Impacto Bruto"] = (df["Impacto Persistentes"].abs() + df["Impacto Nuevos"].abs()).round(2)
    df["Impacto Neto"] = (df["Impacto Persistentes"] + df["Impacto Nuevos"]).round(2)
    df["Recurrencia"] = (df["Persistentes"] >= 3).astype(int)
    df["Concentración"] = (df["Impacto Bruto"] >= 5000).astype(int)
    df["Crecimiento Anómalo"] = ((df["Nuevos"] >= 5) | (df["Var Saldo"].abs() >= 10000)).astype(int)

    df["Score Riesgo"] = (
        (df["Impacto Bruto"].clip(lower=0) / 1000.0) * 1.6 +
        df["Persistentes"] * 2.2 +
        df["Nuevos"] * 1.0 +
        df["Recurrencia"] * 7.0 +
        df["Concentración"] * 8.0 +
        df["Crecimiento Anómalo"] * 4.0 +
        (df["Var Saldo"].abs().clip(lower=0) / 2500.0)
    ).round(2)

    def risk_level(score: float) -> str:
        if score >= 30:
            return "Crítico"
        if score >= 18:
            return "Alto"
        if score >= 8:
            return "Medio"
        return "Bajo"

    def risk_reason(row) -> str:
        reasons = []
        if row["Persistentes"] > 0:
            reasons.append(f"persistencia ({int(row['Persistentes'])})")
        if row["Nuevos"] > 0:
            reasons.append(f"nuevos ({int(row['Nuevos'])})")
        if abs(row["Impacto Bruto"]) >= 5000:
            reasons.append("importe alto")
        if row["Recurrencia"]:
            reasons.append("recurrencia")
        if row["Crecimiento Anómalo"]:
            reasons.append("crecimiento")
        return ", ".join(reasons) if reasons else "sin señales relevantes"

    def review_priority(row) -> str:
        if row["Nivel Riesgo"] == "Crítico" or (row["Impacto Bruto"] >= 20000 and row["Persistentes"] > 0):
            return "Inmediata"
        if row["Nivel Riesgo"] == "Alto":
            return "Alta"
        if row["Nivel Riesgo"] == "Medio":
            return "Media"
        return "Baja"

    df["Nivel Riesgo"] = df["Score Riesgo"].apply(risk_level)
    df["Motivo Riesgo"] = df.apply(risk_reason, axis=1)
    df["Prioridad Revisión"] = df.apply(review_priority, axis=1)
    return df


def generate_insights(summary, accounts_df, duplicated_prev=None, duplicated_curr=None):
    insights = []
    duplicated_prev = duplicated_prev if duplicated_prev is not None else pd.DataFrame()
    duplicated_curr = duplicated_curr if duplicated_curr is not None else pd.DataFrame()

    if not accounts_df.empty:
        critical = accounts_df.sort_values(["Score Riesgo", "Impacto Bruto"], ascending=[False, False]).head(5)
        for _, row in critical.iterrows():
            if row["Nivel Riesgo"] in ("Crítico", "Alto") and (row["Persistentes"] > 0 or row["Nuevos"] > 0):
                sev = "Crítica" if row["Nivel Riesgo"] == "Crítico" else "Alta"
                insights.append((
                    sev,
                    "Cuenta prioritaria",
                    f"Cuenta {row['Cuenta']} ({row['Nombre']}) · prioridad {row['Prioridad Revisión'].lower()} · {row['Motivo Riesgo']}."
                ))

    if summary.get("impacto_persistentes", 0.0) >= 10000:
        insights.append(("Alta", "Impacto persistente elevado", f"El impacto persistente asciende a {summary.get('impacto_persistentes', 0.0):,.2f} €."))
    if summary.get("nuevos", 0) >= 10:
        insights.append(("Media", "Nuevos relevantes", f"Se incorporan {summary.get('nuevos', 0)} partidas pendientes nuevas."))
    if summary.get("persistentes", 0) >= 25:
        insights.append(("Media", "Persistencia relevante", f"Se mantienen {summary.get('persistentes', 0)} partidas pendientes entre periodos."))
    if abs(summary.get("variacion_saldo", 0.0)) > 10000:
        insights.append(("Alta", "Variación fuerte de saldo", f"La variación global del pendiente es {summary.get('variacion_saldo', 0.0):,.2f} €."))
    if len(duplicated_prev) + len(duplicated_curr) > 0:
        insights.append(("Media", "Duplicados exactos", f"Se han detectado {len(duplicated_prev) + len(duplicated_curr)} duplicidades exactas sobre pendientes."))

    sev_order = {"Crítica": 0, "Alta": 1, "Media": 2, "Baja": 3}
    out = pd.DataFrame(insights, columns=["Severidad", "Tipo Insight", "Detalle"])
    if out.empty:
        return out
    out["_sev"] = out["Severidad"].map(sev_order).fillna(99)
    return out.sort_values(["_sev", "Tipo Insight"]).drop(columns=["_sev"]).reset_index(drop=True)


def get_top_problem_accounts(accounts_df, top_n=10):
    if accounts_df.empty:
        return accounts_df.copy()
    cols = [
        "Cuenta", "Nombre", "Prioridad Revisión", "Nivel Riesgo", "Score Riesgo", "Motivo Riesgo",
        "Persistentes", "Nuevos", "Corregidos",
        "Impacto Persistentes", "Impacto Nuevos", "Impacto Corregidos",
        "Impacto Bruto", "Impacto Neto", "Var Saldo", "Var Movimientos"
    ]
    existing = [c for c in cols if c in accounts_df.columns]
    ordered = accounts_df.sort_values(
        ["Score Riesgo", "Impacto Persistentes", "Impacto Bruto", "Var Saldo"],
        ascending=[False, False, False, False],
    )[existing]
    return ordered[(ordered.get("Persistentes", 0) > 0) | (ordered.get("Nuevos", 0) > 0)].head(top_n).reset_index(drop=True)


def compare_accounts(previous_accounts, current_accounts):
    prev_mov = flatten_accounts(previous_accounts, exclude_analysis_accounts=True, unmatched_only=True)
    curr_mov = flatten_accounts(current_accounts, exclude_analysis_accounts=True, unmatched_only=True)

    prev_counter = Counter(prev_mov["ClaveComparacion"].tolist()) if not prev_mov.empty else Counter()
    curr_counter = Counter(curr_mov["ClaveComparacion"].tolist()) if not curr_mov.empty else Counter()

    persistent_keys, corrected_keys, new_keys = [], [], []
    all_keys = set(prev_counter) | set(curr_counter)
    for k in all_keys:
        overlap = min(prev_counter.get(k, 0), curr_counter.get(k, 0))
        if overlap:
            persistent_keys.extend([k] * overlap)
        if prev_counter.get(k, 0) > overlap:
            corrected_keys.extend([k] * (prev_counter[k] - overlap))
        if curr_counter.get(k, 0) > overlap:
            new_keys.extend([k] * (curr_counter[k] - overlap))

    def rows_from_keys(df, keys):
        cols = ["Cuenta", "Nombre", "Fecha", "Asiento", "Contrapartida", "Descripción", "Importe", "Lado", "Referencia"]
        if df.empty:
            return pd.DataFrame(columns=cols)
        need = Counter(keys)
        out = []
        for _, row in df.iterrows():
            k = row["ClaveComparacion"]
            if need.get(k, 0) > 0:
                out.append({c: row[c] for c in cols})
                need[k] -= 1
        return pd.DataFrame(out)

    persistent_df = rows_from_keys(curr_mov, persistent_keys)
    corrected_df = rows_from_keys(prev_mov, corrected_keys)
    new_df = rows_from_keys(curr_mov, new_keys)

    prev_summary = build_account_summary(previous_accounts, exclude_analysis_accounts=True, unmatched_only=True)
    curr_summary = build_account_summary(current_accounts, exclude_analysis_accounts=True, unmatched_only=True)

    account_compare = prev_summary.merge(
        curr_summary,
        on=["Cuenta", "Nombre"],
        how="outer",
        suffixes=("_Anterior", "_Actual"),
    ).fillna(0)

    if account_compare.empty:
        account_compare = pd.DataFrame(columns=[
            "Cuenta", "Nombre", "Movimientos_Anterior", "Debe_Anterior", "Haber_Anterior", "Saldo_Anterior",
            "Movimientos_Actual", "Debe_Actual", "Haber_Actual", "Saldo_Actual",
        ])

    account_compare["Var Movimientos"] = account_compare.get("Movimientos_Actual", 0) - account_compare.get("Movimientos_Anterior", 0)
    account_compare["Var Debe"] = account_compare.get("Debe_Actual", 0) - account_compare.get("Debe_Anterior", 0)
    account_compare["Var Haber"] = account_compare.get("Haber_Actual", 0) - account_compare.get("Haber_Anterior", 0)
    account_compare["Var Saldo"] = account_compare.get("Saldo_Actual", 0) - account_compare.get("Saldo_Anterior", 0)
    account_compare["Tipo Problema"] = account_compare["Var Saldo"].apply(lambda x: "OK" if abs(x) < 1 else ("Aumento saldo" if x > 0 else "Disminución saldo"))

    account_compare = enrich_accounts_with_phase1_metrics(account_compare, persistent_df, corrected_df, new_df)

    duplicates_prev = detect_duplicates(prev_mov)
    duplicates_curr = detect_duplicates(curr_mov)
    grouped_prev = detect_grouped_payments(prev_mov)
    grouped_curr = detect_grouped_payments(curr_mov)

    high_risk = int((account_compare["Nivel Riesgo"].isin(["Crítico", "Alto"])).sum()) if not account_compare.empty else 0

    summary = {
        "cuentas_comparadas": int(len(account_compare)),
        "movimientos_anterior": int(len(prev_mov)),
        "movimientos_actual": int(len(curr_mov)),
        "persistentes": int(len(persistent_df)),
        "corregidos": int(len(corrected_df)),
        "nuevos": int(len(new_df)),
        "saldo_anterior": round(float(prev_summary["Saldo"].sum()) if not prev_summary.empty else 0.0, 2),
        "saldo_actual": round(float(curr_summary["Saldo"].sum()) if not curr_summary.empty else 0.0, 2),
        "variacion_saldo": round((float(curr_summary["Saldo"].sum()) if not curr_summary.empty else 0.0) - (float(prev_summary["Saldo"].sum()) if not prev_summary.empty else 0.0), 2),
        "duplicados_anterior": int(len(duplicates_prev)),
        "duplicados_actual": int(len(duplicates_curr)),
        "agrupados_anterior": int(len(grouped_prev)),
        "agrupados_actual": int(len(grouped_curr)),
        "cuentas_excluidas_regla": ", ".join(EXCLUDED_ANALYSIS_PREFIXES),
        "metodologia": "Comparación de partidas pendientes no conciliadas tras conciliación interna por cuenta",
        "cuentas_riesgo_alto": high_risk,
    }
    summary.update(build_state_impact_summary(persistent_df, corrected_df, new_df))

    ordered_accounts = account_compare.sort_values(
        ["Score Riesgo", "Impacto Persistentes", "Impacto Bruto", "Cuenta", "Nombre"],
        ascending=[False, False, False, True, True],
    ).reset_index(drop=True)

    return {
        "summary": summary,
        "accounts": ordered_accounts,
        "persistentes": persistent_df.reset_index(drop=True),
        "corregidos": corrected_df.reset_index(drop=True),
        "nuevos": new_df.reset_index(drop=True),
        "insights": generate_insights(summary, ordered_accounts, duplicates_prev, duplicates_curr),
        "top_accounts": get_top_problem_accounts(ordered_accounts),
        "duplicados_anterior": duplicates_prev,
        "duplicados_actual": duplicates_curr,
        "agrupados_anterior": grouped_prev,
        "agrupados_actual": grouped_curr,
    }
