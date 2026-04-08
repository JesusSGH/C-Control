
"""
Motor de conciliación contable v3.1.
- Trata correctamente importes negativos para la lógica de conciliación
- Mantiene la columna/origen original para exportación
- Excluye 570* y 572* del análisis interno
- Devuelve claves summary compatibles con app.py
- Soporta formato estándar y formato Holded (detección automática)
"""
import pandas as pd
import numpy as np
from itertools import combinations
from dataclasses import dataclass, field
from typing import Optional
import re
import time as _time
from collections import defaultdict

_HOLDED_ACCOUNT_RE = re.compile(r"^\s*(\d{3,})\s*-\s*(.+?)\s*$")


@dataclass
class Movement:
    row_idx: int
    fecha: Optional[object]
    asiento: Optional[str]
    contrapartida: Optional[str]
    descripcion: Optional[str]
    importe: float                  # importe absoluto para lógica de conciliación
    side: str                       # lado lógico: 'debe' o 'haber'
    original_side: Optional[str] = None   # lado visual/original en el mayor
    original_importe: Optional[float] = None  # importe con signo tal como venía en el mayor
    match_id: Optional[int] = None
    match_type: Optional[str] = None
    incidence_type: Optional[str] = None
    related_info: Optional[str] = None


@dataclass
class Account:
    codigo: str
    nombre: str
    start_row: int
    end_row: int
    movements: list = field(default_factory=list)
    debe_total: float = 0.0
    haber_total: float = 0.0
    saldo: float = 0.0


@dataclass
class MatchResult:
    match_id: int
    debe_indices: list
    haber_indices: list
    importe: float
    match_type: str
    confidence: str


def detect_columns(df):
    col_map = {
        'cuenta': None, 'fecha': None, 'asiento': None,
        'contrapartida': None, 'descripcion': None,
        'debe': None, 'haber': None, 'saldo': None
    }
    patterns = {
        'cuenta': r'cuenta|account|cta',
        'fecha': r'fecha|date|fcha',
        'asiento': r'asiento|entry|apunte|nº|num',
        'contrapartida': r'contra|offset|cpart',
        'descripcion': r'comen|descr|concepto|detalle|texto|ref',
        'debe': r'debe|debit|cargo',
        'haber': r'haber|credit|abono|crédito',
        'saldo': r'saldo|balance|acum',
    }
    cols_lower = {i: str(c).lower().strip() for i, c in enumerate(df.columns)}
    for role, pattern in patterns.items():
        for idx, col_name in cols_lower.items():
            if re.search(pattern, col_name):
                col_map[role] = df.columns[idx]
                break
    if not col_map['debe'] or not col_map['haber']:
        numeric_cols = df.select_dtypes(include=[np.number]).columns.tolist()
        if col_map['saldo']:
            numeric_cols = [c for c in numeric_cols if c != col_map['saldo']]
        if len(numeric_cols) >= 2 and not col_map['debe']:
            col_map['debe'] = numeric_cols[0]
            col_map['haber'] = numeric_cols[1]
    return col_map


def parse_mayor_file(filepath, sheet_name=None):
    ext = str(filepath).lower()
    if ext.endswith('.csv'):
        for enc in ['utf-8', 'latin-1', 'cp1252']:
            for sep in [',', ';', '\t']:
                try:
                    df = pd.read_csv(filepath, encoding=enc, sep=sep)
                    if len(df.columns) > 2:
                        return df
                except Exception:
                    continue
    return pd.read_excel(filepath, sheet_name=sheet_name or 0, header=None)


def _finalize_account(current_account, account_entries, end_row):
    current_account.movements = account_entries
    current_account.end_row = end_row
    current_account.debe_total = round(sum(m.importe for m in account_entries if m.side == 'debe'), 2)
    current_account.haber_total = round(sum(m.importe for m in account_entries if m.side == 'haber'), 2)
    current_account.saldo = round(current_account.debe_total - current_account.haber_total, 2)
    return current_account


def _is_holded_format(df):
    """Detecta si un DataFrame sin cabecera tiene formato Holded/libro mayor por bloques."""
    if df.empty or len(df.columns) < 5:
        return False
    first_col = df.iloc[:, 0].astype(str).fillna("")
    second_col = df.iloc[:, 1].astype(str).fillna("")
    has_libro_mayor = any("libro mayor" in v.lower() for v in first_col.head(5).tolist())
    has_account_line = any(_HOLDED_ACCOUNT_RE.match(v or "") for v in first_col.tolist())
    has_concepto = any(str(v).strip().lower() == "concepto" for v in second_col.tolist())
    return has_libro_mayor and has_account_line and has_concepto


def _extract_accounts_holded(df):
    """Parser de formato Holded: bloques por cuenta con cabecera repetida."""
    accounts = []
    i, n = 0, len(df)

    while i < n:
        raw_val = df.iat[i, 0]
        # Account headers are always strings, never datetime/float
        if not isinstance(raw_val, str):
            i += 1
            continue
        cell0 = raw_val.strip()
        match = _HOLDED_ACCOUNT_RE.match(cell0)
        if not match:
            i += 1
            continue

        code, name = match.group(1), match.group(2).strip()
        account_entries = []
        i += 1

        # Saltar hasta cabecera Fecha/Concepto
        while i < n:
            raw0 = df.iat[i, 0]
            c0 = str(raw0).strip() if pd.notna(raw0) else ""
            c1 = str(df.iat[i, 1]).strip().lower() if pd.notna(df.iat[i, 1]) else ""
            if c0.lower() == "fecha" and c1 == "concepto":
                i += 1
                break
            if isinstance(raw0, str) and _HOLDED_ACCOUNT_RE.match(c0):
                break
            i += 1

        # Leer movimientos del bloque
        row_counter = 0
        while i < n:
            raw0 = df.iat[i, 0]
            c0 = str(raw0).strip() if pd.notna(raw0) else ""
            c1 = str(df.iat[i, 1]).strip() if pd.notna(df.iat[i, 1]) else ""

            if isinstance(raw0, str) and _HOLDED_ACCOUNT_RE.match(c0):
                break
            if c1.lower() == "total":
                i += 1
                while i < n:
                    next0 = df.iat[i, 0]
                    if pd.notna(next0) and str(next0).strip():
                        break
                    i += 1
                break
            if not c0 and not c1:
                i += 1
                continue

            debe_raw = _parse_number(df.iat[i, 4] if df.shape[1] > 4 else 0.0)
            haber_raw = _parse_number(df.iat[i, 5] if df.shape[1] > 5 else 0.0)
            fecha = df.iat[i, 0] if pd.notna(df.iat[i, 0]) else None
            descripcion = str(df.iat[i, 1]).strip() if df.shape[1] > 1 and pd.notna(df.iat[i, 1]) else ""
            documento = str(df.iat[i, 2]).strip() if df.shape[1] > 2 and pd.notna(df.iat[i, 2]) else ""

            if debe_raw == 0.0 and haber_raw == 0.0 and not descripcion:
                i += 1
                continue

            base_kwargs = dict(
                row_idx=i,
                fecha=fecha,
                asiento=documento,
                contrapartida="",
                descripcion=descripcion,
            )

            if debe_raw > 0:
                account_entries.append(Movement(
                    **base_kwargs, importe=abs(debe_raw), side='debe',
                    original_side='debe', original_importe=debe_raw))
            elif debe_raw < 0:
                account_entries.append(Movement(
                    **base_kwargs, importe=abs(debe_raw), side='haber',
                    original_side='debe', original_importe=debe_raw))

            if haber_raw > 0:
                account_entries.append(Movement(
                    **base_kwargs, importe=abs(haber_raw), side='haber',
                    original_side='haber', original_importe=haber_raw))
            elif haber_raw < 0:
                account_entries.append(Movement(
                    **base_kwargs, importe=abs(haber_raw), side='debe',
                    original_side='haber', original_importe=haber_raw))

            row_counter += 1
            i += 1

        if account_entries:
            acc = Account(codigo=code, nombre=name, start_row=0, end_row=i)
            accounts.append(_finalize_account(acc, account_entries, i))

    return accounts


def extract_accounts_from_mayor(df):
    # Auto-detect Holded format
    if _is_holded_format(df):
        holded_accounts = _extract_accounts_holded(df)
        if holded_accounts:
            col_map = {
                'cuenta': None, 'fecha': df.columns[0] if len(df.columns) > 0 else None,
                'asiento': None, 'contrapartida': None,
                'descripcion': df.columns[1] if len(df.columns) > 1 else None,
                'debe': df.columns[4] if len(df.columns) > 4 else None,
                'haber': df.columns[5] if len(df.columns) > 5 else None,
                'saldo': df.columns[6] if len(df.columns) > 6 else None,
            }
            return holded_accounts, col_map

    accounts = []
    header_row = None
    for i in range(min(20, len(df))):
        row_vals = [str(v).lower().strip() for v in df.iloc[i].values if pd.notna(v)]
        row_text = ' '.join(row_vals)
        if 'debe' in row_text and 'haber' in row_text:
            header_row = i
            break
        if 'cuenta' in row_text and ('fecha' in row_text or 'asiento' in row_text):
            header_row = i
            break
    if header_row is None:
        header_row = 0

    headers = df.iloc[header_row].values
    col_names = [str(h).strip() if pd.notna(h) else f'col_{i}' for i, h in enumerate(headers)]
    df.columns = col_names
    df = df.iloc[header_row + 1:].reset_index(drop=True)

    col_map = detect_columns(df)
    cuenta_col = col_map.get('cuenta') or df.columns[0]
    fecha_col = col_map.get('fecha') or df.columns[1]
    asiento_col = col_map.get('asiento') or df.columns[2]
    contra_col = col_map.get('contrapartida') or (df.columns[3] if len(df.columns) > 3 else None)
    desc_col = col_map.get('descripcion') or (df.columns[4] if len(df.columns) > 4 else None)
    debe_col = col_map.get('debe') or df.columns[5]
    haber_col = col_map.get('haber') or df.columns[6]

    current_account = None
    account_entries = []

    for idx, row in df.iterrows():
        cuenta_val = row.get(cuenta_col)
        fecha_val = row.get(fecha_col)
        debe_val = row.get(debe_col)
        haber_val = row.get(haber_col)
        desc_val = row.get(desc_col) if desc_col else None
        asiento_val = row.get(asiento_col)

        fecha_str = str(fecha_val) if pd.notna(fecha_val) else ''
        if 'suma' in fecha_str.lower() or 'total' in fecha_str.lower():
            continue

        is_account_header = False
        if pd.notna(cuenta_val):
            cuenta_str = str(cuenta_val).strip()
            if cuenta_str.replace(' ', '').isdigit() and len(cuenta_str.replace(' ', '')) >= 4:
                if pd.isna(fecha_val) or str(fecha_val).strip() == '':
                    is_account_header = True

        if is_account_header:
            if current_account and account_entries:
                accounts.append(_finalize_account(current_account, account_entries, idx))
            nombre = str(asiento_val).strip() if pd.notna(asiento_val) else ''
            if not nombre and desc_col:
                nombre = str(row.get(desc_col, '')).strip()
            if nombre == 'nan':
                nombre = ''
            current_account = Account(codigo=str(cuenta_val).strip(), nombre=nombre, start_row=idx, end_row=idx)
            account_entries = []
            continue

        if current_account:
            d = _parse_number(debe_val)
            h = _parse_number(haber_val)

            base_kwargs = dict(
                row_idx=idx,
                fecha=fecha_val if pd.notna(fecha_val) else None,
                asiento=str(asiento_val).strip() if pd.notna(asiento_val) else None,
                contrapartida=str(row.get(contra_col, '')).strip() if contra_col and pd.notna(row.get(contra_col)) else None,
                descripcion=str(desc_val).strip() if pd.notna(desc_val) else None,
            )

            # Debe > 0 => Debe lógico y visual
            if d > 0:
                account_entries.append(Movement(
                    **base_kwargs,
                    importe=abs(d),
                    side='debe',
                    original_side='debe',
                    original_importe=d
                ))
            # Debe < 0 => Haber lógico, pero visualmente sigue en Debe negativo
            elif d < 0:
                account_entries.append(Movement(
                    **base_kwargs,
                    importe=abs(d),
                    side='haber',
                    original_side='debe',
                    original_importe=d
                ))

            # Haber > 0 => Haber lógico y visual
            if h > 0:
                account_entries.append(Movement(
                    **base_kwargs,
                    importe=abs(h),
                    side='haber',
                    original_side='haber',
                    original_importe=h
                ))
            # Haber < 0 => Debe lógico, pero visualmente sigue en Haber negativo
            elif h < 0:
                account_entries.append(Movement(
                    **base_kwargs,
                    importe=abs(h),
                    side='debe',
                    original_side='haber',
                    original_importe=h
                ))

    if current_account and account_entries:
        accounts.append(_finalize_account(current_account, account_entries, len(df)))

    return accounts, col_map


def _parse_number(val):
    if val is None or (isinstance(val, float) and np.isnan(val)):
        return 0.0
    if isinstance(val, (int, float)):
        return float(val)
    s = str(val).strip().replace(' ', '').replace('.', '').replace(',', '.')
    try:
        return float(s)
    except Exception:
        return 0.0


def _desc_similarity(a, b):
    if not a or not b:
        return 0.0
    words_a = set(str(a).lower().split())
    words_b = set(str(b).lower().split())
    stopwords = {'de', 'la', 'el', 'en', 'a', 'del', 'los', 'las', 'un', 'una', 'y', 'o', 'por', 'para', 'con', 'se', 'su'}
    words_a -= stopwords
    words_b -= stopwords
    if not words_a or not words_b:
        return 0.0
    return len(words_a & words_b) / max(len(words_a), len(words_b))


def reconcile_account(account, tolerance=0.01, max_combo=5):
    debe = [m for m in account.movements if m.side == 'debe' and m.match_id is None]
    haber = [m for m in account.movements if m.side == 'haber' and m.match_id is None]
    if not debe or not haber:
        return []

    matches = []
    match_counter = [0]

    def next_id():
        match_counter[0] += 1
        return match_counter[0]

    matched_debe = set()
    matched_haber = set()
    PASS_BUDGET = 8.0

    for d in debe:
        if d.row_idx in matched_debe:
            continue
        best_match, best_sim = None, -1
        for h in haber:
            if h.row_idx in matched_haber:
                continue
            if abs(d.importe - h.importe) <= tolerance:
                sim = _desc_similarity(d.descripcion, h.descripcion)
                if sim > best_sim:
                    best_sim = sim
                    best_match = h
        if best_match:
            mid = next_id()
            d.match_id = mid
            d.match_type = '1:1'
            best_match.match_id = mid
            best_match.match_type = '1:1'
            matched_debe.add(d.row_idx)
            matched_haber.add(best_match.row_idx)
            confidence = 'alta' if best_sim > 0.3 else 'media'
            matches.append(MatchResult(mid, [d.row_idx], [best_match.row_idx], d.importe, '1:1', confidence))

    remaining_debe = sorted([d for d in debe if d.row_idx not in matched_debe], key=lambda x: x.fecha if x.fecha is not None else pd.Timestamp.min)
    remaining_haber = [h for h in haber if h.row_idx not in matched_haber]
    t_pass = _time.time()
    for d in remaining_debe:
        if _time.time() - t_pass > PASS_BUDGET or d.row_idx in matched_debe:
            continue
        candidates = [h for h in remaining_haber if h.row_idx not in matched_haber]
        if len(candidates) > 40:
            candidates = sorted(candidates, key=lambda c: abs(c.importe))[:40]
        found = _find_subset_sum(candidates, matched_haber, d.importe, tolerance, max_combo)
        if found:
            mid = next_id()
            d.match_id = mid
            d.match_type = '1:N_parent'
            matched_debe.add(d.row_idx)
            h_idxs = []
            for h in found:
                h.match_id = mid
                h.match_type = '1:N_child'
                matched_haber.add(h.row_idx)
                h_idxs.append(h.row_idx)
            matches.append(MatchResult(mid, [d.row_idx], h_idxs, d.importe, '1:N', 'media'))

    remaining_debe = [d for d in debe if d.row_idx not in matched_debe]
    remaining_haber = sorted([h for h in haber if h.row_idx not in matched_haber], key=lambda x: x.fecha if x.fecha is not None else pd.Timestamp.min)
    t_pass = _time.time()
    for h in remaining_haber:
        if _time.time() - t_pass > PASS_BUDGET or h.row_idx in matched_haber:
            continue
        candidates = [d for d in remaining_debe if d.row_idx not in matched_debe]
        if len(candidates) > 40:
            candidates = sorted(candidates, key=lambda c: abs(c.importe))[:40]
        found = _find_subset_sum(candidates, matched_debe, h.importe, tolerance, max_combo)
        if found:
            mid = next_id()
            h.match_id = mid
            h.match_type = '1:N_parent'
            matched_haber.add(h.row_idx)
            d_idxs = []
            for d2 in found:
                d2.match_id = mid
                d2.match_type = '1:N_child'
                matched_debe.add(d2.row_idx)
                d_idxs.append(d2.row_idx)
            matches.append(MatchResult(mid, d_idxs, [h.row_idx], h.importe, 'N:1', 'media'))

    rem_d = sorted([d for d in debe if d.row_idx not in matched_debe], key=lambda x: x.fecha if x.fecha is not None else pd.Timestamp.min)
    rem_h = sorted([h for h in haber if h.row_idx not in matched_haber], key=lambda x: x.fecha if x.fecha is not None else pd.Timestamp.min)
    if rem_d and rem_h:
        _greedy_match(rem_d, rem_h, matched_debe, matched_haber, matches, match_counter, tolerance)

    return matches


def _greedy_match(debe_list, haber_list, m_d, m_h, matches, counter, tol):
    MAX_POOL = 200
    BUDGET = 5.0

    def nid():
        counter[0] += 1
        return counter[0]

    def _one_dir(targets, pool, mt, mp, t_side, mt_label):
        t0 = _time.time()
        for t in targets:
            if _time.time() - t0 > BUDGET or t.row_idx in mt:
                continue
            avail = [p for p in pool if p.row_idx not in mp]
            if not avail or len(avail) > MAX_POOL:
                continue
            acc, total = [], 0.0
            for p in avail:
                if p.row_idx in mp:
                    continue
                if total + p.importe <= t.importe + tol:
                    acc.append(p)
                    total += p.importe
                    if abs(total - t.importe) <= tol:
                        mid = nid()
                        t.match_id = mid
                        t.match_type = '1:N_parent'
                        mt.add(t.row_idx)
                        idxs = []
                        for pm in acc:
                            pm.match_id = mid
                            pm.match_type = '1:N_child'
                            mp.add(pm.row_idx)
                            idxs.append(pm.row_idx)
                        if t_side == 'debe':
                            matches.append(MatchResult(mid, [t.row_idx], idxs, t.importe, mt_label, 'baja'))
                        else:
                            matches.append(MatchResult(mid, idxs, [t.row_idx], t.importe, mt_label, 'baja'))
                        break

    _one_dir(debe_list, haber_list, m_d, m_h, 'debe', '1:N_greedy')
    _one_dir(haber_list, debe_list, m_h, m_d, 'haber', 'N:1_greedy')


def _find_subset_sum(candidates, already_matched, target, tolerance, max_size):
    available = [c for c in candidates if c.row_idx not in already_matched and c.importe <= target + tolerance]
    if not available:
        return None
    available.sort(key=lambda x: x.importe, reverse=True)
    pool = available[:min(len(available), 30)]
    for size in range(2, min(max_size + 1, len(pool) + 1)):
        if size > 5 and len(pool) > 20:
            break
        for combo in combinations(pool, size):
            if abs(sum(c.importe for c in combo) - target) <= tolerance:
                return list(combo)
    return None


def generate_summary(accounts, all_matches):
    def is_financial(acc):
        code = str(acc.codigo)
        return code.startswith('570') or code.startswith('572')

    financial_accounts = [a for a in accounts if is_financial(a)]
    non_financial_accounts = [a for a in accounts if not is_financial(a)]

    accounts_with_both = [
        a for a in non_financial_accounts
        if any(m.side == 'debe' for m in a.movements)
        and any(m.side == 'haber' for m in a.movements)
    ]
    one_side_accounts = [a for a in non_financial_accounts if a not in accounts_with_both]

    analyzed_movements = sum(len(a.movements) for a in accounts_with_both)
    total_matched = sum(1 for a in accounts_with_both for m in a.movements if m.match_id is not None)

    fully, partially, not_rec = [], [], []
    for acc in accounts_with_both:
        mc = sum(1 for m in acc.movements if m.match_id is not None)
        ratio = mc / len(acc.movements) if acc.movements else 0
        if ratio >= 0.95:
            fully.append(acc)
        elif ratio > 0:
            partially.append(acc)
        else:
            not_rec.append(acc)

    excluded_financial_movements = sum(len(a.movements) for a in financial_accounts)
    excluded_nonconcilable_movements = sum(len(a.movements) for a in one_side_accounts)

    return {
        'total_accounts': len(accounts),
        'accounts_with_both_sides': len(accounts_with_both),
        'accounts_one_side_only': len(one_side_accounts),

        'analyzed_accounts': len(accounts_with_both),
        'excluded_accounts': len(financial_accounts) + len(one_side_accounts),
        'analyzed_movements': analyzed_movements,
        'excluded_movements': excluded_financial_movements + excluded_nonconcilable_movements,

        'excluded_financial_accounts': len(financial_accounts),
        'excluded_financial_movements': excluded_financial_movements,
        'excluded_nonconcilable_accounts': len(one_side_accounts),
        'excluded_nonconcilable_movements': excluded_nonconcilable_movements,
        'excluded_one_side_accounts': len(one_side_accounts),
        'excluded_one_side_movements': excluded_nonconcilable_movements,

        'fully_reconciled': len(fully),
        'partially_reconciled': len(partially),
        'not_reconciled': len(not_rec),

        'total_movements': analyzed_movements,
        'total_matched': total_matched,
        'total_unmatched': analyzed_movements - total_matched,
        'match_rate': round((total_matched / analyzed_movements * 100) if analyzed_movements > 0 else 0, 1),
        'total_debe': round(sum(a.debe_total for a in accounts_with_both), 2),
        'total_haber': round(sum(a.haber_total for a in accounts_with_both), 2),
        'balance': round(sum(a.debe_total for a in accounts_with_both) - sum(a.haber_total for a in accounts_with_both), 2),

        'fully_reconciled_list': fully,
        'partially_reconciled_list': partially,
        'not_reconciled_list': not_rec,
        'one_side_list': one_side_accounts,
        'financial_list': financial_accounts,
    }


# ===============================
# Post-análisis de pendientes
# ===============================

SUGGESTION_555_PREFIXES = ("400", "4100", "170", "520", "523", "465")


def _normalize_text_inc(text: str) -> str:
    if text is None:
        return ""
    return re.sub(r"\s+", " ", str(text).strip().lower())


def _same_day_or_close(d1, d2, max_days=5):
    try:
        dt1 = pd.to_datetime(d1)
        dt2 = pd.to_datetime(d2)
        return abs((dt1 - dt2).days) <= max_days
    except Exception:
        return False


def _desc_similarity_inc(a, b):
    ta = set(_normalize_text_inc(a).split())
    tb = set(_normalize_text_inc(b).split())
    stop = {"de", "la", "el", "en", "a", "del", "los", "las", "un", "una", "y", "o", "por", "para", "con", "se", "su"}
    ta -= stop
    tb -= stop
    if not ta or not tb:
        return 0.0
    return len(ta & tb) / max(len(ta), len(tb))


def detect_recurrent_patterns(account):
    grouped = defaultdict(list)
    for m in getattr(account, "movements", []) or []:
        if getattr(m, "match_id", None) is not None:
            continue
        key = (
            round(float(getattr(m, "importe", 0.0) or 0.0), 2),
            str(getattr(m, "side", "") or "").lower().strip(),
            _normalize_text_inc(getattr(m, "descripcion", "")),
        )
        grouped[key].append(m)

    recurrent_ids = set()
    for group in grouped.values():
        if len(group) < 3:
            continue
        fechas = []
        for m in group:
            try:
                if getattr(m, "fecha", None) is not None:
                    fechas.append(pd.to_datetime(m.fecha))
            except Exception:
                continue
        fechas = sorted(fechas)
        if len(fechas) < 3:
            continue
        diffs = [(fechas[i] - fechas[i - 1]).days for i in range(1, len(fechas))]
        if diffs and all(25 <= d <= 35 for d in diffs):
            for m in group:
                recurrent_ids.add(id(m))
    return recurrent_ids


def detect_duplicates(account):
    movements = [m for m in (getattr(account, "movements", []) or []) if getattr(m, "match_id", None) is None]
    duplicates = {}
    for i, m1 in enumerate(movements):
        for j, m2 in enumerate(movements):
            if i >= j:
                continue
            same_amount = round(float(getattr(m1, "importe", 0.0) or 0.0), 2) == round(float(getattr(m2, "importe", 0.0) or 0.0), 2)
            same_side = str(getattr(m1, "side", "") or "").lower().strip() == str(getattr(m2, "side", "") or "").lower().strip()
            same_desc = _normalize_text_inc(getattr(m1, "descripcion", "")) == _normalize_text_inc(getattr(m2, "descripcion", ""))
            same_date = _same_day_or_close(getattr(m1, "fecha", None), getattr(m2, "fecha", None), max_days=2)
            asiento_1 = _normalize_text_inc(getattr(m1, "asiento", ""))
            asiento_2 = _normalize_text_inc(getattr(m2, "asiento", ""))
            same_asiento = bool(asiento_1 and asiento_2 and asiento_1 == asiento_2)
            if same_amount and same_side and same_desc and same_date:
                duplicates[id(m2)] = m1
            elif same_amount and same_side and same_asiento and same_date and same_desc:
                duplicates[id(m2)] = m1
    return duplicates


def _movement_signature_ok(m1, m2, same_side_required=False):
    amount_ok = abs(round(float(getattr(m1, "importe", 0.0) or 0.0), 2) - round(float(getattr(m2, "importe", 0.0) or 0.0), 2)) <= 0.01
    if not amount_ok:
        return False
    side1 = str(getattr(m1, "side", "") or "").lower().strip()
    side2 = str(getattr(m2, "side", "") or "").lower().strip()
    if same_side_required:
        if side1 != side2:
            return False
    else:
        if side1 == side2:
            return False
    if not _same_day_or_close(getattr(m1, "fecha", None), getattr(m2, "fecha", None), max_days=5):
        return False
    desc_sim = _desc_similarity_inc(getattr(m1, "descripcion", ""), getattr(m2, "descripcion", ""))
    asiento1 = _normalize_text_inc(getattr(m1, "asiento", ""))
    asiento2 = _normalize_text_inc(getattr(m2, "asiento", ""))
    same_asiento = bool(asiento1 and asiento2 and asiento1 == asiento2)
    return desc_sim >= 0.35 or same_asiento or (not getattr(m1, "descripcion", None) and not getattr(m2, "descripcion", None))


def cross_account_suggestions(accounts):
    account_movs = []
    for acc in accounts:
        code = str(getattr(acc, "codigo", "") or "")
        for m in getattr(acc, "movements", []) or []:
            if getattr(m, "match_id", None) is not None:
                continue
            account_movs.append((acc, code, m))

    for acc1, code1, m1 in account_movs:
        if getattr(m1, "incidence_type", None) == "Posible duplicado":
            continue

        for acc2, code2, m2 in account_movs:
            if acc1 is acc2:
                continue
            if m1 is m2:
                continue

            # Proveedores entre 400 y 4100
            if (code1.startswith("400") or code1.startswith("4100")) and (code2.startswith("400") or code2.startswith("4100")):
                if _movement_signature_ok(m1, m2, same_side_required=False):
                    m1.incidence_type = "Posible proveedor alternativo"
                    m1.related_info = f"{code2} - {getattr(acc2, 'nombre', '')}"
                    break

            # Cuenta 555 contra cuentas objetivo
            if code1.startswith("555") and any(code2.startswith(p) for p in SUGGESTION_555_PREFIXES):
                if _movement_signature_ok(m1, m2, same_side_required=False):
                    m1.incidence_type = "Posible cruce con otra cuenta"
                    m1.related_info = f"{code2} - {getattr(acc2, 'nombre', '')}"
                    break

            # Y también desde 400/4100/170/etc hacia 555
            if any(code1.startswith(p) for p in SUGGESTION_555_PREFIXES) and code2.startswith("555"):
                if _movement_signature_ok(m1, m2, same_side_required=False):
                    if code1.startswith("400") or code1.startswith("4100"):
                        m1.incidence_type = "Posible relación con 555"
                    else:
                        m1.incidence_type = "Posible cruce con 555"
                    m1.related_info = f"{code2} - {getattr(acc2, 'nombre', '')}"
                    break


def post_analysis(accounts):
    for acc in accounts:
        recurrent_ids = detect_recurrent_patterns(acc)
        duplicate_map = detect_duplicates(acc)

        for m in getattr(acc, "movements", []) or []:
            if getattr(m, "match_id", None) is not None:
                m.incidence_type = None
                m.related_info = None
                continue

            m.incidence_type = None
            m.related_info = None

            if id(m) in recurrent_ids:
                m.incidence_type = "Patrón recurrente"
                continue

            if id(m) in duplicate_map:
                related = duplicate_map[id(m)]
                m.incidence_type = "Posible duplicado"
                m.related_info = f"Posible duplicado de fila {getattr(related, 'row_idx', '')}"

    cross_account_suggestions(accounts)
