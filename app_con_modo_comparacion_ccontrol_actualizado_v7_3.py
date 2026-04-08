import html
import os
import re
import tempfile
from dataclasses import dataclass, field
from pathlib import Path

from io import BytesIO
import pandas as pd
import streamlit as st

from engine import parse_mayor_file, extract_accounts_from_mayor, reconcile_account, generate_summary, post_analysis
from excel_export import export_reconciliation

from comparison_engine_ccontrol_actualizado_v6_RECUPERADO import compare_accounts, is_excluded_account
from excel_export_comparison_ccontrol_actualizado_v6_RECUPERADO import export_period_comparison


st.set_page_config(page_title="C-Control", page_icon="📊", layout="wide", initial_sidebar_state="expanded")



@dataclass
class ParsedMovement:
    fecha: object = None
    descripcion: str = ""
    asiento: str = ""
    contrapartida: str = ""
    importe: float = 0.0
    side: str = ""
    debe: float = 0.0
    haber: float = 0.0
    saldo: float = 0.0
    documento: str = ""
    tags: str = ""
    match_id: object = None


@dataclass
class ParsedAccount:
    codigo: str
    nombre: str
    movements: list = field(default_factory=list)
    debe_total: float = 0.0
    haber_total: float = 0.0
    saldo: float = 0.0


_ACCOUNT_LINE_RE = re.compile(r"^\s*(\d{3,10})\s+-\s+(.+?)\s*$")


def _safe_float(value) -> float:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return 0.0
    if isinstance(value, (int, float)):
        return round(float(value), 2)
    s = str(value).strip().replace(".", "").replace(",", ".")
    try:
        return round(float(s), 2)
    except Exception:
        return 0.0


def _build_account(code: str, name: str, movements: list[ParsedMovement]) -> ParsedAccount:
    debe_total = round(sum(float(getattr(m, "debe", 0.0) or 0.0) for m in movements), 2)
    haber_total = round(sum(float(getattr(m, "haber", 0.0) or 0.0) for m in movements), 2)
    saldo = round(float(getattr(movements[-1], "saldo", 0.0) or 0.0), 2) if movements else 0.0
    return ParsedAccount(codigo=str(code), nombre=str(name), movements=movements, debe_total=debe_total, haber_total=haber_total, saldo=saldo)


def parse_holded_accounts(file_path: str):
    try:
        raw = pd.read_excel(file_path, sheet_name=0, header=None)
    except Exception:
        return []

    if raw.empty:
        return []

    # Detección flexible del layout Holded/libro mayor por bloques
    first_col = raw.iloc[:, 0].astype(str).fillna("")
    second_col = raw.iloc[:, 1].astype(str).fillna("")
    looks_holded = (
        any("libro mayor" in v.lower() for v in first_col.head(5).tolist())
        and any(_ACCOUNT_LINE_RE.match(v or "") for v in first_col.tolist())
        and any(str(v).strip().lower() == "concepto" for v in second_col.tolist())
    )
    if not looks_holded:
        return []

    accounts = []
    i = 0
    n = len(raw)

    while i < n:
        cell0 = str(raw.iat[i, 0]).strip() if pd.notna(raw.iat[i, 0]) else ""
        match = _ACCOUNT_LINE_RE.match(cell0)
        if not match:
            i += 1
            continue

        code, name = match.group(1), match.group(2).strip()
        movements = []
        i += 1

        # Saltar hasta cabecera Fecha/Concepto
        while i < n:
            c0 = str(raw.iat[i, 0]).strip() if pd.notna(raw.iat[i, 0]) else ""
            c1 = str(raw.iat[i, 1]).strip().lower() if pd.notna(raw.iat[i, 1]) else ""
            if c0.lower() == "fecha" and c1 == "concepto":
                i += 1
                break
            if _ACCOUNT_LINE_RE.match(c0):
                break
            i += 1

        # Leer movimientos del bloque
        while i < n:
            c0 = str(raw.iat[i, 0]).strip() if pd.notna(raw.iat[i, 0]) else ""
            c1 = str(raw.iat[i, 1]).strip() if pd.notna(raw.iat[i, 1]) else ""

            if _ACCOUNT_LINE_RE.match(c0):
                break
            if c1.lower() == "total":
                i += 1
                # saltar líneas vacías posteriores
                while i < n:
                    next0 = str(raw.iat[i, 0]).strip() if pd.notna(raw.iat[i, 0]) else ""
                    if next0:
                        break
                    i += 1
                break
            if not c0 and not c1:
                i += 1
                continue

            debe = _safe_float(raw.iat[i, 4] if raw.shape[1] > 4 else 0.0)
            haber = _safe_float(raw.iat[i, 5] if raw.shape[1] > 5 else 0.0)
            saldo = _safe_float(raw.iat[i, 6] if raw.shape[1] > 6 else 0.0)
            fecha = raw.iat[i, 0]
            descripcion = str(raw.iat[i, 1]).strip() if raw.shape[1] > 1 and pd.notna(raw.iat[i, 1]) else ""
            documento = str(raw.iat[i, 2]).strip() if raw.shape[1] > 2 and pd.notna(raw.iat[i, 2]) else ""
            tags = str(raw.iat[i, 3]).strip() if raw.shape[1] > 3 and pd.notna(raw.iat[i, 3]) else ""

            if debe == 0.0 and haber == 0.0 and saldo == 0.0 and not descripcion:
                i += 1
                continue

            if debe > 0 and haber > 0:
                # caso atípico; priorizamos lado por mayor importe
                side = "debe" if debe >= haber else "haber"
                importe = debe if side == "debe" else haber
            else:
                side = "debe" if debe > 0 else "haber"
                importe = debe if debe > 0 else haber

            movements.append(
                ParsedMovement(
                    fecha=fecha,
                    descripcion=descripcion,
                    asiento=documento,
                    contrapartida="",
                    importe=round(float(importe or 0.0), 2),
                    side=side,
                    debe=debe,
                    haber=haber,
                    saldo=saldo,
                    documento=documento,
                    tags=tags,
                    match_id=None,
                )
            )
            i += 1

        accounts.append(_build_account(code, name, movements))

    return [acc for acc in accounts if acc.movements]


def load_accounts_auto(file_path: str):
    parser_name = "estándar"
    try:
        df = parse_mayor_file(file_path)
        accounts, _ = extract_accounts_from_mayor(df)
        valid_accounts = [acc for acc in (accounts or []) if getattr(acc, "movements", None)]
        if valid_accounts:
            return valid_accounts, parser_name
    except Exception:
        pass

    holded_accounts = parse_holded_accounts(file_path)
    if holded_accounts:
        return holded_accounts, "holded"

    return [], "desconocido"


def export_reconciliation_fallback(accounts, summary_ui):
    rows = []
    for acc in accounts:
        for m in getattr(acc, "movements", []) or []:
            rows.append({
                "Cuenta": getattr(acc, "codigo", ""),
                "Nombre": getattr(acc, "nombre", ""),
                "Fecha": getattr(m, "fecha", None),
                "Descripción": getattr(m, "descripcion", ""),
                "Documento": getattr(m, "documento", getattr(m, "asiento", "")),
                "Debe": round(float(getattr(m, "debe", 0.0) or 0.0), 2),
                "Haber": round(float(getattr(m, "haber", 0.0) or 0.0), 2),
                "Saldo": round(float(getattr(m, "saldo", 0.0) or 0.0), 2),
                "Lado": getattr(m, "side", ""),
                "Importe": round(float(getattr(m, "importe", 0.0) or 0.0), 2),
                "Estado": "Conciliado" if getattr(m, "match_id", None) is not None else "Pendiente",
            })

    buffer = BytesIO()
    df_rows = pd.DataFrame(rows)
    df_summary = pd.DataFrame([
        ("Cuentas detectadas", summary_ui.get("total_accounts", 0)),
        ("Cuentas analizadas", summary_ui.get("analyzed_accounts", 0)),
        ("Cuentas excluidas", summary_ui.get("excluded_accounts", 0)),
        ("Movimientos analizados", summary_ui.get("analyzed_movements", 0)),
        ("Movimientos conciliados", summary_ui.get("total_matched", 0)),
        ("Movimientos pendientes", summary_ui.get("total_unmatched", 0)),
        ("Impacto pendiente", summary_ui.get("pending_amount", 0.0)),
        ("Tasa real", f"{summary_ui.get('match_rate', 0)}%"),
    ], columns=["Métrica", "Valor"])

    with pd.ExcelWriter(buffer, engine="xlsxwriter") as writer:
        df_summary.to_excel(writer, sheet_name="Resumen", index=False)
        df_rows.to_excel(writer, sheet_name="Movimientos", index=False)

        wb = writer.book
        header = wb.add_format({"bold": True, "bg_color": "#14444E", "font_color": "white", "border": 1})
        money = wb.add_format({"num_format": "#,##0.00"})
        green = wb.add_format({"bg_color": "#D9F0EC"})
        red = wb.add_format({"bg_color": "#F8D7D3"})

        for sheet_name, dfw in {"Resumen": df_summary, "Movimientos": df_rows}.items():
            ws = writer.sheets[sheet_name]
            for c, col in enumerate(dfw.columns):
                ws.write(0, c, col, header)
                ws.set_column(c, c, min(max(len(str(col)) + 4, 14), 40))
            for col in dfw.columns:
                if col in {"Debe", "Haber", "Saldo", "Importe", "Valor"}:
                    idx = dfw.columns.get_loc(col)
                    ws.set_column(idx, idx, 14, money)
            if sheet_name == "Movimientos" and not dfw.empty and "Estado" in dfw.columns:
                estado_idx = dfw.columns.get_loc("Estado")
                for r in range(1, len(dfw) + 1):
                    estado = str(dfw.iloc[r - 1, estado_idx])
                    ws.set_row(r, 22, green if estado == "Conciliado" else red)
            ws.freeze_panes(1, 0)

    buffer.seek(0)
    return buffer

st.markdown(
    """
<style>
:root{
    --bg:#07111f; --card:#111c2e; --card-2:#14243b; --border:rgba(148,163,184,.14); --text:#e5edf6;
    --muted:#93a4b8; --primary:#22c55e; --secondary:#06b6d4; --warning:#f59e0b; --danger:#ef4444;
    --shadow:0 14px 32px rgba(0,0,0,.28);
}
html, body, [data-testid="stAppViewContainer"], .stApp{background:linear-gradient(180deg,#07111f 0%, #091523 100%) !important;color:var(--text);}
[data-testid="stHeader"], header[data-testid="stHeader"]{background:transparent !important;height:0 !important;border:none !important;}
[data-testid="stToolbar"], .stAppDeployButton, [data-testid="stDecoration"]{display:none !important;}
.block-container{max-width:1450px;padding-top:0.2rem;padding-bottom:2rem;}
[data-testid="stSidebar"]{background:linear-gradient(180deg,#08121f 0%, #0c1726 100%);border-right:1px solid var(--border);} [data-testid="stSidebar"] *{color:var(--text) !important;}
.cc-header{display:flex;justify-content:space-between;align-items:center;gap:18px;background:linear-gradient(135deg,#081422 0%, #0e2138 55%, #12304d 100%);border:1px solid var(--border);border-radius:22px;padding:18px 22px;box-shadow:var(--shadow);margin-bottom:16px;}
.cc-brand{display:flex;flex-direction:column;gap:4px;} .cc-title{font-size:32px;font-weight:800;letter-spacing:.3px;color:#f8fafc;} .cc-sub{font-size:13px;color:var(--muted);} .cc-pill{display:inline-flex;align-items:center;gap:8px;padding:8px 14px;border-radius:999px;background:rgba(6,182,212,.10);border:1px solid rgba(6,182,212,.25);color:#d9fbff;font-size:12px;font-weight:600;}
.metric-grid{display:grid;grid-template-columns:repeat(4, minmax(0,1fr));gap:14px;margin:12px 0 16px;} .metric-card{background:linear-gradient(180deg,var(--card) 0%, var(--card-2) 100%);border:1px solid var(--border);border-radius:18px;padding:18px 18px 16px;box-shadow:var(--shadow);} .metric-label{color:var(--muted);font-size:12px;text-transform:uppercase;letter-spacing:.08em;margin-bottom:8px;} .metric-value{color:#f8fafc;font-size:30px;font-weight:800;line-height:1.05;} .metric-green{color:var(--primary);} .metric-cyan{color:var(--secondary);} .metric-orange{color:var(--warning);} .metric-red{color:var(--danger);}
.kpi-strip{display:grid;grid-template-columns:repeat(4, minmax(0,1fr));gap:12px;margin:0 0 14px;} .kpi-mini{background:rgba(255,255,255,.03);border:1px solid var(--border);border-radius:14px;padding:12px 14px;min-height:76px;} .kpi-mini .label{font-size:11px;color:var(--muted);text-transform:uppercase;letter-spacing:.08em;} .kpi-mini .value{font-size:18px;font-weight:800;color:#f8fafc;margin-top:6px;} .kpi-mini .value.small{font-size:13px;line-height:1.35;font-weight:600;color:#dbe7f6;}
.stTextInput > div > div > input{background:#0c1728 !important;color:#e5edf6 !important;border:1px solid rgba(148,163,184,.18) !important;border-radius:12px !important;}
.stDownloadButton > button{width:100%;background:linear-gradient(135deg,#22c55e 0%, #06b6d4 100%);color:white;border:none;border-radius:14px;padding:12px 18px;font-weight:700;}
.stTabs [data-baseweb="tab-list"]{gap:8px;} .stTabs [data-baseweb="tab"]{background:rgba(255,255,255,.02);border:1px solid var(--border);border-radius:12px 12px 0 0;color:var(--text);padding:10px 14px;} .stTabs [aria-selected="true"]{background:linear-gradient(180deg,#10213a 0%, #163153 100%) !important;}
.alert-card{border:1px solid var(--border);border-left:4px solid #64748b;background:linear-gradient(180deg,#10202f 0%, #14253a 100%);padding:11px 14px;border-radius:14px;margin-bottom:10px;} .alert-critica{border-left-color:#ef4444;} .alert-alta{border-left-color:#f59e0b;} .alert-media{border-left-color:#06b6d4;} .alert-title{font-weight:700;color:#f8fafc;font-size:13px;margin-bottom:4px;} .alert-detail{color:#d7e3f1;font-size:13px;} .alert-reason{color:#93a4b8;font-size:12px;margin-top:6px;}
.table-wrap{background:linear-gradient(180deg,#0d1728 0%, #111f34 100%);border:1px solid var(--border);border-radius:18px;overflow:auto;max-height:460px;box-shadow:var(--shadow);} .table-wrap table{width:100%;border-collapse:collapse;font-size:13px;background:transparent;color:#e5edf6;} .table-wrap thead th{position:sticky;top:0;background:#10213a;color:#f8fafc;text-align:left;padding:10px 12px;border-bottom:1px solid var(--border);z-index:2;} .table-wrap tbody td{padding:9px 12px;border-bottom:1px solid rgba(148,163,184,.10);vertical-align:top;background:rgba(0,0,0,.06);} .table-wrap tbody tr:nth-child(even) td{background:rgba(255,255,255,.02);} .table-empty{padding:18px;border:1px dashed var(--border);border-radius:16px;color:var(--muted);background:#0d1728;}
@media (max-width:1100px){.metric-grid,.kpi-strip{grid-template-columns:repeat(2,minmax(0,1fr));}.cc-header{flex-direction:column;align-items:flex-start;}}
</style>
""",
    unsafe_allow_html=True,
)


def save_uploaded_to_temp(uploaded_file):
    suffix = Path(uploaded_file.name).suffix
    tmp = tempfile.NamedTemporaryFile(delete=False, suffix=suffix)
    tmp.write(uploaded_file.getvalue())
    tmp.flush()
    tmp.close()
    return tmp.name


def filter_accounts_for_analysis(accounts):
    filtered, excluded = [], []
    for acc in accounts:
        code = str(getattr(acc, "codigo", "") or "")
        if is_excluded_account(code):
            excluded.append(code)
            continue
        filtered.append(acc)
    return filtered, excluded


def build_single_summary(accounts, excluded_codes):
    analyzed_accounts = len(accounts)
    analyzed_movements = sum(len(getattr(acc, "movements", []) or []) for acc in accounts)
    total_matched = 0
    total_unmatched_amount = 0.0
    for acc in accounts:
        for m in getattr(acc, "movements", []) or []:
            if getattr(m, "match_id", None) is not None:
                total_matched += 1
            else:
                total_unmatched_amount += float(getattr(m, "importe", 0.0) or 0.0)
    total_unmatched = max(analyzed_movements - total_matched, 0)
    match_rate = round((total_matched / analyzed_movements) * 100, 1) if analyzed_movements else 0.0
    return {
        "total_accounts": analyzed_accounts + len(excluded_codes),
        "analyzed_accounts": analyzed_accounts,
        "excluded_accounts": len(excluded_codes),
        "analyzed_movements": analyzed_movements,
        "total_matched": total_matched,
        "total_unmatched": total_unmatched,
        "match_rate": match_rate,
        "pending_amount": round(total_unmatched_amount, 2),
    }


def process_single_file(uploaded_file):
    tmp_path = save_uploaded_to_temp(uploaded_file)
    try:
        accounts, parser_used = load_accounts_auto(tmp_path)
        accounts, excluded_codes = filter_accounts_for_analysis(accounts)
        all_matches = []
        for acc in accounts:
            if not getattr(acc, "movements", None):
                continue
            try:
                all_matches.extend(reconcile_account(acc, tolerance=0.01, max_combo=5))
            except Exception:
                # Si el motor base no soporta el objeto importado, dejamos la cuenta sin romper la sesión.
                continue
        post_analysis(accounts)
        try:
            post_analysis(accounts)
        except Exception:
            pass
        summary_ui = build_single_summary(accounts, excluded_codes)
        summary_ui["parser_used"] = parser_used
        try:
            summary_export = generate_summary(accounts, all_matches)
            excel_buffer = export_reconciliation(accounts, all_matches, summary_export)
        except Exception:
            excel_buffer = export_reconciliation_fallback(accounts, summary_ui)
        return accounts, summary_ui, excel_buffer
    finally:
        try:
            os.unlink(tmp_path)
        except PermissionError:
            pass


def process_for_compare(uploaded_file):
    tmp_path = save_uploaded_to_temp(uploaded_file)
    try:
        accounts, _parser_used = load_accounts_auto(tmp_path)
        accounts, _excluded = filter_accounts_for_analysis(accounts)
        for acc in accounts:
            if not getattr(acc, "movements", None):
                continue
            try:
                reconcile_account(acc, tolerance=0.01, max_combo=5)
            except Exception:
                continue
        post_analysis(accounts)
        try:
            post_analysis(accounts)
        except Exception:
            pass
        return accounts
    finally:
        try:
            os.unlink(tmp_path)
        except PermissionError:
            pass



def process_special_file(uploaded_file):
    tmp_path = save_uploaded_to_temp(uploaded_file)
    try:
        # En análisis especial cargamos TODO el mayor sin aplicar exclusiones.
        accounts, parser_used = load_accounts_auto(tmp_path)
        return accounts, parser_used
    finally:
        try:
            os.unlink(tmp_path)
        except PermissionError:
            pass


def _same_day_close_special(d1, d2, max_days=20):
    try:
        dt1 = pd.to_datetime(d1)
        dt2 = pd.to_datetime(d2)
        return abs((dt1 - dt2).days) <= max_days
    except Exception:
        return False


_CUENTAS_OBJETIVO_555 = ("400", "4100", "170", "520", "523", "465", "1700")


def _reconcile_555_internal(acc_555):
    """
    Conciliación interna de la cuenta 555: debe vs haber, 1:1 y 1:N.
    Devuelve dict {id(mov): match_group_id} para todos los movimientos casados.
    Rápido: índice hash por importe.
    """
    movs = getattr(acc_555, "movements", []) or []
    debe = [m for m in movs if str(getattr(m, "side", "")).lower() == "debe"]
    haber = [m for m in movs if str(getattr(m, "side", "")).lower() == "haber"]

    matched = {}   # id(mov) -> group_id
    used_d = set()
    used_h = set()
    gid = [0]

    def next_gid():
        gid[0] += 1
        return gid[0]

    # Índice haber por importe redondeado
    haber_idx = {}
    for h in haber:
        k = round(float(getattr(h, "importe", 0.0) or 0.0), 2)
        haber_idx.setdefault(k, []).append(h)

    # 1:1
    for d in debe:
        if id(d) in used_d:
            continue
        k = round(float(getattr(d, "importe", 0.0) or 0.0), 2)
        for h in haber_idx.get(k, []):
            if id(h) in used_h:
                continue
            g = next_gid()
            matched[id(d)] = g
            matched[id(h)] = g
            used_d.add(id(d))
            used_h.add(id(h))
            break

    # 1:N (debe → N haberes)
    for d in debe:
        if id(d) in used_d:
            continue
        target = round(float(getattr(d, "importe", 0.0) or 0.0), 2)
        avail_h = [h for h in haber if id(h) not in used_h]
        acc_sum, acc_group = 0.0, []
        for h in sorted(avail_h, key=lambda x: getattr(x, "importe", 0.0), reverse=True):
            imp = round(float(getattr(h, "importe", 0.0) or 0.0), 2)
            if acc_sum + imp <= target + 0.01:
                acc_group.append(h)
                acc_sum += imp
            if abs(acc_sum - target) <= 0.01:
                g = next_gid()
                matched[id(d)] = g
                used_d.add(id(d))
                for h2 in acc_group:
                    matched[id(h2)] = g
                    used_h.add(id(h2))
                break

    # N:1 (N debes → haber)
    for h in haber:
        if id(h) in used_h:
            continue
        target = round(float(getattr(h, "importe", 0.0) or 0.0), 2)
        avail_d = [d for d in debe if id(d) not in used_d]
        acc_sum, acc_group = 0.0, []
        for d in sorted(avail_d, key=lambda x: getattr(x, "importe", 0.0), reverse=True):
            imp = round(float(getattr(d, "importe", 0.0) or 0.0), 2)
            if acc_sum + imp <= target + 0.01:
                acc_group.append(d)
                acc_sum += imp
            if abs(acc_sum - target) <= 0.01:
                g = next_gid()
                matched[id(h)] = g
                used_h.add(id(h))
                for d2 in acc_group:
                    matched[id(d2)] = g
                    used_d.add(id(d2))
                break

    return matched


def _build_ref_index_555(accounts):
    """
    Construye índice hash {(importe_redondeado, side): [(code, name, mov), ...]}
    para las cuentas objetivo. O(n) — sin bucles cuadráticos.
    """
    idx = {}
    for acc in accounts:
        code = str(getattr(acc, "codigo", "") or "")
        if not code.startswith(_CUENTAS_OBJETIVO_555):
            continue
        name = str(getattr(acc, "nombre", "") or "")
        for m in getattr(acc, "movements", []) or []:
            imp = round(float(getattr(m, "importe", 0.0) or 0.0), 2)
            side = str(getattr(m, "side", "") or "").lower().strip()
            idx.setdefault((imp, side), []).append((code, name, m))
    return idx


def _extract_keywords(text: str) -> set:
    """
    Extrae tokens significativos de una descripción para cruce semántico.
    Prioriza: números de referencia, códigos de préstamo, palabras clave contables.
    """
    if not text:
        return set()
    t = str(text).upper()
    tokens = set()

    # Números de referencia largos (≥6 dígitos) → identificadores de operación
    for num in re.findall(r'\d{6,}', t):
        tokens.add(num)

    # Palabras clave contables relevantes (≥4 chars, excluye stopwords)
    stopwords = {
        "DEBE", "HABER", "TOTAL", "SALDO", "APERTURA", "EJERCICIO",
        "FRA", "FACT", "FACTURA", "ASIENTO", "FECHA", "CUENTA",
        "DEL", "LOS", "LAS", "UNA", "PARA", "CON", "POR", "SIN",
        "NaN", "NONE", "NULL"
    }
    for word in re.findall(r'[A-ZÁÉÍÓÚÑ]{4,}', t):
        if word not in stopwords:
            tokens.add(word)

    return tokens


def _desc_cross_score(desc1: str, desc2: str) -> tuple[float, str]:
    """
    Devuelve (score 0-1, motivo legible) para el cruce semántico entre dos descripciones.
    score >= 0.4 → candidato válido.
    """
    kw1 = _extract_keywords(desc1)
    kw2 = _extract_keywords(desc2)
    if not kw1 or not kw2:
        return 0.0, ""

    common = kw1 & kw2
    if not common:
        return 0.0, ""

    # Números de referencia compartidos valen doble (son identificadores únicos)
    ref_common = {k for k in common if k.isdigit()}
    word_common = common - ref_common

    score = 0.0
    motivo_parts = []

    if ref_common:
        score += 0.6 * min(len(ref_common), 2) / 2
        motivo_parts.append(f"Ref. compartida: {', '.join(sorted(ref_common))}")

    if word_common:
        word_score = len(word_common) / max(len(kw1 - ref_common), len(kw2 - ref_common), 1)
        score += 0.4 * min(word_score, 1.0)
        motivo_parts.append(f"Palabras: {', '.join(sorted(word_common))}")

    return min(score, 1.0), " | ".join(motivo_parts)


# Prefijos donde se activa cruce semántico (préstamos, acreedores LP)
_PREFIXES_SEMANTIC = ("170", "1700", "520", "523")


def _build_555_cross_by_description(accounts):
    """
    Segunda pasada: cruce 555 ↔ cuentas objetivo por similitud de descripción.
    Solo para cuentas de préstamos/deudas LP donde el importe exacto no coincide
    (la cuota incluye capital + intereses; la 170x solo registra el principal).
    Ventana de fecha ampliada a 45 días.
    Devuelve lista de filas con columna 'Motivo cruce' y 'Diferencia importe'.
    """
    cuentas_555 = [a for a in accounts if str(getattr(a, "codigo", "") or "").startswith("555")]
    cuentas_semantica = [
        a for a in accounts
        if str(getattr(a, "codigo", "") or "").startswith(_PREFIXES_SEMANTIC)
    ]
    if not cuentas_555 or not cuentas_semantica:
        return []

    # Índice semántico: para cada movimiento de cuentas objetivo, sus keywords
    sem_index = []  # [(code, name, mov, keywords)]
    for acc in cuentas_semantica:
        code = str(getattr(acc, "codigo", "") or "")
        name = str(getattr(acc, "nombre", "") or "")
        for m in getattr(acc, "movements", []) or []:
            kw = _extract_keywords(getattr(m, "descripcion", "") or "")
            if kw:
                sem_index.append((code, name, m, kw))

    rows = []
    seen = set()  # evitar duplicados

    for acc in cuentas_555:
        code555 = str(getattr(acc, "codigo", "") or "")
        name555 = str(getattr(acc, "nombre", "") or "")
        for m in getattr(acc, "movements", []) or []:
            desc555 = getattr(m, "descripcion", "") or ""
            kw555 = _extract_keywords(desc555)
            if not kw555:
                continue
            imp555 = round(float(getattr(m, "importe", 0.0) or 0.0), 2)
            side555 = str(getattr(m, "side", "") or "").lower().strip()
            fecha555 = getattr(m, "fecha", None)

            for code2, name2, m2, kw2 in sem_index:
                # No cruzar mismo lado (préstamo: ambos en debe normalmente → buscar cualquier lado)
                score, motivo = _desc_cross_score(desc555, getattr(m2, "descripcion", "") or "")
                if score < 0.4:
                    continue
                if not _same_day_close_special(fecha555, getattr(m2, "fecha", None), max_days=45):
                    continue

                imp2 = round(float(getattr(m2, "importe", 0.0) or 0.0), 2)
                diferencia = round(imp555 - imp2, 2)

                # Dedup key
                dk = (code555, str(fecha555)[:10], imp555, code2, str(getattr(m2, "fecha", None))[:10], imp2)
                if dk in seen:
                    continue
                seen.add(dk)

                rows.append({
                    "Cuenta 555": code555,
                    "Nombre 555": name555,
                    "Fecha 555": fecha555,
                    "Descripción 555": desc555,
                    "Asiento 555": getattr(m, "asiento", ""),
                    "Importe 555": imp555,
                    "Lado 555": side555,
                    "Posible cuenta": code2,
                    "Nombre posible": name2,
                    "Fecha posible": getattr(m2, "fecha", None),
                    "Descripción posible": getattr(m2, "descripcion", "") or "",
                    "Asiento posible": getattr(m2, "asiento", ""),
                    "Importe posible": imp2,
                    "Diferencia importe": diferencia,
                    "Score": round(score, 2),
                    "Motivo cruce": motivo,
                })

    # Ordenar por score descendente
    rows.sort(key=lambda x: x["Score"], reverse=True)
    return rows


def _build_555_analysis_rows(accounts):
    """
    Cruces externos 555 ↔ cuentas objetivo.
    Pasada 1: por importe exacto (todos los prefijos objetivo).
    Pasada 2: por descripción semántica (prefijos de préstamos/deudas LP).
    Devuelve (df_exacto, df_semantico).
    """
    cuentas_555 = [a for a in accounts if str(getattr(a, "codigo", "") or "").startswith("555")]
    ref_index = _build_ref_index_555(accounts)

    rows = []
    for acc in cuentas_555:
        code555 = str(getattr(acc, "codigo", "") or "")
        name555 = str(getattr(acc, "nombre", "") or "")
        for m in getattr(acc, "movements", []) or []:
            importe = round(float(getattr(m, "importe", 0.0) or 0.0), 2)
            side = str(getattr(m, "side", "") or "").lower().strip()
            opposite_side = "haber" if side == "debe" else "debe"
            candidates = ref_index.get((importe, opposite_side), [])
            for code2, name2, m2 in candidates:
                if _same_day_close_special(getattr(m, "fecha", None), getattr(m2, "fecha", None), max_days=20):
                    rows.append({
                        "Cuenta 555": code555,
                        "Nombre 555": name555,
                        "Fecha 555": getattr(m, "fecha", None),
                        "Descripción 555": getattr(m, "descripcion", ""),
                        "Asiento 555": getattr(m, "asiento", ""),
                        "Importe": importe,
                        "Lado 555": side,
                        "Posible cuenta": code2,
                        "Nombre posible": name2,
                        "Fecha posible": getattr(m2, "fecha", None),
                        "Descripción posible": getattr(m2, "descripcion", ""),
                        "Asiento posible": getattr(m2, "asiento", ""),
                        "Lado posible": opposite_side,
                        "Motivo cruce": "Importe exacto",
                        "Diferencia importe": 0.0,
                        "Score": 1.0,
                    })

    df_exacto = pd.DataFrame(rows)
    if not df_exacto.empty:
        dedup_cols = [
            "Cuenta 555", "Fecha 555", "Importe", "Lado 555",
            "Posible cuenta", "Fecha posible",
            "Descripción 555", "Descripción posible"
        ]
        df_exacto = df_exacto.drop_duplicates(subset=dedup_cols).reset_index(drop=True)

    # Pasada 2: semántica
    sem_rows = _build_555_cross_by_description(accounts)
    df_sem = pd.DataFrame(sem_rows) if sem_rows else pd.DataFrame()

    # Normalizar columnas para que ambos df sean compatibles en UI
    # df_exacto tiene "Importe"; df_sem tiene "Importe 555" e "Importe posible"
    if not df_exacto.empty:
        df_exacto = df_exacto.rename(columns={"Importe": "Importe 555"})
        df_exacto["Importe posible"] = df_exacto["Importe 555"]

    return df_exacto, df_sem


def _norm_asset_name(name: str) -> str:
    """
    Normaliza nombre de activo/amortización para cruce semántico.
    Elimina prefijos de amortización y deja tokens alfa-num relevantes (>=3 chars).
    """
    import difflib as _dl
    t = str(name).upper()
    for pref in (
        "AMORT.ACUM", "AMORT. ACUM", "AMORTIZACION ACUMULADA", "AMORT.ACUMULADA",
        "AMORT ACUM", "AMORT.", "AMORTIZACION", "AMORT.ACUMULADAS",
        "AMORTIZACIONES ACUMULADAS", "AMORTIZACIONES",
    ):
        t = t.replace(pref, "")
    t = re.sub(r'[^A-ZÁÉÍÓÚÑ0-9 ]', ' ', t)
    tokens = [w for w in t.split() if len(w) >= 3]
    return " ".join(tokens)


def _asset_name_similarity(n1: str, n2: str) -> float:
    import difflib as _dl
    return _dl.SequenceMatcher(None, _norm_asset_name(n1), _norm_asset_name(n2)).ratio()


def _match_amort_for_activo(code_activo: str, name_activo: str, amortizaciones: list) -> tuple:
    """
    Intenta encontrar la cuenta de amortización correspondiente a un activo.
    Nivel 1: sufijo exacto (código[2:] == amort[2:])  — rápido, para numeraciones simples
    Nivel 2: últimos 7 dígitos compartidos            — resuelve 215000007 vs 281500007
    Nivel 3: similitud de nombre normalizado >= 0.40  — fallback semántico
    Devuelve (am_acc, metodo_cruce) o (None, None).
    """
    code_a = str(code_activo)
    suffix_a = code_a[2:] if len(code_a) > 2 else code_a
    tail_a = code_a[-7:] if len(code_a) >= 7 else code_a

    # Nivel 1: sufijo idéntico
    for am in amortizaciones:
        code_m = str(getattr(am, "codigo", ""))
        suffix_m = code_m[2:] if len(code_m) > 2 else code_m
        if suffix_m == suffix_a:
            return am, "sufijo"

    # Nivel 2: últimos 7 dígitos
    for am in amortizaciones:
        code_m = str(getattr(am, "codigo", ""))
        tail_m = code_m[-7:] if len(code_m) >= 7 else code_m
        if tail_m == tail_a and tail_a.isdigit():
            return am, "sufijo-7"

    # Nivel 3: similitud de nombre
    best_score, best_am = 0.0, None
    for am in amortizaciones:
        sc = _asset_name_similarity(name_activo, getattr(am, "nombre", ""))
        if sc > best_score:
            best_score, best_am = sc, am
    if best_score >= 0.40 and best_am is not None:
        return best_am, f"nombre({best_score:.0%})"

    return None, None


def _build_assets_analysis_rows(accounts):
    """
    Análisis de activos y amortizaciones:
    - Detecta activos (2xx excluyendo 28xx y 29xx)
    - Cruza con amortizaciones (28xx) por sufijo exacto → últimos 7 dígitos → nombre
    - Evalúa si la amortización es completa, parcial o inexistente
    - Detecta amortizaciones huérfanas (28xx sin su 2xx en el mayor)
    - Cruza con cuenta 681 (dotaciones del ejercicio)
    """
    activos = [a for a in accounts if
               str(getattr(a, "codigo", "") or "").startswith("2") and
               not str(getattr(a, "codigo", "") or "").startswith("28") and
               not str(getattr(a, "codigo", "") or "").startswith("29")]
    amortizaciones = [a for a in accounts if str(getattr(a, "codigo", "") or "").startswith("28")]
    cuentas_681 = [a for a in accounts if str(getattr(a, "codigo", "") or "").startswith("681")]

    # Índice 681 por nombre normalizado
    idx_681_by_name = {}
    for acc681 in cuentas_681:
        norm = _norm_asset_name(getattr(acc681, "nombre", ""))
        idx_681_by_name[norm] = acc681

    def _find_681(name_activo: str):
        """Busca la cuenta 681 más similar al nombre del activo."""
        norm_a = _norm_asset_name(name_activo)
        best_score, best_acc = 0.0, None
        for norm_m, acc681 in idx_681_by_name.items():
            sc = _asset_name_similarity(name_activo, getattr(acc681, "nombre", ""))
            if sc > best_score:
                best_score, best_acc = sc, acc681
        if best_score >= 0.35 and best_acc is not None:
            dot = round(abs(float(getattr(best_acc, "haber_total", 0.0) or
                                  float(getattr(best_acc, "debe_total", 0.0) or 0.0))), 2)
            return best_acc, dot
        return None, 0.0

    # Conjuntos para detectar huérfanas
    matched_amort_ids = set()

    rows = []
    for acc in activos:
        code = str(getattr(acc, "codigo", "") or "")
        name_a = str(getattr(acc, "nombre", "") or "")
        saldo_activo = round(float(getattr(acc, "saldo", 0.0) or 0.0), 2)

        am_acc, metodo = _match_amort_for_activo(code, name_a, amortizaciones)

        if am_acc:
            matched_amort_ids.add(id(am_acc))
            code_am = str(getattr(am_acc, "codigo", ""))
            saldo_amort = round(abs(float(getattr(am_acc, "saldo", 0.0) or 0.0)), 2)
            ratio = round((saldo_amort / saldo_activo * 100), 1) if saldo_activo else 0.0
            if ratio >= 95:
                estado = "Amortizado"
            elif ratio > 0:
                estado = f"Parcial ({ratio}%)"
            else:
                estado = "Amortización a cero"
        else:
            code_am = "—"
            saldo_amort = 0.0
            ratio = 0.0
            estado = "Sin amortizar"

        # Cruce 681
        acc_681, dotacion_681 = _find_681(name_a)
        code_681 = str(getattr(acc_681, "codigo", "")) if acc_681 else "—"
        nombre_681 = str(getattr(acc_681, "nombre", "")) if acc_681 else "—"
        if acc_681:
            estado_681 = "✅ Dotada" if dotacion_681 > 0 else "⚠️ Cuenta existe, dotación=0"
        else:
            estado_681 = "❌ Sin dotación 681"

        rows.append({
            "Cuenta activo": code,
            "Nombre activo": name_a,
            "Saldo activo": saldo_activo,
            "Cuenta amort.": code_am if am_acc else "—",
            "Nombre amort.": getattr(am_acc, "nombre", "") if am_acc else "—",
            "Saldo amort.": saldo_amort,
            "% Amortizado": ratio,
            "Método cruce": metodo or "—",
            "Estado": estado,
            "Cuenta 681": code_681,
            "Nombre 681": nombre_681,
            "Dotación ejercicio": dotacion_681,
            "Estado dotación": estado_681,
            "Movimientos activo": len(getattr(acc, "movements", []) or []),
        })

    # Amortizaciones huérfanas (28xx no cruzadas con ningún activo)
    huerfanas = []
    for am in amortizaciones:
        if id(am) in matched_amort_ids:
            continue
        code_am = str(getattr(am, "codigo", ""))
        huerfanas.append({
            "Cuenta activo": "—",
            "Nombre activo": "Sin activo asociado",
            "Saldo activo": 0.0,
            "Cuenta amort.": code_am,
            "Nombre amort.": getattr(am, "nombre", ""),
            "Saldo amort.": round(abs(float(getattr(am, "saldo", 0.0) or 0.0)), 2),
            "% Amortizado": 0.0,
            "Método cruce": "—",
            "Estado": "Amort. huérfana",
            "Cuenta 681": "—",
            "Nombre 681": "—",
            "Dotación ejercicio": 0.0,
            "Estado dotación": "—",
            "Movimientos activo": 0,
        })

    return pd.DataFrame(rows + huerfanas)


def _export_special_555_excel(accounts, df_555, df_sem=None):
    """
    Excel análisis 555 con:
    - Hoja 'Mayor_555': mayor completo, internamente conciliados en verde,
      candidatos externos (exacto + semántico) en amarillo.
    - Hoja 'Mayor_Destino': mayores de cuentas objetivo.
    - Hoja 'Conciliados_555': movimientos casados internamente.
    - Hoja 'Posibles_Cruces': cruces por importe exacto.
    - Hoja 'Cruces_Semanticos': cruces por referencia/descripción (préstamos).
    - Hoja 'Resumen'.
    """
    if df_sem is None:
        df_sem = pd.DataFrame()
    output = BytesIO()

    # --- Construir conjuntos para lookup rápido ---
    internal_matches = {}
    for acc in accounts:
        code = str(getattr(acc, "codigo", "") or "")
        if code.startswith("555"):
            internal_matches[code] = _reconcile_555_internal(acc)

    # Combinar exacto + semántico para lookup de color en el mayor
    external_lookup = set()
    external_detail = {}
    for df_src in [df_555, df_sem]:
        if df_src is None or df_src.empty:
            continue
        imp_col = "Importe 555" if "Importe 555" in df_src.columns else "Importe"
        for _, row in df_src.iterrows():
            k = (str(row.get("Cuenta 555", "")),
                 round(float(row.get(imp_col, 0.0) or 0.0), 2),
                 str(row.get("Lado 555", "")).lower())
            external_lookup.add(k)
            motivo = str(row.get("Motivo cruce", "Importe exacto"))
            dest = f"{row.get('Posible cuenta', '')} - {row.get('Nombre posible', '')} [{motivo}]"
            external_detail[k] = dest

    external_dest_lookup = set()
    external_dest_detail = {}
    for df_src in [df_555, df_sem]:
        if df_src is None or df_src.empty:
            continue
        imp_col = "Importe posible" if "Importe posible" in df_src.columns else "Importe"
        lado_col = "Lado posible" if "Lado posible" in df_src.columns else "Lado 555"
        for _, row in df_src.iterrows():
            k = (str(row.get("Posible cuenta", "")),
                 round(float(row.get(imp_col, 0.0) or 0.0), 2),
                 str(row.get(lado_col, "")).lower())
            external_dest_lookup.add(k)
            dest = f"{row.get('Cuenta 555', '')} - {row.get('Nombre 555', '')}"
            external_dest_detail[k] = dest

    with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
        wb = writer.book

        # --- Formatos ---
        fmt_header = wb.add_format({"bold": True, "bg_color": "#14444E", "font_color": "white",
                                    "border": 1, "align": "center", "valign": "vcenter"})
        fmt_acc_hdr = wb.add_format({"bold": True, "bg_color": "#D9D9D9", "border": 1,
                                     "font_size": 11})
        fmt_green = wb.add_format({"bg_color": "#D9F0EC", "border": 1, "num_format": "#,##0.00"})
        fmt_green_txt = wb.add_format({"bg_color": "#D9F0EC", "border": 1})
        fmt_yellow = wb.add_format({"bg_color": "#FFF3CD", "border": 1, "num_format": "#,##0.00"})
        fmt_yellow_txt = wb.add_format({"bg_color": "#FFF3CD", "border": 1})
        fmt_pending = wb.add_format({"bg_color": "#F8D7D3", "border": 1, "num_format": "#,##0.00"})
        fmt_pending_txt = wb.add_format({"bg_color": "#F8D7D3", "border": 1})
        fmt_money = wb.add_format({"border": 1, "num_format": "#,##0.00"})
        fmt_normal = wb.add_format({"border": 1})
        fmt_date = wb.add_format({"border": 1, "num_format": "dd/mm/yyyy"})
        fmt_green_date = wb.add_format({"bg_color": "#D9F0EC", "border": 1, "num_format": "dd/mm/yyyy"})
        fmt_yellow_date = wb.add_format({"bg_color": "#FFF3CD", "border": 1, "num_format": "dd/mm/yyyy"})
        fmt_pending_date = wb.add_format({"bg_color": "#F8D7D3", "border": 1, "num_format": "dd/mm/yyyy"})
        fmt_sum = wb.add_format({"bold": True, "bg_color": "#D9D9D9", "border": 1,
                                  "num_format": "#,##0.00"})

        COLS_555 = ["Fecha", "Descripción", "Asiento", "Contrapartida",
                    "Debe", "Haber", "Estado", "Grupo Match", "Posible cruce con"]
        COL_W_555 = [12, 38, 12, 14, 14, 14, 16, 12, 36]

        def _write_mayor_sheet(ws, accounts_to_render, internal_match_map,
                               ext_lookup, ext_detail, sheet_label):
            """Escribe el mayor de una o varias cuentas en la hoja ws."""
            row = 0
            # Cabecera columnas
            for c, (h, w) in enumerate(zip(COLS_555, COL_W_555)):
                ws.write(row, c, h, fmt_header)
                ws.set_column(c, c, w)
            ws.freeze_panes(1, 0)
            row += 1

            for acc in accounts_to_render:
                code = str(getattr(acc, "codigo", "") or "")
                name = str(getattr(acc, "nombre", "") or "")
                im = internal_match_map.get(code, {})

                # Cabecera cuenta
                ws.merge_range(row, 0, row, len(COLS_555) - 1,
                                f"{code}  –  {name}", fmt_acc_hdr)
                row += 1

                sum_debe, sum_haber = 0.0, 0.0
                for m in getattr(acc, "movements", []) or []:
                    imp = round(float(getattr(m, "importe", 0.0) or 0.0), 2)
                    side = str(getattr(m, "side", "") or "").lower()
                    debe_v = imp if side == "debe" else None
                    haber_v = imp if side == "haber" else None
                    sum_debe += debe_v or 0.0
                    sum_haber += haber_v or 0.0

                    grp = im.get(id(m))
                    ext_k = (code, imp, side)
                    is_ext = ext_k in ext_lookup
                    is_int = grp is not None

                    if is_int:
                        estado = "Conciliado interno"
                        f_txt, f_num, f_dt = fmt_green_txt, fmt_green, fmt_green_date
                    elif is_ext:
                        estado = "Posible cruce externo"
                        f_txt, f_num, f_dt = fmt_yellow_txt, fmt_yellow, fmt_yellow_date
                    else:
                        estado = "Pendiente"
                        f_txt, f_num, f_dt = fmt_pending_txt, fmt_pending, fmt_pending_date

                    fecha_v = getattr(m, "fecha", None)
                    try:
                        fecha_v = pd.to_datetime(fecha_v)
                    except Exception:
                        fecha_v = None

                    ws.write(row, 0, fecha_v, f_dt if fecha_v else f_txt)
                    ws.write(row, 1, getattr(m, "descripcion", "") or "", f_txt)
                    ws.write(row, 2, getattr(m, "asiento", "") or "", f_txt)
                    ws.write(row, 3, getattr(m, "contrapartida", "") or "", f_txt)
                    ws.write(row, 4, debe_v, f_num)
                    ws.write(row, 5, haber_v, f_num)
                    ws.write(row, 6, estado, f_txt)
                    ws.write(row, 7, grp or "", f_txt)
                    ws.write(row, 8, ext_detail.get(ext_k, "") if is_ext else "", f_txt)
                    row += 1

                # Fila suma
                ws.write(row, 1, "Suma movimientos", fmt_sum)
                ws.write(row, 4, round(sum_debe, 2), fmt_sum)
                ws.write(row, 5, round(sum_haber, 2), fmt_sum)
                for c in [0, 2, 3, 6, 7, 8]:
                    ws.write(row, c, "", fmt_sum)
                row += 2

        # --- Hoja Mayor_555 ---
        cuentas_555 = [a for a in accounts if str(getattr(a, "codigo", "") or "").startswith("555")]
        ws555 = wb.add_worksheet("Mayor_555")
        _write_mayor_sheet(ws555, cuentas_555, internal_matches,
                           external_lookup, external_detail, "555")

        # --- Hoja Mayor_Destino ---
        cuentas_destino = [a for a in accounts
                           if str(getattr(a, "codigo", "") or "").startswith(_CUENTAS_OBJETIVO_555)]
        ws_dest = wb.add_worksheet("Mayor_Destino")
        _write_mayor_sheet(ws_dest, cuentas_destino, {},
                           external_dest_lookup, external_dest_detail, "destino")

        # --- Hoja Conciliados_555 ---
        conc_rows = []
        for acc in cuentas_555:
            code = str(getattr(acc, "codigo", "") or "")
            im = internal_matches.get(code, {})
            for m in getattr(acc, "movements", []) or []:
                grp = im.get(id(m))
                if grp is None:
                    continue
                imp = round(float(getattr(m, "importe", 0.0) or 0.0), 2)
                side = str(getattr(m, "side", "") or "").lower()
                conc_rows.append({
                    "Cuenta": code,
                    "Fecha": getattr(m, "fecha", None),
                    "Descripción": getattr(m, "descripcion", "") or "",
                    "Asiento": getattr(m, "asiento", "") or "",
                    "Debe": imp if side == "debe" else None,
                    "Haber": imp if side == "haber" else None,
                    "Grupo": grp,
                    "Tipo": "Conciliado interno",
                })
        df_conc = pd.DataFrame(conc_rows)
        df_conc.to_excel(writer, sheet_name="Conciliados_555", index=False)
        ws_conc = writer.sheets["Conciliados_555"]
        for c, col in enumerate(df_conc.columns):
            ws_conc.write(0, c, col, fmt_header)
            ws_conc.set_column(c, c, 18 if col in ("Descripción",) else 13)
        ws_conc.freeze_panes(1, 0)

        # --- Hoja Posibles_Cruces ---
        if not df_555.empty:
            df_555.to_excel(writer, sheet_name="Posibles_Cruces", index=False)
            ws_pos = writer.sheets["Posibles_Cruces"]
            for c, col in enumerate(df_555.columns):
                ws_pos.write(0, c, col, fmt_header)
                ws_pos.set_column(c, c, 20 if "Descripción" in str(col) or "Nombre" in str(col) else 13)
            ws_pos.freeze_panes(1, 0)
            fmt_row_yellow = wb.add_format({"bg_color": "#FFF3CD"})
            for r in range(1, len(df_555) + 1):
                ws_pos.set_row(r, 18, fmt_row_yellow)
        else:
            pd.DataFrame(columns=["Sin posibles cruces detectados"]).to_excel(
                writer, sheet_name="Posibles_Cruces", index=False)

        # --- Hoja Cruces_Semanticos (préstamos/refs) ---
        fmt_orange_row = wb.add_format({"bg_color": "#FAEADF"})
        if not df_sem.empty:
            df_sem.to_excel(writer, sheet_name="Cruces_Semanticos", index=False)
            ws_sem = writer.sheets["Cruces_Semanticos"]
            for c, col in enumerate(df_sem.columns):
                ws_sem.write(0, c, col, fmt_header)
                w = 36 if col in ("Descripción 555", "Descripción posible", "Motivo cruce") else \
                    20 if "Nombre" in str(col) else 14
                ws_sem.set_column(c, c, w)
                if col in ("Importe 555", "Importe posible", "Diferencia importe"):
                    ws_sem.set_column(c, c, 16,
                        wb.add_format({"bg_color": "#FAEADF", "num_format": "#,##0.00", "border": 1}))
            ws_sem.freeze_panes(1, 0)
            for r in range(1, len(df_sem) + 1):
                ws_sem.set_row(r, 20, fmt_orange_row)
        else:
            pd.DataFrame(columns=["Sin cruces semánticos detectados"]).to_excel(
                writer, sheet_name="Cruces_Semanticos", index=False)

        # --- Hoja Resumen ---
        n555 = len(cuentas_555)
        n_dest = len(cuentas_destino)
        total_int = sum(len(v) for v in internal_matches.values()) // 2
        resumen = pd.DataFrame([
            ("Cuentas 555 analizadas", n555),
            ("Cuentas objetivo revisadas", n_dest),
            ("Cuentas objetivo (prefijos)", ", ".join(_CUENTAS_OBJETIVO_555)),
            ("Movimientos conciliados internamente (555)", total_int),
            ("Cruces exactos (mismo importe)", len(df_555) if not df_555.empty else 0),
            ("Cruces semánticos (préstamos/refs)", len(df_sem) if not df_sem.empty else 0),
            ("", ""),
            ("Leyenda: Verde", "Conciliado internamente dentro de la 555"),
            ("Leyenda: Amarillo", "Candidato cruce exacto (400/4100/1700/...)"),
            ("Leyenda: Naranja", "Candidato cruce semántico por referencia (170x préstamos)"),
            ("Leyenda: Rojo", "Pendiente sin correspondencia detectada"),
            ("", ""),
            ("Hoja Posibles_Cruces", "Cruces por importe exacto"),
            ("Hoja Cruces_Semanticos", "Cruces por nº préstamo / referencia compartida (diferencia = intereses)"),
        ], columns=["Métrica", "Valor"])
        resumen.to_excel(writer, sheet_name="Resumen", index=False)
        ws_res = writer.sheets["Resumen"]
        for c, col in enumerate(resumen.columns):
            ws_res.write(0, c, col, fmt_header)
            ws_res.set_column(c, c, 52 if c == 0 else 32)
        ws_res.freeze_panes(1, 0)

    output.seek(0)
    return output


def _export_special_assets_excel(accounts, df_assets):
    output = BytesIO()

    amort_rows = []
    for acc in accounts:
        code = str(getattr(acc, "codigo", "") or "")
        if code.startswith("28"):
            amort_rows.append({
                "Cuenta amortización": code,
                "Nombre": getattr(acc, "nombre", ""),
                "Movimientos": len(getattr(acc, "movements", []) or []),
                "Debe total": getattr(acc, "debe_total", 0.0),
                "Haber total": getattr(acc, "haber_total", 0.0),
                "Saldo": getattr(acc, "saldo", 0.0),
            })
    df_amort = pd.DataFrame(amort_rows)

    df_681 = pd.DataFrame([
        {
            "Cuenta 681": str(getattr(acc, "codigo", "")),
            "Nombre 681": str(getattr(acc, "nombre", "")),
            "Movimientos": len(getattr(acc, "movements", []) or []),
            "Debe total": round(float(getattr(acc, "debe_total", 0.0) or 0.0), 2),
            "Haber total": round(float(getattr(acc, "haber_total", 0.0) or 0.0), 2),
            "Saldo": round(float(getattr(acc, "saldo", 0.0) or 0.0), 2),
        }
        for acc in accounts if str(getattr(acc, "codigo", "") or "").startswith("681")
    ])

    _est_col = "Estado" if "Estado" in (df_assets.columns if not df_assets.empty else []) else None
    df_sin = df_assets[df_assets["Estado"] == "Sin amortizar"].copy() if not df_assets.empty else pd.DataFrame()
    df_parcial = df_assets[df_assets["Estado"].str.startswith("Parcial", na=False)].copy() if not df_assets.empty else pd.DataFrame()
    df_ok = df_assets[df_assets["Estado"] == "Amortizado"].copy() if not df_assets.empty else pd.DataFrame()
    df_huerfanas = df_assets[df_assets["Estado"] == "Amort. huérfana"].copy() if not df_assets.empty else pd.DataFrame()
    df_sin_dot = df_assets[
        df_assets.get("Estado dotación", pd.Series(dtype=str)).str.startswith("❌", na=False)
    ].copy() if not df_assets.empty and "Estado dotación" in df_assets.columns else pd.DataFrame()

    with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
        df_assets.to_excel(writer, sheet_name="Activos", index=False)
        df_amort.to_excel(writer, sheet_name="Amortizaciones", index=False)
        df_sin.to_excel(writer, sheet_name="Sin_amortizar", index=False)
        df_parcial.to_excel(writer, sheet_name="Amort_parcial", index=False)
        df_ok.to_excel(writer, sheet_name="Amortizados_OK", index=False)
        if not df_huerfanas.empty:
            df_huerfanas.to_excel(writer, sheet_name="Amort_huerfanas", index=False)
        if not df_681.empty:
            df_681.to_excel(writer, sheet_name="Dotaciones_681", index=False)
        if not df_sin_dot.empty:
            df_sin_dot.to_excel(writer, sheet_name="Sin_dotacion_681", index=False)

        wb = writer.book
        fmt_header = wb.add_format({"bold": True, "bg_color": "#14444E", "font_color": "white",
                                    "border": 1, "align": "center"})
        fmt_money = wb.add_format({"num_format": "#,##0.00", "border": 1})
        fmt_red = wb.add_format({"bg_color": "#F8D7D3", "border": 1})
        fmt_orange = wb.add_format({"bg_color": "#FAEADF", "border": 1})
        fmt_green = wb.add_format({"bg_color": "#D9F0EC", "border": 1})
        fmt_yellow = wb.add_format({"bg_color": "#FFF3CD", "border": 1})
        fmt_blue = wb.add_format({"bg_color": "#DBEAFE", "border": 1})

        money_cols = {"Saldo activo", "Saldo amort.", "Debe total", "Haber total",
                      "Saldo", "Dotación ejercicio"}
        wide_cols = {"Nombre activo", "Nombre amort.", "Nombre 681", "Estado dotación",
                     "Método cruce", "Nombre"}

        color_by_estado = {
            "Amortizado": fmt_green,
            "Sin amortizar": fmt_red,
            "Amort. huérfana": fmt_yellow,
            "Amortización a cero": fmt_orange,
        }

        sheet_map = {
            "Activos": df_assets,
            "Amortizaciones": df_amort,
            "Sin_amortizar": df_sin,
            "Amort_parcial": df_parcial,
            "Amortizados_OK": df_ok,
        }
        if not df_huerfanas.empty:
            sheet_map["Amort_huerfanas"] = df_huerfanas
        if not df_681.empty:
            sheet_map["Dotaciones_681"] = df_681
        if not df_sin_dot.empty:
            sheet_map["Sin_dotacion_681"] = df_sin_dot

        for sheet_name, dfw in sheet_map.items():
            if dfw is None or dfw.empty:
                continue
            ws = writer.sheets[sheet_name]
            for c, col in enumerate(dfw.columns):
                ws.write(0, c, col, fmt_header)
                if col in money_cols:
                    ws.set_column(c, c, 16, fmt_money)
                elif col in wide_cols:
                    ws.set_column(c, c, 36)
                else:
                    ws.set_column(c, c, min(max(len(str(col)) + 4, 12), 22))

            if sheet_name == "Activos" and "Estado" in dfw.columns:
                estado_idx = dfw.columns.get_loc("Estado")
                dot_idx = dfw.columns.get_loc("Estado dotación") if "Estado dotación" in dfw.columns else None
                for r in range(1, len(dfw) + 1):
                    estado = str(dfw.iloc[r - 1, estado_idx])
                    fmt = color_by_estado.get(estado, fmt_orange if "Parcial" in estado else None)
                    if fmt:
                        ws.set_row(r, 22, fmt)
                    # Colorear celda 681 en azul si sin dotación
                    if dot_idx is not None:
                        dot_val = str(dfw.iloc[r - 1, dot_idx])
                        if dot_val.startswith("❌"):
                            ws.write(r, dot_idx, dot_val, fmt_blue)

            ws.freeze_panes(1, 0)

    output.seek(0)
    return output


def render_brand(mode_name):
    st.markdown(
        f"""
        <div class="cc-header">
            <div class="cc-brand">
                <div class="cc-title">C-Control</div>
                <div class="cc-sub">{mode_name} · Automatización y control contable basado en datos</div>
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )




def _display_parser_name(raw_name: str) -> str:
    parser_raw = str(raw_name or "desconocido").strip().lower()
    parser_display_map = {
        "estándar": "SAGE",
        "estandar": "SAGE",
        "holded": "HOLDED",
        "a3": "A3",
        "desconocido": "DESCONOCIDO",
    }
    return parser_display_map.get(parser_raw, parser_raw.upper())


def render_metrics_single(summary):
    parser_name = _display_parser_name(summary.get("parser_used", "desconocido"))
    st.markdown(
        f"""
        <div class="metric-grid">
            <div class="metric-card"><div class="metric-label">Cuentas detectadas en fichero</div><div class="metric-value">{summary.get('total_accounts', 0):,}</div></div>
            <div class="metric-card"><div class="metric-label">Cuentas válidas para análisis</div><div class="metric-value metric-cyan">{summary.get('analyzed_accounts', 0):,}</div></div>
            <div class="metric-card"><div class="metric-label">Movimientos conciliados</div><div class="metric-value metric-green">{summary.get('total_matched', 0):,}</div></div>
            <div class="metric-card"><div class="metric-label">Impacto pendiente</div><div class="metric-value metric-orange">{summary.get('pending_amount', 0.0):,.2f} €</div></div>
        </div>
        <div class="kpi-strip">
            <div class="kpi-mini"><div class="label">Formato detectado</div><div class="value">{parser_name}</div></div>
            <div class="kpi-mini"><div class="label">Cuentas excluidas por regla</div><div class="value">{summary.get('excluded_accounts', 0):,}</div></div>
            <div class="kpi-mini"><div class="label">Movimientos en cuentas válidas</div><div class="value">{summary.get('analyzed_movements', 0):,}</div></div>
            <div class="kpi-mini"><div class="label">Movimientos pendientes</div><div class="value">{summary.get('total_unmatched', 0):,}</div></div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def render_metrics_compare(s):
    st.markdown(
        f"""
        <div class="metric-grid">
            <div class="metric-card"><div class="metric-label">Persistentes</div><div class="metric-value">{s['persistentes']:,}</div></div>
            <div class="metric-card"><div class="metric-label">Nuevos</div><div class="metric-value metric-orange">{s['nuevos']:,}</div></div>
            <div class="metric-card"><div class="metric-label">Impacto total pendiente</div><div class="metric-value metric-red">{s.get('impacto_total_pendiente', 0.0):,.2f} €</div></div>
            <div class="metric-card"><div class="metric-label">Cuentas críticas</div><div class="metric-value metric-cyan">{s.get('cuentas_criticas', 0):,}</div></div>
        </div>
        """,
        unsafe_allow_html=True,
    )
    foco = html.escape(str(s.get('principal_foco_revision', '') or 'Sin foco principal detectado'))
    st.markdown(
        f"""
        <div class="kpi-strip">
            <div class="kpi-mini"><div class="label">Impacto persistentes</div><div class="value">{s.get('impacto_persistentes', 0.0):,.2f} €</div></div>
            <div class="kpi-mini"><div class="label">Impacto nuevos</div><div class="value">{s.get('impacto_nuevos', 0.0):,.2f} €</div></div>
            <div class="kpi-mini"><div class="label">Impacto crítico</div><div class="value">{s.get('impacto_critico', 0.0):,.2f} €</div></div>
            <div class="kpi-mini"><div class="label">Principal foco revisión</div><div class="value small">{foco}</div></div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def format_cell(value):
    if pd.isna(value):
        return ""
    if isinstance(value, float):
        return f"{value:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
    return html.escape(str(value))


def show_table(df: pd.DataFrame, height: int = 420):
    if df is None or df.empty:
        st.markdown('<div class="table-empty">Sin datos para mostrar.</div>', unsafe_allow_html=True)
        return
    header = "".join(f"<th>{html.escape(str(col))}</th>" for col in df.columns)
    rows = []
    for _, row in df.iterrows():
        rows.append("<tr>" + "".join(f"<td>{format_cell(v)}</td>" for v in row.tolist()) + "</tr>")
    table_html = f'<div class="table-wrap" style="max-height:{height}px;"><table><thead><tr>{header}</tr></thead><tbody>{"".join(rows)}</tbody></table></div>'
    st.markdown(table_html, unsafe_allow_html=True)


def apply_filter(df: pd.DataFrame, query: str) -> pd.DataFrame:
    if df is None or df.empty or not query:
        return df
    q = str(query).strip().upper()
    if not q:
        return df
    mask = df.astype(str).apply(lambda col: col.str.upper().str.contains(q, na=False))
    return df[mask.any(axis=1)].reset_index(drop=True)


def render_alerts(df: pd.DataFrame):
    if df is None or df.empty:
        return
    st.markdown("### Alertas")
    class_map = {"Crítica": "alert-critica", "Alta": "alert-alta", "Media": "alert-media", "Baja": ""}
    for _, row in df.head(8).iterrows():
        sev = str(row.get("Severidad", "Media"))
        css = class_map.get(sev, "")
        title = html.escape(f"{sev} · {row.get('Tipo Insight', '')}")
        detail = html.escape(str(row.get("Detalle", "")))
        reason = html.escape(str(row.get("Motivo Alerta", "")))
        st.markdown(f'<div class="alert-card {css}"><div class="alert-title">{title}</div><div class="alert-detail">{detail}</div><div class="alert-reason">{reason}</div></div>', unsafe_allow_html=True)



def reset_mode_state(current_mode: str):
    last_mode = st.session_state.get("_cc_last_mode")
    if last_mode is None:
        st.session_state["_cc_last_mode"] = current_mode
        return
    if last_mode != current_mode:
        for key in ["single", "prev", "curr", "analysis_special", "special_mode", "filtro_single", "filtro_compare"]:
            if key in st.session_state:
                del st.session_state[key]
        st.session_state["_cc_last_mode"] = current_mode
        st.rerun()


def main():
    with st.sidebar:
        st.markdown("## C-Control")
        st.caption("Conciliación automática y comparación de pendientes")
        mode = st.radio("Modo", ["Conciliación de mayor", "Comparación entre periodos", "Análisis especial"], label_visibility="collapsed")
        reset_mode_state(mode)
        st.markdown("---")
        st.markdown("### Reglas")
        st.caption("Excluidas: 570, 572, grupo 6, grupo 7, 102, 170, 215, 281 y 300.")
        st.caption("Entrada automática: formatos Sage, Holded y A3, sin mapeo manual.")
        st.caption("Paquete portable: usa siempre esta carpeta completa en cualquier ordenador.")

    if mode == "Conciliación de mayor":
        render_brand("Conciliación de mayor")

        c_up1, c_up2 = st.columns([4, 1])
        with c_up1:
            uploaded_file = st.file_uploader(
                "Sube tu fichero de mayores",
                type=["xlsx", "xls", "csv"],
                key="single"
            )
        with c_up2:
            st.write("")
            st.write("")
            if st.button("🔄 Nuevo", key="reset_single"):
                for k in ["single", "filtro_single"]:
                    if k in st.session_state:
                        del st.session_state[k]
                st.rerun()

        if not uploaded_file:
            return

        with st.spinner("Procesando mayor..."):
            accounts, summary, excel_buffer = process_single_file(uploaded_file)
        render_metrics_single(summary)
        st.download_button(
            "📥 Descargar conciliación (.xlsx)",
            data=excel_buffer,
            file_name=f"conciliacion_{uploaded_file.name.rsplit('.', 1)[0]}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )
        detail_filter = st.text_input("🔎 Filtrar por cuenta / nombre / descripción", key="filtro_single")
        tab1, tab2 = st.tabs(["Resumen", "Detalle cuentas"])
        with tab1:
            df = pd.DataFrame([
                ("Formato detectado", _display_parser_name(summary.get("parser_used", "desconocido"))),
                ("Cuentas detectadas en fichero", summary.get("total_accounts", 0)),
                ("Cuentas válidas para análisis", summary.get("analyzed_accounts", 0)),
                ("Cuentas excluidas por regla", summary.get("excluded_accounts", 0)),
                ("Movimientos en cuentas válidas", summary.get("analyzed_movements", 0)),
                ("Movimientos conciliados", summary.get("total_matched", 0)),
                ("Movimientos pendientes", summary.get("total_unmatched", 0)),
                ("Impacto pendiente", summary.get("pending_amount", 0.0)),
                ("Tasa real", f"{summary.get('match_rate', 0)}%"),
            ], columns=["Métrica", "Valor"])
            show_table(df, height=320)
        with tab2:
            rows = []
            for acc in accounts:
                if not getattr(acc, "movements", None):
                    continue
                matched = sum(1 for m in acc.movements if getattr(m, "match_id", None) is not None)
                unmatched = len(acc.movements) - matched
                debe_n = sum(1 for m in acc.movements if str(getattr(m, "side", "")).lower() == "debe")
                haber_n = sum(1 for m in acc.movements if str(getattr(m, "side", "")).lower() == "haber")
                rows.append({
                    "Cuenta": acc.codigo,
                    "Nombre": acc.nombre,
                    "Movimientos": len(acc.movements),
                    "Debe movs": debe_n,
                    "Haber movs": haber_n,
                    "Pendientes": unmatched,
                    "Conciliados": matched,
                    "Incidencias": sum(1 for m in acc.movements if getattr(m, "incidence_type", None)),
                    "Cuenta un solo lado": "Sí" if debe_n == 0 or haber_n == 0 else "No",
                    "Debe": getattr(acc, "debe_total", 0.0),
                    "Haber": getattr(acc, "haber_total", 0.0),
                    "Saldo": getattr(acc, "saldo", 0.0),
                })
            show_table(apply_filter(pd.DataFrame(rows), detail_filter), height=540)

    elif mode == "Comparación entre periodos":
        render_brand("Comparación entre periodos")

        c_reset1, c_reset2 = st.columns([4, 1])
        with c_reset2:
            st.write("")
            if st.button("🔄 Nuevo", key="reset_compare"):
                for k in ["prev", "curr", "filtro_compare"]:
                    if k in st.session_state:
                        del st.session_state[k]
                st.rerun()

        c1, c2 = st.columns(2)
        with c1:
            prev_file = st.file_uploader("Mayor anterior", type=["xlsx", "xls", "csv"], key="prev")
        with c2:
            curr_file = st.file_uploader("Mayor actual", type=["xlsx", "xls", "csv"], key="curr")
        if not prev_file or not curr_file:
            return

        with st.spinner("Conciliando y comparando periodos..."):
            prev_accounts = process_for_compare(prev_file)
            curr_accounts = process_for_compare(curr_file)
            if not prev_accounts or not curr_accounts:
                st.error("Uno de los ficheros no ha podido interpretarse con el parser actual. Ahora se soportan formatos Sage, Holded y A3.")
                return
            result = compare_accounts(prev_accounts, curr_accounts)
            excel_buffer = export_period_comparison(result, prev_file.name, curr_file.name)

        s = result["summary"]
        render_metrics_compare(s)
        render_alerts(result["insights"])

        st.download_button(
            "📥 Descargar comparación (.xlsx)",
            data=excel_buffer,
            file_name=f"comparacion_{Path(prev_file.name).stem}_vs_{Path(curr_file.name).stem}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )

        compare_filter = st.text_input("🔎 Filtrar por proveedor / cuenta / nombre / texto", key="filtro_compare")

        tabs = st.tabs(["Resumen ejecutivo", "Top cuentas", "Evolución cuentas", "Pendientes", "Agrupados", "Duplicados"])
        with tabs[0]:
            df = pd.DataFrame([
                ("Periodo anterior", prev_file.name),
                ("Periodo actual", curr_file.name),
                ("Principal foco revisión", s.get("principal_foco_revision", "")),
                ("Cuentas comparadas", s["cuentas_comparadas"]),
                ("Pendientes periodo anterior", s["movimientos_anterior"]),
                ("Pendientes periodo actual", s["movimientos_actual"]),
                ("Pendientes persistentes", s["persistentes"]),
                ("Pendientes corregidos", s["corregidos"]),
                ("Pendientes nuevos", s["nuevos"]),
                ("Impacto persistentes", s.get("impacto_persistentes", 0.0)),
                ("Impacto corregidos", s.get("impacto_corregidos", 0.0)),
                ("Impacto nuevos", s.get("impacto_nuevos", 0.0)),
                ("Impacto total pendiente", s.get("impacto_total_pendiente", 0.0)),
                ("Impacto crítico", s.get("impacto_critico", 0.0)),
                ("Cuentas riesgo alto/crítico", s.get("cuentas_riesgo_alto", 0)),
                ("Cuentas críticas", s.get("cuentas_criticas", 0)),
                ("Variación saldo", s["variacion_saldo"]),
            ], columns=["Métrica", "Valor"])
            show_table(df, height=400)
        with tabs[1]:
            show_table(apply_filter(result["top_accounts"], compare_filter), height=450)
        with tabs[2]:
            show_table(apply_filter(result["accounts"], compare_filter), height=560)
        with tabs[3]:
            sub1, sub2, sub3 = st.columns(3)
            with sub1:
                st.markdown("#### Persistentes")
                show_table(apply_filter(result["persistentes"], compare_filter), height=390)
            with sub2:
                st.markdown("#### Nuevos")
                show_table(apply_filter(result["nuevos"], compare_filter), height=390)
            with sub3:
                st.markdown("#### Corregidos")
                show_table(apply_filter(result["corregidos"], compare_filter), height=390)
        with tabs[4]:
            a1, a2 = st.columns(2)
            with a1:
                st.markdown("#### Agrupados anterior")
                show_table(apply_filter(result["agrupados_anterior"], compare_filter), height=390)
            with a2:
                st.markdown("#### Agrupados actual")
                show_table(apply_filter(result["agrupados_actual"], compare_filter), height=390)
        with tabs[5]:
            d1, d2 = st.columns(2)
            with d1:
                st.markdown("#### Duplicados anterior")
                show_table(apply_filter(result["duplicados_anterior"], compare_filter), height=420)
            with d2:
                st.markdown("#### Duplicados actual")
                show_table(apply_filter(result["duplicados_actual"], compare_filter), height=420)

    else:
        render_brand("Análisis especial")

        c_up1, c_up2 = st.columns([4, 1])
        with c_up1:
            uploaded_file = st.file_uploader(
                "Sube tu fichero de mayores",
                type=["xlsx", "xls", "csv"],
                key="analysis_special"
            )
        with c_up2:
            st.write("")
            st.write("")
            if st.button("🔄 Nuevo", key="reset_analysis_special"):
                for k in ["analysis_special", "special_mode"]:
                    if k in st.session_state:
                        del st.session_state[k]
                st.rerun()

        if not uploaded_file:
            return

        special_mode = st.radio(
            "Tipo de análisis",
            ["Cuenta 555", "Activos y amortización"],
            key="special_mode",
            horizontal=True
        )

        with st.spinner("Analizando fichero..."):
            accounts, parser_used = process_special_file(uploaded_file)

        if special_mode == "Cuenta 555":
            st.markdown("## Análisis cuenta 555")

            st.markdown(
                """
                <div style="display:flex;gap:12px;margin-bottom:12px;flex-wrap:wrap;">
                  <span style="background:#1a3a2a;border:1.5px solid #22c55e;color:#86efac;
                    padding:5px 13px;border-radius:8px;font-size:12px;font-weight:700;">
                    🟢 Conciliado interno (555↔555)</span>
                  <span style="background:#1e2a0f;border:1.5px solid #84cc16;color:#d9f99d;
                    padding:5px 13px;border-radius:8px;font-size:12px;font-weight:700;">
                    🟡 Cruce exacto (mismo importe)</span>
                  <span style="background:#2d1b10;border:1.5px solid #f97316;color:#fdba74;
                    padding:5px 13px;border-radius:8px;font-size:12px;font-weight:700;">
                    🟠 Cruce semántico (préstamo/referencia)</span>
                  <span style="background:#2d1010;border:1.5px solid #ef4444;color:#fca5a5;
                    padding:5px 13px;border-radius:8px;font-size:12px;font-weight:700;">
                    🔴 Pendiente sin correspondencia</span>
                </div>
                """,
                unsafe_allow_html=True,
            )

            df_exacto, df_sem = _build_555_analysis_rows(accounts)
            excel_555 = _export_special_555_excel(accounts, df_exacto, df_sem)
            st.download_button(
                "📥 Descargar análisis 555 (.xlsx)",
                data=excel_555,
                file_name=f"analisis_555_{uploaded_file.name.rsplit('.', 1)[0]}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            )

            tab_exacto, tab_sem, tab_stats = st.tabs([
                f"Cruces exactos ({len(df_exacto)})",
                f"Cruces semánticos — préstamos/refs ({len(df_sem)})",
                "Estadísticas"
            ])
            with tab_exacto:
                if not df_exacto.empty:
                    show_table(df_exacto, height=520)
                else:
                    st.markdown('<div class="table-empty">Sin cruces exactos de importe detectados.</div>', unsafe_allow_html=True)
            with tab_sem:
                if not df_sem.empty:
                    st.caption("Cruces por número de préstamo / referencia compartida. La columna 'Diferencia importe' es el tramo de intereses (va a la 665 o similar). Ordenados por Score descendente.")
                    show_table(df_sem, height=520)
                else:
                    st.markdown('<div class="table-empty">Sin cruces semánticos detectados.</div>', unsafe_allow_html=True)
            with tab_stats:
                cuentas_555_list = [a for a in accounts if str(getattr(a, "codigo", "") or "").startswith("555")]
                stat_rows = []
                for acc in cuentas_555_list:
                    im = _reconcile_555_internal(acc)
                    total_m = len(getattr(acc, "movements", []) or [])
                    conc_int = len(im) // 2 if im else 0
                    code_acc = str(getattr(acc, "codigo", ""))
                    n_exacto = len(df_exacto[df_exacto["Cuenta 555"] == code_acc]) if not df_exacto.empty else 0
                    n_sem = len(df_sem[df_sem["Cuenta 555"] == code_acc]) if not df_sem.empty else 0
                    stat_rows.append({
                        "Cuenta": code_acc,
                        "Nombre": getattr(acc, "nombre", ""),
                        "Total movimientos": total_m,
                        "Conciliados interno": conc_int * 2,
                        "Cruces exactos": n_exacto,
                        "Cruces semánticos": n_sem,
                        "Pendientes": total_m - conc_int * 2,
                    })
                show_table(pd.DataFrame(stat_rows), height=320)

        else:
            st.markdown("## Análisis de activos y amortización")

            st.markdown(
                """
                <div style="display:flex;gap:12px;margin-bottom:12px;flex-wrap:wrap;">
                  <span style="background:#1a3a2a;border:1.5px solid #22c55e;color:#86efac;
                    padding:5px 13px;border-radius:8px;font-size:12px;font-weight:700;">
                    🟢 Amortizado ≥95%</span>
                  <span style="background:#2d2010;border:1.5px solid #f59e0b;color:#fcd34d;
                    padding:5px 13px;border-radius:8px;font-size:12px;font-weight:700;">
                    🟡 Amort. huérfana (28x sin activo)</span>
                  <span style="background:#2d1b10;border:1.5px solid #f97316;color:#fdba74;
                    padding:5px 13px;border-radius:8px;font-size:12px;font-weight:700;">
                    🟠 Amortización parcial</span>
                  <span style="background:#2d1010;border:1.5px solid #ef4444;color:#fca5a5;
                    padding:5px 13px;border-radius:8px;font-size:12px;font-weight:700;">
                    🔴 Sin amortizar</span>
                  <span style="background:#0f1f3a;border:1.5px solid #60a5fa;color:#bfdbfe;
                    padding:5px 13px;border-radius:8px;font-size:12px;font-weight:700;">
                    🔵 Sin dotación 681</span>
                </div>
                """,
                unsafe_allow_html=True,
            )

            df_assets = _build_assets_analysis_rows(accounts)
            excel_assets = _export_special_assets_excel(accounts, df_assets)
            st.download_button(
                "📥 Descargar análisis activos (.xlsx)",
                data=excel_assets,
                file_name=f"analisis_activos_{uploaded_file.name.rsplit('.', 1)[0]}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            )

            if not df_assets.empty:
                n_sin = int((df_assets["Estado"] == "Sin amortizar").sum())
                n_parc = int(df_assets["Estado"].str.startswith("Parcial", na=False).sum())
                n_ok = int((df_assets["Estado"] == "Amortizado").sum())
                n_huerfanas = int((df_assets["Estado"] == "Amort. huérfana").sum())
                n_sin_dot = int(df_assets.get("Estado dotación", pd.Series(dtype=str)).str.startswith("❌", na=False).sum()) if "Estado dotación" in df_assets.columns else 0

                tab_all, tab_sin, tab_parc, tab_ok, tab_681 = st.tabs([
                    f"Todos ({len(df_assets)})",
                    f"Sin amortizar ({n_sin})",
                    f"Parcial ({n_parc})",
                    f"Amortizados OK ({n_ok})",
                    f"Sin dotación 681 ({n_sin_dot})",
                ])
                with tab_all:
                    show_table(df_assets, height=520)
                with tab_sin:
                    df_sin_ui = df_assets[df_assets["Estado"] == "Sin amortizar"]
                    if df_sin_ui.empty:
                        st.markdown('<div class="table-empty">✅ Todos los activos tienen amortización asociada.</div>', unsafe_allow_html=True)
                    else:
                        show_table(df_sin_ui, height=420)
                with tab_parc:
                    df_parc_ui = df_assets[df_assets["Estado"].str.startswith("Parcial", na=False)]
                    if df_parc_ui.empty:
                        st.markdown('<div class="table-empty">Sin activos con amortización parcial.</div>', unsafe_allow_html=True)
                    else:
                        show_table(df_parc_ui, height=420)
                with tab_ok:
                    df_ok_ui = df_assets[df_assets["Estado"] == "Amortizado"]
                    if df_ok_ui.empty:
                        st.markdown('<div class="table-empty">Sin activos totalmente amortizados.</div>', unsafe_allow_html=True)
                    else:
                        show_table(df_ok_ui, height=420)
                with tab_681:
                    if "Estado dotación" in df_assets.columns:
                        df_681_ui = df_assets[df_assets["Estado dotación"].str.startswith("❌", na=False)]
                        if df_681_ui.empty:
                            st.markdown('<div class="table-empty">✅ Todos los activos tienen dotación 681 detectada.</div>', unsafe_allow_html=True)
                        else:
                            st.caption(f"{len(df_681_ui)} activos sin dotación de amortización del ejercicio detectada en cuenta 681.")
                            show_table(df_681_ui[["Cuenta activo", "Nombre activo", "Saldo activo",
                                                   "Cuenta amort.", "% Amortizado", "Estado",
                                                   "Cuenta 681", "Estado dotación"]], height=420)
                    else:
                        st.markdown('<div class="table-empty">No se detectaron cuentas 681 en el fichero.</div>', unsafe_allow_html=True)
            else:
                st.markdown('<div class="table-empty">Sin cuentas de activos detectadas para revisar amortización.</div>', unsafe_allow_html=True)


if __name__ == "__main__":
    main()
