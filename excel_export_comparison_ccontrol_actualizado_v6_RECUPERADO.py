from io import BytesIO
import pandas as pd


def export_period_comparison(result, prev_name, curr_name):
    buffer = BytesIO()
    summary = result["summary"]
    df_summary = pd.DataFrame([
        ("Periodo anterior", prev_name),
        ("Periodo actual", curr_name),
        ("Metodología", summary.get("metodologia", "")),
        ("Cuentas comparadas", summary.get("cuentas_comparadas", 0)),
        ("Pendientes periodo anterior", summary.get("movimientos_anterior", 0)),
        ("Pendientes periodo actual", summary.get("movimientos_actual", 0)),
        ("Pendientes persistentes", summary.get("persistentes", 0)),
        ("Pendientes corregidos", summary.get("corregidos", 0)),
        ("Pendientes nuevos", summary.get("nuevos", 0)),
        ("Impacto persistentes", summary.get("impacto_persistentes", 0.0)),
        ("Impacto corregidos", summary.get("impacto_corregidos", 0.0)),
        ("Impacto nuevos", summary.get("impacto_nuevos", 0.0)),
        ("Impacto total pendiente", summary.get("impacto_total_pendiente", 0.0)),
        ("Cuentas riesgo alto/crítico", summary.get("cuentas_riesgo_alto", 0)),
        ("Duplicados anterior", summary.get("duplicados_anterior", 0)),
        ("Duplicados actual", summary.get("duplicados_actual", 0)),
        ("Agrupados anterior", summary.get("agrupados_anterior", 0)),
        ("Agrupados actual", summary.get("agrupados_actual", 0)),
        ("Saldo pendiente anterior", summary.get("saldo_anterior", 0.0)),
        ("Saldo pendiente actual", summary.get("saldo_actual", 0.0)),
        ("Variación saldo pendiente", summary.get("variacion_saldo", 0.0)),
        ("Cuentas excluidas", summary.get("cuentas_excluidas_regla", "")),
    ], columns=["Métrica", "Valor"])

    with pd.ExcelWriter(buffer, engine="xlsxwriter") as writer:
        data_map = {
            "Resumen": df_summary,
            "Alertas": result["insights"],
            "Top Cuentas": result["top_accounts"],
            "Evolución cuentas": result["accounts"],
            "Persistentes": result["persistentes"],
            "Corregidos": result["corregidos"],
            "Nuevos": result["nuevos"],
            "Duplicados Anterior": result["duplicados_anterior"],
            "Duplicados Actual": result["duplicados_actual"],
            "Agrupados Anterior": result["agrupados_anterior"],
            "Agrupados Actual": result["agrupados_actual"],
        }
        for sheet_name, df in data_map.items():
            df.to_excel(writer, sheet_name=sheet_name, index=False)

        wb = writer.book
        header = wb.add_format({"bold": True, "bg_color": "#14444E", "font_color": "white", "border": 1, "align": "center"})
        money = wb.add_format({"num_format": "#,##0.00"})
        note = wb.add_format({"text_wrap": True, "valign": "top"})
        orange = wb.add_format({"bg_color": "#FAEADF"})
        green = wb.add_format({"bg_color": "#D9F0EC"})
        red = wb.add_format({"bg_color": "#F8D7D3"})
        yellow = wb.add_format({"bg_color": "#FFF3CD"})

        numeric_money_cols = {
            "Importe", "Valor", "Saldo_Anterior", "Saldo_Actual", "Var Saldo",
            "Debe_Anterior", "Debe_Actual", "Haber_Anterior", "Haber_Actual",
            "Var Debe", "Var Haber", "Importe Grupo Debe", "Importe Grupo Haber",
            "Impacto Persistentes", "Impacto Nuevos", "Impacto Corregidos",
            "Impacto Bruto", "Impacto Neto", "Impacto", "Score Riesgo"
        }

        for sheet_name, df in data_map.items():
            ws = writer.sheets[sheet_name]
            for c, col in enumerate(df.columns):
                ws.write(0, c, col, header)
                width = min(max(len(str(col)) + 4, 14), 48)
                if col in ("Detalle", "Motivo", "Observación", "Descripción", "Motivo Riesgo", "Valor"):
                    width = 46
                ws.set_column(c, c, width)

            for col in df.columns:
                if col in numeric_money_cols:
                    idx = df.columns.get_loc(col)
                    ws.set_column(idx, idx, 16, money)
                if col in ["Detalle", "Motivo", "Observación", "Descripción", "Motivo Riesgo", "Valor"]:
                    idx = df.columns.get_loc(col)
                    ws.set_column(idx, idx, 46, note)

            if sheet_name == "Alertas" and not df.empty and "Severidad" in df.columns:
                sev_idx = df.columns.get_loc("Severidad")
                for r in range(1, len(df) + 1):
                    sev = str(df.iloc[r - 1, sev_idx])
                    fmt = red if sev == "Crítica" else yellow if sev == "Media" else green
                    if sev == "Alta":
                        fmt = orange
                    ws.set_row(r, 26, fmt)
            if sheet_name == "Top Cuentas" and not df.empty and "Nivel Riesgo" in df.columns:
                risk_idx = df.columns.get_loc("Nivel Riesgo")
                for r in range(1, len(df) + 1):
                    risk = str(df.iloc[r - 1, risk_idx])
                    fmt = red if risk in ("Crítico", "Alto") else yellow if risk == "Medio" else green
                    ws.set_row(r, 24, fmt)
            if sheet_name == "Resumen":
                for r in range(1, len(df) + 1):
                    ws.set_row(r, 22, orange if r <= 14 else green)
            ws.freeze_panes(1, 0)

    buffer.seek(0)
    return buffer
