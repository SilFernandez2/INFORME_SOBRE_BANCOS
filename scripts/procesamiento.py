# -*- coding: utf-8 -*-
"""
procesamiento.py — Funciones que encapsulan los Programas 2, 3, 4 y 5.
"""

import csv
from pathlib import Path

import pandas as pd
from pandas.tseries.offsets import DateOffset

from utils import (
    detect_encoding, tab_to_csv_no_parse,
    count_cols_semicolon, get_periodo_informe_from_csv,
)


# ===========================================================================
# PROGRAMA 2 — Tec_Cont y Info_Hist
# ===========================================================================

def procesar_tec_cont(extract_dir: Path, month_dir: Path) -> None:
    """Convierte todos los TXT de Tec_Cont a CSV (TAB → ;)."""
    tec = next(extract_dir.rglob("Tec_Cont"), None)
    if tec is None:
        raise FileNotFoundError("No encontré Tec_Cont dentro de lo extraído.")

    out_root = month_dir / "Tec_Cont_csv"

    for folder in sorted([p for p in tec.iterdir() if p.is_dir()]):
        out_dir = out_root / folder.name
        txts = sorted(folder.glob("*.txt"))
        if not txts:
            continue
        ok = 0
        for txt in txts:
            tab_to_csv_no_parse(txt, out_dir / f"{txt.stem}.csv")
            ok += 1
        print(f"   Tec_Cont/{folder.name}: {ok} archivos convertidos.")


def _read_infohist_txt(path: Path) -> pd.DataFrame:
    enc  = detect_encoding(path)
    rows = []
    maxlen = 0
    with open(path, "r", encoding=enc, errors="replace", newline="") as f:
        reader = csv.reader(f, delimiter="\t", quotechar='"', doublequote=True)
        for row in reader:
            if not row or not any(str(x).strip() for x in row):
                continue
            maxlen = max(maxlen, len(row))
            rows.append(row)
    if not rows:
        return pd.DataFrame()
    rows = [r + [""] * (maxlen - len(r)) for r in rows]
    df   = pd.DataFrame(rows, columns=[f"col_{i+1:02d}" for i in range(maxlen)])
    for c in df.columns:
        df[c] = df[c].astype(str).str.strip().str.strip('"')
    return df


def procesar_info_hist(extract_dir: Path, month_dir: Path) -> None:
    """Genera info_hist_activas.csv e info_hist_bajas.csv."""
    info_hist = next(extract_dir.rglob("Info_Hist"), None)
    if info_hist is None:
        raise FileNotFoundError("No encontré la carpeta Info_Hist dentro de lo extraído.")

    out_dir = month_dir / "Info_Hist_csv"
    out_dir.mkdir(parents=True, exist_ok=True)

    for folder_name, out_name in [("Activas", "info_hist_activas.csv"),
                                   ("Bajas",   "info_hist_bajas.csv")]:
        folder = next(info_hist.rglob(folder_name), None)
        if folder is None or not Path(folder).exists():
            raise FileNotFoundError(f"No encontré subcarpeta {folder_name}.")

        txts = sorted(Path(folder).glob("*.txt"))
        if not txts:
            raise FileNotFoundError(f"No hay .txt en {folder}")

        dfs = []
        for txt in txts:
            df = _read_infohist_txt(txt)
            if df.empty:
                continue
            df.insert(0, "__entidad", txt.stem)
            df.insert(1, "__source_file", txt.name)
            dfs.append(df)

        if dfs:
            resultado = pd.concat(dfs, ignore_index=True)
            resultado.to_csv(out_dir / out_name, index=False, encoding="utf-8-sig")
            print(f"   Info_Hist/{folder_name}: {len(resultado)} filas → {out_name}")


# ===========================================================================
# PROGRAMA 3 — Tabla histórica inf_adi (Cantidad de Cuentas)
# ===========================================================================

CODIGO_OBJETIVO    = "400100001000"
CATEGORIA_OBJETIVO = "Cantidad de Cuentas"
SUBDIR_INF_ADI     = Path("Tec_Cont_csv/inf_adi")


def _normalizar(x) -> str:
    if pd.isna(x):
        return ""
    return str(x).replace("\ufeff", "").strip().strip('"').strip()


def generar_tabla_inf_adi(base_dir: Path) -> None:
    """Genera inf_adi_cantidad_cuentas_stock.csv/xlsx con imputación de nulos."""
    filas = []

    for mes_dir in sorted([p for p in base_dir.iterdir() if p.is_dir()]):
        inf_adi_dir = mes_dir / SUBDIR_INF_ADI
        if not inf_adi_dir.exists():
            continue

        archivos = sorted([p for p in inf_adi_dir.glob("*.csv") if p.stem.isdigit()])
        for archivo in archivos:
            try:
                df = pd.read_csv(archivo, sep=";", header=None, dtype=str,
                                 encoding="utf-8-sig", engine="python")
                if df.shape[1] < 6:
                    continue
                col4 = df.iloc[:, 3].map(_normalizar)
                col5 = df.iloc[:, 4].map(_normalizar)
                mask = (col4 == CODIGO_OBJETIVO) & (col5 == CATEGORIA_OBJETIVO)
                if mask.any():
                    temp = pd.DataFrame({
                        "mes_archivo":    mes_dir.name,
                        "cod_entidad":    df.iloc[:, 0].map(_normalizar)[mask],
                        "entidad":        df.iloc[:, 1].map(_normalizar)[mask],
                        "periodo":        df.iloc[:, 2].map(_normalizar)[mask],
                        "codigo":         col4[mask],
                        "categoria":      col5[mask],
                        "stock_fecha":    df.iloc[:, -1].map(_normalizar)[mask],
                        "valor_col_-2":   df.iloc[:, -2].map(_normalizar)[mask] if df.shape[1] >= 2 else "",
                        "valor_col_-3":   df.iloc[:, -3].map(_normalizar)[mask] if df.shape[1] >= 3 else "",
                        "archivo_fuente": archivo.name,
                    })
                    filas.append(temp)
            except Exception as e:
                print(f"   ⚠️  {archivo}: {e}")

    if not filas:
        print("   ⚠️  No se encontraron filas para inf_adi Cantidad de Cuentas.")
        return

    df_final = pd.concat(filas, ignore_index=True)
    df_final["cod_entidad"]  = pd.to_numeric(df_final["cod_entidad"],  errors="coerce").astype("Int64")
    df_final["periodo"]      = pd.to_numeric(df_final["periodo"],      errors="coerce").astype("Int64")
    df_final["stock_fecha"]  = pd.to_numeric(df_final["stock_fecha"],  errors="coerce")
    df_final["valor_col_-2"] = pd.to_numeric(df_final["valor_col_-2"], errors="coerce")
    df_final["fecha_periodo"] = pd.to_datetime(
        df_final["periodo"].astype(str), format="%Y%m", errors="coerce"
    )

    df_final["stock_original"]              = df_final["stock_fecha"]
    df_final["imputado_desde_mes_siguiente"] = False
    lookup = df_final.set_index(["cod_entidad", "fecha_periodo"])

    for idx, row in df_final.iterrows():
        if pd.isna(row["stock_fecha"]) or row["stock_fecha"] == 0:
            try:
                sig = lookup.loc[(row["cod_entidad"], row["fecha_periodo"] + DateOffset(months=1))]
                if isinstance(sig, pd.DataFrame):
                    sig = sig.iloc[0]
                if pd.notna(sig["stock_fecha"]) and pd.notna(sig["valor_col_-2"]):
                    if sig["stock_fecha"] == sig["valor_col_-2"]:
                        df_final.at[idx, "stock_fecha"] = sig["valor_col_-2"]
                        df_final.at[idx, "imputado_desde_mes_siguiente"] = True
            except KeyError:
                pass

    df_final["stock_fecha"] = df_final["stock_fecha"].fillna(0).astype("int64")

    out_csv  = base_dir / "inf_adi_cantidad_cuentas_stock.csv"
    out_xlsx = base_dir / "inf_adi_cantidad_cuentas_stock.xlsx"
    df_final.to_csv(out_csv, index=False, encoding="utf-8-sig")
    df_final.to_excel(out_xlsx, index=False)
    print(f"   ✅ Tabla inf_adi: {len(df_final)} filas → {out_csv.name}")


# ===========================================================================
# PROGRAMA 4 — AA000 / AA110 / AA910 → Excel histórico
# ===========================================================================

ARCHIVOS_AA  = {"AA000.txt", "AA110.txt", "AA910.txt"}
CATEGORIA_HIST = "Cantidad de Cuentas"


def _txt_to_df(src_txt: Path) -> pd.DataFrame:
    enc  = detect_encoding(src_txt)
    rows = []
    with open(src_txt, "r", encoding=enc, errors="replace") as f:
        for line in f:
            rows.append(line.rstrip("\n\r").split("\t"))
    maxlen = max(len(r) for r in rows) if rows else 0
    rows = [r + [""] * (maxlen - len(r)) for r in rows]
    df = pd.DataFrame(rows)
    df = df.apply(lambda col: col.map(
        lambda x: str(x).replace("\ufeff", "").strip().strip('"') if pd.notna(x) else x
    ))
    for c in df.columns[5:]:
        df[c] = pd.to_numeric(df[c], errors="coerce")
        non_null = df[c].dropna()
        if len(non_null) > 0 and (non_null % 1 == 0).all():
            df[c] = df[c].astype("Int64")
    return df


def generar_tabla_info_sistema(base_dir: Path) -> None:
    """Genera info_sistema_hist.csv/xlsx leyendo AA000/AA110/AA910 de todos los meses."""
    out_dir = base_dir / "Info_sistema_hist"
    out_dir.mkdir(parents=True, exist_ok=True)

    for mes_dir in sorted([p for p in base_dir.iterdir()
                           if p.is_dir() and p.name != "Info_sistema_hist"]):
        candidatos = (list(mes_dir.rglob("Entfin/Tec_Cont/inf_adi")) +
                      list(mes_dir.rglob("Entfin/Tec_Cont/info_adi")))
        if not candidatos:
            continue
        info_adi_dir = candidatos[0]
        out_mes = out_dir / mes_dir.name
        out_mes.mkdir(parents=True, exist_ok=True)
        for txt_file in info_adi_dir.glob("*.txt"):
            if txt_file.name in ARCHIVOS_AA:
                df = _txt_to_df(txt_file)
                df.to_excel(out_mes / txt_file.with_suffix(".xlsx").name,
                            index=False, header=False)

    dfs_hist = []
    for mes_dir in sorted([p for p in base_dir.iterdir()
                           if p.is_dir() and p.name != "Info_sistema_hist"]):
        candidatos = (list(mes_dir.rglob("Entfin/Tec_Cont/inf_adi")) +
                      list(mes_dir.rglob("Entfin/Tec_Cont/info_adi")))
        if not candidatos:
            continue
        info_adi_dir = candidatos[0]
        for txt_file in sorted(info_adi_dir.glob("*.txt")):
            if txt_file.name not in {"AA000.txt", "AA100.txt", "AA910.txt"}:
                continue
            df = _txt_to_df(txt_file)
            if df.shape[1] < 10:
                continue
            df_filt = df[df[4].astype(str).str.strip().eq(CATEGORIA_HIST)].copy()
            if df_filt.empty:
                continue
            df_sal = df_filt[[0, 1, 2, 3, 4, 9]].copy()
            df_sal.insert(0, "mes", mes_dir.name)
            df_sal.insert(1, "archivo", txt_file.name)
            df_sal.columns = ["mes", "archivo", "col_1", "col_2", "col_3", "col_4", "categoria", "valor_col10"]
            df_sal["valor_col10"] = pd.to_numeric(df_sal["valor_col10"], errors="coerce")
            dfs_hist.append(df_sal)

    if dfs_hist:
        hist = pd.concat(dfs_hist, ignore_index=True)
        hist.to_csv(out_dir / "info_sistema_hist.csv",  index=False, encoding="utf-8-sig")
        hist.to_excel(out_dir / "info_sistema_hist.xlsx", index=False)
        print(f"   ✅ info_sistema_hist: {len(hist)} filas")


# ===========================================================================
# PROGRAMA 5 — Normalización balres (dividir montos por 1000)
# ===========================================================================

def _leer_flexible(path: Path, sep: str) -> tuple[pd.DataFrame, str]:
    for enc in ["utf-8-sig", "latin-1", "cp1252"]:
        try:
            df = pd.read_csv(path, sep=sep, header=None, dtype=str,
                             encoding=enc, engine="python")
            return df, enc
        except UnicodeDecodeError:
            continue
    raise ValueError(f"No se pudo leer {path}")


def normalizar_balres(
    base_dir: Path,
    subcarpeta: str = "balres",
    ultimas_n_cols: int = 5,
    divisor: float = 1000,
) -> None:
    """
    Genera balres_corregido (CSV) y balres_corregido_excel (XLSX)
    con las últimas N columnas de montos divididas por divisor.
    """
    meses    = [p for p in sorted(base_dir.iterdir()) if p.is_dir()]
    total_csv  = 0
    total_xlsx = 0

    for mes_dir in meses:
        origen = mes_dir / "Tec_Cont_csv" / subcarpeta
        salida_csv  = mes_dir / "Tec_Cont_csv" / f"{subcarpeta}_corregido"
        salida_xlsx = mes_dir / "Tec_Cont_csv" / f"{subcarpeta}_corregido_excel"

        if not origen.exists():
            continue

        salida_csv.mkdir(parents=True, exist_ok=True)
        salida_xlsx.mkdir(parents=True, exist_ok=True)

        for archivo in sorted(origen.glob("*.csv")):
            if archivo.stem.lower() == "formato":
                continue
            try:
                df, _ = _leer_flexible(archivo, sep=";")
                df = df.apply(lambda col: col.map(
                    lambda x: str(x).strip().strip('"') if pd.notna(x) else ""
                ))
                if df.shape[1] < ultimas_n_cols:
                    continue

                df_num = df.copy()
                for col in df.columns[-ultimas_n_cols:]:
                    limpio = df[col].astype(str).str.strip().replace(
                        {"": pd.NA, "nan": pd.NA, "None": pd.NA}
                    )
                    num = pd.to_numeric(limpio, errors="coerce") / divisor
                    # CSV: texto con 1 decimal
                    df[col] = num.map(lambda x: f"{x:.1f}" if pd.notna(x) else "")
                    # Excel: float real (Power BI lo lee bien)
                    df_num[col] = num

                df.to_csv(salida_csv / archivo.name, sep=";",
                          header=False, index=False, encoding="utf-8-sig")
                df_num.to_excel(salida_xlsx / archivo.with_suffix(".xlsx").name,
                                index=False, header=False)
                total_csv  += 1
                total_xlsx += 1
            except Exception as e:
                print(f"   ⚠️  {archivo.name}: {e}")

    print(f"   ✅ balres_corregido CSV:   {total_csv} archivos")
    print(f"   ✅ balres_corregido Excel: {total_xlsx} archivos (listos para Power BI)")