# -*- coding: utf-8 -*-
"""
verificador.py — Verificaciones de integridad sobre los archivos procesados.
"""

import csv
from dataclasses import dataclass, field
from pathlib import Path

import pandas as pd

from utils import detect_encoding


# ---------------------------------------------------------------------------
# Estructuras de resultado
# ---------------------------------------------------------------------------

@dataclass
class ResultadoVerificacion:
    ok: bool = True
    errores: list[str] = field(default_factory=list)
    advertencias: list[str] = field(default_factory=list)
    resumen: dict = field(default_factory=dict)

    def agregar_error(self, msg: str) -> None:
        self.ok = False
        self.errores.append(msg)

    def agregar_advertencia(self, msg: str) -> None:
        self.advertencias.append(msg)

    def texto_reporte(self) -> str:
        lineas = []
        if self.ok:
            lineas.append("✅ Verificación OK")
        else:
            lineas.append(f"❌ Verificación FALLIDA — {len(self.errores)} error(es)")
        for e in self.errores:
            lineas.append(f"  ERROR: {e}")
        for w in self.advertencias:
            lineas.append(f"  WARN:  {w}")
        if self.resumen:
            lineas.append("  Resumen:")
            for k, v in self.resumen.items():
                lineas.append(f"    {k}: {v}")
        return "\n".join(lineas)


# ---------------------------------------------------------------------------
# Verificación de CSV de una carpeta (Tec_Cont)
# ---------------------------------------------------------------------------

MIN_COLUMNAS = {
    "balres":  6,
    "indicad": 6,
    "inf_adi": 5,
}

def verificar_csv_carpeta(
    carpeta: Path,
    nombre_carpeta: str,
    min_archivos: int = 1,
) -> ResultadoVerificacion:
    """Verifica todos los CSVs de una carpeta Tec_Cont."""
    res = ResultadoVerificacion()
    min_cols = MIN_COLUMNAS.get(nombre_carpeta, 5)

    if not carpeta.exists():
        res.agregar_error(f"Carpeta no encontrada: {carpeta}")
        return res

    csvs = sorted(carpeta.glob("*.csv"))
    csvs_entidad = [c for c in csvs if c.stem.isdigit()]

    res.resumen["carpeta"]     = str(carpeta)
    res.resumen["total_csv"]   = len(csvs)
    res.resumen["csv_entidad"] = len(csvs_entidad)

    if len(csvs_entidad) < min_archivos:
        res.agregar_error(
            f"{nombre_carpeta}: se esperaban al menos {min_archivos} CSV de entidades, "
            f"se encontraron {len(csvs_entidad)}."
        )
        return res

    errores_cols  = 0
    errores_vacios = 0
    filas_totales  = 0

    for csv_path in csvs_entidad:
        try:
            df = pd.read_csv(
                csv_path, sep=";", header=None, dtype=str,
                encoding="utf-8-sig", engine="python"
            )
            if df.shape[1] < min_cols:
                errores_cols += 1
                res.agregar_advertencia(
                    f"{csv_path.name}: solo {df.shape[1]} columnas (mínimo {min_cols})."
                )
            filas_validas = df.dropna(how="all").shape[0]
            filas_totales += filas_validas
            if filas_validas == 0:
                errores_vacios += 1
                res.agregar_advertencia(f"{csv_path.name}: archivo vacío.")
        except Exception as e:
            res.agregar_error(f"{csv_path.name}: error al leer — {e}")

    res.resumen["filas_totales"]    = filas_totales
    res.resumen["errores_columnas"] = errores_cols
    res.resumen["archivos_vacios"]  = errores_vacios

    if errores_cols > len(csvs_entidad) * 0.10:
        res.agregar_error(
            f"{nombre_carpeta}: {errores_cols}/{len(csvs_entidad)} archivos con columnas "
            "incorrectas. Posible problema en el .7z del BCRA."
        )

    return res


# ---------------------------------------------------------------------------
# Verificación de Info_Hist
# ---------------------------------------------------------------------------

def verificar_info_hist(info_hist_dir: Path) -> ResultadoVerificacion:
    """Verifica los CSV de Info_Hist (activas y bajas)."""
    res = ResultadoVerificacion()

    for nombre in ["info_hist_activas.csv", "info_hist_bajas.csv"]:
        csv_path = info_hist_dir / nombre
        if not csv_path.exists():
            res.agregar_error(f"No se generó {nombre}.")
            continue
        try:
            df = pd.read_csv(csv_path, encoding="utf-8-sig", dtype=str)
            if df.empty:
                res.agregar_error(f"{nombre}: sin filas.")
            else:
                res.resumen[nombre] = f"{len(df)} filas, {df.shape[1]} columnas"
                if df.shape[1] < 3:
                    res.agregar_error(f"{nombre}: muy pocas columnas ({df.shape[1]}).")
        except Exception as e:
            res.agregar_error(f"{nombre}: error al leer — {e}")

    return res


# ---------------------------------------------------------------------------
# Verificación de tablas históricas
# ---------------------------------------------------------------------------

def verificar_tabla_historica(csv_path: Path, nombre: str) -> ResultadoVerificacion:
    """Verifica que una tabla histórica tenga datos coherentes."""
    res = ResultadoVerificacion()

    if not csv_path.exists():
        res.agregar_error(f"{nombre}: archivo no encontrado en {csv_path}.")
        return res

    try:
        df = pd.read_csv(csv_path, encoding="utf-8-sig", dtype=str)
        if df.empty:
            res.agregar_error(f"{nombre}: tabla vacía.")
        else:
            res.resumen[nombre] = f"{len(df)} filas, {df.shape[1]} columnas"
            col_mes = next(
                (c for c in df.columns if "mes" in c.lower() or "periodo" in c.lower()),
                None
            )
            if col_mes:
                meses_unicos = df[col_mes].nunique()
                res.resumen["meses_en_tabla"] = meses_unicos
                if meses_unicos == 0:
                    res.agregar_error(f"{nombre}: columna '{col_mes}' sin valores.")
    except Exception as e:
        res.agregar_error(f"{nombre}: error al leer — {e}")

    return res


# ---------------------------------------------------------------------------
# Verificación completa de un mes procesado
# ---------------------------------------------------------------------------

def verificar_mes_completo(month_dir: Path) -> ResultadoVerificacion:
    """Ejecuta todas las verificaciones sobre la carpeta de un mes procesado."""
    consolidado = ResultadoVerificacion()
    consolidado.resumen["mes_dir"] = str(month_dir)

    if not month_dir.exists():
        consolidado.agregar_error(f"Directorio del mes no encontrado: {month_dir}")
        return consolidado

    tec_cont_csv = month_dir / "Tec_Cont_csv"
    for carpeta_nombre in ["balres", "indicad", "inf_adi"]:
        carpeta = tec_cont_csv / carpeta_nombre
        if carpeta.exists():
            r = verificar_csv_carpeta(carpeta, carpeta_nombre)
            if not r.ok:
                for e in r.errores:
                    consolidado.agregar_error(f"[{carpeta_nombre}] {e}")
            for w in r.advertencias:
                consolidado.agregar_advertencia(f"[{carpeta_nombre}] {w}")
            consolidado.resumen[f"tec_cont_{carpeta_nombre}"] = r.resumen

    info_hist_dir = month_dir / "Info_Hist_csv"
    if info_hist_dir.exists():
        r = verificar_info_hist(info_hist_dir)
        if not r.ok:
            for e in r.errores:
                consolidado.agregar_error(f"[Info_Hist] {e}")
        consolidado.resumen["info_hist"] = r.resumen

    return consolidado


def verificar_tablas_finales(base_dir: Path) -> ResultadoVerificacion:
    """Verifica las tablas históricas consolidadas."""
    consolidado = ResultadoVerificacion()

    tablas = {
        "inf_adi_cantidad_cuentas": base_dir / "inf_adi_cantidad_cuentas_stock.csv",
        "info_sistema_hist":        base_dir / "Info_sistema_hist" / "info_sistema_hist.csv",
    }

    for nombre, path in tablas.items():
        r = verificar_tabla_historica(path, nombre)
        if not r.ok:
            for e in r.errores:
                consolidado.agregar_error(f"[{nombre}] {e}")
        consolidado.resumen[nombre] = r.resumen

    return consolidado