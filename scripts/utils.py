# -*- coding: utf-8 -*-
"""
utils.py — Utilidades compartidas por todos los módulos BCRA.
"""

import os
import re
import subprocess
import time
from pathlib import Path

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


# ---------------------------------------------------------------------------
# Sistema
# ---------------------------------------------------------------------------

def run(cmd: list[str]) -> str:
    """Ejecuta un comando de sistema. Lanza RuntimeError si falla."""
    p = subprocess.run(cmd, capture_output=True, text=True)
    if p.returncode != 0:
        raise RuntimeError(p.stderr)
    return p.stdout


def detect_encoding(path: Path) -> str:
    """Detecta si el archivo es UTF-8 o Latin-1."""
    try:
        with open(path, "r", encoding="utf-8") as f:
            f.read(4096)
        return "utf-8"
    except UnicodeDecodeError:
        return "latin-1"


# ---------------------------------------------------------------------------
# Descarga con reintentos
# ---------------------------------------------------------------------------

def download_file(url: str, out_dir: Path, max_retries: int = 5) -> Path:
    """
    Descarga un archivo a out_dir con reintentos automáticos.
    Devuelve el Path del archivo descargado.
    """
    out_dir.mkdir(parents=True, exist_ok=True)
    fname = url.split("/")[-1].split("?")[0]
    path = out_dir / fname

    session = requests.Session()
    retry = Retry(
        total=max_retries,
        connect=max_retries,
        read=max_retries,
        backoff_factor=2,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("http://", adapter)
    session.mount("https://", adapter)

    last_error = None
    for attempt in range(1, max_retries + 1):
        try:
            if path.exists():
                path.unlink()
            with session.get(url, stream=True, timeout=(30, 300)) as r:
                r.raise_for_status()
                with open(path, "wb") as f:
                    for chunk in r.iter_content(chunk_size=1024 * 1024):
                        if chunk:
                            f.write(chunk)
            if path.exists() and path.stat().st_size > 0:
                return path
            raise RuntimeError("Archivo descargado quedó vacío.")
        except Exception as e:
            last_error = e
            print(f"⚠️  Intento {attempt}/{max_retries} falló para {fname}: {e}")
            time.sleep(2 * attempt)

    raise RuntimeError(
        f"No se pudo descargar {fname} luego de {max_retries} intentos. "
        f"Último error: {last_error}"
    )


# ---------------------------------------------------------------------------
# Extracción 7z
# ---------------------------------------------------------------------------

def extract_with_7z(archive: Path, out_dir: Path) -> Path:
    """Extrae un archivo .7z en out_dir usando el binario 7z del sistema."""
    out_dir.mkdir(parents=True, exist_ok=True)
    run(["7z", "t", str(archive)])
    run(["7z", "x", "-y", f"-o{out_dir}", str(archive)])
    return out_dir


# ---------------------------------------------------------------------------
# CSV helpers
# ---------------------------------------------------------------------------

def tab_to_csv_no_parse(src_txt: Path, dst_csv: Path, sep: str = ";") -> None:
    """
    Convierte TXT tabulado a CSV reemplazando TAB por sep.
    100% fiel: no interpreta tipos ni comillas.
    """
    enc = detect_encoding(src_txt)
    dst_csv.parent.mkdir(parents=True, exist_ok=True)
    with (
        open(src_txt, "r", encoding=enc, errors="replace", newline="") as fin,
        open(dst_csv, "w", encoding="utf-8-sig", newline="") as fout,
    ):
        for line in fin:
            fout.write(line.replace("\t", sep))


def first_line_raw(csv_path: Path) -> str | None:
    """Devuelve la primera línea no vacía del CSV, sin parsear."""
    with open(csv_path, "r", encoding="utf-8-sig", errors="replace") as f:
        for line in f:
            stripped = line.rstrip("\n")
            if stripped.strip():
                return stripped
    return None


def count_cols_semicolon(csv_path: Path) -> int:
    """Cuenta columnas de la primera línea de un CSV separado por ';'."""
    line = first_line_raw(csv_path)
    return len(line.split(";")) if line else 0


def get_periodo_informe_from_csv(csv_path: Path) -> str:
    """Extrae el campo periodo_informe (columna 3) de la primera fila."""
    line = first_line_raw(csv_path)
    if not line:
        raise RuntimeError(f"CSV vacío: {csv_path}")
    parts = line.split(";")
    if len(parts) < 3:
        raise RuntimeError(f"CSV sin suficientes campos: {csv_path}")
    return parts[2].strip().strip('"')
