# -*- coding: utf-8 -*-
"""
estado.py — Lectura y escritura del archivo estado.json del repo.

El archivo estado.json vive en la raíz del repositorio y tiene esta estructura:
{
    "ultimo_mes_procesado": "Noviembre 2025",
    "fecha_procesamiento": "2025-12-03",
    "intentos_fallidos": 0,
    "meses_procesados": ["Diciembre 2024", "Enero 2025", ..., "Noviembre 2025"]
}
"""

import json
from datetime import date
from pathlib import Path

ESTADO_PATH = Path(__file__).parent.parent / "estado.json"


def _defaults() -> dict:
    return {
        "ultimo_mes_procesado": None,
        "fecha_procesamiento": None,
        "intentos_fallidos": 0,
        "ultimo_error": [],
        "meses_procesados": [],
    }


def leer_estado(path: Path = ESTADO_PATH) -> dict:
    """Devuelve el estado actual. Si el archivo no existe, devuelve defaults."""
    if not path.exists():
        return _defaults()
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)
    defaults = _defaults()
    defaults.update(data)
    return defaults


def guardar_estado(estado: dict, path: Path = ESTADO_PATH) -> None:
    """Guarda el estado en disco."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(estado, f, ensure_ascii=False, indent=2)
    print(f"✅ estado.json actualizado: {path}")


def marcar_mes_procesado(mes_texto: str, path: Path = ESTADO_PATH) -> None:
    """
    Marca un mes como procesado exitosamente.
    Resetea el contador de intentos fallidos.
    """
    estado = leer_estado(path)
    estado["ultimo_mes_procesado"] = mes_texto
    estado["fecha_procesamiento"] = date.today().isoformat()
    estado["intentos_fallidos"] = 0
    if mes_texto not in estado["meses_procesados"]:
        estado["meses_procesados"].append(mes_texto)
    guardar_estado(estado, path)


def incrementar_intentos_fallidos(path: Path = ESTADO_PATH) -> int:
    """
    Incrementa el contador de intentos fallidos.
    Devuelve el nuevo valor del contador.
    """
    estado = leer_estado(path)
    estado["intentos_fallidos"] = estado.get("intentos_fallidos", 0) + 1
    guardar_estado(estado, path)
    return estado["intentos_fallidos"]


def resetear_intentos(path: Path = ESTADO_PATH) -> None:
    """Resetea el contador de intentos fallidos a 0."""
    estado = leer_estado(path)
    estado["intentos_fallidos"] = 0
    guardar_estado(estado, path)


def mes_ya_procesado(mes_texto: str, path: Path = ESTADO_PATH) -> bool:
    """Devuelve True si el mes ya fue procesado en un ciclo anterior."""
    estado = leer_estado(path)
    return mes_texto in estado.get("meses_procesados", [])
