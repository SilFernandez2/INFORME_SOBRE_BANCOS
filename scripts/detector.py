#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
detector.py — Punto de entrada del Workflow A (detector diario).

Lógica:
  1. Verifica si hoy cae dentro del rango de búsqueda (día 1 al 15 del mes).
  2. Lee estado.json para saber si el mes esperado ya fue procesado.
  3. Hace scraping del BCRA buscando un nuevo .7z.
  4. Si lo encuentra y es nuevo: envía email de alerta.
  5. Si no lo encuentra: termina silenciosamente.
"""

import os
import sys
from datetime import datetime, timezone, timedelta

sys.path.insert(0, os.path.dirname(__file__))

from estado import leer_estado, mes_ya_procesado
from scraper import get_latest_month
from notificador import enviar_alerta_nuevo_mes


# ---------------------------------------------------------------------------
# Configuración
# ---------------------------------------------------------------------------

DIA_INICIO = 1
DIA_FIN    = 15

REPO_URL = os.environ.get(
    "GITHUB_REPO_URL",
    "https://github.com/TU_USUARIO/TU_REPO/actions/workflows/procesar_mes.yml"
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def esta_en_rango_busqueda() -> bool:
    """Devuelve True si hoy (hora Argentina UTC-3) está entre día 1 y 15."""
    ahora_arg = datetime.now(timezone.utc) - timedelta(hours=3)
    dia = ahora_arg.day
    en_rango = DIA_INICIO <= dia <= DIA_FIN
    if not en_rango:
        print(f"ℹ️  Hoy es día {dia} — fuera del rango ({DIA_INICIO}-{DIA_FIN}). Nada que hacer.")
    return en_rango


def set_github_output(key: str, value: str) -> None:
    """Escribe una variable de salida en $GITHUB_OUTPUT."""
    github_output = os.environ.get("GITHUB_OUTPUT")
    if github_output:
        with open(github_output, "a") as f:
            f.write(f"{key}={value}\n")
    print(f"  output → {key}={value}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    print("=" * 60)
    print("BCRA Detector — Workflow A")
    print("=" * 60)

    # 1. ¿Estamos en el rango de días?
    if not esta_en_rango_busqueda():
        sys.exit(0)

    # 2. Estado actual
    estado = leer_estado()
    ultimo = estado.get("ultimo_mes_procesado")
    print(f"Último mes procesado: {ultimo or '(ninguno)'}")

    # 3. Scraping BCRA
    print("\n🔍 Consultando el sitio del BCRA...")
    try:
        mes_texto, link_pdf, link_7z = get_latest_month()
    except Exception as e:
        print(f"❌ Error al acceder al BCRA: {e}")
        sys.exit(1)

    if not link_7z:
        print("ℹ️  No se encontró ningún .7z disponible.")
        sys.exit(0)

    print(f"   Último mes en BCRA: {mes_texto}")
    print(f"   Link .7z: {link_7z}")

    # 4. ¿Ya procesamos este mes?
    if mes_ya_procesado(mes_texto):
        print(f"✅ '{mes_texto}' ya está en la base. Nada que hacer.")
        sys.exit(0)

    # 5. Nuevo mes — enviar alerta
    print(f"\n🆕 Nuevo mes detectado: {mes_texto}")
    print("📧 Enviando email de alerta...")

    run_url = f"{REPO_URL}?mes={mes_texto.replace(' ', '+')}"

    try:
        enviar_alerta_nuevo_mes(mes_texto, link_7z, run_url)
    except Exception as e:
        print(f"⚠️  No se pudo enviar el email: {e}")

    # 6. Exportar outputs para GitHub Actions
    set_github_output("nuevo_mes", mes_texto)
    set_github_output("link_7z", link_7z)
    set_github_output("nuevo_mes_detectado", "true")

    print(f"\n✅ Detector finalizado. Esperando aprobación para procesar '{mes_texto}'.")


if __name__ == "__main__":
    main()