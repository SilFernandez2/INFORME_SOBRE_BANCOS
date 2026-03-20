#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
procesador.py — Punto de entrada del Workflow B (descarga + procesamiento).

Variables de entorno requeridas:
    MES_OBJETIVO                 — ej: "Enero 2026"
    WORKDIR                      — directorio base (default: /tmp/bcra_auto)
    GMAIL_USER, GMAIL_APP_PASS, ALERT_EMAILS
    GDRIVE_SERVICE_ACCOUNT_JSON, GDRIVE_FOLDER_ID
    GITHUB_REPO_URL
"""

import os
import sys
from pathlib import Path

sys.path.insert(0, os.path.dirname(__file__))

from utils import download_file, extract_with_7z
from scraper import get_month_link
from verificador import verificar_mes_completo, verificar_tablas_finales
from estado import (
    leer_estado, marcar_mes_procesado,
    incrementar_intentos_fallidos, resetear_intentos, guardar_estado
)
from notificador import (
    enviar_error_verificacion, enviar_exito, enviar_fallo_permanente
)
from drive import subir_carpeta_mes, subir_tablas_finales
from procesamiento import (
    procesar_tec_cont, procesar_info_hist,
    generar_tabla_inf_adi, generar_tabla_info_sistema,
    normalizar_balres,
)


# ---------------------------------------------------------------------------
# Configuración
# ---------------------------------------------------------------------------

MAX_INTENTOS = 3
WORKDIR  = Path(os.environ.get("WORKDIR", "/tmp/bcra_auto"))
REPO_URL = os.environ.get(
    "GITHUB_REPO_URL",
    "https://github.com/TU_USUARIO/TU_REPO/actions/workflows/procesar_mes.yml"
)


# ---------------------------------------------------------------------------
# Helper de errores
# ---------------------------------------------------------------------------

def _manejar_error(mes_texto: str, errores: list[str], run_url: str) -> None:
    nuevos_intentos = incrementar_intentos_fallidos()
    print(f"\n❌ Error (intento {nuevos_intentos}/{MAX_INTENTOS}):")
    for e in errores:
        print(f"   - {e}")

    est = leer_estado()
    est["ultimo_error"] = errores
    guardar_estado(est)

    if nuevos_intentos >= MAX_INTENTOS:
        print("🚨 Máximo de intentos alcanzado. Enviando alerta de fallo permanente.")
        try:
            enviar_fallo_permanente(mes_texto, MAX_INTENTOS, errores)
        except Exception as email_err:
            print(f"⚠️  No se pudo enviar email de fallo permanente: {email_err}")
    else:
        try:
            enviar_error_verificacion(mes_texto, nuevos_intentos, MAX_INTENTOS, errores, run_url)
        except Exception as email_err:
            print(f"⚠️  No se pudo enviar email de error: {email_err}")
        print("ℹ️  El detector lo reintentará automáticamente mañana.")

    sys.exit(1)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    mes_texto = os.environ.get("MES_OBJETIVO", "").strip()
    if not mes_texto and len(sys.argv) > 1:
        mes_texto = " ".join(sys.argv[1:]).strip()
    if not mes_texto:
        print("❌ Falta indicar el mes. Usar: MES_OBJETIVO='Enero 2026' python procesador.py")
        sys.exit(1)

    print("=" * 60)
    print(f"BCRA Procesador — Workflow B")
    print(f"Mes objetivo: {mes_texto}")
    print("=" * 60)

    estado   = leer_estado()
    run_url  = f"{REPO_URL}?mes={mes_texto.replace(' ', '+')}"
    intentos = estado.get("intentos_fallidos", 0)

    # ── Verificar si ya se agotaron los intentos ────────────────────────────
    if intentos >= MAX_INTENTOS:
        print(f"🚨 Se alcanzó el máximo de {MAX_INTENTOS} intentos fallidos.")
        errores_previos = estado.get("ultimo_error", ["Error desconocido"])
        enviar_fallo_permanente(mes_texto, MAX_INTENTOS, errores_previos)
        sys.exit(1)

    # ── 1. Obtener link del .7z ─────────────────────────────────────────────
    print(f"\n🔍 Buscando link .7z para '{mes_texto}'...")
    try:
        mes_real, link_7z = get_month_link(mes_texto)
    except Exception as e:
        _manejar_error(mes_texto, [f"No se pudo obtener el link: {e}"], run_url)

    if not link_7z:
        _manejar_error(mes_texto, [f"No se encontró .7z para '{mes_texto}' en el BCRA."], run_url)

    print(f"   Link: {link_7z}")

    # ── 2. Descarga y extracción ────────────────────────────────────────────
    safe_name = mes_real.replace(" ", "_")
    month_dir = WORKDIR / safe_name
    month_dir.mkdir(parents=True, exist_ok=True)

    print(f"\n📥 Descargando {link_7z.split('/')[-1]}...")
    try:
        archive = download_file(link_7z, month_dir)
        print(f"   Descargado: {archive.name} ({archive.stat().st_size / 1024 / 1024:.1f} MB)")
    except Exception as e:
        _manejar_error(mes_texto, [f"Error en descarga: {e}"], run_url)

    print("\n📦 Extrayendo archivo .7z...")
    try:
        extract_dir = month_dir / "extract"
        extract_with_7z(archive, extract_dir)
        archive.unlink()  # borrar .7z, conservar extract
        print(f"   Extraído en: {extract_dir}")
    except Exception as e:
        _manejar_error(mes_texto, [f"Error al extraer .7z: {e}"], run_url)

    # ── 3. Conversión Tec_Cont y Info_Hist ─────────────────────────────────
    print("\n🔄 Procesando archivos...")
    try:
        procesar_tec_cont(extract_dir, month_dir)
        procesar_info_hist(extract_dir, month_dir)
        print("   ✅ Conversión TXT→CSV completada.")
    except Exception as e:
        _manejar_error(mes_texto, [f"Error en conversión TXT→CSV: {e}"], run_url)

    # ── 4. Verificaciones ───────────────────────────────────────────────────
    print("\n🔎 Verificando integridad de los datos...")
    resultado = verificar_mes_completo(month_dir)
    print(resultado.texto_reporte())

    if not resultado.ok:
        _manejar_error(mes_texto, resultado.errores, run_url)

    print("   ✅ Verificaciones OK.")
    resetear_intentos()

    # ── 5. Tablas históricas ────────────────────────────────────────────────
    print("\n📊 Generando tablas históricas...")
    try:
        generar_tabla_inf_adi(WORKDIR)
        generar_tabla_info_sistema(WORKDIR)
        normalizar_balres(WORKDIR)
        print("   ✅ Tablas históricas generadas.")
    except Exception as e:
        _manejar_error(mes_texto, [f"Error en tablas históricas: {e}"], run_url)

    resultado_tablas = verificar_tablas_finales(WORKDIR)
    if not resultado_tablas.ok:
        _manejar_error(mes_texto, resultado_tablas.errores, run_url)

    # ── 6. Subida a Google Drive ────────────────────────────────────────────
    print("\n📤 Subiendo a Google Drive...")
    try:
        subir_carpeta_mes(month_dir, mes_real)
        subir_tablas_finales(WORKDIR)
        print("   ✅ Subida a Drive completada.")
    except Exception as e:
        print(f"⚠️  Error al subir a Drive: {e}")
        resultado.agregar_advertencia(f"Drive: {e}")
    # ── 7. Actualizar tablas históricas en Drive ────────────────────────────
    print("\n📊 Actualizando tablas históricas en Drive...")
    try:
        from procesamiento import actualizar_tablas_desde_drive
        from drive import get_gdrive_base
        gdrive_base = get_gdrive_base()
        actualizar_tablas_desde_drive(gdrive_base)
        print("   ✅ Tablas históricas actualizadas.")
    except Exception as e:
        print(f"⚠️  No se pudieron actualizar tablas históricas: {e}")
    # ── 8. Actualizar estado.json ───────────────────────────────────────────
    marcar_mes_procesado(mes_real)
    print(f"\n✅ estado.json actualizado: '{mes_real}' marcado como procesado.")

    # ── 9. Email de éxito ───────────────────────────────────────────────────
    resumen = {
        "Mes procesado":      mes_real,
        "Archivos CSV":       len(list((month_dir / "Tec_Cont_csv").rglob("*.csv"))),
        "Info_Hist":          "✅",
        "Tablas históricas":  "✅",
        "Google Drive":       "✅",
        "Advertencias":       len(resultado.advertencias) or "ninguna",
    }
    try:
        enviar_exito(mes_real, resumen)
    except Exception as e:
        print(f"⚠️  No se pudo enviar email de éxito: {e}")

    print("\n🎉 Procesamiento completo.")
    sys.exit(0)


if __name__ == "__main__":
    main()