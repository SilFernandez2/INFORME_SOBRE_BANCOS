# -*- coding: utf-8 -*-
"""
notificador.py — Envío de emails via Gmail (contraseña de app).

Variables de entorno requeridas:
    GMAIL_USER      — dirección Gmail del remitente
    GMAIL_APP_PASS  — contraseña de aplicación (16 caracteres sin espacios)
    ALERT_EMAILS    — lista separada por comas: "a@x.com,b@x.com"
"""

import os
import smtplib
import textwrap
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText


# ---------------------------------------------------------------------------
# Config desde entorno
# ---------------------------------------------------------------------------

def _get_config() -> dict:
    user  = os.environ.get("GMAIL_USER", "")
    pwd   = os.environ.get("GMAIL_APP_PASS", "")
    dests = [e.strip() for e in os.environ.get("ALERT_EMAILS", user).split(",") if e.strip()]
    if not user or not pwd:
        raise EnvironmentError(
            "Faltan variables de entorno GMAIL_USER y/o GMAIL_APP_PASS. "
            "Configurarlas como secrets en el repositorio de GitHub."
        )
    return {"user": user, "pwd": pwd, "dests": dests}


# ---------------------------------------------------------------------------
# Envío base
# ---------------------------------------------------------------------------

def _enviar(asunto: str, cuerpo_html: str, cuerpo_txt: str) -> None:
    cfg = _get_config()
    msg = MIMEMultipart("alternative")
    msg["Subject"] = asunto
    msg["From"]    = cfg["user"]
    msg["To"]      = ", ".join(cfg["dests"])

    msg.attach(MIMEText(cuerpo_txt, "plain", "utf-8"))
    msg.attach(MIMEText(cuerpo_html, "html",  "utf-8"))

    with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:
        server.login(cfg["user"], cfg["pwd"])
        server.sendmail(cfg["user"], cfg["dests"], msg.as_string())

    print(f"📧 Email enviado a: {', '.join(cfg['dests'])}")
    print(f"   Asunto: {asunto}")


# ---------------------------------------------------------------------------
# Templates de email
# ---------------------------------------------------------------------------

def enviar_alerta_nuevo_mes(mes_texto: str, link_7z: str, run_url: str) -> None:
    """Alerta cuando se detecta un nuevo .7z en el BCRA."""
    asunto = f"[BCRA] 📦 Nuevo mes disponible: {mes_texto}"
    html = f"""
    <div style="font-family:Arial,sans-serif;max-width:600px;margin:0 auto">
      <h2 style="color:#1a56db">📦 Nuevo informe BCRA disponible</h2>
      <p>Se detectó un nuevo archivo publicado en el sitio del BCRA.</p>
      <table style="border-collapse:collapse;width:100%">
        <tr><td style="padding:8px;font-weight:bold">Mes</td>
            <td style="padding:8px">{mes_texto}</td></tr>
        <tr style="background:#f3f4f6">
            <td style="padding:8px;font-weight:bold">Archivo .7z</td>
            <td style="padding:8px"><a href="{link_7z}">{link_7z.split('/')[-1]}</a></td></tr>
      </table>
      <br>
      <p>Para iniciar la descarga y procesamiento, hacé click en el botón:</p>
      <a href="{run_url}"
         style="display:inline-block;background:#1a56db;color:#fff;padding:12px 24px;
                text-decoration:none;border-radius:6px;font-weight:bold">
        ▶ Iniciar descarga en GitHub Actions
      </a>
      <br><br>
      <p style="color:#6b7280;font-size:12px">
        Este link lleva directo a GitHub Actions. Cualquier miembro del equipo
        puede disparar el workflow desde ahí.
      </p>
    </div>
    """
    txt = textwrap.dedent(f"""
        Nuevo informe BCRA disponible
        ==============================
        Mes:      {mes_texto}
        Archivo:  {link_7z}

        Para iniciar la descarga:
        {run_url}
    """)
    _enviar(asunto, html, txt)


def enviar_error_verificacion(
    mes_texto: str,
    intento: int,
    max_intentos: int,
    errores: list[str],
    run_url: str,
) -> None:
    """Notifica un fallo en las verificaciones de datos."""
    asunto = f"[BCRA] ❌ Error al procesar {mes_texto} (intento {intento}/{max_intentos})"
    errores_html = "".join(f"<li>{e}</li>" for e in errores)
    accion = (
        "El sistema <strong>reintentará automáticamente mañana</strong>."
        if intento < max_intentos
        else "Se alcanzó el <strong>máximo de reintentos</strong>. "
             "Revisá manualmente el archivo en el sitio del BCRA."
    )
    html = f"""
    <div style="font-family:Arial,sans-serif;max-width:600px;margin:0 auto">
      <h2 style="color:#dc2626">❌ Error en verificación de datos BCRA</h2>
      <p>El procesamiento de <strong>{mes_texto}</strong> falló en el intento
         <strong>{intento} de {max_intentos}</strong>.</p>
      <h3>Errores detectados:</h3>
      <ul style="color:#dc2626">{errores_html}</ul>
      <h3>¿Qué hacer?</h3>
      <p>{accion}</p>
      <a href="{run_url}"
         style="display:inline-block;background:#dc2626;color:#fff;padding:12px 24px;
                text-decoration:none;border-radius:6px;font-weight:bold">
        🔁 Reintentar manualmente ahora
      </a>
      <br><br>
      <p style="color:#6b7280;font-size:12px">
        Si el error persiste luego de {max_intentos} intentos, puede ser un problema
        en el archivo original del BCRA. Verificar en:<br>
        <a href="https://www.bcra.gob.ar/informacion-sobre-entidades-financieras/">
          bcra.gob.ar — Información sobre Entidades Financieras
        </a>
      </p>
    </div>
    """
    txt = textwrap.dedent(f"""
        Error al procesar {mes_texto} (intento {intento}/{max_intentos})
        Errores:
        {"".join(f'  - {e}' + chr(10) for e in errores)}
        Reintentar: {run_url}
    """)
    _enviar(asunto, html, txt)


def enviar_exito(mes_texto: str, resumen: dict) -> None:
    """Confirma que el procesamiento del mes terminó sin errores."""
    asunto = f"[BCRA] ✅ Procesamiento completo: {mes_texto}"
    items_html = "".join(
        f"<tr{'style=background:#f3f4f6' if i%2 else ''}>"
        f"<td style='padding:6px 8px;font-weight:bold'>{k}</td>"
        f"<td style='padding:6px 8px'>{v}</td></tr>"
        for i, (k, v) in enumerate(resumen.items())
    )
    html = f"""
    <div style="font-family:Arial,sans-serif;max-width:600px;margin:0 auto">
      <h2 style="color:#16a34a">✅ Procesamiento BCRA completado</h2>
      <p>El mes <strong>{mes_texto}</strong> fue descargado, verificado y
         subido a Google Drive correctamente.</p>
      <h3>Resumen:</h3>
      <table style="border-collapse:collapse;width:100%">{items_html}</table>
      <br>
      <p style="color:#6b7280;font-size:12px">
        Los archivos están disponibles en Google Drive en la carpeta
        <strong>bcra_data/{mes_texto}</strong>.
      </p>
    </div>
    """
    txt = textwrap.dedent(f"""
        Procesamiento BCRA completado: {mes_texto}
        {chr(10).join(f'  {k}: {v}' for k, v in resumen.items())}
    """)
    _enviar(asunto, html, txt)


def enviar_fallo_permanente(mes_texto: str, max_intentos: int, errores: list[str]) -> None:
    """Alerta cuando se agotaron todos los intentos."""
    asunto = f"[BCRA] 🚨 Fallo permanente en {mes_texto} — intervención manual requerida"
    errores_html = "".join(f"<li>{e}</li>" for e in errores)
    html = f"""
    <div style="font-family:Arial,sans-serif;max-width:600px;margin:0 auto">
      <h2 style="color:#7c3aed">🚨 Fallo permanente — acción requerida</h2>
      <p>Se agotaron los <strong>{max_intentos} intentos</strong> de procesar
         <strong>{mes_texto}</strong> sin éxito.</p>
      <h3>Errores registrados:</h3>
      <ul>{errores_html}</ul>
      <h3>Próximos pasos sugeridos:</h3>
      <ol>
        <li>Verificar el archivo .7z descargando manualmente desde el BCRA.</li>
        <li>Revisar si hay un error en los datos de alguna entidad específica.</li>
        <li>Si el archivo del BCRA está bien, reiniciar el proceso manualmente.</li>
      </ol>
      <a href="https://www.bcra.gob.ar/informacion-sobre-entidades-financieras/"
         style="display:inline-block;background:#7c3aed;color:#fff;padding:12px 24px;
                text-decoration:none;border-radius:6px;font-weight:bold">
        🔍 Ver sitio BCRA
      </a>
    </div>
    """
    txt = textwrap.dedent(f"""
        FALLO PERMANENTE — {mes_texto} ({max_intentos} intentos)
        {chr(10).join(f'  - {e}' for e in errores)}
        Revisar: https://www.bcra.gob.ar/informacion-sobre-entidades-financieras/
    """)
    _enviar(asunto, html, txt)
    