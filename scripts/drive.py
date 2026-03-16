# -*- coding: utf-8 -*-
"""
drive.py — Subida y bajada de archivos a Google Drive via Service Account.

Variables de entorno requeridas:
    GDRIVE_SERVICE_ACCOUNT_JSON  — contenido del JSON de la service account
    GDRIVE_FOLDER_ID             — ID de la carpeta raíz en Drive
"""

import json
import os
import io
from pathlib import Path

from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload, MediaIoBaseDownload


SCOPES = ["https://www.googleapis.com/auth/drive"]


# ---------------------------------------------------------------------------
# Autenticación
# ---------------------------------------------------------------------------

def _get_service():
    """Crea el cliente de Drive autenticado con la service account."""
    raw = os.environ.get("GDRIVE_SERVICE_ACCOUNT_JSON")
    if not raw:
        raise EnvironmentError(
            "Falta la variable de entorno GDRIVE_SERVICE_ACCOUNT_JSON."
        )
    info = json.loads(raw)
    creds = service_account.Credentials.from_service_account_info(info, scopes=SCOPES)
    return build("drive", "v3", credentials=creds)


def _root_folder_id() -> str:
    fid = os.environ.get("GDRIVE_FOLDER_ID")
    if not fid:
        raise EnvironmentError("Falta la variable de entorno GDRIVE_FOLDER_ID.")
    return fid


# ---------------------------------------------------------------------------
# Helpers de carpetas
# ---------------------------------------------------------------------------

def _get_or_create_folder(service, name: str, parent_id: str) -> str:
    """Devuelve el ID de una subcarpeta en Drive. La crea si no existe."""
    query = (
        f"name='{name}' and mimeType='application/vnd.google-apps.folder' "
        f"and '{parent_id}' in parents and trashed=false"
    )
    results = service.files().list(q=query, fields="files(id,name)").execute()
    files = results.get("files", [])
    if files:
        return files[0]["id"]

    meta = {
        "name": name,
        "mimeType": "application/vnd.google-apps.folder",
        "parents": [parent_id],
    }
    folder = service.files().create(body=meta, fields="id").execute()
    return folder["id"]


# ---------------------------------------------------------------------------
# Subida
# ---------------------------------------------------------------------------

def subir_archivo(local_path: Path, drive_folder_id: str, nombre_drive: str | None = None) -> str:
    """Sube un archivo a Drive. Si ya existe lo reemplaza. Devuelve el file ID."""
    service = _get_service()
    nombre = nombre_drive or local_path.name

    query = f"name='{nombre}' and '{drive_folder_id}' in parents and trashed=false"
    results = service.files().list(q=query, fields="files(id,name)").execute()
    existing = results.get("files", [])

    media = MediaFileUpload(str(local_path), resumable=True)

    if existing:
        file_id = existing[0]["id"]
        service.files().update(fileId=file_id, media_body=media).execute()
        print(f"  ↺  Actualizado en Drive: {nombre}")
    else:
        meta = {"name": nombre, "parents": [drive_folder_id]}
        f = service.files().create(body=meta, media_body=media, fields="id").execute()
        file_id = f["id"]
        print(f"  ↑  Subido a Drive: {nombre}")

    return file_id


def subir_carpeta_mes(month_dir: Path, mes_texto: str) -> None:
    """Sube todos los CSV/XLSX/TXT de la carpeta de un mes a Drive."""
    service = _get_service()
    root_id = _root_folder_id()

    bcra_id = _get_or_create_folder(service, "bcra_data", root_id)
    mes_id  = _get_or_create_folder(service, mes_texto, bcra_id)

    extensiones = {".csv", ".xlsx", ".txt"}
    archivos = [
        p for p in month_dir.rglob("*")
        if p.is_file() and p.suffix.lower() in extensiones
    ]

    print(f"\n📤 Subiendo {len(archivos)} archivos de '{mes_texto}' a Drive...")

    for archivo in sorted(archivos):
        rel = archivo.relative_to(month_dir)
        parent_id = mes_id
        for parte in rel.parts[:-1]:
            parent_id = _get_or_create_folder(service, parte, parent_id)
        subir_archivo(archivo, parent_id)

    print(f"✅ Subida completa → Drive/bcra_data/{mes_texto}/")


def subir_tablas_finales(base_dir: Path) -> None:
    """Sube las tablas históricas consolidadas a Drive/bcra_data/tablas_historicas/."""
    service  = _get_service()
    root_id  = _root_folder_id()
    bcra_id  = _get_or_create_folder(service, "bcra_data", root_id)
    tablas_id = _get_or_create_folder(service, "tablas_historicas", bcra_id)

    archivos_objetivo = [
        base_dir / "inf_adi_cantidad_cuentas_stock.csv",
        base_dir / "inf_adi_cantidad_cuentas_stock.xlsx",
        base_dir / "Info_sistema_hist" / "info_sistema_hist.csv",
        base_dir / "Info_sistema_hist" / "info_sistema_hist.xlsx",
    ]

    print("\n📤 Subiendo tablas históricas a Drive...")
    for path in archivos_objetivo:
        if path.exists():
            subir_archivo(path, tablas_id)
        else:
            print(f"  ⚠️  No encontrado: {path}")


# ---------------------------------------------------------------------------
# Descarga
# ---------------------------------------------------------------------------

def descargar_carpetas_mis(destino_base: Path) -> None:
    """Descarga todos los archivos de bcra_data/ de Drive al runner."""
    service = _get_service()
    root_id = _root_folder_id()

    def _list_files(parent_id: str) -> list[dict]:
        results = service.files().list(
            q=f"'{parent_id}' in parents and trashed=false",
            fields="files(id,name,mimeType)",
            pageSize=1000,
        ).execute()
        return results.get("files", [])

    def _descargar_recursivo(folder_id: str, local_dir: Path) -> None:
        local_dir.mkdir(parents=True, exist_ok=True)
        for item in _list_files(folder_id):
            if item["mimeType"] == "application/vnd.google-apps.folder":
                _descargar_recursivo(item["id"], local_dir / item["name"])
            else:
                local_path = local_dir / item["name"]
                if local_path.exists():
                    continue
                req = service.files().get_media(fileId=item["id"])
                buf = io.BytesIO()
                dl  = MediaIoBaseDownload(buf, req)
                done = False
                while not done:
                    _, done = dl.next_chunk()
                with open(local_path, "wb") as f:
                    f.write(buf.getvalue())
                print(f"  ↓  Descargado: {local_path}")

    query = f"name='bcra_data' and '{root_id}' in parents and trashed=false"
    res   = service.files().list(q=query, fields="files(id)").execute()
    archivos = res.get("files", [])
    if not archivos:
        print("⚠️  No se encontró la carpeta bcra_data en Drive.")
        return

    bcra_id = archivos[0]["id"]
    print(f"\n📥 Descargando datos históricos de Drive → {destino_base}...")
    _descargar_recursivo(bcra_id, destino_base)
    print("✅ Descarga completa.")