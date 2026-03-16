# 🏦 BCRA — Automatización Informes Entidades Financieras

Automatización completa de descarga, procesamiento y actualización mensual de los informes sobre entidades financieras publicados por el BCRA.

---

## ¿Qué hace?

El sistema consulta automáticamente el sitio del BCRA, detecta cuando se publica un nuevo mes y envía una alerta por email. Al confirmar, descarga el archivo, lo procesa y guarda los resultados en Google Drive.

```
Cada día hábil (lun-vie), del 1 al 15 de cada mes:

  GitHub Actions consulta el BCRA
       │
       ├── No hay nada nuevo → termina, reintenta mañana
       │
       └── Nuevo mes detectado → email de alerta a los destinatarios
                                        │
                              alguien aprueba con un click
                                        │
                              descarga + procesamiento automático
                                        │
                              archivos en Google Drive + email de confirmación
```

---

## Estructura del repositorio

```
├── .github/
│   └── workflows/
│       ├── detector.yml        # Cron diario — busca nuevo mes
│       ├── procesar_mes.yml    # Manual — descarga y procesa
│       └── setup_base.yml      # Una sola vez — arma la base histórica
├── scripts/
│   ├── detector.py             # Entry point Workflow A
│   ├── procesador.py           # Entry point Workflow B
│   ├── procesamiento.py        # Programas 2, 3, 4 y 5
│   ├── scraper.py              # Scraping del sitio BCRA
│   ├── verificador.py          # Verificaciones de integridad
│   ├── notificador.py          # Emails via Gmail
│   ├── drive.py                # Google Drive (subida y bajada)
│   ├── estado.py               # Manejo de estado.json
│   └── utils.py                # Utilidades compartidas
├── estado.json                 # Registro del último mes procesado
└── README.md
```

---

## Archivos que genera por cada mes

```
Google Drive → bcra_data/
├── <Mes_Año>/
│   ├── Tec_Cont_csv/
│   │   ├── balres/                    ← CSV por entidad
│   │   ├── balres_corregido/          ← montos ÷ 1000 en CSV
│   │   ├── balres_corregido_excel/    ← montos ÷ 1000 en Excel (Power BI)
│   │   ├── indicad/
│   │   ├── inf_adi/
│   │   └── ... (otras carpetas)
│   └── Info_Hist_csv/
│       ├── info_hist_activas.csv
│       └── info_hist_bajas.csv
└── tablas_historicas/
    ├── inf_adi_cantidad_cuentas_stock.csv
    ├── inf_adi_cantidad_cuentas_stock.xlsx
    ├── info_sistema_hist.csv
    └── info_sistema_hist.xlsx
```

---

## Setup inicial (una sola vez)

### 1. Clonar el repositorio

```bash
git clone https://github.com/TU_USUARIO/TU_REPO.git
```

### 2. Configurar Google Drive

1. Ir a [Google Cloud Console](https://console.cloud.google.com) y crear un proyecto
2. Habilitar la **Google Drive API**
3. Crear una **Service Account** y descargar el JSON de credenciales
4. Crear una carpeta `bcra_data` en tu Google Drive
5. Compartir esa carpeta con el email de la service account (rol Editor)
6. Copiar el ID de la carpeta (el string largo en la URL de Drive)

### 3. Configurar Gmail

1. Activar la verificación en dos pasos en tu cuenta Gmail
2. Ir a [Contraseñas de aplicación](https://myaccount.google.com/apppasswords)
3. Crear una contraseña nueva → nombre: `bcra-bot`
4. Guardar la contraseña de 16 letras generada

### 4. Cargar los secrets en GitHub

Ir a **Settings → Secrets and variables → Actions → New repository secret**

| Secret | Descripción |
|--------|-------------|
| `GMAIL_USER` | Tu dirección Gmail (ej: usuario@gmail.com) |
| `GMAIL_APP_PASS` | Contraseña de app de 16 letras sin espacios |
| `ALERT_EMAILS` | Destinatarios separados por coma: `a@x.com,b@x.com` |
| `GDRIVE_SERVICE_ACCOUNT_JSON` | Contenido completo del JSON de la service account |
| `GDRIVE_FOLDER_ID` | ID de la carpeta bcra_data en Drive |

### 5. Armar la base histórica

La primera vez hay que descargar todos los meses históricos. Esto se hace desde **Google Colab** usando el notebook `BCRA_automatizacion_completa.ipynb` (no incluido en este repo — ver documentación).

Una vez armada la base en Drive, el sistema queda listo para correr automáticamente.

---

## Operación mensual

El sistema funciona solo una vez configurado:

- **Workflow A** (`detector.yml`) corre automáticamente cada día hábil del 1 al 15
- Al detectar un mes nuevo, envía email de alerta con link para aprobar
- Cualquier destinatario puede hacer click en el link para disparar el **Workflow B**
- **Workflow B** descarga, procesa, verifica y sube a Drive
- Al terminar envía email de confirmación

### Reintentar manualmente

Si un procesamiento falló:

1. Ir a **Actions → BCRA — Procesar mes**
2. Click en **Run workflow**
3. Escribir el mes (ej: `Febrero 2026`)
4. Si querés ignorar el contador de fallos: marcar `forzar = true`

---

## Emails que envía el sistema

| Email | Cuándo |
|-------|--------|
| 📦 Nuevo mes disponible | Se detectó un .7z nuevo en el BCRA |
| ✅ Procesamiento completo | El mes fue procesado y subido a Drive correctamente |
| ❌ Error en verificación | Algo falló — incluye detalle del error |
| 🚨 Fallo permanente | Se agotaron los 3 intentos — requiere revisión manual |

---

## Notas técnicas

- **Zona horaria**: el cron corre en UTC. El script ajusta internamente a UTC-3 (Argentina) para validar el rango de días 1-15.
- **Reintentos**: máximo 3 intentos fallidos por mes. Al tercer fallo se envía alerta especial y el proceso se pausa hasta intervención manual.
- **Idempotencia**: si un mes ya figura en `estado.json`, el detector no lo vuelve a procesar aunque lo encuentre en el BCRA.
- **Commits automáticos**: los cambios en `estado.json` usan `[skip ci]` para no disparar workflows recursivamente.
- **Espacio en disco**: los runners de GitHub Actions tienen ~14 GB libres. Un año de datos del BCRA ocupa aproximadamente 3-5 GB procesados.

---

## Dependencias

```
playwright
requests
pandas
openpyxl
pdfplumber
nest_asyncio
google-api-python-client
google-auth
```

Sistema: `p7zip-full`, `chromium` (instalados automáticamente por los workflows).
