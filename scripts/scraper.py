# -*- coding: utf-8 -*-
"""
scraper.py — Navega el sitio del BCRA y detecta el último mes publicado con .7z.

Exporta:
    find_latest_month_with_files()  -> (mes_texto, link_pdf, link_7z)
    find_7z_for_month(target_text)  -> (mes_real, link_7z)
"""

import re
import asyncio
from pathlib import Path

import nest_asyncio
from playwright.async_api import async_playwright

nest_asyncio.apply()

BCRA_URL = "https://www.bcra.gob.ar/informacion-sobre-entidades-financieras/"


# ---------------------------------------------------------------------------
# Helpers internos de navegación
# ---------------------------------------------------------------------------

async def _safe_select(page, sel, value: str) -> None:
    """Selecciona una opción del dropdown y espera que cargue la página."""
    try:
        async with page.expect_navigation(wait_until="networkidle", timeout=6000):
            await sel.select_option(value)
    except Exception:
        await sel.select_option(value)
        await page.wait_for_timeout(1200)
        await page.wait_for_load_state("networkidle")


async def _get_month_selector(page):
    """Devuelve el <select> principal de meses (el que tiene >= 6 opciones)."""
    selects = page.locator("select")
    if await selects.count() == 0:
        raise RuntimeError("No encontré el dropdown (<select>) de meses en el sitio del BCRA.")
    for i in range(await selects.count()):
        cand = selects.nth(i)
        if await cand.locator("option").count() >= 6:
            return cand
    return selects.first


async def _get_links_on_page(page) -> tuple[str | None, str | None]:
    """
    Escanea todos los <a> de la página y devuelve (link_pdf, link_7z).
    Prioriza el .7z etiquetado "Datos Abiertos".
    """
    a_loc = page.locator("a")
    n = await a_loc.count()
    pdfs, z7s = [], []

    for i in range(n):
        href = await a_loc.nth(i).get_attribute("href")
        if not href:
            continue
        txt = ((await a_loc.nth(i).inner_text()) or "").strip()
        if href.startswith("/"):
            href = "https://www.bcra.gob.ar" + href

        if href.lower().endswith(".7z"):
            z7s.append((href, txt))
        if href.lower().endswith(".pdf"):
            pdfs.append((href, txt))

    link_7z = None
    for href, txt in z7s:
        if re.search(r"Datos Abiertos", txt, re.I):
            link_7z = href
            break
    if not link_7z and z7s:
        link_7z = z7s[0][0]

    link_pdf = None
    for href, txt in pdfs:
        if re.search(r"informaci[oó]n de entidades financieras", txt, re.I) and \
           not re.search(r"ayuda", txt, re.I):
            link_pdf = href
            break
    if not link_pdf:
        for href, txt in pdfs:
            if not re.search(r"ayuda", txt, re.I):
                link_pdf = href
                break

    return link_pdf, link_7z


async def _get_all_options(page) -> list[dict]:
    """Devuelve todas las opciones del selector de meses como lista de dicts."""
    sel = await _get_month_selector(page)
    options = await sel.locator("option").evaluate_all(
        "opts => opts.map(o => ({value: o.value, text: (o.textContent||'').trim()}))"
    )
    return [
        o for o in options
        if o["value"] and o["text"] and o["text"].lower() != "seleccionar"
    ]


# ---------------------------------------------------------------------------
# API pública
# ---------------------------------------------------------------------------

async def find_latest_month_with_files() -> tuple[str | None, str | None, str | None]:
    """
    Recorre los meses del dropdown (del más nuevo al más viejo) y devuelve
    el primero que tenga un .7z publicado.

    Returns:
        (mes_texto, link_pdf, link_7z)
    """
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()
        await page.goto(BCRA_URL, wait_until="networkidle")

        sel = await _get_month_selector(page)
        options = await _get_all_options(page)

        for opt in options:
            await _safe_select(page, sel, opt["value"])
            link_pdf, link_7z = await _get_links_on_page(page)
            if link_7z:
                await browser.close()
                return opt["text"], link_pdf, link_7z

        await browser.close()
        return None, None, None


async def find_7z_for_month(target_text: str) -> tuple[str | None, str | None]:
    """
    Selecciona un mes específico del dropdown y devuelve (mes_real, link_7z).
    """
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()
        await page.goto(BCRA_URL, wait_until="networkidle")

        sel = await _get_month_selector(page)
        options = await _get_all_options(page)

        match = None
        for opt in options:
            if target_text.lower() in opt["text"].lower():
                match = opt
                break

        if match is None:
            await browser.close()
            raise ValueError(
                f"No encontré '{target_text}' en el dropdown. "
                f"Opciones disponibles: {[o['text'] for o in options]}"
            )

        await _safe_select(page, sel, match["value"])
        _, link_7z = await _get_links_on_page(page)
        await browser.close()

        return match["text"], link_7z


def get_latest_month() -> tuple[str | None, str | None, str | None]:
    """Wrapper síncrono para find_latest_month_with_files()."""
    return asyncio.run(find_latest_month_with_files())


def get_month_link(target_text: str) -> tuple[str | None, str | None]:
    """Wrapper síncrono para find_7z_for_month()."""
    return asyncio.run(find_7z_for_month(target_text))
