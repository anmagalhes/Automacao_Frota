# ocr_pdf.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import os
import time
from pathlib import Path
from typing import Optional

import requests
import pdfplumber

OCR_SPACE_APIKEY_DEFAULT = os.environ.get("OCR_SPACE_APIKEY", "helloworld")
OCR_TIMEOUT = 90
OCR_SLEEP_FREE = 1.2  # evita throttle na key free
OCR_MAX_RETRY = 2
OCR_ENGINE = 2  # 1=legacy 2=moderno


def extrair_texto_ocr_space(caminho_pdf: Path, apikey: Optional[str] = None) -> str:
    apikey = apikey or OCR_SPACE_APIKEY_DEFAULT
    url = "https://api.ocr.space/parse/image"
    for tentativa in range(1, OCR_MAX_RETRY + 1):
        try:
            with open(caminho_pdf, "rb") as f:
                resp = requests.post(
                    url,
                    files={"file": f},
                    data={
                        "apikey": apikey,
                        "language": "por",
                        "isOverlayRequired": False,
                        "OCREngine": OCR_ENGINE,
                    },
                    timeout=OCR_TIMEOUT,
                )
            j = resp.json()
            if j.get("IsErroredOnProcessing"):
                print(f"[⚠] OCR erro (tentativa {tentativa}): {j.get('ErrorMessage')}")
            parsed = j.get("ParsedResults") or []
            if parsed:
                texto = "\n".join([r.get("ParsedText", "") for r in parsed])
                if apikey == "helloworld":
                    time.sleep(OCR_SLEEP_FREE)
                return texto
        except Exception as e:
            print(f"[⚠] Falha OCR (tentativa {tentativa}): {e}")
            time.sleep(1.0 * tentativa)  # backoff
    return ""


def extrair_texto_pdf(caminho_pdf: Path, apikey: Optional[str] = None, *, force_ocr: bool = False) -> str:
    if force_ocr:
        return extrair_texto_ocr_space(caminho_pdf, apikey=apikey)
    texto = ""
    try:
        with pdfplumber.open(caminho_pdf) as pdf:
            pedacos = []
            for pagina in pdf.pages:
                try:
                    tx = pagina.extract_text() or ""
                    if tx:
                        pedacos.append(tx)
                except Exception as e:
                    print(f"[⚠] Erro extraindo página: {e}")
        texto = "\n".join(pedacos).strip()
        if len(texto) >= 120 and "CID:" not in texto.upper():
            return texto
    except Exception as e:
        print(f"[⚠] Erro pdfplumber: {e}")

    print("[↻] Tentando OCR.Space como fallback...")
    return extrair_texto_ocr_space(caminho_pdf, apikey=apikey)
