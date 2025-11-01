# -*- coding: utf-8 -*-
from __future__ import annotations

import os
import time
import threading
from pathlib import Path
from typing import Callable, Dict, List, Optional

import pandas as pd


def _default_normalizar_texto_basico(txt: str | None) -> str:
    """Normalizador padrão (básico e seguro)."""
    if not txt:
        return ""
    return " ".join(txt.replace("\r", " ").replace("\n", " ").split())


class ProcessorThread(threading.Thread):
    """
    Classe UNIFICADA: aceita tanto a assinatura LEGADA quanto a NOVA.

    ─ Modo LEGADO (a sua UI atual):
        ProcessorThread(pasta, apikey, ui, *, saida_excel_path=..., extra_campos=..., ...)

      - 'ui' é um objeto com métodos: msg(str), progresso(pct,[i,n]) e done()
      - Dispatcher é inferido de ui.root.after, se existir.
      - Se não vierem funções/régua injetadas via kwargs, a classe tenta usar o 'core':
            from core import extrair_texto_pdf, extrair_campos_crlv_regex, CAMPOS_PADRAO

    ─ Modo NOVO (recomendado):
        ProcessorThread(
            pasta, apikey,
            *, extrair_texto_pdf=..., extrair_campos_crlv=..., campos_padrao=...,
            on_msg=..., on_progress=..., on_done=..., dispatcher=..., ...
        )
    """

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(daemon=True)

        if len(args) < 2:
            raise TypeError(
                "Uso: ProcessorThread(pasta, apikey, ui=LEGADO...) OU "
                "ProcessorThread(pasta, apikey, *, extrair_texto_pdf=..., extrair_campos_crlv=..., campos_padrao=...)"
            )

        pasta, apikey = args[0], args[1]
        self.pasta = str(pasta)
        self.apikey = (apikey or "").strip()

        # Detecta assinatura nova (keyword-only) pela presença dos 3 obrigatórios
        has_new_keys = all(k in kwargs for k in ("extrair_texto_pdf", "extrair_campos_crlv", "campos_padrao"))
        legacy_signature = len(args) >= 3 and not has_new_keys

        # Defaults / parâmetros opcionais
        self.normalizar_texto_basico = kwargs.get("normalizar_texto_basico", _default_normalizar_texto_basico)
        self.saida_excel_path = str(kwargs.get("saida_excel_path")) if kwargs.get("saida_excel_path") else None
        self.column_map = kwargs.get("column_map") or {}
        self.build_frota_df = kwargs.get("build_frota_df")
        self.write_df_to_existing_template = kwargs.get("write_df_to_existing_template")
        self.excel_sheet_name = kwargs.get("excel_sheet_name", "FROTA-Layout_excel_Geral")
        self.excel_header_row = int(kwargs.get("excel_header_row", 4))
        self.excel_data_start_row = int(kwargs.get("excel_data_start_row", 6))
        self.salvar_texto_bruto_no_excel = bool(kwargs.get("salvar_texto_bruto_no_excel", False))
        self.gerar_csv_falhas = bool(kwargs.get("gerar_csv_falhas", True))
        self.debug_console = bool(kwargs.get("debug_console", False))
        self.debug_log_arquivo = bool(kwargs.get("debug_log_arquivo", False))
        self.debug_salvar_txt = bool(kwargs.get("debug_salvar_txt", False))
        self.debug_text_limit = int(kwargs.get("debug_text_limit", 1200))

        self.cancelado = False
        self._resultados: List[Dict[str, str]] = []
        self._falhas: List[Dict[str, str]] = []
        self._log_fp = None

        # Callbacks / dispatcher (preenchidos adiante)
        self._on_msg = kwargs.get("on_msg")
        self._on_progress = kwargs.get("on_progress")
        self._on_done = kwargs.get("on_done")
        self._dispatcher = kwargs.get("dispatcher")

        # Funções principais & régua
        self.extrair_texto_pdf: Optional[Callable[[str, str], str]] = None
        self.extrair_campos_crlv: Optional[Callable[[str, Dict[str, str]], Dict[str, str]]] = None
        self.campos_padrao: Dict[str, str] = {}

        if legacy_signature:
            # --------- MODO LEGADO ---------
            ui = args[2]

            # callbacks vindos da UI (se disponíveis)
            self._on_msg = getattr(ui, "msg", self._on_msg)
            self._on_progress = getattr(ui, "progresso", self._on_progress)
            self._on_done = getattr(ui, "done", self._on_done)

            # dispatcher padrão via Tkinter .after, se disponível
            if self._dispatcher is None:
                root = getattr(ui, "root", None)
                if root is not None and hasattr(root, "after"):
                    self._dispatcher = lambda delay, fn: root.after(delay, fn)

            # tentar usar funções/régua injetadas; se não houver, tentar módulo core
            self.extrair_texto_pdf = kwargs.get("extrair_texto_pdf")
            self.extrair_campos_crlv = kwargs.get("extrair_campos_crlv")
            self.campos_padrao = kwargs.get("campos_padrao") or {}

            if self.extrair_texto_pdf is None or self.extrair_campos_crlv is None or not self.campos_padrao:
                try:
                    # >>> usa o CORE (separado do UI) <<<
                    from LEITOR_DOCUMENTO_CLRV import (
                        extrair_texto_pdf as _etp,
                        extrair_campos_crlv_regex as _ecc,
                        CAMPOS_PADRAO as _CP,  # <- lista de colunas
                    )
                    # converte a régua do core (lista) para dict de defaults
                    _cp_dict = {c: "" for c in (_CP or [])}

                    # wrapper para casar assinatura: (texto, campos_padrao_dict) -> dict completo
                    def _ecc_wrapper(texto: str, campos_padrao: dict) -> dict:
                        base = _ecc(texto) or {}
                        saida = {k: "" for k in campos_padrao.keys()}
                        for k, v in base.items():
                            if v is not None and str(v).strip() != "":
                                saida[k] = v
                        return saida

                    self.extrair_texto_pdf = self.extrair_texto_pdf or _etp
                    self.extrair_campos_crlv = self.extrair_campos_crlv or _ecc_wrapper
                    self.campos_padrao = self.campos_padrao or _cp_dict

                except Exception as e:
                    # fallback antigo via 'extractor.py' se existir
                    try:
                        from extractor import (
                            extrair_texto_pdf as _etp,
                            extrair_campos_crlv as _ecc,
                            CAMPOS_PADRAO as _CP,
                        )
                        self.extrair_texto_pdf = self.extrair_texto_pdf or _etp
                        self.extrair_campos_crlv = self.extrair_campos_crlv or _ecc
                        self.campos_padrao = self.campos_padrao or dict(_CP)
                    except Exception as e2:
                        raise TypeError(
                            "Modo LEGADO detectado, mas não foi possível obter funções/régua do 'core' nem do 'extractor'. "
                            "Injete via kwargs (extrair_texto_pdf, extrair_campos_crlv, campos_padrao) "
                            "ou garanta um 'core.py' com extratores e CAMPOS_PADRAO."
                        ) from e2

            # extras vindos da UI (sobrescrevem)
            extra_campos = kwargs.get("extra_campos") or {}
            self.campos_padrao.update({k: v for k, v in extra_campos.items() if str(v or "").strip()})

        else:
            # --------- MODO NOVO ---------
            self.extrair_texto_pdf = kwargs["extrair_texto_pdf"]
            self.extrair_campos_crlv = kwargs["extrair_campos_crlv"]
            self.campos_padrao = dict(kwargs["campos_padrao"] or {})

            # extras vindos da UI (sobrescrevem)
            extra_campos = kwargs.get("extra_campos") or {}
            self.campos_padrao.update({k: v for k, v in extra_campos.items() if str(v or "").strip()})

    # ------------------------ Utils ------------------------
    def cancelar(self) -> None:
        self.cancelado = True

    def _ensure_xlsx(self, path: Path) -> Path:
        return path if path.suffix.lower() == ".xlsx" else path.with_suffix(".xlsx")

    def _dispatch(self, fn) -> None:
        if self._dispatcher:
            try:
                self._dispatcher(0, fn)
                return
            except Exception:
                pass
        try:
            fn()
        except Exception:
            pass
    def _msg(self, text: str) -> None:
        if self._on_msg:
            self._dispatch(lambda: self._on_msg(text))

    def _progress(self, pct: float, current: Optional[int] = None, total: Optional[int] = None) -> None:
        if self._on_progress:
            self._dispatch(lambda: self._on_progress(pct, current, total))

    # ------------------------ Logging ------------------------
    def _open_log(self) -> None:
        if not self.debug_log_arquivo:
            return
        try:
            self._log_fp = open(os.path.join(self.pasta, "crlv_debug.log"), "a", encoding="utf-8")
            print(f"[LOG] Escrevendo em: {self._log_fp.name}", flush=True)
        except Exception as e:
            print(f"[LOG] Falha ao abrir log: {e}", flush=True)
            self._log_fp = None

    def _clog(self, msg: str) -> None:
        if self.debug_console:
            print(msg, flush=True)
        if self._log_fp:
            try:
                self._log_fp.write(msg + "\n")
                self._log_fp.flush()
            except Exception:
                pass
    # ------------------------ Execução ------------------------
    def run(self) -> None:
        try:
            arquivos_pdf = [f for f in os.listdir(self.pasta) if f.lower().endswith(".pdf")]
            arquivos_pdf.sort()
            if not arquivos_pdf:
                self._msg("Nenhum PDF encontrado.")
                return

            self._open_log()
            total = len(arquivos_pdf)

            for i, nome_arquivo in enumerate(arquivos_pdf, 1):
                if self.cancelado:
                    self._msg("Processamento cancelado pelo usuário.")
                    self._clog("[⛔] Processamento cancelado pelo usuário.")
                    break

                caminho_pdf = os.path.join(self.pasta, nome_arquivo)
                try:
                    # 1) OCR/extração bruta e normalização
                    texto = self.extrair_texto_pdf(caminho_pdf, self.apikey)
                    texto_norm = self.normalizar_texto_basico(texto)

                    # (opcional) debug do texto
                    self._clog("\n🔍 Texto extraído (normalizado):")
                    self._clog("=" * 80)
                    trecho = (texto_norm or "")[: self.debug_text_limit]
                    self._clog(trecho if trecho else "(vazio)")
                    if len(texto_norm) > self.debug_text_limit:
                        self._clog("... [cortado]")
                    self._clog("=" * 80)

                    # (opcional) salvar .ocr.txt
                    if self.debug_salvar_txt:
                        try:
                            base = os.path.splitext(nome_arquivo)[0]
                            ts = time.strftime("%Y%m%d-%H%M%S")
                            with open(os.path.join(self.pasta, f"{base}.{ts}.ocr.txt"), "w", encoding="utf-8") as ftxt:
                                ftxt.write(texto or "")
                            self._clog(f"[💾] Texto OCR salvo em {base}.ocr.txt")
                        except Exception as e:
                            self._clog(f"[⚠] Falha ao salvar .ocr.txt: {e}")

                    # 2) Extração dos campos (usa wrapper no modo legado)
                    dados = self.extrair_campos_crlv(texto_norm, self.campos_padrao.copy())
                    dados["Arquivo"] = nome_arquivo
                    if self.salvar_texto_bruto_no_excel:
                        dados["_TextoBruto"] = texto or ""

                    # 3) Garante colunas esperadas pela régua
                    for col in self.campos_padrao.keys():
                        dados.setdefault(col, "")

                    self._resultados.append(dados)

                    # 4) Progresso
                    self._progress(i / total * 100.0, i, total)

                    # 5) Faltantes (diagnóstico)
                    faltantes = [
                        c
                        for c in self.campos_padrao.keys()
                        if c not in ("Arquivo", "_TextoBruto") and not (str(dados.get(c) or "").strip())
                    ]

                    if faltantes:
                        self._msg(f"⚠ {nome_arquivo}: faltando {', '.join(faltantes)}")
                        self._falhas.append({"Arquivo": nome_arquivo, "Faltantes": ", ".join(faltantes)})
                    else:
                        self._msg(f"✔ {nome_arquivo}")

                except Exception as e:
                    self._msg(f"❌ Erro em {nome_arquivo}: {e}")
                    self._falhas.append({"Arquivo": nome_arquivo, "Erro": str(e)})

            # ----- Persistência -----
            if self._resultados:
                df_cols = list(self.campos_padrao.keys())
                if self.salvar_texto_bruto_no_excel:
                    df_cols += ["_TextoBruto"]

                df_cru = pd.DataFrame(self._resultados, columns=df_cols).drop_duplicates(
                    subset=["Arquivo"], keep="last"
                )

                # Transformação opcional (template)
                if self.build_frota_df is not None:
                    df_saida = self.build_frota_df(df_cru, defaults={})
                else:
                    df_saida = df_cru

                out_path = Path(self.saida_excel_path) if self.saida_excel_path else Path(self.pasta) / f"crlv_consolidado_{time.strftime('%Y%m%d-%H%M%S')}.xlsx"
                out_path = self._ensure_xlsx(out_path)
                out_path.parent.mkdir(parents=True, exist_ok=True)

                if self.write_df_to_existing_template is not None and self.column_map:
                    self.write_df_to_existing_template(
                        xlsx_path=out_path,
                        df=df_saida,
                        sheet_name=self.excel_sheet_name,
                        header_row=self.excel_header_row,
                        data_start_row=self.excel_data_start_row,
                        column_map=self.column_map,
                        strict=False,
                    )
                else:
                    df_saida.to_excel(out_path, index=False, engine="openpyxl")

                if self.gerar_csv_falhas and self._falhas:
                    try:
                        pd.DataFrame(self._falhas).to_csv(
                            Path(self.pasta) / f"crlv_falhas_{time.strftime('%Y%m%d-%H%M%S')}.csv",
                            index=False, sep=";", encoding="utf-8"
                        )
                    except Exception:
                        pass

                self._msg(f"✔ Excel atualizado: {out_path}")
            else:
                self._msg("Nenhum resultado para salvar.")
        finally:
            if self._log_fp:
                try:
                    self._log_fp.close()
                except Exception:
                    pass
            if self._on_done:
                self._dispatch(self._on_done)
