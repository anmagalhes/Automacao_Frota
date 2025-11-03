# ===================== DEBUG / LOG =====================
DEBUG_CONSOLE = True            # imprime no console
DEBUG_LOG_ARQUIVO = True        # também registra em arquivo crlv_debug.log na pasta dos PDFs
DEBUG_TEXT_LIMIT = 6000         # limite de caracteres ao mostrar o texto extraído
DEBUG_SALVAR_TXT = False        # salva NOME.ocr.txt com o texto bruto por arquivo

# -*- coding: utf-8 -*-
import sys, os
from pathlib import Path
import re
import json
import time
import queue
import threading
import requests
import pdfplumber
import pandas as pd
import tkinter as tk
from tkinter import filedialog, messagebox, ttk

import unicodedata
from typing import Optional, Set

sys.path.append(os.path.dirname(os.path.abspath(__file__)))

# ===================== CONFIGURAÇÕES =====================
APP_TITULO = "Frota Data | Leitor Documento - CLRV"
SALVAR_TEXTO_BRUTO_NO_EXCEL = True
GERAR_JSON_POR_ARQUIVO = False
GERAR_CSV_FALHAS = True


# OCR.Space
OCR_SPACE_APIKEY_DEFAULT = os.environ.get("OCR_SPACE_APIKEY", "helloworld")
OCR_TIMEOUT = 90
OCR_SLEEP_FREE = 1.2      # evita throttle na key free
OCR_MAX_RETRY = 2
OCR_ENGINE = 2            # 1=legacy 2=engine moderno

# Colunas fixas do Excel
CAMPOS_PADRAO = [
    "Arquivo",
    "Placa",
    "Renavam",
    "Chassi",
    "Motor",
    "Ano Fabricação",
    "Ano Modelo",
    "Modelo",
    "Fabricante",
    "Modelo_Limpo",
    "Cor",
    "Combustível",
    "Combustivel_Principal",
    "Combustivel_Secundario",
    "Espécie / Tipo",
    "Categoria",
    "Capacidade",
    "Potência/Cilindrada",
    "Peso Bruto Total",
    "Carroceria",
    "Proprietário",
    "CPF",
    "CNPJ",
    "Local",
    "UF",
    "Data Emissão",
    "Número do CRV",
    "Código Segurança CLA",
     "NumeroSegurancaCRV",
     "CENTRO",
     "CENTRO_CUSTO",
     "EQUIPAMENTO",
     "TIPO_VEICULO",
     "DIVISAO",
     "PERFIL_CARTALO",
     "TIPO_CARURANTE_OLEO",
     "TIPO_ANO_VEICULO",
     "GERENCIA"
]

def coalesce_por_veiculo(rows):
    """
    Une registros do mesmo veículo.
    Ordem de prioridade da chave: Renavam -> Placa -> Número do CRV -> NumeroSegurancaCRV.
    Normaliza valores para evitar duplicidade por formatação.
    Preenche somente campos vazios na linha base.
    """

    def _num(v):  # só dígitos
        return re.sub(r"\D", "", str(v)) if v is not None else ""

    def _placa(v):  # UPPER, sem hífen/espaços
        if v is None: return ""
        return str(v).upper().replace("-", "").replace(" ", "").strip()

    def chave(rec):
        cand = [
            ("Renavam", _num(rec.get("Renavam"))),
            ("Placa", _placa(rec.get("Placa"))),
            ("Número do CRV", _num(rec.get("Número do CRV"))),
            ("NumeroSegurancaCRV", _num(rec.get("NumeroSegurancaCRV"))),
        ]
        for k, v in cand:
            if v:
                return (k, v)
        return ("__Arquivo__", (rec.get("Arquivo") or "").strip())

    merged, fontes = {}, {}

    for rec in rows:
        k = chave(rec)
        if k not in merged:
            merged[k] = rec.copy()
            fontes[k] = [rec.get("Arquivo")]
        else:
            base = merged[k]
            for col, val in rec.items():
                if (not base.get(col)) and (val is not None) and str(val).strip():
                    base[col] = val
            fontes[k].append(rec.get("Arquivo"))

    out = list(merged.values())
    for row in out:
        k = chave(row)
        row["_MergeFontes"] = "; ".join([x for x in (fontes.get(k) or []) if x])
    return out



def formatar_codigo(texto: str) -> str:
    """
    Formata um texto com 7 caracteres em 4 primeiros + hífen + 3 últimos.
    Tudo em maiúsculas, sem espaços.
    """
    if not texto:
        return ""

    texto = texto.upper().replace(" ", "")

    if len(texto) == 7:
        return texto[:4] + "-" + texto[4:]

    return texto  # Retorna original se não tiver 7 caracteres


def _first_digit_block_ocr(s: str, min_len=8, max_len=15) -> str | None:
    """
    Encontra primeiro bloco tipo número (aceitando confusões OCR) e retorna só dígitos.
    """
    for m in re.finditer(rf"[0-9OIlB]{{{min_len},{max_len}}}", s):
        fix = _fix_ocr_digits(m.group(0))
        dig = re.sub(r"\D", "", fix)
        if min_len <= len(dig) <= max_len:
            return dig
    return None

def _normalize(s: str) -> str:
    if not s:
        return ""
    s = unicodedata.normalize("NFKD", s)
    s = "".join(c for c in s if not unicodedata.combining(c))
    s = s.replace("\r", "\n")
    # mantém quebras de linha: vamos trabalhar por linhas
    return s

def _fix_ocr_digits(s: str) -> str:
    """Corrige confusões comuns do OCR só no trecho candidato a número."""
    m = {"O":"0","o":"0","I":"1","i":"1","l":"1","L":"1","B":"8","b":"8","S":"5","s":"5"}
    return "".join(m.get(ch, ch) for ch in s)

def _extract_first_number_chunk(s: str, min_len=8, max_len=16) -> str | None:
    # aceita dígitos misturados com confusões OCR
    for m in re.finditer(rf"[0-9OIlB]{{{min_len},{max_len}}}", s):
        cand = _fix_ocr_digits(m.group(0))
        digits = re.sub(r"\D", "", cand)
        if min_len <= len(digits) <= max_len:
            return digits
    return None

def _build_output_picker(self):
    frm_out = ttk.Frame(self.root)
    frm_out.pack(fill=tk.X, padx=10, pady=(2, 4))

    # 🔹 Cria o label que faltava
    self.lbl_out_dest = ttk.Label(frm_out, text="Arquivo de saída: (não definido)")
    self.lbl_out_dest.pack(side=tk.LEFT, expand=True, anchor="w")

    btn_save_as = ttk.Button(frm_out, text="Salvar como…", command=self.escolher_saida_excel)
    btn_save_as.pack(side=tk.RIGHT)


def escolher_saida_excel(self):
    if getattr(self, "pasta", None):
        initial_dir = self.pasta
    else:
        initial_dir = str(Path.home() / "Documents")

    default_name = f"crlv_consolidado_{time.strftime('%Y%m%d-%H%M%S')}.xlsx"

    path = filedialog.asksaveasfilename(
        title="Escolher arquivo de saída",
        defaultextension=".xlsx",
        initialdir=initial_dir,
        initialfile=default_name,
        filetypes=[("Planilha Excel (*.xlsx)", "*.xlsx"),
                   ("CSV separado por ; (*.csv)", "*.csv"),
                   ("Todos os arquivos", "*.*")]
    )
    if not path:
        return
    self.saida_excel_path = path
    # 🔹 Agora isso não quebra, pois lbl_out_dest existe:
    self.lbl_out_dest.config(text=f"Arquivo de saída: {path}")


# ===================== PARA O DENTRAR SER =====================

NUM_PAT = re.compile(r"\d+(?:[.,]\d+)?")

NUM_PAT_ANY = re.compile(r"(-?\d+(?:[.,]\d+)?)", re.ASCII)

def is_missing(v):
    if v is None: return True
    if isinstance(v, str) and not v.strip(): return True
    # para numéricos, considerar 'faltante' apenas se None
    return False

def to_float_safe(s: str) -> float | None:
    s = s.replace(",", ".")
    m = re.search(r"-?\d+(?:\.\d+)?", s)
    return float(m.group()) if m else None

def find_number_after_smart(label_regex: str, text: str, *, max_ahead_chars: int = 220) -> float | None:
    """
    Busca o número 'mais plausível' após o rótulo:
      - ignora tokens numéricos imediatamente seguidos de 'CV'
      - prioriza números decimais (com . ou ,)
      - fallback: escolhe o maior número > 0 se houver múltiplos
    """
    parts = re.split(label_regex, text, flags=re.IGNORECASE)
    if len(parts) < 2:
        return None

    tail = parts[-1][:max_ahead_chars]
    # remove quebras para simplificar
    tail_flat = re.sub(r"[\r\n]+", " ", tail)

    # encontre TODOS os números e filtre os com 'CV' colado à direita
    cands = []
    for m in NUM_PAT_ANY.finditer(tail_flat):
        num_txt = m.group(1)
        end = m.end()
        # pega os próximos 2 chars para verificar 'CV'
        next_two = tail_flat[end:end+2].upper()
        if next_two == "CV":  # ignora '0CV' etc.
            continue
        # classifica: decimal tem prioridade
        is_decimal = "." in num_txt or "," in num_txt
        val = to_float_safe(num_txt)
        if val is not None:
            cands.append((is_decimal, val))

    if not cands:
        return None

    # 1º criterio: algum decimal? pegue o primeiro decimal
    for is_decimal, val in cands:
        if is_decimal:
            return val

    # 2º criterio: senão, pegue o maior > 0 (evita capturar zeros “ruído”)
    pos = [v for _, v in cands if v > 0]
    if pos:
        return max(pos)

    # 3º criterio: último recurso — primeiro da lista
    return cands[0][1]



def find_number_after(label_regex: str, text: str, *, max_ahead_chars: int = 160) -> float | None:
    """
    Encontra o 1º número (ex.: 0.15, 0,28, 123) até 'max_ahead_chars' após 'label_regex'.
    Tolerante a quebras de linha e a texto intermediário.
    """
    # Pegue o pedaço após o ÚLTIMO label (no DETRAN-SE, há blocos duplicados em 2 páginas)
    parts = re.split(label_regex, text, flags=re.IGNORECASE)
    if len(parts) < 2:
        return None
    tail = parts[-1][:max_ahead_chars]  # janela
    # varre números na janela
    m = NUM_PAT.search(tail)
    if not m:
        return None
    return to_float_safe(m.group(0))

VIN_PAT = re.compile(r"\b([A-HJ-NPR-Z0-9]{17})\b")

def extract_chassi(t_norm: str) -> str | None:
    """
    Prioriza o valor até ~1000 chars após a palavra 'CHASSI'
    e, se não achar, pega a última ocorrência válida no doc.
    """
    # 1) depois do rótulo CHASSI
    blocos = re.split(r"\bCHASSI\b", t_norm, flags=re.IGNORECASE)
    if len(blocos) > 1:
        trecho = blocos[-1][:1000]  # janela maior por causa das quebras de OCR e 2ª página
        m = VIN_PAT.search(trecho.replace("\n", " ").upper())
        if m:
            return m.group(1)

    # 2) fallback global: última ocorrência válida
    cands = VIN_PAT.findall(t_norm.replace("\n", " ").upper())
    return cands[-1] if cands else None


# Padrões válidos (tradicional AAA#### e Mercosul AAA#A##)
RE_PLACA_VALID = re.compile(r"^(?:[A-Z]{3}\d{4}|[A-Z]{3}\d[A-Z]\d{2})$")


def _normalizar_basico(s: str) -> str:
    if not s:
        return ""
    s = unicodedata.normalize("NFKD", s)
    s = "".join(c for c in s if not unicodedata.combining(c))
    s = s.replace("\r", " ").replace("\n", " ")
    s = re.sub(r"\s+", " ", s)
    return s.strip().lower()

def _corrigir_confusoes_ocr_num(s: str) -> str:
    """
    Correções conservadoras de OCR somente para números:
    - O ↔ 0
    - I ↔ 1
    - l ↔ 1
    - B ↔ 8
    - S ↔ 5 (menos comum; aplicar só quando faz sentido)
    Aplica apenas em blocos candidatos.
    """
    # Trabalhar só em chars comuns de confusão
    mapa = {
        "o": "0", "O": "0",
        "i": "1", "I": "1", "l": "1", "L": "1",
        "b": "8", "B": "8",
        # "s": "5", "S": "5",  # ative se necessário
    }
    return "".join(mapa.get(ch, ch) for ch in s)


def extrair_num_seguranca_crv(texto: str, renavam: str = None) -> str | None:
    if not texto:
        return None

    t = texto

    ROTULO_CRV = r"""
        N(?:[\.º°]\s*)?                 # N., Nº, N°
        [ÚU]MERO\W*DE\W*SEGURAN[ÇC]A    # NUMERO DE SEGURANCA
        \W*(?:DO\W*)?CRV                # (DO )? CRV
        [\s:\-]*                        # separadores usuais
        (?:CAT\b)?                      # 'CAT' pode aparecer colado
        [\s\r\n]*                       # quebras/espacos
    """

    lab = re.search(ROTULO_CRV, t, flags=re.IGNORECASE | re.VERBOSE)
    candidato = None

    if lab:
        start = lab.end()
        janela = t[start:start + 200]

        # Reduz ruído antes de procurar os 11 dígitos
        janela_limp = re.sub(r"\bCAT\b", " ", janela, flags=re.IGNORECASE)
        # normaliza espaços/quebras
        janela_limp = re.sub(r"[ \t\r\n]+", " ", janela_limp)

        # Corrige OCR e busca EXATAMENTE 11 dígitos com boundaries
        win = _fix_ocr_digits(janela_limp)
        m11 = re.search(r'(?<!\d)\d{11}(?!\d)', win)
        if m11:
            candidato = m11.group(0)

    if not candidato:
        # Fallback onde o rótulo pode estar “quebrado”
        m2 = re.search(
            r"N[ÚU]MERO\s*DE\s*SEGURAN[ÇC]A.*?CRV([^0-9]+)(\d{11})",
            t, flags=re.IGNORECASE | re.DOTALL
        )
        if m2:
            candidato = m2.group(2)

    # Anti-RENAVAM
    if candidato and renavam:
        ren = re.sub(r"\D", "", str(renavam))
        if len(ren) >= 8 and candidato == ren:
            return None

    return candidato

def extrair_num_seguranca_crv_pag2(texto: str) -> str | None:
    """
    Extrai o 'Número de Segurança do CRV' quando ele aparece na PÁGINA 2 do CRLV-e
    (bloco oficial da SENATRAN). Corrige OCR e captura EXATAMENTE 11 dígitos.
    """
    if not texto:
        return None

    mrot = re.search(r"N[ÚU]MERO\s*DE\s*SEGURAN[ÇC]A\s*DO\s*CRV", texto, flags=re.IGNORECASE)
    if not mrot:
        return None

    # Janela curta após o rótulo (evita pegar o “10 Benefícios...”)
    janela = texto[mrot.end(): mrot.end() + 160]

    # Correção leve de OCR
    mapa = {"O":"0","o":"0","I":"1","i":"1","l":"1","L":"1","B":"8","b":"8"}
    win = "".join(mapa.get(ch, ch) for ch in janela)

    # 11 dígitos contíguos
    m11 = re.search(r'(?<!\d)\d{11}(?!\d)', win)
    if m11:
        return m11.group(0)
    return None



# ===================== UTILITÁRIOS =====================
def normalizar_texto_basico(texto: str) -> str:
    if not texto:
        return ""
    texto = texto.replace("\r\n", "\n").replace("\r", "\n")
    texto = re.sub(r"[ \t]+", " ", texto)
    texto = "\n".join(ln.strip() for ln in texto.split("\n"))
    texto = re.sub(r"\n{3,}", "\n\n", texto)
    # corrigir alguns caracteres bizarros do OCR
    m = {
        "Ę": "E", "Ě": "E", "Â": "A", "Î": "I", "Ô": "O", "Û": "U",
        "Ä": "A", "Ö": "O", "Ü": "U"
    }
    for k, v in m.items():
        texto = texto.replace(k, v)
    return texto

def linhas_texto(texto: str):
    return [ln.strip() for ln in normalizar_texto_basico(texto).splitlines() if ln.strip()]

def limpar_valor(v):
    if v is None:
        return None
    v = v.strip()
    # Remove somente "Nº", "N°" (símbolo de ordinal), sem cortar "NO"
    v = re.sub(r"^(?:N[\u00BA\u00B0]\s*)", "", v)  # \u00BA = º ; \u00B0 = °
    return v if v else None

def _tokey(s: str) -> str:
    """Upper + sem acento + espaços normalizados para comparar rótulos."""
    if not s: return ""
    up = unicodedata.normalize("NFD", s.upper())
    up = "".join(ch for ch in up if unicodedata.category(ch) != "Mn")
    up = re.sub(r"\s+", " ", up).strip()
    return up

# Lista de rótulos “conhecidos” (com e sem acento; comparação usa _tokey)
_KNOWN_LABELS = {
    "CÓDIGO RENAVAM","RENAVAM","PLACA","EXERCÍCIO","ANO FABRICAÇÃO","ANO MODELO","NÚMERO DO CRV",
    "CÓDIGO DE SEGURANÇA DO CLA","MARCA / MODELO / VERSÃO","ESPÉCIE / TIPO","PLACA ANTERIOR / UF",
    "CHASSI","COR PREDOMINANTE","COMBUSTÍVEL","OBSERVAÇÕES DO VEÍCULO","MENSAGENS SENATRAN",
    "CATEGORIA","CAPACIDADE","POTÊNCIA CILINDRADA","POTÊNCIA/CILINDRADA","PESO BRUTO TOTAL","CMT",
    "EIXOS","LOTAÇÃO","MOTOR","CARROCERIA","NOME","NOME/RAZÃO SOCIAL","CPF / CNPJ","CPF/ CNPJ","CPF/CNPJ",
    "LOCAL","DATA","INFORMAÇÕES DO SEGURO DPVAT","DADOS DO SEGURO DPVAT","CAT. TARIF","DATA DE QUITAÇÃO",
    "CÓDIGO DE SEGURANÇA DO CLA",
    "CODIGO DE SEGURANCA DO CLA",
    "CÓD. SEGURANÇA CLA",
    "COD. SEGURANCA CLA",
    "CODIGO SEGURANCA CLA",
    "CÓDIGO DE SEGURANÇA DO CRLV-E",
    "CODIGO DE SEGURANCA DO CRLV-E",
    "CLA",   # alguns OCR encurtam o rótulo
    "CAT",   # aparece logo abaixo do rótulo (vamos pular como ruído)
    "CÓDIGO DE SEGURANÇA DO CLA", "CODIGO DE SEGURANCA DO CLA",
    "CÓD. SEGURANÇA CLA", "COD. SEGURANCA CLA",
    "CÓDIGO DE SEGURANÇA DO CRLV-E", "CODIGO DE SEGURANCA DO CRLV-E",
    "NÚMERO DE SEGURANÇA DO CRV", "NUMERO DE SEGURANCA DO CRV",
    "PLACA EXERCÍCIO", "PLACA EXERCICIO",
    "LOCAL", "DATA",
}
KNOWN_LABEL_KEYS = {_tokey(x) for x in _KNOWN_LABELS}

def is_known_label(ln: str) -> bool:
    """True se a linha for um rótulo conhecido (insensível a acentos)."""
    if not ln: return False
    t = _tokey(ln)
    if t in KNOWN_LABEL_KEYS:
        return True
    # rótulo explícito com dois pontos
    if ":" in ln:
        return True
    return False


# SKIP values para ruído:
SKIP_VALUES = {"***", "*******/**", "*", "*.*", "CMT"}

# --- Helpers semânticos/rotulagem ---
def is_label_line(ln: str) -> bool:
    if not ln: return False
    t = ln.strip().upper()
    if re.search(r"\d", t): return False
    return len(t) <= 30 and re.match(r"^[A-ZÀ-Ü /().-]+$", t) is not None


def val_motor(v: str) -> bool:
    if not v: return False
    s = re.sub(r"[^A-Z0-9\-]", "", v.upper())
    return len(s) >= 6

def val_potcil(v: str) -> bool:
    return bool(re.search(r"\d", v or ""))

def val_capacidade(v: str) -> bool:
    if not v: return False
    t = v.upper().strip()
    if "/" in t and "KG" not in t:  # evita OCV/162 etc.
        return False
    if re.fullmatch(r"[A-Z0-9\-]{8,}", t):  # evita KC22E2S207770 (motor)
        return False
    return bool(re.search(r"\d", t))

def proximo_valor(linhas, idx, max_lookahead=8, validator=None):
    """
    Pega a próxima linha útil após 'idx', pulando rótulos (com/sem ':') e SKIP_VALUES.
    """
    for j in range(1, max_lookahead + 1):
        if idx + j >= len(linhas): break
        cand = linhas[idx + j].strip()
        if not cand: continue
        up = cand.upper()
        if up in SKIP_VALUES: continue
        if is_known_label(cand):  # <<< trocado
            continue
        if validator:
            if validator(cand): return cand, idx + j
            else: continue
        return cand, idx + j
    return None, None


def pick_first_token_after(label_regex: str,
                           texto: str,
                           max_ahead_chars: int = 400,
                           reject: Optional[Set[str]] = None) -> Optional[str]:
    """
    Acha o 1º token (A-Z0-9-) após o rótulo, ignorando ruídos (ex.: 'QRCODE', '***', 'CMT').
    Útil para: MOTOR (SE), etc.
    """
    reject = {x.upper() for x in (reject or set())}

    lab = re.search(label_regex, texto, re.IGNORECASE)
    if not lab:
        return None

    janela = texto[lab.end(): lab.end() + max_ahead_chars]
    for m in re.finditer(r"\b[A-Z0-9\-]{6,}\b", janela, re.IGNORECASE):
        tok = m.group(0).upper()
        if tok in reject:
            continue
        return tok
    return None

def val_chassi(v: str) -> bool:
    if not v:
        return False
    s = re.sub(r"[^A-Z0-9]", "", v.upper())
    # exige letras E números, sem espaços, comprimento típico
    return (11 <= len(s) <= 25) and re.search(r"[A-Z]", s) and re.search(r"[0-9]", s)

def somente_digitos(s):
    return re.sub(r"\D", "", s or "")

def formatar_cnpj(cnpj):
    d = somente_digitos(cnpj)
    if len(d) != 14:
        return cnpj
    return f"{d[0:2]}.{d[2:5]}.{d[5:8]}/{d[8:12]}-{d[12:14]}"

def formatar_cpf(cpf):
    d = somente_digitos(cpf)
    if len(d) != 11:
        return cpf
    return f"{d[0:3]}.{d[3:6]}.{d[6:9]}-{d[9:11]}"

def is_cor(valor: str) -> bool:
    if not valor:
        return False
    v = valor.upper()
    cores = {
        "BRANCA","BRANCO","PRETA","PRETO","PRATA","PRATEADO","VERMELHA","VERMELHO",
        "AZUL","VERDE","AMARELA","AMARELO","CINZA","MARROM","DOURADA","LARANJA","BEGE"
    }
    return any(c in v for c in cores)

def is_combustivel(valor: str) -> bool:
    if not valor:
        return False
    v = valor.upper().replace("Á","A").replace("Í","I").replace("Ç","C")
    combustiveis = {
        "GASOLINA","ALCOOL","ETANOL","DIESEL","GNV","FLEX","ALCOOL/GASOLINA","GASOLINA/ALCOOL",
        "ELETRICO","HÍBRIDO","HIBRIDO","BIO","BIODIESEL","GASOLINA C","GASOLINA A"
    }
    return any(c in v for c in combustiveis)

def normalizar_placa(placa: str) -> str:
    """
    Tenta corrigir confusões O/0 e I/1 na placa Mercosul (ABC1D23).
    Se for padrão antigo (ABC1234), mantém.
    """
    if not placa:
        return placa
    p = re.sub(r"[^A-Z0-9]", "", placa.upper())
    # Se tamanho não for 7, desiste
    if len(p) != 7:
        return placa
    # Padrão antigo ABC1234
    if re.match(r"^[A-Z]{3}\d{4}$", p):
        return p
    # Tenta Mercosul ABC1D23
    chars = list(p)
    # 1-3 letras
    for i in range(3):
        if chars[i].isdigit():
            # corrige 1 -> I, 0 -> O quando vier dígito
            if chars[i] == "1": chars[i] = "I"
            elif chars[i] == "0": chars[i] = "O"
    # 4º deve ser dígito (se vier O, troca pra 0)
    if chars[3] == "O": chars[3] = "0"
    if not chars[3].isdigit():
        # se for letra que pareça dígito
        mapa = {"O": "0", "B": "8", "S": "5"}
        chars[3] = mapa.get(chars[3], chars[3])
    # 5º deve ser letra (se vier 1/0, troca por I/O)
    if chars[4].isdigit():
        chars[4] = "I" if chars[4] == "1" else ("O" if chars[4] == "0" else chars[4])
    # 6-7 dígitos
    for i in (5, 6):
        if not chars[i].isdigit():
            mapa = {"O": "0", "B": "8", "S": "5"}
            chars[i] = mapa.get(chars[i], chars[i])
    pp = "".join(chars)
    # se agora casar com Mercosul, retorna
    if re.match(r"^[A-Z]{3}\d[A-Z]\d{2}$", pp):
        return pp
    # senão retorna p limpo mesmo
    return p

def extrair_num_capacidade(s: str):
    """
    Extrai 'número + unidade opcional' de uma string de capacidade.
    Aceita: 0.15, 0,15, 500, 500 KG, 10 L, 02P (pessoas).
    Rejeita linhas de POT/CIL (CV, POT, CIL, 'x/y' sem KG) e códigos longos (motor).
    Retorna string padronizada (número com vírgula + unidade opcional) ou None.
    """
    if not s:
        return None
    t = s.strip().upper()

    # Rejeitar linhas típicas de Pot/Cil ou CV
    if re.search(r"\b(CV|POT|CIL)\b", t):
        return None
    # Rejeitar "x/y" que não seja kg/pessoas (ex.: 0CV/162)
    if "/" in t and "KG" not in t and not re.search(r"\bP\b", t):
        return None
    # Rejeitar coisa que parece motor/código longo
    if re.fullmatch(r"[A-Z0-9\-]{8,}", t):
        return None

    # PESSOAS: 01P, 1P, 10P...
    m = re.search(r"\b0*([0-9]+)\s*P\b", t)
    if m:
        return f"{int(m.group(1))}P"

    # COM UNIDADE: kg, t/ton, l
    m = re.search(r"(\d+(?:[.,]\d+)?)[ ]*(KG|T|TON|L)\b", t)
    if m:
        num = m.group(1).replace(".", ",")  # padroniza vírgula
        und = m.group(2)
        return f"{num} {und}".strip()

    # SOMENTE NÚMERO (int/decimal)
    m = re.search(r"\b\d+(?:[.,]\d+)?\b", t)
    if m:
        num = m.group(0).replace(".", ",")
        return num

    return None

def parse_marca_modelo(modelo_raw: str):
    """
    Separa 'Modelo' em (Fabricante, Modelo_Limpo).
    Regras:
      - Se houver '/', usa a PRIMEIRA como separador: 'HONDA/CG 160 CARGO' -> ('HONDA', 'CG 160 CARGO')
      - Remove asteriscos, espaços duplicados e barras adicionais no começo/fim.
      - Se não houver '/', tenta inferir: se a primeira 'palavra' é 'marca' conhecida, usa como Fabricante.
      - Se não conseguir separar, devolve (None, modelo normalizado).
    """
    if not modelo_raw:
        return (None, None)

    s = (modelo_raw or "").strip()
    # remove lixos comuns
    s = s.strip("* ").replace("  ", " ").strip()
    s = re.sub(r"\s{2,}", " ", s)

    # lista básica de marcas (pode ampliar conforme necessidade)
    marcas = {
        "AGRALE","AUDI","BMW","CAOA","CHEVROLET","CHEV","CHERY","CITROEN","DAF","DAIHATSU","DODGE",
        "EFFA","FIAT","FORD","GEELY","GMC","HAFEI","HINO","HONDA","HYUNDAI","IVECO","JAC","JEEP",
        "KAWASAKI","KIA","LAND ROVER","LEXUS","MAN","MASERATI","MERCEDES","MERCEDES-BENZ","MITSUBISHI",
        "NEW HOLLAND","NISSAN","PEUGEOT","RENAULT","SCANIA","SHINERAY","SPRINTER","SUBARU","SUZUKI",
        "TOYOTA","TRIUMPH","VOLKSWAGEN","VW","VOLVO","YAMAHA"
    }

    if "/" in s:
        left, right = s.split("/", 1)
        marca = left.strip(" /-").upper()
        modelo = right.strip(" /-")
        # se a marca vier repetida no início do modelo, remove
        if modelo.upper().startswith(marca + " "):
            modelo = modelo[len(marca):].lstrip()
        return (marca or None, modelo or None)

    # sem '/', tenta inferir: pega primeira 'palavra' como marca se bater na lista
    tokens = s.split()
    if tokens:
        t0 = tokens[0].upper()
        # marcas compostas (LAND ROVER, MERCEDES BENZ...)
        if len(tokens) >= 2 and f"{t0} {tokens[1].upper()}" in marcas:
            marca = f"{t0} {tokens[1].upper()}"
            modelo = " ".join(tokens[2:]).strip() or None
            return (marca, modelo)
        # marca simples
        if t0 in marcas:
            marca = t0
            modelo = " ".join(tokens[1:]).strip() or None
            return (marca, modelo)

    # fallback: não separa
    return (None, s or None)


def _upper_sem_acento(s: str) -> str:
    import unicodedata
    if s is None:
        return None
    up = unicodedata.normalize("NFD", s.upper())
    return "".join(ch for ch in up if unicodedata.category(ch) != "Mn")

def split_combustivel(valor: str):
    """
    Recebe string de combustível (ex.: 'ALCOOL/GASOLINA', 'GASOLINA C', 'DIESEL S10', 'FLEX').
    Retorna (principal, secundario) em UPPER, sem acento.
    Regras:
      - Se tiver separador, divide e pega os 2 primeiros tokens válidos.
      - Normaliza sinonímias: ETANOL -> ALCOOL; HÍBRIDO -> HIBRIDO; ELÉTRICO -> ELETRICO.
      - Mantém especificação: 'GASOLINA C', 'DIESEL S10' (com sufixo).
      - 'FLEX': por padrão, principal = FLEX; secundario = None.
        (Se preferir mapear FLEX -> ALCOOL/GASOLINA, veja comentário ao final.)
    """
    if not valor:
        return (None, None)

    s = _upper_sem_acento(valor)
    s = s.replace("\\", "/").replace("|", "/")
    # separadores possíveis
    for sep in ["/", "+", ",", ";"]:
        s = s.replace(sep, "/")
    # também trata ' E ' (com espaços) e ' - ' como separador
    s = s.replace(" E ", "/").replace(" - ", " ")
    s = re.sub(r"\s+", " ", s).strip()

    # Normalizações simples de termos
    # Mapeia ETANOL -> ALCOOL; HIBRIDO/HIBRIDA -> HIBRIDO; ELETRICO sem acento
    s = s.replace("ETANOL", "ALCOOL")
    s = s.replace("HÍBRIDO", "HIBRIDO").replace("HIDBRIDO", "HIBRIDO")
    s = s.replace("ELÉTRICO", "ELETRICO").replace("ELETRICO", "ELETRICO")

    # Quebra em partes (no máx 2 úteis)
    partes = [p.strip() for p in s.split("/") if p.strip()]
    # Limpa duplicados exatos e mantém ordem
    seen = set()
    limpas = []
    for p in partes:
        if p not in seen:
            limpas.append(p)
            seen.add(p)

    # Regras adicionais para manter sufixos: GASOLINA C, DIESEL S10 etc.
    def normalizar_parte(p):
        # já está uppercase/sem acento; só limpa espaços extras:
        p = re.sub(r"\s+", " ", p).strip()
        return p

    limpas = [normalizar_parte(p) for p in limpas]

    # Se veio só um item:
    if len(limpas) == 1:
        unico = limpas[0]
        # Caso FLEX: por padrão, mantemos FLEX no principal
        # Se quiser converter FLEX -> (ALCOOL, GASOLINA), troque abaixo conforme comentário.
        return (unico, None)

    # Dois ou mais: pega os 2 primeiros
    principal = limpas[0]
    secundario = limpas[1] if len(limpas) > 1 else None
    return (principal, secundario)



PLACA_PATS = [
    r"\bPLACA\s*[:\-]?\s*([A-Z0-9]{7})\b",
    r"\bPLACA\s+EXERC[ÍI]CIO\s*[\r\n ]+([A-Z0-9]{7})\b",
    r"\bPLACA\s+ANTERIOR\s*/\s*UF\s*[\r\n ]+([A-Z0-9]{7})\b",  # alguns DETRANs repetem a placa aqui
]

# Formatos válidos (BR tradicional e Mercosul 2018+):
re_placa_valid = re.compile(
    r"^(?:[A-Z]{3}\d{4}|[A-Z]{3}\d[A-Z]\d{2})$"
)

def fix_ocr_plate(token: str) -> str:
    """
    Corrige ambiguidades comuns de OCR em placas:
      - O ↔ 0, I ↔ 1, B ↔ 8, S ↔ 5 (apenas onde faz sentido).
    Aplica regras posicionais do padrão Mercosul (AAA#A##) e Tradicional (AAA####).
    """
    t = token.strip().upper()
    if len(t) != 7:
        return t

    def is_letter(c): return 'A' <= c <= 'Z'
    def is_digit(c):  return '0' <= c <= '9'

    # Tentativa 1: Mercosul AAA#A##
    m = list(t)
    # posições: 0,1,2 letras | 3 dígito | 4 letra | 5,6 dígitos
    # Corrigir posição 3,5,6 para dígito e 0,1,2,4 para letra quando houver ambiguidade típica
    ambig = {'O':'0','I':'1','B':'8','S':'5'}
    # posições dígito: 3,5,6
    for i in (3,5,6):
        if not is_digit(m[i]) and m[i] in ambig:
            m[i] = ambig[m[i]]
    # posições letra: 0,1,2,4
    rev_ambig = {'0':'O','1':'I','8':'B','5':'S'}
    for i in (0,1,2,4):
        if not is_letter(m[i]) and m[i] in rev_ambig:
            m[i] = rev_ambig[m[i]]

    cand1 = "".join(m)
    if re_placa_valid.match(cand1):
        return cand1

    # Tentativa 2: Tradicional AAA####
    m = list(t)
    for i in (0,1,2):
        if not is_letter(m[i]) and m[i] in rev_ambig:
            m[i] = rev_ambig[m[i]]
    for i in (3,4,5,6):
        if not is_digit(m[i]) and m[i] in ambig:
            m[i] = ambig[m[i]]

    cand2 = "".join(m)
    if re_placa_valid.match(cand2):
        return cand2

    return t  # retorna original se não validar

def extract_placa(t_norm: str) -> str | None:
    # 1) Procura rótulos específicos
    for pat in PLACA_PATS:
        m = re.search(pat, t_norm, re.IGNORECASE)
        if m:
            tok = fix_ocr_plate(m.group(1))
            if re_placa_valid.match(tok):
                return tok

    # 2) Fallback contextual: linha com PLACA e próximo token 7 chars
    m = re.search(r"\bPLACA\b[^\S\r\n]*\n?([A-Z0-9]{7})", t_norm, re.IGNORECASE)
    if m:
        tok = fix_ocr_plate(m.group(1))
        if re_placa_valid.match(tok):
            return tok

    # 3) Fallback global (apenas se RENAVAM/CHASSI já foram captados): escolher a melhor placa entre candidatos
    cands = re.findall(r"\b[A-Z0-9]{7}\b", t_norm.upper())
    for c in cands:
        tok = fix_ocr_plate(c)
        if re_placa_valid.match(tok):
            return tok
    return None

# ===================== SERGIPE =====================

def find_last_after(label_regex, texto, max_lines=6):
    """
    Retorna o último candidato (A-Z0-9>=10) nas próximas 'max_lines' linhas após o rótulo.
    Útil para CHASSI no SE (há '*******/**' antes do final válido).
    """
    lab = re.search(label_regex, texto, re.IGNORECASE)
    if not lab:
        return None
    # janela: até N quebras de linha
    pos = lab.end()
    fim = pos
    brks = 0
    while fim < len(texto) and brks < max_lines:
        if texto[fim] == "\n":
            brks += 1
        fim += 1
    trecho = texto[pos:fim]
    ultimo = None
    for m in re.finditer(r"\b[A-Z0-9]{10,}\b", trecho, re.IGNORECASE):
        cand = m.group(0).upper()
        if "*" in cand:
            continue
        ultimo = cand
    return ultimo


# ===================== OCR / PDF =====================
def extrair_texto_ocr_space(caminho_pdf, apikey):
    url = 'https://api.ocr.space/parse/image'
    for tentativa in range(1, OCR_MAX_RETRY + 1):
        try:
            with open(caminho_pdf, 'rb') as f:
                resp = requests.post(
                    url,
                    files={'file': f},
                    data={
                        'apikey': apikey,
                        'language': 'por',
                        'isOverlayRequired': False,
                        'OCREngine': OCR_ENGINE
                    },
                    timeout=OCR_TIMEOUT
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

def extrair_texto_pdf(caminho_pdf, apikey):
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
            # Heurísticas: se muito curto, ou com "CID:" (texto embaralhado), cai no OCR
            if len(texto) >= 120 and "CID:" not in texto.upper():
                return texto
    except Exception as e:
        print(f"[⚠] Erro pdfplumber: {e}")

    print("[🔄] Tentando OCR.Space como fallback...")
    return extrair_texto_ocr_space(caminho_pdf, apikey)

# ===================== EXTRAÇÃO (LOOKAHEAD + REGEX) =====================
def pegar_valor_depois_de_label(linhas, label_regex, max_lookahead=4, filtro_valor=None):
    lab = re.compile(label_regex, re.IGNORECASE)
    for i, ln in enumerate(linhas):
        if lab.search(ln):
            # caminhar pelas próximas N linhas
            for j in range(1, max_lookahead + 1):
                if i + j >= len(linhas):
                    break
                cand = linhas[i + j].strip()
                if not cand:
                    continue
                # rótulo com ":" (já ignorávamos)
                if re.match(r"^[A-ZÀ-Ü0-9/ .-]{1,30}:$", cand.upper()):
                    continue
                # rótulo sem ":" (ex.: "COR PREDOMINANTE", "POTÊNCIA CILINDRADA")
                if is_label_line(cand):
                    continue
                # filtro semântico opcional
                if filtro_valor:
                    ok = filtro_valor(cand)
                    if isinstance(ok, str) and ok.strip():
                        return ok.strip()
                    if ok is True:
                        return cand
                    # ok False => continua buscando
                    continue
                else:
                    return cand
    return None


def extrair_campos_crlv_regex(texto: str):
    t = normalizar_texto_basico(texto)
    campos = {}

    campos["Renavam"] = _buscar_primeiro(t, [
        r"(?:C[ÓO]DIGO\s*)?RENAVAM[:\s]*([\d\.]{9,14})",
        r"RENAVAM\s*\n\s*([\d\.]{9,14})",
    ])
    campos["Número do CRV"] = _buscar_primeiro(t, [
        r"(?:N[ÚU]MERO\s+DO\s+CRV|CRV)\s*[:\s]*([A-Z0-9\-]{6,})",
    ])
    campos["Ano Fabricação"] = _buscar_primeiro(t, [
        r"ANO\s*(?:DE\s*)?FABRICA[ÇC][ÃA]O\s*[:\s]*([12]\d{3})",
    ])
    campos["Ano Modelo"] = _buscar_primeiro(t, [
        r"ANO\s*MODELO\s*[:\s]*([12]\d{3})",
    ])
    campos["Categoria"] = _buscar_primeiro(t, [
        r"\bCATEGORIA\b\s*[:\s]*([A-ZÀ-Ü ]+)",
    ])
    campos["Espécie / Tipo"] = _buscar_primeiro(t, [
        r"ESP[ÉE]CIE\s*/\s*TIPO\s*[:\s]*([A-ZÀ-Ü ]+)",
    ])
    return campos

def _buscar_primeiro(texto_norm: str, padroes):
    for padrao in padroes:
        m = re.search(padrao, texto_norm, flags=re.IGNORECASE | re.DOTALL)
        if m:
            return limpar_valor(m.group(1))
    return None

def extrair_kv_generico(texto: str):
    linhas = [ln.strip() for ln in texto.splitlines()]
    kv = {}
    ultimo_label = None
    for ln in linhas:
        if ":" in ln:
            rotulo, valor = ln.split(":", 1)
            rotulo, valor = rotulo.strip(), valor.strip()
            if valor:
                kv[rotulo] = valor
                ultimo_label = None
            else:
                ultimo_label = rotulo
        else:
            if ultimo_label and ln:
                kv[ultimo_label] = ln.strip()
                ultimo_label = None
    return kv

def mapear_kv_para_campos(kv_dict):
    resultado = {}

    def norm_label(s: str):
        s = s.upper().strip()
        s = re.sub(r"[^A-Z0-9/ ]", "", s)
        s = re.sub(r"\s+", " ", s)
        return s

    mapa = {
        "PLACA": "Placa",
        "RENAVAM": "Renavam",
        "CHASSI": "Chassi",
        "N DO MOTOR": "Motor",
        "MOTOR": "Motor",
        "ANO DE FABRICACAO": "Ano Fabricação",
        "ANO FABRICACAO": "Ano Fabricação",
        "ANO MODELO": "Ano Modelo",
        "MARCA/MODELO/VERSAO": "Modelo",
        "MARCA/MODELO": "Modelo",
        "MODELO": "Modelo",
        "COR": "Cor",
        "COR PREDOMINANTE": "Cor",
        "COMBUSTIVEL": "Combustível",
        "ESPECIE/TIPO": "Espécie / Tipo",
        "TIPO": "Espécie / Tipo",
        "CATEGORIA": "Categoria",
        "CAPACIDADE": "Capacidade",
        "POT/CIL": "Potência/Cilindrada",
        "POTENCIA/CILINDRADA": "Potência/Cilindrada",
        "PESO BRUTO TOTAL": "Peso Bruto Total",
        "PBT": "Peso Bruto Total",
        "CARROCERIA": "Carroceria",
        "NOME": "Proprietário",
        "NOME/RAZAO SOCIAL": "Proprietário",
        "CPF": "CPF",
        "CNPJ": "CNPJ",
        "CPF/CNPJ": "CPF/CNPJ",
        "LOCAL": "Local",
        "MUNICIPIO/UF": "Local_UF",
        "UF": "UF",
        "DATA EMISSAO": "Data Emissão",
        "NUMERO DO CRV": "Número do CRV",
        "CRV": "Número do CRV",
        "MENSAGENS SENATRAN": "Mensagens SENATRAN",
        "SEGURO DPVAT": "Seguro DPVAT",
    }

    for rotulo, valor in kv_dict.items():
        rl = norm_label(rotulo)
        alvo = mapa.get(rl)
        if not alvo:
            rl2 = rl.replace(" ", "")
            mapa_compacto = {
                "MARCA/MODELO/VERSAO": "Modelo",
                "CPF/CNPJ": "CPF/CNPJ",
                "MUNICIPIO/UF": "Local_UF",
            }
            alvo = mapa.get(rl2) or mapa_compacto.get(rl2)

        if alvo:
            if alvo == "Local_UF":
                v = valor.strip().upper().replace("-", " ").replace("/", " ")
                partes = v.split()
                if len(partes) >= 2 and len(partes[-1]) == 2:
                    resultado["Local"] = " ".join(partes[:-1])
                    resultado["UF"] = partes[-1]
                else:
                    resultado["Local"] = valor
            elif alvo == "CPF/CNPJ":
                dig = somente_digitos(valor)
                if len(dig) <= 11:
                    resultado["CPF"] = valor
                else:
                    resultado["CNPJ"] = valor
            else:
                resultado[alvo] = valor

    return resultado

def extrair_campos_crlv(texto: str):
    t_norm = normalizar_texto_basico(texto)
    lns = [ln.strip() for ln in t_norm.splitlines() if ln.strip()]
    dados = {k: None for k in CAMPOS_PADRAO if k != "Arquivo"}

    # === (1) Regex estáveis ===
    m = re.search(r"(?:C[ÓO]DIGO\s*)?RENAVAM[:\s]*([\d\.]{9,14})", t_norm, re.IGNORECASE);  dados["Renavam"] = limpar_valor(m.group(1)) if m else None
    m = re.search(r"(?:N[ÚU]MERO\s+DO\s+CRV|CRV)\s*[:\s]*([A-Z0-9\-]{6,})", t_norm, re.IGNORECASE);  dados["Número do CRV"] = limpar_valor(m.group(1)) if m else None
    m = re.search(r"ANO\s*(?:DE\s*)?FABRICA[ÇC][ÃA]O\s*[:\s]*([12]\d{3})", t_norm, re.IGNORECASE);  dados["Ano Fabricação"] = limpar_valor(m.group(1)) if m else None
    m = re.search(r"ANO\s*MODELO\s*[:\s]*([12]\d{3})", t_norm, re.IGNORECASE);                     dados["Ano Modelo"] = limpar_valor(m.group(1)) if m else None
    m = re.search(r"\bCATEGORIA\b\s*[:\s]*([A-ZÀ-Ü ]+)", t_norm);                                   dados["Categoria"] = limpar_valor(m.group(1)) if m else None
    m = re.search(r"ESP[ÉE]CIE\s*/\s*TIPO\s*[:\s]*([A-ZÀ-Ü ]+)", t_norm);                           dados["Espécie / Tipo"] = limpar_valor(m.group(1)) if m else None

    # === (2) Scanner rótulo → valor (lookahead adaptativo) ===
    rotulos = [
        (re.compile(r"^\bPLACA\b$", re.IGNORECASE), "Placa", lambda v: True, 8),
        (re.compile(r"^\bCHASSI\b$", re.IGNORECASE), "Chassi", val_chassi, 60),
        (re.compile(r"^\bMOTOR\b$", re.IGNORECASE), "Motor", val_motor, 12),
        (re.compile(r"^MARCA\s*/\s*MODELO(?:\s*/\s*VERS[ÃA]O)?$", re.IGNORECASE), "Modelo", lambda v: True, 4),
        (re.compile(r"^COR(?:\s*PREDOMINANTE)?$", re.IGNORECASE), "Cor", is_cor, 60),
        (re.compile(r"^COMBUST[ÍI]VEL|^COMB$", re.IGNORECASE), "Combustível", is_combustivel, 12),
        (re.compile(r"^\bCAPACIDADE\b|^CAP\.$", re.IGNORECASE), "Capacidade", val_capacidade, 50),
        (re.compile(r"^PESO\s+BRUTO\s+TOTAL$|^PBT(?:\s*\(KG\))?$", re.IGNORECASE), "Peso Bruto Total", lambda v: bool(re.search(r"\d", v)), 20),
        (re.compile(r"^\bCARROCERIA\b$", re.IGNORECASE), "Carroceria", lambda v: True, 12),
        (re.compile(r"^(?:NOME|NOME/RAZ[ÃA]O\s+SOCIAL)$", re.IGNORECASE), "Proprietário",
         lambda v: (v.upper() not in SKIP_VALUES) and len(v) >= 5 and re.search(r"[A-ZÀ-Ü]", v) and not re.fullmatch(r"[\d.,/ -]+", v), 12),
        (re.compile(r"^CPF\s*/\s*CNPJ$|^CPF/ ?CNPJ$", re.IGNORECASE), "CPF/CNPJ", lambda v: bool(re.search(r"\d", v)), 8),
        (re.compile(r"^\bLOCAL\b$", re.IGNORECASE), "Local", lambda v: not re.match(r"^(CPF|CNPJ|DATA)\b", v, re.IGNORECASE), 8),
        (re.compile(r"^\bDATA(?:\s*DE)?\s*EMISS[ÃA]O?$|^DATA$", re.IGNORECASE), "Data Emissão",
         lambda v: re.match(r"\d{2}/\d{2}/\d{4}$", v) is not None, 8),
        (re.compile(r"^POT[ÊE]NCIA[ /]?CILINDRADA$|^POT/CIL$", re.IGNORECASE), "Potência/Cilindrada", val_potcil, 8),
        (re.compile(r"^(C[ÓO]D(?:\.|IGO)?\s*DE\s*SEGURAN[ÇC]A\s*DO\s*CLA|C[ÓO]D(?:\.|IGO)?\s*SEGURAN[ÇC]A\s*CLA|C[ÓO]DIGO\s*DE\s*SEGURAN[ÇC]A\s*DO\s*CRLV[- ]?E|^CLA$)$", re.IGNORECASE),
            "Código Segurança CLA",
            # validador: primeira linha com 8–20 dígitos
            lambda v: re.search(r"\b\d{8,20}\b", v.replace(" ", "")) is not None,
            10  # lookahead maior porque no seu OCR há 'CAT' no meio
            ),
    ]

    i = 0
    while i < len(lns):
        ln = lns[i]
        for pad, campo, validator, la in rotulos:
            if pad.match(ln):
                valor, idxv = proximo_valor(lns, i, max_lookahead=la, validator=validator)
                if valor:
                    if campo == "Placa":
                        dados["Placa"] = normalizar_placa(valor)
                    elif campo == "Chassi":
                        dados["Chassi"] = re.sub(r"[^A-Z0-9]", "", valor.upper())
                    elif campo == "Carroceria":
                        cv = valor.upper()
                        cv = (cv.replace("NAO", "NÃO").replace("N A O", "NÃO").replace("N A", "NÃO ").replace("NA O", "NÃO"))
                        if cv.upper() not in SKIP_VALUES: dados["Carroceria"] = limpar_valor(cv)
                    elif campo == "Proprietário":
                        dados["Proprietário"] = limpar_valor(valor)
                    elif campo == "CPF/CNPJ":
                        dig = re.sub(r"\D", "", valor)
                        if len(dig) <= 11: dados["CPF"] = formatar_cpf(valor)
                        else:              dados["CNPJ"] = formatar_cnpj(valor)

                    elif campo == "Capacidade":
                        # Janela: do rótulo CAPACIDADE até o próximo rótulo conhecido
                        # (evita capturar 162 de 'POTÊNCIA/CILINDRADA')
                        j = i + 1
                        while j < len(lns) and not is_known_label(lns[j]):
                            cand = lns[j].strip()
                            if cand.upper() not in SKIP_VALUES:
                                valnum = extrair_num_capacidade(cand)
                                if valnum:
                                    dados["Capacidade"] = limpar_valor(valnum)
                                    break
                            j += 1

                        # Capacidade: se ainda vazia, varre a partir do rótulo até encontrar número
                        if not dados.get("Capacidade"):
                            for idx, ln in enumerate(lns):
                                if _tokey(ln) in {_tokey("CAPACIDADE"), _tokey("CAP.")}:
                                    j = idx + 1
                                    while j < len(lns) and not is_known_label(lns[j]):
                                        cand = lns[j].strip()
                                        if cand.upper() not in SKIP_VALUES:
                                            valnum = extrair_num_capacidade(cand)
                                            if valnum:
                                                dados["Capacidade"] = limpar_valor(valnum)
                                                break
                                        j += 1
                                    if dados.get("Capacidade"):
                                        break

                    elif campo == "Código Segurança CLA":
                        # pula linhas 'CAT' e rótulos; captura a primeira sequência numérica 8-20 dígitos
                        # se 'valor' direto não servir, procura nas linhas seguintes (o validator já garantiu que tem número)
                        def pick_code(s: str):
                            if not s: return None
                            s2 = s.replace(" ", "")
                            m = re.search(r"\b\d{8,20}\b", s2)
                            return m.group(0) if m else None

                        code = pick_code(valor)
                        if not code:
                            # varre mais algumas linhas no mesmo bloco
                            j = i + 1
                            while j < len(lns) and j <= i + 10:
                                cand = lns[j].strip()
                                if cand.upper() in SKIP_VALUES or is_known_label(cand) or _tokey(cand) == _tokey("CAT"):
                                    j += 1
                                    continue
                                code = pick_code(cand)
                                if code:
                                    break
                                j += 1
                        if code:
                            dados["Código Segurança CLA"] = code

                            # Código Segurança CLA: fallback global por regex “largão”
                        if not dados.get("Código Segurança CLA"):
                            m = re.search(
                                r"(C[ÓO]D(?:\.|IGO)?\s*DE\s*SEGURAN[ÇC]A\s*DO\s*CLA|C[ÓO]D(?:\.|IGO)?\s*SEGURAN[ÇC]A\s*CLA|C[ÓO]DIGO\s*DE\s*SEGURAN[ÇC]A\s*DO\s*CRLV[- ]?E)[\s:\n\r]*"
                                r"(?:CAT\s*)?[\s:\n\r]*([0-9][0-9 \t]{7,30})",
                                t_norm, re.IGNORECASE
                            )
                            if m:
                                code = re.sub(r"\D", "", m.group(2))
                                if 8 <= len(code) <= 20:
                                    dados["Código Segurança CLA"] = code

                    elif campo == "Potência/Cilindrada":
                        txt = valor.upper().replace(" OCV", " 0CV").replace("OCV", "0CV")
                        dados["Potência/Cilindrada"] = limpar_valor(txt)
                    elif campo == "Local":
                        if not dados.get("Local"): dados["Local"] = limpar_valor(valor)
                    elif campo == "Modelo":
                        if valor.strip("* "): dados["Modelo"] = limpar_valor(valor)
                        else:
                            prox, _ = proximo_valor(lns, i+1, max_lookahead=3, validator=lambda v: True)
                            if prox: dados["Modelo"] = limpar_valor(prox)
                    else:
                        dados[campo] = limpar_valor(valor)
                break
        i += 1

    # === (3) Correções específicas de layout (varredura abrangente) ===

    # 3.1 COMBUSTÍVEL: procure Cor e Combustível juntos após esse rótulo
    if not (dados.get("Cor") and dados.get("Combustível")):
        for idx, ln in enumerate(lns):
            if re.search(r"COMBUST[ÍI]VEL|^COMB$", ln, re.IGNORECASE):
                cor_found, comb_found = dados.get("Cor"), dados.get("Combustível")
                for j in range(idx+1, min(idx+15, len(lns))):
                    cand = lns[j].strip()
                    if cand.upper() in SKIP_VALUES or is_known_label(cand):
                        continue
                    if not cor_found and is_cor(cand):
                        cor_found = cand
                    elif not comb_found and is_combustivel(cand):
                        comb_found = cand
                    # Caso invertido: se achar Comb antes e depois uma Cor válida, mantém ambos
                if cor_found and not dados.get("Cor"): dados["Cor"] = limpar_valor(cor_found)
                if comb_found and not dados.get("Combustível"): dados["Combustível"] = limpar_valor(comb_found)
                if dados.get("Cor") and dados.get("Combustível"):
                    break

    # 3.2 CHASSI: pegue a ÚLTIMA ocorrência válida (no rodapé do seu PDF)
    if not dados.get("Chassi"):
        last_ch = None
        for m in re.finditer(r"CHASSI\s*[\r\n ]+([A-Z0-9*/\-]{6,})", t_norm, re.IGNORECASE):
            cand = m.group(1).strip()
            if "*" in cand:
                continue
            if val_chassi(cand):
                last_ch = re.sub(r"[^A-Z0-9]", "", cand.upper())
        if last_ch:
            dados["Chassi"] = last_ch

    # 3.3 CAPACIDADE: se ainda vazia, varra até encontrar número (evita motor/POT/CIL)
    # Capacidade: se ainda vazia, varre a partir do rótulo até encontrar número (evita motor/POT/CIL)
    if not dados.get("Capacidade"):
        for idx, ln in enumerate(lns):
            if _tokey(ln) in {_tokey("CAPACIDADE"), _tokey("CAP.")}:
                # examina até 50 linhas adiante (ou até próximo rótulo conhecido)
                for j in range(idx + 1, min(idx + 50, len(lns))):
                    cand = lns[j].strip()
                    if cand.upper() in SKIP_VALUES or is_known_label(cand):
                        continue
                    valnum = extrair_num_capacidade(cand)
                    if valnum:
                        dados["Capacidade"] = limpar_valor(valnum)
                        break
                if dados.get("Capacidade"):
                    break


    # 3.4 PROPRIETÁRIO: se vazio/ruim, pegue a linha após NOME com letras (>=5)
    if not dados.get("Proprietário") or len(dados.get("Proprietário") or "") < 5 or re.fullmatch(r"[\d.,/ -]+", dados.get("Proprietário") or ""):
        for idx, ln in enumerate(lns):
            if _tokey(ln) in {_tokey("NOME"), _tokey("NOME/RAZÃO SOCIAL")}:
                val, _ = proximo_valor(
                    lns, idx, max_lookahead=10,
                    validator=lambda v: (v.upper() not in SKIP_VALUES) and len(v) >= 5 and re.search(r"[A-ZÀ-Ü]", v) and not re.fullmatch(r"[\d.,/ -]+", v)
                )
                if val:
                    dados["Proprietário"] = limpar_valor(val)
                    break

    # 3.5 CARROCERIA: reforço global
    if not dados.get("Carroceria"):
        m = re.search(r"\bCARROCERIA\b\s*[\r\n ]+([A-ZÀ-Ü ]+)", t_norm, re.IGNORECASE)
        if m:
            cv = m.group(1).upper()
            cv = (cv.replace("NAO", "NÃO").replace("N A O", "NÃO").replace("N A", "NÃO ").replace("NA O", "NÃO"))
            if cv.upper() not in SKIP_VALUES:
                dados["Carroceria"] = limpar_valor(cv)

    # 3.6 Local + UF no rodapé (prioritário)
    ESTADOS_BR = {"AC","AL","AM","AP","BA","CE","DF","ES","GO","MA","MG","MS","MT","PA","PB","PE","PI","PR","RJ","RN","RO","RR","RS","SC","SE","SP","TO"}
    pad_cidade_uf = re.compile(r"^([A-ZÀ-Ü][A-ZÀ-Ü ]{2,})\s+([A-Z]{2})$")
    for ln in reversed(lns[-60:] if len(lns) > 60 else lns):
        m = pad_cidade_uf.match(ln)
        if m and m.group(2).upper() in ESTADOS_BR:
            dados["Local"] = limpar_valor(m.group(1))
            dados["UF"] = limpar_valor(m.group(2)).upper()
            break

    # 3.7 Mensagens / DPVAT (blocos)
    m = re.search(r"MENSAGENS\s+SENATRAN\s*(.+?)\n(?:DADOS\s+DO\s+SEGURO|SEGURO|DPVAT|$)", t_norm, re.IGNORECASE | re.DOTALL);
    if m: dados["Mensagens SENATRAN"] = limpar_valor(m.group(1))
    m = re.search(r"(?:INFORMA[ÇC][ÕO]ES\s+DO\s+SEGURO\s+DPVAT|SEGURO\s+DPVAT)\s*(.+?)\n(?:OBS|$)", t_norm, re.IGNORECASE | re.DOTALL);
    if m: dados["Seguro DPVAT"] = limpar_valor(m.group(1))



# ================== FALLBACKS ESPECÍFICOS DETRAN-SE ==================

    t_norm_up = t_norm.upper()

    # DETRAN-SE: CAPACIDADE (valor pode vir 1-2 linhas abaixo do rótulo)
    if dados.get("Capacidade") in (None, "", 0):
        cap = find_number_after(r"\bCAPACIDADE\b", t_norm_up, max_ahead_chars=200)
        if cap is not None:
            dados["Capacidade"] = cap
        else:
            # fallback extra: procure depois do par "CATEGORIA CAPACIDADE"
            cap = find_number_after(r"\bCATEGORIA\s+CAPACIDADE\b", t_norm_up, max_ahead_chars=220)
            if cap is not None:
                dados["Capacidade"] = cap
            else:
                print("[DBG] CAPACIDADE ainda não encontrada (após CAPACIDADE).")

    # PBT (PESO BRUTO TOTAL vem ao lado de POTÊNCIA/CILINDRADA e o valor 0.28 aparece na linha seguinte)
    if not dados.get("Peso Bruto Total") or str(dados.get("Peso Bruto Total")).strip() in ("", "0", "0.0"):
        # Janela a partir do rótulo direto
        pbt = find_number_after_smart(r"\bPESO\s+BRUTO\s+TOTAL\b", t_norm.upper(), max_ahead_chars=240)

        # Fallback: quando o OCR agrupa com POTÊNCIA/CILINDRADA
        if pbt is None:
            pbt = find_number_after_smart(
                r"\bPOT[ÊE]NCIA\s*/\s*CILINDRADA\s+PESO\s+BRUTO\s+TOTAL\b",
                t_norm.upper(),
                max_ahead_chars=260
            )

        if pbt is not None:
            dados["Peso Bruto Total"] = pbt
        else:
            print("[DBG] PBT ainda não encontrada (após PBT).")


    # (SE) COR + COMBUSTÍVEL em linha composta ("COR PREDOMINANTE COMBUSTÍVEL BRANCA ALCOOL/GASOLINA * * *")
    if (not dados.get("Cor")) or (not dados.get("Combustível")) or (" " in (dados.get("Cor") or "")):
        m = re.search(
            r"COR\s*PREDOMINANTE\s+COMBUST[ÍI]VEL\s+(.*?)\s+(ALCOOL|ETANOL|GASOLINA(?:\s+[AC])?|DIESEL(?:\s+S\d+)?|GNV|ELETRICO|H[ÍI]BRIDO)(?:/| )?(ALCOOL|ETANOL|GASOLINA(?:\s+[AC])?|DIESEL(?:\s+S\d+)?|GNV|ELETRICO|H[ÍI]BRIDO)?",
            t_norm, re.IGNORECASE | re.DOTALL
        )
        if m:
            # COR: pega só a 1ª palavra de cor
            cor_tok = (m.group(1) or "").strip().split()[0].upper()
            if is_cor(cor_tok):
                dados["Cor"] = cor_tok
            # COMB: normaliza ETANOL->ALCOOL
            comb1 = (m.group(2) or "").upper().replace("ETANOL", "ALCOOL")
            comb2 = (m.group(3) or "")
            comb2 = comb2.upper().replace("ETANOL", "ALCOOL") if comb2 else None
            comb_full = comb1 if not comb2 else f"{comb1}/{comb2}"
            if is_combustivel(comb_full):
                dados["Combustível"] = comb_full

    # (SE) MOTOR: ignora ruídos ('QRCode', '***', 'CMT') e pega o primeiro token plausível
        motor = pick_first_token_after(r"\bMOTOR\b", t_norm, max_ahead_chars=400, reject={"QRCODE", "***", "CMT"})
        if motor and val_motor(motor):
            dados["Motor"] = motor

# CHASSI: pega a última ocorrência válida algumas linhas abaixo do rótulo
    if not dados.get("Chassi"):
        ch = extract_chassi(t_norm_up)
        # Se tiver um validador próprio, aplique:
        # if ch and val_chassi(ch): dados["Chassi"] = ch
        # else: mantém None
        if ch:
            dados["Chassi"] = ch

    # (SE) ANO FAB + ANO MOD na mesma linha (garantia extra)
    if not dados.get("Ano Fabricação") or not dados.get("Ano Modelo"):
        m = re.search(r"ANO\s*FABRICA[ÇC][ÃA]O\s+ANO\s*MODELO\s+([12]\d{3})\s+([12]\d{3})", t_norm, re.IGNORECASE)
        if m:
            dados["Ano Fabricação"] = dados.get("Ano Fabricação") or m.group(1)
            dados["Ano Modelo"]     = dados.get("Ano Modelo")     or m.group(2)


    # TIOPM_ANO_VEICULO = Ano Modelo / Ano Fabricação
    ano_mod = dados.get("Ano Modelo")
    ano_fab = dados.get("Ano Fabricação")
    if ano_mod and ano_fab:
        dados["TIPO_ANO_VEICULO"] = f"{ano_mod}/{ano_fab}"

    # (SE) LOCAL/UF/DATA — formatos 1 (mesma linha) e 2 (labels em coluna)
    if not (dados.get("Local") and dados.get("UF") and dados.get("Data Emissão")):
        m = re.search(r"\bLOCAL\b\s*\bDATA\b.*?([A-ZÀ-Ü ]+?)\s+([A-Z]{2})\s+(\d{2}/\d{2}/\d{4})",
                    t_norm, re.IGNORECASE | re.DOTALL)
        if m:
            dados["Local"] = limpar_valor(m.group(1))
            dados["UF"] = m.group(2).upper()
            dados["Data Emissão"] = m.group(3)
    if not (dados.get("Local") and dados.get("UF") and dados.get("Data Emissão")):
        m = re.search(r"\bLOCAL\b\s*[\r\n ]+([A-ZÀ-Ü ]+)\s+([A-Z]{2})\s*[\r\n ]+\bDATA\b\s*[\r\n ]+(\d{2}/\d{2}/\d{4})",
                    t_norm, re.IGNORECASE)
        if m:
            dados["Local"] = limpar_valor(m.group(1))
            dados["UF"] = m.group(2).upper()
            dados["Data Emissão"] = m.group(3)

    # (SE) ESPÉCIE / TIPO — evita capturar 'CAT' (de CLA)
    if (not dados.get("Espécie / Tipo")) or dados.get("Espécie / Tipo") in {"CAT","CAPACIDADE","PESO BRUTO TOTAL"}:
        m = re.search(r"ESP[ÉE]CIE\s*/\s*TIPO\s*[\r\n ]+([A-ZÀ-Ü ]{5,})", t_norm, re.IGNORECASE)
        if m:
            cand = m.group(1).strip()
            # corta no primeiro rótulo típico da sequência SE
            cand = re.split(r"\bCAT\.?\.?\s*TARIF\b|\bPLACA\s+ANTERIOR\b|\bC[ÓO]DIGO\b", cand)[0].strip()
            if cand not in {"CAT","CAPACIDADE"} and len(cand) >= 5:
                dados["Espécie / Tipo"] = cand

    # (SE) CATEGORIA — reforça conjunto fechado
    if (not dados.get("Categoria")) or dados.get("Categoria") in {"CAPACIDADE","ESPÉCIE / TIPO"}:
        m = re.search(r"\b(PARTICULAR|OFICIAL|ALUGUEL|COLE[ÇC][ÃA]O|DIPLOM[AÁ]TICO)\b", t_norm, re.IGNORECASE)
        if m:
            dados["Categoria"] = m.group(1).upper()

    # (SE) PBT — não gravar 0 quando existe 0.28
    if (not dados.get("Peso Bruto Total")) or str(dados.get("Peso Bruto Total")).strip() in {"0","0.0","0,0"}:
        m = re.search(r"\bPESO\s+BRUTO\s+TOTAL\b(?:.*?)(\d+(?:[.,]\d+)?)",
                    t_norm, re.IGNORECASE | re.DOTALL)
        if m:
            dados["Peso Bruto Total"] = m.group(1)

    # (SE) Código Segurança CLA (com 'CAT' no meio)
    if not dados.get("Código Segurança CLA"):
        m = re.search(r"C[ÓO]DIGO\s*DE\s*SEGURAN[ÇC]A\s*DO\s*CLA.*?([0-9]{8,20})",
                    t_norm, re.IGNORECASE | re.DOTALL)
        if m:
            dados["Código Segurança CLA"] = m.group(1)

    # (SE) Número Segurança CRV — pág. 2 (se você já incluiu a coluna no schema)
    if not dados.get("Número Segurança CRV"):
        m = re.search(r"N[ÚU]MERO\s+DE\s+SEGURAN[ÇC]A\s+DO\s+CRV\s*[\r\n ]+([0-9]{8,20})",
                    t_norm, re.IGNORECASE)
        if m:
            dados["Número Segurança CRV"] = m.group(1)

    # ============== Sanitização final de COR (fica só a cor) ==============
    if dados.get("Cor"):
        cores = re.findall(r"\b(BRANCA|PRETA|PRATA|VERMELHA|AZUL|VERDE|AMARELA|CINZA|MARROM|DOURADA|LARANJA|BEGE)\b",
                        dados["Cor"].upper())
        dados["Cor"] = cores[0] if cores else None

    # (SE) PLACA
    if not dados.get("Placa"):
        placa = extract_placa(t_norm_up)
        if placa:
            dados["Placa"] = placa

    # (SE) NÚMERO DE SEGURANÇA DO CRV
    if not dados.get("NumeroSegurancaCRV"):
        ren = dados.get("Renavam")

        # 1) Tenta rótulo (qualquer página)
        num_seg = extrair_num_seguranca_crv(texto, renavam=ren)

        # 2) Se não achou, tenta PÁGINA 2 (SENATRAN)
        if not num_seg:
            num_seg = extrair_num_seguranca_crv_pag2(texto)

        # 3) Sanitização dura: extrai exatamente 11 dígitos com boundaries
        if num_seg:
            m = re.search(r'(?<!\d)\d{11}(?!\d)', str(num_seg))
            num_seg = m.group(0) if m else None

        # 4) Anti-falso-positivo: igual ao RENAVAM -> descarta
        if num_seg and ren:
            if re.sub(r"\D", "", str(ren)) == re.sub(r"\D", "", str(num_seg)):
                num_seg = None

        # 5) Atribui somente se for EXATAMENTE 11 dígitos
        if num_seg and len(num_seg) == 11:
            dados["NumeroSegurancaCRV"] = num_seg



# CAPACIDADE
    if dados.get("Capacidade") in (None, "", 0):
        # permite quebra de linha entre rótulo e número
        m = re.search(r"\bCAPACIDADE\b\s*([0-9\.,]+)", t_norm_up, flags=re.DOTALL)
        if not m:
            # fallback: captura a linha toda e extrai o primeiro número
            m = re.search(r"\bCAPACIDADE\b([^\n\r]{0,30})", t_norm_up)
            if m:
                dados["Capacidade"] = to_float_safe(m.group(1))
        else:
            dados["Capacidade"] = to_float_safe(m.group(1))

        if dados.get("Capacidade") is None:
            print("[DBG] CAPACIDADE não encontrada após o rótulo.")

    # PBT (PESO BRUTO TOTAL)
    if not dados.get("Peso Bruto Total") or str(dados.get("Peso Bruto Total")).strip() in ("", "0"):
        pbt = find_number_after(r"\bPESO\s+BRUTO\s+TOTAL\b", t_norm_up, max_ahead_chars=180)
        if pbt is None:
            # fallback: rótulos na mesma linha (OCR agrupa com POTÊNCIA/CILINDRADA)
            pbt = find_number_after(r"\bPOT[ÊE]NCIA\s*/\s*CILINDRADA\s+PESO\s+BRUTO\s+TOTAL\b",
                                    t_norm_up, max_ahead_chars=240)
        if pbt is not None:
            dados["Peso Bruto Total"] = pbt
        else:
            print("[DBG] PBT ainda não encontrada (após PBT).")

    # DETRAN-SE: POTÊNCIA/CILINDRADA
    m = re.search(r"POT[ÊE]NCIA\s*/\s*CILINDRADA\s+([A-Z0-9/\.\s]+)", t_norm_up)
    if m:
        raw = m.group(1)
        # potencia (CV)
        mcv = re.search(r"(\d+)\s*CV", raw)
        # cilindrada
        mcc = re.search(r"/\s*(\d{2,4})\b", raw)  # 125, 160, 999 etc.
        pot = mcv.group(1) if mcv else None
        cil = mcc.group(1) if mcc else None
        if pot or cil:
            if pot and cil:
                dados["Potência/Cilindrada"] = f"{pot}CV/{cil}"
            elif cil:
                dados["Potência/Cilindrada"] = f"{cil}"  # ou deixar None se preferir obrigar

    # Proprietário (NOME)
    if not dados.get("Proprietário"):
        # pega o trecho entre 'NOME' e 'CPF / CNPJ'
        m = re.search(r"\bNOME\b\s*([\s\S]{1,120}?)\bCPF\s*/\s*CNPJ\b", t_norm, flags=re.IGNORECASE)
        if m:
            nome_raw = m.group(1)
            # limpa quebras e múltiplos espaços
            nome = re.sub(r"[\r\n]+", " ", nome_raw).strip()
            # remove eventuais rotulações perdidas
            nome = re.sub(r"\bASSINADO DIGITALMENTE PELO DETRAN\b", "", nome, flags=re.IGNORECASE).strip()
            # se ficar muito curto/ruidoso, guarda None
            if len(nome) >= 3:
                dados["Proprietário"] = nome.upper()
            else:
                print("[DBG] NOME capturado muito curto: ", repr(nome))
        else:
            print("[DBG] Bloco NOME ... CPF/CNPJ não localizado.")



    # === (4) Normalizações finais ===
    if dados.get("CNPJ"): dados["CNPJ"] = formatar_cnpj(dados["CNPJ"])
    if dados.get("CPF"):  dados["CPF"]  = formatar_cpf(dados["CPF"])
    if dados.get("Placa"):
        dados["Placa"] = normalizar_placa(dados["Placa"])

    # === Derivados de 'Modelo' ===
    fab, mod = parse_marca_modelo(dados.get("Modelo"))
    dados["Fabricante"] = fab
    dados["Modelo_Limpo"] = mod

    if dados.get("Fabricante"):
        dados["Fabricante"] = dados["Fabricante"].upper()
    if dados.get("Modelo_Limpo"):
         dados["Modelo_Limpo"] = dados["Modelo_Limpo"].upper()

    # === Derivados de 'Combustível' ===
    c_pri, c_sec = split_combustivel(dados.get("Combustível"))

    # Garante UPPER (mesmo que o helper já trate)
    dados["Combustivel_Principal"]  = c_pri.upper() if c_pri else None
    dados["Combustivel_Secundario"] = c_sec.upper() if c_sec else None

    # Garante todas as colunas
    for col in CAMPOS_PADRAO:
        if col != "Arquivo" and col not in dados:
            dados[col] = None

    for campo in ["Placa"]:
        if dados.get(campo):
            dados[campo] = formatar_codigo(dados[campo])

    return dados


# ===================== PIPELINE EM LOTE + GUI =====================
class ProcessorThread(threading.Thread):
    def __init__(self, pasta, apikey, ui, saida_excel_path=None, **kwargs):
        super().__init__(daemon=True)
        self.pasta = pasta
        self.apikey = apikey or OCR_SPACE_APIKEY_DEFAULT
        self.ui = ui
        self.cancelado = False
        self.saida_excel_path = saida_excel_path

        #Aceita extra_campos se vier (compatível com versões antigas e novas do UI)
        self.extra_campos = kwargs.get("extra_campos") or {}


  # Coletores simples para um possível pós-processamento/salvamento
        self._result_rows = []   # Estrutura: list[dict]
        self._errors = []        # Estrutura: list[str]

    def _ensure_xlsx(self, path: Path) -> Path:
            return path if path.suffix.lower() == ".xlsx" else path.with_suffix(".xlsx")

    def cancelar(self):
        self.cancelado = True

    def _ensure_ext(self, path: Path) -> Path:
        """Garante .xlsx ou .csv; se sem extensão, usa .xlsx por padrão."""
        if path.suffix.lower() in (".xlsx", ".csv"):
            return path
        return path.with_suffix(".xlsx")

    def _make_unique(self, path: Path) -> Path:
        """Evita sobrescrever arquivo existente: cria nome único."""
        if not path.exists():
            return path
        stem, suf, parent = path.stem, path.suffix, path.parent
        ts = time.strftime("%Y%m%d-%H%M%S")
        candidate = parent / f"{stem}_{ts}{suf}"
        i = 1
        while candidate.exists():
            candidate = parent / f"{stem}_{ts}({i}){suf}"
            i += 1
        return candidate

    def _has_write_access(self, path: Path) -> bool:
        """Verifica permissão de escrita na pasta de destino."""
        try:
            path.parent.mkdir(parents=True, exist_ok=True)
            probe = path.parent / f".__write_test_{int(time.time())}.tmp"
            with open(probe, "w", encoding="utf-8") as f:
                f.write("ok")
            probe.unlink(missing_ok=True)
            return True
        except Exception:
            return False

    def run(self):
        arquivos_pdf = [f for f in os.listdir(self.pasta) if f.lower().endswith(".pdf")]
        arquivos_pdf.sort()
        if not arquivos_pdf:
            self.ui.msg("Nenhum PDF encontrado.")
            return

        # Prepara log em arquivo (opcional)
        log_fp = None
        if DEBUG_LOG_ARQUIVO:
            try:
                log_fp = open(os.path.join(self.pasta, "crlv_debug.log"), "a", encoding="utf-8")
                print(f"[LOG] Escrevendo em: {log_fp.name}", flush=True)
            except Exception as e:
                print(f"[LOG] Falha ao abrir log: {e}", flush=True)
                log_fp = None

        def clog(msg: str):
            """Console + arquivo (se habilitado)"""
            if DEBUG_CONSOLE:
                print(msg, flush=True)
            if log_fp:
                try:
                    log_fp.write(msg + "\n")
                    log_fp.flush()
                except Exception:
                    pass

        resultados = []
        falhas = []
        total = len(arquivos_pdf)

        try:
            for i, nome_arquivo in enumerate(arquivos_pdf, 1):
                if self.cancelado:
                    self.ui.msg("Processamento cancelado pelo usuário.")
                    clog("[⛔] Processamento cancelado pelo usuário.")
                    break

                caminho_pdf = os.path.join(self.pasta, nome_arquivo)
                try:
                    texto = extrair_texto_pdf(caminho_pdf, self.apikey)
                    texto_norm = normalizar_texto_basico(texto)

                    # === DEBUG: texto extraído (normalizado) ===
                    clog("\n🔍 Texto extraído (normalizado):")
                    clog("=" * 80)
                    if texto_norm:
                        trecho = texto_norm[:DEBUG_TEXT_LIMIT]
                        clog(trecho)
                        if len(texto_norm) > DEBUG_TEXT_LIMIT:
                            clog("... [cortado]")
                    else:
                        clog("(vazio)")
                    clog("=" * 80)

                    # (Opcional) salva .ocr.txt por arquivo
                    if DEBUG_SALVAR_TXT:
                        try:
                            base = os.path.splitext(nome_arquivo)[0]
                            ts = time.strftime('%Y%m%d-%H%M%S')
                            with open(os.path.join(self.pasta, f"{base}.{ts}.ocr.txt"), "w", encoding="utf-8") as ftxt:

                                ftxt.write(texto)
                            clog(f"[💾] Texto OCR salvo em {base}.ocr.txt")
                        except Exception as e:
                            clog(f"[⚠] Falha ao salvar .ocr.txt: {e}")

                    # Extrai os campos
                    dados = extrair_campos_crlv(texto_norm)
                    dados["Arquivo"] = nome_arquivo

                    if SALVAR_TEXTO_BRUTO_NO_EXCEL:
                        dados["_TextoBruto"] = texto

                    # >>> injeta os extras vindos da UI (se houver)
                    for k, v in (self.extra_campos or {}).items():
                        if v is None or (isinstance(v, str) and not v.strip()):
                            continue
                        dados[k] = v
                        # loga cada par aplicado neste arquivo
                        clog(f"[EXTRA] {k} = {dados[k]}")

                    # Garante colunas
                    for col in CAMPOS_PADRAO:
                        dados.setdefault(col, "")

                    resultados.append(dados)

                    # UI progresso
                    self.ui.progresso(i / total * 100)

                    # Determina faltantes
                    faltantes = [c for c in CAMPOS_PADRAO if c not in ("Arquivo",) and not (dados.get(c) and str(dados.get(c)).strip())]

                    # === DEBUG: campos extraídos por arquivo ===
                    clog(f"[✔] Processado: {nome_arquivo}")
                    clog("Dados extraídos:")
                    for chave in CAMPOS_PADRAO:
                        clog(f"  {chave}: {dados.get(chave)}")
                    if SALVAR_TEXTO_BRUTO_NO_EXCEL:
                        clog("  _TextoBruto: (salvo no Excel)")

                    if faltantes:
                        self.ui.msg(f"⚠ {nome_arquivo}: faltando {', '.join(faltantes)}")
                        falhas.append({"Arquivo": nome_arquivo, "Faltantes": ", ".join(faltantes)})
                        clog(f"⚠ Faltantes: {', '.join(faltantes)}")
                    else:
                        self.ui.msg(f"✔ {nome_arquivo}")

                    clog("-" * 80)

                except Exception as e:
                    self.ui.msg(f"❌ Erro em {nome_arquivo}: {e}")
                    falhas.append({"Arquivo": nome_arquivo, "Erro": str(e)})
                    clog(f"[❌] Erro em {nome_arquivo}: {e}")
                # fim try arquivo

            # Salva Excel exatamente no caminho escolhido pelo usuário

            # --- Fim do processamento dos arquivos ---
            # No final do processamento:
            # --- Fim do processamento dos arquivos ---
# No final do processamento:
            if resultados:
                # 0) Mescla registros do MESMO veículo
                resultados_merged = coalesce_por_veiculo(resultados)

                # Log de auditoria
                try:
                    clog(f"[MERGE] Linhas antes: {len(resultados)} | depois: {len(resultados_merged)}")
                except Exception:
                    pass

                resultados = resultados_merged

                # 1) Monta o DF “cru” com todos os resultados do lote
                colunas = CAMPOS_PADRAO + (["_TextoBruto"] if SALVAR_TEXTO_BRUTO_NO_EXCEL else [])
                # (Opcional) se quiser ver as fontes no Excel:
                if any("_MergeFontes" in r for r in resultados):
                    colunas = list(dict.fromkeys(colunas + ["_MergeFontes"]))

                df = pd.DataFrame(resultados, columns=colunas)
                # <- NÃO use drop_duplicates por "Arquivo" aqui, pois já consolidamos por veículo.

                # 2) Transforma para o layout exato da planilha (cabeçalho linha 4)
                from transform_frota import build_frota_df

                # (Opcional) defaults SAP / fixos:
                defaults = {
                    "EQTYP": "V",
                     "INGRP": "PM1",
                     "GEWRK": "FRT-MEC",
                     "EXPIRY_DATE": "31.12.9999",
                     "MWERT4": "AGUARDANDO ATIVACAO",
                     "MWERT25" : "AGUARDANDO ATIVACAO",
                     "INDFIM" : "X",
                     "MWERT14":"01"
                }

                df_frota = build_frota_df(df, defaults=defaults)

                column_map = {
                    "EQTYP": 'EQTYP = "V"-Veículos',  # caso o cabeçalho seja esse texto
                     "INGRP": "INGRP (Fixo = PM1)",

                     "SWERK": "SWERK",
                     "IWERK": "IWERK",
                     "KOSTL": "KOSTL",
                     "TPLNR": "TPLNR",
                     "MWERT14": "MWERT14",
                     "RBNR": "RBNR"
                }


                # 3) Define o caminho de saída escolhido no "Salvar como..." OU um padrão com timestamp
                out_path = Path(self.saida_excel_path) if self.saida_excel_path else Path(self.pasta) / f"crlv_consolidado_{time.strftime('%Y%m%d-%H%M%S')}.xlsx"
                out_path = self._ensure_xlsx(out_path)
                out_path.parent.mkdir(parents=True, exist_ok=True)

                # (Opcional) Se você quer garantir que um TEMPLATE seja usado quando o arquivo ainda não existir:
                # from utils_paths import resource_path
                # import shutil
                # template_path = resource_path("templates", "ModeloFrota.xlsx")
                # if not out_path.exists():
                #     shutil.copy(template_path, out_path)

                # 4) Atualiza o Excel EXISTENTE, apenas a aba desejada, preservando layout
                from excel_writer import write_df_to_existing_template
                write_df_to_existing_template(
                    xlsx_path=out_path,
                    df=df_frota,
                    sheet_name="FROTA-Layout_excel_Geral",
                    header_row=4,     # cabeçalho na linha 4
                    data_start_row=6, # limpar da linha 6 para baixo e escrever
                    column_map=column_map,  # df_frota já tem os nomes iguais aos cabeçalhos da planilha
                    strict=False,
                )

                # 5) (Opcional) Exporta um CSV com as falhas detectadas no lote
                if GERAR_CSV_FALHAS and falhas:
                    try:
                        pd.DataFrame(falhas).to_csv(
                            Path(self.pasta) / f"crlv_falhas_{time.strftime('%Y%m%d-%H%M%S')}.csv",
                            index=False, sep=";", encoding="utf-8"
                        )
                    except Exception as e:
                        clog(f"[⚠] Falha ao salvar CSV de falhas: {e}")

                self.ui.msg(f"✔ Excel atualizado: {out_path}")
                clog(f"[💾] Excel atualizado (uma única escrita, preservando layout): {out_path}")

            else:
                self.ui.msg("Nenhum resultado para salvar.")
                clog("[ℹ] Nenhum dado extraído.")


        finally:
            if log_fp:
                try:
                    log_fp.close()
                except Exception:
                    pass
            self.ui.done()

# ===================== MAIN =====================
#if __name__ == "__main__":
#    ui = UI()
#    ui.run()

# pyinstaller --noconfirm --onefile --windowed --clean --name LEITOR_DOCUMENTO_CLRV --hidden-import=pdfplumber --hidden-import=extract_msg --hidden-import=openpyxl.styles.numbers "ui.py"

#myenv\Scripts\Activate.ps1
