# transform_frota.py
from __future__ import annotations

import re
from typing import Optional, Tuple, Any, Dict
import pandas as pd


# ========================= Helpers =========================

def _col(df: pd.DataFrame, name: str) -> pd.Series:
    """Retorna a coluna 'name' se existir, senão uma Series de None do mesmo tamanho."""
    if name in df.columns:
        return df[name]
    return pd.Series([None] * len(df), index=df.index, dtype=object)

def _first_not_null(*series: pd.Series) -> pd.Series:
    """Por linha, retorna o primeiro valor não vazio (não-nulo e não-string vazia) entre as séries."""
    valid = [s for s in series if s is not None]
    if not valid:
        return pd.Series([None] * 0, dtype=object)
    out = valid[0]
    for s in valid[1:]:
        out = out.where(out.notna() & (out.astype(str).str.strip() != ""), s)
    return out

def to_date_dmy(s: Optional[str]) -> Optional[pd.Timestamp]:
    if not s or not str(s).strip():
        return None
    return pd.to_datetime(str(s).strip(), dayfirst=True, errors="coerce")

def parse_pot_cil(s: Optional[str]) -> Tuple[Optional[float], Optional[str], Optional[float], Optional[str]]:
    """
    Ex.: '75CV/999' -> (75, 'CV', 999, 'CC')
    """
    if not s:
        return None, None, None, None
    txt = str(s).upper().strip().replace(" OCV", " 0CV").replace("OCV", "0CV")
    mcv  = re.search(r"(\d+)\s*CV", txt)
    mcil = re.search(r"/\s*(\d{2,4})\b", txt)
    power = float(mcv.group(1)) if mcv else None
    cap   = float(mcil.group(1)) if mcil else None
    return power, ("CV" if power is not None else None), cap, ("CC" if cap is not None else None)

def normalize_weight(value: Any) -> Tuple[Optional[float], Optional[str]]:
    """
    Regra exemplo:
      - v <= 20  -> (v, 'T')   (toneladas)
      - v  > 20  -> (v, 'KG')  (quilogramas)
    """
    if value is None or str(value).strip() == "":
        return None, None
    try:
        v = float(str(value).replace(",", "."))
    except Exception:
        return None, None
    return (v, "T") if v <= 20 else (v, "KG")

def normalize_fuel(s: Optional[str]) -> Tuple[Optional[str], Optional[str]]:
    if not s:
        return None, None
    upper = str(s).upper()
    parts = re.split(r"[\/\+\;\,\s]+", upper)
    parts = [p for p in parts if p]
    if not parts:
        return None, None

    def _norm(x: Optional[str]) -> Optional[str]:
        if not x:
            return x
        return (
            x.replace("ETANOL", "ALCOOL")
             .replace("ÁLCOOL", "ALCOOL")
             .replace("HÍBRIDO", "HIBRIDO")
        )

    pri = _norm(parts[0])
    sec = _norm(parts[1]) if len(parts) > 1 else None
    return pri, sec


def build_seq_codes(base: str, n: int, *, step: int = 1) -> list[str]:
    """
    Gera n códigos sequenciais preservando prefixo e zero-padding do bloco numérico final.
    Ex.: base='EPD-0001265', n=4 -> ['EPD-0001265','EPD-0001266','EPD-0001267','EPD-0001268']
    Se base não terminar com dígitos, apenda contadores simples (base1, base2, ...).
    """
    if not base:
        return [None] * n
    s = base.strip()
    m = re.search(r'^(.*?)(\d+)$', s)
    if not m:
        # sem dígitos no final -> base + contador simples
        return [f"{s}{i+1}" for i in range(n)]
    prefix, num = m.group(1), m.group(2)
    width = len(num)
    start = int(num)
    return [f"{prefix}{str(start + i*step).zfill(width)}" for i in range(n)]

# ========================= Transform principal =========================

OUT_COLS = [
    "EQUNR_SAP","EQART","EQTYP","SHTXT","GROES","INBDT","HERST","TYPBZ","INVNR","BAUJJ",
    "SWERK","ABCKZ","BUKRS","GSBER","KOSTL","IWERK","INGRP","GEWRK","WERGW","EQFNR",
    "RBNR","TPLNR","LICENSE_NUM","EXPIRY_DATE","FLEET_VIN","CHASSIS_NUM","BRGEW","GEWEI",
    "GROSS_WGT","LOAD_WGT","LOAD_VOL","VOL_UNIT","LOAD_HGT","LOAD_DIM_UNIT","LOAD_WID(15)",
    "LOAD_LEN(15)","NO_COMPART","FLEET_USE","ENGINE_TYPE","ENGINE_SNR","ENGINE_POWER",
    "UNIT_POWER","ENGINE_CAP","UNIT_CAP","SPEED_MAX","SPEED_UNIT","REVOLUTIONS","ENGINE_CYL",
    "FUEL_PRI","FUEL_SEC","OIL_TYPE","MWERT1","MWERT2","MWERT3","MWERT4","MWERT5","MWERT6",
    "MWERT8","MWERT9","MWERT10","MWERT11","MWERT12","MWERT14","MWERT15","MWERT16","MWERT17",
    "MWERT18","MWERT19","MWERT20","MWERT21","MWERT22","MWERT23","MWERT25","MWERT26",
    "MWERT27","MWERT28","MWERT29","MSGRP","NUM_AXLE","INDFIM"
]

def build_frota_df(
    df_in: pd.DataFrame,
    *,
    defaults: Optional[Dict[str, Any]] = None,
) -> pd.DataFrame:
    """
    Transforma o DataFrame do CRLV (CAMPOS_PADRAO + derivados) no layout da planilha
    'FROTA-Layout_excel_Geral' (cabeçalho na linha 4; dados a partir da linha 6).

    defaults: dicionário com valores fixos (ex.: {"EQTYP":"V","INGRP":"PM1","IWERK":"MP01","SWERK":"MP01","BUKRS":"1000","KOSTL":"CC1234"})
    """
    defaults = defaults or {}

    # Cria DF de saída já com o MESMO ÍNDICE do df_in (mantém alinhamento linha-a-linha)
    out = pd.DataFrame(index=df_in.index, columns=OUT_COLS, dtype=object)

    # --- Mapeamentos diretos do CRLV ---
     # --- Mapeamentos diretos do CRLV ---
    out["SHTXT"]        = _first_not_null(_col(df_in, "Modelo_Limpo"), _col(df_in, "Modelo"))
    out["LICENSE_NUM"]  = _col(df_in, "Placa")
    out["HERST"]        = _col(df_in, "Fabricante")                 # Marca
    out["TYPBZ"]        = _first_not_null(_col(df_in, "Modelo_Limpo"), _col(df_in, "Modelo"))
    out["BAUJJ"]        = _col(df_in, "Ano Fabricação")
    out["FLEET_VIN"]    = _col(df_in, "Renavam")                     # se VIN == Chassi
    out["CHASSIS_NUM"]  = _col(df_in, "Chassi")
    out["FUEL_PRI"]  = _col(df_in, "Combustivel_Principal")
    out["FUEL_SEC"]  = _col(df_in, "Combustivel_Secundario")

    out["MWERT1"]  = _col(df_in, "TIPO_ANO_VEICULO")   # ANO MODELO / ANO FABRICACAO
    out["MWERT5"]  = _col(df_in, "Local")

    out["MWERT6"]  = _col(df_in, "UF")
    out["MWERT11"]  = _col(df_in, "Número do CRV")
    out["MWERT15"]  = _col(df_in, "CNPJ")
    out["MWERT16"]  = _col(df_in, "Proprietário")

    out["ENGINE_SNR"]   = _col(df_in, "Motor")
    out["MWERT3"]        = _col(df_in, "Cor")
    out["MWERT12"]        = _col(df_in, "NumeroSegurancaCRV")
    out["RBNR"]        = _col(df_in, "PERFIL_CARTALOGO")
    out["OIL_TYPE"]        = _col(df_in, "TIPO_CARURANTE_OLEO")
    out["INVNR"]        = _col(df_in, "GERENCIA")




    out["EXPIRY_DATE"]  = _col(df_in, "Data Emissão").apply(to_date_dmy)

    # ------------------ 🔽 ADIÇÃO: extras da UI 🔽 ------------------

    # CENTRO -> SWERK / IWERK (planta / centro manutenção)
    out["SWERK"] = _col(df_in, "CENTRO")
    out["IWERK"] = _col(df_in, "CENTRO")
    out["WERGW"] = _col(df_in, "CENTRO")

    # Opcional: manter também em RBNR (se o seu layout/integração utilizar)
    out["RBNR"]  = _col(df_in, "CENTRO")

    # CENTRO_CUSTO -> KOSTL
    out["KOSTL"] = _col(df_in, "CENTRO_CUSTO")

   # DIVISAO -> KOSTL
    out["GSBER"] = _col(df_in, "DIVISAO")

    # EQUIPAMENTO -> EQUNR_SAP (nº técnico). Se preferir EQFNR, troque a coluna.

    # ------------------ Sequência para EQUIPAMENTO -> TPLNR ------------------
    equip_series = _col(df_in, "EQUIPAMENTO")

    # padroniza para string e identifica não vazios
    equip_vals = equip_series.astype(str).str.strip()
    mask_nonempty = equip_vals.str.len() > 0
    unique_nonempty = equip_vals[mask_nonempty].unique()

    if len(unique_nonempty) == 1:
        # Usuário digitou UM valor base (ex.: "EPD-0001265") para o lote inteiro
        base = unique_nonempty[0]
        seq = build_seq_codes(base, len(df_in))
        out["EQUNR_SAP"] = pd.Series(seq, index=df_in.index, dtype=object)
    else:
        # Já veio linha a linha (ou está vazio) -> usa como está
        out["EQUNR_SAP"] = equip_series

    out["EQART"]  = _col(df_in, "TIPO_VEICULO")

    # --- Peso bruto -> BRGEW/GEWEI ---
    pbt = _col(df_in, "Peso Bruto Total")
    brgew_vals, gewei_vals = [], []
    for v in pbt:
        val, unit = normalize_weight(v)
        brgew_vals.append(val); gewei_vals.append(unit)
   # out["BRGEW"] = pd.Series(brgew_vals, index=df_in.index)
    # Se quiser forçar unidade, passe defaults={"GEWEI":"KG"}; senão, usa a inferida
   # out["GEWEI"] = pd.Series(gewei_vals, index=df_in.index).where(pd.Series(gewei_vals).notna(), defaults.get("GEWEI"))

    # --- Potência/Cilindrada ---
    potcil = _col(df_in, "Potência/Cilindrada")
    p_list, pu_list, c_list, cu_list = [], [], [], []
    for s in potcil:
        p, pu, c, cu = parse_pot_cil(s)
        p_list.append(p); pu_list.append(pu); c_list.append(c); cu_list.append(cu)
   # out["ENGINE_POWER"] = pd.Series(p_list, index=df_in.index)
    #out["UNIT_POWER"]   = pd.Series(pu_list, index=df_in.index)
    #out["ENGINE_CAP"]   = pd.Series(c_list, index=df_in.index)
    #out["UNIT_CAP"]     = pd.Series(cu_list, index=df_in.index)

    # --- Combustível ---
    comb = _col(df_in, "Combustível")
    pri_list, sec_list = [], []
    for s in comb:
        a, b = normalize_fuel(s)
        pri_list.append(a); sec_list.append(b)
   # out["FUEL_PRI"] = pd.Series(pri_list, index=df_in.index).fillna(_col(df_in, "Combustivel_Principal"))
   # out["FUEL_SEC"] = pd.Series(sec_list, index=df_in.index).fillna(_col(df_in, "Combustivel_Secundario"))

    # --- Defaults SAP/fixos (injetáveis) ---
    for k, v in defaults.items():
        if k in OUT_COLS:
            out[k] = v


    # --- MWERTx / infos úteis não mapeadas diretamente ---
    #out["MWERT1"]  = _col(df_in, "Categoria")          # ANO MODELO/ ANO FABRICACAO
    #out["MWERT2"]  = _col(df_in, "Espécie / Tipo")
    #out["MWERT3"]  = _col(df_in, "Número do CRV")
    #out["MWERT4"]  = _col(df_in, "Renavam")
    #out["MWERT5"]  = _col(df_in, "Proprietário")

    # Exemplos adicionais (opcional):
    # out["MWERT6"]  = _first_not_null(_col(df_in, "Local"), _col(df_in, "UF"))
    # out["MWERT11"] = _col(df_in, "Combustível")

    # Garante a ordem final
    return out[OUT_COLS]
