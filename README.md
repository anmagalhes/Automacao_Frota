# Frota Data | Leitor Documento - CLRV

**Versão:** 1.0
**Autor:** Antonio Melo Magalhães
**Última atualização:** 01/11/2025

---

## 📘 Visão Geral
O **Frota Data | Leitor Documento - CLRV** é uma aplicação desktop desenvolvida em Python para leitura automatizada de documentos de veículos (CRLV) em formato PDF. A ferramenta extrai os dados relevantes, normaliza o texto, identifica campos específicos e exporta os resultados para um arquivo Excel com layout pré-definido, preservando estilos e formatação.

---

## ⚙️ Funcionalidades
- Leitura em lote de arquivos PDF.
- Extração de texto via OCR com API externa.
- Normalização e limpeza do texto.
- Identificação de campos padrão do CRLV.
- Exportação para Excel com layout corporativo.
- Geração de CSV com falhas (campos ausentes).
- Interface com barra de progresso e mensagens ao usuário.

---

## 🧩 Estrutura do Projeto

```
LEITOR_DOCUMENTOS/
├── excel_writer.py          # Módulo que escreve no Excel preservando layout
├── transform_frota.py       # Função build_frota_df para aplicar defaults SAP
├── utils_paths.py           # Funções auxiliares para caminhos e templates
├── config.py                # Flags e constantes como CAMPOS_PADRAO
└── README.md
```

---

## 📄 Documentação Técnica

### 1. `excel_writer.py`
- Função principal: `write_df_to_existing_template(...)`
- Objetivo: Escrever um `DataFrame` em uma aba específica de um Excel existente, mantendo estilos, bordas, alinhamentos e validações.
- Parâmetros:
  - `xlsx_path`: caminho do arquivo Excel.
  - `df`: DataFrame com os dados.
  - `sheet_name`: nome da aba.
  - `header_row`: linha onde estão os cabeçalhos.
  - `data_start_row`: linha onde começam os dados.
  - `column_map`: mapeamento opcional de colunas.
  - `strict`: se `True`, exige correspondência exata de colunas.

### 2. `transform_frota.py`
- Função: `build_frota_df(df, defaults)`
- Objetivo: Aplicar valores fixos SAP aos dados extraídos.
- Exemplo de defaults:
```python
{
  "EQTYP": "V",
  "INGRP": "PM1",
  "GEWRK": "FRT-MEC",
  "EXPIRY_DATE": "31.12.9999",
  "MWERT4": "AGUARDANDO ATIVACAO",
  "MWERT25": "AGUARDANDO ATIVACAO",
  "INDFIM": "X",
  "MWERT14": "01"
}
```

---

## 🧪 Execução

### Requisitos:
- Python 3.10+
- Bibliotecas: `pandas`, `openpyxl`, `os`, `time`, `tkinter`, `requests`

### Como rodar:
```bash
python main.py
```

---

## 📤 Exportações
- Excel: `crlv_consolidado_YYYYMMDD-HHMMSS.xlsx`
- CSV de falhas: `crlv_falhas_YYYYMMDD-HHMMSS.csv`
- Texto OCR (opcional): `nome_arquivo.timestamp.ocr.txt`

---

## 🛠️ Futuras Melhorias
- Validação automática de campos obrigatórios.
- Integração com banco de dados.
- Interface web com upload de arquivos.
- Geração de relatórios em PDF.

---

## 👨‍💻 Autor
**Antonio Melo Magalhães**
https://www.linkedin.com/in/antonio-melo-m/
Analista de Planejamento Logístico II | Especialista em BI Logístico | Python & Power BI
