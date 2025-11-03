# ui.py
# -*- coding: utf-8 -*-

import os
import time
import tkinter as tk
from tkinter import filedialog, messagebox, ttk
from tkinter.scrolledtext import ScrolledText


# Thread do pipeline (módulo separado)
from processor_thread import ProcessorThread


import unicodedata, re
from typing import Optional, Dict, Any

def _norm_token(s: str) -> str:
    """Remove acentos, espaços/hífens/underscores e deixa MAIÚSCULAS."""
    if not s:
        return ""
    s = unicodedata.normalize("NFD", str(s))
    s = "".join(ch for ch in s if unicodedata.category(ch) != "Mn")
    s = re.sub(r"[\s\-\_/]+", "", s).upper()
    return s

def map_perfil_por_equipamento(equip: str) -> Optional[str]:
    s = _norm_token(equip)
    if not s: return None
    pre = s[:3]
    mapa = {
        "CAM": "CAMINHAO",
        "MOT": "MOTO",
        "VLE": "CARRO",          # troque para "VEICULO LEVE" se preferir
        "SER": "SEMI-REBOQUE",
        "SRE": "SEMI-REBOQUE",
        "ONI": "ÔNIBUS",
    }
    return mapa.get(pre)

def map_combustivel_por_equipamento(equip: str) -> Optional[str]:
    s = _norm_token(equip)
    if not s: return None
    pre = s[:3]
    if pre in ("VLE", "MOT"):
        return "GASOLINA"
    if pre in ("CAM", "SER", "SRE", "ONI"):
        return "OLEO M.DIESE"
    return None

def map_tipo_oleo_sugerido(equip: str) -> Optional[str]:
    fam = map_combustivel_por_equipamento(equip)
    if fam == "GASOLINA":    return "SAE 5W30"
    if fam == "OLEO DIESEL": return "SAE 15W40"
    return None

from LEITOR_DOCUMENTO_CLRV import (
    APP_TITULO,
    OCR_SPACE_APIKEY_DEFAULT,
    ProcessorThread,
)

# ---------- Tema escuro ----------
def setup_dark_theme(root: tk.Tk):
    root.configure(bg="#0b1220")
    style = ttk.Style(root)
    try:
        style.theme_use("clam")
    except tk.TclError:
        pass

    palette = {
        "bg": "#0b1220",
        "panel": "#0f172a",
        "panel2": "#111827",
        "text": "#e5e7eb",
        "muted": "#9ca3af",
        "primary": "#10b981",        # VERDE corporativo (CTA)
        "primaryActive": "#065f46",  # verde escuro (hover/press)
        "accent": "#22d3ee",         # ciano (destaques)
        "border": "#1f2937",
    }

    # Frames / Cards
    style.configure("TFrame", background=palette["bg"])
    style.configure("Card.TFrame", background=palette["panel"], borderwidth=1, relief=tk.SOLID)

    # Textos
    style.configure("Header.TLabel", background=palette["bg"], foreground=palette["text"],
                    font=("Segoe UI", 14, "bold"))
    style.configure("TLabel", background=palette["panel"], foreground=palette["text"],
                    font=("Segoe UI", 10))
    style.configure("Muted.TLabel", background=palette["panel"], foreground=palette["muted"],
                    font=("Segoe UI", 9))

    # Entradas
    style.configure("TEntry",
                    fieldbackground=palette["panel2"],
                    foreground=palette["text"],
                    insertcolor=palette["text"])
    style.map("TEntry",
              fieldbackground=[("readonly", palette["panel2"]),
                               ("focus", "#0e1b2b")])

    # Botões CTA/Ghost
    base_font = ("Segoe UI", 10, "semibold")
    style.configure("TButton", font=base_font)

    style.configure(
        "Accent.TButton",
        foreground=palette["text"],
        background=palette["primary"],
        borderwidth=0,
        padding=(14, 8),
        font=("Segoe UI Semibold", 11),
    )
    style.map(
        "Accent.TButton",
        foreground=[("disabled", palette["muted"]), ("!disabled", palette["text"])],
        background=[
            ("disabled", "#1a2433"),
            ("pressed", palette["primaryActive"]),
            ("active", "#0f9e72"),
            ("!disabled", palette["primary"]),
        ],
    )

    style.configure(
        "Ghost.TButton",
        foreground=palette["text"],
        background=palette["panel2"],
        borderwidth=1,
        padding=(8, 5),
        font=("Segoe UI", 10),
    )
    style.map(
        "Ghost.TButton",
        foreground=[("disabled", palette["muted"]), ("!disabled", palette["text"])],
        background=[
            ("disabled", palette["panel2"]),
            ("pressed", "#0c141f"),
            ("active", "#0c141f"),
            ("!disabled", palette["panel2"]),
        ],
        bordercolor=[("active", palette["accent"]), ("!disabled", palette["border"])],
    )

    style.configure("TCheckbutton", background=palette["bg"], foreground=palette["text"])

    style.configure("TProgressbar",
                    background=palette["accent"],
                    troughcolor=palette["panel2"],
                    bordercolor=palette["border"])

    root.configure(cursor="arrow")
    return palette

class UI:
    def __init__(self):
        self.root = tk.Tk()
        self.root.title(APP_TITULO)

       # ---------- Janela no TOPO (centralizada no X) ----------
        target_w, target_h = 1080, 640
        min_w, min_h = 940, 560

        self.root.update_idletasks()
        screen_w = self.root.winfo_screenwidth()

        x = max(0, (screen_w - target_w) // 2)  # centro X
        y = 6                                   # no topo (margem 6px)
        self.root.geometry(f"{target_w}x{target_h}+{x}+{y}")
        self.root.minsize(min_w, min_h)

        self.palette = setup_dark_theme(self.root)

        # ---------- HEADER ----------
        header = ttk.Frame(self.root, style="TFrame")
        header.pack(fill=tk.X, padx=10, pady=(8, 4))
        ttk.Label(header, text=APP_TITULO, style="Header.TLabel").pack(side=tk.LEFT)

        ttk.Label(
                    header,
                    text="Automatize a extração do CRLV e gere planilhas para cadastro de veículos",
                    style="Muted.TLabel"
                ).pack(side=tk.LEFT, padx=(12, 0))

        header_sep = ttk.Separator(self.root, orient="horizontal")
        header_sep.pack(fill=tk.X, padx=10, pady=(0, 6))


        # ---------- AÇÕES PRINCIPAIS ----------
        top = ttk.Frame(self.root, style="Card.TFrame")
        top.pack(fill=tk.X, padx=10, pady=(0, 6), ipady=2)

        # Deixe o espaço crescer no eixo X
        top.columnconfigure(2, weight=1)

        # Botão de seleção de pasta
        self.btn_sel = ttk.Button(
            top,
            text="Selecionar Pasta de PDFs",
            command=self.selecionar_pasta,
            style="Accent.TButton",
            cursor="hand2",
        )
        self.btn_sel.grid(row=0, column=0, padx=(10, 8), pady=6, sticky="w")

        top_spacer = ttk.Frame(top, style="Card.TFrame")
        top_spacer.grid(row=0, column=1, sticky="ew")

        runbar = ttk.Frame(top, style="Card.TFrame")
        runbar.grid(row=0, column=2, padx=(0, 10), pady=6, sticky="e")

        try:
            self._ico_run = tk.PhotoImage(file="icons/play_16.png")
        except Exception:
            self._ico_run = None

        self.btn_run = ttk.Button(
            runbar,
            text="Processar PDFs",
            image=self._ico_run,
            compound="left",
            command=self.executar,
            state=tk.DISABLED,          # desabilitado -> fica cinza (map)
            style="Accent.TButton",
            cursor="hand2",
        )
        self.btn_run.pack(side=tk.LEFT, padx=(0, 8))

        self.btn_cancel = ttk.Button(
            runbar,
            text="Cancelar",
            command=self.cancelar,
            state=tk.DISABLED,
            style="Ghost.TButton",
            cursor="hand2",
        )
        self.btn_cancel.pack(side=tk.LEFT)

        # Micro-hint enquanto o Processar está desabilitado
        self.hint_run = ttk.Label(runbar, text="Selecione uma pasta", style="Muted.TLabel")
        self.hint_run.pack(side=tk.LEFT, padx=(8, 0))

        # Resumo da pasta (logo abaixo da linha)
        self.lbl_pasta_inline = ttk.Label(
            top, text="(nenhuma pasta selecionada)", style="Muted.TLabel"
        )
        self.lbl_pasta_inline.grid(row=1, column=0, columnspan=3, sticky="w", padx=10, pady=(0, 4))

        # Atalhos
        self.root.bind("<Return>", lambda e: self._trigger_if_enabled(self.btn_run))
        self.root.bind("<KP_Enter>", lambda e: self._trigger_if_enabled(self.btn_run))
        self.root.bind("<Escape>", lambda e: self._trigger_if_enabled(self.btn_cancel))
        self.root.bind("<F1>", lambda e: self.show_about())

        # ---------- LABEL DA PASTA (secundário; pode remover se preferir) ----------
        self.lbl_pasta = ttk.Label(self.root, text="Pasta: (não selecionada)")
        self.lbl_pasta.pack(padx=10, pady=(4, 2), anchor="w")

        # 🔐 API Key efetiva (sem mostrar no UI)
        self.api_key_effective = OCR_SPACE_APIKEY_DEFAULT or os.environ.get(
            "OCR_SPACE_APIKEY", "helloworld"
        )

        # Atalhos: Enter -> Processar; Esc -> Cancelar
        self.root.bind("<Return>", lambda e: self._trigger_if_enabled(self.btn_run))
        self.root.bind("<KP_Enter>", lambda e: self._trigger_if_enabled(self.btn_run))
        self.root.bind("<Escape>", lambda e: self._trigger_if_enabled(self.btn_cancel))


        # 🔐 API Key efetiva (sem mostrar no UI)
        self.api_key_effective = OCR_SPACE_APIKEY_DEFAULT or os.environ.get("OCR_SPACE_APIKEY", "helloworld")

        # ---------- CAMPOS EXTRAS (2 linhas x 3 campos) ----------
        extras = ttk.Frame(self.root, style="Card.TFrame")
        extras.pack(fill=tk.X, padx=10, pady=6, ipady=4)

        # Deixe as colunas das entradas (1,3,5) expansíveis
        for c in range(6):
            extras.columnconfigure(c, weight=1 if c in (1, 3, 5) else 0)

        padx_lbl, padx_in = 8, 8
        pady = 6

        # 1ª LINHA: GERENCIA | CENTRO | CENTRO_CUSTO
        ttk.Label(extras, text="GERENCIA").grid(row=0, column=0, padx=padx_lbl, pady=pady, sticky="e")
        self.in_gerencia = self._make_upper_entry(extras, width=24)
        self.in_gerencia.grid(row=0, column=1, padx=padx_in, pady=pady, sticky="we")

        ttk.Label(extras, text="CENTRO").grid(row=0, column=2, padx=padx_lbl, pady=pady, sticky="e")
        self.var_centro = tk.StringVar(value="")
        self.in_centro = self._make_upper_entry(extras, width=16, textvariable=self.var_centro)
        self.in_centro.grid(row=0, column=3, padx=padx_in, pady=pady, sticky="we")

        ttk.Label(extras, text="CENTRO_CUSTO").grid(row=0, column=4, padx=padx_lbl, pady=pady, sticky="e")
        self.in_ccusto = self._make_upper_entry(extras, width=16)
        self.in_ccusto.grid(row=0, column=5, padx=padx_in, pady=pady, sticky="we")

        # 2ª LINHA: EQUIPAMENTO | TIPO_VEICULO | CAIXA
        ttk.Label(extras, text="EQUIPAMENTO").grid(row=1, column=0, padx=padx_lbl, pady=pady, sticky="e")
        self.in_equip = self._make_upper_entry(extras, width=24)
        self.in_equip.grid(row=1, column=1, padx=padx_in, pady=pady, sticky="we")

        ttk.Label(extras, text="TIPO_VEICULO").grid(row=1, column=2, padx=padx_lbl, pady=pady, sticky="e")
        self.in_tipo = self._make_upper_entry(extras, width=24)
        self.in_tipo.grid(row=1, column=3, padx=padx_in, pady=pady, sticky="we")

        ttk.Label(extras, text="DIVISAO").grid(row=1, column=4, padx=padx_lbl, pady=pady, sticky="e")

        self.var_caixa = tk.StringVar(value="")  # inicia vazi
        self.in_caixa = self._make_upper_entry(extras, width=16, textvariable=self.var_caixa)
        self.in_caixa.grid(row=1, column=5, padx=padx_in, pady=pady, sticky="we")


        # ====== CÁLCULO AUTOMÁTICO DO CAIXA A PARTIR DO CENTRO ======
        def _update_caixa(*_args):
            centro = (self.var_centro.get() or "").upper()
            letras = "".join(ch for ch in centro if ch.isalpha())
            ult3 = letras[-3:] if letras else ""
            self.var_caixa.set(f"D{ult3}" if ult3 else "")

        # Recalcula sempre que CENTRO muda
        self.var_centro.trace_add("write", _update_caixa)

        # Inicializa uma vez (caso já exista valor no CENTRO; aqui começa vazio)
        _update_caixa()

        # ---------- PROGRESSO ----------
        prog_card = ttk.Frame(self.root, style="Card.TFrame")
        prog_card.pack(fill=tk.X, padx=10, pady=(6, 4))

        # Status textual acima da barra
        self.progress_label = ttk.Label(prog_card, text="Pronto", style="Muted.TLabel")
        self.progress_label.pack(padx=8, pady=(8, 0), anchor="w")

        self.progress = ttk.Progressbar(prog_card, mode="determinate")
        self.progress.pack(padx=8, pady=8, fill=tk.X)

        # ---------- TOGGLE DO LOG + LOG (inicialmente oculto) ----------
        toggle = ttk.Frame(self.root, style="TFrame")
        toggle.pack(fill=tk.X, padx=10, pady=(0, 4))
        self.var_show_log = tk.BooleanVar(value=False)
        self.chk_log = ttk.Checkbutton(
            toggle,
            text="Mostrar log detalhado ▸",
            variable=self.var_show_log,
            command=self.toggle_log
        )
        self.chk_log.configure(cursor="hand2")
        self.chk_log.pack(anchor="w")

        self.log_frame = ttk.Frame(self.root, style="Card.TFrame")
        self.log_text = ScrolledText(
            self.log_frame, height=2,  # menor para ganhar espaço
            bg="#0f172a", fg="#e5e7eb", insertbackground="#e5e7eb",
            relief=tk.FLAT, borderwidth=0
        )
        self.log_text.pack(fill=tk.BOTH, expand=True, padx=8, pady=8)

        # ---------- FOOTER (DESENVOLVEDOR) ----------
        footer = ttk.Frame(self.root, style="TFrame")
        footer.pack(fill=tk.X, side=tk.BOTTOM, padx=10, pady=(4, 8))

        dev_text = "Desenvolvido por Antonio Melo Magalhães • (85) 98133-4112"
        self.lbl_dev = tk.Label(
            footer,
            text=dev_text,
            bg=self.palette["bg"],
            fg=self.palette["accent"],            # destaque em ciano
            font=("Segoe UI Semibold", 10)
        )
        self.lbl_dev.pack(anchor="center")

        # Interação: copiar telefone ao clicar
        def _copy_phone(_=None):
            self.root.clipboard_clear()
            self.root.clipboard_append("(85) 98133-4112")
            self.root.update_idletasks()
            messagebox.showinfo("Contato copiado", "Telefone copiado para a área de transferência.")

        self.lbl_dev.bind("<Enter>", lambda e: self.lbl_dev.configure(cursor="hand2", fg="#44e6ff"))
        self.lbl_dev.bind("<Leave>", lambda e: self.lbl_dev.configure(cursor="arrow", fg=self.palette["accent"]))
        self.lbl_dev.bind("<Button-1>", _copy_phone)

        # Estado
        self.pasta = None
        self.worker = None
        self.saida_excel_path = None

        # Menu (Ajuda)
        menubar = tk.Menu(self.root)
        menu_ajuda = tk.Menu(menubar, tearoff=0)
        menu_ajuda.add_command(label="Sobre", command=self.show_about)
        menubar.add_cascade(label="Ajuda", menu=menu_ajuda)
        self.root.config(menu=menubar)




    # ------------- Helpers -------------

    # Entry que força UPPERCASE e ajusta cursores
    def _make_upper_entry(self, parent, width=16, textvariable=None):
        var = textvariable or tk.StringVar()
        entry = ttk.Entry(parent, width=width, textvariable=var)

        def _to_upper(*_):
            val = var.get()
            new = val.upper()
            if val != new:
                pos = entry.index(tk.INSERT)
                var.set(new)
                try:
                    entry.icursor(pos)
                except Exception:
                    pass

        var.trace_add("write", _to_upper)

        entry.bind("<Enter>", lambda e: entry.configure(cursor="xterm"))
        entry.bind("<Leave>", lambda e: entry.configure(cursor="arrow"))

        return entry

    def _trigger_if_enabled(self, btn: ttk.Button):
        if str(btn["state"]) != "disabled":
            btn.invoke()

    # ------------- Ações UI -------------

    def show_about(self):
        messagebox.showinfo("Sobre", f"{APP_TITULO}\nDesenvolvido por Antonio Melo Magalhães")

    def toggle_log(self):
        if self.var_show_log.get():
            self.chk_log.config(text="Mostrar log detalhado ▾")
            self.log_frame.pack(fill=tk.BOTH, expand=True, padx=10, pady=(0, 6))
        else:
            self.chk_log.config(text="Mostrar log detalhado ▸")
            self.log_frame.forget()

    def selecionar_pasta(self):
        pasta = filedialog.askdirectory()
        if not pasta:
            return
        self.pasta = pasta
        #self.lbl_pasta.config(text=f"Pasta: {pasta}")
        self.btn_run.config(state=tk.NORMAL)
        self.btn_cancel.config(state=tk.DISABLED)

    # Coleta dos extras (mantém existentes + inclui novos)
    def _coletar_campos_extras(self):
        return {
 # 1) Lê os campos já existentes (sem alterar)
            "CENTRO": (self.in_centro.get() or "").strip() or None,
            "CENTRO_CUSTO": (self.in_ccusto.get() or "").strip() or None,
            "EQUIPAMENTO": (self.in_equip.get() or "").strip() or None,
            "TIPO_VEICULO": (self.in_tipo.get() or "").strip() or None,
            # Novos
            "GERENCIA": (getattr(self, "in_gerencia", None).get() if getattr(self, "in_gerencia", None) else "").strip() or None,
            "DIVISAO": (getattr(self, "in_caixa", None).get() if getattr(self, "in_caixa", None) else "").strip() or None,
        }

    def executar(self):
        try:
            # 1) API key obrigatória
            apikey = (self.api_key_effective or "").strip()
            if not apikey:
                messagebox.showwarning("API Key", "API Key não configurada.")
                return

            # 2) Pasta obrigatória
            if not getattr(self, "pasta", None):
                messagebox.showwarning("Aviso", "Selecione a pasta de PDFs primeiro.")
                return

            # 3) Caminho de saída
            default_name = f"crlv_consolidado_{time.strftime('%Y%m%d-%H%M%S')}.xlsx"
            path = filedialog.asksaveasfilename(
                title="Escolher arquivo de saída",
                defaultextension=".xlsx",
                initialdir=self.pasta,
                initialfile=default_name,
                filetypes=[
                    ("Planilha Excel (*.xlsx)", "*.xlsx"),
                    ("CSV separado por ; (*.csv)", "*.csv"),
                    ("Todos os arquivos", "*.*"),
                ],
            )
            self.saida_excel_path = path or os.path.join(self.pasta, default_name)
            if not path:
                self.msg(f"ℹ Nenhum caminho escolhido. Será salvo automaticamente em: {self.saida_excel_path}")

            # 4) Coleta dos extras
            try:
                extras = self._coletar_campos_extras() or {}
            except Exception as e:
                self.msg(f"[WARN] Falha ao coletar campos extras: {e}")
                extras = {}

            # >>> PARAMETRIZAÇÃO AQUI (pós-coleta, SEM tocar na função validada)
            try:
                equip = extras.get("EQUIPAMENTO")
                if equip:
                    # PERFIL_CARTALOGO (só preenche se ausente/vazio)
                    if not extras.get("PERFIL_CARTALOGO") or str(extras["PERFIL_CARTALOGO"]).strip() == "":
                        c = map_perfil_por_equipamento(equip)
                        if c:
                            extras["PERFIL_CARTALOGO"] = c


        # *** COMBUSTÍVEL/ÓLEO: usa a CHAVE EXATA pedida ***
                if not extras.get("TIPO_CARURANTE_OLEO") or str(extras["TIPO_CARURANTE_OLEO"]).strip() == "":
                    c = map_combustivel_por_equipamento(equip)
                    if c:
                        extras["TIPO_CARURANTE_OLEO"] = c

                # Log de auditoria (opcional)
                self.msg(
                    f"[REGRAS] EQUIP={equip or 'None'} | PERFIL={extras.get('PERFIL_CARTALOGO')} "
                    f"| COMB={extras.get('TIPO_CARURANTE_OLEO')}"
                )
            except Exception as e:
                self.msg(f"[WARN] Falha ao parametrizar extras: {e}")

            # 5) Preparação da UI
            if self.var_show_log.get():
                try:
                    self.log_text.delete("1.0", tk.END)
                except Exception:
                    pass

            self.progress["value"] = 0
            self.progress_label.config(text="Iniciando…")
            self.btn_run.config(state=tk.DISABLED)
            self.btn_sel.config(state=tk.DISABLED)
            self.btn_cancel.config(state=tk.NORMAL)

            # 6) Inicia a thread (leva os extras prontos)
            self.worker = ProcessorThread(
                self.pasta,
                apikey,
                self,
                saida_excel_path=self.saida_excel_path,
                extra_campos=extras,
                on_msg=self.msg,
                on_progress=self.progresso,
                on_done=self.done,
                dispatcher=lambda delay, fn: self.root.after(delay, fn),
            )
            self.worker.start()

        except Exception as e:
            # Falha geral: restaura UI e mostra erro
            try:
                self.btn_run.config(state=tk.NORMAL)
                self.btn_sel.config(state=tk.NORMAL)
                self.btn_cancel.config(state=tk.DISABLED)
            except Exception:
                pass
            messagebox.showerror("Erro", f"Falha ao iniciar processamento:\n{e}")
            raise

    def cancelar(self):
        if self.worker:
            try:
                self.worker.cancelar()
            except Exception:
                pass
            self.msg("Solicitada a interrupção. Aguardando finalizar o arquivo atual...")
        self.progress_label.config(text="Cancelado")

    # ---------- Callbacks esperados pela ProcessorThread ----------

    def msg(self, texto: str):
        if self.var_show_log.get():
            self.log_text.insert(tk.END, texto + "\n")
            self.log_text.see(tk.END)
            self.root.update_idletasks()

    def set_progress(self, percent: int | float, current: int | None = None, total: int | None = None):
        """Atualiza barra e texto 'XX% • Arquivo i/n'."""
        try:
            p = max(0, min(100, int(percent)))
        except Exception:
            p = 0
        self.progress.configure(value=p)
        status = f"{p}%"
        if current is not None and total is not None and total > 0:
            status += f" • Arquivo {current}/{total}"
        self.progress_label.config(text=status)
        self.root.update_idletasks()

    def progresso(self, val, current=None, total=None):
        """
        Compatível com assinatura antiga (apenas 'val') e nova (val, current, total).
        A thread pode chamar: ui.progresso(35) ou ui.progresso(35, 3, 10)
        """
        try:
            self.set_progress(val, current, total)
        except Exception:
            # Fallback mínimo: só atualiza a barra
            try:
                self.progress["value"] = float(val)
            except Exception:
                pass
        self.root.update_idletasks()

    def done(self):
        self.btn_run.config(state=tk.NORMAL)
        self.btn_sel.config(state=tk.NORMAL)
        self.btn_cancel.config(state=tk.DISABLED)
        self.progress_label.config(text="Concluído")
        messagebox.showinfo("Concluído", "Processamento finalizado.")

    # ---------- Mainloop ----------
    def run(self):
        self.root.mainloop()


if __name__ == "__main__":
    UI().run()
