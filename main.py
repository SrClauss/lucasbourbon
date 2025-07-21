import tkinter as tk
from tkinter import ttk, filedialog, messagebox, scrolledtext
import threading
import openpyxl
import hashlib
import os
import json
import sys
import queue
import contextlib
import time
import psutil
from datetime import datetime
from login import login
from extractor import search_product
from selenium.common.exceptions import WebDriverException, TimeoutException

# Mantendo os cabeçalhos originais
HEADERS = [
    "code", "name", "pricing", "discount", "pricing_with", "cofins_tax", 
    "cofins_value", "difalst_tax", "difalst_value", "fecop_tax", "fecop_value",
    "icmi_value", "icms_tax", "icms_value", "ipi_tax", "ipi_value", "pis_tax",
    "pis_value", "st_tax", "st_value", "weight", "status", "country_of_origin",
    "customs_tariff", "possibility_to_return", "row_num"
]

header_labels = {
    "code": "Código",
    "name": "Nome",
    "pricing": "Preço",
    "discount": "Desconto",
    "pricing_with": "Preço com Impostos",
    "cofins_tax": "Cofins",
    "cofins_value": "Cofins Valor",
    "difalst_tax": "Difal ST",
    "difalst_value": "Difal ST Valor",
    "fecop_tax": "Fecop",
    "fecop_value": "Fecop Valor",
    "icmi_value": "ICMI Valor",
    "icms_tax": "ICMS",
    "icms_value": "ICMS Valor",
    "ipi_tax": "IPI",
    "ipi_value": "IPI Valor",
    "pis_tax": "PIS",
    "pis_value": "PIS Valor",
    "st_tax": "ST",
    "st_value": "ST Valor",
    "weight": "Peso",
    "status": "Status",
    "country_of_origin": "País de Origem",
    "customs_tariff": "Tarifa Aduaneira",
    "possibility_to_return": "Possibilidade de Devolução"
}

class Application(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title("Atlas Copco Scraper - Completo")
        self.geometry("1200x800")

        # Determina o caminho base correto para o config.json.
        # Isso garante que o arquivo seja encontrado tanto no modo de desenvolvimento (.py)
        # quanto quando empacotado como um executável (.exe).
        if getattr(sys, 'frozen', False):
            # Rodando como um executável PyInstaller
            self.base_path = os.path.dirname(sys.executable)
        else:
            # Rodando como um script Python normal
            self.base_path = os.path.dirname(os.path.abspath(__file__))
        self.config_path = os.path.join(self.base_path, 'config.json')
        
        # CORREÇÃO: Inicializa as filas de log ANTES de chamar check_expiration
        self.login_log_queue = queue.Queue()
        self.scraper_log_queue = queue.Queue()

        self.config = self.load_config()
        if not self.config:
            self.destroy()
            sys.exit()

        self.stop_event = threading.Event()
        self.tasks_queue = queue.Queue()
        self.results_queue = queue.Queue()
        self.unsaved_data = []
        self.worker_threads = []
        self.threads_lock = threading.Lock() # Lock para acesso seguro às threads
        self.saved_rows = set() # Novo: Conjunto para rastrear todas as linhas salvas
        self.reprocess_rows = set() # NOVO: Conjunto para rastrear linhas a serem reprocessadas (buracos)
        self.total_items = 0
        self.saved_items_count = 0
        default_workers = self.config.get("scraping_settings", {}).get("num_workers", 3)
        self.num_workers_var = tk.IntVar(value=default_workers)
        default_headless = self.config.get("system", {}).get("chrome_options", {}).get("headless", False)
        self.headless_var = tk.BooleanVar(value=default_headless)

        self.create_widgets()
        self.process_login_log_queue()
        self.process_scraper_log_queue()

    def load_config(self):
        try:
            with open(self.config_path, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception as e:
            messagebox.showerror("Erro", f"Erro ao carregar o arquivo de configuração:\n{self.config_path}\n\n{e}")
            return None

    def create_widgets(self):
        # Frame de arquivos
        file_frame = ttk.LabelFrame(self, text="Controle de Arquivos", padding=10)
        file_frame.pack(fill=tk.X, padx=10, pady=5)
        
        ttk.Button(file_frame, text="Selecionar Arquivo de Entrada", 
                  command=self.select_input_file).pack(side=tk.LEFT)
        self.input_label = ttk.Label(file_frame, text="Nenhum arquivo selecionado")
        self.input_label.pack(side=tk.LEFT, padx=5)
        
        ttk.Button(file_frame, text="Selecionar Saída", 
                  command=self.select_output_file).pack(side=tk.LEFT)
        self.output_label = ttk.Label(file_frame, text="Mesma pasta do arquivo de entrada")
        self.output_label.pack(side=tk.LEFT, padx=5)
        
        # Área de logs
        main_log_frame = ttk.Frame(self)
        main_log_frame.pack(fill=tk.BOTH, expand=True, padx=10, pady=5)
        main_log_frame.columnconfigure(0, weight=1)
        main_log_frame.columnconfigure(1, weight=1)
        main_log_frame.rowconfigure(0, weight=1)

        # Log da Esquerda (Login e Sistema)
        left_log_frame = ttk.LabelFrame(main_log_frame, text="Logs de Login e Sistema", padding=5)
        left_log_frame.grid(row=0, column=0, sticky="nsew", padx=(0, 5))
        self.login_log_area = scrolledtext.ScrolledText(
            left_log_frame,
            state='disabled',
            font=('Consolas', 10),
        )
        self.login_log_area.pack(fill=tk.BOTH, expand=True)

        # Log da Direita (Raspagem)
        right_log_frame = ttk.LabelFrame(main_log_frame, text="Logs de Raspagem", padding=5)
        right_log_frame.grid(row=0, column=1, sticky="nsew", padx=(5, 0))
        self.scraper_log_area = scrolledtext.ScrolledText(
            right_log_frame, state='disabled', font=('Consolas', 10)
        )
        self.scraper_log_area.pack(fill=tk.BOTH, expand=True)
        
        # Controles de progresso
        progress_frame = ttk.Frame(self)
        progress_frame.pack(fill=tk.X, padx=10, pady=5)
        
        self.progress_var = tk.DoubleVar()
        self.progress = ttk.Progressbar(
            progress_frame,
            variable=self.progress_var,
            maximum=100,
            orient=tk.HORIZONTAL,
            mode='determinate'
        )
        self.progress.pack(side=tk.LEFT, fill=tk.X, expand=True)
        
        self.progress_label = ttk.Label(progress_frame, text="0/0")
        self.progress_label.pack(side=tk.LEFT, padx=5)
        
        # Controles de execução
        ctrl_frame = ttk.Frame(self)
        ctrl_frame.pack(fill=tk.X, padx=10, pady=5)
        
        self.start_btn = ttk.Button(
            ctrl_frame,
            text="INICIAR PROCESSAMENTO",
            command=self.start_process,
            style='Accent.TButton'
        )
        self.start_btn.pack(side=tk.LEFT)
        
        self.stop_btn = ttk.Button(
            ctrl_frame,
            text="PARAR E SALVAR",
            command=self.stop_process,
            state='disabled'
        )
        self.stop_btn.pack(side=tk.LEFT, padx=5)
        
        # Seletor de Workers
        ttk.Label(ctrl_frame, text="Workers:").pack(side=tk.LEFT, padx=(20, 2))
        self.workers_spinbox = ttk.Spinbox(
            ctrl_frame,
            from_=1,
            to=20,
            textvariable=self.num_workers_var,
            width=5
        )
        self.workers_spinbox.pack(side=tk.LEFT)
        
        self.headless_check = ttk.Checkbutton(
            ctrl_frame,
            text="Rodar em 2º plano (headless)",
            variable=self.headless_var,
            onvalue=True,
            offvalue=False
        )
        self.headless_check.pack(side=tk.LEFT, padx=(10, 0))
        
        self.status_var = tk.StringVar(value="Pronto para iniciar")
        ttk.Label(
            ctrl_frame,
            textvariable=self.status_var,
            font=('Arial', 10)
        ).pack(side=tk.LEFT, padx=10)
        
        # Empacotando da direita para a esquerda para manter a ordem
        self.eta_var = tk.StringVar(value="ETA: --:--:--")
        ttk.Label(ctrl_frame, textvariable=self.eta_var, font=('Arial', 10, 'italic')).pack(side=tk.RIGHT, padx=10)
        
        self.speed_var = tk.StringVar(value="-- itens/min")
        ttk.Label(ctrl_frame, textvariable=self.speed_var, font=('Arial', 10, 'italic')).pack(side=tk.RIGHT, padx=5)

    def process_login_log_queue(self):
        """Processa mensagens da fila de logs de login/sistema e atualiza a GUI."""
        try:
            while True:
                message = self.login_log_queue.get_nowait()
                self._update_log_area(self.login_log_area, message)
        except queue.Empty:
            pass
        finally:
            self.after(100, self.process_login_log_queue)

    def process_scraper_log_queue(self):
        """Processa mensagens da fila de logs de raspagem e atualiza a GUI."""
        try:
            while True:
                message = self.scraper_log_queue.get_nowait()
                self._update_log_area(self.scraper_log_area, message)
        except queue.Empty:
            pass
        finally:
            self.after(100, self.process_scraper_log_queue)

    def _update_log_area(self, area, message):
        """Helper para atualizar uma área de texto de log."""
        area.config(state='normal')
        area.insert(tk.END, message + "\n")
        area.see(tk.END)
        area.config(state='disabled')
        
        # Mantém apenas as últimas 200 linhas
        lines = int(area.index('end-1c').split('.')[0])
        if lines > 200:
            area.delete(1.0, 2.0)

    def log(self, message):
        """Envia uma mensagem para a área de log de Login/Sistema."""
        self.login_log_queue.put(message)

    def calculate_file_hash(self, filepath):
        """Calcula hash SHA-256 do arquivo para controle de versão"""
        hash_sha256 = hashlib.sha256()
        with open(filepath, "rb") as f:
            for chunk in iter(lambda: f.read(4096), b""):
                hash_sha256.update(chunk)
        return hash_sha256.hexdigest()

    def select_input_file(self):
        file_path = filedialog.askopenfilename(
            title="Selecione o arquivo Excel de entrada",
            filetypes=[("Arquivos Excel", "*.xlsx *.xls")]
        )
        if file_path:
            self.input_file = file_path
            self.input_label.config(text=os.path.basename(file_path))
            self.input_hash = self.calculate_file_hash(file_path)
            
            # Carrega planilhas para seleção
            try:
                wb = openpyxl.load_workbook(file_path, read_only=True)
                sheets = wb.sheetnames
                wb.close()
                
                # Diálogo para selecionar planilha
                selected = self.ask_sheet_selection(sheets)
                if selected:
                    self.selected_sheet = selected
                    self.login_log_queue.put(f"Planilha selecionada: {selected}")
            except Exception as e:
                messagebox.showerror("Erro", f"Não foi possível ler o arquivo:\n{str(e)}")

    def ask_sheet_selection(self, sheets):
        """Diálogo para seleção de planilha com radiobox"""
        popup = tk.Toplevel(self)
        popup.title("Selecionar Planilha")
        popup.geometry("300x300")
        
        tk.Label(popup, text="Selecione a planilha para processar:").pack(pady=10)
        
        selected = tk.StringVar(value=sheets[0])
        for sheet in sheets:
            rb = tk.Radiobutton(
                popup,
                text=sheet,
                variable=selected,
                value=sheet,
                padx=20,
                pady=5
            )
            rb.pack(anchor='w')
        
        result = []
        def on_ok():
            result.append(selected.get())
            popup.destroy()
        
        tk.Button(popup, text="OK", command=on_ok).pack(pady=10)
        
        popup.grab_set()
        self.wait_window(popup)
        return result[0] if result else None
    
    def select_output_file(self):
        default_name = ""
        if hasattr(self, 'input_file'):
            default_name = os.path.splitext(os.path.basename(self.input_file))[0] + "_PROCESSADO.xlsx"
        
        file_path = filedialog.asksaveasfilename(
            title="Salvar resultado como",
            defaultextension=".xlsx",
            filetypes=[("Arquivos Excel", "*.xlsx")],
            initialfile=default_name
        )
        if file_path:
            self.output_file = file_path
            self.output_label.config(text=os.path.basename(file_path))

    def _find_and_queue_buracos(self, log_prefix=""):
        """
        Scans the output file for empty 'Status' cells (buracos) and adds them to the tasks_queue.
        Removes these buracos from saved_rows.
        Returns the number of buracos found and queued.
        """
        buracos_found_in_scan = set()
        try:
            if not os.path.exists(self.output_file):
                self.login_log_queue.put(f"{log_prefix}Arquivo de saída não existe para verificar buracos.")
                return 0

            wb = openpyxl.load_workbook(self.output_file)
            if self.selected_sheet not in wb.sheetnames:
                self.login_log_queue.put(f"{log_prefix}Planilha de dados '{self.selected_sheet}' não encontrada no arquivo de saída para verificar buracos.")
                return 0

            data_sheet = wb[self.selected_sheet]
            status_col_idx = HEADERS.index("status") + 1 # Coluna 'Status' (índice baseado em 1)

            self.login_log_queue.put(f"{log_prefix}Iniciando varredura por buracos na planilha de saída...")

            # Iterate over all rows in the output sheet to find empty status cells
            for row_num in range(2, data_sheet.max_row + 1): # min_row=2 to ignore header
                status_cell_value = data_sheet.cell(row=row_num, column=status_col_idx).value

                if not status_cell_value: # If status is empty (None or empty string)
                    buracos_found_in_scan.add(row_num)
                    # If this buraco was mistakenly in saved_rows, remove it
                    if row_num in self.saved_rows:
                        self.saved_rows.remove(row_num)
        except Exception as e:
            self.login_log_queue.put(f"{log_prefix}ERRO ao escanear buracos na planilha de saída: {str(e)}")
            return 0

        if buracos_found_in_scan:
            self.login_log_queue.put(f"{log_prefix}Detectados {len(buracos_found_in_scan)} novo(s) buraco(s) na planilha de saída.")
            self.login_log_queue.put(f"{log_prefix}Linhas dos buracos: {sorted(list(buracos_found_in_scan))}")
            
            # Add these buracos to the tasks queue
            wb_input = openpyxl.load_workbook(self.input_file, read_only=True)
            sheet_input = wb_input[self.selected_sheet]

            for row_num in sorted(list(buracos_found_in_scan)): # Process buracos in order
                code = sheet_input.cell(row=row_num, column=1).value # Assuming code is in column 1
                if code:
                    self.tasks_queue.put((str(code).zfill(10), row_num))
                    self.login_log_queue.put(f"{log_prefix}Adicionado buraco à fila: Código {str(code).zfill(10)} (linha {row_num})")
            
            wb_input.close()
        else:
            self.login_log_queue.put(f"{log_prefix}Nenhum buraco encontrado na planilha de saída nesta varredura.")
        
        return len(buracos_found_in_scan)


    def check_output_continuity(self):
        """
        Verifica se podemos continuar de onde paramos,
        incluindo a validação de linhas parcialmente processadas (apenas vazias).
        """
        self.reprocess_rows.clear() # Limpa o set de reprocessamento antes de cada verificação
        self.saved_rows.clear() # Garante que saved_rows seja populado do zero pelo arquivo ou permaneça vazio

        if not os.path.exists(self.output_file):
            self.login_log_queue.put("Arquivo de saída não encontrado. Iniciando um novo processamento.")
            return True  # Arquivo novo, pode começar do zero

        try:
            wb = openpyxl.load_workbook(self.output_file)
            data_sheet_name = self.selected_sheet # Nome da planilha de dados
            
            if "Metadata" in wb.sheetnames:
                meta_sheet = wb["Metadata"]
                metadata = {
                    "input_hash": meta_sheet.cell(row=1, column=2).value,
                    "saved_rows_str": meta_sheet.cell(row=4, column=2).value
                }
                
                if metadata["input_hash"] == self.input_hash:
                    if metadata["saved_rows_str"]:
                        self.saved_rows = {int(r) for r in str(metadata["saved_rows_str"]).split(',')}

                    # --- NOVA LÓGICA PARA VALIDAR LINHAS SALVAS ---
                    # Esta parte agora apenas identifica os buracos para o diálogo inicial.
                    # A adição à fila será feita em run_scraping e _find_and_queue_buracos
                    if data_sheet_name in wb.sheetnames:
                        data_sheet = wb[data_sheet_name]
                        status_col_idx = HEADERS.index("status") + 1 # Coluna 'Status' (índice baseado em 1)
                        
                        self.login_log_queue.put("Verificando integridade das linhas processadas anteriormente para diálogo inicial...")
                        
                        for row_num in range(2, data_sheet.max_row + 1):
                            status_cell_value = data_sheet.cell(row=row_num, column=status_col_idx).value
                            
                            if not status_cell_value:
                                self.reprocess_rows.add(row_num)
                                if row_num in self.saved_rows:
                                    self.saved_rows.remove(row_num)
                    
                    if self.reprocess_rows:
                        self.login_log_queue.put(f"Detectadas {len(self.reprocess_rows)} linha(s) com status vazio (buracos) para o diálogo inicial.")
                    else:
                        self.login_log_queue.put("Nenhum buraco (linha com status vazio) detectado para o diálogo inicial.")
                    # --- FIM DA NOVA LÓGICA ---

                    response = messagebox.askyesnocancel(
                        "Continuar Processamento?",
                        f"Foi encontrado um processamento anterior para este arquivo com {len(self.saved_rows)} itens já salvos (após validação).\n"
                        f"{len(self.reprocess_rows)} linha(s) será(ão) reprocessada(s) por estar(em) com status vazio (buracos).\n\n"
                        "Deseja continuar de onde parou?\n\n"
                        "Sim = Continuar (reprocessará buracos primeiro)\n"
                        "Não = Reiniciar do zero (sobrescreverá o arquivo de saída)\n"
                        "Cancelar = Abortar",
                        icon='question'
                    )

                    if response is None: # Cancelar
                        return False
                    elif response: # Sim (Continuar)
                        self.login_log_queue.put(f"Continuando processamento. {len(self.saved_rows)} linhas salvas serão ignoradas e {len(self.reprocess_rows)} buracos serão reprocessados primeiro.")
                        return True
                    else: # Não (Reiniciar)
                        self.login_log_queue.put("Reiniciando o processamento do zero. O arquivo de saída será sobrescrito.")
                        self.saved_rows.clear()
                        self.reprocess_rows.clear() # Limpa também os buracos se for reiniciar
                        if os.path.exists(self.output_file):
                            os.remove(self.output_file)
                        return True
                else:
                    resp = messagebox.askyesno(
                        "Arquivo de saída existente",
                        "O arquivo de saída foi gerado a partir de um arquivo de entrada diferente.\n\n"
                        "Deseja SOBRESCREVER completamente o arquivo de saída?\n"
                        "(Selecionar 'Não' cancelará a operação)",
                        icon='warning'
                    )
                    if resp:
                        self.login_log_queue.put("Sobrescrevendo arquivo de saída existente (input hash diferente).")
                        self.saved_rows.clear()
                        self.reprocess_rows.clear()
                        if os.path.exists(self.output_file):
                            os.remove(self.output_file)
                    return resp
            # Verifica se é o formato antigo para pedir para sobrescrever
            elif wb.active.cell(row=1, column=1).value == "##METADATA##":
                resp = messagebox.askyesno(
                    "Formato Antigo Detectado",
                    "O arquivo de saída está em um formato antigo. Deseja sobrescrevê-lo para usar o novo formato com metadados separados?",
                    icon='warning'
                )
                if resp:
                    self.login_log_queue.put("Sobrescrevendo arquivo de saída (formato antigo detectado).")
                    self.saved_rows.clear()
                    self.reprocess_rows.clear()
                    if os.path.exists(self.output_file):
                        os.remove(self.output_file)
                return resp
            
            # Se o arquivo existe mas não tem Metadata ou formato antigo, assume que é para sobrescrever
            else:
                resp = messagebox.askyesno(
                    "Arquivo de saída existente",
                    "O arquivo de saída existe, mas não contém metadados de processamento anterior.\n\n"
                    "Deseja SOBRESCREVER completamente o arquivo de saída?\n"
                    "(Selecionar 'Não' cancelará a operação)",
                    icon='warning'
                )
                if resp:
                    self.login_log_queue.put("Sobrescrevendo arquivo de saída (sem metadados).")
                    self.saved_rows.clear()
                    self.reprocess_rows.clear()
                    if os.path.exists(self.output_file):
                        os.remove(self.output_file)
                return resp
            
        except Exception as e:
            self.login_log_queue.put(f"Erro inesperado ao verificar continuidade do arquivo de saída: {str(e)}. Assumindo novo processamento.")
            # Em caso de erro ao ler o arquivo de saída, assume que é um novo processamento
            self.saved_rows.clear()
            self.reprocess_rows.clear() # Limpa também os buracos em caso de erro
            if os.path.exists(self.output_file):
                try:
                    os.remove(self.output_file)
                    self.login_log_queue.put("Arquivo de saída existente removido devido a erro de leitura.")
                except Exception as rm_e:
                    self.login_log_queue.put(f"Erro ao tentar remover arquivo de saída: {rm_e}")
            return True

    def start_process(self):
        if not hasattr(self, 'input_file'):
            messagebox.showwarning("Aviso", "Selecione um arquivo de entrada primeiro!")
            return
        
        self.reprocess_rows = set() # Reseta as linhas a reprocessar
        self.saved_rows = set() # Garante que saved_rows seja populado do zero pelo arquivo ou permaneça vazio

        if not self.check_output_continuity():
            return
        
        self.saved_items_count = len(self.saved_rows)
        
        self.start_btn.config(state='disabled')
        self.stop_btn.config(state='normal')
        self.workers_spinbox.config(state='disabled') # Desabilita workers spinbox durante o processo
        self.headless_check.config(state='disabled')
        self.stop_event.clear()
        
        # Inicia thread de processamento
        threading.Thread(target=self.run_scraping, daemon=True).start()

    def _worker_manager(self, headless_mode):
        """
        Gerencia o pool de workers: inicia, monitora e substitui workers que falharam.
        Também adiciona novos workers se o valor no spinbox for aumentado.
        """
        worker_serial_id = 0
        while not self.stop_event.is_set():
            with self.threads_lock:
                # 1. Remove threads que já terminaram (falharam ou concluíram)
                self.worker_threads = [t for t in self.worker_threads if t.is_alive()]

                # 2. Verifica se precisa de mais workers
                target_workers = self.num_workers_var.get()
                current_workers = len(self.worker_threads)
                
                if current_workers < target_workers:
                    needed = target_workers - current_workers
                    self.login_log_queue.put(f"MANAGER: Solicitando {needed} novo(s) worker(s) para atingir o total de {target_workers}.")
                    for _ in range(needed):
                        if self.stop_event.is_set(): break
                        worker_serial_id += 1
                        thread = threading.Thread(target=self._scraper_worker, args=(worker_serial_id, headless_mode), daemon=True)
                        thread.start()
                        self.worker_threads.append(thread)
                        time.sleep(15) # Intervalo crucial para não sobrecarregar o login
            
            time.sleep(5) # Verifica a cada 5 segundos por mudanças ou falhas

    def _scraper_worker(self, worker_id, headless_mode):
        """
        Função do worker. Cada worker é uma thread que gerencia seu próprio ciclo de vida.
        """
        self.login_log_queue.put(f"[Worker {worker_id}] Iniciando...")
        driver = None

        while not self.stop_event.is_set():
            try:
                # 1. Fazer login (ou refazer login se o driver falhou)
                if not driver:
                    self.login_log_queue.put(f"[Worker {worker_id}] Tentando fazer login...")
                    driver = login(headless=headless_mode, log_queue=self.login_log_queue)
                    if not driver:
                        self.login_log_queue.put(f"[Worker {worker_id}] ❌ Falha no login. Tentando novamente em 30s.")
                        time.sleep(30)
                        continue
                    self.login_log_queue.put(f"[Worker {worker_id}] ✅ Login bem-sucedido.")

                # 2. Processar tarefas da fila
                while not self.stop_event.is_set():
                    try:
                        # Pega uma tarefa da fila, com timeout para poder verificar o stop_event
                        code, row_num = self.tasks_queue.get(timeout=1)
                        
                        # Processa o produto
                        data = search_product(driver, code, worker_id=worker_id, row_num=row_num, log_queue=self.scraper_log_queue)
                        
                        # Coloca o resultado na fila de resultados
                        self.results_queue.put(data)
                        
                        self.tasks_queue.task_done()

                    except queue.Empty:
                        # Fila vazia, continua esperando por mais tarefas
                        continue
                    except (WebDriverException, TimeoutException) as e:
                        self.scraper_log_queue.put(f"🚨 [Worker {worker_id}] Erro no navegador: {type(e).__name__}. Reiniciando o worker.")
                        # Devolve a tarefa para a fila para ser reprocessada
                        self.tasks_queue.put((code, row_num))
                        raise # Força o reinício do worker

            except Exception as e:
                self.login_log_queue.put(f"🚨 [Worker {worker_id}] Erro crítico, worker será reiniciado: {e}")
                # O contextlib.suppress evita um novo try/except aninhado
                with contextlib.suppress(Exception):
                    if driver:
                        driver.quit()
                driver = None
                time.sleep(15) # Espera antes de tentar um novo login

        # Limpeza final do worker
        if driver:
            with contextlib.suppress(Exception):
                driver.quit()
        self.login_log_queue.put(f"[Worker {worker_id}] Finalizado.")

    def run_scraping(self):
        try:
            self.login_log_queue.put("\n=== INICIANDO PROCESSAMENTO ===")
            self.tasks_queue = queue.Queue() # Limpa a fila de tarefas
            self.results_queue = queue.Queue() # Limpa a fila de resultados

            # 1. Carrega códigos da planilha de entrada e preenche a fila de tarefas
            # (linhas novas + buracos iniciais do check_output_continuity)
            wb_input = openpyxl.load_workbook(self.input_file, read_only=True)
            sheet_input = wb_input[self.selected_sheet]
            
            tasks_to_queue_initially = []

            # Adiciona os buracos encontrados na checagem inicial (check_output_continuity)
            if self.reprocess_rows:
                self.login_log_queue.put(f"Priorizando {len(self.reprocess_rows)} linha(s) com status vazio para reprocessamento inicial...")
                for row_idx in sorted(list(self.reprocess_rows)):
                    code = sheet_input.cell(row=row_idx, column=1).value
                    if code:
                        tasks_to_queue_initially.append((str(code).zfill(10), row_idx))
                        self.login_log_queue.put(f"Adicionado à fila de prioridade: Código {str(code).zfill(10)} (linha {row_idx})")
            
            # Adiciona as linhas restantes que não foram processadas (e não são buracos já adicionados)
            for row_idx, row in enumerate(sheet_input.iter_rows(min_row=2, max_col=1, values_only=True), start=2):
                if row[0] and row_idx not in self.saved_rows and row_idx not in self.reprocess_rows:
                    tasks_to_queue_initially.append((str(row[0]).zfill(10), row_idx))
            
            if tasks_to_queue_initially:
                self.login_log_queue.put(f"Total de {len(tasks_to_queue_initially)} tarefas iniciais na fila (buracos + novas).")
                for task in tasks_to_queue_initially:
                    self.tasks_queue.put(task)
            else:
                self.login_log_queue.put("Nenhum item novo ou buraco inicial encontrado para processar.")

            self.total_items = sheet_input.max_row - 1 # Total de itens na planilha de entrada
            wb_input.close() # Fecha a planilha de entrada

            self.progress_var.set(self.saved_items_count)
            self.progress["maximum"] = self.total_items
            self.progress_label.config(text=f"{self.saved_items_count}/{self.total_items}")
            self.status_var.set(f"Processando {self.saved_items_count}/{self.total_items}")
            
            # 2. Inicia o gerenciador de workers em uma thread separada
            headless_mode = self.headless_var.get()
            manager_thread = threading.Thread(target=self._worker_manager, args=(headless_mode,), daemon=True)
            manager_thread.start()

            # 3. Coleta resultados e atualiza a UI em um loop contínuo
            start_time = time.time()
            last_speed_update = start_time
            items_processed_in_session = 0 # Contagem de itens processados nesta sessão

            # Loop principal de processamento
            while not self.stop_event.is_set():
                # Tenta pegar um resultado da fila
                try:
                    data = self.results_queue.get(timeout=1) # Timeout para permitir verificação do stop_event
                    if data:
                        self.unsaved_data.append(data)
                        items_processed_in_session += 1
                        
                        # Atualiza status, velocidade e ETA
                        current_time = time.time()
                        elapsed_seconds = current_time - start_time
                        if elapsed_seconds > 2 and current_time - last_speed_update > 1.5:
                            speed_per_second = items_processed_in_session / elapsed_seconds
                            self.speed_var.set(f"{(speed_per_second * 60):.1f} itens/min")
                            
                            # ETA: Estimativa baseada no total de itens da planilha de entrada
                            # e no total de itens já salvos + processados nesta sessão.
                            items_remaining_total = self.total_items - (self.saved_items_count + items_processed_in_session)
                            if speed_per_second > 0 and items_remaining_total > 0:
                                eta_seconds = items_remaining_total / speed_per_second
                                m, s = divmod(eta_seconds, 60)
                                h, m = divmod(m, 60)
                                self.eta_var.set(f"ETA: {int(h):02d}:{int(m):02d}:{int(s):02d}")
                            else:
                                self.eta_var.set("ETA: --:--:--")
                            
                            last_speed_update = current_time

                        with self.threads_lock:
                            active_workers = len([t for t in self.worker_threads if t.is_alive()])
                        target_workers = self.num_workers_var.get()
                        self.status_var.set(f"Processando {self.saved_items_count + items_processed_in_session}/{self.total_items} | Workers: {active_workers}/{target_workers}")
                        
                        # Salva em lotes
                        save_batch_size = target_workers * 5
                        if len(self.unsaved_data) >= save_batch_size:
                            self.save_data()
                except queue.Empty:
                    # Se a fila de resultados está vazia, verifica a fila de tarefas.
                    # Se a fila de tarefas também está vazia, é hora de escanear por buracos.
                    if self.tasks_queue.empty():
                        self.login_log_queue.put("Fila de tarefas vazia. Verificando por buracos na planilha de saída...")
                        buracos_encontrados_nesta_varredura = self._find_and_queue_buracos(log_prefix="[VARREDURA BUR.] ")
                        
                        if buracos_encontrados_nesta_varredura == 0:
                            # Se não encontrou buracos e a fila de tarefas ainda está vazia,
                            # e não há dados não salvos, então o processo está realmente concluído.
                            if self.unsaved_data: # Salva o que sobrou antes de parar
                                self.save_data()
                            self.login_log_queue.put("Nenhum buraco adicional encontrado. Processamento finalizado.")
                            break # Sai do loop principal

                    # Pequena pausa para evitar loop intenso se as filas estiverem vazias
                    time.sleep(0.5) 
                    continue # Continua o loop para verificar novamente as filas

            # Salva quaisquer dados restantes no buffer, especialmente após uma parada
            if self.unsaved_data:
                self.save_data()
            
            # Log de conclusão
            if not self.stop_event.is_set():
                self.login_log_queue.put("\nPROCESSAMENTO CONCLUÍDO COM SUCESSO!")
            else:
                self.login_log_queue.put("\nProcessamento interrompido pelo usuário. Dados restantes foram salvos.")
            
        except Exception as e:
            self.login_log_queue.put(f"\nERRO DURANTE PROCESSAMENTO: {str(e)}")
            import traceback
            self.log(traceback.format_exc())
        finally:
            self.cleanup()
            self.status_var.set("Concluído" if not self.stop_event.is_set() else "Interrompido")
            self.speed_var.set("-- itens/min")
            self.eta_var.set("ETA: --:--:--")
            self.start_btn.config(state='normal')
            self.stop_btn.config(state='disabled')
            self.workers_spinbox.config(state='normal')
            self.headless_check.config(state='normal')
    
    def save_data(self):
        """Salva os dados mantendo TODOS os cabeçalhos originais"""
        if not hasattr(self, 'output_file'):
            output_path = os.path.join(
                os.path.dirname(self.input_file),
                os.path.splitext(os.path.basename(self.input_file))[0] + "_PROCESSADO.xlsx"
            )
        else:
            output_path = self.output_file
        
        try:
            items_to_save_count = len(self.unsaved_data)
            if items_to_save_count == 0:
                return
            # Cria/abre arquivo de saída
            if os.path.exists(output_path):
                wb = openpyxl.load_workbook(output_path)
                if self.selected_sheet in wb.sheetnames:
                    data_sheet = wb[self.selected_sheet]
                else: # Se a planilha não existe no arquivo, cria uma nova
                    data_sheet = wb.create_sheet(self.selected_sheet)
                    data_sheet.append([header_labels.get(h, h) for h in HEADERS])
            else:
                wb = openpyxl.Workbook()
                data_sheet = wb.active
                data_sheet.title = self.selected_sheet
                data_sheet.append([header_labels.get(h, h) for h in HEADERS])
            
            # Garante que a planilha de metadados exista
            if "Metadata" in wb.sheetnames:
                meta_sheet = wb["Metadata"]
            else:
                meta_sheet = wb.create_sheet("Metadata")
            
            # Atualiza o conjunto de linhas salvas com os novos itens
            newly_saved_rows = {item.get('row_num') for item in self.unsaved_data if item.get('row_num')}
            self.saved_rows.update(newly_saved_rows)
            
            last_processed_row = max(self.saved_rows) if self.saved_rows else 0
            
            # Atualiza metadados na planilha "Metadata"
            meta_sheet['A1'], meta_sheet['B1'] = "Input File Hash", self.input_hash
            meta_sheet['A2'], meta_sheet['B2'] = "Last Processed Row", last_processed_row
            meta_sheet['A3'], meta_sheet['B3'] = "Timestamp", datetime.now().strftime("%d/%m/%Y %H:%M:%S")
            meta_sheet['A4'], meta_sheet['B4'] = "Saved Rows", ",".join(map(str, sorted(list(self.saved_rows))))

            
            # Adiciona dados na linha correta do arquivo de origem
            for item in self.unsaved_data:
                row_num = item.get('row_num')
                if not row_num:
                    # Fallback de segurança: adiciona no final se a linha não for encontrada
                    self.login_log_queue.put(f"AVISO: Item {item.get('code')} sem número de linha. Adicionando ao final.")
                    data_sheet.append([item.get(h, "") for h in HEADERS])
                    continue

                row_data = [item.get(h, "") for h in HEADERS]
                # Escreve os dados na linha específica, em vez de apenas adicionar ao final
                for col_idx, value in enumerate(row_data, start=1):
                    data_sheet.cell(row=row_num, column=col_idx, value=value)
            
            wb.save(output_path)
            
            self.saved_items_count += items_to_save_count
            self.progress_var.set(self.saved_items_count)
            self.progress_label.config(text=f"{self.saved_items_count}/{self.total_items}")

            self.unsaved_data = []  # Limpa dados salvos
            self.login_log_queue.put(f"Lote de {items_to_save_count} salvo. Total salvo: {self.saved_items_count} linhas.")
        except Exception as e:
            self.login_log_queue.put(f"ERRO AO SALVAR: {str(e)}")
    
    def stop_process(self):
        self.stop_event.set()
        self.status_var.set("Finalizando...")
        self.login_log_queue.put("\nSolicitação de parada recebida - salvando dados...")

    def save_config(self):
        """Salva as configurações (como o número de workers) no config.json"""
        if not self.config:
            return
        try:
            if "scraping_settings" not in self.config:
                self.config["scraping_settings"] = {}
            self.config["scraping_settings"]["num_workers"] = self.num_workers_var.get()
            
            if "system" not in self.config:
                self.config["system"] = {}
            if "chrome_options" not in self.config["system"]:
                self.config["system"]["chrome_options"] = {}
            self.config["system"]["chrome_options"]["headless"] = self.headless_var.get()

            with open(self.config_path, 'w', encoding='utf-8') as f:
                json.dump(self.config, f, indent=4, ensure_ascii=False)
        except Exception as e:
            print(f"Aviso: Não foi possível salvar as preferências em config.json: {e}")
    
    def cleanup(self):
        """Garante o fechamento de todos os drivers"""
        self.login_log_queue.put("\nSinalizando para workers finalizarem...")
        if not self.stop_event.is_set():
            self.stop_event.set()
        with self.threads_lock:
            # CORREÇÃO: Removido o 'self in' extra que causava erro de sintaxe
            for thread in self.worker_threads: 
                thread.join(timeout=5) # Espera um pouco para as threads terminarem

        try:
            for proc in psutil.process_iter(['pid', 'name']):
                if proc.info['name'] == 'chromedriver.exe':
                    proc.kill()
        except Exception as e:
            self.login_log_queue.put(f"Erro ao limpar processos chromedriver: {e}")
        self.login_log_queue.put("Limpeza concluída.")
    
    def on_closing(self):
        if messagebox.askokcancel("Sair", "Deseja realmente sair?"):
            self.stop_process()
            self.status_var.set("Finalizando... Por favor, aguarde.")
            # Executa a limpeza pesada em uma thread para não travar a UI
            threading.Thread(target=self._perform_cleanup_and_exit, daemon=True).start()

    def _perform_cleanup_and_exit(self):
        """Executa tarefas de longa duração e fecha a aplicação."""
        self.save_config()
        self.cleanup()
        self.after(0, self.destroy)

if __name__ == "__main__":
    app = Application()
    app.protocol("WM_DELETE_WINDOW", app.on_closing)
    app.mainloop()
