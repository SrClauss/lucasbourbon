from selenium import webdriver
from selenium.webdriver.chrome.options import Options as ChromeOptions
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import json
import os
import sys
import traceback
from typing import Optional, Union
import queue

class AtlasCopcoLogin:
    def __init__(self, headless: bool = False, log_queue: Optional[queue.Queue] = None):
        """
        Configuração idêntica ao script original
        """
        self.config = self._load_config()
        self.headless = headless
        self.driver = None
        self.log_queue = log_queue

    def _load_config(self) -> dict:
        """Carrega credenciais do config.json exatamente como no original"""
        try:
            # Determina o caminho base correto para o config.json.
            if getattr(sys, 'frozen', False):
                # Rodando como um executável PyInstaller
                base_path = os.path.dirname(sys.executable)
            else:
                # Rodando como um script Python normal
                base_path = os.path.dirname(os.path.abspath(__file__))
            config_path = os.path.join(base_path, 'config.json')
            with open(config_path, 'r') as f:
                return json.load(f)
    
        except Exception as e:
            self._log(f"ERRO: Não foi possível carregar config.json: {str(e)}")
            raise

    def _log(self, message: str):
        """Envia a mensagem para a fila de logs ou imprime no console."""
        if self.log_queue:
            self.log_queue.put(message)
        else:
            print(message)

    def _configure_driver(self) -> webdriver.Chrome:
        """Configuração IDÊNTICA ao driver original"""
        options = ChromeOptions()
        
        # Configurações originais exatas
        options.add_argument('--log-level=3')
        options.add_experimental_option('excludeSwitches', ['enable-logging'])
        options.add_argument("--disable-extensions")
        options.add_argument("--disable-gpu")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--window-size=1200,800")  # Tamanho fixo original
        
        if self.headless:
            options.add_argument("--headless=new")
        
        return webdriver.Chrome(options=options)

    def login(self) -> Optional[webdriver.Chrome]:
        """
        Fluxo de login IDÊNTICO ao original com logs simplificados.
        """
        try:
            self._log("Iniciando o processo de login...") # Log simplificado de início
            self.driver = self._configure_driver()
            self.driver.get("https://ctshoponline.atlascopco.com/pt-BR/login")
            
            # 1. Aceitar cookies
            WebDriverWait(self.driver, 15).until(
                EC.element_to_be_clickable((By.ID, "onetrust-accept-btn-handler"))
            ).click()
            
            # 2. Clicar em 'Conecte-se'
            WebDriverWait(self.driver, 15).until(
                EC.element_to_be_clickable((By.XPATH, "//button[contains(., 'Conecte-se')]"))
            ).click()
            
            # 3. Obter credenciais do config
            credentials = self.config.get('credentials')
            if not credentials or 'username' not in credentials or 'password' not in credentials:
                self._log("ERRO: A seção 'credentials' com 'username' e 'password' não foi encontrada ou está incompleta no config.json.")
                if self.driver:
                    self.driver.quit()
                return None

            # 4. Preencher email
            email_input = WebDriverWait(self.driver, 15).until(
                EC.element_to_be_clickable((By.XPATH, "//input[@type='email']"))
            )
            email_input.send_keys(credentials["username"])
            
            # 5. Submeter email
            WebDriverWait(self.driver, 15).until(
                EC.element_to_be_clickable((By.XPATH, "//input[@type='submit']"))
            ).click()
            
            # 6. Preencher senha
            password_input = WebDriverWait(self.driver, 15).until(
                EC.element_to_be_clickable((By.XPATH, "//input[@type='password']"))
            )
            password_input.send_keys(credentials["password"])
            
            # 7. Clicar em 'Entrar'
            WebDriverWait(self.driver, 15).until(
                EC.element_to_be_clickable((By.ID, "idSIButton9"))
            ).click()
            
            # 8. Recusar permanecer conectado
            WebDriverWait(self.driver, 15).until(
                EC.element_to_be_clickable((By.ID, "idBtn_Back"))
            ).click()
            
            # Verificação de login
            WebDriverWait(self.driver, 30).until(
                EC.presence_of_element_located((By.XPATH, "//p[contains(., 'Welcome') and .//b[text()='Vendas']]"))
            )
            self._log("Login bem-sucedido!") # Log de sucesso
            return self.driver
            
        except Exception as e:
            self._log(f"❌ Falha no login: {str(e)}") # Log de falha
            self._log(traceback.format_exc())
            if self.driver:
                self.driver.quit()
            return None

    def logout(self):
        """Fecha o driver exatamente como no original"""
        if self.driver:
            try:
                self.driver.quit()
            except:
                pass
            finally:
                self.driver = None

# Função de compatibilidade para manter o mesmo uso do original
def login(headless: bool = False, log_queue: Optional[queue.Queue] = None):
    """Versão de função compatível com o código original"""
    service = AtlasCopcoLogin(headless=headless, log_queue=log_queue)
    return service.login()

if __name__ == "__main__":
    # Teste (igual ao uso original)
    driver = login()
    if driver:
        input("Pressione Enter para sair...")
        driver.quit()
