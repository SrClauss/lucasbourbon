from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, StaleElementReferenceException, NoSuchElementException
import traceback
from typing import Optional
import queue

def search_product(driver, product_code, worker_id=None, row_num=None, log_queue: Optional[queue.Queue] = None):
    """
    Extrai dados de um produto com estrutura de erro robusta
    """
 
    log_prefix = f"[Worker {worker_id}] " if worker_id else ""
    log_line = f"linha {row_num}: " if row_num else ""
    
    def _log(message: str):
        """Helper para enviar logs para a fila ou console."""
        if log_queue:
            log_queue.put(message)
        else:
            print(message)

    try:
        # Acesso à página do produto
        driver.get(f"https://ctshoponline.atlascopco.com/en-GB/products/{product_code}")
        _log(f"{log_prefix}{log_line}Acessando: {product_code}")
        
        # Localizadores
        locators = {
            "product_name": (By.XPATH, "//*[@id='__next']/div/div/div[1]/div[2]/section/div/div[1]/h1"),
            "not_found": (By.XPATH, "//h2[contains(., 'The server cannot find the requested resource.')]"),
            "no_longer_available": (By.XPATH, "//*[contains(text(), 'The product is no longer available')]"),
            "cannot_add": (By.XPATH, "//h5[contains(., 'Product cannot be added to cart')]")
        }

        # Verificação inicial
        try:
            element = WebDriverWait(driver, 10).until(
                EC.any_of(
                    EC.presence_of_element_located(locators["product_name"]),
                    EC.presence_of_element_located(locators["not_found"])
                )
            )
        except TimeoutException:
            _log(f"{log_prefix}{log_line}❌ Timeout: {product_code}")
            return {"code": product_code, "name": "", "status": "Tempo Esgotado", "row_num": row_num}

        # Verifica se produto não foi encontrado
        if element.tag_name == 'h2':
            _log(f"{log_prefix}{log_line}❌ Não encontrado: {product_code}")
            return {"code": product_code, "name": "", "status": "Não Encontrado", "row_num": row_num}

        # Inicializa produto com campos vazios
        product = {
            "code": product_code,
            "name": element.text,
            "status": "Disponível",
            "row_num": row_num,
            "pricing": "",
            "discount": "",
            "pricing_with": "",
            "cofins_tax": "",
            "cofins_value": "",
            "difalst_tax": "",
            "difalst_value": "",
            "fecop_tax": "",
            "fecop_value": "",
            "icmi_value": "",
            "icms_tax": "",
            "icms_value": "",
            "ipi_tax": "",
            "ipi_value": "",
            "pis_tax": "",
            "pis_value": "",
            "st_tax": "",
            "st_value": "",
            "weight": "",
            "country_of_origin": "",
            "customs_tariff": "",
            "possibility_to_return": ""
        }

        # SEÇÃO 1: EXTRAÇÃO DE PREÇOS
        try:
            # Muda para aba de Pricing
            pricing_tab = WebDriverWait(driver, 10).until(
                EC.element_to_be_clickable((By.XPATH, "//button[contains(., 'Pricing')]"))
            )
            driver.execute_script("arguments[0].click();", pricing_tab)
            xpath_com_condicao = "(//div[@role='tabpanel']//td)[1][contains(., 'BRL') or contains(., 'R$')]"
            # Aguarda dados carregarem
            WebDriverWait(driver, 15).until(
                EC.presence_of_element_located((By.XPATH, xpath_com_condicao))
            ).text
            # Extrai dados de preço
            tds = [td.text for td in driver.find_elements(By.XPATH, "//div[@role='tabpanel']//td")]


            product["pricing"] = tds[0].replace("R$", "").replace("BRL ", "")
            product["discount"] = "0" if tds[1] == "-" else tds[1]
            product["pricing_with"] = tds[2].replace("R$", "").replace("BRL ", "")

                
        except Exception as e:
   
            # Se falhar, verifica se o produto está indisponível
            if driver.find_elements(*locators["no_longer_available"]) or driver.find_elements(*locators["cannot_add"]):
                _log(f"{log_prefix}{log_line}⚠️ Produto indisponível: {product_code}")
                product["status"] = "Indisponível"
                # Retorna o produto aqui, pois não haverá mais dados
                _log(f"{log_prefix}{log_line}✅ Sucesso (Indisponível): {product_code}")
                return product
            else:
                _log(f"{log_prefix}{log_line}⚠️ Erro preços: {str(e)}")

        # SEÇÃO 2: EXTRAÇÃO DE IMPOSTOS
        try:
            # Muda para aba de Taxes
            taxes_tab = WebDriverWait(driver, 10).until(
                EC.element_to_be_clickable((By.XPATH, "//button[contains(., 'Taxes')]"))
            )
            driver.execute_script("arguments[0].click();", taxes_tab)
            
            # Aguarda dados carregarem
            table = WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.XPATH, "//div[@role='tabpanel']//table"))
            )
            
            # Extrai células da tabela
            cells = [cell.text for cell in driver.find_elements(
                By.XPATH, "//div[@role='tabpanel']//td[@data-cy='informationTableCell']"
            )]
            
            # Processamento de valores fiscais
            def _parse_tax(tax_str):
                if not tax_str:
                    return "", ""
                if "% (BRL " in tax_str:
                    parts = tax_str.split("% (BRL ")
                    return parts[0], parts[1].replace(")", "")
                elif "BRL " in tax_str:
                    return "", tax_str.split("BRL ")[1]
                return "", tax_str

            # Mapeia células para campos (com verificação de índice)
            tax_fields = [
                ("cofins", 1), ("difalst", 3), ("fecop", 5),
                ("icms", 9), ("ipi", 11), ("pis", 13), ("st", 15)
            ]
            
            for field, index in tax_fields:
                if len(cells) > index:
                    tax, value = _parse_tax(cells[index])
                    product[f"{field}_tax"] = tax
                    product[f"{field}_value"] = value
                    
        except Exception as e:
            _log(f"{log_prefix}{log_line}⚠️ Erro impostos: {str(e)}")

        # SEÇÃO 3: INFORMAÇÕES DO PRODUTO
        try:
            # Muda para aba de informações
            info_tab = WebDriverWait(driver, 10).until(
                EC.element_to_be_clickable((By.XPATH, "//button[contains(., 'Product information')]"))
            )
            driver.execute_script("arguments[0].click();", info_tab)
            
            # Aguarda tabela carregar
            table = WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.XPATH, "//div[@role='tabpanel']//table"))
            )
            
            # Processa linhas da tabela
            for tr in table.find_elements(By.TAG_NAME, "tr"):
                tds = tr.find_elements(By.TAG_NAME, "td")
                if len(tds) < 2:
                    continue
                    
                key = tds[0].text.strip().lower()
                value = tds[1].text.strip()
                
                if "country of origin" in key:
                    product["country_of_origin"] = value
                elif "customs tariff" in key:
                    product["customs_tariff"] = value
                elif "weight" in key:
                    product["weight"] = value
                elif "possibility to return" in key:
                    product["possibility_to_return"] = value
                    
        except Exception as e:
            _log(f"{log_prefix}{log_line}⚠️ Erro informações: {str(e)}")

        _log(f"{log_prefix}{log_line}✅ Sucesso: {product_code}")
        return product

    except Exception as e:
        _log(f"{log_prefix}{log_line}❌ ERRO GRAVE: {str(e)}")
        return {"code": product_code, "name": "", "status": f"ERRO GRAVE: {str(e)}", "row_num": row_num}