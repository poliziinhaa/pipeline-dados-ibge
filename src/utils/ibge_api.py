import requests
import time

def buscar_dados_sidra(url, retries=3, backoff_factor=1):
    """
    Realiza uma requisição GET na API do SIDRA com mecanismo de retry.
    
    Args:
        url (str): Endpoint da API.
        retries (int): Número de tentativas em caso de falha.
        backoff_factor (int): Tempo de espera progressivo entre tentativas.
        
    Returns:
        list: Lista de dicionários (JSON) ou lista vazia em caso de erro persistente.
    """
    attempt = 0
    while attempt < retries:
        try:
            response = requests.get(url, timeout=60)
            
            if response.status_code == 200:
                return response.json()
            elif response.status_code == 429: # Too Many Requests
                print(f"Throttling (429) na URL. Aguardando...")
                time.sleep(2)
            else:
                print(f"Erro {response.status_code} na URL: {url}")
                
        except Exception as e:
            print(f"Falha de conexão (Tentativa {attempt+1}/{retries}): {str(e)}")
        
        attempt += 1
        time.sleep(backoff_factor * attempt) # Backoff exponencial simples
        
    print(f"Falha definitiva após {retries} tentativas na URL: {url}")
    return [] # Retorna vazio, mas não quebra o loop principal (decisão de design)