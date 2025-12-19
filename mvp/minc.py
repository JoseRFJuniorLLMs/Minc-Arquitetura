import requests
import pandas as pd
import time
import urllib3

# Silenciar avisos de SSL (comum em redes governamentais)
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


def extrair_dados_salic_final(limite=100, offset=0):
    """
    Consome o endpoint estável de produção do SALIC.
    URL confirmada: https://api.salic.cultura.gov.br/api/v1/projetos
    """
    # A sutil diferença está no '/api/' antes do '/v1/'
    url = "https://api.salic.cultura.gov.br/api/v1/projetos"

    params = {
        'limit': limite,
        'offset': offset,
        'format': 'json'
    }

    headers = {
        "Accept": "application/json",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
    }

    try:
        print(f"📡 Tentando Produção: Offset {offset}...")
        # Adicionado verify=False e timeout robusto
        response = requests.get(url, params=params, headers=headers, timeout=45, verify=False)

        if response.status_code == 200:
            dados = response.json()
            # Tratamento da estrutura HAL+JSON
            return dados.get('_embedded', {}).get('projetos', [])
        elif response.status_code == 404:
            print("❌ Erro 404: O endpoint mudou. Tentando rota alternativa...")
            return None
        else:
            print(f"⚠️ Resposta inesperada: {response.status_code}")
            return []

    except Exception as e:
        print(f"💥 Falha de rede: {e}")
        return []


def pipeline_minc():
    total_desejado = 200
    base_final = []

    for skip in range(0, total_desejado, 100):
        lote = extrair_dados_salic_final(limite=100, offset=skip)

        # Se a primeira tentativa falhar, tenta a URL curta (fallback)
        if lote is None:
            url_curta = "https://api.salic.cultura.gov.br/v1/projetos"
            print(f"🔄 Tentando fallback: {url_curta}")
            res = requests.get(url_curta, params={'limit': 100, 'offset': skip}, verify=False)
            if res.status_code == 200:
                lote = res.json().get('_embedded', {}).get('projetos', [])

        if not lote:
            break

        base_final.extend(lote)
        print(f"✅ {len(base_final)} registros capturados.")
        time.sleep(1)

    if base_final:
        df = pd.DataFrame(base_final)
        nome_arquivo = f"raw_salic_projetos_{int(time.time())}.csv"
        df.to_csv(nome_arquivo, index=False, encoding='utf-8-sig')
        print(f"\n🏆 SUCESSO! Arquivo '{nome_arquivo}' gerado com {len(df)} linhas.")
    else:
        print("\n🤔 A API não retornou dados. O servidor pode estar bloqueando requisições automatizadas.")


if __name__ == "__main__":
    pipeline_minc()