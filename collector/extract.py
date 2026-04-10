import requests
from bs4 import BeautifulSoup
import time
import random
import re
import json
import os
import logging
import sys

# Config do logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:125.0) Gecko/20100101 Firefox/125.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14.4; rv:124.0) Gecko/20100101 Firefox/124.0",
]

session = requests.Session()
N8N_WEBHOOK_URL = "http://n8n:5678/webhook/coleta-magalu"

def gerar_headers(referer=None):
    # Gerar um conj. de headers a fim de simular um navegador real
    ua = random.choice(USER_AGENTS)
    is_firefox = "Firefox" in ua

    headers = {
        "User-Agent": ua,
        "Accept": (
            "text/html,application/xhtml+xml,application/xml"
            ";q=0.9,image/avif,image/webp,*/*;q=0.8"
        ),
        "Accept-Language": "pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7",
        "Accept-Encoding": "gzip, deflate, br",

        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1",
        "Cache-Control": "max-age=0",

        "Sec-Fetch-Dest": "document",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-Site": "none" if not referer else "same-origin",
        "Sec-Fetch-User": "?1",

        "sec-ch-ua-mobile": "?0",
    }

    if not is_firefox:
        headers["sec-ch-ua"] = (
            '"Chromium";v="124", "Google Chrome";v="124", "Not-A.Brand";v="99"'
        )
        headers["sec-ch-ua-platform"] = random.choice(
            ['"Windows"', '"macOS"', '"Linux"']
        )

    if referer:
        headers["Referer"] = referer

    return headers


def renovar_sessao(indice):
    #Limpa cookies/headers da sessão a cada 10 produtos
    global session
    if indice > 0 and indice % 10 == 0:
        session.cookies.clear()
        session.headers.clear()
        logger.info("🔄 Sessão renovada (anti-fingerprint).")

# Envio dos dados pro N8N
def enviar_para_n8n(dados):
    try:
        response = requests.post(N8N_WEBHOOK_URL, json=dados, timeout=10)
        if response.status_code == 200:
            logger.info(f"Sucesso: {dados['produto_titulo'][:30]}... enviado ao BigQuery.")
        else:
            logger.warning(f"WARNING - n8n status {response.status_code}")
    except Exception as e:
        logger.error(f"ERRO - Falha de conexão com n8n: {e}")

# Função de Scraper do Magazineluiza
def extrair_detalhes_produto(url, indice, referer=None):
    try:
        renovar_sessao(indice)
        headers = gerar_headers(referer=referer)
        response = session.get(url, headers=headers, timeout=15)

        if response.status_code != 200:
            logger.warning(f"WARNING -  Status {response.status_code} para {url}")
            return None

        soup = BeautifulSoup(response.text, "html.parser")

        # Dicionario de dados
        item_data = {
            "timestamp_coleta": time.strftime('%Y-%m-%d %H:%M:%S'),
            "eh_full": False,
            "produto_titulo": "N/A",
            "preco_original": "N/A",
            "preco_pix": "N/A",
            "num_parcelas": "N/A",
            "valor_parcela": "N/A",
            "valor_total_parcelado": "N/A",
            "produto_nota": "N/A",
            "produto_num_avaliacoes": "N/A",
            "vendedor_nome": "Não encontrado",
            "vendedor_reputacao": "N/A",
            "vendedor_qtd_vendas": "N/A",
            "vendedor_desde": "N/A",
            "vendedor_entrega_prazo": "N/A",
            "vendedor_atendimento": "N/A",
            "url_produto": url
        }

        # Vê se o produto é Full
        full_tag = soup.find("div", {"data-testid": "closeness-tag"})
        if full_tag and "Full" in full_tag.get_text():
            item_data["eh_full"] = True

        # Título do Produto
        titulos_tags = soup.find_all("h1", {"data-testid": "heading"})
        
        titulo_final = "N/A"
        for tag in titulos_tags:
            classes = tag.get("class", [])
            if "hidden" not in classes:
                titulo_final = tag.get_text(strip=True)
                break
        
        item_data["produto_titulo"] = titulo_final

        # Preços e Parcelamento
        original = soup.find("p", {"data-testid": "price-original"})
        if original:
            item_data["preco_original"] = original.get_text(strip=True)

        pix_box = soup.find("p", {"data-testid": "price-value"})
        if pix_box:
            txt = pix_box.get_text(strip=True).replace("\xa0", " ")
            item_data["preco_pix"] = f"R$ {txt.split('R$')[-1].strip()}"

        parc_tag = soup.find("span", string=re.compile(r"em \d+x de"))
        if parc_tag:
            txt_p = parc_tag.get_text(strip=True).replace("\xa0", " ")
            m_total = re.search(r"R\$\s?([\d\.,]+)\s?em", txt_p)
            m_vezes = re.search(r"(\d+)x", txt_p)
            m_valor = re.search(r"de\s?R\$\s?([\d\.,]+)", txt_p)
            if m_total: item_data["valor_total_parcelado"] = m_total.group(1)
            if m_vezes: item_data["num_parcelas"] = m_vezes.group(1)
            if m_valor: item_data["valor_parcela"] = m_valor.group(1)

        # Nota do Produto
        rating_tag = soup.find("span", {"data-testid": "rating-label"})
        if rating_tag:
            txt_r = rating_tag.get_text(strip=True)
            m_nota = re.search(r"([\d\.]+)", txt_r)
            m_count = re.search(r"\((\d+)\)", txt_r)
            if m_nota: item_data["produto_nota"] = m_nota.group(1)
            if m_count: item_data["produto_num_avaliacoes"] = m_count.group(1)

        # Dados do Vendedor
        loja = soup.find("h3", class_=re.compile(r"font-sm-medium"))
        if loja:
            item_data["vendedor_nome"] = loja.get_text(strip=True)

        nota_v = soup.find("div", class_=re.compile(r"text-success-default"))
        if nota_v:
            item_data["vendedor_reputacao"] = nota_v.get_text(strip=True)

        # Qtd de Vendas  do vendedor
        vendas = soup.find("p", string=re.compile(r"\+\d+|mil|vendas", re.I))
        if vendas:
            item_data["vendedor_qtd_vendas"] = vendas.get_text(strip=True)

        # Tempo do vendedor na magalu
        desde_tag = soup.find("p", string=re.compile(r"desde.*\d{4}", re.I))
        if not desde_tag:
            tags_potenciais = soup.find_all("p", string=re.compile(r"desde|há", re.I))
            for t in tags_potenciais:
                if re.search(r"\d+", t.get_text()):
                    desde_tag = t
                    break

        item_data["vendedor_desde"] = desde_tag.get_text(strip=True) if desde_tag else "N/A"

        # Entrega no Prazo
        entrega_lbl = soup.find("p", string=re.compile(r"Entrega", re.I))
        if entrega_lbl:
            val = entrega_lbl.find_next_sibling("p")
            if val: item_data["vendedor_entrega_prazo"] = val.get_text(strip=True)

        # Atendimento
        atend_lbl = soup.find("p", string=re.compile(r"Atendimento", re.I))
        if atend_lbl:
            val = atend_lbl.find_next_sibling("p")
            if val: item_data["vendedor_atendimento"] = val.get_text(strip=True)

        return item_data

    except Exception as e:
        logger.error(f"ERROR - Erro ao extrair item {indice}: {e}")
        return None

# Loop de acesso
def coletar_e_processar(query, qtd=3):
    url_busca = f"https://www.magazineluiza.com.br/busca/{query.replace(' ', '-')}/"
    logger.info(f"Buscando termo: {query.upper()}")

    headers_busca = gerar_headers()
    try:
        resp = session.get(url_busca, headers=headers_busca, timeout=15)
        soup = BeautifulSoup(resp.text, "html.parser")

        links = []
        for a in soup.find_all("a", href=True):
            href = a.get("href")
            if "/p/" in href and "magazineluiza.com.br" not in href:
                links.append("https://www.magazineluiza.com.br" + href)

        links = list(dict.fromkeys(links))[:qtd]
        logger.info(f"Encontrados {len(links)} links. Iniciando extração dos dados dos produtos...")

        for i, link in enumerate(links):
            res = extrair_detalhes_produto(link, i + 1, referer=url_busca)
            if res:
                enviar_para_n8n(res)
            time.sleep(random.uniform(4, 7))

    except Exception as e:
        logger.error(f"ERRO - Erro na busca por {query}: {e}")

# Carrega JSON que contém os produtos que queremos ver os dados
def carregar_alvos():
    caminho = "/opt/airflow/collector/smartphones_alvo.json"
    if not os.path.exists(caminho):
        caminho = "collector/smartphones_alvo.json"
    with open(caminho, "r", encoding="utf-8") as f:
        return json.load(f)

# Main
if __name__ == "__main__":
    try:
        config = carregar_alvos()
        smartphones = config.get("smartphones", config.get("monitoramento", []))
        qtd = config["config"]["itens_por_modelo"]

        logger.info(f"INÍCIO - Iniciando pipeline")

        for modelo in smartphones:
            coletar_e_processar(modelo, qtd=qtd)
            time.sleep(random.uniform(5, 10))

        logger.info("Pipeline finalizadao com sucesso!")

    except Exception as e:
        logger.critical(f"ERRO - Falha: {e}")