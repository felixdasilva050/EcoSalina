from flask import Flask, jsonify, request, abort
from analytics import TranspetroAnalytics  # Importa APENAS a classe
import pandas as pd
from typing import Optional

# ====================================================================
# 1. VARIÁVEIS DE CONFIGURAÇÃO (Definidas aqui para evitar ImportError)
# ====================================================================
EVENTOS_FILE = 'ResultadoQueryEventos.csv'
CONSUMO_FILE = 'ResultadoQueryConsumo.csv'
REVESTIMENTO_FILE = 'Dados navios Hackathon.xlsx - Especificacao revestimento.csv'
IWS_FILE = 'Relatorios IWS.xlsx - Planilha1.csv'

app = Flask(__name__)
# Tipagem para garantir que o objeto de análise seja carregado
analytics: Optional[TranspetroAnalytics] = None
df_conformidade_normam: pd.DataFrame = pd.DataFrame()

# ====================================================================
# 2. INICIALIZAÇÃO E PRÉ-CÁLCULO
# ====================================================================

try:
    # 1. Instancia a classe de análise (carrega todos os dados)
    analytics = TranspetroAnalytics(EVENTOS_FILE, CONSUMO_FILE, REVESTIMENTO_FILE, IWS_FILE)

    # 2. Pré-calcula a Conformidade NORMAM 401 (Métrica 5) na inicialização
    if analytics:
        df_conformidade_normam = analytics.calcular_conformidade_normam_401()

    print("\n[INFO] Análise e pré-cálculos prontos. API Flask inicializada.")

except Exception as e:
    print(f"\n[FATAL ERROR] Falha ao inicializar a classe de análise. Verifique logs e arquivos: {e}")
    analytics = None


# Função de apoio para checagem de serviço
def check_analytics_ready():
    """Verifica se o serviço de análise foi inicializado com sucesso."""
    if analytics is None:
        abort(503, description="Serviço de análise indisponível. Verifique logs de inicialização e arquivos de dados.")


# ====================================================================
# 3. ROTAS (ENDPOINTS)
# ====================================================================

@app.route('/', methods=['GET'])
def home():
    """Rota inicial para verificar o status da API."""
    return jsonify({
        "status": "online" if analytics else "inicializacao_falhou",
        "message": "API de Análise Transpetro / Hackathon",
        "endpoints_principais": [
            "/metrics/total_embarcacoes",
            "/metrics/consumo_mensal",
            "/metrics/navegacao_diaria",
            "/metrics/risco_bioincrustacao_frota",
            "/metrics/conformidade_normam",
            "/metrics/clima_navio/<ship_name>"
        ]
    })


# --- ENDPOINTS DO DASHBOARD (Métricas 1, 3 e 4) ---

@app.route('/metrics/total_embarcacoes', methods=['GET'])
def get_total_embarcacoes():
    """Métrica 1: Retorna o total de embarcações únicas."""
    check_analytics_ready()
    total = analytics.calcular_total_embarcacoes()
    return jsonify({"metrica": "total_embarcacoes", "valor": total})


@app.route('/metrics/consumo_mensal', methods=['GET'])
def get_consumo_mensal():
    """Métrica 3: Retorna o consumo total de combustível agrupado por mês."""
    check_analytics_ready()
    df = analytics.calcular_consumo_mensal_total()
    dados = df.to_dict(orient='records')
    return jsonify({"metrica": "consumo_mensal", "dados": dados})


@app.route('/metrics/navegacao_diaria', methods=['GET'])
def get_navegacao_diaria():
    """Métrica 4: Retorna o número de embarcações navegando por dia."""
    check_analytics_ready()
    df = analytics.calcular_embarcacoes_navegando_por_dia()
    df['Data'] = df['Data'].astype(str)
    dados = df.to_dict(orient='records')
    return jsonify({"metrica": "navegacao_diaria", "dados": dados})


# --- ENDPOINTS DE RISCO E CONFORMIDADE (Métricas 5 e 8) ---

@app.route('/metrics/conformidade_normam', methods=['GET'])
def get_conformidade_normam():
    """Métrica 5: Retorna a Conformidade NORMAM 401 por navio e mês."""
    if df_conformidade_normam.empty:
        abort(503, description="Dados de Conformidade NORMAM 401 indisponíveis após pré-cálculo.")

    dados = df_conformidade_normam.to_dict(orient='records')
    return jsonify({"metrica": "conformidade_normam", "dados": dados})


@app.route('/metrics/risco_bioincrustacao_frota', methods=['GET'])
def get_risco_bioincrustacao_frota():
    """Métrica 8: Retorna o Risco de Bioincrustação (1-5) para toda a frota por mês (POR EMBARCAÇÃO)."""
    check_analytics_ready()

    # Usa o DataFrame pré-calculado da Conformidade (Métrica 5)
    df_conformidade = df_conformidade_normam

    # CHAMADA CORRIGIDA: Agora o método existe na classe
    df = analytics.calcular_risco_bioincrustacao_frota(df_conformidade)

    df['Mês/Ano'] = df['Mês/Ano'].astype(str)

    dados = df.to_dict(orient='records')
    return jsonify({"metrica": "risco_bioincrustacao_frota", "dados": dados})


@app.route('/metrics/clima_navio/<string:ship_name>', methods=['GET'])
def get_clima_navio(ship_name):
    """Métrica 6: Retorna dados climáticos históricos para uma embarcação específica."""
    check_analytics_ready()

    df_clima = analytics.obter_dados_climaticos_navio(ship_name)

    if df_clima.empty:
        abort(404, description=f"Dados climáticos não encontrados ou erro na API para {ship_name}.")

    df_clima['DataHoraGMT'] = df_clima['DataHoraGMT'].astype(str)

    dados = df_clima.to_dict(orient='records')
    return jsonify({"navio": ship_name, "dados_climaticos": dados})


# ====================================================================
# 4. EXECUÇÃO
# ====================================================================

if __name__ == '__main__':
    print("\n--- INICIANDO SERVIDOR FLASK ---")
    print("Acesse: http://127.0.0.1:8000/")

    app.run(host="0.0.0.0", port=8000, debug=True)