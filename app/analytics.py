import pandas as pd
import os
import requests
from urllib.parse import urlencode
from datetime import datetime, timedelta, date
from typing import Dict, Any, List


# ==============================================================================
# CLASSE PRINCIPAL DE ANÁLISE DE DADOS
# ==============================================================================

class TranspetroAnalytics:
    """
    Assistente de Programação para o Hackathon Brasil / Transpetro.
    Focado em processamento de dados de navios, consumo, eventos, conformidade NORMAM 401
    e integração com API externa para análise climática.
    """

    # Mapeamentos de colunas para carregamento robusto
    REVESTIMENTO_COLS = {
        'Nome do navio': 'shipName',
        'Data da aplicacao': 'DataAplicacao',
        'Cr1. Período base de verificação': 'T_base',
        'Cr1. Parada máxima acumulada no período': 'T_max'
    }
    IWS_COLS = {
        'Embarcação': 'shipName',
        'Tipo de incrustação da embarcação': 'TipoIncrustacao',
        'Data': 'DataRelatorio'
    }

    def __init__(self, eventos_path: str, consumo_path: str, revestimento_path: str, iws_path: str):
        """Inicializa a classe e carrega todos os DataFrames."""
        print("Iniciando o carregamento e pré-processamento dos dados...")

        # Carregamentos essenciais
        self.df_eventos = self._carregar_eventos(eventos_path)
        self.df_consumo = self._carregar_consumo(consumo_path)

        # Carregamentos robustos
        self.df_revestimento = self._carregar_revestimento(revestimento_path)
        self.df_iws = self._carregar_relatorios_iws(iws_path)

        self.df_consolidado = self._consolidar_dados()

        if self.df_consolidado.empty:
            print("⚠️ Atenção: A base consolidada (Eventos + Consumo) está vazia.")
        else:
            print("✅ Dados carregados e consolidados com sucesso.")

    # --- MÉTODOS DE CARREGAMENTO DE DADOS ---

    def _carregar_csv_robusto(self, path: str, required_cols_map: Dict[str, str]) -> pd.DataFrame:
        """Função auxiliar para carregar arquivos CSV com múltiplas opções de separador e encoding."""
        if not os.path.exists(path):
            print(f"ERRO: Arquivo não encontrado em: {path}")
            return pd.DataFrame()

        df = pd.DataFrame()
        # Opções de carregamento (separador, encoding)
        carregamento_options = [
            {'sep': ',', 'encoding': 'utf-8'},
            {'sep': ';', 'encoding': 'utf-8'},
            {'sep': ';', 'encoding': 'latin1'},
            {'sep': ',', 'encoding': 'latin1'}
        ]

        for options in carregamento_options:
            try:
                df_temp = pd.read_csv(path, **options)

                # 1. Limpeza de nomes de colunas: strip e remove aspas
                df_temp.columns = df_temp.columns.str.strip().str.replace('"', '', regex=False)

                # 2. Verifica se as colunas essenciais existem após a limpeza
                if all(col in df_temp.columns for col in required_cols_map.keys()):
                    df = df_temp
                    break
            except Exception:
                pass

        if df.empty:
            print(f"ERRO fatal ao carregar {path}: Falha na leitura ou colunas essenciais não encontradas.")
            return pd.DataFrame()

        # 3. Renomear e limpar dados
        df.rename(columns=required_cols_map, inplace=True)
        # Garante que 'shipName' exista antes da limpeza de dados
        if 'shipName' in df.columns:
            df['shipName'] = df['shipName'].astype(str).str.strip()

        return df

    def _carregar_eventos(self, path: str) -> pd.DataFrame:
        """Carrega e pré-processa o DataFrame de eventos."""
        try:
            df = pd.read_csv(path)
            df.columns = df.columns.str.strip().str.replace('"', '', regex=False)
            df.rename(columns={'startGMTDate': 'startGMTDate', 'endGMTDate': 'endGMTDate'}, inplace=True)
            df['startGMTDate'] = pd.to_datetime(df['startGMTDate'], errors='coerce')
            df['endGMTDate'] = pd.to_datetime(df['endGMTDate'], errors='coerce')
            return df
        except Exception as e:
            print(f"Erro ao carregar o arquivo de eventos: {e}")
            return pd.DataFrame()

    def _carregar_consumo(self, path: str) -> pd.DataFrame:
        """Carrega e pré-processa o DataFrame de consumo."""
        try:
            df = pd.read_csv(path)
            df.rename(columns={'SESSION_ID': 'sessionId', 'CONSUMED_QUANTITY': 'consumedQuantity'}, inplace=True)
            df.columns = df.columns.str.strip().str.replace('"', '', regex=False)
            df['consumedQuantity'] = pd.to_numeric(df['consumedQuantity'], errors='coerce').fillna(0)
            return df
        except Exception as e:
            print(f"Erro ao carregar o arquivo de consumo: {e}")
            return pd.DataFrame()

    def _carregar_revestimento(self, path: str) -> pd.DataFrame:
        """Carrega o DataFrame de Especificacao revestimento com robustez e formatação."""
        df = self._carregar_csv_robusto(path, self.REVESTIMENTO_COLS)
        if df.empty:
            return pd.DataFrame()

        # Conversão de tipos específica
        df['DataAplicacao'] = pd.to_datetime(df['DataAplicacao'], errors='coerce', dayfirst=True)
        df['T_base'] = pd.to_numeric(df['T_base'], errors='coerce')
        df['T_max'] = pd.to_numeric(df['T_max'], errors='coerce')
        df.dropna(subset=['DataAplicacao', 'T_base', 'T_max'], inplace=True)
        print(f"  -> Linhas de revestimento válidas carregadas: {len(df)}")
        return df

    def _carregar_relatorios_iws(self, path: str) -> pd.DataFrame:
        """Carrega o DataFrame de Relatórios IWS, cria a variável alvo de risco."""
        df = self._carregar_csv_robusto(path, self.IWS_COLS)

        if df.empty:
            return pd.DataFrame()

        # Conversão de Data
        df['DataRelatorio'] = pd.to_datetime(df['DataRelatorio'], errors='coerce', dayfirst=True)
        df.dropna(subset=['shipName', 'DataRelatorio'], inplace=True)

        # Criação da Variável Alvo (Risco Binário)
        if 'TipoIncrustacao' in df.columns:
            df['RiscoBioincrustacao'] = 0
            # Alto Risco: Duras, Cracas, Calcárea
            df.loc[df['TipoIncrustacao'].astype(str).str.contains(r'(Duras|Craca|calcárea)', case=False,
                                                                  na=False), 'RiscoBioincrustacao'] = 1

            print(f"  -> Linhas de relatórios IWS válidas carregadas: {len(df)}")
            return df[['shipName', 'DataRelatorio', 'RiscoBioincrustacao']].copy()
        else:
            print(
                "AVISO: Coluna 'Tipo de incrustação da embarcação' não encontrada no IWS. Retornando DataFrame vazio para ML.")
            return pd.DataFrame()

    def _consolidar_dados(self) -> pd.DataFrame:
        """Realiza a união (merge) dos dados de eventos e consumo."""
        if self.df_eventos.empty or self.df_consumo.empty:
            return pd.DataFrame()

        # Remove duplicatas de sessionId no df_eventos antes do merge
        df_eventos_limpo = self.df_eventos[['sessionId', 'shipName', 'startGMTDate', 'eventName']].drop_duplicates(
            subset=['sessionId'])

        df = pd.merge(
            df_eventos_limpo,
            self.df_consumo[['sessionId', 'consumedQuantity']],
            on='sessionId',
            how='inner'
        )
        return df

    # ==========================================================================
    # MÉTODOS DO DASHBOARD (MÉTRICAS 1 a 4)
    # ==========================================================================

    def calcular_total_embarcacoes(self) -> int:
        if self.df_eventos.empty:
            return 0
        return self.df_eventos['shipName'].nunique()

    def calcular_embarcacoes_operando(self) -> int:
        """Métrica 2: Número de embarcações únicas envolvidas em 'NAVEGACAO'."""
        if self.df_eventos.empty:
            return 0
        df_operando = self.df_eventos[self.df_eventos['eventName'] == 'NAVEGACAO']
        return df_operando['shipName'].nunique()

    def calcular_consumo_mensal_total(self) -> pd.DataFrame:
        """Métrica 3: Consumo total de combustível por mês e ano."""
        if self.df_consolidado.empty:
            return pd.DataFrame({'Mês/Ano': [], 'Consumo Total (unidade)': []})

        df = self.df_consolidado.copy()
        df['Mes_Ano'] = df['startGMTDate'].dt.to_period('M')

        consumo_mensal = df.groupby('Mes_Ano')['consumedQuantity'].sum().reset_index()

        consumo_mensal['Mês/Ano'] = consumo_mensal['Mes_Ano'].astype(str)
        consumo_mensal.rename(columns={'consumedQuantity': 'Consumo Total (unidade)'}, inplace=True)

        return consumo_mensal[['Mês/Ano', 'Consumo Total (unidade)']].sort_values(by='Mês/Ano').reset_index(drop=True)

    def calcular_embarcacoes_navegando_por_dia(self) -> pd.DataFrame:
        """
        Métrica 4: Calcula o número total de embarcações únicas navegando para cada dia.
        """
        if self.df_eventos.empty:
            return pd.DataFrame({'Data': [], 'Embarcações Navegando': []})

        df_nav = self.df_eventos[self.df_eventos['eventName'] == 'NAVEGACAO'].copy()
        df_nav.dropna(subset=['startGMTDate', 'endGMTDate'], inplace=True)

        df_nav['start_date'] = df_nav['startGMTDate'].dt.normalize()
        df_nav['end_date'] = df_nav['endGMTDate'].dt.normalize()

        date_ranges = []
        for _, row in df_nav.iterrows():
            dates = pd.date_range(start=row['start_date'], end=row['end_date'], freq='D')
            date_ranges.extend([(row['shipName'], date) for date in dates])

        df_daily_nav = pd.DataFrame(date_ranges, columns=['shipName', 'Data'])

        if df_daily_nav.empty:
            return pd.DataFrame({'Data': [], 'Embarcações Navegando': []})

        df_resultado = df_daily_nav.groupby('Data')['shipName'].nunique().reset_index()
        df_resultado.rename(columns={'shipName': 'Embarcações Navegando'}, inplace=True)

        return df_resultado[['Data', 'Embarcações Navegando']].sort_values(by='Data').reset_index(drop=True)

    # ==========================================================================
    # ANÁLISE DE CONFORMIDADE (MÉTRICA 5)
    # ==========================================================================

    def calcular_conformidade_normam_401(self) -> pd.DataFrame:
        """
        Métrica 5: Calcula o nível de conformidade do revestimento NORMAM 401
        para cada navio por mês.
        """
        if self.df_revestimento.empty or self.df_eventos.empty:
            print("AVISO: Dados de revestimento ou eventos vazios. Não é possível calcular a conformidade.")
            return pd.DataFrame({'Mês/Ano': [], 'shipName': [], 'Conformidade (%)': []})

        df_r = self.df_revestimento.copy()

        min_date = df_r['DataAplicacao'].min().to_period('M')
        max_date = pd.to_datetime('today').to_period('M')
        meses = pd.period_range(start=min_date, end=max_date, freq='M')
        navios_unicos = df_r['shipName'].unique()

        resultados = []

        for ship in navios_unicos:
            df_ship = df_r[df_r['shipName'] == ship].sort_values('DataAplicacao')
            last_app_date = pd.NaT
            T_base_current = 0
            T_max_current = 0

            for mes in meses:
                data_fim_mes = mes.to_timestamp(how='end')

                aplicacao_recente = df_ship[df_ship['DataAplicacao'] <= data_fim_mes]

                if not aplicacao_recente.empty:
                    ultima_app = aplicacao_recente.iloc[-1]

                    if ultima_app['DataAplicacao'] > last_app_date or pd.isna(last_app_date):
                        last_app_date = ultima_app['DataAplicacao']
                        T_base_current = ultima_app['T_base']
                        T_max_current = ultima_app['T_max']

                if pd.isna(last_app_date) or T_base_current == 0 or T_max_current == 0:
                    conformidade = 0.0
                else:
                    T_passado_dias = (data_fim_mes - last_app_date).days
                    T_passado_meses = T_passado_dias / 30.437

                    consumo_base = T_passado_meses / T_base_current
                    consumo_max = T_passado_meses / T_max_current

                    conformidade = 1.0 - max(consumo_base, consumo_max)
                    conformidade = max(0.0, conformidade) * 100.0

                resultados.append({
                    'Mês/Ano': str(mes),
                    'shipName': ship,
                    'Conformidade (%)': round(conformidade, 2)
                })

        return pd.DataFrame(resultados)

    # ==========================================================================
    # INTEGRAÇÃO API E ML (MÉTRICAS 6 e 7)
    # ==========================================================================

    def obter_dados_climaticos_navio(self, ship_name: str,
                                     api_url: str = "https://archive-api.open-meteo.com/v1/era5") -> pd.DataFrame:
        """
        Métrica 6: Busca dados climáticos históricos (ERA5) em chunks de 1 ano,
        usando endpoint de arquivo e respeitando o limite de 2022-01-01.
        """
        if self.df_eventos.empty:
            print("ERRO: DataFrame de eventos vazio. Não é possível obter coordenadas e datas.")
            return pd.DataFrame()

        # 1. Preparação de Dados
        df_navio = self.df_eventos[self.df_eventos['shipName'].astype(str).str.strip() == ship_name.strip()].copy()

        if df_navio.empty:
            print(f"ERRO: Navio '{ship_name}' não encontrado no DataFrame de eventos.")
            return pd.DataFrame()

        coords = df_navio.dropna(subset=['decLatitude', 'decLongitude'])
        if coords.empty:
            print(f"ERRO: Não há coordenadas válidas para o navio '{ship_name}'.")
            return pd.DataFrame()

        latitude = coords.iloc[0]['decLatitude']
        longitude = coords.iloc[0]['decLongitude']

        # Definição dos limites de data
        API_START_LIMIT = datetime(2022, 1, 1).date()
        API_MAX_CHUNK_DAYS = 365

        start_date_eventos = df_navio['startGMTDate'].min().date()
        end_date_eventos = df_navio['endGMTDate'].max().date()

        current_start = max(start_date_eventos, API_START_LIMIT)
        target_end = end_date_eventos

        if current_start > target_end:
            print("AVISO: Período anterior ao limite da API (2022-01-01).")
            return pd.DataFrame()

        print(f"  Endpoint: {api_url}")
        print(f"  Período da Requisição (Segmentado): De {current_start} até {target_end}")

        # 2. Loop de Requisição em Chunks
        all_clima_data = []

        while current_start <= target_end:
            chunk_end = min(current_start + timedelta(days=API_MAX_CHUNK_DAYS - 1), target_end)

            start_date_chunk = current_start.strftime('%Y-%m-%d')
            end_date_chunk = chunk_end.strftime('%Y-%m-%d')

            params = {
                'latitude': latitude,
                'longitude': longitude,
                'hourly': 'temperature_2m,apparent_temperature',
                'timezone': 'America/Sao_Paulo',
                'start_date': start_date_chunk,
                'end_date': end_date_chunk
            }

            full_url = f"{api_url}?{urlencode(params)}"
            print(f"  Buscando chunk: De {start_date_chunk} até {end_date_chunk}")

            try:
                response = requests.get(full_url, timeout=30)
                response.raise_for_status()
                data = response.json()

                if 'hourly' in data:
                    df_clima_chunk = pd.DataFrame(data['hourly'])
                    all_clima_data.append(df_clima_chunk)
                else:
                    print(f"AVISO: API sem dados para o chunk. Retorno: {data.get('reason', 'N/A')}")

            except requests.exceptions.RequestException as e:
                print(f"ERRO na API para o chunk {start_date_chunk} a {end_date_chunk}. Detalhe: {e}")

            current_start = chunk_end + timedelta(days=1)

        # 3. Consolidação e Formatação Final
        if not all_clima_data:
            return pd.DataFrame()

        df_final = pd.concat(all_clima_data, ignore_index=True)
        df_final.rename(columns={'time': 'DataHoraGMT'}, inplace=True)
        df_final['DataHoraGMT'] = pd.to_datetime(df_final['DataHoraGMT'])

        print(f"  Chamada API bem-sucedida. Total de {len(df_final)} registros horários obtidos.")
        return df_final

    def predizer_risco_bioincrustacao(self, df_clima: pd.DataFrame, lookback_days: int = 30) -> pd.DataFrame:
        """
        Métrica 7: Estrutura o Dataset de Treinamento (Features e Target)
        para predição de risco de bioincrustação.
        """
        if self.df_iws.empty or df_clima.empty:
            print("AVISO: Dados IWS ou climáticos estão vazios. Não é possível gerar o dataset de ML.")
            return pd.DataFrame()

        # Pré-processamento dos Dados Climáticos
        df_clima['DataHoraGMT'] = df_clima['DataHoraGMT'].dt.normalize()
        df_clima.rename(columns={'temperature_2m': 'Temperatura'}, inplace=True)

        # Agrupa a temperatura por dia
        df_clima_diario = df_clima.groupby(df_clima['DataHoraGMT'].dt.date)['Temperatura'].mean().reset_index()
        df_clima_diario.rename(columns={'DataHoraGMT': 'Data'}, inplace=True)

        df_final = []

        # Itera sobre cada Relatório IWS
        for _, relatorio in self.df_iws.iterrows():
            ship_name = relatorio['shipName']
            data_relatorio = relatorio['DataRelatorio'].date()

            data_inicio_lookback = data_relatorio - timedelta(days=lookback_days)

            # Filtra a temperatura média no período de lookback
            temp_lookback = df_clima_diario[
                (df_clima_diario['Data'] >= data_inicio_lookback) &
                (df_clima_diario['Data'] < data_relatorio)
                ]['Temperatura']

            if not temp_lookback.empty:
                temp_media_30d = temp_lookback.mean()
            else:
                continue

            # Feature: Dias desde o início dos dados (Placeholder simples)
            dias_desde_inicio = (data_relatorio - self.df_eventos['startGMTDate'].min().date()).days

            df_final.append({
                'shipName': ship_name,
                'DataRelatorio': data_relatorio,
                'TemperaturaMedia30D': round(temp_media_30d, 2),
                'DiasDesdeInicio': dias_desde_inicio,
                'RiscoBioincrustacao': relatorio['RiscoBioincrustacao']
            })

        return pd.DataFrame(df_final)

    def calcular_risco_bioincrustacao_regra_simples(self, ship_name: str,
                                                    df_conformidade: pd.DataFrame) -> pd.DataFrame:
        """
        Calcula o risco de bioincrustação (escala 1 a 5) por mês,
        baseado em regras simples de Latitude, Exposição em Porto e Conformidade do Revestimento.

        Args:
            ship_name: Nome do navio.
            df_conformidade: DataFrame resultante de calcular_conformidade_normam_401().

        Returns:
            DataFrame com as colunas 'Mês/Ano', 'Risco Total (1-5)'.
        """
        if self.df_eventos.empty:
            print("AVISO: DataFrame de eventos vazio.")
            return pd.DataFrame()

        # 1. Filtra eventos e limpa
        df_navio = self.df_eventos[self.df_eventos['shipName'].astype(str).str.strip() == ship_name.strip()].copy()
        df_navio.dropna(subset=['startGMTDate', 'decLatitude', 'eventName'], inplace=True)

        if df_navio.empty:
            print(f"AVISO: Navio '{ship_name}' não encontrado ou sem dados de localização/tempo válidos.")
            return pd.DataFrame()

        # 2. Engenharia de Features Mensais

        # Cria a coluna de Mês/Ano para agrupamento
        df_navio['Mes_Ano'] = df_navio['startGMTDate'].dt.to_period('M')
        df_navio['AbsLat'] = df_navio['decLatitude'].abs()

        # Tempo de exposição (em horas)
        df_navio['horas_em_porto'] = df_navio.apply(
            lambda row: row['duration'] if row['eventName'] == 'EM PORTO' else 0, axis=1
        )

        # Agrupamento Mensal
        df_mensal = df_navio.groupby('Mes_Ano').agg(
            MediaAbsLat=('AbsLat', 'mean'),
            TotalHorasPorto=('horas_em_porto', 'sum')
        ).reset_index()

        # Converte Horas para Dias
        df_mensal['TotalDiasPorto'] = df_mensal['TotalHorasPorto'] / 24

        # 3. Lógica de Risco (Regras Simples 1-5)
        # Tabela de Pontuação Máxima: 5.0

        # Risco Base (Temperatura/Latitude) - Max 2.5 pontos
        # Quanto menor a Latitude Absoluta, maior a pontuação (mais quente)
        df_mensal['R_base'] = 0.0
        df_mensal.loc[df_mensal['MediaAbsLat'] < 20, 'R_base'] = 2.5  # Trópicos/Águas Quentes
        df_mensal.loc[df_mensal['MediaAbsLat'] >= 20, 'R_base'] = 1.0  # Temperado/Águas Mais Frias

        # Risco de Exposição (Tempo Parado) - Max 1.5 pontos
        df_mensal['R_exp'] = 0.0
        df_mensal.loc[df_mensal['TotalDiasPorto'] > 10, 'R_exp'] = 1.5
        df_mensal.loc[(df_mensal['TotalDiasPorto'] > 3) & (df_mensal['TotalDiasPorto'] <= 10), 'R_exp'] = 0.5

        # 4. Integração do Risco de Revestimento (Conformidade NORMAM 401) - Max 1.0 ponto
        df_conform = df_conformidade[df_conformidade['shipName'].astype(str).str.strip() == ship_name.strip()].copy()
        df_conform['Mes_Ano'] = df_conform['Mês/Ano'].astype(str).str.strip().str.replace('-', '',
                                                                                          regex=False).str.replace('P',
                                                                                                                   '',
                                                                                                                   regex=False).apply(
            lambda x: pd.Period(x, freq='M'))

        # Merge do risco de coating (Conformidade)
        df_mensal = pd.merge(df_mensal, df_conform[['Mes_Ano', 'Conformidade (%)']], on='Mes_Ano', how='left')
        df_mensal['Conformidade (%)'].fillna(100.0, inplace=True)  # Assume 100% se não houver dados

        df_mensal['R_coat'] = 0.0
        df_mensal.loc[df_mensal['Conformidade (%)'] < 25, 'R_coat'] = 1.0  # Risco Alto (Coating falhando)
        df_mensal.loc[(df_mensal['Conformidade (%)'] >= 25) & (df_mensal['Conformidade (%)'] < 75), 'R_coat'] = 0.5

        # 5. Cálculo do Risco Final
        df_mensal['Risco Total'] = df_mensal['R_base'] + df_mensal['R_exp'] + df_mensal['R_coat']

        # Normalização para a escala 1-5 e arredondamento
        # Max Risco Teórico = 2.5 + 1.5 + 1.0 = 5.0
        # Min Risco Teórico = 1.0 + 0.0 + 0.0 = 1.0
        df_mensal['Risco Total (1-5)'] = df_mensal['Risco Total'].clip(lower=1.0, upper=5.0).round().astype(int)

        df_mensal['Mês/Ano'] = df_mensal['Mes_Ano'].astype(str)

        return df_mensal[['Mês/Ano', 'Risco Total (1-5)', 'MediaAbsLat', 'TotalDiasPorto', 'Conformidade (%)']]


# ==============================================================================
# EXECUÇÃO PRINCIPAL
# ==============================================================================

if __name__ == "__main__":
    # ⚠️ DEFINIÇÃO DOS NOMES DE ARQUIVO:
    # Use os nomes de arquivo COMPLETO que você tem no seu diretório.
    eventos_file = 'ResultadoQueryEventos.csv'
    consumo_file = 'ResultadoQueryConsumo.csv'
    revestimento_file = 'Dados navios Hackathon.xlsx - Especificacao revestimento.csv'
    iws_file = 'Relatorios IWS.xlsx - Planilha1.csv'

    SHIP_ALVO = 'DANIEL PEREIRA'  # Navio usado para exemplo de API/ML

    # Inicializa a classe e carrega todos os dados
    analytics = TranspetroAnalytics(eventos_file, consumo_file, revestimento_file, iws_file)

    print(f"\n[DEBUG] df_eventos tem {len(analytics.df_eventos)} linhas.")
    print(f"[DEBUG] df_revestimento tem {len(analytics.df_revestimento)} linhas.")
    print(f"[DEBUG] df_iws tem {len(analytics.df_iws)} linhas.")  # Verifica se o IWS foi carregado

    print("\n" + "=" * 70)
    print("## RESULTADOS DO PROJETO BASE TRANSPETRO ##")
    print("=" * 70)

    # 1, 2, 3. Métricas do Dashboard
    total_embarcacoes = analytics.calcular_total_embarcacoes()
    embarcacoes_operando_total = analytics.calcular_embarcacoes_operando()
    consumo_mensal = analytics.calcular_consumo_mensal_total()

    print(f"\n1. Total de Embarcações: {total_embarcacoes} navios")
    print(f"2. Total ÚNICO de Embarcações em NAVEGACAO: {embarcacoes_operando_total} navios")

    print("\n3. Consumo Total por Mês:")
    if not consumo_mensal.empty:
        print(consumo_mensal.to_markdown(index=False, numalign="left", stralign="left"))
    else:
        print("Nenhum dado de consumo pôde ser calculado.")

    # 4. Total de Embarcações Navegando por Dia
    embarcacoes_diarias = analytics.calcular_embarcacoes_navegando_por_dia()

    print("\n" + "=" * 70)
    print("4. Total de Embarcações Navegando por Dia (Primeiras 10 linhas):")
    if not embarcacoes_diarias.empty:
        print(embarcacoes_diarias.head(10).to_string(index=False))
    else:
        print("Nenhum dado de navegação diária encontrado.")

    # 5. Conformidade NORMAM 401
    # Captura o resultado para usar na Métrica 8 (Risco Simples)
    print("\n" + "=" * 70)
    print("5. Nível de Conformidade NORMAM 401 (Últimos 10 registros):")
    conformidade_normam = analytics.calcular_conformidade_normam_401()

    if not conformidade_normam.empty:
        print(conformidade_normam.tail(10).to_string(index=False))
    else:
        print("Nenhum dado de conformidade NORMAM 401 pôde ser calculado. (Verifique df_revestimento)")

    # 6. Busca de Dados Climáticos (API)
    print("\n" + "=" * 70)
    print(f"6. BUSCA DE DADOS CLIMÁTICOS HISTÓRICOS PARA O NAVIO {SHIP_ALVO}")
    print("=" * 70)

    df_clima = analytics.obter_dados_climaticos_navio(SHIP_ALVO)

    if not df_clima.empty:
        print("\nDados Climáticos Obtidos (Primeiras 5 linhas):")
        print(df_clima.head().to_string(index=False))
    else:
        print(f"Não foi possível obter dados climáticos para o navio {SHIP_ALVO}.")

    # 7. Estruturação do Dataset para Predição de Bioincrustação
    print("\n" + "=" * 70)
    print("7. ESTRUTURAÇÃO DO DATASET PARA PREDIÇÃO DE BIOINCRUSTAÇÃO (ML)")
    print("=" * 70)

    # Esta função depende de df_iws e df_clima
    df_treinamento = analytics.predizer_risco_bioincrustacao(df_clima, lookback_days=30)

    if not df_treinamento.empty:
        print(f"Dataset de Treinamento (X e Y) Gerado com {len(df_treinamento)} observações:")
        print("Features: TemperaturaMedia30D, DiasDesdeInicio | Target: RiscoBioincrustacao")
        print(df_treinamento.head().to_string(index=False))
    else:
        print(
            "Dataset de Treinamento não gerado. Verifique se há sobreposição de datas e dados válidos entre IWS e Clima.")

    print("\n" + "#" * 70)
    print("## FIM DA EXECUÇÃO DO PROJETO BASE ##")
    print("#" * 70)

    # 8. NOVO: Cálculo de Risco Simples (Regra de Negócio)
    print("\n" + "=" * 70)
    print(f"8. RISCO DE BIOINCRUSTAÇÃO (ESCALA 1-5) PARA {SHIP_ALVO} (Regra Simples)")
    print("=" * 70)

    risco_bioincrustacao = analytics.calcular_risco_bioincrustacao_regra_simples(SHIP_ALVO, conformidade_normam)

    if not risco_bioincrustacao.empty:
        print("Risco de Incrustação Mensal (1=Pouco, 5=Muita Incrustação):")
        print(risco_bioincrustacao.tail(10).to_string(index=False))

        # Visualização das métricas de risco que contribuíram
        print("\nComponentes de Risco (Últimos 5 meses):")
        print(risco_bioincrustacao[
                  ['Mês/Ano', 'MediaAbsLat', 'TotalDiasPorto', 'Conformidade (%)', 'Risco Total (1-5)']].tail(
            5).to_markdown(index=False, numalign="left", stralign="left"))
    else:
        print(f"Não foi possível calcular o risco para o navio {SHIP_ALVO}.")

    print("\n" + "#" * 70)
    print("## FIM DA EXECUÇÃO DO PROJETO BASE ##")
    print("#" * 70)