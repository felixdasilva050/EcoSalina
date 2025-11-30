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
    Classe de análise de dados para o Hackathon Transpetro.
    Contém todos os métodos de carregamento e cálculo de métricas.
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
        print("Iniciando o carregamento e pré-processamento dos dados...")

        self.df_eventos = self._carregar_eventos(eventos_path)
        self.df_consumo = self._carregar_consumo(consumo_path)

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
            return pd.DataFrame()

        df = pd.DataFrame()
        carregamento_options = [
            {'sep': ',', 'encoding': 'utf-8'},
            {'sep': ';', 'encoding': 'utf-8'},
            {'sep': ';', 'encoding': 'latin1'},
            {'sep': ',', 'encoding': 'latin1'}
        ]

        for options in carregamento_options:
            try:
                df_temp = pd.read_csv(path, **options)

                df_temp.columns = df_temp.columns.str.strip().str.replace('"', '', regex=False)

                if all(col in df_temp.columns for col in required_cols_map.keys()):
                    df = df_temp
                    break
            except Exception:
                pass

        if df.empty:
            return pd.DataFrame()

        df.rename(columns=required_cols_map, inplace=True)
        if 'shipName' in df.columns:
            df['shipName'] = df['shipName'].astype(str).str.strip()

        return df

    def _carregar_eventos(self, path: str) -> pd.DataFrame:
        try:
            df = pd.read_csv(path)
            df.columns = df.columns.str.strip().str.replace('"', '', regex=False)
            df.rename(columns={'startGMTDate': 'startGMTDate', 'endGMTDate': 'endGMTDate'}, inplace=True)
            df['startGMTDate'] = pd.to_datetime(df['startGMTDate'], errors='coerce')
            df['endGMTDate'] = pd.to_datetime(df['endGMTDate'], errors='coerce')
            return df
        except Exception:
            return pd.DataFrame()

    def _carregar_consumo(self, path: str) -> pd.DataFrame:
        try:
            df = pd.read_csv(path)
            df.rename(columns={'SESSION_ID': 'sessionId', 'CONSUMED_QUANTITY': 'consumedQuantity'}, inplace=True)
            df.columns = df.columns.str.strip().str.replace('"', '', regex=False)
            df['consumedQuantity'] = pd.to_numeric(df['consumedQuantity'], errors='coerce').fillna(0)
            return df
        except Exception:
            return pd.DataFrame()

    def _carregar_revestimento(self, path: str) -> pd.DataFrame:
        df = self._carregar_csv_robusto(path, self.REVESTIMENTO_COLS)
        if df.empty:
            return pd.DataFrame()

        df['DataAplicacao'] = pd.to_datetime(df['DataAplicacao'], errors='coerce', dayfirst=True)
        df['T_base'] = pd.to_numeric(df['T_base'], errors='coerce')
        df['T_max'] = pd.to_numeric(df['T_max'], errors='coerce')
        df.dropna(subset=['DataAplicacao', 'T_base', 'T_max'], inplace=True)
        print(f"  -> Linhas de revestimento válidas carregadas: {len(df)}")
        return df

    def _carregar_relatorios_iws(self, path: str) -> pd.DataFrame:
        df = self._carregar_csv_robusto(path, self.IWS_COLS)

        if df.empty:
            return pd.DataFrame()

        df['DataRelatorio'] = pd.to_datetime(df['DataRelatorio'], errors='coerce', dayfirst=True)
        df.dropna(subset=['shipName', 'DataRelatorio'], inplace=True)

        if 'TipoIncrustacao' in df.columns:
            df['RiscoBioincrustacao'] = 0
            df.loc[df['TipoIncrustacao'].astype(str).str.contains(r'(Duras|Craca|calcárea)', case=False,
                                                                  na=False), 'RiscoBioincrustacao'] = 1

            print(f"  -> Linhas de relatórios IWS válidas carregadas: {len(df)}")
            return df[['shipName', 'DataRelatorio', 'RiscoBioincrustacao']].copy()
        else:
            return pd.DataFrame()

    def _consolidar_dados(self) -> pd.DataFrame:
        if self.df_eventos.empty or self.df_consumo.empty:
            return pd.DataFrame()

        df_eventos_limpo = self.df_eventos[['sessionId', 'shipName', 'startGMTDate', 'eventName']].drop_duplicates(
            subset=['sessionId'])

        df = pd.merge(
            df_eventos_limpo,
            self.df_consumo[['sessionId', 'consumedQuantity']],
            on='sessionId',
            how='inner'
        )
        return df

    # --- MÉTODOS DO DASHBOARD (MÉTRICAS 1 a 4) ---

    def calcular_total_embarcacoes(self) -> int:
        if self.df_eventos.empty:
            return 0
        return self.df_eventos['shipName'].nunique()

    def calcular_embarcacoes_operando(self) -> int:
        if self.df_eventos.empty:
            return 0
        df_operando = self.df_eventos[self.df_eventos['eventName'] == 'NAVEGACAO']
        return df_operando['shipName'].nunique()

    def calcular_consumo_mensal_total(self) -> pd.DataFrame:
        if self.df_consolidado.empty:
            return pd.DataFrame({'Mês/Ano': [], 'Consumo Total (unidade)': []})

        df = self.df_consolidado.copy()
        df['Mes_Ano'] = df['startGMTDate'].dt.to_period('M')

        consumo_mensal = df.groupby('Mes_Ano')['consumedQuantity'].sum().reset_index()

        consumo_mensal['Mês/Ano'] = consumo_mensal['Mes_Ano'].astype(str)
        consumo_mensal.rename(columns={'consumedQuantity': 'Consumo Total (unidade)'}, inplace=True)

        return consumo_mensal[['Mês/Ano', 'Consumo Total (unidade)']].sort_values(by='Mês/Ano').reset_index(drop=True)

    def calcular_embarcacoes_navegando_por_dia(self) -> pd.DataFrame:
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

    # --- ANÁLISE DE CONFORMIDADE (MÉTRICA 5) ---

    def calcular_conformidade_normam_401(self) -> pd.DataFrame:
        if self.df_revestimento.empty or self.df_eventos.empty:
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

    # --- INTEGRAÇÃO API (MÉTRICA 6) ---

    def obter_dados_climaticos_navio(self, ship_name: str,
                                     api_url: str = "https://archive-api.open-meteo.com/v1/era5") -> pd.DataFrame:
        """Métrica 6: Busca dados climáticos históricos (ERA5) em chunks de 1 ano."""
        if self.df_eventos.empty:
            return pd.DataFrame()

        df_navio = self.df_eventos[self.df_eventos['shipName'].astype(str).str.strip() == ship_name.strip()].copy()

        if df_navio.empty:
            return pd.DataFrame()

        coords = df_navio.dropna(subset=['decLatitude', 'decLongitude'])
        if coords.empty:
            return pd.DataFrame()

        latitude = coords.iloc[0]['decLatitude']
        longitude = coords.iloc[0]['decLongitude']

        API_START_LIMIT = datetime(2022, 1, 1).date()
        API_MAX_CHUNK_DAYS = 365

        start_date_eventos = df_navio['startGMTDate'].min().date()
        end_date_eventos = df_navio['endGMTDate'].max().date()

        current_start = max(start_date_eventos, API_START_LIMIT)
        target_end = end_date_eventos

        if current_start > target_end:
            return pd.DataFrame()

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

            try:
                response = requests.get(full_url, timeout=30)
                response.raise_for_status()
                data = response.json()

                if 'hourly' in data:
                    df_clima_chunk = pd.DataFrame(data['hourly'])
                    all_clima_data.append(df_clima_chunk)

            except requests.exceptions.RequestException:
                pass

            current_start = chunk_end + timedelta(days=1)

        if not all_clima_data:
            return pd.DataFrame()

        df_final = pd.concat(all_clima_data, ignore_index=True)
        df_final.rename(columns={'time': 'DataHoraGMT'}, inplace=True)
        df_final['DataHoraGMT'] = pd.to_datetime(df_final['DataHoraGMT'])

        return df_final

    # --- PREDIÇÃO/RISCO (MÉTRICA 8) ---

    def calcular_risco_bioincrustacao_frota(self, df_conformidade: pd.DataFrame) -> pd.DataFrame:
        """
        Métrica 8: Calcula o risco de bioincrustação (escala 1 a 5) por mês,
        para TODA A FROTA, baseado em regras simples de Latitude, Exposição e Conformidade.
        """
        if self.df_eventos.empty:
            return pd.DataFrame()

        navios_unicos = self.df_eventos['shipName'].astype(str).str.strip().unique()
        resultados_frota = []

        for ship_name in navios_unicos:
            df_navio = self.df_eventos[self.df_eventos['shipName'].astype(str).str.strip() == ship_name].copy()
            df_navio.dropna(subset=['startGMTDate', 'decLatitude', 'eventName'], inplace=True)

            if df_navio.empty:
                continue

            # 2. Engenharia de Features Mensais
            df_navio['Mes_Ano'] = df_navio['startGMTDate'].dt.to_period('M')
            df_navio['AbsLat'] = df_navio['decLatitude'].abs()

            df_navio['horas_em_porto'] = df_navio.apply(
                lambda row: row['duration'] if row['eventName'] == 'EM PORTO' else 0, axis=1
            )

            df_mensal = df_navio.groupby('Mes_Ano').agg(
                MediaAbsLat=('AbsLat', 'mean'),
                TotalHorasPorto=('horas_em_porto', 'sum')
            ).reset_index()

            df_mensal['shipName'] = ship_name
            df_mensal['TotalDiasPorto'] = df_mensal['TotalHorasPorto'] / 24

            # 3. Lógica de Risco Base (Latitude) - Max 2.5 pontos
            df_mensal['R_base'] = 0.0
            df_mensal.loc[df_mensal['MediaAbsLat'] < 20, 'R_base'] = 2.5
            df_mensal.loc[df_mensal['MediaAbsLat'] >= 20, 'R_base'] = 1.0

            # Risco de Exposição (Tempo Parado) - Max 1.5 pontos
            df_mensal['R_exp'] = 0.0
            df_mensal.loc[df_mensal['TotalDiasPorto'] > 10, 'R_exp'] = 1.5
            df_mensal.loc[(df_mensal['TotalDiasPorto'] > 3) & (df_mensal['TotalDiasPorto'] <= 10), 'R_exp'] = 0.5

            # 4. Integração do Risco de Revestimento (Conformidade NORMAM 401) - Max 1.0 ponto
            df_conform_ship = df_conformidade[df_conformidade['shipName'].astype(str).str.strip() == ship_name].copy()

            if not df_conform_ship.empty:
                df_conform_ship['Mes_Ano'] = df_conform_ship['Mês/Ano'].astype(str).str.strip().str.replace('-', '',
                                                                                                            regex=False).str.replace(
                    'P', '', regex=False).apply(lambda x: pd.Period(x, freq='M'))
                df_mensal = pd.merge(df_mensal, df_conform_ship[['Mes_Ano', 'Conformidade (%)']], on='Mes_Ano',
                                     how='left')
            else:
                df_mensal['Conformidade (%)'] = 100.0

            df_mensal['Conformidade (%)'].fillna(100.0, inplace=True)

            df_mensal['R_coat'] = 0.0
            df_mensal.loc[df_mensal['Conformidade (%)'] < 25, 'R_coat'] = 1.0
            df_mensal.loc[(df_mensal['Conformidade (%)'] >= 25) & (df_mensal['Conformidade (%)'] < 75), 'R_coat'] = 0.5

            # 5. Cálculo do Risco Final
            df_mensal['Risco Total'] = df_mensal['R_base'] + df_mensal['R_exp'] + df_mensal['R_coat']
            df_mensal['Risco Total (1-5)'] = df_mensal['Risco Total'].clip(lower=1.0, upper=5.0).round().astype(int)
            df_mensal['Mês/Ano'] = df_mensal['Mes_Ano'].astype(str)

            resultados_frota.append(df_mensal[['shipName', 'Mês/Ano', 'Risco Total (1-5)']])

        if not resultados_frota:
            return pd.DataFrame({'shipName': [], 'Mês/Ano': [], 'Risco Total (1-5)': []})

        return pd.concat(resultados_frota, ignore_index=True)

    def predizer_risco_bioincrustacao(self, df_clima: pd.DataFrame, lookback_days: int = 30) -> pd.DataFrame:
        """
        Métrica 7: Estrutura o Dataset de Treinamento (Mantida vazia, pois não é utilizada na Métrica 8).
        """
        # Apenas um placeholder, pois a Métrica 8 (Regra Simples) foi a escolhida para a API.
        return pd.DataFrame()