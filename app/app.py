import pandas as pd
import os
from typing import Optional, Dict, Any


class TranspetroAnalytics:
    """
    Assistente de Programação para o Hackathon Brasil / Transpetro.
    Classe focada na análise de dados de eventos e consumo de embarcações,
    atendendo aos requisitos do dashboard.
    """

    def __init__(self, eventos_path: str, consumo_path: str, revestimento_path: str):
        """
        Inicializa a classe e carrega os DataFrames.

        Args:
            eventos_path: Caminho para ResultadoQueryEventos.csv.
            consumo_path: Caminho para ResultadoQueryConsumo.csv.
            revestimento_path: Caminho para Especificacao revestimento.csv.
        """
        print("Carregando e pré-processando dados...")
        self.df_eventos = self._carregar_eventos(eventos_path)
        self.df_consumo = self._carregar_consumo(consumo_path)
        self.df_revestimento = self._carregar_revestimento(revestimento_path)  # NOVO
        self.df_consolidado = self._consolidar_dados()
        print("Dados prontos para análise.")

    # ... [Mantenha _carregar_eventos e _carregar_consumo inalterados] ...

    def _carregar_revestimento(self, path: str) -> pd.DataFrame:
        """
        Carrega e pré-processa o DataFrame de Especificacao revestimento,
        usando lógica robusta para múltiplos separadores e codificações.
        """
        if not os.path.exists(path):
            print(f"ERRO: Arquivo de revestimento não encontrado em: {path}")
            return pd.DataFrame()

        print(f"  -> Tentando carregar revestimento de: {path}")
        df = pd.DataFrame()

        # Combina separadores e codificações mais comuns
        carregamento_options = [
            {'sep': ',', 'encoding': 'utf-8'},
            {'sep': ';', 'encoding': 'utf-8'},
            {'sep': ';', 'encoding': 'latin1'},  # Adiciona latin1 (comum em CSVs BR/PT)
            {'sep': ',', 'encoding': 'latin1'}
        ]

        for options in carregamento_options:
            try:
                df_temp = pd.read_csv(path, **options)
                # Verifica se a leitura resultou em mais de uma coluna e tem dados
                if len(df_temp.columns) > 1 and not df_temp.empty:
                    df = df_temp
                    print(
                        f"  -> Carregado com sucesso usando sep='{options['sep']}' e encoding='{options['encoding']}'")
                    break
            except Exception:
                # Silencia o erro para tentar a próxima opção
                pass

        if df.empty:
            print(
                "ERRO fatal ao carregar o arquivo de revestimento: Falha ao carregar com todas as opções. Verifique o separador ou codificação.")
            return pd.DataFrame()

        # Renomear colunas
        df.rename(columns={
            'Nome do navio': 'shipName',
            'Data da aplicacao': 'DataAplicacao',
            'Cr1. Período base de verificação': 'T_base',
            'Cr1. Parada máxima acumulada no período': 'T_max'
        }, inplace=True)

        # Conversão de tipos. dayfirst=True é mantido para datas no formato D-M-A
        df['DataAplicacao'] = pd.to_datetime(df['DataAplicacao'], errors='coerce', dayfirst=True)
        df['T_base'] = pd.to_numeric(df['T_base'], errors='coerce')
        df['T_max'] = pd.to_numeric(df['T_max'], errors='coerce')

        # Limpeza de dados inválidos para o cálculo
        df.dropna(subset=['DataAplicacao', 'T_base', 'T_max'], inplace=True)

        print(f"  -> Linhas de dados de revestimento válidas carregadas: {len(df)}")
        return df

    def _carregar_eventos(self, path: str) -> pd.DataFrame:
        """Carrega e pré-processa o DataFrame de eventos."""
        if not os.path.exists(path):
            print(f"ERRO: Arquivo de eventos não encontrado em: {path}")
            return pd.DataFrame()

        print(f"  -> Carregando eventos de: {path}")
        try:
            df = pd.read_csv(path)
            # Conversão de tipos de colunas de data (crucial para análise temporal)
            df['startGMTDate'] = pd.to_datetime(df['startGMTDate'], errors='coerce')
            df['endGMTDate'] = pd.to_datetime(df['endGMTDate'], errors='coerce')
            return df
        except Exception as e:
            print(f"ERRO ao carregar ou processar o arquivo de eventos: {e}")
            return pd.DataFrame()

    def _carregar_consumo(self, path: str) -> pd.DataFrame:
        """Carrega e pré-processa o DataFrame de consumo."""
        if not os.path.exists(path):
            print(f"ERRO: Arquivo de consumo não encontrado em: {path}")
            return pd.DataFrame()

        print(f"  -> Carregando consumo de: {path}")
        try:
            df = pd.read_csv(path)
            # Renomear colunas
            df.rename(columns={'SESSION_ID': 'sessionId', 'CONSUMED_QUANTITY': 'consumedQuantity'}, inplace=True)
            # Limpar e converter a coluna de consumo para numérico
            df['consumedQuantity'] = pd.to_numeric(df['consumedQuantity'], errors='coerce').fillna(0)
            return df
        except Exception as e:
            print(f"ERRO ao carregar ou processar o arquivo de consumo: {e}")
            return pd.DataFrame()

    def _consolidar_dados(self) -> pd.DataFrame:
        """Realiza a união (merge) dos dados de eventos e consumo pelo sessionId."""
        if self.df_eventos.empty or self.df_consumo.empty:
            return pd.DataFrame()

        # Para o merge, pegamos as informações essenciais do evento (shipName e data)
        # e as combinamos com o consumo.
        df_eventos_limpo = self.df_eventos[['sessionId', 'shipName', 'startGMTDate', 'eventName']].drop_duplicates(
            subset=['sessionId'])

        df = pd.merge(
            df_eventos_limpo,
            self.df_consumo[['sessionId', 'consumedQuantity']],
            on='sessionId',
            how='inner'  # Apenas sessões que têm tanto evento quanto consumo
        )
        return df

    # --- Métodos de Cálculo para o Dashboard ---

    def calcular_total_embarcacoes(self) -> int:
        """
        Métrica 1: Calcula o número total de embarcações únicas.
        """
        if self.df_eventos.empty:
            return 0
        return self.df_eventos['shipName'].nunique()

    def calcular_embarcacoes_operando(self) -> int:
        """
        Métrica 2: Calcula o número de embarcações que possuem registros de NAVEGACAO.
        """
        if self.df_eventos.empty:
            return 0

        # Filtra os eventos que são de NAVEGACAO
        df_operando = self.df_eventos[self.df_eventos['eventName'] == 'NAVEGACAO']

        # Conta os nomes de navios únicos envolvidos
        return df_operando['shipName'].nunique()

    def calcular_consumo_mensal_total(self) -> pd.DataFrame:
        """
        Métrica 3: Calcula o consumo total de combustível por mês e ano.
        Usa a data de início (startGMTDate) da sessão como referência temporal.
        """
        if self.df_consolidado.empty:
            return pd.DataFrame({'Mês/Ano': [], 'Consumo Total (unidade)': []})

        df = self.df_consolidado.copy()

        # 1. Cria uma coluna de Mês/Ano (período)
        df['Mes_Ano'] = df['startGMTDate'].dt.to_period('M')

        # 2. Agrupa por Mês/Ano e soma o consumo
        consumo_mensal = df.groupby('Mes_Ano')['consumedQuantity'].sum().reset_index()

        # 3. Formata o resultado
        consumo_mensal['Mês/Ano'] = consumo_mensal['Mes_Ano'].astype(str)
        consumo_mensal.rename(columns={'consumedQuantity': 'Consumo Total (unidade)'}, inplace=True)

        return consumo_mensal[['Mês/Ano', 'Consumo Total (unidade)']].sort_values(by='Mês/Ano')

    def calcular_embarcacoes_navegando_por_dia(self) -> pd.DataFrame:
        """
        Calcula o número total de embarcações únicas que estavam em evento 'NAVEGACAO'
        para cada dia do período.

        Retorna um DataFrame com as colunas 'Data' e 'Embarcações Navegando'.
        """
        if self.df_eventos.empty:
            return pd.DataFrame({'Data': [], 'Embarcações Navegando': []})

        # 1. Filtra apenas os eventos de NAVEGACAO
        df_nav = self.df_eventos[self.df_eventos['eventName'] == 'NAVEGACAO'].copy()

        # 2. Simplifica as datas para o formato de dia
        df_nav['start_date'] = df_nav['startGMTDate'].dt.normalize()
        # Adicionamos 1 dia ao final para garantir que o dia de término seja incluído no range,
        # pois pd.date_range é inclusivo no início e fim.
        df_nav['end_date'] = (df_nav['endGMTDate'].dt.normalize() + pd.Timedelta(days=1))

        # 3. Cria uma série de datas por evento (Explosão da série temporal)
        date_ranges = []
        for index, row in df_nav.iterrows():
            # Gera todas as datas entre o início e o fim do evento
            dates = pd.date_range(start=row['start_date'], end=row['end_date'], freq='D', closed='left').normalize()
            # Adiciona uma tupla (shipName, date) para cada dia de navegação
            date_ranges.extend([(row['shipName'], date) for date in dates])

        # 4. Converte para DataFrame
        df_daily_nav = pd.DataFrame(date_ranges, columns=['shipName', 'Data'])

        if df_daily_nav.empty:
            return pd.DataFrame({'Data': [], 'Embarcações Navegando': []})

        # 5. Agrupa por Data e conta o número de navios únicos
        df_resultado = df_daily_nav.groupby('Data')['shipName'].nunique().reset_index()

        # 6. Formata o resultado
        df_resultado.rename(columns={'shipName': 'Embarcações Navegando'}, inplace=True)

        return df_resultado.sort_values(by='Data').reset_index(drop=True)

    def calcular_embarcacoes_navegando_por_dia(self) -> pd.DataFrame:
        """
        Calcula o número total de embarcações únicas que estavam em evento 'NAVEGACAO'
        para cada dia do período.

        Retorna um DataFrame com as colunas 'Data' e 'Embarcações Navegando'.
        """
        if self.df_eventos.empty:
            return pd.DataFrame({'Data': [], 'Embarcações Navegando': []})

        # 1. Filtra apenas os eventos de NAVEGACAO
        df_nav = self.df_eventos[self.df_eventos['eventName'] == 'NAVEGACAO'].copy()

        # Remove eventos com datas nulas ou inválidas
        df_nav.dropna(subset=['startGMTDate', 'endGMTDate'], inplace=True)

        # 2. Normaliza as datas para o formato de dia (timestamp 00:00:00)
        df_nav['start_date'] = df_nav['startGMTDate'].dt.normalize()
        df_nav['end_date'] = df_nav['endGMTDate'].dt.normalize()

        # 3. Cria uma série de datas por evento (Explosão da série temporal)
        date_ranges = []
        for index, row in df_nav.iterrows():
            # Gera todas as datas entre o início e o fim do evento, incluindo ambos (padrão do date_range)
            # Isto resolve o erro 'closed'
            dates = pd.date_range(start=row['start_date'], end=row['end_date'], freq='D')

            # Adiciona uma tupla (shipName, date) para cada dia de navegação
            date_ranges.extend([(row['shipName'], date) for date in dates])

        # 4. Converte a lista de tuplas para DataFrame
        df_daily_nav = pd.DataFrame(date_ranges, columns=['shipName', 'Data'])

        if df_daily_nav.empty:
            return pd.DataFrame({'Data': [], 'Embarcações Navegando': []})

        # 5. Agrupa por Data e conta o número de navios únicos
        df_resultado = df_daily_nav.groupby('Data')['shipName'].nunique().reset_index()

        # 6. Formata o resultado
        df_resultado.rename(columns={'shipName': 'Embarcações Navegando'}, inplace=True)

        return df_resultado[['Data', 'Embarcações Navegando']].sort_values(by='Data').reset_index(drop=True)

    def calcular_conformidade_normam_401(self) -> pd.DataFrame:
        """
        Calcula o nível de conformidade do revestimento NORMAM 401 para cada navio por mês,
        com base nos períodos máximos permitidos.
        """
        if self.df_revestimento.empty or self.df_eventos.empty:
            print("Dados de revestimento ou eventos vazios. Não é possível calcular a conformidade.")
            return pd.DataFrame({'Mês/Ano': [], 'shipName': [], 'Conformidade (%)': []})

        df_r = self.df_revestimento.copy()

        # 1. Determina o período de análise
        min_date = df_r['DataAplicacao'].min().to_period('M')
        max_date = pd.to_datetime('today').to_period('M')

        # Cria a série temporal de todos os meses entre o início e o fim da análise
        meses = pd.period_range(start=min_date, end=max_date, freq='M')

        # Lista de navios únicos na base
        navios_unicos = df_r['shipName'].unique()

        resultados = []

        # 2. Itera sobre cada navio e cada mês
        for ship in navios_unicos:
            df_ship = df_r[df_r['shipName'] == ship].sort_values('DataAplicacao')

            # Inicializa a última data de aplicação válida
            last_app_date = pd.NaT
            T_base_current = 0
            T_max_current = 0

            for mes in meses:
                data_fim_mes = mes.to_timestamp(how='end')

                # Atualiza os parâmetros se houver uma nova aplicação neste mês ou anterior
                # Pega a última aplicação que ocorreu *antes ou no* mês atual
                aplicacao_recente = df_ship[df_ship['DataAplicacao'] <= data_fim_mes]

                if not aplicacao_recente.empty:
                    ultima_app = aplicacao_recente.iloc[-1]

                    # Se a última aplicação for mais recente que a que estamos rastreando
                    if ultima_app['DataAplicacao'] > last_app_date:
                        last_app_date = ultima_app['DataAplicacao']
                        T_base_current = ultima_app['T_base']
                        T_max_current = ultima_app['T_max']

                # Se não há uma data de aplicação válida para o navio até este mês, a conformidade é 0
                if pd.isna(last_app_date):
                    conformidade = 0.0
                else:
                    # Calcula o tempo decorrido (em meses)
                    T_passado_dias = (data_fim_mes - last_app_date).days
                    T_passado_meses = T_passado_dias / 30.437  # Média de dias por mês

                    # Calcula a Conformidade Proporcional (%)
                    # Quanto da vida útil total (T_base e T_max) foi consumido
                    consumo_base = T_passado_meses / T_base_current
                    consumo_max = T_passado_meses / T_max_current

                    # 1 - o máximo consumo (o mais restritivo)
                    conformidade = 1.0 - max(consumo_base, consumo_max)

                    # Garante que o valor esteja entre 0 e 1 (0% a 100%)
                    conformidade = max(0.0, conformidade) * 100.0

                resultados.append({
                    'Mês/Ano': str(mes),
                    'shipName': ship,
                    'Conformidade (%)': round(conformidade, 2)
                })

        return pd.DataFrame(resultados)


# --- Execução Principal do Script ---
if __name__ == "__main__":
    # Nomes dos arquivos de dados (ajuste se for diferente)
    eventos_file = 'ResultadoQueryEventos.csv'
    consumo_file = 'ResultadoQueryConsumo.csv'
    revestimento_file = 'Dados navios Hackathon.xlsx - Especificacao revestimento.csv'  # NOVO ARQUIVO

    # Inicializa a classe e carrega os dados
    analytics = TranspetroAnalytics(eventos_file, consumo_file, revestimento_file)
    print("\n" + "=" * 70)
    print("## RESULTADOS DO DASHBOARD DE NAVEGAÇÃO E CONSUMO - TRANSPETRO ##")
    print("=" * 70)

    # 1. Total de Embarcações
    total_embarcacoes = analytics.calcular_total_embarcacoes()
    print(f"\n1. Total de Embarcações na base de dados: {total_embarcacoes} navios")

    # 2. Total de Embarcações Operando
    embarcacoes_operando = analytics.calcular_embarcacoes_operando()
    print(f"2. Total de Embarcações que registraram NAVEGACAO: {embarcacoes_operando} navios")

    # 3. Consumo Total por Mês
    consumo_mensal = analytics.calcular_consumo_mensal_total()

    print("\n3. Combustível Total Consumido por Mês:")
    if not consumo_mensal.empty:
        # Imprime o resultado formatado em tabela
        print(consumo_mensal.to_markdown(index=False, numalign="left", stralign="left"))
    else:
        print("Nenhum dado de consumo pôde ser calculado.")

    print("\n" + "=" * 70)

    # 4. Embarcações Navegando por Dia
    embarcacoes_diarias = analytics.calcular_embarcacoes_navegando_por_dia()

    print("\n" + "=" * 70)
    print("4. Total de Embarcações Navegando por Dia:")

    if not embarcacoes_diarias.empty:
        print("\nPrimeiras linhas da tabela de navegação diária:")
        # Uso de .to_string() para evitar a dependência 'tabulate' se você não a quiser instalar,
        # ou mantenha to_markdown() se já tiver instalado 'tabulate'
        print(embarcacoes_diarias.head().to_string(index=False))
    else:
        print("Nenhum dado de navegação diária encontrado.")

    print("=" * 70)

    # 5. Conformidade NORMAM 401 por Navio e Mês
    conformidade_normam = analytics.calcular_conformidade_normam_401()

    print("\n" + "=" * 70)
    print("5. Nível de Conformidade NORMAM 401 por Navio e Mês (Métrica Proporcional):")

    if not conformidade_normam.empty:
        # Mostra os 10 registros mais recentes para visualização
        print("\nÚltimos 10 registros da tabela de conformidade:")
        print(conformidade_normam.tail(10).to_string(index=False))
    else:
        print("Nenhum dado de conformidade NORMAM 401 pôde ser calculado.")

    print("=" * 70)