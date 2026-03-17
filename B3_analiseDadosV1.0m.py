import marimo

__generated_with = "0.20.4"
app = marimo.App(width="medium")


@app.cell
def _():
    # Análise exploratória de dados de arquivos disponíveis na B3
    # exploratory data analysis of available files from B3 data
    # data wrangling with B3 data, and some functions of yfinance
    # teste para tratamento e leitura de dados da B3 e algumas funções do modulo yfinance
    # e outras rotinas e funções para testar funcionalidades de ferramentas(bibliotecas) e gráficos

    import os
    import glob
    import zipfile

    import pandas as pd
    import pyarrow

    # tirei algumas celulas com yfinance, logo, sem uso aqui, mas deixei os imports caso queira usar depois
    # import yfinance as yf
    # import openpyxl
    # import urllib3

    # resolvendo problemas dentro do VS code, conflito com widgets/CDN
    # deixei default como "browser" para evitar problemas
    # mas, no código, acabei mudando tudo para 'vscode'
    #
    import plotly.io as pio
    pio.renderers.default = "browser"  # evita renderer de widget/CDN no VS Code
    # pio.renderers.default = "vscode"

    import matplotlib.pyplot as plt
    import statsmodels.api as sm
    import plotly.graph_objects as go
    import plotly.express as px
    from plotly.subplots import make_subplots

    import time
    from datetime import datetime, timezone
    # Define timezone
    tz = timezone.utc
    return (
        datetime,
        glob,
        go,
        make_subplots,
        os,
        pd,
        px,
        pyarrow,
        time,
        tz,
        zipfile,
    )


@app.cell
def _():
    # registro do 'log' das operações na B3 
    # de acordo com doc. da B3: 'SeriesHistoricas_Layout.pdf', versão 2.0, data 05/10/2000
    # obs: rodapé do doc. indica data 13/04/2017, na pag.2 indica histórico da atualização do doc. 05/10/2020

    """
    	TIPREG:  formato N(03), típo de registro, valor fixo = 01,  
    	DATAREG: formato N(08) 'AAAAMMDD', data da transação,
    	CODBDI: formato X(02), classificação dos papeis do Boletim Diário BDI,
    	CODNEG: formato X(12), código de negociação do papel,
    	TPMERC: formato N(03), tipo de mercado,
    	NONRES: formato X(12), nome resumido da empresa emissora do papel,
    	ESPECI: formato X(10), especificação do papel, 
    	PRAZOT: formato X(03), prazo em dias do mercado a termo, 
    	MODREF: formato X(04), moeda de referência, normalmente “R$”, 
    	PREABE: formato (11)V99, preço de abertura do papel, 
    	PREMAX: formato (11)V99, preço máximo do papel, 
    	PREMIN: formato (11)V99, preço mínimo do papel, 
    	PREMED: formato (11)V99, preço médio do papel,
    	PREULT: formato (11)V99, preço do último negócio do papel,
    	PREOFC: formato (11)V99, preço da melhor oferta de compra do papel,
    	PREOFV: formato (11)V99, preço da melhor oferta de venda do papel,
    	TOTNEG: formato N(05), número de negócios efetuados com o papel,
    	QUATOT: formato N(18), quantidade total de títulos negociados neste papel,
    	VOLTOT: formato (16)V99, volume total de títulos negociados neste papel,
    	PREEXE: formato (11)V99, preço de exercício para o m. de opções ou valor do contrato para o m. de termo secundário,
    	INDOPC: formato N(01), indicador de correção de preços de exercícios ou valores de contrato para os m. de opções ou termo secundário,
    	DATVEN: formato N(08) 'AAAAMMDD', Data do vencimento para os mercados de opções ou termo secundário,
    	FATCOT: formato N(07), fator de cotação do papel, 1 para cotação unitária, 1000 para lotes de mil ações,
    	PTOEXE: formato (07)V06, Preço de exercício em pontos para opções referenciadas em dólar ou valor de contrato em pontos para termo secundário,
    	CODISI: formato X(12), código do papel no sistema isin ou código inerno do papel,
    	DISMES: formato N(03), número de distribuição do papel, número de sequência do papel correspondente ao estado de direito vigente,
    """

    registro_size = 245
    registro_quantidade = 0

    colunasB3 = ['TIPREG','DATAPREG','CODBDI','CODNEG','TPMERC','NOMRES','ESPECI','PRAZOT', 'MODREF',
                     'PREABE','PREMAX','PREMIN','PREMED','PREULT','PREOFC','PREOFV','TOTNEG','QUATOT','VOLTOT',
                     'PREEXE','INDOPC','DATVEN','FATCOT','PTOEXE','CODISI','DISMES']
    return (registro_size,)


@app.cell
def _():
    # other tables from B3 log to identify columns
    # definição de outras tabelas utilizadas no registro histórico da B3

    # CÓDIGOS DE BDI (boletim diário de informação, códigos de classificação dos papeis)
    CODBDI = ['02', '05', '06', '07', '08', '09', '10', '11', '12', '14', '18', '22', '26',
              '32', '33', '38','42','46','48','49','50','51','52','53','54','56','58','60',
              '61','62','66','68','70','71','74','75','78','82','83','84','90','96','99']

    CODBDI_desc = ["LOTE PADRAO", "SANCIONADAS PELOS REGULAMENTOS BMFBOVESPA", "CONCORDATARIAS", "RECUPERACAO EXTRAJUDICIAL", "RECUPERAÇÃO JUDICIAL",
                   "RAET - REGIME DE ADMINISTRACAO ESPECIAL TEMPORARIA", "DIREITOS E RECIBOS", "INTERVENCAO", "FUNDOS IMOBILIARIOS",
                   "CERT.INVEST/TIT.DIV.PUBLICA", "OBRIGACÕES", "BÔNUS (PRIVADOS)", "APOLICES/BÔNUS/TITULOS PUBLICOS", 
                   "EXERCICIO DE OPCOES DE COMPRA DE INDICES", "EXERCICIO DE OPCOES DE VENDA DE INDICES", "EXERCICIO DE OPCOES DE COMPRA", 
                   "EXERCICIO DE OPCOES DE VENDA", "LEILAO DE NAO COTADOS", "LEILAO DE PRIVATIZACAO", 
                   "LEILAO DO FUNDO RECUPERACAO ECONOMICA ESPIRITO SANTO", "LEILAO", "LEILAO FINOR", "LEILAO FINAM", "LEILAO FISET", 
                   "LEILAO DE ACÕES EM MORA", "VENDAS POR ALVARA JUDICIAL", "OUTROS", "PERMUTA POR ACÕES", "META", "MERCADO A TERMO",
                   "DEBENTURES COM DATA DE VENCIMENTO ATE 3 ANOS", "DEBENTURES COM DATA DE VENCIMENTO MAIOR QUE 3 ANOS", "FUTURO COM RETENCAO DE GANHOS",
                   "MERCADO DE FUTURO", "OPCOES DE COMPRA DE INDICES", "OPCOES DE VENDA DE INDICES", "OPCOES DE COMPRA", "OPCOES DE VENDA", "BOVESPAFIX",
                   "SOMA FIX", "TERMO VISTA REGISTRADO", "MERCADO FRACIONARIO", "TOTAL GERAL"]

    # TPMERC - RELAÇÃO DOS TIPO DE MERCADO
    TPMERC = {
    "010":"VISTA", 
    "012":"EXERCÍCIO DE OPÇÕES DE COMPRA", 
    "013":"EXERCÍCIO DE OPÇÕES DE VENDA", 
    "017":"LEILÃO", 
    "020":"FRACIONÁRIO", 
    "030":"TERMO", 
    "050":"FUTURO COM RETENÇÃO DE GANHO", 
    "060":"FUTURO COM MOVIMENTAÇÃO CONTÍNUA", 
    "070":"OPÇÕES DE COMPRA", 
    "080":"OPÇÕES DE VENDA" 
    }

    #":"INDOPC":"-":"RELAÇÃO DOS":"VALORES":"PARA":"CORREÇÃO DE":"CONTRATOS1":"US$":"CORREÇÃO PELA":"TAXA":"DO DÓLAR":"
    INDOPC = {
    "1":"US$",
    "2":"TJLP",
    "8":"IGPM",
    "9":"URV"
    }
    return


@app.cell
def _(datetime, registro_size, tz):
    # analysis of B3 transaction log register
    # analisa registo histórico da B3
    # retorna dicionário 'registro'

    def analisa_registro (reg):
        registro = {}
        if len(reg) != registro_size:
            return registro

        # colunasB3 from-To (leitura da linha/registro)
        if reg[0:2] == '01':  # Registro de dados
            registro['DATAPREG'] = datetime.strptime(reg[2:10], '%Y%m%d').replace(tzinfo=tz).date()
            registro['CODBDI'] = reg[10:12].strip()
            registro['CODNEG'] = reg[12:24].strip()
            registro['TPMERC'] = int(reg[24:27])
            registro['NOMRES'] = reg[27:39].strip()
            registro['ESPECI'] = reg[39:49].strip()
            registro['PRAZOT'] = int(reg[49:52]) if reg[49:52].strip() else 0
            registro['MODREF'] = reg[52:56].strip()
            registro['PREABE'] = float(reg[56:69])/100 if reg[56:69].strip() else 0.0
            registro['PREMAX'] = float(reg[69:82])/100 if reg[69:82].strip() else 0.0
            registro['PREMIN'] = float(reg[82:95])/100 if reg[82:95].strip() else 0.0
            registro['PREMED'] = float(reg[95:108])/100 if reg[95:108].strip() else 0.0
            registro['PREULT'] = float(reg[108:121])/100 if reg[108:121].strip() else 0.0
            registro['PREOFC'] = float(reg[121:134])/100 if reg[121:134].strip() else 0.0
            registro['PREOFV'] = float(reg[134:147])/100 if reg[134:147].strip() else 0.0
            registro['TOTNEG'] = int(reg[147:152]) if reg[147:152].strip() else 0
            registro['QUATOT'] = int(reg[152:170]) if reg[152:170].strip() else 0
            registro['VOLTOT'] = float(reg[170:188])/100 if reg[170:188].strip() else 0.0
            registro['PREEXE'] = float(reg[188:201])/100 if reg[188:201].strip() else 0.0
            registro['INDOPC'] = int(reg[201:202]) if reg[201:202].strip() else 0
            try:
                registro['DATVEN'] = datetime.strptime(reg[202:210], '%Y%m%d').replace(tzinfo=tz).date()
            except (ValueError, OSError):
                registro['DATVEN'] = None
            registro['FATCOT'] = int(reg[210:217]) if reg[210:217].strip() else 0
            registro['PTOEXE'] = float(reg[217:230])/1000000 if reg[217:230].strip() else 0.0
            registro['CODISI'] = reg[230:242].strip()
            registro['DISMES'] = int(reg[242:245]) if reg[242:245].strip() else 0
        elif reg[0:2] == '99':
            registros_quantidade = int(reg[31:42])    
        return registro

    return (analisa_registro,)


@app.function
# print on terminal times from executionTimes

def printTimesTerminal(arq, readT, create_dfT, saveT, filterT, totalT, totalRegs):
    print(f"\n{arq}")
    print(f"\nExecution times:")
    print(f"Reading file: {readT:.2f} seconds") 
    print(f"Creating DataFrame: {create_dfT:.2f} seconds")
    print(f"Saving files: {saveT:.2f} seconds")
    print(f"Filtering and saving: {filterT:.2f} seconds")
    print(f"Total execution time: {totalT:.2f} seconds")
    print(f"Total registers read: {totalRegs}")


@app.cell
def _(datetime):
    # function to log time of function execution
    # função para gravar tempo de execução de leitura de arquivos/logs da B3

    def executionTimes(mensagem, read_time, df_time, save_time, filter_time, total_time):
        current_time = datetime.now().strftime("%Y-%m-%d %H:%M")                                       
        with open('b3_logs/execution_times.txt', 'a') as f:
            f.write(f"{current_time}\n")
            f.write(f"{mensagem}\n")
            f.write(f"Reading file: {read_time:.2f} seconds\n")
            f.write(f"Creating DataFrame: {df_time:.2f} seconds\n")
            f.write(f"Saving files: {save_time:.2f} seconds\n")
            f.write(f"Filtering and saving: {filter_time:.2f} seconds\n")
            f.write(f"Total execution time: {total_time:.2f} seconds\n")
            f.write("\n")  # add line, acrescenta linha em branco

    return (executionTimes,)


@app.cell
def _(datetime):
    # função que registra erros, log function of errors

    def reg_erros(mensagem):
        current_time = datetime.now().strftime("%Y-%m-%d %H:%M")
        with open('b3_logs/erros.txt', 'a') as f:
            f.write(f"{current_time}, Erro: {mensagem}\n")

    return (reg_erros,)


@app.cell
def _(reg_erros):
    # um 'IF' repetitivo que está em várias funções de processamento
    # an 'if' used many times
    # que verifica qual o formato que é para gravar(saida) e grava no destino correto(destino)

    # df_entrada -> o dataframe a ser convertido para outro formato e/ou filtrado
    # saida -> formato de saida (explo: csv, parquet, pickle)
    # destino -> pasta destino (explo: 'mercado_FIIs', dentro da pasta do formato correspondente)
    # ano -> ano dos dados do arquivo (explo: 2000(AAAA) do COTAHISTAAAA.TXT)


    def faz_o_IF (df_entrada, saida, destino, ano):

        if saida == 'csv':
            arquivo3 = f"b3_{saida}/{destino}/{destino}{ano}.{saida}"
            df_entrada.to_csv(arquivo3, index=False)
        elif saida == 'pickle':
            saida2 = 'pkl'
            arquivo3 = f"b3_{saida}/{destino}/{destino}{ano}.{saida2}"
            df_entrada.to_pickle(arquivo3, protocol=5)
        elif saida == 'parquet':
            arquivo3 = f"b3_{saida}/{destino}/{destino}{ano}.{saida}"
            df_entrada.to_parquet(arquivo3)
        else:
            mensagem = f"erro de parametro (parametro: {saida}) de qual formato de arquivo a gravar"
            reg_erros(mensagem)
            return 'ERRO'

    return (faz_o_IF,)


@app.cell
def _(analisa_registro, executionTimes, pd, reg_erros, time, zipfile):
    # function to process zip->csv 
    # read: folder:'b3_zip/' all files: (COTAHIST_Axxxx.zip files - anual serie)
    # write: folder:'b3_csv/b3_data/' all files: (b3_dataxxxx.csv)

    # o arquivo zip não é um 'csv file'.
    # cada linha é uma string de 245 caracteres que precisa dividir (split) e criar um csv file
    # a função 'analisa_registro' faz esse trabalho.

    # IMPORTANTE: 
    # parametro 'arquivo' é passado sem a extensão .zip

    # Após o processamento dos COTAHIST, o Total geral de registros de 2000 a 27-fev-2026: 20,241,889

    # logs históricos da B3 (com opções: série anual ou mensal ou diária) estão disponíveis em:
    # https://www.b3.com.br/pt_br/market-data-e-indices/servicos-de-dados/market-data/historico/mercado-a-vista/series-historicas/

    def process_V2_b3_data_ZIP(arquivo):
        start_time = time.time()
        ano = arquivo[-4:]

        # Path to the ZIP file
        arquivo2 = 'b3_zip/' + arquivo + '.ZIP'
        zip_path = arquivo2
        file_name = arquivo + '.TXT'

        #lista de dicionarios (registro é um dic).....
        data = []

        #unzip files
        read_start = time.time()

        with zipfile.ZipFile(zip_path, 'r') as zf:
            with zf.open(file_name, 'r') as f:
                lines = f.readlines()
                # Skip first and last lines (pula primeira e última linha)
                for line in lines[1:-1]:
                    line_str = line.decode('latin-1').strip()  # codificação, encoding
                    if len(line_str) == 245:
                        registro = analisa_registro(line_str)
                        if registro:  # só se não vazio, Only add if not empty
                            data.append(registro)
                    else:
                        mensagem = f"erro no arquivo {file_name} na leitura da linha {line_str} -nao tem 245 car"
                        reg_erros(mensagem)

        read_time = time.time() - read_start

        # Create DataFrame
        df_start = time.time()
        df = pd.DataFrame(data)
        df_time = time.time() - df_start

        total_regs_ano = len(df)
        # print(f"Total de registros lidos: {total_reg_ano}")

        # Save to folder without filters
        save_start = time.time()
        arquivo3 = 'b3_csv/b3_data/b3_data' + ano + '.csv'
        df.to_csv(arquivo3, index=False)
        save_time = time.time() - save_start

        # zip -> csv doesn't have filter (zip->csv não tem filtros)
        filter_time = 0
        total_time = time.time() - start_time

        # Print times on terminal
        # printTimesTerminal(arquivo, read_time, df_time, save_time, filter_time, total_time, total_regs_ano)

        # Save times to file using function 'executionTimes' (log de tempo de execução)
        mensagem = f"Tempo de execução zip->csv {ano} regsAno:{total_regs_ano}"
        executionTimes(mensagem, read_time, df_time, save_time, filter_time, total_time)

        return total_regs_ano

    return (process_V2_b3_data_ZIP,)


@app.cell
def _():
    # divided the process (zip->csv) in two lists just because of cpu timing.
    # dividi em dois blocos (lista1 e lista2) para não 'travar' a máquina vários minutos
    # listas utilizadas em  'process_V2_b3_data_ZIP' (zip->csv)

    lista1_zips = ["COTAHIST_A2000","COTAHIST_A2001","COTAHIST_A2002","COTAHIST_A2003","COTAHIST_A2004","COTAHIST_A2005",
                   "COTAHIST_A2006","COTAHIST_A2007","COTAHIST_A2008","COTAHIST_A2009","COTAHIST_A2010","COTAHIST_A2011", 
                   "COTAHIST_A2012","COTAHIST_A2013","COTAHIST_A2014","COTAHIST_A2015","COTAHIST_A2016"]

    lista2_zips = ["COTAHIST_A2017","COTAHIST_A2018","COTAHIST_A2019","COTAHIST_A2020","COTAHIST_A2021", "COTAHIST_A2022",
                   "COTAHIST_A2023","COTAHIST_A2024","COTAHIST_A2025","COTAHIST_A2026"]

    # se quiser executar todos em uma 'rodada', abaixo lista
    lista3_zips = ["COTAHIST_A2000","COTAHIST_A2001","COTAHIST_A2002","COTAHIST_A2003","COTAHIST_A2004","COTAHIST_A2005",
                   "COTAHIST_A2006","COTAHIST_A2007","COTAHIST_A2008","COTAHIST_A2009","COTAHIST_A2010","COTAHIST_A2011", 
                   "COTAHIST_A2012","COTAHIST_A2013","COTAHIST_A2014","COTAHIST_A2015","COTAHIST_A2016","COTAHIST_A2017",
                   "COTAHIST_A2018","COTAHIST_A2019","COTAHIST_A2020","COTAHIST_A2021","COTAHIST_A2022","COTAHIST_A2023",
                   "COTAHIST_A2024","COTAHIST_A2025","COTAHIST_A2026"]
    return (lista3_zips,)


@app.cell
def _():
    # divided the process(csv->other formats) in two lists just because of cpu timing.
    # dividi em dois blocos para não 'travar' a máquina vários minutos
    # listas utilizadas em 'process_b3_data_csv' (csv -> parquet, pickle, csv)

    # para processar arquivos csv -> parquet ou pickle (2000 -> 2016)
    lista1_csvs = ["b3_data2000","b3_data2001","b3_data2002","b3_data2003","b3_data2004","b3_data2005",
                   "b3_data2006","b3_data2007","b3_data2008","b3_data2009","b3_data2010","b3_data2011",
                   "b3_data2012","b3_data2013","b3_data2014","b3_data2015","b3_data2016"]

    # para processar arquivos csv -> parquet,pickle (20017 -> 2026)
    lista2_csvs = ["b3_data2017","b3_data2018","b3_data2019","b3_data2020","b3_data2021","b3_data2022",
                   "b3_data2023","b3_data2024","b3_data2025","b3_data2026"]

    # se quiser processar todos de uma vez
    lista3_csvs = ["b3_data2000","b3_data2001","b3_data2002","b3_data2003","b3_data2004","b3_data2005",
                   "b3_data2006","b3_data2007","b3_data2008","b3_data2009","b3_data2010","b3_data2011",
                   "b3_data2012","b3_data2013","b3_data2014","b3_data2015","b3_data2016","b3_data2017",
                   "b3_data2018","b3_data2019","b3_data2020","b3_data2021","b3_data2022","b3_data2023",
                   "b3_data2024","b3_data2025","b3_data2026"]
    return (lista3_csvs,)


@app.cell
def _(process_V2_b3_data_ZIP, time):
    # processando todos arquivos zip -> csv 
    # processa somente a conversão de zip para csv (para a pasta './b3_csv/b3_data')
    # lista_zip => defina lista1_zips ou lista2_zips ou lista3_zips

    def processa_arqs_zip(lista_zip):

        # tempo total de execução do processo zip->csv,
        start_zip_csv_time = time.time()

        lista = lista_zip
        # ONLY output format csv
        TregN = 0
        for arq in lista:
            regN = process_V2_b3_data_ZIP(arq)
            TregN = TregN + regN

        print(f"\nTotal registros zip->csv: {TregN}\n") 

        total_zip_csv_time = time.time() - start_zip_csv_time
        print(f"Tempo total execução zip->csv: {total_zip_csv_time:.2f} seconds\n")

    return (processa_arqs_zip,)


@app.cell
def _(lista3_zips, processa_arqs_zip):
    # *** tire o comentário para operação que quiser realizar ***

    # descomprimindo zips para csv em duas etapas
    #processa_arqs_zip(lista1_zips)
    #processa_arqs_zip(lista2_zips)

    # descomprimindo em uma só etapa
    processa_arqs_zip(lista3_zips)

    # descomprimindo SÓ DE UM ANO - usado para atualizar ano corrente.
    #lista_doAno = ["COTAHIST_A2026"]
    #processa_arqs_zip(lista_doAno)
    return


@app.cell
def _(executionTimes, faz_o_IF, pd, time):
    #
    # *** nesta função só filtra os mercados: avista, FIIs, frac (fracionário) ***
    #
    # converte (no caso de parquet e pickle) e filtra arquivos csv da B3 (em '/b3_data') para saidas parquet,pickle,csv 
    # nesta função só filtra os mercados: avista, FIIs, frac (fracionário)

    # read csv files (from /b3_data) and save filtered files as csv, parquet, pickle
    # used to create files from folders: mercado_avista, mercado_frac, mercado_FIIs

    # para processar arquivos da B3 (indicando qual ano a ser processado e para separar/filtrar só colunas de interesse)
    # we will use only the bellow columns
    # ['DATAPREG', 'CODBDI', 'CODNEG', 'TPMERC', 'NOMRES', 'ESPECI', 'MODREF', 'PREABE', 'PREMAX',
    #  'PREMIN', 'PREMED', 'PREULT', 'TOTNEG', 'QUATOT', 'VOLTOT', FATCOT, 'CODISI']

    # lê csv e salva como parquet, pickle, csv
    # separa/filtra os mercados: mercado_avista, mercado_frac (fracionário) e mercado_FIIs (Fundos Imobiliários)
    # e grava também b3_data (log histório da B3 sem filtros) no formato escolhido

    # Folders used:
    # arquivos csv vão para  as pasta:
    #           /b3_csv/b3_data; arquivo csv com todo movimento do COTAHIST
    #           /b3_csv/mercado_avista; arquivo csv (TPMERC == 10 and CODBDI == '02') à vista e lote padrão
    #           /b3_csv/mercado_FIIs; arquivo csv fundos imobiliários (TPMERC == 10 and CODBDI == '12')
    #           /b3_csv/mercado_fracionario; arquivo do mercado fracionário (TPMERC == 20 and CODBDI == '96')
    #
    # arquivos parquet vão para /b3_parquet/ ; mesma estrutura do csv
    # arquivos pickle vão para /b3_pickle/ ; mesma estrutura do csv
    #
    # obs: mercado fracionário é para comprar ações 'avulsas' (fractional share), ou seja, comprar menos de um lote padrão (normalmente 100 ações)

    def process_b3_data_csv(arquivo, saida):

        ano = arquivo[-4:]
        start_time = time.time()

        # Path to the csv file
        arquivo2 = 'b3_csv/b3_data/' + arquivo + '.csv'

        # read file and sort by date of register
        read_start = time.time()
        df0 = pd.read_csv(arquivo2, dtype={"CODBDI":str})
        read_time = time.time() - read_start

        # sort and reset index
        df_start = time.time()
        df = df0.sort_values('DATAPREG')
        df = df.reset_index(drop=True)
        df_time = time.time() - df_start

        # Save to folder '/b3_data' if not using 'saida' = 'csv'
        # because we already have b3_dataXXXX.csv files
        # otherwise, if filtering 'parquet', 'pickle', create b3_dataxxxx.parquet or pkl 

        save_start = time.time()
        if saida != 'csv':
            destino = 'b3_data'
            faz_o_IF (df, saida, destino, ano)

        save_time = time.time() - save_start

        # Filter for mercado a vista (TPMERC == 10 and CODBDI == '02')
        # and for FIIs (TPMERC == 10 and CODBDI == '12')
        filter_start = time.time()

        selected_columns = ['DATAPREG', 'CODBDI', 'CODNEG', 'TPMERC', 'NOMRES', 'ESPECI', 'MODREF', 'PREABE',
                            'PREMAX', 'PREMIN', 'PREMED', 'PREULT', 'TOTNEG', 'QUATOT', 'VOLTOT', 'FATCOT', 'CODISI']

        # save mercado a vista
        mask_avista = (df['TPMERC'] == 10) & (df['CODBDI'] == '02')
        df_avista = df[mask_avista][selected_columns]
        destino = 'mercado_avista'
        faz_o_IF (df_avista, saida, destino, ano)

        # save mercado FII - fundos imobiliários
        mask_fiis = (df['TPMERC'] == 10) & (df['CODBDI'] == '12')
        df_fiis = df[mask_fiis][selected_columns]
        destino = 'mercado_FIIs'
        faz_o_IF (df_fiis, saida, destino, ano)

        # save mercado_fracionario - compra de ações 'avulsas' (fractional share)
        mask_frac = (df['TPMERC'] == 20) & (df['CODBDI'] == '96')
        df_fraction = df[mask_frac][selected_columns]
        destino = 'mercado_frac'
        faz_o_IF (df_fraction, saida, destino, ano)

        filter_time = time.time() - filter_start

        total_regs_ano = len(df0)
        regsAvista = len(df_avista)
        regsFIIs = len(df_fiis)
        regsFrac = len(df_fraction)
        total_regs_ano = len(df0)

        total_time = time.time() - start_time

        # Print Execution times on terminal
        printTimesTerminal(arquivo, read_time, df_time, save_time, filter_time, total_time, total_regs_ano)

        print(f"Filtered registers for mercado a vista (CODBDI=02): {regsAvista}")
        print(f"Filtered registers for FIIs (CODBDI=12): {regsFIIs}")
        print(f"Filtered registers for Frac (CODBDI='96'): {regsFrac}\n")

        # Save time to file
        mensagem = f"Execution times csv->{saida} {ano} regsAno:{total_regs_ano} regsAvista:{regsAvista} regsFIIs:{regsFIIs} regsFrac:{regsFrac}"

        executionTimes(mensagem, read_time, df_time, save_time, filter_time, total_time)

        return total_regs_ano

    return (process_b3_data_csv,)


@app.cell
def _(process_b3_data_csv):
    # usa a função 'process_b3_data_csv(arquivo, saida)' para filtar os mercados avista, FIIs, frac(fracionário)
    # com opção de saída/salvar em formato parquet, pickle ou csv
    # parameters:
    # saidaForm: use 'parquet' or 'pickle' or 'csv'
    # listaArqs: use lista1_csvs or lista2_csvs or lista3_csvs or to create a list for one year:'listaArqs = ["b3_data2026"]' 

    def filtraMercAvistaFIIfrac(listaArqs, saidaForm):
        saida = saidaForm # use 'parquet' or 'pickle' or 'csv'
        TregN = 0
        for arq in listaArqs:
            regN = process_b3_data_csv(arq, saida)
            TregN = TregN + regN

        # print(f"\nTotal registros de 2000->2016 csv->{saida}: {TregN}\n")
    return (filtraMercAvistaFIIfrac,)


@app.cell
def _(filtraMercAvistaFIIfrac, lista3_csvs):
    # usando a função acima ('filtraMercadosAvistaFIIfrac(listaArqs, saidaForm)') para filtrar mercados avista, FIIs e fracionário (frac)

    lista_afiltrar = lista3_csvs
    saida_formato = 'parquet'   # 'csv', 'pickle' ou 'parquet'
    filtraMercAvistaFIIfrac(lista_afiltrar, saida_formato)
    return


@app.cell
def _(executionTimes, faz_o_IF, pd, time):
    # Ler csv('b3_data - histórico B3) e identificar códigos de BDR e UNITS, ações_ON_PN e outros papeis
    # gravar arquivo 'mercado_bdrXXXX.xxx' (na pasta 'mercado_bdr')
    # gravar arquivo 'mercado_unitsXXXX.xxx' (na pasta 'mercado_units')
    # gravar arquivo 'mercado_acaoONPNBRXXXX.xxx' 'filtrado' só ON e PN (na pasta 'mercado_acaoONPNBR')
    # gravar arquivo 'mercado_outros -> outros papeis que não são, do 'mercado à vista', FIIs, fracionários, BDR, UNITs ou acao ON PN.
    # vai ler a pasta b3_data (arquivos: b3_dataXXXX.csv; XXXX -> ano), salva mercados como parquet, pickle,csv

    # definições:
    # mercado_acaoONPNBR -> só ações do mercado nacional (On e PN, xxxx3, xxxx4 e classes de preferenciais xxxx5,6,7,8)
    # units -> são junção de ações já cotadas (explo: adcd11 = 2*adc3 + 1*adc4 (soma de ordinarias e preferenciais))
    # BDRs -> são cotadas em bolsa estrangeira (exemplo JBS saiu (2025) da B3 e foi para USA, e aqui, agora, só tem BDR)

    # read csv file (b3_dataxxxx.csv) and save as parquet, pickle or csv to folders: mercado_bdr, mercado_units, mercado_outros, mercado_ONPNBR

    def process_b3_To_BDR_units_etc(arquivo, saida):

        ano = arquivo[-4:]

        start_time = time.time()

        # Path to the csv file
        arquivo2 = 'b3_csv/b3_data/' + arquivo + '.csv'

        # read file and sort by date of register
        read_start = time.time()

        df0 = pd.read_csv(arquivo2, dtype={"CODBDI":str})

        read_time = time.time() - read_start

        # sort and reset index
        df_start = time.time()
        df = df0.sort_values('DATAPREG')
        df = df.reset_index(drop=True)
        df_time = time.time() - df_start

        # Save to folder mercado_bdr
        save_start = time.time()
        filter_start = time.time()

        # df_bdr - pega CODISI (padrão ISIN):verifica se tem 'BDR' na posição [6:9]
        mask_bdr = df['CODISI'].str[6:9] == 'BDR'
        df_bdr = df[mask_bdr].reset_index(drop=True)
        destino = 'mercado_bdr'
        faz_o_IF (df_bdr, saida, destino, ano)


        # Save to folder mercado_units
        # Keep rows where last two characters is '11'
        # I used the bellow filter using 'isin' with one item, because, maybe, I may need to change, to add also letters
        mask_units = df['CODNEG'].str[4:6].isin(['11']) & (df['CODISI'].str[6:9] != 'BDR') & (df['TPMERC'] == 10) & (df['CODBDI'] == '02')
        df_units = df[mask_units].reset_index(drop=True)
        destino = 'mercado_units'
        faz_o_IF (df_units, saida, destino, ano)

        # Filter for mercado de acoes ON PN BR (TPMERC == 10 and CODBDI == '02')
        # and for CODNEG diferent from final 11, 33 and 34

        # save mercado de acoes ON PN BR to folder /mercado_acaoONPNBR
        mask_ONPN = df['CODNEG'].str.len().eq(5) & df['CODNEG'].str[-1].isin(list('345678')) & (df['TPMERC'] == 10) & (df['CODBDI'] == '02')
        df_acao_ONPNBR = df[mask_ONPN].reset_index(drop=True)
        destino = 'mercado_acaoONPNBR'
        faz_o_IF (df_acao_ONPNBR, saida, destino, ano)

        # eliminar todas os registros já escolhidos
        mask1 = ~mask_ONPN  # não é ação(XXXX) ON(N) ou PN(N)
        mask2 = ~mask_units # não é unit nem BDR
        mask3 = ~mask_bdr
        # e elimina também da filtragem dos avista, FIIs e fracionário
        mask_avista = (df['TPMERC'] == 10) & (df['CODBDI'] == '02')
        mask_fiis = (df['TPMERC'] == 10) & (df['CODBDI'] == '12')
        mask_frac = (df['TPMERC'] == 20) & (df['CODBDI'] == '96')
        mask4 = ~mask_avista & ~mask_fiis & ~mask_frac 

        # Save to folder mercado_outros
        df_outros = df[mask1 & mask2 & mask3 & mask4].reset_index(drop=True)
        destino = 'mercado_outros'
        faz_o_IF (df_outros, saida, destino, ano)

        save_time = time.time() - save_start
        filter_time = time.time() - filter_start

        total_regs_av = len(df0)
        regsacaoONPNBR = len(df_acao_ONPNBR)
        regsBDRs = len(df_bdr)
        regsUnits = len(df_units)
        regsOutros = len(df_outros)

        total_time = time.time() - start_time

        # Print Execution times on terminal
        printTimesTerminal(arquivo, read_time, df_time, save_time, filter_time, total_time, total_regs_av)

        print(f"\nFiltered registers for mercado acoes ONPN BR: {regsacaoONPNBR}")
        print(f"Filtered registers for BDRs: {regsBDRs}")
        print(f"Filtered registers for Units: {regsUnits}")
        print(f"Filtered registers for Outros: {regsOutros}")
        total = regsacaoONPNBR + regsBDRs + regsUnits + regsOutros
        print(f" total acoes+bdr+units+outros:  {total} \n")
        print(f"Total registers read: {total_regs_av}")

        # Save time to file
        mensagem = f"Execution times csv->{saida} {ano} regsTotalAV:{total_regs_av}  regsacaoONPNBR:{regsacaoONPNBR} regsBDRs:{regsBDRs} regsUnits:{regsUnits} regsOutros:{regsOutros}"

        executionTimes(mensagem, read_time, df_time, save_time, filter_time, total_time)

        return regsacaoONPNBR

    return (process_b3_To_BDR_units_etc,)


@app.cell
def _(os, process_b3_To_BDR_units_etc):
    # filtra ações ON e PN, BDRs, Units, e Outros papéis
    # utiliza a função 'process_b3_To_BDR_units_etc(arquivo, saida)'
    # de 2000 a 2026
    # 
    # saida -> csv, parquet, pickle
    # pasta_csv -> é criada a lista de arquivos existentes em /b3_csv//b3_data (2000 - 2026)

    def filtra_BDRs_ONPN_etc(pasta_csv, saida_etc):

        # cria lista de arquivos para filtrar
        lista_path = pasta_csv
        lista_arquivos = os.listdir(lista_path)
        lista_semCSV = [nome.replace(".csv", "") for nome in lista_arquivos]

        #processa a filtragem de todos arquivos existentes em 'b3_csv/b2_data'
        for arquivo in lista_semCSV:
            process_b3_To_BDR_units_etc(arquivo, saida_etc)

    return (filtra_BDRs_ONPN_etc,)


@app.cell
def _(filtra_BDRs_ONPN_etc):
    # tirar comentário (ou comentar uma(1) e tirar comentário de outra(2)) para utilizar função

    # 1) utilização da função 'filtra_BDRs_ONPN_etc(pasta_csv, saida_etc)'
    # saida: csv ou parquet ou pickle
    saida_Etc = 'csv'               # 'csv' ou 'parquet' ou 'pickle'
    pasta_filtra = 'b3_csv/b3_data'
    filtra_BDRs_ONPN_etc(pasta_filtra, saida_Etc)

    # 2) Ou se quiser só atualizar um ano (explo: atualizar ano corrente)
    # saida_Etc = 'csv'
    # arq_semCSV = "b3_data2026"  # definir qual o ano que quer atualizar
    # process_b3_To_BDR_units_etc(arq_semCSV, saida_Etc)
    return


@app.cell
def _(os, pd):
    # função para converter os arquivos da 'evolução_diáriaAAAA.csv' do índice IBOV
    # muda os arquivos de uma pasta com padrão csv BR (; e número com 000.000,00) para csv USA (, e número 000000.00)
    # pasta: /b3_csv/b3_ibov/ibovEvolucaoDiaria_brcsv
    # remove cabeçado (a primeira linha)
    # remove rodapé (footer) duas últimas linhas
    # isto, para termos uma tabela formatada para analisar

    def converte_formata_arq_ibov():
        path_brcsv = "b3_csv/b3_ibov/ibovEvolucaoDiaria_brcsv/"
        arquivos = [f for f in os.listdir(path_brcsv) if os.path.isfile(os.path.join(path_brcsv, f))]

        for arq in arquivos:
            caminho = path_brcsv + arq
            df = pd.read_csv(caminho, sep=';', decimal=',', thousands='.', skiprows=1, skipfooter=2, engine='python', encoding='latin1')
            path_salva = f"b3_csv/b3_ibov/ibovEvolucaoDiaria_brcsv/{arq}"
            df.to_csv(path_salva, sep=',', decimal='.', index=False, encoding='utf-8')
        return

    return (converte_formata_arq_ibov,)


@app.cell
def _(converte_formata_arq_ibov):
    converte_formata_arq_ibov()
    return


@app.cell
def _(os, pd, reg_erros, time):
    # create a file with unique values of stock codes of all years
    # cria um arquivo com os codigos (unicos) CODNEG das ações de 2000->2026
    # também funciona para códigos de outras colunas
    # exemplos de uso: 
    #        para verificar 'quem entrou e quem saiu" da bolsa, e em que ano.
    #        

    def lista_valores_unicos_por_ano(input_dir, column_name, output_file):
        """
        Objetivo: ler todos os anos - cada ano é um arquivo -  do movimento(log) da B3 e verifcar quantos
        tickers(ações) unicos existem por ano. Cria um arquivo com cada coluna sendo um ano.

        Lê todos os arquivos de um diretorio (input_dir) e extrai os valores unicos de uma coluna (column_name)
        e salva em um arquivo csv (output_file). 

        Reads all CSV files in a directory, extracts unique values (tickers) from a given column,
        groups them by the year (last 4 characters of filename), and saves them into
        a single CSV file with one column per year.

        :param input_dir: Directory containing CSV files
        :param column_name: Name of the column to extract unique values from
        :param output_file: Path to save the output CSV file
        """
        start_time = time.time()
        year_data = {}  # Dictionary: {year: [unique values]}

        for filename in os.listdir(input_dir):
            if filename.lower().endswith(".csv"):
                file_path = os.path.join(input_dir, filename)

                # Extract year from last 4 characters before extension
                year = filename[-8:-4]  # e.g., "mercado_acaoONPNBR" → "2020"
                if not year.isdigit():
                    print(f"ignorando {filename}: últimos 4 chars antes de .xxx não é ano.")
                    continue

                try:
                    df = pd.read_csv(file_path)

                    if column_name not in df.columns:
                        print(f"Coluna: '{column_name}' não encontrada em: {filename}, ignorando.")
                        continue
                    # Get sorted unique values for this year
                    unique_vals = sorted(df[column_name].dropna().unique())
                    year_data[year] = unique_vals
                except Exception as e:
                    print(f"Erro de leitura {filename}: {e}")
                    mensagem = f"erro de leitura {filename}: {e}"
                    reg_erros(mensagem)

        if not year_data:
            print("Não tem dados para salvar.")
            return

        # Convert dictionary to DataFrame with columns as years
        max_len = max(len(vals) for vals in year_data.values())
        for year in year_data:
            # Pad with None so all columns have equal length
            year_data[year] += [None] * (max_len - len(year_data[year]))

        result_df = pd.DataFrame(year_data)
        end_time = time.time() - start_time

        # Save to CSV
        result_df.to_csv(output_file, index=False)
        print(f"tempo: {end_time:.2f} ; Valores unicos, por ano, salvos em: {output_file}")

    return (lista_valores_unicos_por_ano,)


@app.cell
def _(lista_valores_unicos_por_ano):
    # valores unicos (CODNEG) do ticker/stock para 'mercado_avistaBR'(ON e PN; final 3, 4 e outras classes PN) 
    # de todas as acoes da B3 (dos anos de 2000 a 2026)

    def ListaValoresUnicosMercadoAvista():

        input_dir = "b3_csv/mercado_acaoONPNBR"  # trocar a pasta para valores unicos de outros mercados. explo: FII ou BDR, etc.
        coluna_a_extrair = "CODNEG"         # Coluna com ticker symbol (código da da ação) 
        saidaF_csv = "b3_analysis/codigos_unicos_acoes/b3_unique_acoesONPNBR_by_year.csv"     # Output file name, arquivo de saída

        lista_valores_unicos_por_ano(input_dir, coluna_a_extrair,saidaF_csv)

    return (ListaValoresUnicosMercadoAvista,)


@app.cell
def _(ListaValoresUnicosMercadoAvista):
    ListaValoresUnicosMercadoAvista()
    return


@app.cell
def _(pd):
    # verifica quantas acoes existem (por ano) na bolsa de valores B3 (On e PN, final 3 e 4) de 2000 a 2026
    # how many different tickers by year
    # salva em "b3_analysis/codigos_unicos_acoes/quant_acoes_ano.csv"

    def verifica_Qtd_acoesUnicasAno():

        stock_code_unique = pd.read_csv("b3_analysis/codigos_unicos_acoes/b3_unique_acoesONPNBR_by_year.csv")

        lista_anos = list(stock_code_unique.columns)
        lista_totAno = []
        for coluna in lista_anos:
            n_linhas = stock_code_unique[coluna].count()
            lista_totAno.append(int(n_linhas))
            print(f"acoes em {coluna}: {n_linhas}")

        df_quant_acoes = pd.DataFrame({
            "ano": lista_anos,
            "tot_acoes": lista_totAno
        })

        df_quant_acoes.to_csv(
            "b3_analysis/codigos_unicos_acoes/b3_unicas_Acoes_Total_2000-2026.csv",
            index=False
        )

        return lista_totAno

    return (verifica_Qtd_acoesUnicasAno,)


@app.cell
def _(verifica_Qtd_acoesUnicasAno):
    listaAcoesPorAno = verifica_Qtd_acoesUnicasAno()
    print(listaAcoesPorAno)
    return


@app.cell
def _(go, pd):
    # cria gráfico mostrando a quantidade de ações 'unicas' de 2000 a 2026
    # gráfico com plotly graph_objects

    def graf_AcoesUnicas2000_2026():

        df = pd.read_csv('b3_analysis/codigos_unicos_acoes/b3_unicas_Acoes_Total_2000-2026.csv')

        # Criação da figura
        fig = go.Figure()

        # Adiciona a série
        fig.add_trace(go.Scatter(
            x=df['ano'],
            y=df['tot_acoes'],
            mode='lines+markers',
            name='ações únicas por ano',
            line=dict(color='blue')
        ))
        # Ajusta layout (equivalente ao plt.figure, plt.title, plt.xlabel, plt.ylabel, plt.grid, plt.legend)
        fig.update_layout(
            title='Quantidade de ações (On e PN) a B3 tinha de 2000 a 2026',
            xaxis=dict(
                title='Ano',
                tickangle=45,   # rotação dos rótulos do eixo x
                showgrid=True   # ativa a grade vertical
            ),
            yaxis=dict(
                title='Quant. Ações Únicas',
                showgrid=True   # ativa a grade horizontal
            ),
            legend=dict(
                title='',       # legenda sem título
                orientation='h', # horizontal (pode ser 'v' para vertical)
                x=0.5,
                xanchor='center',
                y=-0.2
            ),
            width=1000,  # equivalente ao figsize=(10,5)
            height=500
        )

        #fig.show()
        fig.show(renderer="vscode")

        return

    return (graf_AcoesUnicas2000_2026,)


@app.cell
def _(graf_AcoesUnicas2000_2026):
    graf_AcoesUnicas2000_2026()
    return


@app.cell
def _(pd):
    # quais as ações que são comuns e 'existem' em todos os anos desde 2000?

    def quaisComunsAtodosAnos():
        df_comuns = pd.read_csv("b3_analysis/codigos_unicos_acoes/b3_unique_acoesONPNBR_by_year.csv")

        # Step 1: Convert each column to a set of unique values
        sets_per_column = [set(df_comuns[col]) for col in df_comuns.columns]

        # Step 2: Find the intersection across all sets
        common_values = set.intersection(*sets_per_column)

        # Step 3: Convert to a list if needed
        common_values_list = list(common_values)
        num_list = len(common_values_list)

        #salva em txt, os tickers/stock que estão presentes em todos os anos de 2000 a 2026 (73 tickers)
        with open('b3_analysis/codigos_unicos_acoes/b3_unicas_AcoesComunsAtodosAnos2026.txt', 'w') as f:
            for item in common_values_list:      
                f.write(f"{item}\n")

        print(sorted(common_values_list))
        print(f"quantidade: {num_list}")

    return (quaisComunsAtodosAnos,)


@app.cell
def _(quaisComunsAtodosAnos):
    quaisComunsAtodosAnos()
    return


@app.cell
def _():
    # ABAIXO - 3 celulas de código para testar timing usando pandas, pyarrow e parquet
    # - abrindo 27 arquivos (b3_data) que, somando, têm 20 milhões de registros 
    # maquina com i5 11300H (8 logical cores), 32 GB RAM, SSD
    # 1) pandas, lendo arquivos no formato csv: 37,96 seg
    # 2) pandas, lendo mesmos arquivos no formato parquet (usando pyarrow): 8.06 seg
    # 3) pyarrow lendo 'direto' os mesmos arquivos no formato parquet: 1.62 seg
    #=================================================================================
    return


@app.cell
def _(os, pd, time):
    #### calcula o total de registros dos anos disponíveis: de 2000 a 27 fevereiro de 2026
    # usando ***pandas** para ver total de registros na ***pasta csv***
    # teste para calcular 'timing' de diferentes bibliotecas

    def calculaTOTRegistros2000_2026_csv():
        start_time = time.time()
        path = "b3_csv/b3_data/"
        #arquivos = os.listdir(path)
        arquivos = [f for f in os.listdir(path) if os.path.isfile(os.path.join(path, f))]
        tot = 0

        for item in arquivos:
            caminho = path + item
            df = pd.read_csv(caminho)
            tot = tot+ df.shape[0]
        end_time = time.time() - start_time

        print(f" tempo: {end_time:.2f} seg. ; Total geral de registros de 2000 a 27-fev-2026: {tot:,}")
        # tempo: 37.96 seg. ; Total geral de registros de 2000 a 27-fev-2026: 20,241,889

        return

    return (calculaTOTRegistros2000_2026_csv,)


@app.cell
def _(os, pd, time):
    # calcula o total de registros dos anos disponíveis: de 2000 a 27 fevereiro de 2026
    # usando ***pandas*** (c/pyarrow instalado) para ver total de registros na ***pasta parquet***
    # teste para calcular 'timing' de diferentes bibliotecas

    def calculaTOTRegistros2000_2026_parquet():

        start_time = time.time()
        path = "b3_parquet/b3_data/"
        #arquivos = os.listdir(path)
        arquivos = [f for f in os.listdir(path) if os.path.isfile(os.path.join(path, f))]
        tot = 0

        for item in arquivos:
            caminho = path + item
            df = pd.read_parquet(caminho)
            tot = tot+df.shape[0]
        end_time = time.time() - start_time

        print(f" tempo: {end_time:.2f} seg. ; Total geral de registros de 2000 a 27-fev-2026: {tot:,}")
        # tempo: 8.06 seg. ; Total geral de registros de 2000 a 27-fev-2026: 20,241,889

        return

    return (calculaTOTRegistros2000_2026_parquet,)


@app.cell
def _(os, pyarrow, time):
    # calcula o total de registros dos anos disponíveis: de 2000 a 27 fev de 2026
    # usando ***pyarrow*** diretamente para ver total de registros na ***pasta parquet***
    # teste para calcular 'timing' de diferentes bibliotecas

    def calculaTOTRegistros2000_2026_pyarrow():

        start_time = time.time()
        path = "b3_parquet/b3_data/"
        #arquivos = os.listdir(path)
        arquivos = [f for f in os.listdir(path) if os.path.isfile(os.path.join(path, f))]
        tot = 0

        for item in arquivos:
            caminho = path + item
            num_linhas = pyarrow.parquet.read_table(caminho).num_rows
            tot = tot+num_linhas

        end_time = time.time() - start_time

        print(f" tempo: {end_time:.2f} seg.; Total geral de registros de 2000 a 27-fev-2026: {tot:,}")
        # *** tempo: 1.62 seg.; Total geral de registros de 2000 a 27-fev-2026: 20,241,889

        return

    return (calculaTOTRegistros2000_2026_pyarrow,)


@app.cell
def _(calculaTOTRegistros2000_2026_csv):
    calculaTOTRegistros2000_2026_csv()
    return


@app.cell
def _(calculaTOTRegistros2000_2026_parquet):
    calculaTOTRegistros2000_2026_parquet()
    return


@app.cell
def _(calculaTOTRegistros2000_2026_pyarrow):
    calculaTOTRegistros2000_2026_pyarrow()
    return


@app.cell
def _(os, pd):
    #calcula a soma de VOLTOT de todos os anos por mercado e monta um dataframe por mercado com total de cada ano
    # e salva na pasta '"b3_analysis/volume_mercados/'

    def calculaVOLTOTporAnoPorMercado():

        mercados = ['mercado_acaoONPNBR', 'mercado_bdr', 'mercado_FIIs', 'mercado_frac', 'mercado_outros', 'mercado_units', 'mercado_avista']

        tipo_arq = 'csv' # ou csv ou pickle

        for mercado_def in mercados:
            path = f"b3_{tipo_arq}/{mercado_def}/"
            arquivos = [f for f in os.listdir(path) if os.path.isfile(os.path.join(path, f))]
            data = []
            vol_final=0
            nome_col = mercado_def[8:]

            for arq_ano in arquivos:
                year = arq_ano[-8:-4]
                caminho = path + arq_ano 
                df = pd.read_csv(caminho)
                vol_ano = df['VOLTOT'].sum()
                vol_final += vol_ano
                vol2 = round(vol_ano,2)

                #data.append({'ano':year, 'volume': vol2})
                data.append({'ano':year, nome_col: vol2})

            df_anos = pd.DataFrame(data)
            #df_anos.rename(columns={'volume': nome_col}, inplace = True)

            path2 = f"b3_analysis/volume_mercados/volume_{mercado_def}.csv"
            df_anos.to_csv(path2, index=False)

            path3 = f"b3_analysis/volume_mercados/tot_vol_{mercado_def}.txt"
            with open(path3, 'w') as f:
                f.write(f"{vol_final:,.2f}")

        return

    return (calculaVOLTOTporAnoPorMercado,)


@app.cell
def _(calculaVOLTOTporAnoPorMercado):
    calculaVOLTOTporAnoPorMercado()
    return


@app.cell
def _(pd):
    # merge das colunas referente aos volumes financeiros dos diferentes mercados ('join')
    # para montar tabela de 2000 2026 com todos 'volumes' de todos os mercados
    # e salva em 'b3_analysis/volume_mercados/todos_mercados_volume2000_2026.csv'

    def calculaVOLTOT_AnoTodosMercados():

        df_ONPNBR = pd.read_csv('b3_analysis/volume_mercados/volume_mercado_acaoONPNBR.csv')
        df_avista = pd.read_csv('b3_analysis/volume_mercados/volume_mercado_avista.csv')
        df_bdr = pd.read_csv('b3_analysis/volume_mercados/volume_mercado_bdr.csv')
        df_FIIs = pd.read_csv('b3_analysis/volume_mercados/volume_mercado_FIIs.csv')
        df_frac = pd.read_csv('b3_analysis/volume_mercados/volume_mercado_frac.csv')
        df_outros = pd.read_csv('b3_analysis/volume_mercados/volume_mercado_outros.csv')
        df_units = pd.read_csv('b3_analysis/volume_mercados/volume_mercado_units.csv')

        todos_mercados_volume2000_2026 = (
            df_ONPNBR
            .merge(df_avista, on="ano", how="left")
            .merge(df_bdr, on="ano", how="left")
            .merge(df_FIIs, on="ano", how="left")
            .merge(df_frac, on="ano", how="left")
            .merge(df_outros, on="ano", how="left")
            .merge(df_units, on="ano", how="left")
        )

        todos_mercados_volume2000_2026.to_csv('b3_analysis/volume_mercados/todos_mercados_volume2000_2026.csv', index = False)
        return

    return (calculaVOLTOT_AnoTodosMercados,)


@app.cell
def _(calculaVOLTOT_AnoTodosMercados):
    calculaVOLTOT_AnoTodosMercados()
    return


@app.cell
def _(go, pd):
    # gráfico com plotly graph_objects (volume financeiro dos mercados da B3 (2000-2026))

    def plotlyVolumeFinMercadosB3():

        df = pd.read_csv('b3_analysis/volume_mercados/todos_mercados_volume2000_2026.csv')

        # Criação da figura
        fig = go.Figure()

        # série acaoONPNBR
        fig.add_trace(go.Scatter(
            x=df['ano'],
            y=df['acaoONPNBR'],
            mode='lines+markers',
            name='ações ON PN',
            line=dict(color='red')
        ))

        # série avista
        fig.add_trace(go.Scatter(
            x=df['ano'],
            y=df['avista'],
            mode='lines+markers',
            name='Mercado à vista',
            line=dict(color='blue')
        ))

        # série bdr
        fig.add_trace(go.Scatter(
            x=df['ano'],
            y=df['bdr'],
            mode='lines+markers',
            name='BDRs',
            line=dict(color='green')
        ))

        # série FIIs
        fig.add_trace(go.Scatter(
            x=df['ano'],
            y=df['FIIs'],
            mode='lines+markers',
            name='FIIs',
            line=dict(color='orange')
        ))

        # série frac
        fig.add_trace(go.Scatter(
            x=df['ano'],
            y=df['frac'],
            mode='lines+markers',
            name='Fracionário',
            line=dict(color='purple')
        ))

        # série outros
        fig.add_trace(go.Scatter(
            x=df['ano'],
            y=df['outros'],
            mode='lines+markers',
            name='Outros Papeis',
            line=dict(color='brown')
        ))

        # série units
        fig.add_trace(go.Scatter(
            x=df['ano'],
            y=df['units'],
            mode='lines+markers',
            name='Units',
            line=dict(color='grey')
        ))

        # Ajusta layout (equivalente ao plt.figure, plt.title, plt.xlabel, plt.ylabel, plt.grid, plt.legend)
        fig.update_layout(
            title='Volumes Financeiros dos mercados da B3 de 2000 a 2026',
            xaxis=dict(
                title='Ano',
                tickangle=45,   # rotação dos rótulos do eixo x
                showgrid=True,   # ativa a grade vertical

            ),
            yaxis=dict(
                title='Volume Financeiro em R$',
                showgrid=True,   # ativa a grade horizontal
                # type='log'     # para ver melhor as alterações nos 'menores volumes'
            ),
            legend=dict(
                title='',       # legenda sem título
                orientation='h', # horizontal (pode ser 'v' para vertical)
                x=0.5,
                xanchor='center',
                y=-0.2
            ),
            width=1000,  # equivalente ao figsize=(10,5)
            height=700
        )

        #fig.show()
        fig.show(renderer="vscode")

    return (plotlyVolumeFinMercadosB3,)


@app.cell
def _(plotlyVolumeFinMercadosB3):
    plotlyVolumeFinMercadosB3()
    return


@app.cell
def _(pd):
    # uma função para corrigir os códigos de ações (CODNEG) a partir de 2025-09-01, para os casos de EMBR3 e ELET3, que mudaram para EMBJ3 e AXIA3, respectivamente.
    # não afeta nenhum valor da ação só o código para consist~^encia das análises, já que, a partir de setembro de 2025, os códigos mudaram, mas são as mesmas ações (EMBR3 -> EMBJ3 e ELET3 -> AXIA3)
    # mesmo assim, na função leCotAcao, é feita nova checagem para garantir que os dados sejam consistentes.

    def corrigir_codneg_2025(filepath):
        """
        Lê o arquivo CSV especificado, aplica correções em 'CODNEG' para registros a partir de 2025-09-01,
        e salva o arquivo modificado no mesmo local.

        Correções:
        - 'EMBR3' -> 'EMBJ3'
        - 'ELET3' -> 'AXIA3'

        :param filepath: Caminho completo para o arquivo CSV.
        """
        df = pd.read_csv(filepath)

        # Converter 'DATAPREG' para datetime se necessário (assumindo formato YYYY-MM-DD)
        df['DATAPREG'] = pd.to_datetime(df['DATAPREG'])

        # Filtrar linhas a partir de 2025-09-01
        mask = df['DATAPREG'] >= '2025-09-01'

        # Aplicar correções
        df.loc[mask & (df['CODNEG'] == 'EMBR3'), 'CODNEG'] = 'EMBJ3'
        df.loc[mask & (df['CODNEG'] == 'ELET3'), 'CODNEG'] = 'AXIA3'

        # Salvar o DataFrame modificado de volta no arquivo
        df.to_csv(filepath, index=False)
        print(f"Arquivo '{filepath}' corrigido e salvo.")

    return (corrigir_codneg_2025,)


@app.cell
def _(corrigir_codneg_2025):
    #  executado para corrigir os códigos de ações EMBR3 e ELET3 para os códigos novos, a partir de 2025-09-01, no arquivo do ano de 2025
    # não afeta nenhum dados/valor só o código da ação (CODNEG) para os registros a partir de 2025-09-01
    corrigir_codneg_2025(r'D:\marimo01_teste\\b3_csv\mercado_acaoONPNBR\mercado_acaoONPNBR2025.csv')
    return


@app.cell
def _(pd):
    # função para ler ranking de acoes do ibov em "b3_analysis/b3_rankingTop20/rk_ibov{ano_mes}.csv"
    # retorna dataframe com código da ação (codigo) e a porcentagem (porc_indice) do ibov que é referente a essa acao

    def leListaRkAcao (ano_mes):
        path_rk = f"b3_analysis/b3_rankingTop20/rk_ibov{ano_mes}.csv"
        df_rk = pd.read_csv(path_rk)
        return df_rk

    return


@app.cell
def _(pd):
    # le (nas pastas csv) o **ano** (não implementei mes e dia), acao por acao, do mercado_avista  (ou outro mercado)
    # e grava topAAAAA_acao.csv em 'b3_analysis/acoesTop20_cotacoes/top{ano}_{acao}.csv'
    # função utilizada junto com função 'LeTodasAcoesRankingAnoMes' para gravar as cotações das ações listadas no rankingTop20

    def leCotAcao(acao, inicio, fim, mercado):
        ano = inicio[:4]  # le o ano da string(inicio) AAAA-MM-DD 
        path_onpnbr = f"b3_csv/{mercado}/{mercado}{ano}.csv" 
        df_cotacoes = pd.read_csv(path_onpnbr)
        df_cotacoes['DATAPREG'] = pd.to_datetime(df_cotacoes['DATAPREG'], format='%Y-%m-%d')  # Descomentado para converter para datetime
        selected_col = ['DATAPREG', 'PREULT']

        # Mapeamento para ações que mudaram de código a partir de 2025-09-01
        mapeamento_codigos = {
            'EMBR3': ['EMBR3', 'EMBJ3'],  # EMBRAER mudou de EMBR3 para EMBJ3
            'ELET3': ['ELET3', 'AXIA3']   # ELETROBRAS mudou de ELET3 para AXIA3
        }

        # Converter inicio para datetime para comparação consistente
        inicio_dt = pd.to_datetime(inicio)

        if acao in mapeamento_codigos:
            # Procurar por qualquer um dos códigos
            df_acao = df_cotacoes[df_cotacoes['CODNEG'].isin(mapeamento_codigos[acao]) & (df_cotacoes['DATAPREG'] >= inicio_dt)][selected_col]
        else:
            # Procurar pelo código normal
            df_acao = df_cotacoes[(df_cotacoes['CODNEG'] == acao) & (df_cotacoes['DATAPREG'] >= inicio_dt)][selected_col]

        df_acao.to_csv(f"b3_analysis/acoesTop20_cotacoes/top{ano}_{acao}.csv", index=False)
        return

    return (leCotAcao,)


@app.cell
def _(leCotAcao, pd):
    # lê todas as acoes do ranking de 202602 (lido acima)
    # obs: e a BPAC11 que é uma unit (do banco BTG) - lê no mercado_units

    def LeTodasAcoesRankingAnoMes(rk_anoMes):

        ano = rk_anoMes[:4]
        mes = rk_anoMes[4:]
        inicio = f"{ano}-{mes}-01"  # Sempre começa no dia 1 do mês

        path_leitura = f"b3_analysis/b3_rankingTop20/rk_ibov{rk_anoMes}.csv"
        df_ranking = pd.read_csv(path_leitura)
        for acao in df_ranking['codigo']:
            print(acao)
            if acao == 'BPAC11': # BAPC11 é uma unit (ON + PN), única no ranking ibov
                leCotAcao(acao,inicio,'a','mercado_units')
            else:
                leCotAcao(acao,inicio,'a','mercado_acaoONPNBR')

    return (LeTodasAcoesRankingAnoMes,)


@app.cell
def _(LeTodasAcoesRankingAnoMes):
    quero_de = '202601'
    LeTodasAcoesRankingAnoMes(quero_de)
    return


@app.cell
def _():
    #
    # ATENÇÃO - NA PASTA TOP20 DO ANO ***2025*** EMBR3 -> EMBJ3 (EMBRAER MUDOU SIMBOLO); ELET3 -> AXIA3 (ELETROBRAS MUDOU SIMBOLO)
    # ficou com top 21 acoes porque SUZB3 e VBBR3 andaram trocando ultimas posicoes
    #
    return


@app.cell
def _(os, pd):
    # concatena todas as cotações de fechamento, por ação, das top20 do ano de 2025 e 2026, na pasta '/b3_analysis/acoesTop20_cotacoesto/p2526'
    # as top20 estão na pasta '/b3_analysis/acoesTop20_cotacoes'

    def concatenaTop20de2025e26():
        path_top = f"b3_analysis/acoesTop20_cotacoes/"
        lista_arq = [f for f in os.listdir(path_top) if os.path.isfile(os.path.join(path_top, f))]
        lista25 = []
        lista26 = []
        listaTop = []
        for item in lista_arq:
            if (item[3:7] == '2025'):
                lista25.append(item)
            elif (item[3:7] == '2026'):
                lista26.append(item)

        path2526 = f"b3_analysis/acoesTop20_cotacoes/top2526"

        for item1 in lista25:
            for item2 in lista26:
                if item1[8:13] == item2[8:13]:
                    df1 = pd.read_csv(f"{path_top}{item1}")
                    df2 = pd.read_csv(f"{path_top}{item2}")
                    df3 = pd.concat([df1,df2])
                    acao = f"{path2526}/{item1[8:13]}.csv"

                    df3.to_csv(acao, index=False)

    return (concatenaTop20de2025e26,)


@app.cell
def _(concatenaTop20de2025e26):
    concatenaTop20de2025e26()
    return


@app.cell
def _(glob, os, pd):
    # Carregar pesos (no índice ibov) das TOP 20 ações do IBOV (referência: fevereiro 2026)
    # cria um dicionário com nome da ação e seu peso (porcentual) no indice ibov (das TOP20) da pasta 'b3_analysis/b3_rankingTop20'

    def carregaTop21Pesos():
        ranking_file = 'b3_analysis/b3_rankingTop20/rk_ibov202602.csv'
        df_ranking = pd.read_csv(ranking_file)
        top21 = df_ranking['codigo'].tolist()
        pesos = dict(zip(df_ranking['codigo'], df_ranking['porc_indice'] / 100))  # pesos em decimal

        #print("TOP 21 ações e seus pesos:")
        #for acao, peso in pesos.items():
        #    print(f"{acao}: {peso:.4f}")
        return (top21, pesos)

    # Carregar cotações das TOP 20 ações (21 porque teve troca de ação nas últimas posições do ranking)

    def carregaCotacoesTop2526():

        cotacoes_dir = 'b3_analysis/acoesTop20_cotacoes/top2526'
        all_files = glob.glob(os.path.join(cotacoes_dir, "*.csv"))

        top21, pesos = carregaTop21Pesos()  # Carrega as TOP 21 ações e seus pesos no índice ibov

        df_cotacoes = {}
        for file in all_files:
            acao = os.path.basename(file)[:-4]
            if acao in top21:
                df = pd.read_csv(file)
                df['DATAPREG'] = pd.to_datetime(df['DATAPREG'])
                df.set_index('DATAPREG', inplace=True)
                df_cotacoes[acao] = df['PREULT']

                # print(f"Carregadas cotações para {len(df_cotacoes)} ações.")
        return (df_cotacoes)

    return carregaCotacoesTop2526, carregaTop21Pesos


@app.cell
def _(os, pd):
    # Carregar evolução diária do IBOV

    def load_ibov_data(year, month=None):

        ibov_dir = 'b3_csv/b3_ibov/ibovEvolucaoDiaria_brcsv'
        if month:
            file = f'Evolucao_Diaria{year}{month:02d}.csv'
        else:
            file = f'Evolucao_Diaria{year}.csv'
        path = os.path.join(ibov_dir, file)
        df = pd.read_csv(path)

        # Pivotar para série temporal
        df_melted = df.melt(id_vars=['Dia'], var_name='Mes', value_name='IBOV')
        df_melted = df_melted.dropna()
        df_melted['Mes_num'] = df_melted['Mes'].map({'Jan':1, 'Fev':2, 'Mar':3, 'Abr':4, 'Mai':5, 'Jun':6, 'Jul':7, 'Ago':8, 'Set':9, 'Out':10, 'Nov':11, 'Dez':12})
        df_melted['Data'] = pd.to_datetime(f'{year}-' + df_melted['Mes_num'].astype(str) + '-' + df_melted['Dia'].astype(str))
        df_melted.set_index('Data', inplace=True)

        return df_melted['IBOV']

    # print(f"IBOV carregado de {ibov.index.min()} a {ibov.index.max()}")
    # IBOV carregado de 2025-01-02 00:00:00 a 2026-02-27 00:00:00
    return (load_ibov_data,)


@app.cell
def _(carregaCotacoesTop2526, carregaTop21Pesos, load_ibov_data, pd):
    # Função de cálculo dos retornos diários TOP21 e IBOV (de 2025-2026)

    def calc_retornos_diarios_top21_ibov():

        # carregas as cotações das ações TOP21 de 2025e2026 da pasta 'b3_analysis/acoesTop20_cotacoes/top2526'
        df_cotacoes = carregaCotacoesTop2526()
        retornos_acoes = {}

        for acao, preco in df_cotacoes.items():
            retornos_acoes[acao] = preco.pct_change()

        # Carregar dados de variação do ibov de 2025 e 2026 da pasta 'b3_csv/b3_ibov/ibovEvolucaoDiaria_brcsv'
        ibov_2025 = load_ibov_data(2025)
        ibov_2026 = load_ibov_data(2026, 2)
        ibov = pd.concat([ibov_2025, ibov_2026]).sort_index()

        # retorno do ibov
        retorno_ibov = ibov.pct_change()

        # Calcula o retorno ponderado das TOP 21

        df_retornos = pd.DataFrame(retornos_acoes)
        df_retornos = df_retornos.dropna()  # remover dias sem dados para todas

        # Aplicar pesos que estão em 'b3_analysis/b3_rankingTop20/rk_ibov202602.csv'
        top21, pesos = carregaTop21Pesos()

        #retorno do TOP21
        retorno_top21 = (df_retornos * pd.Series(pesos)).sum(axis=1)

        return(retorno_ibov, retorno_top21, ibov)

    return (calc_retornos_diarios_top21_ibov,)


@app.cell
def _(calc_retornos_diarios_top21_ibov):
    # função para cálculo do retorno acumulado

    def calc_retornos_acumulados_top21_ibov():

        retorno_ibov, retorno_top21, ibov = calc_retornos_diarios_top21_ibov()

        # retornos acumulados do TOP21 e do ibov
        retorno_top21_acum = (1 + retorno_top21).cumprod() - 1
        retorno_ibov_acum = (1 + retorno_ibov).cumprod() - 1

        return (retorno_top21_acum, retorno_ibov_acum)

    return (calc_retornos_acumulados_top21_ibov,)


@app.cell
def _(pd):
    # Função do gráfico para mostrar a contribuição das TOP21 no índice IBOV

    def contribuicao_TOP21_IBOV( ret_TOP21ac, ret_IBOVac):

        r_TOP21ac = ret_TOP21ac
        r_IBOVac =  ret_IBOVac

        # Combinar em um DataFrame para plotar
        df_plot = pd.DataFrame({
            'IBOVac': r_IBOVac,
            'TOP21ac': r_TOP21ac
        }).dropna()

        # Gráfico com Plotly
        import plotly.graph_objects as go

        fig1 = go.Figure()
        fig1.add_trace(go.Scatter(x=df_plot.index, y=df_plot['IBOVac'], mode='lines', name='IBOV ac'))
        fig1.add_trace(go.Scatter(x=df_plot.index, y=df_plot['TOP21ac'], mode='lines', name='TOP 21 Ações'))
        fig1.update_layout(title='Retorno Acumulado: IBOV vs TOP 21 Ações',
                          xaxis_title='Data',
                          yaxis_title='Retorno Acumulado')
        #fig1.show()
        fig1.show(renderer="vscode")

        # Calcular contribuição: diferença entre IBOV e TOP21
        contribuicao = df_plot['IBOVac'] - df_plot['TOP21ac']
        fig2 = go.Figure()
        fig2.add_trace(go.Scatter(x=contribuicao.index, y=contribuicao, mode='lines', name='Contribuição das Outras Ações'))
        fig2.update_layout(title='Diferença no Retorno Acumulado (IBOVac - TOP21ac)',
                           xaxis_title='Data',
                           yaxis_title='Diferença')
        #fig2.show()
        fig2.show(renderer="vscode")

    return (contribuicao_TOP21_IBOV,)


@app.cell
def _(calc_retornos_acumulados_top21_ibov, contribuicao_TOP21_IBOV):
    # Gráfico para mostrar a contribuição das TOP21 no índice IBOV

    ret_TOP21ac,ret_IBOVac = calc_retornos_acumulados_top21_ibov()

    contribuicao_TOP21_IBOV( ret_TOP21ac, ret_IBOVac)
    return


@app.cell
def _(pd, px):
    # Função do gráfico para mostrar a correlação entre retornos diários do TOP21 e IBOV
    # Combina retornos em um DataFrame

    def plotlyCorrelacao_TOP21_IBOV(retorno_top21, retorno_ibov):

        df_retornos_combinados = pd.DataFrame({
            'TOP21': retorno_top21,
            'IBOV': retorno_ibov
        }).dropna()

        correlacao = df_retornos_combinados['TOP21'].corr(df_retornos_combinados['IBOV'])
        print(f"Correlação de Pearson entre retornos diários do TOP21 e IBOV: {correlacao:.4f}")

        # Gráfico scatter plot com linha de regressão

        fig_corr = px.scatter(df_retornos_combinados, x='TOP21', y='IBOV', trendline='ols',
                              title=f'Correlação entre Retornos Diários: TOP21 vs IBOV (Corr = {correlacao:.4f})',
                              labels={'TOP21': 'Retorno Diário TOP21', 'IBOV': 'Retorno Diário IBOV'})
        #fig_corr.show()
        fig_corr.show(renderer="vscode")

    return (plotlyCorrelacao_TOP21_IBOV,)


@app.cell
def _(calc_retornos_diarios_top21_ibov, plotlyCorrelacao_TOP21_IBOV):
    # gráfico da correlação entre retornos diários do TOP21 e IBOV

    retIbov, retTop21, ibov = calc_retornos_diarios_top21_ibov()

    plotlyCorrelacao_TOP21_IBOV(retTop21, retIbov)
    return


@app.cell
def _(
    calc_retornos_diarios_top21_ibov,
    carregaCotacoesTop2526,
    carregaTop21Pesos,
    go,
    make_subplots,
    pd,
):
    # Criar gráfico de influência das TOP 21 ações no IBOV, similar ao analise_influencia_top20_ibov.py

    def plotlyGraficoInfluenciaTop21_IBOV():

        # Definir período inicial como 2025-09-01
        DATE_INI = pd.Timestamp("2025-09-01")

        # carregando variáveis: top21, pesos, df_cotacoes, ibov
        top21, pesos = carregaTop21Pesos()
        df_cotacoes = carregaCotacoesTop2526 ()
        ret_ibov, ret_top21, ibov = calc_retornos_diarios_top21_ibov()

        # Filtrar IBOV a partir de DATE_INI
        ibov_filtrado = ibov[ibov.index >= DATE_INI]

        # Calcular contribuições individuais no período filtrado
        retornos_individuais = {}
        for acao in top21:
            if acao in df_cotacoes:
                preco = df_cotacoes[acao]
                preco_filtrado = preco[preco.index >= DATE_INI]
                if not preco_filtrado.empty:
                    ret = (preco_filtrado.iloc[-1] - preco_filtrado.iloc[0]) / preco_filtrado.iloc[0] * 100
                    retornos_individuais[acao] = ret

        # Contribuição em p.p. = retorno * peso / 100
        contribuicoes = {}
        for acao in top21:
            if acao in retornos_individuais:
                contrib = retornos_individuais[acao] * pesos[acao]
                contribuicoes[acao] = contrib

        df_contrib = pd.DataFrame(list(contribuicoes.items()), columns=['ticker', 'contribuicao_pp'])
        df_contrib = df_contrib.sort_values('contribuicao_pp', ascending=False)

        # Retorno do IBOV no período filtrado
        retorno_ibov_total = (ibov_filtrado.iloc[-1] - ibov_filtrado.iloc[0]) / ibov_filtrado.iloc[0] * 100

        # Soma das contribuições
        soma_contrib = df_contrib['contribuicao_pp'].sum()

        # Gráfico com subplots

        cores_contrib = ["#2ecc71" if v >= 0 else "#e74c3c" for v in df_contrib["contribuicao_pp"]]

        fig_influencia = make_subplots(
            rows=3, cols=1,
            subplot_titles=(
                "Contribuição de cada ação TOP21 para a variação do IBOV (p.p.)",
                "Retorno individual das ações × Peso no índice",
                "IBOV diário no período",
            ),
            row_heights=[0.38, 0.35, 0.27],
            vertical_spacing=0.12,
        )

        # Subplot 1: contribuição em p.p.
        fig_influencia.add_trace(
            go.Bar(
                x=df_contrib["ticker"],
                y=df_contrib["contribuicao_pp"],
                marker_color=cores_contrib,
                text=[f"{v:+.2f} p.p." for v in df_contrib["contribuicao_pp"]],
                textposition="outside",
                hovertemplate=(
                    "<b>%{x}</b><br>"
                    "Contribuição: %{y:+.3f} p.p.<br>"
                    "<extra></extra>"
                ),
                cliponaxis=False,
                name="Contribuição (p.p.)",
            ),
            row=1, col=1,
        )

        # Linha da variação real do IBOV
        fig_influencia.add_hline(
            y=retorno_ibov_total, line_dash="dash", line_color="#2980b9", line_width=2,
            annotation_text=f"IBOV real: {retorno_ibov_total:+.2f}%",
            annotation_position="top right",
            row=1, col=1,
        )

        # Linha da soma das contribuições
        fig_influencia.add_hline(
            y=soma_contrib, line_dash="dot", line_color="#8e44ad", line_width=2,
            annotation_text=f"Σ contribuições TOP21: {soma_contrib:+.2f} p.p.",
            annotation_position="bottom right",
            row=1, col=1,
        )

        # Subplot 2: retorno vs peso (bubble)
        df_bubble = pd.DataFrame({
            'ticker': list(retornos_individuais.keys()),
            'retorno_pct': list(retornos_individuais.values()),
            'peso_medio': [pesos[acao] for acao in retornos_individuais.keys()],
            'contribuicao_pp': [contribuicoes[acao] for acao in retornos_individuais.keys()]
        })

        fig_influencia.add_trace(
            go.Scatter(
                x=df_bubble["retorno_pct"],
                y=df_bubble["peso_medio"],
                mode="markers+text",
                text=df_bubble["ticker"],
                textposition="top center",
                textfont=dict(size=10),
                marker=dict(
                    size=df_bubble["contribuicao_pp"].abs() * 12,
                    color=df_bubble["contribuicao_pp"],
                    colorscale="RdYlGn",
                    showscale=True,
                    colorbar=dict(
                        title="Contrib. (p.p.)",
                        x=1.02,
                        len=0.33,
                        y=0.52,
                    ),
                    line=dict(color="white", width=1),
                ),
                hovertemplate=(
                    "<b>%{text}</b><br>"
                    "Retorno: %{x:+.2f}%<br>"
                    "Peso médio: %{y:.2f}%<br>"
                    "<extra></extra>"
                ),
                name="Retorno vs Peso",
            ),
            row=2, col=1,
        )
        fig_influencia.add_vline(x=0, line_color="grey", line_dash="dot", row=2, col=1)

        # Subplot 3: IBOV diário
        fig_influencia.add_trace(
            go.Scatter(
                x=ibov_filtrado.index,
                y=ibov_filtrado.values,
                mode="lines",
                line=dict(color="#2980b9", width=2),
                fill="tozeroy",
                fillcolor="rgba(41,128,185,0.12)",
                hovertemplate="Data: %{x|%d/%m/%Y}<br>IBOV: %{y:,.0f}<extra></extra>",
                name="IBOV",
            ),
            row=3, col=1,
        )

        # Layout global
        pct_explicado = soma_contrib / retorno_ibov_total * 100 if retorno_ibov_total != 0 else 0

        fig_influencia.update_layout(
            title={
                "text": (
                    f"Influência das Ações TOP21 na Valorização do IBOV<br>"
                    f"<sup>Período: {ibov_filtrado.index[0].date()} → {ibov_filtrado.index[-1].date()} | IBOV: {retorno_ibov_total:+.2f}% real | "
                    f"TOP21 explicam {pct_explicado:.1f}% ({soma_contrib:+.2f} p.p.)</sup>"
                ),
                "x": 0.5, "xanchor": "center", "font": {"size": 18},
            },
            template="plotly_white",
            height=950,
            showlegend=False,
            margin=dict(l=70, r=130, t=110, b=60),
        )

        fig_influencia.update_xaxes(tickangle=-35, tickfont=dict(size=11), row=1, col=1)
        fig_influencia.update_yaxes(ticksuffix=" p.p.", row=1, col=1)
        fig_influencia.update_xaxes(title_text="Retorno da ação (%)", ticksuffix="%", row=2, col=1)
        fig_influencia.update_yaxes(title_text="Peso no IBOV (%)", ticksuffix="%", row=2, col=1)
        fig_influencia.update_xaxes(title_text="Data", tickformat="%b/%Y", row=3, col=1)
        fig_influencia.update_yaxes(title_text="Pontos", tickformat=",.0f", row=3, col=1)

        #fig_influencia.show()
        fig_influencia.show(renderer="vscode")

    return (plotlyGraficoInfluenciaTop21_IBOV,)


@app.cell
def _(plotlyGraficoInfluenciaTop21_IBOV):
    plotlyGraficoInfluenciaTop21_IBOV()
    return


if __name__ == "__main__":
    app.run()
