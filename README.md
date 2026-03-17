# Projeto B3 Data Analysis - Análise Exploratória de dados da B3

## Preambulo da VERSÃO 1.0 marimo (+ environment uv) idêntica à versão 4 do jupyter notebook

O preâmbulo abaixo refere-se à versão 4.0 com jupiter notebook também disponível no github (repositório: 'B3_analiseDadosV3').

A estrutura dos diretórios é idêntica. 

Primeiras impressões do marimo são muito boas, muito mais rápido, 'inteligente' para ver dependências que faltam e sugere sua instalação e após roda novamente a celula. O uv trata disso automaticamente.

Observação: instalei o uv, criei um ambiente virtual com uv, e dentro instalei o marimo e tudo roda "redondo".

Só que ainda estou com problemas com a integração com o copilot Pro dentro do marimo. O modelo de código ok, mas o modelo de chat está indicando 'estouro de tokens', o que não é possível. A verificar...

Para quem usa o jupyter, a 'curva de aprendizado' será quase 'transparente' com ressalvas para as configurações de AI, MCP, etc. Mas, dependendo do background de cada um, também poderá será um 'piece of cake'.

FIM.

## Abaixo o preambulo da versão 4.0 (do jupyter notebook)

Acabei de, praticamente, reescrever 70%, ou mais, do código, porque o estou deixando mais 'friendly' para importá-lo para o marimo (novo notebook que estou testando. Ambiente virtual c/uv: marimo + uv, para testar opção de substituição de anaconda, conda, jupyter e pip).

O código foi todo estruturado em funções, o que também facilita, qualquer um, utilizar partes, que forem mais úteis.

Tem também um arquivo 'LEIAME_funcoes.md', criado pelo "meu amigo" Grok Code Fast 1, com a lista de todas as funções do notebook.

O environment que utilizei também está, agora, disponível. É o 'python2026-torchStream.yaml" que uso com conda e gerenciado no Anaconda.
É um "super-dimensionado" porque uso para outras "brincadeiras", também. Não precisa de "tudo isso"...

Bom divertimento!


## Descrição

Este projeto realiza análise exploratória e visualização de dados da Bolsa de Valores Brasileira (B3), focando no índice IBOV (Índice Bovespa) e nas ações que o compõem. O notebook principal `B3_analiseDadosV3.ipynb` analisa a influência das 21 ações TOP do IBOV na variação do índice, utilizando dados históricos de cotações e rankings mensais.

O ojetivo era verificar o porquê do aumento agressivo do indice IBOV de setembro de 2025 para 2026. No entanto, ao aprofundar um pouco mais na quantidade de dados disponibilizados pela B3, foi-se estendendo mais, a análise, e tem uma série de questões e inclusive técnicas, por exemplo, usando pandas e pyarrow/parquet mostrando a abismal diferença de tempo de execução de rotinas/funções.

O projeto calcula retornos diários, correlações, retornos acumulados e gera gráficos interativos com Plotly para ilustrar a contribuição das ações TOP no desempenho do IBOV. Isto, além de outras 'perguntas', como foi a variação da quantidade de ações (ON e PN) ao longo dos últimos 20 anos, como foi o comportamento do volume financeiro, etc.etc. Tem dados da participação estrangeira na bolsa na pasta '\b3_analysis\b3-relatoriosDadosMercadoDaB3', foi verificado que existem códigos novos do CODBDI que não estão no doc. ofical da B3 'SeriesHistoricas_Layout-2017.pdf' (pasta:/b3_docs), nem com ajuda de AI, Copilot e Gemini, estes conseguiram gerar uma tabela atualizada que fosse unânime, etc,etc,etc

Para todas as conversões zip->csv e depois ->parquet, pickle, csv, e salvas nas pastas corretas (formato/mercado_xxx) tem rotinas/funções prontas no jupyter notebook.

A proposta, aqui, é disponibilizar várias funções, para outras ensaios que façam, mostrar alguns insights, e também, alguns detalhes não documentados ou desatualizados, como códigos (categórios e/ou numéricos) que são utilizados para tipificar o ticker (papel/ação), assim como não houve a preocupação em fazer uma 'análise exploratória' como 'deve ser', iniciando com 'limpeza', verificação de valores nulos, faltantes, outliers, etc., indo direto para "exploração" e extração de insights.

## Umas dicas: 
"FIRST THINGS FIRST": Para iniciar, deve fazer o download dos arquivos históricos COTAHIST da B3 (zipados) e colocar na pasta 'b3_zip' (link está abaixo). Aqui só tem alguns dos arquivos para demonstrar como ficam as pastas 'populadas' com os dados. 

Se mantiver todos os zips (de 2000 a 2026), mais tudo (copias) em csv, parquet e picklet, vai ocupar em torno de 15 GB. 

Só o número total de registros de operações, dos 27 anos (2000-2026), da pasta '/b3_data', são +/- 20 milhões de operações.

As funções do notebook fazem tudo, de descompactar, trocar formatos, dividir em mercados (a vista, FIIs, BDRs, etc...) e colocar na pasta correta. Só precisam dos arquivos zip da B3.

Finalizando as dicas, isto é um trabalho de um completo leigo sobre a B3 com objetivo de conhecer um pouco o funcionamento e entender os tipos de mercado que existem.

Minha conclusão, que pode estar errada, é que a variação do IBOV é devido principalmente a 20 ações, embora façam parte do IBOV, de 2026, 79 ações, que o investidor estrangeiro representa entre 50% e 60% da fatia do mercado (/b3_analysis/b3-relatoriosDadosMercadoDaB3/05-FatiaDeInvestidores-mesAmes.csv), que as ações 'unicas' eram, em 2000, 661, e agora, em 2026, são 372, etc.etc.etc....

## Estrutura do Projeto

```
B3_dataV2/
├── B3_analiseDadosV3.ipynb          # Notebook principal com análises e gráficos
├── README.md                  # Este arquivo
├── b3_analysis/
│   ├── acoesTop20_cotacoes/
│   │   ├── top2526/           # Cotações diárias das TOP 21 ações (2025-2026)
│   │   └── ibov_indice2025-2026.csv  # Dados do IBOV
│   └── b3_rankingTop20/       # Rankings mensais das TOP 20 ações do IBOV
├── b3_csv/                    # primeira pasta a ser populada com dados (descompactados dos zip da B3 para 'b3_data')
│   ├── b3_data/               # Dados gerais da B3 em CSV
│   ├── b3_ibov/
│   │   └── ibovEvolucaoDiaria_brcsv/  # Evolução diária do IBOV (2010-2026), entre outras pastas e informações
│   ├── mercado_acaoONPNBR/    # Mercado de ações ON/PN/BR
│   ├── mercado_avista/        # Mercado à vista
│   ├── mercado_bdr/           # Mercado de BDRs
│   ├── mercado_FIIs/          # Mercado de Fundos Imobiliários
│   ├── mercado_frac/          # Mercado fracionário
│   ├── mercado_outros/        # Outros mercados
│   └── mercado_units/         # Mercado de units
├── b3_parquet/                # Dados em formato Parquet  ('espelho' do csv)                (*obs: 1)
├── b3_pickle/                 # Dados serializados em Pickle ('espelho' do csv)
├── b3_zip/                    # Arquivos ZIP originais - COTAHIST - downloads da B3  (*obs: 2)
└── b3_logs/                   # Logs de execução
```

(*1) arquivos parquet e pickle são criados por funções disponíveis no jupyter notebook ( só precisa dos 'zipados' da B3)

(*2) arquivos 'zip' COTAHIST_Axxxx.zip disponíveis em:
logs históricos da B3 (com opções: série anual ou mensal ou diária) estão disponíveis em (usei anual):
https://www.b3.com.br/pt_br/market-data-e-indices/servicos-de-dados/market-data/historico/mercado-a-vista/series-historicas/




## Dados Utilizados para o cáculo da influência das TOP 20 no índice IBOV

- **Rankings mensais do IBOV**: Arquivos CSV com códigos das ações e porcentagens no índice (ex.: `rk_ibov202509.csv` para setembro 2025).
- **Cotações diárias das ações**: Arquivos CSV com data (`DATAPREG`) e preço de fechamento (`PREULT`) para cada ação TOP.
- **Evolução diária do IBOV**: Arquivos CSV com pontos diários do índice, organizados por mês.

Período analisado: Principalmente de setembro 2025 a fevereiro 2026.

## Notebook B3_analiseDadosV3.ipynb

### Visão Geral
O notebook é dividido em células que carregam dados, processam cálculos e geram visualizações. Ele usa bibliotecas como Pandas, pyArrow, Plotly e NumPy para manipulação e gráficos.

### Principais Seções

1. **Imports e Configurações**: Carrega bibliotecas necessárias (pandas, plotly, etc.) e define timezone.

2. **Carregamento de Pesos (TOP 21)**: Lê o ranking de fevereiro 2026 (`rk_ibov202602.csv`) para obter pesos das ações no IBOV.

3. **Carregamento de Cotações**: Carrega preços diários das 21 ações TOP da pasta `top2526`.

4. **Carregamento do IBOV**: Processa arquivos de evolução diária para obter série temporal do índice.

5. **Cálculos de Retornos**:
   - Retornos diários das ações e do IBOV.
   - Retorno ponderado das TOP 21 (baseado nos pesos).

6. **Gráficos**:
   - **Retorno Acumulado**: Compara IBOV vs. portfólio TOP 21.
   - **Diferença Acumulada**: Mostra contribuição das ações fora do TOP 21.
   - **Correlação Scatter Plot**: Retornos diários com linha de regressão.
   - **Gráfico de Influência (Subplots)**: Barras de contribuição, scatter retorno vs. peso, e linha do IBOV diário.

### Como Executar

1. **Pré-requisitos**: (também tem o arquivo requirements.yml do ambiente que usei - não precisa de 'tudo isso', mas...)
   - Python 3.8+
   - Ambiente Conda: `conda activate D:\conda_envs\python2026-torchStream`
   - Bibliotecas: pandas, plotly, numpy, pyarrow, polars, yfinance (instale com `pip install` se necessário).

2. **Execução**:
   - Abra o notebook no VS Code ou Jupyter Lab.
   - Execute as células sequencialmente (algumas células podem demorar devido ao processamento de dados).
   - Os gráficos são interativos e exibidos inline.

3. **Saídas**:
   - Gráficos Plotly interativos.
   - Prints de correlações e estatísticas.

### Interpretações dos Gráficos

- **Retorno Acumulado**: Mostra se as TOP 21 acompanham o IBOV (alta correlação esperada, ~70% do índice).
- **Correlação**: Scatter plot com linha OLS; coeficiente próximo a 1 indica forte influência.
- **Influência**: Quantifica quanto cada ação contribuiu para a variação do IBOV em pontos percentuais.

## Dependências Principais

- pandas
- plotly
- numpy
- pyarrow
- polars (não usado)
- yfinance (opcional)
- matplotlib (opcional)

Instale com pip ou anaconda: (Obs: ainda não testei, mas quero brevemente testar o Marimo e uv, para substituir anaconda, conda, pip e jupyter):

```
pip install pandas plotly numpy pyarrow polars yfinance matplotlib
```
## Atenção para alguns detalhes

**Tickers que mudaram:**

| Antigo | Novo | Evento/detalhe | Data 1 | Data 2 | Observacao |
| --- | --- | --- | --- | --- | --- |
| EMBR3 | EMBJ3 | - | 20251103 | - | - |
| ELET3 | AXIA3 | - | 20251110 | - | - |
| ELET5 | AXIA5 | - | 20251110 | - | - |
| ELET6 | AXIA6 | - | 20251110 | - | - |
| ALSO3 | ALOS3 | - | 20231025 | - | em 2023 |
| JBSS3 | JBSS32 | saiu da B3 / BDR | 20250606 | 20250609 | JBSS32 |
| MRFG3 | MRBRF3 | - | 202509 | - | - |
| BRFS3 | MRBRF3 | 1 BRF = 0.8521 MRF | 20250925 | - | fusao |
| CPFL6 | CPFL3 | unifica em CPFL3 | 20251222 | - | - |
| CPFL5 | CPFL3 | unifica em CPFL3 | - | - | - |



## Licença

Este projeto é para fins de compartilhar conhecimento e é livre para usar, copiar, etc. Dados da B3 são públicos.

## Conclusão

A análise no notebook `B3_analiseDadosV3.ipynb` demonstra que as 21 ações TOP do IBOV, representando aproximadamente 70% do índice, têm uma forte influência em sua variação. Os gráficos mostram correlações elevadas e contribuições significativas, com VALE3 e ITUB4 sendo destaques em peso e impacto.

Para visualizar o resultado final, execute a última célula do notebook, que gera o gráfico de influência completo:

![Gráfico de Influência das TOP 21 Ações no IBOV](images/grafico_influencia_top21_ibov.png)

*Imagem: Gráfico interativo com subplots mostrando contribuição das ações, retorno vs. peso e evolução do IBOV (captura de tela do Plotly).*

