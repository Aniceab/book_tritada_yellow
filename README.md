Validação de Qualidade de Dados e Análise de Táxis Amarelos

Subtítulo:
Automatização de validação e análise de dados armazenados no S3 com integração Hive.

Objetivos da Solução:

Validação de Qualidade de Dados (DQ):

Garantir que os dados atendam a critérios de qualidade:
Validação de schema.
Verificação de valores nulos.
Identificação de duplicatas.
Validação de regras de negócio.
Validação de volumetria (número mínimo de linhas).
Armazenamento Estruturado:

Salvar os resultados da validação em uma estrutura particionada no S3, compatível com Hive.
Análise de Dados:

Responder às perguntas analíticas:
Qual é o valor médio arrecadado por mês por todos os táxis amarelos?
Qual é a média de passageiros por hora e por dia?

Arquitetura da Solução:

Pipeline de Validação de Dados:

Entrada: Arquivos Parquet no S3.
Processamento: Validação de qualidade de dados.
Saída: Resultados da validação salvos no S3.
Estrutura Hive:

Resultados armazenados no S3 em formato Parquet, particionados por data.
Consultas Analíticas:

Queries Hive para responder às perguntas analíticas.

Estrutura do Projeto 

book_tritada_yellow/
│
├── component-DQ/
│   ├── src/
│   │   ├── main.py          # Script principal para executar a validação de qualidade de dados
│   │   ├── dq.py            # Módulo com funções de validação de qualidade de dados
│   │   ├── hive.py          # Script para criação da tabela Hive
│   │   └── utils.py         # Funções auxiliares (se necessário)
│   └── tests/
│       └── test_dq.py       # Testes unitários para validação de qualidade de dados
│
├── component-scripts/
│   ├── src/
│   │   ├── main.py          # Script principal para processamento de dados
│   │   ├── process_data.py  # Módulo para processar arquivos Parquet do S3
│   │   └── utils.py         # Funções auxiliares (se necessário)
│   └── tests/
│       └── test_process.py  # Testes unitários para o processamento de dados
│
├── config/
│   ├── config.yaml          # Arquivo de configuração com buckets, prefixos e regras de validação
│
├── dockerfile               # Dockerfile para criar a imagem do projeto
├── requirements.txt         # Dependências do projeto
├── .env                     # Variáveis de ambiente (chaves AWS, etc.)
├── README.md                # Documentação do projeto
└── scripts/
    ├── create_hive_table.sql # Script SQL para criar a tabela Hive
    └── queries.sql           # Queries Hive para análise de dados