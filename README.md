# Pipeline de Dados IBGE - Estimativa Populacional

Este projeto implementa um pipeline de Engenharia de Dados ponta a ponta (End-to-End) utilizando Databricks, PySpark e Delta Lake. O objetivo é extrair dados oficiais de estimativa populacional da API do IBGE (SIDRA), processá-los através de uma arquitetura em camadas (Medallion Architecture) e disponibilizar um modelo dimensional (Star Schema) otimizado para Analytics e Business Intelligence.

## Arquitetura da Solução

O projeto segue a arquitetura Medallion, garantindo a evolução da qualidade, governança e rastreabilidade do dado em cada etapa.

### 1. Camada Bronze (Ingestão)
* **Notebook:** `src/notebooks/bronze/bronze_ibge_6579`
* **Origem:** API REST do IBGE (Tabela 6579).
* **Processo:** Ingestão dos dados brutos e gravação no formato Delta Lake.
* **Estratégia:** Deduplicação total baseada no conteúdo para evitar redundância e adição de colunas de controle (`data_ingestao`).

### 2. Camada Silver (Tratamento e Regras de Negócio)
* **Notebook:** `src/notebooks/silver/silver_ibge_6579`
* **Transformações:**
    * Limpeza de caracteres inválidos (tratamento de "..." como nulo).
    * Conversão segura de tipos de dados (`Try Cast`).
    * Padronização de nomes de colunas para snake_case.
* **Lógica de Unicidade:** Aplicação de Window Functions para manter apenas o registro mais recente de cada município/ano, baseado na data de ingestão.
* **Escrita:** Upsert (SCD Tipo 1) para garantir que a camada Silver reflita a versão oficial mais atualizada.

### 3. Camada Gold (Modelagem Dimensional)
* **Notebook:** `src/notebooks/gold/gold_populacao_ibge`
* **Modelo:** Star Schema (Esquema Estrela).
* **Dimensão (`dim_localidade`):** Normalização geográfica com extração e separação da sigla da UF via tratamento de string.
* **Fato (`fct_populacao`):** Tabela transacional contendo chaves estrangeiras, métricas populacionais e colunas de auditoria de todo o pipeline.

## Estrutura do Projeto

A estrutura de diretórios foi organizada para separar responsabilidades (código de orquestração vs. bibliotecas reutilizáveis.

![image_1769872211931.png](./image_1769872211931.png "image_1769872211931.png")
