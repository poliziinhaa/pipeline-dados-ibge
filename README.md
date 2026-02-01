# Pipeline de Dados IBGE - Estimativa Populacional

## Descrição da Solução
Este projeto implementa um pipeline de Engenharia de Dados ponta a ponta (End-to-End) utilizando **Databricks**, **PySpark** e **Delta Lake**. O objetivo é extrair dados oficiais de estimativa populacional da API do IBGE (SIDRA), processá-los através de uma arquitetura em camadas (Medallion Architecture) e disponibilizar um modelo dimensional (Star Schema) otimizado para Analytics e Business Intelligence.

A solução garante a integridade, rastreabilidade e qualidade dos dados desde a ingestão bruta até o consumo analítico.

---

## Arquitetura da Solução (Medallion)
O projeto segue a arquitetura Medalhão, garantindo a evolução da qualidade, governança e rastreabilidade do dado em cada etapa.

### 1. Camada Bronze (Ingestão)
* **Notebook:** `src/notebooks/bronze/bronze_ibge_6579`
* **Origem:** API REST do IBGE (Tabela 6579).
* **Processo:** Ingestão dos dados brutos e gravação no formato Delta Lake.
* **Estratégia:** Deduplicação total baseada no conteúdo para evitar redundância e adição de colunas de controle.

### 2. Camada Silver (Tratamento e Regras de Negócio)
* **Notebook:** `src/notebooks/silver/silver_ibge_6579`
* **Transformações:**
    * Limpeza de caracteres inválidos (tratamento de "..." como nulo).
    * Conversão segura de tipos de dados (`Try Cast`).
    * Padronização de nomes de colunas para *snake_case*.
* **Lógica de Unicidade:** Aplicação de Window Functions para manter apenas o registro mais recente de cada município/ano, baseado na data de ingestão.
* **Escrita:** Upsert (SCD Tipo 1) para garantir que a camada Silver reflita sempre a versão oficial mais atualizada.

### 3. Camada Gold (Modelagem Dimensional)
* **Notebook:** `src/notebooks/gold/gold_populacao_ibge`
* **Modelo:** Star Schema (Esquema Estrela).
* **Dimensão (`dim_localidade`):** Normalização geográfica com extração e separação da sigla da UF via tratamento de string.
* **Fato (`fct_populacao`):** Tabela transacional contendo chaves estrangeiras, métricas populacionais e colunas de auditoria de todo o pipeline.

---

## Decisões Adotadas (Design Decisions)

### 1. Modularização do Código
A lógica pesada foi abstraída para a pasta `src/utils`. Os notebooks atuam apenas como orquestradores, facilitando testes e manutenção.
* `ibge_api.py`: Conexão resiliente com a API da Sidra.
* `dataframe_utils.py`: Funções puras de transformação.
* `delta_utils.py`: Abstração de escrita e Merge (Upsert).

### 2. Estratégia de Carga (SCD Tipo 1)
Optou-se pelo SCD Tipo 1 (Upsert) nas camadas Silver e Gold para garantir que o Data Warehouse reflita o estado atual dos dados oficiais, corrigindo valores passados caso o IBGE publique revisões, mantendo a simplicidade para o time de BI.

### 3. Modelagem Star Schema na Gold
A separação em Fato e Dimensão foi escolhida para otimizar a performance em ferramentas de visualização (Power BI/Tableau), reduzindo a redundância de textos (nomes de municípios) na tabela de fatos numéricos.

### 4. Job Pipeline
Foi criado um job pipeline para orquestração de todo o processo, a escolha foi semanalmente, pois no póprio site da API, há um texto corrido com todas atualizações (não é sempre, mas ocorre), então dessa forma é possível garantir os dados atualizados. 

---

## Evidências da Arquitetura Medalhão

Abaixo estão as evidências da implementação da arquitetura no Unity Catalog, demonstrando a organização lógica e a linhagem dos dados.

### 1. Estrutura no Unity Catalog (Bronze, Silver, Gold)
*Visualização da organização dos schemas/databases no Databricks.*

![Cole o print da estrutura do Unity Catalog aqui](./Imagens/estrutura_arquitetura_medalhao)

### 2. Data Lineage (Linhagem de Dados)
*Rastreabilidade completa demonstrando o fluxo Origem -> Bronze -> Silver -> Gold.*

![Cole o print da estrutura do Unity Catalog aqui](./Imagens/lineage_projeto)

### 3. Resultados das Tabelas (Amostra de Dados)

#### Tabela Bronze (Dados Brutos)
![Cole o print da estrutura do Unity Catalog aqui](./Imagens/tabela_bronze)

#### Tabela Silver (Dados Tratados)
![Cole o print da estrutura do Unity Catalog aqui](./Imagens/tabela_silver)

#### Tabela Gold (Fato e Dimensão)
![Cole o print da estrutura do Unity Catalog aqui](./Imagens/tabela_dim_localidade)

![Cole o print da estrutura do Unity Catalog aqui](./Imagens/tabela_fct_populacao)

#### Evidência de uma possível visualização em um relatório
![Cole o print da estrutura do Unity Catalog aqui](./Imagens/evidencia_dados)
---

## Estrutura do Projeto (Diretórios)
A estrutura de diretórios foi organizada para separar responsabilidades (código de orquestração vs. bibliotecas reutilizáveis).

![Estrutura de Pastas](./Imagens/organizacao_projeto)

---

## Instruções de Execução

### Pré-requisitos
* Ambiente Databricks com suporte a Unity Catalog (recomendado) ou Hive Metastore.
* Cluster com Databricks Runtime 12.2 LTS ou superior.

### Passo a Passo
1.  **Clone o repositório** no seu Databricks Workspace.
2.  **Execute o pipeline** seguindo a ordem de dependência:
    1.  Execute o notebook `src/notebooks/bronze/bronze_ibge_6579` para ingestão.
    2.  Execute o notebook `src/notebooks/silver/silver_ibge_6579` para tratamento.
    3.  Execute o notebook `src/notebooks/gold/gold_populacao_ibge` para modelagem final.
3.  **Validação:** Consulte as tabelas na camada Gold para verificar os dados agregados e tratados.

---