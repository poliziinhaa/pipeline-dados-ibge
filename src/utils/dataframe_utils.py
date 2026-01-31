from pyspark.sql import DataFrame
from pyspark.sql.functions import col, count, when, row_number, desc
from typing import List, Dict, Any
from pyspark.sql import Window

def renomear_colunas(df: DataFrame, mapa_de_para: Dict[str, str]) -> DataFrame:
    """
    Renomeia múltiplas colunas de uma vez baseado em um dicionário.
    
    Args:
        df: DataFrame original.
        mapa_de_para: Dicionário onde { "nome_antigo": "nome_novo" }.
    
    Returns:
        DataFrame com colunas renomeadas.
    """
    df_novo = df
    for nome_antigo, nome_novo in mapa_de_para.items():
        df_novo = df_novo.withColumnRenamed(nome_antigo, nome_novo)
    
    return df_novo

def converter_tipo_colunas(df: DataFrame, colunas: List[str], novo_tipo: Any) -> DataFrame:
    """
    Converte uma lista de colunas para um tipo específico de forma segura.
    Se encontrar valores inválidos, transforma em NULL em vez de dar erro.
    
    Args:
        df: DataFrame original.
        colunas: Lista de nomes de colunas.
        novo_tipo: Tipo do Spark (IntegerType(), LongType(), etc).
    """
    df_novo = df
    for c in colunas:
        if c in df.columns:
            df_novo = df_novo.withColumn(c, col(c).try_cast(novo_tipo))
        else:
            print(f" Aviso: A coluna '{c}' não existe no DataFrame. Conversão pulada.")
            
    return df_novo

def excluir_colunas(df: DataFrame, colunas: List[str]) -> DataFrame:
    """
    Remove uma lista de colunas do DataFrame.
    Se a coluna não existir, ela não quebra o código).
    
    Args:
        df: DataFrame original.
        colunas: Lista de strings com os nomes das colunas a remover.
    
    Returns:
        DataFrame sem as colunas especificadas.
    """
    # O asterisco (*) desempacota a lista, pois o drop do PySpark aceita múltiplos argumentos
    return df.drop(*colunas)

# Adicione ao final de src/utils/dataframe_utils.py


def analisar_nulos(df: DataFrame) -> None:
    """
    Imprime um relatório da quantidade e porcentagem de nulos em cada coluna.
    """
    if len(df.columns) == 0:
        print(" O DataFrame está vazio (sem colunas).")
        return

    # Total de linhas 
    total_linhas = df.count()
    
    if total_linhas == 0:
        print(" O DataFrame não possui linhas.")
        return

    print(f"\n Análise de Nulos (Total de linhas: {total_linhas}):")
    print("-" * 50)

    # Monta as expressões de agregação para todas as colunas
    expressoes = [count(when(col(c).isNull(), 1)).alias(c) for c in df.columns]

    # Executa a agregação 
    resultado = df.select(expressoes).collect()[0]

    # Formata a saída
    tem_nulos = False
    for coluna in df.columns:
        qtd_nulos = resultado[coluna]
        
        # Só imprime se tiver nulos 
        if qtd_nulos > 0:
            tem_nulos = True
            porcentagem = (qtd_nulos / total_linhas) * 100
            print(f"{coluna}: {qtd_nulos} nulos ({porcentagem:.2f}%)")
    
    if not tem_nulos:
        print("Nenhuma coluna possui valores nulos. Dados 100% preenchidos!")
    print("-" * 50)

def deduplicar_registros(df: DataFrame, chaves_unicas: List[str], coluna_ordenacao: str) -> DataFrame:
    """
    Remove duplicatas mantendo apenas o registro mais recente baseado na coluna de ordenação.
    
    A lógica é:
    1. Agrupa pelos campos em 'chaves_unicas'
    2. Ordena de forma decrescente pela 'coluna_ordenacao'
    3. Cria um ranking (row_number)
    4. Filtra apenas o número mais recente).
    
    Args:
        df: DataFrame original com possíveis duplicatas.
        chaves_unicas: Lista de colunas que definem a unicidade do registro (Ex: ['id', 'ano']).
        coluna_ordenacao: Nome da coluna temporal para decidir qual fica (Ex: 'data_ingestao').
    
    Returns:
        DataFrame deduplicado.
    """
    print(f"Iniciando deduplicacao. Chaves: {chaves_unicas} | Criterio: Maior '{coluna_ordenacao}'")
    
    # Cria a janela de particionamento
    janela = Window.partitionBy(*chaves_unicas).orderBy(col(coluna_ordenacao).desc())
    
    # Aplica o filtro
    df_dedup = df \
        .withColumn("rn", row_number().over(janela)) \
        .filter(col("rn") == 1) \
        .drop("rn")
        
    return df_dedup