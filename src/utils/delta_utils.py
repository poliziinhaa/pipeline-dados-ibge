from pyspark.sql import DataFrame, SparkSession
from delta.tables import DeltaTable
from typing import List

def salvar_delta_merge(
    df: DataFrame, 
    tabela_destino: str, 
    chaves_match: List[str],
    modo_carga_inicial: str = "overwrite",
    fazer_update: bool = True
) -> None:
    """
    Função universal para escrita no Delta Lake. Gerencia criação de tabela e estratégias de Merge.
    
    Args:
        df: DataFrame com os dados a serem gravados.
        tabela_destino: Nome completo da tabela.
        chaves_match: Lista de colunas usadas como chave única.
        modo_carga_inicial: 'overwrite' ou 'append'. Usado APENAS se a tabela não existir.
        fazer_update: Define o comportamento se a chave já existir.
                      - True (Upsert): Atualiza os dados existentes (Comum na Silver).
                      - False (Insert Only): Ignora dados existentes e mantém o histórico original (Comum na Bronze/Logs).
    """
    spark = SparkSession.getActiveSession()
    
    print(f"[Delta Utils] Iniciando operacao de escrita em '{tabela_destino}'...")
    
    # Verifica se a tabela já existe
    if not spark.catalog.tableExists(tabela_destino):
        print(f"Tabela nao encontrada. Criando carga inicial (Modo: {modo_carga_inicial})...")
        
        df.write \
            .format("delta") \
            .mode(modo_carga_inicial) \
            .option("mergeSchema", "true") \
            .saveAsTable(tabela_destino)
            
        print("Carga inicial concluida com sucesso.")
        return

    # Se a tabela existe, prepara a lógica do MERGE
    print(f"Tabela encontrada. Executando MERGE. Chaves: {chaves_match} | Update se existir: {fazer_update}")
    
    delta_table = DeltaTable.forName(spark, tabela_destino)
    
    # Monta a condição de igualdade (Null Safe)
    condicao = " AND ".join([f"target.{c} <=> source.{c}" for c in chaves_match])
    
    # Inicia a construção do Merge
    merge_builder = delta_table.alias("target").merge(
        df.alias("source"), 
        condicao
    )
    
    # Lógica Condicional: Adiciona o UPDATE apenas se solicitado
    if fazer_update:
        merge_builder = merge_builder.whenMatchedUpdateAll()
    
    # O INSERT de novos registros é mandatório em qualquer cenário de ingestão
    merge_builder = merge_builder.whenNotMatchedInsertAll()
    
    # Executa a ação final
    merge_builder.execute()
    
    print("Merge finalizado com sucesso.")