import sys
import boto3
import csv
import io
import json
import logging
import botocore
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from pyspark.context import SparkContext
from pyspark.sql.functions import year, month, to_date, col
from awsglue.job import Job
from pyspark.sql.utils import AnalysisException
from delta.tables import DeltaTable
from pyspark.sql.functions import current_timestamp
# ------------------------------
# Configuração de logs estruturados
# ------------------------------
logger = logging.getLogger()
logger.setLevel(logging.INFO)

def log_info(message, **kwargs):
    log_entry = {"level": "INFO", "message": message, **kwargs}
    logger.info(json.dumps(log_entry))

def log_error(message, **kwargs):
    log_entry = {"level": "ERROR", "message": message, **kwargs}
    logger.error(json.dumps(log_entry))

# ------------------------------
# Parâmetros do Glue Job
# ------------------------------
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'dataset_id'])

# ------------------------------
# Inicializa Spark e Glue
# ------------------------------
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
s3 = boto3.client('s3')

def main(dataset_id):
    try:
        # 1. Carregar tabela de controle
        controle = tabela_de_controle(dataset_id)
        log_info("Tabela de controle carregada.", dataset_id=dataset_id)
    
        # 2. Ler e validar dados da origem
        df = verifica_inconsistencias(controle)
    
        # 4. Extrair partições
        df, particoes = extrai_particoes(df, controle['coluna_particao'], controle['tipo_particao'])
        log_info("Partições extraídas.", particoes=particoes)
    
        # 5. Aplicar query (se existir)
        df = aplica_query(df, controle['query'])
    
        # 6. Gravar os dados
        escreve_dados(df, controle, particoes)
        log_info("Ingestão concluída com sucesso.", modo_ingestao=controle['modo_ingestao'])
        
    except Exception as e:
        log_error("Erro no pipeline.", error=str(e))
        raise e


def tabela_de_controle(dataset_id):
    bucket_name = 'arquivos-controle' # Substitua pelo nome do seu bucket
    object_key = 'arquivo-controle-tabela-democratizacao/arquivo_controle_tabela_democratizacao.csv' # Substitua pelo nome do seu arquivo de controle
    response = s3.get_object(Bucket=bucket_name, Key=object_key)
    csv_content = response['Body'].read().decode('utf-8')
    reader = csv.DictReader(io.StringIO(csv_content))
    for row in reader:
        if row['dataset_id'] == dataset_id:
            # Atribui cada valor às variáveis correspondentes
            bucket_origem = row['bucket_origem']
            caminho_origem = row['caminho_origem']
            bucket_destino = row['bucket_destino']
            caminho_destino = row['caminho_destino']
            formato_origem = row['formato_origem']
            formato_destino = row['formato_destino']
            coluna_particao = row['coluna_particao']
            tipo_particao = row['tipo_particao']
            compressao = row['compressao']
            glue_database = row['glue_database']
            glue_tabela = row['glue_tabela']
            colunas_esperadas = row['colunas_esperadas']
            modo_ingestao = row['modo_ingestao']
            chaves = row['chaves']
            query = row['query']
            # Retorna todas as variáveis como um dicionário
            return {
                'bucket_origem': bucket_origem,
                'caminho_origem': caminho_origem,
                'bucket_destino': bucket_destino,
                'caminho_destino': caminho_destino,
                'formato_origem': formato_origem,
                'formato_destino': formato_destino,
                'coluna_particao': coluna_particao,
                'tipo_particao': tipo_particao,
                'compressao': compressao,
                'glue_database': glue_database,
                'glue_tabela': glue_tabela,
                'colunas_esperadas': colunas_esperadas,
                'modo_ingestao': modo_ingestao,
                'chaves': chaves,
                'query': query,
            }
    raise ValueError(f"dataset_id '{dataset_id}' não encontrado na tabela de controle.")

def verifica_inconsistencias(tabela_de_controle):
    
    # 1. Verifica se o arquivo esta vazio
    try:
        df = spark.read.format(tabela_de_controle['formato_origem']).load(f"s3://{tabela_de_controle['bucket_origem']}/{tabela_de_controle['caminho_origem']}")
        if df.limit(1).count() == 0:
            raise ValueError("O arquivo de origem está vazio.")
        log_info("Arquivo de origem carregado com sucesso.", bucket=tabela_de_controle['bucket_origem'], caminho=tabela_de_controle['caminho_origem'])
    except AnalysisException as e:
        log_error("Erro ao carregar o arquivo de origem.", error=str(e))
        raise ValueError(f"Erro ao carregar o arquivo de origem: {str(e)}") from e
    
    # 2. Verifica se as colunas esperadas foram definidas
    if not tabela_de_controle['colunas_esperadas']:
        raise ValueError("Colunas esperadas não definidas no arquivo de controle.")

    colunas_esperadas = [c.strip() for c in tabela_de_controle['colunas_esperadas'].split(',')]
    colunas_arquivo = df.columns

    # 1. Verifica colunas
    if colunas_arquivo != colunas_esperadas:
        log_error(
            "Colunas inconsistentes.",
            colunas_arquivo=colunas_arquivo,
            colunas_esperadas=colunas_esperadas
        )
        raise ValueError("Colunas do arquivo de dados não batem com as colunas esperadas no arquivo de controle.")

    log_info("Colunas e ordem conferem com as colunas esperadas.", 
            colunas_arquivo=colunas_arquivo, 
            colunas_esperadas=colunas_esperadas)

    return df


def extrai_particoes(df, coluna_particao, tipo_particao):
    particoes = []
    if tipo_particao == 'data':
        df = df.withColumn('ano', df[coluna_particao].substr(1, 4)) \
                 .withColumn('mes', df[coluna_particao].substr(6, 2))
        particoes = ['ano', 'mes']
    elif tipo_particao == 'normal':  
        df = df     
        particoes = coluna_particao.split(',')
    else:
        particoes = []
    return df, particoes
    
def aplica_query(df, query):
    if query and query.strip().lower() != 'na':
        try:
            df.createOrReplaceTempView("tabela_temp")
            df = spark.sql(query)
            log_info("Query aplicada com sucesso.", query=query)
        except Exception as e:
            log_error("Erro ao aplicar a query.", error=str(e), query=query)
            raise ValueError(f"Erro ao aplicar a query: {str(e)}") from e
    else:
        log_info("Nenhuma query aplicada.")
    return df    
    
def escreve_dados(df, tabela_de_controle, particoes):
    
    modo = tabela_de_controle['modo_ingestao'].lower()
    if modo not in ['overwrite', 'append', 'upsert']:
        raise ValueError(f"Modo de ingestão '{modo}' não suportado.")
    
    
    if tabela_de_controle['modo_ingestao'] == 'overwrite':
        df.write \
            .partitionBy(particoes) \
            .mode('overwrite') \
            .format(tabela_de_controle['formato_destino']) \
            .save(f"s3://{tabela_de_controle['bucket_destino']}/{tabela_de_controle['caminho_destino']}")
    elif tabela_de_controle['modo_ingestao'] == 'append':
        df.write \
            .partitionBy(particoes) \
            .mode('append') \
            .format(tabela_de_controle['formato_destino']) \
            .save(f"s3://{tabela_de_controle['bucket_destino']}/{tabela_de_controle['caminho_destino']}")
    elif tabela_de_controle['modo_ingestao'] == 'upsert':
        
        delta_table = DeltaTable.forPath(spark, f"s3://{tabela_de_controle['bucket_destino']}/{tabela_de_controle['caminho_destino']}")
        
        # Criar a condição do merge dinamicamente
        merge_condition = " AND ".join([f"target.{c} = source.{c}" for c in tabela_de_controle['chaves']])
        
        delta_table.alias("target").merge(
            df.alias("source"),
            merge_condition
        ).whenMatchedUpdateAll() \
        .whenNotMatchedInsertAll() \
        .execute()
    else:
        raise ValueError(f"Modo de ingestão '{tabela_de_controle['modo_ingestao']}' não suportado.")
    
        
if __name__ == "__main__":
    dataset_id = args['dataset_id']  # passado como parâmetro do Glue Job
    main(dataset_id)
    job.commit()  # Finaliza o Glue Job

