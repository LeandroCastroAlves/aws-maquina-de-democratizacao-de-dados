import sys
import json
import logging
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from pyspark.context import SparkContext
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
# Parâmetros do Glue Job passados pela Lambda
# ------------------------------
args = getResolvedOptions(
    sys.argv,
    [
        'JOB_NAME',
        'bucket_origem', 'caminho_origem',
        'bucket_destino', 'caminho_destino',
        'formato_origem', 'formato_destino',
        'coluna_particao', 'tipo_particao',
        'compressao', 'glue_database', 'glue_tabela',
        'colunas_esperadas', 'modo_ingestao',
        'chaves', 'query'
    ]
)

# ------------------------------
# Inicializa Spark e Glue
# ------------------------------
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = glueContext.create_dynamic_frame.from_catalog # Se precisar
job.init(args['JOB_NAME'], args)

# ------------------------------
# Funções de ingestão
# ------------------------------

def verifica_inconsistencias(args):
    try:
        df = spark.read.format(args['formato_origem']) \
            .load(f"s3://{args['bucket_origem']}/{args['caminho_origem']}")
        if df.limit(1).count() == 0:
            raise ValueError("O arquivo de origem está vazio.")
        log_info("Arquivo de origem carregado com sucesso.", bucket=args['bucket_origem'], caminho=args['caminho_origem'])
    except AnalysisException as e:
        log_error("Erro ao carregar o arquivo de origem.", error=str(e))
        raise ValueError(f"Erro ao carregar o arquivo de origem: {str(e)}") from e

    if not args['colunas_esperadas']:
        raise ValueError("Colunas esperadas não definidas.")

    colunas_esperadas = [c.strip() for c in args['colunas_esperadas'].split(',')]
    colunas_arquivo = df.columns
    if colunas_arquivo != colunas_esperadas:
        log_error("Colunas inconsistentes.", colunas_arquivo=colunas_arquivo, colunas_esperadas=colunas_esperadas)
        raise ValueError("Colunas do arquivo de dados não batem com as colunas esperadas.")
    log_info("Colunas e ordem conferem com as colunas esperadas.", colunas_arquivo=colunas_arquivo)
    return df

def extrai_particoes(df, coluna_particao, tipo_particao):
    particoes = []
    if tipo_particao == 'data':
        df = df.withColumn('ano', df[coluna_particao].substr(1,4)) \
               .withColumn('mes', df[coluna_particao].substr(6,2))
        particoes = ['ano', 'mes']
    elif tipo_particao == 'normal':
        particoes = coluna_particao.split(',')
    return df, particoes

def aplica_query(df, query):
    if query and query.strip().lower() != 'na':
        df.createOrReplaceTempView("tabela_temp")
        df = spark.sql(query)
        log_info("Query aplicada com sucesso.", query=query)
    else:
        log_info("Nenhuma query aplicada.")
    return df

def escreve_dados(df, args, particoes):
    modo = args['modo_ingestao'].lower()
    if modo not in ['overwrite','append','upsert']:
        raise ValueError(f"Modo de ingestão '{modo}' não suportado.")

    if modo == 'overwrite':
        df.write.partitionBy(particoes).mode('overwrite').format(args['formato_destino']) \
            .save(f"s3://{args['bucket_destino']}/{args['caminho_destino']}")
    elif modo == 'append':
        df.write.partitionBy(particoes).mode('append').format(args['formato_destino']) \
            .save(f"s3://{args['bucket_destino']}/{args['caminho_destino']}")
    elif modo == 'upsert':
        delta_table = DeltaTable.forPath(spark, f"s3://{args['bucket_destino']}/{args['caminho_destino']}")
        chaves = args['chaves'].split(',') if ',' in args['chaves'] else [args['chaves']]
        merge_condition = " AND ".join([f"target.{c}=source.{c}" for c in chaves])
        delta_table.alias("target").merge(df.alias("source"), merge_condition) \
            .whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

# ------------------------------
# Execução do pipeline
# ------------------------------
def main(args):
    try:
        df = verifica_inconsistencias(args)
        df, particoes = extrai_particoes(df, args['coluna_particao'], args['tipo_particao'])
        df = aplica_query(df, args['query'])
        escreve_dados(df, args, particoes)
        log_info("Ingestão concluída com sucesso.", modo_ingestao=args['modo_ingestao'])
    except Exception as e:
        log_error("Erro no pipeline.", error=str(e))
        raise e

if __name__ == "__main__":
    main(args)
    job.commit()
