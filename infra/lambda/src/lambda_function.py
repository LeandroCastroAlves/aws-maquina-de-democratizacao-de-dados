import boto3
import csv
import io
import logging
import json

# ------------------------------
# Logs estruturados
# ------------------------------
logger = logging.getLogger()
logger.setLevel(logging.INFO)

def log_info(message, **kwargs):
    logger.info(json.dumps({"level": "INFO", "message": message, **kwargs}))

def log_error(message, **kwargs):
    logger.error(json.dumps({"level": "ERROR", "message": message, **kwargs}))

# ------------------------------
# Inicializa clientes AWS
# ------------------------------
s3 = boto3.client('s3')
glue = boto3.client('glue')

# ------------------------------
# Função para ler tabela de controle
# ------------------------------
def ler_tabela_de_controle():
    bucket = 'arquivos-controle'
    chave = 'arquivo-controle-tabela-democratizacao/arquivo_controle_tabela_democratizacao.csv'
    response = s3.get_object(Bucket=bucket, Key=chave)
    content = response['Body'].read().decode('utf-8')
    return list(csv.DictReader(io.StringIO(content)))

# ------------------------------
# Lambda handler
# ------------------------------
def lambda_handler(event, context):
    try:
        # 1️⃣ Pega o arquivo que caiu no bucket
        nome_arquivo = event['Records'][0]['s3']['object']['key']
        log_info("Arquivo recebido", arquivo=nome_arquivo)

        # 2️⃣ Lê tabela de controle
        tabela_de_controle = ler_tabela_de_controle()

        # 3️⃣ Procura dataset correspondente pelo prefixo do arquivo
        dataset_info = next((row for row in tabela_de_controle if row['prefixo'] in nome_arquivo), None)
        if not dataset_info:
            raise ValueError(f"Nenhum dataset encontrado para o arquivo: {nome_arquivo}")

        # 4️⃣ Dispara Glue passando somente os argumentos necessários
        response = glue.start_job_run(
            JobName=dataset_info['nome_job'],
            Arguments={
                '--bucket_origem': dataset_info['bucket_origem'],
                '--caminho_origem': dataset_info['caminho_origem'],
                '--bucket_destino': dataset_info['bucket_destino'],
                '--caminho_destino': dataset_info['caminho_destino'],
                '--formato_origem': dataset_info['formato_origem'],
                '--formato_destino': dataset_info['formato_destino'],
                '--coluna_particao': dataset_info['coluna_particao'],
                '--tipo_particao': dataset_info['tipo_particao'],
                '--compressao': dataset_info['compressao'],
                '--glue_database': dataset_info['glue_database'],
                '--glue_tabela': dataset_info['glue_tabela'],
                '--colunas_esperadas': dataset_info['colunas_esperadas'],
                '--modo_ingestao': dataset_info['modo_ingestao'],
                '--chaves': dataset_info['chaves'],
                '--query': dataset_info['query']
            }
        )

        log_info("Job do Glue iniciado com sucesso",
                 job_run_id=response['JobRunId'],
                 job_name=dataset_info['nome_job'])

        return {
            'statusCode': 200,
            'body': f"Job do Glue iniciado com sucesso: {response['JobRunId']}"
        }

    except Exception as e:
        log_error("Erro no Lambda", error=str(e))
        raise e
