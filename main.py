

import base64
import json
import os
import pandas as pd
import logging as log
from datetime import datetime
from google.cloud.logging.resource import Resource
from google.cloud import bigquery, logging
from google.cloud import storage


def to_storage(bucket_name, message, destination_blob_name):

    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    bucketfolder = os.environ['folder_name']
    blob = bucket.blob(destination_blob_name)
    blob.upload_from_string(bucketfolder + message)

def to_bigquery(dataset, table, document):

    bigquery_client = bigquery.Client()
    dataset_ref = bigquery_client.get_dataset(dataset)
    table_ref = dataset_ref.table(table)
    table_id = bigquery_client.get_table(table_ref)
    errors = bigquery_client.insert_rows_json(table_id, [document])
    if errors:
        log.exception('[ERRO BIQUERY] Inserir valores: ' + str(errors))
        to_storage(os.environ['bucket_name'], str(document),os.environ['folder_name'] + os.environ['dataset'] + '.' + os.environ['table'] + '_' + str(datetime.today())[0:19])


def pubsub_to_bigq(event, context):
    global logger, grupo_log, hora_ini, pubsub_message, res
    try:
        hora_ini = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        # Configuracoes Estrutura Logging
        log_name = 'log_data_ingestion'
        grupo_log = 'DATA_INGESTION'
        lista = []  # lista para receber os valores do Json

        # Cria o objeto logger para o log de carga
        res = Resource(type="cloud_function",
                       labels={
                           "function_name": os.environ['job_name'],
                           "region": "us-central1"
                       }
                       )

        logging_client = logging.Client()
        logger = logging_client.logger(log_name)

        pubsub_message = base64.b64decode(event['data']).decode('utf-8')
        pubsub_message = pubsub_message.replace('\n', '')

        # Load na variavel 'pubsub_message' para o formato json.
        js = json.loads(pubsub_message)

        # foi criado uma lista para receber os valores do json que esta na variavel 'js'
        # append na lista com o conteudo do Json
        lista.append([js['idInvoice'], js['tpRegistro'], js['empresa'], js['clienteFaturamento'], js['clientePagador'],
                      js['clienteIntermed'],
                      js['clienteEntregador'], js['clienteArrend'], js['clienteComissao'], js['ordemVenda'],
                      js['notaFiscal'], js['notaFiscalRef'],
                      js['dataFaturamento'], js['sistemaOrigem'], js['tipoFaturamento'], js['tipoRegistro'],
                      js['tipoOperacao'], js['cfop'], js['dataVencimento'],
                      js['pedidoIndustrial'], js['filler'], js['items'], js['conds'], js['caracteristicasVeiculo'],
                      js['dataEnvioGCP']])

        # Lista columns para mudar os nomes das colunas do DataFrame
        columns = ['idInvoice', 'tpRegistro', 'empresa', 'clienteFaturamento',
                   'clientePagador', 'clienteIntermed', 'clienteEntregador',
                   'clienteArrend', 'clienteComissao', 'ordemVenda', 'notaFiscal',
                   'notaFiscalRef', 'dataFaturamento', 'sistemaOrigem', 'tipoFaturamento',
                   'tipoRegistro', 'tipoOperacao', 'cfop', 'dataVencimento', 'pedidoIndustrial',
                   'filler', 'items', 'conds', 'caracteristicasVeiculo', 'dataEnvioGCP']

        # Crio um Dataframe com a lista preenchida pelo Json
        dfpsm = pd.DataFrame(lista)
        
        # Renomeia as colunas do Dataframe com a lista Columns criada
        dfpsm.columns = columns

        # Converte a dataFaturamento e dataVencimento para Datetime e depois converto para string
        dfpsm['dataFaturamento'] = pd.to_datetime(dfpsm['dataFaturamento'])
        #dfpsm['dataVencimento'] = pd.to_datetime(dfpsm['dataVencimento'])
        dfpsm['dataFaturamento'] = dfpsm['dataFaturamento'].astype(str)
        #dfpsm['dataVencimento'] = dfpsm['dataVencimento'].astype(str)

        # Converte o Dataframe para Json
        js2 = dfpsm.to_json(orient='records')

        # Retira os colchetes do novo Json
        js2 = js2[1:]
        js2 = js2[:-1]

        js2.info()


        to_bigquery(os.environ['dataset'], os.environ['table'], json.loads(js2))

        logger.log_struct({'Grupo_Log': grupo_log, 'Status_Execucao': 'Sucesso', 'Nome_Job': os.environ['job_name'],
                           'Dataset': os.environ['dataset'], 'Tabela': os.environ['table'], 'Inicio': hora_ini,
                           'Fim': datetime.now().strftime('%Y-%m-%d %H:%M:%S')},
                          severity='INFO', resource=res)


    except base64.binascii.Error as ex:
        logger.log_struct({'Grupo_Log': grupo_log, 'Status_Execucao': 'Erro', 'Nome_Job': os.environ['job_name'],
                           'Dataset': os.environ['dataset'], 'Tabela': os.environ['table'], 'Inicio': hora_ini,
                           'Fim': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                           'Msg_Erro': '[ERRO CONVERTER MSG BASE64_STRING] ' + str(ex)},
                          severity='ERROR', resource=res)

        to_storage(os.environ['bucket_name'], pubsub_message,
                   os.environ['dataset'] + '.' + os.environ['table'] + '_' + str(datetime.today())[0:19])
    except json.JSONDecodeError as ex:
        logger.log_struct({'Grupo_Log': grupo_log, 'Status_Execucao': 'Erro', 'Nome_Job': os.environ['job_name'],
                           'Dataset': os.environ['dataset'], 'Tabela': os.environ['table'], 'Inicio': hora_ini,
                           'Fim': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                           'Msg_Erro': '[ERRO JSON INVALIDO] ' + str(ex)},
                          severity='ERROR', resource=res)

        to_storage(os.environ['bucket_name'], pubsub_message,
                   os.environ['dataset'] + '.' + os.environ['table'] + '_' + str(datetime.today())[0:19])
    except KeyError:
        logger.log_struct({'Grupo_Log': grupo_log, 'Status_Execucao': 'Erro', 'Nome_Job': os.environ['job_name'],
                           'Dataset': os.environ['dataset'], 'Tabela': os.environ['table'], 'Inicio': hora_ini,
                           'Fim': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                           'Msg_Erro': '[ERRO ENVIRONMENT VARIABLES] Parametros dataset e table requeridos] ' + str(
                               ex)},
                          severity='ERROR', resource=res)

        to_storage(os.environ['bucket_name'], pubsub_message,
                   os.environ['dataset'] + '.' + os.environ['table'] + '_' + str(datetime.today())[0:19])
    except Exception as ex:
        logger.log_struct({'Grupo_Log': grupo_log, 'Status_Execucao': 'Erro', 'Nome_Job': os.environ['job_name'],
                           'Dataset': os.environ['dataset'], 'Tabela': os.environ['table'], 'Inicio': hora_ini,
                           'Fim': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                           'Msg_Erro': '[ERRO GENERICO] ' + str(ex)},
                          severity='ERROR', resource=res)

        to_storage(os.environ['bucket_name'], pubsub_message,
                   os.environ['dataset'] + '.' + os.environ['table'] + '_' + str(datetime.today())[0:19])


