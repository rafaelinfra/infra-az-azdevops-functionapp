import logging
import pandas as pd
import json
import os
from io import BytesIO, StringIO
from azure.storage.filedatalake import DataLakeServiceClient
from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient


# método responsável por inicializar a conexão com azure gen2
# Alterado o STORAGE_ACCOUNT_NAME
def initialize_storage_account_ad():
    try:
        global service_client

        credential = DefaultAzureCredential()
        service_client = DataLakeServiceClient(
            account_url="{}://{}.dfs.core.windows.net".format(
                "https", os.environ['STORAGE_ACCOUNT_NAME']),
            credential=credential)

    except Exception as e:
        logging.error(e)
        raise Exception('Erro ao efetuar autenticação com o Azure:', e)

#método responsável por inicializar a conexão com o azure gen2 alternativo
# o azure gen2 alternativo utiliza a variável STORAGE_ACCOUNT_NAME_DEV
def initialize_storage_account_ad_dev():
    try:
        # criação da variável service_client_dev, pois estará associada a um storage alternativo
        global service_client_dev
        credential = DefaultAzureCredential()
        service_client_dev = DataLakeServiceClient(
            account_url="{}://{}.dfs.core.windows.net".format(
                "https", os.environ['STORAGE_ACCOUNT_NAME_DEV']),
            credential=credential)

    except Exception as e:
        logging.error(e)
        raise Exception('Erro ao efetuar autenticação com o Azure no storage alternativo:', e)


def upload_file_to_directory_bulk(fs_name,
                                  file_path,
                                  file_name,
                                  df: pd.DataFrame):
    initialize_storage_account_ad()
    try:
        file_system_client = service_client.get_file_system_client(
            file_system=fs_name)
        directory_client = file_system_client.get_directory_client(file_path)
        file_client = directory_client.get_file_client(file_name)
        buffer = BytesIO()
        df.to_parquet(buffer, engine='pyarrow', index=False)
        file_client.upload_data(buffer.getvalue(), overwrite=True)
    except Exception as e:
        logging.error(e)
        raise Exception('Erro ao salvar arquivo no datalake: ', e)

# Método voltado para salvar dados no formato .csv 
def upload_file_to_directory_bulk2(fs_name,
                                  file_path,
                                  file_name,
                                  df: pd.DataFrame):
    initialize_storage_account_ad()
    try:
        file_system_client = service_client.get_file_system_client(
            file_system=fs_name)
        directory_client = file_system_client.get_directory_client(file_path)
        file_client = directory_client.get_file_client(file_name)
        buffer = StringIO()
        df.to_csv(buffer, sep=';', encoding='ANSI', index=False)
        buffer.seek(0)
        file_client.upload_data(buffer.getvalue(), overwrite=True)
    except Exception as e:
        logging.error(e)
        raise Exception('Erro ao salvar arquivo no datalake: ', e)


def download_file_from_directory(fs_name, file_path, file_name):
    try:
        file_system_client = service_client.get_file_system_client(
            file_system=fs_name)
        directory_client = file_system_client.get_directory_client(file_path)
        file_client = directory_client.get_file_client(file_name)
        download = file_client.download_file()
        downloaded_bytes = download.readall()
        return downloaded_bytes
    except Exception as e:
        logging.error(e)
        raise Exception(e)
    
# Método similar ao método anterior, mas focado em uso de storage alternativo
def download_file_from_directory_dev(fs_name, file_path, file_name):
    try:
        file_system_client = service_client_dev.get_file_system_client(
            file_system=fs_name)
        directory_client = file_system_client.get_directory_client(file_path)
        file_client = directory_client.get_file_client(file_name)
        download = file_client.download_file()
        downloaded_bytes = download.readall()
        return downloaded_bytes
    except Exception as e:
        logging.error(e)
        raise Exception(e)


def save_data(data,
              subject: str,
              file_system: str,
              file_path: str,
              file_name: str):
    df = data
    try:
        if len(df) > 0:
            upload_file_to_directory_bulk(
                file_system, file_path, file_name, df)
            logging.info(f'{subject} processo concluido com sucesso.')

        else:
            logging.info(
                f'{subject} processo nao possui dados para armazenar.')

    except Exception as e:
        logging.error(e)
        raise Exception(f'Erro na função save_data{subject}: {str(e)}')
    
def save_data2(data,
              subject: str,
              file_system: str,
              file_path: str,
              file_name: str):
    df = data
    try:
        if len(df) > 0:
            upload_file_to_directory_bulk2(
                file_system, file_path, file_name, df)
            logging.info(f'{subject} processo concluido com sucesso.')

        else:
            logging.info(
                f'{subject} processo nao possui dados para armazenar.')

    except Exception as e:
        logging.error(e)
        raise Exception(f'Erro na função save_data{subject}: {str(e)}')


def get_directories(mock=False, arqJson=None):
    if not mock:
        actual_dir = os.path.dirname(os.path.abspath('__file__'))
    else:
        actual_dir = '/home'
    try:
        with open(os.path.join(actual_dir, arqJson)) as file:
            data = json.load(file)

        return data['DatasetTarget'], data['DatasetSource']
    except Exception as e:
        logging.error(e)
        raise FileNotFoundError('Erro get_directories:', e)


def check_blob_exist(fs_name, file_path, file_name):
    try:
        file_system_client = service_client.get_file_system_client(
            file_system=fs_name)
        directory_client = file_system_client.get_directory_client(file_path)
        file_client = directory_client.get_file_client(file_name)

        file_client.get_file_properties()

        return True
    except Exception as e:
        logging.error('Verificar se blob existe:', e)
        return False

def list_directory_contents(fs_name, file_path):
    try:
        # filePath = filePath + filename
        initialize_storage_account_ad()
        file_system_client = service_client.get_file_system_client(
            file_system=fs_name)
        paths = file_system_client.get_paths(path=file_path)
        return paths
    except Exception as e:
        print(e)

# Método responsável por capturar items parametrizados de um arquivo JSON
def get_items_json(arq):
    initialize_storage_account_ad()
    try:
        errors = []

        global DatasetSource
        global DatasetTarget

        DatasetTarget, DatasetSource = get_directories(arqJson= arq)

        if DatasetTarget is None or DatasetSource is None:
            errors.append(
                'Não foi possivel obter o arquivo de parametros de diretorios.')
        else:
            logging.info('Arquivo de parametros carregado com sucesso.')
    except Exception:
        logging.info('Erro ao carregar dados básicos da Function ')

#Obter KEY via Key Vault
def get_secret_key_vault(key):
    try:
        credential = DefaultAzureCredential()
        url_key_vault = os.environ['KEY_VAULT_URL']
        client = SecretClient(vault_url=url_key_vault, credential=credential)
        key_value = client.get_secret(name=key)
        return key_value.value
    except Exception as e:
        logging.error(f'Erro ao retornar os parametros do KEY_VAULT: {e}')

#função para verificar se o arquivo existe e retorna false sem erro
#verifica se o arquivo existe e se nao existir retorna false ao inves de um log de erro
def check_blob_exist_v2(fs_name, file_path, file_name):
    try:
        file_system_client = service_client.get_file_system_client(
            file_system=fs_name)
        directory_client = file_system_client.get_directory_client(file_path)
        file_client = directory_client.get_file_client(file_name)

        file_client.get_file_properties()

        return True
    except:
        return False
    
#ler arquivos no datalake 
def read_any(fs_name, file_path, file_name, _format,  mode='pandas', storage_account = None, **kwargs):
    try:
        initialize_storage_account_ad()
        file_bytes = download_file_from_directory(fs_name, file_path, file_name)
   
    except Exception as e:
        raise Exception(f'falha ao ler arquivo {file_name}: {e}')

    else:
        if mode == 'pandas':

            format_map = {
                "parquet": pd.read_parquet,
                "csv": pd.read_csv,
                "excel": pd.read_excel,
            }

            func = format_map[_format]
        
        return func(BytesIO(file_bytes), **kwargs)

#ler arquivos no datalake, mas utilizando a funcao initialize_storage_account_ad2, que permite utilizar storage alternativo
def read_any_dev(fs_name, file_path, file_name, _format,  mode='pandas', storage_account = None, **kwargs):
    try:
        initialize_storage_account_ad_dev()
        file_bytes = download_file_from_directory_dev(fs_name, file_path, file_name)
   
    except Exception as e:
        raise Exception(f'falha ao ler arquivo {file_name} no storage alternativo: {e}')

    else:
        if mode == 'pandas':

            format_map = {
                "parquet": pd.read_parquet,
                "csv": pd.read_csv,
                "excel": pd.read_excel,
            }

            func = format_map[_format]
        
        return func(BytesIO(file_bytes), **kwargs)
    
def read_parquet(fs_name, file_path, file_name, mode='pandas', **kwargs):
    return read_any(fs_name, file_path, file_name, 'parquet', mode=mode, engine = 'pyarrow', **kwargs)


def read_csv(fs_name, file_path, file_name, mode='pandas', **kwargs):
    return read_any(fs_name, file_path, file_name, 'csv', mode=mode, **kwargs)


def read_excel(fs_name, file_path, file_name, mode='pandas', **kwargs):
    return read_any(fs_name, file_path, file_name, 'excel', mode=mode, engine = 'openpyxl', **kwargs)