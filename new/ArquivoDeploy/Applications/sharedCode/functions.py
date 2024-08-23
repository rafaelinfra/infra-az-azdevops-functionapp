import logging
import pandas as pd
import json
import os
from io import BytesIO
from azure.storage.filedatalake import DataLakeServiceClient
from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient
import base64


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

def get_gcp_credentials():
    try:
        key_vault = SecretClient(
            vault_url= os.environ['KEY_VAULT_URL'],
            credential=DefaultAzureCredential()
        )

        private_key = key_vault.get_secret(name='sct-gcp-jornada-consumidor')
        decoded_private_key = base64.b64decode(private_key.value).decode("utf-8")

        client_email = key_vault.get_secret(name='usu-gcp-jornada-consumidor')

        credentials = {
                        "private_key": decoded_private_key,
                        "client_email": client_email.value,
                        "token_uri": "https://oauth2.googleapis.com/token"}
                
        return credentials

    except Exception as e:
        logging.error(e)
        raise Exception('Erro get_gcp_credentials:', e)

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
    
def read_parquet(fs_name, file_path, file_name, mode='pandas', **kwargs):
    return read_any(fs_name, file_path, file_name, 'parquet', mode=mode, engine = 'pyarrow', **kwargs)


def read_csv(fs_name, file_path, file_name, mode='pandas', **kwargs):
    return read_any(fs_name, file_path, file_name, 'csv', mode=mode, **kwargs)


def read_excel(fs_name, file_path, file_name, mode='pandas', **kwargs):
    return read_any(fs_name, file_path, file_name, 'excel', mode=mode, engine = 'openpyxl', **kwargs)

def get_directories_v1(mock = False, arqJson = 'af_fontesinternas_jetoil_debitos.json', lane = 'DatasetTarget'):
    if mock == False:
        actual_dir = os.path.dirname(os.path.abspath(__file__))
    else:
        actual_dir = '/home'
    try:
        with open(os.path.join(actual_dir, arqJson)) as file:
            data = json.load(file)
            directories = data[lane]

        return directories
    except Exception as e:
        logging.error(e)
        raise FileNotFoundError('Erro get_directories_v1:', e)

def delete_file(fs_name, file_path, file_name):
    try:
        initialize_storage_account_ad()
        file_system_client = service_client.get_file_system_client(file_system=fs_name)
        directory_client = file_system_client.get_directory_client(file_path)
        file_client = directory_client.get_file_client(file_name)
        file_client.delete_file()
    
    except Exception as e:
        raise Exception(f"Erro ao deletar arquivo {file_path, file_name}. Erro: {e}")

def upload_any(file, fs_name, uri, _format, storage_account = None, **kwargs):
    initialize_storage_account_ad()
    format_map = {"parquet": pd.DataFrame.to_parquet, "excel": pd.DataFrame.to_excel, "csv": pd.DataFrame.to_csv}
    func = format_map[_format]
    
    try:
        path, filename = os.path.split(uri)
        file_system_client = service_client.get_file_system_client(file_system=fs_name)
        directory_client = file_system_client.get_directory_client(path)
        file_client = directory_client.get_file_client(filename)
        
        if _format == 'csv':
            buffer = BytesIO(func(file, **kwargs).encode('utf-8'))     
        elif _format == 'excel': #Condicional criada para o Excel para permitir upload do arquivo com varias abas
            buffer = file
        else:
            buffer = BytesIO()
            func(file, buffer, **kwargs)
            
        #file_client.upload_data(buffer.getvalue(), overwrite=True) #Comentado
        file_client.upload_data(buffer, overwrite=True) #Incluso
        
    except Exception as e:
        logging.error(e)
        raise Exception('Erro ao salvar arquivo no datalake: ', e)