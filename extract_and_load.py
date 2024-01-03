import boto3
from configparser import ConfigParser
import os
import subprocess
import pandas as pd
def access_s3():
    parser = ConfigParser()
    parser.read('pipeline.conf')
    access_key = parser.get('aws_boto_credentials', 'access_key')
    secret_key = parser.get('aws_boto_credentials', 'secret_key')
    bucket_name = parser.get('aws_boto_credentials', 'bucket_name')
    return access_key, secret_key, bucket_name

def extract_data():
    access_key, secret_key, bucket_name = access_s3()
    path_current = os.getcwd()
    path_dir_scrapy_execute = f'{path_current}\extract\extract'
    command = f'scrapy crawl extract_data -a access_key={access_key} -a secret_key={secret_key} -a bucket_name={bucket_name} '
    os.chdir(path_dir_scrapy_execute)
    subprocess.run(command, shell=True)
    print('Extraccion exitosa')

def transform_data(local_folder_path, local_folder_path_destination):
    os.chdir('../../')
    list_file = os.listdir(local_folder_path)
    for file in list_file:
        df = pd.read_csv(f'{local_folder_path}/{file}',delimiter=';', encoding='latin1')
        new_file_csv =f'{local_folder_path_destination}/{file}'
        df.to_csv(new_file_csv, index=False, delimiter=';',encoding='utf-8')
    print('Transformacion exitosa')

def load_data_to_s3( local_folder_path:str, s3_dir: str,):
    
    parser = ConfigParser()
    parser.read('pipeline.conf')
    access_key = parser.get('aws_boto_credentials', 'access_key')
    secret_key = parser.get('aws_boto_credentials', 'secret_key')
    bucket_name = parser.get('aws_boto_credentials', 'bucket_name')
    client = boto3.client('s3', aws_access_key_id =access_key, aws_secret_access_key= secret_key)
    list_file = os.listdir(f'./{local_folder_path}')
    for file in list_file:
        file_path = f'{local_folder_path}/{file}'
        object_name = f'{s3_dir}/{file}'
        client.upload_file(file_path, bucket_name, object_name )
    print('Carga exitosa')
if __name__ == '__main__':
    extract_data()
    transform_data('./downloaded-data', './transform-data')
    load_data_to_s3('./transform-data', 'reportepcservicios')