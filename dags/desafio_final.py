import boto3
import pymongo
import requests
import json
import pandas as pd
from datetime import datetime
from airflow.decorators import dag, task
from sqlalchemy import create_engine
from airflow.models import Variable

#set secrets keys
aws_access_key_id = Variable.get("aws_access_key_id")
aws_secret_access_key = Variable.get('aws_secret_access_key')
mongo_secret = Variable.get('mongo_secret')
mongo_url = "unicluster.ixhvw.mongodb.net/ibge?retryWrites=true&w=majority"

#load s3 client
s3_client = boto3.client(
    's3',
    aws_access_key_id=aws_access_key_id,
    aws_secret_access_key=aws_secret_access_key
)

#set default args airflow
default_args = {
    'owner': 'Cristian Firmino',
    'depends_on_past': False,
    'start_date': datetime(2021,8,4,21,10),
    'email': 'cmendesfirmino@gmail.com',
    'email_on_failure': False,
    'email_on_retry': False
}
#create dag
@dag(default_args=default_args, schedule_interval=None, \
    description="ETL de dados do IBGE para Data Lake e DW", tags=['aws', 'postgres'])
def desafio_final_etl():
    """
    Um flow para obter dados do IBGE de uma base do mongodb
    """
    
    @task
    def extrai_mongo():
        string_conexao = f"mongodb+srv://estudante_igti:{mongo_secret}@{mongo_url}"
        client = pymongo.MongoClient(string_conexao)
        print("String conexao:")
        print(string_conexao)
        db = client.ibge
        pnad_collect = db.pnadc20203
        df = pd.DataFrame(list(pnad_collect.find()))
        filename = '/usr/local/airflow/data/pndac20203.csv' 
        df.to_csv(filename, index=False, encoding='utf-8', sep=';')
        return filename
    @task 
    def extrai_api():
        res = requests.get("https://servicodados.ibge.gov.br/api/v1/localidades/estados/MG/mesorregioes")
        resjson = json.loads(res.text)
        df = pd.DataFrame(resjson)[['id','nome']]
        filename = '/usr/local/airflow/data/dimensao_mesorregioes_mg.csv'
        df.to_csv(filename, sep=';', index=False, encoding='utf-8')
        return filename
    @task
    def upload_to_s3(filename):
        print(f"Got filename:{filename}")
        print(f"Got object name: {filename[19:]}")
        s3_client.upload_file(filename, 'firmito1', filename[19:])
    @task
    def write_to_postgres(filename):
        conn = create_engine('postgresql://airflow:airflow@postgres:5432/postgres')
        df = pd.read_csv(filename, sep=';')
        if filename[-14:] == "pndac20203.csv":
            df = df.loc[(df.idade >=20) & (df.idade <=40) & (df.sexo == "Mulher")]
        df.to_sql(filename[24:-4], conn, index=False, if_exists='replace', method='multi', chunksize=1000)
    mongo = extrai_mongo()
    api = extrai_api()
    up_mongo = upload_to_s3(mongo)
    up_api = upload_to_s3(api)
    wr_mongo = write_to_postgres(mongo)
    wr_api = write_to_postgres(api)

desafio_final_etl = desafio_final_etl()

