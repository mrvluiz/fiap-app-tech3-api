
import pandas as pd
import os 
import pandas as pd
import boto3
import pandas as pd
import io
import kaggle

from os import listdir
from os.path import isfile, join
from datetime import datetime
from flask import Flask, jsonify
from flask_restx import Api, Resource


KAGGLE_DS_MEDICAL_NO_SHOW = 'joniarroba/noshowappointments/data'
KAGGLE_DATASET_NAME = 'gkalpolukcu/knn-algorithm-dataset'
KAGGLE_PATH = 'kaggle-download'

BUCKET_NAME = 'bucket-fiap-tech3-dw'
BUCKET_PATH_TRAINING = 'ML-CANCER-TRAINING'
BUCKET_PATH_TEST = 'ML-CANCER-TEST'


app  = Flask(__name__)
app.json.sort_keys = False
api = Api(app,
            version='1.0.0',
            title='API Services TECH-Challenger3 - @mrvluiz',
            description='API Services TECH-Challenger3 - @mrvluiz',
            default='Principal'
            )

@api.route("/AWS/salvar/treino")
@api.doc(description="Baixa DataSet do Kaggle, Salva no AWS S3 e Retorna o objeto gravado")
class Producao(Resource):        
        def post(self):         
                Download_Kaggle_DataSet(KAGGLE_DATASET_NAME, KAGGLE_PATH)
                result = Send_DataSet_To_AWS(KAGGLE_PATH, BUCKET_NAME, BUCKET_PATH_TRAINING) 
                return result

@api.route("/AWS/salvar/teste")
@api.doc(description="Baixa DataSet do Kaggle, Salva no AWS S3 e Retorna o objeto gravado")
class Producao(Resource):        
        def post(self):        
                Download_Kaggle_DataSet(KAGGLE_DATASET_NAME, KAGGLE_PATH)
                result = Send_DataSet_To_AWS(KAGGLE_PATH, BUCKET_NAME, BUCKET_PATH_TEST) 
                return result


def GetSessionAWS ():
    session = boto3.Session(aws_access_key_id='ASIAYKRRLZFBWKOHCI35',
        aws_secret_access_key='caM+CxBhAyX7MiSa3BsEgrAJa7ocf1x0tiN1dFuz',
        aws_session_token='IQoJb3JpZ2luX2VjEHAaCXVzLXdlc3QtMiJHMEUCIAmMbJ86Kg3qLOkaiHd3AAC7ncngAK4xWdc23cdq875sAiEAwzDrZAGkRF6S77fwoUlnLJbq37PT7KTfD9C0/iabjt8qvQIIqf//////////ARABGgw1NzI0MDc5MjUwNTkiDM3wD/oL0CkQ8JaVLCqRAhojZwq2w1Ohn94SKIaJAgaZZwZU0t3CB+KFDGuFc78fsWCmFEabJzWjVbz61kbiV/e86yqkMiVIKTA+ux+DC4dOFcZEe8KMtUHwlVegiL/uWP1OxqvBR8Xa7XYGwI9PjHKjSnN2nj/O1hUipb92lP9/s08Bhx7t188N1CmAimgWwHD/uWoHj5xf5Lo84z2H+m4hpggnIb7vIoe4im5blS4QtNBkrE6polYeTLYhnpstpMOQeXiPlBEFs60kKwvPtJgGaCt9AfqACqsU3YKfSiAm7pUw4zNF5tfk/Pfmy/lV2FVH1BHA7ttft4Da8STW6advejkHHwUzpb71+5TL53YVWEhTd+fOIVGfJu7K/Wjx4TDx78C3BjqdASk6f0s4oDRlKmdknS4Nea2uTx+bxFlw45DofLODISf8i1OsyPnywzR+esYtrOEnKWbDLyyOjSsMgv+Ef2EIf0vvcyHe4uK+KKCrBYD2j3gZ5tS7xahvRTr6Y7+edJksjjIaBgVJZIRpnkwU26RGvxbLuNwhJcR3kPl0P7F0lyyMUn1AOJc0nNoFd5oksQy7BaOJfdFLwftZZ35SE6Q='
        )
    s3 = session.resource('s3')
    return s3


def Read_From_AWS (BucketName, BucketPath):         
    s3 = GetSessionAWS()   
    my_bucket = s3.Bucket(BucketName)       

    for my_bucket_object in my_bucket.objects.all():     
        splitedKey = my_bucket_object.key.split("/")

        if splitedKey[0] == BucketPath:
            content_object =  my_bucket_object.get()['Body'].read()           
            df = pd.read_csv(io.BytesIO(content_object))
            df.head()
            return pd
   
    
def Send_DataSet_To_AWS (DownloadPath, BucketName, BucketPath):     
    s3 = GetSessionAWS()

    files = []
    path_destiny = os.path.join(DownloadPath, BucketPath) 
    arrayDir =  os.listdir(path_destiny)
    for file in arrayDir:   
        file_name, file_ext = os.path.splitext(file)      
        file_destiny = os.path.join(path_destiny, file)
        file_key = file #file_name + datetime.now().strftime('[%Y%m%d_%H%M%S]') + file_ext

        if BucketPath != "":
              file_key =   BucketPath + "/" + file_key

        with open(file_destiny, 'rb') as data:        
            s3.Bucket(BucketName).put_object(Key=file_key, Body=data, Tagging='KeyStatus=RawParquet') 
            files.append({'objeto':file_key, 'resultado': 'Sucesso'}) 

    return files

def Download_Kaggle_DataSet (DataSetName, DownloadPath, BucketPath):       
    
    path_destiny = os.path.join(DownloadPath, BucketPath) 
    kaggle.api.authenticate()
    kaggle.api.dataset_download_files(DataSetName, path=path_destiny, unzip=True, force=True)
    os.listdir(path_destiny)

if __name__ == "__main__":


    #result = Download_Kaggle_DataSet(KAGGLE_DATASET_NAME, KAGGLE_PATH, BUCKET_PATH_TEST)

    #result = Send_DataSet_To_AWS(KAGGLE_PATH, BUCKET_NAME, BUCKET_PATH_TEST) 


    app.run()