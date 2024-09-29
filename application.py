
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


KAGGLE_DS_MEDICAL_NO_SHOW = 'joniarroba/noshowappointments'
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

@api.route("/AWS/salvar-dataset")
@api.doc(description="Baixa DataSet do Kaggle, Salva no AWS S3 e Retorna o objeto gravado")
class Producao(Resource):        
        def post(self):         
                Download_Kaggle_DataSet(KAGGLE_DS_MEDICAL_NO_SHOW, KAGGLE_PATH)
                result = Send_DataSet_To_AWS(KAGGLE_PATH, BUCKET_NAME, BUCKET_PATH_TRAINING) 
                return result

def GetSessionAWS ():
    session = boto3.Session(aws_access_key_id='ASIAYKRRLZFBV3JQ5BXU',
        aws_secret_access_key='9CYFron0dXSocqhSdHvDHKkw770qANWk4dJIa7fj',
        aws_session_token='IQoJb3JpZ2luX2VjEAQaCXVzLXdlc3QtMiJGMEQCICXiLelMY6WBI2sjQuljJtXihH4j+NJDL/+nHDV/cB0qAiAtgL6WaqlxOow7kTROShXVrE+m1XgqoGrlAElBFgZxpCq0AghNEAEaDDU3MjQwNzkyNTA1OSIM3T/0E82f8V2q5ekDKpECqH87QOlaHPlTeVHCsKTDZhvX3d/YfjGT+PxKTZyiFOZDK3X3EyLisp/1RiyJeCv4oaYSuBrfAn+4FLQ6Br2TYwIbZ0lkx+4YeFeIXSOymioYL26UyIdoPo0I0fbNfK4wY77babIqrSkiYXSE1u36xhaAWIeelh8iNcVDbd+XBaZKI6KLWpzAluqW6XPoQ4FL6EinKp49L5NLRsq2mNHlvzs/I5m4WZoHcHuFLhxkbz+Ciehg/Km0qRy+J00ZSoDky31JwYu21b74mZPzXT9L8K3MRCKUpbyyO6ogKK+tJT2deoOyGeLINtMkDdjBWskIiSP7cg4QdZIcARtvbth3waz5ApdcRlz0UcMjNytW5QA3MKy84bcGOp4B2nvSflkxB3/5Ufz+sJwq5VDpbPeMujGFag4ngUNFY4kJ6JgtGC93ajrbdX0mrRZcn6HEFn8hvzXmJc31a1If7G0Qh/iY1kA8Zax96stLgSEwDJB1OOXOzQH2g/PCQnXlN1L6xWMR/Oip94vG8MXzVfW9cOvgIBr+h32a40Sv/SJRt2ROd9bA+m7S9usqUvoL5oG22z/+IRQn/0Qtm2o='
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