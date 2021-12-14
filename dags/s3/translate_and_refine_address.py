import io
import pandas as pd

import boto3
from sqlalchemy import create_engine

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from s3.data_pipeline.heuristic_translate import Translator, output_translations
from s3.data_pipeline.refine_address import run_refinement

def translate_address():
    s3 = boto3.resource('s3')
    s3_bucket_name = 'shipping-watch'

    input_file_path = 'client/dataset/may-out.csv'
    output_file_path = 'client/translation/may-output.csv'
    
    translator = Translator(s3, s3_bucket_name)
    output_translations(translator, s3, s3_bucket_name, input_file_path, output_file_path)    
    
def refine_address():
    s3 = boto3.resource('s3')
    s3_bucket_name = 'shipping-watch'
    
    input_file_name = 'client/translation/may-output.csv'
    output_file_name = 'client/translation/may-refine-output.csv'

    obj = s3.Object(s3_bucket_name, input_file_name)
    translate_address = pd.read_csv(obj.get()['Body'])
    print(f'Get translation files.')
  
    # refine address
    refine_output = run_refinement(translate_address, 20)
    acc = (len(refine_output)-len(refine_output[refine_output['refinedko']=='']))*100/len(refine_output)
    print(f'Accuracy: {round(acc,1)} %.')

    # Save refine output data in S3
    csv_buffer = io.StringIO()
    refine_output.to_csv(csv_buffer)
    s3.Object('shipping-watch', output_file_name).put(Body=csv_buffer.getvalue())
    
    
with DAG(
    dag_id='translate_and_refine_address',
    schedule_interval=None,
    start_date=days_ago(2),
    max_active_runs=1,
    #tags=['config'],
) as dag:
    translate_address_op = PythonOperator(
        task_id='translate_address',
        python_callable=translate_address, 
        dag=dag
    ) 
    refine_address_op = PythonOperator(
        task_id='refine_address',
        python_callable=refine_address, 
        dag=dag
    ) 
    
    translate_address_op >> refine_address_op
