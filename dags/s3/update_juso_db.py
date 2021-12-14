import datetime
import requests
import zipfile
import io
import pandas as pd
import boto3

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from s3.create_juso_db import _process_en_row, _process_ko_row, save_df_in_s3

def download_zip(s3, s3_bucket_name, s3_key, **context):
    # check datetime
    now = datetime.datetime.now()
    year = str(now.year)
    month = str(now.month-1).zfill(2)
    print(f'Date {year} {month}.')
    
    en_http = f"https://www.juso.go.kr/dn.do?reqType=ALLENG&regYmd={year}&ctprvnCd=01&\
    gubun=ENG&stdde={year}{month}&fileName={year}{month}_%25EC%2598%2581%25EB%25AC%\
    25B8%25EC%25A3%25BC%25EC%2586%258CDB_%25EB%25B3%2580%25EB%258F%2599%25EB%25B6%2584.zip\
    &realFileName={year}{month}ALLENG01.zip&engj&indutyCd=999&purpsCd=999&indutyRm=%EC\
    %88%98%EC%A7%91%EC%A2%85%EB%A3%8C&purpsRm=%EC%88%98%EC%A7%91%EC%A2%85%EB%A3%8C"

    ko_http = f"https://www.juso.go.kr/dn.do?reqType=ALLRDNM&regYmd={year}&ctprvnCd=01&\
    gubun=RDNM&stdde={year}{month}&fileName={year}{month}_%25EA%25B1%25B4%25EB%25AC%25BCDB_%25EB%25B3%\
    2580%25EB%258F%2599%25EB%25B6%2584.zip&realFileName={year}{month}ALLRDNM01.zip&rdnm&indutyCd=999&\
    purpsCd=999&indutyRm=%EC%88%98%EC%A7%91%EC%A2%85%EB%A3%8C&purpsRm=%EC%88%98%EC%A7%91%EC%A2%85%EB%A3%8C"

    en_indata = requests.get(en_http)
    ko_indata = requests.get(ko_http)

    try:
        # upload english data 
        with zipfile.ZipFile(io.BytesIO(en_indata.content)) as z:
            zList = z.namelist()
            print(zList)
            for i in zList:
                data = io.BytesIO(z.read(i))
                s3.Object(s3_bucket_name, s3_key+'english/'+year+'/'+month+'/'+i).put(Body=data)

        # upload korean data
        with zipfile.ZipFile(io.BytesIO(ko_indata.content)) as z:
            zList = z.namelist()
            print(zList)
            for i in zList:
                data = io.BytesIO(z.read(i))
                s3.Object(s3_bucket_name, s3_key+'korean/'+year+'/'+month+'/'+i).put(Body=data)
        return year, month
    except:
        print('No update data.')
        return 0, 0
    
    
def merge_db(s3, s3_bucket_name, **context):
    year, month = context['task_instance'].xcom_pull(task_ids='Download_new_address')
    if year==month==0:
        print(f'There is no update data.')
        return
#     obj = s3.Object(s3_bucket_name,'Dataframe/en_df.csv')
#     en_df = obj.get()['Body']
#     en_df = pd.read_csv(en_df)
    
#     obj = s3.Object(s3_bucket_name,'Dataframe/ko_df.csv')
#     ko_df = obj.get()['Body']
#     ko_df = pd.read_csv(ko_df)
    
    obj = s3.Object(s3_bucket_name,'juso_db/Dataframe/ko_en_df.csv')
    ko_en_df = obj.get()['Body']
    ko_en_df = pd.read_csv(ko_en_df)
    ko_en_df.pop('Unnamed: 0')
    print('Get korean-english DB.')

    # new english address
    obj = s3.Object(s3_bucket_name, f'juso_db/english/{year}/{month}/rn_mod.txt')
    body = obj.get()['Body'].read().decode('cp949').split('\r\n')
    english_addresses = []
    for row in body[:-1]:
        if row:
            english_addresses.append(_process_en_row(row))
            
    # new korean address 
    obj = s3.Object(s3_bucket_name, f'juso_db/korean/{year}/{month}/build_mod.txt')
    body = obj.get()['Body'].read().decode('cp949').split('\r\n')
    korean_addresses = []
    for row in body[:-1]:
        if row:
            korean_addresses.append(_process_ko_row(row))
    new_en_df = pd.DataFrame(english_addresses)
    new_ko_df = pd.DataFrame(korean_addresses)
    new_merge_df = pd.merge(new_ko_df, new_en_df, how='inner', on=['building_management_code'], suffixes=('_ko', '_en'))
    #new_merge_df = pd.merge(ko_df, en_df, how='inner', on=['building_management_code', 'postal_code'], suffixes=('_ko', '_en')) 
    
    update_df = pd.concat([ko_en_df, new_merge_df], ignore_index=True)
    print('Get new address and merge origin DB.')
    
    # upload data
    save_df_in_s3(update_df, s3, s3_bucket_name, 'juso_db/Dataframe', 'ko_en_df.csv')
    print(f'There is update data {year} {month}.')

s3 = boto3.resource('s3')
s3_bucket_name = 'shipping-watch'
bucket = s3.Bucket(s3_bucket_name)

# scheduler_interval = "min hour day month year"
    
with DAG(
    dag_id='update_juso_db',
    schedule_interval='0 15 15 * *',
    start_date=days_ago(2),
    max_active_runs=12,
    #tags=['config'],
) as dag:
    download_new_address = PythonOperator(
        task_id='Download_new_address',
        python_callable=download_zip, 
        op_kwargs={'s3': s3,'s3_bucket_name': s3_bucket_name, 's3_key': 'juso_db/'},
        provide_context=True,
        dag=dag
    ) 
    
    merge_new_address = PythonOperator(
        task_id='Merge_DB',
        python_callable=merge_db,
        op_kwargs={'s3': s3, 's3_bucket_name': s3_bucket_name},
        provide_context=True,
        dag=dag
    )
    
    download_new_address >> merge_new_address
    
    
    
    
    
    
    
   
