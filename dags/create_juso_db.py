import boto3
import pandas as pd
from io import StringIO

def _process_en_row(row_str):
    """Processes a single row of a en address data file."""
    comps = row_str.split('|')
    assert len(comps) == 18
    return {
    'city_name': comps[1], 'gu_name': comps[2], 'dong_name': comps[3],
    'ri_name': comps[4], 'street_name': comps[9],
    'building_management_code': comps[13], 'postal_code': comps[14],
    }

def _process_ko_row(row_str):
    """Processes a single row of a ko address data file."""
    comps = row_str.split('|')
    assert len(comps) == 31
    return {
    'city_name': comps[1], 'gu_name': comps[2], 'dong_name': comps[3],
    'ri_name': comps[4], 'street_name': comps[9],
    'building_management_code': comps[15], 'haeng_dong_name': comps[18],
    'postal_code': comps[19], 'gu_building_name': comps[25],
    }

def save_df_in_s3(df, s3, s3_bucket_name, s3_key, out_name):
    csv_buffer = StringIO()
    df.to_csv(csv_buffer)
    s3.Object(s3_bucket_name, s3_key+'/'+out_name).put(Body=csv_buffer.getvalue())
    out_path = s3_bucket_name+'/'+s3_key+'/'+out_name
    print(f'S3 path: {out_path}. ')
    
def read_address_and_save_dataframe(s3, s3_bucket_name, s3_key, bucket, proc_fn):
    """Reads address data files and save dataframe in s3"""
    english_address = []
    korean_address  = []

    for obj in bucket.objects.all():
        key = obj.key
        if 'juso_db' not in key: continue
        # english address
        if 'rn' in key and 'txt' in key:
            print(key)
            body = obj.get()['Body'].read().decode('cp949').split('\r\n')
            for row in body[:-1]:
                if row:
                    english_address.append(proc_fn['english'](row))
        # korean address 
        if 'build' in key and 'txt' in key:
            print(key)
            body = obj.get()['Body'].read().decode('cp949').split('\r\n')
            for row in body[:-1]:
                if row:
                    korean_address.append(proc_fn['korean'](row))
    
    en_df = pd.DataFrame(english_address)
    ko_df = pd.DataFrame(korean_address)
    print(f'# of english address: {len(en_df)}.')
    print(f'# of korean address: {len(ko_df)}.')
    
    save_df_in_s3(en_df, s3, s3_bucket_name, s3_key, 'Dataframe/en_df.csv')
    save_df_in_s3(ko_df, s3, s3_bucket_name, s3_key, 'Dataframe/ko_df.csv')
    print(f'save english, korean dataframe in s3.')

    # merge 
    ko_en_df = pd.merge(ko_df, en_df, how='inner', on=['building_management_code', 'postal_code'], suffixes=('_ko', '_en'))
    save_df_in_s3(ko_en_df, s3, s3_bucket_name, s3_key,  'Dataframe/ko_en_df.csv')
    print(f'save en-ko dataframe in s3.')
    
if __name__=='__main__':
    s3 = boto3.resource('s3')
    s3_bucket_name = 'shipping-watch'
    s3_key = 'juso_db'
    
    bucket = s3.Bucket(s3_bucket_name)
    read_address_and_save_dataframe(s3, s3_bucket_name, s3_key, bucket, {'english' : _process_en_row, 'korean' : _process_ko_row})