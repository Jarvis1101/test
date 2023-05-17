from google.cloud import bigquery
from google.cloud import storage
from datetime import datetime, timedelta
import os
from google.cloud import logging
from math import ceil
from opensearchpy import OpenSearch

#Environment Variable
host=os.environ.get('Host')
port=os.environ.get('Open_Port')
Username=os.environ.get('Username')
Pass=os.environ.get('Password')
Scheme=os.environ.get('Scheme')
index_name=os.environ.get('index_name')

#opensearch client
opensearch_client=OpenSearch(hosts=[{'host':host,'port':port,'scheme':Scheme}],
                              http_compress=True,
                              http_auth=(Username,Pass),
                              use_ssl=True,
                              ssl_assert_hostname=False,
                              ssl_show_warn=False,
                              verify_certs=False,

                             )
opensearch_client.info()

client = bigquery.Client()
ts = datetime.now()
CHUNK_SIZE = int(os.environ.get('CHUNK_SIZE'))
date_path =  ts.strftime('%Y-%m-%d')
logging_client = logging.Client()

# The name of the log to write to
log_name = "elastic"
# Selects the log to write to
logger = logging_client.logger(log_name)

def split_dataframe(df, chunk_size = CHUNK_SIZE): 
    chunks = list()
    num_chunks = ceil(len(df)/chunk_size)
    print(num_chunks)
    for i in range(num_chunks):
        chunks.append(df.iloc[i*chunk_size:(i+1)*chunk_size])
    return chunks

def big_query_to_csv():
    query = """
        select CAST(GENERATE_UUID() as STRING) as id,product_code,product_name,brand_name,
        CONCAT('["', category_l1, '", "', category_l2, '", "',category_l3, '"]') tags 
        from `gotoko-infra-prod.reference.dd_product` order by product_code
    """.format(date_path)
    df = client.query(query).to_dataframe()
    df.columns = [
            'id',
            'product_code',
            'product_name', 
            'brand_name', 
            'tags', 
            ]
    print(df)
    for index, row in df.iterrows():
        document_id=row['id']
        document={
            'product_code':row['product_code'],
            'product_name':row['product_name'],
            'brand_name':row['brand_name']
        }
        
        query = {
            'size': 5,
            'query': {
                'multi_match': {
                'query': row['product_code'],
                'fields': ['product_code']
                }
            }
        }

        results = opensearch_client.search(
            body = query,
            index = index_name
        )

        if results["hits"]["total"]["value"] > 0:
            document_id=results["hits"]["hits"][0]["_id"]
            del_res=opensearch_client.delete(
                index=index_name,
                id=document_id
            )

        response=opensearch_client.index(
            index=index_name,
            body=document,
            id=document_id,
            refresh=True
        )
        logger.log_text("ID: {} with document: {} is inserted by scheduler!".format(document_id, document), severity="INFO")

    logger.log_text("Data Inserted!", severity="INFO")


def opensearch_main(request):
    """Responds to any HTTP request.
    Args:
        request (flask.Request): HTTP request object.
    Returns:
        The response text or any set of values that can be turned into a
        Response object using
        `make_response <http://flask.pocoo.org/docs/1.0/api/#flask.Flask.make_response>`.
    """
    request_json = request.get_json()
    big_query_to_csv()
    
    if request.args and 'message' in request.args:
        return request.args.get('message')
    elif request_json and 'message' in request_json:
        return request_json['message']
    else:
        return f'Job Executed!'