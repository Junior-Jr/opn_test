from binlist_to_s3_operator import BinListToS3Operator
from airflow import DAG
from airflow.models.param import Param

with DAG (
    "binlist_to_s3",
    description="Get binlist data and store it in S3",
    catchup = False,
    params={
        "bin": Param(default=12345678 ,type="integer"),
        "bucket_name": Param(default="your-s3-bucket-name" ,type="string")
    }
) as dag:
    
    get_bin_store_task = BinListToS3Operator(
        task_id="get-bin-and-store-task", 
        bin="{{ params.bin }}", 
        bucket_name="{{ params.bucket_name }}"
        )