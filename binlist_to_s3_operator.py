import requests

from airflow.models.baseoperator import BaseOperator

def get_bin(bin: int):

    try:
        #Send a GET request to the binlist API
        url = f"https://lookup.binlist.net/{bin}"
        headers = {
            "Accept-Version": "3"
        }
        response = requests.get(url, headers=headers)
        return response
    
    except requests.exceptions.RequestException as e:
        print("Error: Failed to retrieve bin information")
        raise e

def save_to_s3(bin: int, bucket_name: str):

    try:
        bin_data = get_bin(bin).json()
        s3_filename = f"{bin}.json"
        s3_bucket = bucket_name
        s3_path = f"s3://{s3_bucket}/{s3_filename}"
                
        # Upload the file to S3
        s3_client = boto3.client("s3")
        s3object = s3_client.Object(bucket_name, s3_path)
        s3object.put(Body=(bytes(json.dumps(bin_data).encode("UTF-8"))))
        
        print(f"JSON response saved to S3 bucket: {s3_path}")
    
    except Exception as e:
        print(f"Error: {e}")
        raise e

class BinListToS3Operator(BaseOperator):
    def __init__(self, bin: int, bucket_name: str, **kwargs) -> None:    
        super().__init__(**kwargs)
        self.bin = bin
        self.bucket_name = bucket_name


    def execute(self, context):
        try:
            bin_json = get_bin(self.bin).json()
            save_to_s3(self.bin, self.bucket_name)
        except Exception as e:
            print(f"Error: {e}")
            raise e