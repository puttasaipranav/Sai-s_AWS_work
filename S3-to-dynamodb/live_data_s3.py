import boto3
import pandas as pd
S3 = boto3.client('s3')

bucket = 'cdeekfors3'
key = 'cim/hj'
def lambda_handler(event,context):
    def list_files(bucket, key, return_dict=False):
            print(f"Get files from: {bucket}/{key}")
            isTrunc = True
            marker = ""
            return_array = []
            while isTrunc:
                response = S3.list_objects(
                    Bucket=bucket,
                    Prefix=key,
                    Marker=marker
                )
                isTrunc = response["IsTruncated"]
                if "Contents" in response:
                    print(len(response['Contents']))
                    for r in response["Contents"]:
                        if return_dict:
                            return_array.append({'key': r['Key'], 'size': r['Size']})
                        else:
                            return_array.append(r['Key'])
                        marker = r['Key']
                else:
                    self.log.info("S3: No files found")
    
            print(f"Done: {bucket}/{key}")
            return return_array
    def get_bucket_size( bucket, key):
        all_files = list_files(bucket, key, True)
        print(all_files)
        total_size = sum([f['size'] for f in all_files])
        r = {'total_objects': len(all_files), 'total_size': total_size}
        print(r)
        path = 's3://cdeekfors3/cim/hj/event.csv'
        df = pd.read_csv(path)
        df.head()
    get_bucket_size(bucket,key)
    