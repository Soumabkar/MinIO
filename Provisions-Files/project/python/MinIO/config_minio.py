from minio import Minio
import configparser
import io
import pandas as pd
from minio.commonconfig import CopySource


# Charger la configuration depuis le fichier config.ini
config = configparser.ConfigParser(allow_no_value=True)
config.read('config.ini')

# Créer une instance du client MinIO en utilisant les paramètres du fichier de configuration
minio_client = Minio(
    config['minio']['endpoint'],
    access_key=config['minio']['access_key'],
    secret_key=config['minio']['secret_key'],
    secure=config.getboolean('minio', 'secure')
)


# Create bucket if it doesn't exist

if not minio_client.bucket_exists("dev"):
        minio_client.make_bucket("dev")

if not minio_client.bucket_exists("dev1"):
        minio_client.make_bucket("dev1")

# lister les buckets
buckets = minio_client.list_buckets()
for bucket in buckets:
    print("Bucket:", bucket.name)

# Upload file (ensure table.csv exists)
try:
    minio_client.fput_object("dev", "dev/table.csv", "table.csv")
    minio_client.fput_object("dev1", "dev1/table_dev1.csv", "table_dev1.csv")
    print("File uploaded successfully!")
except FileNotFoundError:
    print("Error: table.csv not found in the current directory.")
except Exception as e:
    print(f"An error occurred: {e}")

# List files in the bucket
if minio_client.bucket_exists("dev"):

    fifles = minio_client.get_object("dev", "dev/table.csv")
    #print("files:", fifles , "type:", type(fifles))
    data = pd.read_csv(io.BytesIO(fifles.read())) 
    print(data)

    result_stat = minio_client.stat_object("dev", "dev/table.csv")
    print("type of result_stat:", type(result_stat))
    print("File size:", result_stat.size, "bytes")

    source = CopySource("dev", "dev/table.csv")
    result_copy = minio_client.copy_object("dev1",# destination bucket
                "dev1/test/table1.csv",   # destination object name
                    source             # source must be CopySource
                    )
    print("Copied successfully:", result_copy)

    minio_client.remove_object("dev" , "dev/table1.csv")
    minio_client.remove_object("dev" , "table1.csv")
