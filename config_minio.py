from minio import Minio
import configparser

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

# lister les buckets
buckets = minio_client.list_buckets()
for bucket in buckets:
    print(bucket.name)

# Create bucket if it doesn't exist

if not minio_client.bucket_exists("dev"):
        minio_client.make_bucket("dev")

# Upload file (ensure table.csv exists)
try:
    minio_client.fput_object("dev", "dev/table.csv", "table.csv")
    print("File uploaded successfully!")
except FileNotFoundError:
    print("Error: table.csv not found in the current directory.")
except Exception as e:
    print(f"An error occurred: {e}")
