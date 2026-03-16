from minio import Minio
from minio.error import S3Error
import os
import io
from typing import Optional
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import logging

MINIO_ENDPOINT   = os.getenv("MINIO_ENDPOINT")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")
MINIO_BUCKET     = os.getenv("MINIO_BUCKET")

log = logging.getLogger(__name__)

class MinIOLoader:
    """Gère l'upload de DataFrames Pandas vers MinIO en format Parquet."""

    def __init__(self):
        self.client = Minio(
            MINIO_ENDPOINT,
            access_key=MINIO_ACCESS_KEY,
            secret_key=MINIO_SECRET_KEY,
            secure=False,
        )
        self._ensure_bucket(MINIO_BUCKET)

    def _ensure_bucket(self, bucket: str = "warehouse") -> None:
        print(f"Vérification du bucket {bucket}...")
        print( MINIO_ENDPOINT, "ah " , type(MINIO_ENDPOINT) )
        print(MINIO_ACCESS_KEY, type(MINIO_ACCESS_KEY))
        print(MINIO_SECRET_KEY , type(MINIO_SECRET_KEY))
        if not self.client.bucket_exists(bucket):
            self.client.make_bucket(bucket)
            log.info(f"Bucket '{bucket}' créé.")
        else:
            log.info(f"Bucket '{bucket}' existant.")

    def upload_dataframe(
        self,
        df: pd.DataFrame,
        object_path: str,
        partition_cols: Optional[list] = None,
    ) -> None:
        """
        Upload un DataFrame en Parquet vers MinIO.
        Si partition_cols est spécifié, écrit en partitions Hive-style.
        Ex: s3://lakehouse/warehouse/orders/year=2024/month=1/data.parquet
        """
        if partition_cols:
            for keys, group in df.groupby(partition_cols):
                # Construire le chemin de partition
                if not isinstance(keys, tuple):
                    keys = (keys,)
                parts = "/".join(
                    f"{col}={val}"
                    for col, val in zip(partition_cols, keys)
                )
                path = f"{object_path}/{parts}/data.parquet"
                self._upload_to_minio(group.drop(columns=partition_cols), path)
        else:
            self._upload_to_minio(df, f"{object_path}/data.parquet")

    def _upload_to_minio(self, df: pd.DataFrame, object_name: str) -> None:
        table = pa.Table.from_pandas(df, preserve_index=False)
        buf = io.BytesIO()
        pq.write_table(table, buf, compression="snappy")
        buf.seek(0)
        size = buf.getbuffer().nbytes
        self.client.put_object(
            MINIO_BUCKET,
            object_name,
            buf,
            size,
            content_type="application/octet-stream",
        )
        log.info(f"  ✅ Uploadé : s3://{MINIO_BUCKET}/{object_name} ({size/1024:.1f} KB)")

    def list_objects(self, prefix: str = "") -> list:
        objects = self.client.list_objects(MINIO_BUCKET, prefix=prefix, recursive=True)
        return [obj.object_name for obj in objects]
