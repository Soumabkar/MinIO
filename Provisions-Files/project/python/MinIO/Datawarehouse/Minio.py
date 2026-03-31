from minio import Minio
import io
from typing import Optional
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from utils.env import env
import logging

MINIO_BUCKET     = env("MINIO_BUCKET")
MINIO_ENDPOINT   = env("MINIO_ENDPOINT")
MINIO_ACCESS_KEY = env("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = env("MINIO_SECRET_KEY")
DATA_FOLDER      = env("DATA_FOLDER")

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

    def _ensure_bucket(self, bucket: str = MINIO_BUCKET) -> None:
        if not self.client.bucket_exists(bucket):
            self.client.make_bucket(bucket)
            log.info(f"Bucket '{bucket}' créé.")
        else:
            log.info(f"Bucket '{bucket}' existant.")

    def upload_dataframe(
        self,
        df: pd.DataFrame,
        table_name: str,
        partition_cols: Optional[list] = None,
    ) -> None:
        """
        Upload un DataFrame en Parquet vers MinIO.
        Chemin final : s3://{MINIO_BUCKET}/{DATA_FOLDER}/{table_name}/[partitions/]data.parquet
        """
        base_path = f"{DATA_FOLDER}/{table_name}"

        if partition_cols:
            for keys, group in df.groupby(partition_cols):
                if not isinstance(keys, tuple):
                    keys = (keys,)
                parts = "/".join(
                    f"{col}={val}" for col, val in zip(partition_cols, keys)
                )
                object_name = f"{base_path}/{parts}/data.parquet"
                self._upload_to_minio(group.drop(columns=partition_cols), object_name)
        else:
            object_name = f"{base_path}/data.parquet"
            self._upload_to_minio(df, object_name)

    def _upload_to_minio(self, df: pd.DataFrame, object_name: str) -> None:
        table = pa.Table.from_pandas(df, preserve_index=False)

        # Convertir timestamps NANOS → MICROS (compatibilité Spark)
        new_schema = [
            field.with_type(pa.timestamp("us", tz=field.type.tz))
            if pa.types.is_timestamp(field.type) and field.type.unit == "ns"
            else field
            for field in table.schema
        ]
        table = table.cast(pa.schema(new_schema))

        buf = io.BytesIO()
        pq.write_table(
            table, buf,
            compression="snappy",
            coerce_timestamps="us",
            allow_truncated_timestamps=True,
        )
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