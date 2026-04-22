import pandas as pd
from config import (
    SRC_CATALOG, SRC_SCHEMA, SRC_TABLE,
    DST_CATALOG, DST_SCHEMA, DST_TABLE,
    SRC_EPSG, DST_EPSG,
)
from trino_client import read_table, insert_dataframe
from geo_transform import transform_coordinates


def run():
    # 1. Lecture source
    print(f"Lecture de {SRC_CATALOG}.{SRC_SCHEMA}.{SRC_TABLE} ...")
    df = read_table(
        catalog=SRC_CATALOG,
        schema=SRC_SCHEMA,
        table=SRC_TABLE,
        query=f"""
            SELECT
                id,
                val1,
                longitude,
                latitude
            FROM {SRC_TABLE}
        """
    )
    print(f"  {len(df)} lignes lues.")

    # 2. Transformation géographique
    #    ST_Transform(ST_SetSRID(ST_Point(lon, lat), 4326), 3785)
    df = transform_coordinates(
        df,
        lon_col="longitude",
        lat_col="latitude",
        src_epsg=SRC_EPSG,
        dst_epsg=DST_EPSG,
        out_x_col="val_geo_x",
        out_y_col="val_geo_y",
    )

    # 3. Sélection des colonnes cibles
    df_insert = df[["val1", "val_geo_x", "val_geo_y"]]

    # 4. Insertion dans la table cible
    insert_dataframe(df_insert, DST_CATALOG, DST_SCHEMA, DST_TABLE)


if __name__ == "__main__":
    run()
