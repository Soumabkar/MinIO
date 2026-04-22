import pandas as pd
from pyproj import Transformer

# Transformer réutilisable (thread-safe, toujours x=lon, y=lat)
_transformer = None


def get_transformer(src_epsg: int, dst_epsg: int) -> Transformer:
    global _transformer
    if _transformer is None:
        _transformer = Transformer.from_crs(
            f"EPSG:{src_epsg}",
            f"EPSG:{dst_epsg}",
            always_xy=True   # x=longitude, y=latitude (comme ST_Point(lon, lat))
        )
    return _transformer


def transform_coordinates(
    df: pd.DataFrame,
    lon_col: str,
    lat_col: str,
    src_epsg: int = 4326,
    dst_epsg: int = 3785,
    out_x_col: str = "geo_x",
    out_y_col: str = "geo_y",
) -> pd.DataFrame:
    """
    Équivalent Python de :
      ST_Transform(ST_SetSRID(ST_Point(lon, lat), src_epsg), dst_epsg)

    Ajoute deux colonnes out_x_col et out_y_col au DataFrame.
    """
    transformer = get_transformer(src_epsg, dst_epsg)

    x, y = transformer.transform(
        df[lon_col].to_numpy(),
        df[lat_col].to_numpy(),
    )

    return df.assign(**{out_x_col: x, out_y_col: y})
