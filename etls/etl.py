import pandas as pd
from pyproj import Transformer


def fetch(url: str) -> pd.DataFrame:
    """
    This function will fetch data from the website and then give out a dataframe
    """
    return pd.read_csv(url)


def transform_df(df: pd.DataFrame) -> pd.DataFrame:
    """
    This function will perform the simple data transformation. Since this is a end to end
    data pipeline we should not do much of the transformation here because that can be done
    inside the warehouse
    """
    
  
    # 1. Check if _id has all unique values

    # change all the dates into date time
    columns_with_date = ["YEAR REGISTERED", "YEAR BUILT", "YEAR EVALUATED"]
    for col in columns_with_date:
        df[col] = pd.to_datetime(df[col].astype(str), format="%Y.0")

    # Delete all rows that do not have both coordinates pairs
    # df.index[df['BoolCol'] == True].tolist()
    df.drop(
        df[
            df["X"].isnull()
            & df["Y"].isnull()
            & df["LONGITUDE"].isnull()
            & df["LATITUDE"].isnull()
        ].index,
        inplace=True,
    )
    #convert the X & Y coordinates into Longitude and latitude  and vice versa when one set is not available
    
def transform_XY_coordinates(coord1,coord2) #TODO add  typing:

    transformer =Transformer.from_crs("EPSG:3857", "EPSG:4326")
    longitude, latitude = transformer.transform(coord1,coord2)
    return longitude, latitude

def transform_LON_LAT_coordinates(coord1,coord2) #TODO add  typing:

    transformer =Transformer.from_crs("EPSG:4326","EPSG:3857")
    X, Y = transformer.transform(coord1,coord2)
    return X,Y



def etl() -> None:
    """
    The main function which will call other functions to perform the ETL
    """
    url = "https://ckan0.cf.opendata.inter.prod-toronto.ca/dataset/4ef82789-e038-44ef-a478-a8f3590c3eb1/resource/7fa98ab2-7412-43cd-9270-cb44dd75b573/download/Apartment%20Building%20Evaluations%202023%20-%20current.csv"
    df_raw = fetch(url)
    df_cleaned = transform_df(df_raw)


if __name__ == "__main__":
    etl()
