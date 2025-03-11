from azure.storage.blob import BlobServiceClient , BlobClient , ContainerClient
import pandas as pd 
import numpy as np
from io import BytesIO 
    

def preprocess():
    
    # Download the file from azure blob storage
    
    conn = BlobServiceCLient.from_connection_string("")

    container = conn.get_container_client("")

    blob = container.get_blob_client("")

    file = blob.download_blob().readall()
    
    df = pd.read_csv(pd.compat.BytesIO(file))
    
    #print(df.head())  
    
    df.dropna(inplace=True)

    df.drop_duplicates(inplace=True)

    df["Date"] = pd.to_datetime(df["Date"])

    df["Product_Code"] = df["Product_Code"].apply(lambda x : x.split("_")[1])

    df["Warehouse"] = df["Warehouse"].apply(lambda x : x.split("_")[1])

    df["Product_Category"] = df["Product_Category"].apply(lambda x : int(x.split("_")[1]))

    df["Warehouse"] = df["Warehouse"].replace({"A" : 0 , "C":1 , "J": 2 , "S":3})

    df["Product_Code"] =  df["Product_Code"].apply(lambda x : int(x))

    df["StateHoliday"] = df["StateHoliday"].astype("bool")

    df["SchoolHoliday"] = df["SchoolHoliday"].astype("bool")

    df["Open"] = df["Open"].astype("bool")

    df["Promo"] = df["Promo"].astype("bool")
    
    #Upload file back to azure blob
    
    output = BytesIO()
    df.to_csv(output)
    output.seek(0)
    
    blob.upload_blob(output,overwrite=True)
    
    
    return