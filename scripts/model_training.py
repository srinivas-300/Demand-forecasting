import pandas as pd 
import numpy as np
from sklearn.preprocessing import StandardScaler
from sklearn.model_selection import train_test_split
from azure.storage.blob import BlobServiceClient , BlobClient , ContainerClient
from io import ByterIO
from statsmodels.tsa.arima.model import ARIMA
import pickle
from sklearn.tree import DecisionTreeRegressor
from sklearn.ensemble import RandomForestRegressor


def upload(container_name,blob_name , model):
    
    with open('f{blob_name}.pkl', 'wb') as f:
        pickle.dump(model, f)
    
    container2 = conn.get_container_client(container_name)
    
    blob2 = container2.get_blob_client(blob_name)
    
    try:
        if blob2.exists():
            blob2.upload_blob('f{blob_name}.pkl',overwrite=True)
        else:
            blob2.upload_blob('f{blob_name}.pkl')
    except Exception as e :
        print(e)
    


def training():
    conn = BlobServiceCLient.from_connection_string("")

    container = conn.get_container_client("")

    blob = container.get_blob_client("")

    file = blob.download_blob().readall()
    
    df = pd.read_csv(pd.compat.BytesIO(file))
    
    df.drop(columns=["Product_id","Product_Code"],inplace=True)
    
    df.sort_values("Date" , inplace=True)
    
    df.set_index("Date",inplace=True)
    
    df.index = pd.to_datetime(df.index)
    
    
    
    df2 = df[df["Warehouse"]==2]
    
    df2=df2.groupby("Date").agg({"Petrol_price": "mean"})
    
    df2 = df2.asfreq('D')
    
    df2['Petrol_price'] = df2['Petrol_price'].fillna(method='ffill')
    
    
    
    #ARIMA
    
    model = ARIMA(df2["Petrol_price"],order=(1,1,1))
    
    model = model.fit()

    upload("","arima_model.pkl" , model)
    
    #Prophet
    
    df2= df2.reset_index().rename(columns={"Date":"ds","Petrol_price":"y"})
    
    df2['ds'] = pd.to_datetime(df2['ds'])
    
    model = Prophet()
    
    model.fit(df2)
    
    upload("","Prophet.pkl" , model)
    
    ##Decision Tree Regressor
    
    x_train , x_test , y_train , y_test = train_test_split(df.drop(columns="Petrol_price") , df["Petrol_price"] , random_state = 10 , test_size = 0.01) 
    
    dt_model = DecisionTreeRegressor()
    dt_model.fit(x_train, y_train)
    upload("","Decision_Tree.pkl" , dt_model)
    
    ##Random Forest Regressor
    
    rf_model = RandomForestRegressor()
    rf_model.fit(x_train, y_train)
    upload("","Random_Forest.pkl" , rf_model)
    
    
    
    
    