from azure.storage.blob import BlobServiceClient , BlobClient ,ContainerClient
import pickle

def predictions(model_name , test_data = []):
    
    # for demand calculation of warehouse 2 
    if model_name == "ARIMA":
        model_name2 = "arima.pkl"
    
    elif model_name == "Decision Tree":
        model_name2 = "dt.pkl"
    
    # for demand calculation of warehouse 2
    
    elif model_name == "Prophet":
        model_name2 = "prophet.pkl"
        
    elif model_name == "Random Forest":
        model_name2 = "rf.pkl"
        
    elif model_name == "XG Boost":
        model_name2 = "xgb.pkl"
        
    conn = BlobServiceClient.from_connection_string("")
    container = conn.get_container_client("")
    blob = container.get_blob_client("f{model_name2}")
    
    with open("model.pkl" , "rb") as f:
        model = pickle.load(f)
        
    if model_name == "ARIMA":
        return model.forecast(steps=7)
    
    elif model_name == "Decision Tree":
        return model.predict(test_data)
    
    elif model_name == "Prophet":
        return model.predict(model.make_future_dataframe(periods=7))
    
    elif model_name == "Random Forest":
        return model.predict(test_data)
    
    elif model_name == "XG Boost":
        return model.predict(test_data)
    
    else:
        return "Model not found!"
        
