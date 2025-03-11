from azure.storage.blob import BlobServiceClient , ContainerClient , BlobService 
from predictions import predictions
import BytesIO
import pandas as pd 
import datetime
import numpy as np


def e_order():
    
    ## Prophet model
    
    demand = predictions("Prophet")
    
    ## Current stock from blob 
    
    conn = BlobServiceClient.from_connection_string("")
    container = conn.get_container_client("")
    blob = cont.get_blob_service("")
    
    file = blob.download_blob().readall()
    
    df = pd.read_csv(pd.compat.BytesIO(file))
    
    # df contains warehouse number , stock , sales , date 
    
    stock = #### determine the stock accordingly 
    
    lead_time = 5  # Days supplier takes to deliver stock
    safety_stock = 50  # Buffer for uncertainties
    
    today = datetime.date.today()
    
    ## average daily demand for each warehouse
    
    daily_avg_demand = df[["date"]==today].groupby("warehouse").agg({"sales":"mean"})
    
    # Compute Reorder Point
    
    reorder_point = (lead_time * np.array(daily_avg_demand.values)) + safety_stock
    
    print(reorder_point)
    
    #Economic order quantity
    
    ordering_cost = 20  # Cost per order
    holding_cost = 5  # Cost per unit held in inventory per year
    annual_demand = daily_avg_demand * 365  # Approximate yearly demand
    
    # EOQ Formula: sqrt((2 * annualdemand * holdingcost) / Holdingcost)
    
    EOQ = np.sqrt((2 * annual_demand * ordering_cost) / holding_cost)
    
    #returning EOQ values for the items that are to be replenished and the warehouse number 
    
    return EOQ(np.where(stock<reorder_point)) , np.where(stock<reorder_point)