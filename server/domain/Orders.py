import json, threading, time
import pandas as pd

class Orders:
    """ 
    This class is meant to be instantiated once when the application starts up
    to store the orders coming into the system
    """
    def __init__(self):
        print("[Orders] - Initializing the orders object")
        self.orders = {}
        self.orders_panda = {}
        self.index = 0
    
    def processOrder(self, order_json):
        if ( len(self.orders_panda) == 0 ):
            for key, value in order_json.items():
                self.orders_panda[key] = {0:value}
            self.index+=1
        else:
            for key, value in order_json.items():
                intermediate = self.orders_panda[key]
                intermediate[self.index] = value
                self.orders_panda[key] = intermediate
            self.index+=1
        self.orders[order_json['order_id']] = order_json
    
    def getOrders(self):
        return self.orders
    
    def getOrdersPanda(self):
        return pd.DataFrame.from_dict(self.orders_panda)