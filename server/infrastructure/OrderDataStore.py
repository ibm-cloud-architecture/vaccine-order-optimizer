import pandas as pd

class OrderDataStore():
    
    instance = None

    @classmethod
    def getInstance(cls):
        if cls.instance == None:
            cls.instance = OrderDataStore()
        return cls.instance 

    def __init__(self):
        self.orders = {}
        

    def getOrders(self):
        jsonArray = []
        for key,value in self.orders.items():
            jsonArray.append(value)
        return jsonArray
    
    def getOrdersAsPanda(self):
        return pd.DataFrame.from_dict(self.orders)

    def processOrder(self, key, order_json):
        print("[OrderDataStore] - addOrder " + str(key) + " " + str(order_json))
        self.orders[key] = order_json
