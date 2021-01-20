import pandas as pd

class DataStore():
    transportations = {}
    inventory = {}
    reefers = {}
    orders = {}
    instance = None

    @classmethod
    def getInstance(cls):
        if cls.instance == None:
            cls.instance = DataStore()
        return cls.instance 

    def __init__(self):
        pass

    def getAllTransportations(self):
        '''
        return an array of json documents
        '''
        jsonArray = []
        for key,value in self.transportations.items():
            jsonArray.append(value)
        return jsonArray
    
    def getAllTransportationsAsPanda(self):
        return pd.DataFrame.from_dict(self.transportations)

    def addTransportation(self,key,transportation):
        self.transportations[key]=transportation

    def getAllReefers(self):
        jsonArray = []
        for key,value in self.reefers.items():
            jsonArray.append(value)
        return jsonArray
    
    def getAllReefersAsPanda(self):
        return pd.DataFrame.from_dict(self.reefers)

    def addReefer(self,key,reefer):
        print("add " + key + " " + str(reefer))
        self.reefers[key]=reefer

    def getAllLotInventory(self):
        jsonArray = []
        for key,value in self.inventory.items():
            jsonArray.append(value)
        return jsonArray
    
    def getAllLotInventoryAsPanda(self):
        return pd.DataFrame.from_dict(self.inventory)

    def addLotToInventory(self,key,lot):
        print("add " + key + " " + lot)
        self.inventory[key]=lot
    
    def getOrders(self):
        jsonArray = []
        for key,value in self.orders.items():
            jsonArray.append(value)
        return jsonArray
    
    def getOrdersAsPanda(self):
        return pd.DataFrame.from_dict(self.orders)

    def processOrder(self, key, order_json):
        self.orders[key] = order_json
