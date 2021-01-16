import pandas as pd

class DataStore():
    transportations = {}
    inventory = {}
    reefers = {}
    instance = None

    @classmethod
    def getInstance(cls):
        if cls.instance == None:
            cls.instance = DataStore()
        return cls.instance 

    def __init__(self):
        pass

    def getAllTransportations(self):
        return self.transportations
    
    def getAllTransportationsAsPanda(self):
        return pd.DataFrame.from_dict(self.transportations)

    def addTransportation(self,key,transportation):
        self.transportations[key]=transportation

    def getAllReefers(self):
        return self.reefers
    
    def getAllReefersAsPanda(self):
        return pd.DataFrame.from_dict(self.reefers)

    def addReefer(self,key,reefer):
        self.reefers[key]=reefer

    def getAllLotInventory(self):
        return self.inventory
    
    def getAllLotInventoryAsPanda(self):
        return pd.DataFrame.from_dict(self.inventory)

    def addLotToInventory(self,key,lot):
        self.inventory[key]=lot
    
