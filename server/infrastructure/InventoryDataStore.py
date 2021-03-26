import pandas as pd

class InventoryDataStore():
    instance = None

    @classmethod
    def getInstance(cls):
        if cls.instance == None:
            cls.instance = InventoryDataStore()
        return cls.instance 

    def __init__(self):
        self.inventory = {}

    def getAllLotInventory(self):
        '''
        return an array of json documents
        '''
        jsonArray = []
        for key,value in self.inventory.items():
            print("getAllLotInventory -> " + str(value))
            jsonArray.append(value)
        return jsonArray

    def getAllLotInventoryAsPanda(self):
        return pd.DataFrame.from_dict(self.inventory)

    def addLotToInventory(self,key,lot):
        print("addInventory " + key + " " + str(lot) + " " + str(self))
        self.inventory[key]=lot
    
