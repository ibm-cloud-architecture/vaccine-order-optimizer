import pandas as pd
import logging

class InventoryDataStore():

    def __init__(self):
        self.inventory = {}

    def getAllLotInventory(self):
        jsonArray = []
        for key,value in self.inventory.items():
            jsonArray.append(value)
        return jsonArray
    

    def getAllLotInventoryAsPanda(self):
        return pd.DataFrame.from_dict(self.inventory)

    def addLotToInventory(self,key,lot):
        logging.info("add lot " + key + " " + str(lot))
        self.inventory[key]=lot
    