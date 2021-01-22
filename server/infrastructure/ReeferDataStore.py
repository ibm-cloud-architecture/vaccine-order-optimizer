import pandas as pd
import logging

class ReeferDataStore():
    
    instance = None

    @classmethod
    def getInstance(cls):
        if cls.instance == None:
            cls.instance = ReeferDataStore()
        print(cls.instance)
        return cls.instance 

    def __init__(self):
        self.reefers = {}
        print(" ReeferDataStore ctr called")


    def getAllReefers(self):
        jsonArray = []
        logging.info(str(self.reefers))
        for key,value in self.reefers.items():
            jsonArray.append(value)
        return jsonArray
    
    def getAllReefersAsPanda(self):
        return pd.DataFrame.from_dict(self.reefers)

    def addReefer(self,key,reefer):
        logging.info("addReefer " + key + " " + str(reefer) + " to " + str(self))
        self.reefers[key]=reefer

