import pandas as pd

class ReeferDataStore():
    reefers = {}
    instance = None

    @classmethod
    def getInstance(cls):
        if cls.instance == None:
            cls.instance = ReeferDataStore()
        return cls.instance 

    def __init__(self):
        pass


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

