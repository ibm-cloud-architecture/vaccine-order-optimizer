import pandas as pd

class TransportationDataStore():
    instance = None

    @classmethod
    def getInstance(cls):
        if cls.instance == None:
            cls.instance = TransportationDataStore()
        return cls.instance 

    def __init__(self):
        self.transportations = {}

    def getAllTransportations(self):
        '''
        return an array of json documents
        '''
        jsonArray = []
        for key,value in self.transportations.items():
            # print("getAllTransportations -> " + str(value))
            jsonArray.append(value)
        return jsonArray
    
    def getAllTransportationsAsPanda(self):
        return pd.DataFrame.from_dict(self.transportations)

    def addTransportation(self,key,transportation):
        print("[TransportationDataStore] - addTransportation " + key + " " + str(transportation))
        self.transportations[key]=transportation

  