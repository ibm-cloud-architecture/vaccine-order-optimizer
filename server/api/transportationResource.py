
from flask import Blueprint, Response
from flasgger import swag_from
from flask_restful import Resource, Api
from server.api.prometheus import track_requests
from server.infrastructure.TransportationDataStore import TransportationDataStore
import logging

"""
 created a new instance of the Blueprint class and bound the DataTransportation and DataTransportationPandas resources to it.
"""

data_transportation_blueprint = Blueprint("data_transportation", __name__)
transportApi = Api(data_transportation_blueprint)

# The python-flask stack includes the prometheus metrics engine. You can ensure your endpoints
# are included in these metrics by enclosing them in the @track_requests wrapper.

class TransportationResource(Resource):  

    def __init__(self,transportationStore):
        self.store = transportationStore

    # Returns the Transportation data in JSON format
    @track_requests
    @swag_from('transportationAPI.yml')
    def get(self):
        logging.debug('[TransportationResource] - calling /api/v1/data/transportations endpoint')
        list = self.store.getAllTransportations()
        print(list)
        return list,202

class TransportationPandasResource(Resource):  
    def __init__(self, transportationStore):
        self.transportationStore = transportationStore
    
    # Returns the Transportation data in pandas format
    @track_requests
    @swag_from('transportationPandasAPI.yml')
    def get(self):
        logging.debug('[TransportationResourcePandas] - calling /api/v1/data/transportations/pandas endpoint')
        return Response(self.transportationStore.getAllTransportationsAsPanda().transpose().to_string(), 202, {'Content-Type': 'text/plaintext'})