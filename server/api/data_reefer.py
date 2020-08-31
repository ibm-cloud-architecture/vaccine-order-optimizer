
from flask import Blueprint, Response
from flasgger import swag_from
from flask_restful import Resource, Api
from server.routes.prometheus import track_requests
from userapp import reefer_consumer
"""
 created a new instance of the Blueprint class and bound the DataReefer and DataReeferPandas resources to it.
"""

data_reefer_blueprint = Blueprint("data_reefer", __name__)
api = Api(data_reefer_blueprint)

# The python-flask stack includes the prometheus metrics engine. You can ensure your endpoints
# are included in these metrics by enclosing them in the @track_requests wrapper.

class DataReefer(Resource):  

    # Returns the Reefer data in JSON format
    @track_requests
    @swag_from('data_reefer.yml')
    def get(self):
        print('[DataReeferResource] - calling /api/v1/data/reefer endpoint')
        return reefer_consumer.getEvents(),202

class DataReeferPandas(Resource):  

    # Returns the Reefer data in pandas format
    @track_requests
    @swag_from('data_reefer_pandas.yml')
    def get(self):
        print('[DataReeferPandasResource] - calling /api/v1/data/reefer/pandas endpoint')
        return Response(reefer_consumer.getEventsPanda().to_string(), 202, {'Content-Type': 'text/plaintext'})

api.add_resource(DataReefer, "/api/v1/data/reefer")
api.add_resource(DataReeferPandas, "/api/v1/data/reefer/pandas")