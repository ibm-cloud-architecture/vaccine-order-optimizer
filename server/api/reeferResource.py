
from flask import Blueprint, Response
from flasgger import swag_from
from flask_restful import Resource, Api
from server.api.prometheus import track_requests
from server.infrastructure.DataStore import DataStore

"""
 created a new instance of the Blueprint class and bound the DataReefer and DataReeferPandas resources to it.
"""

data_reefer_blueprint = Blueprint("data_reefer", __name__)
api = Api(data_reefer_blueprint)

# The python-flask stack includes the prometheus metrics engine. You can ensure your endpoints
# are included in these metrics by enclosing them in the @track_requests wrapper.

class ReeferResource(Resource):  

    # Returns the Reefer data in JSON format
    @track_requests
    @swag_from('reeferAPI.yml')
    def get(self):
        print('[DataReeferResource] - calling /api/v1/data/reefer endpoint')
        ds = DataStore.getInstance()
        return ds.getAllReefers(),202

api.add_resource(ReeferResource, "/api/v1/data/reefer")