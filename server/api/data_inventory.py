
from flask import Blueprint, Response
from flasgger import swag_from
from flask_restful import Resource, Api
from server.routes.prometheus import track_requests
from userapp import inventory_consumer
"""
 created a new instance of the Blueprint class and bound the DataInventory and DataInventoryPandas resources to it.
"""

data_inventory_blueprint = Blueprint("data_inventory", __name__)
api = Api(data_inventory_blueprint)

# The python-flask stack includes the prometheus metrics engine. You can ensure your endpoints
# are included in these metrics by enclosing them in the @track_requests wrapper.

class DataInventory(Resource):  

    # Returns the Inventory data in JSON format
    @track_requests
    @swag_from('data_inventory.yml')
    def get(self):
        print('[DataInventoryResource] - calling /api/v1/data/inventory endpoint')
        return inventory_consumer.getEvents(),202

class DataInventoryPandas(Resource):  

    # Returns the Inventory data in pandas format
    @track_requests
    @swag_from('data_inventory_pandas.yml')
    def get(self):
        print('[DataInventoryPandasResource] - calling /api/v1/data/inventory/pandas endpoint')
        return Response(inventory_consumer.getEventsPanda().to_string(), 202, {'Content-Type': 'text/plaintext'})

api.add_resource(DataInventory, "/api/v1/data/inventory")
api.add_resource(DataInventoryPandas, "/api/v1/data/inventory/pandas")