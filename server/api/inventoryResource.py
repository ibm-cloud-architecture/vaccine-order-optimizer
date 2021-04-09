
from flask import Blueprint
from flasgger import swag_from
from flask_restful import Resource, Api
from server.api.prometheus import track_requests

import logging
"""
 created a new instance of the Blueprint class and bound the DataInventory and DataInventoryPandas resources to it.
"""

data_inventory_blueprint = Blueprint("data_inventory", __name__)
inventoryApi = Api(data_inventory_blueprint)

# The python-flask stack includes the prometheus metrics engine. You can ensure your endpoints
# are included in these metrics by enclosing them in the @track_requests wrapper.

class DataInventory(Resource):  

    def __init__(self, inventoryStore):
        self.inventoryStore = inventoryStore
    
    # Returns the Inventory data in JSON format
    @track_requests
    @swag_from('data_inventory.yml')
    def get(self):
        logging.debug('[DataInventoryResource] - calling /api/v1/data/inventory endpoint')
        return self.inventoryStore.getAllLotInventory(),200, {'Content-Type' : 'application/json'}

class DataInventoryPandas(Resource):  
    def __init__(self, inventoryStore):
        self.inventoryStore = inventoryStore
    
    # Returns the Inventory data in pandas format
    @track_requests
    @swag_from('data_inventory_pandas.yml')
    def get(self):
        logging.debug('[DataInventoryPandasResource] - calling /api/v1/data/inventory/pandas endpoint')
        return self.inventoryStore.getAllLotInventoryAsPanda().transpose().to_string(), 200

