
from flask import Blueprint
from flasgger import swag_from
from flask_restful import Resource, Api
from server.routes.prometheus import track_requests
from userapp import reefer_consumer, inventory_consumer, transportation_consumer, orders
"""
 created a new instance of the Blueprint class and bound the Data resource to it.
"""

data_blueprint = Blueprint("data", __name__)
api = Api(data_blueprint)

# The python-flask stack includes the prometheus metrics engine. You can ensure your endpoints
# are included in these metrics by enclosing them in the @track_requests wrapper.

class Data(Resource):  

    # Returns the aggregations of all data
    @track_requests
    @swag_from('data.yml')
    def get(self):
        print('[DataResource] - calling /api/v1/data endpoint')
        # Aggregate all data in a single dict
        aggregate_dict = {}
        aggregate_dict['REEFER'] = reefer_consumer.getEvents()
        aggregate_dict['INVENTORY'] = inventory_consumer.getEvents()
        aggregate_dict['TRANSPORTATION'] = transportation_consumer.getEvents()
        aggregate_dict['ORDERS'] = orders.getOrders()
        return aggregate_dict,202

api.add_resource(Data, "/api/v1/data")