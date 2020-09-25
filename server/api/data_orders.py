
from flask import Blueprint, Response
from flasgger import swag_from
from flask_restful import Resource, Api
from server.routes.prometheus import track_requests
from userapp import orders
"""
 created a new instance of the Blueprint class and bound the DataOrders and DataOrdersPandas resources to it.
"""

data_orders_blueprint = Blueprint("data_orders", __name__)
api = Api(data_orders_blueprint)

# The python-flask stack includes the prometheus metrics engine. You can ensure your endpoints
# are included in these metrics by enclosing them in the @track_requests wrapper.

class DataOrders(Resource):  

    # Returns the Orders data in JSON format
    @track_requests
    @swag_from('data_orders.yml')
    def get(self):
        print('[DataOrdersResource] - calling /api/v1/data/orders endpoint')
        return orders.getOrders(),202

class DataOrdersPandas(Resource):  

    # Returns the Orders data in pandas format
    @track_requests
    @swag_from('data_orders_pandas.yml')
    def get(self):
        print('[DataOrdersPandasResource] - calling /api/v1/data/orders/pandas endpoint')
        return Response(orders.getOrdersPanda().to_string(), 202, {'Content-Type': 'text/plaintext'})

api.add_resource(DataOrders, "/api/v1/data/orders")
api.add_resource(DataOrdersPandas, "/api/v1/data/orders/pandas")