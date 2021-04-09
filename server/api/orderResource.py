
from flask import Blueprint, request
import logging, json
from flasgger import swag_from
from flask_restful import Resource, Api
from server.api.prometheus import track_requests
from server.infrastructure.OrderDataStore import OrderDataStore

"""
 created a new instance of the Blueprint class and bound the NewOrder resource to it.
"""

orders_blueprint = Blueprint("OrderResource", __name__)
api = Api(orders_blueprint)

class OrderResource(Resource):  

    # Need to support asynchronous HTTP Request, return 202 accepted while starting 
    # the processing of generating events. The HTTP header needs to return a
    # location to get the status of the simulator task    
    @track_requests
    @swag_from('orderAPI.yml')
    def post(self):
        
        print('[OrderResource] - New Order post request received')
        order_json = request.get_json(force=True)
        print('[OrderResource] - Order object ' + json.dumps(order_json))
        # TBD: Do some data validation so that we make sure the order comes with the attributes and values we expect
        # Process order and add it to the existing orders
        OrderDataStore.getInstance().processOrder(order_json['order_id'],order_json)
        return "New order processed", 202
    
    @track_requests
    # @swag_from('getOrderAPI.yml')
    def get(self):
        """Get Orders data in JSON format
        ---
        responses:
            202:
                description: Orders data in JSON format
                examples:
                    application-json:  {"VO001": {
                                    "destination": "Paris, France",
                                    "order_id": "VO001",
                                    "priority": 10,
                                    "quantity": 300,
                                    "request_delivery_date": "7/12/2020"
                                },
                                "VO002": {
                                    "destination": "Madrid, Spain",
                                    "order_id": "VO002",
                                    "priority": 2,
                                    "quantity": 100,
                                    "request_delivery_date": "15/12/2020"
                                }
                                }
        """
        print('[OrderResource] - calling /api/v1/orders endpoint')
        return OrderDataStore.getInstance().getOrders(),202

api.add_resource(OrderResource, "/api/v1/orders")