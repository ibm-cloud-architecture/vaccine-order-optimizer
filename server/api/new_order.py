
from flask import Blueprint, request
import logging, json
from flasgger import swag_from
from flask_restful import Resource, Api
from server.routes.prometheus import track_requests
from userapp import orders
"""
 created a new instance of the Blueprint class and bound the NewOrder resource to it.
"""

new_order_blueprint = Blueprint("new_order", __name__)
api = Api(new_order_blueprint)

class NewOrder(Resource):  

    # Need to support asynchronous HTTP Request, return 202 accepted while starting 
    # the processing of generating events. The HTTP header needs to return a
    # location to get the status of the simulator task    
    @track_requests
    @swag_from('new_order.yml')
    def post(self):
        print('[NewOrder] - New Order post request received')
        order_json = request.get_json(force=True)
        print('[NewOrder] - Order object ' + json.dumps(order_json))
        # TBD: Do some data validation so that we make sure the order comes with the attributes and values we expect
        # Process order and add it to the existing orders
        orders.processOrder(order_json)
        return "New order processed", 202

api.add_resource(NewOrder, "/api/v1/new-order")