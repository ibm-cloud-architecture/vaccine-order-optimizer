
from flask import Blueprint, request, abort
import logging, json
from flasgger import swag_from
from flask_restful import Resource, Api
from server.routes.prometheus import track_requests
from userapp.server.domain.doaf_vaccine_order_optimizer import VaccineOrderOptimizer
from flask import current_app as app
from userapp import reefer_consumer, inventory_consumer, transportation_consumer, orders
"""
 created a new instance of the Blueprint class and bound the OrderShipmentController resource to it.
"""

new_order_blueprint = Blueprint("new_order", __name__)
api = Api(new_order_blueprint)

# The python-flask stack includes the prometheus metrics engine. You can ensure your endpoints
# are included in these metrics by enclosing them in the @track_requests wrapper.

class OrderShipmentController(Resource):  

    def get(self):
        return {"status": "success", "message": "present "}
    

    # Need to support asynchronous HTTP Request, return 202 accepted while starting 
    # the processing of generating events. The HTTP header needs to return a
    # location to get the status of the simulator task    
    @track_requests
    @swag_from('new_order.yml')
    def post(self):
        print('[OrderShipmentController] - New Order post request received')
        order_json = request.get_json(force=True)
        print('[OrderShipmentController] - Order object ' + json.dumps(order_json))
        # do some data validation
        # Process order and add it to the existing orders
        orders.processOrder(order_json)
        # #####optimizer = VaccineOrderOptimizer(start_date=date(2020, 7, 6), debug=False)
        # call: optimizer.optimize(orders)
        # New call: optimizer.optimize(orders,reefer_consumer.getEvents(), inventory_consumer.getEvents(), transportation_consumer.getEvents())

        # if not 'containerID' in order:
        #     abort(400) 
        return { "reason": "New order processed"},202
    


api.add_resource(OrderShipmentController, "/voro/api/v1/")