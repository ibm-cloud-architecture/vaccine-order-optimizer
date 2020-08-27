
from flask import Blueprint, request, abort
import logging
from flasgger import swag_from
from flask_restful import Resource, Api
from server.routes.prometheus import track_requests
from userapp.server.domain.doaf_vaccine_order_optimizer import VaccineOrderOptimizer
from flask import current_app as app
from userapp import reefer_consumer, inventory_consumer, transportation_consumer
"""
 created a new instance of the Blueprint class and bound the Controller resource to it.
"""

control_blueprint = Blueprint("control", __name__)
api = Api(control_blueprint)

# The python-flask stack includes the prometheus metrics engine. You can ensure your endpoints
# are included in these metrics by enclosing them in the @track_requests wrapper.

class OrderShipmentController(Resource):  

    def get(self):
        return {"status": "success", "message": "present "}
    

    # Need to support asynchronous HTTP Request, return 202 accepted while starting 
    # the processing of generating events. The HTTP header needs to return a
    # location to get the status of the simulator task    
    @track_requests
    @swag_from('controlapi.yml')
    def post(self):
        app.logger.info("post order received: ")
        order = request.get_json(force=True)
        app.logger.info(order)
        # do some data validation
        # prepare the data add the order to existing orders
        optimizer = VaccineOrderOptimizer(start_date=date(2020, 7, 6), debug=False)
        # call: optimizer.optimize(orders)
        # New call: optimizer.optimize(orders,reefer_consumer.getEvents(), inventory_consumer.getEvents(), transportation_consumer.getEvents())

        if not 'containerID' in order:
            abort(400) 
        return { "reason": "order optimization started"},202
    


api.add_resource(OrderShipmentController, "/voro/api/v1/")