
from flask import Blueprint, request, Response
import logging, json
from flasgger import swag_from
from flask_restful import Resource, Api
from server.routes.prometheus import track_requests
from userapp.server.domain.doaf_vaccine_order_optimizer import VaccineOrderOptimizer
from userapp import reefer_consumer, inventory_consumer, transportation_consumer, orders
from datetime import date
"""
 created a new instance of the Blueprint class and bound the NewOrder resource to it.
"""

optimize_blueprint = Blueprint("optimize", __name__)
api = Api(optimize_blueprint)

# The python-flask stack includes the prometheus metrics engine. You can ensure your endpoints
# are included in these metrics by enclosing them in the @track_requests wrapper.

class Optimize(Resource):  

    # Need to support asynchronous HTTP Request, return 202 accepted while starting 
    # the processing of generating events. The HTTP header needs to return a
    # location to get the status of the simulator task    
    @track_requests
    @swag_from('optimize.yml')
    def post(self):
        print('[Optimize] - calling /api/v1/optimize endpoint')
        # Create the optimizer
        optimizer = VaccineOrderOptimizer(start_date=date(2020, 7, 6), debug=False)
        optimizer.prepare_data(orders.getOrdersPanda(), reefer_consumer.getEventsPanda(), inventory_consumer.getEventsPanda(), transportation_consumer.getEventsPanda())
        optimizer.optimize()
        
        # Get the optimization solution
        plan_orders, plan_orders_details, plan_shipments = optimizer.get_sol_panda()
        result = "Orders\n"
        result += "------------------\n"
        result += plan_orders.to_string() + "\n\n"
        result += "Order Details\n"
        result += "------------------\n"
        result += plan_orders_details.to_string() + "\n\n"
        result += "Shipments\n"
        result += "------------------\n"
        result += plan_shipments.to_string()
        
        return Response(result, 202, {'Content-Type': 'text/plaintext'})

api.add_resource(Optimize, "/api/v1/optimize")