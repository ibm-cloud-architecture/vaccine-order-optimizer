
from flask import Blueprint, request, Response
import logging, json
from flasgger import swag_from
from flask_restful import Resource, Api
from server.api.prometheus import track_requests
from server.domain.doaf_vaccine_order_optimizer import VaccineOrderOptimizer
from server.infrastructure import ReeferConsumer as reefer_consumer
from server.infrastructure import InventoryConsumer as inventory_consumer
from server.infrastructure import TransportationConsumer as transportation_consumer
from server.domain import Orders as orders
from datetime import date
"""
 created a new instance of the Blueprint class and bound the NewOrder resource to it.
"""

optimize_blueprint = Blueprint("optimize", __name__)
api = Api(optimize_blueprint)


class Optimize(Resource):  
    '''
    Expose an optimize API to process an order with existing orders
    '''
    # Need to support asynchronous HTTP Request, return 202 accepted while starting 
    # the processing of generating events. The HTTP header needs to return a
    # location to get the status of the simulator task    
    @track_requests
    @swag_from('optimize.yml')
    def post(self):
        print('[Optimize] - calling /api/v1/optimize endpoint')
        # Create the optimizer
        optimizer = VaccineOrderOptimizer(start_date=date(2020, 9, 1), debug=False)
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