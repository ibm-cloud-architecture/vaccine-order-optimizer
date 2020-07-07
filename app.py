# Import modules
from doaf_vaccine_order_optimizer import VaccineOrderOptimizer
from flask import Flask, request
import pandas as pd
from datetime import date

# Flask configuration
app = Flask(__name__)

@app.route("/")
def home():
    return "Vaccine Optimization for Orders and Reefers!"

@app.route('/optimize', methods=['POST', 'GET'])
def optimize():
    # initialize the optimizer and load reference data
    optimizer = VaccineOrderOptimizer(start_date=date(2020, 7, 6), debug=False)
    optimizer.load_data_csv("TC001")

    # Read json and convert to orders dataframe
    orders = pd.DataFrame(request.get_json(force=True))
    orders['RDD'] = pd.to_datetime(orders['RDD'], format='%m/%d/%Y')
    optimizer.optimize(orders)
    
    # post optimiztion
    print("\n  ".join(optimizer.log_msgs))
    return optimizer.get_sol_json()

# Run app
if __name__ == '__main__':
    app.run()

# Start the server
# python -m flask run

# Sample calls: 
# curl -X POST http://127.0.0.1:5000/optimize -H "Content-Type:application/json" -d "{\"ORDER_ID\":{\"0\":\"VO001\",\"1\":\"VO002\"},\"DESTINATION\":{\"0\":\"Paris, France\",\"1\":\"London, England\"},\"QTY\":{\"0\":300,\"1\":600},\"RDD\":{\"0\":\"7\/12\/2020\",\"1\":\"7\/11\/2020\"},\"PRIORITY\":{\"0\":10,\"1\":5}}"
# curl -X POST http://127.0.0.1:5000/optimize -H "Content-Type:application/json" -d "{\"ORDER_ID\":{\"0\":\"VO001\",\"1\":\"VO002\"},\"DESTINATION\":{\"0\":\"Paris, France\",\"1\":\"London, England\"},\"QTY\":{\"0\":300,\"1\":800},\"RDD\":{\"0\":\"7\/12\/2020\",\"1\":\"7\/11\/2020\"},\"PRIORITY\":{\"0\":10,\"1\":5}}"
