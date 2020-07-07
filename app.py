# Import modules
from doaf_vaccine_order_optimizer import VaccineOrderOptimizer
from flask import Flask, render_template, request
import os
import pandas as pd
from datetime import date

# Get current working directory
cwd = os.getcwd()

# Flask configuration
app = Flask(__name__)
port = int(os.getenv('PORT', 8000))


@app.route("/")
def home():
    return "Hello, Flask!"

# @app.route('/optimize', methods=["POST"])
@app.route('/optimize')
def optimize():
    # req = request.get_json()
    # print("Json: ", req)
    optimizer = VaccineOrderOptimizer(start_date=date(2020, 7, 6), debug=False)
    optimizer.load_data_csv("TC001")
    orders = pd.read_csv(os.path.join('data', 'csv', 'TC001', 'ORDER.csv'))
    orders['RDD'] = pd.to_datetime(orders['RDD'], format='%m/%d/%Y')
    optimizer.optimize(orders)

    return optimizer.get_sol_json()

# Run app
if __name__ == '__main__':
    app.run(host='0.0.0.0', port=port, debug=True)