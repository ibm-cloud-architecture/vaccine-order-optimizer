
import os.path
import pandas as pd
import numpy as np

from collections import defaultdict, namedtuple
from datetime import date, timedelta
from docplex.mp.model import Model


DO_URL = "https://api-oaas.docloud.ibmcloud.com/job_manager/rest/v1"
DO_KEY = ""
REEFER_CAP = 100
RDD_WIN_PRE = 1
RDD_WIN_AFT = 1
RDD_PENALTY_EARLY = 5
RDD_PENALTY_LATE = 10
UNMET_ORDER_PENALTY = 50

Lane = namedtuple("Lane", ['lane_id', 'from_loc', 'to_loc', 'transit_time', 'reefer_cost', 'fixed_cost'])
Order = namedtuple('Order', ['order_id', 'destination', 'qty', 'rdd', 'priority'])

class VaccineOrderOptimizer(object):
    ''' Vaccine Order Optimizer: 

    Optimize the delivery for vaccine orders, including reefer alloction
    '''
    def __init__(self, start_date=date.today(), plan_horizon=7, debug=False):
        ''' Constructor
        '''
        self.name = "Vaccine Order Optimizer"
        self.debug = debug        
        self.start_date = start_date
        self.plan_horizon = 7

        self.log_msgs = []
        self.warning_msgs = []

        if self.debug: 
            print(f"{self.name} instantiated for ({start_date} - {start_date+timedelta(plan_horizon)})")
        self.log_msgs.append(f"Planning Horizon: ({start_date} - {start_date+timedelta(plan_horizon)})")

        self.data = {}

    def load_data_excel(self, name): 
        ''' Load data from excel file

        name: is the base name of the file, suffix will be added
        '''
        fname = os.path.join('data', 'excel', f'{name}.xlsx')
        assert os.path.exists(fname)

        if self.debug: 
            print(f"Load data from excel file: {fname}")
        self.log_msgs.append(f"* Load data from excel file: {fname}")
        
        self.data['REEFER'] = pd.read_excel(fname, 'REEFER')
        self.data['INVENTORY'] = pd.read_excel(fname, 'INVENTORY')
        self.data['TRANSPORTATION'] = pd.read_excel(fname, 'TRANSPORTATION')
        self.data['ORDER'] = pd.read_excel(fname, 'ORDER')

        self.process_data()
        
    def load_data_csv(self, name): 
        ''' Load data from csv files from a folder folder
        '''
        dir = os.path.join('data', 'csv', name)
        assert os.path.exists(dir)

        if self.debug: 
            print(f"Load data from csv files from: {dir}")
        self.log_msgs.append(f"Load data from csv files from: {dir}")

    def process_data(self): 
        ''' Process & Verify Data
        '''
        self.supplier_locs = set()          # Set of supplier locs
        self.inv_qty = defaultdict(int)     # Qty available at supplier loc, keyed by supplier loc/day
        self.inv_lots = defaultdict(list)   # Lot available at supplier loc, keyed by supplier loc/day
        for _, rec in self.data['INVENTORY'].iterrows(): 
            loc = rec['LOCATION']
            if pd.isnull(rec['DATE_AVAILABLE']) or rec['DATE_AVAILABLE'].date() <= self.start_date: 
                day = 0
            else: 
                day = (rec['DATE_AVAILABLE'].date() - self.start_date).days
            qty = 0 if pd.isnull(rec['QTY']) or rec['QTY'] < 0 else rec['QTY']
            
            self.supplier_locs.add(loc)
            if qty > 0: 
                self.inv_qty[(loc, day)] += qty
                self.inv_lots[(loc, day)].append((rec['LOT_ID'], qty))

        self.inv_qty = dict(self.inv_qty)
        self.inv_lots = dict(self.inv_lots)

        self.customer_locs = set()              # Set of customer sites
        self.reefer_qty = defaultdict(int)      # Reefer qty available at loc, keyed by loc/day
        self.reefer_info = defaultdict(list)    # Reefer info available at loc, keyed by loc/day
        for _, rec in self.data['REEFER'].iterrows(): 
            loc = rec['LOCATION']
            if pd.isnull(rec['DATE_AVAILABLE']) or rec['DATE_AVAILABLE'].date() <= self.start_date: 
                day = 0
            else: 
                day = (rec['DATE_AVAILABLE'].date() - self.start_date).days
            
            if loc not in self.supplier_locs: 
                self.customer_locs.add(loc)
            self.reefer_qty[(loc, day)] += 1
            self.reefer_info[(loc, day)].append((rec['REEFER_ID'], rec['STATUS']))

        self.reefer_qty = dict(self.reefer_qty)
        self.reefer_info = dict(self.reefer_info)

        self.lanes = {}
        for _, rec in self.data['TRANSPORTATION'].iterrows(): 
            from_loc = rec['FROM_LOC']
            to_loc = rec['TO_LOC']
            if from_loc not in self.supplier_locs: 
                self.customer_locs.add(from_loc)
            if to_loc not in self.supplier_locs: 
                self.customer_locs.add(to_loc)
            self.lanes[(from_loc, to_loc)] = Lane(rec['LANE_ID'], from_loc, to_loc, int(rec['TRANSIT_TIME']), rec['REEFER_COST'], rec['FIXED_COST'])

    def optimize(self, orders): 
        ''' Optimize for a list of orders
        '''
        self.orders = {}
        for _, rec in orders.iterrows(): 
            if pd.isnull(rec['RDD']) or rec['RDD'].date() <= self.start_date: 
                rdd = 1     # Default to day 1
            else: 
                rdd = (rec['RDD'].date() - self.start_date).days

            loc = rec['DESTINATION']
            if loc in self.customer_locs: 
                self.orders[rec['ORDER_ID']] = Order(rec['ORDER_ID'], loc, int(rec['QTY']), rdd, rec['PRIORITY'])
            else: 
                print(f"Order {rec} is ignored due to unknown customer location")

        if self.debug: 
            print(f"supplier Locations: {self.supplier_locs}\n")
            print(f"Customer Locations: {self.customer_locs}\n")
            print(f"Inventory Qty : {self.inv_qty}\n")
            print(f"Inventory Lots: {self.inv_lots}\n")
            print(f"Reefer Qty    : {self.reefer_qty}\n")
            print(f"Reefer Info   : {self.reefer_info}\n")
            print(f"Lanes: {self.lanes}\n")
            print(f"Orders: {self.orders}\n")

        self.log_msgs.append("------ Data Summary ------")
        self.log_msgs.append(f" Customers: {len(self.customer_locs)}")
        self.log_msgs.append(f" Supplies : {len(self.supplier_locs)}")
        self.log_msgs.append(f" Orders   : {len(self.orders)}")
        
        self.construct_network()
        self.build_model()

    def construct_network(self): 
        ''' Construct time-space netwrok
        '''
        # Nodes corresponding to suppliers (supplier/day)
        self.N_S = {(s,d) for s in self.supplier_locs for d in range(self.plan_horizon+1)}
        # Nodes corresponding to customers (customer/day)
        self.N_C = {(c,d) for c in self.customer_locs for d in range(self.plan_horizon+1)}
        # Nodes corresponding to orders
        self.N_O = {(o.order_id, o.rdd) for o in self.orders.values()}
        # Set of nodes in the time-space network
        self.N = self.N_S | self.N_C | self.N_O

        # Set of arcs from supplier to itself in the next period
        self.A_S = {((s,d), (s,d+1)) for (s,d) in self.N_S if d+1<=self.plan_horizon}
        # Set of arcs from customer to itself in the next period
        self.A_C = {((c,d), (c,d+1)) for (c,d) in self.N_C if d+1<=self.plan_horizon}
        # Set of arcs from suppliers to customers
        self.A_SC = {((s,d), (c,d+self.lanes[(s,c)].transit_time)) for (s,d) in self.N_S for c in self.customer_locs 
                        if (s,c) in self.lanes and d+self.lanes[(s,c)].transit_time<=self.plan_horizon}
        # Set of arcs between suppliers
        self.A_SS = {((s1,d), (s2,d+self.lanes[(s1,s2)].transit_time)) for (s1,d) in self.N_S for s2 in self.supplier_locs 
                        if (s1,s2) in self.lanes and d+self.lanes[(s1,s2)].transit_time<=self.plan_horizon and s1!=s2}
        # Set of arcs from customers to orders
        self.A_CO = {((c,d1), (o,d2)) for (c,d1) in self.N_C for (o,d2) in self.N_O 
                        if c==self.orders[o].destination and d1>=d2-RDD_WIN_PRE and d1<=d2+RDD_WIN_AFT}
        # Set of arcs from customers to suppliers (for reefer relocation)
        self.A_CS = {((c,d), (s,d+self.lanes[(c,s)].transit_time)) for (c,d) in self.N_C for s in self.supplier_locs
                        if (c,s) in self.lanes and d+self.lanes[(c,s)].transit_time<=self.plan_horizon}
        # Set of arcs in the time-space network
        self.A = self.A_S | self.A_C | self.A_SC | self.A_SS | self.A_CO | self.A_CS
        # Set of arcs where reefers flow
        self.A_R = self.A_S | self.A_C | self.A_SC | self.A_SS | self.A_CS
        # Set of arcs where vaccines flow
        self.A_V = self.A_S | self.A_SC | self.A_CO

        # Vaccine order demand at order node o
        self.D_o = {(o.order_id, o.rdd): o.qty for o in self.orders.values()}
        # Priority value for order node o
        self.P_o = {(o.order_id, o.rdd): o.priority for o in self.orders.values()}
        # Vaccines becoming available (current inventory + new production) at supplier node s
        self.V_s = {(s,d): self.inv_qty[(s,d)] for (s,d) in self.inv_qty}
        # Reefers becoming available at node n
        self.R_n = {(n,d): self.reefer_qty[(n,d)] for (n,d) in self.reefer_qty}
        # Logistics cost per reefer on arc a
        self.L_RF_a = {a: self.lanes[(a[0][0], a[1][0])].reefer_cost for a in self.A_R}
        # Logistics fixed cost of shipping on arc a
        self.L_FX_a = {a: self.lanes[(a[0][0], a[1][0])].fixed_cost for a in self.A_R}
        # Order fulfillment penalty/cost on arc 
        self.F_V_a = {a: self.get_fulfill_penalty(a) for a in self.A_CO}

        # # of vaccine carried by a reefer 
        self.U = REEFER_CAP
        # Penalty value for unmet demand
        self.V = UNMET_ORDER_PENALTY
        # Big-M, equal # of reefer in the system
        self.M = self.data['REEFER'].shape[0]

        if self.debug: 
            print(f"N_S: {self.N_S}\n")
            print(f"N_C: {self.N_C}\n")
            print(f"N_O: {self.N_O}\n")
            print(f"A_S: {self.A_S}\n")
            print(f"A_C: {self.A_C}\n")
            print(f"A_SC: {self.A_SC}\n")
            print(f"A_SS: {self.A_SS}\n")
            print(f"A_CO: {self.A_CO}\n")
            print(f"D_o: {self.D_o}\n")
            print(f"P_o: {self.P_o}\n")
            print(f"V_s: {self.V_s}\n")
            print(f"L_RF_a: {self.L_RF_a}\n")
            print(f"L_FX_a: {self.L_FX_a}\n")
            print(f"F_V_a: {self.F_V_a}\n")

        self.log_msgs.append("---- Network Summary -----")
        self.log_msgs.append(f" Nodes: {len(self.N)}")
        self.log_msgs.append(f" Arcs : {len(self.A)}")

    def get_fulfill_penalty(self, a): 
        ''' Get Order fulfillment penalty/cost 
        '''
        diff = a[0][1] - a[1][1]
        return 0 if diff==0 else RDD_PENALTY_EARLY*abs(diff) if diff<0 else RDD_PENALTY_LATE*diff

    def get_name(self, n): 
        return n.split(',')[0]

    def build_model(self):
        ''' Build optimization model
        '''
        self.model = Model(name=self.name)

        # (Linear) # of vaccine flowing through arc a
        self.var_x = self.model.continuous_var_dict(self.A_V, lb=0, name=lambda a: "x %s %s %s %s"%(self.get_name(a[0][0]), a[0][1], self.get_name(a[1][0]), a[1][1]))
        # (Integer) # of vaccine reefers flowing through arc a
        self.var_y = self.model.integer_var_dict(self.A_R, lb=0, name=lambda a: "y %s %s %s %s"%(self.get_name(a[0][0]), a[0][1], self.get_name(a[1][0]), a[1][1]))
        # (Binary) Where there is a shipment on arc a
        self.var_z = self.model.binary_var_dict(self.A_R, lb=0, name=lambda a: "z %s %s %s %s"%(self.get_name(a[0][0]), a[0][1], self.get_name(a[1][0]), a[1][1]))
        # (Linear) Unmet demand at order node o
        self.var_w = self.model.continuous_var_dict(self.N_O, lb=0, name=lambda o: "w %s %s"%(o[0], o[1]))

        # 1.	Capacity constraints on supplier to customer arcs
        self.model.add_constraints([self.var_x[a] <= self.U * self.var_y[a] for a in self.A_SC],
                                   ["CON_CAP %s %s %s %s"%(self.get_name(a[0][0]), a[0][1], self.get_name(a[1][0]), a[1][1]) for a in self.A_SC])

        # 2.	Balance constraint at supplier for vaccine reefers
        self.model.add_constraints([self.model.sum(self.var_y[(m,s)] for m in self.N if (m,s) in self.A_R) + (self.R_n[s] if s in self.R_n else 0)
                                    == self.model.sum(self.var_y[(s,n)] for n in self.N if (s,n) in self.A_R)
                                        for s in self.N_S if s[1] != self.plan_horizon],
                                   ["CON_R_BAL_SUPPLIER %s %s"%(self.get_name(s[0]), s[1]) for s in self.N_S if s[1] != self.plan_horizon])

        # 3.	Balance constraint at supplier for vaccine reefers
        self.model.add_constraints([self.model.sum(self.var_y[(m,c)] for m in self.N if (m,c) in self.A_R) + (self.R_n[c] if c in self.R_n else 0)
                                    == self.model.sum(self.var_y[(c,n)] for n in self.N if (c,n) in self.A_R)
                                        for c in self.N_C if c[1] != self.plan_horizon],
                                   ["CON_R_BAL_CUSTOMER %s %s"%(self.get_name(c[0]), c[1]) for c in self.N_C if c[1] != self.plan_horizon])

        # 4.	Balance constraint at supplier for vaccine
        self.model.add_constraints([(self.V_s[s] if s in self.V_s else 0) + (self.var_x[((s[0], s[1]-1), s)] if s[1] >= 1 else 0)
                                    == self.model.sum(self.var_x[(s,n)] for n in self.N if (s,n) in self.A_V)
                                        for s in self.N_S if s[1] != self.plan_horizon],
                                   ["CON_V_BAL_SUPPLIER %s %s"%(self.get_name(s[0]), s[1]) for s in self.N_S if s[1] != self.plan_horizon])

         # 5.	Balance constraint at customer for vaccine
        self.model.add_constraints([self.model.sum(self.var_x[(m,c)] for m in self.N if (m,c) in self.A_V)
                                    == self.model.sum(self.var_x[(c,o)] for o in self.N_O if (c,o) in self.A_CO)
                                        for c in self.N_C],
                                   ["CON_V_BAL_CUSTOMER %s %s"%(self.get_name(c[0]), c[1]) for c in self.N_C if c[1] != self.plan_horizon])

        # 6.	Balance constraint at order node for vaccine
        self.model.add_constraints([self.model.sum(self.var_x[(c,o)] for c in self.N_C if (c,o) in self.A_CO)
                                    == self.D_o[o] - self.var_w[o]
                                        for o in self.N_O],
                                   ["CON_V_BAL_ORDER %s %s"%(self.get_name(o[0]), o[1]) for o in self.N_O])    

        # 7.	Shipping notification on lane
        self.model.add_constraints([self.var_y[a] <= self.M * self.var_z[a] for a in self.A_R],
                                   ["CON_SHIPPING_LANE_LB %s %s %s %s"%(self.get_name(a[0][0]), a[0][1], self.get_name(a[1][0]), a[1][1]) for a in self.A_R]) 

        # 8.	Shipping notification on lane (Upper Bound)
        self.model.add_constraints([self.var_z[a] <= self.var_y[a]  for a in self.A_R],
                                   ["CON_SHIPPING_LANE_UB %s %s %s %s"%(self.get_name(a[0][0]), a[0][1], self.get_name(a[1][0]), a[1][1]) for a in self.A_R])            


        self.obj_logistics_fix = self.model.sum(self.L_FX_a[a]*self.var_z[a] for a in self.A_R)
        self.obj_logistics_var = self.model.sum(self.L_RF_a[a]*self.var_y[a] for a in self.A_R)
        self.obj_fulfillment = self.model.sum(self.model.sum(self.P_o[o]*self.F_V_a[(c,o)]*self.var_x[(c,o)] for c in self.N_C if (c,o) in self.A_CO) for o in self.N_O)
        self.obj_unmet_order = self.model.sum(self.P_o[o]*self.V*self.var_w[o] for o in self.N_O)

        self.model.minimize(self.obj_logistics_fix + self.obj_logistics_var + self.obj_fulfillment + self.obj_unmet_order)
        self.solve()
        self.process_plans()

    def solve(self): 
        ''' Solve the model
        '''
        if DO_KEY == '': 
            ms = self.model.solve(log_output=self.debug, agent='local')
        else: 
            ms = self.model.solve(log_output=self.debug, url=DO_URL, key=DO_KEY) 

        if not ms:
            details = self.model.get_solve_details()
            print("Optimization failed: %s"%details.status)
            return 1

        details = self.model.get_solve_details()
        self.log_msgs.append("-- Optimization Summary --")
        self.log_msgs.append(" Variables  : %d"%self.model.number_of_variables)
        self.log_msgs.append(" Constrains : %d"%self.model.number_of_constraints)
        self.log_msgs.append(" Objective  : %.2f"%self.model.objective_value)
        self.log_msgs.append(" Best Bound : %.2f"%details.best_bound)
        self.log_msgs.append(" MIP Rel Gap: %.2f"%details.mip_relative_gap)
        self.log_msgs.append(" Solve Time : %.2f"%details.time)
        self.log_msgs.append(" Status     : %s"%details.status)

        self.sol_x = {a:int(round(self.var_x[a].solution_value, 0)) for a in self.A_V if round(self.var_x[a].solution_value, 0) > 0}
        self.sol_y = {a:int(round(self.var_y[a].solution_value, 0)) for a in self.A_R if round(self.var_y[a].solution_value, 0) > 0}
        self.sol_z = {a:int(round(self.var_z[a].solution_value, 0)) for a in self.A_R if round(self.var_z[a].solution_value, 0) > 0}
        self.sol_w = {o:int(round(self.var_w[o].solution_value, 0)) for o in self.N_O if round(self.var_w[o].solution_value, 0) > 0}

        if self.debug: 
            print(f"sol_x: {self.sol_x}\n")
            print(f"sol_y: {self.sol_y}\n")
            print(f"sol_z: {self.sol_z}\n")
            print(f"sol_w: {self.sol_w}\n")
            self.model.export_to_stream("model_doaf_voo.lp")

    def process_plans(self): 
        ''' Get plans
        '''
        arrivals = defaultdict(list)
        deliveries = defaultdict(list)
        for a in self.sol_x: 
            if a[1] in self.N_C: 
                arrivals[a[1]].append([a[0][0], a[0][1], self.sol_x[a], self.get_cost(a, self.sol_y[a])])
            if a[1] in self.N_O: 
                deliveries[a[1]].append([a[0][0], a[0][1], self.sol_x[a]])

        rec_orders = []
        for o in deliveries: 
            order_id = o[0]
            dest = self.orders[order_id].destination
            order_qty = self.orders[order_id].qty
            priority = self.orders[order_id].priority
            rdd = self.get_date(self.orders[order_id].rdd)
            min_dt = self.get_date(min(v[1] for v in deliveries[o]))
            max_dt = self.get_date(max(v[1] for v in deliveries[o]))
            del_cnt = len(deliveries[o])
            del_qty = sum(v[2] for v in deliveries[o])
            rem = self.sol_w[o] if o in self.sol_w else 0
            rec_orders.append([order_id, dest, order_qty, priority, rdd, min_dt, max_dt, del_cnt, del_qty, rem])

        self.plan_orders = pd.DataFrame(rec_orders, columns=['Order ID', 'Destination', 'Order Qty', 'Order Priority', 'Order RDD', 
                                                             'First Delivery', 'Last Delivery', 'Number of Deliveries', 'Delivered Qty', 'Remaining Qty'])
        self.plan_orders.sort_values(['Order ID'], inplace=True)

        rec_order_details = []
        for o in deliveries: 
            order_id = o[0]                 # Order ID
            del_dt = self.get_date(o[1])    # Delivery date
            for delivery in deliveries[o]: 
                c = (delivery[0], delivery[1])
                qty = delivery[2]
                for i in range(len(arrivals[c])):
                    s_qty =  min(arrivals[c][i][2], qty)
                    if s_qty > 0: 
                        supplier = arrivals[c][i][0]                # Supplier
                        dep_dt = self.get_date(arrivals[c][i][1])   # Departure Date
                        reefer = s_qty 
                        s_cost = (s_qty/arrivals[c][i][2])*arrivals[c][i][3]
                        qty -= s_qty
                        arrivals[c][i][2] -= s_qty
                        arrivals[c][i][3] -= s_cost
                        rec_order_details.append([order_id, supplier, dep_dt, del_dt, s_qty, s_cost])
            
        self.plan_order_details = pd.DataFrame(rec_order_details, columns=['Order ID', 'Supplier', 'Departure Date', 'Delivery Date', 'Qty', 'Cost'])
        self.plan_order_details.sort_values(['Order ID', 'Supplier', 'Departure Date'], inplace=True)

        rec_shipments = []
        for a in self.sol_y: 
            if a[0][0] != a[1][0] and a not in self.sol_x: 
                rec_shipments.append(['Reposition', a[0][0], self.get_date(a[0][1]), a[1][0], self.get_date(a[1][1]), 0, self.sol_y[a], self.get_cost(a, self.sol_y[a])])
        for a in self.sol_x: 
            if a[0] in self.N_S and a[1] in self.N_C: 
                rec_shipments.append(['Delivery', a[0][0], self.get_date(a[0][1]), a[1][0], self.get_date(a[1][1]), self.sol_x[a], self.sol_y[a], self.get_cost(a, self.sol_y[a])])

        self.plan_shipments = pd.DataFrame(rec_shipments, columns=['Type', 'From', 'Departure Date', 'To', 'Arrival Date', 'Qty', 'Reefers', 'Cost'])
        self.plan_shipments.sort_values(['Type', 'From', 'Departure Date', 'To'], inplace=True)

        if self.debug: 
            print(self.plan_orders)
            print(self.plan_order_details)
            print(self.plan_shipments)

    def get_cost(self, a, reefers): 
        ''' Assess the transportation cost for the given number of reefers on an arc
        '''
        if reefers <= 0 or (a[0][0], a[1][0]) not in self.lanes: 
            return 0

        fx_rate = self.lanes[(a[0][0], a[1][0])].fixed_cost
        rf_rate = self.lanes[(a[0][0], a[1][0])].reefer_cost
        return fx_rate + rf_rate*reefers

    def get_date(self, d): 
        return self.start_date + timedelta(d)

    def get_arc_info(self, a): 
        ''' Get list of information for a
        '''
        fx_cost = self.L_FX_a[a] if a in self.L_FX_a else 0
        rf_cost = self.L_RF_a[a] if a in self.L_RF_a else 0
        order_qty = self.D_o[a[1]] if a[1] in self.D_o else 0
        vaccine = self.V_s[a[0]] if a[0] in self.V_s else 0
        reefer = self.R_n[a[0]] if a[0] in self.R_n else 0

        return [fx_cost, rf_cost, order_qty, vaccine, reefer]

    def write_solution(self, fname): 
        ''' Write solution to an Excel file
        '''
        sol = {}
        for a, v in self.sol_x.items(): 
            if a not in sol: 
                sol[a] = [v, 0, 0]
            else:
                sol[a][0] = v
        for a, v in self.sol_y.items(): 
            if a not in sol: 
                sol[a] = [0, v, 0]
            else:
                sol[a][1] = v
        for a, v in self.sol_z.items(): 
            if a not in sol: 
                sol[a] = [0, 0, v]
            else:
                sol[a][2] = v

        sol_rec = [[a[0][0], self.get_date(a[0][1]), a[1][0], self.get_date(a[1][1]), v[0], v[1], v[2], "Y" if a[0][0]==a[1][0] else "N"] + self.get_arc_info(a) for a,v in sol.items()]
        sol_df = pd.DataFrame(sol_rec, columns=['FROM_LOC', 'FROM_DATE', 'TO_LOC', 'TO_DATE', 'X', 'Y', 'Z', 
                                                'INTRA', 'ARC_FIX_COST', 'ARC_REEFER_COST', 'ORDER_QTY', 'VACCINE SUPPLY', 'REEFER SUPPLY'])
        sol_df.sort_values(['FROM_DATE', 'FROM_LOC', 'TO_DATE', 'TO_LOC'], inplace=True)
        
        with pd.ExcelWriter(os.path.join('data', 'excel', 'sol', fname)) as writer: 
            self.plan_orders.to_excel(writer,'Orders', index=False)
            self.plan_order_details.to_excel(writer,'Order Details', index=False)
            self.plan_shipments.to_excel(writer,'Shipments', index=False)
            sol_df.to_excel(writer,'SOL_COMBO', index=False)
            writer.save()

if __name__ == '__main__':
    optimizer = VaccineOrderOptimizer(start_date=date(2020, 7, 6), debug=False)

    # optimizer.load_data_csv("TC001")
    optimizer.load_data_excel("TC001")
    
    orders = pd.read_excel(os.path.join('data', 'excel', f'TC001.xlsx'), 'ORDER')
    optimizer.optimize(orders)

    optimizer.write_solution("TC001_Sol.xlsx")
    print("\n  ".join(optimizer.log_msgs))
