
import os.path
import pandas as pd
import numpy as np

from collections import defaultdict, namedtuple
from datetime import date, timedelta
from docplex.mp.model import Model

REEFER_CAP = 100
RDD_WIN_PRE = 1
RDD_WIN_AFT = 1
RDD_PENALTY_EARLY = 5
RDD_PENALTY_LATE = 10
UNMET_ORDER_PENALTY = 50

Lane = namedtuple("Lane", ['lane_id', 'from_loc', 'to_loc', 'transit_time', 'reefer_cost', 'fixed_cost'])
Order = namedtuple('Order', ['orderID', 'deliveryLocation', 'qty', 'rdd', 'priority'])

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

    # def load_data_excel(self, name): 
    #     ''' Load data from excel file

    #     name: is the base name of the file, suffix will be added
    #     '''
    #     fname = os.path.join('data', 'excel', f'{name}.xlsx')
    #     assert os.path.exists(fname)

    #     if self.debug: 
    #         print(f"Load data from excel file: {fname}")
    #     self.log_msgs.append(f"* Load data from excel file: {fname}")
        
    #     self.data['REEFER'] = pd.read_excel(fname, 'REEFER')
    #     self.data['INVENTORY'] = pd.read_excel(fname, 'INVENTORY')
    #     self.data['TRANSPORTATION'] = pd.read_excel(fname, 'TRANSPORTATION')

    #     self.process_data()
        
    # def load_data_csv(self, name): 
    #     ''' Load data from csv files from a folder folder
    #     '''
    #     dir = os.path.join('/project/userapp/data/csv', name)
    #     # dir = os.path.join('data', 'csv', name)
    #     assert os.path.exists(dir)

    #     if self.debug: 
    #         print(f"Load data from csv files from: {dir}")
    #     self.log_msgs.append(f"Load data from csv files from: {dir}")

    #     self.data['REEFER'] = pd.read_csv(os.path.join(dir, "REEFER.csv"))
    #     self.data['REEFER']['DATE_AVAILABLE'] = pd.to_datetime(self.data['REEFER']['DATE_AVAILABLE'], format='%m/%d/%Y')

    #     self.data['INVENTORY'] = pd.read_csv(os.path.join(dir, "INVENTORY.csv"))
    #     self.data['INVENTORY']['DATE_AVAILABLE'] = pd.to_datetime(self.data['INVENTORY']['DATE_AVAILABLE'], format='%m/%d/%Y')

    #     self.data['TRANSPORTATION'] = pd.read_csv(os.path.join(dir, "TRANSPORTATION.csv"))

    #     self.process_data()
    
    def prepare_data(self, orders, reefer, inventory, transportation): 
        if self.debug: print('[VaccineOrderOptimizer] - [BEGIN] - prepare_data')
        ''' Prepare the data
        '''
        ###############
        # Question: Do we need to replace empty strings in every column?
        ###############

        # print('---------- Printing for debugging ----------------')
        # pd.options.display.max_columns = None
        # print(reefer.transpose())

        self.data['REEFER'] = reefer.transpose()
        self.data['REEFER']['date_available'] = self.data['REEFER']['date_available'].replace(r'^\s*$', np.nan, regex=True)
        self.data['REEFER']['date_available'] = pd.to_datetime(self.data['REEFER']['date_available'], format='%m/%d/%Y')

        # print('---------- Printing for debugging ----------------')
        # pd.options.display.max_columns = None
        # print(self.data['REEFER'])

        self.data['INVENTORY'] = inventory.transpose()
        self.data['INVENTORY']['date_available'] = self.data['INVENTORY']['date_available'].replace(r'^\s*$', np.nan, regex=True)
        self.data['INVENTORY']['date_available'] = pd.to_datetime(self.data['INVENTORY']['date_available'], format='%m/%d/%Y')

        self.data['TRANSPORTATION'] = transportation.transpose()

        self.data['ORDERS'] = orders.transpose()
        self.data['ORDERS']['deliveryDate'] = self.data['ORDERS']['deliveryDate'].replace(r'^\s*$', np.nan, regex=True)
        self.data['ORDERS']['deliveryDate'] = pd.to_datetime(self.data['ORDERS']['deliveryDate'], format='%Y-%m-%d')

        self.process_data()
        if self.debug: print('[VaccineOrderOptimizer] - [END] - prepare_data')

    def process_data(self): 
        if self.debug: print('[VaccineOrderOptimizer] - [BEGIN] - process_data')
        ''' Process & Verify Data
        '''
        self.supplier_locs = set()          # Set of supplier locs
        self.inv_qty = defaultdict(int)     # Qty available at supplier loc, keyed by supplier loc/day
        self.inv_lots = defaultdict(list)   # Lot available at supplier loc, keyed by supplier loc/day
        for _, rec in self.data['INVENTORY'].iterrows(): 
            loc = rec['location']
            if pd.isnull(rec['date_available']) or rec['date_available'].date() <= self.start_date: 
                day = 0
            else: 
                day = (rec['date_available'].date() - self.start_date).days
            qty = 0 if pd.isnull(rec['qty']) or rec['qty'] < 0 else rec['qty']
            
            self.supplier_locs.add(loc)
            if qty > 0: 
                self.inv_qty[(loc, day)] += qty
                self.inv_lots[(loc, day)].append((rec['lot_id'], qty))

        self.inv_qty = dict(self.inv_qty)
        self.inv_lots = dict(self.inv_lots)

        self.customer_locs = set()              # Set of customer sites
        self.reefer_qty = defaultdict(int)      # Reefer qty available at loc, keyed by loc/day
        self.reefer_info = defaultdict(list)    # Reefer info available at loc, keyed by loc/day
        for _, rec in self.data['REEFER'].iterrows(): 
            loc = rec['location']
            if pd.isnull(rec['date_available']) or rec['date_available'].date() <= self.start_date: 
                day = 0
            else: 
                day = (rec['date_available'].date() - self.start_date).days
            
            if loc not in self.supplier_locs: 
                self.customer_locs.add(loc)
            self.reefer_qty[(loc, day)] += 1
            self.reefer_info[(loc, day)].append((rec['reefer_id'], rec['status']))

        self.reefer_qty = dict(self.reefer_qty)
        self.reefer_info = dict(self.reefer_info)

        self.lanes = {}
        for _, rec in self.data['TRANSPORTATION'].iterrows(): 
            from_loc = rec['from_loc']
            to_loc = rec['to_loc']
            if from_loc not in self.supplier_locs: 
                self.customer_locs.add(from_loc)
            if to_loc not in self.supplier_locs: 
                self.customer_locs.add(to_loc)
            self.lanes[(from_loc, to_loc)] = Lane(rec['lane_id'], from_loc, to_loc, int(rec['transit_time']), rec['reefer_cost'], rec['fixed_cost'])
        if self.debug: print('[VaccineOrderOptimizer] - [END] - process_data')

    def optimize(self): 
        if self.debug: print('[VaccineOrderOptimizer] - [BEGIN] - optimize')
        ''' Optimize for a list of orders
        '''
        self.orders = {}
        for _, rec in self.data['ORDERS'].iterrows(): 
            if pd.isnull(rec['deliveryDate']) or rec['deliveryDate'].date() <= self.start_date: 
                rdd = 1     # Default to day 1
            else: 
                rdd = (rec['deliveryDate'].date() - self.start_date).days

            loc = rec['deliveryLocation']
            if loc in self.customer_locs: 
                self.orders[rec['orderID']] = Order(rec['orderID'], loc, int(rec['quantity']), rdd, rec['priority'])
            else: 
                print(f"Order\n{rec} \nis ignored due to unknown customer location")

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
        if self.debug: print('[VaccineOrderOptimizer] - [END] - optimize')

    def construct_network(self): 
        if self.debug: print('[VaccineOrderOptimizer] - [BEGIN] - construct_network')
        ''' Construct time-space netwrok
        '''
        # Nodes corresponding to suppliers (supplier/day)
        self.N_S = {(s,d) for s in self.supplier_locs for d in range(self.plan_horizon+1)}
        # Nodes corresponding to customers (customer/day)
        self.N_C = {(c,d) for c in self.customer_locs for d in range(self.plan_horizon+1)}
        # Nodes corresponding to orders
        self.N_O = {(o.orderID, o.rdd) for o in self.orders.values()}
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
                        if c==self.orders[o].deliveryLocation and d1>=d2-RDD_WIN_PRE and d1<=d2+RDD_WIN_AFT}
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
        self.D_o = {(o.orderID, o.rdd): o.qty for o in self.orders.values()}
        # Priority value for order node o
        self.P_o = {(o.orderID, o.rdd): o.priority for o in self.orders.values()}
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

        if self.debug: print('[VaccineOrderOptimizer] - [END] - construct_network')

    def get_fulfill_penalty(self, a): 
        ''' Get Order fulfillment penalty/cost 
        '''
        diff = a[0][1] - a[1][1]
        return 0 if diff==0 else RDD_PENALTY_EARLY*abs(diff) if diff<0 else RDD_PENALTY_LATE*diff

    def get_name(self, n):
        if  type(n) == str:
            return n.split(',')[0]
        return str(n)

    def build_model(self):
        if self.debug: print('[VaccineOrderOptimizer] - [BEGIN] - build_model')
        ''' Build optimization model
        '''
        self.model = Model(name=self.name)

        print('111111')
        # (Linear) # of vaccine flowing through arc a
        self.var_x = self.model.continuous_var_dict(self.A_V, lb=0, name=lambda a: "x %s %s %s %s"%(self.get_name(a[0][0]), a[0][1], self.get_name(a[1][0]), a[1][1]))
        print('22222222')
        # (Integer) # of vaccine reefers flowing through arc a
        self.var_y = self.model.integer_var_dict(self.A_R, lb=0, name=lambda a: "y %s %s %s %s"%(self.get_name(a[0][0]), a[0][1], self.get_name(a[1][0]), a[1][1]))
        print('333333333')
        # (Binary) Where there is a shipment on arc a
        self.var_z = self.model.binary_var_dict(self.A_R, lb=0, name=lambda a: "z %s %s %s %s"%(self.get_name(a[0][0]), a[0][1], self.get_name(a[1][0]), a[1][1]))
        print('444444444')
        # (Linear) Unmet demand at order node o
        self.var_w = self.model.continuous_var_dict(self.N_O, lb=0, name=lambda o: "w %s %s"%(o[0], o[1]))
        print('5555555')

        # 1.	Capacity constraints on supplier to customer arcs
        self.model.add_constraints([self.var_x[a] <= self.U * self.var_y[a] for a in self.A_SC],
                                   ["CON_CAP %s %s %s %s"%(self.get_name(a[0][0]), a[0][1], self.get_name(a[1][0]), a[1][1]) for a in self.A_SC])

        print('66666666')
        # 2.	Balance constraint at supplier for vaccine reefers
        self.model.add_constraints([self.model.sum(self.var_y[(m,s)] for m in self.N if (m,s) in self.A_R) + (self.R_n[s] if s in self.R_n else 0)
                                    == self.model.sum(self.var_y[(s,n)] for n in self.N if (s,n) in self.A_R)
                                        for s in self.N_S if s[1] != self.plan_horizon],
                                   ["CON_R_BAL_SUPPLIER %s %s"%(self.get_name(s[0]), s[1]) for s in self.N_S if s[1] != self.plan_horizon])

        print('777777777')
        # 3.	Balance constraint at supplier for vaccine reefers
        self.model.add_constraints([self.model.sum(self.var_y[(m,c)] for m in self.N if (m,c) in self.A_R) + (self.R_n[c] if c in self.R_n else 0)
                                    == self.model.sum(self.var_y[(c,n)] for n in self.N if (c,n) in self.A_R)
                                        for c in self.N_C if c[1] != self.plan_horizon],
                                   ["CON_R_BAL_CUSTOMER %s %s"%(self.get_name(c[0]), c[1]) for c in self.N_C if c[1] != self.plan_horizon])

        print('888888888')
        # 4.	Balance constraint at supplier for vaccine
        self.model.add_constraints([(self.V_s[s] if s in self.V_s else 0) + (self.var_x[((s[0], s[1]-1), s)] if s[1] >= 1 else 0)
                                    == self.model.sum(self.var_x[(s,n)] for n in self.N if (s,n) in self.A_V)
                                        for s in self.N_S if s[1] != self.plan_horizon],
                                   ["CON_V_BAL_SUPPLIER %s %s"%(self.get_name(s[0]), s[1]) for s in self.N_S if s[1] != self.plan_horizon])

        print('99999999999')
        # 5.	Balance constraint at customer for vaccine
        self.model.add_constraints([self.model.sum(self.var_x[(m,c)] for m in self.N if (m,c) in self.A_V)
                                    == self.model.sum(self.var_x[(c,o)] for o in self.N_O if (c,o) in self.A_CO)
                                        for c in self.N_C],
                                   ["CON_V_BAL_CUSTOMER %s %s"%(self.get_name(c[0]), c[1]) for c in self.N_C if c[1] != self.plan_horizon])

        print('AAAAAAAAAAAAA')
        # 6.	Balance constraint at order node for vaccine
        self.model.add_constraints([self.model.sum(self.var_x[(c,o)] for c in self.N_C if (c,o) in self.A_CO)
                                    == self.D_o[o] - self.var_w[o]
                                        for o in self.N_O],
                                   ["CON_V_BAL_ORDER %s %s"%(self.get_name(o[0]), o[1]) for o in self.N_O])    

        print('BBBBBBBBBBBBB')
        # 7.	Shipping notification on lane
        self.model.add_constraints([self.var_y[a] <= self.M * self.var_z[a] for a in self.A_R],
                                   ["CON_SHIPPING_LANE_LB %s %s %s %s"%(self.get_name(a[0][0]), a[0][1], self.get_name(a[1][0]), a[1][1]) for a in self.A_R]) 

        print('CCCCCCCCCCCCCC')
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
        if self.debug: print('[VaccineOrderOptimizer] - [END] - build_model')

    def solve(self): 
        if self.debug: print('[VaccineOrderOptimizer] - [BEGIN] - solve')
        ''' Solve the model
        '''
        print("Solve optimization with DO local")
        ms = self.model.solve(log_output=self.debug, agent='local')

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
        if self.debug: print('[VaccineOrderOptimizer] - [END] - solve')

    def process_plans(self): 
        if self.debug: print('[VaccineOrderOptimizer] - [BEGIN] - process_plans')
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
            orderID = o[0]
            dest = self.orders[orderID].deliveryLocation
            order_qty = self.orders[orderID].qty
            priority = self.orders[orderID].priority
            rdd = self.get_date(self.orders[orderID].rdd)
            min_dt = self.get_date(min(v[1] for v in deliveries[o]))
            max_dt = self.get_date(max(v[1] for v in deliveries[o]))
            del_cnt = len(deliveries[o])
            del_qty = sum(v[2] for v in deliveries[o])
            rem = self.sol_w[o] if o in self.sol_w else 0
            rec_orders.append([orderID, dest, order_qty, priority, rdd, min_dt, max_dt, del_cnt, del_qty, rem])

        self.plan_orders = pd.DataFrame(rec_orders, columns=['Order ID', 'DeliveryLocation', 'Order Qty', 'Order Priority', 'Order RDD', 
                                                             'First Delivery', 'Last Delivery', 'Number of Deliveries', 'Delivered Qty', 'Remaining Qty'])
        self.plan_orders.sort_values(['Order ID'], inplace=True)

        rec_order_details = []
        for o in deliveries: 
            orderID = o[0]                 # Order ID
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
                        rec_order_details.append([orderID, supplier, dep_dt, del_dt, s_qty, s_cost])
            
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

        sol_combo = {}
        for a, v in self.sol_x.items(): 
            sol_combo[a] = [v, 0, 0]
        for a, v in self.sol_y.items(): 
            if a not in sol_combo: 
                sol_combo[a] = [0, v, 0]
            else:
                sol_combo[a][1] = v
        for a, v in self.sol_z.items(): 
            if a not in sol_combo: 
                sol_combo[a] = [0, 0, v]
            else:
                sol_combo[a][2] = v

        sol_rec = [[a[0][0], self.get_date(a[0][1]), a[1][0], self.get_date(a[1][1]), v[0], v[1], v[2], "Y" if a[0][0]==a[1][0] else "N"] + self.get_arc_info(a) for a,v in sol_combo.items()]
        self.sol_combo = pd.DataFrame(sol_rec, columns=['FROM_LOC', 'FROM_DATE', 'TO_LOC', 'TO_DATE', 'X', 'Y', 'Z', 
                                                'INTRA', 'ARC_FIX_COST', 'ARC_REEFER_COST', 'ORDER_QTY', 'VACCINE SUPPLY', 'REEFER SUPPLY'])
        self.sol_combo.sort_values(['FROM_DATE', 'FROM_LOC', 'TO_DATE', 'TO_LOC'], inplace=True)

        if self.debug: 
            print(self.plan_orders)
            print(self.plan_order_details)
            print(self.plan_shipments)
            print(self.sol_combo)
        if self.debug: print('[VaccineOrderOptimizer] - [END] - process_plans')

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
        if self.debug: print('[VaccineOrderOptimizer] - [BEGIN] - write_solution')
        ''' Write solution to an Excel file
        '''
        with pd.ExcelWriter(os.path.join('data', 'excel', 'sol', fname)) as writer: 
            self.plan_orders.to_excel(writer,'Orders', index=False)
            self.plan_order_details.to_excel(writer,'Order Details', index=False)
            self.plan_shipments.to_excel(writer,'Shipments', index=False)
            self.sol_combo.to_excel(writer,'SOL_COMBO', index=False)
            writer.save()
        if self.debug: print('[VaccineOrderOptimizer] - [END] - write_solution')

    def write_solution_csv(self, dir): 
        ''' Write solution to csv files
        '''
        self.plan_orders.to_csv(os.path.join(dir, 'Orders.csv'), index=False)
        self.plan_order_details.to_csv(os.path.join(dir, 'Order Details.csv'), index=False)
        self.plan_shipments.to_csv(os.path.join(dir, 'Shipments.csv'), index=False)
        self.sol_combo.to_csv(os.path.join(dir, 'SOL_COMBO.csv'), index=False)

    def get_sol_json(self): 
        return {"Orders":self.plan_orders.to_json(orient='records', date_format='iso'), 
                 "OrderDetails":self.plan_order_details.to_json(orient='records', date_format='iso'),
                 "Shipments":self.plan_shipments.to_json(orient='records', date_format='iso')}
    
    def get_sol_panda(self):
        return self.plan_orders, self.plan_order_details, self.plan_shipments
    
    def getLogs(self):
        return "\n  ".join(self.log_msgs)

if __name__ == '__main__':
    optimizer = VaccineOrderOptimizer(start_date=date(2020, 7, 6), debug=False)

    # optimizer.load_data_excel("TC001")
    # orders = pd.read_excel(os.path.join('data', 'excel', f'TC001.xlsx'), 'ORDER')
    # optimizer.optimize(orders)
    # optimizer.write_solution("TC001_Sol.xlsx")

    optimizer.load_data_csv("TC001")
    orders = pd.read_csv(os.path.join('data', 'csv', 'TC001', 'ORDER.csv'))
    print(orders.to_json(date_format='iso', orient='records'))
    orders['RDD'] = pd.to_datetime(orders['RDD'], format='%m/%d/%Y')
    optimizer.optimize(orders)
    # optimizer.write_solution_csv(os.path.join('data', 'csv', 'TC001', 'sol'))

    print("\n  ".join(optimizer.log_msgs))
    print(optimizer.get_sol_json())
