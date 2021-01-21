import requests, unittest, os
import pandas as pd

urlBase = os.getenv("VORO_URL","http://localhost:5000/api/v1")

class ValidateAPI(unittest.TestCase):

    def test_lot_inventory(self):
        print("----- Get inventory -----")
        url = urlBase + "/data/inventory"
        rep = requests.get(url)
        lots = rep.json()
        print(lots)
        self.assertEqual(len(lots),9)
    
    def test_lot_inventoryPandas(self):
        print("----- Get inventory as Pandas -----")
        url = urlBase + "/data/inventory/pandas"
        rep = requests.get(url)
        lots = rep.text
        print(lots)
        df = pd.DataFrame.from_records(lots)
        self.assertTrue(df.size > 850)

    def test_get_reefers(self):
        print("----- Get reefers -----")
        url = urlBase + "/data/reefers"
        rep = requests.get(url)
        records = rep.json()
        print(records)
        self.assertEqual(len(records),10)
    
    def test_get_transportations(self):
        print("----- Get transportations -----")
        url = urlBase + "/data/transportations"
        rep = requests.get(url)
        records = rep.json()
        print(records)
        self.assertEqual(len(records),14)

if __name__ == '__main__':
    unittest.main()