import requests
import json
import time

class Data:
    def __init__(self, month, gridcompany, activesupplierpergridarea):
        self.Month = month
        self.GridCompany = gridcompany
        self.ActiveSupplierPerGridArea = activesupplierpergridarea
    
    def to_dict(self):
        return {
            "Month": self.Month,
            "GridCompany": self.GridCompany,
            "ActiveSupplierPerGridArea": self.ActiveSupplierPerGridArea
        }

class API:
    @staticmethod
    def extract():
        url: str = "https://api.energidataservice.dk/dataset/ElectricitySuppliersPerGridarea?offset=0&sort=Month%20DESC"
        response = requests.get(url)
        return response.json()["records"]
    
    @staticmethod
    def transform(data):
        transfomed_data: list[Data] = [Data((d["Month"]), d["GridCompany"], d["ActiveSupplierPerGridArea"]) for d in data]
        return transfomed_data
    
    @staticmethod
    def load(data: list[Data]):
        with open("data.json", "w") as f:
            json.dump([d.to_dict() for d in data], f, indent=4)

def main():
    while True:
        data: list[Data] = API.extract()
        transformed_data = API.transform(data)
        API.load(transformed_data)
        time.sleep(60*60*24*30) # 1 month


if __name__ == "__main__":
    main()