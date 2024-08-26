import requests
import json

class Data:
    def __init__(self, month, gridcompany, activesupplierpergridarea):
        self.data 
        self.Month = month
        self.GridCompany = gridcompany
        self.ActiveSupplierPerGridArea = activesupplierpergridarea

class API:
    url: str = "https://api.energidataservice.dk/dataset/ElectricitySuppliersPerGridarea?offset=0&sort=Month%20DESC"
    @staticmethod
    def extract(self):
        response = requests.get(self.url)
        return response.json()
    
    @staticmethod
    def transform(self, data):
        transfomed_data: list[Data] = [Data(d["Month"], d["GridCompany"], d["ActiveSupplierPerGridArea"]) for d in data]
        return transfomed_data
    
    @staticmethod
    def load(self, data):
        with open("data.json", "w") as f:
            json.dump(data, f)

def main():
    data = API.extract()
    transformed_data = API.transform(data)
    API.load(transformed_data)