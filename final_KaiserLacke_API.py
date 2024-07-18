#API code for fetching data

from pymongo import MongoClient
from flask import Flask, request, jsonify
app = Flask(__name__)
#server = app.server

# Create a connection to the MongoDB server
client = MongoClient('mongodb+srv://fraab:yqbUXmAN7YuzRl8F@heitecinnovations.h9cdy.mongodb.net/?retryWrites=true&w=majority&appName=HeitecInnovations')

import datetime

def unix_to_datetime(unix_timestamp_ms):
    unix_timestamp_s = unix_timestamp_ms / 1000  # Convert milliseconds to seconds
    return datetime.datetime.fromtimestamp(unix_timestamp_s)

def calculate_time_diff(unix_timestamp1_ms, unix_timestamp2_ms):
    dt1 = unix_to_datetime(unix_timestamp1_ms)
    dt2 = unix_to_datetime(unix_timestamp2_ms)
    time_diff = dt2 - dt1
    hours_diff = time_diff.total_seconds() / 3600
    return hours_diff

# Select the database and collection
db1 = client.smart_energy
collection_PV_1 = db1.pv
collection_power_1 = db1.power

db2 = client.EnergyData
collection_PV_2 = db2.PV
collection_power_2 = db2.ElectricEnergy


@app.route("/get-pv")
def get_pv():

    PV_old = list(collection_PV_1.find({"sensorId": "PV-Data_1"}))
    PV_new = list(collection_PV_2.find({"sensorId": "PV-Data_1"}))
    PV = PV_old + PV_new
    filtered_PV = [{"time": doc["time"], "pv": doc["W"]/1000} for doc in PV]
    filtered_PV = sorted(filtered_PV, key=lambda x: x['time'])
    #grouping pv data
    import math
    def reduce_data(data, reduction_factor):
        # Calculate the desired number of output rows
        target_length = len(data) // reduction_factor
        # Calculate the interval based on the target length
        interval = len(data) // target_length
        reduced_data = []
        for i in range(0, len(data), interval):
            end = min(i + interval, len(data))
            group = data[i:end]
            avg_time = sum(item['time'] for item in group) / len(group)
            avg_pv = sum(item['pv'] for item in group) / len(group)
            reduced_data.append({'time': avg_time, 'pv': avg_pv})
        return reduced_data

    filtered_PV = reduce_data(filtered_PV, 6)
    pv_data = []
    # Print each document
    for document in filtered_PV:
        pv_data.append(document)

    return jsonify(pv_data), 200

@app.route("/get-purchase")
def get_purchase():

    purchase_old = list(collection_power_1.find({"sensorId": "BC:DD:C2:78:FB:3F"}))
    purchase_new = list(collection_power_2.find({"sensorId": "BC:DD:C2:78:FB:3F"}))
    purchase = purchase_old + purchase_new
    #purchase_prev_value = 0
    #filtered_purchase = [{"time": doc["time"], "purchase": (doc["kWh"] - purchase_prev_value) / calculate_time_diff(purchase[-2]['time'], purchase[-1]['time']) if "kWh" in doc else None} for doc in purchase if "W" not in doc]
    new_purchase = []
    for row in purchase:
        if "W" not in row.keys():
            new_purchase.append(row)
    purchase = new_purchase  # Update original list (optional)
    prev_values = {}
    filtered_purchase = []
    for doc in purchase:
        filtered_doc = {"time": doc["time"]}  # Start with time
        for key, value in doc.items():
            if key == "kWh":
                prev_non_zero = prev_values.get(key, None)
                # Divide by 0.5 after subtraction (use float division)
                new_value = (0 if value == 0 else (abs(value - prev_non_zero if prev_non_zero is not None else 0) * 1000) / calculate_time_diff(purchase[-2]['time'], purchase[-1]['time']))
                filtered_doc["purchase"] = new_value  # Add calculated kWh
                if value != 0:  # Update previous non-zero 'kWh' only if non-zero
                    prev_values[key] = value
        filtered_purchase.append(filtered_doc)
    
    purchase_data = []
    # Print each document
    for document in filtered_purchase:
        purchase_data.append(document)

    return jsonify(purchase_data), 200

if __name__ == "__main__":
    app.run(debug=True)
