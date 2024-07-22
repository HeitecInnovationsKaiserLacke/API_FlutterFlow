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

def kWh_to_kw(t, key_):
    p=[]
    old =  t[0]['kWh']
    old_time = t[0]['time']
    for i in range(len(t)):
        new = t[i]['kWh']
        time_new = t[i]['time']
        temp = {'time': t[i]['time'], key_: 0.0 if t[i]['kWh'] == 0 else ((t[i]['kWh'] - old)/(calculate_time_diff(old_time, t[i]['time']) if i>0 else 1))}
        p.append(temp)
        if p[i][key_] != 0:
            old = new
            old_time = time_new
    return p

def remove_error_record(t, key_, oldkey_):
    p=[]
    old =  t[0][oldkey_]
    old_time = t[0]['time']
    for i in range(len(t)):
        new = t[i][oldkey_]
        time_new = t[i]['time']
        temp = {'time': t[i]['time'], key_: 0.0 if t[i][oldkey_] < 0 else t[i][oldkey_]}
        p.append(temp)
        old = new
    return p

def m3_to_kw(t, key_):
    brennwert_kwh_per_m3 = 11.548
    p=[]
    old =  t[0]['m3']
    old_time = t[0]['time']
    for i in range(len(t)):
        new = t[i]['m3']
        time_new = t[i]['time']
        time_diff = calculate_time_diff(old_time, t[i]['time'])
        temp = {'time': t[i]['time'],key_: 0.0 if t[i]['m3'] == 0 else ((t[i]['m3'] - old)*(brennwert_kwh_per_m3)/(time_diff if i>0 else 1))}
        p.append(temp)
        if p[i][key_] != 0:
            old = new
            old_time = time_new
    return p

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


# Import the necessary libraries
from pymongo import MongoClient

# Create a connection to the MongoDB server
client = MongoClient('mongodb+srv://fraab:yqbUXmAN7YuzRl8F@heitecinnovations.h9cdy.mongodb.net/?retryWrites=true&w=majority&appName=HeitecInnovations')

# Select the database and collection
db1 = client.smart_energy
collection_PV_1 = db1.pv
collection_power_1 = db1.power
collection_gas_1 = db1.gas

db2 = client.EnergyData
collection_PV_2 = db2.PV
collection_power_2 = db2.ElectricEnergy
collection_gas_2 = db2.Heating

#-----------------------------------PV-----------------------------------
PV_old = list(collection_PV_1.find({"sensorId": "PV-Data_1"}))
PV_new = list(collection_PV_2.find({"sensorId": "PV-Data_1"}))
PV = PV_old + PV_new
filtered_PV = [{"time": doc["time"], "pv": doc["W"]/1000} for doc in PV]
filtered_PV = sorted(filtered_PV, key=lambda x: x['time'])
filtered_PV = reduce_data(filtered_PV, 6)
filtered_PV = [{"time": unix_to_datetime(doc["time"]), "pv": doc['pv']} for doc in filtered_PV if doc["time"] >= 1702598400000]

#-----------------------------------Purchased power-----------------------------------
purchase_old = list(collection_power_1.find({"sensorId": "BC:DD:C2:78:FB:3F"}))
purchase_new = list(collection_power_2.find({"sensorId": "BC:DD:C2:78:FB:3F"}))
purchase = purchase_old + purchase_new
new_purchase = []
for row in purchase:
  if "W" not in row.keys():
    new_purchase.append(row)
purchase = new_purchase
filtered_purchase = kWh_to_kw(purchase, 'purchase')
filtered_purchase = sorted(filtered_purchase, key=lambda x: x['time'])
filtered_purchase = [{"time": unix_to_datetime(doc["time"]), "purchase": doc['purchase']*100} for doc in filtered_purchase if doc["time"] >= 1702598400000]

#-----------------------------------Feed power-----------------------------------
feed_old = list(collection_power_1.find({"sensorId": "BC:DD:C2:78:FB:3E_E"}))
feed_old = [doc for doc in feed_old if doc['time'] != 1701344669276.0]
feed_new = list(collection_power_2.find({"sensorId": "BC:DD:C2:78:FB:3E_E"}))
filtered_feed = sorted((kWh_to_kw((feed_old + feed_new), 'feed')), key=lambda x: x['time'])
filtered_feed = [{"time": (doc["time"]), "feed": doc['feed']*100} for doc in filtered_feed if doc["time"] >= 1702598400000]


#-----------------------------------Consumption-----------------------------------
consumption = []
for pv_doc, feed_doc, purchase_doc in zip(filtered_PV, filtered_feed, filtered_purchase):
    pv_value = pv_doc['pv']  # assuming 'value' is the field you want to operate on
    feed_value = feed_doc['feed']
    purchase_value = purchase_doc['purchase']
    
    consumption_value = purchase_value + pv_value - feed_value
    consumption_doc = {'time': pv_doc['time'], 'consumption': consumption_value}
    consumption.append(consumption_doc)

#-----------------------------------Main Gas-----------------------------------
gas_main_old = list(collection_gas_1.find({"sensorId": "E0:5A:1B:A1:C5:F4"}))
for doc in gas_main_old:
    if 'm2' in doc:
      doc['m3'] = doc.pop('m2')
gas_main_new = list(collection_gas_2.find({"sensorId": "E0:5A:1B:A1:C5:F4"}))
#gas_main = remove_error_record((m3_to_kw(gas_main_old, 'gas') + m3_to_kw(gas_main_new, 'gas')), 'gas', 'gas')
gas_main = sorted((m3_to_kw(gas_main_old, 'gas') + m3_to_kw(gas_main_new, 'gas')), key=lambda x: x['time'])
gas_main = [{"time": unix_to_datetime(doc["time"]), "gas": doc['gas']} for doc in gas_main if doc["time"] >= 1702598400000]
gas_main = remove_error_record(gas_main, 'gas', 'gas')

#-----------------------------------Thermoil Gas-----------------------------------
gas_therm_old = list(collection_gas_1.find({"sensorId": "08:3A:F2:B6:6A:E4"}))
for doc in gas_therm_old:
    if 'm2' in doc:
      doc['m3'] = doc.pop('m2')
gas_therm_new = list(collection_gas_2.find({"sensorId": "08:3A:F2:B6:6A:E4"}))
#gas_main = remove_error_record((m3_to_kw(gas_main_old, 'gas') + m3_to_kw(gas_main_new, 'gas')), 'gas', 'gas')
gas_therm = sorted((m3_to_kw(gas_therm_old, 'gas') + m3_to_kw(gas_therm_new, 'gas')), key=lambda x: x['time'])
gas_therm = [{"time": unix_to_datetime(doc["time"]), "gas": doc['gas']} for doc in gas_therm if doc["time"] >= 1702598400000]
gas_therm = remove_error_record(gas_therm, 'gas', 'gas')

#-----------------------------------Graph Plot-----------------------------------
import plotly.express as px

# Extract the time and value columns from each filtered list
consumption_times = [doc["time"] for doc in consumption]
consumption_values = [doc["consumption"] for doc in consumption]

pv_times = [doc["time"] for doc in filtered_PV]
pv_values = [doc["pv"] for doc in filtered_PV]

purchase_times = [doc["time"] for doc in filtered_purchase]
purchase_values = [doc["purchase"] for doc in filtered_purchase]

feed_times = [doc["time"] for doc in filtered_feed]
feed_values = [doc["feed"] for doc in filtered_feed]

maingas_times = [doc["time"] for doc in gas_main]
maingas_values = [doc["gas"] for doc in gas_main]

thermgas_times = [doc["time"] for doc in gas_therm]
thermgas_values = [doc["gas"] for doc in gas_therm]

import plotly.graph_objects as go

fig = go.Figure(data=[
    #go.Scatter(x=consumption_times, y=consumption_values, name='Consumption Profile'),
    go.Scatter(x=pv_times, y=pv_values, name='PV Power'),
    go.Scatter(x=purchase_times, y=purchase_values, name='Purchase Power'),
    go.Scatter(x=feed_times, y=feed_values, name='Feed_In Power'),
])
fig_load = go.Figure(data=[
    go.Scatter(x=consumption_times, y=consumption_values, name='Consumption Profile'),
])
fig_gas = go.Figure(data=[
    go.Scatter(x=maingas_times, y=maingas_values, name='Main Gas'),
    go.Scatter(x=thermgas_times, y=thermgas_values, name='Thermoil Gas')
])

fig.update_layout(#title='PV, Purchase, and Feed Power',
                  xaxis_title='Time',
                  yaxis_title='kW', 
                  legend=dict(orientation="h", yanchor="bottom", y=1.02, itemclick="toggle"))
fig_load.update_layout(title='Net Consumption Profile',
                  xaxis_title='Time',
                  yaxis_title='kW')
fig_gas.update_layout(#title='Gas',
                  xaxis_title='Time',
                  yaxis_title='kW', 
                  legend=dict(orientation="h", yanchor="bottom", y=1.02, itemclick="toggle"))

#-----------------------------------App Layout-----------------------------------
import dash
from dash.dependencies import Input, Output, State
import dash_bootstrap_components as dbc
from dash import html
from dash import dcc

app = dash.Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP])

""" app.css.append_css({
    'external_url': 'https://fonts.googleapis.com/css?family=Open+Sans'
}) """

# Define the sidebar
sidebar = html.Div(
    [
        html.H2("KaiserLacke", className="display-4"),
        html.Hr(className="my-2"),
        dbc.Nav(
            [
                dbc.NavItem(dbc.NavLink("Energy Data", href="Energy", id="energy-tab")),
                dbc.NavItem(dbc.NavLink("Gas Data", href="Gas", id="gas-tab")),
                html.Br(),
            ],
            vertical=True,
            pills=True,
        ),
    ],
    style={"position": "fixed", "top": 0, "left": 0, "bottom": 0, "width": "20rem", "padding": "2rem"},
)

# Define the page content
page_content = html.Div(id="page-content", style={"margin-left": "20rem", "margin-top": "50px"})

# Define the layout
app.layout = html.Div([sidebar, dcc.Loading(page_content), 
            dcc.Interval(id="interval-component", interval= 10*60*1000, n_intervals=0,), #Refreshes every 10 minutes - time in milliseconds
            ])

# Initial state for the tabs
initial_active_tab = "energy-tab"

# Define the callback to update the page content and active tab
@app.callback(
    [Output("page-content", "children"), Output("energy-tab", "active"), Output("gas-tab", "active"),],
    [Input("energy-tab", "n_clicks"), Input("gas-tab", "n_clicks"),],
    [State("energy-tab", "active"), State("gas-tab", "active"),]
)
def update_page_content(energy_clicks, gas_clicks, current_energy_active, current_gas_active):
    ctx = dash.callback_context
    if not ctx.triggered:
        # Initial state
        return html.Div([
            html.H1("Energy Data", style={"textAlign": "center"}),
            dcc.Graph(id="energy-graph", figure=fig, style={"height": "600px"}),
            dcc.Graph(id="consumption-graph", figure=fig_load, style={"height": "600px"}),
        ]), True, False
    else:
        button_id = ctx.triggered[0]['prop_id'].split('.')[0]
        if button_id == "energy-tab":
            return html.Div([
                html.H1("Energy Data", style={"textAlign": "center"}),
                dcc.Graph(id="energy-graph", figure=fig, style={"height": "600px"}),
                dcc.Graph(id="consumption-graph", figure=fig_load, style={"height": "600px"}),
            ]), True, False
        elif button_id == "gas-tab":
            return html.Div([
                html.H1("Gas Data", style={"textAlign": "center"}),
                dcc.Graph(id="gas-graph", figure=fig_gas, style={"height": "600px"}),
            ]), False, True

if __name__ == "__main__":
    app.run_server()