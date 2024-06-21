import pvlib
import pandas as pd
import matplotlib.pyplot as plt


df = pd.read_parquet("df_full.parquet")
job_table = pd.read_parquet("job_table.parquet")
co2 = pd.read_csv("co2_data.csv")


print("Import")
df = df.loc['2020-05-05':'2020-06-05']
df_aux = df.sum(axis = 1)
df_aux = df_aux.apply(lambda x: x/1000) # Passagem para kw
pue = 1.1
df_aux = df_aux.apply(lambda x: x*pue)
#df_aux.fillna(0)
consumos = df_aux.to_list()
maxpower = max(consumos)
print(f"O valor de consumo máximo instantâneo é igual a {maxpower} KW ")








# inputs
peakpower = 600 # 1 kW of installed capacity (can be scaled later)
target_year = 2023
lat, lon = 41.45257, -8.28961
# pvlib power calculation
pv_gen = pvlib.iotools.get_pvgis_hourly(lat, lon, start=2015, end=2015, surface_tilt=30, surface_azimuth=0, pvcalculation=True, peakpower=peakpower)
# pre-process
pv_gen = pv_gen[0]['P'].apply(lambda x: x / 1000) # get power in kW
pv_gen = pv_gen.resample('15min').mean()
pv_gen.index = pv_gen.index.map(lambda x: x.replace(year=target_year)) # replace 2015 by target year
pvgen = pv_gen.loc['2023-05-05 15:45:00+00:00 ':'2023-06-05 23:45:00+00:00']
pvgen = pvgen.fillna(method = 'backfill')
pvgen = pvgen.fillna(0)

pv = pvgen.to_list()

pnet = []
for i in range (len(pv)):
    pnet.append(pv[i] - consumos[i])

rede = []
for j in range(len(pnet)):
    if(pnet[j] > 0):
        rede.append("Excesso")
    if(pnet[j] == 0):
        rede.append("Zero")
    if(pnet[j] < 0):
        rede.append("Comprar à rede")
    

data = pd.DataFrame()
#data.set_index(pv_gen.index)
data['Consumos'] = consumos
data['PV'] = pv
data['Pnet'] = pnet
data['Status'] = rede
data['Comprar'] = data['Status'] == "Comprar à rede"
data['Excedeu'] = data['Status'] == "Excesso"
data['Zeros'] = data['Status'] == "Zero"
data = data.set_index(pvgen.index)

compras = data['Comprar'].sum()
excedentes = data['Excedeu'].sum()
zeros = data['Zeros'].sum()

co2['datetime'] = pd.to_datetime(co2['datetime'])
co2['datetime'] = co2['datetime'].dt.strftime("%Y-%m-%d %H:%M:%S+00:00")
co2 = co2.set_index(co2['datetime'])
co2.drop("datetime", axis = 'columns', inplace = True)
co2.index = pd.to_datetime(co2.index)
co2 = co2.loc['2023-05-05 15:45:00+00:00 ':'2023-06-05 23:45:00+00:00']
co2 = co2.resample("15min").mean()
#media = co2['value'].mean()
#total = co2['value'].sum()
data['Intensidade Carbónica'] = co2['value'] 
data['Enet'] = data['Pnet']* 0.25
data['Enet Emissions'] = data['Enet'].copy()
data['Enet Emissions'] = data['Enet Emissions'].apply(lambda x: 0 if x>0 else x)
data['Emissões Indiretas de CO2'] = data["Enet"] * data["Intensidade Carbónica"]


plt.plot(data.index, data['PV'], label = 'PV')
plt.plot(data.index, data['Consumos'], label = 'Consumos')
plt.plot(data.index, data['Pnet'], label = 'Pnet')
plt.plot(data.index, data['Enet Emissions'], label = 'Energia Importada')
plt.legend()
plt.show()







print(f"Para esta simulação, temos um cenário em que se teve que ir buscar energia à rede {compras} vezes. No entanto, foi possível produzir mais do que o que se consumiu em {excedentes} das vezes.\nHouve {zeros} períodos em que nem se consumiu nem se produziu.\n ")
print("fim")