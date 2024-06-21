import pandas as pd
import matplotlib.pyplot as plt

df = pd.read_parquet("job_table.parquet")


df['test_freq'] = df['node_power_consumption'].apply(lambda x: x.size) #numero de medidas de potencia
df['test_freq'] = df['run_time'] / df['test_freq'] # por segundo, quantas medidas de potencia se efetuam
hours = 5
new_list = list(filter(lambda x: x >= 3600*hours, df['run_time']))
print('De todos os jobs ({}), apenas {} executam em mais de {} horas '.format(len(df['run_time']),len(new_list),hours))
#df_filtrada = df[df['run_time']>=3600*hours] #Nova dataframe sem os jobs que correram abaixo de 5h 
max= df['end_time'].max()
min = df['start_time'].min()
print('O job que começou mais cedo, começou em {}'.format(min))
print('O job que acabou mais tarde, acabou em {}'.format(max))
tempo = '15min'

df_full = pd.DataFrame(index=pd.date_range(min,max, freq=tempo)) 
df_full = df_full.resample(tempo).mean()
cont = 0
for i, df in df.iterrows():    

    id_ = df['job_id']
    expected_times = pd.date_range(df['start_time'], df['end_time'], periods=df['node_power_consumption'].size)
    df_temp = pd.DataFrame(index = expected_times, data = {'consumo':df['node_power_consumption']})
    df_temp = df_temp.resample(tempo).mean()
    df_temp = df_temp.reindex(df_full.index)
    df_full[id_] = df_temp['consumo']
    cont = cont +1
    print(f"{cont}\n")
    
   
print("ciclo...Ok")
dfaux = df_full.sum(axis = 1)
dfaux.to_parquet("df_consumo.parquet")
#novo grafico

print('fim')
