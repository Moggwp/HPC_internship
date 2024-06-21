import pandas as pd
import matplotlib.pyplot as plt 
import numpy as np
from sklearn.neighbors import KNeighborsTransformer

df = pd.read_parquet("df_full.parquet")
df2 = pd.read_parquet("job_table.parquet")
#df = df.fillna(0)
min = df.index.min()
max = min + pd.DateOffset(days = 90)
df_filter = df.loc[min:max] 
df_filter = df_filter.dropna(axis= 1, how= 'all') #vai-nos dar apenas os jobs que correram em uma semana
nomes = df_filter.columns.tolist() # ID's dos jobs que correram neste intervalo de tempo
count_total = len(df.columns.tolist())
count = len(nomes)
print(f"Desde {min} até {max}, correram {count} jobs, de um total de {count_total}. Nesta amostra são considerados {round(count/count_total * 100,2)} % dos jobs ")

#Criação do primeiro gráfico
df_operacional = df_filter.notnull().astype(int) # Sempre que o valor não é um NaN ou nulo, atribuimos o valor lógico igual a 1, indicando que esse job está atualmente a correr
#df_operacional.sum(axis = 1) #vai a todas as linhas e soma todos os 1s , de modo a termos o numero total de jobs
#df_operacional.sum(axis = 1).plot()
#plt.title("Número de jobs a correr em simultâneo") 
#plt.show()

#remover o gap com 0s 

#Repetir para o consumo
df_aux = df_operacional.sum(axis = 1)
df_aux = df_aux.to_frame()
df_aux = df_aux.rename(columns= {0:"value"})
df_aux = df_aux.resample('h').mean()
df_aux['date'] = df_aux.index

#Criação dos Histogramas para o número de jobs
media = df_aux['value'].groupby(df_aux['value'].index.hour).mean()
percentil_10 = df_aux['value'].groupby(df_aux['value'].index.hour).quantile(0.1)
percentil_90 = df_aux['value'].groupby(df_aux['value'].index.hour).quantile(0.9)
bar_width = 0.25
r1 = np.arange(len(media))
r2 = [x + bar_width for x in r1]
r3 = [x + bar_width for x in r2]
plt.bar(r1, media, color='b', width=bar_width, alpha=0.5, label='Média')
plt.bar(r2, percentil_10, color='g', width=bar_width, alpha=0.5, label='10º percentil')
plt.bar(r3, percentil_90, color='r', width=bar_width, alpha=0.5, label='90º percentil')
plt.xlabel('Hora do dia')
plt.ylabel('Número de jobs')
plt.title('Número de jobs por hora do dia')
plt.legend()
plt.show()

###
df_consumo = df_filter.sum(axis = 1)
df_consumo = df_consumo.to_frame()
df_consumo = df_consumo.rename(columns = {0:"value"})
df_consumo = df_consumo.resample('h').mean()
df_consumo['date'] = df_consumo.index

media = df_consumo['value'].groupby(df_consumo['value'].index.hour).mean()
percentil_10 = df_consumo['value'].groupby(df_consumo['value'].index.hour).quantile(0.1)
percentil_90 = df_consumo['value'].groupby(df_consumo['value'].index.hour).quantile(0.9)
bar_width = 0.25
r1 = np.arange(len(media))
r2 = [x + bar_width for x in r1]
r3 = [x + bar_width for x in r2]
plt.bar(r1, media, color='b', width=bar_width, alpha=0.5, label='Média')
plt.bar(r2, percentil_10, color='g', width=bar_width, alpha=0.5, label='10º percentil')
plt.bar(r3, percentil_90, color='r', width=bar_width, alpha=0.5, label='90º percentil')
plt.xlabel('Hora do dia')
plt.ylabel('Consumo (W)')
plt.title('Consumo de jobs por hora do dia')
plt.legend()
plt.show()
print("Gráfico 1: OK")

#Gráfico do consumo do HPC em função do numero de jobs a correr em simultaneo
plt.scatter(df_operacional.sum(axis=1), df_filter.sum(axis=1))
plt.title('Evolução do consumo total do HPC em função do número de jobs que está a correr em simultâneo')
plt.xlabel('Número de jobs a correr em simultâneo')
plt.ylabel('Consumo total (W)')
plt.show()
print("Gráfico 2: OK")


# Gráfico dos jobs submetidos por hora do dia
df2['intervalo'] = df2['start_time'] - df2['start_time']
df2['submit_time'].apply(lambda x: x.hour).hist()
plt.title('Número de jobs submetidos por hora do dia')
plt.xlabel('Hora do dia')
plt.ylabel('Número de jobs')
plt.show()
print("Gráfico 3: OK")




print("fim do programa")
