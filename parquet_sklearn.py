import pandas as pd
from sklearn.cluster import KMeans
import matplotlib.pyplot as plt
import numpy as np
import kneed 
from scipy.stats import skew
from scipy.stats import kurtosis
from sklearn.preprocessing import StandardScaler




df_consumos_inicial = pd.read_parquet("df_full.parquet")
job_table = pd.read_parquet("job_table.parquet")

Id = df_consumos_inicial.columns
lista = Id.tolist()
df_filtrada = job_table[job_table['job_id'].isin(lista)] 
df_filtrada['total_consumption'] = df_consumos_inicial.sum(axis=0).to_list()
df_filtrada['avg_consumption'] = df_consumos_inicial.mean(axis= 0).to_list()
df_filtrada['std_consumption'] = df_consumos_inicial.std(axis = 0).to_list()
df_filtrada['median_consumption'] = df_consumos_inicial.median(axis = 0).to_list()
df_filtrada['max_consumption'] = df_consumos_inicial.max(axis=0).to_list()
df_filtrada['load_factor'] = df_filtrada['avg_consumption']/df_filtrada['max_consumption']
df_filtrada['min_consumption'] = df_consumos_inicial.min(axis = 0).to_list()
df_filtrada['skewness'] = 3 * (df_filtrada['avg_consumption'] - df_filtrada['median_consumption'])/df_filtrada['std_consumption']

print("Fim da criação de  features")











 
print("Seccção 1 de Debug")



print("K-Means")
#Elbow method
print("Elbow Method:\n")
df_filtrada['hora'] = df_filtrada['submit_time'].apply(lambda x: x.hour)
X = df_filtrada[['run_time', 'load_factor', 'std_consumption', 'hora','avg_consumption']]
scaler = StandardScaler()
X_new = scaler.fit_transform(X)
X_new = pd.DataFrame(data=X_new, columns=X.columns)




cluster_numbers = 20
inertia = []
for k in range(1, cluster_numbers):
    kmeans = KMeans(n_clusters = k, random_state  = 40).fit(X_new)
    inertia.append(kmeans.inertia_)

print(f"Valores de inércia:{inertia}")
best_cluster = kneed.KneeLocator(range(1, cluster_numbers), inertia, curve='convex', direction='decreasing').elbow
fig, ax = plt.subplots()
ax.plot(range(1, cluster_numbers), inertia)
plt.title("Elbow Method")
plt.xlabel("Nº Clusters")
plt.ylabel("Valores de Inertia")
plt.draw()
print(f"O melhor número de clusters é igual a {best_cluster}")

kmeans = KMeans(best_cluster)
kmeans.fit(X_new)
centers = kmeans.cluster_centers_
centers_ = scaler.inverse_transform(centers)
labels = kmeans.labels_

x, y = 'load_factor', 'run_time'
fig, ax = plt.subplots()
ax.scatter(X[x], X[y], c=kmeans.labels_, cmap='viridis')
ax.scatter(centers_[:, X.columns.get_loc(x)], centers_[:, X.columns.get_loc(y)], c='red', marker='x')
for i, c in enumerate(centers_):
    plt.annotate(i, (centers_[:, X.columns.get_loc(x)][i], centers_[:, X.columns.get_loc(y)][i]))
ax.set_xlabel(x)
ax.set_ylabel(y)
ax.set_title('Clustering')
plt.draw()


plt.show()

df_filtrada['cluster'] = kmeans.predict(X_new)
job_id = []
j = 0


for i in range(best_cluster):
    #plt.clf()
    fig, ax = plt.subplots()
    job_id = df_filtrada[df_filtrada['cluster'] == i]['job_id'].to_list()
    df_temp = df_consumos_inicial[job_id]
    for j in range(500):
        try:
            df_temp.iloc[:, j].dropna(inplace=False).reset_index(drop=True).plot()
        except:
            continue
    plt.xlabel("Time step")
    plt.ylabel("Consumption") 
    plt.savefig(f'jobs_timeseries_{i}.png')
    plt.show()





print("fim")



