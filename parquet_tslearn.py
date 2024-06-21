
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import kneed
from sklearn.preprocessing import StandardScaler
from tslearn.datasets import CachedDatasets
from tslearn.clustering import TimeSeriesKMeans
from tslearn.preprocessing import TimeSeriesScalerMeanVariance, TimeSeriesResampler
from sklearn.model_selection import train_test_split
from tslearn.svm import TimeSeriesSVR
from sklearn.preprocessing import StandardScaler
import kneed




df = pd.read_parquet("df_full.parquet")
job_table = pd.read_parquet("job_table.parquet")
print(f"Número de timestamps de 15 min: {len(df.index)}")





df_nova = pd.DataFrame(index = range(140))
Id = df.columns
lista = Id.tolist()


temp = pd.DataFrame(index= range(df.count().max()))


for i in (lista):
   lista_consumos= df[i].dropna().to_list()
   aux = pd.Series(lista_consumos)
   aux = aux.reindex(range(temp.shape[0]))
   job_id = i
   temp[job_id] = aux
   # temp = pd.concat([temp, aux], axis = 1)


temp.fillna(0, inplace = True)


nr_clusters = 5



scaler = StandardScaler()
X = temp.to_numpy().T
X = X[:500, :]

#VERSAO COM NaNs
resampler = TimeSeriesResampler(sz = 100)
#X_new = scaler.fit_transform(X)
#X_new = resampler.fit(X_new)

#Versao com 0s
X_new = scaler.fit_transform(X)

inertia = []
for k in range(1, nr_clusters):
    print(f"Ciclo {k}")
    kmeans = TimeSeriesKMeans(n_clusters = k, random_state  = 40, metric ='dtw', verbose = 2).fit(X_new)
    #kmeans = TimeSeriesKMeans(n_clusters = k, random_state  = 40).fit(X_new)
    inertia.append(kmeans.inertia_)

best_cluster = kneed.KneeLocator(range(1, nr_clusters), inertia, curve='convex', direction='decreasing').elbow

kmeans = TimeSeriesKMeans(n_clusters = 4, random_state = 40, metric = 'dtw', verbose = 2,max_iter_barycenter=20, n_jobs = 4).fit(X_new)
#kmeans = TimeSeriesKMeans(n_clusters = best_cluster, random_state = 40).fit(X_new)
centers = kmeans.cluster_centers_
labels = kmeans.labels_
y_pred = kmeans.predict(X_new)

print("predict")
#plot

for yi in range(best_cluster):
    plt.subplot(best_cluster, 1, yi + 1)
    for xx in X_new[y_pred == yi]:
        xx_ = scaler.inverse_transform(xx.reshape(1, -1))
        plt.plot(xx_.ravel(), "k-", alpha=.2)
        plt.ylabel("Consumption")
        plt.xlabel("Time Step")
    plt.plot(scaler.inverse_transform(kmeans.cluster_centers_[yi].ravel().reshape(1, -1))[0], "r-")
    plt.xlim(0, temp.shape[0])
    plt.text(0.55, 0.85,'Cluster %d' % (yi + 1),
             transform=plt.gca().transAxes)
   # if yi == 1:
        #plt.title("Euclidean $k$-means")
    
plt.show()
print("fim do prog")






   


