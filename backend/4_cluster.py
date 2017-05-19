"""
Copyright (c) 2017 Conrad Indiono

This program is free software: you can redistribute it and/or modify it under
the terms of the GNU General Public License as published by the Free Software
Foundation, either version 3 of the License, or (at your option) any later
version.

This program is distributed in the hope that it will be useful, but WITHOUT ANY
WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
PARTICULAR PURPOSE.  See the GNU General Public License for more details.

You should have received a copy of the GNU General Public License along with
this program (see file COPYING). If not, see <http://www.gnu.org/licenses/>.
"""

import pandas as pd, numpy as np, matplotlib.pyplot as plt
from sklearn.cluster import DBSCAN
from geopy.distance import great_circle
from shapely.geometry import MultiPoint
df = pd.read_csv('lat_long_key.csv')
coords = df.as_matrix(columns=['latitude', 'longitude'])

def extract_clusters(coords, distance=0.01):
    """
    extract clusters from the given distance matrix using max distance
    for each element with the other elements within the cluster.
    
    returns: (cluster_labels, number of clusters)
    """
    kms_per_radian = 6371.0088
    epsilon = distance / kms_per_radian
    db = DBSCAN(eps=epsilon, min_samples=1, algorithm='ball_tree', metric='haversine').fit(np.radians(coords))
    cluster_labels = db.labels_
    num_clusters = len(set(cluster_labels))
    clusters = pd.Series([coords[cluster_labels == n] for n in range(num_clusters)])
    return cluster_labels, num_clusters, clusters

def get_centermost_point(cluster):
    centroid = (MultiPoint(cluster).centroid.x, MultiPoint(cluster).centroid.y)
    centermost_point = min(cluster, key=lambda point: great_circle(point, centroid).m)
    return tuple(centermost_point)

def centermost_point_df(clusters, label):
    results = []
    for n,cluster in enumerate(clusters):
        x, y = get_centermost_point(cluster)
        results.append((n, x, y))
    cluster_ids, lats, lons = zip(*results)
    return pd.DataFrame({'cluster_id':cluster_ids, ('lat_'+label):lats, ('lon_'+label):lons})

#clusters = pd.Series([coords[cluster_labels == n] for n in range(num_clusters)])
#import ipdb
#ipdb.set_trace()
#print('Number of clusters: {}'.format(num_clusters))

#with open("clusters.csv", "w") as out:
#    out.write("LATITUDE,LONGITUDE,CLUSTER_ID\n")
#    for cluster_id, cluster in enumerate(clusters):
#        for x in cluster:
#            line = ",".join([str(x[0]), str(x[1]), str(cluster_id)])
#            out.write( line )
#            out.write( "\n" )


# distance = 40m
cluster_labels, num_clusters, clusters = extract_clusters(coords, 0.04)
cluster_points_df = centermost_point_df(clusters, "40m")
df_40m = df.assign(cluster_id_40m=cluster_labels)
df_40m = df_40m.merge(cluster_points_df, left_on="cluster_id_40m", right_on="cluster_id")
df_40m = df_40m.drop("cluster_id", 1)
df_40m.to_csv("clusters_40m.csv", index=False)
print "40m distance cluster count:", num_clusters

# distance = 30m
cluster_labels, num_clusters, clusters = extract_clusters(coords, 0.03)
cluster_points_df = centermost_point_df(clusters, "30m")
df_30m = df.assign(cluster_id_30m=cluster_labels)
df_30m = df_30m.merge(cluster_points_df, left_on="cluster_id_30m", right_on="cluster_id")
df_30m = df_30m.drop("cluster_id", 1)
df_30m.to_csv("clusters_30m.csv", index=False)
print "30m distance cluster count:", num_clusters

# distance = 25m
cluster_labels, num_clusters, clusters = extract_clusters(coords, 0.025)
cluster_points_df = centermost_point_df(clusters, "25m")
df_25m = df.assign(cluster_id_25m=cluster_labels)
df_25m = df_25m.merge(cluster_points_df, left_on="cluster_id_25m", right_on="cluster_id")
df_25m = df_25m.drop("cluster_id", 1)
df_25m.to_csv("clusters_25m.csv", index=False)
print "25m distance cluster count:", num_clusters

# distance = 10m
cluster_labels, num_clusters, clusters = extract_clusters(coords, 0.01)
cluster_points_df = centermost_point_df(clusters, "10m")
df_10m = df.assign(cluster_id_10m=cluster_labels)
df_10m = df_10m.merge(cluster_points_df, left_on="cluster_id_10m", right_on="cluster_id")
df_10m = df_10m.drop("cluster_id", 1)
df_10m.to_csv("clusters_10m.csv", index=False)
print "10m distance cluster count:", num_clusters
