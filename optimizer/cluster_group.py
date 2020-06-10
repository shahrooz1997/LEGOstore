import pandas as pd
import numpy as np
import sklearn
import argparse
from argparse import ArgumentParser
from sklearn.cluster import DBSCAN
from sklearn import metrics
import matplotlib.pyplot as plt

def parse_args():
    parser = ArgumentParser(description = 'Process cmd args for placements')
    parser.add_argument('-f','--file-name', dest='fname', required=True)
    parser.add_argument('-o','--out-file', dest='outfile', required=False)
    parser.add_argument('-i','--out-id', dest='outid', required=False)
    args = parser.parse_args()
    return args

def get_df(fname):
    """ Make dataframe from csv
    """
    df = pd.read_csv(fname, header=None)
    df = df.rename(columns={0:'key', 1:'op', 2:'group', 3:'dc'})
    return df

def print_score(X, labels_true, labels, n_clusters_, n_noise_):
    print("Homogeneity: %0.3f" % metrics.homogeneity_score(labels_true, labels))
    print("Completeness: %0.3f" % metrics.completeness_score(labels_true, labels))
    print("V-measure: %0.3f" % metrics.v_measure_score(labels_true, labels))
    print("Adjusted Rand Index: %0.3f"
	  % metrics.adjusted_rand_score(labels_true, labels))
    print("Adjusted Mutual Information: %0.3f"
	  % metrics.adjusted_mutual_info_score(labels_true, labels))
    print("Silhouette Coefficient: %0.3f"
	  % metrics.silhouette_score(X, labels))

def cluster(X, labels_true=None):
    db = DBSCAN().fit(X)
    core_samples_mask = np.zeros_like(db.labels_, dtype=bool)
    core_samples_mask[db.core_sample_indices_] = True
    labels = db.labels_
    n_clusters_ = len(set(labels)) - (1 if -1 in labels else 0)
    n_noise_ = list(labels).count(-1)
    #print_score(X, labels_true, labels, n_clusters_, n_noise_)
    unique_labels = set(labels)
    print(unique_labels)
    colors = [plt.cm.Spectral(each)
	      for each in np.linspace(0, 1, len(unique_labels))]
    for k, col in zip(unique_labels, colors):
        if k == -1:
	    # Black used for noise.
            col = [0, 0, 0, 1]
        class_member_mask = (labels == k)
        xy = X[class_member_mask & core_samples_mask]
        plt.plot(xy.values[:, 0], xy.values[:, 1], 'o', markerfacecolor=tuple(col), markeredgecolor='k', markersize=14)
        xy = X[class_member_mask & ~core_samples_mask]
        plt.plot(xy.values[:, 0], xy.values[:, 1], 'o', markerfacecolor=tuple(col), markeredgecolor='k', markersize=6)

    plt.title('Groups using Clustering')
    plt.xlabel('Arrival rate per key')
    plt.ylabel('Read:Write')
    plt.show()


def main(args, fname=None):
    filename = fname if fname else args.fname
    X = np.genfromtxt(filename, delimiter=',')
    dataset = pd.DataFrame({'Arrival_Rate':X[:,0],'Read_Write_Ratio':X[:,1]})
    cluster(dataset, None)

if __name__ == "__main__":
    args = parse_args()
    print(args)
    main(args)
