# Copyright Redis Ltd. 2021 - present
# Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
# the Server Side Public License v1 (SSPLv1).

import matplotlib.pyplot as plt
import numpy as np

from sklearn.tree import DecisionTreeClassifier
from sklearn import tree
'''------------------- batches BF -----------------'''


def print_tree(clf):
    tree.plot_tree(clf)
    plt.savefig('batches_vs_adhoc-BF.pdf')
    plt.show()
    n_nodes = clf.tree_.node_count
    children_left = clf.tree_.children_left
    children_right = clf.tree_.children_right
    feature = clf.tree_.feature
    threshold = clf.tree_.threshold

    node_depth = np.zeros(shape=n_nodes, dtype=np.int64)
    is_leaves = np.zeros(shape=n_nodes, dtype=bool)
    stack = [(0, 0)]  # start with the root node id (0) and its depth (0)
    while len(stack) > 0:
        # `pop` ensures each node is only visited once
        node_id, depth = stack.pop()
        node_depth[node_id] = depth

        # If the left and right child of a node is not the same we have a split
        # node
        is_split_node = children_left[node_id] != children_right[node_id]
        # If a split node, append left and right children and depth to `stack`
        # so we can loop through them
        if is_split_node:
            stack.append((children_left[node_id], depth + 1))
            stack.append((children_right[node_id], depth + 1))
        else:
            is_leaves[node_id] = True

    print("The binary tree structure has {n} nodes and has the following tree structure:\n".format(n=n_nodes))
    for i in range(n_nodes):
        if is_leaves[i]:
            print("{space}node={node} is a leaf node.".format(space=node_depth[i] * "\t", node=i))
        else:
            print("{space}node={node} is a split node: go to node {left} if X[:, {feature}] <= {threshold} "
                  "else to node {right}.".format(space=node_depth[i] * "\t", node=i, left=children_left[i],
                                                 feature=feature[i], threshold=threshold[i], right=children_right[i]))


if __name__ == '__main__':
    clf = DecisionTreeClassifier(max_leaf_nodes=10)
    # features: (index_size, dim, r)
    # labels: 1: adhoc better, -1: batches better
    y = []

    # The following labels represent the results for experiments that were held for the following combinations:
    # index_sizes: 1000, 10000, 100000, 1M, 10M
    # dim: 10, 100, 1000 (except for size=10M with dim=1000)
    # ratio: 0.1-0.8 with steps of 0.1
    X_1 = np.array([[10**i, 10**j, l/10] for i in range(3, 8) for j in range(1, 4) for l in range(1, 9) if not (i == 7 and j == 3)])
    # (1000, [10,100,1000], 0.1-0.8)
    y.extend([1, 1, 1, 1, 1, 1, 1, 1])
    y.extend([1, 1, 1, 1, 1, 1, 1, 1])
    y.extend([1, 1, 1, 1, 1, 1, 1, 1])
    # (10000, [10,100,1000], 0.1-0.8)
    y.extend([1, -1, 1, -1, -1, -1, -1, -1])
    y.extend([1, 1, 1, -1, -1, -1, -1, -1])
    y.extend([1, 1, 1, 1, 1, 1, 1, -1])
    # (100000, [10,100,1000], 0.1-0.8)
    y.extend([1, -1, -1, -1, -1, -1, -1, -1])
    y.extend([1, 1, 1, -1, -1, -1, -1, -1])
    y.extend([1, 1, 1, 1, 1, 1, 1, -1])
    # (1M, [10,100,1000], 0.1-0.8)
    y.extend([1, -1, -1, -1, -1, -1, -1, -1])
    y.extend([1, 1, -1, -1, -1, -1, -1, -1])
    y.extend([1, 1, 1, 1, 1, 1, -1, -1])
    # (10M, [10,100], 0.1-0.8)
    y.extend([1, -1, -1, -1, -1, -1, -1, -1])
    y.extend([1, 1, -1, -1, -1, -1, -1, -1])

    # The following labels represent the results for experiments that were held for the following combinations:
    # index_sizes: 50000, 500000, 5M
    # dim: 5, 50, 500
    X_2 = np.array([[5*(10**i), 5*(10**j), l/10] for i in range(4, 7) for j in range(3) for l in range(1, 9)])
    # (50000, [5,50,500], 0.1-0.8)
    y.extend([1, -1, -1, -1, -1, -1, -1, -1])
    y.extend([1, 1, -1, -1, -1, -1, -1, -1])
    y.extend([1, 1, 1, 1, 1, -1, -1, -1])
    # (500000, [5,50,500], 0.1-0.8)
    y.extend([1, -1, -1, -1, -1, -1, -1, -1])
    y.extend([1, -1, -1, -1, -1, -1, -1, -1])
    y.extend([1, 1, 1, 1, 1, -1, -1, -1])
    # (5M, [5,50,500], 0.1-0.8)
    y.extend([1, -1, -1, -1, -1, -1, -1, -1])
    y.extend([1, -1, -1, -1, -1, -1, -1, -1])
    y.extend([1, 1, 1, 1, 1, -1, -1, -1])

    # The total train set is the concatenation of the two combinations sets above.
    X = np.concatenate((X_1, X_2))
    clf = clf.fit(X, y)

    print_tree(clf)
    print(clf.predict([[5e5, 600, 0.6]]))
