"""
Make charts from ReJSONBenchmark's output
"""

import matplotlib.pyplot as plt
import numpy as np

data = np.genfromtxt('benchmark.csv', delimiter=',', names=True, dtype=None)

# Each JSON value has 4 operations: set root, get root, set path, get path
NOP = 4

# The number of JSON values
N = int(len(data) / NOP)

# The x locations
ind = np.arange(N)

# The bars' width
width = (1 - .3) / NOP

colors = ['r','g','b','y']

fig, ax = plt.subplots()

# Iterate each operation
for oidx in range(NOP):
    d = data[oidx::NOP]
    # ax1 = ax[oidx]

    # plt.subplot(NOP,1,oidx+1)

    # Plot the rate as bars
    plt.bar(ind+oidx*width, d['rate'], width, align='center', color=colors[oidx])

    # Plot the latency as line
    # tax = plt.twinx()
    # tax.set_yscale('log')
    # tax.plot(ind, d['avgLatency'], color='r')

    # plt.title('{} rate and average latency'.format(d['title'][0]))
    # ax1.set_ylabel('Rate (op/s)', color='b')
    # ax1.set_xlabel('Object size (bytes)')
    # for t in ax1.get_yticklabels():
    #     t.set_color('b')
    # ax2.set_ylabel('Average latency (msec)', color='r')
    # for t in ax2.get_yticklabels():
    #     t.set_color('r')
    # plt.xticks(ind, d['size']) 

plt.grid(True)
plt.xticks(ind + width, d['size'])
plt.show()
