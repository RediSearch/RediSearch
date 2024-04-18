import matplotlib.pyplot as plt
import matplotlib as mpl

xs = [10000, 30000, 50000, 70000, 90000, 110000, 130000, 150000, 170000, 190000, 210000, 230000, 250000]

# ------------------------------------------------------------------------------
# Before Raz PR (commit 71a4af2)
before_raz_tag = [58.32,   58.07,   57.86,   56.88,   58.05,   57.36,   58.06,   56.49,   57.18,   56.87,   58.12,   57.80,   57.55]
before_raz_txt = [73.67,   73.42,   73.50,   73.83,   73.65,   73.27,   73.45,   73.58,   73.68,   73.15,   73.57,   73.40,   73.26]
before_raz_num = [22.58,   24.32,   23.49,   20.43,   24.28,   21.98,   24.39,   19.11,   21.44,   20.54,   24.51,   23.45,   22.47]
before_raz_geo = [23.27,   27.51,   24.65,   16.78,   26.15,   24.28,   26.44,   17.92,   23.75,   25.40,   24.22,   23.39,   22.66]
before_raz_geoshape = [20.23,   23.39,   22.22,   22.94,   23.27,   20.96,   23.36,   17.86,   20.39,   19.27,   23.57,   22.54,   21.56]
before_raz_tagwithsuffixtrie = [86.65,   86.18,   85.89,   85.66,   85.57,   85.44,   85.43,   85.17,   85.12,   84.97,   85.19,   85.08,   85.02]
before_raz_txtwithsuffixtrie = [101.77,   89.87,   89.68,   89.64,   89.46,   89.35,   89.31,   89.27,   89.22,   89.07,   89.15,   89.09,   89.04]

before_raz_tag = [x / 100.0 for x in before_raz_tag]
before_raz_txt = [x / 100.0 for x in before_raz_txt]
before_raz_num = [x / 100.0 for x in before_raz_num]
before_raz_geo = [x / 100.0 for x in before_raz_geo]
before_raz_geoshape = [x / 100.0 for x in before_raz_geoshape]
before_raz_tagwithsuffixtrie = [x / 100.0 for x in before_raz_tagwithsuffixtrie]
before_raz_txtwithsuffixtrie = [x / 100.0 for x in before_raz_txtwithsuffixtrie]

# ------------------------------------------------------------------------------
# After Raz PR: MOD-5920 (commit: dc04e4952)
after_raz_tag = [44.16,   43.83,   43.67,   42.39,   43.90,   42.95,   43.84,   41.71,   42.60,   42.15,   43.82,   43.41,   43.06]
after_raz_txt = [64.08,   63.82,   63.88,   64.30,   64.08,   63.56,   63.84,   64.02,   64.18,   63.46,   64.01,   63.80,   63.61]
after_raz_num = [22.36,   24.22,   23.41,   20.47,   24.17,   22.09,   24.36,   19.31,   21.59,   20.46,   24.54,   23.48,   22.52]
after_raz_geo = [23.20,   27.49,   24.83,   16.24,   26.36,   24.35,   26.55,   17.74,   23.50,   25.28,   24.22,   23.26,   22.48]
after_raz_geoshape = [19.29,   23.39,   22.38,   19.16,   23.27,   20.86,   23.30,   18.00,   20.35,   19.32,   23.61,   22.46,   21.55]
after_raz_tagwithsuffixtrie = [50.91,   50.96,   50.97,   51.18,   51.53,   51.59,   51.89,   51.25,   51.31,   50.99,   51.83,   51.59,   51.53]
after_raz_txtwithsuffixtrie= [105.61,   68.94,   68.88,   69.12,   68.92,   68.90,   69.02,   69.12,   69.15,   68.87,   69.27,   69.20,   69.18]

after_raz_tag = [x / 100.0 for x in after_raz_tag]
after_raz_txt = [x / 100.0 for x in after_raz_txt]
after_raz_num = [x / 100.0 for x in after_raz_num]
after_raz_geo = [x / 100.0 for x in after_raz_geo]
after_raz_geoshape = [x / 100.0 for x in after_raz_geoshape]
after_raz_tagwithsuffixtrie = [x / 100.0 for x in after_raz_tagwithsuffixtrie]
after_raz_txtwithsuffixtrie = [x / 100.0 for x in after_raz_txtwithsuffixtrie]

# ------------------------------------------------------------------------------
# Nafraf's PR: MOD-5920 MOD-5866 MOD-5977 (branch: nafraf_fix-inv-idx-mem-report commit: dc17b2d)
nafraf_tag = [14.87,   14.44,   13.97,   14.27,   14.35,   12.95,   14.41,   11.18,   12.59,   11.96,   14.50,   13.87,   13.33]
nafraf_txt = [44.18,   43.76,   43.79,   44.44,   44.11,   43.32,   43.77,   44.09,   44.33,   43.23,   44.09,   43.76,   43.45]
nafraf_num = [22.02,   23.86,   22.85,   19.71,   23.45,   21.46,   23.80,   18.74,   20.89,   19.96,   24.00,   22.82,   22.09]
nafraf_geo = [21.82,   26.07,   22.79,   15.32,   24.96,   22.91,   25.07,   16.31,   22.24,   23.98,   23.01,   21.96,   21.28]
nafraf_geoshape = [21.23,   23.40,   22.58,   19.03,   23.16,   20.88,   23.42,   18.00,   20.28,   19.12,   23.57,   22.50,   21.59]
nafraf_tagwithsuffixtrie = [41.48,   41.28,   41.03,   41.07,   41.35,   41.37,   41.67,   40.85,   40.88,   40.47,   41.46,   41.16,   41.03]
nafraf_txtwithsuffixtrie = [41.48,   41.28,   41.03,   41.07,   41.35,   41.37,   41.67,   40.85,   40.88,   40.47,   41.46,   41.16,   41.03]

nafraf_tag = [x / 100.0 for x in nafraf_tag]
nafraf_txt = [x / 100.0 for x in nafraf_txt]
nafraf_num = [x / 100.0 for x in nafraf_num]
nafraf_geo = [x / 100.0 for x in nafraf_geo]
nafraf_geoshape = [x / 100.0 for x in nafraf_geoshape]
nafraf_tagwithsuffixtrie = [x / 100.0 for x in nafraf_tagwithsuffixtrie]
nafraf_txtwithsuffixtrie = [x / 100.0 for x in nafraf_txtwithsuffixtrie]


# ------------------------------------------------------------------------------
# Plot results
def plot_results(xs, before_raz, after_raz, nafraf, field_type):
    plt.plot(xs, before_raz, ls='--', label='Old', c='r')
    plt.plot(xs, after_raz, ls='--', label='MOD-5920', c='b')
    plt.plot(xs, nafraf, label='MOD-5920 MOD-5866 MOD-5977', c='b')
    axes = plt.gca()
    axes.set_ylim([0, 1])
    axes.yaxis.set_major_formatter(mpl.ticker.StrMethodFormatter('{x:.0%}'))
    axes.xaxis.set_major_formatter(mpl.ticker.StrMethodFormatter('{x:,.0f}'))
    plt.title('Memory Reporting Improvement - ' + field_type)
    plt.xlabel('Number of documents')
    plt.ylabel('Normalized deviation')
    plt.legend()
    plt.grid()
    plt.savefig(field_type + '.pdf')
    plt.close()


plot_results(xs, before_raz_tag, after_raz_tag, nafraf_tag, 'TAG')
plot_results(xs, before_raz_txt, after_raz_txt, nafraf_txt, 'TEXT')
plot_results(xs, before_raz_num, after_raz_num, nafraf_num, 'NUMERIC')
plot_results(xs, before_raz_geo, after_raz_geo, nafraf_geo, 'GEO')
plot_results(xs, before_raz_geoshape, after_raz_geoshape, nafraf_geoshape, 'GEOSHAPE')
plot_results(xs, before_raz_tagwithsuffixtrie, after_raz_tagwithsuffixtrie, nafraf_tagwithsuffixtrie, 'TAG_WITH_SUFFIX_TRIE')
plot_results(xs, before_raz_txtwithsuffixtrie, after_raz_txtwithsuffixtrie, nafraf_txtwithsuffixtrie, 'TEXT_WITH_SUFFIX_TRIE')


# ------------------------------------------------------------------------------
# Another one, with the `rss` memory instead of the `used`.

# xs = [100, 400, 800, 1500, 5000]

# ys_tag = []     # Percentage (accuracy)
# ys_text = []    # Percentage (accuracy)

# plt.scatter(xs, ys_tag, label='Tagging')
# plt.scatter(xs, ys_text, label='Text')
# plt.title('Memory Improvement')
# plt.xlabel('Number of documents')
# plt.ylabel('Normalized accuracy')
# plt.show()
