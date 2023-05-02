from pyspark import SparkContext
import itertools as it
import sys
import functools
def fmtToTricycle(fmtd):
    out_tricycle = []
    for elem in fmtd[1]:
        if elem != 'exists':
            this_cycle = (elem[1],) + fmtd[0]
            out_tricycle.append(this_cycle)
    return out_tricycle

def csvToNodes(in_rdd):
    out_rdd = in_rdd.map(lambda x: tuple(x.split(',')))
    return out_rdd

def nodesToTricycles(nodes_rdd):
    adj_rdd = nodes_rdd.groupByKey()\
                       .mapValues(set)\
                       .mapValues(sorted)\
                       .map(lambda x: 
                               (x[0], list(filter(lambda y: y > x[0], x[1]))))

    exist_rdd = adj_rdd.flatMapValues(lambda x: x)\
                       .map(lambda x: (x,'exists'))

    pending_rdd = adj_rdd.flatMapValues(lambda x: it.combinations(x,2))\
                         .map(lambda x: (x[1],('pending', x[0])))

    fmtd_rdd = exist_rdd.union(pending_rdd)
    out_rdd = fmtd_rdd.groupByKey()\
                      .mapValues(list)\
                      .filter(lambda x: len(x[1])> 1)\
                      .flatMap(fmtToTricycle)
    return out_rdd
def getTC(csv):
    out = nodesToTricycles(csvToNodes(csv))
    return out.collect()

if __name__ == '__main__':
    if len(sys.argv) > 1:
        sc = SparkContext()
        filenames = sys.argv[1:]
        rdd = sc.textFile(",".join(filenames))
        print('Tricycles:\n', getTC(rdd))
    else:
        print(f'append coma separated filenames to {sys.argv[0]} call\n')
