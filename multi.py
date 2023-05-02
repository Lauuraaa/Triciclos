from pyspark import SparkContext
import itertools as it
import sys
# Toma una lista de valores separados por comas y devuelve una lista de tuplas conteniendo esos valores.
def csvPairsToTuples(lst):
    return list(map(lambda y: tuple(y.split(',')), lst))
# Toma una tupla de dos elementos y devuelve la tupla ordenada de manera que el primer elemento
# siempre sea el mayor de los dos, en caso de sean iguales, devuelve "None"
def nodeOrganize(node):
    if node[0] < node[1]:
        return node
    elif node[1] < node[0]:
        return (node[1], node[0])
    else:
        return None
# Toma la rdd donde se encuentran los triciclos y la convierte a un formato mas legible.
def formatFiledTricicle(rdd):
    out = rdd.map(lambda x: (x[0][0], (x[1][1][1],x[0][1][0],x[0][1][1])))
    return out
# De un directorio extrae los triciclos de cada uno de los archivos que contenga.
def getFromDirTC(dire):
    file_nodes_rdd = sc.wholeTextFiles(dire)\
                      .map(lambda x: (x[0].split("/")[-1], x[1]))\
                      .mapValues(lambda x: x.splitlines())\
                      .mapValues(csvPairsToTuples)\
                      .flatMap(lambda x: list(map(lambda y: (x[0], y), x[1])))\
                      .mapValues(nodeOrganize)\
                      .filter(lambda x: x[1] is not None)\
                      .map(lambda x: ((x[0], x[1][0]), x[1][1]))\
                      .groupByKey()\
                      .mapValues(set)\
                      .mapValues(sorted)
    exist_rdd = file_nodes_rdd.flatMapValues(lambda x: x)\
                              .map(lambda x: ((x[0][0],(x[0][1],x[1])), 'exists'))
    pending_rdd = file_nodes_rdd.flatMapValues(lambda x: it.combinations(x,2))\
                                .map(lambda x: ((x[0][0],x[1]), ('pending', x[0][1])))
    fmtd_rdd = exist_rdd.union(pending_rdd)
    out_rdd = fmtd_rdd.groupByKey()\
                      .mapValues(list)\
                      .filter(lambda x: len(x[1])> 1)
    fmtd_out = formatFiledTricicle(out_rdd).groupByKey().mapValues(list)
    return fmtd_out
if __name__ == '__main__':
    if len(sys.argv) > 1:
        sc = SparkContext()
        directory = sys.argv[1]
        out_put = getFromDirTC(directory).collect()
        print("If the file name doesn't show, there were no tricicles")
        for line in sorted(out_put):
            print(line)
    else:
        print(f'append directory to {sys.argv[0]} call\n')
