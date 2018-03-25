from itertools import combinations
import argparse

# Path for spark source folder
#os.environ['SPARK_HOME']="/Users/Lohitt/spark/spark-2.1.1-bin-hadoop2.7"

# Need to Explicitly point to python3 if you are using Python 3.x
#os.environ['PYSPARK_PYTHON']="/Library/Frameworks/Python.framework/Versions/3.5/bin/python3.5"

#You might need to enter your local IP
#os.environ['SPARK_LOCAL_IP']="192.168.2.138"

#Path for pyspark and py4j
#sys.path.append("/Users/Lohitt/spark/spark-2.1.1-bin-hadoop2.7/python")
#sys.path.append("/Users/Lohitt/spark/spark-2.1.1-bin-hadoop2.7/python/lib/py4j-0.10.4-src.zip")

try:
    from pyspark import SparkContext
    from pyspark import SparkConf

    print("Spark Import success")
except ImportError as e:
    print ("Cannot Import")

parser = argparse.ArgumentParser()
parser.add_argument("input", help="input directory path without filename. eg., /share/genedata/large")
parser.add_argument("--output", help="output directory name.", default="Task3")
parser.add_argument("--min_conf", type=float, help="indicate the confidence threshold. eg., 0.6",default=0.6)

args = parser.parse_args()

def map_to_pair(record):
    try:
        cols = record.strip().split('\t')
        support_cnt = cols[0]
        gene_seqs = tuple(cols[1:])
        return (gene_seqs, support_cnt)
    except:
        return []

def conf_calc_map(gene_comb, record):
    try:
        gene_seq, support = record[0], int(record[1])
        subsets = list()
        for i in range(1,len(gene_seq)):
            subsets = subsets + list(combinations(gene_seq, i))
        return [(gene_seq, str(set)+"-"+str(gene_seq) ,round(float(support)/float(gene_comb.get(set)),2)) for set in subsets]
    except:
        return []

if __name__ == "__main__":
    sc = SparkContext(appName='Assignment3_task3')

    task2_op = sc.textFile(args.input+'/part-00000')

    task2_op = task2_op.map(map_to_pair)

    gene_combinations = dict(task2_op.collect())

    rules = dict()

    task2_op = task2_op.filter(lambda s: len(s[0])>1).flatMap(lambda row: conf_calc_map(gene_combinations, row))

    task2_op = task2_op.filter(lambda s: s[2]>=args.min_conf).sortBy(lambda s:s[2], ascending=False)

    task2_op.map(lambda s: str(s[0]) + '\t' + str(s[1]) + '\t' + str(s[2])).coalesce(1).saveAsTextFile(args.output)











