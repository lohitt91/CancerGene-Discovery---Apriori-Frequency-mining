from itertools import combinations
from collections import defaultdict
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
parser.add_argument("--output", help="output directory name.", default="Task2")
parser.add_argument("--num_iter", type=int, help="indicate the number of max-itemset", default=2)
parser.add_argument("--min_sup", type=float, help="indicate the minimum support as a percentage",default=0.3)

args = parser.parse_args()

def geo_extract(record):
    patient_id, gene_id, exp_val = record.strip().split(",")
    try:
        return (patient_id,(gene_id, exp_val))
    except:
        return []

def gene_filter(record):
    patient_id, values = record
    gene_id, exp_val = values
    try:
        if(int(float(exp_val))>1250000):
            return True
        else:
            return False
    except ValueError:
        return False

def patient_filter(record):
    patient_id, disease = record
    try:
        cancer_list=["breast-cancer","prostate-cancer", "pancreatic-cancer", "leukemia", "lymphoma"]
        if disease in cancer_list:
            return True
        else:
            return False
    except:
        return False

def pair_patient_to_disease(record):
  try:
    patient_id, age, gender, post_code, disease_list, drug_response = record.strip().split(",")
    diseases = disease_list.strip().split(" ")
    return [(patient_id, disease) for disease in diseases]
  except:
    return []

def sum_patient_count(reduced_count, current_count):
  return reduced_count+current_count

def min_support_filter(record):
    gene_id, cnt, ms= record
    try:
        if cnt>ms:
            return True
        else:
            return False
    except:
        return False

def generate_item_combs(items,n):
    return list(combinations(items,n))

def returnItems(itemSet, transactionList):
    localSet = defaultdict(int)
    for item in itemSet:
        if set(item).issubset(transactionList):
            localSet[item] += 1
    return [(item, localSet[item]) for item in localSet]

if __name__ == "__main__":
    sc = SparkContext(appName='Task2')

    #geo = sc.textFile("/Users/Sra1Phani/Documents/Cloud_computing_input_files/small/GEO.txt")
    print(args.input+'/GEO.txt')
    #geo = sc.textFile("/share/genedata/small/GEO.txt")
    geo = sc.textFile(args.input+'/GEO.txt')
    header = geo.first()
    geo = geo.filter(lambda row: row != header)
    geo = geo.map(lambda row: geo_extract(row)).filter(gene_filter)

    print(args.input + '/PatientMetaData.txt')
    patient = sc.textFile(args.input+'/PatientMetaData.txt')
    # patient = sc.textFile("/share/genedata/small/PatientMetaData.txt")
    header = patient.first()
    patient = patient.filter(lambda row: row != header)
    patient = patient.flatMap(pair_patient_to_disease).filter(patient_filter)

    support_pct = args.min_sup
    patient_cnt = patient.keys().distinct().count()
    min_support = round(patient_cnt * support_pct)

    joined_set = geo.join(patient).map(lambda row: (row[0], row[1][0][0], row[1][0][1]))

    transaction_set = joined_set.map(lambda s: (s[0], s[1])).groupByKey().map(lambda s: (s[0], set(s[1])))

    items = joined_set.map(lambda row: (row[1], row[2])).keys().distinct().collect()
    #items = set(items)

    final_set = sc.emptyRDD()

    for i in range(args.num_iter):
        itemset_combs = generate_item_combs(items,i+1)
        retitems = transaction_set.values().flatMap(lambda row: returnItems(itemset_combs, row)).reduceByKey(sum_patient_count) \
            .map(lambda row: (row[0], row[1], min_support)).filter(min_support_filter).map(lambda row: (row[1], row[0]))

        final_set = final_set.union(retitems)


    final_set = final_set.sortBy(lambda x: x[0],ascending=False)
    final_set.map(lambda x: str(x[0]) + '\t' + "\t".join(y for y in x[1])).coalesce(1).saveAsTextFile(args.output)
