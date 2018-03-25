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
parser.add_argument("--output", help="output directory name.", default="Task1")

args = parser.parse_args()

#Defining GEO and PatientMetaData Objects
def geo_extract(record):
    patient_id, gene_id, exp_val = record.strip().split(",")
    try:
        return (patient_id,(gene_id, exp_val))
    except:
        return []

def pair_patient_to_disease(record):
  try:
    patient_id, age, gender, post_code, disease_list, drug_response = record.strip().split(",")
    diseases = disease_list.strip().split(" ")
    return [(patient_id, disease) for disease in diseases]
  except:
    return []

#Defining Filter criteria
def gene_filter(record):
    patient_id, values = record
    gene_id, exp_val = values
    try:
        if(gene_id=="42" and int(float(exp_val))>1250000):
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

#Defining Reduce Function to count support
def sum_patient_count(reduced_count, current_count):
  return reduced_count+current_count

#Main
if __name__ == "__main__":
    sc = SparkContext(appName='Task1')

    geoData = sc.textFile(args.input + '/GEO.txt')
    header = geoData.first()
    geoData = geoData.filter(lambda row: row != header)
    geoData = geoData.map(lambda row: geo_extract(row)).filter(gene_filter)


    patientData = sc.textFile(args.input + '/PatientMetaData.txt')
    header = patientData.first()
    patientData = patientData.filter(lambda row: row!=header)
    patientData = patientData.flatMap(pair_patient_to_disease).filter(patient_filter)
    patientData = patientData.map(lambda row: (row[0],row[1]))

#Perform a join on PatientID field and then map CancerType and Patient Count
    joined_set = geoData.join(patientData).values().map(lambda row: (row[1],1)).reduceByKey(sum_patient_count).sortBy(lambda x:(x[1],x[0]),ascending=False)

    joined_set.map(lambda x: str(x[0]) + '\t' + str(x[1]).strip('[]')).coalesce(1).saveAsTextFile(args.output)
