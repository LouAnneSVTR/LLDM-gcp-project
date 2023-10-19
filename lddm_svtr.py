#This fil aims to create n cluster with a given node in python

import subprocess
import sys
import time
import google.cloud.dataproc_v1
from google.cloud import dataproc_v1 as dataproc

def create_cluster(project_id, region, cluster_name):
    # Create the cluster client.
    cluster_client = dataproc.ClusterControllerClient(
        client_options={"api_endpoint": f"{region}-dataproc.googleapis.com:443"}
    )

    # Create the cluster config.
    cluster = {
        "project_id": project_id,
        "cluster_name": cluster_name,
        "config": {
            "master_config": {"num_instances": 1, "machine_type_uri": "n1-standard-4"},
            "worker_config": {"num_instances": 2, "machine_type_uri": "n1-standard-4"},
        },
    }

    # Create the cluster.
    operation = cluster_client.create_cluster(
        request={"project_id": project_id, "region": region, "cluster": cluster}
    )
    result = operation.result()

    print(f"Cluster created successfully: {result.cluster_name}")


    clean_data(project_id, region, cluster_name)
    pig = run_pig_job(region, cluster_name)
    spark = run_spark_job(region, cluster_name)
    write_table_to_txt(pig, spark)
    

    # Delete the cluster once the job has terminated.
    operation = cluster_client.delete_cluster(
        request={
            "project_id": project_id,
            "region": region,
            "cluster_name": cluster_name,
        }
    )
    operation.result()

    print(f"Cluster {cluster_name} successfully deleted.")



def clean_data(project_id, region, cluster_name):
    ## copy spark code
    command = "gsutil cp dataproc.py gs://lsdm_data_svtr/"
    subprocess.run ( [command] , shell=True ) 

    ## copy spark code
    command = "gsutil cp pagerank.py gs://lsdm_data_svtr/"
    subprocess.run ( [command] , shell=True ) 

    ## Clean out directory
    command = "gsutil rm -rf gs://lsdm_data_svtr/out"
    subprocess.run ( [command] , shell=True ) 


def run_pig_job(region, cluster_name):
    command = f"gcloud dataproc jobs submit pig --region {region} --cluster {cluster_name} -f gs://lsdm_data_svtr/dataproc.py"
    
    start = time.time()
    subprocess.run ( [command] , shell=True )
    end = time.time()

    print(f"Job Pig finished successfully with time : {end - start}")

    return end - start


def run_spark_job(region, cluster_name):
    command = f"gcloud dataproc jobs submit pyspark --region {region} --cluster {cluster_name} gs://lsdm_data_svtr/pagerank.py  -- gs://public_lddm_data/small_page_links.nt 3"

    start = time.time()
    subprocess.run ( [command] , shell=True )
    end = time.time()

    print(f"Job Spark finished successfully with time : {end - start}")

    return end - start


def write_table_to_txt(pig, spark):
    # Ouvrir le fichier en mode écriture
    with open('result_data.txt', 'w') as file:
        # Écrire l'en-tête du tableau
        file.write("Pig\tSpark\tNode\n")
        file.write(str(pig) + '\t' + str(spark) + '\t' + str(2))



if __name__ == "__main__":
    if len(sys.argv) < 4:
        project_id   = "lsdm-40181"
        region       = "europe-central2"
        cluster_name = "clusterlsdm"
    else:
        project_id   = sys.argv[1]
        region       = sys.argv[2]
        cluster_name = sys.argv[3]

    create_cluster(project_id, region, cluster_name)

    #node = [2,4,6]
    #for n in node: 
        #create_cluster(project_id, region, cluster_name, n)    
    