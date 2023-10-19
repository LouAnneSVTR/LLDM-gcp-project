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
    #run_pig_job(region, cluster_name)
    run_spark_job(region, cluster_name)



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
    ## copy data
    command = "gsutil cp small_page_links.nt gs://myown_bucket/"
    subprocess.run ( [command] , shell=True ) 
    #os.system("gsutil cp small_page_links.nt gs://myown_bucket/")

    ## copy pig code
    command = "gsutil cp pagerank-notype.py gs://myown_bucket/"
    subprocess.run ( [command] , shell=True ) 
    #os.system("gsutil cp pagerank-notype.py gs://myown_bucket/")

    ## Clean out directory
    command = "gsutil rm -rf gs://myown_bucket/out"
    subprocess.run ( [command] , shell=True ) 
    #os.system("gsutil rm -rf gs://myown_bucket/out")



def run_pig_job(region, cluster_name):
    command = f"gcloud dataproc jobs submit pig --region {region} --cluster {cluster_name} -f gs://lsdm_data_svtr/dataproc.py"
    
    start = time.time()
    subprocess.run ( [command] , shell=True )
    end = time.time()

    timer = start - end

    print(f"Job finished successfully: ")


def run_spark_job(region, cluster_name):
    command = "gcloud dataproc jobs submit pig --region {region} --cluster {cluster_name} -f gs://lsdm_data_svtr/pagerank.py"

    start = time.time()
    subprocess.run ( [command] , shell=True )
    end = time.time()

    timer = start - end

    print(f"Job finished successfully: ")



#def collect_data(project_id, region, cluster_name):



if __name__ == "__main__":
    if len(sys.argv) < 4:
        project_id = "lsdm-40181"
        region = "europe-central2"
        cluster_name = "clusterlsdm"
    else:
        project_id = sys.argv[1]
        region = sys.argv[2]
        cluster_name = sys.argv[3]

    create_cluster(project_id, region, cluster_name)