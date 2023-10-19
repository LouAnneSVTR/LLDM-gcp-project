#This fil aims to create n cluster with a given node in python

import sys
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

    ############# JOB #############    
    # Create the job client for Pig
    job_client = dataproc.JobControllerClient(
        client_options={"api_endpoint": f"{region}-dataproc.googleapis.com:443"}
    )

    # Create the job config.
    job = {
        "placement": {"cluster_name": cluster_name},
        "pig_job": {"main_python_file_uri": f"gs://lsdm_data_svtr/dataproc.py"},
    }

    operation = job_client.submit_job_as_operation(
        request={"project_id": project_id, "region": region, "job": job}
    )
    #response = operation.result()

    print(f"Pig Job finished successfully: ")
    ############# JOB #############
    

    #gsutil rm -rf gs://lsdm_data_svtr/out
    #gcloud dataproc jobs submit pig --region europe-west1 --cluster cluster-a35a -f gs://lsdm_data_svtr/dataproc.py


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


 
if __name__ == "__main__":
    if len(sys.argv) < 4:
        sys.exit("python create_cluster.py project_id region cluster_name")

    project_id = sys.argv[1]
    region = sys.argv[2]
    cluster_name = sys.argv[3]

    create_cluster(project_id, region, cluster_name)


# how to run PySpark job
#https://www.freecodecamp.org/news/what-is-google-dataproc/