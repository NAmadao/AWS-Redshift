import time
import json
import boto3
import configparser
from utility_functions import *

config= configparser.ConfigParser()
config.read_file(open('dwh_creation.cfg'))

# Load db configuration and credentials into dictionaries
credentials_dict, dwh_params_dict, paths_dict = get_param_dicts(config)

DWH_CLUSTER_IDENTIFIER = dwh_params_dict.get('cluster_id')

try:
    # Create IAM role with S3 access
    roleArn = create_iam_role(credentials_dict, dwh_params_dict)
        
# Error if Key or Secret were incorrect
except Exception as e:
    print(e)

try:
    # Initiate creation of redshift cluster
    redshift = create_redshift_cluster(credentials_dict, dwh_params_dict, roleArn)
    
except Exception as e:
    print(e)
    
else:
    # Check if cluster has been created. This takes 5-10minutes
    print('Creating Cluster...')
    Props = redshift.describe_clusters(
        ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]
    while Props['ClusterStatus'] != 'available':
        time.sleep(3)
        Props = redshift.describe_clusters(
            ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]
    
    DWH_ENDPOINT=Props['Endpoint']['Address']
    Vpc_Id=Props['VpcId']
    
    # Write cluster parameters to dwh.cfg
    write_to_dwhcfg(DWH_ENDPOINT, dwh_params_dict, roleArn, paths_dict)

    try:
        # Open TCP port of the cluster
        open_Ports(credentials_dict, dwh_params_dict, Vpc_Id)
    except:
        pass

    print("Redshift Cluster has been created. \
        Cluster parameters have been dumped to dwh.cfg")