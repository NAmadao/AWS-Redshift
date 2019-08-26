import boto3
import time
import configparser
from utility_functions import delete_redshift_cluster, delete_iam_role

config= configparser.ConfigParser()
config.read_file(open('dwh_creation.cfg'))

# Load db parameters from config file
KEY=config.get('AWS','KEY')
SECRET=config.get('AWS','SECRET')
DWH_CLUSTER_IDENTIFIER=config.get('DWH', 'DWH_CLUSTER_IDENTIFIER')
DWH_IAM_ROLENAME=config.get('DWH','DWH_IAM_ROLENAME')

# Initiate cluster deletion
redshift = delete_redshift_cluster(KEY, SECRET, DWH_CLUSTER_IDENTIFIER)

# Check if cluster deletion completed. This takes 10-15mins
print('Deleting Cluster...')
while True:
    try:
        Props = redshift.describe_clusters(
            ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]
        time.sleep(3)
    except:
        break

# Delete IAM role.
delete_iam_role(KEY, SECRET, DWH_IAM_ROLENAME)

print("Deleted IAM role and Cluster!")
