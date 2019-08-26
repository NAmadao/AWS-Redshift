import time
import json
import boto3
import configparser

def get_param_dicts(config):
    """
    Load configuration parameters from dwh_creation.cfg into dictionaries
    
    Args:
        config - configparser object
    Returns:
        credentials_dict - dictionary contains iam role key and secret
        dwh_params_dict - dictionary contains redshift configuration params
        paths_dict - dictionary contains s3 bucket paths
    """
    # Read parameters from creation config file
    KEY=config.get('AWS','KEY')
    SECRET=config.get('AWS','SECRET')

    DWH_CLUSTER_TYPE=config.get('DWH','DWH_CLUSTER_TYPE')
    DWH_NODE_TYPE=config.get('DWH','DWH_NODE_TYPE')
    DWH_NUM_NODES=config.get('DWH','DWH_NUM_NODES')
    DWH_DB=config.get('DWH','DWH_DB')
    DWH_IAM_ROLENAME=config.get('DWH','DWH_IAM_ROLENAME')
    DWH_CLUSTER_IDENTIFIER=config.get('DWH','DWH_CLUSTER_IDENTIFIER')
    DWH_DB_USER=config.get('DWH','DWH_DB_USER')
    DWH_DB_PASSWORD=config.get('DWH','DWH_DB_PASSWORD')
    DWH_PORT=config.get('DWH','DWH_PORT')

    LOG_DATA = 's3://udacity-dend/log_data'
    LOG_JSONPATH = 's3://udacity-dend/log_json_path.json'
    SONG_DATA = 's3://udacity-dend/song_data'

    credentials_dict = {'key':KEY,'secret':SECRET}
    dwh_params_dict = {'cluster_type':DWH_CLUSTER_TYPE, 'node_type':DWH_NODE_TYPE,
    'num_nodes':DWH_NUM_NODES, 'db_name':DWH_DB, 'iam_role':DWH_IAM_ROLENAME,
    'cluster_id':DWH_CLUSTER_IDENTIFIER, 'db_user':DWH_DB_USER,
    'db_pass':DWH_DB_PASSWORD, 'port':DWH_PORT
    }
    paths_dict = {'log_data':LOG_DATA, 'log_jsonpath':LOG_JSONPATH,
    'song_data':SONG_DATA}

    return credentials_dict, dwh_params_dict, paths_dict 

def create_iam_role(credentials_dict, dwh_params_dict):
    """
    Create IAM role and attach Adminstrator policy
    
    Args:
        credentials_dict - dictionary contains iam role key and secret
        dwh_params_dict - dictionary contains redshift configuration params
    Returns:
        roleArn - IAM role ARN
    """
    KEY = credentials_dict.get('key')
    SECRET = credentials_dict.get('secret')
    DWH_IAM_ROLENAME = dwh_params_dict.get('iam_role')
    Policy = "arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess"

    # Create IAM role to allow Redshift to access S3
    iam = boto3.client(
        'iam', region_name="us-west-2",
        aws_access_key_id=KEY,
        aws_secret_access_key=SECRET
    )

    dwhRole=iam.create_role(
        Path='/',
        RoleName=DWH_IAM_ROLENAME,
        Description="Allows Redshift cluster to call AWS services",
        AssumeRolePolicyDocument=json.dumps(
            {'Statement':[{'Action':'sts:AssumeRole',
                           'Effect':'Allow',
                           'Principal':{'Service':'redshift.amazonaws.com'}}],
             'Version':'2012-10-17'})
    )

    iam.attach_role_policy(
        RoleName=DWH_IAM_ROLENAME,
        PolicyArn=Policy)['ResponseMetadata']['HTTPStatusCode']
    
    # S3 access ARN
    roleArn = iam.get_role(RoleName=DWH_IAM_ROLENAME)['Role']['Arn'] 
    
    return roleArn

def create_redshift_cluster(credentials_dict, dwh_params_dict, roleArn):
    """
    Create redshift cluster
    
    Args:
        credentials_dict - dictionary contains iam role key and secret
        dwh_params_dict - dictionary contains redshift configuration params
        roleArn - IAM role ARN
    Returns:
        redshift - redshift object
    """
    KEY = credentials_dict.get('key')
    SECRET = credentials_dict.get('secret')

    DWH_CLUSTER_TYPE = dwh_params_dict.get('cluster_type')
    DWH_NODE_TYPE = dwh_params_dict.get('node_type')
    DWH_NUM_NODES = dwh_params_dict.get('num_nodes')
    DWH_DB = dwh_params_dict.get('db_name')
    DWH_CLUSTER_IDENTIFIER = dwh_params_dict.get('cluster_id')
    DWH_DB_USER = dwh_params_dict.get('db_user')
    DWH_DB_PASSWORD = dwh_params_dict.get('db_pass')

    redshift = boto3.client(
        'redshift', region_name='us-west-2',
        aws_access_key_id=KEY,
        aws_secret_access_key=SECRET
        )
    
    response = redshift.create_cluster(
        ClusterType=DWH_CLUSTER_TYPE,
        NodeType=DWH_NODE_TYPE,
        NumberOfNodes=int(DWH_NUM_NODES),
        DBName=DWH_DB,
        ClusterIdentifier=DWH_CLUSTER_IDENTIFIER,
        MasterUsername=DWH_DB_USER,
        MasterUserPassword=DWH_DB_PASSWORD,
        IamRoles=[roleArn]
        )
    
    return redshift

def write_to_dwhcfg(DWH_ENDPOINT, dwh_params_dict, roleArn, paths_dict):
    """
    Write redshift parameters to dwh.cfg file
    
    Args:
        DWH_ENDPOINT - redshift db host
        dwh_params_dict - dictionary contains redshift configuration params
        roleArn - IAM role ARN
        paths_dict - dictionary contains s3 bucket paths
        
    Returns:
        None
    """
    DWH_DB = dwh_params_dict.get('db_name')
    DWH_DB_USER = dwh_params_dict.get('db_user')
    DWH_DB_PASSWORD = dwh_params_dict.get('db_pass')
    DWH_PORT = dwh_params_dict.get('port')

    LOG_DATA = paths_dict.get('log_data')
    LOG_JSONPATH = paths_dict.get('log_jsonpath')
    SONG_DATA = paths_dict.get('song_data')

    dwhcfg_string = "[CLUSTER]\nHOST={}\nDB_NAME={}\nDB_USER={}\
        \nDB_PASSWORD={}\nDB_PORT={}"
    dwhcfg_final = dwhcfg_string.format(DWH_ENDPOINT, DWH_DB, DWH_DB_USER, 
    DWH_DB_PASSWORD, DWH_PORT)
    
    dwhcfg_string2 = "\n\n[IAM_ROLE]\nARN='{}'"
    dwhcfg_final2 = dwhcfg_string2.format(roleArn)

    dwhcfg_string3 = "\n\n[S3]\nLOG_DATA='{}'\nLOG_JSONPATH='{}'\nSONG_DATA='{}'"
    dwhcfg_final3 = dwhcfg_string3.format(LOG_DATA,LOG_JSONPATH,SONG_DATA)

    f=open('dwh.cfg','w')
    f.write(dwhcfg_final)
    f.write(dwhcfg_final2)
    f.write(dwhcfg_final3)
    f.close()
    
def open_Ports(credentials_dict, dwh_params_dict, Vpc_Id):
    """
    Open TCP ports on redshift cluster
    
    Args:
        credentials_dict - dictionary contains iam role key and secret
        dwh_params_dict - dictionary contains redshift configuration params
        Vpc_Id - redshift cluster VPC Id
    Returns:
        None
    """
    KEY = credentials_dict.get('key')
    SECRET = credentials_dict.get('secret')
    DWH_PORT = dwh_params_dict.get('port')

    ec2 = boto3.resource('ec2',
    region_name="us-west-2",
    aws_access_key_id=KEY,
    aws_secret_access_key=SECRET)
    
    vpc=ec2.vpc(id=Vpc_Id)
    defaultSg=list(vpc.security_groups.all())[0]
    
    defaultSg.authorize_ingress(
        GroupName=defaultSg.group_name,
        CidrIp='0.0.0.0/0',
        IpProtocol='TCP',
        FromPort=int(DWH_PORT),
        ToPort=int(DWH_PORT)
    )
    
def delete_redshift_cluster(KEY, SECRET, DWH_CLUSTER_IDENTIFIER):
    """
    Delete redshift cluster
    
    Args:
        KEY - AWS Access key ID
        SECRET - AWS Secret Access key
        DWH_CLUSTER_IDENTIFIER - AWS redshift cluster identifier
    Returns:
        redshift - redshift db object
    """
    redshift = boto3.client('redshift', region_name='us-west-2',
    aws_access_key_id=KEY,
    aws_secret_access_key=SECRET)

    response=redshift.delete_cluster(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER,
    SkipFinalClusterSnapshot=True)

    return redshift

def delete_iam_role(KEY, SECRET, DWH_IAM_ROLENAME):
    """
    Delete redshift cluster
    
    Args:
        KEY - AWS Access key ID
        SECRET - AWS Secret Access key
        DWH_IAM_ROLENAME - IAM rolename
    Returns:
        None
    """
    Policy = "arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess"

    iam = boto3.client(
        'iam', region_name="us-west-2",
        aws_access_key_id=KEY,
        aws_secret_access_key=SECRET)

    iam.detach_role_policy(RoleName=DWH_IAM_ROLENAME,
    PolicyArn=Policy)

    iam.delete_role(RoleName=DWH_IAM_ROLENAME)
