import json
import boto3
import base64
import logging
import traceback
#from variables import *
logger = logging.getLogger()
logger.addHandler(logging.StreamHandler()) # Writes to console
logger.setLevel(logging.DEBUG)
#Set this True if you want to decable the boto logs 
decablebotologs = True


if decablebotologs == True :
    logging.getLogger('boto3').setLevel(logging.CRITICAL)
    logging.getLogger('botocore').setLevel(logging.CRITICAL)
    logging.getLogger('s3transfer').setLevel(logging.CRITICAL)
    logging.getLogger('urllib3').setLevel(logging.CRITICAL)
    
# Get Security Group Ids
def get_security_group_ids(vSGNames, ec2_east_client):
    logger.debug("inside get_security_group_ids----------")
    #logger.debug("inside get_security_group_ids"+str(vSGNames))
    vSGIdsList = []
    for vsg in vSGNames:
        sgresponse = ec2_east_client.describe_security_groups(
            Filters=[
            {
                'Name': 'group-name',
                'Values': [
                    vsg,
                ]
            }
            ]
           ) 
        logger.debug("inside get_security_group_ids"+vsg)
        for sg in sgresponse["SecurityGroups"]    :
            vSGIdsList.append(sg["GroupId"])
    
    return vSGIdsList

# Get Subnet Ids from Subnet Names
def get_subnetids(vSubnetName, ec2_east_client):
    #logger.debug("inside get_subnetids"+vSubnetName)
    snresponse = ec2_east_client.describe_subnets(
        Filters=[
            {
                'Name': 'tag:Name',
                'Values': [
                    vSubnetName,
                ]
            },
        ]
        )
    vSubnet_Id = ""
    logger.debug(snresponse)
    vSubnet_Id = snresponse["Subnets"][0]["SubnetId"]
    
    return vSubnet_Id


# Convert base64 encoded text to Normal Text
def get_text_from_encoded(vUserData):
    logger.debug("inside get_text_from_encoded")
    base64_data = vUserData
    base64_bytes = base64_data.encode('ascii')
    data_bytes = base64.b64decode(base64_bytes)
    vText = data_bytes.decode('ascii')
    
    return vText

# Launch EC2 Instances
def launch_ec2_instances(inst, ec2_east_client):
    logger.debug("inside launch_ec2_instances")
    try:
        vLaunchResult = "Failed"
        vResourceData = inst.get("resource_data")
        vInstanceID = inst.get("resource_id")
        vInstanceName = vResourceData.get("InstanceName")
        vImage_Id = get_latest_backup_image_id(vInstanceID, ec2_east_client,  vInstanceName)
        if len(vImage_Id) == 0:
            vLaunchResult = "No Instance Backup"
            print("No Backup: ", vInstanceName)
            return vLaunchResult
        vInstanceType = vResourceData.get("InstanceType")
        vKeyName = vResourceData.get("KeyName")
        
        # if vResourceData.get("KeyName") == None:
        #     vLaunchResult = "No KeyName"
        #     return vLaunchResult
            
        vIamInstanceProfile = vResourceData.get("IamInstanceProfile")
        vUserData = vResourceData.get("UserData")
        #vUserData = vUserData_Manual
        # get_text_from_encoded(vUserData)
        # print(vUserData)
        vPrivateIpAddress = vResourceData.get("PrivateIpAddress")
        vSubnetId = get_subnetids(vResourceData["SubnetName"], ec2_east_client)
        vSecurityGroupsIds = get_security_group_ids(vResourceData["SecurityGroups"], ec2_east_client)
    
        vTags = []
        for tg in vResourceData.get("Tags"):
            if (tg["Key"])[0:3] != "aws":
                vTags.append(tg.copy())
        # print(vTags)
        # Launch Instance based on latest ami from backup
        if vKeyName == None:
            runInstanceResponse = ec2_east_client.run_instances(
                ImageId=vImage_Id,
                InstanceType = vInstanceType,
                # KeyName = vKeyName,
                MaxCount = 1,
                MinCount = 1,
                SecurityGroupIds = vSecurityGroupsIds,
                IamInstanceProfile = {
                    "Arn": vIamInstanceProfile
                    },
                SubnetId = vSubnetId,
                # UserData=vUserData,
                PrivateIpAddress = vPrivateIpAddress,
                TagSpecifications = [
                    {
                        'ResourceType': 'instance',
                        'Tags': vTags
                    }
                    ],
                DryRun=False
                )
            vLaunchResult = "Launched"
            return vLaunchResult
        elif vKeyName != None:
            runInstanceResponse = ec2_east_client.run_instances(
                ImageId=vImage_Id,
                InstanceType = vInstanceType,
                KeyName = vKeyName,
                MaxCount = 1,
                MinCount = 1,
                SecurityGroupIds = vSecurityGroupsIds,
                IamInstanceProfile = {
                    "Arn": vIamInstanceProfile
                    },
                SubnetId = vSubnetId,
                # UserData=vUserData,
                PrivateIpAddress = vPrivateIpAddress,
                TagSpecifications = [
                    {
                        'ResourceType': 'instance',
                        'Tags': vTags
                    }
                    ],
                DryRun=False
                )
            
            vLaunchResult = "Launched"
            return vLaunchResult
    except Exception as e:
        print(str(e))
        return vLaunchResult


# Get Latest EC2 VMs Backup Images
def get_latest_backup_image_id(vInstanceID, ec2_east_client, vInstanceName):
    logger.debug("inside get_latest_backup_image_id")
    amiresponse = ec2_east_client.describe_images(
        Owners=[
        "self",
        ],
        Filters=[
            {
                'Name': 'tag:Name',
                'Values': [
                    vInstanceName,
                ]
            },
        ]
        )
        
    vimg = None
    vImageId = ""
    vCreationDate = ""
    for img in amiresponse["Images"]:
        if img.get("State") != "available":
            continue
        if vimg == None:
            vimg = img
            vImageId = img.get("ImageId")
            vCreationDate = img.get("CreationDate")
        if vInstanceID  in str(img.get("ImageLocation")):
            if img.get("CreationDate") > vimg.get("CreationDate"):
                vImageId = img.get("ImageId")
                vCreationDate = img.get("CreationDate")

    return vImageId
    

# Get Latest RDS Snapshot ARN
def get_rds_latest_snapshot_arn(vDBId, rds_east_client):
    logger.debug("inside get_rds_latest_snapshot_arn")
    response = rds_east_client.describe_db_snapshots(
    DBInstanceIdentifier=vDBId
    )
    
    vRDS_Snapshot_Info = response["DBSnapshots"]
    vDBSnapshotARN = ""
    vDBSnapCreationDate = ""
    for rdssnap in vRDS_Snapshot_Info:
        if rdssnap["Status"] == "available":
            if vDBSnapshotARN == "":
                vDBSnapshotARN = rdssnap["DBSnapshotArn"]
                vDBSnapCreationDate = rdssnap["SnapshotCreateTime"]
            if rdssnap["SnapshotCreateTime"] > vDBSnapCreationDate:
                vDBSnapshotARN = rdssnap["DBSnapshotArn"]
                vDBSnapCreationDate = rdssnap["SnapshotCreateTime"]
    # print(vDBSnapCreationDate, vDBSnapshotARN)
    return vDBSnapshotARN


# Get Group IDs from East Region from Name List
def get_vpc_sg_ids(vNameList, ec2_east_client):
    logger.debug("inside get_vpc_sg_ids")
    vGroupIdList = []
    response = ec2_east_client.describe_security_groups(
        Filters=[
            {
                'Name': 'group-name',
                'Values': vNameList
            }
            ]
        )
    for sg in response["SecurityGroups"]:
        vGroupIdList.append(sg["GroupId"])
    return vGroupIdList
    

def lambda_handler(event, context):
    logger.info('## Started ####')
    # Create EC2 Client
    ec2_east_client = boto3.client("ec2", region_name="us-gov-east-1")
    
    # Create RDS Client in East Region
    rds_east_client = boto3.client("rds", region_name="us-gov-east-1")
    
    # create DynamoDB Client
    dynamodb_client = boto3.resource("dynamodb", region_name="us-gov-east-1")
    
    #Feth Information from Dynamodb
    resources_info = dynamodb_client.Table("Production_Resource_Info").scan()
    
    # Get List of Ec2 & RDS
    vEc2LaunchResult = []
    vInstanceID = ""
    # TEC2 = 0
    # TRDS = 0
    for inst in resources_info["Items"]:
        # For EC2  moved code to diffrent function
        #if inst.get("resource_type") == "EC2":
        #    # pass
        #    launchresult = {}
        #    vInstanceID = inst.get("resource_id")
        #    vInstanceName = inst.get("resource_data").get("InstanceName")
        #    launch_response = launch_ec2_instances(inst, ec2_east_client)
            
        #    launchresult["Instance Id"] = vInstanceID
        #    launchresult["Instnace Name"] = vInstanceName
        #    launchresult["Launch Result"] = launch_response
            
        #    vEc2LaunchResult.append(launchresult.copy())
            # TEC2 = 1
            
        if inst.get("resource_type") == "RDS":
            # Launch DB Cluster
            # crdsResponse = rds_east_client.restore_db_cluster_from_snapshot(
            #     DBClusterIdentifier="feds-db",
            #     SnapshotIdentifier=
            #     )
            
            # pass
            
            try:
                RDS_Data = inst.get("resource_data")
                logger.info("Got RDS data:{}".format(RDS_Data))
                vDBId = RDS_Data["DBInstanceIdentifier"]
                vDBName = ""
                if RDS_Data.get("DBName") is not None:
                    vDBName = RDS_Data["DBName"]
                if RDS_Data.get("DBClusterIdentifier") is not None:
                    # Call DB Cluster Instance Restore 
                    continue
                if vDBName != None:
                    rdsresponse = rds_east_client.restore_db_instance_from_db_snapshot(
                        DBInstanceIdentifier=RDS_Data["DBInstanceIdentifier"],
                        DBSnapshotIdentifier=get_rds_latest_snapshot_arn(vDBId, rds_east_client),
                        DBInstanceClass=RDS_Data["DBInstanceClass"],
                        DBName=vDBName,
                        DBParameterGroupName=RDS_Data["DBParameterGroupName"],
                        DBSubnetGroupName=RDS_Data["DBSubnetGroupName"],
                        Engine=RDS_Data["Engine"],
                        MultiAZ=RDS_Data["MultiAZ"],
                        PubliclyAccessible=RDS_Data["PubliclyAccessible"],
                        EnableCustomerOwnedIp=RDS_Data["EnableCustomerOwnedIp"],
                        VpcSecurityGroupIds=get_vpc_sg_ids(RDS_Data["VpcSecurityGroups"], ec2_east_client)
                        )
                    #print("DB Instance created: ", rdsresponse["DBInstance"]["DBName"])
                    logger.debug("--- DB Instance created:"+rdsresponse["DBInstance"]["DBName"])
                    # TRDS = 1
                else:
                    rdsresponse = rds_east_client.restore_db_instance_from_db_snapshot(
                        DBInstanceIdentifier=RDS_Data["DBInstanceIdentifier"],
                        DBSnapshotIdentifier=get_rds_latest_snapshot_arn(vDBId, rds_east_client),
                        DBInstanceClass=RDS_Data["DBInstanceClass"],
                        DBParameterGroupName=RDS_Data["DBParameterGroupName"],
                        DBSubnetGroupName=RDS_Data["DBSubnetGroupName"],
                        Engine=RDS_Data["Engine"],
                        MultiAZ=RDS_Data["MultiAZ"],
                        PubliclyAccessible=RDS_Data["PubliclyAccessible"],
                        EnableCustomerOwnedIp=RDS_Data["EnableCustomerOwnedIp"],
                        VpcSecurityGroupIds=get_vpc_sg_ids(RDS_Data["VpcSecurityGroups"], ec2_east_client),DryRun=False
                        )
                    #print("DB Instance created: ", rdsresponse["DBInstance"]["DBName"])
                    logger.debug("--- DB Instance created:"+rdsresponse["DBInstance"]["DBName"])
                    # TRDS = 1
            except Exception as e:
                print("Error: ", str(e))
                logger.exception("message")
                logger.error("error at DB Instance create "+str(e))
        # vEc2LaunchResult
    vFinalMsg = "Done"
    return {
        'statusCode': 200,
        'body': vFinalMsg
    }
    logger.info('## End ####')

