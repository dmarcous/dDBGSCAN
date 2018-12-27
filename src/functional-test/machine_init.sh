#!/usr/bin/env bash

# Usage:
# --cluster_name
# --ec2_attributes
# --service_role
# --log_uri
# --tags

# Set defaults
CLUSTER_NAME="dmarcous-cluster"
EC2_ATTRIBUTES=""
SERVICE_ROLE=""
LOG_URI=""
TAGS=""

# Read params
echo 'Reading script params...'
#while [ $# -gt 0 ]; do
for i in "$@"
do
case $i in
--cluster_name=*)
CLUSTER_NAME="${i#*=}"
shift
;;
--ec2_attributes=*)
EC2_ATTRIBUTES="${i#*=}"
shift
;;
--service_role=*)
SERVICE_ROLE="${i#*=}"
shift
;;
--log_uri=*)
LOG_URI="${i#*=}"
shift
;;
--tags=*)
TAGS="${i#*=}"
shift
;;
-*)
# do not exit out, just note failure
echo "unrecognized option: ${i#*=}"
;;
*)
break;
;;
esac
shift
done
echo 'Running with parameters : '
echo "CLUSTER_NAME = ${CLUSTER_NAME}"
echo "EC2_ATTRIBUTES = ${EC2_ATTRIBUTES}"
echo "SERVICE_ROLE = ${SERVICE_ROLE}"
echo "LOG_URI = ${LOG_URI}"
echo "TAGS = ${TAGS}"

aws emr create-cluster --name $CLUSTER_NAME --ec2-attributes $EC2_ATTRIBUTES --service-role $SERVICE_ROLE --log-uri $LOG_URI --applications Name=Spark --instance-group Name=Master,InstanceGroupType=MASTER,InstanceType=i2.4xlarge,InstanceCount=1 Name=Core,InstanceGroupType=CORE,InstanceType=i2.4xlarge,InstanceCount=3 --visible-to-all-users --enable-debugging --tags $TAGS --release-label emr-5.19.0
