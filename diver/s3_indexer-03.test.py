##!/bin/python3
'''
ref: https://docs.amazonaws.cn/en_us/amazondynamodb/latest/developerguide/GettingStarted.Python.html
changeLog:
    - 2021.06.12
    - initial 
'''

import boto3, os, sys
from botocore.exceptions import ClientError

# Variables
tableName = "S3Archive"
region = "ap-northeast-2"
delimiter = ", "

dynamodb = boto3.resource('dynamodb', region_name=region)
def createTable(tName,region):
    table = dynamodb.create_table(
    TableName=tName,
        KeySchema=[
        {
            'AttributeName': 'FileName',
            'KeyType': 'HASH'  #Partition key
        },
        {
            'AttributeName': 'TarName',
            'KeyType': 'RANGE'  #Sort key
        },
    ],
        AttributeDefinitions=[
        {
            'AttributeName': 'FileName',
            'AttributeType': 'S'
        },
        {
            'AttributeName': 'TarName',
            'AttributeType': 'S'
        },
    ],
        ProvisionedThroughput={
            'ReadCapacityUnits': 100,
            'WriteCapacityUnits': 100
        }
    )

    table.meta.client.get_waiter('table_exists').wait(TableName=tName)
    print('Table has been created, please continue to insert data.')

def loadData(tableName, tarName, fileInfoList):
    table = dynamodb.Table(tableName)
    try:
        response = table.put_item(
            Item={
                'FileName': fileInfoList[0],
                'TarName': tarName,
                'FileInfo': {
                    'size': fileInfoList[1],
                    'mtime_ns': fileInfoList[2],
                    'storageClass': fileInfoList[3],
                    'bucketName': fileInfoList[4],
                },
            }
        )
    except ClientError as e:
        print(e.response['Error']['Message'])
    else:
        print("PutItem succeeded:", tarName, fileInfoList[0])

def getFileInfoList(manifestFile): 
    superFileInfoList = [] 
    with open(manifestFile, 'r') as fn: 
        for line in fn.readlines(): 
            superFileInfoList.append([line.split(delimiter)[0], line.split(delimiter)[1], line.split(delimiter)[2], line.split(delimiter)[3], line.split(delimiter)[4].replace('\n','')]) 
    #print("fileInfoList: ", fileInfoList)
    return superFileInfoList 

#def getManifestFile(manifestDir):
#    manifestFiles =  os.listdir(manifestDir)
#    for mf in manifestFiles:
#        manifestFile = os.path.join(manifestDir, mf)
#        fileInfoList = getFileInfoList(manifestFile)


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print ("Usage: %s ct | ld | help" % sys.argv[0])
        print ("use python3, not compatible with python2!!!")
        sys.exit()
    elif sys.argv[1] == "ct":
        ## create Table "S3Archive"
        createTable(tableName, region)
    elif sys.argv[1] == "ld":
        tarName = "filelist-20210612_122721.gz"
        manifestDir = "s3_archiver_filelist"
        #fileInfoList = ["/data/nfsshare/fs1/d0001/dir0020/file0500", "13875", "1576823524000000000", "STANDARD"]
        ## getManifestFiles
        manifestFiles =  os.listdir(manifestDir)
        for mf in manifestFiles:
            manifestFile = os.path.join(manifestDir, mf)
            superFileInfoList = getFileInfoList(manifestFile)
            for fileInfoList in superFileInfoList:
                tarName = "snowball-" + mf + ".tar"
                print("fileInfoList: ", fileInfoList)
                loadData(tableName, tarName, fileInfoList)
    else:
        print ("Usage: %s ct | ld | help" % sys.argv[0])
        print ("!!! use python3, not compatible with python2!!!")
        print ("ct: create table ,S3Archive")
        print ("ld: load data from tarfile")
