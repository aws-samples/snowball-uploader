#!/bin/python2

#s3_inven = "million_files_filelist.csv"
s3_inven = "s3_inventory.csv"
org_file = "nas_source.txt"
success_log = "success.log"
mismatch_log = "mismatch.log"

## initializing log
#open(success_log,'w').close()
#open(error_log,'w').close()

def log_success(orgLine):
    with open(success_log,'a+') as success:
        success.write(orgLine)

def log_mismatch(orgLine):
    with open(mismatch_log,'a+') as mismatch:
        mismatch.write(orgLine)

trantab = ("\r\n", None)
## generate s3 inventory lists
invenList2 = []
with open(s3_inven) as invenFile:
    invenLinesList = invenFile.readlines()
    for invenLine in invenLinesList:
        invenLine2 = invenLine.split(",")[1].translate(None, '"')
        #invenLine2 = invenLine2.translate(None, "\r")
        invenList2.append(invenLine2)

#print("invenList2: ", invenList2)

## generate org lists
orgList2 = []
with open(org_file) as orgFile:
    for orgLine in orgFile:
        orgLine2 = orgLine.translate(None, "\r\n")
        orgList2.append(orgLine2)

#print("orgList2: ", orgList2)

## compare both list
invenSet = set(invenList2)
orgSet = set(orgList2)
commonSet = invenSet & orgSet
#commonSet = set(setOrg).intersection(set(setInven))
for common in commonSet:
    log_success(common +"\n")

## logging files, not in S3 inventory 
diffSet = orgSet.difference(invenSet)
for diff in diffSet:
    log_mismatch(diff +"\n")
