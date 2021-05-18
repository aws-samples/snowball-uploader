# Filename Validataion between S3 inventory and NAS source files

## Description

 I made simple python script to verify that files uploaded well from Snowball to S3.
It is simple but very fast comparing to use “grep”.
only it took 2 sec when performing with 2million files while with “grep”, it took 2min 40sec.this script needs two files, src.txt and s3 inventory csv.

## How to Work

You need to prepare two files, one is original filelist and S3 inventory csv.
Here is original file list

```
$cat src.txt
fs1/d0001/dir0001/file0001
fs1/d0001/dir0001/file0002
fs1/d0001/dir0001/file0003
fs1/d0001/dir0001/file0004
fs1/d0001/dir0001/file0005
fs1/d0001/dir0001/file0006
fs1/d0001/dir0001/file0007
fs1/d0001/dir0001/file0008
fs1/d0001/dir0001/file0009
fs1/d0001/dir0001/file00010
```

Next is S3 inventory CSV file

```
$cat inven.csv
"million-files-nfs-12af56f0-4f24-11eb-aa52-02fa2bc4ef37","fs1/","0"
"million-files-nfs-12af56f0-4f24-11eb-aa52-02fa2bc4ef37","fs1/d0001/","0"
"million-files-nfs-12af56f0-4f24-11eb-aa52-02fa2bc4ef37","fs1/d0001/dir0001/","0"
"million-files-nfs-12af56f0-4f24-11eb-aa52-02fa2bc4ef37","fs1/d0001/dir0001/file0001","104857600"
"million-files-nfs-12af56f0-4f24-11eb-aa52-02fa2bc4ef37","fs1/d0001/dir0001/file0002","104857600"
"million-files-nfs-12af56f0-4f24-11eb-aa52-02fa2bc4ef37","fs1/d0001/dir0001/file0003","104857600"
"million-files-nfs-12af56f0-4f24-11eb-aa52-02fa2bc4ef37","fs1/d0001/dir0001/file0004","104857600"
"million-files-nfs-12af56f0-4f24-11eb-aa52-02fa2bc4ef37","fs1/d0001/dir0001/file0005","104857600"
"million-files-nfs-12af56f0-4f24-11eb-aa52-02fa2bc4ef37","fs1/d0001/dir0001/file0006","33216"
"million-files-nfs-12af56f0-4f24-11eb-aa52-02fa2bc4ef37","fs1/d0001/dir0001/file0007","12960"
```

then, create a script, named “file_comp-2.py


```
 1 #!/bin/python2
  2 
  3 s3_inven = "inven.csv" # s3 inventory file
  4 org_file = "src.txt"   # original source file list from filesystem
  5 success_log = "success.log"
  6 mismatch_log = "mismatch.log"
  7 
  8 def log_success(orgLine):
  9     with open(success_log,'a+') as success:
 10         success.write(orgLine)
 11 
 12 def log_mismatch(orgLine):
 13     with open(mismatch_log,'a+') as mismatch:
 14         mismatch.write(orgLine)
 15 
 16 trantab = ("\r\n", None)
 17 ## generate s3 inventory lists
 18 invenList2 = []
 19 with open(s3_inven) as invenFile:
 20     invenLinesList = invenFile.readlines()
 21     for invenLine in invenLinesList:
 22         invenLine2 = invenLine.split(",")[1].translate(None, '"')
 23         #invenLine2 = invenLine2.translate(None, "\r")
 24         invenList2.append(invenLine2)
 25 
 26 #print("invenList2: ", invenList2)
 27 
 28 ## generate org lists
 29 orgList2 = []
 30 with open(org_file) as orgFile:
 31     for orgLine in orgFile:
 32         orgLine2 = orgLine.translate(None, "\r\n")
 33         orgList2.append(orgLine2)
 34 
 35 #print("orgList2: ", orgList2)
 36 
 37 ## compare both list
 38 invenSet = set(invenList2)
 39 orgSet = set(orgList2)
 40 commonSet = invenSet & orgSet
 41 #commonSet = set(setOrg).intersection(set(setInven))
 42 for common in commonSet:
 43     log_success(common +"\n")
 44 
 45 ## logging files, not in S3 inventory 
 46 diffSet = orgSet.difference(invenSet)
 47 for diff in diffSet:
 48     log_mismatch(diff +"\n")
```

Finally, run the script.

```
[ec2-user@ip-172-31-38-36 test]$ python3 inven.csv 
[ec2-user@ip-172-31-38-36 test]$ cat success.log 
fs1/d0001/dir0001/file0006
fs1/d0001/dir0001/file0007
fs1/d0001/dir0001/file0004
fs1/d0001/dir0001/file0005
fs1/d0001/dir0001/file0002
fs1/d0001/dir0001/file0003
fs1/d0001/dir0001/file0001
[ec2-user@ip-172-31-38-36 test]$ cat mismatch.log 
fs1/d0001/dir0001/file00010
fs1/d0001/dir0001/file0008
fs1/d0001/dir0001/file0009
```

success.log show you files which included both src.txt and inven.csv, and mismatch.log show you files which exist only src.txt, not in inven.csv. 

