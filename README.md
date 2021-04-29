# Snowball Uploader
A script to move a billions files to snowball efficiently
- Date: Feb 20, 2021
- Written by: Yongki Kim (hatsari@gmail.com)

## Change Logs
```
  - 2021.02.20
    - save filelist_dir as filelist-currentdata.gz when executing genlist
  - 2021.02.20
    - performance improvement of genlist; dumping file list, not each line
  - 2021.02.20
    - replacing scandir.walk to os.walk. already os.walk module patched with scandir after python3.5
  - 2021.02.10
    - replacing os.path with scandir.path to improve performance of file listing
  - 2021.02.09
    - python2 compatibility for "open(filename, endoding)"
  - 2021.02.01
    - modifying to support Windows
    - refactoring for more accurate defining of variables
  - 2021.01.26
    - multi processing support for parallel uploading of tar files
    - relevant parameter: max_process
  - 2021.01.25
    - removing yaml feature, due for it to cause too much cpu consumtion and low performance
    - fixing bug which use two profiles(sbe1, default), now only use "sbe1" profile
    - showing progress
  - 2020.02.25
    - changing filelist file to contain the target filename
  - 2020.02.24
    - fixing FIFO error
    - adding example of real snowball configuration
  - 2020.02.22 - limiting multi-thread numbers
    - adding multi-threading to improve performance 
    - adding fifo operation to reducing for big file which is over max_part_size 
  - 2020.02.19
    - removing tarfiles_one_time logic
    - spliting buffer by max_part_size
  - 2020.02.18:
    - supprt snowball limit:
      - max_part_size: 512mb
      - min_part_size: 5mb
  - 2020.02.14: 
    - modifying for python3 
    - support korean in Windows
  - 2020.02.12: adding features 
    - gen_filelist by size
  - 2020.02.10: changing filename from tar_to_s3_v7_multipart.py to snowball_uploader_8.py
  - adding features which can split tar file by size and count.
  - adding feature which create file list
  - showing help message
```

## Introduction
*Snowball_uploader* is developed to move many of files efficiently to *Snowball* or *SnowballEdge* which is AWS's appliance to migrate petabyte files to S3. Especially, when there are millions of small files, it takes too long time to transfer them, then it will delay the project and cause high cost for lending the Snowball.
However, using *snowball_uploader*, you can shorten the transfer time. It archives the files into a part in memory, and sends big chunk, and aggregates in several tar files.

### Performance Comparison between Uploading files individually and Uploading with script
At first, I would show you the performance result. The 1st snowball result is measured when uploading each file while changing the name, and the 2nd result is measured when applying script which makes archive files with tar and send to snowball on memory. With below table and numbers, you will notice at least 7 times better performance with the 2nd option.

- the 1st snowball performance: uploading each files with **aws s3 cp**
- the 2nd snowball performance: uploading chunked files **snowball_uploader draft version**
----

| Target                       | No. of Files | Total Capacity | NAS -> Snowball Time   | Snowball -> S3 Time | Failed Objects |
|------------------------------|:-------------:|:--------------:|:---------------:|:--------------:|----------------|
| the 1st snowball performance | 19,567,430     |2,408 GB        | 1W              | 113 hour       | 954            |
| the 2nd snowball performance | approx. 119,577,235| 14,708 GB       | 1W              | 26 hour        | 0              |

### Snowball Edge Manual
- snowball edge data migration: https://d1.awsstatic.com/whitepapers/snowball-edge-data-migration-guide.pdf?did=wp_card&trk=wp_card


## USAGE
### Prerequisites
- python3.5
  - python2 would work as well, but only English file name 
- boto3
- awscli
### Execution
#### changing parameters
```python
bucket_name = "your-own-bucket"
session = boto3.Session(profile_name='sbe1')
s3 = session.client('s3', endpoint_url='http://10.10.10.10:8080')
# or below
#s3 = boto3.client('s3', endpoint_url='https://s3.ap-northeast-2.amazonaws.com')
#s3 = boto3.client('s3', region_name='ap-northeast-2', endpoint_url='https://s3.ap-northeast-2.amazonaws.com', aws_access_key_id=None, aws_secret_access_key=None)
target_path = '/move/to/s3/orgin/'   ## very important!! change to your source directory
max_tarfile_size = 10 * 1024 ** 3 # 10GB
max_part_size = 300 * 1024 ** 2 # 300MB
min_part_size = 5 * 1024 ** 2 # 5MB
max_process = 5  # concurrent processes, set the value to less than filelist files in file list_dir
if os.name == 'nt':
    filelist_dir = "C:/Temp/fl_logdir_dkfjpoiwqjefkdjf/"  #for windows
else:
    filelist_dir = '/tmp/fl_logdir_dkfjpoiwqjefkdjf/'    #for linux
```
These parameters are crucial to run as you wish
-----
  - **bucket_name** : input your bucket name
  - **session = boto3.Session(profile_name='sbe1')**: input aws profile name
  - **target_path**: input directory path which you want to transfer to *Snowball*
    - if target_path = '/move/to/s3/origin/', it will move to s3://'bucket_name'/move/to/s3/origin/
    - if target_path = '.', it will move to s3://'bucket_name'/
    - so, it is very important where you execute the command *snowball_uploader* and fix the *target_path*
    - I suggest that you would test the script with sample data before applying to your data.
  - **max_tarfile_size**: tar file size which will be uploaded to *Snowball*
    - the value should be under 100 GB
    - *snowball_uploader* archives files to tar file in *Snowball*, and this tar file will be extracted automatically.
    - *Metadata={"snowball-auto-extract": "true"}*, this metadata is add to the tar file.
    - snowball limit ref: https://docs.aws.amazon.com/snowball/latest/developer-guide/batching-small-files.html
  - **max_part_size**: max multi part size, *Snowball* limits max-multi-part size to 512MB
      - this script used multi-part-upload feature of S3 to aggregate the files into one big tar file
      - snowball limit ref: https://docs.aws.amazon.com/snowball/latest/ug/limits.html
  - **min_part_size**: minimum multi part size, *Snowball* limits min-multi-part size to 5MB
      - ref: https://docs.aws.amazon.com/snowball/latest/ug/limits.html
  - **max_process**: numbers of concurrent processes, *snowball_uploader* uses multiple processes to increase the upload speed
  - **filelist_dir**: where filelist file generated
    - /tmp/fl_logdir_dkfjpoiwqjefkdjf/ directory is fixed, and this directory removed and re-created whenever you run the script with *genlist* parameter.

#### genlist
``` shell
ec2-user> python3 snowball_uploader.py genlist
```

*genlist* parameter generates the manifest files containing original files and target files.
this parameter should be run before coping the files.

![genlist](http://g.recordit.co/joXk2dcJBB.gif)

- files list to be transferred
files list split by the sum of files to fix the tar file size, max tar file size should be under 100GB.
``` shell
ec2-user> ls /tmp/fl_logdir_dkfjpoiwqjefkdjf
fl_1.yml fl_2.yml fl_3.yml fl_4.yml fl_5.yml
```

- the contents of file list
``` shell
ec2-suer> cat f1_1.yaml
- ./snowball_uploader_11_failed.py: ./snowball_uploader_11_failed.py
- ./success_fl_2.yaml_20200226_002049.log: ./success_fl_2.yaml_20200226_002049.log
- ./file_list.txt: ./file_list.txt
- ./snowball-fl_1-20200218_151840.tar: ./snowball-fl_1-20200218_151840.tar
- ./bytesio_test.py: ./bytesio_test.py
- ./filelist_dir1_10000.txt: ./filelist_dir1_10000.txt
- ./snowball_uploader_14_success.py: ./snowball_uploader_14_success.py
- ./error_fl_1.txt_20200225_022018.log: ./error_fl_1.txt_20200225_022018.log
- ./snowball_uploader_debug_success.py: ./snowball_uploader_debug_success.py
- ./success_fl_1.txt_20200225_022018.log: ./success_fl_1.txt_20200225_022018.log
- ./snowball_uploader_20_thread.py: ./snowball_uploader_20_thread.py
- ./success_fl_1.yml_20200229_173222.log: ./success_fl_1.yml_20200229_173222.log
- ./snowball_uploader_14_ing.py: ./snowball_uploader_14_ing.py
```
  - manifest file is written in yaml format
  - the left key is the original file name
  - the right value is the target file name, if you want to change the file name on the S3, you can change it with *rename_file* method.
```python
def rename_file(org_file):
    target_file = org_file  ##
return target_file
```
#### cp_snowball
- *cp_snowball* parameter will transfer files to *snowball*
![genlist](http://g.recordit.co/Gq1Z7Tv4MU.gif)

- When the script runs, it creates two log files, success_'file_name'_'timestamp'.log and error_'file_name'_'timestamp'.log
  - success_'file_name'_'timestamp'.log: it contains the name of files which archived into tarfile successfully
  - error_'file_name'_'timestamp'.log: it contains the name of files which is not exist in file system, even though written in filelist.
  - with these logs, you can check which is transferred and which is not.
## HOW IT WORKS

``` python
    #print ('\n')
    print ('genlist: ')
    print ('this option will generate files which are containing target files list in %s'% (filelist_dir))
    #print ('\n')
    print ('cp_snowball: ')
    print ('cp_snowball option will copy the files on server to snowball efficiently')
    print ('the mechanism is here:')
    print ('1. reads the target file name from the one filelist file in filelist directory')
    print ('2. accumulates files to max_part_size in memory')
    print ('3. if it reachs max_part_size, send it to snowball using MultiPartUpload')
    print ('4. during sending data chunk, threads are invoked to max_thread')
    print ('5. after complete to send, tar file is generated in snowball')
    print ('6. then, moves to the next filelist file recursively')
```

## Conclusion
I'm not a professional programmer so it may have some flaw, error handling is very poor. And this script may consume huge amount of memory if you set too high numbers of parameters (max_threads, max_part_size, and max_tarfile_size), then it can cause the freezing of system. so test it several times with sample data.
When I used it in customer sites, it reduced the consuming time over 10 times. I hope you could get help from this script as well.
