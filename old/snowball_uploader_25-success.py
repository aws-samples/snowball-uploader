'''
status: completed
version: v25
way: using multi-part uploading
ref: https://gist.github.com/teasherm/bb73f21ed2f3b46bc1c2ca48ec2c1cf5
changelog:
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
'''

import boto3
import tarfile
import io
import os.path
from datetime import datetime
import sys
import shutil
import multiprocessing
import time
import scandir
import math

bucket_name = "your-own-bucket"
session = boto3.Session(profile_name='sbe1')
s3 = session.client('s3', endpoint_url='http://10.10.10.10:8080')
# or below
#s3 = session.client('s3', endpoint_url='https://s3.ap-northeast-2.amazonaws.com')
#s3 = boto3.client('s3', region_name='ap-northeast-2', endpoint_url='https://s3.ap-northeast-2.amazonaws.com', aws_access_key_id=None, aws_secret_access_key=None)
#target_path = 'dataset/mill/dataset2/fs1/d0001'   ## very important!! change to your source directory
target_path = '/data/dataset'   ## very important!! change to your source directory
max_tarfile_size = 100 * 1024 ** 3 # 10GiB, 100GiB is max limit of snowball
max_part_size = 500 * 1024 ** 2 # 500MB, 500MiB is max limit of snowball
max_process = 5  # max process number, set the value to less than filelist files in filelist_dir 
if os.name == 'nt':
    filelist_dir = "C:/tmp/fl_logdir_dkfjpoiwqjefkdjf/"  #for windows
else:
    filelist_dir = '/tmp/fl_logdir_dkfjpoiwqjefkdjf/'    #for linux

#### don't need to modify from here
min_part_size = 5 * 1024 ** 2 # 5MiB, Don't change it, it is limit of snowball
max_part_count = int(math.ceil(max_tarfile_size / max_part_size))
current_time = datetime.now().strftime("%Y%m%d_%H%M%S")
parts = []
delimiter = ', '
## if python2, exclude encoding parameter 
if sys.version_info.major > 2:
    do_open = lambda filename, flag: open(filename, flag, encoding='utf-8')
else:
    do_open = lambda filename, flag: open(filename, flag)
## Caution: you have to modify rename_file function to fit your own naming rule
def rename_file(org_file):
    #return org_file + "_new_name"
    return org_file

def gen_filelist():
    sum_size = 0
    fl_prefix = 'fl_'
    fl_index = 1
    shutil.rmtree(filelist_dir,ignore_errors=True)
    try:
        os.makedirs(filelist_dir)
    except: pass
    print('generating file list by size %s bytes' % max_tarfile_size)
    for r,d,f in scandir.walk(target_path):
        for file in f:
            file_name = os.path.join(r,file)
            fl_name = filelist_dir + '/' + fl_prefix + str(fl_index) + ".txt"
            sum_size = sum_size + os.path.getsize(file_name)
            if max_tarfile_size < sum_size:
                fl_index = fl_index + 1            
                sum_size = 0
            with do_open(fl_name, 'a') as fl_content:
                target_file_name = rename_file(file_name)
                fl_content.write(file_name + delimiter + target_file_name + '\n')                
                print('%s, %s' % (file_name, target_file_name))
    print('file lists are generated!!')
    print('check %s' % filelist_dir)
    return os.listdir(filelist_dir)

def get_org_files_list(source_file):
    filelist = []
    with do_open(source_file, 'r') as fn:
        for line in fn.readlines():
            filelist.append({line.split(delimiter)[0]:line.split(delimiter)[1].replace('\n','')})
    return filelist

def log_error(error_log, org_file, str_suffix):
    with do_open(error_log,'a+') as err:
        err.write(org_file + str_suffix)

def log_success(success_log, target_file, str_suffix):
    with do_open(success_log,'a+') as success:
        success.write(target_file + str_suffix)

def create_mpu(key_name):
    mpu = s3.create_multipart_upload(Bucket=bucket_name, Key=key_name, Metadata={"snowball-auto-extract": "true"})
    mpu_id = mpu["UploadId"]
    return mpu_id

def upload_mpu(key_name, mpu_id, data, index):
    #part = s3.upload_part(Body=data, Bucket=bucket_name, Key=key_name, UploadId=mpu_id, PartNumber=index, ContentLength=max_buf_size)
    part = s3.upload_part(Body=data, Bucket=bucket_name, Key=key_name, UploadId=mpu_id, PartNumber=index)
    parts.append({"PartNumber": index, "ETag": part["ETag"]})
    #print ('parts list: %s' % str(parts))
    return parts

def complete_mpu(key_name, mpu_id, parts):
    result = s3.complete_multipart_upload(
        Bucket=bucket_name,
        Key=key_name,
        UploadId=mpu_id,
        MultipartUpload={"Parts": parts})
    return result

def adjusting_parts_order(mpu_parts):
    return sorted(mpu_parts, key=lambda item: item['PartNumber'])

def buf_fifo(buf):
    tmp_buf = io.BytesIO()            # added for FIFO operation
    tmp_buf.write(buf.read())    # added for FIFO operation
    #print ('3. before fifo, recv_buf_size: %s' % len(buf.getvalue()))
    #print('3.before fifo, recv_buf_pos : %s' % buf.tell())
    buf.seek(0,0)
    buf.truncate(0)
    tmp_buf.seek(0,0)
    buf.write(tmp_buf.read())
    return buf

def copy_to_snowball(error_log, success_log, key_name, org_files_list):
    tar_file_size = 0
    recv_buf = io.BytesIO()
    mpu_id = create_mpu(key_name)
    parts_index = 1
    s_log = success_log
    e_log = error_log
    with tarfile.open(fileobj=recv_buf, mode="w") as tar:
        for files_dict in org_files_list:
            for org_file, target_file in files_dict.items():
                if os.path.isfile(org_file):
                    tar.add(org_file, arcname=target_file)
                    #print ('1. recv_buf_size: %s' % len(recv_buf.getvalue()))
                    log_success(s_log, target_file, " is archiving \n" )
                    recv_buf_size = recv_buf.tell()
                    #print ('1. recv_buf_pos: %s' % recv_buf.tell())
                    if recv_buf_size > max_part_size:
                        print('multi part uploading:  %s / %s , size: %s' % (parts_index, max_part_count, recv_buf_size))
                        chunk_count = int(recv_buf_size / max_part_size)
                        tar_file_size = tar_file_size + recv_buf_size
                        print('%s is accumulating, size: %s' % (key_name, tar_file_size))
                        #print('chunk_count: %s ' % chunk_count)
                        for buf_index in range(chunk_count):
                            start_pos = buf_index * max_part_size
                            recv_buf.seek(start_pos,0)
                            mpu_parts = upload_mpu(key_name, mpu_id, recv_buf.read(max_part_size), parts_index)
                            parts_index += 1
                        ####################
                        buf_fifo(recv_buf)
                        recv_buf_size = recv_buf.tell()
                        #print('3.after fifo, recv_buf_pos : %s' % recv_buf.tell())
                        #print ('3. after fifo, recv_buf_size: %s' % len(recv_buf.getvalue()))
                    else:
                        pass
                        #print('accumulating files...')
                else:
                    log_error(e_log, org_file," does not exist\n")
                    print (org_file + ' is not exist...............................................\n')
    recv_buf.seek(0,0)
    mpu_parts = upload_mpu(key_name, mpu_id, recv_buf.read(), parts_index)
    parts_index += 1
    mpu_parts = adjusting_parts_order(mpu_parts)
    complete_mpu(key_name, mpu_id, mpu_parts)
    ### print metadata
    meta_out = s3.head_object(Bucket=bucket_name, Key=key_name)
    print ('\n metadata info: %s' % str(meta_out)) 
    log_success(s_log, str(meta_out), '!!\n')
    print ("\n tar file: %s \n" % key_name)
    log_success(s_log, key_name, ' is uploaded successfully\n')

def snowball_uploader_help(**args):
    print ("Usage: %s 'genlist | cp_snowball | help'" % sys.argv[0])
    print ("use python3, not compatible with python2!!!")    
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
    print ('4. tar files uploading to Snowball concurrently according to  max_process')
    print ('5. after complete to send, tar file is generated in snowball')
    print ('6. then, moves to the next filelist file recursively')

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print ("Usage: %s genlist | cp_snowball | help" % sys.argv[0])
        print ("use python3, not compatible with python2!!!")
        sys.exit()
    elif sys.argv[1] == "genlist":
        gen_filelist()
    elif sys.argv[1] == "cp_snowball":
        source_files =  os.listdir(filelist_dir)
        max_source_files = len(source_files)
        source_files_count = 0
        task_process = []
        task_index = 0
        for sf in source_files:
            error_log = ('error_%s_%s.log' % (sf, current_time))
            success_log = ('success_%s_%s.log' % (sf, current_time))
            source_file = os.path.join(filelist_dir, sf)
            #org_files_list = open(source_file, encoding='utf8').readlines()
            org_files_list = get_org_files_list(source_file)
            key_name = ("snowball-%s-%s.tar" % (sf[:-4], current_time))
            #print ("key_name:", key_name)
            #print ('\n0. ###########################')
            #print ('0. %s is starting' % sf)
            #print ('0. ###########################')
            #copy_to_snowball(org_files_list)
            task_process.append(multiprocessing.Process(target = copy_to_snowball, args=(error_log, success_log, key_name, org_files_list,)))
            task_process[-1].start()
            source_files_count+=1
            #print ('1. ###########################')
            print ('1. %s is processing, transfered tar files: %s / %s' % (sf, source_files_count, max_source_files))
            #print ('1. ###########################')
            parts = []
            if task_index >= max_process:
                pjoin = [ proc.join() for proc in task_process ]
                task_index = 0
                task_process = []
            task_index += 1
        print ('part progess of tar file could not reach the max, sorry for inconvenience')
    else:
        snowball_uploader_help()

