'''
status: developing
version: v03
supported: over python3.5
changelog:
  - 2021.06.10
    - v03
    - eliminate "renaming filename" feature
    - support to select storageClass
    - elimidating dependency of "sbe1" profile when running getlist
    - initial feature deriven from snowball_uploader
    - onl
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
import math

###### Change below variables
bucket_name = "your-own-bucket"
profile_name = "sbe1"

endpoint = "https://s3.ap-northeast-2.amazonaws.com"
#endpoint = "http://10.10.10.10:8080"
#s3 = session.client('s3', endpoint_url='http://10.10.10.10:8080')
# or below
#s3 = boto3.client('s3', region_name='ap-northeast-2', endpoint_url='https://s3.ap-northeast-2.amazonaws.com', aws_access_key_id=None, aws_secret_access_key=None)
target_path = "/data"   ## very important!! change to your source directory
if os.name == "nt":
    filelist_dir = "C:/tmp/fl_logdir_dkfjpoiwqjefkdjf"  #for windows
else:
    filelist_dir = "./s3_archiver_filelist"    #for linux
s3_class = "GLACIER" # ['STANDARD'|'REDUCED_REDUNDANCY'|'STANDARD_IA'|'ONEZONE_IA'|'INTELLIGENT_TIERING'|'GLACIER'|'DEEP_ARCHIVE']

##### Optional variables
max_tarfile_size = 10 * (1024 ** 3) # 10GiB, 100GiB is max limit of snowball
max_part_size = 500 * (1024 ** 2) # 500MB, 500MiB is max limit of snowball
max_process = 5  # max process number, set the value to less than filelist files in filelist_dir 

#### don't need to modify from here
min_part_size = 32 * 1024 ** 2 # 32MiB for S3, 5MiB for SnowballEdge
max_part_count = int(math.ceil(max_tarfile_size / max_part_size))
current_time = datetime.now().strftime("%Y%m%d_%H%M%S")
parts = []
delimiter = ', '
## if python2, exclude encoding parameter 
if sys.version_info.major > 2:
    do_open = lambda filename, flag: open(filename, flag, encoding='utf-8')
else:
    do_open = lambda filename, flag: open(filename, flag)

def write_to_file(fl_name, subfl_list):
    with do_open(fl_name, 'w') as f:
        for line in subfl_list:
            f.write("%s\n" %line)
        #writer = csv.writer(fl_content)
        #writer.writerows(subfl_list)
    return 0

def gen_filelist():
    sum_size = 0
    fl_prefix = 'fl_'
    fl_index = 1
    subfl_list = []
    shutil.rmtree(filelist_dir,ignore_errors=True)
    try:
        os.makedirs(filelist_dir)
    except: pass
    print('generating file list by size %s bytes' % max_tarfile_size)
    for r,d,f in os.walk(target_path):
        for file in f:
            fl_name = filelist_dir + '/' + fl_prefix + str(fl_index) + ".txt"
            file_name = os.path.join(r,file)
            f_meta = os.stat(file_name)
            f_inode = f_meta.st_ino
            f_size = f_meta.st_size
            f_mtime_ns = f_meta.st_mtime_ns # nanoseconds, for python3
            #f_mtime = f_meta.st_mtime # seconds, For python2
            #f_dict[f_inode] = {"fname":file_name, "fsize":f_size}
            sum_size = sum_size + f_size
            #target_file_name = rename_file(file_name)
            f_info = [file_name ,str(f_size), str(f_mtime_ns), s3_class, bucket_name]
            #f_info = [file_name , target_file_name]
            f_info_str = delimiter.join(f_info)
            subfl_list.append(f_info_str)
            if max_tarfile_size < sum_size:
                write_to_file(fl_name, subfl_list)
                fl_index = fl_index + 1
                print('%s is generated' % fl_name)
                sum_size = 0
                subfl_list=[]
    ## generate file list for remaings
    write_to_file(fl_name, subfl_list)
    ## archive file list files with tar.gz
    fl_arc_file = "filelist-" + current_time +".gz" 
    with tarfile.open(fl_arc_file, "w:gz") as tar:
        tar.add(filelist_dir, arcname=os.path.basename(filelist_dir))
    print('file lists are generated!!')
    print('check %s' % filelist_dir)
    return 0

def get_org_files_list(source_file):
    filelist = []
    with do_open(source_file, 'r') as fn:
        for line in fn.readlines():
            #filelist.append({line.split(delimiter)[0]:line.split(delimiter)[1].replace('\n','')})
            filelist.append(line.split(delimiter)[0].replace('\n',''))
    return filelist

def log_error(error_log, org_file, str_suffix):
    with do_open(error_log,'a+') as err:
        err.write(org_file + str_suffix)

def log_success(success_log, org_file, str_suffix):
    with do_open(success_log,'a+') as success:
        success.write(org_file + str_suffix)

def create_mpu(key_name):
    mpu = s3.create_multipart_upload(Bucket=bucket_name, Key=key_name, StorageClass=s3_class, Metadata={"snowball-auto-extract": "true"})
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
        for org_file in org_files_list:
            if os.path.isfile(org_file):
                tar.add(org_file)
                #print ('1. recv_buf_size: %s' % len(recv_buf.getvalue()))
                log_success(s_log, org_file, " is archiving \n" )
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
        session = boto3.Session(profile_name=profile_name)
        s3 = session.client('s3', endpoint_url=endpoint)
        #source_files =  [ f for f in os.listdir(filelist_dir) if os.path.isfile(f)]
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
        #print ('part progess of tar file could not reach the max, sorry for inconvenience')
        print ("Uploading Finished")
    else:
        snowball_uploader_help()

