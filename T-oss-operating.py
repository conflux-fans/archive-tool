#!/usr/bin/env python3
# -*- coding: UTF-8 -*-
# auth limeng
# Version python3+
# Mode：pip3 install oss2 psutil requests
# Shell：apt install pigz , yum install pigz

import oss2, os, sys, datetime, time, threading, tarfile, requests, json
from oss2 import SizedFileAdapter, determine_part_size
from oss2.models import PartInfo
from traceback import print_exc
from subprocess import getoutput
from psutil import process_iter,Process

def print_log(T,exe_print_log):
    sleep_time = 180 #180s == 3min
    print("Determine whether the program is synchronizing, if it is synchronizing, kill the process, the whole process will not last at most 3 minutes...")
    while sleep_time > 0:
        output_log = getoutput(exe_print_log)
        if output_log:
            T[0] = 'True'
            break
        else:
            time.sleep(1)
            sleep_time = sleep_time - 1

class Process_operating:
    def __init__(self, process_name:str, check_txt:str):
        self.process_name = process_name
        self.check_txt = check_txt

    def kill_process(self):
        pids = process_iter()
        for pid in pids:
            if(pid.name() == self.process_name):
                pid_list.append(pid.pid)
                #Process(pid.pid).terminate()
                Process(pid.pid).kill()
        if pid_list:
            print(f"Kill process：{self.process_name}\nPID：{pid_list}\n")
            with open(self.check_txt, 'w+') as f:
                f.write('conflux syncing')
        else:
            msg = "Process is not running. exit!"
            print(f"Porcess：{self.process_name} is not running. exit!")
            dingding_text = "## **"+dingding_title+"**\n * Alarm content："+msg+"\n"
            dingding_msg(dingding_title, dingding_text)
            sys.exit(1)

    def start_process(self, start_shell:str):
        if pid_list:
            exe = os.system(start_shell)
            if exe == 0:
                print("The program started successfully！")
            else:
                with open(self.check_txt, 'w+') as f:
                    f.write('conflux not sync')
                msg = "The program failed to start!"
                print(msg)
                dingding_text = "## **"+dingding_title+"**\n * Alarm content："+msg+"\n"
                dingding_msg(dingding_title, dingding_text)

class Write_Script:
    def __init__(self, conflux_version:str, project_name:str ,today:str, keyname:str):
        self.conflux_version = conflux_version
        self.project_name = project_name
        self.today = today
        self.keyname = keyname

    def write_bash(self, write_file_name_bash:str):
        try:
            with open(write_file_name_bash, 'w') as f:
                f.write("#!/bin/bash\n")
                f.write("#Version: " + self.conflux_version + "\n")
                f.write("#DB_Name: " + self.keyname + "\n")
                f.write("#Customize_Date_Format: " + self.today + "\n")
                f.write("#Retain for 1 days, every day at 4 a.m 、12 p.m 、20 p.m backup, format: year, month, day, hour\n")
                f.write("#If there is no curl, please install: https://curl.se/download.html\n")
            with open(write_file_name_bash, 'a') as f:
                f.write("Date=" + self.today + "\n")
                f.write("Download_path=https://conflux-blockchain-data.oss-cn-beijing.aliyuncs.com/fullnode-db/T/" + self.project_name + "-${Date}" + ".tar.gz" + "\n")
                f.write("httpcode=`curl -I -m 10 -o /dev/null -s -w %{http_code} $Download_path`\n")
                f.write("[ $httpcode -eq 200 ] && curl -C - -O $Download_path || echo 'Sorry, service is upgrading or download the script again, Please try again later !'\n")
        except Exception as e:
            msg = "Failed to write download script bash!"
            print(f"Failed_msg: {msg}, err: {e}")

    def write_bat(self, write_file_name_bat:str):
        try:
            with open(write_file_name_bat, 'w') as f:
                f.write("@echo off\n")
                f.write("::Version: " + self.conflux_version + "\n")
                f.write("::DB_Name: " + self.keyname + "\n")
                f.write("::Customize_Date_Format: " + self.today + "\n")
                f.write("::Retain for 1 days, every day at 4 a.m 、12 p.m 、20 p.m backup, format: year, month, day, hour\n")
                f.write("::If there is no curl, please install: https://curl.se/download.html\n")
            with open(write_file_name_bat, 'a') as f:
                f.write("set Date=" + self.today + "\n")
                f.write("set Download_path=https://conflux-blockchain-data.oss-cn-beijing.aliyuncs.com/fullnode-db/T/" + self.project_name + "-%Date%" + ".tar.gz" + "\n")
                f.write("for /F %%i in ('curl -I -m 10 -o /dev/null -s -w %%{http_code} %Download_path%') do set httpcode=%%i\n")
                f.write("if '%httpcode%' == '200' (curl -C - -O %Download_path%) else (echo 'Sorry, service is upgrading or download the script again, Please try again later !')\n")
        except Exception as e:
            msg = "Failed to write download script bat!"
            print(f"Failed_msg: {msg}, err: {e}")

class OSS_upload:
    def __init__(self):
        try:
            bucket = "xxx"
            url = "oss-cn-beijing-internal.aliyuncs.com"
            auth = oss2.Auth(
                'xxx', 
                'xxx'
                )
            self.oss = oss2.Bucket(
                auth, url, bucket
                )
        except Exception as e:
            msg = "OSS link is abnormal!"
            print(f"Failed_msg: {msg}, err: {e}")
            dingding_text = "## **"+dingding_title+"**\n * Alarm content："+msg+"\n"
            dingding_msg(dingding_title, dingding_text)

    @staticmethod
    def targz(output_filepath:str, filelist:str, workdir:str):
        try:
            print("Start compression...")
            os.chdir(workdir)
            os.system("tar '--use-compress-program=pigz -9 -p 2' -cf %s %s" % (output_filepath, filelist))
            #with tarfile.open(output_filepath, "w:gz") as tar:
            #    for line in filelist:
            #        tar.add(line, arcname=os.path.basename(line))
            print("File compression succeeded!")
        except Exception as e:
            msg = "File compression failed, the program exits!"
            print(f"Failed_msg: {msg}, err: {e}")
            Process_operating.start_process(start_shell)
            dingding_text = "## **"+dingding_title+"**\n * Alarm content："+msg+"\n"
            dingding_msg(dingding_title, dingding_text)
            sys.exit(1)

    def upload_oss(self, output_filepath:str, bucket_prefix:str):
        try:
            total_size = os.path.getsize(output_filepath)
            part_size = determine_part_size(total_size, preferred_size=2 * 1024 * 1024)

            upload_id = self.oss.init_multipart_upload(bucket_prefix).upload_id
            parts = []

            print(f"File {output_filepath} is uploading...")
            with open(output_filepath, 'rb') as fileobj:
                part_number = 1
                offset = 0
                while offset < total_size:
                    num_to_upload = min(part_size, total_size - offset)
                    result = self.oss.upload_part(bucket_prefix, upload_id, part_number,
                                                SizedFileAdapter(fileobj, num_to_upload))
                    parts.append(PartInfo(part_number, result.etag))
                    offset += num_to_upload
                    part_number += 1
            self.oss.complete_multipart_upload(bucket_prefix, upload_id, parts)
            print(f"File {bucket_prefix} Uploaded successfully！")
            os.remove(output_filepath) #Delete directory compressed files
        except Exception as e:
            msg = f"File {output_filepath} upload failed!"
            print(f"Failed_msg: {msg}, err: {e}")
            dingding_text = "## **"+dingding_title+"**\n * Alarm content："+msg+"\n"
            dingding_msg(dingding_title, dingding_text)

def dingding_msg(dingding_title,dingding_text):
    json_text = {
        "msgtype": "markdown",
        "markdown": {
            "title": dingding_title,
            "text": dingding_text
        },
        "at":{
            "atMobiles":[
#                ""
            ],
        "isAtAll": "true"
        }
    }
    headers={
        'Content-Type':'application/json'
    }
    requests.post(dingding_url,data=json.dumps(json_text),headers=headers)


if __name__ == '__main__':
    """
    #OSS information
    :project_name：Project name
    :today：Project date
    :keyname：Project archive name, script name
    :bucket_prefix：Bucket prefix path
    :bucket：Bucket name
    """
    project_name = "conflux-fullnode-db-snapshot"
    today = time.strftime("%Y-%m-%d-%H", time.localtime())
    #today = (datetime.datetime.now()+datetime.timedelta(hours=8)).strftime("%Y-%m-%d-%H")
    keyname = project_name + '-' + today + ".tar.gz"
    keyname_bash = "download.sh"
    keyname_bat = "download.bat"
    bucket_prefix = "fullnode-db" + "/" + "T" + "/" + keyname
    bucket_prefix_bash = "fullnode-db" + "/" + "T" + "/" + keyname_bash
    bucket_prefix_bat = "fullnode-db" + "/" + "T" + "/" + keyname_bat

    """
    #Program information
    :process_name：Process name
    :conflux_version：Get the program version number
    :start_shell：Starting program
    :dingding_url：Dingding url token
    :dingding_title：Alarm title
    """
    process_name = "conflux_T"
    conflux_version = getoutput('/data/T/conflux-service/conflux_T --version')
    start_shell = "bash /data/T/conflux-service/start.sh"
    dingding_url = "xxx"
    dingding_title = "Conflux fullnode-snapshot-T_BJ"

    """
    #Path information
    :filelist: Need to package the directory
    :output_path：Output path
    :output_filepath: Save the compressed filelist
    :write_file_name：Download script name
    :logpath：Exception log path
    :logfile：Exception log file
    :exe_print_log：Print program log bash command
    """
    workdir = "/data/T/conflux-service"
    filelist = 'blockchain_data pos_db pos_config'
    output_path = "/data/T/"
    output_filepath = output_path + keyname
    write_file_name_bash = output_path + "download.sh"
    write_file_name_bat = output_path + "download.bat"
    logpath = "/data/T/logs/"
    logfile = logpath + "oss-operating-error-" + today + ".log"
    check_txt = logpath + 'check-conflux-status.txt'
    exe_print_log = "tail -n -100 /data/T/conflux-service/log/conflux.log |grep -wi 'Catch-up mode: false'"

    """
    :T：Print pid status, default is'Flase'
    :pid_list：pid num
    """
    T = ['False']
    pid_list = []

    try:
        print_log(T,exe_print_log)

        if T[0] == 'True':
            if not os.path.isdir(output_path):
                os.makedirs(output_path)
            if not os.path.isdir(logpath):
                os.makedirs(logpath)

            P = Process_operating(process_name, check_txt)
            P.kill_process()
            
            getoutput('rm -f /data/T/conflux-service/pos_config/pos_key')

            OSS_upload.targz(output_filepath, filelist, workdir)

            W = Write_Script(conflux_version, project_name, today, keyname)
            W.write_bash(write_file_name_bash)
            W.write_bat(write_file_name_bat)

            OSS = OSS_upload()
            OSS.upload_oss(output_filepath,bucket_prefix)
            OSS.upload_oss(write_file_name_bash,bucket_prefix_bash)
            OSS.upload_oss(write_file_name_bat,bucket_prefix_bat)

            P.start_process(start_shell)
            
            P_M = Process_operating("conflux_M", "/data/M/logs/check-conflux-status.txt")
            P_M.kill_process()
        else:
            msg = f"The program data synchronization is not up to date, please check. This backup of {keyname} failed！"
            print(f"Failed_msg: {msg}")
            dingding_text = "## **"+dingding_title+"**\n * Alarm content："+msg+"\n"
            dingding_msg(dingding_title, dingding_text)

    except Exception:
        print_exc(file=open(logfile, 'w+'))
        msg = f"The program is running abnormally, and the Error log is in\n{logpath}"
        print(f"Failed_msg: {msg}")
