# -*- coding:utf-8 -*-
import os
import time
import string
import subprocess
import sys 
import requests
import socket

hostname = socket.gethostname()
ip = socket.gethostbyname(hostname)
date = time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time()))


def send_msg(title,message,to_party=11):
    send_msg_url = 'http://op-01.gzproduction.com:9527/api/msg/send'
    payload = {'title': title, 'message': message, 'to_party': to_party}
    ret = requests.get(send_msg_url, params=payload)
    return '0' == ret.json()['code']

def monitor(project):
    send_msg("server:"+hostname,"Alarm host:"+hostname+"\nAlarm address:"+ip+ "\nAlarm Content:"+ project + ': error' + "\nAlarm grading:"+"High"+"\nAlarm time:"+date)

if __name__ == "__main__":
    project = 'mongo_kss'
    monitor(project)

