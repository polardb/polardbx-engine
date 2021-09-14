# coding=utf-8
import gzip
import time
import socket
import ssl
import sys
import requests
import json
from datetime import datetime

def send_tpcc_msg(dingding_url, tpmc, url):
    text = "### TPCC性能回归结果\n"
    text += "> Date: " + datetime.now().strftime("%Y-%m-%d %H:%M:%S") + "\n\n"
    text += "> TpmC: " + tpmc + "\n\n"
    text += "> More details: [Jenkins Job](" + url + ")\n\n"
    r = requests.post(
            url=dingding_url,
            data=json.dumps( {"msgtype": "markdown",
                "markdown": {"title": "TPCC性能回归", "text": text},
                }),
            headers={'Content-Type': 'application/json'})
    print(r.text)

def send_mtr_msg(dingding_url, failed_cases, url):
    text = "### MTR每日回归结果\n"
    text += "> Date: " + datetime.now().strftime("%Y-%m-%d %H:%M:%S") + "\n\n"
    text += "> Failed cases: " + failed_cases + "\n\n"
    text += "> More details: [Jenkins Job](" + url + ")\n\n"
    r = requests.post(
            url=dingding_url,
            data=json.dumps( {"msgtype": "markdown",
                "markdown": {"title": "MTR每日回归", "text": text},
                }),
            headers={'Content-Type': 'application/json'})
    print(r.text)

def send_sysbench_msg(dingding_url, results, url):
    text = "### Sysbench每日回归结果\n"
    text += "> Date: " + datetime.now().strftime("%Y-%m-%d %H:%M:%S") + "\n\n"
    text += "> Results: " + results + "\n\n"
    text += "> More details: [Jenkins Job](" + url + ")\n\n"
    r = requests.post(
            url=dingding_url,
            data=json.dumps( {"msgtype": "markdown",
                "markdown": {"title": "Sysbench每日回归", "text": text},
                }),
            headers={'Content-Type': 'application/json'})
    print(r.text)


if __name__ == '__main__':
    if len(sys.argv) != 5:
        print("\nUSAGE:dingding.py mode webhook content url\n")
        sys.exit(1)
    mode = sys.argv[1]
    dingding_url = sys.argv[2]
    content = sys.argv[3]
    info_url = sys.argv[4]

    if mode == "tpcc":
        send_tpcc_msg(dingding_url, content, info_url)
    elif mode == "mtr":
        send_mtr_msg(dingding_url, content, info_url)
    elif mode == "sysbench":
        send_sysbench_msg(dingding_url, content, info_url)



