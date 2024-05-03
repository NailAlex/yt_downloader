#!/usr/bin/python3

from sys import platform
import subprocess
from ytd_modules import ev_tools


# ---------------------------------------------------------------------------------------------------------------
def extract_video_id(aurl=""):
    if aurl == "":
        return None
    elif aurl.find('v=') == -1:
        return None
    urrl = aurl[aurl.find("?") + 1:]
    if urrl.find("&") > -1:
        urrls = urrl.split("&")
        for u in urrls:
            if u.find("v=") > -1:
                urrl = u[2:]
                break
    else:
        urrl = urrl[2:]
    return urrl


# ---------------------------------------------------------------------------------------------------------------
def get_bot_ips(ename: str):
    if platform == "linux" or platform == "linux2":
        state, eext_ip = ev_tools.get_http_json_request("https://api.ipify.org?format=json", {}, timeout=30)
        if state in ["NOT_FOUND", "BAD_SRV_ANSWER", "BAD_CONNECTION", "TIMEOUT"]:
            eext_ip = {"ip": "127.0.0.1"}
        ss2 = f"ifconfig {ename} | grep inet | grep netmask | awk -F "'" "'" '"'{print $2}'"'"
        eint_ip = {"ip": subprocess.check_output(ss2, shell=True).decode('utf-8').strip()}
        ss3 = f"ifconfig {ename} | grep ether | awk -F "'" "'" '"'{print $2}'"'"
        eint_mac = {"mac": subprocess.check_output(ss3, shell=True).decode('utf-8').strip()}
    else:
        eext_ip = {"ip": "127.0.0.1"}
        eint_ip = {"ip": "127.0.0.1"}
        eint_mac = {"mac": "00:00:00:00:00:00"}
    return eext_ip, eint_ip, eint_mac


# ---------------------------------------------------------------------------------------------------------------
def get_info(ename="eth0", aname="superbot", aid="0"):
    ext_ip, int_ip, int_mac = get_bot_ips(ename)
    return "Имя сервера: {}\n" \
           "ОС сервера: {}\n" \
           "Имя бота: {}\n" \
           "MAC-адрес сервера: {}\n" \
           "Внутренний IP-адрес сервера: {}\n" \
           "Внешний IP-адрес сервера: {}\n" \
           "ID канала запроса: {}".format(subprocess.check_output("hostname", shell=True).decode('utf-8').strip(),
                                          platform,
                                          aname,
                                          int_mac["mac"],
                                          int_ip["ip"],
                                          ext_ip["ip"],
                                          aid)


# ---------------------------------------------------------------------------------------------------------------
def generate_answer_body(jobb):
    body = ""
    if len(jobb) == 0:
        return ""
    if len(jobb["done_urls"]) > 0:
        idxx = 1
        body += "<strong>Успешно скачанные файлы в сетевую папку (всего: {}):\n</strong>".format(len(jobb["done_urls"]))
        for fn in jobb["done_urls"]:
            body += '<em style="white-space:normal">'
            body += str(idxx) + '. {}'.format(fn["filename"]) + '</em>\n\n'
            idxx += 1
    if len(jobb["bad_urls"]) > 0:
        idxx = 0
        body += "<strong>Проблемы со скачиванием следующих ID (всего: {}):\n</strong>".format(len(jobb["bad_urls"]))
        for fn in jobb["bad_urls"]:
            body += '<em style="white-space:normal">'
            body += str(idxx) + '. {}'.format(fn["url"]) + '</em>\n\n'
            idxx += 1
    return body



