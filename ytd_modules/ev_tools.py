#!/usr/bin/python3

import os
import sys
import json
import requests
import traceback
import urllib.parse
import smtplib
import urllib3
from sys import platform

EV_BAD_SERVER_ANSWERS = ["NOT_FOUND", "BAD_CONNECTION", "BAD_SRV_ANSWER"]

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


# ---------------------------------------------------------------------------------------------------------------
def cut_spaces(text="", symbol=" "):
    i = 0
    if len(text) == 0:
        return ""
    elif text.find(symbol) > -1:
        while text[i] == symbol:
            text = text[1:]
        while text[len(text)-1] == symbol:
            text = text[:-1]
            i = 1
        while i < len(text)-1:
            if text[i] == symbol and text[i+1] == symbol:
                text = text[:i+1] + text[i+2:]
            else:
                i += 1
    return text


# ---------------------------------------------------------------------------------------------------------------
def cut_spaces_inside(text="", symbol=" "):
    i = 0
    if len(text) == 0:
        return ""
    elif text.find(symbol) > -1:
        while i < len(text)-1:
            if text[i] == symbol and text[i+1] == symbol:
                text = text[:i+1] + text[i+2:]
            else:
                i += 1
    return text


# ---------------------------------------------------------------------------------------------------------------
def change_spaces(text="", symbol="_"):
    if len(text) == 0:
        return ""
    if text.find(" ") > -1:
        text = text.replace(" ", symbol)
    return text


# ------------------------------------------------------------
def save_json_data(data, name=""):
    if data is not None or len(name) > 0:
        with open(name, mode="w", encoding="utf-8") as data_file:
            json.dump(data, data_file, indent=2)
        data_file.close()


# ------------------------------------------------------------
def load_json_data(filename=""):
    if len(filename) == 0:
        return None
    try:
        if os.path.exists(filename):
            with open(filename, mode="r", encoding="utf-8") as data_file:
                str1 = data_file.read()
                data = json.loads(str1)
            data_file.close()
            return data
        else:
            return None
    except EOFError or OSError or json.JSONDecodeError:
        exc_type, exc_value, exc_traceback = sys.exc_info()
        print("TraceBack:")
        traceback.print_tb(exc_traceback, limit=1, file=sys.stdout)
        print("Exception:")
        tr_back = traceback.format_exception(etype=exc_type, value=exc_value, tb=exc_traceback, chain=True)
        tr_back_str = ""
        for i in tr_back:
            tr_back_str = tr_back_str + i + "\n"
        print(tr_back_str)
        return None


# ------------------------------------------------------------
def check_list_by_param(mylist, mykey, value):
    if len(mylist) == 0 or mylist is None or len(mykey) == 0:
        return False, -1, None
    idx = 0
    try:
        for item in mylist:
            if item[mykey] == value:
                return True, idx, item
            idx += 1
    except KeyError:
        return False, -1, None
    return False, -1, None


# ------------------------------------------------------------
def remove_item_by_str_value(mylist, value, inverse=False):
    if len(mylist) == 0:
        return "LIST_CLEAR", None
    result = mylist
    j = len(result)-1
    while j > -1:
        if inverse is False:
            if result[j].lower().find(value.lower()) > -1:
                del result[j]
        else:
            if result[j].lower().find(value.lower()) == -1:
                del result[j]
        j -= 1
    return "OK", result


# ------------------------------------------------------------
def send_url_request(url, payload=None, ajson=None, verify=False, atype="GET", def_not_found="NOT_FOUND", def_bad_conn="BAD_CONNECTION", auth=None, parse=False, timeout=120):
    try:
        if parse:
            url = urllib.parse.quote(url, safe=":/?&=")
        if atype == "GET":
            r = requests.get(url, data=payload, timeout=timeout, auth=auth, verify=verify)
        else:
            r = requests.post(url, data=payload, json=ajson, timeout=timeout, auth=auth, verify=verify)
        if r.status_code == 200:
            return "OK", r, r.text
        else:
            return r.status_code, None, None
    except requests.exceptions.InvalidSchema:
        return "BAD_PROTOCOL", None, None
    except requests.Timeout:
        return "TIMEOUT", None, None
    except requests.ConnectionError or requests.HTTPError:
        return def_bad_conn, None, None
    except requests.exceptions.RequestException:
        return def_not_found, None, None


# ------------------------------------------------------------
def get_http_json_request(url,
                          payload=None,
                          def_not_found="NOT_FOUND",
                          def_bad_srv="BAD_SRV_ANSWER",
                          def_bad_conn="BAD_CONNECTION",
                          timeout=120,
                          auth=None,
                          verify=False,
                          parse=False):
    code, r, _ = send_url_request(atype="GET", url=url, payload=payload, def_not_found=def_not_found, def_bad_conn=def_bad_conn, timeout=timeout, auth=auth, verify=verify, parse=parse)
    if code == "OK":
        try:
            data = r.json()
            if len(data) == 0:
                return def_not_found, None
            return "OK", data
        except json.JSONDecodeError:
            return def_bad_srv, None
    else:
        return code, None


# ------------------------------------------------------------
def post_http_json_request(url,
                           payload=None,
                           ajson=None,
                           verify=False,
                           def_not_found="NOT_FOUND",
                           def_bad_srv="BAD_SRV_ANSWER",
                           def_bad_conn="BAD_CONNECTION",
                           timeout=120,
                           auth=None,
                           parse=False):
    code, r, _ = send_url_request(atype="POST",
                                  url=url,
                                  payload=payload,
                                  ajson=ajson,
                                  def_not_found=def_not_found,
                                  def_bad_conn=def_bad_conn,
                                  timeout=timeout,
                                  auth=auth,
                                  verify=verify,
                                  parse=parse)
    if code == "OK":
        try:
            data = r.json()
            if len(data) == 0:
                return def_not_found, None
            return "OK", data
        except json.JSONDecodeError:
            return def_bad_srv, None
    else:
        return code, None


# ------------------------------------------------------------
def send_simple_telegram_msg(log=None, token=None, channel_id=0, text=""):
    url = "https://api.telegram.org/bot"
    if token is None:
        return "NO_TOKEN"
    url += token
    method = url + "/sendMessage"
    if text == "":
        return "NO_MESSAGE"
    if channel_id == 0:
        return "NO_CHANID"
    r = requests.post(method, data={"chat_id": channel_id, "text": text})
    if r.status_code != 200:
        log.log("Telegram post error. HTTP Error is {}".format(r.status_code))
        return "TG_ERROR_"+str(r.status_code)


# ---------------------------------------------------------------------------------------------------------------
def send_email_msg(host="127.0.0.1", port=25, sender="127.0.0.1@localhost.local", to="", msg="test"):
    if to == "":
        return -1
    try:
        s = smtplib.SMTP(host=host, port=port)
    except smtplib.SMTPConnectError or smtplib.SMTPDataError or smtplib.SMTPHeloError or smtplib.SMTPServerDisconnected:
        return None

    if s is not None:
        res = s.sendmail(from_addr=sender, to_addrs=[to], msg=msg)
        s.quit()
        return res


# ---------------------------------------------------------------------------------------------------------------
def slash_processing(filename):
    if len(filename) > 0:
        if platform == 'win32':
            filename = cut_spaces_inside(filename.replace("/", "\\"), "\\")
        else:
            filename = cut_spaces_inside(filename.replace("\\", "/"), "/")
    return filename
