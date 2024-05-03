#!/usr/bin/python3

import datetime
from ytd_modules import mytinylogger, ev_tools
import os
import subprocess
import time
from sys import platform
import json
import re
import multiprocessing

MAX_RES_VIDEO_CMD = 'yt-dlp --cookies "{}" --merge-output-format mp4 -i {} --force-overwrites ' \
                    '-P "{}" -o "{}" -r {} -q -S "height:2160" -- {}'

HD_RES_VIDEO_CMD = 'yt-dlp --cookies "{}" --merge-output-format mp4 -i {} --force-overwrites ' \
                   '-P "{}" -o "{}" -S "height:1080" -r {} -q -- {}'

MAX_RES_AUDIO_CMD = 'yt-dlp --cookies "{}" -i {} --force-overwrites ' \
                    '-P "{}" -o "{}" -x --audio-format flac --audio-quality 940k -r {} -q -f ba -- {}'

HD_RES_AUDIO_CMD = 'yt-dlp --cookies "{}" -i {} --force-overwrites ' \
                   '-P "{}" -o "{}" -x --audio-format mp3 --audio-quality 320k -r {} -q -f ba -- {}'

GET_STREAM_INFO_CMD = 'yt-dlp --cookies "{}" -j -- {}'

GET_MAX_VIDEO_NAME = 'yt-dlp --cookies "{}" -i -O "%(uploader)s@@@%(title)s@@@[%(id)s][%(resolution)s]" -s -S "height:3840" -- {}'

GET_HD_VIDEO_NAME = 'yt-dlp --cookies "{}" -i -O "%(uploader)s@@@%(title)s@@@[%(id)s][%(resolution)s]" -s -S "height:1080" -- {}'

GET_AUDIO_NAME = 'yt-dlp --cookies "{}" -i -O "%(uploader)s@@@%(title)s@@@[%(id)s]" -x -s -f ba -- {}'

YOUTUBE_URL_SPAM = "https://www.youtube.com/watch?v={}"

# --print filename
RESTRICTED_CHARS = ['\\', '/', '"', "'", '|', ':', ';']


# ---------------------------------------------------------------------------------------------------------------
def download_worker_task(aconfig,
                         alock: multiprocessing.Lock,
                         jobs: multiprocessing.Queue,
                         astatus: multiprocessing.Queue):
    cookies_file = ""
    alog = mytinylogger.MyTinyLogger(aconfig["log_worker_logfile"], aconfig["log_worker_errfile"], 400000, 8)

    def prepare_cookies_file(pid):
        src_file = ev_tools.slash_processing(os.path.abspath(os.path.dirname(__file__)).replace("ytd_modules", "") + aconfig["cookies_src_file"])
        pid_file = ev_tools.slash_processing(os.path.abspath(os.path.dirname(__file__)).replace("ytd_modules", "") + aconfig["cookies_path"] + "/" + "cookies_{}.txt".format(str(pid)))

        if os.path.exists(pid_file):
            os.remove(pid_file)

        if platform == 'win32':
            subprocess.check_output('copy {} {}'.format(src_file, pid_file), shell=True)
        else:
            subprocess.check_output('cp {} {}'.format(src_file, pid_file), shell=True)
        return pid_file

    def print_completed_job(ajob, l_now, aresult="url_dl_complete", arestart=False, aaction="", anotify=False):
        alog.log("DL_JOB_WORKER {}: Job {} completed in {}".format(os.getpid(), ajob["uuid"], datetime.datetime.now() - l_now))
        alock.acquire()
        astatus.put({"etype": "job_worker",
                     "state": aresult,
                     "pid": os.getpid(),
                     "lat": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S.ffffff"),
                     "job": ajob,
                     "restart": arestart,
                     "action": aaction,
                     "notify": anotify
                     })
        alock.release()

    def close_worker():
        nonlocal cookies_file
        os.remove(cookies_file)
        alock.acquire()
        astatus.put({"etype": "job_worker", "state": "dl_complete", "pid": os.getpid(), "lat": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S.ffffff")})
        alock.release()
        alog.log("DL_JOB_WORKER: worker pid {}: All jobs complete".format(os.getpid()))

    def check_save_path(apath):
        apath = ev_tools.slash_processing(apath + "/" + datetime.datetime.now().strftime("%Y-%m-%d"))
        if os.path.exists(apath) is False:
            os.mkdir(apath)
        return apath

    def get_stream_info(idxx):
        acookies_file = prepare_cookies_file(os.getpid())
        ss = GET_STREAM_INFO_CMD.format(acookies_file, idxx)
        dd = subprocess.check_output(ss, shell=True).decode('utf-8').strip()
        try:
            dd = dd.strip()
            js = json.loads(dd)
            return js
        except json.JSONDecoder:
            return None

    def backup_json_stream_data(mid, apath, fname):
        mdata_name = apath + "/" + os.path.splitext(fname)[0] + '_dump.json'
        mdata = get_stream_info(mid)
        if aconfig["test_mode"] is True:
            print("---------- {}".format(mdata_name))
        ev_tools.save_json_data(mdata, mdata_name)

    def replace_restricted_chars(ss, symb=" "):
        ss = ss.encode('utf-8', 'ignore').decode("utf-8")
        for rs in RESTRICTED_CHARS:
            if ss.find(rs) > -1:
                ss = ss.replace(rs, symb)
        ss = ev_tools.change_spaces(ss, "_")
        ss = re.sub(u"([\u1D10-\u1DEA\u2E80-\u2FD5\u3000-\u303F\u30A0-\u30FF\u31F0-\u31FF\u3220-\u3243\u3280-\u337F\
        \u3400-\u4DB5\u4e00-\u9fa5\uF900-\uFA6A\uFF5F-\uFF9F])", "", ss)
        return ss

    def get_stream_name(idxx, mediatype='video', maxres=False):
        acookies_file = prepare_cookies_file(os.getpid())
        if mediatype == 'video':
            if maxres is True:
                ss = GET_MAX_VIDEO_NAME.format(acookies_file, idxx)
            else:
                ss = GET_HD_VIDEO_NAME.format(acookies_file, idxx)
        else:
            ss = GET_AUDIO_NAME.format(acookies_file, idxx)
        dd = subprocess.check_output(ss, shell=True).decode('utf-8').strip()
        if len(dd) > 0:
            print(dd)
            ds = dd.split("@@@")
            max_title_len = 124 - (25 + len(ds[0]))
            ds[0] = replace_restricted_chars(ds[0].encode("utf-8").decode())
            ds[0] = ev_tools.cut_spaces(ds[0], symbol="_")
            ds[1] = replace_restricted_chars(ds[1].encode("utf-8").decode())
            ds[1] = ev_tools.cut_spaces(ds[1], symbol="_")
            if len(ds[1]) > max_title_len:
                ds[1] = ds[1][0:max_title_len - 5]
            dd = ds[0] + "_-_" + ds[1] + ds[2]
            if mediatype == 'video':
                dd += ".mp4"
            elif mediatype == 'audio' and maxres is True:
                dd += ".flac"
            elif mediatype == 'audio' and maxres is False:
                dd += ".mp3"
        else:
            dd = "unknown media file"
        return dd

    def send_filename_to_main(aname, auuid, ajid):
        alock.acquire()
        astatus.put({"unit": "dl_worker",
                     "state": "set_url_filename",
                     "pid": os.getpid(),
                     "uuid": auuid,
                     "job_id": ajid,
                     "filename": aname
                     })
        alock.release()

    def dump_ytdlp_command(command="", y_id="NOID"):
        if len(command) > 0:
            aname = ev_tools.slash_processing(os.path.abspath(os.path.dirname(__file__)).replace("ytd_modules", "")+"/"+aconfig["dumps_savepath"]+"/command_{}_dump.txt".format(y_id))
            ev_tools.save_json_data(command, aname)

    time.sleep(1)
    if not jobs.empty():
        if aconfig["test_mode"] is True:
            alog.log("DL_JOB_WORKER: Start processing queue. My PID is {}".format(os.getpid()))
        if aconfig["save_description"] is True:
            save_description = "--write-description"
        else:
            save_description = ""

        cookies_file = prepare_cookies_file(os.getpid())
        while not jobs.empty():
            nowt = datetime.datetime.now()
            alock.acquire()
            job = jobs.get()
            alock.release()
            try:
                alog.log("DL_JOB_WORKER: PID: {}. Downloading file YoutubeID: {}  maxResolution: {}".format(os.getpid(), job["yt_id"], job["maxResolution"]))
                name = get_stream_name(job["yt_id"], job["mediatype"], job["maxResolution"])
                dl_result = "url_dl_complete"
                if job["mediatype"] == "audio":
                    path = check_save_path(aconfig["audio_save_path"])
                    job["filename"] = name
                    if job["maxResolution"] is True:
                        cmd = MAX_RES_AUDIO_CMD.format(cookies_file,
                                                       save_description,
                                                       path,
                                                       name,
                                                       aconfig["max_worker_bandwidth"],
                                                       job["yt_id"])
                    else:
                        cmd = HD_RES_AUDIO_CMD.format(cookies_file,
                                                      save_description,
                                                      path,
                                                      job["filename"],
                                                      aconfig["max_worker_bandwidth"],
                                                      job["yt_id"])

                    send_filename_to_main(job["filename"], job["uuid"], job["id"])
                    if aconfig["test_dump_json"] is True:
                        backup_json_stream_data(job["yt_id"], path, job["filename"])
                    if aconfig["test_dump_ytdlp_cmd"] is True:
                        dump_ytdlp_command(cmd, job["uuid"])
                    rr = subprocess.check_output(cmd, shell=True)
                    if len(rr) == 0:
                        alog.log("Downloaded and saved new audio file. URL ID: {}".format(job["yt_id"]))
                        print_completed_job(job, nowt, aresult=dl_result)
                    if len(rr) > 0:
                        alog.log("Error downloading or processing file. URL ID: {}".format(job["yt_id"]))
                        dl_result = "url_dl_error"
                        print_completed_job(job, nowt, aresult=dl_result, arestart=False, anotify=True, aaction="delete_job")

                if job["mediatype"] == "video":
                    path = check_save_path(aconfig["video_save_path"])
                    job["filename"] = name
                    if job["maxResolution"] is True:
                        cmd = MAX_RES_VIDEO_CMD.format(cookies_file,
                                                       save_description,
                                                       path,
                                                       job["filename"],
                                                       aconfig["max_worker_bandwidth"],
                                                       job["yt_id"])
                    else:
                        cmd = HD_RES_VIDEO_CMD.format(cookies_file,
                                                      save_description,
                                                      path,
                                                      job["filename"],
                                                      aconfig["max_worker_bandwidth"],
                                                      job["yt_id"])

                    send_filename_to_main(job["filename"], job["uuid"], job["id"])
                    if aconfig["test_dump_json"] is True:
                        backup_json_stream_data(job["yt_id"], path, job["filename"])
                    if aconfig["test_dump_ytdlp_cmd"] is True:
                        dump_ytdlp_command(cmd, job["uuid"])
                    rr = subprocess.check_output(cmd, shell=True).decode()
                    if len(rr) == 0 and aconfig["test_mode"] is True:
                        alog.log("Complete downloading and saving new media file. URL ID: {}".format(job["yt_id"]))
                        print_completed_job(job, nowt, aresult="url_dl_complete")
                    if len(rr) > 0 and aconfig["test_mode"] is True:
                        alog.log("Error downloading or processing media file. URL ID: {}".format(job["yt_id"]))
                        print_completed_job(job, nowt, aresult="url_dl_error", arestart=False, anotify=True, aaction="delete_job")

            except Exception:
                alog.log_err()
                alog.log("DL_WORKER: Exception in main cycle. See detailed error log")
                ress = {
                    "unit": "dl_worker",
                    "state": "exception",
                    "pid": os.getpid(),
                    "uuid": job["uuid"],
                    "restart": True,
                    "action": "restart_job",
                    "notify": False
                }
                alock.acquire()
                astatus.put(ress)
                alock.release()
    close_worker()


# --------------------------------------------------------------------------------------------------
class WorkerManager(object):
    """docstring"""

    def __init__(self, aconfig,
                 alock: multiprocessing.Lock,
                 astatus: multiprocessing.Queue,
                 ajobs: multiprocessing.Queue,
                 worker_task,
                 timeout=120
                 ):
        """Constructor"""
        self.max_workers = aconfig["max_workers"]
        self.workers = list()
        self.pids = list()
        self.jobs = ajobs
        self.status = astatus
        self.lock = alock
        self.worker_task = worker_task
        self.kill_timeout = timeout
        self.config = aconfig
        # Инициализация фиксированного списка воркеров
        for i in range(self.max_workers):
            self.pids.append(0)
        for i in range(self.max_workers):
            worker = self.create_new_worker()
            self.workers.append(worker)
        # Количество активных воркеров
        self.workers_alive = 0
        self.workers_pid_str = ""
        # lat - Last Active Time - последнее время активности воркера.
        self.lat = []
        for i in range(self.max_workers):
            self.lat.append(datetime.datetime.fromordinal(1))

    def create_new_worker(self, ):
        worker = multiprocessing.Process(target=self.worker_task, args=(self.config, self.lock, self.jobs, self.status))
        return worker

    def proc_start(self, ):
        """
        Start the Worker
        """
        # Запуск нового воркера - проверяем с начала фиксированого списка(pids) если есть остановленные воркеры(=0).
        # Если число воркеров превышает максимальное, то игнорируем.
        # После запуска воркера обновляем workers_alive и соответствующий элемент в pids

        flag = False
        for i in range(len(self.pids)):
            if self.pids[i] == 0:
                self.workers[i] = self.create_new_worker()
                self.workers[i].start()
                self.pids[i] = self.workers[i].pid
                self.workers_alive += 1
                self.lat[i] = datetime.datetime.fromordinal(1)
                flag = True
                break
        return flag

    def proc_stop(self, pid_to_stop):
        """
        Stop the Worker!
        """
        flag = False
        for i in range(len(self.pids)):
            if self.pids[i] == pid_to_stop:
                self.workers[i].kill()
                self.pids[i] = 0
                self.lat[i] = datetime.datetime.fromordinal(1)
                if self.workers_alive > 0:
                    self.workers_alive -= 1
                flag = True
                break
        return flag

    def killall(self):
        """
        Kill the Worker!
        """
        for i in range(self.max_workers):
            if self.pids[i] != 0:
                self.workers[i].kill()
                self.pids[i] = 0
                self.lat[i] = datetime.datetime.fromordinal(1)
        self.workers_alive = 0

    def update(self):
        """
        Update and close workers
        """
        # убиваем неактивных более воркеров
        now = datetime.datetime.now()
        for i in range(self.max_workers):
            if self.lat[i] is not datetime.datetime.fromordinal(1):
                if (self.pids[i] != 0) and (self.workers[i].is_alive() is False) and \
                        (now - self.lat[i] >= datetime.timedelta(seconds=self.kill_timeout)):
                    self.workers[i].kill()
                    self.pids[i] = 0
                    if self.workers_alive > 0:
                        self.workers_alive -= 1
                    self.lat[i] = datetime.datetime.fromordinal(1)

        # геренируем строку пидов всех воркеров
        self.workers_pid_str = ""
        for i in range(self.max_workers):
            self.workers_pid_str = self.workers_pid_str + str(self.pids[i]) + " "
