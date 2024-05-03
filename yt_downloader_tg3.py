#!/usr/bin/python3

import os
from ytd_modules import mytinylogger, ev_tools, ytd_utils, ytd_worker, ytd_telebot
import multiprocessing
import time
import datetime
import subprocess

# инстансы очередей
t_queue = multiprocessing.Queue()
t_control = multiprocessing.Queue()
w_queue = multiprocessing.Queue()
status = multiprocessing.Queue()
lock = multiprocessing.Lock()

# константы
CONFIG_FILENAME = "./yt_downloader.config"
# --windows-filenames
HOST_NAME = subprocess.check_output("hostname", shell=True).decode('utf-8').strip()

# конфиг и логгер
config = ev_tools.load_json_data(CONFIG_FILENAME)
if config["test_mode"] is True:
    config["name"] += "-test"
    config["NAME"] += "-test"

if config["local_savepath"] != "":
    config["save_path"] = config["local_savepath"]
    config["video_save_path"] = ev_tools.slash_processing(os.path.abspath(os.path.dirname(__file__)) + config["save_path"] + "/video")
    config["audio_save_path"] = ev_tools.slash_processing(os.path.abspath(os.path.dirname(__file__)) + config["save_path"] + "/audio")
elif config["remote_savepath"] != "":
    config["save_path"] = config["remote_savepath"]
    config["video_save_path"] = ev_tools.slash_processing(config["save_path"] + "/video")
    config["audio_save_path"] = ev_tools.slash_processing(config["save_path"] + "/audio")

else:
    config["save_path"] = "/data/storage"

if os.path.exists(config["video_save_path"]) is False:
    os.mkdir(config["video_save_path"])
if os.path.exists(config["audio_save_path"]) is False:
    os.mkdir(config["audio_save_path"])


# ---------------------------------------------------------------------------------------------------------------
# ---------------------------------------------------------------------------------------------------------------
# ---------------------------------------------------------------------------------------------------------------

class MyProcessManager(object):
    """My Gateway Process Manager"""

    # ==========================
    def __init__(self, aconfig,
                 alog: mytinylogger,
                 alock: multiprocessing.Lock,
                 astatus: multiprocessing.Queue,
                 aw_queue: multiprocessing.Queue, dl_worker_task,
                 at_control: multiprocessing.Queue,
                 at_queue: multiprocessing.Queue, t_worker_task):
        """Constructor"""
        self.log = alog
        self.lock = alock   # локер очередей
        self.status = astatus  # входящая очередь статусных сообщений от емейл-сервера и телеграм-бота
        self.jobs = aw_queue  # исходящая очередь заданий воркерам на закачку
        self.t_control = at_control  # исходящая очередь проверки состояния телеграм-бота
        self.t_queue = at_queue  # исходящая очередь сообщений для телеграм-бота
        self.config = aconfig
        self.t_worker_task = t_worker_task   # ссылка на процедуру воркера телебота
        self.t_worker = None  # инстанс воркера телебота
        self.worker_check_delay = self.config["ping_time"]  # время в минутах между проверками Емейлера и Телебота
        self.worker_fail_ping_count = self.config["fail_ping_count"]
        self.t_worker_next_check_time = datetime.datetime.now() + datetime.timedelta(minutes=self.worker_check_delay)
        self.t_worker_ping_count = 0
        self.url_cache = list()
        self.job_cache = list()
        self.download_timeout = 300
        self.dl_worker_task = dl_worker_task  # ссылка на процедуру воркера-загрузчика

        if os.path.exists(self.config["url_cache_file"]) is False:
            ev_tools.save_json_data(self.url_cache, self.config["url_cache_file"])

        self.workermanager = ytd_worker.WorkerManager(self.config,
                                                      alock=self.lock,
                                                      astatus=self.status,
                                                      ajobs=self.jobs,
                                                      worker_task=self.dl_worker_task,
                                                      timeout=self.download_timeout)

    # ==========================
    def create_telebot(self, ):
        self.t_worker = multiprocessing.Process(target=self.t_worker_task, args=(self.config, self.log, self.lock, self.status, self.t_control, self.t_queue))

    def stop(self, ):
        self.t_worker.kill()
        self.workermanager.killall()

    def delete_url_from_cache(self, auuid, is_done=True):
        _, url_idx, urr = ev_tools.check_list_by_param(self.url_cache, "uuid", auuid)
        if url_idx > -1 and urr is not None:
            self.url_cache.pop(url_idx)
            job_idx = len(self.job_cache) - 1
            while job_idx > -1:
                ret_status = "UNKNOWN_UUID"
                u_idx = len(self.job_cache[job_idx]["urls"]) - 1
                url_found = False
                while u_idx > -1:
                    if self.job_cache[job_idx]["urls"][u_idx]["uuid"] == auuid:
                        if is_done is True:
                            self.job_cache[job_idx]["done_urls"].append(self.job_cache[job_idx]["urls"][u_idx])
                            self.job_cache[job_idx]["urls"].pop(u_idx)
                            ret_status = "ADD_DONE_URL"
                            url_found = True
                            break
                        else:
                            self.job_cache[job_idx]["bad_urls"].append(self.job_cache[job_idx]["urls"][u_idx])
                            self.job_cache[job_idx]["urls"].pop(u_idx)
                            ret_status = "ADD_BAD_URL"
                            url_found = True
                            break
                    u_idx -= 1

                if url_found is True and self.job_cache[job_idx]["urls_given"] == len(self.job_cache[job_idx]["done_urls"]) + len(self.job_cache[job_idx]["bad_urls"]):
                    ret_status = "JOB_COMPLETE"
                    job = self.job_cache[job_idx]
                    self.job_cache.pop(job_idx)
                    return ret_status, job

                if url_found is True and ret_status in ["ADD_DONE_URL", "ADD_BAD_URL"]:
                    job = self.job_cache[job_idx]
                    return ret_status, job
                job_idx -= 1
        else:
            return "UNKNOWN_UUID", None

    def generate_status_answer_body(self, userid, chatid):
        urls_work = list()
        for urr in self.url_cache:
            if urr["id"]["chat_id"] == chatid and urr["id"]["user_id"] == userid:
                if urr["state"] == "downloading":
                    urls_work.append({"id": urr["yt_id"], "state": "(загружается)"})
                elif urr["state"] == "NEW_URL":
                    urls_work.append({"id": urr["yt_id"], "state": "(в очереди)"})
        for jobb in self.job_cache:
            if jobb["id"]["chat_id"] == chatid and jobb["id"]["user_id"] == userid:
                if len(jobb["done_urls"]) > 0:
                    for urr in jobb["done_urls"]:
                        urls_work.append({"id": urr["yt_id"], "state": "(успешно загружeн)"})
                if len(jobb["bad_urls"]) > 0:
                    for urr in jobb["bad_urls"]:
                        urls_work.append({"id": urr["yt_id"], "state": '(ОШИБКА ЗАГРУЗКИ)'})
        body = ""

        if len(urls_work) == 0:
            body = '<strong>У Вас нет активных заданий на скачивание с Youtube</strong>'
        else:
            body += '<strong>Состояние Ваших ссылок Youtube:</strong>\n'
            body += '<em style="white-space:normal">'
            idx = 1
            for uw in urls_work:
                body += "<b>{}. {}</b>     {}\n".format(str(idx), str(uw["id"]), uw["state"])
                idx += 1
            body += '</em>'

        return body

    def clear_all_jobs(self):
        if len(self.job_cache) and len(self.url_cache) > 0:
            for jb in self.job_cache:
                abody = "<strong>К сожалению, администратор бота запросил полную очистку очередей загрузки и следующие активные закачки Youtube URL не будут выполнены:</strong>\n"
                abody += '<em style="white-space:normal">'
                for ur in jb["urls"]:
                    abody += "{}, ".format(ur["yt_id"])
                abody = abody[:-2] + "</em>"

                if len(jb["done_urls"]) > 0:
                    abody += "<strong>Успешно скачанные до очистки Youtube URL: </strong>\n"
                    abody += '<em style="white-space:normal">'
                    for ur in jb["done_urls"]:
                        abody += "{}, ".format(ur["yt_id"])
                    abody = abody[:-2] + "</em>"

                if len(jb["bad_urls"]) > 0:
                    abody += "<strong>Неудачно скачанные до очистки Youtube URL:</strong>\n"
                    abody += '<em style="white-space:normal">'
                    for ur in jb["bad_urls"]:
                        abody += "{}, ".format(ur["yt_id"])
                    abody = abody[:-2] + "</em>"
                self.lock.acquire()
                self.t_queue.put({"unit": "t_worker",
                                  "state": "t_send",
                                  "id": jb["id"],
                                  "uuid": "0",
                                  "body": abody
                                  })
                self.lock.release()

            body = "<strong>Успешно удалено {} заданий от пользователей с общим количеством в {} ссылок.</strong> \n" \
                   '<em style="white-space:normal">Оповещения об очистке заданий были разосланы всем пользователям</em>'.format(len(self.job_cache), len(self.url_cache))
            self.url_cache = list()
            self.job_cache = list()
            self.workermanager.killall()
            ev_tools.save_json_data(self.url_cache, config["url_cache_file"])
            ev_tools.save_json_data(self.job_cache, config["jobs_cache_file"])
            return body
        else:
            return ""

    def run(self, ):
        try:
            self.create_telebot()
            self.t_worker.start()
            # подгружаем из файла бэкап кэша сообщений и если он не пуст, грузим их в очередь заданий.
            # Значит бот был остановлен с неполностью отправленной очередью сообщений в прошлом запуске
            self.url_cache = ev_tools.load_json_data(self.config["url_cache_file"])
            if len(self.url_cache) > 0:
                if self.config["test_mode"] is True:
                    print("Job cache is not empty. Add jobs in queue")
                self.job_cache = ev_tools.load_json_data(self.config["jobs_cache_file"])
                for res in self.url_cache:
                    self.lock.acquire()
                    self.jobs.put(res)
                    self.lock.release()
                    self.workermanager.proc_start()

            # Главный цикл
            while True:
                t_worker_restart = False
                # читаем входящую очередь статусов от телебота и воркеров
                if not self.status.empty():
                    while not self.status.empty():
                        self.lock.acquire()
                        st = self.status.get()
                        self.lock.release()
                        # От подпроцессов пришло состояние свершившегося исключения
                        if st["state"] == "exception":
                            # Телебот прислал исключение
                            if st["unit"] == "t_worker":
                                t_worker_restart = True

                            # Воркер прислал исключение
                            elif st["unit"] == "dl_worker":
                                # Воркер прислал команду на удаление URL так как произошла проблема в его закачке
                                if st["action"] == "delete_job":
                                    self.delete_url_from_cache(st["job"]["uuid"], is_done=False)
                                # Воркер прислал команду на перезапуск закачки URL с uuid так как что-то произошло не по вине закачивалки.
                                elif st["action"] == "restart_job":
                                    for jb in self.url_cache:
                                        if jb["uuid"] == st["uuid"]:
                                            self.lock.acquire()
                                            self.jobs.put(jb)
                                            self.lock.release()
                                            self.workermanager.proc_stop(st["pid"])
                                            time.sleep(0.5)
                                            self.workermanager.proc_start()
                                            if self.config["test_mode"] is True:
                                                print("WORKERMANAGER: Restart job ID {} after downloader worker crash".format(st["uuid"]))
                                            break
                            self.log.log('Exception in {} subproccess, location - {}. See error log file for details'.format(st["unit"], st["location"]))
                        # отвечаем на PONG-запрос от внешних подпроцессов
                        elif st["state"] == "pong":
                            if st["unit"] == "t_worker":
                                self.t_worker_ping_count = 0
                                self.t_worker_next_check_time = datetime.datetime.now() + datetime.timedelta(minutes=self.worker_check_delay)
                                if self.config["test_mode"] is True:
                                    print("Telebot PONG recieved")
                        # Добавляем пришедшее задание на скачивание от телебота
                        elif st["state"] == "t_add_urls":
                            if self.config["test_mode"] is True:
                                print("From TeleBot recieve job and URL list. Add to cache!")
                            self.job_cache.append(st)
                            for url in st["urls"]:
                                self.url_cache.append(url)
                                self.lock.acquire()
                                self.jobs.put(url)
                                self.lock.release()
                                self.workermanager.proc_start()
                            ev_tools.save_json_data(self.url_cache, config["url_cache_file"])
                            ev_tools.save_json_data(self.job_cache, config["jobs_cache_file"])
                        # отвечаем на пришедшее от воркера сообщение об удачной закачке URL
                        elif st["state"] == "set_url_filename":
                            _, idx_url, _ = ev_tools.check_list_by_param(self.url_cache, "uuid", st["uuid"])
                            if idx_url > -1:
                                self.url_cache[idx_url]["filename"] = st["filename"]
                                self.url_cache[idx_url]["state"] = "downloading"

                            _, idx_job, _ = ev_tools.check_list_by_param(self.job_cache, "id", st["job_id"])
                            if idx_job > -1:
                                _, idx_job_url, _ = ev_tools.check_list_by_param(self.job_cache[idx_job]["urls"], "uuid", st["uuid"])
                                if idx_job_url > -1:
                                    self.job_cache[idx_job]["urls"][idx_job_url]["filename"] = st["filename"]
                                    self.job_cache[idx_job]["urls"][idx_job_url]["state"] = "downloading"

                            ev_tools.save_json_data(self.url_cache, config["url_cache_file"])
                            ev_tools.save_json_data(self.job_cache, config["jobs_cache_file"])

                        elif st["state"] in ["url_dl_failed", "url_dl_complete"]:
                            is_done = True
                            if st["state"] == "url_dl_failed":
                                if self.config["test_mode"] is True:
                                    print("URL Download failed UUID - {}".format(st['job']["uuid"]))
                                is_done = False
                            elif st["state"] == "url_dl_complete":
                                if self.config["test_mode"] is True:
                                    print("URL Download complete UUID - {}".format(st['job']["uuid"]))
                            state, jobb = self.delete_url_from_cache(st["job"]["uuid"], is_done=is_done)
                            ev_tools.save_json_data(self.url_cache, config["url_cache_file"])
                            ev_tools.save_json_data(self.job_cache, config["jobs_cache_file"])
                            if state == "JOB_COMPLETE" and jobb is not None:
                                body = ytd_utils.generate_answer_body(jobb)
                                self.lock.acquire()
                                self.t_queue.put({"unit": "t_worker",
                                                  "state": "t_send",
                                                  "id": st["job"]["id"],
                                                  "uuid": st["job"]["uuid"],
                                                  "body": body
                                                  })
                                self.lock.release()

                        # реагируем на желание пользователя узнать о статусе его задания
                        elif st["state"] == "t_get_job_status":
                            body = self.generate_status_answer_body(st["id"]["user_id"], st["id"]["chat_id"])
                            self.lock.acquire()
                            self.t_queue.put({"unit": "t_worker",
                                              "state": "t_send",
                                              "id": st["id"],
                                              "uuid": "0",
                                              "body": body
                                              })
                            self.lock.release()

                        # реагируем на желание админа очистить все очереди
                        elif st["state"] == "t_clear_all_jobs":
                            body = self.clear_all_jobs()
                            if len(body) > 0:
                                self.lock.acquire()
                                self.t_queue.put({"unit": "t_worker",
                                                  "state": "t_send",
                                                  "id": st["id"],
                                                  "uuid": "0",
                                                  "body": body
                                                  })
                                self.lock.release()

                # пингаем субпроцессы если пришло время
                now = datetime.datetime.now()
                if now >= self.t_worker_next_check_time:
                    if config["test_mode"] is True:
                        print("Send TeleBot PING")
                    self.lock.acquire()
                    self.t_control.put({"unit": "t_worker", "state": "ping"})
                    self.lock.release()
                    self.t_worker_ping_count += 1
                    self.t_worker_next_check_time = datetime.datetime.now() + datetime.timedelta(minutes=self.worker_check_delay)

                if not self.t_worker.is_alive() or t_worker_restart is True or self.t_worker_ping_count > self.worker_fail_ping_count:
                    if config["test_mode"] is True:
                        print(f"T_WORKER  {self.t_worker.is_alive()}   {t_worker_restart}   {self.t_worker_ping_count}")
                    self.t_worker.kill()
                    time.sleep(3)
                    self.create_telebot()
                    self.t_worker.start()
                    self.t_worker_ping_count = 0
                    if config["test_mode"] is True:
                        print("Restart TelegramBot")
                self.workermanager.update()
                if self.config["test_mode"]:
                    print("Active workers: {} of {}. Jobs in Queue: {}".format(self.workermanager.workers_alive,
                                                                               self.workermanager.max_workers,
                                                                               len(self.url_cache)))
                time.sleep(config["sleep_time"])
        except KeyboardInterrupt:
            return -1
        except BaseException:
            self.log.log("Error in Process Manager run cycle. See detailed error log")
            self.log.log_err()
            return -2


# --------------------------------------------------------------------------------------------------
# --------------------------------------------------------------------------------------------------
# --------------------------------------------------------------------------------------------------
# --------------------------------------------------------------------------------------------------
# --------------------------------------------------------------------------------------------------
# --------------------------------------------------------------------------------------------------
# --------------------------------------------------------------------------------------------------

if __name__ == '__main__':
    log = mytinylogger.MyTinyLogger(config["log_logfile"], config["log_errfile"], 400000, 8)
    log.log("Service {} has started".format(config["NAME"]))
    myPM = MyProcessManager(config, log, lock, status, w_queue, ytd_worker.download_worker_task, t_control, t_queue, ytd_telebot.telebot_worker_task)
    cc = myPM.run()
    if cc == -1:
        log.log("Daemon {} stopped by keyboard".format(config["NAME"]))
    elif cc == -2:
        log.log("Daemon {} stopped by exception. See error log file for detals".format(config["NAME"]))
    myPM.stop()
