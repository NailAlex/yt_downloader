#!/usr/bin/python3

# Base components import
import asyncio
import multiprocessing
from sys import platform
import uuid
from ytd_modules import mytinylogger, ev_tools, ytd_utils
import datetime
import subprocess

# Aiogram components import
import aiogram
import aiogram.exceptions
from aiogram import Bot, Dispatcher, types
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import Message, ReplyKeyboardMarkup, KeyboardButton, FSInputFile
from aiogram.enums.parse_mode import ParseMode
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.context import FSMContext
from aiogram.filters import Command, CommandStart


# ==============================================================================================================
# Базовая функция-контейнер для отдельного процесса где будет крутиться телебот
def telebot_worker_task(aconfig,
                        alog: mytinylogger.MyTinyLogger,
                        alock: multiprocessing.Lock,
                        astatus: multiprocessing.Queue,
                        control: multiprocessing.Queue,
                        queue: multiprocessing.Queue):

    # создаем инстансы всякого разного
    flag_maxres = {"locked": False, "media": "", "maxResolution": False}
    bot = Bot(token=aconfig["TOKEN"])
    print("Create Bot complete")
    dp = Dispatcher(storage=MemoryStorage())
    print("Create Dispatcher complete")

    async def bot_start():
        await bot.delete_webhook(drop_pending_updates=True)
        alock.acquire()
        astatus.put({"unit": "t_worker",
                     "state": "STARTED"
                     })
        alock.release()
        if aconfig.get("CONTROL_CHANID") is not None and aconfig["CONTROL_CHANID"] != 0:
            ev_tools.send_simple_telegram_msg(log=alog, token=aconfig["TOKEN"],
                                              channel_id=aconfig["CONTROL_CHANID"],
                                              text="TG-YT-Downloader "'"{}"'" запущен\n"
                                                   "Сервер: {} (ОС: {})\n".format(aconfig["NAME"],
                                                                                  subprocess.check_output("hostname", shell=True).decode('utf-8').strip(),
                                                                                  platform))

    class NewURL(StatesGroup):
        url = State()

    def get_id(message):
        return {"msg_id": message.message_id, "chat_id": message.chat.id, "user_id": message.from_user.id}

    def add_urls_to_cache(message, isVideo=True, maxResolution=False):
        try:
            if message.text == "":
                return
            urls = list()
            splits = message.text.split("\n")
            if isVideo is True:
                mediatype = "video"
            else:
                mediatype = "audio"

            added_urls = ""
            for sp in splits:
                url = sp.strip()
                y_id = ytd_utils.extract_video_id(url)
                sp = {"uuid": uuid.uuid4().hex,
                      "yt_id": y_id,
                      "id": get_id(message),
                      "state": "NEW_URL",
                      "restart_count": 0,
                      "mediatype": mediatype,
                      "maxResolution": maxResolution,
                      "filename": ""
                      }
                urls.append(sp)
                added_urls += y_id + ', '
            ress = {"state": "t_add_urls",
                    "id": get_id(message),
                    "urls": urls,
                    "urls_given": len(urls),
                    "bad_urls": list(),
                    "done_urls": list(),
                    }
            alog.log("User ID {} add list of URLs for downloading: {}".format(message.from_user.id, added_urls[:-2]))
            alock.acquire()
            astatus.put(ress)
            alock.release()
            return ress
        except TypeError or KeyError or ValueError as exx:
            alog.log("Exception in location {}. See detailed error log for details".format("add_urls_to_cache"))
            alog.log_err()
            alock.acquire()
            astatus.put({"unit": "t_worker",
                         "state": "exception",
                         "restart": False,
                         "description": exx.message,
                         "location": "add_urls_to_cache"
                         })
            alock.release()

    # --------------------------------
    @dp.message(CommandStart())
    async def cmd_start(message: Message):
        """
        This handler will be called when user sends `/start` or `/help` command
        """
        # is_admin = await check_is_admin(message)
        kb = [
                 [
                  KeyboardButton(text="/dlvideo"),          # закачка видео в разрешении 1080р (стандарт)
                  KeyboardButton(text="/dlvideomax"),       # закачка видео в максимальном разрешении 4K
                  KeyboardButton(text="/dlaudio"),          # закачка аудио в формате MP3 320k (стандарт)
                  KeyboardButton(text="/dlaudiomax")        # закачка аудио в максимальном качестве в формате FLAC 940k
                 ],
                 [
                  KeyboardButton(text="/info"),             # получить инфо о боте и канале
                  KeyboardButton(text="/status")            # получить статус активных закачек от пользователя
                 ]
            ]
        if 0 < aconfig["CONTROL_CHANID"] == message.chat.id:     # действия только для админа
            kb.append([types.KeyboardButton(text="/logs"),       # получить логи
                       types.KeyboardButton(text="/clear")])     # очистить ВСЕ очереди закачки и остановить воркеры

        keyboard = ReplyKeyboardMarkup(keyboard=kb, resize_keyboard=True, one_time_keyboard=True, input_field_placeholder='Выбери команду')
        alog.log("User ID {} call /start menu".format(message.from_user.id))
        try:
            await message.reply("Привет!\nВыбери действие", reply_markup=keyboard)
        except aiogram.exceptions.TelegramNetworkError or aiogram.exceptions.TelegramForbiddenError or aiogram.exceptions.TelegramBadRequest or TypeError or KeyError as exx:
            alog.log("Exception in location {}. See detailed error log for details".format("cmd_start"))
            alog.log_err()
            alock.acquire()
            astatus.put({"unit": "t_worker",
                         "state": "exception",
                         "restart": False,
                         "description": exx.message,
                         "location": "cmd_start"
                         })
            alock.release()

    # --------------------------------
    @dp.message(Command("info"))
    async def cmd_info(message: Message):
        try:
            await bot.send_message(message.chat.id, ytd_utils.get_info(aconfig["ether_iface_name"], aconfig["name"], message.chat.id))
            alog.log("User ID {} call info sheet".format(message.from_user.id))

        except aiogram.exceptions.TelegramNetworkError or aiogram.exceptions.TelegramForbiddenError or aiogram.exceptions.TelegramBadRequest or TypeError or KeyError as exx:
            alog.log("Exception in location {}. See detailed error log for details".format("cmd_info"))
            alog.log_err()
            alock.acquire()
            astatus.put({"unit": "t_worker",
                         "state": "exception",
                         "restart": False,
                         "description": exx.message,
                         "location": "cmd_info"
                         })
            alock.release()

    # --------------------------------
    @dp.message(Command("logs"))
    async def cmd_logs(message: Message):
        if 0 < aconfig["CONTROL_CHANID"] == message.chat.id:
            try:
                lg1 = FSInputFile(aconfig["log_logfile"])
                lg2 = FSInputFile(aconfig["log_errfile"])
                await bot.send_document(message.chat.id, reply_to_message_id=message.message_id, document=lg1)
                await bot.send_document(message.chat.id, reply_to_message_id=message.message_id, document=lg2)
                alog.log("User ID {} send request for getting logs".format(message.from_user.id))
            except aiogram.exceptions.TelegramNetworkError or aiogram.exceptions.TelegramForbiddenError or aiogram.exceptions.TelegramBadRequest or TypeError or KeyError as exx:
                alog.log("Exception in location {}. See detailed error log for details".format("cmd_logs"))
                alog.log_err()
                alock.acquire()
                astatus.put({"unit": "t_worker",
                             "state": "exception",
                             "restart": False,
                             "description": exx.message,
                             "location": "cmd_logs"
                             })
                alock.release()

    # --------------------------------
    @dp.message(Command("status"))
    async def cmd_status(message: Message):
        sp = {"unit": "t_worker",
              "id": {"msg_id": message.message_id, "chat_id": message.chat.id, "user_id": message.from_user.id},
              "state": "t_get_job_status"
              }
        alock.acquire()
        astatus.put(sp)
        alock.release()
        alog.log("User ID {} send request for getting status".format(message.from_user.id))

    # --------------------------------
    @dp.message(Command(commands=['dlvideo', 'dlvideomax', 'dlaudio', 'dlaudiomax']))
    async def cmd_dlurl(message: types.Message, state: FSMContext):
        nonlocal flag_maxres
        try:
            if flag_maxres["locked"] is True:
                await bot.send_message(message.chat.id, text='Вы не ввели ссылки по предыдущей команде', reply_to_message_id=message.message_id, disable_web_page_preview=True)
                return
            flag_maxres["locked"] = True
            if message.text in ["/dlvideo", "/dlvideomax"]:
                flag_maxres["media"] = "video"
                if message.text == "/dlvideomax":
                    flag_maxres["maxResolution"] = True
            elif message.text in ["/dlaudio", "/dlaudiomax"]:
                flag_maxres["media"] = "audio"
                if message.text == "/dlaudiomax":
                    flag_maxres["maxResolution"] = True
            await state.set_state(NewURL.url)
            await bot.send_message(message.chat.id, text='Введите список ссылок на скачивание:', reply_to_message_id=message.message_id, disable_web_page_preview=True)
        except aiogram.exceptions.TelegramNetworkError or aiogram.exceptions.TelegramForbiddenError or aiogram.exceptions.TelegramBadRequest or TypeError or KeyError as exx:
            alog.log("Exception in location {}. See detailed error log for details".format("cmd_dlurl"))
            alog.log_err()
            alock.acquire()
            astatus.put({"unit": "t_worker",
                         "state": "exception",
                         "restart": False,
                         "description": exx.message,
                         "location": "cmd_dlurl"
                         })
            alock.release()

    # --------------------------------
    @dp.message(NewURL.url)
    async def add_urls(message: types.Message, state: FSMContext):
        nonlocal flag_maxres
        if message.text.find("https://") == -1:
            return
        if flag_maxres["locked"] is True:
            if flag_maxres["media"] == 'video':
                is_video = True
            else:
                is_video = False
            resss = add_urls_to_cache(message, isVideo=is_video, maxResolution=flag_maxres["maxResolution"])
            flag_maxres["locked"] = False
            if flag_maxres["media"] == "video":
                media_type_str = "видео"
            else:
                media_type_str = "аудио"
            try:
                await bot.send_message(message.chat.id, text='Принято в работу {} ссылок на скачивание {}.\n'
                                                             'Команда /status покажет данные по текущей очереди заданий'.format(media_type_str, len(resss["urls"])),
                                                        reply_to_message_id=message.message_id,
                                                        disable_web_page_preview=True)
                alog.log("User ID {} send request for download media by Youtube URLs".format(message.from_user.id))
            except aiogram.exceptions.TelegramNetworkError or aiogram.exceptions.TelegramForbiddenError or aiogram.exceptions.TelegramBadRequest or TypeError or KeyError as exx:
                alog.log("Exception in location {}. See detailed error log for details".format("cmd_add_urls"))
                alog.log_err()
                alock.acquire()
                astatus.put({"unit": "t_worker",
                             "state": "exception",
                             "restart": False,
                             "description": exx.message,
                             "location": "add_urls"
                             })
                alock.release()

        await state.clear()

    # --------------------------------
    @dp.message(Command("clear"))
    async def cmd_clear(message: Message):
        if 0 < aconfig["CONTROL_CHANID"] == message.chat.id:
            alog.log("Administrator ID {} send request for clear all queue jobs".format(message.from_user.id))
            alock.acquire()
            astatus.put({"unit": "t_worker",
                         "state": "t_clear_all_jobs",
                         "id": get_id(message)
                         })
            alock.release()

    # -------------------------------------------------------------------------------------------
    async def control_task(sleep_for):
        try:
            while True:
                if aconfig["test_mode"] is True:
                    print(f"Telebot waiting for jobs. Now {datetime.datetime.now()}")
                # получаем задание на отправку из очереди
                if not queue.empty():
                    while not queue.empty():
                        alock.acquire()
                        job = queue.get()
                        alock.release()
                        ares = await bot.send_message(chat_id=job["id"]["chat_id"],
                                                      text=job["body"],
                                                      reply_to_message_id=job["id"]["msg_id"],
                                                      disable_web_page_preview=True,
                                                      parse_mode=ParseMode.HTML
                                                      )
                        if ares is None:
                            alock.acquire()
                            astatus.put({"unit": "t_worker", "state": "t_send_fail", "uuid": job["uuid"]})
                            alock.release()
                        else:
                            if ares.message_id > 0:
                                alock.acquire()
                                astatus.put({"unit": "t_worker", "state": "t_send_ok", "uuid": job["uuid"]})
                                alock.release()

                if not control.empty():
                    while not control.empty():
                        alock.acquire()
                        req = control.get()
                        alock.release()
                        if req["unit"] == "t_worker" and req["state"] == "ping":
                            alock.acquire()
                            astatus.put({"unit": "t_worker", "state": "pong"})
                            alock.release()

                await asyncio.sleep(sleep_for)
        except aiogram.exceptions.TelegramNetworkError or aiogram.exceptions.TelegramForbiddenError or aiogram.exceptions.TelegramBadRequest or TypeError or KeyError as exx:
            alog.log("Exception in location {}. See detailed error log for details".format("control_task"))
            alog.log_err()
            alock.acquire()
            astatus.put({"unit": "t_worker",
                         "state": "exception",
                         "restart": True,
                         "description": exx.message,
                         "location": "control_task"
                         })
            alock.release()

    async def run_telebot():
        dp.startup.register(bot_start)
        print("Register Dispatcher startup procedures")
        await dp.start_polling(bot, close_bot_session=True)

# ======================================================================================================
#  ОСНОВНАЯ ЧАСТЬ МОДУЛЯ ТЕЛЕБОТА
# ======================================================================================================

    background_tasks = set()
    loop = asyncio.get_event_loop()
    asyncio.set_event_loop(loop)
    print("Create Loop complete")
    task = loop.create_task(control_task(3))
    print("Create background task complete")
    background_tasks.add(task)
    try:
        loop.run_until_complete(run_telebot())
    except aiogram.exceptions.TelegramNetworkError or aiogram.exceptions.TelegramForbiddenError or aiogram.exceptions.TelegramBadRequest or TypeError or KeyError as ex:
        alog.log_err()
        alock.acquire()
        astatus.put({"unit": "t_worker",
                     "state": "exception",
                     "restart": False,
                     "description": ex.message,
                     "location": "telebot_main"
                     })
        alock.release()
    finally:
        task.add_done_callback(background_tasks.discard)
        loop.close()
        print("Close Bot instances")
