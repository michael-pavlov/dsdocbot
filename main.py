# -*- coding: utf-8 -*-
# @Author Michael Pavlov

import logging
import time
import datetime
from flask import Flask, request
import mysql.connector
from mysql.connector import Error
from mysql.connector import pooling
from logging.handlers import RotatingFileHandler
# import config as config
from docxtpl import DocxTemplate, RichText, Listing
import telebot
import math
import os
import num2words
import calendar
import random
import sys

VERSION = "1.45"

# paths
TMP_PATH = "tmp_docs\\"
TEMPLATE_PATH = ""

ENT_EXP_REPORT_TEMPLATE_FILE = TEMPLATE_PATH + "ent_exp_template.docx"
ENT_EXP_MEMO_TEMPLATE_FILE = TEMPLATE_PATH + "ent_exp_memo_template.docx"
MONEY_TO_ACC_TEMPLATE_FILE = TEMPLATE_PATH + "money_to_account_template.docx"
MONEY_TO_PERSON_TEMPLATE_FILE = TEMPLATE_PATH + "money_to_person_template.docx"
REIMBURSEMENT_TEMPLATE_FILE = TEMPLATE_PATH + "reimbursement_template.docx"
TRACKLIST_TEMPLATE_FILE = TEMPLATE_PATH + "track_list_template.docx"
CMD_ACT_TEMPLATE_FILE = TEMPLATE_PATH + "cmd_act_template.docx"

class DOCBot:

    def __init__(self, env = 'heroku', mode = 'online', proxy=True):

        self.env = env

        self.logger = logging.getLogger("dsdoc_bot")
        self.logger.setLevel(logging.DEBUG)

        if self.env == 'heroku':
            handler = logging.StreamHandler(sys.stdout)
            # handler.setLevel(logging.DEBUG)
            formatter = logging.Formatter('%(name)s: %(levelname)s: %(message)s')
            handler.setFormatter(formatter)
            self.logger.addHandler(handler)

            self.TG_BOT_TOKEN = os.environ['TOKEN']
            self.HEROKU_NAME = "dsdocbot"
            self.DB_USER = os.environ['DB_USER']
            self.DB_PASSWORD = os.environ['DB_PASSWORD']
            self.DB_HOST = os.environ['DB_HOST']
            self.DB_PORT = os.environ['DB_PORT']
            self.DB_DATABASE = "bots"
            self.TMP_PATH = ""
            self.ADMIN_ID = os.environ['ADMIN_ID']

            self.GLOBAL_RECONNECT_COUNT = int(os.environ['GLOBAL_RECONNECT_COUNT'])

            self.bot = telebot.TeleBot(self.TG_BOT_TOKEN)

            # Настройка Flask
            self.server = Flask(__name__)
            self.TELEBOT_URL = 'telebot_webhook/'
            self.BASE_URL = "https://" + self.HEROKU_NAME + ".herokuapp.com/"

            self.server.add_url_rule('/' + self.TELEBOT_URL + self.TG_BOT_TOKEN, view_func=self.process_updates,
                                     methods=['POST'])
            self.server.add_url_rule("/", view_func=self.webhook)

        elif self.env == 'local':
            handler = RotatingFileHandler("dsdoc_bot.log", mode='a', encoding='utf-8', backupCount=5,
                                     maxBytes=16 * 1024 * 1024)
            formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
            handler.setFormatter(formatter)
            self.logger.addHandler(handler)

            self.TG_BOT_TOKEN = config.TG_BOT_TOKEN
            self.DB_USER = config.DB_USER
            self.DB_PASSWORD = config.DB_PASSWORD
            self.DB_HOST = config.DB_HOST
            self.DB_PORT = config.DB_PORT
            self.DB_DATABASE = config.DB_DATABASE
            self.TMP_PATH = config.TMP_PATH
            self.ADMIN_ID = config.ADMIN_ID

            self.GLOBAL_RECONNECT_COUNT = int(config.GLOBAL_RECONNECT_COUNT)

            self.bot = telebot.TeleBot(self.TG_BOT_TOKEN)
            if proxy:
                telebot.apihelper.proxy = config.PROXY
        else:
            print("Bot() Exit! Unknown environment:" + str(env))
            quit()

        # common operations
        self.reconnect_count = self.GLOBAL_RECONNECT_COUNT
        self.GLOBAL_RECONNECT_INTERVAL = 5
        self.RECONNECT_ERRORS = []

        self.MAIN_HELP_LINK = ""

        self.markup_commands = ["Представительские", "Возмещение расходов", "Выдача под отчёт", 'Зачисление под отчёт',
                                'Путевой лист', 'Акт CMD']

        # привязываем хенделр сообщений к боту:
        self.bot.set_update_listener(self.handle_messages)
        handler_dic = self.bot._build_handler_dict(self.callback_func)
        # привязываем хенделр колбеков inline-клавиатуры к боту:
        self.bot.add_callback_query_handler(handler_dic)

        try:
            self.connection_pool = mysql.connector.pooling.MySQLConnectionPool(pool_name="my_pool",
                                                                          pool_size=32,
                                                                          pool_reset_session=True,
                                                                          host=self.DB_HOST, port=self.DB_PORT,
                                                                          database=self.DB_DATABASE,
                                                                          user=self.DB_USER,
                                                                          password=self.DB_PASSWORD)

            connection_object = self.connection_pool.get_connection()

            if connection_object.is_connected():
                db_Info = connection_object.get_server_info()
                cursor = connection_object.cursor()
                cursor.execute("select database();")
                record = cursor.fetchone()

        except Error as e:
            self.logger.critical("Error while connecting to MySQL using Connection pool ", e)
        finally:
            # closing database connection.
            if (connection_object.is_connected()):
                cursor.close()
                connection_object.close()


    def process_updates(self):
        self.bot.process_new_updates([telebot.types.Update.de_json(request.stream.read().decode("utf-8"))])
        return "!", 200

    def webhook(self):
        self.bot.remove_webhook()
        self.bot.set_webhook(url=self.BASE_URL + self.TELEBOT_URL + self.TG_BOT_TOKEN)
        return "!", 200

    # method for inserts|updates|deletes
    def db_execute(self, query, params, comment=""):
        error_code = 1
        try:
            self.logger.debug("db_execute() " + comment)
            connection_local = self.connection_pool.get_connection()
            if connection_local.is_connected():
                cursor_local = connection_local.cursor()
                result = cursor_local.execute(query, params)
                connection_local.commit()
                error_code = 0
        except mysql.connector.Error as error:
            connection_local.rollback()  # rollback if any exception occured
            print("Failed {}".format(error))
        finally:
            # closing database connection.
            if (connection_local.is_connected()):
                cursor_local.close()
                connection_local.close()
        if error_code == 0:
            return True
        else:
            return False

    # method for selects
    def db_query(self, query, params, comment=""):
        try:
            self.logger.debug("db_query() " + comment)
            connection_local = self.connection_pool.get_connection()
            if connection_local.is_connected():
                cursor_local = connection_local.cursor()
                cursor_local.execute(query, params)
                result_set = cursor_local.fetchall()

                self.logger.debug("db_query().result_set:" + str(result_set))
                if result_set is None or len(result_set) <= 0:
                    result_set = []
                cursor_local.close()
        except mysql.connector.Error as error:
            print("Failed {}".format(error))
            result_set = []
        finally:
            # closing database connection.
            if (connection_local.is_connected()):
                connection_local.close()
        return result_set

    def run(self):
        if self.env == 'heroku':
            while True:
                try:
                    self.logger.info("Server run. Version: " + VERSION)
                    self.webhook()
                    self.server.run(host="0.0.0.0", port=int(os.environ.get('PORT', 5000)))
                except Exception as e:
                    self.logger.critical("Cant start DSDocBot. RECONNECT" + str(e))
                    time.sleep(2)
        if self.env == 'local':
            while True:
                try:
                    self.bot.remove_webhook()
                    self.logger.info("Server run. Version: " + VERSION)
                    self.bot.polling()
                except Exception as e:
                    self.logger.critical("Cant start DSDocBot. RECONNECT " + str(e))
                    time.sleep(2)

    # еще совсем не готова
    def callback_func(self,callback_message):

        def getTagsFromMessage(text):
            tag_str = str(text[text.find("Tags") + 5:]).strip()
            tags = tag_str.split(", ")
            return tags

        # TODO
        data = callback_message.data
        # if data == "+":


        tags = getTagsFromMessage(callback_message.message.text)
        tags.remove(data)
        tags_k = tags.copy()
        tags_k.append("ALL")
        tags_k.append("+")
        new_text = callback_message.message.text[:callback_message.message.text.find("Tags")-2] + "\n\n*Tags*: " + ', '.join(tags)
        # self.bot.edit_message_text(chat_id=callback_message.message.chat.id, message_id=callback_message.message.message_id, text=new_text, reply_markup=self.inline_keyboard(tags_k),parse_mode='Markdown')

        # self.bot.answer_callback_query(callback_message.id)
        return

    def command_start(self, message):
        self.logger.info("Receive Start command from chat ID:" + str(message.chat.id))
        if message.from_user.username is not None:
            user_name = message.from_user.username
        else:
            user_name = message.from_user.first_name

        if self.new_user(message.chat.id, user_name):
            self.bot.send_message(message.chat.id, "Your are in. tap /help",reply_markup=self.markup_keyboard(self.markup_commands))
            self.bot.send_message(self.ADMIN_ID, "New user: " + str(user_name))
        else:
            self.bot.send_message(message.chat.id, "Welcome back " + str(message.from_user.username) + ". Tap /help",reply_markup=self.markup_keyboard(self.markup_commands))

    def command_help(self, message):
        try:
            self.logger.info("Receive Help command from chat ID:" + str(message.chat.id))
            self.db_execute("update docbot_users set state = %s where user_id = %s", ("", message.chat.id),"Update State")
            self.bot.send_message(message.chat.id, "Help:\n"
                              "/help - show this message\n"
                              "/usage - show example\n"
                              #"/addurl - add url to your profile\n"
                              #"/edit - edit your profile(TBD)\n"
                              #"/qq - get incomming questions(TBD)\n"
                              #"/... - ...\n"
                              #"/clear - clear tag-list\n"
                              "support - @MichaelPavlov\n"
                              "\n", parse_mode='Markdown',reply_markup=self.markup_keyboard(self.markup_commands))
        except Exception as e:
            self.logger.critical("Cant execute Help command. " + str(e))
        return

    def command_usage(self, message):
        try:
            self.logger.info("Receive Usage command from chat ID:" + str(message.chat.id))
            self.db_execute("update docbot_users set state = %s where user_id = %s", ("", message.chat.id),"Update State")
            self.bot.send_message(message.chat.id,
                                "report_date : 10.12.2018\n"
                                "partner : ООО \"Т2 Мобайл\"\n"
                                "place : ООО \"Сити\"\n"
                                "address : г. Москва, Красная пл., д. 1\n"
                                "topic : обсуждение контракта\n"
                                "bill_amount : 5199-00\n"
                                "bill_id : 18\n"
                                "bill_datetime : 01.03.2018 13:00\n"
                                "delta_person : Чернышев А.В. Генеральный директор\n"
                                "partner_person : Молчанский А.А. Директор департамента больших данных\n"
                                "\n",reply_markup=self.markup_keyboard(self.markup_commands))
        except Exception as e:
            self.logger.critical("Cant execute Usage command. " + str(e))
        return

    def new_user(self, user_id, user_name):
        if len(self.db_query("select user_id from docbot_users where user_id=%s", (user_id,), "Check User exist")) > 0:
            return False
        # add user:
        elif self.db_execute("insert into docbot_users (name,user_id,tags) values (%s,%s,%s)", (user_name, user_id, ""), "Add new User"):
            return True
        else:
            return False

    def markup_keyboard(self, list, remove=False):
        if not remove:
            markupkeyboard = telebot.types.ReplyKeyboardMarkup(resize_keyboard=True, row_width=2)
            markupkeyboard.add(*[telebot.types.KeyboardButton(name) for name in list])
        else:
            markupkeyboard = telebot.types.ReplyKeyboardRemove(selective=False)
        return markupkeyboard

    def inline_keyboard(self, list):
        inlinekeyboard = telebot.types.InlineKeyboardMarkup(row_width=7)
        inlinekeyboard.add(*[telebot.types.InlineKeyboardButton(text=name, callback_data=name) for name in list])
        return inlinekeyboard

    def handle_messages(self, messages):
        for message in messages:
            if message.reply_to_message is not None:
                # TODO Process reply messagr
                return
            if message.text.startswith("/start"):
                self.command_start(message)
                return
            if message.text.startswith("/help"):
                self.command_help(message)
                return
            if message.text.startswith("/usage"):
                self.command_usage(message)
                return
            if message.text.startswith("/broadcast"):
                if int(message.chat.id) == int(self.ADMIN_ID):
                    self.broadcast(message.text.replace("/broadcast ",""))
                else:
                    self.bot.reply_to(message, "You are not admin")
                return
            if message.text.startswith("Представительские"):
                if self.db_execute("update docbot_users set state = %s where user_id = %s",("ent_exp", message.chat.id), "Update State"):
                    self.bot.send_message(message.chat.id, "Напишите вводные в таком формате:\n"
                                          "report_date : 10.12.2018\n"
                                          "partner : ООО \"Т2 Мобайл\"\n"
                                          "place : ООО \"Сити\"\n"
                                          "address : г. Москва, Красная пл., д. 1\n"
                                          "topic : обсуждение контракта\n"
                                          "bill_amount : 5199-00\n"
                                          "bill_id : 18\n"
                                          "bill_datetime : 01.03.2018 13:00\n"
                                          "delta_person : Чернышев А.В. Генеральный директор\n"
                                          "partner_person : Молчанский А.А. Директор департамента больших данных\n"
                                          "\n", reply_markup=self.markup_keyboard([],remove=True))
                else:
                    self.bot.send_message(message.chat.id, "ops...")
                return
            if message.text.startswith("Возмещение расходов"):
                if self.db_execute("update docbot_users set state = %s where user_id = %s",("reimbursement", message.chat.id), "Update State"):
                    self.bot.send_message(message.chat.id, "Напишите вводные в таком формате:\n"
                                          "report_date : 16.05.2019\n"
                                          "event_date : 10.12.2018\n"
                                          "amount : 3200\n"
                                          "delta_person : Павлов\n"
                                          "receipt : true\n"
                                          "tech_receipt : true\n"
                                          "reason : на покупку запчастей для сервера\n"
                                          "\n", reply_markup=self.markup_keyboard([],remove=True))
                else:
                    self.bot.send_message(message.chat.id, "ops...")
                return
            if message.text.startswith("Выдача под отчёт"):
                if self.db_execute("update docbot_users set state = %s where user_id = %s",("money_to_person", message.chat.id), "Update State"):
                    self.bot.send_message(message.chat.id, "Напишите вводные в таком формате:\n"
                                          "report_date : 10.12.2019\n"
                                          "amount : 3200\n"
                                          "delta_person : Павлов\n"
                                          "reason : на командировочные расходы\n"
                                          "\n", reply_markup=self.markup_keyboard([],remove=True))
                else:
                    self.bot.send_message(message.chat.id, "ops...")
                return
            if message.text.startswith("Зачисление под отчёт"):
                if self.db_execute("update docbot_users set state = %s where user_id = %s",("money_transfer", message.chat.id), "Update State"):
                    self.bot.send_message(message.chat.id, "Напишите вводные в таком формате:\n"
                                          "report_date : 10.12.2018\n"
                                          "amount : 3200\n"
                                          "delta_person : Павлов\n"
                                          "reason : на командировочные расходы\n"
                                          "\n", reply_markup=self.markup_keyboard([],remove=True))
                else:
                    self.bot.send_message(message.chat.id, "ops...")
                return
            if message.text.startswith("Путевой лист"):
                if self.db_execute("update docbot_users set state = %s where user_id = %s",("track_list", message.chat.id), "Update State"):
                    self.bot.send_message(message.chat.id, "Напишите вводные в таком формате:\n"
                                            "report_year : 2019\n" + \
                                            "report_month : 5\n" + \
                                            "delta_person : Соболев\n" + \
                                            "distance : 1850\n"
                                            "smolensk : true\n" + \
                                          "\n", reply_markup=self.markup_keyboard([],remove=True))
                else:
                    self.bot.send_message(message.chat.id, "ops...")
                return
            if message.text.startswith("Акт CMD"):
                if self.db_execute("update docbot_users set state = %s where user_id = %s",("act_cmd", message.chat.id), "Update State"):
                    self.bot.send_message(message.chat.id, "Напишите вводные в таком формате:\n"
                                            "act_num : 189\n" + \
                                            "order_num : 185/10068700\n" + \
                                            "order_date : 10.09.2019\n" + \
                                            "order_subj : Доработка CMD. Разработка алгоитма фильтрации для MITEL\n" + \
                                            "amount : 137590\n" + \
                                          "\n", reply_markup=self.markup_keyboard([],remove=True))
                else:
                    self.bot.send_message(message.chat.id, "ops...")
                return


            # проверка на статусы:
            state = self.db_query("select state from docbot_users where user_id=%s", (message.chat.id,), "Get State")[0][0]

            if state == "ent_exp":
                # проверяем на нужный формат
                data = self.create_entertainment_expenses_docs(message.text)
                if data["isvalid"]:
                    self.db_execute("update docbot_users set state = %s where user_id = %s", ("", message.chat.id),"Update State")
                    print("good, path is:", data["report_filepath"])
                    doc = open(data["report_filepath"], 'rb')
                    self.bot.send_document(message.chat.id, doc, reply_markup=self.markup_keyboard(self.markup_commands))
                    self.logger.info("Create new doc from user:" + str(message.chat.id) + "; file:" + data["report_filepath"])
                    if data.get("memo") is not None:
                        print("good, memo path is:", data["memo_filepath"])
                        doc = open(data["memo_filepath"], 'rb')
                        self.bot.send_document(message.chat.id, doc, reply_markup=self.markup_keyboard(self.markup_commands))
                else:
                    self.bot.reply_to(message, "Unknown format. Try again or Tap /usage")
                    self.logger.info("Unknown format from user:" + str(message.chat.id) + "; text:" + message.text)
                return

            if state == "money_transfer":
                # проверяем на нужный формат
                data = self.create_money_to_account_docs(message.text)
                if data["isvalid"]:
                    self.db_execute("update docbot_users set state = %s where user_id = %s", ("", message.chat.id),"Update State")
                    print("good, path is:", data["report_filepath"])
                    doc = open(data["report_filepath"], 'rb')
                    self.bot.send_document(message.chat.id, doc, reply_markup=self.markup_keyboard(self.markup_commands))
                    self.logger.info("Create new doc from user:" + str(message.chat.id) + "; file:" + data["report_filepath"])
                else:
                    self.bot.reply_to(message, "Unknown format or User. Try again or Tap /usage")
                    self.logger.info("Unknown format from user:" + str(message.chat.id) + "; text:" + message.text)
                return

            if state == "money_to_person":
                # проверяем на нужный формат
                data = self.create_money_to_person_docs(message.text)
                if data["isvalid"]:
                    self.db_execute("update docbot_users set state = %s where user_id = %s", ("", message.chat.id),"Update State")
                    print("good, path is:", data["report_filepath"])
                    doc = open(data["report_filepath"], 'rb')
                    self.bot.send_document(message.chat.id, doc, reply_markup=self.markup_keyboard(self.markup_commands))
                    self.logger.info("Create new doc from user:" + str(message.chat.id) + "; file:" + data["report_filepath"])
                else:
                    self.bot.reply_to(message, "Unknown format or User. Try again or Tap /usage")
                    self.logger.info("Unknown format from user:" + str(message.chat.id) + "; text:" + message.text)
                return

            if state == "reimbursement":
                # проверяем на нужный формат
                data = self.create_reimbursement_docs(message.text)
                if data["isvalid"]:
                    self.db_execute("update docbot_users set state = %s where user_id = %s", ("", message.chat.id),"Update State")
                    print("good, path is:", data["report_filepath"])
                    doc = open(data["report_filepath"], 'rb')
                    self.bot.send_document(message.chat.id, doc, reply_markup=self.markup_keyboard(self.markup_commands))
                    self.logger.info("Create new doc from user:" + str(message.chat.id) + "; file:" + data["report_filepath"])
                else:
                    self.bot.reply_to(message, "Unknown format or User. Try again or Tap /usage")
                    self.logger.info("Unknown format from user:" + str(message.chat.id) + "; text:" + message.text)
                return

            if state == "track_list":
                # проверяем на нужный формат
                data = self.create_tracklist_docs(message.text)
                if data["isvalid"]:
                    self.db_execute("update docbot_users set state = %s where user_id = %s", ("", message.chat.id),"Update State")
                    print("good, path is:", data["report_filepath"])
                    doc = open(data["report_filepath"], 'rb')
                    self.bot.send_document(message.chat.id, doc, reply_markup=self.markup_keyboard(self.markup_commands))
                    self.logger.info("Create new doc from user:" + str(message.chat.id) + "; file:" + data["report_filepath"])
                else:
                    self.bot.reply_to(message, "Unknown format or User. Try again or Tap /usage")
                    self.logger.info("Unknown format from user:" + str(message.chat.id) + "; text:" + message.text)
                return

            if state == "act_cmd":
                # проверяем на нужный формат
                data = self.create_cmdact_docs(message.text)
                if data["isvalid"]:
                    self.db_execute("update docbot_users set state = %s where user_id = %s", ("", message.chat.id),"Update State")
                    print("good, path is:", data["report_filepath"])
                    doc = open(data["report_filepath"], 'rb')
                    self.bot.send_document(message.chat.id, doc, reply_markup=self.markup_keyboard(self.markup_commands))
                    self.logger.info("Create new doc from user:" + str(message.chat.id) + "; file:" + data["report_filepath"])
                else:
                    self.bot.reply_to(message, "Unknown format or User. Try again or Tap /usage")
                    self.logger.info("Unknown format from user:" + str(message.chat.id) + "; text:" + message.text)
                return


            # Если ничего не сработало:
            self.logger.info("Recieve unknown command from user:" + str(message.chat.id) + "; text:" + message.text)
            self.bot.reply_to(message,text="Tap command", reply_markup=self.markup_keyboard(self.markup_commands),parse_mode='Markdown')

    def create_entertainment_expenses_docs(self, text):
        data = {}
        data["isvalid"] = False

        try:
            # разбиваем текст на словарь
            for line in text.split("\n"):
                key = line[0:line.find(":")].strip()
                value = line[line.find(":") + 1:].strip()
                if len(key) > 0: data[key] = value
        except Exception as e:
            self.logger.warning("Receive message:" + str(e))
            return data

        # проверяем дату чека
        try:
            bill_datetime = datetime.datetime.strptime(data["bill_datetime"], "%d.%m.%Y %H:%M")
            # Вытаскиваем минуты, округляем вверх до ближайших 15 минут - это последняя точка в цепи
            minutes_approx = math.ceil(bill_datetime.minute / 15.0) * 15
            bill_datetime = bill_datetime.replace(minute=0)
            bill_datetime += datetime.timedelta(seconds=minutes_approx * 60)
            #
            time_discuss_end = str((bill_datetime - datetime.timedelta(minutes=0)).strftime("%H:%M"))
            time_discuss_start = str((bill_datetime - datetime.timedelta(minutes=60)).strftime("%H:%M"))
            #
            time_service_end = str((bill_datetime - datetime.timedelta(minutes=60)).strftime("%H:%M"))
            time_service_start = str((bill_datetime - datetime.timedelta(minutes=90)).strftime("%H:%M"))
            #
            time_guest_arrive_end = str((bill_datetime - datetime.timedelta(minutes=90)).strftime("%H:%M"))
            time_guest_arrive_start = str((bill_datetime - datetime.timedelta(minutes=105)).strftime("%H:%M"))
            #
            meeting_date = str((bill_datetime - datetime.timedelta(minutes=105)).strftime("%d.%m.%Y")) + "г."
        except Exception as e:
            self.logger.warning("Problem with bill_datetime:" + str(e))
            return data

        # маппим остальные сообщения
        try:
            report_date = str(data["report_date"]) + " г."
            partner_name = data["partner"]
            place_name = data["place"]
            place_address = data["address"]
            topic_to_discuss = data["topic"]
            bill_amount = data["bill_amount"]
            bill_id = data["bill_id"]
            delta_persons = data["delta_person"]
            partner_persons = data["partner_person"]
        except Exception as e:
            self.logger.warning("Problem with other field:" + str(e))
            return data

        # строим файл с отчетом
        try:
            doc = DocxTemplate(ENT_EXP_REPORT_TEMPLATE_FILE)

            context = {'report_date': report_date,
                       'meeting_date': meeting_date,
                       'partner_name': partner_name,
                       'place_name': place_name,
                       'place_address': place_address,
                       'topic_to_discuss': topic_to_discuss,
                       'bill_amount': bill_amount,
                       'bill_id': bill_id,
                       'time_guest_arrive_start': time_guest_arrive_start,
                       'time_guest_arrive_end': time_guest_arrive_end,
                       'time_service_start': time_service_start,
                       'time_service_end': time_service_end,
                       'time_discuss_start': time_discuss_start,
                       'time_discuss_end': time_discuss_end,
                       'delta_persons': RichText(" • " + delta_persons),
                       'partner_persons': RichText(" • " + partner_persons)}

            doc.render(context)
            # генерируем имя файла
            report_filename = "ds_meeting_report_" + str((bill_datetime - datetime.timedelta(minutes=105)).strftime("%Y-%m-%d"))
            # оно должно быть уникальным, поэтому првоеряем на наличие пока не найдем свободное
            i = 0
            while True:
                if os.path.exists(TMP_PATH + report_filename + ".docx"):
                    i = i + 1
                    if i == 1:
                        report_filename = report_filename + "_" + str(i)
                    else:
                        report_filename = report_filename[:-1] + str(i)
                else:
                    break
            doc.save(TMP_PATH + report_filename + ".docx")
            # передаем ссылку на файл обратно для отправки
            data["report_filepath"] = TMP_PATH + report_filename + ".docx"
        except Exception as e:
            self.logger.warning("Problem with save docs:" + str(e))
            return data

        # если служебка не нужна, то выходим с успешным флагом
        if data.get("memo") is None:
            data["isvalid"] = True
            return data

        # строим файл с запиской
        try:
            doc = DocxTemplate(ENT_EXP_MEMO_TEMPLATE_FILE)

            context = {'report_date': report_date,
                       'meeting_date': meeting_date,
                       'partner_name': partner_name,
                       'place_name': place_name,
                       'bill_amount': bill_amount,
                       'bill_id': bill_id,
                       'delta_persons': delta_persons}

            doc.render(context)
            # генерируем имя файла
            memo_filename = "ds_meeting_memo_" + str((bill_datetime - datetime.timedelta(minutes=105)).strftime("%Y-%m-%d"))
            # оно должно быть уникальным, поэтому првоеряем на наличие пока не найдем свободное
            i = 0
            while True:
                if os.path.exists(TMP_PATH + memo_filename + ".docx"):
                    i = i + 1
                    if i == 1:
                        memo_filename = memo_filename + "_" + str(i)
                    else:
                        memo_filename = memo_filename[:-1] + str(i)
                else:
                    break
            doc.save(TMP_PATH + memo_filename + ".docx")
            # передаем ссылку на файл обратно для отправки
            data["memo_filepath"] = TMP_PATH + memo_filename + ".docx"
        except Exception as e:
            self.logger.warning("Problem with save docs:" + str(e))
            return data

        # если дошли сюда - значит все хорошо и можно передавать ОК
        data["isvalid"] = True
        return data

    def create_money_to_account_docs(self, text):
        data = {}
        data["isvalid"] = False

        try:
            # разбиваем текст на словарь
            for line in text.split("\n"):
                key = line[0:line.find(":")].strip()
                value = line[line.find(":") + 1:].strip()
                if len(key) > 0: data[key] = value
        except Exception as e:
            self.logger.warning("Receive message:" + str(e))
            return data

        # маппим остальные сообщения
        try:
            file_date = datetime.datetime.strptime(data["report_date"], "%d.%m.%Y")
            report_date = str(data["report_date"]) + " г."
            reason = data["reason"]
            amount = data["amount"]
            amount_txt = num2words.num2words(int(amount), lang='ru')
            delta_person = data["delta_person"]
        except Exception as e:
            self.logger.warning("Problem with other field:" + str(e))
            return data

        # ищем сотрудника
        try:
            delta_person_detailed = self.db_query("select full_name,full_name_r,full_name_d,position,position_r,position_d,account  from docbot_refs_employers where full_name like %s", ("%"+delta_person+"%",), "Get State")[0]
        except Exception as e:
            self.logger.warning("No User found:" + str(e))
            return data

        # строим файл с отчетом
        try:
            doc = DocxTemplate(MONEY_TO_ACC_TEMPLATE_FILE)

            context = {'report_date': report_date,
                       'reason': reason,
                       'amount': amount,
                       'amount_txt': amount_txt,
                       'delta_person_name_r': delta_person_detailed[1],
                       'delta_person_position': delta_person_detailed[3],
                       'delta_person_name': delta_person_detailed[0],
                       'account': delta_person_detailed[6]}

            doc.render(context)
            # генерируем имя файла
            report_filename = "ds_money_trasfer_order_" + str((file_date).strftime("%Y-%m-%d"))
            # оно должно быть уникальным, поэтому првоеряем на наличие пока не найдем свободное
            i = 0
            while True:
                if os.path.exists(TMP_PATH + report_filename + ".docx"):
                    i = i + 1
                    if i == 1:
                        report_filename = report_filename + "_" + str(i)
                    else:
                        report_filename = report_filename[:-1] + str(i)
                else:
                    break
            doc.save(TMP_PATH + report_filename + ".docx")
            # передаем ссылку на файл обратно для отправки
            data["report_filepath"] = TMP_PATH + report_filename + ".docx"
        except Exception as e:
            self.logger.warning("Problem with save docs:" + str(e))
            return data

        # если дошли сюда - значит все хорошо и можно передавать ОК
        data["isvalid"] = True
        return data

    def create_money_to_person_docs(self, text):
        data = {}
        data["isvalid"] = False

        try:
            # разбиваем текст на словарь
            for line in text.split("\n"):
                key = line[0:line.find(":")].strip()
                value = line[line.find(":") + 1:].strip()
                if len(key) > 0: data[key] = value
        except Exception as e:
            self.logger.warning("Receive message:" + str(e))
            return data

        # маппим остальные сообщения
        try:
            file_date = datetime.datetime.strptime(data["report_date"], "%d.%m.%Y")
            report_date = str(data["report_date"]) + " г."
            reason = data["reason"]
            amount = data["amount"]
            amount_txt = num2words.num2words(int(amount), lang='ru')
            delta_person = data["delta_person"]
        except Exception as e:
            self.logger.warning("Problem with other field:" + str(e))
            return data

        # ищем сотрудника
        try:
            delta_person_detailed = self.db_query("select full_name,full_name_r,full_name_d,position,position_r,position_d,account from docbot_refs_employers where full_name like %s", ("%"+delta_person+"%",), "Get State")[0]
        except Exception as e:
            self.logger.warning("No User found:" + str(e))
            return data

        # строим файл с отчетом
        try:
            doc = DocxTemplate(MONEY_TO_PERSON_TEMPLATE_FILE)

            context = {'report_date': report_date,
                       'reason': reason,
                       'amount': amount,
                       'amount_txt': amount_txt,
                       'delta_person_name_d': delta_person_detailed[2],
                       'delta_person_position': delta_person_detailed[3],
                       'delta_person_name': delta_person_detailed[0]}

            doc.render(context)
            # генерируем имя файла
            report_filename = "ds_money_to_person_order_" + str((file_date).strftime("%Y-%m-%d"))
            # оно должно быть уникальным, поэтому првоеряем на наличие пока не найдем свободное
            i = 0
            while True:
                if os.path.exists(TMP_PATH + report_filename + ".docx"):
                    i = i + 1
                    if i == 1:
                        report_filename = report_filename + "_" + str(i)
                    else:
                        report_filename = report_filename[:-1] + str(i)
                else:
                    break
            doc.save(TMP_PATH + report_filename + ".docx")
            # передаем ссылку на файл обратно для отправки
            data["report_filepath"] = TMP_PATH + report_filename + ".docx"
        except Exception as e:
            self.logger.warning("Problem with save docs:" + str(e))
            return data

        # если дошли сюда - значит все хорошо и можно передавать ОК
        data["isvalid"] = True
        return data

    def create_reimbursement_docs(self, text):
        data = {}
        data["isvalid"] = False

        try:
            # разбиваем текст на словарь
            for line in text.split("\n"):
                key = line[0:line.find(":")].strip()
                value = line[line.find(":") + 1:].strip()
                if len(key) > 0: data[key] = value
        except Exception as e:
            self.logger.warning("Receive message:" + str(e))
            return data

        # маппим остальные сообщения
        try:
            file_date = datetime.datetime.strptime(data["report_date"], "%d.%m.%Y")
            event_date = str(data["event_date"]) + " г."
            report_date = str(data["report_date"]) + " г."
            reason = data["reason"]
            amount = data["amount"]
            amount_txt = num2words.num2words(int(amount), lang='ru')

            # разбираемся с чеками
            fiscal_receipt_block = ""
            sales_receipt_block = ""
            if data.get("receipt") is not None:
                fiscal_receipt_block = "Кассовый чек от " + event_date
            if data.get("tech_receipt") is not None:
                sales_receipt_block = "Товарный чек от " + event_date
            delta_person = data["delta_person"]
        except Exception as e:
            self.logger.warning("Problem with other field:" + str(e))
            return data

        # ищем сотрудника
        try:
            delta_person_detailed = self.db_query("select full_name,full_name_r,full_name_d,position,position_r,position_d,account from docbot_refs_employers where full_name like %s", ("%"+delta_person+"%",), "Get State")[0]
        except Exception as e:
            self.logger.warning("No User found:" + str(e))
            return data

        # строим файл с отчетом
        try:
            doc = DocxTemplate(REIMBURSEMENT_TEMPLATE_FILE)

            context = {'report_date': report_date,
                       'event_date': event_date,
                       'sales_receipt_block': sales_receipt_block,
                       'fiscal_receipt_block': fiscal_receipt_block,
                       'reason': reason,
                       'amount': amount,
                       'amount_txt': amount_txt,
                       'delta_person_name_r': delta_person_detailed[1],
                       'delta_person_position': delta_person_detailed[3],
                       'delta_person_name': delta_person_detailed[0],
                       'delta_person_position_r': delta_person_detailed[4]}

            doc.render(context)
            # генерируем имя файла
            report_filename = "ds_reimbursement_note" + str((file_date).strftime("%Y-%m-%d"))
            # оно должно быть уникальным, поэтому првоеряем на наличие пока не найдем свободное
            i = 0
            while True:
                if os.path.exists(TMP_PATH + report_filename + ".docx"):
                    i = i + 1
                    if i == 1:
                        report_filename = report_filename + "_" + str(i)
                    else:
                        report_filename = report_filename[:-1] + str(i)
                else:
                    break
            doc.save(TMP_PATH + report_filename + ".docx")
            # передаем ссылку на файл обратно для отправки
            data["report_filepath"] = TMP_PATH + report_filename + ".docx"
        except Exception as e:
            self.logger.warning("Problem with save docs:" + str(e))
            return data

        # если дошли сюда - значит все хорошо и можно передавать ОК
        data["isvalid"] = True
        return data

    def create_tracklist_docs(self, text):
        data = {}
        data["isvalid"] = False

        # значение по умолчанию, может быть переопределено из сообщения. Строкове чтобы не запутаться и делать преобразование в одном месте
        data["max_usage"] = "2"

        try:
            # разбиваем текст на словарь
            for line in text.split("\n"):
                key = line[0:line.find(":")].strip()
                value = line[line.find(":") + 1:].strip()
                if len(key) > 0: data[key] = value
        except Exception as e:
            self.logger.warning("Receive message:" + str(e))
            return data

        # маппим остальные сообщения
        try:
            file_date = datetime.datetime.now()
            if len(data["report_month"]) == 2:
                report_month_mm = data["report_month"]
            else:
                report_month_mm = "0" + data["report_month"]

            report_date = "1-" + \
                          str(max(calendar.monthrange(int(data["report_year"]), int(data["report_month"])))) + \
                          "." + report_month_mm + "." + data["report_year"]

            report_num = data["report_year"] + "/" + data["report_month"] + "-1"

            delta_person = data["delta_person"]
        except Exception as e:
            self.logger.warning("Problem with other field:" + str(e))
            return data

        # ищем сотрудника
        try:
            delta_person_detailed = self.db_query(
                "select full_name,position_r,drive_license,car_model,car_number,table_num,normative,oil_type from docbot_refs_employers where full_name like %s",
                ("%" + delta_person + "%",), "Get State")[0]
        except Exception as e:
            data["error_message"] = "No User found"
            self.logger.warning("No User found:" + str(e))
            return data

        # формируем список треков
        try:
            # max_usage = int(data["max_usage"]) # вычисляется автоматически
            total_distance = int(data["distance"])
            raw_distance_list = self.db_query("select distance_km from docbot_refs_trackpoints",(),"Get State")
            distance_list = []
            for dist in raw_distance_list:
                distance_list.append(dist[0])

            # если нет флага Смоленск, то выкидываем максимальный элемент
            # строго две точки в Смоленске!
            if data.get("smolensk") is None:
                distance_list.remove(max(distance_list))
                distance_list.remove(max(distance_list))

            # вычисляем max_usage
            summ = 0
            for item in distance_list:
                summ += item
            max_usage_predict = total_distance / summ
            max_usage = math.ceil(max_usage_predict)

            # Основной расчет
            summ = 0
            positions_count = 0
            i = 0
            result_list = []

            while summ < total_distance:
                # проверяем что список не кончился
                if len(distance_list) == 0:
                    # print("list is empty")
                    break
                max_val = max(distance_list)

                delta = max_val * (max_usage - i)
                if i <= max_usage:
                    if (total_distance - summ - delta >= 0) and delta > 0:
                        # кладем в сумму
                        summ += delta
                        for j in range(max_usage - i):
                            result_list.append(max_val)
                        # print("add", delta, "is", max_val, "*", str(max_usage - i))
                        positions_count += max_usage - i
                        i = 0
                        distance_list.remove(max_val)
                    else:
                        # если не подошло, подбираем дальше
                        i += 1
                else:
                    # совсем не подошло, переходим к следующему по списку числу
                    # TODO проверять на отсутствие повторяющихся чисел
                    distance_list.remove(max_val)
                    i = 0
            if summ != total_distance:
                data["error_message"] = "DELTA:" + str(total_distance - summ)
                return data
            random.shuffle(result_list)


            # вытаскиваем данные из БД и заполняем структуру для таблицы
            items = []
            index = 1
            for distance_key in result_list:
                item = {}
                raw_distance_line = dBot.db_query("select start_point,end_point,distance_km,travel_time_h  from docbot_refs_trackpoints where distance_km = '%s'",(distance_key,), "Get Data")[0]
                item["idx"] = str(index)
                item["from"] = raw_distance_line[0]
                item["to"] = raw_distance_line[1]
                item["shh"] = '9'
                item["smm"] = '00'
                item["ehh"] = str(9+int(raw_distance_line[3]))
                item["emm"] = '00'
                item["dist"] = raw_distance_line[2]
                items.append(item)

                index += 1

            print(items)
        except Exception as e:
            self.logger.warning("Problem with tracking list:" + str(e))
            return data

        # строим файл с отчетом
        try:
            doc = DocxTemplate(TRACKLIST_TEMPLATE_FILE)

            context = {'report_date': report_date,
                       'NUM': report_num,
                       'car_model': delta_person_detailed[3],
                       'car_number': delta_person_detailed[4],
                       'delta_person_name': delta_person_detailed[0],
                       'delta_person_position_r': delta_person_detailed[1],
                       'driver_license': delta_person_detailed[2],
                       'oil_type': delta_person_detailed[7],
                       'norma': delta_person_detailed[6],
                       'table_num': delta_person_detailed[5],
                       'total_dist': data["distance"],
                       'items': items
                       }

            doc.render(context)
            # генерируем имя файла
            report_filename = "ds_track_list_" + str(delta_person_detailed[5]) + "_" + str((file_date).strftime("%Y-%m-%d"))
            # оно должно быть уникальным, поэтому првоеряем на наличие пока не найдем свободное
            i = 0
            while True:
                if os.path.exists(TMP_PATH + report_filename + ".docx"):
                    i = i + 1
                    if i == 1:
                        report_filename = report_filename + "_" + str(i)
                    else:
                        report_filename = report_filename[:-1] + str(i)
                else:
                    break
            doc.save(TMP_PATH + report_filename + ".docx")
            # передаем ссылку на файл обратно для отправки
            data["report_filepath"] = TMP_PATH + report_filename + ".docx"
        except Exception as e:
            self.logger.warning("Problem with save docs:" + str(e))
            return data

        # если дошли сюда - значит все хорошо и можно передавать ОК
        data["isvalid"] = True
        return data

    def create_cmdact_docs(self, text):
        data = {}
        data["isvalid"] = False

        try:
            # разбиваем текст на словарь
            for line in text.split("\n"):
                key = line[0:line.find(":")].strip()
                value = line[line.find(":") + 1:].strip()
                if len(key) > 0: data[key] = value
        except Exception as e:
            self.logger.warning("Receive message:" + str(e))
            return data

        # маппим остальные сообщения
        try:
            file_date = file_date = datetime.datetime.now()
            act_num = data["act_num"]
            report_date = str((file_date).strftime("%d.%m.%Y")) + " г."
            order_num = data["order_num"]
            order_date = str(datetime.datetime.strptime(data["order_date"], "%d.%m.%Y")) + " г."
            order_subject = data["order_subj"]

            ## Шаманство с копейками
            amount_raw = float(data["amount"])
            amount_cc = int(amount_raw * 100)
            vat_cc = int(amount_cc * 0.2)
            total_amount_cc = amount_cc + vat_cc

            # достаем целые и дробные обратно
            amount_value = int(amount_cc / 100)
            amount_cents = amount_cc % 100
            if amount_cents == 0: amount_cents = "00"
            total_amount_value = int(total_amount_cc / 100)
            total_amount_cents = total_amount_cc % 100
            if total_amount_cents == 0: total_amount_cents = "00"
            vat_value = int(vat_cc / 100)
            vat_cents = vat_cc % 100
            if vat_cents == 0: vat_cents = "00"

            # полные числа
            amount_full = str(amount_value) + "," + str(amount_cents)
            vat_full = str(vat_value) + "," + str(vat_cents)
            vat_txt = num2words.num2words(vat_value, lang='ru') + " и " + str(vat_cents) + "/100"
            total_amount_full = str(total_amount_value) + "," + str(total_amount_cents)
            total_amount_txt = num2words.num2words(total_amount_value, lang='ru') + " и " + str(total_amount_cents) + "/100"
        except Exception as e:
            self.logger.warning("Problem with other field:" + str(e))
            return data

        # строим файл с отчетом
        try:
            doc = DocxTemplate(CMD_ACT_TEMPLATE_FILE)

            context = {'report_date': report_date,
                       'act_num': act_num,
                       'order_num': order_num,
                       'order_date': order_date,
                       'order_subject': order_subject,
                       'amount': amount_full,
                       'vat': vat_full,
                       'vat_txt': vat_txt,
                       'total_amount': total_amount_full,
                       'total_amount_txt': total_amount_txt}

            doc.render(context)
            # генерируем имя файла
            report_filename = "ds_cmd_vk_act_" + str((file_date).strftime("%Y-%m-%d"))
            # оно должно быть уникальным, поэтому првоеряем на наличие пока не найдем свободное
            i = 0
            while True:
                if os.path.exists(TMP_PATH + report_filename + ".docx"):
                    i = i + 1
                    if i == 1:
                        report_filename = report_filename + "_" + str(i)
                    else:
                        report_filename = report_filename[:-1] + str(i)
                else:
                    break
            doc.save(TMP_PATH + report_filename + ".docx")
            # передаем ссылку на файл обратно для отправки
            data["report_filepath"] = TMP_PATH + report_filename + ".docx"
        except Exception as e:
            self.logger.warning("Problem with save docs:" + str(e))
            return data

        # если дошли сюда - значит все хорошо и можно передавать ОК
        data["isvalid"] = True
        return data

    def broadcast(self, message):
        try:
            for item in self.db_query("select user_id from docbot_users", (), "Get all Users"):
                self.bot.send_message(item[0],message)
        except Exception as e:
            self.logger.warning("Cant send broadcast message:" + str(e))

if __name__ == '__main__':

    dBot = DOCBot(env='heroku', mode='online', proxy=False)
    dBot.run()





