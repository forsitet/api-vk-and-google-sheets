import requests
import apiclient
import httplib2
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime
import time
import os
from dotenv import load_dotenv
import schedule
from telethon.sync import TelegramClient
from telethon import functions, types
from data import POST_AFTER_DATE, NAME_TABLE_CONTENT, NAME_TABLE_SUM_VIEW, TAG
from collections import namedtuple

Ambs = namedtuple("Ambs", ["second_name", "first_name", "vk", "tg", "vk_group"])

def configurate_google_sheet():
    CREDENTIALS_FILE = "key.json"
    spreadsheet_id = os.getenv("SPREADSHEET_ID")
    credentials = ServiceAccountCredentials.from_json_keyfile_name(
        CREDENTIALS_FILE,
        ['https://www.googleapis.com/auth/spreadsheets',
         'https://www.googleapis.com/auth/drive'])
    httpAuth = credentials.authorize(httplib2.Http())
    service = apiclient.discovery.build('sheets', 'v4', http=httpAuth)
    return service, spreadsheet_id


def post_info_vk(response, amb, source):
    """
    Фильтрует последние 100 постов на стене по хэштегу #АмбассадорыVK
    """
    try:
        post_after_date = datetime.strptime(POST_AFTER_DATE, "%d.%m.%Y")
        rows = []
        rows_sum_views = []
        sum_view = 0
        try:
            cnt = response.json()["response"]["count"]
        except:
            print(f"[{source}] У {amb.second_name} {amb.first_name} нет постов (cnt=0)")
        else:
            if cnt > 0 and cnt:
                for i in range(cnt):
                    response_item = response.json()["response"]["items"][i]
                    view = 0
                    # У клипов берём просмотры по клипу
                    if (bool(response_item["attachments"]) and
                            response_item["attachments"][0]["type"] == "video" and
                            "Клип" in response_item["attachments"][0]["video"]["title"]):
                        view = response_item["attachments"][0]["video"]["views"]
                    if view == 0:  # Выполняется, если условие выше не выполнилось или у нас недостаточно прав для просмотра информации по клипу
                        view = response_item["views"]["count"]
                    id_post = response_item["id"]
                    id_user = response_item["from_id"]
                    url_post = f"https://vk.com/wall{id_user}_{id_post}"
                    post_date = response_item["date"]
                    post_date = datetime.fromtimestamp(int(post_date))
                    if post_date >= post_after_date:
                        rows.append((amb.second_name, amb.first_name, url_post, view, post_date.strftime('%d.%m.%Y')))
                        sum_view += int(view)
                if sum_view > 0:
                    rows_sum_views.append([amb.second_name, amb.first_name, sum_view])
            else:
                 print(f"[{source}] У {amb.second_name} {amb.first_name} нет постов")
    except Exception as err:
        print(f"[{source}] Ошибка у {response.json()["error"]["request_params"][0]["value"]} [post_info_vk] {err}")

    if not(rows_sum_views):
        rows_sum_views.append([amb.second_name, amb.first_name, 0])
    return rows, rows_sum_views


def post_info_tg(channel_username, amb):
    """
    Фильтрует последние 400 постов на стене по хэштегу #АмбассадорыVK
    """
    api_id = os.getenv("API_ID_TG")
    api_hash = os.getenv("API_HASH_TG")
    rows = []
    sum_view = 0
    post_after_date = datetime.strptime(POST_AFTER_DATE, "%d.%m.%Y")
    try:
        with TelegramClient('session_name', api_id, api_hash) as client:
            channel = client.get_entity(channel_username)
            history = client(functions.messages.GetHistoryRequest(
                peer=channel,
                limit=400,
                offset_date=None,
                offset_id=0,
                max_id=0,
                min_id=0,
                add_offset=0,
                hash=0,
            ))
    except:
        print(f"У {amb.second_name} {amb.first_name} частный тг-канал, охватов нема")

    else:
        for message in history.messages:
            if message.message and TAG in message.message:  # Проверяем наличие строки в тексте сообщения
                message_link = f"https://t.me/{channel_username.replace('@', '')}/{message.id}"
                post_date = message.date.replace(tzinfo=None)
                if post_date >= post_after_date:
                    rows.append((amb.second_name, amb.first_name, message_link, message.views, post_date.strftime('%d.%m.%Y')))
                    sum_view += int(message.views)
                
    return rows, sum_view


def get_ambs_info():
    N = int(os.getenv("COL"))
    service, spreadsheet_id = configurate_google_sheet()
    tags = service.spreadsheets().values().get(
        spreadsheetId=spreadsheet_id,
        range="'tags'!A:E",
        majorDimension='ROWS'
    ).execute()

    ambs_info = []
    for i in range(len(tags["values"])):
        amb = tags["values"][i]
        while len(amb) != N:
            amb.append("")
        ambs_info.append(Ambs(*(map(lambda x: x.strip(), amb))))
    return ambs_info
 

def del_sheets(name_table):
    service, spreadsheet_id = configurate_google_sheet()
    service.spreadsheets().values().clear(
        spreadsheetId=spreadsheet_id,
        range=name_table
    ).execute()


def send_sheets(data, name_table):
    service, spreadsheet_id = configurate_google_sheet()
    values = service.spreadsheets().values().batchUpdate(
        spreadsheetId=spreadsheet_id,
        body={
            "valueInputOption": "USER_ENTERED",
            "data": [
                {"range": name_table,
                 "majorDimension": "ROWS",
                 "values": [elem for elem in data]},
            ]
        }
    ).execute()


def parser():
    name_table_content = NAME_TABLE_CONTENT + "!A:E"
    name_table_sum_view = NAME_TABLE_SUM_VIEW + "!A:C"
    load_dotenv()
    version = os.getenv("VERSION")
    token = os.getenv("TOKEN")
    method_api = "wall.search"
    url = "https://api.vk.com/method/" + method_api
    rows = [("Фамилия амбассадора", "Имя амбассадора", "Ссылка на пост", "Охват", "Дата поста")]
    rows_sum_view = [("Фамилия амбассадора", "Имя амбассадора", "Суммарный охват")]
    ambs = get_ambs_info()
    for amb in ambs:
        time.sleep(0.4)
        params = {"owner_id": amb.vk, "v": version, "access_token": token, "query": TAG, "owners_only": 1,
                  "count": 100, "extended": 1, "lang": 0}
        response = requests.get(url, params=params)
        res_info_vk = post_info_vk(response, amb, "Профиль")
        rows.extend(res_info_vk[0])

        if amb.tg != "":
            links = amb.tg.split("\n")
            for link in links:
                res_info_tg = post_info_tg(link.strip().replace("https://t.me/", ""), amb)
                res_info_vk[1][0][2] += res_info_tg[1]
                rows.extend(res_info_tg[0])

        if amb.vk_group != "":
            links = amb.vk_group.split("\n")
            for link in links:
                params = {"domain": link.strip().replace("https://vk.com/", ""), "v": version, "access_token": token, "query": TAG, "owners_only": 1,
                    "count": 100, "extended": 1, "lang": 0}
                response = requests.get(url, params=params)
                res_info_vk_group = post_info_vk(response, amb, "Группа")
                rows.extend(res_info_vk_group[0])
                res_info_vk[1][0][2] += res_info_vk_group[1][0][2]
                time.sleep(0.2)
            
        rows_sum_view.extend(res_info_vk[1])
    del_sheets(name_table_content)
    send_sheets(rows, name_table_content)
    del_sheets(name_table_sum_view)
    send_sheets(rows_sum_view, name_table_sum_view)


def main():
    # Убрать комменты, если будем использовать на сервере, это планировщик запуска
    # schedule.every().day.at("21:00").do(parser)
    # while True:
    #     schedule.run_pending()
    parser()


if __name__ == "__main__":
    main()
