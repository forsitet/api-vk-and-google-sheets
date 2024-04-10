import requests
import apiclient
import httplib2
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime
import time
import os
from dotenv import load_dotenv
import schedule
from date import POST_AFTER_DATE


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


def post_info(response):
    """
    Фильтрует последние 100 постов на стене по хэштегу #АмбассадорыVK
    """
    try:
        post_after_date = datetime.strptime(POST_AFTER_DATE, "%d.%m.%Y")
        rows = []
        rows_sum_views = []
        sum_view = 0
        cnt = response.json()["response"]["count"]
        if cnt > 0:
            first_name = response.json()["response"]["profiles"][0]["first_name"]
            last_name = response.json()["response"]["profiles"][0]["last_name"]
            if not (bool(first_name) and bool(last_name)):
                # При репосте из группы не всегда однозначно заносятся поля
                first_name = response.json()["response"]["profiles"][1]["first_name"]
                last_name = response.json()["response"]["profiles"][1]["last_name"]

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
                    rows.append((last_name, first_name, url_post, view, post_date.strftime('%d.%m.%Y')))
                    sum_view += int(view)
            rows_sum_views.append((last_name, first_name, sum_view))
    except:
        print(f"Ошибка у {response.json()["error"]["request_params"][0]["value"]}")

    return rows, rows_sum_views


def get_tags():
    service, spreadsheet_id = configurate_google_sheet()
    tags = service.spreadsheets().values().get(
        spreadsheetId=spreadsheet_id,
        range="'tags'!C:C",
        majorDimension='COLUMNS'
    ).execute()
    return tags["values"][0]


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
    load_dotenv()
    version = os.getenv("VERSION")
    token = os.getenv("TOKEN")
    method_api = "wall.search"
    url = "https://api.vk.com/method/" + method_api
    ambs = get_tags()
    rows = [("Фамилия амбассадора", "Имя амбассадора", "Ссылка на пост", "Охват", "Дата поста")]
    rows_sum_view = [("Фамилия амбассадора", "Имя амбассадора", "Суммарный охват")]
    for domain in ambs:
        time.sleep(1)
        params = {"domain": domain, "v": version, "access_token": token, "query": "#АмбассадорыVK", "owners_only": 1,
                  "count": 100, "extended": 1, "lang": 0}
        response = requests.get(url, params=params)
        res_info = post_info(response)
        rows.extend(res_info[0])
        rows_sum_view.extend(res_info[1])
    send_sheets(rows, "'Контент'!A:E")
    send_sheets(rows_sum_view, "'Итог'!A:C")


def main():
    # Убрать комменты, если будем использовать на сервере, это планировщик запуска
    # schedule.every().day.at("21:00").do(parser)
    # while True:
    #     schedule.run_pending()
    parser()


if __name__ == "__main__":
    main()
