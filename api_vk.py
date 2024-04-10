import requests
import apiclient
import httplib2
from oauth2client.service_account import ServiceAccountCredentials
import datetime
import time
import os
from dotenv import load_dotenv
import schedule


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
        rows = []
        cnt = response.json()["response"]["count"]
        for i in range(cnt):
            view = response.json()["response"]["items"][i]["views"]["count"]
            first_name = response.json()["response"]["profiles"][0]["first_name"]
            last_name = response.json()["response"]["profiles"][0]["last_name"]
            id_post = response.json()["response"]["items"][i]["id"]
            id_user = response.json()["response"]["items"][i]["from_id"]
            url_post = f"https://vk.com/wall{id_user}_{id_post}"
            time = response.json()["response"]["items"][i]["date"]
            time = datetime.datetime.fromtimestamp(int(time)).strftime('%d.%m.%Y')
            rows.append((last_name, first_name, url_post, view, time))
    except:
        print(f"Ошибка у {response.json()["error"]["request_params"][0]["value"]}")
    return rows


def get_tags():
    service, spreadsheet_id = configurate_google_sheet()
    tags = service.spreadsheets().values().get(
        spreadsheetId=spreadsheet_id,
        range="'tags'!C:C",
        majorDimension='COLUMNS'
    ).execute()
    return tags["values"][0]


def send_sheets(data):
    service, spreadsheet_id = configurate_google_sheet()
    values = service.spreadsheets().values().batchUpdate(
        spreadsheetId=spreadsheet_id,
        body={
            "valueInputOption": "USER_ENTERED",
            "data": [
                {"range": "'Контент'!A:E",
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
    for domain in ambs:
        time.sleep(1)
        params = {"domain": domain, "v": version, "access_token": token, "query": "#АмбассадорыVK", "owners_only": 1,
                  "count": 100, "extended": 1, "lang": 0}
        response = requests.get(url, params=params)
        rows.extend(post_info(response))
    send_sheets(rows)


def main():
    #Убрать комменты, если будем использовать на сервере, это планировщик запуска
    # schedule.every().day.at("21:00").do(parser)
    # while True:
    #     schedule.run_pending()
    parser()


if __name__ == "__main__":
    main()
