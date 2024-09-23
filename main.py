import asyncio

from pandas import read_csv, json_normalize
from aiohttp import ClientSession
from json import dump
from config import CSV_PATH, API_URL,RESULT_JSON_PATH


def parce_json(data_json):#обработка ответа от API с помощю pandas
    data = json_normalize(data_json)
    return data

def write_json(data_json: dict):#запись файла с нужными данными
    data_frame = parce_json(data_json)
    data = data_frame.to_json (orient='index') 
    with open(RESULT_JSON_PATH, "w", encoding="utf-8") as file:
        dump(data, file)

async def get_api_data(row: dict):
    async with ClientSession() as session: # [3]
        async with session.get(API_URL) as resp: # [4]
            response = await resp.read() # [5]
            write_json(response['data'])
def main():
    data = read_csv(CSV_PATH)
    data_dict = data.to_dict(orient='records')
    for row in data_dict:
        asyncio.run(get_api_data(row))



if __name__ == "__main__":
    main()