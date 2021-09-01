import configparser
import json

from telethon.sync import TelegramClient
# класс для работы с сообщениями
from telethon.tl.functions.messages import GetHistoryRequest

# для корректного переноса времени сообщений в json

# Считываем учетные данные
config = configparser.ConfigParser()
config.read("config.ini")

# Присваиваем значения внутренним переменным
api_id = config['Telegram']['api_id']
api_hash = config['Telegram']['api_hash']
username = config['Telegram']['username']

client = TelegramClient(username, int(api_id), api_hash)
client.start()


async def dump_all_messages(channel):
    """Записывает json-файл с информацией о всех сообщениях канала/чата"""
    offset_msg = 0  # номер записи, с которой начинается считывание
    limit_msg = 100  # максимальное число записей, передаваемых за один раз

    all_messages = []  # список всех сообщений
    # total_messages = 0
    total_count_limit = 0  # поменяйте это значение, если вам нужны не все сообщения

    while True:
        history = await client(GetHistoryRequest(
            peer=channel,
            offset_id=offset_msg,
            offset_date=None, add_offset=0,
            limit=limit_msg, max_id=0, min_id=0,
            hash=0
        ))
        if not history.messages:
            break
        messages = history.messages
        for message in messages:
            content = message.raw_text
            if content is None or content.strip() == "":
                continue
            all_messages.append({
                "content": content,
                "date": message.date
            })
        offset_msg = messages[len(messages) - 1].id
        total_messages = len(all_messages)
        if total_count_limit != 0 and total_messages >= total_count_limit:
            break

    return all_messages


async def main():
    path = "Dataset.csv"
    with open(path) as file:
        channels_content = {}
        for channel_name in file.readlines()[:10]:
            print(f"Trying to get channel {channel_name}")
            try:
                channel_name = channel_name.strip()
                channel = await client.get_entity(channel_name)
                messages = await dump_all_messages(channel)
                channels_content[channel_name] = messages
            except Exception as exp:
                print(f"Exception happened on channel {channel_name} {exp}")
        with open('channel_messages.json', 'w', encoding='utf8') as outfile:
            json.dump(channels_content, outfile, ensure_ascii=False, default=str)


with client:
    client.loop.run_until_complete(main())

# from_date = datetime.strptime('20.08.2020 14:00:00', '%d.%m.%Y %H:%M:%S').replace(tzinfo=timezone.utc)
# to_date = datetime.strptime('30.08.2020 14:00:00', '%d.%m.%Y %H:%M:%S').replace(tzinfo=timezone.utc)
#
# pre_first_msg = bot.get_messages(channel, offset_date=from_date, limit=1)[0]
# first_msg = bot.get_messages(channel, min_id=pre_first_msg.id, limit=1, reverse=True)[0]
# last_msg = bot.get_messages(channel, offset_date=to_date, limit=1)[0]
#
# messages_between = bot.get_messages(channel, min_id=first_msg.id, max_id=last_msg.id) + [first_msg, last_msg]
