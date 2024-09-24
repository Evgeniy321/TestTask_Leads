import telebot
from config import token


bot = telebot.Telebot(token)
chat_id = ''

@bot.message_handle(commands=['start'])
def start(message):
    global chat_id
    chat_id = message.chat.id

def send_telegram_message(message):
    bot.send_message(chat_id, message)

# Колбэк при успешном выполнении потока
def flow_success_handler(flow, state):
    message = f"Поток {flow.name} завершился успешно!"
    send_telegram_message(message)

# Колбэк при неудачном выполнении потока
def flow_failure_handler(flow, state):
    message = f"Поток {flow.name} завершился с ошибкой!"
    send_telegram_message(message)