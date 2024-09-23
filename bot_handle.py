import telebot
from config import token


bot = telebot.Telebot(token)
chat_id = ''

@bot.message_handle(commands=['start'])
def start(message):
    global client_id
    chat_id = message.chat.id
    #start deloy with prefect
    bot.send_message(chat_id, "start flow")

@bot.message_handler(function = lambda message: True)
def notice(message):
    pass#обработчик сервера на выполение задач