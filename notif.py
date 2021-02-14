import requests
import vk_api
import sys
from threading import Thread, Lock
import os
import time
import datetime
import traceback
from vk_api.longpoll import VkLongPoll, VkEventType
import pymysql.cursors
import argparse
import configparser
class DateFormatError(Exception):
    pass
def exceptionDecorator(func):
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            print_tb(e)
            raise (e)
    return wrapper
def sqlExceptionDecorator(func):
    def handle_reconnect(self,text,*args, **kwargs):
        print(text)
        self.connect()
        return func(self,*args, **kwargs)
    def wrapper(self,*args, **kwargs):
        try:
            return func(self,*args, **kwargs)
        except pymysql.err.OperationalError as e:
            return handle_reconnect(self,"OperationalError. Trying to reconnect...",*args, **kwargs)
        except pymysql.err.InterfaceError as e:
            return handle_reconnect(self,"InterfaceError. Trying to reconnect...",*args, **kwargs)
        except Exception as e:
            print_tb(e)
            raise (e)
    return wrapper
@exceptionDecorator
def sendNotifies(events):
    for event in events:
        write_msg(event["userId"],event["randomId"],"Напоминаю: сегодня -" + event["message"])

def notifierThread(nextTimeNotify,sqlClient,lock):
    while True:
        lock.acquire()
        if datetime.datetime.now().timestamp() >= nextTimeNotify[0]:
            try:
                events = sqlClient.getEventsByTimestamp(nextTimeNotify[0])
                sendNotifies(events)
                sqlClient.clearEventsByEvents(events)
                nextTimeNotify[0] = sqlClient.getMinTimestamp()
            except Exception as e:
                print_tb(e)
                pass
        lock.release()
        time.sleep(1)

class SqlClient:
    def __init__(self,configFile):
        config = configparser.ConfigParser()
        config.read(configFile)
        self.db_config = config["DB"]
        self.connect()
    def connect(self):
        self.connection = pymysql.connect(host=self.db_config["host"],
                             user=self.db_config["user"],
                             password=self.db_config["password"],
                             db=self.db_config["db"],
                             charset='utf8mb4',
                             cursorclass=pymysql.cursors.DictCursor)
    @sqlExceptionDecorator
    def addUser(self,userId):
        with self.connection.cursor() as cursor:
            sql = "INSERT INTO `users` (`userId`) VALUES (%s)"
            cursor.execute(sql, (userId)) 
        self.connection.commit()
    @sqlExceptionDecorator
    def addEvent(self,event):
        with self.connection.cursor() as cursor:
            sql = "INSERT INTO `events` (`userId`, `randomId`, `timestamp`,`message`,`everyYear`) VALUES (%s,%s,%s,%s,%s)"
            cursor.execute(sql, (event['userId'],event['randomId'],event['timestamp'],event['message'],event['everyYear'])) 
        self.connection.commit()
    @sqlExceptionDecorator
    def getUsers(self):
        result = []
        with self.connection.cursor() as cursor:
            sql = "SELECT `userId` FROM `users`"
            cursor.execute(sql)
            result = [x['userId'] for x in cursor.fetchall()]
        return result
    @sqlExceptionDecorator
    def getEventByUserId(self, userId):
        result = []
        with self.connection.cursor() as cursor:
            sql = "SELECT * FROM `events` WHERE userID=%s"
            cursor.execute(sql,(str(userId)))
            result = cursor.fetchall()
        return result  
    @sqlExceptionDecorator      
    def getEventsByTimestamp(self,timestamp):   
        result = []
        with self.connection.cursor() as cursor:
            sql = "SELECT * FROM `events` WHERE timestamp=%s"
            cursor.execute(sql,(str(timestamp)))
            result = cursor.fetchall()
        return result 
    @sqlExceptionDecorator
    def clearEventsByEvents(self,events):
        with self.connection.cursor() as cursor:
            sqlDel = "DELETE FROM `events` WHERE id=%s"
            sqlUpdate = "UPDATE `events` SET timestamp = %s WHERE id=%s"
            for event in events:
                if event["everyYear"] == False:
                    cursor.execute(sqlDel, (event["id"])) 
                else:
                    startDate =  datetime.datetime.fromtimestamp(event["timestamp"])
                    newDate = startDate.replace(startDate.year + 1)
                    newTimestamp =newDate.timestamp()
                    cursor.execute(sqlUpdate, (newTimestamp,event["id"])) 
        self.connection.commit()
    @sqlExceptionDecorator
    def clearEventsByIndex(self,index,userId):
        events = self.getEventByUserId(userId)
        if (index < 0 or index >= len(events)): #for userfriendly i-face
            print("Incorrect index with size=", len(events))
            raise IndexError()
        with self.connection.cursor() as cursor:
            sqlDel = "DELETE FROM `events` WHERE id=%s"
            cursor.execute(sqlDel, (events[index]["id"]))
        self.connection.commit()
    @sqlExceptionDecorator
    def clearAllEvents(self,userId):
        with self.connection.cursor() as cursor:
            sqlDel = "DELETE FROM `events` WHERE userId=%s"
            cursor.execute(sqlDel, (userId)) 
        self.connection.commit()
    @sqlExceptionDecorator
    def getMinTimestamp(self):
        with self.connection.cursor() as cursor:
            sql = "SELECT MIN(timestamp) as min_timestamp FROM `events`"
            cursor.execute(sql)
            result = cursor.fetchone()
            return result['min_timestamp']
    @sqlExceptionDecorator
    def getEventsCount(self,userId):
        with self.connection.cursor() as cursor:
            sql ="SELECT COUNT(*) from events where userId=%s"
            cursor.execute(sql,(userId))
            result = cursor.fetchone()
            return result["COUNT(*)"]
    @sqlExceptionDecorator
    def checkVipUser(self,userId):
        with self.connection.cursor() as cursor:
            sql ="SELECT vip from users where userId=%s"
            cursor.execute(sql,(userId))
            result = cursor.fetchone()
            return bool(result["vip"])
def write_msg(user_id,random_id, message):
    vk.messages.send(user_id=user_id,random_id=random_id,message=message)
    print("Sending to user=" + str(user_id) + " message " + message)
def print_tb(e):
    print (''.join(traceback.TracebackException.from_exception(e).format()))
def getHelpMessage():
    msg =  'Со мной ты никогда не забудешь поздравить друга с днем рождения или про годовщину свадьбы своих родителей!'
    msg+= '\nЯ понимаю сообщения в следующем формате:\n1)add <date> <message> - Добавить напоминание с сообщением <message> в день <date>'
    msg+= '\n2)delete <index> - Удалить напоминание. Вместо <index> нужно указать порядковый номер записи, который можно узнать из команды print(см ниже)\n3)delete all - Удалить все напоминания\n4)print - Вывести список всех напоминаний'
    msg+= '\n5)help - вывести это сообщение'
    msg+= '\nФормат даты: DD.MM.YYYY HH:MM или DD.MM HH:MM. В первом случае напоминание сработает только один раз, во втором будет работать каждый год. Указывайте московское время.'
    msg+= '\nПримеры:\nadd 05.02.2021 12:00 День рождения Юли\n5 февраля в 12 часов дня по мск в 2021 году тебе придет напоминание от меня с текстом "Напоминаю: сегодня - День рождения Юли"'
    msg+= '\nadd 29.11 10:00 День матери\nКаждый год 29 ноября в 10 часов дня по мск тебе будет приходить напоминание от меня с текстом "Напоминаю: сегодня - День матери"'
    msg += "\n!!!Внимание!!! Количество символов для одного сообщения - не более ста, количество событий для одного пользователя - не более " + str(server_config["maxEventsPerUser"])
    msg += "\nДля увеличения кол-ва событий свяжитесь с автором(раздел контакты в группе)"
    return msg
def userInputToTimestamp(dateStr,timeStr,newEvent):
    now = datetime.datetime.now()
    formatString = "%d.%m.%Y %H:%M"
    datetimeStr = dateStr+ ' ' + timeStr
    if len(dateStr) > 5:
        newEvent["everyYear"] = False
        timestamp = dateToTimestamp(datetimeStr,formatString)
    else:
        newEvent["everyYear"] = True
        checkDateTimeStr = dateStr + '.' + str(now.year) + ' ' + timeStr
        checkTimestamp = dateToTimestamp(checkDateTimeStr,formatString)
        if checkTimestamp > now.timestamp():
            timestamp = checkTimestamp
        else:
            checkDateTimeStr = dateStr + '.' + str(now.year + 1) + ' ' + timeStr
            timestamp =dateToTimestamp(checkDateTimeStr,formatString)
    newEvent["timestamp"] = timestamp
def formatEvents(events):
    resStr = ""
    index = 1
    for event in events:
        resStr += (str(index) + ") " + timestampToDate(event["timestamp"]) + " " + event["message"] + '\n')
        index += 1
    return resStr
def timestampToDate(timestamp):
    return datetime.datetime.fromtimestamp(timestamp).strftime("%d.%m.%Y %H:%M")
def dateToTimestamp(dateTimeStr,formatString):
    try:
        return time.mktime(datetime.datetime.strptime(dateTimeStr,formatString).timetuple())
    except ValueError as e:
        print_tb(e)
        raise DateFormatError("Неверный формат даты")
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--token', type=str,
                        help='your vk group token')
    parser.add_argument('--config',
                        help='path to config file')
    args = parser.parse_args()
    vk_session = vk_api.VkApi(token=args.token)
    longpoll = VkLongPoll(vk_session)
    vk = vk_session.get_api()
    config = configparser.ConfigParser()
    config.read(args.config)
    server_config = config["Server"]
    sqlClient = SqlClient(args.config)
    usersList = sqlClient.getUsers()
    lock = Lock()
    nextTime =[]
    minTime = int(sqlClient.getMinTimestamp())
    nextTime.append(minTime)
    notifierWorker = Thread(target=notifierThread,args=(nextTime,sqlClient,lock,))
    notifierWorker.start()
    maxEventsCountDict ={}
    for userId in usersList:
        maxEventsCount = int(server_config["maxEventsPerUser"])
        if sqlClient.checkVipUser(userId):
            maxEventsCount = int(server_config["maxEventsPerVip"])
        maxEventsCountDict[userId] = maxEventsCount
    print("main cycle started")
    while True:
        try:
            for event in longpoll.listen():
                if event.type == VkEventType.MESSAGE_NEW and event.to_me:
                    print(event.user_id, " got message ", event.text)
                    if str(event.user_id) in usersList: #TODO: refact str using
                        try:
                            request = event.text.split()
                            command = request[0].lower()
                            if command == "help":
                                write_msg(event.user_id,event.random_id,getHelpMessage())
                            elif command == "add":
                                count = sqlClient.getEventsCount(event.user_id)
                                if count > maxEventsCountDict[str(event.user_id)]:
                                    write_msg(event.user_id,event.random_id, "Превышен лимит событий на пользователя. Удалите лишние события или свяжитесь с автором для увеличения лимита")
                                    continue
                                newEvent = {}
                                dateStr = request[1]
                                timeStr = request[2]
                                userInputToTimestamp(dateStr,timeStr,newEvent)
                                del request[0:3]
                                message = ' '.join(request)
                                if len(message) > int(server_config["maxSymbolsPerMessage"]):
                                    write_msg(event.user_id,event.random_id, "Слишком длинное сообщение")
                                    continue
                                newEvent['userId'] = event.user_id
                                newEvent['randomId'] = event.random_id
                                newEvent['message'] = message[:-1]
                                sqlClient.addEvent(newEvent)
                                count += 1 
                                lock.acquire()
                                nextTime[0] = sqlClient.getMinTimestamp()
                                lock.release()             
                                write_msg(event.user_id, event.random_id,'Событие зарегистрировано! Осталось событий: ' + str(maxEventsCountDict[str(event.user_id)] - count))
                            elif command == "print":
                                pr_events = sqlClient.getEventByUserId(event.user_id)
                                msg = "Зарегистрированные события:\n" if len(pr_events) > 0 else "Нет зарегестрированных событий\n"
                                write_msg(event.user_id,event.random_id,msg + formatEvents(pr_events) + "Осталось событий: " + str(maxEventsCountDict[str(event.user_id)] - sqlClient.getEventsCount(event.user_id)))
                            elif command == "delete":
                                if request[1] == "all":
                                    sqlClient.clearAllEvents(event.user_id)
                                    write_msg(event.user_id,event.random_id, "Все события удалены!")
                                else:
                                    try:
                                        index = int(request[1]) - 1 #indexes for user starts with 1
                                        sqlClient.clearEventsByIndex(index,event.user_id)
                                        write_msg(event.user_id,event.random_id, "Событие удалено! Осталось событий: " + str(maxEventsCountDict[str(event.user_id)] - sqlClient.getEventsCount(event.user_id)))
                                    except IndexError as e:
                                        write_msg(event.user_id,event.random_id,"Некорректный номер записи")
                            else:
                                write_msg(event.user_id,event.random_id, "Неизвестный формат сообщения")
                        except DateFormatError as e:
                            write_msg(event.user_id,event.random_id,str(e))
                        except Exception as e:
                            print_tb(e)
                            write_msg(event.user_id,event.random_id,'Что-то пошло не так')
                    else:
                        write_msg(event.user_id,event.random_id,'И тебе привет! '+ getHelpMessage())
                        sqlClient.addUser(event.user_id)
                        usersList.append(str(event.user_id))
                        maxEventsCount = int(server_config["maxEventsPerUser"])
                        if sqlClient.checkVipUser(event.user_id):
                            maxEventsCount = int(server_config["maxEventsPerVip"])
                        maxEventsCountDict[str(event.user_id)] = maxEventsCount
                        
        except requests.exceptions.ReadTimeout:
            print("request timeout")
        except Exception as e:
            print_tb(e)

