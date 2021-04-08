import requests as req
import datetime
curtime=datetime.datetime.now()-datetime.timedelta(seconds=43200) #Получаем текущую дату !!!!ДЕЛЬТА УЧИТЫВАЕТ ЧАСОВОЙ ПОЯС!!!
date_str=datetime.datetime.strftime(curtime, '%Y%m%d-%H') #Конвертим дату в стринг в формате 20210408-11
#Создаем списки каналов для каждой станции
KLYT=['KLYT***HAE', 'KLYT***HAN', 'KLYT***HK2']
IVST=['IVST***HAE','IVST***HAN','IVST***HK2']
Durations=3600 #Длительность выборки 1 час
url='http://hub2.emsd.ru:9000/' # Адрес нашего сервера
print(date_str)

def get_file(date_str,channel,request): # Процедура запроса на вход дата, канал, строка запроса
    print(request)
    resp = req.get(request) #Запрос
    content=resp.content #Присваеваем содержимое запроса
    station_name=channel[0:4] # Получаем название станции формат KLYT
    print(station_name)
    channel=channel[7:10] #Название канала формат HAE
    print(channel)
    file_name=date_str+'_'+station_name+'_'+channel+'.msd' #Создаем имя файла формата 20210408-06_IVST_HAE.msd
    with open(file_name, 'wb') as file: #Открываем файл на запись
        file.write(content) #Записываем файл

#Создаем строки запросов для каждого канала это для KLYT
for every in KLYT: #Перебираем списки каналов
    #Создаем строку запроса: сервер+дата в формате 20210408-05+минуты 00 + секунды 00 расширение с ? и запрос =
    #канал+ год + %2F + месяц + %2F + день ++ час + %3A00 минуты 00 %3A00 секунды 00 + длительность запроса 3600
    request=url+date_str+'-00-00.msd?DATREQ='+every+'+'+date_str[0:4]+'%2F'+date_str[4:6]+'%2F'+date_str[6:8]+'+'+date_str[9:11]+'%3A00%3A00+3600'
    get_file(date_str,every,request) #Процедура запроса. На вход дата формате 20210408-05б, канал, строка запроса)
for every in IVST:
    request=url+date_str+'-00-00.msd?DATREQ='+every+'+'+date_str[0:4]+'%2F'+date_str[4:6]+'%2F'+date_str[6:8]+'+'+date_str[9:11]+'%3A00%3A00+3600'
    get_file(date_str,every,request)
