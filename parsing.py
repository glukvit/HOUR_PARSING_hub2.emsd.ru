import requests as req
import datetime
import glob
import obspy
import pandas as pd
import os

path = '/home/gluk/TEMP_DATA/STREAM/KLYT_hour_averaging.csv' #Тестовый путь !!!ПОМЕНЯТЬ НА РАБОЧЕМ СКРИПТЕ!!!
curtime = datetime.datetime.now()-datetime.timedelta(seconds=46800) #Получаем текущую дату !!!!ДЕЛЬТА УЧИТЫВАЕТ ЧАСОВОЙ ПОЯС+ еще один час чтобы получить уже прошедший час!!!
date_str = datetime.datetime.strftime(curtime, '%Y%m%d-%H') #Конвертим дату в стринг в формате 20210408-11
date_for_fin_df = datetime.datetime.strftime(curtime, '%Y%m%d%H')
print(date_for_fin_df)

#Создаем списки каналов для каждой станции
KLYT = ['KLYT***HAE', 'KLYT***HAN', 'KLYT***HK2']
Durations=3600 #Длительность выборки 1 час
url = 'http://hub2.emsd.ru:9000/' # Адрес нашего сервера

def df_mean(df): #Создание окончательно датафрейм который запишется в файл
    df_fin = pd.DataFrame()#Пустой датафрейм куда запишем окончательные осредненные данные
    df_fin.loc[0,'DATE'] = date_for_fin_df #В окончательный датафрейм первый столбец дата в формате 2021041005
    df_fin.loc[0,'HAE'] = df['HAE'].mean() #Осредняем до одного столбец с минутными отсчетами до одного часа
    df_fin.loc[0,'HAN'] = df['HAN'].mean()
    df_fin.loc[0,'HK2'] = df['HK2'].mean()
    return(df_fin)

def decimate(datas): #Децимация. 
    decimated_st = datas.decimate(6000,strict_length = False, no_filter = True) #6000 фактор децимации делает из 100 Гц. один отсчет в минуту. 
    return(decimated_st)#Возвращает дицимированные данные в сейсмическом формате в поцедуру make_df_temp

def make_df_temp(file_name,station_name,channel): #Временный датафрейм.Децимация. На вход имя обрабатываемого msd 
    st=obspy.read(file_name) # Открываем как сейсмический формат
#    print(st[0].stats)
    if station_name =='KLYT' and channel=='HAE': #Если имя станции и канал совпадают то:
        decimated_st=decimate(st[0]) #Берем один канал (он единственный в файле) и децимируем
        df_temp=pd.DataFrame({'HAE':decimated_st.data}) #Записываем децимированные данные в соответстующий столбец временной датафрейм
    elif station_name =='KLYT' and channel=='HAN':
        decimated_st=decimate(st[0])
        df_temp=pd.DataFrame({'HAN':decimated_st.data})
    elif station_name =='KLYT' and channel=='HK2':
        decimated_st=decimate(st[0])
        df_temp=pd.DataFrame({'HK2':decimated_st.data})
  #  print(decimated_st.data)
    os.remove(file_name) #Удаляем msd файл
    return(df_temp) #Возвращаем временный датафрейм с децимированными данными в основной цикл

def get_file(date_str,channel,request): # Процедура запроса на вход дата, канал, строка запроса
    resp = req.get(request) #Запрос
    content=resp.content #Присваеваем содержимое запроса
    station_name=channel[0:4] # Получаем название станции формат KLYT
    channel=channel[7:10] #Название канала формат HAE
    file_name=date_str+'_'+station_name+'_'+channel+'.msd' #Создаем имя файла формата 20210408-06_IVST_HAE.msd
    with open(file_name, 'wb') as file: #Открываем файл на запись
        file.write(content) #Записываем файл
    df_temp=make_df_temp(file_name,station_name,channel) #Процедура создания временного датафрейм. На вход имяю.msd файла, название станции, имя канала
#    print(df_temp)
    return(df_temp)
    
df=pd.DataFrame() #Объявляем пустой датафрей. В него запишием все каналы с дискретизацией 1 отсчет в минуту.
df_fin=pd.DataFrame()#Пустой датафрейм куда запишем окончательные осредненные данные добавляемые в файл.
#Создаем строки запросов для каждого канала это для KLYT
for every in KLYT: #Перебираем списки каналов
    #Создаем строку запроса: сервер+дата в формате 20210408-05+минуты 00 + секунды 00 расширение с ? и запрос =
    #канал+ год + %2F + месяц + %2F + день ++ час + %3A00 минуты 00 %3A00 секунды 00 + длительность запроса 3600
    request=url+date_str+'-00-00.msd?DATREQ='+every+'+'+date_str[0:4]+'%2F'+date_str[4:6]+'%2F'+date_str[6:8]+'+'+date_str[9:11]+'%3A00%3A00+3600'
    df_temp=get_file(date_str,every,request) #Процедура запроса. На вход дата формате 20210408-05б, канал, строка запроса,пустой df_temp
    df=pd.concat([df,df_temp], axis=1) #Объединяем финальный датафрейм df и фрейм на котором новые данные df_temp. Добавляются столбцы

df_new_line = df_mean(df) #Осредняем в процедурем mean. Получаем одну строку за 1 час вместо 60 минутных строк.
#Дозапись новых данный в существующий файл датафрейм двухнедельных часовых файлов.
if os.path.exists(path): # Проверяем наличие двухнедельного файла 
    df_fin=pd.read_csv(path) #Если есть открываем файл
    if len(df_fin) > 336: # Проверяем длину файла если больше чем 336 строк(2 недели по одному часу) то:
        df_fin.drop(index=0, inplace=True) # Удаляем первую строку (старые данные) по индексу
        df_fin=df_fin.reset_index(drop=True) #Восстанавливаем порядок индексов
df_fin=pd.concat([df_fin,df_new_line]) # Добавляем новые данные из df_new_line в записываемый в файл df_fin. 
df_fin=df_fin.drop_duplicates(subset=['DATE'],keep=False) #Если есть дубликаты в строках удаляем. Проверка дубликатов по столбцу 'DATE'
#df_fin=df_fin.reset_index(drop=True) #Если не сбросить индекс записывает без удалений дубликатов!!!
df_fin.to_csv(path, index=False) #Тестовый путь
