import requests as req
import datetime
import glob
import obspy
import pandas as pd

curtime=datetime.datetime.now()-datetime.timedelta(seconds=46800) #Получаем текущую дату !!!!ДЕЛЬТА УЧИТЫВАЕТ ЧАСОВОЙ ПОЯС+ еще один час чтобы получить уже прошедший час!!!
date_str=datetime.datetime.strftime(curtime, '%Y%m%d-%H') #Конвертим дату в стринг в формате 20210408-11
#Создаем списки каналов для каждой станции
KLYT=['KLYT***HAE', 'KLYT***HAN', 'KLYT***HK2']
IVST=['IVST***HAE','IVST***HAN','IVST***HK2']
Durations=3600 #Длительность выборки 1 час
url='http://hub2.emsd.ru:9000/' # Адрес нашего сервера
def decimate(datas):
    decimated_st=datas.decimate(6000,strict_length=False, no_filter=True)
    return(decimated_st)

def make_df_temp(file_name,station_name,channel,df_temp):
    st=obspy.read(file_name)
    if station_name =='KLYT' and channel=='HAE':
        decimated_st=decimate(st[0])
        df_temp=pd.Series(decimated_st.data)
  #      df_temp=pd.DataFrame({'HAE':decimated_st.data})
    elif station_name =='KLYT' and channel=='HAN':
        decimated_st=decimate(st[0])
        df_temp=pd.Series(decimated_st.data)
 #       df_temp=pd.DataFrame({'HAN':decimated_st.data})
        
    return(df_temp)

def get_file(date_str,channel,request,df_temp): # Процедура запроса на вход дата, канал, строка запроса
    resp = req.get(request) #Запрос
    content=resp.content #Присваеваем содержимое запроса
    station_name=channel[0:4] # Получаем название станции формат KLYT
    channel=channel[7:10] #Название канала формат HAE
    file_name=date_str+'_'+station_name+'_'+channel+'.msd' #Создаем имя файла формата 20210408-06_IVST_HAE.msd
    with open(file_name, 'wb') as file: #Открываем файл на запись
        file.write(content) #Записываем файл
    df_temp=make_df_temp(file_name,station_name,channel,df_temp)
    return(df_temp)
    
    
columns=['HAE','HAN','HK2']
#df_temp=pd.Series()
df=pd.DataFrame(columns=columns)
#Создаем строки запросов для каждого канала это для KLYT
for every in KLYT: #Перебираем списки каналов
    #Создаем строку запроса: сервер+дата в формате 20210408-05+минуты 00 + секунды 00 расширение с ? и запрос =
    #канал+ год + %2F + месяц + %2F + день ++ час + %3A00 минуты 00 %3A00 секунды 00 + длительность запроса 3600
    request=url+date_str+'-00-00.msd?DATREQ='+every+'+'+date_str[0:4]+'%2F'+date_str[4:6]+'%2F'+date_str[6:8]+'+'+date_str[9:11]+'%3A00%3A00+3600'
    df_temp=pd.Series(get_file(date_str,every,request,df_temp)) #Процедура запроса. На вход дата формате 20210408-05б, канал, строка запроса,пустой df_temp
    df=pd.DataFrame(data=df_temp,columns=every[7:10] )

print(df)   
# for every in IVST:
#      request=url+date_str+'-00-00.msd?DATREQ='+every+'+'+date_str[0:4]+'%2F'+date_str[4:6]+'%2F'+date_str[6:8]+'+'+date_str[9:11]+'%3A00%3A00+3600'
#      get_file(date_str,every,request,df_temp)
