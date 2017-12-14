from django.shortcuts import render
from django.shortcuts import render_to_response,redirect
from django.template.context_processors import csrf
import json
import requests
from django.http import JsonResponse
from django.views.decorators.csrf import csrf_exempt
from CH.forms import RequestForm
import pprint
@csrf_exempt
def main(request):
    def MaxLenNum(array_dates):
        """Возвращает порядок элемента списка с наибольшей длиной"""
        max_len = 0
        for array_num in range(len(array_dates)):
            if len(array_dates[array_num]) > len(array_dates[max_len]):
                max_len = array_num
        return(max_len)

    def RecStats(n,i,updimensions,table,up_dim_info):
        """Рекурсивный метод для добавления вложенных структур в stats"""
        #Добавляем фильтры
        try:
            updimensions.pop(n)
        except:
            pass
        #Добавляем параметры в список с параметрами верхних уровней
        #Если предыдущий уровень-сегмент, не добавляем фильтр
        try:
            updimensions.append(
            "{updimension}='{updimension_val}'".format(updimension_val=i[dimensionslist_with_segments[n]],
                                                       updimension=dimensionslist_with_segments[n]))
        except:
            pass
        sub=[]
        if type(dimensionslist_with_segments[n+1]) == list:
            num_seg = len(dimensionslist_with_segments[n+1])
        else:
            num_seg = 0
        #Добавляем информацию о верхнем уровне в группу сегментов
        if (num_seg!=0 or 'segment' in dimensionslist_with_segments[n+1]) and up_dim_info != 1:
            up_dim_info['label']='Все данные'
            sub.append(up_dim_info)
        for num in range(num_seg):
            array_dates = []
            updimensions = []
            seg = json.loads(requests.get(
                'https://s.analitika.online/api/reference/segments/{num_seg}/'.format(num_seg=int(dimensionslist_with_segments[n+1][num][7:])),
                headers=headers).content.decode('utf-8'))['real_definition']
            seg_filt = seg.partition("==")[0] + "=='" + seg.partition("==")[2] + "'"
            seg_label = json.loads(requests.get(
                'https://s.analitika.online/api/reference/segments/{num_seg}/'.format(num_seg=int(dimensionslist_with_segments[n+1][num][7:])),
                headers=headers).content.decode('utf-8'))['name']
            updimensions.append(seg_filt)
            updimensions.append(seg_filt)
            updm = ' AND '.join(updimensions)
            counter=0
            for date in period:
                q = '''SELECT '{label_val}' as label,'{segment_val}' as segment,{metric_counts} FROM {table}
                                                                      WHERE 1 {filt} AND {date_field} BETWEEN '{date1}' AND '{date2}' AND {updimensions}
                                                                      FORMAT JSON
                                                                    '''.format(
                    label_val=seg_label,
                    segment_val=seg,
                    updimensions=updm,
                    metric_counts=metric_counts,
                    date1=date['date1'],
                    date2=date['date2'], filt=filt,
                    table=table, date_field=date_field)

                array_dates.append(json.loads(get_clickhouse_data(q, 'http://85.143.172.199:8123'))['data'])
                # Если сегмент пуст, добавляем нулевые значение в dates
                if array_dates[counter] == []:
                    empty_dict = {'label': seg_label,
                                  'segment': seg}
                    for metric in metrics:
                        empty_dict[metric] = 0
                    array_dates[counter].append(empty_dict)
                counter+=1
            counter=0
            for i in array_dates[0]:
                stat_dict = {'label': i['label'],
                             'segment': i['segment']}
                dates = []

                for m in range(len(array_dates)):
                    metrics_dict = dict.fromkeys(metrics)
                    for j in metrics_dict:
                        try:
                            metrics_dict[j] = array_dates[m][counter][j]
                        except:
                            metrics_dict[j]=0
                    dates.append({'date1': period[m]['date1'], 'date2': period[m]['date2'], 'metrics': metrics_dict})
                stat_dict['dates'] = dates
                if len(dimensionslist_with_segments) > n+2:
                    # Добавляем подуровень
                    stat_dict['sub'] = RecStats(n+1, i, updimensions, table,1)
                sub.append(stat_dict)
                counter+=1
        # Если dimension является сегментом(не группой сегментов а отдельным сегментом):
        if 'segment' in dimensionslist_with_segments[n+1]:
            array_dates = []
            array_dates = []
            updimensions = []
            seg = json.loads(requests.get(
                'https://s.analitika.online/api/reference/segments/{num_seg}/'.format(num_seg=int(dimensionslist_with_segments[n+1][7:])),
                headers=headers).content.decode('utf-8'))['real_definition']
            seg_filt = seg.partition("==")[0] + "=='" + seg.partition("==")[2] + "'"
            seg_label = json.loads(requests.get(
                'https://s.analitika.online/api/reference/segments/{num_seg}/'.format(num_seg=int(dimensionslist_with_segments[n+1][7:])),
                headers=headers).content.decode('utf-8'))['name']
            updimensions.append(seg_filt)
            updimensions.append(seg_filt)
            updm = ' AND '.join(updimensions)
            for date in period:
                q = '''SELECT '{label_val}' as label,'{segment_val}' as segment,{metric_counts} FROM {table}
                                                                             WHERE 1 {filt} AND {date_field} BETWEEN '{date1}' AND '{date2}' AND {seg_filt} AND {updm}
                                                                             FORMAT JSON
                                                                           '''.format(
                    label_val=seg_label,
                    segment_val=seg,
                    seg_filt=seg_filt,
                    updm=updm,
                    metric_counts=metric_counts,
                    date1=date['date1'],
                    date2=date['date2'], filt=filt,
                    table=table, date_field=date_field)
                array_dates.append(json.loads(get_clickhouse_data(q, 'http://85.143.172.199:8123'))['data'])
                if array_dates[0]==[]:
                    empty_dict={'label':seg_label,
                                           'segment':seg}
                    for metric in metrics:
                        empty_dict[metric]=0
                    array_dates[0].append(empty_dict)
            counter=0
            for i in array_dates[0]:
                stat_dict = {'label': i['label'],
                             'segment': i['segment'], }
                dates = []

                for m in range(len(array_dates)):
                    metrics_dict = dict.fromkeys(metrics)
                    for j in metrics_dict:
                        try:
                            metrics_dict[j] = array_dates[m][counter][j]
                        except:
                            metrics_dict[j]=0
                    dates.append({'date1': period[m]['date1'], 'date2': period[m]['date2'], 'metrics': metrics_dict})
                stat_dict['dates'] = dates
                # если размер dimensions больше 1, заполняем подуровень
                if len(dimensionslist_with_segments) > n+2:
                    # Добавляем подуровень
                    up_dim = stat_dict.copy()
                    stat_dict['sub'] = RecStats(n+1, i, updimensions, table, up_dim)
                sub.append(stat_dict)
                counter+=1
        elif num_seg==0:
            array_dates = []
            updm=' AND '.join(updimensions)
            dimension = dimensionslist_with_segments[n + 1]
            if sort_column == "":
                sort_column_in_query = dimension
            else:
                sort_column_in_query = sort_column
            for date in period:
                q = '''SELECT {dimension},{metric_counts} FROM {table}
                                                                    WHERE 1 {filt} AND {updimensions} AND {date_field} BETWEEN '{date1}' AND '{date2}'
                                                                    GROUP BY {dimension} {having}
                                                                    ORDER BY {sort_column} {sort_order}
                                                                    FORMAT JSON
                                                                    '''.format(dimension=dimension,
                                                                               updimensions=updm,sort_column=sort_column_in_query,sort_order=sort_order,
                                                                               metric_counts=metric_counts,
                                                                               date1=date['date1'],
                                                                               date2=date['date2'], filt=filt,
                                                                               having=having, table=table,
                                                                               date_field=date_field)

                array_dates.append(json.loads(get_clickhouse_data(q, 'http://85.143.172.199:8123'))['data'])
            counter=0
            for i2 in array_dates[MaxLenNum(array_dates)]:
                stat_dict = {'label': i2[dimensionslist_with_segments[n + 1]],
                             'segment':'{label}=={value}'.format(label=dimensionslist_with_segments[n + 1]
                                                                 ,value=i2[dimensionslist_with_segments[n + 1]])}
                dates = []

                for m in range(len(array_dates)):
                    metrics_dict = dict.fromkeys(metrics)
                    for j in metrics_dict:
                        try:
                            metrics_dict[j] = array_dates[m][counter][j]
                        except:
                            metrics_dict[j]=0
                    dates.append({'date1': period[m]['date1'], 'date2': period[m]['date2'], 'metrics': metrics_dict})
                stat_dict['dates'] = dates
                if n != len(dimensionslist_with_segments) - 2:
                    up_dim=stat_dict.copy()#Передаем словарь с информацией о вернем уровне "Все файлы"
                    stat_dict['sub'] = RecStats(n + 1, i2, updimensions, table,up_dim)
                sub.append(stat_dict)
                counter+=1
        return sub
    def get_clickhouse_data(query,host,connection_timeout=1500):
        """Метод для обращения к базе данных CH"""
        r=requests.post(host,params={'query':query},timeout=connection_timeout)
        return r.text
    def AddCounts(period,dimension_counts,filt,sort_order,table):
        """Добавление ключа Counts в ответ"""
        date_filt=[]
        for dates in period:
            date_filt.append("({date_field} BETWEEN '".format(date_field=date_field)+str(dates['date1'])+"' AND '"+str(dates['date2'])+"')")
        date_filt=' OR '.join(date_filt)
        if date_filt==[]:
            date_filt=1

        q = ''' SELECT {dimension_counts}
                     FROM {table}
                     WHERE 1 {filt} AND {date_filt}
                     ORDER BY NULL {sort_order}
                     FORMAT JSON
                    '''.format(date1=period[0]['date1'], date2=period[0]['date2'], dimension_counts=dimension_counts, filt=filt,
                               sort_order=sort_order,table=table,date_filt=date_filt)
            # Объеденяем словарь с датами со словарем  вернувшихся значений каждого из запрошенных параметров
        a = json.loads(get_clickhouse_data(q, 'http://85.143.172.199:8123'))['data'][0]
            #изменяем значения показателей на целочисленный тип
        b={}
        for key in a.keys():
            b[key[1:]]=a[key]
        #except:
            #a=dict.fromkeys(dimensionslist,0)
        return b
    def AddMetricSums(period,metric_counts,filt,metrics,sort_order,table):
        """Добавление ключа metric_sums в ответ"""
        dates = []
        for date in period:
            # Запрос на получение сумм показателей без фильтра
            q_total = ''' SELECT {metric_counts}
                    FROM {table}
                    WHERE {date_field} BETWEEN '{date1}' AND '{date2}'
                    ORDER BY NULL {sort_order}
                    FORMAT JSON
                   '''.format(date1=date['date1'], date2=date['date2'], metric_counts=metric_counts,
                              sort_order=sort_order,table=table,date_field=date_field)

            # С фильтром
            q = ''' SELECT {metric_counts}
                                FROM {table}
                                WHERE 1 {filt} AND {date_field} BETWEEN '{date1}' AND '{date2}'
                                ORDER BY NULL {sort_order}
                                FORMAT JSON
                               '''.format(date1=date['date1'], date2=date['date2'], metric_counts=metric_counts,
                                          filt=filt, sort_order=sort_order,table=table,date_field=date_field)
            #Проверка на существование записей, если их нет, возвращаем нули
            try:
                a = json.loads(get_clickhouse_data(q_total, 'http://85.143.172.199:8123'))['data'][0]
                b = json.loads(get_clickhouse_data(q, 'http://85.143.172.199:8123'))['data'][0]
                # Создаем словарь, ключи которого это элементы списка metrics
                metric_dict = dict.fromkeys(metrics)
                # Заполняем этот словарь полученными из базы данными
                for (i, j, k) in zip(metric_dict, a, b):
                    metric_dict[i] = {"total_sum": a[j], "sum": b[k]}
            except:
                metric_dict=dict.fromkeys(metrics)
                for i in metric_dict:
                    metric_dict[i]={"total_sum":0,"sum":0}
            # Добавляем в него даты
            metric_dict.update(date)
            dates.append(metric_dict)
        return dates
    def AddMetricSumsWithFilt(period,metric_counts,filt,metrics,sort_order,table):
        ar_d=[]
        for date in period:
            t = '''SELECT {metric_counts} FROM {table}
                    WHERE 1 {filt} AND {date_field} BETWEEN '{date1}' AND '{date2}'
                    FORMAT JSON
                    '''.format(
                metric_counts=metric_counts,
                date1=date['date1'],
                date2=date['date2'], filt=filt,
                table=table, date_field=date_field)

            ar_d.append(json.loads(get_clickhouse_data(t, 'http://85.143.172.199:8123'))['data'])
        counter=0
        for i in ar_d[0]:
            try:
                st_d = {'label': 'Все данные',
                            'segment': filter.replace(',',' OR ').replace(';',' AND ') }
            except:
                st_d = {'label': 'Все данные',
                             'segment': ''}

            dates = []

            for m in range(len(ar_d)):
                metrics_dict = dict.fromkeys(metrics)
                for j in metrics_dict:
                    metrics_dict[j] = ar_d[m][counter][j]
                dates.append({'date1': period[m]['date1'], 'date2': period[m]['date2'], 'metrics': metrics_dict})
            st_d['dates'] = dates
            counter+=1
        return st_d
    def AddStats(dim,metric_counts,filt,limit,having,period,metrics,table):
        """Добавление ключа stats в ответ без сегментов"""

        stats = []
        array_dates = []
        updimensions = []
        if 'segment' in dim[0]:
            seg_filt = segments[int(dimensionslist_with_segments[0][7:])-1]['segment_filt']
            for date in period:
                q = '''SELECT {metric_counts} FROM {table}
                                                        WHERE 1 {filt} AND {date_field} BETWEEN '{date1}' AND '{date2}' AND {seg_filt}
                                                        FORMAT JSON
                                                        '''.format(seg_filt=seg_filt,
                                                                   metric_counts=metric_counts,
                                                                   date1=date['date1'],
                                                                   date2=date['date2'],filt=filt,table=table,date_field=date_field)
                array_dates.append(json.loads(get_clickhouse_data(q, 'http://85.143.172.199:8123'))['data'])
                updimensions.append(seg_filt)
                updimensions.append(seg_filt)

        else:
            for date in period:
                q = '''SELECT {dimensions},{metric_counts} FROM {table}
                                   WHERE 1 {filt} AND {date_field} BETWEEN '{date1}' AND '{date2}'
                                   GROUP BY {dimensions}  {having}
                                   {limit}
                                   FORMAT JSON
                                   '''.format(dimensions=dim[0], metric_counts=metric_counts,
                                              date1=date['date1'],
                                              date2=date['date2'], filt=filt, limit=limit,
                                              having=having,table=table,date_field=date_field)
                array_dates.append(json.loads(get_clickhouse_data(q, 'http://85.143.172.199:8123'))['data'])

        for i in array_dates[0]:
            if 'segment' in dim[0]:
                stat_dict = {'label': segments[int(dim[0][7:])-1]['label'],'segment':segments[int(dim[0][7:])-1]['segment']}
            else:
                stat_dict = {'label': i[dim[0]]}
            dates = []
            metrics_dict = dict.fromkeys(metrics)
            for m in range(len(array_dates)):
                for (j, k) in zip(metrics_dict, i):
                    metrics_dict[j] = i[j]
                dates.append({'date1': period[m]['date1'], 'date2': period[m]['date2'], 'metrics': metrics_dict})
            stat_dict['dates'] = dates

            if len(dim) > 1:
                # Добавляем подуровень
                try:
                    updimensions.append("{updimension}='{updimension_val}'".format(updimension_val=i[dim[0]],
                                                                               updimension=dim[0]))
                except:
                    pass
                stat_dict['sub'] = RecStats(0, i, updimensions,table)
            stats.append(stat_dict)
        return stats
    def AddStats2(dim, metric_counts, filt, limit, having, period, metrics, table):
        """Добавление ключа stats в ответ"""
        stats = []
        #Определяем, есть ли вначале dimensions группа сегментов
        if type(dim[0])==list:
            num_seg=len(dim[0])
        else:
            num_seg=0
        if 'segment' in dim[0] or num_seg!=0:
            stats.append(AddMetricSumsWithFilt(period, metric_counts, filt, metrics, sort_order, table))
        #Если dimension является сегментом(не группой сегментов а отдельным сегментом):
        if 'segment' in dim[0]:

            array_dates = []
            updimensions = []
            seg=json.loads(requests.get('https://s.analitika.online/api/reference/segments/{num_seg}/'.format(num_seg=int(dim[0][7:])),
                                headers=headers).content.decode('utf-8'))['real_definition']
            seg_filt=seg.partition("==")[0]+"=='"+seg.partition("==")[2]+"'"
            seg_label=json.loads(requests.get('https://s.analitika.online/api/reference/segments/{num_seg}/'.format(num_seg=int(dim[0][7:])),
                                headers=headers).content.decode('utf-8'))['name']

            for date in period:
                q = '''SELECT '{label_val}' as label,'{segment_val}' as segment,{metric_counts} FROM {table}
                                                                      WHERE 1 {filt} AND {date_field} BETWEEN '{date1}' AND '{date2}' AND {seg_filt}
                                                                      FORMAT JSON
                                                                    '''.format(
                    label_val=seg_label,
                    segment_val=seg,
                    seg_filt=seg_filt,
                    metric_counts=metric_counts,
                    date1=date['date1'],
                    date2=date['date2'], filt=filt,
                    table=table, date_field=date_field)

                array_dates.append(json.loads(get_clickhouse_data(q, 'http://85.143.172.199:8123'))['data'])

            counter=0
            for i in array_dates[0]:
                stat_dict = {'label': i['label'],
                                'segment': i['segment'],}
                dates = []

                for m in range(len(array_dates)):
                    metrics_dict = dict.fromkeys(metrics)
                    for j in metrics_dict:
                        try:
                            metrics_dict[j] = array_dates[m][counter][j]
                        except:
                            metrics_dict[j]=0
                    dates.append({'date1': period[m]['date1'], 'date2': period[m]['date2'], 'metrics': metrics_dict})
                stat_dict['dates'] = dates

                # если размер dimensions больше 1, заполняем подуровень
                if len(dim) > 1:
                        # Добавляем подуровень
                    updimensions.append(seg_filt)
                    updimensions.append(seg_filt)
                    up_dim = stat_dict.copy()
                    stat_dict['sub'] = RecStats(0, i, updimensions, table,up_dim)
                stats.append(stat_dict)
                counter+=1
        elif num_seg==0:
            updimensions = []
            array_dates = []
            if sort_column=="":
                sort_column_in_query=dim[0]
            else:
                sort_column_in_query=sort_column
            for date in period:
                q = '''SELECT {dimension},{metric_counts} FROM {table}
                                   WHERE 1 {filt} AND {date_field} BETWEEN '{date1}' AND '{date2}'
                                   GROUP BY {dimension}  {having}
                                   ORDER BY {sort_column} {sort_order}
                                   {limit}
                                   FORMAT JSON
                                   '''.format(dimension=dim[0], metric_counts=metric_counts,
                                              date1=date['date1'],sort_column=sort_column_in_query,
                                              date2=date['date2'], filt=filt, limit=limit,sort_order=sort_order,
                                              having=having, table=table, date_field=date_field)

                array_dates.append(json.loads(get_clickhouse_data(q, 'http://85.143.172.199:8123'))['data'])
                #определим самый большой список в array_dates
            counter=0
            for i in array_dates[MaxLenNum(array_dates)]:
                stat_dict = {'label': i[dim[0]],
                             'segment': '{label}=={value}'.format(label=dim[0]
                                                                  , value=i[dim[0]])
                             }
                #print(stat_dict)
                dates = []
                for m in range(len(array_dates)):
                    metrics_dict = dict.fromkeys(metrics)
                    for j in metrics_dict:
                        try:
                            metrics_dict[j] = array_dates[m][counter][j]
                        except:
                            metrics_dict[j]=0
                    dates.append({'date1': period[m]['date1'], 'date2': period[m]['date2'], 'metrics': metrics_dict})

                stat_dict['dates'] = dates
                if len(dim) > 1:
                    # Добавляем подуровень
                    try:
                        updimensions.append("{updimension}='{updimension_val}'".format(updimension_val=i[dim[0]],
                                                                                       updimension=dim[0]))
                    except:
                        pass
                    up_dim=stat_dict.copy()#Передаем словарь с информацией о вернем уровне "Все файлы"
                    stat_dict['sub'] = RecStats(0, i, updimensions, table,up_dim)
                stats.append(stat_dict)
                counter+=1
        #Для групп сегментов
        for num in range(num_seg):
            array_dates = []
            updimensions = []
            seg = json.loads(requests.get(
                'https://s.analitika.online/api/reference/segments/{num_seg}/'.format(num_seg=int(dim[0][num][7:])),
                headers=headers).content.decode('utf-8'))['real_definition']
            seg_filt = seg.partition("==")[0] + "=='" + seg.partition("==")[2] + "'"
            seg_label = json.loads(requests.get(
                'https://s.analitika.online/api/reference/segments/{num_seg}/'.format(num_seg=int(dim[0][num][7:])),
                headers=headers).content.decode('utf-8'))['name']
            counter=0
            for date in period:
                q = '''SELECT '{label_val}' as label,'{segment_val}' as segment,{metric_counts} FROM {table}
                                                                      WHERE 1 {filt} AND {date_field} BETWEEN '{date1}' AND '{date2}' AND {seg_filt}
                                                                      FORMAT JSON
                                                                    '''.format(
                                                                                   label_val=seg_label,
                                                                                   segment_val=seg,
                                                                                   seg_filt=seg_filt,
                                                                                   metric_counts=metric_counts,
                                                                                   date1=date['date1'],
                                                                                   date2=date['date2'], filt=filt,
                                                                                   table=table, date_field=date_field)

                array_dates.append(json.loads(get_clickhouse_data(q, 'http://85.143.172.199:8123'))['data'])
                if array_dates[counter] == []:
                    empty_dict = {'label': seg_label,
                                  'segment': seg}
                    for metric in metrics:
                        empty_dict[metric] = 0
                    array_dates[counter].append(empty_dict)
                counter+=1
            counter = 0
            for i in array_dates[0]:
                stat_dict = {'label': i['label'],
                                'segment': i['segment'],}
                dates = []
                for m in range(len(array_dates)):
                    metrics_dict = dict.fromkeys(metrics)
                    for j in metrics_dict:
                        try:
                            metrics_dict[j] = array_dates[m][counter][j]
                        except:
                            metrics_dict[j]=0
                    dates.append({'date1': period[m]['date1'], 'date2': period[m]['date2'], 'metrics': metrics_dict})
                stat_dict['dates'] = dates
                # если размер списка  dimensions больше размера группы, заполняем подуровень
                if len(dim) > 1:
                        # Добавляем подуровень
                    updimensions.append(seg_filt)
                    updimensions.append(seg_filt)
                    stat_dict['sub'] = RecStats(0, i, updimensions, table,1)
                stats.append(stat_dict)
                counter+=1
        return stats
    def OldFilterParse(filt_dict):
        """Метод для перевода словаря global_filter в строку для sql запроса"""
        filt=[]
        having=[]
        for i in filt_dict:
            sub_filt=[]
            for j in i['filters']:
                Not=""
                if j['not']=='true' or j['not']=='True':
                    Not="NOT"
                if j['filter_operator'] == 'match':
                    filt_str = " {Not} {filter_operator}({parameter},'{expressions}') ".format(parameter=j['parameter']
                                                                                           , Not=Not, filter_operator=j['filter_operator'],
                                                                                          expressions=j['expressions'][0])
                else:
                    if j['filter_operator']=="IN":
                        expressions='('+','.join(j['expressions'])+')'
                    else:
                        expressions=j['expressions'][0]
                        if type(expressions)==str:
                            expressions="'"+expressions+"'"

                    filt_str=' {Not}{parameter} {filter_operator} {expressions} '.format(parameter=j['parameter']
                                                                   ,Not=Not,filter_operator=j['filter_operator'],expressions=expressions)
                sub_filt.append(filt_str)
            sub_filt=' ('+i['global_filter_operator'].join(sub_filt)+') '
            filt.append(sub_filt)

        #filt=filt_dict['operator'].join(filt)
        filt='AND'.join(filt)

        return filt
    def FilterParse(filt_string):
        """Метод для перевода  global_filter в строку для sql запроса"""
        #filt_string=filt_string.replace(',',' OR ')
        #filt_string = filt_string.replace(';', ' AND ')
        #print(filt_string.partition('=@'))
        simple_operators=['==','!=','>=','<=','>','<']
        like_operators=['=@','!@','=^','=$','!^','!&']
        like_str=[" LIKE '%{val}%'"," NOT LIKE '%{val}%'"," LIKE '{val}%'"," LIKE '%{val}'"," NOT LIKE '{val}%'"," NOT LIKE '%{val}'"]
        match_operators=['=~','!~']
        match_str=[" match({par}?'{val}')"," NOT match({par}?'{val}')"]
        separator_indices=[]
        for i in range(len(filt_string)):
            if filt_string[i]==',' or filt_string[i]==';':
                separator_indices.append(i)
        separator_indices.append(len(filt_string))
        end_filt=""
        for i in range(len(separator_indices)):
            if i==0:
                sub_str = filt_string[0:separator_indices[i]]
            else:
                sub_str=filt_string[separator_indices[i-1]+1:separator_indices[i]]
            for j in simple_operators:
                if sub_str.partition(j)[2]=='':
                    pass
                else:
                    sub_str=sub_str.partition(j)[0]+j+"'"+sub_str.partition(j)[2]+"'"
                    break
            for j in range(len(like_operators)):
                if sub_str.partition(like_operators[j])[2]=='':
                    pass
                else:
                    sub_str = sub_str.partition(like_operators[j])[0] +like_str[j].format(val=sub_str.partition(like_operators[j])[2])
                    break
            for j in range(len(match_operators)):
                if sub_str.partition(match_operators[j])[2]=='':
                    pass
                else:
                    sub_str = match_str[j].format(val=sub_str.partition(match_operators[j])[2],par=sub_str.partition(match_operators[j])[0])
                    break
            try:
                end_filt=end_filt+sub_str+filt_string[separator_indices[i]]
            except:
                end_filt = end_filt + sub_str

        end_filt=end_filt.replace(',',' OR ')
        end_filt=end_filt.replace(';',' AND ')
        end_filt = end_filt.replace('?', ',')
        return end_filt
    if request.method=='POST':
        #Заголовки для запроса сегментов
        headers = {
            'Authorization': 'JWT eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJ1c2VyX2lkIjoxMSwiZW1haWwiOiIiLCJleHAiOjE0NzU3NjQwODAsInVzZXJuYW1lIjoiYXBpIn0.2Pj7lqRxuB6aBd4qgeMCaE_O5qIkm4QDmepcTwioqgA',
            'Content-Type': 'application/json'}
        #сегменты
        segments=[{'label':'Платный трафик', 'segment':"referrerType == campaign",'segment_filt':"referrerType == 'campaign'"},
            {'label':'Только Россия', 'segment':"countryCode == ru",'segment_filt':"countryCode == 'ru'"},
            {'label':'Только Chrome','segment':"browserName == Chrome",'segment_filt':"browserName == 'Chrome'"}]
        # Парсинг json
        try:
            sort_order=json.loads(request.body.decode('utf-8'))['sort_order']
        except:
            sort_order=""
        #сортировка по переданному показателю
        try:
            sort_column = json.loads(request.body.decode('utf-8'))['sort_column']
        except:
            sort_column=""

        dimensionslist_with_segments=json.loads(request.body.decode('utf-8'))['dimensions']
        dimensionslist = []
        #Создание списка параметров без сегментов
        for d in dimensionslist_with_segments:
            if 'segment' not in d and type(d)!=list:
                dimensionslist.append(d)
        metrics = json.loads(request.body.decode('utf-8'))['metrics']
        #если в запросе не указан сдвиг, зададим его равным нулю
        try:
            offset = json.loads(request.body.decode('utf-8'))['offset']
        except:
            offset='0'
        #если в запросе не указан лимит, зададим его путой строкой, если указан, составим строку LIMIT...
        try:
            limit = 'LIMIT '+str(offset)+','+str(json.loads(request.body.decode('utf-8'))['limit'])
        except:
            limit=''

        period = json.loads(request.body.decode('utf-8'))['periods']
        try:
            filter = json.loads(request.body.decode('utf-8'))['filter']

        except:
            filt=" "
        else:
            filt="AND"+"("+FilterParse(filter)+")"
        try:
            filter_metric = json.loads(request.body.decode('utf-8'))['filter_metric']
        except:
            having=" "
        else:
            having = 'HAVING'+' '+FilterParse(filter_metric)
        #если список dimensionslist пуст, значит были переданы только сегменты
        try:
            #Проверка на пренадленость параметров к таблице.
            if dimensionslist[0] in ['adclient','campaign','stat_date','banner',
                                     'keyword','shows','clicks','spend','utm_source',
                                     'utm_medium','utm_campaign','utm_term','utm_content']:
                table = 'CHdatabase.adstat'
                date_field='stat_date'
            else:
                table = 'CHdatabase.visits ALL INNER JOIN CHdatabase.hits USING idVisit'
                date_field = 'serverDate'
        except:
            table = 'CHdatabase.hits ALL INNER JOIN CHdatabase.visits USING idVisit'
            date_field = 'serverDate'

        #Формируем массив с count() для каждого параметра
        dimension_counts=[]
        for i in dimensionslist:
            dimension_counts.append("CAST(uniq({dimension}),'Int') as h{dimension}".format(dimension=i))
        dimension_counts=','.join(dimension_counts)

        # ФОрмируем массив с запросом каждого показателя в SQL
        metric_counts=[]
        for i in metrics:
            if 'goal' in i:
                if '_conversion' in i:
                    metric_counts.append(" floor((sum(Type='goal' and goalId={N})/uniq(idVisit))*100,2) as goal{N}_conversion".format(N=i.partition("_conversion")[0][4:]))
                else:
                    metric_counts.append("CAST(sum(Type='goal' AND goalId={N}),'Int') as goal{N}".format(N=i[4:]))
            if i=="nb_visits":
                metric_counts.append("CAST(uniq(idVisit),'Int') as nb_visits")
            if i in ['clicks','spend','shows']:
                metric_counts.append("CAST(sum({metric}),'Int') as {metric}".format(metric=i))
            if i=="nb_actions":
                metric_counts.append("CAST(sum(actions),'Int') as nb_actions")
            if i=="nb_visitors":
                metric_counts.append("CAST(uniq(visitorId),'Int') as nb_visitors")
        metric_counts=','.join(metric_counts)
        print(metric_counts)

        #Добавляем в выходной словарь параметр counts
        resp={}#Выходной словарь
        resp['counts'] = {}
        resp['counts']=AddCounts(period,dimension_counts,filt,sort_order,table)

        # Добавляем в выходной словарь параметр metric_sums
        resp['metric_sums']={}
        resp['metric_sums']['dates'] = AddMetricSums(period,metric_counts,filt,metrics,sort_order,table)

        # Добавляем stats
        resp['stats']=AddStats2(dimensionslist_with_segments,metric_counts,filt,limit,having,period,metrics,table)
        pprint.pprint(resp)
        response=JsonResponse(resp,safe=False,)
        response['Access-Control-Allow-Origin']='*'
        return response
    else:
        args={}
        args.update(csrf(request))
        args['form']=RequestForm
        return render_to_response('mainAPI.html',args)


