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
    def RecStats(n,i,updimensions,table):
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
        num_seg = 0
        for i in dimensionslist_with_segments[n+1:]:
            if 'segment' in i:
                num_seg += 1
            else:
                break
        for num in range(n+1,n+1+num_seg):
            array_dates = []
            seg_filt = segments[int(dimensionslist_with_segments[num][7:]) - 1]['segment_filt']
            updimensions.append(seg_filt)
            updimensions.append(seg_filt)
            updm = ' AND '.join(updimensions)
            for date in period:
                q = '''SELECT '{label_val}' as label,'{segment_val}' as segment,{metric_counts} FROM {table}
                                                                      WHERE 1 {filt} AND {date_field} BETWEEN '{date1}' AND '{date2}' AND {updimensions}
                                                                      FORMAT JSON
                                                                    '''.format(
                    label_val=segments[int(dimensionslist_with_segments[num][7:]) - 1]['label'],
                    segment_val=segments[int(dimensionslist_with_segments[num][7:]) - 1]['segment'],
                    updimensions=updm,
                    metric_counts=metric_counts,
                    date1=date['date1'],
                    date2=date['date2'], filt=filt,
                    table=table, date_field=date_field)

                array_dates.append(json.loads(get_clickhouse_data(q, 'http://85.143.172.199:8123'))['data'])

            for i in array_dates[0]:
                stat_dict = {'label': i['label'],
                             'segment': i['segment']}
                dates = []
                metrics_dict = dict.fromkeys(metrics)
                for m in range(len(array_dates)):
                    for (j, k) in zip(metrics_dict, i):
                        metrics_dict[j] = i[j]
                    dates.append({'date1': period[m]['date1'], 'date2': period[m]['date2'], 'metrics': metrics_dict})
                stat_dict['dates'] = dates
                if len(dimensionslist_with_segments) > n+1+num_seg:
                    # Добавляем подуровень
                    stat_dict['sub'] = RecStats(n+num_seg, i, updimensions, table)
                sub.append(stat_dict)
        if num_seg==0:
            array_dates = []
            updm=' AND '.join(updimensions)
            dimension = dimensionslist_with_segments[n + 1]
            for date in period:
                q = '''SELECT {dimension},{metric_counts} FROM {table}
                                                                    WHERE 1 {filt} AND {updimensions} AND {date_field} BETWEEN '{date1}' AND '{date2}'
                                                                    GROUP BY {dimension} {having}
                                                                    FORMAT JSON
                                                                    '''.format(dimension=dimension,
                                                                               updimensions=updm,
                                                                               metric_counts=metric_counts,
                                                                               date1=date['date1'],
                                                                               date2=date['date2'], filt=filt,
                                                                               having=having, table=table,
                                                                               date_field=date_field)

                array_dates.append(json.loads(get_clickhouse_data(q, 'http://85.143.172.199:8123'))['data'])
            for i2 in array_dates[0]:
                stat_dict = {'label': i2[dimensionslist_with_segments[n + 1]]}
                dates = []
                metrics_dict = dict.fromkeys(metrics)
                for m in range(len(array_dates)):
                    for (j, k) in zip(metrics_dict, i2):
                        metrics_dict[j] = i2[j]
                    dates.append({'date1': period[m]['date1'], 'date2': period[m]['date2'], 'metrics': metrics_dict})
                stat_dict['dates'] = dates
                if n != len(dimensionslist_with_segments) - 2:
                    stat_dict['sub'] = RecStats(n + 1, i2, updimensions, table)
                sub.append(stat_dict)
        return sub
    def get_clickhouse_data(query,host,connection_timeout=1500):
        """Метод для обращения к базе данных CH"""
        r=requests.post(host,params={'query':query},timeout=connection_timeout)
        return r.text
    def AddCounts(period,dimension_counts,filt,sort_order,table):
        """Добавление ключа Counts в ответ"""
        dates = []
        for date in period:
            q = ''' SELECT {dimension_counts}
                     FROM {table}
                     WHERE 1 {filt} AND {date_field} BETWEEN '{date1}' AND '{date2}'
                     ORDER BY NULL {sort_order}
                     FORMAT JSON
                    '''.format(date1=date['date1'], date2=date['date2'], dimension_counts=dimension_counts, filt=filt,
                               sort_order=sort_order,table=table,date_field=date_field)

            # Объеденяем словарь с датами со словарем  вернувшихся значений каждого из запрошенных параметров
            try:
                a = json.loads(get_clickhouse_data(q, 'http://85.143.172.199:8123'))['data'][0]
            #заменяем словарь с uniqExact(dimension) на dimension
                b=dict.fromkeys(dimensionslist)
                for i in dimensionslist:
                    b[i]=a['uniqExact({name})'.format(name=i)]
            except:
                b=dict.fromkeys(dimensionslist,0)
                print(b)

            b.update(date)
            dates.append(b)
        return dates
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
        #Определяем, есть ли вначале dimensions группа сегментов, и если есть, то сколько мегментов стоит подряд
        num_seg=0
        for i in  dim:
            if 'segment' in i:
                num_seg+=1
            else:
                break
        #Для групп сегментов
        for num in range(num_seg):
            array_dates = []
            updimensions = []
            seg_filt = segments[int(dim[num][7:]) - 1]['segment_filt']
            for date in period:
                q = '''SELECT '{label_val}' as label,'{segment_val}' as segment,{metric_counts} FROM {table}
                                                                      WHERE 1 {filt} AND {date_field} BETWEEN '{date1}' AND '{date2}' AND {seg_filt}
                                                                      FORMAT JSON
                                                                    '''.format(
                                                                                   label_val=segments[int(dim[num][7:]) - 1]['label'],
                                                                                   segment_val=segments[int(dim[num][7:]) - 1]['segment'],
                                                                                   seg_filt=seg_filt,
                                                                                   metric_counts=metric_counts,
                                                                                   date1=date['date1'],
                                                                                   date2=date['date2'], filt=filt,
                                                                                   table=table, date_field=date_field)

                array_dates.append(json.loads(get_clickhouse_data(q, 'http://85.143.172.199:8123'))['data'])
            for i in array_dates[0]:
                stat_dict = {'label': i['label'],
                                'segment': i['segment']}
                dates = []
                metrics_dict = dict.fromkeys(metrics)
                for m in range(len(array_dates)):
                    for (j, k) in zip(metrics_dict, i):
                        metrics_dict[j] = i[j]
                    dates.append({'date1': period[m]['date1'], 'date2': period[m]['date2'], 'metrics': metrics_dict})
                stat_dict['dates'] = dates
                # если размер списка  dimensions больше размера группы, заполняем подуровень
                if len(dim) > num_seg:
                        # Добавляем подуровень
                    updimensions.append(seg_filt)
                    updimensions.append(seg_filt)
                    stat_dict['sub'] = RecStats(num_seg-1, i, updimensions, table)
                stats.append(stat_dict)

        #если сегментов вначале списка нет
        if num_seg==0:
            updimensions = []
            array_dates = []
            for date in period:
                q = '''SELECT {dimensions},{metric_counts} FROM {table}
                                   WHERE 1 {filt} AND {date_field} BETWEEN '{date1}' AND '{date2}'
                                   GROUP BY {dimensions}  {having}
                                   {limit}
                                   FORMAT JSON
                                   '''.format(dimensions=dim[0], metric_counts=metric_counts,
                                              date1=date['date1'],
                                              date2=date['date2'], filt=filt, limit=limit,
                                              having=having, table=table, date_field=date_field)
                array_dates.append(json.loads(get_clickhouse_data(q, 'http://85.143.172.199:8123'))['data'])
            for i in array_dates[0]:

                stat_dict = {'label': i[dim[0]]}
                #print(stat_dict)
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
                    stat_dict['sub'] = RecStats(0, i, updimensions, table)
                stats.append(stat_dict)
        return stats
    def FilterParse(filt_dict):
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

    if request.method=='POST':
        #сегменты
        segments=[{'label':'Платный трафик', 'segment':"referrerType == campaign",'segment_filt':"referrerType == 'campaign'"},
            {'label':'Только Россия', 'segment':"countryCode == ru",'segment_filt':"countryCode == 'ru'"},
            {'label':'Только Chrome','segment':"browserName == Chrome",'segment_filt':"browserName == 'Chrome'"}]

        # Парсинг json
        sort_order=json.loads(request.body.decode('utf-8'))['sort_order']
        dimensionslist_with_segments=json.loads(request.body.decode('utf-8'))['dimensions']
        dimensionslist = dimensionslist_with_segments.copy()
        for d in dimensionslist:
            if 'segment' in d:
                dimensionslist.remove(d)

        metrics = json.loads(request.body.decode('utf-8'))['metrics']
        #если в запросе не указан сдвиг, зададим его равным нулю
        try:
            offset = json.loads(request.body.decode('utf-8'))['offset']
        except:
            offset='0'
        #если в запросе не указан лимит, зададим его путой строкой, если указан, составим строку LIMIT...
        try:
            limit = 'LIMIT '+offset+','+json.loads(request.body.decode('utf-8'))['limit']
        except:
            limit=''

        period = json.loads(request.body.decode('utf-8'))['periods']
        try:
            global_filter = json.loads(request.body.decode('utf-8'))['global_filter']

        except:
            filt=" "
        else:
            filt="AND"+"("+FilterParse(global_filter)+")"

        try:
            global_filter_metric = json.loads(request.body.decode('utf-8'))['global_filter_metric']
        except:
            having=" "
        else:
            having = 'HAVING'+' '+FilterParse(global_filter_metric)


        #Проверка на пренадленость параметров к таблице.
        if dimensionslist[0] in ['adclient','campaign','stat_date','banner',
                                 'keyword','shows','clicks','spend','utm_source',
                                 'utm_medium','utm_campaign','utm_term','utm_content']:
            table = 'CHdatabase.adstat'
            date_field='stat_date'
        else:
            table = 'CHdatabase.hits ALL INNER JOIN CHdatabase.visits USING idVisit'
            date_field = 'serverDate'

        #Формируем массив с count() для каждого параметра
        dimension_counts=[]
        for i in dimensionslist:
            dimension_counts.append('count(DISTINCT {dimension})'.format(dimension=i))

        dimension_counts=','.join(dimension_counts)

        # ФОрмируем массив с sum() для каждого показателя
        metric_counts=[]
        for i in metrics:
            if 'goal' in i:
                metric_counts.append("sum(Type='goal' AND goalId={N}) as goal{N}".format(N=i[4:]))
            if i=="nb_visits":
                metric_counts.append('count(*) as nb_visits')
            if i in ['clicks','spend','shows']:
                metric_counts.append('sum({metric}) as {metric}'.format(metric=i))
            if i=="nb_actions":
                metric_counts.append('sum(actions) as nb_actions')
        metric_counts=','.join(metric_counts)
        #Добавляем в выходной словарь параметр counts
        resp={}#Выходной словарь
        resp['counts'] = {}
        resp['counts']['dates']=AddCounts(period,dimension_counts,filt,sort_order,table)

        # Добавляем в выходной словарь параметр metric_sums
        resp['metric_sums']={}
        resp['metric_sums']['dates'] = AddMetricSums(period,metric_counts,filt,metrics,sort_order,table)
        # Добавляем stats
        resp['stats']=AddStats2(dimensionslist_with_segments,metric_counts,filt,limit,having,period,metrics,table)
        pprint.pprint(resp)
        return JsonResponse(resp,safe=False)
    else:
        args={}
        args.update(csrf(request))
        args['form']=RequestForm
        return render_to_response('mainAPI.html',args)


