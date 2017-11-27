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
        updimensions.append("{updimension}='{updimension_val}'".format(updimension_val=i[dimensionslist[n]],updimension=dimensionslist[n]))
        updm=' AND '.join(updimensions)
        sub=[]
        array_dates = []
        for date in period:
            q = '''SELECT {dimension},{metric_counts} FROM {table}
                                                    WHERE 1 {filt} AND {updimensions} AND {date_field} BETWEEN '{date1}' AND '{date2}'
                                                    GROUP BY {dimension} {having}
                                                    FORMAT JSON
                                                    '''.format(dimension=dimensionslist[n+1],
                                                               updimensions=updm,
                                                               metric_counts=metric_counts,
                                                               date1=date['date1'],
                                                               date2=date['date2'],filt=filt,having=having,table=table,date_field=date_field)
            array_dates.append(json.loads(get_clickhouse_data(q, 'http://85.143.172.199:8123'))['data'])

        for i2 in array_dates[0]:
            stat_dict = {'label': i2[dimensionslist[n+1]]}
            dates = []
            metrics_dict = dict.fromkeys(metrics)
            for m in range(len(array_dates)):
                for (j, k) in zip(metrics_dict, i2):
                    metrics_dict[j] = i2[j]
                dates.append({'date1': period[m]['date1'], 'date2': period[m]['date2'], 'metrics': metrics_dict})
            stat_dict['dates'] = dates
            if n != len(dimensionslist) - 2:
                stat_dict['sub']=RecStats(n+1,i2,updimensions,table)
            sub.append(stat_dict)
        return sub
    def get_clickhouse_data(query,host,connection_timeout=1500):
        """Метод для обращения к базе данных CH"""
        r=requests.post(host,params={'query':query},timeout=connection_timeout)
        return r.text
    def FirstVer():
        """Метод с первым вариантом работы программы"""
        if flat == "" or flat == "true":
            q = '''SELECT {dimensions}{nb_visits[0]}{nb_actions[0]}{goal1[0]} FROM
            (SELECT {dimensions}{nb_visits[1]}{nb_actions[1]}{goal1[1]} FROM (SELECT idVisit,{dimensions}{nb_visits[2]}{nb_actions[2]} FROM CHdatabase.visits WHERE 1 {where}
            GROUP BY idVisit,{dimensions}  )
            ALL INNER JOIN
            (SELECT idVisit{goal1[2]}  FROM  CHdatabase.hits  GROUP BY idVisit ) USING idVisit )
            GROUP BY {dimensions}
            FORMAT JSON
            '''.format(dimensions=dimensions, where=where, sort_order=sort_order, filt=global_filter,
                       nb_visits=nb_visits, nb_actions=nb_actions, goal1=goal1)
            if limit == "" and offset != "":
                resp = json.loads(get_clickhouse_data(q, 'http://85.143.172.199:8123'))['data'][int(offset):]
            if limit != "" and offset == "":
                resp = json.loads(get_clickhouse_data(q, 'http://85.143.172.199:8123'))['data'][:int(limit)]
            if limit == "" and offset == "":
                resp = json.loads(get_clickhouse_data(q, 'http://85.143.172.199:8123'))['data']
            if limit != "" and offset != "":
                resp = json.loads(get_clickhouse_data(q, 'http://85.143.172.199:8123'))['data'][
                       int(offset):int(offset) + int(limit)]
            print(resp)

        else:
            # формирование древовидного отчета
            levels = len(dimensionslist)
            sub = []
            labels = []
            q2 = '''
            SELECT {dimension},sum(nb_visits) as nb_visits,sum(nb_actions) as nb_actions,sum(goal1) as goal1 FROM
            (SELECT {dimension},nb_visits,nb_actions,goal1 FROM (SELECT idVisit, {dimension},count(*) as nb_visits,sum(actions) as nb_actions FROM CHdatabase.visits WHERE 1 {where}
            {filt}  GROUP BY idVisit,{dimension}  )
            ALL INNER JOIN
            (SELECT idVisit,sum(Type='goal' AND goalId=1) as goal1  FROM  CHdatabase.hits  GROUP BY idVisit ) USING idVisit )
            GROUP BY {dimension}
            '''.format(dimension=dimensionslist[0], where=where, filt=global_filter)
            tree = []
            l = ((get_clickhouse_data(q2, 'http://85.143.172.199:8123')).split('\n'))
            l.remove('')
            labels = []
            visits = []
            actions = []
            goals = []

            for i in l:
                i = i.split('\t')
                labels.append(i[0])
                visits.append(i[1])
                actions.append(i[2])
                goals.append(i[3])
            for (i, v, a, g) in zip(labels, visits, actions, goals):
                sub1 = []
                tree.append({'label': i, 'segment': dimensionslist[0] + "==",
                             'metrics': {'nb_actions': a, 'nb_visits': v, 'goal1': g}, 'sub1': sub1})
                if levels > 1:
                    labels2, visits2, actions2, goals2 = SubQuery(0, i, 1)
                    for (j, v2, a2, g2) in zip(labels2, visits2, actions2, goals2):
                        sub2 = []
                        sub1.append({'label': j, 'segment': dimensionslist[0] + "==  " + dimensionslist[1] + "==",
                                     'metrics': {'nb_actions': a2, 'nb_visits': v2, 'goal1': g2}, 'sub2': sub2})
                        if levels > 2:
                            labels3, visits3, actions3, goals3 = SubQuery(1, j, 2)
                            for (k, v3, a3, g3) in zip(labels3, visits3, actions3, goals3):
                                sub3 = []
                                sub2.append({'label': k,
                                             'segment': dimensionslist[0] + "==  " + dimensionslist[1] + "== " +
                                                        dimensionslist[2] + "==",
                                             'metrics': {'nb_actions': a3, 'nb_visits': v3, 'goal1': g3}, 'sub3': sub3})
                                if levels > 3:
                                    labels4, visits4, actions4, goals4 = SubQuery(2, k, 3)
                                    for (m, v4, a4, g4) in zip(labels4, visits4, actions4, goals4):
                                        sub4 = []
                                        sub3.append({'label': m,
                                                     'segment': dimensionslist[0] + "==  " + dimensionslist[1] + "== " +
                                                                dimensionslist[2] + "== " + dimensionslist[3] + "==",
                                                     'metrics': {'nb_actions': a4, 'nb_visits': v4, 'goal1': g4},
                                                     'sub4': sub4})
                                        if levels > 4:
                                            labels5, visits5, actions5, goals5 = SubQuery(3, m, 4)
                                            for (n, v5, a5, g5) in zip(labels5, visits5, actions5, goals5):
                                                sub4.append({'label': n,
                                                             'segment': dimensionslist[0] + "==  " + dimensionslist[
                                                                 1] + "== " + dimensionslist[2] + "== " +
                                                                        dimensionslist[3] + "== " + dimensionslist[
                                                                            4] + "==",
                                                             'metrics': {'nb_actions': a5, 'nb_visits': v5,
                                                                         'goal1': g5}})

            # pprint.pprint(json.dumps(tree))
            resp = tree
    def SubQuery(d1,i,d2):
        """Вложенные структуры для старого способа"""
        try:
            i=int(i)
        except:
            i="'"+i+"'"
        q3='''
        SELECT {dimension2},sum(nb_visits) as nb_visits,sum(nb_actions) as nb_actions,sum(goal1) as goal1 FROM (SELECT {dimension2},nb_visits,nb_actions,goal1 FROM (SELECT idVisit,{dimension2},count(*) as nb_visits,sum(actions) as nb_actions FROM CHdatabase.visits WHERE {dimension}={value} {where}
            {filt}  GROUP BY idVisit,{dimension2}  )
            ALL INNER JOIN
            (SELECT idVisit,sum(Type='goal' AND goalId=1) as goal1  FROM  CHdatabase.hits  GROUP BY idVisit ) USING idVisit )
            GROUP BY {dimension2}
        '''.format(dimension=dimensionslist[d1],value=i,dimension2=dimensionslist[d2],where=where,filt=global_filter)
        l=((get_clickhouse_data(q3,'http://85.143.172.199:8123')).split('\n'))
        l.remove('')
        labels=[]
        visits=[]
        actions=[]
        goals=[]

        for i in l:
            i=i.split('\t')
            labels.append(i[0])
            visits.append(i[1])
            actions.append(i[2])
            goals.append(i[3])
        return labels,visits,actions,goals
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
    def AddStats(dimensionslist,metric_counts,filt,limit,having,period,metrics,table):
        """Добавление ключа stats в ответ"""
        stats = []
        array_dates = []
        for date in period:
            q = '''SELECT {dimensions},{metric_counts} FROM {table}
                               WHERE 1 {filt} AND {date_field} BETWEEN '{date1}' AND '{date2}'
                               GROUP BY {dimensions}  {having}
                               {limit}
                               FORMAT JSON
                               '''.format(dimensions=dimensionslist[0], metric_counts=metric_counts,
                                          date1=date['date1'],
                                          date2=date['date2'], filt=filt, limit=limit,
                                          having=having,table=table,date_field=date_field)
            array_dates.append(json.loads(get_clickhouse_data(q, 'http://85.143.172.199:8123'))['data'])

        for i in array_dates[0]:
            stat_dict = {'label': i[dimensionslist[0]]}
            dates = []
            metrics_dict = dict.fromkeys(metrics)
            for m in range(len(array_dates)):
                for (j, k) in zip(metrics_dict, i):
                    metrics_dict[j] = i[j]
                dates.append({'date1': period[m]['date1'], 'date2': period[m]['date2'], 'metrics': metrics_dict})
            stat_dict['dates'] = dates

            if len(dimensionslist) > 1:
                # Добавляем подуровень
                updimensions = []
                updimensions.append("{updimension}='{updimension_val}'".format(updimension_val=i[dimensionslist[0]],
                                                                               updimension=dimensionslist[0]))
                stat_dict['sub'] = RecStats(0, i, updimensions,table)
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
                if j['not']=='True':
                    Not="NOT"
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
        # Парсинг json
        try:
            flat = json.loads(request.body.decode('utf-8'))['flat']
        except:
            flat=''
        sort_order=json.loads(request.body.decode('utf-8'))['sort_order']
        dimensionslist=json.loads(request.body.decode('utf-8'))['dimensions']
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

        period = json.loads(request.body.decode('utf-8'))['period']
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

        dimensions = ','.join(dimensionslist)

        #Проверка на пренадленость параметров к таблице.

        if dimensionslist[0] in ['adclient','campaign','stat_date','banner','keyword','shows','clicks','spend','utm_source','utm_medium','utm_campaign','utm_term','utm_content']:

            table = 'CHdatabase.adstat'
            date_field='stat_date'
        else:
            table = 'CHdatabase.visits'
            date_field = 'serverDate'

        #Формируем массив с count() для каждого параметра
        dimension_counts=[]
        for i in dimensionslist:
            dimension_counts.append('count(DISTINCT {dimension})'.format(dimension=i))

        dimension_counts=','.join(dimension_counts)
        # ФОрмируем массив с sum() для каждого показателя
        metric_counts=[]
        for i in metrics:
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
        resp['stats']=AddStats(dimensionslist,metric_counts,filt,limit,having,period,metrics,table)
        pprint.pprint(resp)
        return JsonResponse(resp,safe=False)
    else:
        args={}
        args.update(csrf(request))
        args['form']=RequestForm
        return render_to_response('mainAPI.html',args)


