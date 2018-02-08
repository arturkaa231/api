from django.shortcuts import render
from django.shortcuts import render_to_response,redirect
from django.template.context_processors import csrf
import json
import requests
from django.http import JsonResponse
from django.views.decorators.csrf import csrf_exempt
import pprint
import re
from datetime import datetime, timedelta
import pytz
def MetricCounts(metrics, headers):
    metric_counts = []
    for i in metrics:
        if 'conversion_rate'==i:
            metric_counts.append(
                "if(uniq(idVisit)=0,0,floor(sum(Type='goal')*100/uniq(idVisit),2)) as {metric}".format(
                    metric=i))
            continue
        if 'nb_new_visitors_per_all_visitors'==i:
            metric_counts.append(
                "if(uniq(visitorId)=0,0,floor(uniqIf(visitorId,visitorType='new')*100/uniq(visitorId),2)) as {metric}".format(
                    metric=i))
            continue
        if 'nb_return_visitors_per_all_visitors'==i:
            metric_counts.append(
                "if(uniq(visitorId)=0,0,floor(uniqIf(visitorId,visitorType='returning')*100/uniq(visitorId),2)) as {metric}".format(
                    metric=i))
            continue
        if 'nb_visits_with_searches'==i:
            metric_counts.append(
                "CAST(countIf(searches>0),'Int') as {metric}".format(
                    metric=i))
            continue
        if 'nb_searches_visits_per_all_visits'==i:
            metric_counts.append(
                "if(uniq(idVisit)=0,0,floor(countIf(searches>0)*100/uniq(idVisit),2)) as {metric}".format(
                    metric=i))
            continue
        if 'nb_searches'==i:
            metric_counts.append(
                "CAST(sum(searches),'Int') as {metric}".format(
                    metric=i))
            continue
        if 'nb_downloas_per_visit'==i:
            metric_counts.append("if(uniq(idVisit)=0,0,floor(sum(Type='download')/uniq(idVisit),2)) as {metric}".format(
                metric=i))
        if 'avg_visit_length'==i:
            metric_counts.append(
                "if(uniq(idVisit)=0,0,floor(sum(visitDuration)/uniq(idVisit),2)) as {metric}".format(
                    metric=i))
            continue
        if 'nb_return_visitors'==i:
            metric_counts.append(
                "CAST(uniq(visitorId)-uniqIf(visitorId,visitorType='new'),'Int') as {metric}".format(
                    metric=i))
            continue
        if 'nb_new_visits_per_all_visits'==i:
            metric_counts.append(
                "if(uniq(idVisit)=0,0,floor(uniqIf(idVisit,visitorType='new')*100/uniq(idVisit),2)) as {metric}".format(
                    metric=i))
            continue

        if 'nb_new_visits'==i:
            metric_counts.append(
                "CAST(uniqIf(idVisit,visitorType='new'),'Int') as {metric}".format(
                    metric=i))
            continue
        if 'nb_new_visitors'==i:
            metric_counts.append(
                "CAST(uniqIf(visitorId,visitorType='new'),'Int') as {metric}".format(
                    metric=i))
            continue
        if 'nb_actions_per_visit'==i:
            metric_counts.append(
                "if(uniq(idVisit)=0,0,floor(count(*)/uniq(idVisit),2)) as {metric}".format(metric=i))
            continue
        if 'nb_pageviews_per_visit'==i:
            metric_counts.append("floor(sum(Type='action')/uniq(idVisit),2) as {metric}".format(metric=i))
            continue
        if 'ctr'==i:
            metric_counts.append(
                "if(sum(shows)=0,0,floor((sum(clicks)/sum(shows))*100,2)) as {metric}".format(metric=i))
            continue
        if 'avg_time_generation'==i:
            metric_counts.append("floor(avg(generationTimeMilliseconds)/1000,2) as {metric}".format(metric=i))
            continue
        if 'nb_downloads'==i:
            metric_counts.append("CAST(sum(Type='download'),'Int') as {metric}".format(metric=i))
            continue
        if 'nb_conversions'==i:
            metric_counts.append("CAST(sum(Type='goal'),'Int') as {metric}".format(metric=i))
            continue
        if 'nb_pageviews'==i:
            metric_counts.append("CAST(sum(Type='action'),'Int') as {metric}".format(metric=i))
            continue
        if 'bounce_rate'==i:
            metric_counts.append(
                "if(uniq(idVisit)=0,0,floor((uniqIf(idVisit,visitDuration=0)*100/uniq(idVisit)),2)) as {metric}".format(
                    metric=i))
            continue
        if 'bounce_count'==i:
            metric_counts.append("CAST((uniqIf(idVisit,visitDuration=0),'Int') as {metric}".format(metric=i))
            continue
        if 'calculated_metric' in i:
            calc_metr = json.loads(requests.get(
                'https://s.analitika.online/api/reference/calculated_metrics?code=calculated_metric{num}'.format(
                    num=int(i[17:])),
                headers=headers).content.decode('utf-8'))['results'][0]['definition']
            calc_metr = calc_metr.replace('impressions', 'sum(Impressions}').replace('nb_actions_per_visit',"if(uniq(idVisit)=0,0,floor(count(*)/uniq(idVisit),2))")\
                    .replace('nb_downloas_per_visit',"if(uniq(idVisit)=0,0,floor(sum(Type='download')/uniq(idVisit),2))").replace('cost', 'sum(Cost)')\
                    .replace('clicks', 'sum(Clicks)').replace('nb_visits_with_searches',"countIf(searches>0)").replace('nb_visits', 'uniq(idVisit)').replace('nb_actions','count(*)')\
                    .replace('nb_visitors', 'uniq(visitorId)').replace('bounce_count','sum(visitDuration=0)').replace('bounce_rate','if(uniq(idVisit)=0,0,floor((sum(visitDuration=0)/uniq(idVisit))*100,2))')\
                    .replace('nb_pageviews',"sum(Type='action')").replace('nb_conversions',"sum(Type='goal')").replace('nb_downloads',"sum(Type='download')")\
                    .replace('avg_time_generation',"floor(avg(generationTimeMilliseconds)/1000,2)").replace('ctr',"if(sum(shows)=0,0,floor((sum(clicks)/sum(shows))*100,2))")\
                    .replace('nb_pageviews_per_visit',"floor(sum(Type='action')/uniq(idVisit),2)").replace('nb_new_visitors_per_all_visitors',"if(uniq(visitorId)=0,0,floor(uniqIf(visitorId,visitorType='new')*100/uniq(visitorId),2))")\
                    .replace('nb_new_visitors',"uniqIf(visitorId,visitorType='new')").replace('nb_new_visits_per_all_visits',"if(uniq(idVisit)=0,0,floor(uniqIf(idVisit,visitorType='new')*100/uniq(idVisit),2))")\
                    .replace('nb_new_visits',"uniqIf(idVisit,visitorType='new')").replace('nb_return_visitors_per_all_visitors',"if(uniq(visitorId)=0,0,floor(uniqIf(visitorId,visitorType='returning')*100/uniq(visitorId),2))").replace('nb_return_visitors',"uniq(visitorId)-uniqIf(visitorId,visitorType='new')")\
                    .replace('avg_visit_length',"if(uniq(idVisit)=0,0,floor(sum(visitDuration)/uniq(idVisit),2))").replace('nb_searches_visits_per_all_visits',"if(uniq(idVisit)=0,0,floor(countIf(searches>0)*100/uniq(idVisit),2))")\
                    .replace('nb_searches',"sum(searches)").replace('conversion_rate',"if(uniq(idVisit)=0,0,floor(sum(Type='goal')*100/uniq(idVisit),2))")

            goal_conversions = re.findall(r'goal\d{1,3}_conversion', calc_metr)
            for goal_conversion in goal_conversions:
                calc_metr = calc_metr.replace(goal_conversion,
                                              "floor((sum(Type='goal' and goalId={N})/uniq(idVisit))*100,2)".format(
                                                  N=goal_conversion.partition("_conversion")[0][4:]))
            goals = re.findall(r'goal\d{1,3}_{0}', calc_metr)
            for goal in goals:
                calc_metr = calc_metr.replace(goal, "sum(Type='goal' AND goalId={N})".format(N=goal[4:]))
            metric_counts.append('floor(' + calc_metr + ',2)' + ' as calculated_metric{N}'.format(N=int(i[17:])))
        if 'goal' in i:
            if '_conversion' in i:
                metric_counts.append(
                    " if(uniq(idVisit)=0,0,floor((sum(Type='goal' and goalId={N})/uniq(idVisit))*100,2)) as goal{N}_conversion".format(
                        N=i.partition("_conversion")[0][4:]))
            else:
                metric_counts.append("CAST(sum(Type='goal' AND goalId={N}),'Int') as goal{N}".format(N=i[4:]))
        if i == "nb_visits":
            metric_counts.append("CAST(uniq(idVisit),'Int') as nb_visits")
        if i in ['clicks', 'cost', 'impressions']:
            metric_counts.append("CAST(sum({Metric}),'Int') as {metric}".format(metric=i,Metric=i.capitalize()))
        if i == "nb_actions":
            metric_counts.append("CAST(count(*),'Int') as nb_actions")
        if i == "nb_visitors":
            metric_counts.append("CAST(uniq(visitorId),'Int') as nb_visitors")
    return metric_counts
@csrf_exempt
def CHapi(request):
    def datesdicts(array_dates, dim,dim_with_alias,table,date_filt,updm):
        q_all = '''SELECT {dimension_with_alias} FROM {table}
                                           WHERE 1 {filt} AND {date_filt} AND {updm}
                                           GROUP BY {dimension}
                                           {limit}
                                           FORMAT JSON
                                           '''.format(dimension_with_alias=dim_with_alias,dimension=dim,updm=updm,
                                                      metric_counts=metric_counts,
                                                      filt=filt, limit=limit,
                                                      sort_order=sort_order,
                                                      table=table.format(dimension=dim), date_filt=date_filt)

        all_labeldicts = json.loads(get_clickhouse_data(q_all, 'http://85.143.172.199:8123'))['data']

        all_label = []
        sorted_array = array_dates[0]
        for i in all_labeldicts:
            all_label.append(i[dim])

        for label in all_label:
            k = 0
            for sub_array in array_dates[0]:
                if sub_array[dim] == label:
                    k = 1
            if k == 0:
                sorted_array.append({dim: label})

        array_dates_dicts = []
        for i in array_dates:
            sub_dict = {}
            for j in i:
                sub_dict[j[dim]] = j
            array_dates_dicts.append(sub_dict)

        return array_dates_dicts
    def MaxLenNum(array_dates):
        """Возвращает порядок элемента списка с наибольшей длиной"""
        max_len = 0
        for array_num in range(len(array_dates)):
            if len(array_dates[array_num]) > len(array_dates[max_len]):
                max_len = array_num
        return(max_len)
    def RecStats(n,i,updimensions,table,up_dim_info,using):
        """Рекурсивный метод для добавления вложенных структур в stats"""
        if dimensionslist_with_segments[n + 1] not in list_of_adstat_par and is_ad:
            return []
        #Добавляем фильтры
        try:
            updimensions.pop(n)
        except:
            pass
        #Добавляем параметры в список с параметрами верхних уровней
        #Если предыдущий уровень-сегмент, не добавляем фильтр
        try:
            if dimensionslist_with_segments[n] in time_dimensions_dict.keys():
                if type(i[dimensionslist_with_segments[n]]) is int:

                    updimensions.append(
                        "{updimension}={updimension_val}".format(updimension=time_dimensions_dict[dimensionslist_with_segments[n]],
                                                                 updimension_val=i[dimensionslist_with_segments[n]]))
                else:
                    updimensions.append(
                        "{updimension}='{updimension_val}'".format(updimension=time_dimensions_dict[dimensionslist_with_segments[n]],
                                                                 updimension_val=i[dimensionslist_with_segments[n]]))
            else:
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
                'https://s.analitika.online/api/reference/segments/{num_seg}/?all=1'.format(num_seg=int(dimensionslist_with_segments[n+1][num][7:])),
                headers=headers).content.decode('utf-8'))['real_definition']
            seg_filt = seg.partition("==")[0] + "=='" + seg.partition("==")[2] + "'"
            seg_label = json.loads(requests.get(
                'https://s.analitika.online/api/reference/segments/{num_seg}/?all=1'.format(num_seg=int(dimensionslist_with_segments[n+1][num][7:])),
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
                'https://s.analitika.online/api/reference/segments/{num_seg}/?all=1'.format(num_seg=int(dimensionslist_with_segments[n+1][7:])),
                headers=headers).content.decode('utf-8'))['real_definition']
            seg_filt = seg.partition("==")[0] + "=='" + seg.partition("==")[2] + "'"
            seg_label = json.loads(requests.get(
                'https://s.analitika.online/api/reference/segments/{num_seg}/?all=1'.format(num_seg=int(dimensionslist_with_segments[n+1][7:])),
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
            using=using+','+dimension#поля, по которым необходимо join-ить таблицы, если запрошены параметры статистики
            if attribution_model == 'first_interaction':
                for date in period:
                    date0 = (datetime.strptime(date['date1'], '%Y-%m-%d') - timedelta(days=int(attribution_lookup_period))).strftime('%Y-%m-%d')
                    q = '''SELECT {dimension},{sum_metric_string}
                               FROM (SELECT visitorId,any({dimension_without_aliases}) as {dimension} FROM {table}
                               WHERE  visitorId IN (SELECT visitorId FROM {table} WHERE 1 {filt} AND {date_field} BETWEEN '{date1}' AND '{date2}')
                               AND {date_field} BETWEEN '{date0}' AND '{date2}' GROUP BY visitorId)
                               ALL INNER JOIN
                               (SELECT {metric_counts},visitorId FROM {table} WHERE 1 {filt} AND {date_field} BETWEEN '{date1}' AND '{date2}' GROUP BY visitorId)
                               USING visitorId
                               GROUP BY {dimension}
                               ORDER BY {sort_column} {sort_order}
                               {limit}
                                                  FORMAT JSON
                                                  '''.format(dimension_with_alias=dimensionslist_with_segments_and_aliases[n+1], date0=str(date0),
                                                             metric_counts=metric_counts,dimension=dimension,dimension_without_aliases=list_with_time_dimensions_without_aliases[n+1],
                                                             sum_metric_string=sum_metric_string,
                                                             date1=date['date1'], sort_column=sort_column_in_query,
                                                             date2=date['date2'], filt=filt, sort_order=sort_order,
                                                             limit=limit,
                                                             table=table.format(dimension=using),
                                                             date_field=date_field)
                    array_dates.append(json.loads(get_clickhouse_data(q, 'http://85.143.172.199:8123'))['data'])
            elif attribution_model == 'last_non-direct_interaction':
                for date in period:
                    date0 = (datetime.strptime(date['date1'], '%Y-%m-%d') - timedelta(days=int(attribution_lookup_period))).strftime('%Y-%m-%d')
                    q = '''SELECT {dimension},{sum_metric_string}
                               FROM (SELECT visitorId,any({dimension_without_aliases}) as {dimension} FROM {table}
                               WHERE  visitorId IN (SELECT visitorId FROM {table} WHERE 1 {filt} AND {date_field} BETWEEN '{date1}' AND '{date2}')
                               AND {date_field} < '{date2}' AND referrerType!='direct' GROUP BY visitorId)
                               ALL RIGHT JOIN
                               (SELECT {metric_counts},visitorId,{dimension} FROM {table} WHERE 1 {filt} AND {date_field} BETWEEN '{date1}' AND '{date2}' GROUP BY {dimension},visitorId)
                               USING visitorId
                               GROUP BY {dimension}
                               ORDER BY {sort_column} {sort_order}
                               {limit}
                                                  FORMAT JSON
                                                  '''.format(dimension_with_alias=dimensionslist_with_segments_and_aliases[n+1],
                                                             metric_counts=metric_counts,dimension=dimension,dimension_without_aliases=list_with_time_dimensions_without_aliases[n+1],
                                                             sum_metric_string=sum_metric_string,
                                                             date1=date['date1'], sort_column=sort_column_in_query,
                                                             date2=date['date2'], filt=filt, sort_order=sort_order,
                                                             limit=limit,
                                                             table=table.format(dimension=using),
                                                             date_field=date_field)
                    array_dates.append(json.loads(get_clickhouse_data(q, 'http://85.143.172.199:8123'))['data'])
            else:
                for date in period:
                    q = '''SELECT {dimension_with_alias},{metric_counts} FROM {table}
                                                                    WHERE 1 {filt} AND {updimensions} AND {date_field} BETWEEN '{date1}' AND '{date2}'
                                                                    GROUP BY {dimension}
                                                                    ORDER BY {sort_column} {sort_order}
                                                                    FORMAT JSON
                                                                    '''.format(dimension_with_alias=dimensionslist_with_segments_and_aliases[n+1],dimension=dimension,
                                                                               updimensions=updm,sort_column=sort_column_in_query,sort_order=sort_order,
                                                                               metric_counts=metric_counts,
                                                                               date1=date['date1'],
                                                                               date2=date['date2'], filt=filt,
                                                                               having=having, table=table.format(dimension=using),
                                                                               date_field=date_field)

                    array_dates.append(json.loads(get_clickhouse_data(q, 'http://85.143.172.199:8123'))['data'])
            dates_dicts=datesdicts(array_dates,dimensionslist_with_segments[n+1],dimensionslist_with_segments_and_aliases[n+1],table,date_filt,updm)

            for i2 in array_dates[MaxLenNum(array_dates)][:lim]:
                stat_dict = {'label': i2[dimensionslist_with_segments[n + 1]],
                             'segment':'{label}=={value}'.format(label=dimensionslist_with_segments[n + 1]
                                                                 ,value=i2[dimensionslist_with_segments[n + 1]])}
                dates = []
                c=0
                for m in range(len(array_dates)):
                    metrics_dict = dict.fromkeys(metrics)
                    for j in metrics_dict:
                        try:
                            metrics_dict[j] = dates_dicts[m][i2[dimensionslist_with_segments[n+1]]][j]
                            c=1
                        except:
                            metrics_dict[j]=0
                    dates.append({'date1': period[m]['date1'], 'date2': period[m]['date2'], 'metrics': metrics_dict})
                if c==0:
                    break
                stat_dict['dates'] = dates
                if n != len(dimensionslist_with_segments) - 2:
                    up_dim=stat_dict.copy()#Передаем словарь с информацией о вернем уровне "Все файлы"
                    stat_dict['sub'] = RecStats(n + 1, i2, updimensions, table,up_dim,using)
                sub.append(stat_dict)
        return sub
    def get_clickhouse_data(query,host,connection_timeout=1500):
        """Метод для обращения к базе данных CH"""
        r=requests.post(host,params={'query':query},timeout=connection_timeout)
        return r.text
    def AddCounts(period,dimension_counts,filt,sort_order,table,date_filt,):
        """Добавление ключа Counts в ответ"""
        a={}
        for dim_num in range(len(dimensionslist)):
            if dimensionslist[dim_num] not in list_of_adstat_par and is_ad:
                tab='CHdatabase.hits_with_visits'
            else:
                tab=table

            if attribution_model=='first_interaction':
                date0 = (datetime.strptime(relative_period[0]['date1'], '%Y-%m-%d') - timedelta(days=30)).strftime('%Y-%m-%d')
                q = '''SELECT CAST(uniq({dimension}),'Int') as h{dimension}
                                    FROM (SELECT visitorId,any({dimension_without_aliases}) as {dimension} FROM {table}
                                    WHERE  visitorId IN (SELECT visitorId FROM {table} WHERE 1 {filt} AND {date_field} BETWEEN '{date1}' AND '{date2}')
                                    AND {date_field} BETWEEN '{date0}' AND '{date2}' GROUP BY visitorId)
                                    ALL INNER JOIN
                                    (SELECT visitorId FROM {table} WHERE 1 {filt}  AND {date_field} BETWEEN '{date1}' AND '{date2}' GROUP BY visitorId)
                                    USING visitorId
                                    ORDER BY NULL {sort_order}
                                                       FORMAT JSON
                                                       '''.format(dimension_with_alias=dimensionslist[dim_num],dimension_without_aliases=list_with_time_dimensions_without_aliases[dim_num],
                                                                  date0=str(date0),dimension_counts=dimension_counts[dim_num],
                                                                  date1=relative_period[0]['date1'],dimension=dimensionslist[dim_num],
                                                                  date2=relative_period[0]['date2'], filt=filt, sort_order=sort_order,
                                                                  limit=limit,
                                                                  table=tab.format(dimension=dimensionslist[dim_num]),
                                                                  date_field=date_field)
            elif attribution_model=='last_non-direct_interaction':
                q = '''SELECT CAST(uniq({dimension}),'Int') as h{dimension}
                                                    FROM (SELECT visitorId,any({dimension_without_aliases}) as {dimension} FROM {table}
                                                    WHERE  visitorId IN (SELECT visitorId FROM {table} WHERE 1 {filt} AND {date_field} BETWEEN '{date1}' AND '{date2}')
                                                    AND {date_field} < '{date2}' AND referrerType!='direct' GROUP BY visitorId)
                                                    ALL RIGHT JOIN
                                                    (SELECT visitorId,{dimension} FROM {table} WHERE 1 {filt}  AND {date_field} BETWEEN '{date1}' AND '{date2}' GROUP BY {dimension},visitorId)
                                                    USING visitorId
                                                    ORDER BY NULL {sort_order}
                                                                       FORMAT JSON
                                                                       '''.format(
                    dimension_with_alias=dimensionslist[dim_num],dimension_without_aliases=list_with_time_dimensions_without_aliases[dim_num],
                    dimension_counts=dimension_counts[dim_num],
                    date1=relative_period[0]['date1'],dimension=dimensionslist[dim_num],
                    date2=relative_period[0]['date2'], filt=filt, sort_order=sort_order,
                    limit=limit,
                    table=tab.format(dimension=dimensionslist[dim_num]),
                    date_field=date_field)
                print(q)
            else:
                q = ''' SELECT {dimension_counts}
                                FROM {table}
                                WHERE 1 {filt} AND {date_filt}
                                ORDER BY NULL {sort_order}
                                FORMAT JSON
                               '''.format(dimension_counts=dimension_counts[dim_num], filt=filt,
                                          sort_order=sort_order, table=tab.format(dimension=dimensionslist[dim_num]),
                                          date_filt=date_filt)
                print(q)
            try:
                a.update(json.loads(get_clickhouse_data(q, 'http://85.143.172.199:8123'))['data'][0])
            except:
                a.update({'h'+dimensionslist[dim_num]:0})
        b = {}
        try:
            # Объеденяем словарь с датами со словарем  вернувшихся значений каждого из запрошенных параметров
            #изменяем значения показателей на целочисленный тип
            for key in a.keys():
                b[key[1:]]=a[key]
        except:
            b=dict.fromkeys(dimensionslist,0)
        return b
    def AddMetricSums(period,metric_counts_list,filt,metrics,sort_order,table):
        """Добавление ключа metric_sums в ответ"""
        dates = []
        metric_counts_visits=[]
        metric_counts_stat=[]
        a={}
        b={}
        #разделлим metric_counts для визитов и для рекламной статистики
        for metr in metric_counts_list:
            if 'clicks' in metr or 'impressions' in metr or 'cost' in metr:
                metric_counts_stat.append(metr)
            else:
                metric_counts_visits.append(metr)
        metric_counts_stat=','.join(metric_counts_stat)
        metric_counts_visits = ','.join(metric_counts_visits)
        for (date,abs_date) in zip(relative_period,period):
            # Запрос на получение сумм показателей без фильтра
            q_total = ''' SELECT {metric_counts}
                    FROM {table}
                    WHERE {date_field} BETWEEN '{date1}' AND '{date2}'
                    ORDER BY NULL {sort_order}
                    FORMAT JSON
                   '''.format(date1=date['date1'], date2=date['date2'], metric_counts=metric_counts,
                              sort_order=sort_order,table=table.format(dimension=dimensionslist[0]),date_field=date_field)

            # С фильтром
            q = ''' SELECT {metric_counts}
                                FROM {table}
                                WHERE 1 {filt} AND {date_field} BETWEEN '{date1}' AND '{date2}'
                                ORDER BY NULL {sort_order}
                                FORMAT JSON
                               '''.format(date1=date['date1'], date2=date['date2'], metric_counts=metric_counts,
                                          filt=filt, sort_order=sort_order,table=table.format(
                                                                              dimension=dimensionslist[0]),date_field=date_field)
            """q_total_stat = ''' SELECT {metric_counts}
                                                    FROM {table}
                                                    WHERE {date_field} BETWEEN '{date1}' AND '{date2}'
                                                    ORDER BY NULL {sort_order}
                                                    FORMAT JSON
                                                   '''.format(date1=date['date1'], date2=date['date2'],
                                                              metric_counts=metric_counts_stat,
                                                              sort_order=sort_order,
                                                              table=table.format(dimension=dimensionslist[0]),
                                                              date_field=date_field)
            print(q_total_stat)
            # С фильтром
            q_stat = ''' SELECT {metric_counts}
                                                                FROM {table}
                                                                WHERE 1 {filt} AND {date_field} BETWEEN '{date1}' AND '{date2}'
                                                                ORDER BY NULL {sort_order}
                                                                FORMAT JSON
                                                               '''.format(date1=date['date1'], date2=date['date2'],
                                                                          metric_counts=metric_counts_stat,
                                                                          filt=filt, sort_order=sort_order,
                                                                          table=table.format(
                                                                              dimension=dimensionslist[0]),
                                                                          date_field=date_field)"""
            #Проверка на существование записей, если их нет, возвращаем нули
            try:
                """if metric_counts_visits != "":
                    a = json.loads(get_clickhouse_data(q_total, 'http://85.143.172.199:8123'))['data'][0]
                    b = json.loads(get_clickhouse_data(q, 'http://85.143.172.199:8123'))['data'][0]
                #Если запрошены показатели рекламной статистики то считаем для них metric_sums по таблице с статистикой
                if metric_counts_stat != "":
                    a.update(json.loads(get_clickhouse_data(q_total_stat, 'http://85.143.172.199:8123'))['data'][0])
                    b.update(json.loads(get_clickhouse_data(q_stat, 'http://85.143.172.199:8123'))['data'][0])"""
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
            metric_dict.update(abs_date)
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
    def AddStats2(dim,dim_with_alias, metric_counts, filt, limit, period, metrics, table,date_filt):
        """Добавление ключа stats в ответ"""
        stats = []
        if dim[0] not in list_of_adstat_par and is_ad:
            return stats
        #Определяем, есть ли вначале dimensions группа сегментов
        if type(dim[0])==list:
            stats.append(AddMetricSumsWithFilt(period, metric_counts, filt, metrics, sort_order, table))
            seg_label_list={}
            #сортировка по имени сегмента
            for i in dim[0]:
                seg_label_list[int(i[7:])]=(json.loads(requests.get(
                    'https://s.analitika.online/api/reference/segments/{num_seg}/?all=1'.format(num_seg=int(i[7:])),
                    headers=headers).content.decode('utf-8'))['name'])
            if sort_order=='desc':
                seg_label_list=sorted(seg_label_list,reverse=True)
            else:
                seg_label_list = sorted(seg_label_list)
        else:
            seg_label_list=[]
        #Если dimension является сегментом(не группой сегментов а отдельным сегментом):
        if 'segment' in dim[0]:
            stats.append(AddMetricSumsWithFilt(period, metric_counts, filt, metrics, sort_order, table))
            array_dates = []
            updimensions = []
            seg=json.loads(requests.get('https://s.analitika.online/api/reference/segments/{num_seg}/?all=1'.format(num_seg=int(dim[0][7:])),
                                headers=headers).content.decode('utf-8'))['real_definition']
            seg_filt=seg.partition("==")[0]+"=='"+seg.partition("==")[2]+"'"
            seg_label=json.loads(requests.get('https://s.analitika.online/api/reference/segments/{num_seg}/?all=1'.format(num_seg=int(dim[0][7:])),
                                headers=headers).content.decode('utf-8'))['name']
            for date in relative_period:
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
                if search_pattern.lower() not in str(i['label']).lower():
                    continue

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
        elif seg_label_list==[]:
            updimensions = []
            array_dates = []
            if sort_column=="":
                sort_column_in_query=dim[0]
            else:
                sort_column_in_query=sort_column
            using=dim[0]
            #если указана модель аттрибуции показатели рассчитываются для первого визита

            if attribution_model=='first_interaction':
                for date in relative_period:
                    date0=(datetime.strptime(date['date1'], '%Y-%m-%d') - timedelta(days=int(attribution_lookup_period))).strftime('%Y-%m-%d')
                    q = '''SELECT {dimension},{sum_metric_string}
                    FROM (SELECT visitorId,any({dimension_without_aliases}) as {dimension} FROM {table}
                    WHERE  visitorId IN (SELECT visitorId FROM {table} WHERE 1 {filt} AND {date_field} BETWEEN '{date1}' AND '{date2}')
                    AND {date_field} BETWEEN '{date0}' AND '{date2}' GROUP BY visitorId)
                    ALL INNER JOIN
                    (SELECT {metric_counts},visitorId FROM {table} WHERE 1 {filt} AND {date_field} BETWEEN '{date1}' AND '{date2}' GROUP BY visitorId)
                    USING visitorId
                    GROUP BY {dimension}
                    ORDER BY {sort_column} {sort_order}
                    {limit}
                                       FORMAT JSON
                                       '''.format(dimension_with_alias=dim_with_alias[0],date0=str(date0),dimension=dim[0],dimension_without_aliases=list_with_time_dimensions_without_aliases[0],
                                                  metric_counts=metric_counts,sum_metric_string=sum_metric_string,
                                                  date1=date['date1'],sort_column=sort_column_in_query,
                                                  date2=date['date2'], filt=filt,sort_order=sort_order,limit=limit,
                                                  table=table.format(dimension=dim[0]), date_field=date_field)
                    print(q)
                    array_dates.append(json.loads(get_clickhouse_data(q, 'http://85.143.172.199:8123'))['data'])
            elif  attribution_model=='last_non-direct_interaction':
                for date in relative_period:
                    q = '''SELECT {dimension},{sum_metric_string}
                    FROM (SELECT visitorId,any({dimension_without_aliases}) as {dimension} FROM {table}
                    WHERE  visitorId IN (SELECT visitorId FROM {table} WHERE 1 {filt} AND {date_field} BETWEEN '{date1}' AND '{date2}')
                    AND {date_field} < '{date2}' AND referrerType!='direct' GROUP BY visitorId)
                    ALL RIGHT JOIN
                    (SELECT {metric_counts},visitorId, {dimension} FROM {table} WHERE 1 {filt} AND {date_field} BETWEEN '{date1}' AND '{date2}' GROUP BY visitorId,{dimension})
                    USING visitorId
                    GROUP BY {dimension}
                    ORDER BY {sort_column} {sort_order}
                    {limit}
                                       FORMAT JSON
                                       '''.format(dimension_with_alias=dim_with_alias[0],dimension=dim[0],dimension_without_aliases=list_with_time_dimensions_without_aliases[0],
                                                  metric_counts=metric_counts,sum_metric_string=sum_metric_string,
                                                  date1=date['date1'],sort_column=sort_column_in_query,
                                                  date2=date['date2'], filt=filt,sort_order=sort_order,limit=limit,
                                                  table=table.format(dimension=dim[0]), date_field=date_field)
                    print(q)
                    array_dates.append(json.loads(get_clickhouse_data(q, 'http://85.143.172.199:8123'))['data'])
            else:
                for date in relative_period:
                    q = '''SELECT {dimension_with_alias},{metric_counts} FROM {table}
                                       WHERE 1 {filt} AND {date_field} BETWEEN '{date1}' AND '{date2}'
                                       GROUP BY {dimension}
                                       ORDER BY {sort_column} {sort_order}
                                       {limit}
                                       FORMAT JSON
                                       '''.format(dimension_with_alias=dim_with_alias[0],dimension=dim[0], metric_counts=metric_counts,
                                                  date1=date['date1'],sort_column=sort_column_in_query,
                                                  date2=date['date2'], filt=filt, limit=limit,sort_order=sort_order,
                                                  table=table.format(dimension=dim[0]), date_field=date_field)
                    print(q)
                    array_dates.append(json.loads(get_clickhouse_data(q, 'http://85.143.172.199:8123'))['data'])

            dates_dicts=datesdicts(array_dates,dim[0],dim_with_alias[0],table,date_filt,1)

                #определим самый большой список в array_dates
            for i in array_dates[MaxLenNum(array_dates)][:lim]:
                if search_pattern.lower() not in str(i[dim[0]]).lower():
                    continue
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
                            metrics_dict[j] = dates_dicts[m][i[dim[0]]][j]
                        except:
                            metrics_dict[j]=0

                    dates.append({'date1': period[m]['date1'], 'date2': period[m]['date2'], 'metrics': metrics_dict})

                stat_dict['dates'] = dates
                if len(dim) > 1:
                    # Добавляем подуровень. Если параметр вычисляемый то подставляем его название из словаря time_dimensions_dict
                    try:
                        if dim[0] in time_dimensions_dict.keys():
                            if type(i[dim[0]]) is int:
                                updimensions.append("{updimension}={updimension_val}".format(updimension=time_dimensions_dict[dim[0]],updimension_val=i[dim[0]]))
                            else:
                                updimensions.append(
                                    "{updimension}='{updimension_val}'".format(updimension=time_dimensions_dict[dim[0]],
                                                                             updimension_val=i[dim[0]]))
                        else:
                            updimensions.append("{updimension}='{updimension_val}'".format(updimension_val=i[dim[0]],
                                                                                       updimension=dim[0]))
                    except:
                        pass
                    up_dim=stat_dict.copy()#Передаем словарь с информацией о вернем уровне "Все файлы"
                    stat_dict['sub'] = RecStats(0, i, updimensions, table,up_dim,dim[0])
                stats.append(stat_dict)
        #Для групп сегментов
        for num in seg_label_list:
            array_dates = []
            updimensions = []
            seg = json.loads(requests.get(
                'https://s.analitika.online/api/reference/segments/{num_seg}/'.format(num_seg=int(num)),
                headers=headers).content.decode('utf-8'))['real_definition']
            seg_filt = seg.partition("==")[0] + "=='" + seg.partition("==")[2] + "'"
            seg_label = json.loads(requests.get(
                'https://s.analitika.online/api/reference/segments/{num_seg}/'.format(num_seg=int(num)),
                headers=headers).content.decode('utf-8'))['name']
            counter=0
            for date in relative_period:
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
                if search_pattern.lower() not in str(i['label']).lower():
                    continue
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
                    try:#если значение в подфильтре целочисленное, то не добавляем кавычки
                        int(sub_str.partition(j)[2])
                        json.loads(get_clickhouse_data('SELECT {par}=={val} FROM CHdatabase.visits ALL INNER JOIN CHdatabase.hits USING idVisit LIMIT 1 FORMAT JSON'.format(par=sub_str.partition(j)[0],val=sub_str.partition(j)[2]), 'http://85.143.172.199:8123'))
                        sub_str = sub_str.partition(j)[0] + j +sub_str.partition(j)[2]
                    except:
                        if sub_str.partition(j)[0] in ['day_of_week_code','month_code',"year","minute","second"]:
                            sub_str = sub_str.partition(j)[0] + j + sub_str.partition(j)[2]
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
        return end_filt.replace('date','toDate(serverTimestamp)').replace('month_code','toMonth(toDate(serverTimestamp))').replace('day_of_week_code',"toDayOfWeek(toDate(serverTimestamp))")\
                .replace('day_of_week',"dictGetString('week','{lang}',toUInt64(toDayOfWeek(toDate(serverTimestamp))))".format(
                        lang=lang))\
            .replace('year',"toYear(toDate(serverTimestamp))").replace('minute',"toMinute(toDateTime(serverTimestamp))").replace('second',"toSecond(toDateTime(serverTimestamp))")\
            .replace('month',"dictGetString('month','{lang}',toUInt64(toMonth(toDate(serverTimestamp))))".format(lang=lang))
    if request.method=='POST':
        #Заголовки для запроса сегментов
        headers = {
            'Authorization': 'eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJ1c2VyX2lkIjoxOSwiZW1haWwiOiIiLCJ1c2VybmFtZSI6ImFydHVyIiwiZXhwIjoxNTE4MTIxNDIyfQ._V0PYXMrE2pJlHlkMtZ_c-_p0y0MIKsv8o5jzR5llpY',
            'Content-Type': 'application/json'}
        # Парсинг json
        try:
            sort_order=json.loads(request.body.decode('utf-8'))['sort_order']
        except:
            sort_order=""
        try:
            lang = json.loads(request.body.decode('utf-8'))['lang']
            if lang == "":
                lang='ru'
            if lang == "en":
                lang='eng'
        except:
            lang = "ru"
        #сортировка по переданному показателю
        try:
            sort_column = json.loads(request.body.decode('utf-8'))['sort_column']
        except:
            sort_column=""
        dimensionslist_with_segments=json.loads(request.body.decode('utf-8'))['dimensions']
        dimensionslist = []
        #Создание списка параметров без сегментов
        dimensionslist_with_segments_and_aliases=[]
        time_dimensions_dict={}
        list_with_time_dimensions_without_aliases=[]#список с параметрами,в которых временные параматры, типа year,month и тд будут представлены без алиасов
        for d in dimensionslist_with_segments:
            if 'segment' not in d and d!=list:
                dimensionslist.append(d)
                if d == 'month':
                    time_dimensions_dict[d] = "dictGetString('month','{lang}',toUInt64(toMonth(toDate(serverTimestamp))))".format(lang=lang)
                    dimensionslist_with_segments_and_aliases.append("dictGetString('month','{lang}',toUInt64(toMonth(toDate(serverTimestamp)))) as month".format(lang=lang))
                    list_with_time_dimensions_without_aliases.append("dictGetString('month','{lang}',toUInt64(toMonth(toDate(serverTimestamp))))".format(lang=lang))
                    continue
                if d == 'second':
                    time_dimensions_dict[d]="toSecond(toDateTime(serverTimestamp))"
                    dimensionslist_with_segments_and_aliases.append("toSecond(toDateTime(serverTimestamp)) as second")
                    list_with_time_dimensions_without_aliases.append("toSecond(toDateTime(serverTimestamp))")
                    continue
                if d == 'minute':
                    time_dimensions_dict[d]="toMinute(toDateTime(serverTimestamp))"
                    dimensionslist_with_segments_and_aliases.append("toMinute(toDateTime(serverTimestamp)) as minute")
                    list_with_time_dimensions_without_aliases.append("toMinute(toDateTime(serverTimestamp))")
                    continue
                if d == 'year':
                    time_dimensions_dict[d]="toYear(toDate(serverTimestamp))"
                    dimensionslist_with_segments_and_aliases.append("toYear(toDate(serverTimestamp)) as year")
                    list_with_time_dimensions_without_aliases.append("toYear(toDate(serverTimestamp))")
                    continue
                if d == 'day_of_week_code':
                    time_dimensions_dict[d]="toDayOfWeek(toDate(serverTimestamp))"
                    dimensionslist_with_segments_and_aliases.append("toDayOfWeek(toDate(serverTimestamp)) as day_of_week_code")
                    list_with_time_dimensions_without_aliases.append("toDayOfWeek(toDate(serverTimestamp))")
                    continue
                if d=='month_code':
                    time_dimensions_dict[d] = "toMonth(toDate(serverTimestamp))"
                    dimensionslist_with_segments_and_aliases.append(
                        "toMonth(toDate(serverTimestamp)) as month_code")
                    list_with_time_dimensions_without_aliases.append(
                        "toMonth(toDate(serverTimestamp))")
                    continue
                if d=='date':
                    time_dimensions_dict[d] = "toDate(serverTimestamp)"
                    dimensionslist_with_segments_and_aliases.append(
                        "toDate(serverTimestamp) as date")
                    list_with_time_dimensions_without_aliases.append(
                        "toDate(serverTimestamp)")
                    continue
                if d == 'day_of_week':
                    time_dimensions_dict[
                        d] = "dictGetString('week','{lang}',toUInt64(toDayOfWeek(toDate(serverTimestamp))))".format(
                        lang=lang)
                    dimensionslist_with_segments_and_aliases.append(
                        "dictGetString('week','{lang}',toUInt64(toDayOfWeek(toDate(serverTimestamp)))) as day_of_week".format(
                            lang=lang))
                    list_with_time_dimensions_without_aliases.append(
                        "dictGetString('week','{lang}',toUInt64(toDayOfWeek(toDate(serverTimestamp))))".format(
                            lang=lang))
                    continue
                list_with_time_dimensions_without_aliases.append(d)

            dimensionslist_with_segments_and_aliases.append(d)

        metrics = json.loads(request.body.decode('utf-8'))['metrics']
        # строка  "sum(metric1),avg(metric2)...". если показатель относительный используется avg, если нет - sum
        sum_metric_string=[]
        for i in metrics:
            if i in ['conversion_rate','bounce_rate','nb_new_visits_per_all_visits','nb_new_visitors_per_all_visitors','avg_time_generation','nb_return_visitors_per_all_visitors','avg_visit_length','nb_pageviews_per_visit','nb_actions_per_visit','nb_downloas_per_visit'] or i.find(r'goal\d{1,3}_conversion') != -1:
                sum_metric_string.append("floor(avg("+i+"),2) as "+i)
            else:
                sum_metric_string.append("CAST(sum(" + i + "),'Int') as " + i)
        sum_metric_string=','.join(sum_metric_string)
        #если в запросе не указан сдвиг, зададим его равным нулю
        try:
            offset = json.loads(request.body.decode('utf-8'))['offset']
        except:
            offset='0'
        #если в запросе не указан лимит, зададим его путой строкой, если указан, составим строку LIMIT...
        try:
            lim=int(json.loads(request.body.decode('utf-8'))['limit'])
            limit = 'LIMIT '+str(offset)+','+str(json.loads(request.body.decode('utf-8'))['limit'])
        except:
            limit=''
            lim=None

        period = json.loads(request.body.decode('utf-8'))['periods']
        try:
            filter = json.loads(request.body.decode('utf-8'))['filter']
        except:
            filt=" "
        else:
            if filter!="":
                filt="AND"+"("+FilterParse(filter)+")"
            else:
                filt="AND 1"

        having = 'HAVING 1'
        try:
            search_pattern=json.loads(request.body.decode('utf-8'))['search_pattern']
        except:
            search_pattern=""
        try:
            attribution_model=json.loads(request.body.decode('utf-8'))['attribution_model']
        except:
            attribution_model=""
        try:
            attribution_lookup_period=json.loads(request.body.decode('utf-8'))['attribution_lookup_period']
        except:
            attribution_lookup_period=""
        try:
            profile_id=json.loads(request.body.decode('utf-8'))['profile_id']
            try:
                timezone = json.loads(requests.get(
                    'https://s.analitika.online/api/profiles/{profile_id}/?all=1'.format(profile_id=profile_id),
                    headers=headers).content.decode('utf-8'))['timezone']
                date1 = datetime.strptime(period[0]['date1'] + '-00', '%Y-%m-%d-%H')
                timezone = pytz.timezone(timezone)
                date1 = timezone.localize(date1)
                time_offset = str(date1)[19:]
                relative_period = []
                for date in period:
                    if time_offset[0] == '+':
                        relative_period.append({'date1': str(
                            datetime.strptime(date['date1'] + '-00', '%Y-%m-%d-%H') - timedelta(
                                hours=int(time_offset[2]) - 3)), 'date2': str(
                            datetime.strptime(date['date2'] + '-00', '%Y-%m-%d-%H') - timedelta(
                                hours=int(time_offset[2]) - 3))})
                    else:
                        relative_period.append({'date1': str(
                            datetime.strptime(date['date1'] + '-00', '%Y-%m-%d-%H') - timedelta(
                                hours=-int(time_offset[2]) - 3)),
                            'date2': str(datetime.strptime(date['date2'] + '-00', '%Y-%m-%d-%H') - timedelta(
                                hours=-int(time_offset[2]) - 3))})
                date_field='toDateTime(serverTimestamp)'
            except:
                relative_period=period
                date_field = 'serverDate'
            try:
                if json.loads(get_clickhouse_data(
                        'SELECT idSite FROM CHdatabase.hits_with_visits WHERE idSite={idSite} FORMAT JSON'.format(
                                idSite=json.loads(requests.get(
                                        'https://s.analitika.online/api/profiles/{profile_id}/?all=1'.format(
                                                profile_id=profile_id), headers=headers).content.decode('utf-8'))[
                                    'site_db_id']), 'http://85.143.172.199:8123'))['data'] == []:
                    filt = 'AND 0'
                else:
                    filt = filt + 'AND idSite==' + str(json.loads(requests.get(
                        'https://s.analitika.online/api/profiles/{profile_id}/?all=1'.format(profile_id=profile_id),
                        headers=headers).content.decode('utf-8'))['site_db_id'])
            except:
                filt = 'AND 0'
        except:
            pass
            relative_period=period
            date_field = 'serverDate'

        #print(json.loads(requests.get('https://s.analitika.online/api/profiles/{profile_id}/'.format(profile_id=profile_id), headers=headers).content.decode('utf-8')))
        print(relative_period)

        table = 'CHdatabase.hits_with_visits'
        list_of_adstat_par=['Clicks','Impressions','Cost','StatDate','idSite', 'AdCampaignId', 'AdBannerId', 'AdChannelId', 'AdDeviceType', 'AdGroupId', 'AdKeywordId',
                       'AdPosition', 'AdPositionType', 'AdRegionId', 'AdRetargetindId', 'AdPlacement', 'AdTargetId', 'AdvertisingSystem', 'DRF', 'campaignContent',
                       'campaignKeyword', 'campaignMedium', 'campaignName', 'campaignSource']
        is_ad=False

        #Выбираем таблицу
        for metr in metrics:
            if metr in ['cost','impressions','clicks']:
                table="CHdatabase.hits_with_visits ALL LEFT JOIN CHdatabase.adstat USING {dimension}"
                is_ad=True
                break
        for dim in dimensionslist:
            if dim in ['Cost', 'Impressions', 'Clicks','StatDate']:
                table = "CHdatabase.hits_with_visits ALL LEFT JOIN CHdatabase.adstat USING {dimension}"
                is_ad = True
                break

        #Формируем массив с count() для каждого параметра
        dimension_counts=[]
        for i in dimensionslist:
            if i in time_dimensions_dict.keys():
                dimension_counts.append("CAST(uniq({dimension}),'Int') as h{dimension_alias}".format(dimension=time_dimensions_dict[i],dimension_alias=i))
            else:
                dimension_counts.append("CAST(uniq({dimension}),'Int') as h{dimension}".format(dimension=i))

        # ФОрмируем массив с запросом каждого показателя в SQL
        metric_counts_list=MetricCounts(metrics,headers)
        metric_counts=','.join(metric_counts_list)
        # Заполнение таблицы с рекламной статистикой
        load_query = "INSERT INTO CHdatabase.adstat VALUES "

        for part in json.loads(requests.get('https://s.analitika.online/api/ad_stat/?all=1', headers=headers).content.decode('utf-8'))['results']:
            query = load_query + "(" + str(list(part.values()))[1:len(str(list(part.values()))) - 1] + ")"
            query = query.replace("None", "'none'")
            #get_clickhouse_data(query, 'http://85.143.172.199:8123')
        #Фильтр по всем датам
        date_filt = []
        for dates in relative_period:
            date_filt.append(
                "({date_field} BETWEEN '".format(date_field=date_field) + str(dates['date1']) + "' AND '" + str(
                    dates['date2']) + "')")
        date_filt = ' OR '.join(date_filt)
        #Добавляем в выходной словарь параметр counts
        resp={}#Выходной словарь
        resp['counts'] = {}
        resp['counts']=AddCounts(period,dimension_counts,filt,sort_order,table,date_filt)
        # Добавляем в выходной словарь параметр metric_sums
        resp['metric_sums']={}
        resp['metric_sums']['dates'] = AddMetricSums(period,metric_counts_list,filt,metrics,sort_order,table)
        stats=AddStats2(dimensionslist_with_segments,dimensionslist_with_segments_and_aliases,metric_counts,filt,limit,period,metrics,table,date_filt)
        # Добавляем stats
        resp['stats']=stats
        pprint.pprint(resp)
        response=JsonResponse(resp,safe=False,)
        response['Access-Control-Allow-Origin']='*'
        return response
    else:
        args={}
        args.update(csrf(request))
        return render_to_response('mainAPI.html',args)
def segment_stat(request):
    def get_clickhouse_data(query, host, connection_timeout=1500):
        """Метод для обращения к базе данных CH"""
        r = requests.post(host, params={'query': query}, timeout=connection_timeout)
        return r.text
    def FilterParse(filt_string):
        """Метод для перевода  global_filter в строку для sql запроса"""
        # filt_string=filt_string.replace(',',' OR ')
        # filt_string = filt_string.replace(';', ' AND ')
        # print(filt_string.partition('=@'))
        simple_operators = ['==', '!=', '>=', '<=', '>', '<']
        like_operators = ['=@', '!@', '=^', '=$', '!^', '!&']
        like_str = [" LIKE '%{val}%'", " NOT LIKE '%{val}%'", " LIKE '{val}%'", " LIKE '%{val}'", " NOT LIKE '{val}%'",
                    " NOT LIKE '%{val}'"]
        match_operators = ['=~', '!~']
        match_str = [" match({par}?'{val}')", " NOT match({par}?'{val}')"]
        separator_indices = []
        for i in range(len(filt_string)):
            if filt_string[i] == ',' or filt_string[i] == ';':
                separator_indices.append(i)
        separator_indices.append(len(filt_string))
        end_filt = ""
        for i in range(len(separator_indices)):
            if i == 0:
                sub_str = filt_string[0:separator_indices[i]]
            else:
                sub_str = filt_string[separator_indices[i - 1] + 1:separator_indices[i]]
            for j in simple_operators:
                if sub_str.partition(j)[2] == '':
                    pass
                else:
                    sub_str = sub_str.partition(j)[0] + j + "'" + sub_str.partition(j)[2] + "'"
                    break
            for j in range(len(like_operators)):
                if sub_str.partition(like_operators[j])[2] == '':
                    pass
                else:
                    sub_str = sub_str.partition(like_operators[j])[0] + like_str[j].format(
                        val=sub_str.partition(like_operators[j])[2])
                    break
            for j in range(len(match_operators)):
                if sub_str.partition(match_operators[j])[2] == '':
                    pass
                else:
                    sub_str = match_str[j].format(val=sub_str.partition(match_operators[j])[2],
                                                  par=sub_str.partition(match_operators[j])[0])
                    break
            try:
                end_filt = end_filt + sub_str + filt_string[separator_indices[i]]
            except:
                end_filt = end_filt + sub_str

        end_filt = end_filt.replace(',', ' OR ')
        end_filt = end_filt.replace(';', ' AND ')
        end_filt = end_filt.replace('?', ',')
        return end_filt
    if request.method=='GET':
        response=dict(request.GET)
        for key in  response.keys():
            response[key]=response[key][0]

        try:
            if response['filter']=='':
                filter=1
            else:
                filter=FilterParse(response['filter'])
        except:
            filter=1
        q_total="""
        SELECT CAST(uniq(visitorId),'Int') as visitors,CAST(uniq(idVisit),'Int') as visits FROM CHdatabase.visits WHERE serverDate BETWEEN '{date1}' AND '{date2}' FORMAT JSON
        """.format(date1=response['date1'],date2=response['date2'])
        try:
            total=json.loads(get_clickhouse_data(q_total, 'http://85.143.172.199:8123'))['data'][0]
        except:
            total={'visitors':0,'visits':0}
        q = """
                SELECT CAST(uniq(visitorId),'Int') as visitors,CAST(uniq(idVisit),'Int') as visits FROM CHdatabase.visits WHERE serverDate BETWEEN '{date1}' AND '{date2}' AND {filter} FORMAT JSON
                """.format(date1=response['date1'], date2=response['date2'],filter=filter)
        try:
            with_filter = json.loads(get_clickhouse_data(q, 'http://85.143.172.199:8123'))['data'][0]
        except:
            with_filter = {'visitors': 0, 'visits': 0}
        visitors={'total_sum':total['visitors'],'sum':with_filter['visitors']}
        visits={'total_sum':total['visits'],'sum':with_filter['visits']}
        response['segment_stat']={'visitors':visitors,'visits':visits}
        print(response)
        return JsonResponse(response, safe=False, )

@csrf_exempt
def diagram_stat(request):

    def get_clickhouse_data(query,host,connection_timeout=1500):
        """Метод для обращения к базе данных CH"""
        r=requests.post(host,params={'query':query},timeout=connection_timeout)
        return r.text
    def AddCounts(date1,date2,dimension_counts,filt,sort_order,table):
        """Добавление ключа Counts в ответ"""
        a={}
        for dim_num in range(len(dimensionslist)):
            if dimensionslist[dim_num] not in list_of_adstat_par and is_ad:
                tab='CHdatabase.hits ALL INNER JOIN CHdatabase.visits USING idVisit'
            else:
                tab=table
            q = ''' SELECT {dimension_counts}
                     FROM {table}
                     WHERE 1 {filt} AND {date_field} BETWEEN '{date1}' AND '{date2}'
                     ORDER BY NULL {sort_order}
                     FORMAT JSON
                    '''.format(date1=date1, date2=date2, dimension_counts=dimension_counts[dim_num], filt=filt,
                               sort_order=sort_order,table=tab.format(dimension=dimensionslist[dim_num]),date_field=date_field)

            try:
                a.update(json.loads(get_clickhouse_data(q, 'http://85.143.172.199:8123'))['data'][0])
            except:
                a.update({'h'+dimensionslist[dim_num]:0})
        b = {}
        try:
            # Объеденяем словарь с датами со словарем  вернувшихся значений каждого из запрошенных параметров
            #изменяем значения показателей на целочисленный тип
            for key in a.keys():
                b[key[1:]]=a[key]
        except:
            b=dict.fromkeys(dimensionslist,0)
        return b
    def AddMetricSums(date1,date2,metric_counts_list,filt,metrics,sort_order,table):
        """Добавление ключа metric_sums в ответ"""
        dates = []
        metric_counts_visits = []
        metric_counts_stat = []
        a = {}
        b = {}
        for metr in metric_counts_list:
            if 'clicks' in metr or 'impressions' in metr or 'cost' in metr:
                metric_counts_stat.append(metr)
            else:
                metric_counts_visits.append(metr)
        metric_counts_stat = ','.join(metric_counts_stat)
        metric_counts_visits = ','.join(metric_counts_visits)
        # Запрос на получение сумм показателей без фильтра
        q_total = ''' SELECT {metric_counts}
                    FROM {table}
                    WHERE {date_field} BETWEEN '{date1}' AND '{date2}'
                    ORDER BY NULL {sort_order}
                    FORMAT JSON
                   '''.format(date1=relative_date1, date2=relative_date2, metric_counts=metric_counts_visits,
                              sort_order=sort_order,table=table,date_field=date_field)

        # С фильтром
        q = ''' SELECT {metric_counts}
                                FROM {table}
                                WHERE 1 {filt} AND {date_field} BETWEEN '{date1}' AND '{date2}'
                                ORDER BY NULL {sort_order}
                                FORMAT JSON
                               '''.format(date1=relative_date1, date2=relative_date2, metric_counts=metric_counts_visits,
                                          filt=filt, sort_order=sort_order,table=table,date_field=date_field)
        q_total_stat = ''' SELECT {metric_counts}
                                                           FROM {table}
                                                           WHERE {date_field} BETWEEN '{date1}' AND '{date2}'
                                                           ORDER BY NULL {sort_order}
                                                           FORMAT JSON
                                                          '''.format(date1=relative_date1, date2=relative_date2,
                                                                     metric_counts=metric_counts_stat,
                                                                     sort_order=sort_order,
                                                                     table=table.format(dimension=dimensionslist[0]),
                                                                     date_field=date_field)
        # С фильтром
        q_stat = ''' SELECT {metric_counts}
                                                                       FROM {table}
                                                                       WHERE 1 {filt} AND {date_field} BETWEEN '{date1}' AND '{date2}'
                                                                       ORDER BY NULL {sort_order}
                                                                       FORMAT JSON
                                                                      '''.format(date1=relative_date1,
                                                                                 date2=relative_date2,
                                                                                 metric_counts=metric_counts_stat,
                                                                                 filt=filt, sort_order=sort_order,
                                                                                 table=table.format(
                                                                                     dimension=dimensionslist[0]),
                                                                                 date_field=date_field)
            #Проверка на существование записей, если их нет, возвращаем нули
        try:
            if metric_counts_visits != "":
                a = json.loads(get_clickhouse_data(q_total, 'http://85.143.172.199:8123'))['data'][0]
                b = json.loads(get_clickhouse_data(q, 'http://85.143.172.199:8123'))['data'][0]
            # Если запрошены показатели рекламной статистики то считаем для них metric_sums по таблице с статистикой
            if metric_counts_stat != "":
                a.update(json.loads(get_clickhouse_data(q_total_stat, 'http://85.143.172.199:8123'))['data'][0])
                b.update(json.loads(get_clickhouse_data(q_stat, 'http://85.143.172.199:8123'))['data'][0])
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
        metric_dict.update({'date1':date1,'date2':date2})
        dates.append(metric_dict)
        return dates
    def AddStats2(dimensionslist,dim_with_aliases, metric_counts, filt, limit, having, date1,date2, metrics, table):
        """Добавление ключа stats в ответ"""
        #Определяем, есть ли вначале dimensions группа сегментов
        q="""
        SELECT {dimensions_with_aliases},({metric_counts}) as metrics
        FROM {table}
        WHERE 1 {filt} AND {date_field} BETWEEN '{date1}' AND '{date2}'
        GROUP BY {dimensions}
        ORDER BY {sort_column} {sort_order}
        {limit}
        FORMAT JSON
        """.format(dimensions_with_aliases=','.join(dim_with_aliases),date1=date1,date2=date2,filt=filt,date_field=date_field,table=table.format(dimension=','.join(dimensionslist)),limit=limit,sort_column=sort_column,sort_order=sort_order,metric_counts=metric_counts,dimensions=','.join(dimensionslist))
        print(q)
        stats=json.loads(get_clickhouse_data(q, 'http://85.143.172.199:8123'))['data']
        #Если показатеь один, обрабатываем ошибку

        try:
            for stat in stats:
                metr = {}
                for metric_num in range(len(stat['metrics'])):
                    metr[metrics[metric_num]]=stat['metrics'][metric_num]


                stat['metrics']=metr
                if dimensionslist!=dimensionslist_with_segments:
                    stat['segment']='Все данные'
        except:
            for stat in stats:
                stat['metrics']={metrics[0]:stat['metrics']}
        for dim in dimensionslist_with_segments:
            if 'segment' in dim:
                seg = json.loads(requests.get(
                    'https://s.analitika.online/api/reference/segments/{num_seg}/?all=1'.format(num_seg=int(dim[7:])),
                    headers=headers).content.decode('utf-8'))['real_definition']
                seg_filt = seg.partition("==")[0] + "=='" + seg.partition("==")[2] + "'"
                seg_label = json.loads(requests.get(
                    'https://s.analitika.online/api/reference/segments/{num_seg}/?all=1'.format(num_seg=int(dim[7:])),
                    headers=headers).content.decode('utf-8'))['name']
                q = """
                        SELECT {dimensions},[{metric_counts}] as metrics
                        FROM {table}
                        WHERE 1 {filt} AND {date_field} BETWEEN '{date1}' AND '{date2}' AND {seg_filt}
                        GROUP BY {dimensions}
                        ORDER BY {sort_column} {sort_order}
                        {limit}
                        FORMAT JSON
                        """.format(date1=date1, date2=date2, filt=filt, date_field=date_field, table=table, limit=limit,
                                   sort_column=sort_column, sort_order=sort_order, metric_counts=metric_counts,
                                   dimensions=','.join(dimensionslist),seg_filt=seg_filt)

                stat_with_segment = json.loads(get_clickhouse_data(q, 'http://85.143.172.199:8123'))['data']

                for stat in stat_with_segment:
                    metr = {}
                    for metric_num in range(len(stat['metrics'])):
                        metr[metrics[metric_num]] = stat['metrics'][metric_num]
                    stat['metrics'] = metr
                    stat['segment']=seg_label
                    stats.append(stat)
        if dimensionslist!=dimensionslist_with_segments:
            for stat_num in range(len(stats)-1,-1,-1):
                if stats[stat_num]['segment']=='Все данные':
                    k=0
                    without_seg=stats[stat_num].copy()
                    without_seg.pop('metrics')
                    without_seg.pop('segment')
                    without_seg=without_seg
                    for i in stats:
                        compare_seg = i.copy()
                        compare_seg.pop('metrics')
                        compare_seg.pop('segment')
                        if without_seg==compare_seg:
                            k+=1
                    if k==1:
                        stats.pop(stat_num)

        return stats
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
                    try:#если значение в подфильтре целочисленное, то не добавляем кавычки
                        int(sub_str.partition(j)[2])
                        json.loads(get_clickhouse_data('SELECT {par}=={val} FROM CHdatabase.visits ALL INNER JOIN CHdatabase.hits USING idVisit LIMIT 1 FORMAT JSON'.format(par=sub_str.partition(j)[0],val=sub_str.partition(j)[2]), 'http://85.143.172.199:8123'))
                        sub_str = sub_str.partition(j)[0] + j +sub_str.partition(j)[2]
                    except:
                        if sub_str.partition(j)[0] in ['day_of_week_code','month_code',"year","minute","second"]:
                            sub_str = sub_str.partition(j)[0] + j + sub_str.partition(j)[2]
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
        return end_filt.replace('date','toDate(serverTimestamp)').replace('month_code','toMonth(toDate(serverTimestamp))').replace('day_of_week_code',"toDayOfWeek(toDate(serverTimestamp))")\
                .replace('day_of_week',"dictGetString('week','{lang}',toUInt64(toDayOfWeek(toDate(serverTimestamp))))".format(
                        lang=lang))\
            .replace('year',"toYear(toDate(serverTimestamp))").replace('minute',"toMinute(toDateTime(serverTimestamp))").replace('second',"toSecond(toDateTime(serverTimestamp))")\
            .replace('month',"dictGetString('month','{lang}',toUInt64(toMonth(toDate(serverTimestamp))))".format(lang=lang))
    if request.method=='POST':
        #Заголовки для запроса сегментов
        headers = {
            'Authorization': 'eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJ1c2VyX2lkIjoxOSwiZW1haWwiOiIiLCJ1c2VybmFtZSI6ImFydHVyIiwiZXhwIjoxNTE4MTIxNDIyfQ._V0PYXMrE2pJlHlkMtZ_c-_p0y0MIKsv8o5jzR5llpY',
            'Content-Type': 'application/json'}
        # Парсинг json
        try:
            sort_order=json.loads(request.body.decode('utf-8'))['sort_order']
        except:
            sort_order=""
        #сортировка по переданному показателю

        dimensionslist_with_segments=json.loads(request.body.decode('utf-8'))['dimensions']
        try:
            sort_column = json.loads(request.body.decode('utf-8'))['sort_column']
        except:
            sort_column=dimensionslist_with_segments[0]
        try:
            lang = json.loads(request.body.decode('utf-8'))['lang']
            if lang == "":
                lang='ru'
            if lang == "en":
                lang='eng'
        except:
            lang = "ru"
        dimensionslist = []
        dimensionslist_with_aliases=[]
        #Создание списка параметров без сегментов
        time_dimensions_dict = {}
        for d in dimensionslist_with_segments:
            if 'segment' not in d and d != list:
                dimensionslist.append(d)
                if d == 'month':
                    time_dimensions_dict[
                        d] = "dictGetString('month','{lang}',toUInt64(toMonth(toDate(serverTimestamp))))".format(
                        lang=lang)
                    dimensionslist_with_aliases.append(
                        "dictGetString('month','{lang}',toUInt64(toMonth(toDate(serverTimestamp)))) as month".format(
                            lang=lang))
                    continue
                if d == 'second':
                    time_dimensions_dict[d] = "toSecond(toDateTime(serverTimestamp))"
                    dimensionslist_with_aliases.append("toSecond(toDateTime(serverTimestamp)) as second")
                    continue
                if d == 'minute':
                    time_dimensions_dict[d] = "toMinute(toDateTime(serverTimestamp))"
                    dimensionslist_with_aliases.append("toMinute(toDateTime(serverTimestamp)) as minute")
                    continue
                if d == 'year':
                    time_dimensions_dict[d] = "toYear(toDate(serverTimestamp))"
                    dimensionslist_with_aliases.append("toYear(toDate(serverTimestamp)) as year")
                    continue
                if d == 'day_of_week_code':
                    time_dimensions_dict[d] = "toDayOfWeek(toDate(serverTimestamp))"
                    dimensionslist_with_aliases.append(
                        "toDayOfWeek(toDate(serverTimestamp)) as day_of_week_code")
                    continue
                if d == 'month_code':
                    time_dimensions_dict[d] = "toMonth(toDate(serverTimestamp))"
                    dimensionslist_with_aliases.append(
                        "toMonth(toDate(serverTimestamp)) as month_code")
                    continue
                if d == 'date':
                    time_dimensions_dict[d] = "toDate(serverTimestamp)"
                    dimensionslist_with_aliases.append(
                        "toDate(serverTimestamp) as date")
                    continue
                if d == 'day_of_week':
                    time_dimensions_dict[
                        d] = "dictGetString('week','{lang}',toUInt64(toDayOfWeek(toDate(serverTimestamp))))".format(
                        lang=lang)
                    dimensionslist_with_aliases.append(
                        "dictGetString('week','{lang}',toUInt64(toDayOfWeek(toDate(serverTimestamp)))) as day_of_week".format(
                            lang=lang))
                    continue
                dimensionslist_with_aliases.append(d)

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

        date1 = json.loads(request.body.decode('utf-8'))['date1']
        date2= json.loads(request.body.decode('utf-8'))['date2']
        try:
            filter = json.loads(request.body.decode('utf-8'))['filter']
            if filter != "":
                filt = "AND"+"("+FilterParse(filter)+")"
            else:
                filt=""
        except:
            filt=""
        try:
            filter_metric = json.loads(request.body.decode('utf-8'))['filter_metric']
        except:
            having=" "
        else:
            having = 'HAVING'+' '+FilterParse(filter_metric)
        #если список dimensionslist пуст, значит были переданы только сегменты
        try:
            profile_id=json.loads(request.body.decode('utf-8'))['profile_id']
            try:
                #Определяем относительное время(с учетом часового пояса)
                timezone = json.loads(requests.get(
                    'https://s.analitika.online/api/profiles/{profile_id}/?all=1'.format(profile_id=profile_id),
                    headers=headers).content.decode('utf-8'))['timezone']
                print(timezone)
                d1 = datetime.strptime(date1 + '-00', '%Y-%m-%d-%H')
                timezone = pytz.timezone(timezone)
                d1 = timezone.localize(d1)
                time_offset = str(d1)[19:]

                if time_offset[0] == '+':
                    relative_date1=str(datetime.strptime(date1 + '-00', '%Y-%m-%d-%H') - timedelta(
                                hours=int(time_offset[2]) - 3))
                    relative_date2=str(datetime.strptime(date2 + '-00', '%Y-%m-%d-%H') - timedelta(
                                hours=int(time_offset[2]) - 3))
                else:
                    relative_date1 = str(datetime.strptime(date1 + '-00', '%Y-%m-%d-%H') - timedelta(
                        hours=-int(time_offset[2]) - 3))
                    relative_date2 = str(datetime.strptime(date2 + '-00', '%Y-%m-%d-%H') - timedelta(
                        hours=-int(time_offset[2]) - 3))

                date_field='toDateTime(serverTimestamp)'
            except:
                relative_date1=date1
                relative_date2 = date2
                date_field = 'serverDate'
            try:
                if json.loads(get_clickhouse_data(
                        'SELECT idSite FROM CHdatabase.hits_with_visits WHERE idSite={idSite} FORMAT JSON'.format(
                                idSite=json.loads(requests.get(
                                        'https://s.analitika.online/api/profiles/{profile_id}/?all=1'.format(
                                                profile_id=profile_id), headers=headers).content.decode('utf-8'))[
                                    'site_db_id']), 'http://85.143.172.199:8123'))['data'] == []:
                    filt = 'AND 0'
                else:
                    filt = filt + 'AND idSite==' + str(json.loads(requests.get(
                        'https://s.analitika.online/api/profiles/{profile_id}/?all=1'.format(profile_id=profile_id),
                        headers=headers).content.decode('utf-8'))['site_db_id'])
            except:
                filt = 'AND 0'
        except:
            pass
            relative_date1=date1
            relative_date2= date2
            date_field = 'serverDate'

        table = 'CHdatabase.hits_with_visits'
        list_of_adstat_par=['Clicks','Impressions','Cost','StatDate','idSite', 'AdCampaignId', 'AdBannerId', 'AdChannelId', 'AdDeviceType', 'AdGroupId', 'AdKeywordId',
                       'AdPosition', 'AdPositionType', 'AdRegionId', 'AdRetargetindId', 'AdPlacement', 'AdTargetId', 'AdvertisingSystem', 'DRF', 'campaignContent',
                       'campaignKeyword', 'campaignMedium', 'campaignName', 'campaignSource']
        is_ad=False
        # Выбираем таблицу
        for metr in metrics:
            if metr in ['cost', 'impressions', 'clicks']:
                table = "CHdatabase.hits_with_visits ALL LEFT JOIN CHdatabase.adstat USING {dimension}"
                is_ad = True
                break
        for dim in dimensionslist:
            if dim in ['Cost', 'Impressions', 'Clicks', 'StatDate']:
                table = "CHdatabase.hits_with_visits ALL LEFT JOIN CHdatabase.adstat USING {dimension}"
                is_ad = True
                break



        #Формируем массив с count() для каждого параметра
        dimension_counts=[]
        for i in dimensionslist:
            if i in time_dimensions_dict.keys():
                dimension_counts.append("CAST(uniq({dimension}),'Int') as h{dimension_alias}".format(dimension=time_dimensions_dict[i],dimension_alias=i))
            else:
                dimension_counts.append("CAST(uniq({dimension}),'Int') as h{dimension}".format(dimension=i))


        # ФОрмируем массив с запросом каждого показателя в SQL
        metric_counts_list = MetricCounts(metrics, headers)
        metric_counts = ','.join(metric_counts_list)
        #Добавляем в выходной словарь параметр counts
        resp={}#Выходной словарь
        resp['counts'] = {}
        resp['counts']=AddCounts(relative_date1,relative_date2,dimension_counts,filt,sort_order,table)

        # Добавляем в выходной словарь параметр metric_sums
        resp['metric_sums']={}
        resp['metric_sums']['dates'] = AddMetricSums(date1,date2,metric_counts_list,filt,metrics,sort_order,table)
        stats=AddStats2(dimensionslist,dimensionslist_with_aliases,metric_counts,filt,limit,having,relative_date1,relative_date2,metrics,table)
        # Добавляем stats
        resp['stats']=stats
        pprint.pprint(resp)
        response=JsonResponse(resp,safe=False,)
        response['Access-Control-Allow-Origin']='*'
        return response


