from django.shortcuts import render
from django.shortcuts import render_to_response,redirect
from django.template.context_processors import csrf
import json
import os.path
from uuid import uuid4
import os
from django.http.response import HttpResponse
from wsgiref.util import FileWrapper
from Word2Vec.settings import BASE_DIR,MEDIA_ROOT
import pandas
import requests
from django.http import JsonResponse
from django.views.decorators.csrf import csrf_exempt
import pprint
import re
from pandas import ExcelWriter
from pandas import ExcelFile
import copy
from datetime import datetime, timedelta
import pytz
def get_all_metrics():
    all_metrics=[]
    return all_metrics
def get_all_dimensions():
    #Дописать
    all_dimensions=['visitorId','idVisit','deviceModel','serverDate','referrerTypeName',
                    'referrerUrl','deviceType','languageCode','language','deviceBrand','operatingSystemName',
                    'visitorType','country','AdCampaignId','AdKeywordId','AdBannerId','AdGroupId',
                    'AdPositionType','AdPosition','AdRegionId','AdRetargetindId','AdTargetId','DRF',
                    'AdPlacement','AdDeviceType','browserName','browserFamily','operatingSystemVersion',
                    'browserVersion','visitServerHour','city','day_of_week','day_of_week_code','month','week',
                    'quarter','month_code','year','minute','second','campaignSource','campaignMedium',
                    'campaignKeyword','campaignContent','visitLocalHour','campaignName','provider','resolution','hour','week_period','quarter_period','month_period']
    return all_dimensions
def get_adstat_dimensions():
    #Дописать
    all_dimensions=['idSite','AdCampaignId','AdBannerId','AdChannelId','AdDeviceType','AdGroupId','AdKeywordId','AdPosition','AdPositionType',
                    'AdRegionId','AdRetargetindId','AdPlacement','AdTargetId','AdvertisingSystem','DRF','campaignContent','campaignKeyword','campaignMedium',
                    'campaignName','campaignSource','StatDate','Impressions','Clicks','Cost','month','month_period','quarter','quarter_period','week','week_period','second',
                    'minute','hour','year','day_of_week_code','date','day_of_week']
    return all_dimensions
def negative_condition(condition):
    negatives = {
        '==': '!=',
        '=@': '!@',
        '>': '<',
        '>=': '<=',
        '=^': '!^',
        '=$': '!$',
        '=~': '!~',

        '!=': '==',
        '!@': '=@',
        '<': '>',
        '<=': '>=',
        '!~': '=~',
        '!^': '=^',
        '!$': '=$'
    }
    if condition in negatives:
        return negatives[condition]

    return condition
def get_query_for_metric_name(metric):
    metrics_query_dict={'conversion_rate':"if(uniq(idVisit)=0,0,floor(sum(Type='goal')*100/uniq(idVisit),2)) as conversion_rate",
                        'nb_new_visitors_per_all_visitors':"if(uniq(visitorId)=0,0,floor(uniqIf(visitorId,visitorType='new')*100/uniq(visitorId),2)) as nb_new_visitors_per_all_visitors",
                        'nb_return_visitors_per_all_visitors':"if(uniq(visitorId)=0,0,floor(CAST(uniq(visitorId)-uniqIf(visitorId,visitorType='new'),'Int')*100/uniq(visitorId),2)) as nb_return_visitors_per_all_visitors",
                        'nb_visits_with_searches':"CAST(countIf(searches>0),'Int') as nb_visits_with_searches",
                        'nb_searches_visits_per_all_visits':"if(uniq(idVisit)=0,0,floor(countIf(searches>0)*100/uniq(idVisit),2)) as nb_searches_visits_per_all_visits",
                        'nb_searches':"CAST(sum(searches),'Int') as nb_searches",
                        'nb_downloas_per_visit':"if(uniq(idVisit)=0,0,floor(sum(Type='download')/uniq(idVisit),2)) as nb_downloas_per_visit",
                        'avg_visit_length':"if(uniq(idVisit)=0,0,floor(sum(visitDuration)/uniq(idVisit),2)) as avg_visit_length",
                        'nb_return_visitors':"CAST(uniq(visitorId)-uniqIf(visitorId,visitorType='new'),'Int') as nb_return_visitors",
                        'nb_new_visits_per_all_visits':"if(uniq(idVisit)=0,0,floor(uniqIf(idVisit,visitorType='new')*100/uniq(idVisit),2)) as nb_new_visits_per_all_visits",
                        'nb_new_visits':"CAST(uniqIf(idVisit,visitorType='new'),'Int') as nb_new_visits",
                        'nb_new_visitors':"CAST(uniqIf(visitorId,visitorType='new'),'Int') as nb_new_visitors",
                        'nb_actions_per_visit':"if(uniq(idVisit)=0,0,floor(count(*)/uniq(idVisit),2)) as nb_actions_per_visi",
                        'nb_pageviews_per_visit':"floor(sum(Type='action')/uniq(idVisit),2) as nb_pageviews_per_visit",
                        'avg_time_generation':"floor(avg(generationTimeMilliseconds)/1000,2) as avg_time_generation",
                        'nb_downloads':"CAST(sum(Type='download'),'Int') as nb_downloads",
                        'nb_conversions':"CAST(sum(Type='goal'),'Int') as nb_conversions",
                        'nb_pageviews':"CAST(sum(Type='action'),'Int') as nb_pageviews",
                        'bounce_rate':"if(uniq(idVisit)=0,0,floor((uniqIf(idVisit,visitDuration=0)*100/uniq(idVisit)),2)) as bounce_rate",
                        'bounce_count':"CAST(uniqIf(idVisit,visitDuration=0),'Int') as bounce_count",
                        'nb_actions':"CAST(count(*),'Int') as nb_actions",
                        'nb_visitors':"CAST(uniq(visitorId),'Int') as nb_visitors",
                        'nb_visits':"CAST(uniq(idVisit),'Int') as nb_visits"
                        }

    return metrics_query_dict[metric]
def get_time_dimensions(key):
    all_time_dimensions = {'month': "dictGetString('month','{lang}',toUInt64(toMonth({date_field})))",
                           'month_period': "concat(toString(toYear({date_field})),concat('-',toString(toMonth({date_field}))))",
                           'quarter': "toQuarter({date_field})",
                           'quarter_period': "concat(toString(toYear({date_field})),concat('-',toString(toQuarter({date_field}))))",
                           'week': "toRelativeWeekNum({date_field}) - toRelativeWeekNum(toStartOfYear({date_field}))",
                           'week_period': "concat(toString(toMonday({date_field})),concat(' ',toString(toMonday({date_field}) +6)))",
                           'second': "toSecond(toDateTime({date_field}))",
                           'minute': "toMinute(toDateTime({date_field}))",
                           'hour': "toHour(toDateTime({date_field}))",
                           'year': "toYear({date_field})",
                           'day_of_week_code': "toDayOfWeek({date_field})",
                           'date': "{date_field}",
                           'day_of_week': "dictGetString('week','{lang}',toUInt64(toDayOfWeek({date_field})))"
                           }
    return all_time_dimensions[key]
def MetricCounts(metrics, headers,dimensionslist,is_all_segments,attribution_model):
    metric_counts = []
    ad_metric_counts = []
    for i in metrics:
        try:
            metric_counts.append(get_query_for_metric_name(i))
        except:
            pass
        if 'calculated_metric' in i:
            calc_metr = json.loads(requests.get(
                'https://s.analitika.online/api/reference/calculated_metrics/{num}/?all=1'.format(
                    num=int(i[17:])),
                headers=headers).content.decode('utf-8'))['definition']

            #Проверяем есть ли в составе calc_metr показатели дл рекламной статистики, если есть то меняем таблицу, из которой будут браться данные
            calc_metr = calc_metr.replace('impressions', 'sum(Impressions}').replace('nb_actions_per_visit',"if(uniq(idVisit)=0,0,count(*)/uniq(idVisit))")\
                    .replace('nb_downloas_per_visit',"if(uniq(idVisit)=0,0,sum(Type='download')/uniq(idVisit))").replace('cost', 'sum(Cost)')\
                    .replace('clicks', "sum(Clicks)").replace('nb_visits_with_searches',"countIf(searches>0)").replace('nb_visits', "uniq(idVisit)").replace('nb_actions',"count(*)")\
                    .replace('nb_visitors', "uniq(visitorId)").replace('bounce_count',"uniqIf(idVisit,visitDuration=0)").replace('bounce_rate','if(uniq(idVisit)=0,0,(uniqIf(idVisit,visitDuration=0)*100/uniq(idVisit)))')\
                    .replace('nb_pageviews',"sum(Type='action')").replace('nb_conversions',"sum(Type='goal')").replace('nb_downloads',"sum(Type='download')")\
                    .replace('avg_time_generation',"avg(generationTimeMilliseconds)/1000").replace('ctr',"if(sum(impressions)=0,0,(sum(clicks)/sum(impressions))*100)")\
                    .replace('nb_pageviews_per_visit',"sum(Type='action')/uniq(idVisit)").replace('nb_new_visitors_per_all_visitors',"if(uniq(visitorId)=0,0,uniqIf(visitorId,visitorType='new')*100/uniq(visitorId))")\
                    .replace('nb_new_visitors',"uniqIf(visitorId,visitorType='new')").replace('nb_new_visits_per_all_visits',"if(uniq(idVisit)=0,0,uniqIf(idVisit,visitorType='new')*100/uniq(idVisit))")\
                    .replace('nb_new_visits',"uniqIf(idVisit,visitorType='new')").replace('nb_return_visitors_per_all_visitors',"if(uniq(visitorId)=0,0,uniqIf(visitorId,visitorType='returning')*100/uniq(visitorId))")\
                    .replace('nb_return_visitors',"uniq(visitorId)-uniqIf(visitorId,visitorType='new')")\
                    .replace('avg_visit_length',"if(uniq(idVisit)=0,0,sum(visitDuration)/uniq(idVisit))").replace('nb_searches_visits_per_all_visits',"if(uniq(idVisit)=0,0,countIf(searches>0)*100/uniq(idVisit))")\
                    .replace('nb_searches',"sum(searches)").replace('conversion_rate',"if(uniq(idVisit)=0,0,sum(Type='goal')*100/uniq(idVisit))")

            goal_conversions = re.findall(r'goal\d{1,3}_conversion', calc_metr)
            for goal_conversion in goal_conversions:
                calc_metr = calc_metr.replace(goal_conversion,
                                              "floor((sum(Type='goal' and goalId={N})/uniq(idVisit))*100,2)".format(
                                                  N=goal_conversion.partition("_conversion")[0][4:]))
            goals = re.findall(r'goal\d{1,3}_{0}', calc_metr)
            for goal in goals:
                calc_metr = calc_metr.replace(goal, "sum(Type='goal' AND goalId={N})".format(N=goal[4:]))
            metric_counts.append('if('+calc_metr+'==inf,0,floor(' + calc_metr + ',2))' + ' as calculated_metric{N}'.format(N=int(i[17:])))
        #Добавление показателей, связанных с целями
        if 'goal' in i:
            if 'goalgroup' in i:
                try:
                 #Добавляем группы целей
                    query=[] # список с (sum(Type='goal' and goalId={N}) для всех целей, что есть в goals в апи
                    for goal in json.loads(requests.get(
                        'https://s.analitika.online/api/reference/goal_groups/{id}?all=1'.format(id=i.partition("_conversion")[0][9:]),
                            headers=headers).content.decode('utf-8'))['goals_code']:
                        query.append("CAST(sum(Type='goal' AND goalId={N}),'Int')".format(N=goal[4:]))
                    query='+'.join(query) #строка запроса с суммой goalN
                    #если в показателе есть _conversion, то вычисляем относительный показатель(делим на колчество визитов)
                    if '_conversion' in i:
                        metric_counts.append('if(uniq(idVisit)=0,0,floor(('+query+')*100/uniq(idVisit),2))  as goalgroup{N}_conversion'.format(
                        N=i.partition("_conversion")[0][9:]))
                    elif '_cost' in i :
                        ad_metric_counts.append('floor(sum(Cost)/'+query+',2) as goalgroup{N}_cost'.format(N=i[9:]))
                    else:
                        metric_counts.append(query + ' as goalgroup{N}'.format(N=i[9:]))
                except:
                    continue
            elif '_conversion' in i:
                metric_counts.append(
                    " if(uniq(idVisit)=0,0,floor((sum(Type='goal' and goalId={N})/uniq(idVisit))*100,2)) as goal{N}_conversion".format(
                        N=i.partition("_conversion")[0][4:]))
            else:
                if 'cost' in i:
                    ad_metric_counts.append("floor(sum(Cost)/sum(Type='goal' AND goalId={N}),2) as goal{N}_cost".format(N=i[4:]))
                else:
                    metric_counts.append("CAST(sum(Type='goal' AND goalId={N}),'Int') as goal{N}".format(N=i[4:]))
        if i in ['clicks', 'cost', 'impressions','ctr']:
            metr="CAST(sum({Metric}),'Int') as {metric}".format(metric=i,Metric=i.capitalize())
            if i=='ctr':
                metr="if(sum(Impressions)=0, 0, floor((sum(Clicks) / sum(Impressions)) * 100, 2)) as {metric}".format(metric=i)
            # Если в dimensions переданы только сегменты или параметры, которых нет в adstat, то меняем таблицу и записываем нули в рекламные показатели
            flag=1
            if is_all_segments == False:
                for dim in dimensionslist:
                    if dim not in get_adstat_dimensions():
                        metric_counts.append("0 as {metric}".format(metric=i))
                        flag=0
                        break
            if flag==1:
                ad_metric_counts.append(metr)
    return metric_counts,ad_metric_counts

@csrf_exempt
def CHapi(request):
    def ChangeName(filename):
        ext = filename.split('.')[-1]
        # get filename
        filename = '{}.{}'.format(uuid4().hex, ext)
        # return the whole path to the file
        return filename
    def ToExcel(stats):
        xls_stats=[]
        try:
            #определяем порядок столбцов(первым идет dimension)
            col_names=[]
            col_names.append(json.loads(requests.get(
                'https://s.analitika.online/api/reference/dimensions/?code={dimension}'.format(dimension=dimensionslist[0]),
                headers=headers).content.decode('utf-8'))['results'][0]['name'])
            for metr in metrics:
                col_names.append(json.loads(requests.get(
                'https://s.analitika.online/api/reference/metrics/?code={metric}'.format(metric=metr),
                headers=headers).content.decode('utf-8'))['results'][0]['name'])
            #Добавляем строку с тоталами
            totals={}
            totals[col_names[0]]='\tИТОГО'
            for metr in metrics:
                totals[json.loads(requests.get(
                'https://s.analitika.online/api/reference/metrics/?code={metric}'.format(metric=metr),
                headers=headers).content.decode('utf-8'))['results'][0]['name']]=str(total_filtered[metr])+'(всего '+str(total[metr])+')'
            xls_stats.append(totals)
            for dic in stats:
                xls_dic={}
                xls_dic[json.loads(requests.get(
                'https://s.analitika.online/api/reference/dimensions/?code={dimension}'.format(dimension=dimensionslist[0]),
                headers=headers).content.decode('utf-8'))['results'][0]['name']]=dic[dimensionslist[0]]
                for metr in metrics:
                    xls_dic[json.loads(requests.get(
                'https://s.analitika.online/api/reference/metrics/?code={metric}'.format(metric=metr),
                headers=headers).content.decode('utf-8'))['results'][0]['name']]=dic[metr]
                xls_stats.append(xls_dic)
            df = pandas.DataFrame(xls_stats)
            df = df[col_names]
        except:
            xls_stats=[]
            xls_dict={json.loads(requests.get(
                'https://s.analitika.online/api/reference/dimensions/?code={dimension}'.format(dimension=dimensionslist[0]),
                headers=headers).content.decode('utf-8'))['results'][0]['name']:''}
            for metr in metrics:
                xls_dict[json.loads(requests.get(
                'https://s.analitika.online/api/reference/metrics/?code={metric}'.format(metric=metr),
                headers=headers).content.decode('utf-8'))['results'][0]['name']]=''
            xls_stats.append(xls_dict)
            df = pandas.DataFrame(xls_stats)
        excel_name=ChangeName('stat.xlsx')
        writer = ExcelWriter(os.path.join(MEDIA_ROOT, excel_name))
        df.to_excel(writer, 'Sheet1', index=False)
        writer.save()
        return excel_name
    def datesdicts(array_dates, dim,dim_with_alias,ad_dim_with_alias,table,date_filt,updm,group_by):
        q_all = '''SELECT {dimension_with_alias} FROM {table}
                                           WHERE 1 {filt} AND {site_filt} AND {date_filt} AND {updm}
                                           GROUP BY {group_by}
                                           '''
        if is_two_tables:
            q_all="SELECT {dimension} FROM ("+q_all+""") ALL FULL OUTER JOIN (SELECT {ad_dimension_with_alias} FROM CHdatabase.adstat
                            WHERE 1 {filt} AND {site_filt} AND {ad_date_filt} AND {updm} GROUP BY {group_by}) USING {dimension}"""
        q_all=q_all.format(dimension_with_alias=dim_with_alias,ad_dimension_with_alias=ad_dim_with_alias,dimension=dim,updm=updm,date_filt=date_filt,
                        site_filt=site_filt, filt=filt,group_by=group_by,ad_date_filt=ad_date_filt,table=table)

        all_labeldicts = json.loads(get_clickhouse_data(q_all+' {limit} FORMAT JSON'.format(limit=limit), 'http://46.4.81.36:8123'))['data']
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
    def RecStats(n,i,updimensions,table,up_dim_info):
        """Рекурсивный метод для добавления вложенных структур в stats"""
        #Добавляем фильтры
        try:
            print(updimensions)
            updimensions.pop(n)
        except:
            pass

        #Добавляем параметры в список с параметрами верхних уровней
        #Если предыдущий уровень-сегмент, не добавляем фильтр
        try:
            if '_path' in dimensionslist_with_segments[n]:
                updimensions.append('visitorId IN {list_of_id}'.format(list_of_id=i['visitorId']))
            else:
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
            seg = json.loads(requests.get(
                'https://s.analitika.online/api/reference/segments/{num_seg}/?all=1'.format(num_seg=int(dimensionslist_with_segments[n+1][num][7:])),
                headers=headers).content.decode('utf-8'))['real_definition']
            seg_filt = seg.partition("==")[0] + "=='" + seg.partition("==")[2] + "'"
            seg_label = json.loads(requests.get(
                'https://s.analitika.online/api/reference/segments/{num_seg}/?all=1'.format(num_seg=int(dimensionslist_with_segments[n+1][num][7:])),
                headers=headers).content.decode('utf-8'))['name']

            updm = ' AND '.join(updimensions)
            counter=0
            for date in relative_period:
                q = '''SELECT '{label_val}' as label,'{segment_val}' as segment,{metric_counts} FROM {table}
                                       WHERE 1 {filt} AND {site_filt} AND {date_field} BETWEEN '{date1}' AND '{date2}' AND {seg_filt} AND {updm}
                                                                                                    '''
                if is_two_tables:
                    q = "SELECT label,segment,{metrics} FROM (" + q + """) ALL FULL OUTER JOIN (SELECT '{label_val}' as label,'{segment_val}' as segment,{ad_metric_counts} FROM CHdatabase.adstat
                                      WHERE 1 {filt} AND {site_filt} AND {ad_date_field} BETWEEN '{date1}' AND '{date2}' AND {seg_filt} AND {updm}) USING label LIMIT 1"""
                q = q.format(label_val=seg_label,
                             site_filt=site_filt,
                             segment_val=seg, updm=updm,
                             seg_filt=FilterParse(seg_filt.replace("'", '')),
                             metric_counts=metric_counts,
                             ad_date_field=ad_date_field,
                             ad_metric_counts=ad_metric_counts,
                             date1=date['date1'], metrics=','.join(metrics),
                             date2=date['date2'], filt=filt,
                             table=table, date_field=date_field)

                array_dates.append(json.loads(get_clickhouse_data(q+' FORMAT JSON', 'http://46.4.81.36:8123'))['data'])
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
                updimensions = updimensions[:n + 1]
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
                    updimensions.append(seg_filt)
                    updimensions.append(seg_filt)
                    stat_dict['sub'] = RecStats(n+1, i, updimensions, table,1)
                sub.append(stat_dict)
                counter+=1
        # Если dimension является сегментом(не группой сегментов а отдельным сегментом):
        if 'segment' in dimensionslist_with_segments[n+1]:
            array_dates = []
            seg = json.loads(requests.get(
                'https://s.analitika.online/api/reference/segments/{num_seg}/?all=1'.format(num_seg=int(dimensionslist_with_segments[n+1][7:])),
                headers=headers).content.decode('utf-8'))['real_definition']
            seg_filt = seg.partition("==")[0] + "=='" + seg.partition("==")[2] + "'"
            seg_label = json.loads(requests.get(
                'https://s.analitika.online/api/reference/segments/{num_seg}/?all=1'.format(num_seg=int(dimensionslist_with_segments[n+1][7:])),
                headers=headers).content.decode('utf-8'))['name']

            updm = ' AND '.join(updimensions)
            for date in relative_period:
                q = '''SELECT '{label_val}' as label,'{segment_val}' as segment,{metric_counts} FROM {table}
                       WHERE 1 {filt} AND {site_filt} AND {date_field} BETWEEN '{date1}' AND '{date2}' AND {seg_filt} AND {updm}
                                                                                    '''
                if is_two_tables:
                    q = "SELECT label,segment,{metrics} FROM (" + q + """) ALL FULL OUTER JOIN (SELECT '{label_val}' as label,'{segment_val}' as segment,{ad_metric_counts} FROM CHdatabase.adstat
                      WHERE 1 {filt} AND {site_filt} AND {ad_date_field} BETWEEN '{date1}' AND '{date2}' AND {seg_filt} AND {updm}) USING label LIMIT 1"""
                q = q.format(label_val=seg_label,
                             site_filt=site_filt,
                             segment_val=seg,updm=updm,
                             seg_filt=FilterParse(seg_filt.replace("'", '')),
                             metric_counts=metric_counts,
                             ad_date_field=ad_date_field,
                             ad_metric_counts=ad_metric_counts,
                             date1=date['date1'], metrics=','.join(metrics),
                             date2=date['date2'], filt=filt,
                             table=table, date_field=date_field)
                print(q)
                array_dates.append(json.loads(get_clickhouse_data(q+" FORMAT JSON", 'http://46.4.81.36:8123'))['data'])
                if array_dates[0]==[]:
                    empty_dict={'label':seg_label,
                                           'segment':seg}
                    for metric in metrics:
                        empty_dict[metric]=0
                    array_dates[0].append(empty_dict)
            counter=0
            for i in array_dates[0]:
                updimensions = updimensions[:n + 1]
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
                    updimensions.append(FilterParse(seg_filt.replace("'", '')))
                    updimensions.append(FilterParse(seg_filt.replace("'", '')))
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

            group_by = dimensionslist_with_segments[n+1]
            if '_path' in dimensionslist_with_segments[n+1]:
                group_by = 'visitorId'
            if attribution_model == 'first_interaction':
                for date in relative_period:
                    date0 = (datetime.strptime(date['date1'], time_format) - timedelta(days=int(attribution_lookup_period))).strftime(time_format)
                    q = '''SELECT alias as {dimension_without_aliases},{sum_metric_string} FROM (SELECT visitorId,any({dimension_without_aliases}) as alias FROM {table}
                                        WHERE  visitorId IN (SELECT visitorId FROM CHdatabase.hits_with_visits WHERE 1 {filt} AND {site_filt} AND {updimensions} AND {date_field} BETWEEN '{date1}' AND '{date2}')
                                        AND {date_field} BETWEEN '{date0}' AND '{date2}' {filt} AND {updimensions} GROUP BY visitorId)
                                        ALL INNER JOIN
                                        (SELECT {metric_counts},visitorId FROM {table} WHERE 1 {filt} AND {updimensions} AND {site_filt} AND {date_field} BETWEEN '{date1}' AND '{date2}' GROUP BY visitorId)
                                        USING visitorId GROUP BY {group_by}'''
                    if is_two_tables:
                        q = "SELECT {dimension},{metrics} FROM (" + q + """) ALL FULL OUTER JOIN (SELECT {ad_dimension_with_alias},{ad_metric_counts} FROM CHdatabase.adstat
                                                                    WHERE 1 {filt} AND {site_filt} AND {updimensions} AND {ad_date_field}  BETWEEN '{date1}' AND '{date2}' GROUP BY {group_by}) USING {dimension}"""
                    q = q.format(table=table, dimension_with_alias=dimensionslist_with_segments_and_aliases[n+1],ad_dimension_with_alias=ad_dimensionslist_with_segments_and_aliases[n+1], dimension=dimension,
                                 sum_metric_string=sum_metric_string, date0=str(date0),updimensions=updm,
                                 metric_counts=metric_counts, date1=date['date1'], ad_metric_counts=ad_metric_counts,
                                 site_filt=site_filt, date2=date['date2'], filt=filt, group_by=group_by,
                                 dimension_without_aliases=list_with_time_dimensions_without_aliases[n+1],
                                 date_field=date_field, ad_date_field=ad_date_field, metrics=','.join(metrics))

                    array_dates.append(json.loads(get_clickhouse_data(q + ' ORDER BY {sort_column} {sort_order} {limit} FORMAT JSON'.format(
                            sort_column=sort_column_in_query, sort_order=sort_order, limit=limit),'http://46.4.81.36:8123'))['data'])
            elif attribution_model == 'last_non-direct_interaction':
                for date in relative_period:
                    q = '''SELECT alias as {dimension_without_aliases},{sum_metric_string}
                               FROM (SELECT visitorId,any({dimension_without_aliases}) as alias FROM {table}
                               WHERE  visitorId IN (SELECT visitorId FROM {table} WHERE 1 {filt} AND {site_filt} AND {updimensions} AND {date_field} BETWEEN '{date1}' AND '{date2}')
                               AND {date_field} < '{date2}' AND referrerType!='direct' {filt} AND {updimensions} GROUP BY visitorId)
                               ALL INNER JOIN
                               (SELECT {metric_counts},visitorId,{dimension_with_alias} FROM {table} WHERE 1 {filt} AND {updimensions} AND {site_filt} AND {date_field} BETWEEN '{date1}' AND '{date2}' GROUP BY {dimension},visitorId)
                               USING visitorId GROUP BY {group_by}'''
                    if is_two_tables:
                        q = "SELECT {dimension},{metrics} FROM (" + q + """) ALL FULL OUTER JOIN (SELECT {ad_dimension_with_alias},{ad_metric_counts} FROM CHdatabase.adstat
                                                                                        WHERE 1 {filt} AND {site_filt} AND {updimensions} AND {ad_date_field}  BETWEEN '{date1}' AND '{date2}' GROUP BY {group_by}) USING {dimension}"""
                    q = q.format(table=table, dimension_with_alias=dimensionslist_with_segments_and_aliases[n + 1],
                                 dimension=dimension,ad_dimension_with_alias=ad_dimensionslist_with_segments_and_aliases[n+1],
                                 sum_metric_string=sum_metric_string, updimensions=updm,
                                 metric_counts=metric_counts, date1=date['date1'], ad_metric_counts=ad_metric_counts,
                                 site_filt=site_filt, date2=date['date2'], filt=filt, group_by=group_by,
                                 dimension_without_aliases=list_with_time_dimensions_without_aliases[n + 1],
                                 date_field=date_field, ad_date_field=ad_date_field, metrics=','.join(metrics))

                    array_dates.append(json.loads(
                        get_clickhouse_data(q + ' ORDER BY {sort_column} {sort_order} {limit} FORMAT JSON'.format(
                            sort_column=sort_column_in_query, sort_order=sort_order, limit=limit),
                                            'http://46.4.81.36:8123'))['data'])
            else:
                for date in relative_period:
                    q = '''SELECT {dimension_with_alias},{metric_counts} FROM {table}
                                                          WHERE 1 {filt} AND {site_filt} AND {updimensions} AND {date_field} BETWEEN '{date1}' AND '{date2}'
                                                          GROUP BY {group_by}'''
                    # Если в запросе к апи были запрошены рекламные показатели или поля, которые есть только в таблице adstat, то объединяем таблицы с хитами и визитами с adstat
                    if is_two_tables:
                        q = "SELECT {dimension},{metrics} FROM (" + q + """) ALL FULL OUTER JOIN (SELECT {ad_dimension_with_alias},{ad_metric_counts} FROM CHdatabase.adstat
                                               WHERE 1 {filt} AND {site_filt} AND {updimensions} AND {ad_date_field} BETWEEN '{date1}' AND '{date2}' GROUP BY {group_by}) USING {dimension}"""
                    q = q.format(table=table, dimension_with_alias=dimensionslist_with_segments_and_aliases[n+1], dimension=dimension,
                                 metric_counts=metric_counts, date1=date['date1'], ad_metric_counts=ad_metric_counts,updimensions=updm,
                                 site_filt=site_filt, date2=date['date2'], filt=filt, group_by=group_by,ad_dimension_with_alias=ad_dimensionslist_with_segments_and_aliases[n+1],
                                 date_field=date_field, ad_date_field=ad_date_field, metrics=','.join(metrics))

                    array_dates.append(json.loads(get_clickhouse_data(q+' ORDER BY {sort_column} {sort_order} FORMAT JSON'
                                                                      .format(sort_column=sort_column_in_query,sort_order=sort_order), 'http://46.4.81.36:8123'))['data'])
            dates_dicts=datesdicts(array_dates,dimensionslist_with_segments[n+1],dimensionslist_with_segments_and_aliases[n+1],ad_dimensionslist_with_segments_and_aliases[n+1],table,date_filt,updm,group_by)
            #возвращаем пусто sub если на данном уровне все показатели равны нулю
            empties = []
            for i in array_dates:
                empty_d = True
                for j in i:
                    if len(list(j.keys())) != 1 and list(j.values()).count(0) != len(list(j.keys())) - 1:
                        empty_d = False
                        break
                empties.append(empty_d)

            if empties.count(True) == len(empties):
                return sub

            for i2 in array_dates[MaxLenNum(array_dates)][:lim]:
                updimensions=updimensions[:n+1]
                stat_dict = {'label': i2[dimensionslist_with_segments[n + 1]],
                             'segment':'{label}=={value}'.format(label=dimensionslist_with_segments[n + 1]
                                                                 ,value=i2[dimensionslist_with_segments[n + 1]])}
                dates = []
                is_all_nulls = True
                for m in range(len(array_dates)):
                    metrics_dict = dict.fromkeys(metrics)
                    for j in metrics_dict:
                        try:
                            metrics_dict[j] = dates_dicts[m][i2[dimensionslist_with_segments[n+1]]][j]
                            is_all_nulls=False
                        except:
                            metrics_dict[j]=0
                    dates.append({'date1': period[m]['date1'], 'date2': period[m]['date2'], 'metrics': metrics_dict})
                if is_all_nulls:
                    continue
                stat_dict['dates'] = dates
                if n != len(dimensionslist_with_segments) - 2:
                    if '_path' in dimensionslist_with_segments[n+1]:
                        updimensions.append('visitorId IN {list_of_id}'.format(list_of_id=i2['visitorId']))
                    else:
                        try:
                            if dimensionslist_with_segments[n+1] in time_dimensions_dict.keys():
                                if type(i2[dimensionslist_with_segments[n+1]]) is int:
                                    updimensions.append("{updimension}={updimension_val}".format(updimension=time_dimensions_dict[dimensionslist_with_segments[n+1]],updimension_val=i2[dimensionslist_with_segments[n+1]]))
                                else:
                                    updimensions.append(
                                        "{updimension}='{updimension_val}'".format(updimension=time_dimensions_dict[dimensionslist_with_segments[n+1]],
                                                                                 updimension_val=i2[dimensionslist_with_segments[n+1]]))
                            else:
                                updimensions.append("{updimension}='{updimension_val}'".format(updimension_val=i2[dimensionslist_with_segments[n+1]],
                                                                                           updimension=dimensionslist_with_segments[n+1]))
                        except:
                            pass

                    up_dim=stat_dict.copy()#Передаем словарь с информацией о вернем уровне "Все файлы"
                    stat_dict['sub'] = RecStats(n + 1, i2, updimensions, table,up_dim)
                sub.append(stat_dict)
        return sub
    def get_clickhouse_data(query,host,connection_timeout=1500):
        """Метод для обращения к базе данных CH"""
        r=requests.post(host,params={'query':query},timeout=connection_timeout)
        return r.text
    def AddCounts(period,dimension_counts,ad_dimension_counts,filt,sort_order,table,date_filt,):
        """Добавление ключа Counts в ответ"""
        a={}
        for dim_num in range(len(dimensionslist)):
            if '_path' in dimensionslist[dim_num]:
                q = """SELECT CAST(uniq(*),'Int') as h{dim} FROM (SELECT toString(groupUniqArray({dim_path}))
                            FROM {table}
                            WHERE 1 {filt} AND {site_filt} AND {date_filt}
                            GROUP BY visitorId
                            FORMAT JSON""".format(dim=dimensionslist[dim_num], dim_path=dimensionslist[dim_num][:-5],
                                                  filt=filt, site_filt=site_filt,
                                                  sort_order=sort_order,
                                                  table=table,
                                                  date_filt=date_filt)
                print(q)
            elif attribution_model=='first_interaction':
                date0 = (datetime.strptime(relative_period[0]['date1'], time_format) - timedelta(days=int(attribution_lookup_period))).strftime(time_format)
                q = '''SELECT CAST(uniq(alias),'Int') as h{dimension}
                                    FROM (SELECT visitorId,any({dimension_without_aliases}) as alias FROM {table}
                                    WHERE  visitorId IN (SELECT visitorId FROM {table} WHERE 1 {filt} AND {site_filt} AND {date_field} BETWEEN '{date1}' AND '{date2}')
                                    AND {date_field} BETWEEN '{date0}' AND '{date2}' {filt} GROUP BY visitorId)
                                    ALL INNER JOIN
                                    (SELECT visitorId FROM {table} WHERE 1 {filt} AND {site_filt}  AND {date_field} BETWEEN '{date1}' AND '{date2}' GROUP BY visitorId)
                                    USING visitorId
                                                       '''.format(dimension_with_alias=dimensionslist_with_segments_and_aliases[dim_num],dimension_without_aliases=list_with_time_dimensions_without_aliases[dim_num],
                                                                  date0=str(date0),dimension_counts=dimension_counts[dim_num],site_filt=site_filt,
                                                                  date1=relative_period[0]['date1'],dimension=dimensionslist[dim_num],
                                                                  date2=relative_period[0]['date2'], filt=filt, sort_order=sort_order,
                                                                  limit=limit,
                                                                  table=table,
                                                                  date_field=date_field)
            elif attribution_model=='last_non-direct_interaction':
                q = '''SELECT CAST(uniq(alias),'Int') as h{dimension}
                                                    FROM (SELECT visitorId,any({dimension_without_aliases}) as alias FROM {table}
                                                    WHERE  visitorId IN (SELECT visitorId FROM {table} WHERE 1 {filt} AND {site_filt} AND {date_field} BETWEEN '{date1}' AND '{date2}')
                                                    AND {date_field} < '{date2}' AND referrerType!='direct' {filt} GROUP BY visitorId)
                                                    ALL INNER JOIN
                                                    (SELECT visitorId,{dimension_with_alias} FROM {table} WHERE 1 {filt}  AND {site_filt} AND {date_field} BETWEEN '{date1}' AND '{date2}' GROUP BY {dimension},visitorId)
                                                    USING visitorId
                                                                       '''.format(
                    dimension_with_alias=dimensionslist_with_segments_and_aliases[dim_num],dimension_without_aliases=list_with_time_dimensions_without_aliases[dim_num],
                    dimension_counts=dimension_counts[dim_num],site_filt=site_filt,
                    date1=relative_period[0]['date1'],dimension=dimensionslist[dim_num],
                    date2=relative_period[0]['date2'], filt=filt, sort_order=sort_order,
                    limit=limit,
                    table=table,
                    date_field=date_field)
                print(q)
            else:
                q = ''' SELECT {dimension_counts}
                                FROM {table}
                                WHERE 1 {filt} AND {site_filt} AND {date_filt}
                               '''
                if is_two_tables:
                    q="""SELECT CAST(uniq({dimension}),'Int') as h{dimension} FROM (SELECT DISTINCT {dimension_with_alias} FROM CHdatabase.hits_with_visits
                            WHERE 1 {filt} AND {site_filt} AND {date_filt}) ALL FULL OUTER JOIN (SELECT DISTINCT {ad_dimension_with_alias} FROM CHdatabase.adstat
                            WHERE 1 {filt} AND {site_filt} AND {ad_date_filt}) USING {dimension}"""
                q=q.format(dimension_counts=dimension_counts[dim_num],dimension=dimensionslist[dim_num],date_filt=date_filt,
                        site_filt=site_filt,dimension_with_alias=dimensionslist_with_segments_and_aliases[dim_num],
                           ad_dimension_with_alias=ad_dimensionslist_with_segments_and_aliases[dim_num], filt=filt,ad_date_filt=ad_date_filt,table=table)
                print(q)
            try:
                a.update(json.loads(get_clickhouse_data(q+' FORMAT JSON', 'http://46.4.81.36:8123'))['data'][0])
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
        total={}
        total_filtered={}
        #разделлим metric_counts для визитов и для рекламной статистики
        for (date,abs_date) in zip(relative_period,period):
            # Запрос на получение сумм показателей без фильтра
            q_total = ''' SELECT {metric_counts}
                    FROM {table}
                    WHERE {date_field} BETWEEN '{date1}' AND '{date2}' AND {site_filt}
                   '''.format(date1=date['date1'], date2=date['date2'], metric_counts=metric_counts,site_filt=site_filt,
                              sort_order=sort_order,table=table,date_field=date_field)
            print(q_total)
            # С фильтром
            q = ''' SELECT {metric_counts}
                                FROM {table}
                                WHERE 1 {filt} AND {site_filt} AND {date_field} BETWEEN '{date1}' AND '{date2}'
                               '''.format(date1=date['date1'], date2=date['date2'], metric_counts=metric_counts,site_filt=site_filt,
                                          filt=filt, sort_order=sort_order,table=table,date_field=date_field)

            #Проверка на существование записей, если их нет, возвращаем нули
            try:
                total = json.loads(get_clickhouse_data(q_total+' FORMAT JSON', 'http://46.4.81.36:8123'))['data'][0]
                total_filtered = json.loads(get_clickhouse_data(q +' FORMAT JSON', 'http://46.4.81.36:8123'))['data'][0]
            except:
                #Если данных нет, запоняе нулями
                ad_metr_dict = {}
                for i in metric_counts_list:
                    ad_metr_dict[i] = 0
                total=ad_metr_dict
                total_filtered=ad_metr_dict
                #Если запрошены две таблицы, объединяем полученные показатели ля hits_with_visits c показателями для adstat
            if is_two_tables:
                ad_q_total=''' SELECT {ad_metric_counts}
                    FROM CHdatabase.adstat
                    WHERE {ad_date_field} BETWEEN '{date1}' AND '{date2}' AND {site_filt}
                   '''.format(date1=date['date1'], date2=date['date2'], ad_metric_counts=ad_metric_counts,site_filt=site_filt,
                              sort_order=sort_order,ad_date_field=ad_date_field)
                print(ad_q_total)
                ad_q = ''' SELECT {ad_metric_counts}
                                    FROM CHdatabase.adstat
                                    WHERE 1 {filt} AND {ad_date_field} BETWEEN '{date1}' AND '{date2}' AND {site_filt}
                                   '''.format(date1=date['date1'], date2=date['date2'],
                                              ad_metric_counts=ad_metric_counts, site_filt=site_filt,
                                              sort_order=sort_order, ad_date_field=ad_date_field,filt=filt)

                try:
                    total.update(json.loads(get_clickhouse_data(ad_q_total+' FORMAT JSON', 'http://46.4.81.36:8123'))['data'][0])
                    total_filtered.update(json.loads(get_clickhouse_data(ad_q+' FORMAT JSON', 'http://46.4.81.36:8123'))['data'][0])
                except:
                    ad_metr_dict={}
                    for i in ad_metric_counts_list:
                        ad_metr_dict[i]=0
                    total.update(ad_metr_dict)
                    total_filtered.update(ad_metr_dict)
                # Создаем словарь, ключи которого это элементы списка metrics
            metric_dict = dict.fromkeys(metrics)
            # Добавляем в словарь даты
            for key in list(metric_dict.keys()):
                sub_metr_dict={}
                try:
                    sub_metr_dict['total_sum']=total[key]
                except:
                    metric_dict[key] = {"total_sum": 0, "sum": 0}
                    continue
                try:
                    sub_metr_dict['sum'] = total_filtered[key]
                except:
                    sub_metr_dict['sum']=0
                metric_dict[key]=sub_metr_dict

            metric_dict.update(abs_date)
            dates.append(metric_dict)
        return total,total_filtered,dates
    def AddMetricSumsWithFilt(relative_period,metric_counts,ad_metric_counts,filt,metrics,sort_order,table):
        #Метод для добавления "Всех данных" для сегмента
        ar_d=[]
        ad_ar_d = []
        for date in relative_period:
            t = '''SELECT {metric_counts} FROM {table}
                    WHERE 1 {filt} AND {site_filt} AND {date_field} BETWEEN '{date1}' AND '{date2}'
                    '''.format(metric_counts=metric_counts,
                date1=date['date1'],site_filt=site_filt,
                date2=date['date2'], filt=filt,metrics=metrics,
                table=table, date_field=date_field)
            if is_two_tables:
                ad_t = """SELECT {ad_metric_counts}
                        FROM CHdatabase.adstat WHERE 1 {filt} AND {site_filt}
                        AND {ad_date_field} BETWEEN '{date1}' AND '{date2}' """.format(metric_counts=metric_counts,
                date1=date['date1'],site_filt=site_filt,date2=date['date2'], filt=filt,metrics=metrics,
                ad_date_field=ad_date_field,ad_metric_counts=ad_metric_counts)
                ad_ar_d.append(json.loads(get_clickhouse_data(ad_t+ ' FORMAT JSON', 'http://46.4.81.36:8123'))['data'])
            ar_d.append(json.loads(get_clickhouse_data(t+ ' FORMAT JSON', 'http://46.4.81.36:8123'))['data'])
        #Объкдиняем данные для разных таблиц, если выбор происходит из нескольких таблиц

        if is_two_tables:
            for (sub,ad_sub) in zip(ar_d,ad_ar_d):
                try:
                    sub[0].update(ad_sub[0])
                except:
                    pass

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
                    try:
                        metrics_dict[j] = ar_d[m][counter][j]
                    except:
                        metrics_dict[j] = 0
                dates.append({'date1': period[m]['date1'], 'date2': period[m]['date2'], 'metrics': metrics_dict})
            st_d['dates'] = dates
            counter+=1
        return st_d
    def AddStats2(dim,dim_with_alias,ad_dim_with_alias,metric_counts, filt, limit, period, metrics, table,date_filt):
        """Добавление ключа stats в ответ"""
        stats = []
        #Определяем, есть ли вначале dimensions группа сегментов
        if type(dim[0])==list:
            stats.append(AddMetricSumsWithFilt(relative_period, metric_counts,ad_metric_counts, filt, metrics, sort_order, table))
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
            stats.append(AddMetricSumsWithFilt(relative_period, metric_counts,ad_metric_counts, filt, metrics, sort_order, table))
            array_dates = []
            seg=json.loads(requests.get('https://s.analitika.online/api/reference/segments/{num_seg}/?all=1'.format(num_seg=int(dim[0][7:])),
                                headers=headers).content.decode('utf-8'))['real_definition']
            seg_filt=seg.partition("==")[0]+"=='"+seg.partition("==")[2]+"'"
            seg_label=json.loads(requests.get('https://s.analitika.online/api/reference/segments/{num_seg}/?all=1'.format(num_seg=int(dim[0][7:])),
                                headers=headers).content.decode('utf-8'))['name']

            for date in relative_period:
                q = '''SELECT '{label_val}' as label,'{segment_val}' as segment,{metric_counts} FROM {table}
                                                                      WHERE 1 {filt} AND {site_filt} AND {date_field} BETWEEN '{date1}' AND '{date2}' AND {seg_filt}
                                                                    '''
                if is_two_tables:
                    q="SELECT label,segment,{metrics} FROM ("+q+""") ALL FULL OUTER JOIN (SELECT '{label_val}' as label,'{segment_val}' as segment,{ad_metric_counts} FROM CHdatabase.adstat
                                                                      WHERE 1 {filt} AND {site_filt} AND {ad_date_field} BETWEEN '{date1}' AND '{date2}' AND {seg_filt}) USING label LIMIT 1"""
                q=q.format(label_val=seg_label,
                    site_filt=site_filt,
                    segment_val=seg,
                    seg_filt=FilterParse(seg_filt.replace("'",'')),
                    metric_counts=metric_counts,
                    ad_date_field=ad_date_field,
                    ad_metric_counts=ad_metric_counts,
                    date1=date['date1'],metrics=','.join(metrics),
                    date2=date['date2'], filt=filt,
                    table=table, date_field=date_field)
                print(q)
                array_dates.append(json.loads(get_clickhouse_data(q+ " FORMAT JSON", 'http://46.4.81.36:8123'))['data'])

            counter=0
            for i in array_dates[0]:
                updimensions = []
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
                    updimensions.append(FilterParse(seg_filt.replace("'",'')))
                    updimensions.append(FilterParse(seg_filt.replace("'",'')))
                    up_dim = stat_dict.copy()
                    stat_dict['sub'] = RecStats(0, i, updimensions, table,up_dim)
                stats.append(stat_dict)
                counter+=1
        elif seg_label_list==[]:
            array_dates = []
            if sort_column=="":
                sort_column_in_query=dim[0]
            else:
                sort_column_in_query=sort_column

            #если указана модель аттрибуции показатели рассчитываются для первого визита
            group_by=dim[0]
            if '_path' in dim[0]:
                group_by='visitorId'
            if attribution_model=='first_interaction':
                for date in relative_period:
                    date0=(datetime.strptime(date['date1'], time_format) - timedelta(days=int(attribution_lookup_period))).strftime(time_format)
                    q = '''SELECT alias as {dimension},{sum_metric_string} FROM (SELECT visitorId,any({dimension_without_aliases}) as alias FROM {table}
                    WHERE  visitorId IN (SELECT visitorId FROM CHdatabase.hits_with_visits WHERE 1 {filt} AND {site_filt} AND {date_field} BETWEEN '{date1}' AND '{date2}')
                    AND {date_field} BETWEEN '{date0}' AND '{date2}' {filt} GROUP BY visitorId)
                    ALL INNER JOIN
                    (SELECT {metric_counts},visitorId FROM {table} WHERE 1 {filt} AND {site_filt} AND {date_field} BETWEEN '{date1}' AND '{date2}' GROUP BY visitorId)
                    USING visitorId GROUP BY {group_by}'''
                    if is_two_tables:
                        q = "SELECT {dimension},{metrics} FROM (" + q + """) ALL FULL OUTER JOIN (SELECT {ad_dimension_with_alias},{ad_metric_counts} FROM CHdatabase.adstat
                                                WHERE 1 {filt} AND {site_filt} AND {ad_date_field} BETWEEN '{date1}' AND '{date2}' GROUP BY {group_by}) USING {dimension}"""
                    q = q.format(table=table, dimension_with_alias=dim_with_alias[0],ad_dimension_with_alias=ad_dim_with_alias[0], dimension=dim[0],
                                 sum_metric_string=sum_metric_string,date0=str(date0),
                                 metric_counts=metric_counts, date1=date['date1'], ad_metric_counts=ad_metric_counts,
                                 site_filt=site_filt, date2=date['date2'], filt=filt, group_by=group_by,
                                 dimension_without_aliases=list_with_time_dimensions_without_aliases[0],
                                 date_field=date_field, ad_date_field=ad_date_field, metrics=','.join(metrics))
                    array_dates.append(json.loads(get_clickhouse_data(q + ' ORDER BY {sort_column} {sort_order} {limit} FORMAT JSON'.format(
                            sort_column=sort_column_in_query, sort_order=sort_order, limit=limit),'http://46.4.81.36:8123'))['data'])

            elif  attribution_model=='last_non-direct_interaction':
                for date in relative_period:
                    q = '''SELECT alias as {dimension},{sum_metric_string}
                    FROM (SELECT visitorId,any({dimension_without_aliases}) as alias FROM {table}
                    WHERE  visitorId IN (SELECT visitorId FROM {table} WHERE 1 {filt} AND {site_filt} AND {date_field} BETWEEN '{date1}' AND '{date2}')
                    AND {date_field} < '{date2}' AND referrerType!='direct' {filt} GROUP BY visitorId)
                    ALL INNER JOIN
                    (SELECT {metric_counts},visitorId, {dimension_with_alias} FROM {table} WHERE 1 {filt} AND {site_filt} AND {date_field} BETWEEN '{date1}' AND '{date2}' GROUP BY visitorId,{group_by})
                    USING visitorId
                    GROUP BY {group_by}'''
                    if is_two_tables:
                        q="SELECT {dimension},{metrics} FROM ("+q+""") ALL FULL OUTER JOIN (SELECT {ad_dimension_with_alias},{ad_metric_counts} FROM CHdatabase.adstat
                            WHERE 1 {filt} AND {site_filt} AND {ad_date_field} BETWEEN '{date1}' AND '{date2}' GROUP BY {group_by}) USING {dimension}"""
                    q = q.format(table=table, dimension_with_alias=dim_with_alias[0],ad_dimension_with_alias=ad_dim_with_alias[0], dimension=dim[0],sum_metric_string=sum_metric_string,
                                 metric_counts=metric_counts, date1=date['date1'], ad_metric_counts=ad_metric_counts,
                                 site_filt=site_filt, date2=date['date2'], filt=filt, group_by=group_by,dimension_without_aliases=list_with_time_dimensions_without_aliases[0],
                                 date_field=date_field, ad_date_field=ad_date_field, metrics=','.join(metrics))
                    print(q+' ORDER BY {sort_column} {sort_order} {limit} FORMAT JSON'.format(sort_column=sort_column_in_query,sort_order=sort_order,limit=limit))
                    array_dates.append(json.loads(get_clickhouse_data(q+' ORDER BY {sort_column} {sort_order} {limit} FORMAT JSON'.format(sort_column=sort_column_in_query,sort_order=sort_order,limit=limit), 'http://46.4.81.36:8123'))['data'])
            else:
                for date in relative_period:
                    q = '''SELECT {dimension_with_alias},{metric_counts} FROM {table}
                                       WHERE 1 {filt} AND {site_filt} AND {date_field} BETWEEN '{date1}' AND '{date2}'
                                       GROUP BY {group_by}'''
                    #Если в запросе к апи были запрошены рекламные показатели или поля, которые есть только в таблице adstat, то объединяем таблицы с хитами и визитами с adstat
                    if is_two_tables:
                        q="SELECT {dimension},{metrics} FROM ("+q+""") ALL FULL OUTER JOIN (SELECT {ad_dimension_with_alias},{ad_metric_counts} FROM CHdatabase.adstat
                            WHERE 1 {filt} AND {site_filt} AND {ad_date_field} BETWEEN '{date1}' AND '{date2}' GROUP BY {group_by}) USING {dimension}"""
                    q=q.format(table=table,dimension_with_alias=dim_with_alias[0],ad_dimension_with_alias=ad_dim_with_alias[0],dimension=dim[0], metric_counts=metric_counts,date1=date['date1'],ad_metric_counts=ad_metric_counts,
                        site_filt=site_filt,date2=date['date2'], filt=filt,group_by=group_by,date_field=date_field,ad_date_field=ad_date_field,metrics=','.join(metrics))

                    #Если текущий параметр содержит в себе _path то изменяе строку запроса
                    if '_path' in dim[0]:
                        q="SELECT {dimension},replaceAll(replaceAll(toString(groupUniqArray(visitorId)),'[','('),']',')') as visitorId,{sum_metric_string} FROM ({q}) GROUP BY {dimension}".format(dimension=dim[0],q=q.replace('SELECT','SELECT visitorId,'),sum_metric_string=sum_metric_string)
                    print(q+' ORDER BY {sort_column} {sort_order} {limit} FORMAT JSON'
                                                                      .format(sort_column=sort_column_in_query,sort_order=sort_order,limit=limit))
                    array_dates.append(json.loads(get_clickhouse_data(q+' ORDER BY {sort_column} {sort_order} {limit} FORMAT JSON'
                                                                      .format(sort_column=sort_column_in_query,sort_order=sort_order,limit=limit), 'http://46.4.81.36:8123'))['data'])

            #если необходимо экспортировать статистику в xlsx
            if export=='xlsx':
                return ToExcel(array_dates[0])
            dates_dicts=datesdicts(array_dates,dim[0],dim_with_alias[0],ad_dim_with_alias[0],table,date_filt,1,group_by)
            #Проверка на выдачу только нулей в показателях. Если тлько нули возвращаем пустой список
            empties=[]
            for i in array_dates:
                empty_d=True
                for j in i:
                    if len(list(j.keys()))!=1 and  list(j.values()).count(0)!=len(list(j.keys()))-1:
                        empty_d=False
                        break
                empties.append(empty_d)
            if empties.count(True)==len(empties):
                return stats
                #определим самый большой список в array_dates
            for i in array_dates[MaxLenNum(array_dates)][:lim]:
                updimensions = []
                if search_pattern.lower() not in str(i[dim[0]]).lower():
                    continue
                stat_dict = {'label': i[dim[0]],
                             'segment': '{label}=={value}'.format(label=dim[0]
                                                                  , value=i[dim[0]])
                             }
                dates = []
                is_all_nulls = True
                for m in range(len(array_dates)):
                    metrics_dict = dict.fromkeys(metrics)

                    for j in metrics_dict:
                        try:
                            metrics_dict[j] = dates_dicts[m][i[dim[0]]][j]
                            is_all_nulls=False
                        except:
                            metrics_dict[j]=0
                    dates.append({'date1': period[m]['date1'], 'date2': period[m]['date2'], 'metrics': metrics_dict})
                #Если все значения показателей во всех временных интерваллах для данного значения параметра равны нулю, то игнорируем это значение параметра
                if is_all_nulls:
                    continue
                stat_dict['dates'] = dates
                if len(dim) > 1:
                    # Добавляем подуровень. Если параметр вычисляемый то подставляем его название из словаря time_dimensions_dict
                    if '_path' in dim[0]:
                        updimensions.append('visitorId IN {list_of_id}'.format(list_of_id=i['visitorId']))
                    else:
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
                    stat_dict['sub'] = RecStats(0, i, updimensions, table,up_dim)
                stats.append(stat_dict)
        #Для групп сегментов
        for num in seg_label_list:
            array_dates = []

            seg = json.loads(requests.get(
                'https://s.analitika.online/api/reference/segments/{num_seg}/?all=1'.format(num_seg=int(num)),
                headers=headers).content.decode('utf-8'))['real_definition']
            seg_filt = seg.partition("==")[0] + "=='" + seg.partition("==")[2] + "'"
            seg_label = json.loads(requests.get(
                'https://s.analitika.online/api/reference/segments/{num_seg}/?all=1'.format(num_seg=int(num)),
                headers=headers).content.decode('utf-8'))['name']
            counter=0
            for date in relative_period:
                q = '''SELECT '{label_val}' as label,'{segment_val}' as segment,{metric_counts} FROM {table}
                                                                                      WHERE 1 {filt} AND {site_filt} AND {date_field} BETWEEN '{date1}' AND '{date2}' AND {seg_filt}
                                                                                    '''
                if is_two_tables:
                    q = "SELECT label,segment,{metrics} FROM (" + q + """) ALL FULL OUTER JOIN (SELECT '{label_val}' as label,'{segment_val}' as segment,{ad_metric_counts} FROM CHdatabase.adstat
                                                                                      WHERE 1 {filt} AND {site_filt} AND {ad_date_field} BETWEEN '{date1}' AND '{date2}' AND {seg_filt}) USING label LIMIT 1"""
                q = q.format(label_val=seg_label,
                             site_filt=site_filt,
                             segment_val=seg,
                             seg_filt=FilterParse(seg_filt.replace("'", '')),
                             metric_counts=metric_counts,
                             ad_date_field=ad_date_field,
                             ad_metric_counts=ad_metric_counts,
                             date1=date['date1'], metrics=','.join(metrics),
                             date2=date['date2'], filt=filt,
                             table=table, date_field=date_field)
                array_dates.append(json.loads(get_clickhouse_data(q +" FORMAT JSON", 'http://46.4.81.36:8123'))['data'])
                if array_dates[counter] == []:
                    empty_dict = {'label': seg_label,
                                  'segment': seg}
                    for metric in metrics:
                        empty_dict[metric] = 0
                    array_dates[counter].append(empty_dict)
                counter+=1
            counter = 0
            for i in array_dates[0]:
                updimensions = []
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
        like_str={'=@':" LIKE '%{val}%'",'!@':" NOT LIKE '%{val}%'",'=^':" LIKE '{val}%'",'=$':" LIKE '%{val}'",'!^':" NOT LIKE '{val}%'",'!&':" NOT LIKE '%{val}'"}
        match_operators=['=~','!~']
        match_str={'=~':" match({par}?'{val}')",'!~':" NOT match({par}?'{val}')"}
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
                    first_arg=sub_str.partition(j)[0]
                    operator = j
                    try:
                        int(sub_str.partition(j)[2])
                        json.loads(get_clickhouse_data(
                            'SELECT {par}=={val} FROM CHdatabase.visits ALL INNER JOIN CHdatabase.hits USING idVisit LIMIT 1 FORMAT JSON'.format(
                                par=sub_str.partition(j)[0], val=sub_str.partition(j)[2]), 'http://46.4.81.36:8123'))
                        second_arg=sub_str.partition(j)[2]
                    except:
                        if first_arg in ['day_of_week_code', 'month_code', "year", "minute", "second"]:
                            second_arg =sub_str.partition(j)[2]
                        else:
                            second_arg= "'" + sub_str.partition(j)[2] + "'"
                    if sub_str.partition(j)[0][0]=='!':
                        operator=negative_condition(j)
                        first_arg=sub_str.partition(j)[0][1:]
                    sub_str=first_arg+operator+second_arg
                    break
            for j in like_operators:
                if sub_str.partition(j)[2]=='':
                    pass
                else:
                    operator = j
                    first_arg=sub_str.partition(j)[0]
                    second_arg=sub_str.partition(j)[2]
                    if sub_str.partition(j)[0][0]=='!':
                        operator = negative_condition(j)
                        first_arg = sub_str.partition(j)[0][1:]
                    sub_str = first_arg +like_str[operator].format(val=second_arg)
                    break
            for j in match_operators:
                if sub_str.partition(j)[2]=='':
                    pass
                else:
                    operator = j
                    first_arg = sub_str.partition(j)[0]
                    second_arg = sub_str.partition(j)[2]
                    if sub_str.partition(j)[0][0]=='!':
                        operator = negative_condition(j)
                        first_arg = sub_str.partition(j)[0][1:]
                    sub_str = match_str[operator].format(val=second_arg,par=first_arg)
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
            .replace('month_period',"concat(toString(toYear(toDate(serverTimestamp))),concat('-',toString(toMonth(toDate(serverTimestamp)))))").replace('week_period',"concat(toString(toMonday(toDate(serverTimestamp))),concat(' ',toString(toMonday(toDate(serverTimestamp)) +6)))")\
            .replace('quarter_period',"concat(toString(toYear(toDate(serverTimestamp))),concat('-',toString(toQuarter(toDate(serverTimestamp))))))").replace('week',"toRelativeWeekNum(toDate(serverTimestamp)) - toRelativeWeekNum(toStartOfYear(toDate(serverTimestamp)))")\
            .replace('quarter',"toString(toQuarter(toDate(serverTimestamp)))").replace('month',"dictGetString('month','{lang}',toUInt64(toMonth(toDate(serverTimestamp))))".format(lang=lang)).replace('hour',"toHour(toDateTime(serverTimestamp))")
    if request.method=='POST':
        #Заголовки для запроса сегментов
        # Если переданы только сегменты
        resp = {}  # Выходной словарь
        headers = {
            'Authorization': 'JWT eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJ1c2VyX2lkIjoxOSwiZW1haWwiOiIiLCJ1c2VybmFtZSI6ImFydHVyIiwiZXhwIjoxNTE4MTIxNDIyfQ._V0PYXMrE2pJlHlkMtZ_c-_p0y0MIKsv8o5jzR5llpY',
            'Content-Type': 'application/json'}
        # Парсинг json

        try:
            export = json.loads(request.body.decode('utf-8'))['export']
        except:
            export = ""
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

        dimensionslist_with_segments=json.loads(request.body.decode('utf-8'))['dimensions']
        dimensionslist = []
        #Создание списка параметров без сегментов
        dimensionslist_with_segments_and_aliases=[]
        time_dimensions_dict={}
        list_with_time_dimensions_without_aliases=[]#список с параметрами,в которых временные параматры, типа year,month и тд будут представлены без алиасов
        ad_dimensionslist_with_segments_and_aliases = []
        ad_time_dimensions_dict = {}
        ad_list_with_time_dimensions_without_aliases = []
        for d in dimensionslist_with_segments:
            if 'segment' not in d and type(d)!=list:
                dimensionslist.append(d)
                try:
                    time_dimensions_dict[d]=get_time_dimensions(d).format(lang=lang,date_field='toDate(serverTimestamp)')
                    dimensionslist_with_segments_and_aliases.append(time_dimensions_dict[d]+' as '+d)
                    list_with_time_dimensions_without_aliases.append(time_dimensions_dict[d])
                    ad_time_dimensions_dict[d] = get_time_dimensions(d).format(lang=lang, date_field='StatDate')
                    ad_dimensionslist_with_segments_and_aliases.append(ad_time_dimensions_dict[d] + ' as ' + d)
                    ad_list_with_time_dimensions_without_aliases.append(ad_time_dimensions_dict[d])
                    continue
                except:
                    list_with_time_dimensions_without_aliases.append(d)
                    ad_list_with_time_dimensions_without_aliases.append(d)
            dimensionslist_with_segments_and_aliases.append(d)
            ad_dimensionslist_with_segments_and_aliases.append(d)

        metrics = json.loads(request.body.decode('utf-8'))['metrics']
        # сортировка по переданному показателю
        try:
            sort_column = json.loads(request.body.decode('utf-8'))['sort_column']
        except:
            sort_column = ""
        if sort_column not in metrics and sort_column!="" and sort_column not in get_all_dimensions() and '_path' not in sort_column:
            metrics.append(sort_column)
        # строка  "sum(metric1),avg(metric2)...". если показатель относительный используется avg, если нет - sum
        sum_metric_string=[]
        for i in metrics:
            if i in ['clicks','ctr','cost','impressions'] or 'cost' in i:
                continue
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
                filt = "AND" + "(" + FilterParse(filter) + ")"

                try:
                    show_filter_label = json.loads(request.body.decode('utf-8'))['show_filter_label']
                except:
                    show_filter_label = ''
                # Если в show_filter_label передали true, то добавляем в результирующий словарь массив filter_label
                if show_filter_label in ['True', 'true', 'TRUE', True]:
                    filter = filter.replace(',', ';')
                    filter = filter.split(';')
                    filter_label = []
                    for sub_filt in filter:
                        filter_label.append(re.split(r'={1,2}', sub_filt)[1].capitalize())
                    resp['filter_label'] = filter_label
            else:
                filt="AND 1"

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
        site_filt=' 1' # по умолчанию фильтра по idSite нет
        time_format = '%Y-%m-%d'
        ad_date_field='StatDate'
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
                    # вторую дату увеличиваем на день
                    date2=(datetime.strptime(date['date2'], '%Y-%m-%d') - timedelta(
                                days=-1)).strftime('%Y-%m-%d')
                    if time_offset[0] == '+':
                        relative_period.append({'date1': str(
                            datetime.strptime(date['date1'] + '-00', '%Y-%m-%d-%H') - timedelta(
                                hours=int(time_offset[2]) - 3)), 'date2': str(
                            datetime.strptime(date2 + '-00', '%Y-%m-%d-%H') - timedelta(
                                hours=int(time_offset[2]) - 3))})
                    else:
                        relative_period.append({'date1': str(
                            datetime.strptime(date['date1'] + '-00', '%Y-%m-%d-%H') - timedelta(
                                hours=-int(time_offset[2]) - 3)),
                            'date2': str(datetime.strptime(date2 + '-00', '%Y-%m-%d-%H') - timedelta(
                                hours=-int(time_offset[2]) - 3))})
                date_field='toDateTime(serverTimestamp)'
                ad_date_field='toDateTime(StatDate)'
                time_format='%Y-%m-%d %H:%M:%S'
            except:
                relative_period=period
                date_field = 'serverDate'
            try:
                if json.loads(get_clickhouse_data(
                        'SELECT idSite FROM CHdatabase.hits_with_visits WHERE idSite={idSite} FORMAT JSON'.format(
                                idSite=json.loads(requests.get(
                                        'https://s.analitika.online/api/profiles/{profile_id}/?all=1'.format(
                                                profile_id=profile_id), headers=headers).content.decode('utf-8'))[
                                    'site_db_id']), 'http://46.4.81.36:8123'))['data'] == []:
                    filt = ' AND 0'
                else:
                    site_filt = ' idSite==' + str(json.loads(requests.get(
                        'https://s.analitika.online/api/profiles/{profile_id}/?all=1'.format(profile_id=profile_id),
                        headers=headers).content.decode('utf-8'))['site_db_id'])
            except:
                filt = ' AND 0'
        except:
            relative_period=period
            date_field = 'serverDate'
        table = 'CHdatabase.hits_with_visits'

        #Формируем массив с count() для каждого параметра
        dimension_counts=[]
        ad_dimension_counts = []
        for i in dimensionslist:
            if i in time_dimensions_dict.keys():
                dimension_counts.append("CAST(uniq({dimension}),'Int') as h{dimension_alias}".format(dimension=time_dimensions_dict[i],dimension_alias=i))
                ad_dimension_counts.append("CAST(uniq({dimension}),'Int') as h{dimension_alias}".format(dimension=ad_time_dimensions_dict[i],dimension_alias=i))
            else:
                dimension_counts.append("CAST(uniq({dimension}),'Int') as h{dimension}".format(dimension=i))
                ad_dimension_counts.append("CAST(uniq({dimension}),'Int') as h{dimension}".format(dimension=i))
        is_two_tables=False

        # Фильтр по всем датам
        date_filt = []
        ad_date_filt = []
        for dates in relative_period:
            date_filt.append(
                "({date_field} BETWEEN '".format(date_field=date_field) + str(dates['date1']) + "' AND '" + str(
                    dates['date2']) + "')")
            ad_date_filt.append(
                "({ad_date_field} BETWEEN '".format(ad_date_field=ad_date_field) + str(
                    dates['date1']) + "' AND '" + str(
                    dates['date2']) + "')")
        date_filt = ' OR '.join(date_filt)
        ad_date_filt = ' OR '.join(ad_date_filt)
        # Проверка, состоит ли dimensions только из сегментов
        is_all_segments = True
        for dim in dimensionslist:
            if 'segment' not in dim:
                is_all_segments=False
        # ФОрмируем массив с запросом каждого показателя в SQL
        metric_counts_list,ad_metric_counts_list=MetricCounts(metrics,headers,dimensionslist,is_all_segments,attribution_model)
        #Если список нерекламных показателей не пуст, делаем строку с показателями из списка
        metric_counts = ','.join(metric_counts_list)
        ad_metric_counts = ','.join(ad_metric_counts_list)
        # если в metrics присутствуют оба вида показателей, то устанавливаем флаг двух таблиц в true
        if metric_counts_list!=[] and ad_metric_counts_list!=[] :
            is_two_tables=True
            for dim in dimensionslist:
                if dim not in get_adstat_dimensions():
                    is_two_tables=False
                    break

        if is_all_segments==True and metric_counts_list!=[] and ad_metric_counts_list!=[]:
            is_two_tables = True
        if metric_counts_list==[]:
            dimension_counts=ad_dimension_counts
            dimensionslist_with_segments_and_aliases=ad_dimensionslist_with_segments_and_aliases
            list_with_time_dimensions_without_aliases=ad_list_with_time_dimensions_without_aliases
            table='CHdatabase.adstat'
            metric_counts=','.join(ad_metric_counts_list)
            date_field=ad_date_field
            date_filt=ad_date_filt

        # Заполнение таблицы с рекламной статистикой
        """load_query = "INSERT INTO CHdatabase.adstat VALUES "
        next='https://s.analitika.online/api/ad_stat/?all=1'
        while json.loads(requests.get(next, headers=headers).content.decode('utf-8'))['next']!='null':
            for part in json.loads(requests.get(next, headers=headers).content.decode('utf-8'))['results']:
                query = load_query + "(" + str(list(part.values())[1:])[1:len(str(list(part.values())[1:])) - 1] + ",1)"
                query = query.replace("None", "'none'")
                get_clickhouse_data(query, 'http://46.4.81.36:8123')
            next=json.loads(requests.get(next, headers=headers).content.decode('utf-8'))['next']"""

        #Добавляем в выходной словарь параметр counts
        resp['counts'] = {}
        try:
            resp['counts'] = AddCounts(period, dimension_counts,ad_dimension_counts, filt, sort_order, table, date_filt)
        except:
            pass
        # Добавляем в выходной словарь параметр metric_sums

        resp['metric_sums']={}
        total,total_filtered,resp['metric_sums']['dates'] = AddMetricSums(period,metric_counts_list,filt,metrics,sort_order,table)
        stats=AddStats2(dimensionslist_with_segments,dimensionslist_with_segments_and_aliases,ad_dimensionslist_with_segments_and_aliases,metric_counts,filt,limit,period,metrics,table,date_filt)
        # Добавляем stats
        resp['stats']=stats
        pprint.pprint(resp)

        if export=='xlsx':
            filename =stats
            content_type = 'application/vnd.ms-excel'
            file_path = os.path.join(MEDIA_ROOT, filename)
            response = HttpResponse(FileWrapper(open(file_path, 'rb')), content_type=content_type)
            response['Content-Disposition'] = 'attachment; filename=%s' % (filename)
            response['Content-Length'] = os.path.getsize(file_path)
            return response
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
        headers = {
            'Authorization': 'JWT eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJ1c2VyX2lkIjoxOSwiZW1haWwiOiIiLCJ1c2VybmFtZSI6ImFydHVyIiwiZXhwIjoxNTE4MTIxNDIyfQ._V0PYXMrE2pJlHlkMtZ_c-_p0y0MIKsv8o5jzR5llpY',
            'Content-Type': 'application/json'}
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
        try:
            profile_id=response['profile_id']
            try:
                #Определяем относительное время(с учетом часового пояса)
                timezone = json.loads(requests.get(
                    'https://s.analitika.online/api/profiles/{profile_id}/?all=1'.format(profile_id=profile_id),
                    headers=headers).content.decode('utf-8'))['timezone']

                d1 = datetime.strptime(response['date1'] + '-00', '%Y-%m-%d-%H')
                timezone = pytz.timezone(timezone)
                d1 = timezone.localize(d1)
                time_offset = str(d1)[19:]
                #Вторую дату увеличиваем на день
                d2 = (datetime.strptime(response['date2'], '%Y-%m-%d') - timedelta(
                        days=-1)).strftime('%Y-%m-%d')

                if time_offset[0] == '+':
                    relative_date1=str(datetime.strptime(response['date1'] + '-00', '%Y-%m-%d-%H') - timedelta(
                                hours=int(time_offset[2]) - 3))
                    relative_date2=str(datetime.strptime(d2 + '-00', '%Y-%m-%d-%H') - timedelta(
                                hours=int(time_offset[2]) - 3))
                else:
                    relative_date1 = str(datetime.strptime(response['date1'] + '-00', '%Y-%m-%d-%H') - timedelta(
                        hours=-int(time_offset[2]) - 3))
                    relative_date2 = str(datetime.strptime(d2 + '-00', '%Y-%m-%d-%H') - timedelta(
                        hours=-int(time_offset[2]) - 3))

                date_field='toDateTime(serverTimestamp)'
            except:
                relative_date1=response['date1']
                relative_date2 = response['date2']
                date_field = 'serverDate'
                site_filter = ' 1'
            try:
                if json.loads(get_clickhouse_data(
                        'SELECT idSite FROM CHdatabase.hits_with_visits WHERE idSite={idSite} FORMAT JSON'.format(
                                idSite=json.loads(requests.get(
                                        'https://s.analitika.online/api/profiles/{profile_id}/?all=1'.format(
                                                profile_id=profile_id), headers=headers).content.decode('utf-8'))[
                                    'site_db_id']), 'http://46.4.81.36:8123'))['data'] == []:
                    site_filter = ' 0'
                else:
                    site_filter = ' idSite==' + str(json.loads(requests.get(
                        'https://s.analitika.online/api/profiles/{profile_id}/?all=1'.format(profile_id=profile_id),
                        headers=headers).content.decode('utf-8'))['site_db_id'])
            except:
                site_filter = ' 0'
        except:
            site_filter =' 1'
            relative_date1=response['date1']
            relative_date2= response['date2']
            date_field = 'serverDate'
        q_total="""
        SELECT CAST(uniq(visitorId),'Int') as visitors,CAST(uniq(idVisit),'Int') as visits FROM CHdatabase.hits_with_visits WHERE {date_field} BETWEEN '{date1}' AND '{date2}' AND {site_filter} FORMAT JSON
        """.format(date1=relative_date1,date2=relative_date2,date_field=date_field,site_filter=site_filter)
        print(q_total)
        try:
            total=json.loads(get_clickhouse_data(q_total, 'http://46.4.81.36:8123'))['data'][0]
        except:
            total={'visitors':0,'visits':0}
        q = """
                SELECT CAST(uniq(visitorId),'Int') as visitors,CAST(uniq(idVisit),'Int') as visits FROM CHdatabase.hits_with_visits WHERE {date_field} BETWEEN '{date1}' AND '{date2}' AND {filter} AND {site_filter} FORMAT JSON
                """.format(date1=relative_date1, date2=relative_date2,filter=filter,date_field=date_field,site_filter=site_filter)
        print(q)
        try:
            with_filter = json.loads(get_clickhouse_data(q, 'http://46.4.81.36:8123'))['data'][0]
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
    def AddCounts(date1,date2,dimension_counts,filt):
        """Добавление ключа Counts в ответ"""
        a={}

        for dim_num in range(len(dimensionslist)):
            q = ''' SELECT {dimension_counts}
                                    FROM {table}
                                    WHERE 1 {filt} AND {site_filt} AND {date_field} BETWEEN '{date1}' AND '{date2}'
                                   '''
            if is_two_tables:
                q="""SELECT CAST(uniq({dimension}),'Int') as h{dimension} FROM (SELECT DISTINCT {dimensions_with_alias} FROM CHdatabase.hits_with_visits
                                WHERE 1 {filt} AND {site_filt} AND {date_field} BETWEEN '{date1}' AND '{date2}') ALL FULL OUTER JOIN (SELECT DISTINCT {ad_dimensions_with_alias} FROM CHdatabase.adstat
                                WHERE 1 {filt} AND {site_filt} AND {ad_date_field} BETWEEN '{date1}' AND '{date2}') USING {dimension}"""
            q=q.format(dimension_counts=dimension_counts[dim_num],dimension=dimensionslist[dim_num],date_field=date_field,
                       ad_dimensions_with_alias=ad_dimensionslist_with_aliases[dim_num],dimensions_with_alias=dimensionslist_with_aliases[dim_num],
                            site_filt=site_filt, filt=filt,ad_date_field=ad_date_field,table=table,date1=date1,date2=date2)
            print(q)

            try:
                a.update(json.loads(get_clickhouse_data(q+' FORMAT JSON', 'http://46.4.81.36:8123'))['data'][0])
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
    def OldAddCounts(date1,date2,dimension_counts,filt,sort_order,table):
        """Добавление ключа Counts в ответ"""
        a={}
        for dim_num in range(len(dimensionslist)):
            q = ''' SELECT {dimension_counts}
                     FROM {table}
                     WHERE 1 {filt} AND {site_filt} AND {date_field} BETWEEN '{date1}' AND '{date2}'
                     ORDER BY NULL {sort_order}
                     FORMAT JSON
                    '''.format(site_filt=site_filt,date1=date1, date2=date2, dimension_counts=dimension_counts[dim_num], filt=filt,
                               sort_order=sort_order,table=table,date_field=date_field)


            try:
                a.update(json.loads(get_clickhouse_data(q, 'http://46.4.81.36:8123'))['data'][0])
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
    def AddMetricSums(rel_date1,rel_date2,metric_counts_list,filt,metrics,sort_order,table):
        """Добавление ключа metric_sums в ответ"""
        dates = []
        a = {}
        b = {}
        # Запрос на получение сумм показателей без фильтра
        q_total = ''' SELECT {metric_counts}
                    FROM {table}
                    WHERE {date_field} BETWEEN '{date1}' AND '{date2}' AND {site_filt}
                    ORDER BY NULL {sort_order}
                    FORMAT JSON
                   '''.format(site_filt=site_filt,date1=rel_date1, date2=rel_date2, metric_counts=metric_counts,
                              sort_order=sort_order,table=table,date_field=date_field)

        # С фильтром
        q = ''' SELECT {metric_counts}
                                FROM {table}
                                WHERE 1 {filt} AND {site_filt} AND {date_field} BETWEEN '{date1}' AND '{date2}'
                                ORDER BY NULL {sort_order}
                                FORMAT JSON
                               '''.format(site_filt=site_filt,date1=rel_date1, date2=rel_date2, metric_counts=metric_counts,
                                          filt=filt, sort_order=sort_order,table=table,date_field=date_field)
        try:
            a = json.loads(get_clickhouse_data(q_total, 'http://46.4.81.36:8123'))['data'][0]
            b = json.loads(get_clickhouse_data(q, 'http://46.4.81.36:8123'))['data'][0]
        except:
            ad_metr_dict = {}
            for i in metric_counts_list:
                ad_metr_dict[i] = 0
            a = ad_metr_dict
            b = ad_metr_dict
        if is_two_tables:
            q_total_stat = ''' SELECT {ad_metric_counts}
                                                               FROM CHdatabase.adstat
                                                               WHERE {ad_date_field} BETWEEN '{date1}' AND '{date2}' AND {site_filt}
                                                              '''.format(site_filt=site_filt,date1=rel_date1, date2=rel_date2,
                                                                         ad_metric_counts=ad_metric_counts,
                                                                         ad_date_field=ad_date_field)
            # С фильтром
            q_stat = ''' SELECT {ad_metric_counts}
                                                                           FROM CHdatabase.adstat
                                                                           WHERE 1 {filt} AND {site_filt} AND {ad_date_field} BETWEEN '{date1}' AND '{date2}'
                                                                          '''.format(site_filt=site_filt,date1=rel_date1,
                                                                                     date2=rel_date2,
                                                                                     ad_metric_counts=ad_metric_counts,
                                                                                     filt=filt,
                                                                                     ad_date_field=ad_date_field)
            print(q_stat)
            print(q_total_stat)
            try:
                a.update(
                    json.loads(get_clickhouse_data(q_total_stat + ' FORMAT JSON', 'http://46.4.81.36:8123'))['data'][0])
                b.update(
                    json.loads(get_clickhouse_data(q_stat + ' FORMAT JSON', 'http://46.4.81.36:8123'))['data'][0])
            except:
                ad_metr_dict = {}
                for i in ad_metric_counts_list:
                    ad_metr_dict[i] = 0
                a.update(ad_metr_dict)
                b.update(ad_metr_dict)
            #Проверка на существование записей, если их нет, возвращаем нули
        metric_dict = dict.fromkeys(metrics)
                # Добавляем в словарь даты
        for key in list(metric_dict.keys()):
            if key in list(a.keys()):
                metric_dict[key] = {"total_sum": a[key], "sum": b[key]}
            else:
                metric_dict[key] = {"total_sum": 0, "sum": 0}
            # Добавляем в него даты
        metric_dict.update({'date1':date1,'date2':date2})
        dates.append(metric_dict)
        return dates
    def AddStats2(dimensionslist,dim_with_aliases, ad_dim_with_aliases,metric_counts, filt, limit, having, date1,date2, metrics, table):
        """Добавление ключа stats в ответ"""
        #Определяем, есть ли вначале dimensions группа сегментов
        q = '''SELECT {dimensions_with_alias},({metric_counts}) as metrics FROM {table}
                                               WHERE 1 {filt} AND {site_filt} AND {date_field} BETWEEN '{date1}' AND '{date2}'
                                               GROUP BY {dimensions}'''
        # Если в запросе к апи были запрошены рекламные показатели или поля, которые есть только в таблице adstat, то объединяем таблицы с хитами и визитами с adstat
        if is_two_tables:
            q = "SELECT {dimensions},(metrics,ad_metrics) as metrics FROM (" + q + """) ALL FULL OUTER JOIN (SELECT {ad_dimensions_with_alias},({ad_metric_counts}) as ad_metrics FROM CHdatabase.adstat
                                    WHERE 1 {filt} AND {site_filt} AND {ad_date_field} BETWEEN '{date1}' AND '{date2}' GROUP BY {dimensions}) USING {dimensions}"""
        q = q.format(table=table, dimensions_with_alias=','.join(dim_with_aliases), dimensions=','.join(dimensionslist), metric_counts=metric_counts,
                     date1=date1, ad_metric_counts=ad_metric_counts,ad_dimensions_with_alias=','.join(ad_dim_with_aliases),
                     site_filt=site_filt, date2=date2, filt=filt, date_field=date_field,
                     ad_date_field=ad_date_field, metrics=','.join(metrics))
        print(q+" ORDER BY {sort_column} {sort_order} {limit} FORMAT JSON".format(limit=limit,sort_column=sort_column,sort_order=sort_order))
        stats=json.loads(get_clickhouse_data(q+" ORDER BY {sort_column} {sort_order} {limit} FORMAT JSON".format(limit=limit,sort_column=sort_column,sort_order=sort_order), 'http://46.4.81.36:8123'))['data']
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
                q = '''SELECT {dimensions_with_alias},[{metric_counts}] as metrics FROM {table}
                                                               WHERE 1 {filt} AND {site_filt} AND ({date_field} BETWEEN '{date1}' AND '{date2}') AND ({seg_filt})
                                                               GROUP BY {dimensions}'''
                # Если в запросе к апи были запрошены рекламные показатели или поля, которые есть только в таблице adstat, то объединяем таблицы с хитами и визитами с adstat
                if is_two_tables:
                    q = "SELECT {dimensions},arrayConcat(metrics,ad_metrics) as metrics FROM (" + q + """) ALL FULL OUTER JOIN (SELECT {ad_dimensions_with_alias},[{ad_metric_counts}] as ad_metrics FROM CHdatabase.adstat
                                                    WHERE 1 {filt} AND {site_filt} AND {ad_date_field} BETWEEN '{date1}' AND '{date2}' AND ({seg_filt}) GROUP BY {dimensions}) USING {dimensions}"""
                q = q.format(table=table,dimensions_with_alias=','.join(dim_with_aliases),ad_dimensions_with_alias=','.join(ad_dim_with_aliases),
                             dimensions=','.join(dimensionslist), metric_counts=metric_counts,
                             date1=date1, ad_metric_counts=ad_metric_counts,seg_filt=FilterParse(seg_filt.replace("'",'')),
                             site_filt=site_filt, date2=date2, filt=filt, date_field=date_field,
                             ad_date_field=ad_date_field)
                print(q)
                stat_with_segment = json.loads(get_clickhouse_data(q+" ORDER BY {sort_column} {sort_order} {limit} FORMAT JSON".format(limit=limit,sort_column=sort_column,sort_order=sort_order), 'http://46.4.81.36:8123'))['data']
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
        simple_operators = ['==', '!=', '>=', '<=', '>', '<']
        like_operators = ['=@', '!@', '=^', '=$', '!^', '!&']
        like_str = {'=@': " LIKE '%{val}%'", '!@': " NOT LIKE '%{val}%'", '=^': " LIKE '{val}%'",
                    '=$': " LIKE '%{val}'", '!^': " NOT LIKE '{val}%'", '!&': " NOT LIKE '%{val}'"}
        match_operators = ['=~', '!~']
        match_str = {'=~': " match({par}?'{val}')", '!~': " NOT match({par}?'{val}')"}
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
                    first_arg=sub_str.partition(j)[0]
                    operator = j
                    try:
                        int(sub_str.partition(j)[2])
                        json.loads(get_clickhouse_data(
                            'SELECT {par}=={val} FROM CHdatabase.visits ALL INNER JOIN CHdatabase.hits USING idVisit LIMIT 1 FORMAT JSON'.format(
                                par=sub_str.partition(j)[0], val=sub_str.partition(j)[2]), 'http://46.4.81.36:8123'))
                        second_arg=sub_str.partition(j)[2]
                    except:
                        if first_arg in ['day_of_week_code', 'month_code', "year", "minute", "second"]:
                            second_arg =sub_str.partition(j)[2]
                        else:
                            second_arg= "'" + sub_str.partition(j)[2] + "'"
                    if sub_str.partition(j)[0][0]=='!':
                        operator=negative_condition(j)
                        first_arg=sub_str.partition(j)[0][1:]
                    sub_str=first_arg+operator+second_arg
                    break
            for j in like_operators:
                if sub_str.partition(j)[2]=='':
                    pass
                else:
                    operator = j
                    first_arg=sub_str.partition(j)[0]
                    second_arg=sub_str.partition(j)[2]
                    if sub_str.partition(j)[0][0]=='!':
                        operator = negative_condition(j)
                        first_arg = sub_str.partition(j)[0][1:]
                    sub_str = first_arg +like_str[operator].format(val=second_arg)
                    break
            for j in match_operators:
                if sub_str.partition(j)[2]=='':
                    pass
                else:
                    operator = j
                    first_arg = sub_str.partition(j)[0]
                    second_arg = sub_str.partition(j)[2]
                    if sub_str.partition(j)[0][0]=='!':
                        operator = negative_condition(j)
                        first_arg = sub_str.partition(j)[0][1:]
                    sub_str = match_str[operator].format(val=second_arg,par=first_arg)
                    break
            try:
                end_filt=end_filt+sub_str+filt_string[separator_indices[i]]
            except:
                end_filt = end_filt + sub_str

        end_filt=end_filt.replace(',',' OR ')
        end_filt=end_filt.replace(';',' AND ')
        end_filt = end_filt.replace('?', ',')
        return end_filt.replace('hour',"toHour(toDateTime(serverTimestamp))").replace('date', 'toDate(serverTimestamp)').replace('month_code',
                                                                           'toMonth(toDate(serverTimestamp))').replace(
            'day_of_week_code', "toDayOfWeek(toDate(serverTimestamp))") \
            .replace('day_of_week',
                     "dictGetString('week','{lang}',toUInt64(toDayOfWeek(toDate(serverTimestamp))))".format(
                         lang=lang)) \
            .replace('year', "toYear(toDate(serverTimestamp))").replace('minute',
                                                                        "toMinute(toDateTime(serverTimestamp))").replace(
            'second', "toSecond(toDateTime(serverTimestamp))") \
            .replace('month_period',
                     "concat(toString(toYear(toDate(serverTimestamp))),concat('-',toString(toMonth(toDate(serverTimestamp)))))").replace(
            'week_period',
            "concat(toString(toMonday(toDate(serverTimestamp))),concat(' ',toString(toMonday(toDate(serverTimestamp)) +6)))") \
            .replace('quarter_period',
                     "concat(toString(toYear(toDate(serverTimestamp))),concat('-',toString(toQuarter(toDate(serverTimestamp))))))").replace(
            'week',
            "toRelativeWeekNum(toDate(serverTimestamp)) - toRelativeWeekNum(toStartOfYear(toDate(serverTimestamp)))") \
            .replace('quarter', "toString(toQuarter(toDate(serverTimestamp)))").replace('month',
                                                                                        "dictGetString('month','{lang}',toUInt64(toMonth(toDate(serverTimestamp))))".format(
                                                                                            lang=lang))
    if request.method=='POST':
        resp = {}  # Выходной словарь
        #Заголовки для запроса сегментов
        headers = {
            'Authorization': 'JWT eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJ1c2VyX2lkIjoxOSwiZW1haWwiOiIiLCJ1c2VybmFtZSI6ImFydHVyIiwiZXhwIjoxNTE4MTIxNDIyfQ._V0PYXMrE2pJlHlkMtZ_c-_p0y0MIKsv8o5jzR5llpY',
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
        ad_dimensionslist_with_aliases=[]#ля рекламных параметров
        #Создание списка параметров без сегментов
        time_dimensions_dict = {}
        ad_time_dimensions_dict = {}#для рекламных параметров
        #Проверка на сложные параметры(временнЫе)
        for d in dimensionslist_with_segments:
            if 'segment' not in d and type(d) != list:
                dimensionslist.append(d)
                try:
                    time_dimensions_dict[d]=get_time_dimensions(d).format(lang=lang,date_field='toDate(serverTimestamp)')
                    dimensionslist_with_aliases.append(time_dimensions_dict[d]+' as '+d)
                    ad_time_dimensions_dict[d] = get_time_dimensions(d).format(lang=lang, date_field='StatDate')
                    ad_dimensionslist_with_aliases.append(ad_time_dimensions_dict[d] + ' as ' + d)
                    continue
                except:
                    dimensionslist_with_aliases.append(d)
                    ad_dimensionslist_with_aliases.append(d)
        print(dimensionslist_with_aliases)
        print(dimensionslist)
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

                try:
                    show_filter_label = json.loads(request.body.decode('utf-8'))['show_filter_label']
                except:
                    show_filter_label = ''
                # Если в show_filter_label передали true, то добавляем в результирующий словарь массив filter_label
                if show_filter_label in ['True', 'true', 'TRUE', True]:
                    filter = filter.replace(',', ';')
                    filter = filter.split(';')
                    print(filter)
                    filter_label = []
                    for sub_filt in filter:
                        filter_label.append(re.split(r'={1,2}', sub_filt)[1].capitalize())
                    resp['filter_label'] = filter_label[len(filter_label)-1]
                    if len(filter_label)>1:
                        resp['filter_parent_label'] = filter_label[len(filter_label) - 2]

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
        site_filt=' 1'#льтр по idSite(по умолчанию нет)
        ad_date_field="StatDate"
        try:
            profile_id=json.loads(request.body.decode('utf-8'))['profile_id']
            try:
                #Определяем относительное время(с учетом часового пояса)
                timezone = json.loads(requests.get(
                    'https://s.analitika.online/api/profiles/{profile_id}/?all=1'.format(profile_id=profile_id),
                    headers=headers).content.decode('utf-8'))['timezone']

                d1 = datetime.strptime(date1 + '-00', '%Y-%m-%d-%H')
                timezone = pytz.timezone(timezone)
                d1 = timezone.localize(d1)
                time_offset = str(d1)[19:]
                #Вторую дату увеличиваем на день
                d2 = (datetime.strptime(date2, '%Y-%m-%d') - timedelta(
                        days=-1)).strftime('%Y-%m-%d')

                if time_offset[0] == '+':
                    relative_date1=str(datetime.strptime(date1 + '-00', '%Y-%m-%d-%H') - timedelta(
                                hours=int(time_offset[2]) - 3))
                    relative_date2=str(datetime.strptime(d2 + '-00', '%Y-%m-%d-%H') - timedelta(
                                hours=int(time_offset[2]) - 3))
                else:
                    relative_date1 = str(datetime.strptime(date1 + '-00', '%Y-%m-%d-%H') - timedelta(
                        hours=-int(time_offset[2]) - 3))
                    relative_date2 = str(datetime.strptime(d2 + '-00', '%Y-%m-%d-%H') - timedelta(
                        hours=-int(time_offset[2]) - 3))
                date_field='toDateTime(serverTimestamp)'
                ad_date_field='toDateTime(StatDate)'
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
                                    'site_db_id']), 'http://46.4.81.36:8123'))['data'] == []:
                    filt = ' AND 0'
                else:
                    site_filt = ' idSite==' + str(json.loads(requests.get(
                        'https://s.analitika.online/api/profiles/{profile_id}/?all=1'.format(profile_id=profile_id),
                        headers=headers).content.decode('utf-8'))['site_db_id'])
            except:
                filt = ' AND 0'
        except:
            relative_date1=date1
            relative_date2= date2
            date_field = 'serverDate'

        table = 'CHdatabase.hits_with_visits'
        list_of_adstat_par=['Clicks','Impressions','Cost','StatDate','idSite', 'AdCampaignId', 'AdBannerId', 'AdChannelId', 'AdDeviceType', 'AdGroupId', 'AdKeywordId',
                       'AdPosition', 'AdPositionType', 'AdRegionId', 'AdRetargetindId', 'AdPlacement', 'AdTargetId', 'AdvertisingSystem', 'DRF', 'campaignContent',
                       'campaignKeyword', 'campaignMedium', 'campaignName', 'campaignSource']


        #Формируем массив с count() для каждого параметра
        dimension_counts=[]
        ad_dimension_counts=[]
        for i in dimensionslist:
            if i in time_dimensions_dict.keys():
                ad_dimension_counts.append("CAST(uniq({dimension}),'Int') as h{dimension_alias}".format(dimension=ad_time_dimensions_dict[i],dimension_alias=i))
                dimension_counts.append("CAST(uniq({dimension}),'Int') as h{dimension_alias}".format(dimension=time_dimensions_dict[i],dimension_alias=i))
            else:
                dimension_counts.append("CAST(uniq({dimension}),'Int') as h{dimension}".format(dimension=i))
                ad_dimension_counts.append("CAST(uniq({dimension}),'Int') as h{dimension}".format(dimension=i))


        # ФОрмируем массив с запросом каждого показателя в SQL
        is_all_segments = True
        is_two_tables=False
        for dim in dimensionslist:
            if 'segment' not in dim:
                is_all_segments = False

        # ФОрмируем массив с запросом каждого показателя в SQL
        metric_counts_list, ad_metric_counts_list = MetricCounts(metrics, headers, dimensionslist, is_all_segments,'')
        # Если список нерекламных показателей не пуст, делаем строку с показателями из списка
        metric_counts = ','.join(metric_counts_list)
        ad_metric_counts = ','.join(ad_metric_counts_list)
        # если в metrics присутствуют оба вида показателей, то устанавливаем флаг двух таблиц в true
        if metric_counts_list != [] and ad_metric_counts_list != []:
            is_two_tables = True
            for dim in dimensionslist:
                if dim not in get_adstat_dimensions():
                    is_two_tables = False
                    break

        if is_all_segments == True and metric_counts_list != [] and ad_metric_counts_list != []:
            is_two_tables = True
        if metric_counts_list == []:
            dimensionslist_with_aliases=ad_dimensionslist_with_aliases
            dimension_counts=ad_dimension_counts
            table = 'CHdatabase.adstat'
            metric_counts = ','.join(ad_metric_counts_list)
            date_field = ad_date_field

        #Добавляем в выходной словарь параметр counts

        resp['counts'] = {}
        if dimensionslist != []:
            resp['counts']=AddCounts(relative_date1,relative_date2,dimension_counts,filt)
        # Добавляем в выходной словарь параметр metric_sums
        resp['metric_sums']={}
        resp['metric_sums']['dates'] = AddMetricSums(relative_date1,relative_date2,metric_counts_list,filt,metrics,sort_order,table)
        stats=AddStats2(dimensionslist,dimensionslist_with_aliases,ad_dimensionslist_with_aliases,metric_counts,filt,limit,having,relative_date1,relative_date2,metrics,table)
        # Добавляем stats
        resp['stats']=stats
        pprint.pprint(resp)
        response=JsonResponse(resp,safe=False,)
        response['Access-Control-Allow-Origin']='*'
        return response


