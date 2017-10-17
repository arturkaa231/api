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
	def get_clickhouse_data(query,host,connection_timeout=1500):
		r=requests.post(host,params={'query':query},timeout=connection_timeout)
		return r.text
	def SubQuery(d1,i,d2):
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
	#Метод для перевода словаря global_filter в строку для sql запроса
	def FilterParse(filt_dict):
		filt=[]
		for i in filt_dict['filters']:
			sub_filt=[]
			for j in i['sub_filters']:
				Not=""
				if j['not']=='True':
					Not="NOT"
				if j['filter_operator']=="IN":
					expressions='('+','.join(j['expressions'])+')'
				else:
					expressions=j['expressions'][0]
				filt_str=' {Not}{parameter} {filter_operator} {expressions} '.format(parameter=j['parameter']
																				   ,Not=Not,filter_operator=j['filter_operator'],expressions=expressions)
				sub_filt.append(filt_str)
			sub_filt=' ('+i['global_filter_operator'].join(sub_filt)+') '
			filt.append(sub_filt)
		filt=filt_dict['operator'].join(filt)
		print(filt)



	if request.method=='POST':
		#Парсинг json
		print(json.loads(request.body.decode('utf-8'))['flat'])
		sort_order=json.loads(request.body.decode('utf-8'))['sort_order']
		dimensionslist=json.loads(request.body.decode('utf-8'))['dimensions']
		dimensions=','.join(dimensionslist)
		metrics=json.loads(request.body.decode('utf-8'))['metrics']
		#Парсинг нужных параметров
		nb_visits=["","",""]
		nb_actions = ["", "", ""]
		goal1 = ["", "", ""]
		for i in metrics:
			if i=="nb_visits":
				nb_visits[0]=',sum(nb_actions) as nb_actions'
				nb_visits[1]=',nb_visits'
				nb_visits[2]=',count(*) as nb_visits'
			if i=="nb_actions":
				nb_actions[0]=',sum(nb_visits) as nb_visits'
				nb_actions[1]=',nb_actions'
				nb_actions[2]=',sum(actions) as nb_actions'
			if i=="goal1":
				goal1[0]=',sum(goal1) as goal1'
				goal1[1]=',goal1'
				goal1[2]=",sum(Type='goal' AND goalId=1) as goal1"

		limit=json.loads(request.body.decode('utf-8'))['limit']
		offset=json.loads(request.body.decode('utf-8'))['offset']
		period=json.loads(request.body.decode('utf-8'))['period']
		where = "AND serverDate BETWEEN"
		k=0
		for i in period:
			if k>=1:
				where=where+" OR serverDate BETWEEN '%s' AND '%s' "%(i['date1'],i['date2'])
			else:
				where = where + " '%s' AND '%s' " % (i['date1'], i['date2'])
			k+=1
		#if date1!="" and date2!="":
			#where="AND serverDate BETWEEN '%s' AND '%s'"%(date1,date2)
		global_filter=json.loads(request.body.decode('utf-8'))['global_filter']
		#if global_filter!="":
			#global_filter='AND '+global_filter.replace(',',' AND ')
		flat=json.loads(request.body.decode('utf-8'))['flat']
		print(global_filter)
		FilterParse(global_filter)
		if flat=="" or flat=="true":
			q='''SELECT {dimensions}{nb_visits[0]}{nb_actions[0]}{goal1[0]} FROM
			(SELECT {dimensions}{nb_visits[1]}{nb_actions[1]}{goal1[1]} FROM (SELECT idVisit,{dimensions}{nb_visits[2]}{nb_actions[2]} FROM CHdatabase.visits WHERE 1 {where}
			GROUP BY idVisit,{dimensions}  )
			ALL INNER JOIN 
			(SELECT idVisit{goal1[2]}  FROM  CHdatabase.hits  GROUP BY idVisit ) USING idVisit )
			GROUP BY {dimensions}
			FORMAT JSON
			'''.format(dimensions=dimensions,where=where,sort_order=sort_order,filt=global_filter,nb_visits=nb_visits,nb_actions=nb_actions,goal1=goal1)
			if limit=="" and offset !="":
				resp=json.loads(get_clickhouse_data(q,'http://85.143.172.199:8123'))['data'][int(offset):]
			if limit!="" and offset =="":
				resp=json.loads(get_clickhouse_data(q,'http://85.143.172.199:8123'))['data'][:int(limit)]
			if limit=="" and offset =="":
				resp=json.loads(get_clickhouse_data(q,'http://85.143.172.199:8123'))['data']
			if limit !="" and offset!="":
				resp=json.loads(get_clickhouse_data(q,'http://85.143.172.199:8123'))['data'][int(offset):int(offset)+int(limit)]
			print(resp)
			
		else:
			#формирование древовидного отчета
			levels=len(dimensionslist)
			sub=[]
			labels=[]
			q2='''
			SELECT {dimension},sum(nb_visits) as nb_visits,sum(nb_actions) as nb_actions,sum(goal1) as goal1 FROM 
			(SELECT {dimension},nb_visits,nb_actions,goal1 FROM (SELECT idVisit, {dimension},count(*) as nb_visits,sum(actions) as nb_actions FROM CHdatabase.visits WHERE 1 {where} 
			{filt}  GROUP BY idVisit,{dimension}  ) 
			ALL INNER JOIN 
			(SELECT idVisit,sum(Type='goal' AND goalId=1) as goal1  FROM  CHdatabase.hits  GROUP BY idVisit ) USING idVisit )
			GROUP BY {dimension}
			'''.format(dimension=dimensionslist[0],where=where,filt=global_filter)
			tree=[]
			l=((get_clickhouse_data(q2,'http://85.143.172.199:8123')).split('\n'))
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
			for (i,v,a,g) in zip(labels,visits,actions,goals):
				sub1=[]
				tree.append({'label':i,'segment':dimensionslist[0]+"==",'metrics':{'nb_actions':a,'nb_visits':v,'goal1':g},'sub1':sub1})
				if levels>1:
					labels2,visits2,actions2,goals2=SubQuery(0,i,1)
					for (j,v2,a2,g2) in zip(labels2,visits2,actions2,goals2):
						sub2=[]
						sub1.append({'label':j,'segment':dimensionslist[0]+"==  "+dimensionslist[1]+"==",'metrics':{'nb_actions':a2,'nb_visits':v2,'goal1':g2},'sub2':sub2})
						if levels>2:
							labels3,visits3,actions3,goals3=SubQuery(1,j,2)
							for (k,v3,a3,g3) in zip(labels3,visits3,actions3,goals3):
								sub3=[]
								sub2.append({'label':k,'segment':dimensionslist[0]+"==  "+dimensionslist[1]+"== "+dimensionslist[2]+"==",'metrics':{'nb_actions':a3,'nb_visits':v3,'goal1':g3},'sub3':sub3})	
								if levels>3: 	
									labels4,visits4,actions4,goals4=SubQuery(2,k,3)
									for (m,v4,a4,g4) in zip(labels4,visits4,actions4,goals4):
										sub4=[]
										sub3.append({'label':m,'segment':dimensionslist[0]+"==  "+dimensionslist[1]+"== "+dimensionslist[2]+"== "+dimensionslist[3]+"==",'metrics':{'nb_actions':a4,'nb_visits':v4,'goal1':g4},'sub4':sub4})
										if levels>4:
											labels5,visits5,actions5,goals5=SubQuery(3,m,4)
											for (n,v5,a5,g5) in zip(labels5,visits5,actions5,goals5):
												sub4.append({'label':n,'segment':dimensionslist[0]+"==  "+dimensionslist[1]+"== "+dimensionslist[2]+"== "+dimensionslist[3]+"== "+dimensionslist[4]+"==",'metrics':{'nb_actions':a5,'nb_visits':v5,'goal1':g5}})
										
									
				
			#pprint.pprint(json.dumps(tree))
			resp=tree
		return JsonResponse(json.dumps(resp),safe=False)
	else:
		args={}
		args.update(csrf(request))
		args['form']=RequestForm
		return render_to_response('mainAPI.html',args)

