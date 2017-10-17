from lxml import etree
from time import time
import datetime

from infi.clickhouse_orm import models as md
from infi.clickhouse_orm import fields as fd
from infi.clickhouse_orm import engines as en
from infi.clickhouse_orm.database import Database
class Visits(md.Model):
# describes datatypes and fields
	idSite=fd.UInt64Field(default=0)
	idVisit = fd.UInt64Field(default=0)
	visitIp=fd.StringField(default='none')
	visitorId=fd.StringField(default='none')
	goalConversions=fd.UInt64Field(default=0)
	siteCurrency=fd.StringField(default='none')
	siteCurrencySymbol=fd.StringField(default='none')
	serverDate=fd.DateField()
	visitServerHour=fd.UInt64Field(default=0)
	lastActionTimestamp=fd.UInt64Field(default=0)
	lastActionDateTime=fd.StringField()
	userId=fd.StringField(default='none')
	visitorType=fd.StringField(default='none')
	visitorTypeIcon=fd.StringField(default='none')
	visitConverted=fd.UInt64Field(default=0)
	visitConvertedIcon=fd.StringField(default='none')
	visitCount=fd.UInt64Field(default=0)
	firstActionTimestamp=fd.UInt64Field(default=0)
	visitEcommerceStatus=fd.StringField(default='none')
	visitEcommerceStatusIcon=fd.StringField(default='none')
	daysSinceFirstVisit=fd.UInt64Field(default=0)
	daysSinceLastEcommerceOrder=fd.UInt64Field(default=0)
	visitDuration=fd.UInt64Field(default=0)
	visitDurationPretty=fd.StringField(default='none')
	searches=fd.UInt64Field(default=0)
	actions=fd.UInt64Field(default=0)
	interactions=fd.UInt64Field(default=0)
	referrerType=fd.StringField(default='none')
	referrerTypeName=fd.StringField(default='none')
	referrerName=fd.StringField(default='none')
	referrerKeyword=fd.StringField(default='none')
	referrerKeywordPosition=fd.UInt64Field(default=0)
	referrerUrl=fd.StringField(default='none')
	referrerSearchEngineUrl=fd.StringField(default='none')
	referrerSearchEngineIcon=fd.StringField(default='none')
	languageCode=fd.StringField(default='none')
	language=fd.StringField(default='none')
	deviceType=fd.StringField(default='none')
	deviceTypeIcon=fd.StringField(default='none')
	deviceBrand=fd.StringField(default='none')
	deviceModel=fd.StringField(default='none')
	operatingSystem=fd.StringField(default='none')
	operatingSystemName=fd.StringField(default='none')
	operatingSystemIcon=fd.StringField(default='none')
	operatingSystemCode=fd.StringField(default='none')
	operatingSystemVersion=fd.StringField(default='none')
	browserFamily=fd.StringField(default='none')
	browserFamilyDescription=fd.StringField(default='none')
	browser=fd.StringField(default='none')
	browserName=fd.StringField(default='none')
	browserIcon=fd.StringField(default='none')
	browserCode=fd.StringField(default='none')
	browserVersion=fd.StringField(default='none')
	events=fd.UInt64Field(default=0)
	continent=fd.StringField(default='none')
	continentCode=fd.StringField(default='none')
	country=fd.StringField(default='none')
	countryCode=fd.StringField(default='none')
	countryFlag=fd.StringField(default='none')
	region=fd.StringField(default='none')
	regionCode=fd.StringField(default='none')
	city=fd.StringField(default='none')	
	location=fd.StringField(default='none')
	latitude=fd.Float64Field(default=0.0)
	longitude=fd.Float64Field(default=0.0)
	visitLocalTime=fd.StringField(default='none')
	visitLocalHour=fd.UInt64Field(default=0)
	daysSinceLastVisit=fd.UInt64Field(default=0)
	customVariables=fd.StringField(default='none')
	resolution=fd.StringField(default='none')
	plugins=fd.StringField(default='none')
	pluginsIcons=fd.StringField(default='none')
	provider=fd.StringField(default='none')
	providerName=fd.StringField(default='none')
	providerUrl=fd.StringField(default='none')
	dimension1=fd.StringField(default='none')
	campaignId=fd.StringField(default='none')
	campaignContent=fd.StringField(default='none')
	campaignKeyword=fd.StringField(default='none')
	campaignMedium=fd.StringField(default='none')
	campaignName=fd.StringField(default='none')
	campaignSource=fd.StringField(default='none')
	serverTimestamp=fd.UInt64Field(default=0)
	serverTimePretty=fd.StringField(default='none')
	serverDatePretty=fd.StringField(default='none')
	serverDatePrettyFirstAction=fd.StringField(default='none')
	serverTimePrettyFirstAction=fd.StringField(default='none')
	totalEcommerceRevenue=fd.Float64Field(default=0.0)
	totalEcommerceConversions=fd.UInt64Field(default=0)
	totalEcommerceItems=fd.UInt64Field(default=0)
	totalAbandonedCartsRevenue=fd.Float64Field(default=0.0)
	totalAbandonedCarts=fd.UInt64Field(default=0)
	totalAbandonedCartsItems=fd.UInt64Field(default=0)
# creating an sampled MergeTree
	engine = en.MergeTree('serverDate', ('idSite','idVisit','visitIp','visitorId','goalConversions','siteCurrency','siteCurrencySymbol','serverDate','visitServerHour',
'lastActionTimestamp','lastActionDateTime','userId','visitorType','visitorTypeIcon','visitConverted','visitConvertedIcon',
'visitCount','firstActionTimestamp','visitEcommerceStatus','visitEcommerceStatusIcon','daysSinceFirstVisit','daysSinceLastEcommerceOrder',
'visitDuration','visitDurationPretty','searches','actions','interactions','referrerType','referrerTypeName','referrerName','referrerKeyword',
'referrerKeywordPosition','referrerUrl','referrerSearchEngineIcon','languageCode','language','deviceType','deviceTypeIcon','deviceBrand','deviceModel',
'operatingSystem','operatingSystemName','operatingSystemIcon','operatingSystemCode','operatingSystemVersion','browserFamily','browserFamilyDescription',
'browser','browserName','browserIcon','browserCode','browserVersion','events','continent','continentCode',
'country','countryCode','countryFlag','region','regionCode','city','location','latitude','longitude','visitLocalTime','visitLocalHour',
'daysSinceLastVisit','customVariables','resolution','plugins','pluginsIcons','provider','providerName','providerUrl','dimension1','campaignId',
'campaignContent','campaignKeyword','campaignMedium','campaignName','campaignSource','serverTimestamp','serverTimePretty','serverDatePretty',
'serverDatePrettyFirstAction','serverTimePrettyFirstAction','totalEcommerceRevenue','totalEcommerceConversions','totalEcommerceItems','totalAbandonedCartsRevenue',
'totalAbandonedCarts','totalAbandonedCartsItems'))
class Hits(md.Model):
	Type=fd.StringField()
	url=fd.StringField(default='none')
	pageTitle=fd.StringField(default='none')
	pageIdAction=fd.UInt64Field(default=0)
	serverTimePretty=fd.StringField()
	pageId=fd.UInt64Field(default=0)
	generationTimeMilliseconds=fd.UInt64Field(default=0)
	generationTime=fd.StringField()
	interactionPosition=fd.UInt64Field(default=0)
	icon=fd.StringField(default='none')
	timestamp=fd.UInt64Field(default=0)
	Date=fd.DateField(default=datetime.datetime(2017,2,2))
	engine = en.MergeTree('Date', ('Type','url','pageTitle','pageIdAction','serverTimePretty',
'pageId','generationTimeMilliseconds','generationTime','interactionPosition','icon','timestamp'))	
def safely_get_data(element, key):
	try:
		for child in element:
			if child.tag == key:
				return child.text
	except:
		return "not found"

def parse_clickhouse_xml(filename, db_name, db_host):
	visits_buffer =[]
	for event, result in etree.iterparse(filename, tag="row"):
# getting values
		
		idSite=safely_get_data(result, 'idSite') 
		idVisit=safely_get_data(result, 'idVisit') 
		visitIp=safely_get_data(result, 'visitIp')  
		visitorId=safely_get_data(result, 'visitorId') 
		goalConversions =safely_get_data(result, 'goalConversions') 
		siteCurrency =safely_get_data(result, 'siteCurrency') 
		siteCurrencySymbol =safely_get_data(result, 'siteCurrencySymbol') 
		serverDate =safely_get_data(result, 'serverDate') 
		visitServerHour=safely_get_data(result, 'visitServerHour') 
		lastActionTimestamp =safely_get_data(result, 'lastActionTimestamp') 
		lastActionDateTime =safely_get_data(result, 'lastActionDateTime') 
		if safely_get_data(result, 'userId'):
			userId=safely_get_data(result, 'userId')
		else:
			userId='none'
		visitorType= safely_get_data(result, 'visitorType')
		if safely_get_data(result, 'visitorTypeIcon'): 
			visitorTypeIcon=safely_get_data(result, 'visitorTypeIcon') 
		else:
			visitorTypeIcon='none' 
		visitConverted =safely_get_data(result, 'visitConverted') 
		if safely_get_data(result, 'visitConvertedIcon'):
			visitConvertedIcon =safely_get_data(result, 'visitConvertedIcon')
		else:
			visitConvertedIcon='none'
		visitCount =safely_get_data(result, 'visitCount') 
		firstActionTimestamp =safely_get_data(result, 'firstActionTimestamp') 
		visitEcommerceStatus =safely_get_data(result, 'visitEcommerceStatus') 
		visitEcommerceStatusIcon =safely_get_data(result, 'visitEcommerceStatusIcon') 
		daysSinceFirstVisit =safely_get_data(result, 'daysSinceFirstVisit') 
		daysSinceLastEcommerceOrder =safely_get_data(result, 'daysSinceLastEcommerceOrder') 
		visitDuration =safely_get_data(result, 'visitDuration') 
		visitDurationPretty =safely_get_data(result, 'visitDurationPretty') 
		searches =safely_get_data(result, 'searches') 
		actions =safely_get_data(result, 'actions') 
		interactions =safely_get_data(result, 'interactions') 
		referrerType =safely_get_data(result, 'referrerType') 
		referrerTypeName =safely_get_data(result, 'referrerTypeName') 
		referrerName =safely_get_data(result, 'referrerName') 
		referrerKeyword =safely_get_data(result, 'referrerKeyword') 
		referrerKeywordPosition =safely_get_data(result, 'referrerKeywordPosition') 
		if safely_get_data(result, 'referrerUrl'):
			referrerUrl =safely_get_data(result, 'referrerUrl')
		else:
			referrerUrl ='none'
		if safely_get_data(result, 'referrerSearchEngineUrl') :
			referrerSearchEngineUrl=safely_get_data(result, 'referrerSearchEngineUrl')
		else:
			referrerSearchEngineUrl='none'
		if safely_get_data(result, 'referrerSearchEngineIcon'):
			referrerSearchEngineIcon =safely_get_data(result, 'referrerSearchEngineIcon')
		else:
			referrerSearchEngineIcon='none'
		if safely_get_data(result, 'languageCode') :
			languageCode =safely_get_data(result, 'languageCode') 
		else:
			languageCode='none'
		language =safely_get_data(result, 'language') 
		deviceType =safely_get_data(result, 'deviceType') 
		deviceTypeIcon =safely_get_data(result, 'deviceTypeIcon') 
		deviceBrand =safely_get_data(result, 'deviceBrand') 
		if safely_get_data(result, 'deviceModel'):
			deviceModel =safely_get_data(result, 'deviceModel') 
		else:
			deviceModel='none'
		operatingSystem =safely_get_data(result, 'operatingSystem') 
		operatingSystemName =safely_get_data(result, 'operatingSystemName') 
		operatingSystemIcon =safely_get_data(result, 'operatingSystemIcon') 
		operatingSystemCode =safely_get_data(result, 'operatingSystemCode') 
		if safely_get_data(result, 'operatingSystemVersion') :
			operatingSystemVersion =safely_get_data(result, 'operatingSystemVersion') 
		else:
			operatingSystemVersion='none'
		browserFamily =safely_get_data(result, 'browserFamily') 
		browserFamilyDescription =safely_get_data(result, 'browserFamilyDescription') 
		browser =safely_get_data(result, 'browser') 
		browserName =safely_get_data(result, 'browserName') 
		browserIcon =safely_get_data(result, 'browserIcon') 
		browserCode =safely_get_data(result, 'browserCode') 
		if safely_get_data(result, 'browserVersion'):
			browserVersion =safely_get_data(result, 'browserVersion') 
		else:
			browserVersion='none'
		events =safely_get_data(result, 'events') 
		continent =safely_get_data(result, 'continent') 
		continentCode=safely_get_data(result, 'continentCode') 
		country =safely_get_data(result, 'country') 
		countryCode =safely_get_data(result, 'countryCode') 
		countryFlag =safely_get_data(result, 'countryFlag') 
		if safely_get_data(result, 'region'):
			region =safely_get_data(result, 'region') 
		else:
			region='none'
		if safely_get_data(result, 'regionCode'):
			regionCode =safely_get_data(result, 'regionCode') 
		else:
			regionCode='none'
		if safely_get_data(result, 'city') :
			city =safely_get_data(result, 'city') 
		else:
			city='none'
		location =safely_get_data(result, 'location') 
		latitude =safely_get_data(result, 'latitude') 
		longitude =safely_get_data(result, 'longitude') 
		visitLocalTime =safely_get_data(result, 'visitLocalTime') 
		visitLocalHour =safely_get_data(result, 'visitLocalHour') 
		daysSinceLastVisit =safely_get_data(result, 'daysSinceLastVisit') 
		customVariables =safely_get_data(result, 'customVariables') 
		resolution =safely_get_data(result, 'resolution') 
		if safely_get_data(result, 'plugins'):
			plugins =safely_get_data(result, 'plugins') 
		else:
			plugins='none'
		if safely_get_data(result, 'pluginsIcons'):
			pluginsIcons =safely_get_data(result, 'pluginsIcons')
		else:
			pluginsIcons='none'
		provider =safely_get_data(result, 'provider') 
		providerName =safely_get_data(result, 'providerName') 
		providerUrl =safely_get_data(result, 'providerUrl') 
		if safely_get_data(result, 'dimension1'):
			dimension1 =safely_get_data(result, 'dimension1') 
		else:
			dimension1='none'
		if safely_get_data(result, 'campaignId') :
			campaignId =safely_get_data(result, 'campaignId') 
		else:
			campaignId ='none'
		campaignContent =safely_get_data(result, 'campaignContent') 
		campaignKeyword =safely_get_data(result, 'campaignKeyword') 
		campaignMedium =safely_get_data(result, 'campaignMedium') 
		campaignName =safely_get_data(result, 'campaignName') 
		campaignSource =safely_get_data(result, 'campaignSource') 
		serverTimestamp=safely_get_data(result, 'serverTimestamp') 
		serverTimePretty=safely_get_data(result, 'serverTimePretty') 
		serverDatePretty=safely_get_data(result, 'serverDatePretty') 
		serverDatePrettyFirstAction =safely_get_data(result, 'serverDatePrettyFirstAction') 
		serverTimePrettyFirstAction =safely_get_data(result, 'serverTimePrettyFirstAction') 
		totalEcommerceRevenue =safely_get_data(result, 'totalEcommerceRevenue') 
		totalEcommerceConversions =safely_get_data(result, 'totalEcommerceConversions') 
		totalEcommerceItems =safely_get_data(result, 'totalEcommerceItems') 
		totalAbandonedCartsRevenue=safely_get_data(result, 'totalAbandonedCartsRevenue') 
		totalAbandonedCarts =safely_get_data(result, 'totalAbandonedCarts') 
		totalAbandonedCartsItems=safely_get_data(result, 'totalAbandonedCartsItems') 
		
# inserting data into clickhouse model representation
		insert_visits = Visits(
			idSite=idSite,
			idVisit=idVisit,
			visitIp=visitIp,  
			visitorId=visitorId, 
			serverDate =serverDate,
			
			)
		visits_buffer.append(insert_visits)
		result.clear()
	hits_buffer =[]
	for event, result in etree.iterparse(filename, tag="actionDetails"):
		Type=safely_get_data(result, 'type')
		if safely_get_data(result, 'url'):
			url=safely_get_data(result, 'url')
		else:
			url='none'
		if safely_get_data(result, 'pageTitle'):
			pageTitle=safely_get_data(result, 'pageTitle')
		else:
			pageTitle='none'
		if safely_get_data(result, 'pageIdAction'):
			pageIdAction=safely_get_data(result, 'pageIdAction')
		else:
			pageIdAction=0
		if safely_get_data(result, 'serverTimePretty'):
			serverTimePretty=safely_get_data(result, 'serverTimePretty')
		else:
			serverTimePretty='none'
		if safely_get_data(result, 'pageId'):
			pageId=safely_get_data(result, 'pageId')
		else:
			pageId=0
		if safely_get_data(result, 'generationTimeMilliseconds'):
			generationTimeMilliseconds=safely_get_data(result, 'generationTimeMilliseconds')
		else:
			generationTimeMilliseconds=0
		if safely_get_data(result, 'generationTime'):
			generationTime=safely_get_data(result, 'generationTime')
		else:
			generationTime='none'
		interactionPosition=safely_get_data(result, 'interactionPosition')
		if safely_get_data(result, 'icon'):
			icon=safely_get_data(result, 'icon')
		else:
			icon='none'
		timestamp=safely_get_data(result, 'timestamp')
		insert_hits = Hits(
			Type=Type,
			url=url,
			pageTitle=pageTitle,
			pageIdAction=pageIdAction,
			serverTimePretty=serverTimePretty,
			pageId=pageId,
			generationTimeMilliseconds=generationTimeMilliseconds,
			generationTime=generationTime,
			interactionPosition=interactionPosition,
			icon=icon,
			timestamp=timestamp,
			)
		
# appends data into couple
		hits_buffer.append(insert_hits)
		result.clear()
# open database with database name and database host values
	db = Database(db_name, db_url=db_host)
# create table to insert prepared data
	db.create_table(Visits)
# insert prepared data into database
	db.insert(visits_buffer)
	db.create_table(Hits)
	db.insert(hits_buffer)
