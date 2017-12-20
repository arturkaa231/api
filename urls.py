from django.conf.urls import url
from api import views

urlpatterns=[
	url(r'^stat/segment_stat/*$',views.segment_stat, name='segment_stat'),
	url(r'^diagram_stat/$',views.diagram_stat, name='diagram_stat'),
	url(r'^stat/$',views.CHapi, name='CHapi'),
	]
