from django.conf.urls import url
from api import dev_views

urlpatterns=[
	url(r'^stat/segment_stat/*$', dev_views.segment_stat, name='segment_stat'),
	url(r'^diagram_stat/$', dev_views.diagram_stat, name='diagram_stat'),
	url(r'^stat/$', dev_views.CHapi, name='CHapi'),
	]
