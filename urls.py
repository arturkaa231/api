from django.conf.urls import url
from CH import views

urlpatterns=[
	url(r'^stat/$',views.CHapi, name='CHapi'),]
