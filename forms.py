from django.forms import ModelForm,fields
from django import forms
class RequestForm(forms.Form):
	flat=forms.CharField(required=False)
	sort_order=forms.CharField(required=False)
	limit=forms.IntegerField(required=False)
	offset=forms.IntegerField(required=False)
	dimensions=forms.CharField(required=False)
	period=forms.DateField(required=False)
	global_filter=forms.CharField(required=False)
