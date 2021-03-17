from django.conf.urls import url
from restapi.views import *


urlpatterns = [
    url(r'api/process-logs/', get_process),
    url(r'', index),
]
