from django.urls import path
from . import views

urlpatterns = [
    path('', views.index),
    path('강남구.html', views.kangnam),
    path('강동구.html', views.kangdong),
    path('강북구.html', views.kangbook),
    path('강서구.html', views.kangseo),
    path('관악구.html', views.kwanak),
    path('광진구.html', views.kwangjin),
    path('구로구.html', views.guro),
    path('금천구.html', views.gumcheon),
    path('노원구.html', views.nowon),
    path('도봉구.html', views.dobong),
    path('동대문구.html', views.dongdaemoon),
    path('동작구.html', views.dongjak),
    path('마포구.html', views.mapo),
    path('서대문구.html', views.seodaemoon),
    path('서초구.html', views.seocho),
    path('성동구.html', views.seongdong),
    path('성북구.html', views.seongbook),
    path('송파구.html', views.songpa),
    path('양천구.html', views.yangcheon),
    path('영등포구.html', views.youngdeongpo),
    path('용산구.html', views.yongsan),
    path('은평구.html', views.eunpyeong),
    path('종로구.html', views.jongro),
    path('중구.html', views.jungkoo),
    path('중랑구.html', views.jungrang),
    
]