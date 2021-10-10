from django.shortcuts import render
from dashboard.traffic_predict import model_load
from numpyencoder import NumpyEncoder
import json

# Create your views here.

def index(request):
    d1, d2, d3, d4, d5 = model_load()
    return render(request, 'dashboard/index.html', {
                                        'd1': d1.to_json(),
                                        'd2': d2.to_json(),
                                        'd3': d3.to_json(),
                                        'd4': d4.to_json(),
                                        'd5': d5.to_json()
                                        })

def kangnam(request):
    return render(request, 'dashboard/details/강남구.html')

def kangdong(request):
    return render(request, 'dashboard/details/강동구.html')    

def kangbook(request):
    return render(request, 'dashboard/details/강북구.html')

def kangseo(request):
    return render(request, 'dashboard/details/강서구.html')

def kwanak(request):
    return render(request, 'dashboard/details/관악구.html')

def kwangjin(request):
    return render(request, 'dashboard/details/광진구.html')

def guro(request):
    return render(request, 'dashboard/details/구로구.html')

def gumcheon(request):
    return render(request, 'dashboard/details/금천구.html')

def nowon(request):
    return render(request, 'dashboard/details/노원구.html')

def dobong(request):
    return render(request, 'dashboard/details/도봉구.html')

def dongdaemoon(request):
    return render(request, 'dashboard/details/동대문구.html')

def dongjak(request):
    return render(request, 'dashboard/details/동작구.html')

def mapo(request):
    return render(request, 'dashboard/details/마포구.html')

def seodaemoon(request):
    return render(request, 'dashboard/details/서대문구.html')

def seocho(request):
    return render(request, 'dashboard/details/서초구.html')

def seongdong(request):
    return render(request, 'dashboard/details/성동구.html')

def seongbook(request):
    return render(request, 'dashboard/details/성북구.html')

def songpa(request):
    return render(request, 'dashboard/details/송파구.html')

def yangcheon(request):
    return render(request, 'dashboard/details/양천구.html')

def youngdeongpo(request):
    return render(request, 'dashboard/details/영등포구.html')

def yongsan(request):
    return render(request, 'dashboard/details/용산구.html')

def eunpyeong(request):
    return render(request, 'dashboard/details/은평구.html')

def jongro(request):
    return render(request, 'dashboard/details/종로구.html')

def jungkoo(request):
    return render(request, 'dashboard/details/중구.html')

def jungrang(request):
    return render(request, 'dashboard/details/중랑구.html')