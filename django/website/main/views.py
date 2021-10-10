from django.shortcuts import render
from django.http import HttpResponse
# from .resultpy import model_load
# from .lstm import model_load2
from django.core.files.storage import FileSystemStorage


def index(request):
    return render(request, 'main/index.html')


# def index(request):
#     pred_y = model_load()
#     return render(request, 'main/index.html', {'pred_y': pred_y})


# def index(request):
#     graph = model_load2()
#     return render(request, 'main/index.html', {'graph': graph})
