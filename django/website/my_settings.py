DATABASES = {
    'default': {
        'ENGINE': 'django.db.backends.mysql',
        'NAME': 'finalPJT',
        'USER': 'mulcam',
        'PASSWORD': 'mulcam',
        'HOST': '3.37.159.174',
        'PORT': '3306',
    }
}
# python manage.py inspectdb 실행 후 출력된 db 찾아서 복사
SECRET_KEY = 'django-insecure-zkai4llctyqc+z#c-c7w2y+43+2kh$70-6nb^8a+9gsge3898!'