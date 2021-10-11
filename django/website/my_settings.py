DATABASES = {
    'default': {
        'ENGINE': 'django.db.backends.mysql',
        'NAME': 'finalPJT',
        'USER': 'mulcam',
        'PASSWORD': '*****',
        'HOST': '*****',
        'PORT': '****',
    }
}
# python manage.py inspectdb 실행 후 출력된 db 찾아서 복사
SECRET_KEY = 'django-insecure-************'
