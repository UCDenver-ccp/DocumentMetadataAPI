FROM python:3.9

ENV PYTHONUNBUFFERED True

ENV APP_HOME .
WORKDIR $APP_HOME
COPY . ./

RUN pip install Flask pymongo gunicorn

CMD exec gunicorn -b 0.0.0.0:8000 --workers 1 --threads 8 --timeout 0 main:app