FROM python:3.9

ENV PYTHONUNBUFFERED True
ENV APP_HOME .
WORKDIR $APP_HOME

# Ensure the directory exists, download the new CA certificate bundle, and set permissions
RUN mkdir -p /etc/ssl/certs && \
    wget https://truststore.pki.rds.amazonaws.com/global/global-bundle.pem -O /etc/ssl/certs/global-bundle.pem && \
    chmod 644 /etc/ssl/certs/global-bundle.pem

COPY . ./

RUN pip install Flask flask-cors pymongo gunicorn

# Set the environment variable to use the new CA bundle
ENV SSL_CERT_FILE=/etc/ssl/certs/global-bundle.pem

CMD exec gunicorn -b 0.0.0.0:8000 --workers 1 --threads 8 --timeout 0 main:app
