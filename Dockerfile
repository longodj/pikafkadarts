FROM ubuntu:16.04

MAINTAINER David Longo "longodj@gmail.com"
ARG HUB="pi"
ENV APP /app

RUN echo "Downloading/installing necessary dependencies"
RUN apt-get update && \
	DEBIAN_FRONTEND=noninteractive apt-get install -y \
		apt-utils && \
	DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
		openssl git python3 librdkafka1 libssl-dev build-essential \
		python3-pip python3-dev python3-setuptools python3-wheel

RUN echo "Setting up librdkafka"
RUN mkdir /librdkafka
COPY ./librdkafka /librdkafka
WORKDIR /librdkafka
RUN bash -c "./configure --prefix=/usr"
RUN bash -c "make -j"
RUN bash -c "make install"

RUN echo "Setting up Confluent's Python Kafka library"
RUN mkdir $APP
WORKDIR $APP
COPY quickstart/python/requirements.txt .
COPY quickstart/python/producer.py .
RUN pip3 install -r requirements.txt

WORKDIR $APP
ENTRYPOINT ["python3"]
CMD ["producer.py", "$HUB"] 


