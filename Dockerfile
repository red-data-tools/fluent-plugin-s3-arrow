FROM ruby:2.7-buster

RUN apt update && \
    apt install -y -V ca-certificates lsb-release wget time gzip && \
    wget https://apache.bintray.com/arrow/$(lsb_release --id --short | tr 'A-Z' 'a-z')/apache-arrow-archive-keyring-latest-$(lsb_release --codename --short).deb && \
    apt install -y -V ./apache-arrow-archive-keyring-latest-$(lsb_release --codename --short).deb

RUN apt update && \
    apt install -y -V libparquet-glib-dev

RUN mkdir /app
WORKDIR /app
COPY . /app
RUN apt update && bundle install
