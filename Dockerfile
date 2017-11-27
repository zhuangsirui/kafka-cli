FROM golang:1.9.2-alpine3.6
MAINTAINER Zhuang Sirui "zhuangsirui@gmail.com"

WORKDIR /go/src/kafka-cli

COPY . .

RUN go install .
