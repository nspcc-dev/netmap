FROM golang:1.11-alpine3.8 as builder

RUN set -x \
    && apk add --no-cache git \
    && mkdir -p /tmp

COPY . /nmrepl

WORKDIR /nmrepl

# https://github.com/golang/go/wiki/Modules#how-do-i-use-vendoring-with-modules-is-vendoring-going-away
# go build -mod=vendor
RUN set -x \
    && export CGO_ENABLED=0 \
    && go build -mod=vendor -o /go/bin/nmrepl ./main.go

# Executable image
FROM alpine:3.8

RUN apk add --update --no-cache \
    graphviz \
    ttf-freefont

COPY --from=builder /go/bin/nmrepl /usr/local/sbin/nmrepl

ADD examples /examples

ENTRYPOINT /usr/local/sbin/nmrepl