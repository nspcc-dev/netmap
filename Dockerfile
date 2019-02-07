FROM golang:1.11-alpine3.8 as builder

RUN set -x \
    && apk add --no-cache git \
    && mkdir -p /tmp

COPY . /nmrepl

WORKDIR /nmrepl

RUN set -x \
    && export CGO_ENABLED=0 \
    && go build -o /go/bin/nmrepl ./cmd/repl

# Executable image
FROM alpine:3.8

RUN apk add --update --no-cache \
    graphviz \
    ttf-freefont

COPY --from=builder /go/bin/nmrepl /usr/local/sbin/nmrepl

ADD examples /examples

ENTRYPOINT /usr/local/sbin/nmrepl