B=\033[0;1m
G=\033[0;92m
R=\033[0m

NAME ?= nmrepl
DIR = ${CURDIR}

LIBS:=grpc

.PHONY: help attach auto up down
# Show this help prompt
help:
	@echo '  Usage:'
	@echo ''
	@echo '    make <target>'
	@echo ''
	@echo '  Targets:'
	@echo ''
	@awk '/^#/{ comment = substr($$0,3) } comment && /^[a-zA-Z][a-zA-Z0-9_-]+ ?:/{ print "   ", $$1, comment }' $(MAKEFILE_LIST) | column -t -s ':' | grep -v 'IGNORE' | sort | uniq

down:
	@echo "\n${B}${G}Stop container${R}\n"
	@docker stop netmap-demo || true
	@docker rm netmap-demo || true

up: down
	@echo "\n${B}${G}build container${R}\n"
	@time docker build -t nmrepl .
	@echo "\n${B}${G}enter inside container:${R}\n"
	@time docker run -v ${DIR}/temp:/pics --rm -it --name netmap-demo nmrepl:latest

# Regenerate proto files:
protoc:
# ^ protoc: LIBS:=$(LIBS),Mpath/to/another/lib
	@find . -type f -name '*.proto' -not -path './vendor/*' \
		-exec protoc \
		--proto_path=$(GOPATH)/src:. \
		--gofast_out=plugins=$(LIBS):. '{}' \;

