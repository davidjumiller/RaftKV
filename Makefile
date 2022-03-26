.PHONY: client tracing clean all

all: server client tracing

server:
	go build -o bin/server ./cmd/server

client:
	go build -o bin/client ./cmd/client

tracing:
	go build -o bin/tracing ./cmd/tracing-server

clean:
	rm -f bin/*
