SHELL = bash
BINDIR = ./bin 

all: pt-align

pt-align: 
	go build -o bin/pt-align go/cmd/pt-align/main.go

test:
	@echo "Hello"