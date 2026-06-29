run:
	@go run .

test:
	@go test -v ./...

build:
	@go build .

clean:
	@rm -f main

.PHONY: run test build clean