.PHONY: test
test:
	go test -cover -coverprofile=coverage.out ./...
	go tool cover -html=coverage.out