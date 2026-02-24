.PHONY: run build generate-testdata seed test tidy clean

run:
	go run ./cmd/server

build:
	go build -o bin/server ./cmd/server

generate-testdata:
	go run ./testdata/generate

seed:
	@echo "Seeding is automatic on first run"

test:
	go test ./... -race

tidy:
	go mod tidy

clean:
	rm -f bin/server wakala.db
