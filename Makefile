.PHONY: build test itest db-up db-down run
build:
	go build ./...
test:
	go test ./...
itest: db-up
	MEM_TEST_CH_ADDR=localhost:9000 go test -count=1 ./...
db-up:
	docker compose up -d --wait clickhouse dgraph
db-down:
	docker compose down -v
run:
	go run ./cmd/memserved
