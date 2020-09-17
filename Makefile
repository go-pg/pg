all:
	TZ= go test ./...
	TZ= go test ./... -short -race
	TZ= go test ./... -run=NONE -bench=. -benchmem
	env GOOS=linux GOARCH=386 go test ./...
	go vet
	golangci-lint run

.PHONY: cleanTest
cleanTest:
	docker rm -fv pg || true

.PHONY: pre-test
pre-test: cleanTest
	docker run -d --name pg -p 5432:5432 -e POSTGRES_HOST_AUTH_METHOD=trust postgres:9.6
	sleep 10
	docker exec pg psql -U postgres -c "CREATE EXTENSION hstore"

.PHONY: test
test: pre-test
	TZ= PGSSLMODE=disable go test ./... -v
