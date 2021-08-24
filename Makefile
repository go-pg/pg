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

.PHONY: test
test:
	TZ= PGSSLMODE=disable go test ./... -v

tag:
	git tag $(VERSION)
	git tag extra/pgdebug/$(VERSION)
	git tag extra/pgotel/$(VERSION)
	git tag extra/pgsegment/$(VERSION)
