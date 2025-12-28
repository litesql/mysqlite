.PHONY: compile
compile:
	go build -ldflags="-s -w" -buildmode=c-shared -o mysqlite.so

.PHONY: builder
builder:
	docker build -f Dockerfile-builder -t builder-mysqlite .

.PHONY: snapshot
snapshot: builder
	docker run -e GITHUB_TOKEN=${GITHUB_TOKEN} builder-mysqlite goreleaser release --clean --snapshot --skip publish
.PHONY: release
release: builder
	docker run -e GITHUB_TOKEN=${GITHUB_TOKEN} builder-mysqlite goreleaser release --clean