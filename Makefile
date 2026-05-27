docker-compose-dev.yaml: topology.yaml configs/base-compose.yaml.tmpl scripts/gen_compose.go
	go run scripts/gen_compose.go

compose:
	go run scripts/gen_compose.go
.PHONY: compose

up: docker-compose-dev.yaml
	docker compose -f docker-compose-dev.yaml up -d --build
.PHONY: up

docker-compose-up:
	docker compose -f docker-compose-dev.yaml up -d --build
.PHONY: docker-compose-up

down:
	docker compose -f docker-compose-dev.yaml stop -t 1
	docker compose -f docker-compose-dev.yaml down
.PHONY: docker-compose-down

logs:
	docker compose -f docker-compose-dev.yaml logs -f
.PHONY: logs

logs-%:
	docker compose -f docker-compose-dev.yaml logs -f $*
