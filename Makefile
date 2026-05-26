docker-compose-dev.yaml: topology.yaml configs/base-compose.yaml.tmpl scripts/gen_compose.go
	go run scripts/gen_compose.go

up: docker-compose-dev.yaml
	docker compose -f docker-compose-dev.yaml up -d --build
.PHONY: up

docker-compose-up:
	docker compose -f docker-compose-dev.yaml up -d --build
.PHONY: docker-compose-up

docker-compose-down:
	docker compose -f docker-compose-dev.yaml stop -t 1
	docker compose -f docker-compose-dev.yaml down
.PHONY: docker-compose-down

docker-compose-logs:
	docker compose -f docker-compose-dev.yaml logs -f
.PHONY: docker-compose-logs
