docker-compose-dev.yaml: topology.yaml configs/base-compose.yaml.tmpl scripts/gen_compose.go
	go run scripts/gen_compose.go

compose:
	go run scripts/gen_compose.go
.PHONY: compose

up: docker-compose-dev.yaml
	docker compose -f docker-compose-dev.yaml up -d --build
.PHONY: up

down:
	docker compose -f docker-compose-dev.yaml stop -t 1
	docker compose -f docker-compose-dev.yaml down
.PHONY: docker-compose-down

ps:
	docker compose -f docker-compose-dev.yaml ps
.PHONY: ps

logs:
	docker compose -f docker-compose-dev.yaml logs --tail=50 -f
.PHONY: logs

logs-%:
	docker compose -f docker-compose-dev.yaml logs -f $*

logs-tail-%:
	docker compose -f docker-compose-dev.yaml logs --tail=50 -f $*
