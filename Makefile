DC := docker compose -f docker-compose-dev.yaml

docker-compose-dev.yaml: topology.yaml configs/base-compose.yaml.tmpl scripts/gen_compose.go
	go run scripts/gen_compose.go

compose:
	rm -f docker-compose-dev.yaml
	$(MAKE) docker-compose-dev.yaml
.PHONY: compose

up: docker-compose-dev.yaml
	$(DC) up -d --build
.PHONY: up

stop:
	$(DC) stop -t 1
.PHONY: stop

kill-%:
	$(DC) kill $*

down:
	$(DC) down -t 1
.PHONY: down

ps:
	$(DC) ps
.PHONY: ps

logs:
	$(DC) logs --tail=50 -f
.PHONY: logs

logs-%:
	$(DC) logs -f $*

logs-tail-%:
	$(DC) logs --tail=50 -f $*