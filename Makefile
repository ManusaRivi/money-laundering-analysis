DC := docker compose -f docker-compose-dev.yaml

docker-compose-dev.yaml: topology.yaml configs/base-compose.yaml.tmpl configs/workers.yaml scripts/gen_compose.go
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

stop-monitors:
	$(DC) stop -t 1 $(shell docker compose -f docker-compose-dev.yaml config --services | grep monitor) || true
.PHONY: stop-monitors

down: stop-monitors
	$(DC) down -t 1
.PHONY: down

clean: stop-monitors
	$(DC) down -v -t 1
	docker run --rm \
		-v $(PWD)/.checkpoints:/checkpoints \
		-v $(PWD)/.output:/output \
		-v $(PWD)/.data:/data \
		alpine sh -c "rm -rf /checkpoints/* /output/*"
.PHONY: clean

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

logs-monitors:
	$(DC) logs --tail=50 -f $(shell docker compose -f docker-compose-dev.yaml config --services | grep monitor) || true

chaos:
	go run ./chaos_monkey

chaos-i-%:
	go run ./chaos_monkey -i $*

chaos-q-%:
	go run ./chaos_monkey -q $*

chaos-s-%:
	go run ./chaos_monkey -s $*

chaos-nuke:
	go run ./chaos_monkey -nuke

verify:
	cd ./scripts && . .venv/bin/activate && python3 verify_output.py