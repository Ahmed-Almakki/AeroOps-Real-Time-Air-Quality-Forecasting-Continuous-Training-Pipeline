start:
	docker compose up -d --wait

destroy:
	docker compose down -v

init_model:
	python -m warmup_model.initail_model
