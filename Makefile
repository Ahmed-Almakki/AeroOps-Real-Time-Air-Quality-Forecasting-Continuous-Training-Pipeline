run: start init_model sensor_reads

start:
	docker compose up -d --wait

destroy:
	docker compose down -v

init_model:
	python -m warmup_model.initail_model

sensor_reads:
	python -m simulator.sensors_reads

hourly_reads:
	python -m simulator.hourly

unit_test:
	pytest tests/unit/

integration_test:
	pytest test/integration
