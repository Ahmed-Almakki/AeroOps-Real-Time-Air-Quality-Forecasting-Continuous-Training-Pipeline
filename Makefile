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

logs_mlflow:
	docker logs air_quality_system-main_flow-1
