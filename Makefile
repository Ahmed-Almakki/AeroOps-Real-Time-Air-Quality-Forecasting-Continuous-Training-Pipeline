start_all: start_full init_model sensor_reads

start_core:
	docker compose up -d --wait

start_full:
	docker compose --profile monitoring up -d --wait

teardown:
	docker compose down -v

build_images:
	docker build -f Dockerfile -t prefect-pollution:0.0.8 .
	docker build -f Dockerfile.db -t pg:0.0.8 .
	docker build -f Dockerfile.flow -t main_flow:0.0.5 .
	docker build -f Dockerfile.kafka -t kafka_connect:0.0.1 .
	docker pull confluentinc/cp-kafka:7.6.0
	docker pull testcontainers/ryuk:0.8.1
# 	docker build -f Dockerfile.ui -t ui:0.0.5 .


# Python Scripts

init_model:
	python -m warmup_model.initail_model

sensor_reads:
	python -m simulator.sensors_reads

hourly_reads:
	python -m simulator.hourly


# testing

test_unit:
	python -m pytest test/unit/

test_integration:
	python -m pytest test/integration/
