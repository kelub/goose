# local test
build:
	docker-compose build

test:
	docker-compose up --build

clean:
	docker-compose down
	docker image prune -f
