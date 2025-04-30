## How to run and edit locally

Pre-requisites
- Docker

1. In terminal change directory to `web`
```
cd web
```
1. Build docker image
```
docker build -t asha-foundation/web-app .
```
1. Run docker container
```
docker run -p 8501:8501 -it --name asha-foundation-web-app -v "$(pwd):/app" asha-foundation/web-app:latest
```
