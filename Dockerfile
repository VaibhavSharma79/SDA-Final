# Runtime image for every Python service (engine, producers, alert sink, verify/reset/test jobs).
# Only the dependencies are baked in. The project folder is bind-mounted at /app by docker-compose.yml,
# so editing a .py file or regenerating the data needs a container restart, not an image rebuild.
FROM python:3.12-slim
ENV PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1 PYTEST_ADDOPTS="-p no:cacheprovider"
WORKDIR /app
COPY requirements-docker.txt .
RUN pip install --no-cache-dir -r requirements-docker.txt
