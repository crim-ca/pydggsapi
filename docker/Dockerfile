FROM python:3.12.11-slim
LABEL description.short="DGGS API"
LABEL description.long="OGC API for DGGS (Discretized Global Grid System) data management and processing."
LABEL maintainer="Francis Charette-Migneault <francis.charette-migneault@crim.ca>"
LABEL vendor="CRIM"
LABEL version="6.6.2"

# setup paths
ENV APP_DIR=/opt/local/src/pydggsapi
ENV APP_CONFIG_DIR=${APP_DIR}/config
WORKDIR ${APP_DIR}

# obtain dependency files
COPY pyproject.toml poetry.lock ${APP_DIR}/

# install runtime/package dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
        ca-certificates \
        gcc \
        g++ \
    && pip install poetry \
    && poetry install --no-root --no-directory \
    && apt-get remove -y \
        gcc \
        g++ \
    && apt-get autoremove -y \
    && rm -rf /var/lib/apt/lists/*

# obtain dependency source files
COPY README.md ${APP_DIR}/
COPY pydggsapi/ ${APP_DIR}/pydggsapi/

# install package (only) incrementally
RUN poetry install --only-root

CMD ["uvicorn", "dggpsapi:app", "--host", "0.0.0.0", "--port", "8000"]
