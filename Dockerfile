FROM astrocrpublic.azurecr.io/runtime:3.0-14

# Installing OS-level (apt) packages; packages.txt may be empty
COPY packages.txt .
RUN if [ -s packages.txt ]; then \
        apt-get update && \
        xargs -a packages.txt apt-get install -y --no-install-recommends && \
        rm -rf /var/lib/apt/lists/*; \
    fi

# Installing extra Python deps (Airflow itself is provided by the runtime).
# requirements.txt lists only direct deps; keep it in sync with pyproject.toml.
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
