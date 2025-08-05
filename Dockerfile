# Use the official Astro Runtime image for Airflow 3.x
FROM astrocrpublic.azurecr.io/runtime:3.0-6

# Switch to root to install OS-level build tools
USER root

# Install build dependencies needed for compiling wheels (e.g. numpy, pandas)
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
      build-essential \
      python3-distutils \
      libffi-dev \
      libssl-dev && \
    rm -rf /var/lib/apt/lists/*

# Switch back to the non-root Astro user
USER astro

# Allow “Test Connection” in the Airflow UI
ENV AIRFLOW__WEBSERVER__CONNECTIONS__ALLOW_CONNECTION_TEST=True

# Copy in your extra Python dependencies
COPY requirements.txt /tmp/requirements.txt

# Install your Python dependencies
RUN pip install --upgrade pip && \
    pip install --no-cache-dir --root-user-action=ignore -r /tmp/requirements.txt