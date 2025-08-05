FROM astrocrpublic.azurecr.io/runtime:3.0-6

# Temporarily switch to root to install OS packages
USER root

# Install system packages for building any remaining wheels (e.g. numpy)
RUN apt-get update \
 && apt-get install -y --no-install-recommends \
      build-essential \
      python3-distutils \
 && rm -rf /var/lib/apt/lists/*

# Switch back to the astro user
USER astro

# (Optional) allow connection tests from the Airflow UI
ENV AIRFLOW__WEBSERVER__CONNECTIONS__ALLOW_CONNECTION_TEST=True

# Copy in & install your extra Python deps 
COPY requirements.txt .
RUN pip install --no-cache-dir --root-user-action=ignore -r requirements.txt
