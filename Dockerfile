FROM apache/airflow:latest

USER root

# OS dependencies
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
       wget \
       curl \
       gnupg \
       xvfb \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Install Google Chrome
RUN wget -q -O - https://dl.google.com/linux/linux_signing_key.pub | gpg --dearmor > /usr/share/keyrings/google-linux.gpg \
    && echo "deb [arch=amd64 signed-by=/usr/share/keyrings/google-linux.gpg] http://dl.google.com/linux/chrome/deb/ stable main" \
       > /etc/apt/sources.list.d/google-chrome.list \
    && apt-get update \
    && apt-get install -y google-chrome-stable \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow

# Selenium modern (pakai Selenium Manager)
# Install Python dependencies
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt

USER root

# Airflow containers run with AIRFLOW_UID and group 0. SeleniumBase downloads
# and patches UC ChromeDriver at runtime, so its driver directory must be
# writable by the shared root group.
RUN chmod -R g+rwX /home/airflow/.local/lib/python*/site-packages/seleniumbase/drivers

USER airflow


# RUN pip install --no-cache-dir selenium
