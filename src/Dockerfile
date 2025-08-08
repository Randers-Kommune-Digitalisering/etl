FROM python:3.10-slim

ENV GROUP_NAME=app
ENV HOME=/app
ENV GROUP_ID=11000
ENV USER_ID=11001
ENV PORT=8080
ENV CHROME_BIN=/usr/bin/chromium
ENV CHROMEDRIVER=/usr/bin/chromedriver
ENV SE_CACHE_PATH=/tmp/selenium_cache

# Add user and group
RUN groupadd --gid $GROUP_ID $GROUP_NAME && \
    useradd --uid $USER_ID --gid $GROUP_ID --home-dir $HOME --create-home app

RUN apt-get update && \
    apt-get install --yes --no-install-recommends \
        build-essential \
        libmariadb-dev \
        libpq-dev \
        chromium \
        chromium-driver && \
    rm -rf /var/lib/apt/lists/*

RUN chmod a+x /usr/bin/chromedriver && \
    chmod a+x /usr/bin/chromium

RUN mkdir -p /tmp/selenium_cache && chmod -R 777 /tmp/selenium_cache

# Copy app files
COPY . $HOME
WORKDIR $HOME

# Set permissions
RUN chown -R $USER_ID:$GROUP_ID $HOME && chmod -R 775 $HOME

# Install Python dependencies
RUN pip install --upgrade pip && \
    pip install -r requirements.txt

EXPOSE $PORT

USER $USER_ID

ENTRYPOINT ["python"]
CMD ["main.py"]