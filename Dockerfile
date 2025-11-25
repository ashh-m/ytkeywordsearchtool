# Base: Apify’s official Python image
FROM apify/actor-python:3.11

# Workdir
WORKDIR /usr/src/app

# Install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Install Playwright browsers (Chromium) and OS deps
# --with-deps will apt-get the necessary system packages in the build stage
RUN python -m playwright install --with-deps chromium

# Copy the rest of the app
COPY . .

# Run the actor
ENTRYPOINT ["python3", "main.py"]