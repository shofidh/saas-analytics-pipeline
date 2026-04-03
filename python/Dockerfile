FROM python:3.10-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# Keep container alive so we can exec into it
CMD ["tail", "-f", "/dev/null"]