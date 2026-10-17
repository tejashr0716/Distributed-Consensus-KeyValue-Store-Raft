FROM python:3.11-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    clang \
    cmake \
    libpq-dev \
    curl \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY include/ /app/include/
COPY src/ /app/src/

RUN clang++ -O3 -fPIC -shared -std=c++17 -I./include src/chandystore.cpp -o libchandystore.dylib

COPY requirements.txt /app/
RUN pip install --no-cache-dir -r requirements.txt

COPY . /app/

EXPOSE 8000

CMD ["uvicorn", "python.api.main:app", "--host", "0.0.0.0", "--port", "8000"]
