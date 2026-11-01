FROM python:3.13-slim
RUN apt-get update && apt-get install -y --no-install-recommends g++ default-jre-headless && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY requirements.lock /app/requirements.lock
RUN pip install --no-cache-dir -r requirements.lock
COPY . /app/
RUN g++ -O3 -std=c++17 -pthread -fPIC -shared -Iinclude src/chandystore.cpp -o libchandystore.so
EXPOSE 8000
CMD ["uvicorn", "python.api.main:app", "--host", "0.0.0.0", "--port", "8000"]
