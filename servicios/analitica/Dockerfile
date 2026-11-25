FROM apache/spark:3.5.3-python3

USER root
WORKDIR /app
COPY requirements.txt .
RUN (python3 -m pip --version >/dev/null 2>&1 || (apt-get update && apt-get install -y python3-pip)) \
    && python3 -m pip install --no-cache-dir -r requirements.txt
COPY main.py .

# El driver de Spark corre dentro de este contenedor (modo cliente) y los
# executors se conectan de vuelta a el por los puertos fijos 7078 y 7079.
USER spark
CMD ["python3", "-m", "uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8004"]