FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY simulador.py .
COPY carga.py .
COPY usuarios_p6.txt .

ENTRYPOINT ["python"]
CMD ["simulador.py"]