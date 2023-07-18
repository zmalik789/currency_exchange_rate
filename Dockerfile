FROM python:3.8
RUN apt update
WORKDIR /app
ENV PYTHONPATH "${PYTHONPATH}:/app"
ADD . /app
RUN pip3 install -r /app/requirements.txt
RUN groupadd -g 999 appuser && useradd -r -u 999 -g appuser appuser && chown -R appuser /app
USER appuser
ENV PYTHONUNBUFFERED=1
ENTRYPOINT ["python3"]
CMD ["currency_exchange.py"]
