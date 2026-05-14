FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY bot.py /app/bot.py
COPY src /app/src

RUN chmod -R a+rX /app/bot.py /app/src

ENV PYTHONPATH=/app/src

EXPOSE 8080

CMD ["python", "-u", "-m", "itsm_agent.main"]
