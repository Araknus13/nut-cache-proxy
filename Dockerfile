FROM python:3.12-slim

# No third-party dependencies -- standard library only.
WORKDIR /app
COPY nut_proxy.py .

EXPOSE 3493

CMD ["python3", "-u", "nut_proxy.py"]
