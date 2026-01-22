FROM python:3.11-slim
LABEL service="db"

# ----------------------------
# Runtime basics
# ----------------------------

WORKDIR /app

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# ----------------------------
# Dependencies
# ----------------------------

COPY db/requirements.txt .
RUN pip install --no-cache-dir --upgrade pip \
 && pip install --no-cache-dir -r requirements.txt


# ----------------------------
# Application code
# ----------------------------

COPY db/ ./db
COPY cache/ ./cache

# ----------------------------
# Runtime
# ----------------------------

EXPOSE 8001

ENV PYTHONPATH=/app
CMD ["uvicorn", "db.service:app", "--host", "0.0.0.0", "--port", "8001"]
