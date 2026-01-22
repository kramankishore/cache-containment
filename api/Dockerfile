FROM python:3.11-slim
LABEL service="api"

# ----------------------------
# Runtime basics
# ----------------------------

WORKDIR /app

# Prevent Python from writing .pyc files
ENV PYTHONDONTWRITEBYTECODE=1

# Ensure stdout/stderr are unbuffered
ENV PYTHONUNBUFFERED=1

# ----------------------------
# Dependencies
# ----------------------------

COPY api/requirements.txt .
RUN pip install --no-cache-dir --upgrade pip \
 && pip install --no-cache-dir -r requirements.txt

# ----------------------------
# Application code
# ----------------------------

COPY api/ ./api
COPY cache/ ./cache

# ----------------------------
# Runtime
# ----------------------------

EXPOSE 8000

ENV PYTHONPATH=/app
CMD ["uvicorn", "api.app:app", "--host", "0.0.0.0", "--port", "8000"]
