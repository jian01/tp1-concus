FROM python:3.8
COPY backup_utils_package /backup_utils_package
COPY requirements.txt .
RUN pip install -r requirements.txt
COPY backup_server /