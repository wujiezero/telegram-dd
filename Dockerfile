# Use official Python image with pip mirror for mainland China
FROM python:3.12 AS compile-image

# Copy requirements.txt first for better caching
COPY requirements.txt ./

# Install dependencies from requirements.txt with multiple mirror support
RUN pip install --no-cache-dir -r requirements.txt || \
    pip install --no-cache-dir -i https://mirrors.aliyun.com/pypi/simple/ -r requirements.txt || \
    pip install --no-cache-dir -i https://pypi.tuna.tsinghua.edu.cn/simple -r requirements.txt

FROM python:3.12-slim AS run-image

COPY --from=compile-image /usr/local/lib/python3.12/site-packages /usr/local/lib/python3.12/site-packages

WORKDIR /app
COPY *.py ./
COPY templates ./templates
RUN chmod 777 /app/*.py

CMD [ "python3", "./telegram-download-daemon.py" ]
