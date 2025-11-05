FROM nvidia/cuda:13.0.1-devel-ubuntu22.04

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update && apt-get install -y --no-install-recommends \
    python3-pip \
    ffmpeg \
    build-essential \
    libxml2-dev \
    libxslt1-dev \
    wget \
    unzip \
 && wget -q https://www.bok.net/Bento4/binaries/Bento4-SDK-1-6-0-641.x86_64-unknown-linux.zip -O /tmp/bento4.zip \
 && unzip /tmp/bento4.zip -d /tmp/bento4 \
 && cp -r /tmp/bento4/Bento4-SDK-1-6-0-641.x86_64-unknown-linux/bin/* /usr/bin/ \
 && rm -rf /tmp/bento4* \
 && apt-get clean \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY GUI/requirements.txt ./GUI/requirements.txt
RUN pip install --no-cache-dir -r GUI/requirements.txt

COPY . .

ENV PYTHONPATH="/app:${PYTHONPATH}"

RUN cd GUI && python3 manage.py migrate

EXPOSE 8000

CMD ["python3", "GUI/manage.py", "runserver", "0.0.0.0:8000"]