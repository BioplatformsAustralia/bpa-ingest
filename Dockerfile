#
# docker run --rm \
#        --interactive \
#        --tty \
#        -v "$(pwd):$(pwd)" \
#        -w $(pwd) \
#        muccg/bpaingest
#
FROM python:3.6-alpine
LABEL maintainer "https://github.com/muccg"

ENV VIRTUAL_ENV /env
ENV PIP_NO_CACHE_DIR="off"
ENV PYTHON_PIP_VERSION 9.0.1
ENV PYTHONIOENCODING=UTF-8

RUN apk --no-cache add \
    ca-certificates \
    git

RUN python -m venv $VIRTUAL_ENV \
    && $VIRTUAL_ENV/bin/pip install --upgrade \
    pip==$PYTHON_PIP_VERSION

ENV PATH $VIRTUAL_ENV/bin:$PATH

WORKDIR /app

COPY requirements.txt /app/
RUN pip install --upgrade -r requirements.txt
        
# Copy code and install the app
COPY . /app
RUN pip install --upgrade -e .

RUN addgroup -g 1000 bpa \
    && adduser -D -h /data -H -S -u 1000 -G bpa bpa \
    && mkdir /data \
    && chown bpa:bpa /data

USER bpa

ENTRYPOINT ["/env/bin/bpa-ingest"]
