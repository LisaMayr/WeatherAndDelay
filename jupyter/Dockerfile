ARG JUPYTER_IMAGE=jupyter/pyspark-notebook:latest
FROM ${JUPYTER_IMAGE}

USER root
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt
USER $NB_UID
