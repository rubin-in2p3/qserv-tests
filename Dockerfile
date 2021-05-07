ARG BASE_IMAGE
FROM $BASE_IMAGE as scaletests-deps

LABEL maintainer="Fabrice Jammes <fabrice.jammes@in2p3.fr>"

RUN apt-get update \
&& apt-get install -y mariadb-client \
&& rm -rf /var/lib/apt/lists/*

WORKDIR /var/www/

RUN pip3 install --upgrade pip

ADD requirements.txt .
RUN pip3 install -r requirements.txt

CMD ["ash"]

#USER qserv
ENV PYTHONPATH=/scaletests/python
ENV PATH="/scaletests/bin:${PATH}"

FROM scaletests-deps
COPY rootfs/scaletests /scaletests
