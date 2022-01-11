FROM alpine:3.7
ARG VERSION=1.12.0
RUN apk add -U --no-cache build-base libevent-dev c-ares-dev libressl-dev python2-dev patch libpq
WORKDIR /tmp
RUN wget https://pgbouncer.github.io/downloads/files/$VERSION/pgbouncer-$VERSION.tar.gz && \
    wget https://github.com/innovate-tech/pgbouncer-rr-patch/archive/v$VERSION.tar.gz && \
    tar -xf pgbouncer-$VERSION.tar.gz && tar -xf v$VERSION.tar.gz  && \
    rm pgbouncer-$VERSION.tar.gz && rm v$VERSION.tar.gz
WORKDIR /tmp/pgbouncer-rr-patch-$VERSION
RUN sh install-pgbouncer-rr-patch.sh /tmp/pgbouncer-$VERSION
WORKDIR /tmp/pgbouncer-$VERSION
RUN ./configure --prefix=/usr/local && make && make install
WORKDIR /tmp/rootfs
RUN mkdir -p usr/bin usr/lib lib && \
    cp /usr/local/bin/pgbouncer usr/bin && \
    for lib in $(ldd usr/bin/pgbouncer | awk '{ print $(NF-1) }'); do \
    cp $lib .$lib; \
    done
RUN apk --update add gcc build-base freetype-dev libpng-dev openblas-dev curl
RUN apk add --no-cache  python-dev py-pip  postgresql postgresql-dev postgresql-libs  libevent libtool 
RUN pip2 install --upgrade pip
RUN export PATH=/usr/pgsql-11/bin/:$PATH
RUN pip2 install setuptools psycopg2-binary psycopg2 regex
COPY entrypoint.sh /entrypoint.sh
COPY routing_rules.py rewrite_query.py pgbouncer.ini users.txt /etc/pgbouncer/

RUN adduser -D -S pgbouncer && \
    mkdir -p  /var/log/pgbouncer /var/run/pgbouncer && \
    chown -R pgbouncer /etc/pgbouncer /var/log/pgbouncer /var/run/pgbouncer /entrypoint.sh && \
    chmod 755 /entrypoint.sh /etc/pgbouncer/routing_rules.py /etc/pgbouncer/rewrite_query.py 
USER pgbouncer
#ENTRYPOINT ["/entrypoint.sh"]
EXPOSE 5439
CMD ["pgbouncer", "/etc/pgbouncer/pgbouncer.ini"]