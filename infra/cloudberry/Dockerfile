FROM ubuntu:22.04

ARG CLOUDBERRY_DEB=apache-cloudberry-db-incubating_2.1.0-1-ubuntu22.04_amd64.deb
ARG CLOUDBERRY_BASE_URL=https://github.com/apache/cloudberry/releases/download/2.1.0-incubating

RUN apt-get update && apt-get install -y --no-install-recommends \
      ca-certificates curl openssh-server sudo procps iproute2 \
    && useradd -m -s /bin/bash gpadmin \
    && curl -fsSL "${CLOUDBERRY_BASE_URL}/${CLOUDBERRY_DEB}" -o "/tmp/${CLOUDBERRY_DEB}" \
    && curl -fsSL "${CLOUDBERRY_BASE_URL}/${CLOUDBERRY_DEB}.sha512" -o "/tmp/${CLOUDBERRY_DEB}.sha512" \
    && cd /tmp && sha512sum -c "${CLOUDBERRY_DEB}.sha512" \
    && apt-get install -y --no-install-recommends "/tmp/${CLOUDBERRY_DEB}" \
    && chown -R gpadmin:gpadmin /usr/local/cloudberry* \
    && mkdir -p /run/sshd /var/lib/cloudberry \
    && chown gpadmin:gpadmin /var/lib/cloudberry \
    && rm -f "/tmp/${CLOUDBERRY_DEB}" "/tmp/${CLOUDBERRY_DEB}.sha512" \
    && rm -rf /var/lib/apt/lists/*

COPY entrypoint.sh /usr/local/bin/cloudberry-entrypoint
RUN chmod +x /usr/local/bin/cloudberry-entrypoint
EXPOSE 7000
ENTRYPOINT ["/usr/local/bin/cloudberry-entrypoint"]
