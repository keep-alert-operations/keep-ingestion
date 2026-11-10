# syntax=docker/dockerfile:1
# Build from the keep-ingestion repository root. Supply a reviewed, mirrored
# Kong image digest; no infrastructure version is selected by this Dockerfile.
ARG KONG_BASE_IMAGE
FROM ${KONG_BASE_IMAGE}
USER root
COPY kong/keep-policy /usr/local/share/lua/5.1/kong/plugins/keep-policy
RUN chgrp -R 0 /usr/local/share/lua/5.1/kong/plugins/keep-policy \
    && chmod -R g+rX /usr/local/share/lua/5.1/kong/plugins/keep-policy
ENV KONG_PLUGINS=bundled,keep-policy \
    KONG_PREFIX=/tmp/kong
# OpenShift supplies its namespace UID. Keep writable state on a mounted /tmp;
# database credentials, TLS material and clock evidence are runtime inputs.
USER 1000:0
