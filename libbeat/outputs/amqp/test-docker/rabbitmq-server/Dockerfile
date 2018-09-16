FROM debian:jessie-slim

ENV TINI_VERSION v0.18.0
ADD https://github.com/krallin/tini/releases/download/${TINI_VERSION}/tini /opt/tini
RUN chmod +x /opt/tini

COPY build.sh /opt/build.sh
RUN /opt/build.sh

ENV RABBITMQ_NODE_PORT 5672
ENV RABBITMQ_DIST_PORT 25672
ENV RABBITMQ_LOGS -
ENV RABBITMQ_SASL_LOGS -

EXPOSE ${RABBITMQ_NODE_PORT}
EXPOSE ${RABBITMQ_DIST_PORT}

RUN ["rabbitmq-plugins", "enable", "rabbitmq_management"]
EXPOSE 15672

ENTRYPOINT ["/opt/tini", "--"]

# note: if you're just running this to test things you need a way to log in
#       because you likely won't be seen as a 'localhost' (guest/guest) user
#       see: https://stackoverflow.com/questions/23669780/rabbitmq-3-3-1-can-not-login-with-guest-guest
#       you can do this sort of thing using 'docker exec' to run commands (like
#       bash perhaps) inside an existing container

CMD ["rabbitmq-server"]
