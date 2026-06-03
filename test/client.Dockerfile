FROM alpine:3.20

RUN apk add --no-cache iproute2 iputils bash ca-certificates

COPY vnt2_cli /usr/local/bin/vnt2_cli
COPY vnt2_ctrl /usr/local/bin/vnt2_ctrl

RUN chmod +x /usr/local/bin/vnt2_cli /usr/local/bin/vnt2_ctrl

WORKDIR /work
CMD ["bash"]
