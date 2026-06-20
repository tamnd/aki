# Consumed by GoReleaser: it copies the already cross-compiled binary out of the
# build context rather than compiling, so the image build is fast and uses the
# same static binary every other artifact ships.
#
# GoReleaser builds one multi-platform image with buildx and stages each
# platform's binary under a $TARGETPLATFORM directory (e.g. linux/amd64/) in the
# build context, so the COPY line selects the right one through the automatic
# TARGETPLATFORM build arg.
FROM alpine:3.21

ARG TARGETPLATFORM

# ca-certificates for TLS; tzdata for sane timestamps in logs.
RUN apk add --no-cache ca-certificates tzdata \
 && adduser -D -H -u 10001 aki \
 && mkdir -p /data \
 && chown aki:aki /data

COPY $TARGETPLATFORM/aki /usr/bin/aki

USER aki
WORKDIR /data

# The database file lives under /data; mount a volume to keep it across runs:
#
#   docker run -v ~/data/aki:/data ghcr.io/tamnd/aki check /data/dump.aki
VOLUME ["/data"]

# Default port matches Redis. The server subcommand arrives with M1.
EXPOSE 6379

ENTRYPOINT ["/usr/bin/aki"]
