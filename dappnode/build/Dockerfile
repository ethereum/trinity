FROM python:3.7

WORKDIR /usr/src/app

# Install deps
RUN apt-get update
RUN apt-get -y install libsnappy-dev gcc g++ cmake

ARG GITREF=v0.1.0-alpha.36
# For regular releases a changing GITREF already ensures to invalidate the cache.
# However, when making continous releases from e.g. a `dev` branch, we need to make
# sure something invalidates the cache.
# See: https://stackoverflow.com/questions/36996046/how-to-prevent-dockerfile-caching-git-clone
ARG CACHE_BUSTER=<make-replaces-this-with-a-timestamp>
RUN git clone https://github.com/ethereum/trinity.git
RUN cd trinity && git checkout $GITREF && pip install -e .[dev] --no-cache-dir

EXPOSE 30303 30303/udp
# Trinity shutdowns aren't yet solid enough to avoid the fix-unclean-shutdown
ENTRYPOINT trinity $EXTRA_OPTS fix-unclean-shutdown && trinity $EXTRA_OPTS
