FROM python:3.7

WORKDIR /usr/src/app

# Install deps
RUN apt-get update
RUN apt-get -y install libsnappy-dev gcc g++ cmake

ARG GIT_REPOSITORY=ethereum/trinity
ARG GITREF=v0.1.0-alpha.36

RUN git clone https://github.com/$GIT_REPOSITORY.git
RUN cd trinity && git checkout $GITREF && pip install -e .[dev] --no-cache-dir

EXPOSE 30303 30303/udp
# Trinity shutdowns aren't yet solid enough to avoid the fix-unclean-shutdown
ENTRYPOINT trinity $EXTRA_OPTS fix-unclean-shutdown && trinity $EXTRA_OPTS
