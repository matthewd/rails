# syntax = docker/dockerfile:experimental

ARG RUBY_IMAGE
FROM ${RUBY_IMAGE:-ruby:latest}

ADD https://www.postgresql.org/media/keys/ACCC4CF8.asc /etc/apt/trusted.gpg.d/pgdg.asc
ADD https://deb.nodesource.com/gpgkey/nodesource.gpg.key /etc/apt/trusted.gpg.d/nodesource.asc
ADD https://dl.yarnpkg.com/debian/pubkey.gpg /etc/apt/trusted.gpg.d/yarn.asc

RUN --mount=type=cache,target=/var/cache/apt \
    --mount=type=cache,target=/var/lib/apt \

    echo "--- :package: Installing system deps" \

    && chmod +r /etc/apt/trusted.gpg.d/*.asc \

    # Keep apt caches
    && rm -f /etc/apt/apt.conf.d/docker-clean \
    && echo 'Binary::apt::APT::Keep-Downloaded-Packages "true";' > /etc/apt/apt.conf.d/keep-cache \

    # Postgres apt sources
    && echo "deb http://apt.postgresql.org/pub/repos/apt/ stretch-pgdg main" > /etc/apt/sources.list.d/pgdg.list \
    # Node apt sources
    && echo "deb http://deb.nodesource.com/node_10.x stretch main" > /etc/apt/sources.list.d/nodesource.list \
    # Yarn apt sources
    && echo "deb http://dl.yarnpkg.com/debian/ stable main" > /etc/apt/sources.list.d/yarn.list \

    # Install all the things
    && apt-get update \
    && apt-get install -y --no-install-recommends \
        postgresql-client mysql-client sqlite3 \
        git nodejs yarn lsof \
        ffmpeg mupdf mupdf-tools poppler-utils \
    # await (for waiting on dependent services)
    && cd /tmp \
    && wget -qc https://github.com/betalo-sweden/await/releases/download/v0.4.0/await-linux-amd64 \
    && install await-linux-amd64 /usr/local/bin/await \
    # clean up
    && rm -rf /tmp/* \

    && gem update --system && gem install bundler \
    && ruby --version && gem --version && bundle --version \
    && mkdir -p /rails

WORKDIR /rails
ENV RAILS_ENV=test RACK_ENV=test

ENV JRUBY_OPTS="--dev -J-Xmx1024M"

ADD actioncable/package.json actioncable/
ADD actiontext/package.json actiontext/
ADD actionview/package.json actionview/
ADD activestorage/package.json activestorage/
ADD package.json yarn.lock .yarnrc ./

RUN --mount=type=cache,target=/usr/local/share/.cache/yarn \
    echo "--- :javascript: Installing JavaScript deps" \
    && yarn install

ADD */*.gemspec tmp/
ADD railties/exe/ railties/exe/
ADD Gemfile Gemfile.lock RAILS_VERSION rails.gemspec ./

RUN --mount=type=cache,target=/usr/local/bundle/cache \
    echo "--- :bundler: Installing Ruby deps" \
    && (cd tmp && for f in *.gemspec; do d="$(basename -s.gemspec "$f")"; mkdir -p "../$d" && mv "$f" "../$d/"; done) \
    && rm Gemfile.lock && bundle install -j 8 && cp Gemfile.lock tmp/Gemfile.lock.updated \
    && echo "--- :floppy_disk: Copying repository contents"

ADD activerecord/test/config.example.yml activerecord/test/config.yml
ADD . ./

RUN mv -f tmp/Gemfile.lock.updated Gemfile.lock
