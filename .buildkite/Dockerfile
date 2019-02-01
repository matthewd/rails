ARG RUBY_IMAGE
FROM ${RUBY_IMAGE:-ruby:latest}

ENV JRUBY_OPTS="--dev -J-Xmx1024M"

RUN gem update --system && gem install bundler

RUN echo "--- :package: Installing system deps" \
    # Postgres apt sources
    && curl -sS https://www.postgresql.org/media/keys/ACCC4CF8.asc | apt-key add - \
    && echo "deb http://apt.postgresql.org/pub/repos/apt/ stretch-pgdg main" > /etc/apt/sources.list.d/pgdg.list \
    # Node apt sources
    && curl -sS https://deb.nodesource.com/gpgkey/nodesource.gpg.key | apt-key add - \
    && echo "deb http://deb.nodesource.com/node_10.x stretch main" > /etc/apt/sources.list.d/nodesource.list \
    # Yarn apt sources
    && curl -sS https://dl.yarnpkg.com/debian/pubkey.gpg | apt-key add - \
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
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/* /tmp/*

RUN mkdir /rails
WORKDIR /rails

ADD Gemfile Gemfile.lock RAILS_VERSION rails.gemspec ./
ADD */*.gemspec tmp/
ADD railties/exe/ railties/exe/

RUN echo "--- :bundler: Installing Ruby deps" \
    && (cd tmp && for f in *.gemspec; do d="$(basename -s.gemspec "$f")"; mkdir -p "../$d" && mv "$f" "../$d/"; done) \
    && rm Gemfile.lock && bundle install -j 8 && cp Gemfile.lock tmp/Gemfile.lock.updated \
    && echo "--- :floppy_disk: Copying repository contents"

ADD . ./

ENV RAILS_ENV=test RACK_ENV=test

RUN mv -f tmp/Gemfile.lock.updated Gemfile.lock \
    && echo "--- :javascript: Installing JavaScript deps" \
    && yarn install
