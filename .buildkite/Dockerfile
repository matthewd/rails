# syntax = docker/dockerfile:experimental

FROM buildpack-deps:stretch AS ruby-trunk

# skip installing gem documentation
RUN mkdir -p /usr/local/etc \
	&& { \
		echo 'install: --no-document'; \
		echo 'update: --no-document'; \
	} >> /usr/local/etc/gemrc

ARG TRUNK_DATE

# some of ruby's build scripts are written in ruby
#   we purge system ruby later to make sure our final image uses what we just built
RUN set -ex \
	\
	&& buildDeps=' \
		bison \
		dpkg-dev \
		libgdbm-dev \
		ruby \
	' \
	&& apt-get update \
	&& apt-get install -y --no-install-recommends $buildDeps \
	&& rm -rf /var/lib/apt/lists/* \
	\
	&& wget -O ruby.tar.gz "https://github.com/ruby/ruby/archive/trunk.tar.gz" \
	\
	&& mkdir -p /usr/src/ruby \
	&& tar -xzf ruby.tar.gz -C /usr/src/ruby --strip-components=1 \
	&& rm ruby.tar.gz \
	\
	&& cd /usr/src/ruby \
	\
# hack in "ENABLE_PATH_CHECK" disabling to suppress:
#   warning: Insecure world writable dir
	&& { \
		echo '#define ENABLE_PATH_CHECK 0'; \
		echo; \
		cat file.c; \
	} > file.c.new \
	&& mv file.c.new file.c \
	\
	&& autoconf \
	&& gnuArch="$(dpkg-architecture --query DEB_BUILD_GNU_TYPE)" \
	&& ./configure \
		--build="$gnuArch" \
		--disable-install-doc \
		--enable-shared \
	&& make -j "$(nproc)" \
	&& make install \
	\
	&& apt-get purge -y --auto-remove $buildDeps \
	&& cd / \
	&& rm -r /usr/src/ruby \
# rough smoke test
	&& ruby --version && gem --version && bundle --version

# install things globally, for great justice
# and don't create ".bundle" in all our apps
ENV GEM_HOME /usr/local/bundle
ENV BUNDLE_PATH="$GEM_HOME" \
	BUNDLE_SILENCE_ROOT_WARNING=1 \
	BUNDLE_APP_CONFIG="$GEM_HOME"
# path recommendation: https://github.com/bundler/bundler/pull/6469#issuecomment-383235438
ENV PATH $GEM_HOME/bin:$BUNDLE_PATH/gems/bin:$PATH
# adjust permissions of a few directories for running "gem install" as an arbitrary user
RUN mkdir -p "$GEM_HOME" && chmod 777 "$GEM_HOME"
# (BUNDLE_PATH = GEM_HOME, no need to mkdir/chown both)

CMD [ "irb" ]


ARG RUBY_IMAGE
FROM ${RUBY_IMAGE:-ruby:latest}

RUN echo "--- :ruby: Updating RubyGems and Bundler" \
    && (gem update --system || gem update --system 2.7.8) \
    && (gem install bundler || true) \
    && gem install bundler -v '< 2' \
    && ruby --version && gem --version && bundle --version \
    && echo "--- :package: Installing system deps" \
    # Postgres apt sources
    && curl -sS https://www.postgresql.org/media/keys/ACCC4CF8.asc | apt-key add - \
    && echo "deb http://apt.postgresql.org/pub/repos/apt/ $(awk -F"[)(]+" '/VERSION=/ {print $2}' /etc/os-release)-pgdg main" > /etc/apt/sources.list.d/pgdg.list \
    # Node apt sources
    && curl -sS https://deb.nodesource.com/gpgkey/nodesource.gpg.key | apt-key add - \
    && echo "deb http://deb.nodesource.com/node_10.x $(awk -F"[)(]+" '/VERSION=/ {print $2}' /etc/os-release) main" > /etc/apt/sources.list.d/nodesource.list \
    # Yarn apt sources
    && curl -sS https://dl.yarnpkg.com/debian/pubkey.gpg | apt-key add - \
    && echo "deb http://dl.yarnpkg.com/debian/ stable main" > /etc/apt/sources.list.d/yarn.list \
    # Install all the things
    && apt-get update \
    && apt-get install -y --no-install-recommends \
        postgresql-client mysql-client sqlite3 \
        git nodejs yarn lsof \
    && (apt-get install -y --no-install-recommends \
        ffmpeg mupdf mupdf-tools poppler-utils || true) \
    # await (for waiting on dependent services)
    && cd /tmp \
    && wget -qc https://github.com/betalo-sweden/await/releases/download/v0.4.0/await-linux-amd64 \
    && install await-linux-amd64 /usr/local/bin/await \
    # clean up
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/* /tmp/* \
    && mkdir /rails

WORKDIR /rails
ENV RAILS_ENV=test RACK_ENV=test
ENV JRUBY_OPTS="--dev -J-Xmx1024M"

ADD .buildkite/await-all /usr/local/bin/
RUN chmod +x /usr/local/bin/await-all

# Wildcard ignores missing files; .empty ensures ADD always has at least
# one valid source: https://stackoverflow.com/a/46801962
ADD .buildkite/.empty actioncable/package.jso[n] actioncable/
ADD .buildkite/.empty actiontext/package.jso[n] actiontext/
ADD .buildkite/.empty actionview/package.jso[n] actionview/
ADD .buildkite/.empty activestorage/package.jso[n] activestorage/
ADD .buildkite/.empty package.jso[n] yarn.loc[k] .yarnr[c] ./

RUN rm -f .empty */.empty \
    && find . -type d -maxdepth 1 -empty -exec rmdir '{}' '+' \
    && if [ -f package.json ]; then \
        echo "--- :javascript: Installing JavaScript deps" \
        && yarn install \
        && yarn cache clean; \
    fi

ADD */*.gemspec tmp/
ADD railties/exe/ railties/exe/
ADD Gemfile Gemfile.lock RAILS_VERSION rails.gemspec ./

RUN echo "--- :bundler: Installing Ruby deps" \
    && (cd tmp && for f in *.gemspec; do d="$(basename -s.gemspec "$f")"; mkdir -p "../$d" && mv "$f" "../$d/"; done) \
    && rm Gemfile.lock && bundle install -j 8 && cp Gemfile.lock tmp/Gemfile.lock.updated \
    && rm -rf /usr/local/bundle/gems/cache \
    && echo "--- :floppy_disk: Copying repository contents"

ADD . ./

RUN mv -f tmp/Gemfile.lock.updated Gemfile.lock \
    && if [ -f package.json ]; then \
        echo "--- :javascript: Building JavaScript package" \
        && (cd actionview && yarn build) \
        && if [ -f railties/test/isolation/assets/package.json ]; then \
            (cd railties/test/isolation/assets && yarn install); \
        fi \
        && yarn cache clean; \
    fi
