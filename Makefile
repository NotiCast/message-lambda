PYTHON_VERSION ?= python3.6

DEPS ?= raven pymysql sqlalchemy

.PHONY: clean deps

deps: $(DEPS)

clean:
	rm -rf libs

libs:
	virtualenv libs -p $(PYTHON_VERSION)

$(DEPS): libs
	( \
		. libs/bin/activate; \
		pip install $@; \
	)
