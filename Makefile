EXAMPLES=example/test_mapred_google.erl example/test_stream.erl example/test_mapred.erl
SOURCES=$(wildcard src/*.erl) $(EXAMPLES)
OBJECTS=$(SOURCES:.erl=.beam)
LIB_ZIP=ske-pi.tar.gz
TEST=test
TEST_ZIP=$(TEST).tar.gz
INCLUDES=-I include/usages.hrl -I include/defines.hrl
# to turn-off debug mode compile with "make DEBUG="
DEBUG=-Ddebug

ebin:
	mkdir ebin

%.beam: %.erl ebin
	erlc $(DEBUG) $(INCLUDES) -o ebin $<

.DEFAULT_GOAL := all
.PHONY: all clean cleanall compress

all: $(SOURCES) $(OBJECTS)
	@mkdir -p logs
	@echo "\nCompilation successful! You may now use SkePi."; \


archive: ske-pi.tar.gz

ske-pi.tar.gz:
	@touch $(LIB_ZIP)
	tar --exclude=$(LIB_ZIP) --exclude=./.git -zcf $(LIB_ZIP) .
	@echo "Creating an archive of this directory and its contents."


clean:
	@echo "Cleaning up all object files."
	rm -rf ebin

cleanall:
	@echo "Cleaning up all log files and object files."
	rm -rf logs
	rm -rf ebin
