EXAMPLES=example/test_mapred_google.erl example/test_stream.erl example/test_mapred.erl
# SOURCES=$(wildcard *.erl) $(EXAMPLES)
SOURCES=utils.erl stream.erl mapred_naive.erl mapred_google.erl pmap.erl $(EXAMPLES)
OBJECTS=$(SOURCES:.erl=.beam)
INCLUDES=-I include/usages.hrl -I include/defines.hrl
# to turn-off debug mode compile with "make DEBUG="
DEBUG=-Ddebug

ebin:
	mkdir ebin

%.beam: %.erl ebin
	erlc $(DEBUG) $(INCLUDES) -o ebin $<

.DEFAULT_GOAL := all
.PHONY: all clean cleanall

all: $(SOURCES) $(OBJECTS)
	mkdir -p logs
	touch logs/info.log

clean:
	rm -rf ebin

cleanall:
	rm -rf ebin
	rm -rf logs
