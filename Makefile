EXAMPLES=example/test_mapred_google.erl example/test_stream.erl example/test_mapred.erl
SOURCES=utils.erl pmap.erl mapred_naive.erl mapred_google.erl stream.erl $(EXAMPLES)
OBJECTS=$(SOURCES:.erl=.beam)
INCLUDES=-I ./include/usages.hrl

ebin:
	mkdir ebin

%.beam: %.erl ebin
	erlc $(INCLUDES) -o ebin $<

.DEFAULT_GOAL := all
.PHONY: all clean

all: $(SOURCES) $(OBJECTS)

clean cleanall:
	rm -rf ebin
