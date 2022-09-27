SOURCES=utils.erl pmap.erl mapred_naive.erl googlemapred.erl stream.erl ./example/test_googlemapred.erl ./example/test_stream.erl ./example/test_mapred.erl
OBJECTS=$(SOURCES:.erl=.beam)

ebin:
	mkdir ebin

%.beam: %.erl ebin
	erlc -o ebin $<

.DEFAULT_GOAL := all
.PHONY: all clean

all: $(SOURCES) $(OBJECTS)

clean cleanall:
	rm -rf ebin
