SOURCES=utils.erl pmap.erl pmapred_naive.erl dmapred.erl stream.erl ./example/test_googlemapred.erl ./example/test_stream.erl ./example/test_pmapred.erl
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
