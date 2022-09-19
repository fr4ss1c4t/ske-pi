SOURCES=utils.erl pmap.erl preduce.erl pmapred.erl gmapred.erl stream.erl ./example/test_stream.erl ./example/test_mapred.erl
OBJECTS=$(SOURCES:.erl=.beam)

ebin:
	mkdir ebin

%.beam: %.erl ebin
	erlc -o ebin $<

.PHONY: all clean

all: $(SOURCES) $(OBJECTS)

clean:
	rm -rf ebin
