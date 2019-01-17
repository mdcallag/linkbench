all: linkbench.so

CFLAGS ?= -O3 -march=native

linkbench.so: linkbench.c
	gcc -shared $< -o $@ \
		-I/usr/include/tarantool -I/usr/local/include/tarantool \
		-lmsgpuck \
		$(CFLAGS) \
		-fPIC

clean:
	rm -f *.so
	rm -rf 51?/ *.vylog *.snap *.xlog *.log

.PHONY: clean
