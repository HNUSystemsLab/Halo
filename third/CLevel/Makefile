all:
	c++  -DTESTS_LIBPMEMOBJ_VERSION=0x0 -I./ \
	-I/usr/local/include/valgrind -Itest/common \
	-Itest/. -std=gnu++11 -c -o clevel.o tests/clevel_hash/clevel_hash_cli.cpp -lpmemobj  -lpthread
	ar rv libHaLo.a libHaLo.o
clean:	
	rm *.a *.o *.so -f