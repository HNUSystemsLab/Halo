CXX := g++ -O0 -ggdb
CFLAGS := -std=c++17 -march=native -L./ -mavx 

all: Halo

Halo: main.cpp libHalo.a
	$(CXX) $(CFLAGS) -o $@ $< -lHalo -lpthread -lpmem
libHalo.a: Halo.cpp Halo.hpp 
	$(CXX) $(CFLAGS) -c -o libHalo.o $<
	ar rv libHalo.a libHalo.o
clean:
	rm -f *.o *.a Halo