CXX = g++
CFLAGS = -std=c++17 -O3 -march=native -L./ -I./Halo
CURR := $(shell pwd)
CCEH_F := $(CFLAGS) -DCCEHT -std=c++17 -O3 -march=native \
	-L./third/pmdk/src/nondebug \
	-I./third/pmdk/src/include  -Wl,-rpath,$(CURR)/third/pmdk/src/nondebug

DASH_F := $(CFLAGS) -DDASHT -std=c++17 -O3 -march=native \
	-L./third/pmdk/src/nondebug \
	-I./third/pmdk/src/include  -Wl,-rpath,$(CURR)/third/pmdk/src/nondebug

CLEVEL_F := $(CFLAGS) -DCLEVELT -std=c++17 -O3 -march=native \
	-I./third/CLevel/src/common -I./third/CLevel -I./third/CLevel/src -I./third/CLevel/src/common



PCM := -I./pcm -L./pcm

CFLAGS_PMDK := -std=c++17 -O3 -I./ -L./

tar = HALO CCEH DASH CLEVEL PCLHT VIPER SOFT CLHT

all: $(tar)

$(tar): LIBPCM

LIBPCM:
	make -C pcm

libHalo.a: Halo/Halo.cpp Halo/Halo.hpp Halo/Pair_t.h
	$(CXX) $(CFLAGS) -c -o libHalo.o $<
	ar rv libHalo.a libHalo.o

libSOFT.a: third/SOFT/ssmem.cpp  third/SOFT/*
	$(CXX) $(CFLAGS) -c -o libSOFT.o $<
	ar rv libSOFT.a libSOFT.o

libPCLHT.a: third/PCLHT/clht_lb_res.cpp
	$(CXX) $(CFLAGS) -c -o libPCLHT.o $< -lvmem
	ar rv libPCLHT.a libPCLHT.o

libCLHT.a: third/CLHT/src/clht_lb_res.c
	$(CXX) $(CFLAGS) -c -o libCLHT.o $^
	ar rv libCLHT.a libCLHT.o 


HALO: benchmark.cpp libHalo.a hash_api.h
	$(CXX) -DHALOT $(CFLAGS) $(PCM) -o $@ $< -lHalo -pthread -mavx -lPCM -lpmem

PCLHT: benchmark.cpp libPCLHT.a hash_api.h
	$(CXX) -DPCLHTT $(CFLAGS) $(PCM) -o $@ $< -lPCLHT -pthread -lvmem -lPCM 

CLHT: benchmark.cpp libCLHT.a hash_api.h
	$(CXX) -DCLHTT $(CFLAGS) $(PCM) -o $@ $< -lCLHT -pthread -lPCM 

SOFT: benchmark.cpp libSOFT.a hash_api.h
	$(CXX) -DSOFTT $(CFLAGS) $(PCM) -o $@ $< -lSOFT -pthread -lvmem -lPCM 

VIPER: benchmark.cpp hash_api.h third/viper/*
	$(CXX) -DVIPERT $(CFLAGS) $(PCM) -o $@ $< -pthread -lpmem -lPCM 

CLEVEL: benchmark.cpp libHalo.a hash_api.h
	$(CXX) $(CLEVEL_F) $(CFLAGS) $(PCM) -o $@ $< -pthread -lpmemobj -lPCM 

CUSTOM_PMDK:
	chmod +x ./third/pmdk/utils/check-os.sh 
	make -C ./third/pmdk/src

CCEH: benchmark.cpp hash_api.h CUSTOM_PMDK
	$(CXX) $(CCEH_F) $(PCM) -o $@ $< -lpthread -lPCM -lpmemobj

DASH: benchmark.cpp hash_api.h CUSTOM_PMDK
	$(CXX) $(DASH_F) $(PCM) -o $@ $< -lpthread -lpmemobj -lPCM 


clean:
	rm -f *.o *.a $(tar)
cleanAll:
	rm -f *.o *.a $(tar)
	make -C pcm clean
	make -C ./third/pmdk/src clean