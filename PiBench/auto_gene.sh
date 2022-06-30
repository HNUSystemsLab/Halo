#positive read
./generator UNIFORM 200000000 200000000 0 8 1 0 0 0 PiBench1
#negative read
./generator UNIFORM 200000000 200000000 0 8 1 0 0 1 PiBench2
#insert
./generator UNIFORM 0 200000000 0 8 0 1 0 1 PiBench3
# remove
./generator UNIFORM 200000000 200000000 0 8 0 0 1 0 PiBench4

#positive read
./generator SELFSIMILAR 200000000 200000000 0.2 8 1 0 0 0 PiBench5
#negative read
./generator SELFSIMILAR 200000000 200000000 0.2 8 1 0 0 1 PiBench6
#insert
./generator SELFSIMILAR 0 200000000 0.2 8 0 1 0 0 PiBench7
#remove
./generator SELFSIMILAR 200000000 200000000 0.2 8 0 0 1 0 PiBench8

# recovery
# ./generator UNIFORM 0 1000000000 0 8 1 0 0 1 PiBench11 1
