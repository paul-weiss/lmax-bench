CXX ?= clang++
CXXFLAGS ?= -O3 -std=c++20 -Wall -Wextra

clob-bench: main.cpp workload.hpp book.hpp tuned.hpp ring.hpp
	$(CXX) $(CXXFLAGS) -o $@ main.cpp

clean:
	rm -f clob-bench
