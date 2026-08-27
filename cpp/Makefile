CXX ?= clang++
CXXFLAGS ?= -O3 -std=c++20 -Wall -Wextra

clob-bench: main.cpp
	$(CXX) $(CXXFLAGS) -o $@ $<

clean:
	rm -f clob-bench
