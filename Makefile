CXX = mpicxx
CXXFLAGS = -std=c++11
LDFLAGS =

all : example.x

example.x : example.o
	$(CXX) $(LDFLAGS) example.o -o $@

example.o : rma_buff.hpp

.PHONY : clean
clean:
	rm -f example.o

.PHONY: distclean
distclean: clean
	rm -f example.x $(GEN)
