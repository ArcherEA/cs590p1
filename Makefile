## Part of the solution for Assignment 1 (CS 464/564)
## by Stefan Bruda.

## Uncomment this line (and comment out the following line) to produce
## an executable that provides some debug output.
#CXXFLAGS = -g -Wall -pedantic -DDEBUG

CXX = g++
CXXFLAGS = -g -Wall -Werror -ansi -pedantic
LDFLAGS = $(CXXFLAGS) -pthread
all: bbserv

bbserv: tcp-utils.cc bbserv.cc
	$(CXX) $(LDFLAGS) -o bbserv tcp-utils.cc bbserv.cc

clean:
	rm -f bbserv *.log *.pid *.conf bbfile *~ *.o *.bak core \#*
