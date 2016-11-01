CXX = mpic++
LDFLAGS = -libverbs
CXXFLAGS = -std=c++11 -g

HEADER = utils.hpp verbs.hpp
TARGET = main
OBJECTS = verbs.o

all: ${TARGET}

main: main.cpp ${OBJECTS} ${HEADER}
	${CXX} ${CXXFLAGS} ${LDFLAGS} $< ${OBJECTS} -o $@

%.o: %.cpp ${HEADER}
	${CXX} ${CXXFLAGS} ${LDFLAGS} $< -c -o $@

.PHONY: clean
clean:
	rm -f *.o
	rm -f ${TARGET}
