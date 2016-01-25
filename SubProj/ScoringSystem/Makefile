TARGET = Pipeline

OBJS = main.o Pipeline.o Processors.o ScoreMethods.o

TEMPS = *~ \\\#*\\\#

REBUILDABLES = $(TARGET) $(OBJS) $(TEMPS)

#Main rules
all: $(TARGET)

$(TARGET): $(OBJS)
	g++ $^ -o $@
	g++ $^ -o ../$@

%.o: %.cpp
	g++ -c $< -o $@

clean:
	rm $(REBUILDABLES)

#Dependencies
main.o: Pipeline.hpp Processors.hpp ScoreMethods.hpp
Pipeline.o: Pipeline.hpp
Processors.o: Processors.hpp
ScoreMethods.o: ScoreMethods.hpp
