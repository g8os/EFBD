LIBRARY = g8storclient.so

SRC = g8stor-python.c
OBJ = g8stor-python.o

CFLAGS  = -pthread -g -fPIC -I/usr/include/python3.5m -W -Wall -O2 -Wno-missing-field-initializers -I../src/ -I/usr/include/hiredis
LDFLAGS = ../src/lib0stor.a -lhiredis -lssl -lcrypto -lsnappy -lz -lm -lpython3.5m

CC = gcc

all: $(LIBRARY)

$(LIBRARY): $(OBJ)
	$(CC) -shared -o $@ $^ $(LDFLAGS)

g8stor-python.o: g8stor-python.c
	$(CC) $(CFLAGS) -c $<

clean:
	rm -fv *.o

mrproper: clean
	rm -fv $(LIBRARY)

