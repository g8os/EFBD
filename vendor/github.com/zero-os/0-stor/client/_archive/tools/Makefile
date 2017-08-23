EXEC = g8stor-cli

SRC = g8stor-cli.c
OBJ = g8stor-cli.o

CFLAGS  = -pthread -W -Wall -O2 -I../src/ -I/usr/include/hiredis -g
LDFLAGS = ../src/lib0stor.a -lhiredis -lssl -lcrypto -lsnappy -lz -lm

CC = gcc

all: $(EXEC)

$(EXEC): $(OBJ)
	$(CC) -o $@ $^ $(LDFLAGS)

g8stor-cli.o: g8stor-cli.c
	$(CC) $(CFLAGS) -c $<

clean:
	rm -fv *.o

mrproper: clean
	rm -fv $(EXEC)
