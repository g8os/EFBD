EXEC = cstor
LIBRARY = cstor.so

SRC = $(wildcard *.c)
OBJ = $(SRC:.c=.o)

CFLAGS  = -pthread -fPIC -I/usr/include/hiredis -I/usr/include/python3.5m -W -Wall -O2 -Wno-missing-field-initializers
LDFLAGS = -lhiredis -lssl -lcrypto -lsnappy -lz -lm -lpython3.5m # -lmsgpack

CC = gcc

all: $(EXEC) $(LIBRARY)

$(EXEC): $(OBJ)
	$(CC) -o $@ $^ $(LDFLAGS)

$(LIBRARY): $(OBJ)
	$(CC) -shared -o $@ $^ $(LDFLAGS)

%.o: %.c
	$(CC) $(CFLAGS) -c $<

clean:
	rm -fv *.o

mrproper: clean
	rm -fv $(EXEC) $(LIBRARY)

