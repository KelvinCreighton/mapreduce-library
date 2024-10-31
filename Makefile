CC = gcc
CFLAGS = -Wall -g
OBJ = threadpool.o mapreduce.o distwc.o

EXEC = wordcount

all: $(EXEC)

threadpool.o: threadpool.c
	$(CC) $(CFLAGS) -c threadpool.c -o threadpool.o

mapreduce.o: mapreduce.c
	$(CC) $(CFLAGS) -c mapreduce.c -o mapreduce.o

distwc.o: distwc.c
	$(CC) $(CFLAGS) -c distwc.c -o distwc.o

wordcount: $(OBJ)
	$(CC) $(CFLAGS) $(OBJ) -o $(EXEC)

clean:
	rm -f $(OBJ) $(EXEC)

test:
	make clean
	make
	./wordcount sample_inputs/sample11.txt sample_inputs/sample10.txt sample_inputs/sample9.txt


