CC = gcc
CFLAGS = -Wall -Werror -pthread -g
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
	make cleanfiles

cleanfiles:
	for file in result-*.txt; do rm -f "$$file"; done

showfiles:
	for file in result-*.txt; do cat "$$file"; done

test1:
	make clean
	make
	./wordcount sample_inputs/sample11.txt
	make showfiles

test2:
	make clean
	make
	./wordcount sample_inputs/sample11.txt sample_inputs/sample10.txt sample_inputs/sample9.txt sample_inputs/mysample1.txt
	make showfiles
