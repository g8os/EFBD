#include <stdio.h>
#include <stdlib.h>
#include <hiredis.h>
#include <lib0stor.h>

int main(int argc, char *argv[]) {
    char *input, *output;
    remote_t *remote = NULL;
    buffer_t *buffer;
    chunk_t **chunks;
    int chunks_length;

    //
    // arguments checker
    //
    if(argc < 3) {
        fprintf(stderr, "[-] usage: %s input-filename output-filename [ardb-addr]\n", argv[0]);
        exit(EXIT_FAILURE);
    }

    input = argv[1];
    output = argv[2];

    printf("[+] uploading file: %s\n", input);
    printf("[+] restoring file: %s\n", output);

    //
    // connect to redis (ardb)
    //
    if(argc == 4) {
        printf("[+] connecting to remote target: %s\n", argv[3]);
        if(!(remote = remote_connect(argv[3], 16379)))
            exit(EXIT_FAILURE);
    }

    //
    // initialize buffer and chunks
    //
    if(!(buffer = bufferize(input))) {
        fprintf(stderr, "[-] cannot bufferize: %s\n", input);
        exit(EXIT_FAILURE);
    }

    chunks_length = buffer->chunks;
    if(!(chunks = (chunk_t **) malloc(sizeof(chunk_t *) * chunks_length))) {
        perror("[-] chunks malloc");
        exit(EXIT_FAILURE);
    }

    //
    // encrypting chunks
    //
    printf("[+] =============================\n");
    printf("[+] encrypting %d chunks\n", buffer->chunks);
    printf("[+] =============================\n");

    for(int i = 0; i < buffer->chunks; i++) {
        const unsigned char *data = buffer_next(buffer);

        // encrypting chunk
        chunk_t *chunk = encrypt_chunk(data, buffer->chunksize);
        buffer->finalsize += chunk->length;

        printf("[+] %s [%s]: %lu bytes\n", chunk->id, chunk->cipher, chunk->length);
        chunks[i] = chunk;
    }

    printf("[+] finalsize: %lu bytes\n", buffer->finalsize);

    //
    // uploading if remote is set
    //
    if(remote) {
        for(int i = 0; i < buffer->chunks; i++) {
            if(!upload_chunk(remote, chunks[i]))
                exit(EXIT_FAILURE);
        }
    }

    buffer_free(buffer);

    //
    // decrypting chunks
    //
    printf("[+] =============================\n");
    printf("[+] decrypting %d chunks\n", chunks_length);
    printf("[+] =============================\n");

    if(!(buffer = buffer_writer(output))) {
        fprintf(stderr, "[-] cannot bufferize: %s\n", output);
        exit(EXIT_FAILURE);
    }

    for(int i = 0; i < chunks_length; i++) {
        chunk_t *output = NULL;

        // decrypting chunk
        if(!(output = decrypt_chunk(chunks[i])))
            fprintf(stderr, "[-] download failed\n");

        if(fwrite(output->data, output->length, 1, buffer->fp) != 1) {
            perror("[-] fwrite");
            return 0;
        }

        buffer->chunks += 1;
        buffer->finalsize += output->length;
        printf("[+] chunk restored: %lu bytes\n", output->length);

        chunk_free(output);
    }

    printf("[+] finalsize: %lu bytes read in %d chunks\n", buffer->finalsize, buffer->chunks);
    buffer_free(buffer);

    //
    // cleaning
    //
    for(int i = 0; i < chunks_length; i++)
        chunk_free(chunks[i]);

    free(chunks);

    if(remote)
        remote_free(remote);

    printf("[+] all done\n");

    return 0;
}
