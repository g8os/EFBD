#ifndef LIBG8STOR_H
    #define LIBG8STOR_H

    typedef struct remote_t {
        redisContext *redis;

    } remote_t;

    typedef struct buffer_t {
        FILE *fp;
        unsigned char *data;
        size_t length;
        size_t current;
        size_t chunksize;
        size_t finalsize;
        int chunks;

    } buffer_t;

    typedef struct chunk_t {
        char *id;
        char *cipher;
        unsigned char *data;
        size_t length;

    } chunk_t;

    // remote manager
    remote_t *remote_connect(const char *host, int port);
    void remote_free(remote_t *remote);

    // file buffer
    buffer_t *bufferize(char *filename);
    buffer_t *buffer_writer(char *filename);
    const unsigned char *buffer_next(buffer_t *buffer);
    void buffer_free(buffer_t *buffer);

    // chunk
    chunk_t *chunk_new(char *hash, char *key, unsigned char *data, size_t length);
    void chunk_free(chunk_t *chunk);

    // encryption
    chunk_t *encrypt_chunk(const char *chunk, size_t chunksize);
    chunk_t *decrypt_chunk(chunk_t *chunk);

    // networking
    chunk_t *upload_chunk(remote_t *remote, chunk_t *chunk);
    chunk_t *download_chunk(remote_t *remote, chunk_t *chunk);

    // deprecated
    chunk_t *upload(remote_t *remote, buffer_t *buffer);
    size_t download(remote_t *remote, chunk_t *chunk, buffer_t *buffer);
#endif
