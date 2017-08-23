#include <Python.h>
#include <stdio.h>
#include <stdlib.h>
#include <hiredis.h>
#include "lib0stor.h"

/*
remote_t *remotes = NULL;

static PyObject *g8storclient_connect(PyObject *self, PyObject *args) {
    (void) self;
    remote_t *remote;
    const char *host;
    int port;

    if (!PyArg_ParseTuple(args, "si", &host, &port))
        return NULL;

    printf("[+] python binding: connecting %s (port: %d)\n", host, port);
    if(!(remote = remote_connect(host, port)))
        return NULL;

    // capsule will encapsulate our remote_t which contains
    // the redis client object
    return PyCapsule_New(remote, "RemoteClient", NULL);
}
*/

static PyObject *g8storclient_encrypt(PyObject *self, PyObject *args) {
    (void) self;
    buffer_t *buffer;
    char *file;

    if(!PyArg_ParseTuple(args, "s", &file))
        return NULL;

    // initialize buffer
    if(!(buffer = bufferize(file)))
        return Py_None;

    // chunks
    PyObject *hashes = PyList_New(buffer->chunks);

    printf("[+] encrypting %d chunks\n", buffer->chunks);
    for(int i = 0; i < buffer->chunks; i++) {
        const unsigned char *data = buffer_next(buffer);

        // encrypting chunk
        chunk_t *chunk = encrypt_chunk(data, buffer->chunksize);

        // inserting hash to the list
        PyObject *pychunk = PyDict_New();
        PyDict_SetItemString(pychunk, "hash", Py_BuildValue("s", chunk->id));
        PyDict_SetItemString(pychunk, "key", Py_BuildValue("s", chunk->cipher));
        PyDict_SetItemString(pychunk, "data", Py_BuildValue("y#", chunk->data, chunk->length));
        PyList_SetItem(hashes, i, pychunk);

        chunk_free(chunk);
    }

    printf("[+] finalsize: %lu bytes\n", buffer->finalsize);

    // cleaning
    buffer_free(buffer);

    return hashes;
}

static PyObject *g8storclient_decrypt(PyObject *self, PyObject *args) {
    (void) self;
    buffer_t *buffer;
    PyObject *hashes;
    char *file;

    if(!PyArg_ParseTuple(args, "Os", &hashes, &file))
        return NULL;

    // initialize buffer
    if(!(buffer = buffer_writer(file)))
        return Py_None;

    // parsing dictionnary
    int chunks = (int) PyList_Size(hashes);

    // chunks
    printf("[+] decrypting %d chunks\n", chunks);
    for(int i = 0; i < chunks; i++) {
        size_t chunksize;
        char *id, *cipher;
        unsigned char **data, *datadup;
        unsigned int length;

        PyObject *item = PyList_GetItem(hashes, i);

        PyArg_Parse(PyDict_GetItemString(item, "hash"), "s", &id);
        PyArg_Parse(PyDict_GetItemString(item, "key"), "s", &cipher);
        PyArg_Parse(PyDict_GetItemString(item, "data"), "y#", &data, &length);

        if(!(datadup = (unsigned char *) malloc(sizeof(char) * length))) {
            perror("malloc");
            return Py_None;
        }

        memcpy(datadup, data, length);

        chunk_t *chunk = chunk_new(strdup(id), strdup(cipher), datadup, length);
        chunk_t *output = NULL;

        // downloading chunk
        if(!(output = decrypt_chunk(chunk)))
            fprintf(stderr, "[-] decrypt failed\n");

        buffer->chunks += 1;
        buffer->finalsize += output->length;
        printf("[+] chunk restored: %lu bytes\n", output->length);

        chunk_free(chunk);
    }

    size_t finalsize = buffer->finalsize;
    printf("[+] finalsize: %lu bytes\n", finalsize);

    // cleaning
    buffer_free(buffer);

    return PyLong_FromLong(finalsize);
}

static PyMethodDef g8storclient_cm[] = {
    // {"connect",  g8storclient_connect,  METH_VARARGS, "Initialize client"},
    {"encrypt", g8storclient_encrypt, METH_VARARGS, "Encrypt a file"},
    {"decrypt", g8storclient_decrypt, METH_VARARGS, "Decrypt a file"},
    {NULL, NULL, 0, NULL}
};

static struct PyModuleDef g8storclientmodule = {
    PyModuleDef_HEAD_INIT,
    "g8storclient", // name of module
    NULL,           // module documentation, may be NULL
    -1,             // -1 if the module keeps state in global variables.
    g8storclient_cm
};

PyMODINIT_FUNC PyInit_g8storclient(void) {
    return PyModule_Create(&g8storclientmodule);
}
