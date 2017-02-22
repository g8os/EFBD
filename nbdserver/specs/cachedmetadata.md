# Specifications for caching of the metadata in the nbdserver

The data in the ardb's is stored using the blake2b hash of the content as it's key so there is an LBA translation for every read operation. Doing a lookup for every read towards an external ardb is way too slow so we need some LBA lookup table caching.
