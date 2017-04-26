#include <snappy-c.h>

int s_uncompress(const char *compressed, size_t compressed_len, char *uncompressed) {
	size_t res_len;
	
	snappy_status ret = snappy_uncompress(compressed, compressed_len, uncompressed, &res_len);
	
	if (ret != SNAPPY_OK) {
		return (int)ret;
	}
	return (int)res_len;
}
