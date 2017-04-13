#include <stdlib.h>
#include <isa-l/erasure_code.h>

inline int init_encode_tab(int k, int m, unsigned char **encode_tab) {
		unsigned char *encode_matrix = (unsigned char *) malloc(sizeof(char) * k * (k + m));
		unsigned char *tmp_encode_tab = (unsigned char *) malloc(sizeof(char) * 32 * k  * ( k + m));

		gf_gen_cauchy1_matrix(encode_matrix, k+m, k);
		ec_init_tables(k, m, &encode_matrix[k * k], tmp_encode_tab);
		*encode_tab = tmp_encode_tab;
		return 0;
}
