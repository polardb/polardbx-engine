#ifndef SM4_INTERFACE_H
#define SM4_INTERFACE_H

#ifdef __cplusplus
extern "C" {
#endif

typedef int STATUS;

#define STATUS_success 0
#define STATUS_prikey_lenerror -10001
#define STATUS_pubkey_lenerror -10002
#define STATUS_plain_lenerror -10003
#define STATUS_cipher_lenerror -10004
#define STATUS_cipherkey_lenerror -10005
#define STATUS_other_lenerror -10006
#define STATUS_plain_formaterror -10009
#define STATUS_cipher_formaterror -10010
#define STATUS_cipherkey_formaterror -10011
#define STATUS_other_formaterror -10012
#define STATUS_buffer_tooshort -10013
#define STATUS_malloc_fail -10017
#define STATUS_notsupport -10018
#define STATUS_parameter_error -10019

#define STATUS_key_hash_error -20007
#define STATUS_undefinederror -99999


//#define true 1
//#define false 0
/*
GetKey函数说明：输入用于生成密钥的原始数据，获取密钥和IV

参数:password：输入参数，生成密钥的原始数据；
		passLen：输入参数，password的数据长度；
		Key：输出参数，生成的SM4密钥；16字节
		IV：输出参数，生成的IV，16字节；IV是用于SM4 ctr模式必须的数据；
*/
int GetKey(unsigned char* password,int passLen,unsigned char Key[16],unsigned char IV[16]);

/*
说明:SM4 CTR模式加密,IV和明文,输出密文。
参数:Key密钥；
		KeyLen:密钥长度，密钥长度为16个字节
		IV初始向量16个字节
		plain明文
		plainLen明文长度
		cipherbuf接受密文buf
		cipherbufLen接受密文buf的长度,此参数说明cipherbuf，是为了防止密文长度不足，越界使用内存.
		cipherLen真实的密文长度:真实的密文长度和明文等长
		offset数据位置；加密一整段数据，offset=0；如果分为多次、分段加密一整段数据，offset就是每次加密数据的位置，从0开始。
返回:0:成功;其他值:失败
*/
int sym_decrypt_ctr_withKey(unsigned char *Key, int KeyLen,
                            const unsigned char IV[16],
                            const unsigned char *cipher, int cipherLen,
                            unsigned char *plainbuf, int plainbufLen,
                            int *plainLen, unsigned int offset);

/*
说明:SM4 CTR模式解密,IV和密文,输出明文。
参数:Key密钥；
		KeyLen密钥长度，密钥长度为16个字节；，
		IV初始向量16个字节
		cipher密文
		cipherLen密文长度
		plainbuf接受明文buf
		plainbufLen接受明文buf的长度
		plainLen真实的明文长度
		offset数据位置；解密一整段数据，offset=0；如果分为多次、分段解密一整段数据，offset就是每次解密数据的位置，从0开始。
返回:0:成功;其他值:失败
*/
int sym_encrypt_ctr_withKey(unsigned char *Key, int KeyLen,
                            const unsigned char IV[16],
                            const unsigned char *plain, int plainLen,
                            unsigned char *cipherbuf, int cipherbufLen,
                            int *cipherLen, unsigned int offset);

#ifdef __cplusplus
}
#endif

#endif
