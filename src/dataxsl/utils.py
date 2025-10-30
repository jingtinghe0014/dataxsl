
from Crypto.Cipher import AES
from Crypto.Util.Padding import unpad, pad

# 解密
def decrypt(content:bytes, word:bytes):
    iv = content[:AES.block_size]  # 提取 IV
    cipher = AES.new(word, AES.MODE_CBC, iv)
    _plain: bytes = unpad(cipher.decrypt(content[AES.block_size:]), AES.block_size)
    return _plain.decode()

def encrypt(content:str, word:bytes):
    cipher = AES.new(word, AES.MODE_CBC)  # 使用 CBC 模式
    _crypto = cipher.encrypt(pad(content.encode(), AES.block_size))
    return cipher.iv + _crypto  # 返回 IV + 密文, keyw):, keyw):

if __name__ == '__main__':

    # plaintext = '123456'
    plaintext = 'HJbvr_8hgjKwe'
    key = b'TplEasT220241218'
    ciphertext = encrypt(plaintext, key)
    ciphertext_hex = ciphertext.hex()
    print("加密结果:", ciphertext_hex)

    decrypted_text = decrypt(bytes.fromhex(ciphertext_hex), key)
    print("解密结果:", decrypted_text)