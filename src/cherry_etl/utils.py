from hashlib import sha256


# https://github.com/solana-foundation/anchor/blob/master/lang/syn/src/codegen/program/common.rs
def svm_anchor_discriminator(name: str, namespace: str = "global") -> bytes:
    preimage = f"{namespace}:{name}".encode("utf-8")
    hash_bytes = sha256(preimage).digest()[:8]

    return hash_bytes


__all__ = ["svm_anchor_discriminator"]
