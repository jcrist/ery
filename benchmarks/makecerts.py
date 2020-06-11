import datetime
import os

from cryptography import x509
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.x509.oid import NameOID


if __name__ == "__main__":
    benchdir = os.path.abspath(os.path.dirname(__file__))
    key = rsa.generate_private_key(
        public_exponent=65537, key_size=2048, backend=default_backend()
    )
    key_bytes = key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )

    subject = issuer = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "ery-bench")])
    now = datetime.datetime.utcnow()
    cert = (
        x509.CertificateBuilder()
        .subject_name(subject)
        .issuer_name(issuer)
        .public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now)
        .not_valid_after(now + datetime.timedelta(days=365))
        .sign(key, hashes.SHA256(), default_backend())
    )
    cert_bytes = cert.public_bytes(serialization.Encoding.PEM)
    with open(os.path.join(benchdir, "cert.pem"), "wb") as f:
        f.write(cert_bytes)
    with open(os.path.join(benchdir, "key.pem"), "wb") as f:
        f.write(key_bytes)
