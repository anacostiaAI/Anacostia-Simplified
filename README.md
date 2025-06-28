A simplified version of distributed Anacostia for the purpose of development.

Instructions for installing certs and running https example:
1. install mkcert:
# brew install mkcert
# mkcert -install

2. use the following commands to generate self-signed cert and key
# mkcert -key-file private_leaf.key -cert-file certificate_leaf.pem localhost 127.0.0.1
# mkcert -key-file private_root.key -cert-file certificate_root.pem localhost 127.0.0.1

3. run `python leaf.py` and `python root.py` in separate terminals
4. visit https://localhost:8001/ in chrome and click on Advanced → Proceed to localhost (unsafe). this allows us to manually accept the certificate for the leaf.
5. visit https://localhost:8000/ in chrome and click on Advanced → Proceed to localhost (unsafe). this allows us to manually accept the certificate for the root.