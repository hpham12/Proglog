# Proglog

## Requirements:
1. A tool to generate TLS certificates. 
	- In this case, I use CFSSL: https://github.com/cloudflare/cfssl. CFSSL allows us to operate a CA ourselves, which is sufficient because we are building internal services.
	- CFSSL has 2 tools that we will need:
		1. cfssl to sign, verify, and bundle TLS certificates and output the results as JSON.
		2. cfssljson to take that JSON output and split them into separate key, certificates, CSR, and bundle files.
	- As we initialize new CA, there are 3 types of files generated: 
		1. ca.pem - This is the CA's public certificate. It is used to verify the server's/client's certificates
		2. ca.csr - (Certificate Signing Request) This is the certificate signing request that was used to generate the ca.pem
		3. ca-key.pem - This is the CA's private certificate. It is used to sign issued certificates and used to create the public certificates ()

	4. How These Files Are Used Together:
			1. Creating Your CA: ca-key.pem (private key) signs the ca.csr to create the ca.pem (public certificate).

			2. Issuing Certificates: The CA uses ca.pem (public certificate) and ca-key.pem (private key) to sign and issue certificates for clients, servers, or devices.

			3. Client/Server Trust: Distribute ca.pem to systems that need to trust certificates issued by your CA.
			4. The client has a copy of the CA’s public certificate (ca.pem) and uses it to:
				1. Verify the signature on the server's certificate.
				2. Ensure the server certificate was indeed issued by the trusted CA.
			
			5. The verification involves cryptographic checks:
				1. The client uses the CA's public key (from ca.pem) to decrypt the signature on the server's certificate.
				2. If the decrypted data matches the server certificate's contents (hash), the certificate is considered valid.

	- When we generate TLS certificates, a few things happen:
		1. Generated a new private key (server-key.pem).
		2. The tool creates the CSR (server.csr) using: The public key, Metadata (e.g., hostname, organization), and A digital signature generated using the private key.
		3. The tool sends the CSR to the CA (or uses the CA files if it's a local CA) to get a signed certificate (server.pem).
		4. CA verifies CSR and issue ceritifcate. Example flow:
			The CSR contains:

				A public key: X
				A hash of the CSR data: H
				A digital signature: S, created as S = Encrypt(H, private key)
				
			The CA:

				Hashes the CSR data itself, resulting in H'.
				Decrypts S using the public key X, resulting in H.

			The CA compares H and H':
			
				If H = H', the CSR is valid.
				If H ≠ H', the CSR is rejected.

	
