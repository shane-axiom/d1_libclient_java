Regarding d1-trusted-certs.crt file
-----------------------------------
This file must have a file extension on it (that matches what's in CertificateManager too, of course)
for it to be found by getResourceAsStream()

On the client side
-------------------
-Get a CILogon certificate using the DataONE skin:
	https://cilogon.org/?skin=DataONE
-Supply a pkcs12 password
	we may try to do away with this
-Note where the files are stored
	usually something like: /tmp/x509up_u503.p12
-The .p12 is a keystore and is the one we want to use (at this point).	
-Set the CertificateManager to use these two values (keystore name and password)
-RestClient (and D1RestClient) will now be able to use CertificateManager to set up SSL connections
	

On the server (Tomcat) side
----------------------------
-Generate a key/keystore for Tomcat to use (or import purchased cert for production systems)
	keytool -genkey -alias tomcat -keyalg RSA -validity 3650 -keystore /tmp/tomcat.keystore
-Download the CILogon Basic CA cert 
	http://ca.cilogon.org/downloads
-Import the CA cert into Tomcat's keystore
	keytool -import -alias cilogon_basic -file ~/Downloads/cilogon-silver.crt -keystore /tmp/tomcat.keystore
-Configure the HTTPS connector to use the keystore and request client certificates
<Connector 
		port="8443" 
		protocol="HTTP/1.1" 
		SSLEnabled="true"
		maxThreads="150" 
		scheme="https" 
		secure="true"
		clientAuth="want" 
		sslProtocol="TLS" 
		keystoreFile="/tmp/tomcat.keystore"
		keystorePass="changeit"
		truststoreFile="/tmp/tomcat.keystore"
		truststorePass="changeit"
	/>
		