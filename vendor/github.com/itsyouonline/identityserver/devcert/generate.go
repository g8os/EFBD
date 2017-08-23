package devcert

//go:generate openssl req -x509 -newkey rsa:2048 -keyout key.pem -out cert.pem -days 1460 -nodes -subj "/C=BE/ST=Gent/L=Lochristi/O=ItsYouOnline/CN=dev.itsyou.online"
//go:generate openssl ecparam -out jwt_key.pem -name secp384r1 -genkey -noout
//go:generate openssl ec -in jwt_key.pem -pubout -out jwt_pub.pem
