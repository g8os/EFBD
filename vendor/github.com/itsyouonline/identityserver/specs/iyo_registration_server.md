

   - 2 S3 backend (of choice): 
- CRC file
- hash file -> hash A (blake)
- hash file -> hash A' (other hash function, NOT BLAKE)
- compress file
- encrypt compressed file with hash A
- hash encrypted file: -> hash B
   - location $month/$hashB
- register the action with 
   - info submitted to Tierion 
   - hash A'
   - time
   - size
   - descr: backup ...
   - CRC
   - encrypted: hash A
   - encrypted: hash B
- encryption is done with: private key of  IYO registration server
