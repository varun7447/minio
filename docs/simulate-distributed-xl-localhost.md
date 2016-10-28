# Simulating distributed XL on localhost


## Internal document intended at testing

Simulating a 4 node distributed XL:

* mkdir /export1 /export2 /export3 /export4
* Terminal-1
```
minio server --address localhost:9001 http://localhost:9001/export1 http://localhost:9002/export2 http://localhost:9003/export3 http://localhost:9004/export4
```
* Terminal-2
```
minio server --address localhost:9002 http://localhost:9001/export1 http://localhost:9002/export2 http://localhost:9003/export3 http://localhost:9004/export4
```
* Terminal-3
```
minio server --address localhost:9003 http://localhost:9001/export1 http://localhost:9002/export2 http://localhost:9003/export3 http://localhost:9004/export4
```
* Terminal-4
```
minio server --address localhost:9004 http://localhost:9001/export1 http://localhost:9002/export2 http://localhost:9003/export3 http://localhost:9004/export4
```

