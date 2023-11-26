## curode - cursus cluster node
#### Default port is 7682

### Building

### Darwin / MacOS

- ``env GOOS=darwin GOARCH=arm go build -o bin/macos-darwin/arm/curode``

- ``env GOOS=darwin GOARCH=amd64 go build -o bin/macos-darwin/amd64/curode``

- ``env GOOS=darwin GOARCH=386 go build -o bin/macos-darwin/386/curode``

- ``env GOOS=darwin GOARCH=arm64 go build -o bin/macos-darwin/arm64/curode``

### Linux
- ``env GOOS=linux GOARCH=386 go build -o bin/linux/386/curode``

- ``env GOOS=linux GOARCH=amd64 go build -o bin/linux/amd64/curode``

- ``env GOOS=linux GOARCH=arm go build -o bin/linux/arm/curode``

- ``env GOOS=linux GOARCH=arm64 go build -o bin/linux/arm64/curode``

### FreeBSD

- ``env GOOS=freebsd GOARCH=arm go build -o bin/freebsd/arm/curode``

- ``env GOOS=freebsd GOARCH=amd64 go build -o bin/freebsd/amd64/curode``

- ``env GOOS=freebsd GOARCH=386 go build -o bin/freebsd/386/curode``


### Windows
- ``env GOOS=windows GOARCH=amd64 go build -o bin/windows/amd64/curode``
- ``env GOOS=windows GOARCH=386 go build -o bin/windows/386/curode``



### Running
To run a cluster node
``` 
./curode

OR

./curode -port=1234
```
1234 is an example.


