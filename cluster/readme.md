## Cursus - CursusDB Cluster
The CursusDB cluster.

#### Default port is 7681

To run a cluster 
``` 
./cursus

OR

./cursus -port=1234
```

if you don't have a ``.clusterconfig``, one will be created where you launch cursus.  The ``.clusterconfig`` is in yaml format.

You can add multiple nodes.  A node could be a pod, commodity serve, so forth.

``.clusterconfig:``
``` 
nodes:
    - 12.34.56.78:7682
    - 87.65.43.21:7682

```


## Building

VERSION to be replaced with V for example ``v1.0.1``

### Darwin / MacOS

- ``env GOOS=darwin GOARCH=amd64 go build -o bin/macos-darwin/amd64/cursus && tar -czf bin/macos-darwin/amd64/cursus-VERSION-amd64.tar.gz -C bin/macos-darwin/amd64/ $(ls  bin/macos-darwin/amd64/)``


- ``env GOOS=darwin GOARCH=arm64 go build -o bin/macos-darwin/arm64/cursus && tar -czf bin/macos-darwin/arm64/cursus-VERSION-arm64.tar.gz -C bin/macos-darwin/arm64/ $(ls  bin/macos-darwin/arm64/)``


### Linux
- ``env GOOS=linux GOARCH=386 go build -o bin/linux/386/cursus && tar -czf bin/linux/386/cursus-VERSION-386.tar.gz -C bin/linux/386/ $(ls  bin/linux/386/)``


- ``env GOOS=linux GOARCH=amd64 go build -o bin/linux/amd64/cursus && tar -czf bin/linux/amd64/cursus-VERSION-amd64.tar.gz -C bin/linux/amd64/ $(ls  bin/linux/amd64/)``


- ``env GOOS=linux GOARCH=arm go build -o bin/linux/arm/cursus && tar -czf bin/linux/arm/cursus-VERSION-arm.tar.gz -C bin/linux/arm/ $(ls  bin/linux/arm/)``


- ``env GOOS=linux GOARCH=arm64 go build -o bin/linux/arm64/cursus && tar -czf bin/linux/arm64/cursus-VERSION-arm64.tar.gz -C bin/linux/arm64/ $(ls  bin/linux/arm64/)``


### FreeBSD

- ``env GOOS=freebsd GOARCH=arm go build -o bin/freebsd/arm/cursus && tar -czf bin/freebsd/arm/cursus-VERSION-arm.tar.gz -C bin/freebsd/arm/ $(ls  bin/freebsd/arm/)``


- ``env GOOS=freebsd GOARCH=amd64 go build -o bin/freebsd/amd64/cursus && tar -czf bin/freebsd/amd64/cursus-VERSION-amd64.tar.gz -C bin/freebsd/amd64/ $(ls  bin/freebsd/amd64/)``


- ``env GOOS=freebsd GOARCH=386 go build -o bin/freebsd/386/cursus && tar -czf bin/freebsd/386/cursus-VERSION-386.tar.gz -C bin/freebsd/386/ $(ls  bin/freebsd/386/)``


### Windows
- ``env GOOS=windows GOARCH=amd64 go build -o bin/windows/amd64/cursus.exe && zip -r -j bin/windows/amd64/cursus-VERSION-x64.zip bin/windows/amd64/cursus.exe``


- ``env GOOS=windows GOARCH=arm64 go build -o bin/windows/arm64/cursus.exe && zip -r -j bin/windows/arm64/cursus-VERSION-x64.zip bin/windows/arm64/cursus.exe``


- ``env GOOS=windows GOARCH=386 go build -o bin/windows/386/cursus.exe && zip -r -j bin/windows/386/cursus-VERSION-x86.zip bin/windows/386/cursus.exe``




### Windows
- ``env GOOS=windows GOARCH=amd64 go build -o bin/windows/amd64/cursus.exe``
- ``env GOOS=windows GOARCH=386 go build -o bin/windows/386/cursus.exe``

