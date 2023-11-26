## cursus
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


### Building

### Darwin / MacOS

- ``env GOOS=darwin GOARCH=arm go build -o bin/macos-darwin/arm/cursus``

- ``env GOOS=darwin GOARCH=amd64 go build -o bin/macos-darwin/amd64/cursus``

- ``env GOOS=darwin GOARCH=386 go build -o bin/macos-darwin/386/cursus``

- ``env GOOS=darwin GOARCH=arm64 go build -o bin/macos-darwin/arm64/cursus``

### Linux
- ``env GOOS=linux GOARCH=386 go build -o bin/linux/386/cursus``

- ``env GOOS=linux GOARCH=amd64 go build -o bin/linux/amd64/cursus``

- ``env GOOS=linux GOARCH=arm go build -o bin/linux/arm/cursus``

- ``env GOOS=linux GOARCH=arm64 go build -o bin/linux/arm64/cursus``

### FreeBSD

- ``env GOOS=freebsd GOARCH=arm go build -o bin/freebsd/arm/cursus``

- ``env GOOS=freebsd GOARCH=amd64 go build -o bin/freebsd/amd64/cursus``

- ``env GOOS=freebsd GOARCH=386 go build -o bin/freebsd/386/cursus``


### Windows
- ``env GOOS=windows GOARCH=amd64 go build -o bin/windows/amd64/cursus``
- ``env GOOS=windows GOARCH=386 go build -o bin/windows/386/cursus``

