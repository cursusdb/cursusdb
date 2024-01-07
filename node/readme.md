## Curode - CursusDB Cluster Node
#### Default port is 7682

## systemd
What is usually done is binaries are pulled from website using wget onto a servers `/opt/cursusdb` directory so if you want to use the service as a default follow that structure.

Once you do that you can simply save a ``curode.service ``under your `/etc/systemd/system` directory: 
``` 
[Unit]
Description=CursusDB Cluster Node Service

[Service]
WorkingDirectory=/opt/cursusdb
ExecStart=/opt/cursusdb/curode

Restart=always
RestartSec=3

[Install]
WantedBy=multi-user.target

```


Then run 
``` 
sudo systemctl daemon-reload
```

``` 
sudo systemctl start curode.service
```

Check the status
```
sudo systemctl status curode.service
```

Make sure you enable so your node starts on boot
```
sudo systemctl enable curode.service
```

## Building

### Darwin / MacOS

- ``env GOOS=darwin GOARCH=amd64 go build -o bin/macos-darwin/amd64/curode && tar -czf bin/macos-darwin/amd64/curode-VERSION-amd64.tar.gz -C bin/macos-darwin/amd64/ $(ls  bin/macos-darwin/amd64/)``


- ``env GOOS=darwin GOARCH=arm64 go build -o bin/macos-darwin/arm64/curode && tar -czf bin/macos-darwin/arm64/curode-VERSION-arm64.tar.gz -C bin/macos-darwin/arm64/ $(ls  bin/macos-darwin/arm64/)``


### Linux
- ``env GOOS=linux GOARCH=386 go build -o bin/linux/386/curode && tar -czf bin/linux/386/curode-VERSION-386.tar.gz -C bin/linux/386/ $(ls  bin/linux/386/)``


- ``env GOOS=linux GOARCH=amd64 go build -o bin/linux/amd64/curode && tar -czf bin/linux/amd64/curode-VERSION-amd64.tar.gz -C bin/linux/amd64/ $(ls  bin/linux/amd64/)``


- ``env GOOS=linux GOARCH=arm go build -o bin/linux/arm/curode && tar -czf bin/linux/arm/curode-VERSION-arm.tar.gz -C bin/linux/arm/ $(ls  bin/linux/arm/)``


- ``env GOOS=linux GOARCH=arm64 go build -o bin/linux/arm64/curode && tar -czf bin/linux/arm64/curode-VERSION-arm64.tar.gz -C bin/linux/arm64/ $(ls  bin/linux/arm64/)``


### FreeBSD

- ``env GOOS=freebsd GOARCH=arm go build -o bin/freebsd/arm/curode && tar -czf bin/freebsd/arm/curode-VERSION-arm.tar.gz -C bin/freebsd/arm/ $(ls  bin/freebsd/arm/)``


- ``env GOOS=freebsd GOARCH=amd64 go build -o bin/freebsd/amd64/curode && tar -czf bin/freebsd/amd64/curode-VERSION-amd64.tar.gz -C bin/freebsd/amd64/ $(ls  bin/freebsd/amd64/)``


- ``env GOOS=freebsd GOARCH=386 go build -o bin/freebsd/386/curode && tar -czf bin/freebsd/386/curode-VERSION-386.tar.gz -C bin/freebsd/386/ $(ls  bin/freebsd/386/)``


### Windows
- ``env GOOS=windows GOARCH=amd64 go build -o bin/windows/amd64/curode.exe && zip -r -j bin/windows/amd64/curode-VERSION-x64.zip bin/windows/amd64/curode.exe``


- ``env GOOS=windows GOARCH=arm64 go build -o bin/windows/arm64/curode.exe && zip -r -j bin/windows/arm64/curode-VERSION-x64.zip bin/windows/arm64/curode.exe``


- ``env GOOS=windows GOARCH=386 go build -o bin/windows/386/curode.exe && zip -r -j bin/windows/386/curode-VERSION-x86.zip bin/windows/386/curode.exe``





### Running
To run a cluster node
``` 
./curode

OR

./curode -port=1234
```
1234 is an example.


