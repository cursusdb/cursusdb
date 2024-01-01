#!/bin/bash
VERSION=v2.0.5

echo "Bundling cluster and node zip and tars into /bin in each node and cluster directory."

(cd node && GOOS=darwin GOARCH=amd64 go build -o bin/macos-darwin/amd64/curode && tar -czf bin/macos-darwin/amd64/curode-$VERSION-amd64.tar.gz -C bin/macos-darwin/amd64/ $(ls  bin/macos-darwin/amd64/))
(cd node && GOOS=darwin GOARCH=arm64 go build -o bin/macos-darwin/arm64/curode && tar -czf bin/macos-darwin/arm64/curode-$VERSION-arm64.tar.gz -C bin/macos-darwin/arm64/ $(ls  bin/macos-darwin/arm64/))
(cd node && GOOS=linux GOARCH=386 go build -o bin/linux/386/curode && tar -czf bin/linux/386/curode-$VERSION-386.tar.gz -C bin/linux/386/ $(ls  bin/linux/386/))
(cd node && GOOS=linux GOARCH=amd64 go build -o bin/linux/amd64/curode && tar -czf bin/linux/amd64/curode-$VERSION-amd64.tar.gz -C bin/linux/amd64/ $(ls  bin/linux/amd64/))
(cd node && GOOS=linux GOARCH=arm go build -o bin/linux/arm/curode && tar -czf bin/linux/arm/curode-$VERSION-arm.tar.gz -C bin/linux/arm/ $(ls  bin/linux/arm/))
(cd node && GOOS=linux GOARCH=arm64 go build -o bin/linux/arm64/curode && tar -czf bin/linux/arm64/curode-$VERSION-arm64.tar.gz -C bin/linux/arm64/ $(ls  bin/linux/arm64/))
(cd node && GOOS=freebsd GOARCH=arm go build -o bin/freebsd/arm/curode && tar -czf bin/freebsd/arm/curode-$VERSION-arm.tar.gz -C bin/freebsd/arm/ $(ls  bin/freebsd/arm/))
(cd node && GOOS=freebsd GOARCH=amd64 go build -o bin/freebsd/amd64/curode && tar -czf bin/freebsd/amd64/curode-$VERSION-amd64.tar.gz -C bin/freebsd/amd64/ $(ls  bin/freebsd/amd64/))
(cd node && GOOS=freebsd GOARCH=386 go build -o bin/freebsd/386/curode && tar -czf bin/freebsd/386/curode-$VERSION-386.tar.gz -C bin/freebsd/386/ $(ls  bin/freebsd/386/))
(cd node && GOOS=windows GOARCH=amd64 go build -o bin/windows/amd64/curode.exe && zip -r -j bin/windows/amd64/curode-$VERSION-x64.zip bin/windows/amd64/curode.exe)
(cd node && GOOS=windows GOARCH=arm64 go build -o bin/windows/arm64/curode.exe && zip -r -j bin/windows/arm64/curode-$VERSION-x64.zip bin/windows/arm64/curode.exe)
(cd node && GOOS=windows GOARCH=386 go build -o bin/windows/386/curode.exe && zip -r -j bin/windows/386/curode-$VERSION-x86.zip bin/windows/386/curode.exe)

(cd cluster && GOOS=darwin GOARCH=amd64 go build -o bin/macos-darwin/amd64/cursus && tar -czf bin/macos-darwin/amd64/cursus-$VERSION-amd64.tar.gz -C bin/macos-darwin/amd64/ $(ls  bin/macos-darwin/amd64/))
(cd cluster && GOOS=darwin GOARCH=arm64 go build -o bin/macos-darwin/arm64/cursus && tar -czf bin/macos-darwin/arm64/cursus-$VERSION-arm64.tar.gz -C bin/macos-darwin/arm64/ $(ls  bin/macos-darwin/arm64/))
(cd cluster && GOOS=linux GOARCH=386 go build -o bin/linux/386/cursus && tar -czf bin/linux/386/cursus-$VERSION-386.tar.gz -C bin/linux/386/ $(ls  bin/linux/386/))
(cd cluster && GOOS=linux GOARCH=amd64 go build -o bin/linux/amd64/cursus && tar -czf bin/linux/amd64/cursus-$VERSION-amd64.tar.gz -C bin/linux/amd64/ $(ls  bin/linux/amd64/))
(cd cluster && GOOS=linux GOARCH=arm go build -o bin/linux/arm/cursus && tar -czf bin/linux/arm/cursus-$VERSION-arm.tar.gz -C bin/linux/arm/ $(ls  bin/linux/arm/))
(cd cluster && GOOS=linux GOARCH=arm64 go build -o bin/linux/arm64/cursus && tar -czf bin/linux/arm64/cursus-$VERSION-arm64.tar.gz -C bin/linux/arm64/ $(ls  bin/linux/arm64/))
(cd cluster && GOOS=freebsd GOARCH=arm go build -o bin/freebsd/arm/cursus && tar -czf bin/freebsd/arm/cursus-$VERSION-arm.tar.gz -C bin/freebsd/arm/ $(ls  bin/freebsd/arm/))
(cd cluster && GOOS=freebsd GOARCH=amd64 go build -o bin/freebsd/amd64/cursus && tar -czf bin/freebsd/amd64/cursus-$VERSION-amd64.tar.gz -C bin/freebsd/amd64/ $(ls  bin/freebsd/amd64/))
(cd cluster && GOOS=freebsd GOARCH=386 go build -o bin/freebsd/386/cursus && tar -czf bin/freebsd/386/cursus-$VERSION-386.tar.gz -C bin/freebsd/386/ $(ls  bin/freebsd/386/))
(cd cluster && GOOS=windows GOARCH=amd64 go build -o bin/windows/amd64/cursus.exe && zip -r -j bin/windows/amd64/cursus-$VERSION-x64.zip bin/windows/amd64/cursus.exe)
(cd cluster && GOOS=windows GOARCH=arm64 go build -o bin/windows/arm64/cursus.exe && zip -r -j bin/windows/arm64/cursus-$VERSION-x64.zip bin/windows/arm64/cursus.exe)
(cd cluster && GOOS=windows GOARCH=386 go build -o bin/windows/386/cursus.exe && zip -r -j bin/windows/386/cursus-$VERSION-x86.zip bin/windows/386/cursus.exe)


echo "Fin"