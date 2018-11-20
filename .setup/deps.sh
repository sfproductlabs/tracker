apt install golang-1.8-go
#git checkout go1.9.3
cd src
export GOROOT_BOOTSTRAP=/usr/lib/go-1.8
./all.bash
/usr/lib/go-1.8/bin/go get github.com/dioptre/tracker
