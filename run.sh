#!/bin/sh
# 記事「go-gc-spike-article」の計測を再現する。記事の数値はこのスクリプトで取得した
# CPU 2コア・メモリ 4GB に固定するのは、記事の検証環境に条件をそろえるため
set -eu
cd "$(dirname "$0")"
IMG=golang:1.27.3
docker run --rm -v "$PWD":/src -w /src "$IMG" sh -c 'go vet . && go build -o gcbench .'
mkdir -p results
run() {
	name=$1; gogc=$2; shift 2
	docker run --rm --cpuset-cpus=0,1 --memory=4g -e GOGC="$gogc" -v "$PWD":/src -w /src "$IMG" ./gcbench "$@" > "results/$name.json"
	echo "$name done"
}
run kv10      100 -mode kv    -d 10s  -w 16
run kv140     100 -mode kv    -d 140s -w 16
# spike/force は読み取りワーカーを2にする。2コアに16本載せるとプリエンプション待ちが遅延を支配するため
run spike15   100 -mode spike -d 15s  -w 2
run force_off off -mode force -d 145s -w 2
run force_100 100 -mode force -d 145s -w 2
