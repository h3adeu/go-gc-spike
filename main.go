package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"math/rand/v2"
	"os"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
)

type node struct {
	next *node
	refs []*node
	data []byte
}

// buildHeap はポインタの多いグラフを targetMB 分作り、ランダム参照用の索引も返す
func buildHeap(targetMB int) (*node, []*node) {
	// 1ノードあたり struct 64B + refs 32B + data 64B + 索引 8B = 168B
	n := targetMB * 1024 * 1024 / 168
	idx := make([]*node, n)
	var head *node
	r := rand.New(rand.NewPCG(1, 2))
	for i := 0; i < n; i++ {
		nd := &node{next: head, refs: make([]*node, 4), data: make([]byte, 64)}
		for j := range nd.refs {
			if i > 0 {
				nd.refs[j] = idx[r.IntN(i)]
			}
		}
		idx[i] = nd
		head = nd
	}
	return head, idx
}

// latStat はゴルーチンごとに持ち、計測中に共有メモリへ書かない（false sharing 回避のためパディング）
type latStat struct {
	ops    uint64
	over1  uint64
	maxNs  int64
	_      [40]byte
}

func (s *latStat) record(d time.Duration) {
	s.ops++
	if d > time.Millisecond {
		s.over1++
	}
	if int64(d) > s.maxNs {
		s.maxNs = int64(d)
	}
}

// pacer は目標レートでアロケーションを生むための単純なスリープ制御
func pace(stop *atomic.Bool, start chan struct{}, bytesPerSec int, chunk int, sink *[]byte) {
	<-start
	perTick := bytesPerSec / 100 / chunk // 10ms ごとに配る
	if perTick < 1 {
		perTick = 1
	}
	next := time.Now()
	for !stop.Load() {
		for i := 0; i < perTick; i++ {
			*sink = make([]byte, chunk)
		}
		next = next.Add(10 * time.Millisecond)
		if d := time.Until(next); d > 0 {
			time.Sleep(d)
		}
	}
}

type sample struct {
	Sec     int     `json:"sec"`
	GC      uint32  `json:"gc"`
	AllocMB float64 `json:"alloc_mb"`
}

type result struct {
	Mode        string   `json:"mode"`
	GOGC        string   `json:"gogc"`
	GoVersion   string   `json:"go_version"`
	GOMAXPROCS  int      `json:"gomaxprocs"`
	DurationSec int      `json:"duration_sec"`
	LiveHeapMB  float64  `json:"live_heap_mb"`
	HeapObjects uint64   `json:"heap_objects"`
	NumGC       uint32   `json:"num_gc"`
	GCAtSec     []int    `json:"gc_at_sec"`
	STWTotalUs  float64  `json:"stw_total_us"`
	STWMaxUs    float64  `json:"stw_max_us"`
	AllocMBps   float64  `json:"alloc_mb_per_sec"`
	BytesPerOp  float64  `json:"bytes_per_op"`
	Ops         uint64   `json:"ops"`
	OpsPerSec   float64  `json:"ops_per_sec"`
	LatMaxUs    float64  `json:"lat_max_us"`
	LatOver1ms  uint64   `json:"lat_over_1ms"`
	LatOver1Pct float64  `json:"lat_over_1ms_pct"`
	Goroutines  int      `json:"goroutines"`
	Samples     []sample `json:"samples"`
}

// ---- kv モード: VortexKV 型。シャード + パディング、in-place 上書き、ゼロアロケのパース ----

type entry struct{ val []byte }

type shard struct {
	mu sync.RWMutex
	m  map[string]*entry
	_  [64]byte
}

const numShards = 256

func fnv(b []byte) uint32 {
	h := uint32(2166136261)
	for _, c := range b {
		h ^= uint32(c)
		h *= 16777619
	}
	return h
}

// parseCmd は RESP 配列を読み、引数を元バッファのスライスとして返す（string 変換しないのでアロケーションなし）
func parseCmd(buf []byte, args [][]byte) [][]byte {
	args = args[:0]
	i := 0
	if buf[i] != '*' {
		return args
	}
	i++
	n := 0
	for buf[i] != '\r' {
		n = n*10 + int(buf[i]-'0')
		i++
	}
	i += 2
	for k := 0; k < n; k++ {
		i++ // '$'
		l := 0
		for buf[i] != '\r' {
			l = l*10 + int(buf[i]-'0')
			i++
		}
		i += 2
		args = append(args, buf[i:i+l])
		i += l + 2
	}
	return args
}

func cmd4(b []byte) uint32 {
	return uint32(b[0]|0x20) | uint32(b[1]|0x20)<<8 | uint32(b[2]|0x20)<<16
}

var (
	cmdGET = cmd4([]byte("get"))
	cmdSET = cmd4([]byte("set"))
)

func runKV(stop *atomic.Bool, workers int, stats []latStat, ready *sync.WaitGroup, start chan struct{}) (live func()) {
	shards := make([]shard, numShards)
	for i := range shards {
		shards[i].m = make(map[string]*entry)
	}
	const keys = 100000
	val := make([]byte, 32)
	for k := 0; k < keys; k++ {
		key := fmt.Sprintf("key:%06d", k)
		s := &shards[fnv([]byte(key))%numShards]
		s.m[key] = &entry{val: append(make([]byte, 0, 64), val...)}
	}
	// 各ワーカーは事前に組み立てた RESP コマンド列を読む（ソケットの代わりに固定バッファ）
	cmds := make([][]byte, 1024)
	r := rand.New(rand.NewPCG(3, 4))
	for i := range cmds {
		key := fmt.Sprintf("key:%06d", r.IntN(keys))
		if i%2 == 0 {
			cmds[i] = []byte(fmt.Sprintf("*2\r\n$3\r\nGET\r\n$%d\r\n%s\r\n", len(key), key))
		} else {
			v := fmt.Sprintf("v%031d", r.IntN(1<<30))
			cmds[i] = []byte(fmt.Sprintf("*3\r\n$3\r\nSET\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n", len(key), key, len(v), v))
		}
	}
	for w := 0; w < workers; w++ {
		ready.Add(1)
		go func(st *latStat, off int) {
			args := make([][]byte, 0, 4)
			out := make([]byte, 0, 128)
			ready.Done()
			<-start
			for i := off; !stop.Load(); i++ {
				t0 := time.Now()
				args = parseCmd(cmds[i&1023], args)
				s := &shards[fnv(args[1])%numShards]
				switch cmd4(args[0]) {
				case cmdGET:
					s.mu.RLock()
					if e, ok := s.m[string(args[1])]; ok {
						out = append(out[:0], e.val...)
					}
					s.mu.RUnlock()
				case cmdSET:
					s.mu.Lock()
					if e, ok := s.m[string(args[1])]; ok {
						e.val = append(e.val[:0], args[2]...)
					}
					s.mu.Unlock()
				}
				st.record(time.Since(t0))
			}
		}(&stats[w], w*61)
	}
	return func() { runtime.KeepAlive(shards) }
}

// ---- force / spike 用の軽量 read op ----

func runReaders(stop *atomic.Bool, idx []*node, workers int, stats []latStat, ready *sync.WaitGroup, start chan struct{}) {
	for w := 0; w < workers; w++ {
		ready.Add(1)
		go func(st *latStat, seed uint64) {
			r := rand.New(rand.NewPCG(seed, seed+1))
			var acc byte
			ready.Done()
			<-start
			for !stop.Load() {
				t0 := time.Now()
				acc ^= idx[r.IntN(len(idx))].data[0]
				st.record(time.Since(t0))
			}
			runtime.KeepAlive(acc)
		}(&stats[w], uint64(w+10))
	}
}

func main() {
	mode := flag.String("mode", "kv", "kv | spike | force")
	dur := flag.Duration("d", 10*time.Second, "計測時間")
	workers := flag.Int("w", 16, "計測用ワーカー数")
	flag.Parse()

	gogc := os.Getenv("GOGC")
	if gogc == "" {
		gogc = "100"
	}

	var (
		stop  atomic.Bool
		ready sync.WaitGroup
		start = make(chan struct{})
		stats = make([]latStat, *workers)
		sink  []byte
		keep  func()
		head  *node
		idx   []*node
	)

	switch *mode {
	case "kv":
		keep = runKV(&stop, *workers, stats, &ready, start)
	case "spike":
		head, idx = buildHeap(500)
		runReaders(&stop, idx, *workers, stats, &ready, start)
		sink2 := new([]byte)
		go pace(&stop, start, 512*1024*1024, 64*1024, &sink)
		go pace(&stop, start, 512*1024*1024, 64*1024, sink2)
	case "force":
		head, idx = buildHeap(600)
		runReaders(&stop, idx, *workers, stats, &ready, start)
		go pace(&stop, start, 900*1024, 512, &sink)
	default:
		fmt.Fprintln(os.Stderr, "unknown mode")
		os.Exit(1)
	}
	ready.Wait()

	// 構築中のGCを計測から外し、120秒タイマーの起点（last_gc）を計測開始にそろえる。GOGC=off でも runtime.GC は動く
	runtime.GC()
	var m0 runtime.MemStats
	runtime.ReadMemStats(&m0)
	res := result{
		Mode: *mode, GOGC: gogc, GoVersion: runtime.Version(), GOMAXPROCS: runtime.GOMAXPROCS(0),
		DurationSec: int(dur.Seconds()), LiveHeapMB: float64(m0.HeapAlloc) / (1 << 20), HeapObjects: m0.HeapObjects,
	}

	close(start)
	t0 := time.Now()
	prev := m0
	tick := time.NewTicker(time.Second)
	for sec := 1; sec <= int(dur.Seconds()); sec++ {
		<-tick.C
		var m runtime.MemStats
		runtime.ReadMemStats(&m)
		for g := prev.NumGC + 1; g <= m.NumGC; g++ {
			p := float64(m.PauseNs[(g+255)%256]) / 1e3
			res.STWTotalUs += p
			if p > res.STWMaxUs {
				res.STWMaxUs = p
			}
			res.GCAtSec = append(res.GCAtSec, sec)
		}
		res.Samples = append(res.Samples, sample{Sec: sec, GC: m.NumGC - prev.NumGC, AllocMB: float64(m.TotalAlloc-prev.TotalAlloc) / (1 << 20)})
		prev = m
	}
	stop.Store(true)
	elapsed := time.Since(t0).Seconds()
	res.Goroutines = runtime.NumGoroutine()
	time.Sleep(50 * time.Millisecond)

	res.NumGC = prev.NumGC - m0.NumGC
	res.AllocMBps = float64(prev.TotalAlloc-m0.TotalAlloc) / (1 << 20) / elapsed
	var maxNs int64
	for i := range stats {
		res.Ops += stats[i].ops
		res.LatOver1ms += stats[i].over1
		if stats[i].maxNs > maxNs {
			maxNs = stats[i].maxNs
		}
	}
	res.LatMaxUs = float64(maxNs) / 1e3
	res.OpsPerSec = float64(res.Ops) / elapsed
	if res.Ops > 0 {
		res.BytesPerOp = float64(prev.TotalAlloc-m0.TotalAlloc) / float64(res.Ops)
		res.LatOver1Pct = float64(res.LatOver1ms) / float64(res.Ops) * 100
	}
	runtime.KeepAlive(head)
	runtime.KeepAlive(idx)
	runtime.KeepAlive(sink)
	if keep != nil {
		keep()
	}
	json.NewEncoder(os.Stdout).Encode(res)
}
