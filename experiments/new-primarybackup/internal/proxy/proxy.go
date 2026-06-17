package proxy

import (
	"context"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"sync"
	"time"
)

// writeMethods is the set of HTTP methods that mutate state.
// All of these are always forwarded to the primary container.
var writeMethods = map[string]bool{
	http.MethodPost:   true,
	http.MethodPut:    true,
	http.MethodPatch:  true,
	http.MethodDelete: true,
}

// NodeProxy is one listener bound to a public port.
// Reads go to its own container; writes go to primary.
type NodeProxy struct {
	listenPort int
	role       string
	targetMu   sync.RWMutex
	target     *url.URL
	primary    *url.URL
	server     *http.Server
	syncFn     func() error
	lockFn     func()
	unlockFn   func()
	statMu           sync.Mutex
	reqCount         int
	totalPrimary     time.Duration
	totalSync        time.Duration
	totalRequest     time.Duration
	logTiming 	 bool
	disableWriteRedirect bool
}

func (n *NodeProxy) getTarget() *url.URL {
	n.targetMu.RLock()
	defer n.targetMu.RUnlock()
	return n.target
}

func (n *NodeProxy) setTarget(u *url.URL) {
	n.targetMu.Lock()
	n.target = u
	n.targetMu.Unlock()
}

// Proxy manages all three NodeProxy instances.
type Proxy struct {
	nodes 		[]*NodeProxy
	switchoverLog 	*os.File
}

func (p *Proxy) SwapTarget(role string, newPort int) {
	for _, n := range p.nodes {
		if n.role == role {
			n.setTarget(hostURL(newPort))
			log.Printf("[proxy] %s target swapped → :%d", role, newPort)
			if p.switchoverLog != nil {
				fmt.Fprintf(p.switchoverLog, "%d %s\n", time.Now().UnixMilli(), role)
			}
			return
		}
	}
}


// NodeConfig describes a single node's public port and its container's host port.
type NodeConfig struct {
	Role          string
	ListenPort    int
	ContainerPort int
	SyncFn        func() error
	LockFn        func()
	UnlockFn      func()
	LogTiming     bool
	DisableWriteRedirect bool
}

// New creates a Proxy from a list of NodeConfigs.
// The first entry whose role == "primary" is used as the write target.
func New(nodes []NodeConfig) *Proxy {
	// Find primary URL.
	var primaryURL *url.URL
	for _, n := range nodes {
		if n.Role == "primary" {
			primaryURL = hostURL(n.ContainerPort)
			break
		}
	}
	if primaryURL == nil {
		panic("proxy.New: no primary node found in config")
	}

	var nodeProxies []*NodeProxy
	for _, n := range nodes {
		nodeProxies = append(nodeProxies, &NodeProxy{
			listenPort: n.ListenPort,
			role:       n.Role,
			target:     hostURL(n.ContainerPort),
			primary:    primaryURL,
			syncFn:     n.SyncFn,
			lockFn:     n.LockFn,
			unlockFn:   n.UnlockFn,
			logTiming:  n.LogTiming,
			disableWriteRedirect: n.DisableWriteRedirect,
		})
	}
	return &Proxy{nodes: nodeProxies}
}

func hostURL(port int) *url.URL {
	u, _ := url.Parse(fmt.Sprintf("http://127.0.0.1:%d", port))
	return u
}

// Start begins listening on all node ports. Non-blocking.
func (p *Proxy) Start() error {
	for _, n := range p.nodes {
		if err := n.start(); err != nil {
			return err
		}
	}
	return nil
}

// Stop gracefully shuts down all proxy servers.
func (p *Proxy) Stop() {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for _, n := range p.nodes {
		if n.server != nil {
			if err := n.server.Shutdown(ctx); err != nil {
				log.Printf("[proxy] shutdown :%d: %v", n.listenPort, err)
			}
		}
	}
}

func (n *NodeProxy) start() error {
	mux := http.NewServeMux()
	mux.HandleFunc("/", n.handle)

	n.server = &http.Server{
		Addr:         fmt.Sprintf(":%d", n.listenPort),
		Handler:      mux,
		ReadTimeout:  30 * time.Second,
		WriteTimeout: 30 * time.Second,
	}

	ln, err := net.Listen("tcp", n.server.Addr)
	if err != nil {
		return fmt.Errorf("listen :%d: %w", n.listenPort, err)
	}

	log.Printf("[proxy] %s listening on :%d → container %s (writes → %s)",
		n.role, n.listenPort, n.target, n.primary)

	go func() {
		if err := n.server.Serve(ln); err != nil && err != http.ErrServerClosed {
			log.Printf("[proxy] :%d server error: %v", n.listenPort, err)
		}
	}()
	return nil
}

func (n *NodeProxy) handle(w http.ResponseWriter, r *http.Request) {
	var dest *url.URL
	var destLabel string
	var requestStart time.Time

	if writeMethods[r.Method] && !n.disableWriteRedirect {
		dest = n.primary
		destLabel = "primary"
	} else {
		dest = n.getTarget()
		destLabel = n.role
	}

	//log.Printf("[proxy:%s] %s %s → %s (%s)", n.role, r.Method, r.URL.Path, destLabel, dest)

	if writeMethods[r.Method] {
		requestStart = time.Now()
		if !n.disableWriteRedirect && n.lockFn != nil {
			n.lockFn()
			requestStart = time.Now() // reset after lock acquired
		}
	}

	rp := httputil.NewSingleHostReverseProxy(dest)
	rp.ErrorHandler = func(w http.ResponseWriter, r *http.Request, err error) {
		if writeMethods[r.Method] && !n.disableWriteRedirect && n.unlockFn != nil {
			//log.Printf("[proxy:%s] releasing lock via error handler", n.role)
			n.unlockFn()
		}
		log.Printf("[proxy:%s] upstream error forwarding to %s: %v", n.role, destLabel, err)
		http.Error(w, "upstream unavailable", http.StatusBadGateway)
	}
	if writeMethods[r.Method] && ((!n.disableWriteRedirect && n.syncFn != nil) || n.logTiming) {
		rp.ModifyResponse = func(resp *http.Response) error {
			defer func() {
				if !n.disableWriteRedirect && n.unlockFn != nil {
					n.unlockFn()
				}
			}()
			primaryElapsed := time.Since(requestStart)
			if !n.disableWriteRedirect {
				if err := n.syncFn(); err != nil {
					log.Printf("[proxy:%s] sync failed: %v", n.role, err)
				}
			}
			totalElapsed := time.Since(requestStart)
			syncElapsed := totalElapsed - primaryElapsed

			n.statMu.Lock()
			n.reqCount++
			n.totalPrimary += primaryElapsed
			n.totalSync += syncElapsed
			n.totalRequest += totalElapsed
			count := n.reqCount
			avgPrimary := n.totalPrimary / time.Duration(count)
			avgSync := n.totalSync / time.Duration(count)
			avgTotal := n.totalRequest / time.Duration(count)
			n.statMu.Unlock()

			if n.logTiming && count%100 == 0 {
				log.Printf("[proxy:%s] avg over %d requests — primary: %v | sync: %v | total: %v",
				n.role, count, avgPrimary, avgSync, avgTotal)
			}
			return nil
		}
	}
	rp.ServeHTTP(w, r)
}

func (p *Proxy) OpenSwitchoverLog(path string) error {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	p.switchoverLog = f
	return nil
}

func (p *Proxy) CloseSwitchoverLog() {
	if p.switchoverLog != nil {
		p.switchoverLog.Close()
	}
}
