package network

import (
	"fmt"
	"github.com/golang/protobuf/proto"
	"github.com/xp/shorttext-db/glogger"
	"github.com/xp/shorttext-db/version"
	"io"
	"io/ioutil"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// NewListener returns a listener for  message transfer between peers.
// It uses timeout listener to identify broken streams promptly.
func NewNormalListener(u url.URL, tlsinfo *TLSInfo) (net.Listener, error) {
	return NewTimeoutListener(u.Host, u.Scheme, tlsinfo, ConnReadTimeout, ConnWriteTimeout)
}

// NewRoundTripper returns a roundTripper used to send requests
// to rafthttp listener of remote peers.
func NewRoundTripper(tlsInfo TLSInfo, dialTimeout time.Duration) (http.RoundTripper, error) {
	// It uses timeout transport to pair with remote timeout listeners.
	// It sets no read/write timeout, because message in requests may
	// take long time to write out before reading out the response.
	return NewTimeoutTransport(tlsInfo, dialTimeout, 0, 0)
}

// newStreamRoundTripper returns a roundTripper used to send stream requests
// to rafthttp listener of remote peers.
// Read/write timeout is set for stream roundTripper to promptly
// find out broken status, which minimizes the number of messages
// sent on broken connection.
func newStreamRoundTripper(tlsInfo TLSInfo, dialTimeout time.Duration) (http.RoundTripper, error) {
	return NewTimeoutTransport(tlsInfo, dialTimeout, ConnReadTimeout, ConnWriteTimeout)
}

// createPostRequest creates a HTTP POST request that sends  message.
func createPostRequest(u url.URL, path string, body io.Reader, ct string, urls URLs, from, cid ID) *http.Request {
	uu := u
	uu.Path = path
	req, err := http.NewRequest("POST", uu.String(), body)
	if err != nil {
		logger.Panicf("unexpected new request error (%v)", err)
	}
	req.Header.Set("Content-Type", ct)
	req.Header.Set("X-Server-From", from.String())
	req.Header.Set("X-Server-Version", ServerVersion)

	setPeerURLsHeader(req, urls)

	return req
}

// checkPostResponse checks the response of the HTTP POST request that sends
// raft message.
func checkPostResponse(resp *http.Response, body []byte, req *http.Request, to ID) error {
	switch resp.StatusCode {
	case http.StatusPreconditionFailed:
		switch strings.TrimSuffix(string(body), "\n") {
		case errIncompatibleVersion.Error():
			logger.Errorf("request sent was ignored by peer %s (server version incompatible)", to)
			return errIncompatibleVersion
		default:
			return fmt.Errorf("unhandled error %q when precondition failed", string(body))
		}
	case http.StatusForbidden:
		return errMemberRemoved
	case http.StatusNoContent:
		return nil
	default:
		return fmt.Errorf("unexpected http status %s while posting to %q", http.StatusText(resp.StatusCode), req.URL.String())
	}
}

// reportCriticalError reports the given error through sending it into
// the given error channel.
// If the error channel is filled up when sending error, it drops the error
// because the fact that error has happened is reported, which is
// good enough.
func reportCriticalError(err error, errc chan<- error) {
	select {
	case errc <- err:
	default:
	}
}

// compareMajorMinorVersion returns an integer comparing two versions based on
// their major and minor version. The result will be 0 if a==b, -1 if a < b,
// and 1 if a > b.
func compareMajorMinorVersion(a, b *version.Version) int {
	na := &version.Version{Major: a.Major, Minor: a.Minor}
	nb := &version.Version{Major: b.Major, Minor: b.Minor}
	switch {
	case na.LessThan(*nb):
		return -1
	case nb.LessThan(*na):
		return 1
	default:
		return 0
	}
}

// serverVersion returns the server version from the given header.
func serverVersion(h http.Header) *version.Version {
	verStr := h.Get("X-Server-Version")
	// backward compatibility with etcd 2.0
	if verStr == "" {
		verStr = "1.0.0"
	}
	return version.Must(version.NewVersion(verStr))
}

// setPeerURLsHeader reports local urls for peer discovery
func setPeerURLsHeader(req *http.Request, urls URLs) {
	if urls == nil {
		// often not set in unit tests
		return
	}
	peerURLs := make([]string, urls.Len())
	for i := range urls {
		peerURLs[i] = urls[i].String()
	}
	req.Header.Set("X-PeerURLs", strings.Join(peerURLs, ","))
}

// addRemoteFromRequest adds a remote peer according to an http request header
func addRemoteFromRequest(tr Transporter, r *http.Request) {
	if from, err := IDFromString(r.Header.Get("X-Server-From")); err == nil {
		if urls := r.Header.Get("X-PeerURLs"); urls != "" {
			tr.AddRemote(from, strings.Split(urls, ","))
		}
	}
}

// GracefulClose drains http.Response.Body until it hits EOF
// and closes it. This prevents TCP/TLS connections from closing,
// therefore available for reuse.
func GracefulClose(resp *http.Response) {
	io.Copy(ioutil.Discard, resp.Body)
	resp.Body.Close()
}

func Size(pb proto.Message) int {
	return proto.Size(pb)
}

func MustMarshal(pb proto.Message) []byte {
	d, err := proto.Marshal(pb)
	if err != nil {
		panic(fmt.Sprintf("marshal should never fail (%v)", err))
	}
	return d
}

func MustUnmarshal(pb proto.Message, data []byte) {
	if err := proto.Unmarshal(data, pb); err != nil {
		panic(fmt.Sprintf("unmarshal should never fail (%v)", err))
	}
}

func Marshal(pb proto.Message) ([]byte, error) {
	return proto.Marshal(pb)
}
func Unmarshal(pb proto.Message, data []byte) error {
	return proto.Unmarshal(data, pb)
}

func SetLogLevel(level string) {
	glogger.SetModuleLevel("network", level)
}
