// Copyright 2025 EMQ Technologies Co., Ltd.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package httpx

import (
	"bytes"
	"fmt"
	"net"
)

var (
	// IPv6 transition mechanism prefixes that embed an IPv4 address.
	nat64Prefix     = mustParseCIDR("64:ff9b::/96") // RFC 6052 (NAT64 Well-Known Prefix)
	sixToFourPrefix = mustParseCIDR("2002::/16")    // RFC 3056 (6to4)
	teredoPrefix    = mustParseCIDR("2001::/32")    // RFC 4380 (Teredo)
	// Carrier-grade NAT (RFC 6598) is not detected by net.IP.IsPrivate.
	cgnatPrefix = mustParseCIDR("100.64.0.0/10")
	// RFC 8215 local-use NAT64 prefix. It is not globally reachable and does
	// not carry an IPv4 address at a fixed position, so it is blocked wholesale
	// rather than decoded. (Only the /96 Well-Known Prefix carries IPv4 in the
	// low 32 bits per RFC 6052.)
	nat64LocalUsePrefix = mustParseCIDR("64:ff9b:1::/48")
)

func mustParseCIDR(s string) *net.IPNet {
	_, n, err := net.ParseCIDR(s)
	if err != nil {
		panic(fmt.Sprintf("httpx: invalid CIDR %q: %v", s, err))
	}
	return n
}

// isInternalIP reports whether ip points to a loopback, private, link-local or
// otherwise reserved address that the SSRF guard must block.
//
// It decodes IPv6 transition mechanism addresses (NAT64 Well-Known Prefix,
// 6to4, Teredo and IPv4-compatible) to extract and recursively validate the
// embedded IPv4 address, so a private/reserved IPv4 destination cannot be
// reached by wrapping it inside an IPv6 transition address (GHSA-4q5r-r938-8xr2).
func isInternalIP(ip net.IP) bool {
	if ip == nil {
		return false
	}
	// Standard private/reserved ranges; works for both IPv4 and IPv6.
	if ip.IsLoopback() || ip.IsPrivate() || ip.IsUnspecified() ||
		ip.IsMulticast() || ip.IsLinkLocalUnicast() || ip.IsLinkLocalMulticast() {
		return true
	}
	if ip4 := ip.To4(); ip4 != nil {
		// Carrier-grade NAT (RFC 6598) is not covered by net.IP.IsPrivate.
		return cgnatPrefix.Contains(ip4)
	}
	ip16 := ip.To16()
	if ip16 == nil {
		return false
	}
	// RFC 8215 local-use NAT64 prefix: block outright, it is not globally
	// reachable and its IPv4 layout is operator-defined.
	if nat64LocalUsePrefix.Contains(ip16) {
		return true
	}
	if inner, ok := embeddedIPv4(ip16); ok {
		return isInternalIP(inner)
	}
	return false
}

// embeddedIPv4 extracts the IPv4 address embedded in an IPv6 transition
// mechanism address. It returns the embedded IPv4 address and true when the
// address belongs to a known transition prefix, otherwise nil and false.
func embeddedIPv4(ip16 net.IP) (net.IP, bool) {
	// IPv4-compatible (::/96 with the first 96 bits set to zero, RFC 4291,
	// deprecated). Loopback (::1) and unspecified (::) have an all-zero prefix
	// too, but they are already rejected by the standard checks in isInternalIP
	// before this function is called.
	var zero [12]byte
	if bytes.Equal(ip16[:12], zero[:]) {
		return ip16[12:16].To4(), true
	}
	switch {
	case nat64Prefix.Contains(ip16):
		// NAT64 Well-Known Prefix (RFC 6052): only /96 is well-known, and the
		// embedded IPv4 address occupies the low 32 bits. Other prefix lengths
		// are operator-chosen Network-Specific Prefixes and are not decoded.
		return ip16[12:16].To4(), true
	case sixToFourPrefix.Contains(ip16):
		// 6to4 (RFC 3056): the embedded IPv4 address occupies bits 16-47.
		return ip16[2:6].To4(), true
	case teredoPrefix.Contains(ip16):
		// Teredo (RFC 4380): the client IPv4 address occupies the low 32 bits
		// but each octet is bit-inverted.
		return net.IPv4(^ip16[12], ^ip16[13], ^ip16[14], ^ip16[15]).To4(), true
	}
	return nil, false
}
