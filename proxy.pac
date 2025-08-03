function FindProxyForURL(url, host) {
    // Convert hostname to lowercase for case-insensitive matching
    host = host.toLowerCase();
    
    // Use proxy for server.palecci.cz and its subdomains
    if (dnsDomainIs(host, ".palecci.cz") || host == "server.palecci.cz") {
        return "PROXY localhost:3128";
    }
    
    // You can add more specific rules here:
    // For example, to proxy specific domains:
    // if (dnsDomainIs(host, ".example.com")) {
    //     return "PROXY localhost:3128";
    // }
    
    // For internal networks, you might want:
    // if (isInNet(host, "10.0.0.0", "255.0.0.0") ||
    //     isInNet(host, "172.16.0.0", "255.240.0.0") ||
    //     isInNet(host, "192.168.0.0", "255.255.0.0")) {
    //     return "DIRECT";
    // }
    
    // Use direct connection for all other addresses
    return "DIRECT";
}
