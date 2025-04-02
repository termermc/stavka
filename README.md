# stavka
Stavka aspires to be a low-overhead, programmable caching server.

# Aspirations
## Low-overhead

Uncached requests should not wait for the cache to be filled before the client starts getting back data.

The time to first byte should be as low as possible. The time it takes for the caching server to get what it wants
should not negatively impact users of a service. Additionally, such delays can cause major issues in scenarios such as
video streaming, where large files are common and byte ranges are expected to return quickly.

---

The plan to achieve this involves proxying uncached requests directly while transparently caching the stream to disk while proxying.

Client byte range requests should not trigger a full download of the file from the origin; rather, the range should be floored to a
configured block size, and the cache should be populated starting from that block, while offsetting the returned stream until it meets
the requested range.

For example, let's say that our block size is 30 bytes, and the client requests the range `50-`.

1. The proxy will request the origin for the range `30-`.
2. The proxy will begin caching the response to disk, starting at the beginning of the range.
3. The client will not receive anything until the first 20 bytes of the response are read, at which time
the offset will effectively be `50-`.

This allows uniform blocks to be cached, but still allows granular byte ranges with minimal waiting on the caching server.
At worst, the client will have to wait for the caching server to have read `block size - 1` before getting data back.

While this has many theoretical benefits, the traditional method of caching full files to disk has its own benefits,
namely that the file will be fully cached for the next request.

To partially satisfy this in our method, we plan to allow for configurable read-ahead, which will allow the caching server to
continue reading even after the end of the range has been reached. We believe this is a better compromise, as time to first byte
and wasteful bandwidth is kept to a minimum, but we can still cache more of the file for future visitors.

The read-ahead strategy can be conceptualized like buffering a YouTube video: the client reads ahead in anticipation of future requests,
but does not buffer the entire video if it exceeds a threshold. Imagine if every hour-long YouTube video buffered completely in the
background, it would be a massive waste every time the user only watched the first few minutes. This is the kind of origin bandwidth
waste we aim to avoid.

## Programmable

Sites have a variety of caching needs; access control, expiration, and other attributes may vary from file to file, and
changes such as a video being privated should not require the origin to delete or rename the underlying file for the CDN
to stop serving it.

Currently, few good solutions exist for dynamic caching behavior.

---

In the past, we have not had great options for dynamic scripting in web servers. Nginx with OpenResty supports Lua, but that puts practical
limits on the performance and flexibility of such scripting. Fortunately, WebAssembly provides an increasingly mature interface for more
predictably performant languages to be run in a portable and sandboxed environment. The solution we propose is to use both long-lived and
per-request WebAssembly instances to provide dynamic caching behavior, providing an interface into the caching server's behavior and requests
and optionally using WASI to provide an interface into the host system as a whole for more complex behavior. Rust will be a first-class
citizen.

WebAssembly modules should be hot-swappable, allowing for not only dynamic caching behavior, but deployment of new and updated behavior
without any downtime.

# Technical Details

Here are some miscellaneous planned technical details about the project.

 - Linux-only with io_uring and Tokio
 - Intended to sit directly in front of the Internet without a reverse proxy
 - Use as little locking as possible
 - Multithreaded
