/// The HTML to return for 404 pages.
pub const NOT_FOUND_HTML: &[u8] = b"<!doctype html>
<html>
<head>
    <title>404 Not Found</title>
</head>
<body>
    <center><h1>404 Not Found</h1></center>
    <hr/>
    <center>stavka</center>
</body>
</html>
";

/// The maximum size (in bytes) of cache block coverage to skip over.
/// To explain what this means, take a look at the following coverage (where `-` is a filled block):
///
/// ```
/// ---_--_---
/// ```
///
/// If the max skip size is 2 blocks in this case (although it is actually measured in bytes), then instead of making 2 HTTP requests
/// for the 2 missing gaps, it will instead make one request starting from the first missing gap and ending at the end of the last missing gap.
///
/// The missing gaps in the coverage below would be fetched from origin (notice the lack of the two filled blocks):
///
/// ```
/// ---____---
/// ```
///
/// This is because there were only 2 blocks filled, so it was skipped and the entire region was fetched from origin.
///
/// This is to prevent making many tiny HTTP requests when one larger one can cover several small gaps in coverage.
pub const MAX_COVERAGE_BLOCK_SKIP_SIZE: u64 = 5 * 1024 * 1024;
