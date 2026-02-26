using System;
using System.IO;
using System.IO.Compression;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;

namespace Jellyfin.Plugin.ImdbRatings.Providers;

public class ImdbFlatFileDownloader
{
    private const string ImdbRatingsUrl = "https://datasets.imdbws.com/title.ratings.tsv.gz";
    private static readonly TimeSpan CacheMaxAge = TimeSpan.FromHours(23);

    private readonly IHttpClientFactory _httpClientFactory;
    private readonly ILogger<ImdbFlatFileDownloader> _logger;
    private readonly string _cachePath;

    public ImdbFlatFileDownloader(
        IHttpClientFactory httpClientFactory,
        ILogger<ImdbFlatFileDownloader> logger,
        string dataPath)
    {
        _httpClientFactory = httpClientFactory;
        _logger = logger;
        _cachePath = Path.Combine(dataPath, "imdb-ratings-cache", "title.ratings.tsv");
    }

    public async Task<string> GetRatingsFilePathAsync(CancellationToken cancellationToken)
    {
        if (IsCacheFresh())
        {
            _logger.LogInformation("IMDb ratings cache is fresh, skipping download");
            return _cachePath;
        }

        await DownloadAndDecompressAsync(cancellationToken).ConfigureAwait(false);
        return _cachePath;
    }

    private bool IsCacheFresh()
    {
        if (!File.Exists(_cachePath))
        {
            return false;
        }

        var lastWrite = File.GetLastWriteTimeUtc(_cachePath);
        return DateTime.UtcNow - lastWrite < CacheMaxAge;
    }

    private async Task DownloadAndDecompressAsync(CancellationToken cancellationToken)
    {
        _logger.LogInformation("Downloading IMDb ratings from {Url}", ImdbRatingsUrl);

        var cacheDir = Path.GetDirectoryName(_cachePath)!;
        Directory.CreateDirectory(cacheDir);

        var tempPath = _cachePath + ".tmp";

        var client = _httpClientFactory.CreateClient("ImdbRatings");
        using var response = await client.GetAsync(ImdbRatingsUrl, HttpCompletionOption.ResponseHeadersRead, cancellationToken).ConfigureAwait(false);
        response.EnsureSuccessStatusCode();

        try
        {
            using var compressedStream = await response.Content.ReadAsStreamAsync(cancellationToken).ConfigureAwait(false);
            using var gzipStream = new GZipStream(compressedStream, CompressionMode.Decompress);
            using var fileStream = new FileStream(tempPath, FileMode.Create, FileAccess.Write, FileShare.None);
            await gzipStream.CopyToAsync(fileStream, cancellationToken).ConfigureAwait(false);
        }
        catch
        {
            TryDeleteFile(tempPath);
            throw;
        }

        File.Move(tempPath, _cachePath, overwrite: true);
        _logger.LogInformation("IMDb ratings file downloaded and decompressed to {Path}", _cachePath);
    }

    private void TryDeleteFile(string path)
    {
        try
        {
            File.Delete(path);
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Failed to clean up temporary file {Path}", path);
        }
    }
}
