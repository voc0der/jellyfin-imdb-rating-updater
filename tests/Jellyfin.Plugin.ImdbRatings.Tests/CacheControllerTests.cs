using Jellyfin.Plugin.ImdbRatings.Api;
using MediaBrowser.Common.Configuration;
using Microsoft.AspNetCore.Mvc;

namespace Jellyfin.Plugin.ImdbRatings.Tests;

public class CacheControllerTests : IDisposable
{
    private readonly string _tempDir;

    public CacheControllerTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), "imdb-cache-tests-" + Guid.NewGuid().ToString("N")[..8]);
        Directory.CreateDirectory(_tempDir);
    }

    public void Dispose()
    {
        if (Directory.Exists(_tempDir))
        {
            Directory.Delete(_tempDir, recursive: true);
        }
    }

    [Fact]
    public void GetStatus_NoCacheFile_ReturnsExistsFalse()
    {
        var controller = CreateController();

        var result = controller.GetStatus();

        var dto = GetValue(result);
        Assert.False(dto.Exists);
        Assert.Equal(0, dto.FileSizeBytes);
        Assert.Null(dto.LastModifiedUtc);
        Assert.False(dto.IsFresh);
        Assert.Equal(23.0, dto.TtlHours);
    }

    [Fact]
    public void GetStatus_FreshCache_ReturnsCorrectInfo()
    {
        var cachePath = CreateCacheFile("tconst\taverageRating\tnumVotes\ntt0000001\t7.5\t1000\n");

        var controller = CreateController();
        var result = controller.GetStatus();

        var dto = GetValue(result);
        Assert.True(dto.Exists);
        Assert.True(dto.FileSizeBytes > 0);
        Assert.NotNull(dto.LastModifiedUtc);
        Assert.True(dto.IsFresh);
    }

    [Fact]
    public void GetStatus_ExpiredCache_ReturnsIsFreshFalse()
    {
        var cachePath = CreateCacheFile("tconst\taverageRating\tnumVotes\n");
        File.SetLastWriteTimeUtc(cachePath, DateTime.UtcNow.AddHours(-24));

        var controller = CreateController();
        var result = controller.GetStatus();

        var dto = GetValue(result);
        Assert.True(dto.Exists);
        Assert.False(dto.IsFresh);
    }

    [Fact]
    public void Delete_CacheExists_DeletesFileAndReturnsNoContent()
    {
        var cachePath = CreateCacheFile("tconst\taverageRating\tnumVotes\n");
        Assert.True(File.Exists(cachePath));

        var controller = CreateController();
        var result = controller.Delete();

        Assert.IsType<NoContentResult>(result);
        Assert.False(File.Exists(cachePath));
    }

    [Fact]
    public void Delete_TempFileExists_AlsoDeleted()
    {
        var cachePath = CreateCacheFile("data");
        var tempPath = cachePath + ".tmp";
        File.WriteAllText(tempPath, "partial");

        var controller = CreateController();
        controller.Delete();

        Assert.False(File.Exists(cachePath));
        Assert.False(File.Exists(tempPath));
    }

    [Fact]
    public void Delete_NoCacheFile_ReturnsNotFound()
    {
        var controller = CreateController();
        var result = controller.Delete();

        Assert.IsType<NotFoundResult>(result);
    }

    [Fact]
    public void Delete_OnlyTempFileExists_ReturnsNoContent()
    {
        var cacheDir = Path.Combine(_tempDir, "imdb-ratings-cache");
        Directory.CreateDirectory(cacheDir);
        var tempPath = Path.Combine(cacheDir, "title.ratings.tsv.tmp");
        File.WriteAllText(tempPath, "partial download");

        var controller = CreateController();
        var result = controller.Delete();

        Assert.IsType<NoContentResult>(result);
        Assert.False(File.Exists(tempPath));
    }

    [Fact]
    public void GetStatus_CacheJustWithinTtl_ReportsAsFresh()
    {
        var cachePath = CreateCacheFile("tconst\taverageRating\tnumVotes\n");
        File.SetLastWriteTimeUtc(cachePath, DateTime.UtcNow.AddHours(-22).AddMinutes(-59));

        var controller = CreateController();
        var result = controller.GetStatus();

        var dto = GetValue(result);
        Assert.True(dto.IsFresh);
    }

    [Fact]
    public void GetStatus_CacheJustPastTtl_ReportsAsExpired()
    {
        var cachePath = CreateCacheFile("tconst\taverageRating\tnumVotes\n");
        File.SetLastWriteTimeUtc(cachePath, DateTime.UtcNow.AddHours(-23).AddMinutes(-1));

        var controller = CreateController();
        var result = controller.GetStatus();

        var dto = GetValue(result);
        Assert.False(dto.IsFresh);
    }

    private CacheController CreateController()
    {
        return new CacheController(new FakeApplicationPaths(_tempDir));
    }

    private string CreateCacheFile(string content)
    {
        var cacheDir = Path.Combine(_tempDir, "imdb-ratings-cache");
        Directory.CreateDirectory(cacheDir);
        var cachePath = Path.Combine(cacheDir, "title.ratings.tsv");
        File.WriteAllText(cachePath, content);
        return cachePath;
    }

    private static CacheStatusDto GetValue(ActionResult<CacheStatusDto> result)
    {
        var okResult = Assert.IsType<OkObjectResult>(result.Result);
        return Assert.IsType<CacheStatusDto>(okResult.Value);
    }

    private sealed class FakeApplicationPaths : IApplicationPaths
    {
        public FakeApplicationPaths(string dataPath)
        {
            DataPath = dataPath;
        }

        public string ProgramDataPath => string.Empty;

        public string WebPath => string.Empty;

        public string ProgramSystemPath => string.Empty;

        public string DataPath { get; }

        public string ImageCachePath => string.Empty;

        public string PluginsPath => string.Empty;

        public string PluginConfigurationsPath => string.Empty;

        public string LogDirectoryPath => string.Empty;

        public string ConfigurationDirectoryPath => string.Empty;

        public string SystemConfigurationFilePath => string.Empty;

        public string CachePath => string.Empty;

        public string TempDirectory => string.Empty;

        public string VirtualDataPath => string.Empty;

        public string TrickplayPath => string.Empty;

        public string BackupPath => string.Empty;

        public void MakeSanityCheckOrThrow()
        {
        }

        public void CreateAndCheckMarker(string name, string value, bool overwrite = false)
        {
        }
    }
}
