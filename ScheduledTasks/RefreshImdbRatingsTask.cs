using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using Jellyfin.Data.Enums;
using Jellyfin.Plugin.ImdbRatings.Configuration;
using Jellyfin.Plugin.ImdbRatings.Providers;
using MediaBrowser.Controller.Entities;
using MediaBrowser.Controller.Library;
using MediaBrowser.Model.Entities;
using MediaBrowser.Model.Tasks;
using Microsoft.Extensions.Logging;

namespace Jellyfin.Plugin.ImdbRatings.ScheduledTasks;

public class RefreshImdbRatingsTask : IScheduledTask
{
    private readonly ILibraryManager _libraryManager;
    private readonly IHttpClientFactory _httpClientFactory;
    private readonly ILogger<RefreshImdbRatingsTask> _logger;
    private readonly ILoggerFactory _loggerFactory;
    private readonly string _dataPath;

    public RefreshImdbRatingsTask(
        ILibraryManager libraryManager,
        IHttpClientFactory httpClientFactory,
        ILogger<RefreshImdbRatingsTask> logger,
        ILoggerFactory loggerFactory,
        MediaBrowser.Common.Configuration.IApplicationPaths applicationPaths)
    {
        _libraryManager = libraryManager;
        _httpClientFactory = httpClientFactory;
        _logger = logger;
        _loggerFactory = loggerFactory;
        _dataPath = applicationPaths.DataPath;
    }

    public string Name => "Refresh IMDb Ratings";

    public string Key => "RefreshImdbRatings";

    public string Description => "Downloads the IMDb ratings flat file and updates CommunityRating on all library items with an IMDb ID.";

    public string Category => "IMDb Ratings";

    public IEnumerable<TaskTriggerInfo> GetDefaultTriggers()
    {
        return new[]
        {
            new TaskTriggerInfo
            {
                Type = TaskTriggerInfoType.DailyTrigger,
                TimeOfDayTicks = TimeSpan.FromHours(3).Ticks
            }
        };
    }

    public async Task ExecuteAsync(IProgress<double> progress, CancellationToken cancellationToken)
    {
        var config = Plugin.Instance?.Configuration ?? new PluginConfiguration();

        _logger.LogInformation("Starting IMDb ratings refresh (minVotes={MinVotes}, movies={Movies}, series={Series})",
            config.MinimumVotes, config.IncludeMovies, config.IncludeSeries);

        // Step 1: Query library items and build a distinct IMDb ID filter set.
        progress.Report(0);
        var items = GetLibraryItems(config);
        if (items.Count == 0)
        {
            _logger.LogInformation("Found 0 library items with IMDb IDs");
            progress.Report(100);
            return;
        }

        var libraryImdbIds = new HashSet<string>(StringComparer.Ordinal);
        for (int i = 0; i < items.Count; i++)
        {
            var imdbId = items[i].GetProviderId(MediaBrowser.Model.Entities.MetadataProvider.Imdb);
            if (!string.IsNullOrWhiteSpace(imdbId))
            {
                libraryImdbIds.Add(imdbId);
            }
        }

        _logger.LogInformation(
            "Found {ItemCount} library items with IMDb IDs ({DistinctIdCount} distinct IDs)",
            items.Count,
            libraryImdbIds.Count);

        if (libraryImdbIds.Count == 0)
        {
            _logger.LogWarning("No valid IMDb IDs found on selected library items — nothing to update");
            progress.Report(100);
            return;
        }

        progress.Report(5);

        // Step 2: Download/cache the ratings file, Step 3: Parse ratings (filtered to library IMDb IDs)
        var downloader = new ImdbFlatFileDownloader(
            _httpClientFactory,
            _loggerFactory.CreateLogger<ImdbFlatFileDownloader>(),
            _dataPath);
        var parser = new ImdbRatingsParser(_loggerFactory.CreateLogger<ImdbRatingsParser>());

        var ratings = await DownloadAndParseWithRetryAsync(
            downloader,
            parser,
            libraryImdbIds,
            progress,
            cancellationToken).ConfigureAwait(false);
        progress.Report(30);
        int lastScanProgressBucket = 30;

        // Step 4: Identify items that need rating updates (without mutating in-memory state)
        var pendingUpdates = new List<(BaseItem Item, BaseItem? Parent, float? OldRating, float NewRating)>();
        int skippedMissingImdbId = 0;
        int skippedBelowMinimumVotes = 0;
        int skippedUnchanged = 0;
        int notFound = 0;
        const int debugSampleLimitPerCategory = 10;
        bool enableItemDebugLogging = config.EnableItemDebugLogging && _logger.IsEnabled(LogLevel.Debug);
        int loggedNotFoundDebugSamples = 0;
        int loggedBelowMinimumDebugSamples = 0;

        for (int i = 0; i < items.Count; i++)
        {
            cancellationToken.ThrowIfCancellationRequested();

            var item = items[i];
            var imdbId = item.GetProviderId(MediaBrowser.Model.Entities.MetadataProvider.Imdb);

            if (string.IsNullOrEmpty(imdbId))
            {
                skippedMissingImdbId++;
            }
            else if (!ratings.TryGetValue(imdbId, out var ratingData))
            {
                if (enableItemDebugLogging && loggedNotFoundDebugSamples < debugSampleLimitPerCategory)
                {
                    loggedNotFoundDebugSamples++;
                    _logger.LogDebug("IMDb ID {ImdbId} not found in ratings file for \"{Name}\"", imdbId, item.Name);
                }
                notFound++;
            }
            else if (ratingData.Votes < config.MinimumVotes)
            {
                if (enableItemDebugLogging && loggedBelowMinimumDebugSamples < debugSampleLimitPerCategory)
                {
                    loggedBelowMinimumDebugSamples++;
                    _logger.LogDebug("Skipping \"{Name}\" — {Votes} votes below minimum {MinVotes}", item.Name, ratingData.Votes, config.MinimumVotes);
                }
                skippedBelowMinimumVotes++;
            }
            else
            {
                var newRating = ratingData.Rating;
                if (item.CommunityRating.HasValue && Math.Abs(item.CommunityRating.Value - newRating) < 0.01f)
                {
                    skippedUnchanged++;
                }
                else
                {
                    pendingUpdates.Add((item, item.GetParent(), item.CommunityRating, newRating));
                }
            }

            double progressPercent = 30 + (60.0 * (i + 1) / items.Count);
            int progressBucket = (int)progressPercent;
            if (progressBucket > lastScanProgressBucket)
            {
                lastScanProgressBucket = progressBucket;
                progress.Report(progressPercent);
            }
        }

        if (enableItemDebugLogging)
        {
            var suppressedNotFoundDebugLines = notFound - loggedNotFoundDebugSamples;
            if (suppressedNotFoundDebugLines > 0)
            {
                _logger.LogDebug(
                    "Suppressed {Count} additional per-item debug logs for IMDb IDs not found in ratings data (sample limit {SampleLimit})",
                    suppressedNotFoundDebugLines,
                    debugSampleLimitPerCategory);
            }

            var suppressedBelowMinimumDebugLines = skippedBelowMinimumVotes - loggedBelowMinimumDebugSamples;
            if (suppressedBelowMinimumDebugLines > 0)
            {
                _logger.LogDebug(
                    "Suppressed {Count} additional per-item debug logs for items below minimum votes (sample limit {SampleLimit})",
                    suppressedBelowMinimumDebugLines,
                    debugSampleLimitPerCategory);
            }
        }

        progress.Report(90);

        // Step 5: Apply ratings and batch save, grouped by parent and chunked
        if (pendingUpdates.Count > 0)
        {
            _logger.LogInformation("Batch saving {Count} updated ratings to database", pendingUpdates.Count);

            const int batchSize = 500;
            var byParent = pendingUpdates.GroupBy(p => p.Parent?.Id ?? Guid.Empty);
            int saved = 0;
            int lastSaveProgressBucket = 90;

            foreach (var group in byParent)
            {
                var parent = group.First().Parent;

                foreach (var chunk in group.Chunk(batchSize))
                {
                    cancellationToken.ThrowIfCancellationRequested();

                    if (parent is null)
                    {
                        // Preserve prior semantics for root/null-parent items.
                        for (int j = 0; j < chunk.Length; j++)
                        {
                            chunk[j].Item.CommunityRating = chunk[j].NewRating;
                            try
                            {
                                await _libraryManager.UpdateItemAsync(
                                    chunk[j].Item,
                                    chunk[j].Parent!, // Preserve prior behavior for root items with no parent.
                                    ItemUpdateType.MetadataEdit,
                                    cancellationToken).ConfigureAwait(false);
                            }
                            catch
                            {
                                chunk[j].Item.CommunityRating = chunk[j].OldRating;
                                throw;
                            }
                        }
                    }
                    else
                    {
                        // Apply ratings immediately before persisting this chunk.
                        var chunkItems = new BaseItem[chunk.Length];
                        for (int j = 0; j < chunk.Length; j++)
                        {
                            chunk[j].Item.CommunityRating = chunk[j].NewRating;
                            chunkItems[j] = chunk[j].Item;
                        }

                        try
                        {
                            await _libraryManager.UpdateItemsAsync(chunkItems, parent, ItemUpdateType.MetadataEdit, cancellationToken).ConfigureAwait(false);
                        }
                        catch
                        {
                            // Revert this chunk's in-memory mutations if the batch save fails/cancels.
                            for (int j = 0; j < chunk.Length; j++)
                            {
                                chunk[j].Item.CommunityRating = chunk[j].OldRating;
                            }

                            throw;
                        }
                    }

                    saved += chunk.Length;

                    double saveProgress = 90 + (10.0 * saved / pendingUpdates.Count);
                    int saveProgressBucket = (int)saveProgress;
                    if (saveProgressBucket > lastSaveProgressBucket)
                    {
                        lastSaveProgressBucket = saveProgressBucket;
                        progress.Report(saveProgress);
                    }
                }
            }
        }

        progress.Report(100);
        var skippedTotal = skippedMissingImdbId + skippedBelowMinimumVotes + skippedUnchanged;
        _logger.LogInformation(
            "IMDb ratings refresh complete: {Updated} updated, {Skipped} skipped ({Unchanged} unchanged, {BelowMinimum} below minimum votes, {MissingImdbId} missing IMDb ID), {NotFound} not found in IMDb ratings",
            pendingUpdates.Count,
            skippedTotal,
            skippedUnchanged,
            skippedBelowMinimumVotes,
            skippedMissingImdbId,
            notFound);
    }

    private async Task<Dictionary<string, (float Rating, int Votes)>> DownloadAndParseWithRetryAsync(
        ImdbFlatFileDownloader downloader,
        ImdbRatingsParser parser,
        IReadOnlySet<string> includeImdbIds,
        IProgress<double> progress,
        CancellationToken cancellationToken)
    {
        try
        {
            var filePath = await GetRatingsFilePathWithTransientRetryAsync(downloader, cancellationToken).ConfigureAwait(false);
            progress.Report(10);
            return await parser.ParseFilteredAsync(filePath, includeImdbIds, cancellationToken).ConfigureAwait(false);
        }
        catch (InvalidDataException ex)
        {
            // Bad data on disk — invalidate cache and re-download.
            _logger.LogWarning(ex,
                "IMDb ratings data failed validation on first attempt; invalidating cache and retrying");

            downloader.InvalidateCache();
            return await RetryDownloadAndParseAsync(
                downloader,
                parser,
                includeImdbIds,
                progress,
                cancellationToken).ConfigureAwait(false);
        }
    }

    private async Task<Dictionary<string, (float Rating, int Votes)>> RetryDownloadAndParseAsync(
        ImdbFlatFileDownloader downloader,
        ImdbRatingsParser parser,
        IReadOnlySet<string> includeImdbIds,
        IProgress<double> progress,
        CancellationToken cancellationToken)
    {
        try
        {
            var filePath = await downloader.GetRatingsFilePathAsync(cancellationToken).ConfigureAwait(false);
            progress.Report(10);
            return await parser.ParseFilteredAsync(filePath, includeImdbIds, cancellationToken).ConfigureAwait(false);
        }
        catch (InvalidDataException retryEx)
        {
            _logger.LogError(retryEx, "IMDb ratings data failed validation after retry");
            throw;
        }
    }

    private async Task<string> GetRatingsFilePathWithTransientRetryAsync(
        ImdbFlatFileDownloader downloader,
        CancellationToken cancellationToken)
    {
        try
        {
            return await downloader.GetRatingsFilePathAsync(cancellationToken).ConfigureAwait(false);
        }
        catch (Exception ex) when (IsTransientNetworkError(ex))
        {
            // Transient download error — try once more after a short delay, or fall back to stale cache.
            _logger.LogWarning(ex, "Transient network error downloading IMDb ratings; retrying once after delay");

            await Task.Delay(TimeSpan.FromSeconds(3), cancellationToken).ConfigureAwait(false);

            try
            {
                return await downloader.GetRatingsFilePathAsync(cancellationToken).ConfigureAwait(false);
            }
            catch (Exception retryEx) when (IsTransientNetworkError(retryEx))
            {
                if (!downloader.HasCacheFile)
                {
                    _logger.LogError(retryEx, "Download failed after retry and no cached ratings file exists");
                    throw;
                }

                _logger.LogWarning(retryEx,
                    "Download failed after retry; falling back to stale cache at {Path}", downloader.CachePath);

                return downloader.CachePath;
            }
        }
    }

    private static bool IsTransientNetworkError(Exception ex)
    {
        return ex is HttpRequestException
            || (ex is IOException && ex is not InvalidDataException);
    }

    private IReadOnlyList<BaseItem> GetLibraryItems(PluginConfiguration config)
    {
        var query = new InternalItemsQuery
        {
            HasImdbId = true,
            IsVirtualItem = false,
            Recursive = true
        };

        var includeTypes = new List<BaseItemKind>();
        if (config.IncludeMovies)
        {
            includeTypes.Add(BaseItemKind.Movie);
        }

        if (config.IncludeSeries)
        {
            includeTypes.Add(BaseItemKind.Series);
            includeTypes.Add(BaseItemKind.Episode);
        }

        if (includeTypes.Count == 0)
        {
            _logger.LogWarning("No library types selected — nothing to update");
            return Array.Empty<BaseItem>();
        }

        query.IncludeItemTypes = includeTypes.ToArray();

        return _libraryManager.GetItemList(query);
    }
}
