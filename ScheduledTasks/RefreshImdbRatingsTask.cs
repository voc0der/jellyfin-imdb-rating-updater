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
                Type = TaskTriggerInfo.TriggerDaily,
                TimeOfDayTicks = TimeSpan.FromHours(3).Ticks
            }
        };
    }

    public async Task ExecuteAsync(IProgress<double> progress, CancellationToken cancellationToken)
    {
        var config = Plugin.Instance?.Configuration ?? new PluginConfiguration();

        _logger.LogInformation("Starting IMDb ratings refresh (minVotes={MinVotes}, movies={Movies}, series={Series})",
            config.MinimumVotes, config.IncludeMovies, config.IncludeSeries);

        // Step 1: Download/cache the ratings file, Step 2: Parse ratings
        progress.Report(0);
        var downloader = new ImdbFlatFileDownloader(
            _httpClientFactory,
            _loggerFactory.CreateLogger<ImdbFlatFileDownloader>(),
            _dataPath);
        var parser = new ImdbRatingsParser(_loggerFactory.CreateLogger<ImdbRatingsParser>());

        var ratings = await DownloadAndParseWithRetryAsync(downloader, parser, progress, cancellationToken).ConfigureAwait(false);
        progress.Report(30);

        // Step 3: Get library items
        var items = GetLibraryItems(config);
        _logger.LogInformation("Found {Count} library items with IMDb IDs", items.Count);

        if (items.Count == 0)
        {
            progress.Report(100);
            return;
        }

        // Step 4: Identify items that need rating updates
        var itemsToUpdate = new List<BaseItem>();
        int skipped = 0;
        int notFound = 0;

        for (int i = 0; i < items.Count; i++)
        {
            cancellationToken.ThrowIfCancellationRequested();

            var item = items[i];
            var imdbId = item.GetProviderId(MediaBrowser.Model.Entities.MetadataProvider.Imdb);

            if (string.IsNullOrEmpty(imdbId))
            {
                skipped++;
            }
            else if (!ratings.TryGetValue(imdbId, out var ratingData))
            {
                _logger.LogDebug("IMDb ID {ImdbId} not found in ratings file for \"{Name}\"", imdbId, item.Name);
                notFound++;
            }
            else if (ratingData.Votes < config.MinimumVotes)
            {
                _logger.LogDebug("Skipping \"{Name}\" — {Votes} votes below minimum {MinVotes}", item.Name, ratingData.Votes, config.MinimumVotes);
                skipped++;
            }
            else
            {
                var newRating = ratingData.Rating;
                if (item.CommunityRating.HasValue && Math.Abs(item.CommunityRating.Value - newRating) < 0.01f)
                {
                    skipped++;
                }
                else
                {
                    item.CommunityRating = newRating;
                    itemsToUpdate.Add(item);
                }
            }

            double progressPercent = 30 + (60.0 * (i + 1) / items.Count);
            progress.Report(progressPercent);
        }

        // Step 5: Batch save updated items, grouped by parent and chunked
        if (itemsToUpdate.Count > 0)
        {
            _logger.LogInformation("Batch saving {Count} updated ratings to database", itemsToUpdate.Count);

            const int batchSize = 500;
            var byParent = itemsToUpdate.GroupBy(i => i.GetParent()?.Id ?? Guid.Empty);
            int saved = 0;

            foreach (var group in byParent)
            {
                var parent = group.First().GetParent() ?? _libraryManager.RootFolder;

                foreach (var chunk in group.Chunk(batchSize))
                {
                    cancellationToken.ThrowIfCancellationRequested();
                    await _libraryManager.UpdateItemsAsync(chunk, parent, ItemUpdateType.MetadataEdit, cancellationToken).ConfigureAwait(false);
                    saved += chunk.Length;

                    double saveProgress = 90 + (10.0 * saved / itemsToUpdate.Count);
                    progress.Report(saveProgress);
                }
            }
        }

        progress.Report(100);
        _logger.LogInformation("IMDb ratings refresh complete: {Updated} updated, {Skipped} skipped, {NotFound} not found",
            itemsToUpdate.Count, skipped, notFound);
    }

    private async Task<Dictionary<string, (float Rating, int Votes)>> DownloadAndParseWithRetryAsync(
        ImdbFlatFileDownloader downloader,
        ImdbRatingsParser parser,
        IProgress<double> progress,
        CancellationToken cancellationToken)
    {
        try
        {
            var filePath = await downloader.GetRatingsFilePathAsync(cancellationToken).ConfigureAwait(false);
            progress.Report(10);
            return await parser.ParseAsync(filePath, cancellationToken).ConfigureAwait(false);
        }
        catch (InvalidDataException ex)
        {
            _logger.LogWarning(ex,
                "IMDb ratings data failed validation/decompression on first attempt; invalidating cache and retrying once");

            var invalidated = downloader.InvalidateCache();
            if (!invalidated)
            {
                _logger.LogWarning("No cached IMDb ratings files were removed before retry");
            }

            try
            {
                var filePath = await downloader.GetRatingsFilePathAsync(cancellationToken).ConfigureAwait(false);
                progress.Report(10);
                return await parser.ParseAsync(filePath, cancellationToken).ConfigureAwait(false);
            }
            catch (InvalidDataException retryEx)
            {
                _logger.LogError(retryEx, "IMDb ratings data failed validation/decompression after retry");
                throw;
            }
        }
    }

    private List<BaseItem> GetLibraryItems(PluginConfiguration config)
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
        }

        if (includeTypes.Count == 0)
        {
            _logger.LogWarning("No library types selected — nothing to update");
            return new List<BaseItem>();
        }

        query.IncludeItemTypes = includeTypes.ToArray();

        return _libraryManager.GetItemList(query);
    }
}
