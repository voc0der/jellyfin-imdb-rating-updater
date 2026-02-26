using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;

namespace Jellyfin.Plugin.ImdbRatings.Providers;

public class ImdbRatingsParser
{
    private const string ExpectedHeader = "tconst\taverageRating\tnumVotes";
    private const int MinExpectedRows = 500_000;
    private const double MaxParseErrorRatio = 0.01;

    private readonly ILogger<ImdbRatingsParser> _logger;

    public ImdbRatingsParser(ILogger<ImdbRatingsParser> logger)
    {
        _logger = logger;
    }

    /// <summary>
    /// Parses the IMDb TSV file into a dictionary of tconst -> (averageRating, numVotes).
    /// </summary>
    public async Task<Dictionary<string, (float Rating, int Votes)>> ParseAsync(string filePath, CancellationToken cancellationToken)
    {
        var ratings = new Dictionary<string, (float Rating, int Votes)>(1_200_000);

        using var reader = new StreamReader(filePath);

        // Validate header line exactly
        var header = await reader.ReadLineAsync(cancellationToken).ConfigureAwait(false);
        if (!string.Equals(header, ExpectedHeader, StringComparison.Ordinal))
        {
            throw new InvalidDataException(
                $"IMDb ratings file has an invalid or missing header: \"{header ?? "(empty file)"}\"");
        }

        int lineCount = 0;
        int parseErrors = 0;

        while (await reader.ReadLineAsync(cancellationToken).ConfigureAwait(false) is { } line)
        {
            cancellationToken.ThrowIfCancellationRequested();
            lineCount++;

            var firstTab = line.IndexOf('\t');
            var secondTab = line.IndexOf('\t', firstTab + 1);

            if (firstTab < 0 || secondTab < 0)
            {
                parseErrors++;
                continue;
            }

            var tconst = line.AsSpan(0, firstTab);
            var ratingSpan = line.AsSpan(firstTab + 1, secondTab - firstTab - 1);
            var votesSpan = line.AsSpan(secondTab + 1);

            if (!float.TryParse(ratingSpan, NumberStyles.Float, CultureInfo.InvariantCulture, out var rating)
                || !int.TryParse(votesSpan, NumberStyles.Integer, CultureInfo.InvariantCulture, out var votes))
            {
                parseErrors++;
                continue;
            }

            ratings[tconst.ToString()] = (rating, votes);
        }

        _logger.LogInformation("Parsed {ValidRows} IMDb ratings from {Total} rows ({Errors} parse errors)",
            ratings.Count, lineCount, parseErrors);

        // Post-parse sanity checks
        if (ratings.Count == 0)
        {
            throw new InvalidDataException("IMDb ratings file contains header but no valid data rows.");
        }

        if (ratings.Count < MinExpectedRows)
        {
            throw new InvalidDataException(
                $"IMDb ratings file appears truncated: only {ratings.Count} valid rows (expected at least {MinExpectedRows}).");
        }

        if (lineCount > 0 && (double)parseErrors / lineCount > MaxParseErrorRatio)
        {
            throw new InvalidDataException(
                $"IMDb ratings file appears corrupt: {parseErrors} parse errors out of {lineCount} rows ({(double)parseErrors / lineCount:P1}).");
        }

        return ratings;
    }
}
