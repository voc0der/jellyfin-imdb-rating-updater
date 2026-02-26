using System;
using System.Buffers;
using System.Buffers.Text;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;

namespace Jellyfin.Plugin.ImdbRatings.Providers;

public class ImdbRatingsParser
{
    private const string ExpectedHeader = "tconst\taverageRating\tnumVotes";
    private const int MinExpectedRows = 500_000;
    private const double MaxParseErrorRatio = 0.01;
    private const int ReadBufferSize = 128 * 1024;
    private static readonly byte[] ExpectedHeaderBytes = Encoding.ASCII.GetBytes(ExpectedHeader);

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
        return await ParseInternalAsync(filePath, includeIds: null, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Parses the IMDb TSV file and retains only rows whose tconst is in <paramref name="includeIds"/>.
    /// Full-file validation still runs on all rows.
    /// </summary>
    public async Task<Dictionary<string, (float Rating, int Votes)>> ParseFilteredAsync(
        string filePath,
        IReadOnlySet<string> includeIds,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(includeIds);
        return await ParseInternalAsync(filePath, includeIds, cancellationToken).ConfigureAwait(false);
    }

    private async Task<Dictionary<string, (float Rating, int Votes)>> ParseInternalAsync(
        string filePath,
        IReadOnlySet<string>? includeIds,
        CancellationToken cancellationToken)
    {
        var initialCapacity = includeIds is null ? 1_200_000 : Math.Max(includeIds.Count, 16);
        var ratings = new Dictionary<string, (float Rating, int Votes)>(initialCapacity);
        HashSet<ulong>? includeNumericIds = includeIds is null ? null : BuildNumericIdSet(includeIds);

        using var stream = new FileStream(
            filePath,
            new FileStreamOptions
            {
                Access = FileAccess.Read,
                BufferSize = ReadBufferSize,
                Mode = FileMode.Open,
                Options = FileOptions.Asynchronous | FileOptions.SequentialScan,
                Share = FileShare.Read
            });

        byte[] buffer = ArrayPool<byte>.Shared.Rent(ReadBufferSize);
        int bufferedCount = 0;
        bool headerProcessed = false;
        int lineCount = 0;
        int validRows = 0;
        int parseErrors = 0;

        try
        {
            while (true)
            {
                if (bufferedCount == buffer.Length)
                {
                    var largerBuffer = ArrayPool<byte>.Shared.Rent(buffer.Length * 2);
                    Buffer.BlockCopy(buffer, 0, largerBuffer, 0, bufferedCount);
                    ArrayPool<byte>.Shared.Return(buffer);
                    buffer = largerBuffer;
                }

                int bytesRead = await stream.ReadAsync(
                    buffer.AsMemory(bufferedCount, buffer.Length - bufferedCount),
                    cancellationToken).ConfigureAwait(false);

                int totalBytes = bufferedCount + bytesRead;
                int lineStart = 0;

                while (lineStart < totalBytes)
                {
                    cancellationToken.ThrowIfCancellationRequested();

                    int newlineOffset = new ReadOnlySpan<byte>(buffer, lineStart, totalBytes - lineStart).IndexOf((byte)'\n');
                    if (newlineOffset < 0)
                    {
                        break;
                    }

                    int lineEndExclusive = lineStart + newlineOffset;
                    var lineBytes = new ReadOnlySpan<byte>(buffer, lineStart, lineEndExclusive - lineStart);
                    if (lineBytes.Length > 0 && lineBytes[^1] == (byte)'\r')
                    {
                        lineBytes = lineBytes[..^1];
                    }

                    ProcessLine(lineBytes);
                    lineStart = lineEndExclusive + 1;
                }

                int remaining = totalBytes - lineStart;
                if (remaining > 0)
                {
                    Buffer.BlockCopy(buffer, lineStart, buffer, 0, remaining);
                }

                bufferedCount = remaining;

                if (bytesRead == 0)
                {
                    if (bufferedCount > 0)
                    {
                        var lineBytes = new ReadOnlySpan<byte>(buffer, 0, bufferedCount);
                        if (lineBytes.Length > 0 && lineBytes[^1] == (byte)'\r')
                        {
                            lineBytes = lineBytes[..^1];
                        }

                        ProcessLine(lineBytes);
                        bufferedCount = 0;
                    }

                    break;
                }
            }
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(buffer);
        }

        if (!headerProcessed)
        {
            throw new InvalidDataException(
                $"IMDb ratings file has an invalid or missing header: \"{"(empty file)"}\"");
        }

        if (includeIds is null)
        {
            _logger.LogInformation("Parsed {ValidRows} IMDb ratings from {Total} rows ({Errors} parse errors)",
                ratings.Count, lineCount, parseErrors);
        }
        else
        {
            _logger.LogInformation(
                "Parsed {MatchedRows} matching IMDb ratings from {ValidRows} valid rows in {Total} rows ({Errors} parse errors)",
                ratings.Count, validRows, lineCount, parseErrors);
        }

        // Post-parse sanity checks
        if (validRows == 0)
        {
            throw new InvalidDataException("IMDb ratings file contains header but no valid data rows.");
        }

        if (validRows < MinExpectedRows)
        {
            throw new InvalidDataException(
                $"IMDb ratings file appears truncated: only {validRows} valid rows (expected at least {MinExpectedRows}).");
        }

        if (lineCount > 0 && (double)parseErrors / lineCount > MaxParseErrorRatio)
        {
            throw new InvalidDataException(
                $"IMDb ratings file appears corrupt: {parseErrors} parse errors out of {lineCount} rows ({(double)parseErrors / lineCount:P1}).");
        }

        return ratings;

        void ProcessLine(ReadOnlySpan<byte> lineBytes)
        {
            if (!headerProcessed)
            {
                if (!lineBytes.SequenceEqual(ExpectedHeaderBytes))
                {
                    throw new InvalidDataException(
                        $"IMDb ratings file has an invalid or missing header: \"{DecodeHeaderForError(lineBytes)}\"");
                }

                headerProcessed = true;
                return;
            }

            lineCount++;

            int firstTab = lineBytes.IndexOf((byte)'\t');
            int secondTab = firstTab < 0 ? -1 : lineBytes[(firstTab + 1)..].IndexOf((byte)'\t');
            if (firstTab < 0 || secondTab < 0)
            {
                parseErrors++;
                return;
            }

            secondTab += firstTab + 1;

            var tconstBytes = lineBytes[..firstTab];
            var ratingBytes = lineBytes[(firstTab + 1)..secondTab];
            var votesBytes = lineBytes[(secondTab + 1)..];

            if (!Utf8Parser.TryParse(ratingBytes, out float rating, out int ratingConsumed)
                || ratingConsumed != ratingBytes.Length
                || !Utf8Parser.TryParse(votesBytes, out int votes, out int votesConsumed)
                || votesConsumed != votesBytes.Length)
            {
                parseErrors++;
                return;
            }

            validRows++;

            if (includeIds is null)
            {
                ratings[Encoding.ASCII.GetString(tconstBytes)] = (rating, votes);
                return;
            }

            if (includeNumericIds is not null
                && TryParseImdbIdNumber(tconstBytes, out var imdbIdNumber)
                && includeNumericIds.Contains(imdbIdNumber))
            {
                ratings[Encoding.ASCII.GetString(tconstBytes)] = (rating, votes);
            }
        }
    }

    private static string DecodeHeaderForError(ReadOnlySpan<byte> headerBytes)
    {
        if (headerBytes.Length == 0)
        {
            return "(empty file)";
        }

        return Encoding.UTF8.GetString(headerBytes);
    }

    private static HashSet<ulong> BuildNumericIdSet(IReadOnlySet<string> includeIds)
    {
        var numericIds = new HashSet<ulong>();
        foreach (var includeId in includeIds)
        {
            if (TryParseImdbIdNumber(includeId, out var numericId))
            {
                numericIds.Add(numericId);
            }
        }

        return numericIds;
    }

    private static bool TryParseImdbIdNumber(string imdbId, out ulong numericId)
    {
        numericId = 0;

        if (string.IsNullOrEmpty(imdbId) || imdbId.Length < 3 || imdbId[0] != 't' || imdbId[1] != 't')
        {
            return false;
        }

        for (int i = 2; i < imdbId.Length; i++)
        {
            char c = imdbId[i];
            if (c < '0' || c > '9')
            {
                return false;
            }

            numericId = (numericId * 10) + (ulong)(c - '0');
        }

        return true;
    }

    private static bool TryParseImdbIdNumber(ReadOnlySpan<byte> imdbId, out ulong numericId)
    {
        numericId = 0;

        if (imdbId.Length < 3 || imdbId[0] != (byte)'t' || imdbId[1] != (byte)'t')
        {
            return false;
        }

        for (int i = 2; i < imdbId.Length; i++)
        {
            byte c = imdbId[i];
            if (c < (byte)'0' || c > (byte)'9')
            {
                return false;
            }

            numericId = (numericId * 10) + (ulong)(c - (byte)'0');
        }

        return true;
    }
}
