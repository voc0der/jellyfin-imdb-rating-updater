using System;

namespace Jellyfin.Plugin.ImdbRatings.Api;

public class CacheStatusDto
{
    public bool Exists { get; set; }

    public long FileSizeBytes { get; set; }

    public DateTime? LastModifiedUtc { get; set; }

    public double TtlHours { get; set; }

    public bool IsFresh { get; set; }
}
