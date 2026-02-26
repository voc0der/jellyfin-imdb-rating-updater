using MediaBrowser.Model.Plugins;

namespace Jellyfin.Plugin.ImdbRatings.Configuration;

public class PluginConfiguration : BasePluginConfiguration
{
    public int MinimumVotes { get; set; } = 1000;

    public bool IncludeMovies { get; set; } = true;

    public bool IncludeSeries { get; set; } = true;
}
