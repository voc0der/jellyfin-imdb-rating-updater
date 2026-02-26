using System;
using System.Collections.Generic;
using Jellyfin.Plugin.ImdbRatings.Configuration;
using MediaBrowser.Common.Configuration;
using MediaBrowser.Common.Plugins;
using MediaBrowser.Model.Plugins;
using MediaBrowser.Model.Serialization;

namespace Jellyfin.Plugin.ImdbRatings;

public class Plugin : BasePlugin<PluginConfiguration>, IHasWebPages
{
    public Plugin(IApplicationPaths applicationPaths, IXmlSerializer xmlSerializer)
        : base(applicationPaths, xmlSerializer)
    {
        Instance = this;
    }

    public override string Name => "IMDb Ratings";

    public override Guid Id => Guid.Parse("f5a3c7e1-9b2d-4f6a-8e0c-1d3b5a7c9e2f");

    public override string Description => "Downloads the IMDb ratings flat file daily and updates CommunityRating on all library items with an IMDb ID.";

    public static Plugin? Instance { get; private set; }

    public IEnumerable<PluginPageInfo> GetPages()
    {
        return new[]
        {
            new PluginPageInfo
            {
                Name = Name,
                EmbeddedResourcePath = GetType().Namespace + ".Configuration.configPage.html"
            }
        };
    }
}
