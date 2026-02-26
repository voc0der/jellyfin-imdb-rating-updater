# Jellyfin IMDb Ratings

A Jellyfin plugin that downloads the [IMDb ratings flat file](https://datasets.imdbws.com/title.ratings.tsv.gz) daily and updates `CommunityRating` on all library items with an IMDb ID. No other metadata is touched.

## Features

- Daily scheduled task (default 3 AM), also triggerable manually from Dashboard
- Downloads and caches the ~2MB compressed IMDb dataset with 23-hour cache
- Configurable minimum votes threshold (default: 1,000)
- Choose which library types to update (Movies, TV Series, or both)
- Progress reporting in the Jellyfin task UI

## Installation

### From Plugin Repository

1. In Jellyfin, go to **Dashboard > Plugins > Repositories**
2. Add: `https://raw.githubusercontent.com/voc0der/jellyfin-imdb-rating-updater/main/manifest.json`
3. Install **IMDb Ratings** from the catalog

### Manual

1. Download the latest release ZIP
2. Extract to your Jellyfin plugins directory
3. Restart Jellyfin

## Configuration

Go to **Dashboard > Plugins > IMDb Ratings**:

- **Minimum Votes** — skip items with fewer IMDb votes than this
- **Include Movies** — update movie ratings
- **Include TV Series** — update series ratings (not individual episodes)

## Building

```bash
dotnet build --configuration Release
```

## License

[MIT](LICENSE)
