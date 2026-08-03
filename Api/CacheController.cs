using System;
using System.IO;
using System.Net.Mime;
using MediaBrowser.Common.Configuration;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;

namespace Jellyfin.Plugin.ImdbRatings.Api;

[ApiController]
[Route("ImdbRatings/Cache")]
[Authorize(Policy = "RequiresElevation")]
[Produces(MediaTypeNames.Application.Json)]
public class CacheController : ControllerBase
{
    private static readonly TimeSpan CacheMaxAge = TimeSpan.FromHours(23);

    private readonly string _cachePath;

    public CacheController(IApplicationPaths applicationPaths)
    {
        _cachePath = Path.Join(applicationPaths.DataPath, "imdb-ratings-cache", "title.ratings.tsv");
    }

    /// <summary>
    /// Gets the current IMDb ratings cache status.
    /// </summary>
    [HttpGet("Status")]
    [ProducesResponseType(StatusCodes.Status200OK)]
    public ActionResult<CacheStatusDto> GetStatus()
    {
        if (!System.IO.File.Exists(_cachePath))
        {
            return Ok(new CacheStatusDto
            {
                Exists = false,
                FileSizeBytes = 0,
                LastModifiedUtc = null,
                TtlHours = CacheMaxAge.TotalHours,
                IsFresh = false
            });
        }

        var info = new FileInfo(_cachePath);
        var lastWrite = info.LastWriteTimeUtc;
        var age = DateTime.UtcNow - lastWrite;

        return Ok(new CacheStatusDto
        {
            Exists = true,
            FileSizeBytes = info.Length,
            LastModifiedUtc = lastWrite,
            TtlHours = CacheMaxAge.TotalHours,
            IsFresh = age < CacheMaxAge
        });
    }

    /// <summary>
    /// Deletes the cached IMDb ratings file so it will be re-downloaded on next task run.
    /// </summary>
    [HttpDelete]
    [ProducesResponseType(StatusCodes.Status204NoContent)]
    [ProducesResponseType(StatusCodes.Status404NotFound)]
    public ActionResult Delete()
    {
        var deleted = false;

        deleted |= TryDeleteFile(_cachePath);
        deleted |= TryDeleteFile(_cachePath + ".tmp");

        if (!deleted)
        {
            return NotFound();
        }

        return NoContent();
    }

    private static bool TryDeleteFile(string path)
    {
        if (!System.IO.File.Exists(path))
        {
            return false;
        }

        System.IO.File.Delete(path);
        return true;
    }
}
