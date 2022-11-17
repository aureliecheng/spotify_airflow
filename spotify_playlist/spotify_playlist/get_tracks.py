from sys import _current_frames
from api.spotify import Spotify
from datasets.tracks import Tracks
import datetime

def parse_tracks(extract_date, playlist_id, tracks, result):
  for item in tracks.get("items", []):
    for artist in item.get("track", {}).get("artists", []):
      row = {
        Tracks.PLAYLIST_ID: playlist_id,
        Tracks.TRACK_ID: item.get("track").get("id"),
        Tracks.ARTIST_ID: artist.get("id"),
        Tracks.DATE: extract_date
      }
      result.append(row)


def main(current_date = datetime.datetime.now().date()):
  spotify_client = Spotify()
  spotify_client.authenticate()

  filename = "/home/simplon/airflow/spotify_playlist/spotify_playlist/playlist.csv"
  with open(filename) as playlists:
    result = []
    for playlist_id in playlists:
        playlist_id = playlist_id.rstrip("\n")
        if playlist_id == "":
          pass

        tracks = spotify_client.playlist_tracks(playlist_id)
        parse_tracks(current_date, playlist_id, tracks, result)
        Tracks.write(result, f"/home/simplon/airflow/spotify_playlist/spotify_playlist/tracks_{current_date.isoformat()}.csv")

if __name__ == "__main__":
  main()

