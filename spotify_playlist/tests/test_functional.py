from spotify_playlist.get_artists import main as artist_main
from spotify_playlist.get_tracks import main as tracks_main
from spotify_playlist.datasets.tracks import Tracks

import datetime
import os

def delete_file_if_exists(filename):
  if os.path.exists(filename):
    os.remove(filename)

def test_get_tracks():
  delete_file_if_exists("playlist.csv")
  delete_file_if_exists("tracks_2000-01-01.csv")
  
  # GIVEN
  os.environ["SPOTIFY_CLIENT_ID"] = os.getenv("SPOTIFY_CLIENT_ID_TEST")
  os.environ["SPOTIFY_CLIENT_SECRET"] = os.getenv("SPOTIFY_CLIENT_SECRET_TEST")
  extract_date = datetime.datetime(year=2000, month=1, day=1).date()
  with open("playlist.csv", "w") as f:
    f.writelines("37i9dQZEVXbMDoHDwVN2tF")

  # WHEN
  tracks_main(extract_date)

  # THEN
  assert len(Tracks.read("tracks_2000-01-01.csv").index) > 0

def test_get_artists():
  delete_file_if_exists("tracks_2000-01-01.csv")
  delete_file_if_exists("artists_2000-01-01.csv")

  # GIVEN
  os.environ["SPOTIFY_CLIENT_ID"] = os.getenv("SPOTIFY_CLIENT_ID_TEST")
  os.environ["SPOTIFY_CLIENT_SECRET"] = os.getenv("SPOTIFY_CLIENT_SECRET_TEST")
  extract_date = datetime.datetime(year=2000, month=1, day=1).date()
  with open("tracks_2000-01-01.csv", "w") as f:
    f.writelines("playlist_id,track_id,artist_id,date\n")
    f.writelines("37i9dQZEVXbMDoHDwVN2tF,4Dvkj6JhhA12EX05fT7y2e,6KImCVD70vtIoJWnq6nGn3,2022-09-08\n")
    f.writelines("37i9dQZEVXbMDoHDwVN2tF,2tTmW7RDtMQtBk7m2rYeSw,716NhGYqD1jl2wI1Qkgq36,2022-09-08\n")
    

  # WHEN
  artist_main(extract_date)

  # THEN
  assert len(Tracks.read("artists_2000-01-01.csv").index) > 0
