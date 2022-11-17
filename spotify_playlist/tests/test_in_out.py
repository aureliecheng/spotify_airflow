from spotify_playlist.in_out import in_out
import pandas as pd

def df_equals(actual, expected):
  actual = actual.reset_index(drop=True)
  expected = expected.reset_index(drop=True)

  return actual.equals(expected)

def test_in_out():
  # GIVEN
  previous = pd.DataFrame([
    {"playlist_id": "a", "track_id": "aa", "artist_id": "aaa", "date": "2022-04-04"},
    {"playlist_id": "a", "track_id": "ab", "artist_id": "aaa", "date": "2022-04-04"},
    {"playlist_id": "b", "track_id": "ba", "artist_id": "baa", "date": "2022-04-04"},
    {"playlist_id": "b", "track_id": "bb", "artist_id": "bba", "date": "2022-04-04"},
  ])

  current = pd.DataFrame([
    {"playlist_id": "a", "track_id": "aa", "artist_id": "aaa", "date": "2022-04-04"},
    {"playlist_id": "b", "track_id": "bb", "artist_id": "bba", "date": "2022-04-04"},
    {"playlist_id": "b", "track_id": "bc", "artist_id": "bca", "date": "2022-04-04"},
  ])


  # WHEN
  actual = in_out(previous, current)

  # THEN
  expected = pd.DataFrame([
    {"playlist_id": "b", "artist_id": "baa", "date": "2022-04-04", "status": "out"},
    {"playlist_id": "b", "artist_id": "bca", "date": "2022-04-04", "status": "in"},
  ])

  actual = actual.reset_index(drop=True)
  expected = expected.reset_index(drop=True)

  assert df_equals(actual, expected)
