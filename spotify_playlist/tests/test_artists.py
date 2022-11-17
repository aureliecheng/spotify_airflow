from spotify_playlist.get_artists import parse_artist
import datetime
def test_parse_artist():
  # GIVEN
  extract_date = datetime.datetime(year=2022, month=8, day=3).date()
  artist_id = "123456"
  input = {
    "external_urls": {
      "spotify": "string"
    },
    "followers": {
      "href": "string",
      "total": 0
    },
    "genres": [
      "Prog rock",
      "Grunge"
    ],
    "href": "string",
    "id": "string",
    "images": [
      {
        "url": "https://i.scdn.co/image/ab67616d00001e02ff9ca10b55ce82ae553c8228\n",
        "height": 300,
        "width": 300
      }
    ],
    "name": "art_name",
    "popularity": 0,
    "type": "artist",
    "uri": "string"
  }

  # WHEN
  actual = parse_artist(extract_date, artist_id, input)

  # THEN
  expected = {
    "id": "123456",
    "name": "art_name",
    "popularity": 0,
    "date": extract_date
  }

  assert actual == expected