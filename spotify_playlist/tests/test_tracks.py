from spotify_playlist.get_tracks import parse_tracks
import datetime

def test_tracks_parsing():
  # GIVEN
  extract_date = datetime.datetime(year=2022, month=8, day=3).date()
  playlist_id = "playlist1"
  input = {
    "href": "https://api.spotify.com/v1/me/shows?offset=0&limit=20\n",
    "items": [
      {
        'added_at': '2022-09-08T10:21:32Z',
        'added_by': {'external_urls': {'spotify': 'https://open.spotify.com/user/'}, 'href': 'https://api.spotify.com/v1/users/', 'id': '', 'type': 'user', 'uri': 'spotify:user:'},
        'is_local': False,
        'primary_color': None,
        'track': {
          'album': {
            'album_type': 'album',
            'artists': [
              {
                'external_urls': {'spotify': 'https://open.spotify.com/artist/7Ln80lUS6He07XvHI8qqHH'},
                'href': 'https://api.spotify.com/v1/artists/7Ln80lUS6He07XvHI8qqHH', 
                'id': '7Ln80lUS6He07XvHI8qqHH', 
                'name': 'Arctic Monkeys', 
                'type': 'artist', 
                'uri': 'spotify:artist:7Ln80lUS6He07XvHI8qqHH'
              }
            ],
            'available_markets': ['CA', 'US'],
            'external_urls': {'spotify': 'https://open.spotify.com/album/6rsQnwaoJHxXJRCDBPkBRw'},
            'href': 'https://api.spotify.com/v1/albums/6rsQnwaoJHxXJRCDBPkBRw',
            'id': '6rsQnwaoJHxXJRCDBPkBRw',
            'images': [{'height': 640, 'url': 'https://i.scdn.co/image/ab67616d0000b2730c8ac83035e9588e8ad34b90', 'width': 640}, {'height': 300, 'url': 'https://i.scdn.co/image/ab67616d00001e020c8ac83035e9588e8ad34b90', 'width': 300}, {'height': 64, 'url': 'https://i.scdn.co/image/ab67616d000048510c8ac83035e9588e8ad34b90', 'width': 64}],
            'name': 'Favourite Worst Nightmare (Standard Version)',
            'release_date': '2007-04-24',
            'release_date_precision': 'day',
            'total_tracks': 12,
            'type': 'album', 
            'uri': 'spotify:album:6rsQnwaoJHxXJRCDBPkBRw'
          },
          'artists': [
            {
              'external_urls': {'spotify': 'https://open.spotify.com/artist/7Ln80lUS6He07XvHI8qqHH'},
              'href': 'https://api.spotify.com/v1/artists/7Ln80lUS6He07XvHI8qqHH',
              'id': '7Ln80lUS6He07XvHI8qqHH',
              'name': 'Arctic Monkeys',
              'type': 'artist',
              'uri': 'spotify:artist:7Ln80lUS6He07XvHI8qqHH'
            }
          ],
          'available_markets': ['CA', 'US'],
          'disc_number': 1,
          'duration_ms': 253586,
          'episode': False,
          'explicit': False,
          'external_ids': {'isrc': 'GBCEL0700074'},
          'external_urls': {'spotify': 'https://open.spotify.com/track/58ge6dfP91o9oXMzq3XkIS'},
          'href': 'https://api.spotify.com/v1/tracks/58ge6dfP91o9oXMzq3XkIS', 
          'id': '58ge6dfP91o9oXMzq3XkIS', 
          'is_local': False, 
          'name': '505', 
          'popularity': 82, 
          'preview_url': 'https://p.scdn.co/mp3-preview/47aca114cd367609671d27ba494c0d90cc5abf48?cid=818e60553ddc4989bd26d9d57ac4229b', 
          'track': True, 
          'track_number': 12, 
          'type': 'track', 
          'uri': 'spotify:track:58ge6dfP91o9oXMzq3XkIS'
          },
        'video_thumbnail': {'url': None}
      },
      {
        'track': {
          'id': 'track1',
          'artists': [
            {
              'id': 'artist2',
              'name': 'test',
            },
            {
              'id': 'artist3',
              'name': 'test',
            }
          ]
        }
      }
    ],
    "limit": 20,
    "next": "https://api.spotify.com/v1/me/shows?offset=1&limit=1",
    "offset": 0,
    "previous": "https://api.spotify.com/v1/me/shows?offset=1&limit=1",
    "total": 4
  }

  # WHEN
  actual = []
  parse_tracks(extract_date, playlist_id, input, actual)

  # THEN
  expected = [
    {"date": extract_date, "track_id": "58ge6dfP91o9oXMzq3XkIS", "artist_id": "7Ln80lUS6He07XvHI8qqHH", "playlist_id": "playlist1"},
    {"date": extract_date, "track_id": "track1", "artist_id": "artist2", "playlist_id": "playlist1"},
    {"date": extract_date, "track_id": "track1", "artist_id": "artist3", "playlist_id": "playlist1"}
  ]
  assert actual == expected