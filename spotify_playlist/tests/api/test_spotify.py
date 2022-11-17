from spotify_playlist.api.spotify import Spotify

import os

def test_prepare_authenticate_body():
    # GIVEN
    del os.environ["SPOTIFY_CLIENT_ID"]
    del os.environ["SPOTIFY_CLIENT_SECRET"]
    client = Spotify("client_id", "client_secret")

    # WHEN
    actual = client.authenticate_body()

    # THEN
    expected = {
        "grant_type": "client_credentials",
        "client_id": "client_id",
        "client_secret": "client_secret",
    }

    assert actual == expected


def test_set_access_token_from_response():
    # GIVEN
    client = Spotify("client_id", "client_secret")
    auth_response = {"access_token": "my_token"}

    # WHEN
    client.set_access_token(auth_response)
    actual = client.access_token

    # THEN
    expected = "my_token"

    assert actual == expected


def test_auth_header():
    # GIVEN
    client = Spotify("client_id", "client_secret")
    client.access_token = "my_token"

    # WHEN
    actual = client.auth_headers()

    # THEN
    expected = {"Authorization": "Bearer my_token"}

    assert actual == expected
