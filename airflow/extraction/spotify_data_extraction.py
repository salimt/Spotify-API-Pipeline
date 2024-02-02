import pathlib

try:
    import spotipy
except ImportError:
    import subprocess

    subprocess.check_call(["pip", "install", "spotipy"])
    import spotipy

import sys
import os
from datetime import datetime

# import spotipy
import pandas as pd
import configparser

# Read Configuration File
config = configparser.ConfigParser()
script_path = pathlib.Path(__file__).parent.resolve()
config_file = "config.conf"
config_file_path = script_path / config_file
if config_file_path.exists():
    config.read(f"{script_path}/{config_file}")
else:
    print(f"Config file '{config_file_path}' does not exist.")

# Read the configuration file
config.read('config.conf')

# Get the client_id and secret values from the 'spotify config' section
SPOTIPY_CLIENT_ID = config.get('spotify config', 'client_id')
SPOTIPY_CLIENT_SECRET = config.get('spotify config', 'secret')
PLAYLIST_ID = config.get('spotify config', 'playlist_id')

# Authentication
from spotipy.oauth2 import SpotifyClientCredentials
import validation as va


def main():
    # output_name = "track_features.csv"
    output_name = sys.argv[1]
    try:

        if os.path.exists(f"/tmp/{output_name}.csv"):
            return True
        else:
            va.validate_input(output_name)
            spotifyInstance = api_connect(SPOTIPY_CLIENT_ID, SPOTIPY_CLIENT_SECRET)
            extracted_data = get_playlist_tracks(spotifyInstance, PLAYLIST_ID)
            trackFeatures = getFeatures(spotifyInstance, extracted_data)
            import_to_csv(trackFeatures, output_name)

    except Exception as e:
        print(f"Error with file input. Error {e}")
        sys.exit(1)
    date_dag_run = datetime.strptime(output_name, "%Y%m%d")


def api_connect(SPOTIPY_CLIENT_ID, SPOTIPY_CLIENT_SECRET):
    print("API Connection Running...")
    """Connect to Spotify API"""
    try:
        # spotify object to access API
        spotify = spotipy.Spotify(client_credentials_manager=SpotifyClientCredentials(SPOTIPY_CLIENT_ID,
                                                                                      SPOTIPY_CLIENT_SECRET))
        return spotify
    except Exception as e:
        print(f"Unable to connect to API. Error: {e}")
        sys.exit(1)


def getPlaylistbyUsername(spotify, username):
    user = username
    playlists = spotify.user_playlists(user)
    print(type(playlists))

    for key in playlists:
        print(key)

    print('\n')
    while playlists:
        for i, playlist in enumerate(playlists['items']):
            print("%s - %s" % (playlist['uri'], playlist['name']))
        if playlists['next']:
            playlists = spotify.next(playlists)
        else:
            break


def get_playlist_tracks(spotify, chosen_playlist):
    print("Get Playlist Tracks Running...")
    results = spotify.playlist_tracks(chosen_playlist)
    tracks = results['items']
    while results['next']:
        results = spotify.next(results)
        tracks.extend(results['items'])

    # Creating data frame with the information from the chosen playlist
    tracklist = tracks
    # Convert tracklist to DataFrame
    tracklist_df = pd.DataFrame(tracklist)
    # Save the structured DataFrame to a CSV file
    data = pd.DataFrame(tracklist_df)
    # data.to_csv('raw_track_data.csv', index=False)
    print(data.columns)
    return tracklist


def getFeatures(spotify, tracklist):
    print("Get Features Running...")
    # Grabbing audio features for each song
    audio_features_names = []
    audio_features_ids = []
    audio_features_artists = []
    # Iterating over 'tracklist' in order to create some lists which will be on our audio features data frame
    for k, v in enumerate(tracklist):
        info = v['track']
        audio_features_names.append(info['name'])
        audio_features_ids.append(info['id'])
        audio_features_artists.append(info['artists'][0]['name'])

    import math

    # Assuming audio_features_ids is a list
    num_rows = len(audio_features_ids)
    num_lists = math.ceil(num_rows / 50)

    audio_features_lists = [[] for _ in range(num_lists)]
    audio_features = []

    for i, audio_feature_id in enumerate(audio_features_ids):
        audio_features_index = i // 50
        audio_features_lists[audio_features_index].append(audio_feature_id)

    for audio_features_list in audio_features_lists:
        audio_features += spotify.audio_features(audio_features_list)

    # Assuming audio_features is a list of dictionaries
    filtered_audio_features = [af for af in audio_features if isinstance(af, dict)]

    audio_features_df = pd.DataFrame(filtered_audio_features)
    audio_features_df.insert(0, "track", audio_features_names, True)  # using insert() to add a column to our data frame
    audio_features_df.insert(1, "artist", audio_features_artists, True)

    # Function to get genres for an artist
    def get_artist_genres(artist_id):
        try:
            artist_info = spotify.artist(artist_id)
            return artist_info['genres']
        except:
            return []

    # Function to get genres for each track
    def get_track_genres(tracklist):
        track_genres = []
        for track_info in tracklist:
            artists = track_info['track']['artists']
            track_genres_per_artist = []
            for artist in artists:
                artist_id = artist['id']
                genres = get_artist_genres(artist_id)
                track_genres_per_artist.extend(genres)
            track_genres.append(track_genres_per_artist)

        return track_genres

    # Getting track genres
    track_genres = get_track_genres(tracklist)

    # Add the 'genres' column to the DataFrame
    audio_features_df['genres'] = [genre if isinstance(genre, list) else [] for genre in track_genres]

    # If there are more rows in 'audio_features_df' than 'track_genres', fill the extra rows with empty lists
    audio_features_df['genres'] = audio_features_df.apply(
        lambda row: row['genres'] + ([] * (len(track_genres) - len(row['genres']))), axis=1)

    print("Audio Features Columns:", audio_features_df.columns)
    return audio_features_df


def import_to_csv(audio_features_df, file_name):
    print("Importing to CSV... /tmp/")
    audio_features_df.to_csv(f"/tmp/{file_name}.csv", index=False)


if __name__ == "__main__":
    main()
