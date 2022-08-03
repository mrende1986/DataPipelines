import os
import csv
import spotipy
import spotipy.util as util

from config.playlists import spotify_playlists
from tools.playlists import get_artists_from_playlist

username = os.environ.get('USERNAME')
scope = 'playlist-read-private'
client_id = os.environ.get('CLIENT_ID')
client_secret = os.environ.get('CLIENT_SECRET')
redirect_uri = 'https://www.google.com/callback/'


try:
    token = util.prompt_for_user_token(username, scope, client_id, client_secret, redirect_uri)
except:
    os.remove(f".cache-{username}")
    token = util.prompt_for_user_token(username)

spotifyObject = spotipy.Spotify(auth=token)

spotipy_object = spotifyObject.current_user()


PLAYLIST = 'best_blues_2021'


def gather_data():
    
    with open("best_blues_2021.csv", 'w') as file:
        # Here are the columns I'll populate
        header = ['Year Released', 'Album Length', 'Album Name', 'Artist']
        # Set the header row to the keys of the dictionary created
        writer = csv.DictWriter(file, fieldnames=header)
        writer.writeheader()
        
        # Calling a function I created in the tools folder that finds the artist of every
        # song in the playlist input. In this case the playlist is best_blues_2021
        # spotify_playlists looks up the playlist uri. The code can be found in the config folder.
        artists = get_artists_from_playlist(spotify_playlists()[PLAYLIST])
        for artist in artists.keys():
            artists_albums = spotifyObject.artist_albums(artist, album_type='album', limit=50)
            # For every artist in the list of artists I'm returning all their albums
            for album in artists_albums['items']:
                # To reduce multiple inputs of the same album I'm limiting results to US only
                if 'US' in artists_albums['items'][0]['available_markets']:
                    album_data = spotifyObject.album(album['uri'])
                    # For every song in the album
                    album_length_ms = 0
                    for song in album_data['tracks']['items']:
                        album_length_ms = song['duration_ms'] + album_length_ms
                    # Create a new row in csv file
                    writer.writerow({'Year Released': album_data['release_date'][:4],
                                     'Album Length': album_length_ms,
                                     'Album Name': album_data['name'],
                                     'Artist': album_data['artists'][0]['name']})



gather_data()
