import spotipy
import os
import spotipy.util as util
from json.decoder import JSONDecodeError

# Get the user name from terminal
username = 'hidden'
scope = 'playlist-read-private'
## You can find other scopes here: https://developer.spotify.com/documentation/general/guides/scopes/
client_id = 'hidden'
client_secret = 'hidden'
redirect_uri = 'https://www.google.com/callback/'

# Erase cache and prompt for user permission
try:
    token = util.prompt_for_user_token(username, scope, client_id, client_secret, redirect_uri)
except:
    os.remove(f".cache-{username}")
    token = util.prompt_for_user_token(username)

spotify = spotipy.Spotify(auth=token)

def get_artists_from_playlist(playlist_uri):
    '''
    :param playlist_uri: Playlist to analyse
    :return: A dictionary(artist uri : artist name) of all primary artists in a playlist.
    '''
    artists = {}
    playlist_tracks = spotify.playlist_tracks(playlist_id=playlist_uri)
    for song in playlist_tracks['items']:
        if song['track']:
            print(song['track']['artists'][0]['name'])
            artists[song['track']['artists'][0]['uri']] = song['track']['artists'][0]['name']
    return artists
