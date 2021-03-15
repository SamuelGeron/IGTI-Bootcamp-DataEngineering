# Imports
import json
from tweepy import OAuthHandler, Stream, StreamListener
from datetime import datetime

# Access Keys
consumer_key = "i4yTWoWw1XVm5VkZfs2JFCjP9"
consumer_secret = "EwOMz7DIuNXczlGLh2mw2V9CXiWKfnzAiGyuieJYjCA0pz9VL2"

access_token = "1112707342876008450-POGZDGukHkPdghlBLVBBTsKjWEaTqq"
access_token_secret = "elUIcqWqi2FZqjuAACQI9CticYBJpYVtYiCNUYKSU5yqx"

# Define output file to store the collected tweets
todays_date = datetime.now().strftime("%Y-%m-%d-%H-%M-%S")
out = open(f"collected_tweets_{todays_date}.txt", "w")

# Implementar classe para conexao com o twitter
class MyListener(StreamListener):

    def on_data(self, data):
        itemString = json.dumps(data)
        out.write(itemString + "\n")
        return True

    def on_error(self, status):
        print(status)

# Implementar a funcao Main
if __name__ == "__main__":
    l = MyListener()
    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)

    stream = Stream(auth, l)
    stream.filter(
        languages=["pt-BR"],
        track=["Disney Plus","Netflix","HBOGO","Globoplay"]
    )
