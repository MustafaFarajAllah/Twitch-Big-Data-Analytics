import os
import requests
from confluent_kafka import Producer
from dotenv import load_dotenv
import json

load_dotenv()

class TwitchAPIProducer:
    def __init__(self):
        self.header = self.twitch_authenticate()
        self.producer_token = self.create_producer()

    def twitch_authenticate(self):
        authentication_request = requests.post(
            "https://id.twitch.tv/oauth2/token",
            params={
                "client_id": os.getenv("TWITCH_CLIENT_ID"),
                "client_secret": os.getenv("TWITCH_CLIENT_SECRET"),
                "grant_type": "client_credentials",
            },
        )
        authentication_request.raise_for_status()
        token = authentication_request.json()["access_token"]
        return {
            "Client-ID": os.getenv("TWITCH_CLIENT_ID"),
            "Authorization": f"Bearer {token}",
        }

    def create_producer(self):
        return Producer({
            "bootstrap.servers": os.getenv("CONFLUENT_BOOTSTRAP_SERVERS"),
            "sasl.mechanisms": "PLAIN",
            "security.protocol": "SASL_SSL",
            "sasl.username": os.getenv("CONFLUENT_API_KEY"),
            "sasl.password": os.getenv("CONFLUENT_API_SECRET"),
            "allow.auto.create.topics": "true",
        })

    def msg_delivery_logs(self, err, msg):
        if err:
            print(f"failed: {err}")
        else:
            print(f"{msg.topic()}  partition {msg.partition()} offset {msg.offset()}")

    def publish(self, topic, records):
        print(f"Publishing {len(records)} records to {topic}")
        for record in records:
            self.producer_token.produce(
                topic,
                key=str(record["id"]),
                value=json.dumps(record).encode("utf-8"),
                on_delivery=self.msg_delivery_logs,
            )
            self.producer_token.poll(0)

    def fetch_data(self, url, params):
        r = requests.get(url, headers=self.header, params=params)
        r.raise_for_status()
        return r.json()

    def Execute_Producer_Pipeline(self):
        streams = self.fetch_data("https://api.twitch.tv/helix/streams", {"first": 100})
        self.publish("stream", streams["data"])

        user_ids = [s["user_id"] for s in streams["data"]]
        game_ids = list({s["game_id"] for s in streams["data"] if s.get("game_id")})

        users = self.fetch_data("https://api.twitch.tv/helix/users", {"id": user_ids})
        self.publish("user", users["data"])

        videos = self.fetch_data("https://api.twitch.tv/helix/videos",{"user_id": user_ids, "first": 100},)
        self.publish("video", videos["data"])

        all_clips = []
        for game_id in game_ids:
            clips = self.fetch_data(
                "https://api.twitch.tv/helix/clips",
                {"game_id": game_id, "first": 20},
            )
            all_clips.extend(clips["data"])
        self.publish("clip", all_clips)

        top_games = self.fetch_data("https://api.twitch.tv/helix/games/top", {"first": 100})
        self.publish("top_game", top_games["data"])

        self.producer_token.flush()


if __name__ == "__main__":
    TwitchAPIProducer().Execute_Producer_Pipeline()
