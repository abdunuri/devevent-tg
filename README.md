# DevEvent Telegram Worker

DevEvent Telegram Worker ingests event posts from Telegram channels and stores structured event candidates in MongoDB for review by the DevEvent web app.

## Main Concept

The worker listens for channel posts through the Telegram Bot API. It saves every accepted post into `raw_posts`, filters by event-related keywords, validates strict event post formatting, and creates review-ready records in `pending_events`.

## Data Flow

1. Telegram channel post arrives.
2. The worker checks channel allowlists and keyword filters.
3. Raw content is deduplicated and stored in MongoDB.
4. Strictly formatted posts with an image are parsed into event fields.
5. Parsed events are inserted into `pending_events` for admin approval.

## Tech Stack

- Python
- Telegram Bot API
- MongoDB / PyMongo
- Requests
- `python-dotenv`

## Required Environment Variables

```env
TELEGRAM_BOT_TOKEN=your_bot_token
MONGODB_URI=your_mongodb_connection_string
MONGODB_DB=test
TELEGRAM_CHANNELS=@channelname,-1001234567890
TELEGRAM_KEYWORDS=hackathon,meetup,conference,workshop,event
APPROVAL_NOTIFY_CHAT_ID=optional_chat_id
```

## Run

```bash
pip install -r requirements.txt
python bot.py
```

Use this worker together with the DevEvent web app's `/admin/pending` review flow.
